//! Source use cases.
//!
//! Purpose: coordinate source creation and reads plus operator-controlled
//! enable/disable and scheduling-gate changes.
//!
//! A source URL is resolved through acquisition interfaces before persistence.
//! Source configuration changes must invalidate the associated feed cache and
//! may create a deduplicated `source_sync` or `feed_rebuild` job. Every source
//! references the stable WeRead account used for authenticated list operations
//! and owns a monotonic `feed_revision` plus an explicit scheduling gate.
//!
//! Feed-visible changes increment the revision and invalidate/rebuild the cache
//! in a shared persistence `UnitOfWork`. Operator actions explicitly clear
//! `authentication_required` or `risk_controlled`; merely reaching another due
//! timestamp must not clear those states. Feed-token lifecycle is owned by its
//! separate application service; deletion and feed-cache invalidation are still
//! future service operations.
//!
//! Non-responsibilities: direct SQL, browser selectors, article fetching, or
//! rendering RSS bytes. PostgreSQL concurrency and duplicate-source handling
//! belong to repositories and transactions. The source repository now provides
//! the transaction-scoped primitive. This first executable slice composes
//! validated source persistence with an atomic initial-job policy; feed-token
//! and feed-cache lifecycle work remains separate.

use chrono::{DateTime, Utc};
use serde_json::json;
use thiserror::Error;

use crate::{
    domain::{
        job::{JobType, NewJob},
        source::{NewSource, SchedulingGate, Source, SourceId},
    },
    persistence::{
        repositories::{
            job_repository::{JobEnqueueTransaction, JobRepositoryError},
            source_repository::{
                PostgresSourceRepository, SourceRepository, SourceRepositoryError,
                SourceTransactionRepository,
            },
        },
        unit_of_work::{UnitOfWork, UnitOfWorkError, UnitOfWorkFactory},
    },
};

/// Errors raised by source lifecycle orchestration.
#[derive(Debug, Error)]
pub enum SourceServiceError {
    /// The source repository rejected a domain value or could not read/write it.
    #[error(transparent)]
    Source(#[from] SourceRepositoryError),
    /// The initial source-sync job could not be persisted.
    #[error(transparent)]
    Job(#[from] JobRepositoryError),
    /// The shared transaction could not be started or committed.
    #[error(transparent)]
    UnitOfWork(#[from] UnitOfWorkError),
}

/// Application-facing source read port used by source use cases.
///
/// The port keeps source orchestration independent from PostgreSQL and makes
/// read behavior unit-testable. The PostgreSQL repository is one adapter; an
/// administrative API or another persistence implementation can provide a
/// different adapter without changing this service.
#[allow(async_fn_in_trait)]
pub trait SourceReader: Clone + Send + Sync {
    /// Finds one source by durable identity.
    async fn find(&self, source_id: SourceId) -> Result<Option<Source>, SourceServiceError>;

    /// Finds one source by normalized book identity.
    async fn find_by_book_id(&self, book_id: &str) -> Result<Option<Source>, SourceServiceError>;
}

/// Application-facing transaction port for source lifecycle mutations.
///
/// The port deliberately exposes no SQLx transaction or independent commit
/// operation to callers other than the enclosing service. Source creation and
/// its initial job therefore remain one atomic unit, while tests can exercise
/// the orchestration with an in-memory implementation.
#[allow(async_fn_in_trait)]
pub trait SourceUnitOfWork {
    /// Inserts one validated source.
    async fn insert_source(&mut self, source: NewSource) -> Result<Source, SourceServiceError>;

    /// Enqueues one source-sync job in the current transaction.
    async fn enqueue_source_sync(&mut self, job: NewJob) -> Result<(), SourceServiceError>;

    /// Changes whether automatic source scheduling is enabled.
    async fn set_enabled(
        &mut self,
        source_id: SourceId,
        enabled: bool,
    ) -> Result<Source, SourceServiceError>;

    /// Changes the operator-controlled scheduling gate.
    async fn set_scheduling_gate(
        &mut self,
        source_id: SourceId,
        gate: SchedulingGate,
    ) -> Result<Source, SourceServiceError>;

    /// Commits the source lifecycle transaction.
    async fn commit(self) -> Result<(), SourceServiceError>
    where
        Self: Sized;
}

/// Application-facing factory for source lifecycle transactions.
#[allow(async_fn_in_trait)]
pub trait SourceUnitOfWorkFactory: Clone + Send + Sync {
    /// Transaction type created by this factory.
    type Transaction<'a>: SourceUnitOfWork + 'a
    where
        Self: 'a;

    /// Begins a source lifecycle transaction.
    async fn begin(&self) -> Result<Self::Transaction<'_>, SourceServiceError>;
}

impl SourceReader for PostgresSourceRepository {
    async fn find(&self, source_id: SourceId) -> Result<Option<Source>, SourceServiceError> {
        Ok(SourceRepository::find(self, source_id).await?)
    }

    async fn find_by_book_id(&self, book_id: &str) -> Result<Option<Source>, SourceServiceError> {
        Ok(SourceRepository::find_by_book_id(self, book_id).await?)
    }
}

impl SourceUnitOfWorkFactory for UnitOfWorkFactory {
    type Transaction<'a> = UnitOfWork<'a>;

    async fn begin(&self) -> Result<Self::Transaction<'_>, SourceServiceError> {
        Ok(UnitOfWorkFactory::begin(self).await?)
    }
}

impl SourceUnitOfWork for UnitOfWork<'_> {
    async fn insert_source(&mut self, source: NewSource) -> Result<Source, SourceServiceError> {
        let mut sources = self.source();
        Ok(sources.insert(source).await?)
    }

    async fn enqueue_source_sync(&mut self, job: NewJob) -> Result<(), SourceServiceError> {
        let mut queue = self.job_enqueue();
        JobEnqueueTransaction::enqueue_job(&mut queue, job).await?;
        Ok(())
    }

    async fn set_enabled(
        &mut self,
        source_id: SourceId,
        enabled: bool,
    ) -> Result<Source, SourceServiceError> {
        let mut sources = self.source();
        Ok(sources.set_enabled(source_id, enabled).await?)
    }

    async fn set_scheduling_gate(
        &mut self,
        source_id: SourceId,
        gate: SchedulingGate,
    ) -> Result<Source, SourceServiceError> {
        let mut sources = self.source();
        Ok(sources.set_scheduling_gate(source_id, gate).await?)
    }

    async fn commit(self) -> Result<(), SourceServiceError> {
        Ok(UnitOfWork::commit(self).await?)
    }
}

/// Source lifecycle use cases backed by PostgreSQL repositories.
///
/// The default type parameters provide the PostgreSQL composition used by the
/// application. The fields themselves depend on application-facing ports so
/// orchestration can be unit-tested with an in-memory implementation and later
/// composed with another persistence adapter.
#[derive(Clone)]
pub struct SourceService<S = PostgresSourceRepository, U = UnitOfWorkFactory> {
    sources: S,
    unit_of_work: U,
}

impl<S, U> SourceService<S, U>
where
    S: SourceReader,
    U: SourceUnitOfWorkFactory,
{
    /// Creates source use cases over one shared PostgreSQL pool.
    pub fn new(sources: S, unit_of_work: U) -> Self {
        Self {
            sources,
            unit_of_work,
        }
    }

    /// Creates a source and, when automatically eligible, its initial sync job
    /// in one transaction.
    ///
    /// If either insert fails, dropping the unit of work rolls back both rows.
    /// A disabled or operator-blocked source is persisted without a queued job;
    /// it can be resumed by a later explicit operator action or scheduler pass.
    pub async fn create(&self, source: NewSource) -> Result<Source, SourceServiceError> {
        let mut unit_of_work = self.unit_of_work.begin().await?;
        let created = unit_of_work.insert_source(source).await?;

        if created.enabled() && created.scheduling_gate().is_automatically_eligible() {
            let job = source_sync_job(&created, Utc::now());
            unit_of_work.enqueue_source_sync(job).await?;
        }

        unit_of_work.commit().await?;
        Ok(created)
    }

    /// Reads one source by durable identity.
    pub async fn find(&self, source_id: SourceId) -> Result<Option<Source>, SourceServiceError> {
        self.sources.find(source_id).await
    }

    /// Reads one source by its normalized WeRead book identifier.
    pub async fn find_by_book_id(
        &self,
        book_id: &str,
    ) -> Result<Option<Source>, SourceServiceError> {
        self.sources.find_by_book_id(book_id).await
    }

    /// Enables or disables automatic scheduling without changing feed content.
    pub async fn set_enabled(
        &self,
        source_id: SourceId,
        enabled: bool,
    ) -> Result<Source, SourceServiceError> {
        let mut unit_of_work = self.unit_of_work.begin().await?;
        let source = unit_of_work.set_enabled(source_id, enabled).await?;
        unit_of_work.commit().await?;
        Ok(source)
    }

    /// Changes the operator-controlled scheduling gate without changing feed
    /// content.
    pub async fn set_scheduling_gate(
        &self,
        source_id: SourceId,
        gate: SchedulingGate,
    ) -> Result<Source, SourceServiceError> {
        let mut unit_of_work = self.unit_of_work.begin().await?;
        let source = unit_of_work.set_scheduling_gate(source_id, gate).await?;
        unit_of_work.commit().await?;
        Ok(source)
    }
}

fn source_sync_job(source: &Source, now: DateTime<Utc>) -> NewJob {
    NewJob {
        job_type: JobType::SourceSync,
        source_id: Some(source.id().as_uuid()),
        priority: source.priority(),
        run_after: source.next_fetch_at(),
        max_attempts: source.max_attempts(),
        payload: json!({"source_id": source.id().to_string()}),
        dedupe_key: format!("source_sync:{}", source.id()),
        now,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use chrono::Duration;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    #[derive(Clone, Default)]
    struct FakeSourceReader {
        source: Arc<Mutex<Option<Source>>>,
    }

    impl SourceReader for FakeSourceReader {
        async fn find(&self, source_id: SourceId) -> Result<Option<Source>, SourceServiceError> {
            Ok(self
                .source
                .lock()
                .await
                .as_ref()
                .filter(|source| source.id() == source_id)
                .cloned())
        }

        async fn find_by_book_id(
            &self,
            book_id: &str,
        ) -> Result<Option<Source>, SourceServiceError> {
            let book_id = book_id.trim();
            Ok(self
                .source
                .lock()
                .await
                .as_ref()
                .filter(|source| source.book_id() == book_id)
                .cloned())
        }
    }

    #[derive(Default)]
    struct FakeState {
        sources: Vec<Source>,
        jobs: Vec<NewJob>,
        commits: usize,
    }

    #[derive(Clone, Default)]
    struct FakeUnitOfWorkFactory {
        state: Arc<Mutex<FakeState>>,
    }

    struct FakeUnitOfWork {
        state: Arc<Mutex<FakeState>>,
        pending_sources: Vec<Source>,
        pending_jobs: Vec<NewJob>,
    }

    impl SourceUnitOfWorkFactory for FakeUnitOfWorkFactory {
        type Transaction<'a> = FakeUnitOfWork;

        async fn begin(&self) -> Result<Self::Transaction<'_>, SourceServiceError> {
            Ok(FakeUnitOfWork {
                state: Arc::clone(&self.state),
                pending_sources: Vec::new(),
                pending_jobs: Vec::new(),
            })
        }
    }

    impl SourceUnitOfWork for FakeUnitOfWork {
        async fn insert_source(&mut self, source: NewSource) -> Result<Source, SourceServiceError> {
            let source = Source::new(source).map_err(|error| {
                SourceServiceError::Source(SourceRepositoryError::Domain(error))
            })?;
            self.pending_sources.push(source.clone());
            Ok(source)
        }

        async fn enqueue_source_sync(&mut self, job: NewJob) -> Result<(), SourceServiceError> {
            self.pending_jobs.push(job);
            Ok(())
        }

        async fn set_enabled(
            &mut self,
            source_id: SourceId,
            enabled: bool,
        ) -> Result<Source, SourceServiceError> {
            self.replace_source(source_id, |source| {
                source_with_scheduling(source, enabled, source.scheduling_gate())
            })
            .await
        }

        async fn set_scheduling_gate(
            &mut self,
            source_id: SourceId,
            gate: SchedulingGate,
        ) -> Result<Source, SourceServiceError> {
            self.replace_source(source_id, |source| {
                source_with_scheduling(source, source.enabled(), gate)
            })
            .await
        }

        async fn commit(self) -> Result<(), SourceServiceError> {
            let mut state = self.state.lock().await;
            for source in self.pending_sources {
                if let Some(existing) = state
                    .sources
                    .iter_mut()
                    .find(|existing| existing.id() == source.id())
                {
                    *existing = source;
                } else {
                    state.sources.push(source);
                }
            }
            state.jobs.extend(self.pending_jobs);
            state.commits += 1;
            Ok(())
        }
    }

    impl FakeUnitOfWork {
        async fn replace_source<F>(
            &mut self,
            source_id: SourceId,
            update: F,
        ) -> Result<Source, SourceServiceError>
        where
            F: FnOnce(&Source) -> Result<Source, SourceServiceError>,
        {
            let current = if let Some(source) = self
                .pending_sources
                .iter()
                .find(|source| source.id() == source_id)
            {
                source.clone()
            } else {
                self.state
                    .lock()
                    .await
                    .sources
                    .iter()
                    .find(|source| source.id() == source_id)
                    .cloned()
                    .ok_or(SourceServiceError::Source(
                        SourceRepositoryError::NotFound { source_id },
                    ))?
            };
            let updated = update(&current)?;
            if let Some(pending) = self
                .pending_sources
                .iter_mut()
                .find(|source| source.id() == source_id)
            {
                *pending = updated.clone();
            } else {
                self.pending_sources.push(updated.clone());
            }
            Ok(updated)
        }
    }

    fn source_with_scheduling(
        source: &Source,
        enabled: bool,
        scheduling_gate: SchedulingGate,
    ) -> Result<Source, SourceServiceError> {
        Source::new(NewSource {
            id: source.id(),
            book_id: source.book_id().to_owned(),
            display_name: source.display_name().to_owned(),
            article_url: source.article_url().clone(),
            enabled,
            sync_interval: source.sync_interval(),
            rss_item_limit: source.rss_item_limit(),
            account_id: source.account_id(),
            scheduling_gate,
            next_fetch_at: source.next_fetch_at(),
            priority: source.priority(),
            max_attempts: source.max_attempts(),
        })
        .map_err(|error| SourceServiceError::Source(SourceRepositoryError::Domain(error)))
    }

    fn new_source(id: u128, book_id: &str) -> NewSource {
        NewSource {
            id: SourceId::from_uuid(Uuid::from_u128(id)),
            book_id: book_id.to_owned(),
            display_name: "Example".to_owned(),
            article_url: "https://mp.weixin.qq.com/s/example"
                .parse()
                .expect("test URL should be valid"),
            enabled: true,
            sync_interval: Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: "2030-01-02T03:04:05Z".parse().expect("valid timestamp"),
            priority: 7,
            max_attempts: 4,
        }
    }

    fn source() -> Source {
        Source::new(new_source(1, "book-1")).expect("test source should be valid")
    }

    #[tokio::test]
    async fn create_enqueues_only_eligible_sources_and_commits_once() {
        let factory = FakeUnitOfWorkFactory::default();
        let service = SourceService::new(FakeSourceReader::default(), factory.clone());

        service
            .create(new_source(1, "book-ready"))
            .await
            .expect("eligible source should be created");
        service
            .create(NewSource {
                enabled: false,
                ..new_source(2, "book-disabled")
            })
            .await
            .expect("disabled source should be created");
        service
            .create(NewSource {
                scheduling_gate: SchedulingGate::RiskControlled,
                ..new_source(3, "book-blocked")
            })
            .await
            .expect("blocked source should be created");

        let state = factory.state.lock().await;
        assert_eq!(state.sources.len(), 3);
        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.jobs[0].source_id, Some(Uuid::from_u128(1)));
        assert_eq!(state.commits, 3);
    }

    #[tokio::test]
    async fn create_does_not_commit_invalid_source_input() {
        let factory = FakeUnitOfWorkFactory::default();
        let service = SourceService::new(FakeSourceReader::default(), factory.clone());

        let result = service
            .create(NewSource {
                id: SourceId::from_uuid(Uuid::nil()),
                ..new_source(1, "book-invalid")
            })
            .await;

        assert!(matches!(
            result,
            Err(SourceServiceError::Source(SourceRepositoryError::Domain(
                crate::domain::source::SourceError::InvalidId
            )))
        ));
        let state = factory.state.lock().await;
        assert!(state.sources.is_empty());
        assert!(state.jobs.is_empty());
        assert_eq!(state.commits, 0);
    }

    #[tokio::test]
    async fn reads_delegate_to_the_injected_source_reader() {
        let reader = FakeSourceReader {
            source: Arc::new(Mutex::new(Some(source()))),
        };
        let service = SourceService::new(reader, FakeUnitOfWorkFactory::default());

        assert!(service
            .find(SourceId::from_uuid(Uuid::from_u128(1)))
            .await
            .expect("source read should succeed")
            .is_some());
        assert!(service
            .find_by_book_id(" book-1 ")
            .await
            .expect("book read should succeed")
            .is_some());
    }

    #[tokio::test]
    async fn operator_scheduling_changes_commit_and_preserve_source_identity() {
        let factory = FakeUnitOfWorkFactory::default();
        let service = SourceService::new(FakeSourceReader::default(), factory.clone());
        let source_id = SourceId::from_uuid(Uuid::from_u128(4));

        let created = service
            .create(new_source(4, "book-operator"))
            .await
            .expect("source should be created");
        let disabled = service
            .set_enabled(source_id, false)
            .await
            .expect("source should be disabled");
        let gated = service
            .set_scheduling_gate(source_id, SchedulingGate::RiskControlled)
            .await
            .expect("source gate should change");

        assert_eq!(disabled.id(), created.id());
        assert!(!disabled.enabled());
        assert_eq!(gated.id(), created.id());
        assert_eq!(gated.scheduling_gate(), SchedulingGate::RiskControlled);
        assert_eq!(gated.feed_revision(), created.feed_revision());

        let state = factory.state.lock().await;
        assert_eq!(state.commits, 3);
        assert_eq!(state.sources, vec![gated]);
    }

    #[test]
    fn source_sync_job_copies_scheduling_and_retry_policy() {
        let job = source_sync_job(&source(), "2029-01-01T00:00:00Z".parse().unwrap());

        assert_eq!(job.job_type, JobType::SourceSync);
        assert_eq!(job.source_id, Some(Uuid::from_u128(1)));
        assert_eq!(job.priority, 7);
        assert_eq!(
            job.run_after,
            "2030-01-02T03:04:05Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(job.max_attempts, 4);
        assert_eq!(
            job.dedupe_key,
            "source_sync:00000000-0000-0000-0000-000000000001"
        );
        assert_eq!(
            job.payload,
            json!({"source_id": "00000000-0000-0000-0000-000000000001"})
        );
        assert_eq!(
            job.now,
            "2029-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
    }
}
