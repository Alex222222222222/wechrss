//! Atomic application handler for claimed source-synchronization jobs.
//!
//! The handler owns the boundary between acquisition and persistence. The
//! injected [`SourceSyncAcquirer`] is responsible for authenticated WeRead
//! list access and credential-free public article fetching; it never receives
//! a database transaction. Acquired pages are normalized outside a
//! transaction, and the final article upserts, source revision/schedule,
//! sync-run completion, optional feed-rebuild enqueue, and fenced job outcome
//! commit together through [`UnitOfWorkFactory`].
//!
//! This is intentionally an adapter-friendly slice. Runtime composition still
//! has to provide the real authenticated WeRead transport and browser pool,
//! but tests and future transports can implement the small acquisition port
//! without coupling this workflow to WebDriver details.

use chrono::{DateTime, Duration, Utc};
use serde_json::json;

use crate::{
    acquisition::{article_page::ExtractedArticlePage, weread::WeReadArticleReference},
    application::{
        source_service::SourceReader,
        sync_service::{
            classify_acquisition_error, ClassifiedSyncFailure, SyncAcquisitionError, SyncService,
            SyncServiceError,
        },
        worker::{JobExecution, JobHandler},
    },
    domain::{
        job::JobType,
        source::{SchedulingGate, Source, SourceId},
        sync::{NewSyncRun, SyncOutcome, SyncRunCompletion, SyncStats},
    },
    persistence::{
        repositories::{
            article_repository::{ArticleRepository, ArticleTransactionRepository},
            job_repository::{JobEnqueueTransaction, JobLease, JobOutcome, JobOutcomeTransaction},
            source_repository::SourceTransactionRepository,
            sync_run_repository::SyncRunTransactionRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

/// Acquisition port consumed by one source-sync job.
///
/// Implementations should keep authenticated list/session work inside
/// `list_article_references` and pass only validated public-page results back
/// through `fetch_article`. The latter operation is deliberately given no
/// account or browser-pool credential capability by this interface.
#[allow(async_fn_in_trait)]
pub trait SourceSyncAcquirer: Send + Sync {
    /// Lists the current normalized WeRead article references for one source.
    async fn list_article_references(
        &self,
        source: &Source,
    ) -> Result<Vec<WeReadArticleReference>, SyncAcquisitionError>;

    /// Fetches and extracts one public article page without credentials.
    async fn fetch_article(
        &self,
        reference: &WeReadArticleReference,
    ) -> Result<ExtractedArticlePage, SyncAcquisitionError>;
}

/// Retry and source-cooldown policy for one source-sync handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSyncJobHandlerConfig {
    retry_after: Duration,
    failure_cooldown: Duration,
}

impl SourceSyncJobHandlerConfig {
    /// Creates a policy with positive bounded delays.
    pub fn new(
        retry_after: Duration,
        failure_cooldown: Duration,
    ) -> Result<Self, SourceSyncJobHandlerConfigError> {
        if retry_after <= Duration::zero() {
            return Err(SourceSyncJobHandlerConfigError::InvalidRetryAfter);
        }
        if failure_cooldown <= Duration::zero() {
            return Err(SourceSyncJobHandlerConfigError::InvalidFailureCooldown);
        }
        Ok(Self {
            retry_after,
            failure_cooldown,
        })
    }

    /// Returns the delay before a retryable queue failure is retried.
    pub const fn retry_after(self) -> Duration {
        self.retry_after
    }

    /// Returns the source cooldown applied to retryable failures.
    pub const fn failure_cooldown(self) -> Duration {
        self.failure_cooldown
    }
}

impl Default for SourceSyncJobHandlerConfig {
    fn default() -> Self {
        Self::new(Duration::minutes(1), Duration::minutes(5))
            .expect("default source-sync delays must be valid")
    }
}

/// Invalid source-sync handler policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SourceSyncJobHandlerConfigError {
    /// A retry delay must not create an immediate polling loop.
    #[error("source-sync retry delay must be positive")]
    InvalidRetryAfter,
    /// A source cooldown must be positive so failures are not hot-looped.
    #[error("source-sync failure cooldown must be positive")]
    InvalidFailureCooldown,
}

/// Dependencies for a source-sync job handler.
pub struct SourceSyncJobHandlerDependencies<S, A, C> {
    /// Source read adapter.
    pub sources: S,
    /// Observation-version allocator.
    pub articles: A,
    /// Shared PostgreSQL transaction factory.
    pub unit_of_work: UnitOfWorkFactory,
    /// Network/browser acquisition adapter.
    pub acquirer: C,
    /// Normalization and archive policy.
    pub sync_service: SyncService,
}

/// Executes one claimed source-sync job.
pub struct SourceSyncJobHandler<S, A, C> {
    dependencies: SourceSyncJobHandlerDependencies<S, A, C>,
    config: SourceSyncJobHandlerConfig,
}

impl<S, A, C> SourceSyncJobHandler<S, A, C> {
    /// Creates a source-sync handler from injected persistence and acquisition
    /// boundaries.
    pub fn new(
        dependencies: SourceSyncJobHandlerDependencies<S, A, C>,
        config: SourceSyncJobHandlerConfig,
    ) -> Self {
        Self {
            dependencies,
            config,
        }
    }

    /// Returns the configured retry/cooldown policy.
    pub const fn config(&self) -> SourceSyncJobHandlerConfig {
        self.config
    }
}

impl<S, A, C> JobHandler for SourceSyncJobHandler<S, A, C>
where
    S: SourceReader,
    A: ArticleRepository,
    C: SourceSyncAcquirer,
{
    async fn execute(&self, lease: &JobLease, now: DateTime<Utc>) -> JobExecution {
        match self.execute_inner(lease, now).await {
            Ok(()) => JobExecution::Committed,
            Err(SourceSyncExecutionError::BeforeRun(outcome)) => outcome,
            Err(SourceSyncExecutionError::AfterRun { context, failure }) => {
                match self.finish_failed_run(lease, *context, failure).await {
                    Ok(()) => JobExecution::Committed,
                    Err(()) => retry_execution(now, self.config.retry_after()),
                }
            }
        }
    }
}

impl<S, A, C> SourceSyncJobHandler<S, A, C>
where
    S: SourceReader,
    A: ArticleRepository,
    C: SourceSyncAcquirer,
{
    async fn execute_inner(
        &self,
        lease: &JobLease,
        worker_now: DateTime<Utc>,
    ) -> Result<(), SourceSyncExecutionError> {
        let source_id = validate_lease(lease).map_err(SourceSyncExecutionError::BeforeRun)?;
        let source = self
            .dependencies
            .sources
            .find(source_id)
            .await
            .map_err(|_| {
                SourceSyncExecutionError::BeforeRun(retry_execution(
                    worker_now,
                    self.config.retry_after(),
                ))
            })?
            .ok_or_else(|| {
                SourceSyncExecutionError::BeforeRun(JobExecution::Failed {
                    error: "source-sync job references a missing source".to_owned(),
                })
            })?;

        let started_at = self
            .dependencies
            .unit_of_work
            .database_now()
            .await
            .map_err(|_| {
                SourceSyncExecutionError::BeforeRun(retry_execution(
                    worker_now,
                    self.config.retry_after(),
                ))
            })?;
        let run_id = uuid::Uuid::new_v4();
        self.start_run(run_id, source_id, lease.job.id(), started_at)
            .await
            .map_err(|_| {
                SourceSyncExecutionError::BeforeRun(retry_execution(
                    worker_now,
                    self.config.retry_after(),
                ))
            })?;

        let context = SourceSyncRunContext {
            source,
            run_id,
            stats: SyncStats::default(),
        };
        let references = match self
            .dependencies
            .acquirer
            .list_article_references(&context.source)
            .await
        {
            Ok(references) => references,
            Err(error) => {
                return Err(SourceSyncExecutionError::AfterRun {
                    context: Box::new(context),
                    failure: classify_acquisition_error(&error),
                });
            }
        };
        let seen = match u32::try_from(references.len()) {
            Ok(seen) => seen,
            Err(_) => {
                return Err(SourceSyncExecutionError::AfterRun {
                    context: Box::new(context),
                    failure: ClassifiedSyncFailure::permanent("source returned too many articles"),
                });
            }
        };

        let mut context = context;
        context.stats.articles_seen = seen;
        let mut observed = Vec::with_capacity(references.len());
        for reference in references {
            let Some(_url) = reference.article_url.as_ref() else {
                context.stats.articles_failed = context.stats.articles_failed.saturating_add(1);
                continue;
            };
            let observation_version = match self
                .dependencies
                .articles
                .allocate_observation_version()
                .await
            {
                Ok(observation_version) => observation_version,
                Err(_) => {
                    return Err(SourceSyncExecutionError::AfterRun {
                        context: Box::new(context),
                        failure: retryable_persistence_failure(),
                    });
                }
            };
            match self.dependencies.acquirer.fetch_article(&reference).await {
                Ok(page) => observed.push((reference, page, observation_version)),
                Err(error) => {
                    let failure = classify_acquisition_error(&error);
                    if failure.outcome() == SyncOutcome::Failed {
                        context.stats.articles_failed =
                            context.stats.articles_failed.saturating_add(1);
                        continue;
                    }
                    return Err(SourceSyncExecutionError::AfterRun {
                        context: Box::new(context),
                        failure,
                    });
                }
            }
        }

        let finished_at = match self.dependencies.unit_of_work.database_now().await {
            Ok(finished_at) => finished_at,
            Err(_) => {
                return Err(SourceSyncExecutionError::AfterRun {
                    context: Box::new(context),
                    failure: retryable_persistence_failure(),
                });
            }
        };
        let mut prepared = Vec::with_capacity(observed.len());
        for (reference, page, observation_version) in observed {
            match self.dependencies.sync_service.prepare_article(
                context.source.id(),
                &reference,
                page,
                observation_version,
                finished_at,
            ) {
                Ok(article) => {
                    context.stats.archived_articles =
                        context.stats.archived_articles.saturating_add(1);
                    prepared.push(article);
                }
                Err(SyncServiceError::MissingPublishedAt | SyncServiceError::Article(_)) => {
                    context.stats.articles_failed = context.stats.articles_failed.saturating_add(1);
                }
            }
        }

        let retry_context = SourceSyncRunContext {
            source: context.source.clone(),
            run_id: context.run_id,
            stats: context.stats,
        };
        self.persist_success(lease, context, prepared, finished_at)
            .await
            .map_err(|_| SourceSyncExecutionError::AfterRun {
                context: Box::new(retry_context),
                failure: retryable_persistence_failure(),
            })
    }

    async fn start_run(
        &self,
        run_id: uuid::Uuid,
        source_id: SourceId,
        job_id: uuid::Uuid,
        started_at: DateTime<Utc>,
    ) -> Result<(), ()> {
        let mut unit_of_work = self
            .dependencies
            .unit_of_work
            .begin()
            .await
            .map_err(|_| ())?;
        unit_of_work
            .sync_runs()
            .start(NewSyncRun {
                id: run_id,
                source_id,
                job_id: Some(job_id),
                started_at,
            })
            .await
            .map_err(|_| ())?;
        unit_of_work.commit().await.map_err(|_| ())
    }

    async fn persist_success(
        &self,
        lease: &JobLease,
        context: SourceSyncRunContext,
        prepared: Vec<crate::application::sync_service::PreparedArticle>,
        finished_at: DateTime<Utc>,
    ) -> Result<(), ()> {
        let mut unit_of_work = self
            .dependencies
            .unit_of_work
            .begin()
            .await
            .map_err(|_| ())?;
        let mut changed = false;
        let mut stats = context.stats;
        {
            let mut articles = unit_of_work.articles();
            for article in prepared {
                let result = articles
                    .upsert(article.article().clone())
                    .await
                    .map_err(|_| ())?;
                if result.created() {
                    stats.articles_created = stats.articles_created.saturating_add(1);
                } else if result.feed_visible_change() {
                    stats.articles_updated = stats.articles_updated.saturating_add(1);
                }
                changed |= result.feed_visible_change();
            }
        }

        let feed_revision = if changed {
            let mut sources = unit_of_work.source();
            Some(
                sources
                    .bump_feed_revision(context.source.id(), context.source.feed_revision())
                    .await
                    .map_err(|_| ())?,
            )
        } else {
            None
        };

        if changed {
            let mut queue = unit_of_work.job_enqueue();
            queue
                .enqueue_job(feed_rebuild_job(&context.source, finished_at))
                .await
                .map_err(|_| ())?;
        }

        let next_fetch_at = finished_at
            .checked_add_signed(context.source.sync_interval())
            .ok_or(())?;
        {
            let mut sources = unit_of_work.source();
            sources
                .update_schedule(context.source.id(), next_fetch_at, None, None)
                .await
                .map_err(|_| ())?;
        }
        unit_of_work
            .sync_runs()
            .finish(
                context.run_id,
                SyncRunCompletion {
                    outcome: SyncOutcome::Succeeded,
                    finished_at,
                    stats,
                    failure: None,
                    feed_revision,
                },
            )
            .await
            .map_err(|_| ())?;
        apply_job_outcome(
            &mut unit_of_work,
            lease,
            JobOutcome::Succeeded {
                job_id: lease.job.id(),
                owner: lease.job.lease_owner().ok_or(())?.to_owned(),
                token: lease.token,
                now: finished_at,
            },
        )
        .await
        .map_err(|_| ())?;
        unit_of_work.commit().await.map_err(|_| ())
    }

    async fn finish_failed_run(
        &self,
        lease: &JobLease,
        context: SourceSyncRunContext,
        classified: ClassifiedSyncFailure,
    ) -> Result<(), ()> {
        let finished_at = self
            .dependencies
            .unit_of_work
            .database_now()
            .await
            .map_err(|_| ())?;
        let retry_at = finished_at.checked_add_signed(self.config.retry_after);
        let cooldown_until = finished_at.checked_add_signed(self.config.failure_cooldown);
        let next_fetch_at = finished_at
            .checked_add_signed(context.source.sync_interval())
            .ok_or(())?;
        let mut unit_of_work = self
            .dependencies
            .unit_of_work
            .begin()
            .await
            .map_err(|_| ())?;

        match classified.outcome() {
            SyncOutcome::AuthenticationRequired => {
                let mut sources = unit_of_work.source();
                sources
                    .set_scheduling_gate(
                        context.source.id(),
                        SchedulingGate::AuthenticationRequired,
                    )
                    .await
                    .map_err(|_| ())?;
                sources
                    .update_schedule(
                        context.source.id(),
                        context.source.next_fetch_at(),
                        None,
                        None,
                    )
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::RiskControlled => {
                let mut sources = unit_of_work.source();
                sources
                    .set_scheduling_gate(context.source.id(), SchedulingGate::RiskControlled)
                    .await
                    .map_err(|_| ())?;
                sources
                    .update_schedule(
                        context.source.id(),
                        context.source.next_fetch_at(),
                        None,
                        None,
                    )
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::RetryableFailure => {
                let retry_at = retry_at.ok_or(())?;
                let mut sources = unit_of_work.source();
                sources
                    .update_schedule(context.source.id(), retry_at, cooldown_until, None)
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::Blocked => {
                let mut sources = unit_of_work.source();
                sources
                    .set_scheduling_gate(context.source.id(), SchedulingGate::RiskControlled)
                    .await
                    .map_err(|_| ())?;
                sources
                    .update_schedule(
                        context.source.id(),
                        context.source.next_fetch_at(),
                        None,
                        None,
                    )
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::Failed => {
                let mut sources = unit_of_work.source();
                sources
                    .update_schedule(context.source.id(), next_fetch_at, None, None)
                    .await
                    .map_err(|_| ())?;
            }
            SyncOutcome::Running | SyncOutcome::Succeeded | SyncOutcome::Deferred => return Err(()),
        }

        let stats = context.stats;
        unit_of_work
            .sync_runs()
            .finish(
                context.run_id,
                SyncRunCompletion {
                    outcome: classified.outcome(),
                    finished_at,
                    stats,
                    failure: Some(classified.failure().clone()),
                    feed_revision: None,
                },
            )
            .await
            .map_err(|_| ())?;
        let outcome = if classified.outcome() == SyncOutcome::RetryableFailure {
            JobOutcome::Retry {
                job_id: lease.job.id(),
                owner: lease.job.lease_owner().ok_or(())?.to_owned(),
                token: lease.token,
                now: finished_at,
                retry_at: retry_at.ok_or(())?,
                error: classified.failure().message().to_owned(),
            }
        } else {
            JobOutcome::Failed {
                job_id: lease.job.id(),
                owner: lease.job.lease_owner().ok_or(())?.to_owned(),
                token: lease.token,
                now: finished_at,
                error: classified.failure().message().to_owned(),
            }
        };
        apply_job_outcome(&mut unit_of_work, lease, outcome)
            .await
            .map_err(|_| ())?;
        unit_of_work.commit().await.map_err(|_| ())
    }
}

enum SourceSyncExecutionError {
    BeforeRun(JobExecution),
    AfterRun {
        context: Box<SourceSyncRunContext>,
        failure: ClassifiedSyncFailure,
    },
}

struct SourceSyncRunContext {
    source: Source,
    run_id: uuid::Uuid,
    stats: SyncStats,
}

fn validate_lease(lease: &JobLease) -> Result<SourceId, JobExecution> {
    if lease.job.job_type() != JobType::SourceSync {
        return Err(JobExecution::Failed {
            error: "source-sync handler received an unsupported job type".to_owned(),
        });
    }
    let source_uuid = lease.job.source_id().ok_or_else(|| JobExecution::Failed {
        error: "source-sync job is missing its source".to_owned(),
    })?;
    let payload_source_id = lease
        .job
        .payload()
        .get("source_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|value| value.parse::<uuid::Uuid>().ok());
    if payload_source_id != Some(source_uuid) {
        return Err(JobExecution::Failed {
            error: "source-sync job payload does not match its source".to_owned(),
        });
    }
    if lease.job.lease_owner().is_none() {
        return Err(JobExecution::Failed {
            error: "source-sync job lease is incomplete".to_owned(),
        });
    }
    Ok(SourceId::from_uuid(source_uuid))
}

fn retry_execution(now: DateTime<Utc>, retry_after: Duration) -> JobExecution {
    now.checked_add_signed(retry_after).map_or_else(
        || JobExecution::Failed {
            error: "source synchronization retry time is outside the supported range".to_owned(),
        },
        |retry_at| JobExecution::Retry {
            retry_at,
            error: "source synchronization persistence is temporarily unavailable".to_owned(),
        },
    )
}

fn retryable_persistence_failure() -> ClassifiedSyncFailure {
    ClassifiedSyncFailure::retryable(
        "source synchronization persistence is temporarily unavailable",
    )
}

fn feed_rebuild_job(source: &Source, now: DateTime<Utc>) -> crate::domain::job::NewJob {
    crate::domain::job::NewJob {
        job_type: JobType::FeedRebuild,
        source_id: Some(source.id().as_uuid()),
        priority: source.priority(),
        run_after: now,
        max_attempts: source.max_attempts(),
        payload: json!({"source_id": source.id().to_string()}),
        dedupe_key: format!("feed_rebuild:{}", source.id()),
        now,
    }
}

async fn apply_job_outcome(
    unit_of_work: &mut crate::persistence::unit_of_work::UnitOfWork<'_>,
    _lease: &JobLease,
    outcome: JobOutcome,
) -> Result<(), crate::persistence::repositories::job_repository::JobRepositoryError> {
    unit_of_work
        .job_outcomes()
        .apply_outcome(outcome)
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::*;
    use crate::acquisition::weread::WeReadAdapterError;
    use crate::domain::job::{Job, NewJob};
    use crate::domain::sync::SyncFailureClass;
    use serde_json::json;
    use uuid::Uuid;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    #[test]
    fn rejects_non_positive_delays() {
        assert_eq!(
            SourceSyncJobHandlerConfig::new(Duration::zero(), Duration::minutes(1)),
            Err(SourceSyncJobHandlerConfigError::InvalidRetryAfter)
        );
        assert_eq!(
            SourceSyncJobHandlerConfig::new(Duration::minutes(1), Duration::zero()),
            Err(SourceSyncJobHandlerConfigError::InvalidFailureCooldown)
        );
    }

    #[test]
    fn retry_time_overflow_is_bounded() {
        assert_eq!(
            retry_execution(DateTime::<Utc>::MAX_UTC, Duration::seconds(1)),
            JobExecution::Failed {
                error: "source synchronization retry time is outside the supported range"
                    .to_owned()
            }
        );
        assert_eq!(
            retry_execution(at(10), Duration::minutes(1)),
            JobExecution::Retry {
                retry_at: at(70),
                error: "source synchronization persistence is temporarily unavailable".to_owned()
            }
        );
    }

    #[test]
    fn permanent_failures_are_terminal_and_retryable_failures_are_retriable() {
        let permanent = ClassifiedSyncFailure::permanent("invalid article");
        assert_eq!(permanent.outcome(), SyncOutcome::Failed);

        let retryable = classify_acquisition_error(&SyncAcquisitionError::ArticlePage(
            crate::acquisition::article_page::ArticlePageError::OperationTimedOut,
        ));
        assert_eq!(retryable.outcome(), SyncOutcome::RetryableFailure);
    }

    #[test]
    fn a_missing_reference_url_is_classified_as_a_per_article_failure() {
        let error = classify_acquisition_error(&SyncAcquisitionError::WeRead(
            WeReadAdapterError::InvalidArticleUrl,
        ));
        assert_eq!(error.outcome(), SyncOutcome::Failed);
        assert_eq!(error.failure().class(), SyncFailureClass::Permanent);
    }

    fn claimed_job(
        job_type: JobType,
        source_id: Option<Uuid>,
        payload: serde_json::Value,
    ) -> JobLease {
        let mut job = Job::new(NewJob {
            job_type,
            source_id,
            priority: 0,
            run_after: at(0),
            max_attempts: 3,
            payload,
            dedupe_key: "test-source-sync".to_owned(),
            now: at(0),
        })
        .expect("test job should be valid");
        let token = job
            .claim("test-worker", at(0), Duration::minutes(5))
            .expect("test job should be claimable");
        JobLease { job, token }
    }

    #[test]
    fn rejects_wrong_job_shapes_before_opening_a_source_or_database() {
        let source_uuid = Uuid::from_u128(1);
        let missing_payload = claimed_job(JobType::SourceSync, Some(source_uuid), json!({}));
        assert_eq!(
            validate_lease(&missing_payload),
            Err(JobExecution::Failed {
                error: "source-sync job payload does not match its source".to_owned()
            })
        );

        let wrong_type = claimed_job(
            JobType::FeedRebuild,
            Some(source_uuid),
            json!({"source_id": source_uuid.to_string()}),
        );
        assert_eq!(
            validate_lease(&wrong_type),
            Err(JobExecution::Failed {
                error: "source-sync handler received an unsupported job type".to_owned()
            })
        );

        let missing_source = claimed_job(JobType::SourceSync, None, json!({}));
        assert_eq!(
            validate_lease(&missing_source),
            Err(JobExecution::Failed {
                error: "source-sync job is missing its source".to_owned()
            })
        );
    }
}
