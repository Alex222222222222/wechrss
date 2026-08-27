//! Repository contract for the durable PostgreSQL-backed job queue.
//!
//! This module is the persistence/application boundary for job scheduling. It
//! defines the operations that a production PostgreSQL repository must expose
//! and includes a small in-memory implementation for unit tests. The memory
//! implementation has the same state and fencing semantics, but it does not
//! replace PostgreSQL row locks or provide a cross-process queue.
//!
//! Responsibilities:
//!
//! - enqueue jobs with active `dedupe_key` uniqueness;
//! - claim one due job atomically for an instance and return its lease token;
//! - persist heartbeats and terminal transitions only for the current owner and
//!   fencing token;
//! - cancel unclaimed work; and
//! - recover expired leases in bounded batches.
//!
//! Non-responsibilities: executing job payloads, calculating browser pacing,
//! deciding retry backoff, rendering RSS, or authorizing HTTP callers. The
//! application service supplies the current clock, lease duration, retry time,
//! and error summary. Error summaries must be safe to persist and must not
//! contain credentials or connection URLs.
//!
//! PostgreSQL implementation requirements:
//!
//! - `enqueue` must rely on a partial unique index for active jobs in
//!   `queued`, `running`, and `retry_wait`, mapping a duplicate to
//!   [`EnqueueResult::AlreadyActive`];
//! - `claim_next` must select due rows with `FOR UPDATE SKIP LOCKED`, assign a
//!   fresh `lease_token`, increment `attempts`, and commit the claim before
//!   returning it;
//! - heartbeat, success, retry, and failure updates must match `id`,
//!   `lease_owner`, `lease_token`, and a live `lease_until` in their update
//!   predicate, so stale workers cannot mutate a later claim; and
//! - recovery must lock only expired running jobs, clear their lease, and
//!   either return them to `queued` or mark them failed when attempts are
//!   exhausted.
//!
//! All mutations should be transaction-friendly. In particular, a future sync
//! service can update articles, archive metadata, feed cache, source status,
//! and job completion within the transaction boundaries defined by the
//! application layer.

use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

use crate::domain::job::{Job, JobError, JobStatus, LeaseToken, NewJob};

/// Result of attempting to enqueue a job.
#[derive(Debug, Clone, PartialEq)]
pub enum EnqueueResult {
    /// A new job row was inserted.
    Inserted(Box<Job>),
    /// An active job with the same deduplication key already exists.
    AlreadyActive {
        /// Identifier of the existing active job.
        job_id: Uuid,
    },
}

/// A successfully claimed job together with the token required for mutations.
#[derive(Debug, Clone, PartialEq)]
pub struct JobLease {
    /// Snapshot of the claimed job returned to the worker.
    pub job: Job,
    /// Fencing token for this particular claim.
    pub token: LeaseToken,
}

/// Operations available while a repository transaction is held.
///
/// A PostgreSQL implementation should back this handle with one SQLx
/// transaction. The application can use the same transaction scope to build
/// transaction-scoped source, article, archive, and feed-cache repositories,
/// then call [`Self::commit`] only after all related changes succeed. Dropping
/// an uncommitted handle must roll the transaction back.
#[allow(async_fn_in_trait)]
pub trait JobRepositoryTransaction {
    /// Inserts a job unless an active job already owns its deduplication key.
    async fn enqueue(&mut self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError>;

    /// Claims the highest-priority due job within this transaction.
    async fn claim_next(
        &mut self,
        owner: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Option<JobLease>, JobRepositoryError>;

    /// Extends a live lease for the owner and claim token.
    async fn heartbeat(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Job, JobRepositoryError>;

    /// Marks a live fenced job as successfully completed.
    async fn succeed(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
    ) -> Result<Job, JobRepositoryError>;

    /// Records a retry or terminal failure when attempts are exhausted.
    async fn retry(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError>;

    /// Permanently fails a live fenced job.
    async fn fail(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError>;

    /// Cancels a queued or retry-wait job without requiring a worker lease.
    async fn cancel(
        &mut self,
        job_id: Uuid,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<Job, JobRepositoryError>;

    /// Recovers up to `limit` expired running jobs.
    async fn recover_expired(
        &mut self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, JobRepositoryError>;

    /// Commits all mutations made through this transaction.
    async fn commit(self) -> Result<(), JobRepositoryError>
    where
        Self: Sized;
}

/// Errors returned by repository implementations.
#[derive(Debug, Error)]
pub enum JobRepositoryError {
    /// A domain transition or validation failed before persistence.
    #[error(transparent)]
    Domain(#[from] JobError),
    /// The requested job row does not exist.
    #[error("job {job_id} was not found")]
    NotFound {
        /// Missing job identifier.
        job_id: Uuid,
    },
    /// The backing store could not complete the operation.
    #[error("job repository storage error: {0}")]
    Storage(String),
}

/// Operations required by application job orchestration.
///
/// The future PostgreSQL implementation should use one transaction per
/// mutation and convert zero-row fenced updates into a domain-level lease
/// error rather than silently reporting success. The trait uses native async
/// methods and is intended to be used through a generic repository parameter;
/// an application that needs dynamic dispatch can add an object-safe adapter.
#[allow(async_fn_in_trait)]
pub trait JobRepository: Send + Sync {
    /// Transaction-scoped repository type used for atomic multi-repository work.
    type Transaction<'a>: JobRepositoryTransaction + 'a
    where
        Self: 'a;

    /// Begins a transaction that must be committed explicitly.
    async fn begin(&self) -> Result<Self::Transaction<'_>, JobRepositoryError>;

    /// Inserts a job unless an active job already owns its deduplication key.
    async fn enqueue(&self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError>;

    /// Claims the highest-priority due job without waiting on another worker.
    async fn claim_next(
        &self,
        owner: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Option<JobLease>, JobRepositoryError>;

    /// Extends a live lease for the owner and claim token.
    async fn heartbeat(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Job, JobRepositoryError>;

    /// Marks a live fenced job as successfully completed.
    async fn succeed(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
    ) -> Result<Job, JobRepositoryError>;

    /// Records a retry or terminal failure when attempts are exhausted.
    async fn retry(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError>;

    /// Permanently fails a live fenced job.
    async fn fail(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError>;

    /// Cancels a queued or retry-wait job without requiring a worker lease.
    async fn cancel(
        &self,
        job_id: Uuid,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<Job, JobRepositoryError>;

    /// Recovers up to `limit` expired running jobs.
    async fn recover_expired(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, JobRepositoryError>;

    /// Returns a snapshot of one job, if it exists.
    async fn find(&self, job_id: Uuid) -> Result<Option<Job>, JobRepositoryError>;
}

/// In-memory repository used for fast unit tests and local orchestration tests.
///
/// Each operation locks the complete store, which gives deterministic
/// single-process behavior but intentionally does not model PostgreSQL's
/// cross-process locking or transaction isolation. Production construction
/// must use a PostgreSQL implementation of [`JobRepository`].
#[derive(Clone, Default)]
pub struct MemoryJobRepository {
    jobs: Arc<Mutex<HashMap<Uuid, Job>>>,
}

impl MemoryJobRepository {
    /// Creates an empty in-memory job store.
    pub fn new() -> Self {
        Self::default()
    }

    fn get_mut(
        jobs: &mut HashMap<Uuid, Job>,
        job_id: Uuid,
    ) -> Result<&mut Job, JobRepositoryError> {
        jobs.get_mut(&job_id)
            .ok_or(JobRepositoryError::NotFound { job_id })
    }
}

fn enqueue_in_store(
    jobs: &mut HashMap<Uuid, Job>,
    spec: NewJob,
) -> Result<EnqueueResult, JobRepositoryError> {
    if let Some(existing) = jobs
        .values()
        .find(|job| job.status().is_active() && job.dedupe_key() == spec.dedupe_key)
    {
        return Ok(EnqueueResult::AlreadyActive {
            job_id: existing.id(),
        });
    }

    let job = Job::new(spec)?;
    jobs.insert(job.id(), job.clone());
    Ok(EnqueueResult::Inserted(Box::new(job)))
}

fn claim_next_in_store(
    jobs: &mut HashMap<Uuid, Job>,
    owner: &str,
    now: DateTime<Utc>,
    lease_for: Duration,
) -> Result<Option<JobLease>, JobRepositoryError> {
    let candidate_id = jobs
        .iter()
        .filter(|(_, job)| {
            matches!(job.status(), JobStatus::Queued | JobStatus::RetryWait)
                && job.run_after() <= now
                && job.attempts() < job.max_attempts()
        })
        .min_by(|(_, left), (_, right)| claim_order(left, right))
        .map(|(job_id, _)| *job_id);

    let Some(candidate_id) = candidate_id else {
        return Ok(None);
    };
    let job = MemoryJobRepository::get_mut(jobs, candidate_id)?;
    let token = job.claim(owner, now, lease_for)?;
    Ok(Some(JobLease {
        job: job.clone(),
        token,
    }))
}

fn heartbeat_in_store(
    jobs: &mut HashMap<Uuid, Job>,
    job_id: Uuid,
    owner: &str,
    token: LeaseToken,
    now: DateTime<Utc>,
    lease_for: Duration,
) -> Result<Job, JobRepositoryError> {
    let job = MemoryJobRepository::get_mut(jobs, job_id)?;
    job.heartbeat(owner, token, now, lease_for)?;
    Ok(job.clone())
}

fn succeed_in_store(
    jobs: &mut HashMap<Uuid, Job>,
    job_id: Uuid,
    owner: &str,
    token: LeaseToken,
    now: DateTime<Utc>,
) -> Result<Job, JobRepositoryError> {
    let job = MemoryJobRepository::get_mut(jobs, job_id)?;
    job.succeed(owner, token, now)?;
    Ok(job.clone())
}

fn retry_in_store(
    jobs: &mut HashMap<Uuid, Job>,
    job_id: Uuid,
    owner: &str,
    token: LeaseToken,
    now: DateTime<Utc>,
    retry_at: DateTime<Utc>,
    error: &str,
) -> Result<Job, JobRepositoryError> {
    let job = MemoryJobRepository::get_mut(jobs, job_id)?;
    job.retry(owner, token, now, retry_at, error)?;
    Ok(job.clone())
}

fn fail_in_store(
    jobs: &mut HashMap<Uuid, Job>,
    job_id: Uuid,
    owner: &str,
    token: LeaseToken,
    now: DateTime<Utc>,
    error: &str,
) -> Result<Job, JobRepositoryError> {
    let job = MemoryJobRepository::get_mut(jobs, job_id)?;
    job.fail(owner, token, now, error)?;
    Ok(job.clone())
}

fn cancel_in_store(
    jobs: &mut HashMap<Uuid, Job>,
    job_id: Uuid,
    now: DateTime<Utc>,
    reason: &str,
) -> Result<Job, JobRepositoryError> {
    let job = MemoryJobRepository::get_mut(jobs, job_id)?;
    job.cancel(now, reason)?;
    Ok(job.clone())
}

fn recover_expired_in_store(
    jobs: &mut HashMap<Uuid, Job>,
    now: DateTime<Utc>,
    limit: usize,
) -> Vec<Job> {
    let mut candidates: Vec<_> = jobs
        .iter()
        .filter_map(|(job_id, job)| {
            (job.status() == JobStatus::Running
                && job
                    .lease_until()
                    .is_some_and(|lease_until| lease_until <= now))
            .then_some((*job_id, job.lease_until()))
        })
        .collect();
    candidates.sort_by_key(|(_, lease_until)| *lease_until);
    candidates.truncate(limit);

    let mut recovered = Vec::with_capacity(candidates.len());
    for (job_id, _) in candidates {
        let job = MemoryJobRepository::get_mut(jobs, job_id)
            .expect("recovery candidates must remain in the store");
        if job.recover_expired_lease(now) {
            recovered.push(job.clone());
        }
    }
    recovered
}

impl std::fmt::Debug for MemoryJobRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryJobRepository")
            .finish_non_exhaustive()
    }
}

/// Transaction handle for [`MemoryJobRepository`].
///
/// The store remains locked for the lifetime of the handle. Mutations are
/// rolled back from the snapshot if the handle is dropped without `commit`,
/// which gives tests a useful approximation of SQLx transaction behavior.
pub struct MemoryJobTransaction<'a> {
    guard: Option<MutexGuard<'a, HashMap<Uuid, Job>>>,
    backup: HashMap<Uuid, Job>,
    committed: bool,
}

impl<'a> MemoryJobTransaction<'a> {
    fn jobs_mut(&mut self) -> Result<&mut HashMap<Uuid, Job>, JobRepositoryError> {
        self.guard
            .as_deref_mut()
            .ok_or_else(|| JobRepositoryError::Storage("transaction is closed".to_owned()))
    }
}

impl std::fmt::Debug for MemoryJobTransaction<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MemoryJobTransaction")
            .field("committed", &self.committed)
            .finish_non_exhaustive()
    }
}

impl Drop for MemoryJobTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            if let Some(mut guard) = self.guard.take() {
                *guard = self.backup.clone();
            }
        }
    }
}

impl JobRepository for MemoryJobRepository {
    type Transaction<'a> = MemoryJobTransaction<'a>;

    async fn begin(&self) -> Result<Self::Transaction<'_>, JobRepositoryError> {
        let guard = self.jobs.lock().await;
        let backup = guard.clone();
        Ok(MemoryJobTransaction {
            guard: Some(guard),
            backup,
            committed: false,
        })
    }

    async fn enqueue(&self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError> {
        let mut jobs = self.jobs.lock().await;
        enqueue_in_store(&mut jobs, spec)
    }

    async fn claim_next(
        &self,
        owner: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Option<JobLease>, JobRepositoryError> {
        let mut jobs = self.jobs.lock().await;
        claim_next_in_store(&mut jobs, owner, now, lease_for)
    }

    async fn heartbeat(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Job, JobRepositoryError> {
        let mut jobs = self.jobs.lock().await;
        heartbeat_in_store(&mut jobs, job_id, owner, token, now, lease_for)
    }

    async fn succeed(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
    ) -> Result<Job, JobRepositoryError> {
        let mut jobs = self.jobs.lock().await;
        succeed_in_store(&mut jobs, job_id, owner, token, now)
    }

    async fn retry(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError> {
        let mut jobs = self.jobs.lock().await;
        retry_in_store(&mut jobs, job_id, owner, token, now, retry_at, error)
    }

    async fn fail(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError> {
        let mut jobs = self.jobs.lock().await;
        fail_in_store(&mut jobs, job_id, owner, token, now, error)
    }

    async fn cancel(
        &self,
        job_id: Uuid,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<Job, JobRepositoryError> {
        let mut jobs = self.jobs.lock().await;
        cancel_in_store(&mut jobs, job_id, now, reason)
    }

    async fn recover_expired(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, JobRepositoryError> {
        let mut jobs = self.jobs.lock().await;
        Ok(recover_expired_in_store(&mut jobs, now, limit))
    }

    async fn find(&self, job_id: Uuid) -> Result<Option<Job>, JobRepositoryError> {
        Ok(self.jobs.lock().await.get(&job_id).cloned())
    }
}

impl JobRepositoryTransaction for MemoryJobTransaction<'_> {
    async fn enqueue(&mut self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError> {
        enqueue_in_store(self.jobs_mut()?, spec)
    }

    async fn claim_next(
        &mut self,
        owner: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Option<JobLease>, JobRepositoryError> {
        claim_next_in_store(self.jobs_mut()?, owner, now, lease_for)
    }

    async fn heartbeat(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Job, JobRepositoryError> {
        heartbeat_in_store(self.jobs_mut()?, job_id, owner, token, now, lease_for)
    }

    async fn succeed(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
    ) -> Result<Job, JobRepositoryError> {
        succeed_in_store(self.jobs_mut()?, job_id, owner, token, now)
    }

    async fn retry(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError> {
        retry_in_store(self.jobs_mut()?, job_id, owner, token, now, retry_at, error)
    }

    async fn fail(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError> {
        fail_in_store(self.jobs_mut()?, job_id, owner, token, now, error)
    }

    async fn cancel(
        &mut self,
        job_id: Uuid,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<Job, JobRepositoryError> {
        cancel_in_store(self.jobs_mut()?, job_id, now, reason)
    }

    async fn recover_expired(
        &mut self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, JobRepositoryError> {
        Ok(recover_expired_in_store(self.jobs_mut()?, now, limit))
    }

    async fn commit(mut self) -> Result<(), JobRepositoryError> {
        self.committed = true;
        self.guard.take();
        Ok(())
    }
}

fn claim_order(left: &Job, right: &Job) -> Ordering {
    right
        .priority()
        .cmp(&left.priority())
        .then_with(|| left.run_after().cmp(&right.run_after()))
        .then_with(|| left.created_at().cmp(&right.created_at()))
        .then_with(|| left.id().cmp(&right.id()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::JobType;
    use chrono::TimeZone;
    use serde_json::json;

    const OWNER: &str = "instance-a";

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn spec(key: &str, priority: i32, run_after: i64) -> NewJob {
        NewJob {
            job_type: JobType::SourceSync,
            source_id: Some(Uuid::nil()),
            priority,
            run_after: at(run_after),
            max_attempts: 2,
            payload: json!({"source_id": Uuid::nil()}),
            dedupe_key: key.to_owned(),
            now: at(0),
        }
    }

    async fn inserted_id(repository: &MemoryJobRepository, job: NewJob) -> Uuid {
        match repository.enqueue(job).await.unwrap() {
            EnqueueResult::Inserted(job) => job.id(),
            EnqueueResult::AlreadyActive { .. } => panic!("job should be inserted"),
        }
    }

    #[tokio::test]
    async fn deduplicates_only_active_jobs() {
        let repository = MemoryJobRepository::new();
        let job_id = inserted_id(&repository, spec("source:one", 1, 0)).await;

        assert_eq!(
            repository.enqueue(spec("source:one", 1, 0)).await.unwrap(),
            EnqueueResult::AlreadyActive { job_id }
        );

        repository
            .cancel(job_id, at(1), "source removed")
            .await
            .unwrap();
        assert!(matches!(
            repository.enqueue(spec("source:one", 1, 2)).await.unwrap(),
            EnqueueResult::Inserted(_)
        ));
    }

    #[tokio::test]
    async fn claims_due_jobs_by_priority_and_skips_future_work() {
        let repository = MemoryJobRepository::new();
        inserted_id(&repository, spec("low", 1, 0)).await;
        inserted_id(&repository, spec("high", 10, 50)).await;
        inserted_id(&repository, spec("future", 100, 500)).await;

        let first = repository
            .claim_next(OWNER, at(100), Duration::seconds(30))
            .await
            .unwrap()
            .expect("a due job should be claimed");
        assert_eq!(first.job.dedupe_key(), "high");

        let second = repository
            .claim_next(OWNER, at(100), Duration::seconds(30))
            .await
            .unwrap()
            .expect("the second due job should be claimed");
        assert_eq!(second.job.dedupe_key(), "low");

        assert!(repository
            .claim_next(OWNER, at(100), Duration::seconds(30))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn forwards_fencing_errors_and_allows_current_claim_completion() {
        let repository = MemoryJobRepository::new();
        let job_id = inserted_id(&repository, spec("source:one", 1, 0)).await;
        let first = repository
            .claim_next(OWNER, at(100), Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        repository.recover_expired(at(130), 10).await.unwrap();
        let second = repository
            .claim_next(OWNER, at(130), Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();

        assert_ne!(first.token, second.token);
        assert!(matches!(
            repository
                .succeed(job_id, OWNER, first.token, at(140))
                .await,
            Err(JobRepositoryError::Domain(JobError::LeaseTokenMismatch))
        ));
        let completed = repository
            .succeed(job_id, OWNER, second.token, at(140))
            .await
            .unwrap();
        assert_eq!(completed.status(), JobStatus::Succeeded);
    }

    #[tokio::test]
    async fn recovers_only_expired_jobs_with_a_batch_limit() {
        let repository = MemoryJobRepository::new();
        inserted_id(&repository, spec("one", 1, 0)).await;
        inserted_id(&repository, spec("two", 1, 0)).await;
        let _first = repository
            .claim_next(OWNER, at(100), Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        let _second = repository
            .claim_next(OWNER, at(100), Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();

        let recovered = repository.recover_expired(at(130), 1).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].status(), JobStatus::Queued);
        assert_eq!(
            repository
                .find(recovered[0].id())
                .await
                .unwrap()
                .unwrap()
                .status(),
            JobStatus::Queued
        );
    }

    #[tokio::test]
    async fn reports_missing_jobs_and_domain_lease_errors() {
        let repository = MemoryJobRepository::new();
        let missing = Uuid::new_v4();
        inserted_id(&repository, spec("token-source", 1, 0)).await;
        let token = repository
            .claim_next(OWNER, at(0), Duration::seconds(30))
            .await
            .unwrap()
            .unwrap()
            .token;
        assert!(matches!(
            repository
                .succeed(missing, OWNER, token, at(0))
                .await,
            Err(JobRepositoryError::NotFound { job_id }) if job_id == missing
        ));

        let job_id = inserted_id(&repository, spec("source:one", 1, 0)).await;
        let lease = repository
            .claim_next(OWNER, at(1), Duration::seconds(30))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            repository
                .heartbeat(
                    job_id,
                    "other-instance",
                    lease.token,
                    at(2),
                    Duration::seconds(30)
                )
                .await,
            Err(JobRepositoryError::Domain(JobError::LeaseOwnerMismatch))
        ));
    }

    #[tokio::test]
    async fn transaction_commits_explicitly_and_rolls_back_on_drop() {
        let repository = MemoryJobRepository::new();

        let rolled_back_id = {
            let mut transaction = repository.begin().await.unwrap();
            let inserted = transaction
                .enqueue(spec("rolled-back", 1, 0))
                .await
                .unwrap();
            match inserted {
                EnqueueResult::Inserted(job) => job.id(),
                EnqueueResult::AlreadyActive { .. } => panic!("job should be inserted"),
            }
        };
        assert!(repository.find(rolled_back_id).await.unwrap().is_none());

        let committed_id = {
            let mut transaction = repository.begin().await.unwrap();
            let inserted = transaction.enqueue(spec("committed", 1, 0)).await.unwrap();
            let job_id = match inserted {
                EnqueueResult::Inserted(job) => job.id(),
                EnqueueResult::AlreadyActive { .. } => panic!("job should be inserted"),
            };
            transaction.commit().await.unwrap();
            job_id
        };
        assert!(repository.find(committed_id).await.unwrap().is_some());
    }
}
