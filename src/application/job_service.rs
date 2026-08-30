//! Durable job lifecycle orchestration.
//!
//! JobService is the worker-facing application facade over the persistence
//! queue ports. It binds one configured instance owner and lease policy to
//! enqueue, claim, heartbeat, and expired-lease recovery operations, and it
//! constructs fenced outcomes for a claimed job in a transaction-scoped
//! UnitOfWork view.
//!
//! Responsibilities:
//!
//! - validate and retain the worker owner, lease duration, and recovery batch
//!   policy;
//! - forward enqueue and allowed-kind claim requests to the durable queue;
//! - make heartbeats use the claimed job's id/token and this service's owner;
//! - expose bounded expired-lease recovery; and
//! - apply success, deferral, retry, permanent failure, or cancellation through
//!   a caller-owned outcome transaction without committing it.
//!
//! Multiple instances may use this service concurrently. PostgreSQL row locks
//! with SKIP LOCKED select independent jobs, while lease expiry recovers work
//! from crashed instances. The database remains authoritative for production
//! lease time and fencing; the explicit now argument is retained only for
//! the repository compatibility/test-clock contract.
//!
//! Claim calls receive the set of job types allowed at the current time. During
//! quiet hours, the worker composition can allow local feed_rebuild jobs
//! while excluding upstream source/article work. Deferral does not consume the
//! retry failure budget.
//!
//! Non-responsibilities: executing source synchronization, deciding browser
//! selectors, calculating retry backoff, or committing business data. Job
//! handlers still perform acquisition outside the transaction and pass their
//! typed result to this facade. A future worker loop owns polling, shutdown,
//! heartbeat task cancellation, metrics, and handler dispatch.
//!
//! PostgreSQL/high-availability considerations: enqueue deduplication,
//! SKIP LOCKED, lease expiry, and fencing are durable repository guarantees;
//! this service has no process-local coordination state. The same
//! transaction-scoped outcome call can commit article/source/sync/cache changes
//! atomically with the job transition. A stale outcome returns the repository's
//! typed fencing error and must not be retried under the old token.

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    domain::job::{Job, JobType, NewJob},
    persistence::repositories::job_repository::{
        EnqueueResult, ExpiredJobRecovery, JobLease, JobOutcome, JobOutcomeTransaction, JobQueue,
        JobRepositoryError,
    },
};

const MAX_RECOVERY_BATCH_LIMIT: usize = 1_000;

/// Validated worker policy bound to one application instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobServiceConfig {
    owner: String,
    lease_for: Duration,
    recovery_batch_limit: usize,
}

impl JobServiceConfig {
    /// Creates a worker policy with a non-empty owner and positive lease.
    ///
    /// A zero recovery limit is allowed as an explicit way to disable recovery
    /// in a process that only claims work. The upper bound prevents one
    /// recovery pass from creating an unbounded result set.
    pub fn new(
        owner: impl Into<String>,
        lease_for: Duration,
        recovery_batch_limit: usize,
    ) -> Result<Self, JobServiceConfigError> {
        let owner = owner.into().trim().to_owned();
        if owner.is_empty() {
            return Err(JobServiceConfigError::EmptyOwner);
        }
        if lease_for <= Duration::zero() || lease_for.num_milliseconds() <= 0 {
            return Err(JobServiceConfigError::InvalidLease);
        }
        if recovery_batch_limit > MAX_RECOVERY_BATCH_LIMIT {
            return Err(JobServiceConfigError::RecoveryLimitTooLarge {
                value: recovery_batch_limit,
            });
        }
        Ok(Self {
            owner,
            lease_for,
            recovery_batch_limit,
        })
    }

    /// Returns the normalized PostgreSQL lease owner.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the lease duration used for claims and heartbeats.
    pub const fn lease_for(&self) -> Duration {
        self.lease_for
    }

    /// Returns the maximum number of expired jobs recovered by one pass.
    pub const fn recovery_batch_limit(&self) -> usize {
        self.recovery_batch_limit
    }
}

/// Invalid worker policy settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum JobServiceConfigError {
    /// A lease owner is required for fencing.
    #[error("job service owner must not be empty")]
    EmptyOwner,
    /// Lease duration must fit in the repository timestamp representation.
    #[error("job service lease must be a positive whole number of milliseconds")]
    InvalidLease,
    /// Recovery results are bounded to protect one maintenance pass.
    #[error("job service recovery batch limit must not exceed 1000, got {value}")]
    RecoveryLimitTooLarge {
        /// Invalid configured limit.
        value: usize,
    },
}

/// Errors returned by the durable queue or outcome transaction.
#[derive(Debug, Error)]
pub enum JobServiceError {
    /// The persistence boundary rejected the requested operation.
    #[error(transparent)]
    Repository(#[from] JobRepositoryError),
}

/// Worker-facing job lifecycle facade.
pub struct JobService<Q> {
    queue: Q,
    config: JobServiceConfig,
}

impl<Q> JobService<Q> {
    /// Creates a service over one queue adapter and instance policy.
    pub fn new(queue: Q, config: JobServiceConfig) -> Self {
        Self { queue, config }
    }

    /// Returns the configured worker policy.
    pub const fn config(&self) -> &JobServiceConfig {
        &self.config
    }
}

impl<Q> JobService<Q>
where
    Q: JobQueue + ExpiredJobRecovery,
{
    /// Enqueues work using the repository's active-job deduplication.
    pub async fn enqueue(&self, spec: NewJob) -> Result<EnqueueResult, JobServiceError> {
        Ok(self.queue.enqueue(spec).await?)
    }

    /// Claims one due job of an allowed type.
    pub async fn claim_next(
        &self,
        now: DateTime<Utc>,
        allowed_job_types: &[JobType],
    ) -> Result<Option<JobLease>, JobServiceError> {
        Ok(self
            .queue
            .claim_next(
                self.config.owner(),
                now,
                self.config.lease_for(),
                allowed_job_types,
            )
            .await?)
    }

    /// Heartbeats a lease using this service's owner and the lease's token.
    pub async fn heartbeat(
        &self,
        lease: &JobLease,
        now: DateTime<Utc>,
    ) -> Result<Job, JobServiceError> {
        Ok(self
            .queue
            .heartbeat(
                lease.job.id(),
                self.config.owner(),
                lease.token,
                now,
                self.config.lease_for(),
            )
            .await?)
    }

    /// Recovers up to the configured number of expired running jobs.
    pub async fn recover_expired(&self, now: DateTime<Utc>) -> Result<Vec<Job>, JobServiceError> {
        Ok(self
            .queue
            .recover_expired(now, self.config.recovery_batch_limit())
            .await?)
    }

    /// Applies a successful outcome through a caller-owned transaction.
    pub async fn succeed<T>(
        &self,
        transaction: &mut T,
        lease: &JobLease,
        now: DateTime<Utc>,
    ) -> Result<Job, JobServiceError>
    where
        T: JobOutcomeTransaction,
    {
        self.apply_lease_outcome(transaction, lease, JobOutcomeKind::Succeeded { now })
            .await
    }

    /// Defers a live job without consuming its retry failure budget.
    pub async fn defer<T>(
        &self,
        transaction: &mut T,
        lease: &JobLease,
        now: DateTime<Utc>,
        resume_at: DateTime<Utc>,
    ) -> Result<Job, JobServiceError>
    where
        T: JobOutcomeTransaction,
    {
        self.apply_lease_outcome(
            transaction,
            lease,
            JobOutcomeKind::Deferred { now, resume_at },
        )
        .await
    }

    /// Records a retryable failure through the shared outcome transaction.
    pub async fn retry<T>(
        &self,
        transaction: &mut T,
        lease: &JobLease,
        now: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: impl Into<String>,
    ) -> Result<Job, JobServiceError>
    where
        T: JobOutcomeTransaction,
    {
        self.apply_lease_outcome(
            transaction,
            lease,
            JobOutcomeKind::Retry {
                now,
                retry_at,
                error: error.into(),
            },
        )
        .await
    }

    /// Records a permanent failure through the shared outcome transaction.
    pub async fn fail<T>(
        &self,
        transaction: &mut T,
        lease: &JobLease,
        now: DateTime<Utc>,
        error: impl Into<String>,
    ) -> Result<Job, JobServiceError>
    where
        T: JobOutcomeTransaction,
    {
        self.apply_lease_outcome(
            transaction,
            lease,
            JobOutcomeKind::Failed {
                now,
                error: error.into(),
            },
        )
        .await
    }

    /// Cancels a job through a caller-owned transaction.
    pub async fn cancel<T>(
        &self,
        transaction: &mut T,
        job_id: Uuid,
        now: DateTime<Utc>,
        reason: impl Into<String>,
    ) -> Result<Job, JobServiceError>
    where
        T: JobOutcomeTransaction,
    {
        Ok(transaction
            .apply_outcome(JobOutcome::Cancelled {
                job_id,
                now,
                reason: reason.into(),
            })
            .await?)
    }

    async fn apply_lease_outcome<T>(
        &self,
        transaction: &mut T,
        lease: &JobLease,
        outcome: JobOutcomeKind,
    ) -> Result<Job, JobServiceError>
    where
        T: JobOutcomeTransaction,
    {
        let (job_id, token) = (lease.job.id(), lease.token);
        let outcome = match outcome {
            JobOutcomeKind::Succeeded { now } => JobOutcome::Succeeded {
                job_id,
                owner: self.config.owner().to_owned(),
                token,
                now,
            },
            JobOutcomeKind::Deferred { now, resume_at } => JobOutcome::Deferred {
                job_id,
                owner: self.config.owner().to_owned(),
                token,
                now,
                resume_at,
            },
            JobOutcomeKind::Retry {
                now,
                retry_at,
                error,
            } => JobOutcome::Retry {
                job_id,
                owner: self.config.owner().to_owned(),
                token,
                now,
                retry_at,
                error,
            },
            JobOutcomeKind::Failed { now, error } => JobOutcome::Failed {
                job_id,
                owner: self.config.owner().to_owned(),
                token,
                now,
                error,
            },
        };
        Ok(transaction.apply_outcome(outcome).await?)
    }
}

enum JobOutcomeKind {
    Succeeded {
        now: DateTime<Utc>,
    },
    Deferred {
        now: DateTime<Utc>,
        resume_at: DateTime<Utc>,
    },
    Retry {
        now: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: String,
    },
    Failed {
        now: DateTime<Utc>,
        error: String,
    },
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::json;

    use super::*;
    use crate::persistence::repositories::job_repository::{
        JobRepository, JobRepositoryTransaction, MemoryJobRepository,
    };

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn config(owner: &str) -> JobServiceConfig {
        JobServiceConfig::new(owner, Duration::seconds(10), 2)
            .expect("test worker configuration should be valid")
    }

    fn job(key: &str, max_attempts: u32) -> NewJob {
        NewJob {
            job_type: JobType::SourceSync,
            source_id: Some(Uuid::from_u128(1)),
            priority: 10,
            run_after: at(0),
            max_attempts,
            payload: json!({"source_id": "1"}),
            dedupe_key: key.to_owned(),
            now: at(0),
        }
    }

    #[test]
    fn validates_owner_lease_and_recovery_limits() {
        assert_eq!(
            JobServiceConfig::new(" ", Duration::seconds(1), 1),
            Err(JobServiceConfigError::EmptyOwner)
        );
        assert_eq!(
            JobServiceConfig::new("worker", Duration::microseconds(1), 1),
            Err(JobServiceConfigError::InvalidLease)
        );
        assert_eq!(
            JobServiceConfig::new("worker", Duration::seconds(1), MAX_RECOVERY_BATCH_LIMIT + 1),
            Err(JobServiceConfigError::RecoveryLimitTooLarge {
                value: MAX_RECOVERY_BATCH_LIMIT + 1
            })
        );
        assert_eq!(
            JobServiceConfig::new(" worker ", Duration::seconds(1), 0)
                .expect("zero recovery should be allowed")
                .owner(),
            "worker"
        );
    }

    #[tokio::test]
    async fn queue_operations_bind_owner_and_recover_expired_work() {
        let repository = MemoryJobRepository::new();
        let service = JobService::new(repository.clone(), config("worker-a"));

        let inserted = service
            .enqueue(job("queue-lifecycle", 2))
            .await
            .expect("enqueue should succeed");
        let lease = match inserted {
            EnqueueResult::Inserted(_) => service
                .claim_next(at(1), &[JobType::SourceSync])
                .await
                .expect("claim should succeed")
                .expect("job should be claimable"),
            EnqueueResult::AlreadyActive { .. } => panic!("test job should be inserted"),
        };
        assert_eq!(lease.job.lease_owner(), Some("worker-a"));

        let heartbeated = service
            .heartbeat(&lease, at(2))
            .await
            .expect("heartbeat should succeed");
        assert_eq!(heartbeated.lease_until(), Some(at(12)));

        let recovered = service
            .recover_expired(at(13))
            .await
            .expect("recovery should succeed");
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].failure_count(), 1);
        assert_eq!(recovered[0].status(), crate::domain::job::JobStatus::Queued);
    }

    #[tokio::test]
    async fn outcome_helpers_use_the_current_lease_and_leave_commit_to_the_caller() {
        let repository = MemoryJobRepository::new();
        let service = JobService::new(repository.clone(), config("worker-a"));
        service
            .enqueue(job("outcome", 2))
            .await
            .expect("enqueue should succeed");
        let lease = service
            .claim_next(at(1), &[JobType::SourceSync])
            .await
            .expect("claim should succeed")
            .expect("job should be claimable");

        let mut transaction = repository.begin().await.expect("transaction should open");
        let deferred = service
            .defer(&mut transaction, &lease, at(2), at(20))
            .await
            .expect("defer should succeed");
        assert_eq!(deferred.status(), crate::domain::job::JobStatus::Deferred);
        assert_eq!(deferred.failure_count(), 0);
        transaction
            .commit()
            .await
            .expect("transaction should commit");

        let deferred_lease = service
            .claim_next(at(20), &[JobType::SourceSync])
            .await
            .expect("deferred job should be claimable")
            .expect("deferred job should be available");
        let mut transaction = repository.begin().await.expect("transaction should open");
        let succeeded = service
            .succeed(&mut transaction, &deferred_lease, at(21))
            .await
            .expect("success should succeed");
        assert_eq!(succeeded.status(), crate::domain::job::JobStatus::Succeeded);
        transaction
            .commit()
            .await
            .expect("transaction should commit");
        assert_eq!(
            repository
                .find(succeeded.id())
                .await
                .expect("job read should succeed")
                .expect("job should remain")
                .status(),
            crate::domain::job::JobStatus::Succeeded
        );
    }
}
