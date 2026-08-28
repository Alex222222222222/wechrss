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
//! - claim one due and currently allowed job atomically for an instance and
//!   return its lease token;
//! - persist heartbeats and terminal transitions only for the current owner and
//!   fencing token;
//! - cancel unclaimed work; and
//! - defer jobs at non-failure eligibility boundaries; and
//! - recover expired leases in bounded batches.
//!
//! Non-responsibilities: executing job payloads, calculating browser pacing,
//! deciding retry backoff, rendering RSS, or authorizing HTTP callers. The
//! application service supplies the current clock, lease duration, retry time,
//! and error summary. Error summaries must be safe to persist and must not
//! contain credentials or connection URLs.
//!
//! PostgreSQL implementation behavior:
//!
//! - `enqueue` must rely on a partial unique index for active jobs in
//!   `queued`, `running`, `retry_wait`, and the planned `deferred` state, mapping a duplicate to
//!   [`EnqueueResult::AlreadyActive`];
//! - the corrected `claim_next` contract must select due allowed rows with `FOR
//!   UPDATE SKIP LOCKED`, assign a fresh `lease_token`, increment
//!   `claim_count`, and commit the claim before returning it; the current first
//!   slice still increments legacy `attempts` pending the documented migration;
//! - heartbeat, success, retry, and failure updates must match `id`,
//!   `lease_owner`, `lease_token`, and a live `lease_until` in their update
//!   predicate, so stale workers cannot mutate a later claim; and
//! - recovery must lock only expired running jobs, clear their lease, and
//!   either return them to `queued` or mark them failed when attempts are
//!   exhausted.
//!
//! Cross-repository mutations use `persistence::unit_of_work`. This repository's
//! current job-only transaction is an implemented interim boundary and must not
//! become the application API for final synchronization commits.
//! In the target interface, the general queue port exposes enqueue, claim,
//! heartbeat, and reads, but no independent worker outcome transition.
//! Success, retry, deferral, cancellation, and failure are available through the
//! transaction-scoped UnitOfWork outcome view. Expired recovery is a dedicated
//! cross-table persistence operation so terminal recovery can advance a source
//! cooldown atomically.
//!
//! Clock contract: the implemented first slice still accepts caller-provided
//! `now` values and binds them into production SQL. That is not the target HA
//! contract. Production PostgreSQL operations must derive a single `db_now`
//! from `clock_timestamp()` inside each statement for claim eligibility, lease
//! creation/renewal, live-fence checks, completion, and recovery. The corrected
//! repository owns its clock; the in-memory repository receives an injectable
//! test clock. Application code supplies retry durations or an absolute deferral
//! instant derived from an authoritative database-time sample, not a timestamp
//! used to judge another worker's lease.

use std::{cmp::Ordering, collections::HashMap, fmt::Display, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use tokio::sync::{Mutex, MutexGuard};
use uuid::Uuid;

use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};

use crate::domain::job::{Job, JobError, JobStatus, JobType, LeaseToken, NewJob, PersistedJob};

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
/// The current PostgreSQL implementation backs this interim job-only handle
/// with one SQLx transaction. Cross-repository application commits must instead
/// use `persistence::unit_of_work`, whose transaction-scoped job view will
/// eventually delegate to these operations. Dropping an uncommitted handle must
/// roll the transaction back.
#[allow(async_fn_in_trait)]
pub trait JobRepositoryTransaction {
    // TODO(design): remove caller-provided `now` from lease-sensitive repository
    // APIs when introducing the repository-owned production/test clock.
    /// Inserts a job unless an active job already owns its deduplication key.
    async fn enqueue(&mut self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError>;

    /// Claims the highest-priority due job within this transaction.
    // TODO(design): accept an allowed JobType set so quiet-hours workers cannot
    // claim upstream work, and add a fenced non-failure defer operation.
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
    // TODO(design): remove this from the general/interim transaction contract;
    // successful business-job completion belongs to UnitOfWork only.
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
/// [`PostgresJobRepository`] uses one transaction per convenience mutation and
/// converts zero-row fenced updates into a domain-level lease error rather than
/// silently reporting success. The trait uses native async methods and is
/// intended to be used through a generic repository parameter; an application
/// that needs dynamic dispatch can add an object-safe adapter.
#[allow(async_fn_in_trait)]
pub trait JobRepository: Send + Sync {
    // TODO(design): split this interim all-in-one trait into a queue port,
    // UnitOfWork outcome view, and atomic expired-recovery operation before any
    // application worker is implemented.
    /// Interim job-only transaction type; multi-repository work uses UnitOfWork.
    type Transaction<'a>: JobRepositoryTransaction + 'a
    where
        Self: 'a;

    /// Begins a transaction that must be committed explicitly.
    async fn begin(&self) -> Result<Self::Transaction<'_>, JobRepositoryError>;

    /// Inserts a job unless an active job already owns its deduplication key.
    async fn enqueue(&self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError>;

    /// Claims the highest-priority due job without waiting on another worker.
    // TODO(design): accept an allowed JobType set and expose defer after the job
    // domain/schema gain the corrected state and counters.
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
    // TODO(design): remove this convenience method from the target public queue
    // port so application code cannot commit success separately from data.
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

const JOB_COLUMNS: &str = "id, job_type, source_id, status, priority, run_after, attempts, max_attempts, lease_owner, lease_token, lease_until, heartbeat_at, started_at, finished_at, last_error, payload_json, dedupe_key, created_at, updated_at";
const JOB_COLUMNS_FROM_JOB: &str = "job.id, job.job_type, job.source_id, job.status, job.priority, job.run_after, job.attempts, job.max_attempts, job.lease_owner, job.lease_token, job.lease_until, job.heartbeat_at, job.started_at, job.finished_at, job.last_error, job.payload_json, job.dedupe_key, job.created_at, job.updated_at";
// TODO(design): after the forward migration, include deferred in active/claim
// SQL and replace legacy attempts columns with claim_count/failure_count.
const ACTIVE_STATUSES: &str = "'queued', 'running', 'retry_wait'";

/// SQLx repository backed by the shared PostgreSQL job table.
///
/// Each convenience mutation opens a transaction, performs the operation, and
/// commits before returning. These convenience operations remain suitable for
/// independent enqueue, claim, heartbeat, and recovery work. Final job
/// completion combined with article, source, sync-run, or feed-cache writes must
/// use the shared UnitOfWork instead of [`JobRepository::begin`].
#[derive(Clone)]
pub struct PostgresJobRepository {
    pool: PgPool,
}

impl std::fmt::Debug for PostgresJobRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresJobRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresJobRepository {
    /// Creates a job repository using an existing configured SQLx pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns the underlying pool for health checks and integration setup.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Transaction-scoped SQLx job repository.
pub struct PostgresJobTransaction<'a> {
    // TODO(design): move ownership of this SQLx transaction to UnitOfWork and
    // expose this implementation only as its transaction-scoped job view.
    transaction: Option<Transaction<'a, Postgres>>,
}

impl<'a> PostgresJobTransaction<'a> {
    async fn begin(pool: &'a PgPool) -> Result<Self, JobRepositoryError> {
        let transaction = pool.begin().await.map_err(storage_error)?;
        Ok(Self {
            transaction: Some(transaction),
        })
    }

    fn transaction_mut(&mut self) -> Result<&mut Transaction<'a, Postgres>, JobRepositoryError> {
        self.transaction
            .as_mut()
            .ok_or_else(|| JobRepositoryError::Storage("transaction is closed".to_owned()))
    }

    async fn find_in_transaction(
        &mut self,
        job_id: Uuid,
    ) -> Result<Option<Job>, JobRepositoryError> {
        let query = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1");
        let transaction = self.transaction_mut()?;
        let row = sqlx::query(&query)
            .bind(job_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
        row.map(decode_job).transpose()
    }

    async fn fenced_update_error(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        operation: FencedOperation<'_>,
    ) -> Result<JobRepositoryError, JobRepositoryError> {
        let Some(mut current) = self.find_in_transaction(job_id).await? else {
            return Ok(JobRepositoryError::NotFound { job_id });
        };

        let result = match operation {
            FencedOperation::Heartbeat(lease_for) => {
                current.heartbeat(owner, token, now, lease_for).map(|_| ())
            }
            FencedOperation::Succeed => current.succeed(owner, token, now),
            FencedOperation::Retry { retry_at, error } => current
                .retry(owner, token, now, retry_at, error)
                .map(|_| ()),
            FencedOperation::Fail { error } => current.fail(owner, token, now, error),
        };

        Ok(match result {
            Err(error) => JobRepositoryError::Domain(error),
            Ok(()) => JobRepositoryError::Storage(
                "fenced job update did not match its lease predicate".to_owned(),
            ),
        })
    }

    async fn cancel_update_error(
        &mut self,
        job_id: Uuid,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<JobRepositoryError, JobRepositoryError> {
        let Some(mut current) = self.find_in_transaction(job_id).await? else {
            return Ok(JobRepositoryError::NotFound { job_id });
        };
        Ok(match current.cancel(now, reason) {
            Err(error) => JobRepositoryError::Domain(error),
            Ok(()) => JobRepositoryError::Storage(
                "job cancellation did not match its state predicate".to_owned(),
            ),
        })
    }
}

impl std::fmt::Debug for PostgresJobTransaction<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresJobTransaction")
            .field("open", &self.transaction.is_some())
            .finish()
    }
}

enum FencedOperation<'a> {
    Heartbeat(Duration),
    Succeed,
    Retry {
        retry_at: DateTime<Utc>,
        error: &'a str,
    },
    Fail {
        error: &'a str,
    },
}

// TODO(design): rewrite the production statements around one database-owned
// timestamp CTE; caller-provided timestamps must not decide lease liveness.
impl JobRepositoryTransaction for PostgresJobTransaction<'_> {
    async fn enqueue(&mut self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError> {
        let job = Job::new(spec)?;
        let query = format!(
            r#"
            INSERT INTO jobs ({JOB_COLUMNS})
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            ON CONFLICT (dedupe_key) WHERE status IN ({ACTIVE_STATUSES}) DO NOTHING
            RETURNING {JOB_COLUMNS}
            "#
        );
        let transaction = self.transaction_mut()?;
        let row = sqlx::query(&query)
            .bind(job.id())
            .bind(job_type_name(job.job_type()))
            .bind(job.source_id())
            .bind(status_name(job.status()))
            .bind(job.priority())
            .bind(job.run_after())
            .bind(i64::from(job.attempts()))
            .bind(i64::from(job.max_attempts()))
            .bind(job.lease_owner().map(str::to_owned))
            .bind(job.lease_token().map(LeaseToken::as_uuid))
            .bind(job.lease_until())
            .bind(job.heartbeat_at())
            .bind(job.started_at())
            .bind(job.finished_at())
            .bind(job.last_error().map(str::to_owned))
            .bind(job.payload())
            .bind(job.dedupe_key())
            .bind(job.created_at())
            .bind(job.updated_at())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;

        if let Some(row) = row {
            return Ok(EnqueueResult::Inserted(Box::new(decode_job(row)?)));
        }

        let row = sqlx::query(&format!(
            "SELECT id FROM jobs WHERE dedupe_key = $1 AND status IN ({ACTIVE_STATUSES}) ORDER BY created_at ASC LIMIT 1"
        ))
        .bind(job.dedupe_key())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?;
        let Some(row) = row else {
            return Err(JobRepositoryError::Storage(
                "active job conflict was not found after insert conflict".to_owned(),
            ));
        };
        Ok(EnqueueResult::AlreadyActive {
            job_id: row.try_get("id").map_err(storage_error)?,
        })
    }

    async fn claim_next(
        &mut self,
        owner: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Option<JobLease>, JobRepositoryError> {
        validate_owner(owner)?;
        let lease_until = lease_expiry(now, lease_for)?;
        let token = LeaseToken::new();
        let query = format!(
            r#"
            WITH candidate AS (
                SELECT id
                FROM jobs
                WHERE status IN ('queued', 'retry_wait')
                  AND run_after <= $1
                  AND attempts < max_attempts
                ORDER BY priority DESC, run_after ASC, created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE jobs AS job
            SET status = 'running',
                attempts = job.attempts + 1,
                lease_owner = $2,
                lease_token = $3,
                lease_until = $4,
                heartbeat_at = $1,
                started_at = COALESCE(job.started_at, $1),
                finished_at = NULL,
                updated_at = $1
            FROM candidate
            WHERE job.id = candidate.id
            RETURNING {JOB_COLUMNS_FROM_JOB}
            "#
        );
        let transaction = self.transaction_mut()?;
        let row = sqlx::query(&query)
            .bind(now)
            .bind(owner)
            .bind(token.as_uuid())
            .bind(lease_until)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;

        row.map(|row| {
            Ok(JobLease {
                job: decode_job(row)?,
                token,
            })
        })
        .transpose()
    }

    async fn heartbeat(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Job, JobRepositoryError> {
        validate_owner(owner)?;
        let lease_until = lease_expiry(now, lease_for)?;
        let query = format!(
            r#"
            UPDATE jobs
            SET lease_until = $5, heartbeat_at = $4, updated_at = $4
            WHERE id = $1
              AND status = 'running'
              AND lease_owner = $2
              AND lease_token = $3
              AND lease_until > $4
            RETURNING {JOB_COLUMNS}
            "#
        );
        let transaction = self.transaction_mut()?;
        let row = sqlx::query(&query)
            .bind(job_id)
            .bind(owner)
            .bind(token.as_uuid())
            .bind(now)
            .bind(lease_until)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
        match row {
            Some(row) => decode_job(row),
            None => Err(self
                .fenced_update_error(
                    job_id,
                    owner,
                    token,
                    now,
                    FencedOperation::Heartbeat(lease_for),
                )
                .await?),
        }
    }

    async fn succeed(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
    ) -> Result<Job, JobRepositoryError> {
        validate_owner(owner)?;
        let query = format!(
            r#"
            UPDATE jobs
            SET status = 'succeeded',
                lease_owner = NULL,
                lease_token = NULL,
                lease_until = NULL,
                last_error = NULL,
                finished_at = $4,
                updated_at = $4
            WHERE id = $1
              AND status = 'running'
              AND lease_owner = $2
              AND lease_token = $3
              AND lease_until > $4
            RETURNING {JOB_COLUMNS}
            "#
        );
        let transaction = self.transaction_mut()?;
        let row = sqlx::query(&query)
            .bind(job_id)
            .bind(owner)
            .bind(token.as_uuid())
            .bind(now)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
        match row {
            Some(row) => decode_job(row),
            None => Err(self
                .fenced_update_error(job_id, owner, token, now, FencedOperation::Succeed)
                .await?),
        }
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
        validate_owner(owner)?;
        validate_error(error)?;
        let query = format!(
            r#"
            UPDATE jobs
            SET status = CASE
                    WHEN attempts >= max_attempts THEN 'failed'
                    ELSE 'retry_wait'
                END,
                lease_owner = NULL,
                lease_token = NULL,
                lease_until = NULL,
                run_after = $5,
                last_error = $6,
                finished_at = CASE
                    WHEN attempts >= max_attempts THEN $4
                    ELSE NULL
                END,
                updated_at = $4
            WHERE id = $1
              AND status = 'running'
              AND lease_owner = $2
              AND lease_token = $3
              AND lease_until > $4
            RETURNING {JOB_COLUMNS}
            "#
        );
        let transaction = self.transaction_mut()?;
        let row = sqlx::query(&query)
            .bind(job_id)
            .bind(owner)
            .bind(token.as_uuid())
            .bind(now)
            .bind(retry_at)
            .bind(error)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
        match row {
            Some(row) => decode_job(row),
            None => Err(self
                .fenced_update_error(
                    job_id,
                    owner,
                    token,
                    now,
                    FencedOperation::Retry { retry_at, error },
                )
                .await?),
        }
    }

    async fn fail(
        &mut self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError> {
        validate_owner(owner)?;
        validate_error(error)?;
        let query = format!(
            r#"
            UPDATE jobs
            SET status = 'failed',
                lease_owner = NULL,
                lease_token = NULL,
                lease_until = NULL,
                last_error = $5,
                finished_at = $4,
                updated_at = $4
            WHERE id = $1
              AND status = 'running'
              AND lease_owner = $2
              AND lease_token = $3
              AND lease_until > $4
            RETURNING {JOB_COLUMNS}
            "#
        );
        let transaction = self.transaction_mut()?;
        let row = sqlx::query(&query)
            .bind(job_id)
            .bind(owner)
            .bind(token.as_uuid())
            .bind(now)
            .bind(error)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
        match row {
            Some(row) => decode_job(row),
            None => Err(self
                .fenced_update_error(job_id, owner, token, now, FencedOperation::Fail { error })
                .await?),
        }
    }

    async fn cancel(
        &mut self,
        job_id: Uuid,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<Job, JobRepositoryError> {
        validate_error(reason)?;
        let query = format!(
            r#"
            UPDATE jobs
            SET status = 'failed',
                last_error = $2,
                finished_at = $3,
                updated_at = $3
            WHERE id = $1
              AND status IN ('queued', 'retry_wait')
              AND lease_owner IS NULL
              AND lease_token IS NULL
              AND lease_until IS NULL
            RETURNING {JOB_COLUMNS}
            "#
        );
        let transaction = self.transaction_mut()?;
        let row = sqlx::query(&query)
            .bind(job_id)
            .bind(reason)
            .bind(now)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;
        match row {
            Some(row) => decode_job(row),
            None => Err(self.cancel_update_error(job_id, now, reason).await?),
        }
    }

    async fn recover_expired(
        &mut self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, JobRepositoryError> {
        let limit = i64::try_from(limit).map_err(|_| {
            JobRepositoryError::Storage("recovery batch limit exceeds PostgreSQL range".to_owned())
        })?;
        let query = format!(
            r#"
            WITH expired AS (
                SELECT id
                FROM jobs
                WHERE status = 'running' AND lease_until <= $1
                ORDER BY lease_until ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $2
            )
            UPDATE jobs AS job
            SET status = CASE
                    WHEN job.attempts >= job.max_attempts THEN 'failed'
                    ELSE 'queued'
                END,
                run_after = $1,
                lease_owner = NULL,
                lease_token = NULL,
                lease_until = NULL,
                last_error = 'worker lease expired',
                finished_at = CASE
                    WHEN job.attempts >= job.max_attempts THEN $1
                    ELSE NULL
                END,
                updated_at = $1
            FROM expired
            WHERE job.id = expired.id
            RETURNING {JOB_COLUMNS_FROM_JOB}
            "#
        );
        let transaction = self.transaction_mut()?;
        let rows = sqlx::query(&query)
            .bind(now)
            .bind(limit)
            .fetch_all(&mut **transaction)
            .await
            .map_err(storage_error)?;
        rows.into_iter().map(decode_job).collect()
    }

    async fn commit(mut self) -> Result<(), JobRepositoryError> {
        let transaction = self
            .transaction
            .take()
            .ok_or_else(|| JobRepositoryError::Storage("transaction is closed".to_owned()))?;
        transaction.commit().await.map_err(storage_error)
    }
}

impl JobRepository for PostgresJobRepository {
    type Transaction<'a> = PostgresJobTransaction<'a>;

    async fn begin(&self) -> Result<Self::Transaction<'_>, JobRepositoryError> {
        PostgresJobTransaction::begin(&self.pool).await
    }

    async fn enqueue(&self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError> {
        let mut transaction = self.begin().await?;
        let result = transaction.enqueue(spec).await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn claim_next(
        &self,
        owner: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Option<JobLease>, JobRepositoryError> {
        let mut transaction = self.begin().await?;
        let result = transaction.claim_next(owner, now, lease_for).await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn heartbeat(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<Job, JobRepositoryError> {
        let mut transaction = self.begin().await?;
        let result = transaction
            .heartbeat(job_id, owner, token, now, lease_for)
            .await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn succeed(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
    ) -> Result<Job, JobRepositoryError> {
        let mut transaction = self.begin().await?;
        let result = transaction.succeed(job_id, owner, token, now).await?;
        transaction.commit().await?;
        Ok(result)
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
        let mut transaction = self.begin().await?;
        let result = transaction
            .retry(job_id, owner, token, now, retry_at, error)
            .await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn fail(
        &self,
        job_id: Uuid,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        error: &str,
    ) -> Result<Job, JobRepositoryError> {
        let mut transaction = self.begin().await?;
        let result = transaction.fail(job_id, owner, token, now, error).await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn cancel(
        &self,
        job_id: Uuid,
        now: DateTime<Utc>,
        reason: &str,
    ) -> Result<Job, JobRepositoryError> {
        let mut transaction = self.begin().await?;
        let result = transaction.cancel(job_id, now, reason).await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn recover_expired(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<Job>, JobRepositoryError> {
        let mut transaction = self.begin().await?;
        let result = transaction.recover_expired(now, limit).await?;
        transaction.commit().await?;
        Ok(result)
    }

    async fn find(&self, job_id: Uuid) -> Result<Option<Job>, JobRepositoryError> {
        let mut transaction = self.begin().await?;
        let result = transaction.find_in_transaction(job_id).await?;
        transaction.commit().await?;
        Ok(result)
    }
}

fn storage_error(error: impl Display) -> JobRepositoryError {
    JobRepositoryError::Storage(error.to_string())
}

fn decode_job(row: PgRow) -> Result<Job, JobRepositoryError> {
    let record = PersistedJob {
        id: row.try_get("id").map_err(storage_error)?,
        job_type: parse_job_type(row.try_get("job_type").map_err(storage_error)?)?,
        source_id: row.try_get("source_id").map_err(storage_error)?,
        status: parse_job_status(row.try_get("status").map_err(storage_error)?)?,
        priority: row.try_get("priority").map_err(storage_error)?,
        run_after: row.try_get("run_after").map_err(storage_error)?,
        attempts: persisted_u32(&row, "attempts")?,
        max_attempts: persisted_u32(&row, "max_attempts")?,
        lease_owner: row.try_get("lease_owner").map_err(storage_error)?,
        lease_token: row
            .try_get::<Option<Uuid>, _>("lease_token")
            .map_err(storage_error)?
            .map(LeaseToken::from_uuid),
        lease_until: row.try_get("lease_until").map_err(storage_error)?,
        heartbeat_at: row.try_get("heartbeat_at").map_err(storage_error)?,
        started_at: row.try_get("started_at").map_err(storage_error)?,
        finished_at: row.try_get("finished_at").map_err(storage_error)?,
        last_error: row.try_get("last_error").map_err(storage_error)?,
        payload: row.try_get("payload_json").map_err(storage_error)?,
        dedupe_key: row.try_get("dedupe_key").map_err(storage_error)?,
        created_at: row.try_get("created_at").map_err(storage_error)?,
        updated_at: row.try_get("updated_at").map_err(storage_error)?,
    };
    Job::from_persisted(record).map_err(JobRepositoryError::from)
}

fn persisted_u32(row: &PgRow, column: &str) -> Result<u32, JobRepositoryError> {
    let value: i64 = row.try_get(column).map_err(storage_error)?;
    u32::try_from(value).map_err(|_| {
        JobRepositoryError::Storage(format!(
            "persisted job column {column} is outside the u32 range"
        ))
    })
}

fn parse_job_type(value: String) -> Result<JobType, JobRepositoryError> {
    match value.as_str() {
        "source_sync" => Ok(JobType::SourceSync),
        "feed_rebuild" => Ok(JobType::FeedRebuild),
        "article_backfill" => Ok(JobType::ArticleBackfill),
        "credential_refresh" => Ok(JobType::CredentialRefresh),
        _ => Err(JobRepositoryError::Storage(format!(
            "unknown persisted job_type: {value}"
        ))),
    }
}

fn parse_job_status(value: String) -> Result<JobStatus, JobRepositoryError> {
    match value.as_str() {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "retry_wait" => Ok(JobStatus::RetryWait),
        "succeeded" => Ok(JobStatus::Succeeded),
        "failed" => Ok(JobStatus::Failed),
        _ => Err(JobRepositoryError::Storage(format!(
            "unknown persisted job status: {value}"
        ))),
    }
}

fn job_type_name(job_type: JobType) -> &'static str {
    match job_type {
        JobType::SourceSync => "source_sync",
        JobType::FeedRebuild => "feed_rebuild",
        JobType::ArticleBackfill => "article_backfill",
        JobType::CredentialRefresh => "credential_refresh",
    }
}

fn status_name(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::RetryWait => "retry_wait",
        JobStatus::Succeeded => "succeeded",
        JobStatus::Failed => "failed",
    }
}

fn validate_owner(owner: &str) -> Result<(), JobRepositoryError> {
    if owner.trim().is_empty() {
        Err(JobRepositoryError::Domain(JobError::EmptyLeaseOwner))
    } else {
        Ok(())
    }
}

fn validate_error(error: &str) -> Result<(), JobRepositoryError> {
    if error.trim().is_empty() {
        Err(JobRepositoryError::Domain(JobError::EmptyError))
    } else {
        Ok(())
    }
}

fn lease_expiry(
    now: DateTime<Utc>,
    lease_for: Duration,
) -> Result<DateTime<Utc>, JobRepositoryError> {
    if lease_for <= Duration::zero() {
        return Err(JobRepositoryError::Domain(JobError::InvalidLeaseDuration));
    }
    now.checked_add_signed(lease_for)
        .ok_or(JobRepositoryError::Domain(JobError::InvalidLeaseDuration))
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
