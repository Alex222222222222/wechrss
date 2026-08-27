//! Durable job domain model.
//!
//! A [`Job`] is the storage-independent representation of one scheduled unit
//! of work. PostgreSQL will persist these fields and coordinate concurrent
//! workers, but the invariants and state transitions live here so they are
//! shared by repositories, application services, and tests.
//!
//! Responsibilities:
//!
//! - identify supported work kinds and explicit lifecycle states;
//! - validate creation values such as the deduplication key and attempt limit;
//! - assign and renew an instance-owned lease with a per-claim fencing token;
//! - prevent stale workers from completing, retrying, or failing a job after
//!   its lease has expired; and
//! - move abandoned work back to the queue with a bounded retry count.
//!
//! Non-responsibilities: SQL, row locks, `SKIP LOCKED`, timers, exponential
//! backoff calculation, browser execution, and HTTP responses. The job
//! repository must use a PostgreSQL transaction and row lock around calls that
//! mutate a persisted job. The scheduler supplies `run_after` values and
//! decides which due sources should be enqueued; this module only checks a
//! candidate job's local invariants.
//!
//! The lifecycle is:
//!
//! ```text
//! queued -> running -> succeeded
//! queued -> running -> retry_wait -> running
//! queued/retry_wait -> failed (cancelled)
//! running -> failed
//! running (expired lease) -> queued | failed
//! ```
//!
//! `attempts` counts claims, so the first successful claim changes it from
//! zero to one. A retry returns to `retry_wait` while attempts remain; once
//! the configured maximum is reached, the retry request becomes terminal
//! `failed`. This makes crash recovery bounded even when a worker disappears
//! immediately after claiming a job.
//!
//! High availability depends on two layers. PostgreSQL must atomically claim a
//! due row using `FOR UPDATE SKIP LOCKED` and persist the lease owner, fencing
//! token, and expiry. This model then rejects work from a stale owner or stale
//! claim, allowing another application replica to recover an expired lease
//! safely. Job handlers still need idempotent article and cache writes because
//! a process can crash after performing side effects but before completing its
//! job.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

/// Categories of durable work known to the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobType {
    /// Fetch the current article list and pages for one source.
    SourceSync,
    /// Render and persist one source's RSS document from archived records.
    FeedRebuild,
    /// Fetch or repair one article that was not complete during a sync.
    ArticleBackfill,
    /// Refresh an account credential or complete an interactive login flow.
    CredentialRefresh,
}

/// Persisted lifecycle state for a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Eligible work that has not been claimed by a worker.
    Queued,
    /// Work currently owned by a worker lease.
    Running,
    /// Work waiting until `run_after` for another attempt.
    RetryWait,
    /// Work completed successfully.
    Succeeded,
    /// Work stopped permanently or exhausted its attempts.
    Failed,
}

impl JobStatus {
    /// Returns whether the status can be selected as active work.
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::RetryWait)
    }

    /// Returns whether no further worker transition is expected.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }
}

/// Input required to create a queued job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewJob {
    /// Kind of work the application service will execute.
    pub job_type: JobType,
    /// Source associated with the job; global jobs may omit it.
    pub source_id: Option<Uuid>,
    /// Higher values may be selected first by the repository.
    pub priority: i32,
    /// Earliest time at which a worker may claim the job.
    pub run_after: DateTime<Utc>,
    /// Maximum number of claims, including the first claim.
    pub max_attempts: u32,
    /// Typed job parameters. Secrets must not be placed in this payload.
    pub payload: Value,
    /// Key used by the repository's active-job uniqueness constraint.
    pub dedupe_key: String,
    /// Creation time supplied by the application clock.
    pub now: DateTime<Utc>,
}

/// Unforgeable identity for one claim of a job lease.
///
/// The owner identifies an application instance, while this token identifies
/// one lease incarnation. Both values are required for worker mutations so a
/// stale worker cannot act after the same instance reclaims an expired job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseToken(Uuid);

impl LeaseToken {
    /// Returns the UUID persisted with the job lease.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// A validated, durable job and its lease state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    id: Uuid,
    job_type: JobType,
    source_id: Option<Uuid>,
    status: JobStatus,
    priority: i32,
    run_after: DateTime<Utc>,
    attempts: u32,
    max_attempts: u32,
    lease_owner: Option<String>,
    lease_token: Option<LeaseToken>,
    lease_until: Option<DateTime<Utc>>,
    heartbeat_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    last_error: Option<String>,
    payload: Value,
    dedupe_key: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl Job {
    /// Creates a queued job with no lease and zero claims.
    pub fn new(spec: NewJob) -> Result<Self, JobError> {
        if spec.max_attempts == 0 {
            return Err(JobError::InvalidAttemptLimit);
        }
        if spec.dedupe_key.trim().is_empty() {
            return Err(JobError::EmptyDedupeKey);
        }

        Ok(Self {
            id: Uuid::new_v4(),
            job_type: spec.job_type,
            source_id: spec.source_id,
            status: JobStatus::Queued,
            priority: spec.priority,
            run_after: spec.run_after,
            attempts: 0,
            max_attempts: spec.max_attempts,
            lease_owner: None,
            lease_token: None,
            lease_until: None,
            heartbeat_at: None,
            started_at: None,
            finished_at: None,
            last_error: None,
            payload: spec.payload,
            dedupe_key: spec.dedupe_key,
            created_at: spec.now,
            updated_at: spec.now,
        })
    }

    /// Returns the immutable job identifier.
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the kind of work represented by this job.
    pub const fn job_type(&self) -> JobType {
        self.job_type
    }

    /// Returns the optional source relationship.
    pub const fn source_id(&self) -> Option<Uuid> {
        self.source_id
    }

    /// Returns the current lifecycle state.
    pub const fn status(&self) -> JobStatus {
        self.status
    }

    /// Returns the scheduling priority.
    pub const fn priority(&self) -> i32 {
        self.priority
    }

    /// Returns the earliest claim time.
    pub const fn run_after(&self) -> DateTime<Utc> {
        self.run_after
    }

    /// Returns the number of worker claims made so far.
    pub const fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Returns the maximum number of claims permitted.
    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Returns the currently assigned instance, if any.
    pub fn lease_owner(&self) -> Option<&str> {
        self.lease_owner.as_deref()
    }

    /// Returns the fencing token for the current lease, if any.
    pub const fn lease_token(&self) -> Option<LeaseToken> {
        self.lease_token
    }

    /// Returns the lease expiry, if the job is currently leased.
    pub const fn lease_until(&self) -> Option<DateTime<Utc>> {
        self.lease_until
    }

    /// Returns the last lease heartbeat time.
    pub const fn heartbeat_at(&self) -> Option<DateTime<Utc>> {
        self.heartbeat_at
    }

    /// Returns when the first worker started this job.
    pub const fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }

    /// Returns when this job reached a terminal state.
    pub const fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }

    /// Returns the most recent retry or recovery error.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Returns the JSON parameters for the worker.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// Returns the key used for active-job deduplication.
    pub fn dedupe_key(&self) -> &str {
        &self.dedupe_key
    }

    /// Returns the insertion timestamp.
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the timestamp of the most recent domain mutation.
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Claims a due queued or retry-wait job for one application instance.
    ///
    /// The repository must perform the due-row selection and update under a
    /// PostgreSQL lock. This method is the second line of defense and updates
    /// the in-memory/domain representation with the same lease values.
    pub fn claim(
        &mut self,
        owner: &str,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<LeaseToken, JobError> {
        validate_owner(owner)?;
        let lease_until = lease_expiry(now, lease_for)?;
        if !matches!(self.status, JobStatus::Queued | JobStatus::RetryWait) {
            return Err(JobError::InvalidTransition {
                status: self.status,
                operation: "claim",
            });
        }
        if self.run_after > now {
            return Err(JobError::NotDue);
        }
        if self.attempts >= self.max_attempts {
            return Err(JobError::AttemptsExhausted);
        }

        let lease_token = LeaseToken(Uuid::new_v4());
        self.attempts += 1;
        self.status = JobStatus::Running;
        self.lease_owner = Some(owner.to_owned());
        self.lease_token = Some(lease_token);
        self.lease_until = Some(lease_until);
        self.heartbeat_at = Some(now);
        self.started_at.get_or_insert(now);
        self.finished_at = None;
        self.updated_at = now;
        Ok(lease_token)
    }

    /// Extends a live lease for its current owner.
    pub fn heartbeat(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        lease_for: Duration,
    ) -> Result<(), JobError> {
        validate_owner(owner)?;
        let lease_until = lease_expiry(now, lease_for)?;
        self.ensure_live_owner(owner, token, now, "heartbeat")?;
        self.lease_until = Some(lease_until);
        self.heartbeat_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Marks a live job as successfully completed by its current owner.
    pub fn succeed(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
    ) -> Result<(), JobError> {
        validate_owner(owner)?;
        self.ensure_live_owner(owner, token, now, "succeed")?;
        self.status = JobStatus::Succeeded;
        self.clear_lease();
        self.last_error = None;
        self.finished_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Records a retry or turns the job terminal when attempts are exhausted.
    ///
    /// The returned status tells the application service whether it should
    /// enqueue future work (`RetryWait`) or record a terminal failure
    /// (`Failed`). Backoff is intentionally calculated by the application
    /// service, so policy changes do not belong in this domain transition.
    pub fn retry(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        retry_at: DateTime<Utc>,
        error: impl Into<String>,
    ) -> Result<JobStatus, JobError> {
        validate_owner(owner)?;
        let error = nonempty_error(error)?;
        self.ensure_live_owner(owner, token, now, "retry")?;

        self.last_error = Some(error);
        self.run_after = retry_at;
        self.clear_lease();
        self.updated_at = now;

        if self.attempts >= self.max_attempts {
            self.status = JobStatus::Failed;
            self.finished_at = Some(now);
            Ok(JobStatus::Failed)
        } else {
            self.status = JobStatus::RetryWait;
            self.finished_at = None;
            Ok(JobStatus::RetryWait)
        }
    }

    /// Permanently fails a live job with an operator-visible error summary.
    pub fn fail(
        &mut self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        error: impl Into<String>,
    ) -> Result<(), JobError> {
        validate_owner(owner)?;
        let error = nonempty_error(error)?;
        self.ensure_live_owner(owner, token, now, "fail")?;
        self.status = JobStatus::Failed;
        self.clear_lease();
        self.last_error = Some(error);
        self.finished_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Cancels an unclaimed job and records a terminal failure reason.
    ///
    /// Queued and retry-wait jobs have no worker owner, so cancellation is an
    /// ownerless administrative transition. Running jobs must use [`Self::fail`]
    /// with their current lease token, preventing an operator action from
    /// racing an active worker without repository-level authorization.
    pub fn cancel(
        &mut self,
        now: DateTime<Utc>,
        reason: impl Into<String>,
    ) -> Result<(), JobError> {
        let reason = nonempty_error(reason)?;
        if !matches!(self.status, JobStatus::Queued | JobStatus::RetryWait) {
            return Err(JobError::InvalidTransition {
                status: self.status,
                operation: "cancel",
            });
        }
        self.status = JobStatus::Failed;
        self.clear_lease();
        self.last_error = Some(reason);
        self.finished_at = Some(now);
        self.updated_at = now;
        Ok(())
    }

    /// Recovers a running job whose lease is absent or no longer valid.
    ///
    /// Returns `true` when this call changed the job. A live job and a job that
    /// has already been recovered both return `false`, which makes a locked
    /// recovery pass naturally idempotent. Jobs with attempts remaining return
    /// to `queued`; jobs at the attempt limit become terminal `failed`.
    pub fn recover_expired_lease(&mut self, now: DateTime<Utc>) -> bool {
        if self.status != JobStatus::Running
            || self
                .lease_until
                .is_some_and(|lease_until| lease_until > now)
        {
            return false;
        }

        self.clear_lease();
        self.run_after = now;
        self.last_error = Some("worker lease expired".to_owned());
        self.updated_at = now;
        if self.attempts >= self.max_attempts {
            self.status = JobStatus::Failed;
            self.finished_at = Some(now);
        } else {
            self.status = JobStatus::Queued;
            self.finished_at = None;
        }
        true
    }

    fn ensure_live_owner(
        &self,
        owner: &str,
        token: LeaseToken,
        now: DateTime<Utc>,
        operation: &'static str,
    ) -> Result<(), JobError> {
        if self.status != JobStatus::Running {
            return Err(JobError::InvalidTransition {
                status: self.status,
                operation,
            });
        }
        if self.lease_owner.as_deref() != Some(owner) {
            return Err(JobError::LeaseOwnerMismatch);
        }
        if self.lease_token != Some(token) {
            return Err(JobError::LeaseTokenMismatch);
        }
        match self.lease_until {
            Some(lease_until) if lease_until > now => Ok(()),
            Some(_) => Err(JobError::LeaseExpired),
            None => Err(JobError::LeaseMissing),
        }
    }

    fn clear_lease(&mut self) {
        self.lease_owner = None;
        self.lease_token = None;
        self.lease_until = None;
    }
}

/// Errors raised when a job violates a domain invariant or transition rule.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum JobError {
    /// A job cannot be created without at least one allowed claim.
    #[error("job max_attempts must be greater than zero")]
    InvalidAttemptLimit,
    /// Active-job deduplication requires a stable non-empty key.
    #[error("job dedupe_key must not be empty")]
    EmptyDedupeKey,
    /// A worker identity cannot be blank.
    #[error("job lease owner must not be empty")]
    EmptyLeaseOwner,
    /// A lease must have a positive representable duration.
    #[error("job lease duration must be positive and fit in the timestamp range")]
    InvalidLeaseDuration,
    /// The job is not due at the supplied clock time.
    #[error("job is not due")]
    NotDue,
    /// The job has no claims remaining.
    #[error("job has exhausted its attempts")]
    AttemptsExhausted,
    /// A worker attempted an operation from an incompatible state.
    #[error("cannot {operation} a job in {status:?} state")]
    InvalidTransition {
        /// Current state at the time of the rejected operation.
        status: JobStatus,
        /// Domain operation requested by the caller.
        operation: &'static str,
    },
    /// A different instance currently owns the lease.
    #[error("job lease belongs to another instance")]
    LeaseOwnerMismatch,
    /// The worker supplied a token from an older lease incarnation.
    #[error("job lease token is no longer current")]
    LeaseTokenMismatch,
    /// The lease ended before the worker attempted its operation.
    #[error("job lease has expired")]
    LeaseExpired,
    /// A running job was missing its lease expiry.
    #[error("running job has no lease expiry")]
    LeaseMissing,
    /// A terminal operation needs a non-empty persisted error summary.
    #[error("job error summary must not be empty")]
    EmptyError,
}

fn validate_owner(owner: &str) -> Result<(), JobError> {
    if owner.trim().is_empty() {
        Err(JobError::EmptyLeaseOwner)
    } else {
        Ok(())
    }
}

fn nonempty_error(error: impl Into<String>) -> Result<String, JobError> {
    let error = error.into();
    if error.trim().is_empty() {
        Err(JobError::EmptyError)
    } else {
        Ok(error)
    }
}

fn lease_expiry(now: DateTime<Utc>, lease_for: Duration) -> Result<DateTime<Utc>, JobError> {
    if lease_for <= Duration::zero() {
        return Err(JobError::InvalidLeaseDuration);
    }
    now.checked_add_signed(lease_for)
        .ok_or(JobError::InvalidLeaseDuration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    const OWNER: &str = "instance-a";
    const OTHER_OWNER: &str = "instance-b";

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn new_job(max_attempts: u32) -> Job {
        Job::new(NewJob {
            job_type: JobType::SourceSync,
            source_id: Some(Uuid::nil()),
            priority: 10,
            run_after: at(100),
            max_attempts,
            payload: json!({"source_id": Uuid::nil()}),
            dedupe_key: "source-sync:nil".to_owned(),
            now: at(90),
        })
        .expect("test job should be valid")
    }

    #[test]
    fn rejects_invalid_creation_values() {
        let invalid_attempts = Job::new(NewJob {
            job_type: JobType::FeedRebuild,
            source_id: None,
            priority: 0,
            run_after: at(0),
            max_attempts: 0,
            payload: Value::Null,
            dedupe_key: "feed".to_owned(),
            now: at(0),
        });
        assert_eq!(invalid_attempts, Err(JobError::InvalidAttemptLimit));

        let empty_key = Job::new(NewJob {
            job_type: JobType::FeedRebuild,
            source_id: None,
            priority: 0,
            run_after: at(0),
            max_attempts: 1,
            payload: Value::Null,
            dedupe_key: "  ".to_owned(),
            now: at(0),
        });
        assert_eq!(empty_key, Err(JobError::EmptyDedupeKey));
    }

    #[test]
    fn claim_requires_due_time_and_records_lease_state() {
        let mut job = new_job(3);
        assert_eq!(
            job.claim(OWNER, at(99), Duration::seconds(30)),
            Err(JobError::NotDue)
        );
        assert_eq!(job.status(), JobStatus::Queued);
        assert_eq!(job.attempts(), 0);

        let token = job
            .claim(OWNER, at(100), Duration::seconds(30))
            .expect("due job should be claimable");
        assert_eq!(job.status(), JobStatus::Running);
        assert_eq!(job.attempts(), 1);
        assert_eq!(job.lease_owner(), Some(OWNER));
        assert_eq!(job.lease_token(), Some(token));
        assert_eq!(job.lease_until(), Some(at(130)));
        assert_eq!(job.heartbeat_at(), Some(at(100)));
        assert_eq!(job.started_at(), Some(at(100)));
        assert!(!job.status().is_terminal());
    }

    #[test]
    fn heartbeat_and_completion_reject_wrong_or_expired_owners() {
        let mut job = new_job(2);
        let token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();

        assert_eq!(
            job.heartbeat(OTHER_OWNER, token, at(110), Duration::seconds(30)),
            Err(JobError::LeaseOwnerMismatch)
        );
        assert_eq!(
            job.succeed(OWNER, token, at(130)),
            Err(JobError::LeaseExpired)
        );
        assert_eq!(job.status(), JobStatus::Running);

        job.heartbeat(OWNER, token, at(120), Duration::seconds(60))
            .expect("current owner should renew a live lease");
        job.succeed(OWNER, token, at(150))
            .expect("current owner should complete a renewed lease");
        assert_eq!(job.status(), JobStatus::Succeeded);
        assert_eq!(job.finished_at(), Some(at(150)));
        assert_eq!(job.lease_owner(), None);
        assert_eq!(job.lease_until(), None);
        assert!(job.status().is_terminal());
    }

    #[test]
    fn retry_waits_then_fails_when_attempts_are_exhausted() {
        let mut job = new_job(2);
        let token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();

        let status = job
            .retry(OWNER, token, at(110), at(200), "temporary browser failure")
            .expect("first attempt should be retryable");
        assert_eq!(status, JobStatus::RetryWait);
        assert_eq!(job.status(), JobStatus::RetryWait);
        assert_eq!(job.attempts(), 1);
        assert_eq!(job.run_after(), at(200));
        assert_eq!(job.last_error(), Some("temporary browser failure"));
        assert_eq!(job.lease_owner(), None);

        let token = job.claim(OWNER, at(200), Duration::seconds(30)).unwrap();
        let status = job
            .retry(OWNER, token, at(210), at(300), "second browser failure")
            .expect("exhaustion should become a terminal result");
        assert_eq!(status, JobStatus::Failed);
        assert_eq!(job.status(), JobStatus::Failed);
        assert_eq!(job.attempts(), 2);
        assert_eq!(job.finished_at(), Some(at(210)));
        assert!(job.status().is_terminal());
    }

    #[test]
    fn expired_lease_recovery_is_idempotent_and_bounded() {
        let mut retryable = new_job(2);
        retryable
            .claim(OWNER, at(100), Duration::seconds(30))
            .unwrap();
        assert!(retryable.recover_expired_lease(at(130)));
        assert_eq!(retryable.status(), JobStatus::Queued);
        assert_eq!(retryable.run_after(), at(130));
        assert_eq!(retryable.last_error(), Some("worker lease expired"));
        assert!(!retryable.recover_expired_lease(at(131)));

        let mut exhausted = new_job(1);
        exhausted
            .claim(OWNER, at(100), Duration::seconds(30))
            .unwrap();
        assert!(exhausted.recover_expired_lease(at(130)));
        assert_eq!(exhausted.status(), JobStatus::Failed);
        assert_eq!(exhausted.finished_at(), Some(at(130)));
    }

    #[test]
    fn invalid_lease_and_terminal_transitions_are_rejected() {
        let mut job = new_job(1);
        assert_eq!(
            job.claim(OWNER, at(100), Duration::zero()),
            Err(JobError::InvalidLeaseDuration)
        );
        assert_eq!(
            job.claim(" ", at(100), Duration::seconds(30)),
            Err(JobError::EmptyLeaseOwner)
        );

        let token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();
        job.fail(OWNER, token, at(110), "operator stopped job")
            .unwrap();
        assert_eq!(
            job.succeed(OWNER, token, at(111)),
            Err(JobError::InvalidTransition {
                status: JobStatus::Failed,
                operation: "succeed"
            })
        );
        assert_eq!(
            job.retry(OWNER, token, at(111), at(120), "late retry"),
            Err(JobError::InvalidTransition {
                status: JobStatus::Failed,
                operation: "retry"
            })
        );
    }

    #[test]
    fn fencing_token_rejects_a_stale_claim_from_the_same_owner() {
        let mut job = new_job(3);
        let first_token = job.claim(OWNER, at(100), Duration::seconds(30)).unwrap();
        assert!(job.recover_expired_lease(at(130)));

        let second_token = job.claim(OWNER, at(130), Duration::seconds(30)).unwrap();
        assert_ne!(first_token, second_token);
        assert_eq!(
            job.succeed(OWNER, first_token, at(140)),
            Err(JobError::LeaseTokenMismatch)
        );
        assert_eq!(job.status(), JobStatus::Running);
        job.succeed(OWNER, second_token, at(140))
            .expect("current claim should remain usable");
    }

    #[test]
    fn queued_and_retry_wait_jobs_can_be_cancelled_without_a_lease() {
        let mut queued = new_job(2);
        queued
            .cancel(at(101), "source was deleted")
            .expect("queued job should be cancellable");
        assert_eq!(queued.status(), JobStatus::Failed);
        assert_eq!(queued.last_error(), Some("source was deleted"));
        assert_eq!(queued.finished_at(), Some(at(101)));

        let mut retry_wait = new_job(2);
        let token = retry_wait
            .claim(OWNER, at(100), Duration::seconds(30))
            .unwrap();
        retry_wait
            .retry(OWNER, token, at(110), at(200), "temporary failure")
            .unwrap();
        retry_wait
            .cancel(at(120), "operator disabled source")
            .expect("retry-wait job should be cancellable");
        assert_eq!(retry_wait.status(), JobStatus::Failed);
        assert_eq!(retry_wait.finished_at(), Some(at(120)));
    }
}
