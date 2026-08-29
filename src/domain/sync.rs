//! Synchronization-run domain values.
//!
//! A [`SyncRun`] is the durable audit record for one source synchronization
//! attempt. It records the outcome, article/archive counts, optional feed
//! revision, and a classified safe error summary. It is deliberately separate
//! from [`crate::domain::job::Job`]: a job is queue coordination, while a run
//! describes what the synchronization observed and how it ended.
//!
//! Responsibilities:
//!
//! - keep the persisted outcome vocabulary stable;
//! - require a running row before a worker can finish a run;
//! - validate outcome/error combinations and non-negative counters; and
//! - expose immutable values for repositories and operational views.
//!
//! Non-responsibilities: executing browser work, classifying raw errors,
//! calculating retries, changing source scheduling gates, writing SQL, or
//! rebuilding RSS. Application services classify an acquisition error and then
//! pass a safe [`SyncFailure`] to the transaction-scoped repository.
//!
//! Cache interaction: a successful run may carry the source feed revision that
//! was published in the same `UnitOfWork`. A deferred, blocked, or failed run
//! does not itself invalidate the feed cache. Article and cache writes remain
//! independently responsible for reporting whether RSS-visible state changed.
//!
//! High availability: multiple replicas may create runs for different leased
//! jobs. The optional `job_id` correlates a run with queue coordination, while
//! the database primary key and row lock make starting/finishing one run
//! idempotent at the persistence boundary.

use chrono::{DateTime, Utc};
use thiserror::Error;
use uuid::Uuid;

use super::source::{FeedRevision, SourceId};

/// Durable result category for one synchronization attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The run has started but no final outcome has been recorded yet.
    Running,
    /// Acquisition and final persistence completed successfully.
    Succeeded,
    /// Work stopped at a quiet-hours boundary without consuming retry budget.
    Deferred,
    /// Authentication must be repaired before automatic work resumes.
    AuthenticationRequired,
    /// Risk-control response requires an operator decision.
    RiskControlled,
    /// Upstream verification or access policy blocked the run.
    Blocked,
    /// The run ended with a retryable failure; the job policy decides when to
    /// claim the next attempt.
    RetryableFailure,
    /// The run ended with a non-retryable or exhausted failure.
    Failed,
}

impl SyncOutcome {
    /// Returns the stable value stored in PostgreSQL.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Deferred => "deferred",
            Self::AuthenticationRequired => "authentication_required",
            Self::RiskControlled => "risk_controlled",
            Self::Blocked => "blocked",
            Self::RetryableFailure => "retryable_failure",
            Self::Failed => "failed",
        }
    }

    /// Parses the stable PostgreSQL representation.
    pub fn parse(value: &str) -> Result<Self, SyncError> {
        match value {
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "deferred" => Ok(Self::Deferred),
            "authentication_required" => Ok(Self::AuthenticationRequired),
            "risk_controlled" => Ok(Self::RiskControlled),
            "blocked" => Ok(Self::Blocked),
            "retryable_failure" => Ok(Self::RetryableFailure),
            "failed" => Ok(Self::Failed),
            _ => Err(SyncError::InvalidOutcome),
        }
    }

    /// Reports whether the row has a final outcome.
    pub const fn is_finished(self) -> bool {
        !matches!(self, Self::Running)
    }

    const fn requires_failure(self) -> bool {
        matches!(
            self,
            Self::AuthenticationRequired
                | Self::RiskControlled
                | Self::Blocked
                | Self::RetryableFailure
                | Self::Failed
        )
    }
}

/// Classification attached to a safe failure summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncFailureClass {
    /// The WeRead login/session has expired or is no longer accepted.
    AuthenticationExpired,
    /// Upstream anti-abuse or risk-control behavior was detected.
    RiskControlled,
    /// Navigation or verification was blocked by an access policy.
    Blocked,
    /// The operation can be retried according to job policy.
    Retryable,
    /// The failure is not expected to succeed by retrying this run.
    Permanent,
}

impl SyncFailureClass {
    /// Returns the stable value stored in PostgreSQL.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuthenticationExpired => "authentication_expired",
            Self::RiskControlled => "risk_controlled",
            Self::Blocked => "blocked",
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
        }
    }

    /// Parses the stable PostgreSQL representation.
    pub fn parse(value: &str) -> Result<Self, SyncError> {
        match value {
            "authentication_expired" => Ok(Self::AuthenticationExpired),
            "risk_controlled" => Ok(Self::RiskControlled),
            "blocked" => Ok(Self::Blocked),
            "retryable" => Ok(Self::Retryable),
            "permanent" => Ok(Self::Permanent),
            _ => Err(SyncError::InvalidFailureClass),
        }
    }
}

/// A classified, secret-safe error summary for a completed unsuccessful run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncFailure {
    class: SyncFailureClass,
    message: String,
}

impl SyncFailure {
    /// Creates a failure after trimming and validating its operator-safe text.
    pub fn new(class: SyncFailureClass, message: impl Into<String>) -> Result<Self, SyncError> {
        let message = message.into().trim().to_owned();
        if message.is_empty() {
            return Err(SyncError::EmptyFailureMessage);
        }
        if message.len() > 4096 {
            return Err(SyncError::FailureMessageTooLong);
        }
        Ok(Self { class, message })
    }

    /// Returns the failure category.
    pub const fn class(&self) -> SyncFailureClass {
        self.class
    }

    /// Returns the safe diagnostic summary.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Counters collected during one synchronization run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncStats {
    /// Number of upstream article records observed.
    pub articles_seen: u32,
    /// Number of article rows created by the final persistence step.
    pub articles_created: u32,
    /// Number of existing article rows changed by the final persistence step.
    pub articles_updated: u32,
    /// Number of article records that could not be completed.
    pub articles_failed: u32,
    /// Number of article records whose HTML was archived.
    pub archived_articles: u32,
    /// Number of optional binary assets archived.
    pub archived_assets: u32,
}

/// Input for creating a new running record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewSyncRun {
    /// Durable run identifier.
    pub id: Uuid,
    /// Source being synchronized.
    pub source_id: SourceId,
    /// Queue job that owns this run, when one exists.
    pub job_id: Option<Uuid>,
    /// Time at which synchronization began.
    pub started_at: DateTime<Utc>,
}

/// Input for finishing an existing running record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRunCompletion {
    /// Final run outcome; `Running` is not accepted here.
    pub outcome: SyncOutcome,
    /// Time at which the outcome was known.
    pub finished_at: DateTime<Utc>,
    /// Counters collected by acquisition and archive stages.
    pub stats: SyncStats,
    /// Failure details for failure-class outcomes.
    pub failure: Option<SyncFailure>,
    /// Feed revision published by the same final transaction, if any.
    pub feed_revision: Option<FeedRevision>,
}

/// Errors raised while constructing or transitioning synchronization runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SyncError {
    /// A run id must not be nil.
    #[error("sync run id must not be nil")]
    InvalidId,
    /// A source id must not be nil.
    #[error("sync run source id must not be nil")]
    InvalidSourceId,
    /// A correlated job id must not be nil when present.
    #[error("sync run job id must not be nil")]
    InvalidJobId,
    /// A stored outcome is not in the supported vocabulary.
    #[error("sync run outcome is invalid")]
    InvalidOutcome,
    /// A stored failure class is not in the supported vocabulary.
    #[error("sync run failure class is invalid")]
    InvalidFailureClass,
    /// A completed unsuccessful outcome must carry a failure.
    #[error("sync run outcome requires a failure")]
    MissingFailure,
    /// A successful or deferred outcome cannot carry a failure.
    #[error("sync run outcome must not carry a failure")]
    UnexpectedFailure,
    /// The failure class must agree with the outcome category.
    #[error("sync run failure class does not match its outcome")]
    MismatchedFailureClass,
    /// A failure summary must contain text.
    #[error("sync run failure message must not be empty")]
    EmptyFailureMessage,
    /// Failure summaries are bounded to prevent unbounded diagnostics.
    #[error("sync run failure message is too long")]
    FailureMessageTooLong,
    /// A completion cannot transition a run that is already finished.
    #[error("sync run is already finished")]
    AlreadyFinished,
    /// A run cannot finish before it started.
    #[error("sync run completion precedes its start")]
    CompletionBeforeStart,
}

/// Immutable durable synchronization result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncRun {
    id: Uuid,
    source_id: SourceId,
    job_id: Option<Uuid>,
    outcome: SyncOutcome,
    stats: SyncStats,
    failure: Option<SyncFailure>,
    feed_revision: Option<FeedRevision>,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl SyncRun {
    /// Creates a new running synchronization record.
    pub fn start(spec: NewSyncRun) -> Result<Self, SyncError> {
        if spec.id.is_nil() {
            return Err(SyncError::InvalidId);
        }
        if spec.source_id.as_uuid().is_nil() {
            return Err(SyncError::InvalidSourceId);
        }
        if spec.job_id.is_some_and(|job_id| job_id.is_nil()) {
            return Err(SyncError::InvalidJobId);
        }
        Ok(Self {
            id: spec.id,
            source_id: spec.source_id,
            job_id: spec.job_id,
            outcome: SyncOutcome::Running,
            stats: SyncStats::default(),
            failure: None,
            feed_revision: None,
            started_at: spec.started_at,
            finished_at: None,
            created_at: spec.started_at,
            updated_at: spec.started_at,
        })
    }

    /// Reconstructs a persisted run after validating its state combinations.
    pub(crate) fn from_parts(parts: SyncRunParts) -> Result<Self, SyncError> {
        if parts.id.is_nil() {
            return Err(SyncError::InvalidId);
        }
        if parts.source_id.as_uuid().is_nil() {
            return Err(SyncError::InvalidSourceId);
        }
        if parts.job_id.is_some_and(|job_id| job_id.is_nil()) {
            return Err(SyncError::InvalidJobId);
        }
        if parts
            .finished_at
            .is_some_and(|finished_at| finished_at < parts.started_at)
        {
            return Err(SyncError::CompletionBeforeStart);
        }
        validate_outcome(parts.outcome, parts.failure.as_ref(), parts.finished_at)?;
        Ok(Self {
            id: parts.id,
            source_id: parts.source_id,
            job_id: parts.job_id,
            outcome: parts.outcome,
            stats: parts.stats,
            failure: parts.failure,
            feed_revision: parts.feed_revision,
            started_at: parts.started_at,
            finished_at: parts.finished_at,
            created_at: parts.created_at,
            updated_at: parts.updated_at,
        })
    }

    /// Applies one final outcome to a running record.
    pub fn finish(mut self, completion: SyncRunCompletion) -> Result<Self, SyncError> {
        if self.outcome.is_finished() {
            return Err(SyncError::AlreadyFinished);
        }
        if !completion.outcome.is_finished() {
            return Err(SyncError::InvalidOutcome);
        }
        if completion.finished_at < self.started_at {
            return Err(SyncError::CompletionBeforeStart);
        }
        validate_outcome(
            completion.outcome,
            completion.failure.as_ref(),
            Some(completion.finished_at),
        )?;
        self.outcome = completion.outcome;
        self.stats = completion.stats;
        self.failure = completion.failure;
        self.feed_revision = completion.feed_revision;
        self.finished_at = Some(completion.finished_at);
        self.updated_at = completion.finished_at;
        Ok(self)
    }

    /// Returns the durable run id.
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the owning source.
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// Returns the correlated queue job id.
    pub const fn job_id(&self) -> Option<Uuid> {
        self.job_id
    }

    /// Returns the final or in-progress outcome.
    pub const fn outcome(&self) -> SyncOutcome {
        self.outcome
    }

    /// Returns collected counters.
    pub const fn stats(&self) -> SyncStats {
        self.stats
    }

    /// Returns the safe failure, when present.
    pub fn failure(&self) -> Option<&SyncFailure> {
        self.failure.as_ref()
    }

    /// Returns the feed revision published by the run, when present.
    pub const fn feed_revision(&self) -> Option<FeedRevision> {
        self.feed_revision
    }

    /// Returns when acquisition started.
    pub const fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    /// Returns when the run finished.
    pub const fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }

    /// Returns when the row was created.
    pub const fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns when the row was last changed.
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
}

/// Trusted persisted fields used by the repository decoder.
#[derive(Debug)]
pub(crate) struct SyncRunParts {
    pub(crate) id: Uuid,
    pub(crate) source_id: SourceId,
    pub(crate) job_id: Option<Uuid>,
    pub(crate) outcome: SyncOutcome,
    pub(crate) stats: SyncStats,
    pub(crate) failure: Option<SyncFailure>,
    pub(crate) feed_revision: Option<FeedRevision>,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

fn validate_outcome(
    outcome: SyncOutcome,
    failure: Option<&SyncFailure>,
    finished_at: Option<DateTime<Utc>>,
) -> Result<(), SyncError> {
    if !outcome.is_finished() {
        if finished_at.is_some() {
            return Err(SyncError::InvalidOutcome);
        }
        return if failure.is_some() {
            Err(SyncError::UnexpectedFailure)
        } else {
            Ok(())
        };
    }
    if finished_at.is_none() {
        return Err(SyncError::InvalidOutcome);
    }
    if outcome.requires_failure() != failure.is_some() {
        return if outcome.requires_failure() {
            Err(SyncError::MissingFailure)
        } else {
            Err(SyncError::UnexpectedFailure)
        };
    }
    if let Some(failure) = failure {
        let class_matches = match outcome {
            SyncOutcome::AuthenticationRequired => {
                failure.class() == SyncFailureClass::AuthenticationExpired
            }
            SyncOutcome::RiskControlled => failure.class() == SyncFailureClass::RiskControlled,
            SyncOutcome::Blocked => failure.class() == SyncFailureClass::Blocked,
            SyncOutcome::RetryableFailure => failure.class() == SyncFailureClass::Retryable,
            // A terminal failure may be either a permanent failure or an
            // exhausted retryable failure. Authentication, risk-control, and
            // blocked failures have dedicated outcomes so callers cannot lose
            // their operator-actionable classification.
            SyncOutcome::Failed => matches!(
                failure.class(),
                SyncFailureClass::Permanent | SyncFailureClass::Retryable
            ),
            SyncOutcome::Running | SyncOutcome::Succeeded | SyncOutcome::Deferred => false,
        };
        if !class_matches {
            return Err(SyncError::MismatchedFailureClass);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};

    use super::*;

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    fn started() -> SyncRun {
        SyncRun::start(NewSyncRun {
            id: Uuid::from_u128(2),
            source_id: source_id(),
            job_id: Some(Uuid::from_u128(3)),
            started_at: at(10),
        })
        .expect("run should start")
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("timestamp should be valid")
    }

    #[test]
    fn successful_run_preserves_stats_and_feed_revision() {
        let run = started()
            .finish(SyncRunCompletion {
                outcome: SyncOutcome::Succeeded,
                finished_at: at(20),
                stats: SyncStats {
                    articles_seen: 4,
                    articles_created: 1,
                    articles_updated: 2,
                    articles_failed: 1,
                    archived_articles: 3,
                    archived_assets: 0,
                },
                failure: None,
                feed_revision: Some(FeedRevision::from_u64(8)),
            })
            .expect("run should finish");

        assert_eq!(run.outcome(), SyncOutcome::Succeeded);
        assert_eq!(run.stats().articles_updated, 2);
        assert_eq!(run.feed_revision().unwrap().as_u64(), 8);
        assert_eq!(run.finished_at(), Some(at(20)));
    }

    #[test]
    fn failure_outcomes_require_bounded_safe_error_details() {
        let failure = SyncFailure::new(SyncFailureClass::Retryable, " temporary timeout ")
            .expect("failure should be valid");
        let run = started()
            .finish(SyncRunCompletion {
                outcome: SyncOutcome::RetryableFailure,
                finished_at: at(20),
                stats: SyncStats::default(),
                failure: Some(failure),
                feed_revision: None,
            })
            .expect("failure should finish");

        assert_eq!(run.failure().unwrap().message(), "temporary timeout");
        assert_eq!(run.failure().unwrap().class(), SyncFailureClass::Retryable);
        assert_eq!(
            SyncFailure::new(SyncFailureClass::Permanent, " "),
            Err(SyncError::EmptyFailureMessage)
        );
    }

    #[test]
    fn deferred_runs_are_non_failure_outcomes() {
        let result = started().finish(SyncRunCompletion {
            outcome: SyncOutcome::Deferred,
            finished_at: at(20),
            stats: SyncStats::default(),
            failure: Some(
                SyncFailure::new(SyncFailureClass::Retryable, "quiet hours")
                    .expect("failure should be valid"),
            ),
            feed_revision: None,
        });

        assert_eq!(result, Err(SyncError::UnexpectedFailure));
    }

    #[test]
    fn finish_rejects_running_or_mismatched_failure_outcomes() {
        let running = started().finish(SyncRunCompletion {
            outcome: SyncOutcome::Running,
            finished_at: at(20),
            stats: SyncStats::default(),
            failure: None,
            feed_revision: None,
        });
        assert_eq!(running, Err(SyncError::InvalidOutcome));

        let mismatched = started().finish(SyncRunCompletion {
            outcome: SyncOutcome::AuthenticationRequired,
            finished_at: at(20),
            stats: SyncStats::default(),
            failure: Some(
                SyncFailure::new(SyncFailureClass::Retryable, "temporary")
                    .expect("failure should be valid"),
            ),
            feed_revision: None,
        });
        assert_eq!(mismatched, Err(SyncError::MismatchedFailureClass));

        let generic_auth_failure = started().finish(SyncRunCompletion {
            outcome: SyncOutcome::Failed,
            finished_at: at(20),
            stats: SyncStats::default(),
            failure: Some(
                SyncFailure::new(SyncFailureClass::AuthenticationExpired, "login expired")
                    .expect("failure should be valid"),
            ),
            feed_revision: None,
        });
        assert_eq!(generic_auth_failure, Err(SyncError::MismatchedFailureClass));

        let before_start = started().finish(SyncRunCompletion {
            outcome: SyncOutcome::Succeeded,
            finished_at: at(9),
            stats: SyncStats::default(),
            failure: None,
            feed_revision: None,
        });
        assert_eq!(before_start, Err(SyncError::CompletionBeforeStart));
    }

    #[test]
    fn outcome_names_are_stable() {
        assert_eq!(
            SyncOutcome::parse("authentication_required").unwrap(),
            SyncOutcome::AuthenticationRequired
        );
        assert_eq!(SyncOutcome::Failed.as_str(), "failed");
        assert_eq!(SyncFailureClass::Retryable.as_str(), "retryable");
    }
}
