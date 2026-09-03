//! One-pass due-work scheduling.
//!
//! This module is the executable application boundary around the atomic
//! [`SchedulerRepository`] operation. A scheduler replica passes its quiet-hours
//! policy to the repository, which samples the PostgreSQL clock and evaluates
//! that policy before locking, filtering, enqueueing, and reserving one bounded
//! batch. When configured, the same pass also schedules due credential-refresh
//! jobs. It does not read source rows first, insert jobs separately, or run
//! synchronization itself.
//!
//! [`Scheduler::run_until_shutdown`] supplies the Tokio polling boundary around
//! the one-pass operation. It keeps deployment-specific shutdown and timing
//! policy outside the repository while ensuring every pass still uses the
//! repository's `FOR UPDATE SKIP LOCKED` transaction.
//!
//! Quiet hours are evaluated against the database-authoritative instant and the
//! configured IANA timezone inside the scheduling transaction. A quiet pass
//! performs no source writes; a pass at the exclusive end boundary proceeds.
//! The repository remains responsible for due-time selection, active-job
//! deduplication, source gates, account expiry selection, and reservation
//! persistence.
//!
//! High availability: every replica may invoke `run_once` concurrently. The
//! scheduler has no process-local coordination state, so disjoint source
//! batches and duplicate suppression come from the database transaction and
//! indexes. A repository failure is returned to the polling loop for metrics
//! and retry handling; it is never converted into a false successful
//! scheduling pass.
//!
//! Non-responsibilities: worker claims, browser work, RSS rendering, and HTTP
//! responses. Feed-cache maintenance jobs may use the same queue independently
//! because this wrapper schedules durable queue work but never executes it.

use std::time::Duration as StdDuration;

use chrono::Duration;
use thiserror::Error;
use tokio::time;

use crate::{
    domain::pacing::QuietHours,
    persistence::repositories::scheduler_repository::{
        EnqueuedCredentialRefresh, EnqueuedSource, SchedulerPass, SchedulerRepository,
        SchedulerRepositoryError,
    },
};

const MAX_BATCH_LIMIT: usize = 1_000;

/// Validated settings for one scheduler pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerConfig {
    batch_limit: usize,
    reservation_for: Duration,
}

impl SchedulerConfig {
    /// Creates bounded scheduler settings.
    pub fn new(
        batch_limit: usize,
        reservation_for: Duration,
    ) -> Result<Self, SchedulerConfigError> {
        if batch_limit == 0 || batch_limit > MAX_BATCH_LIMIT {
            return Err(SchedulerConfigError::InvalidBatchLimit { value: batch_limit });
        }
        if reservation_for <= Duration::zero() || reservation_for.num_milliseconds() <= 0 {
            return Err(SchedulerConfigError::InvalidReservation);
        }
        Ok(Self {
            batch_limit,
            reservation_for,
        })
    }

    /// Returns the maximum number of sources one pass may enqueue.
    pub const fn batch_limit(self) -> usize {
        self.batch_limit
    }

    /// Returns the short source reservation duration.
    pub const fn reservation_for(self) -> Duration {
        self.reservation_for
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            batch_limit: 100,
            reservation_for: Duration::minutes(1),
        }
    }
}

/// Invalid scheduler-pass settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SchedulerConfigError {
    /// The scheduler must use a positive bounded batch size.
    #[error("scheduler batch limit must be between 1 and 1000, got {value}")]
    InvalidBatchLimit {
        /// Configured batch size.
        value: usize,
    },
    /// Reservations must be representable as a positive millisecond interval.
    #[error("scheduler reservation must be a positive whole number of milliseconds")]
    InvalidReservation,
    /// The credential refresh window cannot be negative.
    #[error("credential refresh window must not be negative")]
    InvalidCredentialRefreshWindow,
    /// Credential refresh jobs need at least one allowed attempt.
    #[error("credential refresh max attempts must be positive")]
    InvalidCredentialRefreshAttempts,
}

/// Settings for scheduling non-interactive credential refresh work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CredentialRefreshScheduleConfig {
    refresh_before: Duration,
    max_attempts: u32,
}

impl CredentialRefreshScheduleConfig {
    /// Creates a bounded credential-refresh scheduling policy.
    pub fn new(refresh_before: Duration, max_attempts: u32) -> Result<Self, SchedulerConfigError> {
        if refresh_before < Duration::zero() {
            return Err(SchedulerConfigError::InvalidCredentialRefreshWindow);
        }
        if max_attempts == 0 {
            return Err(SchedulerConfigError::InvalidCredentialRefreshAttempts);
        }
        Ok(Self {
            refresh_before,
            max_attempts,
        })
    }

    /// Returns the window before expiry in which a refresh job is created.
    pub const fn refresh_before(self) -> Duration {
        self.refresh_before
    }

    /// Returns the retry budget assigned to refresh jobs.
    pub const fn max_attempts(self) -> u32 {
        self.max_attempts
    }
}

/// Validated polling policy for the shutdown-aware scheduler loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedulerLoopConfig {
    poll_interval: StdDuration,
    error_backoff: StdDuration,
}

impl SchedulerLoopConfig {
    /// Creates a loop policy with positive polling and error waits.
    pub fn new(
        poll_interval: StdDuration,
        error_backoff: StdDuration,
    ) -> Result<Self, SchedulerLoopConfigError> {
        if poll_interval.is_zero() {
            return Err(SchedulerLoopConfigError::InvalidPollInterval);
        }
        if error_backoff.is_zero() {
            return Err(SchedulerLoopConfigError::InvalidErrorBackoff);
        }
        Ok(Self {
            poll_interval,
            error_backoff,
        })
    }

    /// Returns the delay between successful or quiet scheduling passes.
    pub const fn poll_interval(self) -> StdDuration {
        self.poll_interval
    }

    /// Returns the delay after a transient repository error.
    pub const fn error_backoff(self) -> StdDuration {
        self.error_backoff
    }
}

/// Invalid scheduler-loop timing policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum SchedulerLoopConfigError {
    /// The scheduler must not poll in a tight loop.
    #[error("scheduler poll interval must be positive")]
    InvalidPollInterval,
    /// Repeated repository errors must be rate limited.
    #[error("scheduler error backoff must be positive")]
    InvalidErrorBackoff,
}

/// Counters returned when a scheduler loop observes shutdown.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SchedulerLoopStats {
    /// Number of scheduling passes, including quiet and failed passes.
    pub passes: u64,
    /// Number of source-sync jobs inserted across all passes.
    pub enqueued_sources: usize,
    /// Number of credential-refresh jobs inserted across all passes.
    pub enqueued_credential_refreshes: usize,
    /// Number of passes skipped because quiet hours were active.
    pub quiet_passes: u64,
    /// Number of repository failures retried by the loop.
    pub errors: u64,
}

/// Result of one scheduler pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerRun {
    /// No source work was enqueued because quiet hours are active.
    SkippedQuietHours {
        /// Credential-refresh jobs inserted before the quiet-hours decision.
        credential_refreshes: Vec<EnqueuedCredentialRefresh>,
    },
    /// The repository completed its atomic enqueue/reservation operation.
    Enqueued {
        /// Sources whose source-sync jobs were inserted.
        sources: Vec<EnqueuedSource>,
        /// Credential-refresh jobs inserted for accounts nearing expiry.
        credential_refreshes: Vec<EnqueuedCredentialRefresh>,
    },
}

/// Errors raised by one scheduler pass.
#[derive(Debug, Error)]
pub enum SchedulerError {
    /// The atomic source enqueue operation failed.
    #[error(transparent)]
    Repository(#[from] SchedulerRepositoryError),
}

/// Application scheduler over an atomic due-source repository.
pub struct Scheduler<R> {
    repository: R,
    config: SchedulerConfig,
    quiet_hours: Option<QuietHours>,
    credential_refresh: Option<CredentialRefreshScheduleConfig>,
}

impl<R> Scheduler<R> {
    /// Creates a scheduler with optional local quiet hours.
    pub fn new(repository: R, config: SchedulerConfig, quiet_hours: Option<QuietHours>) -> Self {
        Self {
            repository,
            config,
            quiet_hours,
            credential_refresh: None,
        }
    }

    /// Enables account credential-refresh scheduling for this scheduler.
    pub fn with_credential_refresh(mut self, config: CredentialRefreshScheduleConfig) -> Self {
        self.credential_refresh = Some(config);
        self
    }

    /// Returns the settings used by this scheduler.
    pub const fn config(&self) -> SchedulerConfig {
        self.config
    }
}

impl<R> Scheduler<R>
where
    R: SchedulerRepository,
{
    /// Runs one bounded scheduling pass.
    pub async fn run_once(&self) -> Result<SchedulerRun, SchedulerError> {
        let credential_refreshes = match self.credential_refresh {
            Some(config) => {
                self.repository
                    .enqueue_due_credential_refreshes(
                        self.config.batch_limit,
                        config.refresh_before,
                        config.max_attempts,
                    )
                    .await?
            }
            None => Vec::new(),
        };
        let result = self
            .repository
            .enqueue_due_sources(
                self.config.batch_limit,
                self.config.reservation_for,
                self.quiet_hours,
            )
            .await?;
        match result {
            SchedulerPass::SkippedQuietHours => Ok(SchedulerRun::SkippedQuietHours {
                credential_refreshes,
            }),
            SchedulerPass::Enqueued(sources) => Ok(SchedulerRun::Enqueued {
                sources,
                credential_refreshes,
            }),
        }
    }

    /// Polls the scheduler until the shutdown watch becomes true or is dropped.
    ///
    /// Every completed pass waits for the configured poll interval so a
    /// scheduler replica cannot hot-loop against PostgreSQL. Repository errors
    /// use a separate backoff and are retried; shutdown is checked before each
    /// pass and while waiting. A pass already inside the repository operation
    /// is allowed to finish before the loop returns.
    pub async fn run_until_shutdown(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        loop_config: SchedulerLoopConfig,
    ) -> SchedulerLoopStats {
        let mut stats = SchedulerLoopStats::default();
        loop {
            if shutdown.has_changed().is_err() || *shutdown.borrow() {
                return stats;
            }

            stats.passes += 1;
            let wait = match self.run_once().await {
                Ok(SchedulerRun::SkippedQuietHours {
                    credential_refreshes,
                }) => {
                    stats.quiet_passes += 1;
                    stats.enqueued_credential_refreshes = stats
                        .enqueued_credential_refreshes
                        .saturating_add(credential_refreshes.len());
                    loop_config.poll_interval()
                }
                Ok(SchedulerRun::Enqueued {
                    sources,
                    credential_refreshes,
                }) => {
                    stats.enqueued_sources = stats.enqueued_sources.saturating_add(sources.len());
                    stats.enqueued_credential_refreshes = stats
                        .enqueued_credential_refreshes
                        .saturating_add(credential_refreshes.len());
                    loop_config.poll_interval()
                }
                Err(error) => {
                    stats.errors += 1;
                    tracing::warn!(error = %error, "scheduler pass failed; retrying");
                    loop_config.error_backoff()
                }
            };

            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return stats;
                    }
                }
                _ = time::sleep(wait) => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, NaiveTime, Utc};
    use tokio::sync::Mutex;

    use super::*;

    type RepositoryCall = (usize, Duration, Option<QuietHours>);
    type RefreshRepositoryCall = (usize, Duration, u32);

    #[derive(Clone)]
    struct RecordingRepository {
        calls: Arc<Mutex<Vec<RepositoryCall>>>,
        refresh_calls: Arc<Mutex<Vec<RefreshRepositoryCall>>>,
        database_now: DateTime<Utc>,
        fail: bool,
    }

    impl Default for RecordingRepository {
        fn default() -> Self {
            Self {
                calls: Arc::default(),
                refresh_calls: Arc::default(),
                database_now: at("2026-08-28T12:00:00Z"),
                fail: false,
            }
        }
    }

    impl RecordingRepository {
        async fn calls(&self) -> Vec<RepositoryCall> {
            self.calls.lock().await.clone()
        }

        async fn refresh_calls(&self) -> Vec<RefreshRepositoryCall> {
            self.refresh_calls.lock().await.clone()
        }
    }

    #[async_trait::async_trait]
    impl SchedulerRepository for RecordingRepository {
        async fn enqueue_due_sources(
            &self,
            limit: usize,
            reservation_for: Duration,
            quiet_hours: Option<QuietHours>,
        ) -> Result<SchedulerPass, SchedulerRepositoryError> {
            self.calls
                .lock()
                .await
                .push((limit, reservation_for, quiet_hours));
            if quiet_hours.is_some_and(|quiet_hours| quiet_hours.is_quiet_at(self.database_now)) {
                return Ok(SchedulerPass::SkippedQuietHours);
            }
            if self.fail {
                Err(SchedulerRepositoryError::InvalidReservation)
            } else {
                Ok(SchedulerPass::Enqueued(Vec::new()))
            }
        }

        async fn enqueue_due_credential_refreshes(
            &self,
            limit: usize,
            refresh_before: Duration,
            max_attempts: u32,
        ) -> Result<Vec<EnqueuedCredentialRefresh>, SchedulerRepositoryError> {
            self.refresh_calls
                .lock()
                .await
                .push((limit, refresh_before, max_attempts));
            Ok(vec![EnqueuedCredentialRefresh::new(
                crate::domain::credentials::WeReadAccountId::from_uuid(uuid::Uuid::from_u128(1)),
                uuid::Uuid::from_u128(2),
            )])
        }
    }

    fn quiet_hours() -> QuietHours {
        QuietHours::new(
            chrono_tz::UTC,
            NaiveTime::from_hms_opt(23, 0, 0).expect("valid test time"),
            NaiveTime::from_hms_opt(7, 0, 0).expect("valid test time"),
        )
        .expect("quiet-hours endpoints differ")
    }

    fn at(value: &str) -> DateTime<Utc> {
        value.parse().expect("valid test timestamp")
    }

    #[test]
    fn rejects_zero_oversized_and_sub_millisecond_settings() {
        assert!(matches!(
            SchedulerConfig::new(0, Duration::seconds(1)),
            Err(SchedulerConfigError::InvalidBatchLimit { value: 0 })
        ));
        assert!(matches!(
            SchedulerConfig::new(MAX_BATCH_LIMIT + 1, Duration::seconds(1)),
            Err(SchedulerConfigError::InvalidBatchLimit { .. })
        ));
        assert!(matches!(
            SchedulerConfig::new(1, Duration::microseconds(1)),
            Err(SchedulerConfigError::InvalidReservation)
        ));
        assert!(matches!(
            SchedulerConfig::new(1, Duration::zero()),
            Err(SchedulerConfigError::InvalidReservation)
        ));
        assert!(matches!(
            CredentialRefreshScheduleConfig::new(Duration::seconds(-1), 3),
            Err(SchedulerConfigError::InvalidCredentialRefreshWindow)
        ));
        assert!(matches!(
            CredentialRefreshScheduleConfig::new(Duration::minutes(5), 0),
            Err(SchedulerConfigError::InvalidCredentialRefreshAttempts)
        ));
    }

    #[test]
    fn rejects_zero_scheduler_loop_waits() {
        assert_eq!(
            SchedulerLoopConfig::new(StdDuration::ZERO, StdDuration::from_secs(1)),
            Err(SchedulerLoopConfigError::InvalidPollInterval)
        );
        assert_eq!(
            SchedulerLoopConfig::new(StdDuration::from_secs(1), StdDuration::ZERO),
            Err(SchedulerLoopConfigError::InvalidErrorBackoff)
        );
    }

    #[tokio::test]
    async fn uses_repository_clock_during_overnight_quiet_hours() {
        let repository = RecordingRepository {
            database_now: at("2026-08-28T01:00:00Z"),
            ..RecordingRepository::default()
        };
        let scheduler = Scheduler::new(
            repository.clone(),
            SchedulerConfig::default(),
            Some(quiet_hours()),
        );

        assert!(matches!(
            scheduler.run_once().await.unwrap(),
            SchedulerRun::SkippedQuietHours { credential_refreshes }
                if credential_refreshes.is_empty()
        ));
        assert_eq!(
            repository.calls().await,
            vec![(100, Duration::minutes(1), Some(quiet_hours()))]
        );
    }

    #[tokio::test]
    async fn enqueues_at_the_exclusive_quiet_hours_end_boundary() {
        let repository = RecordingRepository::default();
        let config = SchedulerConfig::new(7, Duration::seconds(45)).unwrap();
        let scheduler = Scheduler::new(repository.clone(), config, Some(quiet_hours()));

        assert!(matches!(
            scheduler.run_once().await.unwrap(),
            SchedulerRun::Enqueued {
                sources,
                credential_refreshes,
            } if sources.is_empty() && credential_refreshes.is_empty()
        ));
        assert_eq!(
            repository.calls().await,
            vec![(7, Duration::seconds(45), Some(quiet_hours()))]
        );
    }

    #[tokio::test]
    async fn delegates_without_quiet_hours_and_propagates_repository_errors() {
        let repository = RecordingRepository::default();
        let scheduler = Scheduler::new(repository.clone(), SchedulerConfig::default(), None);
        assert!(matches!(
            scheduler.run_once().await.unwrap(),
            SchedulerRun::Enqueued {
                sources,
                credential_refreshes,
            } if sources.is_empty() && credential_refreshes.is_empty()
        ));
        assert_eq!(repository.calls().await.len(), 1);

        let failing_repository = RecordingRepository {
            fail: true,
            ..RecordingRepository::default()
        };
        let scheduler =
            Scheduler::new(failing_repository.clone(), SchedulerConfig::default(), None);
        assert!(matches!(
            scheduler.run_once().await,
            Err(SchedulerError::Repository(
                SchedulerRepositoryError::InvalidReservation
            ))
        ));
        assert_eq!(failing_repository.calls().await.len(), 1);
    }

    #[tokio::test]
    async fn forwards_credential_refresh_schedule_and_reports_inserted_jobs() {
        let repository = RecordingRepository::default();
        let scheduler = Scheduler::new(repository.clone(), SchedulerConfig::default(), None)
            .with_credential_refresh(
                CredentialRefreshScheduleConfig::new(Duration::minutes(5), 4)
                    .expect("refresh configuration should be valid"),
            );

        let SchedulerRun::Enqueued {
            sources,
            credential_refreshes,
        } = scheduler
            .run_once()
            .await
            .expect("scheduler should succeed")
        else {
            panic!("scheduler should not be quiet in this test");
        };
        assert!(sources.is_empty());
        assert_eq!(credential_refreshes.len(), 1);
        assert_eq!(
            repository.refresh_calls().await,
            vec![(100, Duration::minutes(5), 4)]
        );
    }

    #[test]
    fn default_settings_are_bounded_and_positive() {
        let config = SchedulerConfig::default();
        assert!(config.batch_limit() > 0);
        assert!(config.batch_limit() <= MAX_BATCH_LIMIT);
        assert!(config.reservation_for() > Duration::zero());
    }
}
