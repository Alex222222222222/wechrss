//! One-pass due-work scheduling.
//!
//! This module is the executable application boundary around the atomic
//! [`SchedulerRepository`] operation. A scheduler replica passes its quiet-hours
//! policy to the repository, which samples the PostgreSQL clock and evaluates
//! that policy before locking, filtering, enqueueing, and reserving one bounded
//! batch. It does not read source rows first, insert jobs separately, or run
//! synchronization itself.
//!
//! The one-pass API is intentionally separate from a future Tokio polling loop.
//! That keeps the policy and orchestration testable, lets deployment choose its
//! shutdown and backoff behavior, and prevents a loop from accidentally
//! bypassing the repository's `FOR UPDATE SKIP LOCKED` transaction.
//!
//! Quiet hours are evaluated against the database-authoritative instant and the
//! configured IANA timezone inside the scheduling transaction. A quiet pass
//! performs no source writes; a pass at the exclusive end boundary proceeds.
//! The repository remains responsible for due-time selection, active-job
//! deduplication, source gates, and reservation persistence.
//!
//! High availability: every replica may invoke `run_once` concurrently. The
//! scheduler has no process-local coordination state, so disjoint source
//! batches and duplicate suppression come from the database transaction and
//! indexes. A repository failure is returned to the future loop for metrics and
//! retry handling; it is never converted into a false successful scheduling
//! pass.
//!
//! Non-responsibilities: worker claims, browser work, retry backoff, sleeping,
//! RSS rendering, and HTTP responses. Feed-cache maintenance jobs may use the
//! same queue independently because this wrapper only schedules source-sync
//! work.

use chrono::Duration;
use thiserror::Error;

use crate::{
    domain::pacing::QuietHours,
    persistence::repositories::scheduler_repository::{
        EnqueuedSource, SchedulerPass, SchedulerRepository, SchedulerRepositoryError,
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
}

/// Result of one scheduler pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerRun {
    /// No upstream source work was enqueued because quiet hours are active.
    SkippedQuietHours,
    /// The repository completed its atomic enqueue/reservation operation.
    Enqueued {
        /// Sources whose source-sync jobs were inserted.
        sources: Vec<EnqueuedSource>,
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
}

impl<R> Scheduler<R> {
    /// Creates a scheduler with optional local quiet hours.
    pub fn new(repository: R, config: SchedulerConfig, quiet_hours: Option<QuietHours>) -> Self {
        Self {
            repository,
            config,
            quiet_hours,
        }
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
        let result = self
            .repository
            .enqueue_due_sources(
                self.config.batch_limit,
                self.config.reservation_for,
                self.quiet_hours,
            )
            .await?;
        match result {
            SchedulerPass::SkippedQuietHours => Ok(SchedulerRun::SkippedQuietHours),
            SchedulerPass::Enqueued(sources) => Ok(SchedulerRun::Enqueued { sources }),
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

    #[derive(Clone)]
    struct RecordingRepository {
        calls: Arc<Mutex<Vec<RepositoryCall>>>,
        database_now: DateTime<Utc>,
        fail: bool,
    }

    impl Default for RecordingRepository {
        fn default() -> Self {
            Self {
                calls: Arc::default(),
                database_now: at("2026-08-28T12:00:00Z"),
                fail: false,
            }
        }
    }

    impl RecordingRepository {
        async fn calls(&self) -> Vec<RepositoryCall> {
            self.calls.lock().await.clone()
        }
    }

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

        assert_eq!(
            scheduler.run_once().await.unwrap(),
            SchedulerRun::SkippedQuietHours
        );
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
            SchedulerRun::Enqueued { sources } if sources.is_empty()
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
            SchedulerRun::Enqueued { sources } if sources.is_empty()
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

    #[test]
    fn default_settings_are_bounded_and_positive() {
        let config = SchedulerConfig::default();
        assert!(config.batch_limit() > 0);
        assert!(config.batch_limit() <= MAX_BATCH_LIMIT);
        assert!(config.reservation_for() > Duration::zero());
    }
}
