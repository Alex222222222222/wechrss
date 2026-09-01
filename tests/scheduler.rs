//! Integration-style tests for scheduler polling and shutdown behavior.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use chrono::Duration;
use tokio::sync::watch;
use wechrss::{
    application::scheduler::{Scheduler, SchedulerConfig, SchedulerLoopConfig, SchedulerLoopStats},
    persistence::repositories::scheduler_repository::{
        SchedulerPass, SchedulerRepository, SchedulerRepositoryError,
    },
};

#[derive(Clone)]
struct ShutdownRepository {
    calls: Arc<AtomicUsize>,
    shutdown: watch::Sender<bool>,
    stop_after: usize,
    fail_first: bool,
}

impl SchedulerRepository for ShutdownRepository {
    async fn enqueue_due_sources(
        &self,
        _limit: usize,
        _reservation_for: Duration,
        _quiet_hours: Option<wechrss::domain::pacing::QuietHours>,
    ) -> Result<SchedulerPass, SchedulerRepositoryError> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        if self.fail_first && call == 1 {
            self.shutdown
                .send(true)
                .expect("scheduler loop receiver should still be alive");
            return Err(SchedulerRepositoryError::InvalidReservation);
        }
        if call >= self.stop_after {
            self.shutdown
                .send(true)
                .expect("scheduler loop receiver should still be alive");
        }
        Ok(SchedulerPass::Enqueued(Vec::new()))
    }
}

fn scheduler(repository: ShutdownRepository) -> Scheduler<ShutdownRepository> {
    Scheduler::new(
        repository,
        SchedulerConfig::new(10, Duration::minutes(1))
            .expect("scheduler configuration should be valid"),
        None,
    )
}

fn loop_config() -> SchedulerLoopConfig {
    SchedulerLoopConfig::new(
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    )
    .expect("scheduler loop configuration should be valid")
}

#[tokio::test]
async fn scheduler_loop_dispatches_multiple_passes_before_shutdown() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let calls = Arc::new(AtomicUsize::new(0));
    let repository = ShutdownRepository {
        calls: calls.clone(),
        shutdown: shutdown_tx,
        stop_after: 2,
        fail_first: false,
    };

    let stats = scheduler(repository)
        .run_until_shutdown(shutdown_rx, loop_config())
        .await;

    assert_eq!(
        stats,
        SchedulerLoopStats {
            passes: 2,
            enqueued_sources: 0,
            enqueued_credential_refreshes: 0,
            quiet_passes: 0,
            errors: 0,
        }
    );
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn scheduler_loop_counts_an_error_before_shutdown() {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let calls = Arc::new(AtomicUsize::new(0));
    let repository = ShutdownRepository {
        calls: calls.clone(),
        shutdown: shutdown_tx,
        stop_after: 1,
        fail_first: true,
    };

    let stats = scheduler(repository)
        .run_until_shutdown(shutdown_rx, loop_config())
        .await;

    assert_eq!(
        stats,
        SchedulerLoopStats {
            passes: 1,
            enqueued_sources: 0,
            enqueued_credential_refreshes: 0,
            quiet_passes: 0,
            errors: 1,
        }
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn scheduler_loop_with_pre_requested_shutdown_skips_repository() {
    let (shutdown_tx, shutdown_rx) = watch::channel(true);
    let calls = Arc::new(AtomicUsize::new(0));
    let repository = ShutdownRepository {
        calls: calls.clone(),
        shutdown: shutdown_tx,
        stop_after: 1,
        fail_first: false,
    };

    let stats = scheduler(repository)
        .run_until_shutdown(shutdown_rx, loop_config())
        .await;

    assert_eq!(stats, SchedulerLoopStats::default());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}
