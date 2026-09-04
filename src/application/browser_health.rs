//! Shared browser-sidecar health state and worker readiness.
//!
//! The WebDriver sidecar is an optional dependency of the HTTP API but a
//! required dependency of browser-backed worker jobs.  This module keeps those
//! contracts separate: API liveness and PostgreSQL readiness remain useful
//! while a sidecar is down, whereas workers use the last health snapshot to
//! avoid claiming work they cannot execute.

use std::time::Duration;

use chrono_tz::Tz;
use tokio::{
    sync::watch,
    time::{self, MissedTickBehavior},
};

use crate::acquisition::{
    browser_pool::BrowserPool,
    webdriver::{WebDriverError, WebDriverFactory, WebDriverHealthCheck},
};

use super::worker::BrowserJobReadiness;

/// Health state for one independently reported runtime component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserComponentStatus {
    /// No probe has completed yet.
    Unknown,
    /// The component passed its latest check.
    Ready,
    /// The component responded but cannot currently accept work.
    NotReady,
    /// The component could not be contacted or its session could not be used.
    Unavailable,
    /// The component responded, but a configured environment invariant failed.
    Mismatch,
}

/// The browser-related readiness snapshot shared by API and workers.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BrowserHealthSnapshot {
    /// Whether the WebDriver sidecar can accept and use sessions.
    pub webdriver: BrowserComponentStatus,
    /// Whether the browser-visible timezone matches the configured timezone.
    pub timezone: BrowserComponentStatus,
    /// Timezone configured for the application and browser profile.
    pub configured_timezone: String,
    /// Timezone observed from the most recent browser session, when available.
    pub observed_timezone: Option<String>,
}

impl BrowserHealthSnapshot {
    /// Returns whether browser-backed worker jobs may be claimed.
    pub fn worker_ready(&self) -> bool {
        self.webdriver == BrowserComponentStatus::Ready
            && self.timezone == BrowserComponentStatus::Ready
    }
}

/// Concurrently readable browser-health state.
#[derive(Clone)]
pub struct BrowserHealth {
    state: watch::Sender<BrowserHealthSnapshot>,
}

impl BrowserHealth {
    /// Creates an initially unknown snapshot for the configured timezone.
    pub fn new(configured_timezone: Tz) -> Self {
        let snapshot = BrowserHealthSnapshot {
            webdriver: BrowserComponentStatus::Unknown,
            timezone: BrowserComponentStatus::Unknown,
            configured_timezone: configured_timezone.to_string(),
            observed_timezone: None,
        };
        let (state, _) = watch::channel(snapshot);
        Self { state }
    }

    /// Returns the latest immutable snapshot.
    pub fn snapshot(&self) -> BrowserHealthSnapshot {
        self.state.borrow().clone()
    }

    /// Replaces the snapshot without requiring a receiver to remain alive.
    pub(crate) fn replace(&self, snapshot: BrowserHealthSnapshot) {
        self.state.send_replace(snapshot);
    }

    /// Returns whether the worker may claim browser-backed jobs.
    pub fn browser_jobs_allowed(&self) -> bool {
        self.snapshot().worker_ready()
    }
}

impl BrowserJobReadiness for BrowserHealth {
    fn browser_jobs_allowed(&self) -> bool {
        BrowserHealth::browser_jobs_allowed(self)
    }
}

/// Periodically probes a WebDriver sidecar and publishes the result.
#[derive(Clone)]
pub struct BrowserHealthMonitor {
    factory: WebDriverFactory,
    probe_pool: BrowserPool,
    health: BrowserHealth,
}

impl BrowserHealthMonitor {
    /// Creates a monitor with an isolated one-session probe pool.
    pub fn new(
        factory: WebDriverFactory,
        health: BrowserHealth,
    ) -> Result<Self, crate::acquisition::browser_pool::BrowserPoolError> {
        Ok(Self {
            factory,
            probe_pool: BrowserPool::new(1)?,
            health,
        })
    }

    /// Creates a monitor that shares the runtime browser capacity with real
    /// jobs. A busy pool causes a probe to be skipped rather than opening an
    /// extra remote session and falsely reporting a one-slot sidecar as down.
    pub fn with_browser_pool(
        factory: WebDriverFactory,
        health: BrowserHealth,
        browser_pool: BrowserPool,
    ) -> Self {
        Self {
            factory,
            probe_pool: browser_pool,
            health,
        }
    }

    /// Runs one health probe and publishes its component statuses.
    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn refresh(&self) {
        let result = self.factory.health_check(&self.probe_pool).await;
        if matches!(result, Ok(WebDriverHealthCheck::Busy)) {
            tracing::debug!("browser health probe skipped while browser capacity is busy");
            return;
        }
        let next = self.snapshot_for_result(result);

        let previous = self.health.snapshot();
        if previous != next {
            tracing::info!(
                webdriver = ?next.webdriver,
                timezone = ?next.timezone,
                "browser health status changed"
            );
        }
        self.health.replace(next);
    }

    fn snapshot_for_result(
        &self,
        result: Result<WebDriverHealthCheck, WebDriverError>,
    ) -> BrowserHealthSnapshot {
        match result {
            Ok(WebDriverHealthCheck::NotReady) => {
                tracing::warn!("WebDriver sidecar is not ready");
                self.snapshot_with(
                    BrowserComponentStatus::NotReady,
                    BrowserComponentStatus::Unknown,
                    None,
                )
            }
            Ok(WebDriverHealthCheck::Busy) => self.health.snapshot(),
            Ok(WebDriverHealthCheck::Ready { environment }) => {
                let timezone_status = if self.factory.expected_timezone().is_some() {
                    BrowserComponentStatus::Ready
                } else {
                    BrowserComponentStatus::Unknown
                };
                self.snapshot_with(
                    BrowserComponentStatus::Ready,
                    timezone_status,
                    Some(environment.timezone),
                )
            }
            Err(WebDriverError::EnvironmentMismatch {
                field: "timezone",
                actual,
                ..
            }) => {
                tracing::warn!(
                    error_kind = "environment_mismatch",
                    "browser timezone mismatched"
                );
                self.snapshot_with(
                    BrowserComponentStatus::Ready,
                    BrowserComponentStatus::Mismatch,
                    Some(actual),
                )
            }
            Err(error) => {
                tracing::warn!(error_kind = error.kind(), "browser health probe failed");
                self.snapshot_with(
                    BrowserComponentStatus::Unavailable,
                    BrowserComponentStatus::Unknown,
                    None,
                )
            }
        }
    }

    /// Runs an immediate probe and then repeats it at the supplied interval.
    pub async fn run_until_shutdown(
        &self,
        mut shutdown: watch::Receiver<bool>,
        interval: Duration,
    ) {
        if interval.is_zero() {
            tracing::error!("browser health interval must be positive");
            return;
        }
        if *shutdown.borrow() {
            return;
        }

        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.refresh().await,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::debug!("browser health monitor stopped");
                        return;
                    }
                }
            }
        }
    }

    fn snapshot_with(
        &self,
        webdriver: BrowserComponentStatus,
        timezone: BrowserComponentStatus,
        observed_timezone: Option<String>,
    ) -> BrowserHealthSnapshot {
        let previous = self.health.snapshot();
        BrowserHealthSnapshot {
            webdriver,
            timezone,
            configured_timezone: previous.configured_timezone,
            observed_timezone,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono_tz::{Asia::Shanghai, UTC};

    use super::*;

    fn monitor(expected_timezone: Option<Tz>) -> BrowserHealthMonitor {
        let health = BrowserHealth::new(UTC);
        let factory = WebDriverFactory::new(
            "http://webdriver.test"
                .parse()
                .expect("test endpoint should parse"),
            crate::config::BrowserEngine::Firefox,
        )
        .with_profile(crate::acquisition::webdriver::BrowserProfile {
            expected_timezone,
            ..Default::default()
        });
        BrowserHealthMonitor::new(factory, health).expect("test monitor should be constructible")
    }

    fn environment(timezone: &str) -> crate::acquisition::webdriver::BrowserEnvironment {
        crate::acquisition::webdriver::BrowserEnvironment {
            user_agent: "test-agent".to_owned(),
            language: "zh-CN".to_owned(),
            languages: vec!["zh-CN".to_owned()],
            timezone: timezone.to_owned(),
            webdriver: Some(true),
            inner_width: 1_280,
            inner_height: 2_000,
        }
    }

    #[test]
    fn browser_jobs_require_both_webdriver_and_timezone_readiness() {
        let health = BrowserHealth::new(UTC);
        assert!(!health.browser_jobs_allowed());

        health.replace(BrowserHealthSnapshot {
            webdriver: BrowserComponentStatus::Ready,
            timezone: BrowserComponentStatus::Unknown,
            configured_timezone: "UTC".to_owned(),
            observed_timezone: None,
        });
        assert!(!health.browser_jobs_allowed());

        health.replace(BrowserHealthSnapshot {
            webdriver: BrowserComponentStatus::Ready,
            timezone: BrowserComponentStatus::Mismatch,
            configured_timezone: "UTC".to_owned(),
            observed_timezone: Some("Asia/Shanghai".to_owned()),
        });
        assert!(!health.browser_jobs_allowed());

        health.replace(BrowserHealthSnapshot {
            webdriver: BrowserComponentStatus::Ready,
            timezone: BrowserComponentStatus::Ready,
            configured_timezone: Shanghai.to_string(),
            observed_timezone: Some(Shanghai.to_string()),
        });
        assert!(health.browser_jobs_allowed());
    }

    #[test]
    fn initial_snapshot_keeps_the_configured_timezone_without_claiming_work() {
        let health = BrowserHealth::new(Shanghai);
        assert_eq!(
            health.snapshot(),
            BrowserHealthSnapshot {
                webdriver: BrowserComponentStatus::Unknown,
                timezone: BrowserComponentStatus::Unknown,
                configured_timezone: "Asia/Shanghai".to_owned(),
                observed_timezone: None,
            }
        );
        assert!(!health.browser_jobs_allowed());
    }

    #[test]
    fn not_ready_probe_blocks_browser_jobs_without_reporting_a_timezone() {
        let monitor = monitor(Some(UTC));
        let snapshot = monitor.snapshot_for_result(Ok(WebDriverHealthCheck::NotReady));

        assert_eq!(snapshot.webdriver, BrowserComponentStatus::NotReady);
        assert_eq!(snapshot.timezone, BrowserComponentStatus::Unknown);
        assert_eq!(snapshot.observed_timezone, None);
    }

    #[test]
    fn ready_probe_reports_the_observed_timezone() {
        let monitor = monitor(Some(UTC));
        let snapshot = monitor.snapshot_for_result(Ok(WebDriverHealthCheck::Ready {
            environment: environment("UTC"),
        }));

        assert_eq!(snapshot.webdriver, BrowserComponentStatus::Ready);
        assert_eq!(snapshot.timezone, BrowserComponentStatus::Ready);
        assert_eq!(snapshot.observed_timezone.as_deref(), Some("UTC"));
        assert!(snapshot.worker_ready());
    }

    #[test]
    fn timezone_mismatch_keeps_webdriver_ready_but_blocks_browser_jobs() {
        let monitor = monitor(Some(UTC));
        let snapshot = monitor.snapshot_for_result(Err(WebDriverError::EnvironmentMismatch {
            field: "timezone",
            expected: "UTC".to_owned(),
            actual: "Asia/Shanghai".to_owned(),
        }));

        assert_eq!(snapshot.webdriver, BrowserComponentStatus::Ready);
        assert_eq!(snapshot.timezone, BrowserComponentStatus::Mismatch);
        assert_eq!(snapshot.observed_timezone.as_deref(), Some("Asia/Shanghai"));
        assert!(!snapshot.worker_ready());
    }

    #[test]
    fn failed_probe_marks_webdriver_unavailable_and_clears_observation() {
        let monitor = monitor(Some(UTC));
        let snapshot = monitor
            .snapshot_for_result(Err(WebDriverError::Connect("transport failure".to_owned())));

        assert_eq!(snapshot.webdriver, BrowserComponentStatus::Unavailable);
        assert_eq!(snapshot.timezone, BrowserComponentStatus::Unknown);
        assert_eq!(snapshot.observed_timezone, None);
        assert!(!snapshot.worker_ready());
    }

    #[test]
    fn busy_probe_preserves_the_last_known_health_state() {
        let monitor = monitor(Some(UTC));
        let ready = BrowserHealthSnapshot {
            webdriver: BrowserComponentStatus::Ready,
            timezone: BrowserComponentStatus::Ready,
            configured_timezone: "UTC".to_owned(),
            observed_timezone: Some("UTC".to_owned()),
        };
        monitor.health.replace(ready.clone());

        assert_eq!(
            monitor.snapshot_for_result(Ok(WebDriverHealthCheck::Busy)),
            ready
        );
    }
}
