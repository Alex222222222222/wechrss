//! Validated pacing and quiet-hours policies.
//!
//! This is the first implemented slice of the Rust service. It is deliberately
//! pure: it validates policy values, samples bounded delays, describes bounded
//! scroll work, and evaluates quiet hours. It does not sleep, access a browser,
//! read environment variables, claim jobs, or contact upstream services.
//!
//! Pacing reduces upstream request pressure and gives lazy page content time to
//! settle. It must not be represented as an anti-detection or control-bypass
//! feature. Quiet hours prevent new upstream work during a configured local-
//! time window while allowing RSS reads and cached responses to continue.

use std::time::Duration;

use chrono::{DateTime, NaiveTime, Utc};
use chrono_tz::Tz;
use rand::Rng;
use rand_distr::{Distribution, Normal};
use thiserror::Error;

/// The operation for which a delay is being sampled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayKind {
    /// Delay before an upstream protocol request.
    Request,
    /// Delay before navigating to an article page.
    PageNavigation,
    /// Delay between page actions.
    PageAction,
    /// Delay after scrolling so lazy content can settle.
    ScrollSettle,
}

/// Errors produced while validating pacing or quiet-hours configuration.
#[derive(Debug, Error, PartialEq)]
pub enum PacingError {
    /// A distribution contains a non-finite or negative value.
    #[error("{field} must be finite and non-negative")]
    InvalidNumber { field: &'static str },
    /// A distribution's lower bound is greater than its upper bound.
    #[error("minimum delay must not exceed maximum delay")]
    InvalidBounds,
    /// Quiet-hour start and end cannot be equal because that is ambiguous.
    #[error("quiet-hours start and end must differ")]
    EqualQuietHourEndpoints,
    /// A scroll policy would permit no useful operation.
    #[error("scroll limits must be greater than zero")]
    InvalidScrollLimits,
}

/// Parameters for a bounded normal distribution in milliseconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DelayDistribution {
    /// Center of the distribution in milliseconds.
    pub mean_ms: f64,
    /// Standard deviation in milliseconds.
    pub stddev_ms: f64,
    /// Inclusive lower bound in milliseconds.
    pub min_ms: f64,
    /// Inclusive upper bound in milliseconds.
    pub max_ms: f64,
}

impl DelayDistribution {
    /// Creates and validates a bounded normal-distribution configuration.
    pub fn new(
        mean_ms: f64,
        stddev_ms: f64,
        min_ms: f64,
        max_ms: f64,
    ) -> Result<Self, PacingError> {
        for (field, value) in [
            ("mean_ms", mean_ms),
            ("stddev_ms", stddev_ms),
            ("min_ms", min_ms),
            ("max_ms", max_ms),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(PacingError::InvalidNumber { field });
            }
        }
        if min_ms > max_ms {
            return Err(PacingError::InvalidBounds);
        }
        Ok(Self {
            mean_ms,
            stddev_ms,
            min_ms,
            max_ms,
        })
    }

    /// Samples a delay and clamps it to the configured inclusive bounds.
    ///
    /// A zero standard deviation is treated as a constant distribution. The
    /// random generator is supplied by the caller so production can use an
    /// entropy-backed generator while tests can use a seeded generator.
    pub fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Duration {
        let sampled_ms = if self.stddev_ms == 0.0 {
            self.mean_ms
        } else {
            // Construction cannot fail because validation rejects negative or
            // non-finite standard deviations.
            Normal::new(self.mean_ms, self.stddev_ms)
                .expect("validated normal distribution")
                .sample(rng)
        };
        let bounded_ms = sampled_ms.clamp(self.min_ms, self.max_ms);
        Duration::from_secs_f64(bounded_ms / 1_000.0)
    }
}

/// Limits and delay distributions shared by all acquisition adapters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PacingPolicy {
    /// Delay before WeRead or other upstream requests.
    pub request: DelayDistribution,
    /// Delay before article-page navigation.
    pub page_navigation: DelayDistribution,
    /// Delay between page actions.
    pub page_action: DelayDistribution,
    /// Delay after a scroll action.
    pub scroll_settle: DelayDistribution,
    /// Maximum number of scroll actions on one page.
    pub max_scroll_steps: u32,
    /// Maximum cumulative scroll distance in CSS pixels.
    pub max_scroll_pixels: u32,
    /// Maximum time allowed for page interaction and scrolling.
    pub max_page_operation: Duration,
}

impl PacingPolicy {
    /// Validates a complete pacing policy.
    pub fn new(
        request: DelayDistribution,
        page_navigation: DelayDistribution,
        page_action: DelayDistribution,
        scroll_settle: DelayDistribution,
        max_scroll_steps: u32,
        max_scroll_pixels: u32,
        max_page_operation: Duration,
    ) -> Result<Self, PacingError> {
        if max_scroll_steps == 0 || max_scroll_pixels == 0 || max_page_operation.is_zero() {
            return Err(PacingError::InvalidScrollLimits);
        }
        Ok(Self {
            request,
            page_navigation,
            page_action,
            scroll_settle,
            max_scroll_steps,
            max_scroll_pixels,
            max_page_operation,
        })
    }

    /// Returns the configured distribution for an operation kind.
    pub const fn distribution(&self, kind: DelayKind) -> DelayDistribution {
        match kind {
            DelayKind::Request => self.request,
            DelayKind::PageNavigation => self.page_navigation,
            DelayKind::PageAction => self.page_action,
            DelayKind::ScrollSettle => self.scroll_settle,
        }
    }
}

/// A local-time quiet-hours window in an IANA timezone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuietHours {
    /// Timezone used to interpret start and end.
    pub timezone: Tz,
    /// Inclusive local start time.
    pub start: NaiveTime,
    /// Exclusive local end time.
    pub end: NaiveTime,
}

impl QuietHours {
    /// Creates a quiet-hours window.
    ///
    /// If `start` is later than `end`, the window crosses midnight. The
    /// interval is start-inclusive and end-exclusive, making adjacent windows
    /// deterministic at their boundaries. Equal endpoints are rejected rather
    /// than interpreted as either zero or twenty-four hours.
    pub fn new(timezone: Tz, start: NaiveTime, end: NaiveTime) -> Result<Self, PacingError> {
        if start == end {
            return Err(PacingError::EqualQuietHourEndpoints);
        }
        Ok(Self {
            timezone,
            start,
            end,
        })
    }

    /// Returns whether an instant falls inside the local quiet-hours window.
    pub fn is_quiet_at(&self, instant: DateTime<Utc>) -> bool {
        let local_time = instant.with_timezone(&self.timezone).time();
        if self.start < self.end {
            local_time >= self.start && local_time < self.end
        } else {
            local_time >= self.start || local_time < self.end
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveTime;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    fn distribution() -> DelayDistribution {
        DelayDistribution::new(2_000.0, 250.0, 1_000.0, 4_000.0).unwrap()
    }

    #[test]
    fn samples_are_deterministic_with_the_same_seed() {
        let policy = distribution();
        let mut first = StdRng::seed_from_u64(7);
        let mut second = StdRng::seed_from_u64(7);

        let first_samples: Vec<_> = (0..10).map(|_| policy.sample(&mut first)).collect();
        let second_samples: Vec<_> = (0..10).map(|_| policy.sample(&mut second)).collect();

        assert_eq!(first_samples, second_samples);
    }

    #[test]
    fn samples_never_escape_configured_bounds() {
        let policy = distribution();
        let mut rng = StdRng::seed_from_u64(99);

        for _ in 0..10_000 {
            let milliseconds = policy.sample(&mut rng).as_secs_f64() * 1_000.0;
            assert!((1_000.0..=4_000.0).contains(&milliseconds));
        }
    }

    #[test]
    fn zero_standard_deviation_is_a_clamped_constant() {
        let policy = DelayDistribution::new(5_000.0, 0.0, 1_000.0, 4_000.0).unwrap();
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(policy.sample(&mut rng), Duration::from_millis(4_000));
    }

    #[test]
    fn invalid_distribution_values_are_rejected() {
        assert_eq!(
            DelayDistribution::new(-1.0, 1.0, 0.0, 2.0),
            Err(PacingError::InvalidNumber { field: "mean_ms" })
        );
        assert_eq!(
            DelayDistribution::new(1.0, f64::NAN, 0.0, 2.0),
            Err(PacingError::InvalidNumber { field: "stddev_ms" })
        );
        assert_eq!(
            DelayDistribution::new(1.0, 1.0, 3.0, 2.0),
            Err(PacingError::InvalidBounds)
        );
    }

    #[test]
    fn policy_rejects_unusable_scroll_limits() {
        let d = distribution();
        assert_eq!(
            PacingPolicy::new(d, d, d, d, 0, 100, Duration::from_secs(1)),
            Err(PacingError::InvalidScrollLimits)
        );
        assert_eq!(
            PacingPolicy::new(d, d, d, d, 1, 0, Duration::from_secs(1)),
            Err(PacingError::InvalidScrollLimits)
        );
        assert_eq!(
            PacingPolicy::new(d, d, d, d, 1, 100, Duration::ZERO),
            Err(PacingError::InvalidScrollLimits)
        );
    }

    #[test]
    fn policy_returns_the_distribution_for_each_operation() {
        let request = distribution();
        let page_navigation = DelayDistribution::new(3.0, 0.0, 3.0, 3.0).unwrap();
        let page_action = DelayDistribution::new(4.0, 0.0, 4.0, 4.0).unwrap();
        let scroll_settle = DelayDistribution::new(5.0, 0.0, 5.0, 5.0).unwrap();
        let policy = PacingPolicy::new(
            request,
            page_navigation,
            page_action,
            scroll_settle,
            4,
            2_000,
            Duration::from_secs(30),
        )
        .unwrap();

        assert_eq!(policy.distribution(DelayKind::Request), request);
        assert_eq!(
            policy.distribution(DelayKind::PageNavigation),
            page_navigation
        );
        assert_eq!(policy.distribution(DelayKind::PageAction), page_action);
        assert_eq!(policy.distribution(DelayKind::ScrollSettle), scroll_settle);
    }

    #[test]
    fn same_day_quiet_window_has_expected_boundaries() {
        let quiet = QuietHours::new(
            chrono_tz::UTC,
            NaiveTime::from_hms_opt(9, 00, 00).unwrap(),
            NaiveTime::from_hms_opt(17, 00, 00).unwrap(),
        )
        .unwrap();

        assert!(quiet.is_quiet_at(utc("2026-08-27T09:00:00Z")));
        assert!(quiet.is_quiet_at(utc("2026-08-27T16:59:59Z")));
        assert!(!quiet.is_quiet_at(utc("2026-08-27T17:00:00Z")));
        assert!(!quiet.is_quiet_at(utc("2026-08-27T08:59:59Z")));
    }

    #[test]
    fn overnight_quiet_window_crosses_midnight() {
        let quiet = QuietHours::new(
            chrono_tz::UTC,
            NaiveTime::from_hms_opt(23, 00, 00).unwrap(),
            NaiveTime::from_hms_opt(7, 00, 00).unwrap(),
        )
        .unwrap();

        assert!(quiet.is_quiet_at(utc("2026-08-27T23:00:00Z")));
        assert!(quiet.is_quiet_at(utc("2026-08-28T06:59:59Z")));
        assert!(!quiet.is_quiet_at(utc("2026-08-28T07:00:00Z")));
        assert!(!quiet.is_quiet_at(utc("2026-08-27T12:00:00Z")));
    }

    #[test]
    fn quiet_window_uses_configured_timezone() {
        let quiet = QuietHours::new(
            chrono_tz::Asia::Shanghai,
            NaiveTime::from_hms_opt(23, 00, 00).unwrap(),
            NaiveTime::from_hms_opt(7, 00, 00).unwrap(),
        )
        .unwrap();

        // 15:00 UTC is 23:00 in Shanghai.
        assert!(quiet.is_quiet_at(utc("2026-08-27T15:00:00Z")));
        // 22:59 Shanghai is 14:59 UTC and remains active.
        assert!(!quiet.is_quiet_at(utc("2026-08-27T14:59:59Z")));
    }

    #[test]
    fn equal_quiet_hour_endpoints_are_rejected() {
        let time = NaiveTime::from_hms_opt(12, 00, 00).unwrap();
        assert_eq!(
            QuietHours::new(chrono_tz::UTC, time, time),
            Err(PacingError::EqualQuietHourEndpoints)
        );
    }

    fn utc(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }
}
