//! Shared upstream pacing and controlled-scroll execution policy.
//!
//! [`PacingController`] is the runtime consumer of the pure domain pacing
//! policy. It samples bounded delays from one shared RNG, sleeps without
//! blocking an executor thread, and creates a bounded scroll plan for a public
//! article page. An entropy-seeded controller is used by production; a seeded
//! controller is available for deterministic tests and reproducible
//! diagnostics.
//!
//! Scroll behavior is deliberately bounded: a small number of meaningful
//! viewport increments, a maximum total distance, and a maximum
//! page-operation duration. Its purpose is to trigger lazy-loaded content, not
//! to imitate arbitrary human behavior or bypass platform controls.
//!
//! Quiet hours are an application scheduler/worker concern. This module does
//! not decide whether a job may start, claim jobs, persist policy, parse
//! article HTML, or classify upstream errors. It only executes delays and
//! describes bounded page actions after the caller has passed the quiet-hours
//! gate.

use std::{sync::Arc, time::Duration};

use rand::{rngs::StdRng, Rng, SeedableRng};
use tokio::sync::Mutex;

use crate::domain::pacing::{DelayKind, PacingPolicy};

/// One bounded downward scroll and the delay used to let lazy content settle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrollStep {
    /// Number of CSS pixels to scroll down.
    pub distance: u32,
    /// Delay after the scroll before the next page operation.
    pub settle: Duration,
}

/// Runtime pacing controller shared by acquisition adapters.
///
/// The mutex protects only RNG state while a sample or plan is generated. It
/// is never held while an async sleep is running, so one slow page cannot block
/// delay generation for unrelated pages.
#[derive(Clone)]
pub struct PacingController {
    policy: PacingPolicy,
    rng: Arc<Mutex<StdRng>>,
}

impl std::fmt::Debug for PacingController {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PacingController")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl PacingController {
    /// Creates a production controller seeded from the operating system.
    pub fn from_entropy(policy: PacingPolicy) -> Self {
        Self {
            policy,
            rng: Arc::new(Mutex::new(StdRng::from_os_rng())),
        }
    }

    /// Creates a deterministic controller for tests and diagnostics.
    pub fn from_seed(policy: PacingPolicy, seed: u64) -> Self {
        Self {
            policy,
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
        }
    }

    /// Returns the immutable policy used by this controller.
    pub const fn policy(&self) -> PacingPolicy {
        self.policy
    }

    /// Returns the maximum time allowed for one page operation.
    pub const fn max_page_operation(&self) -> Duration {
        self.policy.max_page_operation()
    }

    /// Samples one delay without sleeping.
    pub async fn sample_delay(&self, kind: DelayKind) -> Duration {
        let mut rng = self.rng.lock().await;
        let delay = self.policy.distribution(kind).sample(&mut *rng);
        tracing::trace!(delay_kind = ?kind, delay_ms = delay.as_millis(), "sampled upstream pacing delay");
        delay
    }

    /// Samples and asynchronously waits for one operation delay.
    pub async fn wait(&self, kind: DelayKind) {
        let delay = self.sample_delay(kind).await;
        if !delay.is_zero() {
            tracing::trace!(delay_kind = ?kind, delay_ms = delay.as_millis(), "waiting before upstream operation");
            tokio::time::sleep(delay).await;
        }
    }

    /// Creates a deterministic-size-bounded plan of downward scroll actions.
    ///
    /// Each action is at least half a viewport when the configured pixel limit
    /// permits it, so a plan does not spend its finite action budget on
    /// one-pixel no-ops. If the total pixel limit is smaller than that minimum,
    /// one smaller action is generated. The plan always contains at least one
    /// step because [`PacingPolicy`] rejects zero scroll limits.
    pub async fn scroll_plan(&self, viewport_height: u32) -> Vec<ScrollStep> {
        let mut rng = self.rng.lock().await;
        let max_pixels = self.policy.max_scroll_pixels();
        let viewport_height = viewport_height.max(1);
        let minimum_distance = (viewport_height / 2).max(1).min(max_pixels);
        let maximum_steps = self
            .policy
            .max_scroll_steps()
            .min(max_pixels / minimum_distance)
            .max(1);
        let step_count = rng.random_range(1..=maximum_steps);
        let mut remaining_pixels = max_pixels;
        let mut steps = Vec::with_capacity(step_count as usize);

        for index in 0..step_count {
            let remaining_steps = step_count - index - 1;
            let pixels_reserved_for_remaining = remaining_steps * minimum_distance;
            let maximum_distance = remaining_pixels
                .saturating_sub(pixels_reserved_for_remaining)
                .min(viewport_height)
                .max(minimum_distance);
            let distance = rng.random_range(minimum_distance..=maximum_distance);
            remaining_pixels -= distance;
            let settle = self
                .policy
                .distribution(DelayKind::ScrollSettle)
                .sample(&mut *rng);
            steps.push(ScrollStep { distance, settle });
        }
        tracing::debug!(
            viewport_height,
            steps = steps.len(),
            total_pixels = steps.iter().map(|step| step.distance).sum::<u32>(),
            "created bounded article scroll plan"
        );
        steps
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::pacing::{DelayDistribution, PacingPolicy};

    fn policy(scroll_steps: u32, scroll_pixels: u32, settle: DelayDistribution) -> PacingPolicy {
        let zero = DelayDistribution::new(0.0, 0.0, 0.0, 0.0).unwrap();
        PacingPolicy::new(
            zero,
            zero,
            zero,
            settle,
            scroll_steps,
            scroll_pixels,
            Duration::from_secs(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn seeded_controllers_generate_the_same_delay_and_scroll_plan() {
        let settle = DelayDistribution::new(20.0, 0.0, 20.0, 20.0).unwrap();
        let first = PacingController::from_seed(policy(4, 4_000, settle), 7);
        let second = PacingController::from_seed(policy(4, 4_000, settle), 7);

        assert_eq!(
            first.sample_delay(DelayKind::PageAction).await,
            Duration::ZERO
        );
        assert_eq!(
            first.scroll_plan(1_000).await,
            second.scroll_plan(1_000).await
        );
    }

    #[tokio::test]
    async fn scroll_plan_respects_steps_pixels_and_meaningful_distance() {
        let controller = PacingController::from_seed(
            policy(
                4,
                4_000,
                DelayDistribution::new(20.0, 0.0, 20.0, 20.0).unwrap(),
            ),
            99,
        );

        let steps = controller.scroll_plan(1_000).await;
        assert!(!steps.is_empty());
        assert!(steps.len() <= 4);
        assert!(steps.iter().all(|step| step.distance >= 500));
        assert!(steps.iter().map(|step| step.distance).sum::<u32>() <= 4_000);
        assert!(steps
            .iter()
            .all(|step| step.settle == Duration::from_millis(20)));
    }

    #[tokio::test]
    async fn tiny_pixel_budget_still_produces_one_bounded_scroll() {
        let controller = PacingController::from_seed(
            policy(4, 3, DelayDistribution::new(0.0, 0.0, 0.0, 0.0).unwrap()),
            1,
        );

        let steps = controller.scroll_plan(1_000).await;
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].distance, 3);
    }

    #[tokio::test]
    async fn zero_viewport_height_is_clamped_without_breaking_scroll_bounds() {
        let controller = PacingController::from_seed(
            policy(8, 10, DelayDistribution::new(0.0, 0.0, 0.0, 0.0).unwrap()),
            11,
        );

        let steps = controller.scroll_plan(0).await;
        assert!(!steps.is_empty());
        assert!(steps.len() <= 8);
        assert!(steps.iter().all(|step| step.distance > 0));
        assert!(steps.iter().map(|step| step.distance).sum::<u32>() <= 10);
    }

    #[tokio::test]
    async fn wait_with_zero_delay_returns_without_blocking() {
        let controller = PacingController::from_seed(
            policy(1, 1, {
                DelayDistribution::new(0.0, 0.0, 0.0, 0.0).unwrap()
            }),
            1,
        );

        tokio::time::timeout(
            Duration::from_millis(10),
            controller.wait(DelayKind::PageNavigation),
        )
        .await
        .expect("zero-delay wait should complete");
    }
}
