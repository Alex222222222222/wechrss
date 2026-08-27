//! Shared upstream pacing and controlled-scroll adapter policy.
//!
//! Owns the future implementation of bounded waits before requests, page
//! navigations, extraction actions, and scroll settling. A truncated normal
//! distribution may be used with configured mean, standard deviation, minimum,
//! and maximum; a seeded RNG must be injectable for deterministic tests.
//!
//! Scroll behavior is deliberately bounded: a small number of viewport
//! increments, a maximum total distance, and a maximum page-operation duration.
//! Its purpose is to trigger lazy-loaded content, not to imitate arbitrary
//! human behavior or bypass platform controls.
//!
//! This module must also consult the quiet-hours gate before an upstream
//! operation. It does not claim jobs, persist policy, parse article HTML, or
//! decide whether an upstream error is retryable.
