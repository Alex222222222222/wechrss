//! Minimal administrative UI integration for the first usable version.
//!
//! The release target is a small set of pages or templates for source
//! management, sync history, archived article inspection, feed-link copying,
//! and operator error states. QR login is explicitly deferred after the first
//! release because it requires user interaction. An article-missed queue and
//! handler is also deferred as a later repair/backfill improvement.
//!
//! The UI remains incremental work in the current tree and must call the same
//! application services/API contracts as other clients.
//!
//! The UI should call the same application services/API contracts as other
//! clients and must not embed database or browser logic. It must avoid
//! rendering secrets and should display risk-control states as actions for the
//! operator rather than silently retrying them.
