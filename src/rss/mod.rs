//! RSS publication subsystem.
//!
//! Provides deterministic rendering from normalized source, article, content,
//! and optional asset-reference values. The pure renderer is executable now:
//! external asset URLs are valid input for the version-one feed, while locally
//! rewritten assets remain an optional later mode. It has no browser, upstream
//! HTTP, scheduler, or PostgreSQL query responsibilities.

pub mod renderer;
