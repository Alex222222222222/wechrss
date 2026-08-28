//! RSS publication subsystem.
//!
//! Provides deterministic rendering from normalized source, article, content,
//! and optional asset-reference values. External asset URLs are valid input for
//! the version-one renderer; locally rewritten assets are an optional later
//! mode. It has no browser, upstream HTTP, scheduler, or PostgreSQL query
//! responsibilities.

pub mod renderer;
