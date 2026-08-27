//! RSS publication subsystem.
//!
//! Provides deterministic rendering from normalized source, article, content,
//! and asset-reference values. It has no browser, upstream HTTP, scheduler, or
//! PostgreSQL query responsibilities.

pub mod renderer;
