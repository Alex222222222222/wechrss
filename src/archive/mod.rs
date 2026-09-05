//! Full article archival subsystem.
//!
//! Converts extracted browser content into safe, durable RSS-ready content.
//! The subsystem always owns sanitization and content hashing. Database-backed
//! binary asset storage and URL rewriting are optional in version one, and
//! remain independent of HTTP routes and scheduling.
//!
//! PostgreSQL stores archive metadata and content references. When optional
//! asset archiving is enabled, the first implementation uses the PostgreSQL
//! database backend. Local-directory and S3-compatible backends are future
//! implementations behind the same storage abstraction. The default path keeps
//! approved external asset URLs in sanitized HTML.

pub mod asset_store;
pub mod sanitizer;
pub mod url_rewriter;
