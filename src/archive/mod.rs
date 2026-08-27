//! Full article archival subsystem.
//!
//! Converts extracted browser content into safe, durable RSS-ready content.
//! The subsystem owns sanitization, content hashing, binary asset storage, and
//! URL rewriting while remaining independent of HTTP routes and scheduling.
//!
//! PostgreSQL stores archive metadata and content references. The `AssetStore`
//! abstraction supports a local persistent volume initially and an
//! S3-compatible backend for Kubernetes later.

pub mod asset_store;
pub mod sanitizer;
pub mod url_rewriter;
