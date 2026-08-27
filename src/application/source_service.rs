//! Source use cases.
//!
//! Purpose: coordinate source creation, identity resolution, updates,
//! enable/disable operations, deletion, and feed-token management.
//!
//! A source URL is resolved through acquisition interfaces before persistence.
//! Source configuration changes must invalidate the associated feed cache and
//! may create a deduplicated `source_sync` or `feed_rebuild` job.
//!
//! Non-responsibilities: direct SQL, browser selectors, article fetching, or
//! rendering RSS bytes. PostgreSQL concurrency and duplicate-source handling
//! belong to repositories and transactions.
