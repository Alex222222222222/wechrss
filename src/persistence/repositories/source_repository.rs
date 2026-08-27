//! Source persistence repository.
//!
//! Stores source identity, display configuration, enabled state, scheduling
//! timestamps, status, and opaque feed-token metadata. It will enforce unique
//! `book_id` values and provide due-source queries for the enqueue loop.
//!
//! Source changes must participate in cache invalidation and job deduplication
//! transactions. This repository does not resolve article URLs or execute
//! synchronization.
