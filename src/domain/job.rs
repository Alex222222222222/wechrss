//! Durable job domain model.
//!
//! Jobs represent individual scheduled executions such as source sync,
//! article backfill, asset download, feed rebuild, or credential refresh.
//!
//! Responsibilities: define statuses, retryability, lease ownership,
//! deduplication keys, attempt limits, and valid transitions:
//!
//! ```text
//! queued -> running -> succeeded
//! queued -> running -> retry_wait -> running
//! queued/running -> failed
//! ```
//!
//! Non-responsibilities: PostgreSQL row locking, timers, browser work, or
//! deciding the HTTP representation of a job.
//!
//! High availability: a lease must identify an owning instance and expire so
//! another instance can recover abandoned work. Job handlers must be
//! idempotent because completion can race with process failure.
