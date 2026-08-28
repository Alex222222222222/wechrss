//! Atomic source-scheduling repository.
//!
//! Purpose: provide the one cross-table operation needed by every scheduler
//! replica without exposing a race-prone due-source list to application code.
//! `enqueue_due_sources(limit)` opens a short transaction, derives `db_now` from
//! PostgreSQL, locks a bounded
//! batch of enabled, `ready`, due sources with `FOR UPDATE SKIP LOCKED`, excludes
//! sources with an active source-sync job, inserts canonical
//! `source_sync:{source_id}` jobs, and records their scheduling reservations.
//!
//! PostgreSQL partial uniqueness remains the final deduplication defense, but it
//! is not the primary loop-control mechanism. Disabled sources and sources
//! blocked for authentication or risk control are never selected. Retry-wait
//! and deferred jobs remain active and therefore exclude another source-sync
//! insertion.
//!
//! Failure behavior: the source reservation and job insertion commit together
//! or not at all. Duplicate conflicts caused by concurrent manual sync requests
//! are normal idempotent outcomes. This repository never calculates quiet
//! hours, runs browser work, or decides a source's terminal scheduling gate.

// TODO(design): implement the atomic PostgreSQL-clocked statement/transaction
// and concurrency/clock-skew tests with multiple repository instances.
