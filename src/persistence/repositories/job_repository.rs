//! Durable job queue repository.
//!
//! Owns enqueue deduplication, PostgreSQL row-lock claiming with `SKIP LOCKED`,
//! lease assignment, heartbeat extension, completion, retry scheduling,
//! cancellation, and expired-lease recovery.
//!
//! The repository must make active `dedupe_key` uniqueness explicit and return
//! only jobs successfully claimed by the current instance. It must not execute
//! job payloads or know browser behavior.
