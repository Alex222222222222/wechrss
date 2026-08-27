//! Durable job queue repository.
//!
//! Owns enqueue deduplication, PostgreSQL row-lock claiming with `SKIP LOCKED`,
//! lease assignment with per-claim fencing tokens, heartbeat extension,
//! completion, retry scheduling, cancellation, and expired-lease recovery.
//!
//! The repository must make active `dedupe_key` uniqueness explicit and return
//! only jobs successfully claimed by the current instance. It must not execute
//! job payloads or know browser behavior. Every heartbeat and terminal update
//! must match both `lease_owner` and `lease_token` to fence stale workers.
