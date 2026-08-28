//! Due-work enqueue loop.
//!
//! The scheduler periodically asks the scheduler repository to atomically reserve
//! and enqueue enabled, `ready`, due sources. The repository operation locks a
//! bounded batch with `FOR UPDATE SKIP LOCKED`, excludes active source-sync
//! jobs, inserts canonical deduplication keys, and records reservations in one
//! transaction. The scheduler must not implement this as a read-list followed
//! by unrelated job inserts.
//!
//! Repeated enqueue passes are safe because active `dedupe_key` uniqueness is
//! enforced by PostgreSQL. Clock skew and process restarts are handled by
//! persisted timestamps and PostgreSQL-authoritative eligibility time, not
//! application wall clocks or in-memory timers.
//!
//! Before enqueueing, the scheduler evaluates the configured quiet-hours
//! policy using the configured IANA timezone. It should not enqueue new
//! upstream fetch jobs during quiet hours. Feed-cache maintenance that does not
//! contact upstream services may remain eligible.
//!
//! Disabled sources and sources gated as `authentication_required` or
//! `risk_controlled` are ineligible even when their timestamp is due. Ordinary
//! exhausted failures receive a durable cooldown so release of an active job
//! deduplication key cannot create an immediate infinite retry loop.

// TODO(design): define the application scheduler loop over the implemented
// repository operation, including quiet-hour policy, bounded polling, and
// metrics. The loop must not reimplement source selection or enqueue work
// through separate repository calls.
