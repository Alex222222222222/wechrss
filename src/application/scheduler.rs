//! Due-work enqueue loop.
//!
//! The scheduler periodically finds enabled sources whose next execution is
//! due and inserts deduplicated jobs. It must never perform browser or article
//! work directly and must not assume that only one application instance is
//! running.
//!
//! Repeated enqueue passes are safe because active `dedupe_key` uniqueness is
//! enforced by PostgreSQL. Clock skew and process restarts are handled by
//! persisted timestamps and job leases, not in-memory timers.
//!
//! Before enqueueing, the scheduler evaluates the configured quiet-hours
//! policy using the configured IANA timezone. It should not enqueue new
//! upstream fetch jobs during quiet hours. Feed-cache maintenance that does not
//! contact upstream services may remain eligible.
