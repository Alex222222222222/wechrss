//! Browser-session capacity and ownership policy.
//!
//! The pool will limit concurrent sessions and expose two non-interchangeable
//! session capabilities: authenticated account sessions and clean ephemeral
//! public sessions. It provides acquisition services with a local session lease
//! and guarantees cleanup when a job finishes or fails. Public sessions start
//! with a fresh profile and cannot import account cookies or credentials. Both
//! capabilities are non-cloneable. An authenticated session can be created only
//! with a live `AccountLeaseGuard`; the guard propagates cancellation when its
//! fencing token is lost.
//! Sessions receive the shared pacing policy and timezone configuration; the
//! pool must not let individual callers bypass those limits.
//!
//! High availability: each application replica has local browser capacity;
//! PostgreSQL job leases prevent duplicate jobs, and a separate PostgreSQL
//! account lease serializes authenticated use of one account across different
//! source jobs. The pool must not be treated as either distributed lock.
//!
//! Non-responsibilities: WebDriver commands, source scheduling, credentials
//! persistence, or RSS caching.

// TODO(design): define distinct non-cloneable AuthenticatedBrowserSession and
// PublicBrowserSession capabilities plus AccountLeaseGuard; do not expose one
// general session type that permits accidental credential reuse.
