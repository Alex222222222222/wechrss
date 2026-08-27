//! Browser-session capacity and ownership policy.
//!
//! The pool will limit concurrent sessions, initially to one active session per
//! account or application instance. It provides acquisition services with a
//! session lease and guarantees cleanup when a job finishes or fails.
//! Sessions receive the shared pacing policy and timezone configuration; the
//! pool must not let individual callers bypass those limits.
//!
//! High availability: each application replica has local browser capacity;
//! PostgreSQL job leases prevent duplicate work across replicas. The pool must
//! not be treated as a distributed lock.
//!
//! Non-responsibilities: WebDriver commands, source scheduling, credentials
//! persistence, or RSS caching.
