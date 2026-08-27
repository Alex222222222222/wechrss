//! Durable job lifecycle orchestration.
//!
//! Owns enqueue requests, active-job deduplication, claim/lease coordination,
//! heartbeat calls, retry backoff, cancellation, completion, and expired-lease
//! recovery through repository interfaces.
//!
//! Multiple instances may run these operations concurrently. PostgreSQL row
//! locks with `SKIP LOCKED` select independent jobs, while lease expiry
//! recovers work from crashed instances.
//!
//! Non-responsibilities: executing source synchronization or deciding browser
//! selectors. Job handlers report typed outcomes back to this service.
