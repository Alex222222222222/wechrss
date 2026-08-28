//! Shared PostgreSQL unit-of-work boundary.
//!
//! Purpose: make article/archive mutations, source revision and schedule
//! changes, sync-run persistence, feed-cache replacement, and fenced job
//! completion atomic without exposing SQLx transactions to application code.
//!
//! The future `UnitOfWorkFactory::begin` creates one short-lived SQLx
//! transaction. Its returned handle exposes transaction-scoped source, article,
//! sync-run, feed-cache, and job repository views. Only the unit of work can
//! commit; dropping it or returning an error rolls all component writes back.
//! Repository views borrow the unit of work and therefore cannot outlive or
//! independently commit their transaction.
//!
//! Minimum executable contract:
//!
//! - `UnitOfWorkFactory::begin()` creates the transaction;
//! - `jobs().verify_fence(job_id, owner, token)` runs before business writes;
//! - transaction-scoped article/source/sync/cache methods apply an idempotent
//!   command and expected feed revision;
//! - `jobs().succeed(...)` is available only through this unit of work, not the
//!   general queue repository; and
//! - `commit(self)` is the only successful exit. Every repository view has no
//!   commit method and borrows the same transaction.
//!
//! Retry, deferral, cancellation, and failure outcomes use the same boundary
//! because they record sync results or alter source scheduling gates/cooldowns.
//! Queue-only enqueue, claim, heartbeat, and read operations remain short
//! independent transactions. Expired-lease recovery is a dedicated atomic
//! persistence operation because exhausting a failure budget may also update the
//! source cooldown.
//!
//! Synchronization data flow:
//!
//! 1. perform browser/network acquisition and normalization without a database
//!    transaction;
//! 2. keep the job lease alive through a separate pool connection;
//! 3. begin a unit of work and verify the job owner, fencing token, and live
//!    lease;
//! 4. verify the expected base feed revision, persist idempotent article/archive
//!    changes, and advance to the candidate revision when feed-visible data
//!    changed;
//! 5. persist an already-rendered cache payload only for that exact revision,
//!    verify/release any feed-build lease, update the sync run and source
//!    schedule/gate, and mark the job successful;
//! 6. commit once.
//!
//! Non-responsibilities: browser calls, sleeping, rendering large documents
//! while locks are held, heartbeat task ownership, retry policy, or HTTP error
//! mapping. A unit of work must use bounded lock/statement timeouts and should
//! be short enough that a worker can safely heartbeat independently.
//!
//! High availability: the final job mutation remains fenced inside the same
//! transaction as business writes. If ownership was lost, the unit of work
//! rolls back instead of publishing writes from a stale worker. Cache
//! compare-and-swap and feed-build fencing checks also happen inside this
//! transaction.

// TODO(design): implement UnitOfWorkFactory and transaction-scoped repository
// views, move success and business-coupled failure completion behind this
// boundary, and prevent SyncService from receiving a job-only commit API.
