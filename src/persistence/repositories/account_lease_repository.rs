//! Distributed WeRead account-lease repository.
//!
//! Purpose: serialize authenticated use of one WeRead account across all
//! application replicas. Source job leases prevent duplicate source work but do
//! not prevent two different sources from using the same account concurrently.
//!
//! Expected operations are `acquire(account_id, owner, lease_for)`,
//! `heartbeat(account_id, owner, token, lease_for)`, and
//! `release(account_id, owner, token)`. Acquisition returns a fresh fencing
//! token and succeeds only when no lease exists or the prior lease has expired.
//! Every mutation compares account ID, owner, token, and lease expiry.
//! The concrete production interface omits caller-provided `now`: SQL derives
//! one statement-local PostgreSQL timestamp for acquisition, heartbeat,
//! release, expiry, and takeover. An injectable clock belongs only to the memory
//! test implementation.
//!
//! Authenticated article-list, detail-URL recovery, login exchange, and
//! credential refresh operations hold this lease and heartbeat it through a
//! separate pool connection. Lease loss cancels the account operation before
//! another upstream request. Public WeChat article extraction neither acquires
//! this lease nor receives credentials.
//!
//! PostgreSQL owns cross-replica exclusion. Local `BrowserPool` capacity remains
//! a separate process-level resource limit and must not be treated as this
//! distributed lock. The first version may have a single account, but that
//! account still has a stable durable identifier rather than an implicit global
//! mutex.
//!
//! Failure behavior mirrors job fencing: stale release or heartbeat requests
//! return a typed ownership error, and expired leases are recoverable. Secret
//! credential values are never stored in the lease row or error text.

// TODO(design): add the account_leases migration, domain lease token/value
// types, PostgreSQL-clocked SQLx repository, and contention/recovery/clock-skew
// tests.
