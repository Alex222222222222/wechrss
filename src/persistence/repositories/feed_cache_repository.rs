//! Persisted RSS feed-cache repository.
//!
//! Stores one rendered XML document per source, its ETag/content hash,
//! generated time, expiry time, and monotonic source feed revision. It provides
//! fast reads for RSS clients and atomic cache replacement after a successful
//! fetch.
//!
//! Freshness defaults to 30 minutes. Stale rows remain serveable for
//! stale-while-revalidate behavior; cache misses may be populated by a feed
//! rebuild use case. Production freshness and remaining-TTL calculations use
//! PostgreSQL server time. This repository never contacts WeChat or the browser.
//!
//! Replacement uses compare-and-swap semantics: write only when the source's
//! current revision equals the rendered revision and the existing cache is not
//! newer. A cache is stale when either its TTL expired or its revision differs
//! from the source revision. Cache-miss population additionally uses a fenced
//! `feed_build_leases` row keyed by source ID so concurrent RSS requests do not
//! all render the same missing feed. Acquisition/takeover uses PostgreSQL server
//! time in a short committed transaction; it is not a connection-scoped
//! advisory lock. Rendering happens after that transaction releases its
//! connection. Final replacement verifies source revision, build owner/token,
//! and the live build lease, then releases the lease in the same `UnitOfWork`.
//! Expected build operations are `acquire_build(source_id, owner, lease_for)`,
//! `heartbeat_build(source_id, owner, token, lease_for)`, and a transaction-
//! scoped fenced release used by final replacement. None accepts a caller wall
//! clock in the PostgreSQL implementation.

// TODO(design): define revision-aware read/CAS replacement plus fenced
// acquire/heartbeat/release build operations, then test expiry takeover,
// stale-owner rejection, revision conflicts, and concurrent true cache misses.
