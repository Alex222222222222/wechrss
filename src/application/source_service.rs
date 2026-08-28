//! Source use cases.
//!
//! Purpose: coordinate source creation, identity resolution, updates,
//! enable/disable operations, deletion, and feed-token management.
//!
//! A source URL is resolved through acquisition interfaces before persistence.
//! Source configuration changes must invalidate the associated feed cache and
//! may create a deduplicated `source_sync` or `feed_rebuild` job. Every source
//! references the stable WeRead account used for authenticated list operations
//! and owns a monotonic `feed_revision` plus an explicit scheduling gate.
//!
//! Feed-visible changes increment the revision and invalidate/rebuild the cache
//! in a shared persistence `UnitOfWork`. Operator actions explicitly clear
//! `authentication_required` or `risk_controlled`; merely reaching another due
//! timestamp must not clear those states.
//!
//! Non-responsibilities: direct SQL, browser selectors, article fetching, or
//! rendering RSS bytes. PostgreSQL concurrency and duplicate-source handling
//! belong to repositories and transactions. The source repository now provides
//! the transaction-scoped primitive; this service still must compose it with
//! identity, cache, and initial-job policies.

// TODO(design): define the source-service ports that compose identity
// resolution, source persistence, feed-cache invalidation, and initial job
// enqueueing. Source identity/create/read and transaction-scoped scheduling
// mutations are now available from the domain and persistence layers.
