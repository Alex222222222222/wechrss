//! Article, content, and asset-metadata repository.
//!
//! Provides idempotent upserts keyed by `review_id`, content-hash checks,
//! article listing, missing-URL backfill queries, and source-scoped RSS input.
//!
//! Binary asset bytes belong to the optional `AssetStore` path; when asset
//! archiving is enabled, this repository stores asset keys, checksums, MIME
//! types, sizes, and article relationships. Article mutations must make the
//! owning feed cache stale or rebuild it in the same workflow by incrementing
//! the source's feed revision in the shared `UnitOfWork`.

//! Expected transaction-scoped operations return whether feed-visible content
//! actually changed, allowing the caller to increment the revision once per
//! committed batch instead of on an idempotent no-op upsert.

// TODO(design): define transaction-scoped upsert results and feed-visible
// change detection for UnitOfWork integration.
