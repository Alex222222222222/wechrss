//! Article, content, and asset-metadata repository.
//!
//! Provides idempotent upserts keyed by `review_id`, content-hash checks,
//! article listing, missing-URL backfill queries, and source-scoped RSS input.
//!
//! Binary asset bytes belong to the optional `AssetStore` path; when asset
//! archiving is enabled, this repository stores asset keys, checksums, MIME
//! types, sizes, and article relationships. Article mutations must make the
//! owning feed cache stale or rebuild it in the same workflow.
