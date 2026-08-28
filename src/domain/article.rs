//! Article and archived-content domain model.
//!
//! An article is identified by the upstream `review_id`, not by its URL. URLs
//! may be absent during listing, later recovered, or changed by the source.
//! The model covers title, author, summary, cover, publish time, original URL,
//! source relationship, content hash, and archived HTML.
//!
//! Responsibilities: describe stable identity, idempotent upsert expectations,
//! content revisions, and optional asset references. Asset references may remain
//! external in version one; binary asset caching is not required for an article
//! to be archived or rendered.
//!
//! Non-responsibilities: HTML sanitization, asset downloads, SQL statements,
//! RSS XML rendering, and browser extraction.
//!
//! RSS interactions: the renderer consumes normalized article/content values;
//! a feed-visible article mutation increments the owning source's monotonic feed
//! revision and causes that exact revision to be rebuilt. Idempotent no-op
//! upserts do not increment the revision.

// TODO(design): define normalized article/archive input and feed-visible change
// result types before implementing transaction-scoped persistence.
