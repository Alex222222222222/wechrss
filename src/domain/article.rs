//! Article and archived-content domain model.
//!
//! An article is identified by the upstream `review_id`, not by its URL. URLs
//! may be absent during listing, later recovered, or changed by the source.
//! The model covers title, author, summary, cover, publish time, original URL,
//! source relationship, content hash, and archived HTML.
//!
//! Responsibilities: describe stable identity, idempotent upsert expectations,
//! content revisions, and asset references.
//!
//! Non-responsibilities: HTML sanitization, asset downloads, SQL statements,
//! RSS XML rendering, and browser extraction.
//!
//! RSS interactions: the renderer consumes normalized article/content values;
//! an article mutation causes the owning source's feed cache to be rebuilt.
