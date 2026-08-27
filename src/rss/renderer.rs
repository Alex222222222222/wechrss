//! RSS XML renderer and cache-payload builder.
//!
//! Renders stable GUIDs from article identity, escaped XML, publication dates,
//! summaries, archived HTML in `content:encoded`, original WeChat links, and
//! rewritten local asset URLs. It also computes a deterministic ETag/content
//! hash for the persisted feed cache.
//!
//! The renderer consumes data supplied by application services and never reads
//! the network. It does not decide cache freshness; `feed_cache_repository` and
//! the feed application use case own the 30-minute TTL and stale-while-
//! revalidate policy.
