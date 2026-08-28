//! RSS XML renderer and cache-payload builder.
//!
//! Renders stable GUIDs from article identity, escaped XML, publication dates,
//! summaries, archived HTML in `content:encoded`, original WeChat links, and
//! either approved external or optionally rewritten local asset URLs. It also
//! computes a deterministic ETag/content hash for the persisted feed cache.
//! The render input includes the source's monotonic feed revision, which is
//! returned with the payload so persistence can perform a compare-and-swap
//! replacement. The renderer itself does not decide whether that revision is
//! still current.
//!
//! The renderer consumes data supplied by application services and never reads
//! the network. It does not decide cache freshness; `feed_cache_repository` and
//! the feed application use case own the 30-minute TTL and stale-while-
//! revalidate policy.

// TODO(design): define revision-carrying RenderFeedInput/RenderedFeed types and
// deterministic fixture tests before implementing XML generation.
