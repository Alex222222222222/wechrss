//! Cached-feed delivery and rebuild orchestration.
//!
//! Purpose: provide the application-layer owner for feed-token lookup,
//! conditional HTTP metadata, cache freshness, cache-miss population, and
//! database-only feed rebuilds. The web layer should call this service instead
//! of coordinating source, article, cache, renderer, and job repositories.
//!
//! Expected interfaces:
//!
//! - `get_feed(feed_token, if_none_match)` returns a typed result such as
//!   `NotModified`, `Fresh`, or `Stale`; response bytes and metadata come from
//!   the persisted cache;
//! - `populate_missing(source_id)` performs a per-source single-flight
//!   render from normalized database rows without contacting a browser; and
//! - `rebuild(source_id, expected_revision)` renders one database snapshot
//!   and asks the cache repository to replace it with compare-and-swap
//!   semantics.
//!
//! Dependencies: source/feed-token lookup, source-scoped RSS input queries,
//! `rss::renderer`, `feed_cache_repository`, `job_service`, and the persistence
//! `UnitOfWorkFactory`. Freshness and lease metadata returned by production
//! persistence are based on PostgreSQL time. The service must not depend on
//! acquisition adapters, WebDriver, WeRead credentials, or raw SQLx types.
//!
//! Data flow: read the source's monotonic `feed_revision`; compare it with the
//! cached revision and expiry; honor the ETag; return stale bytes immediately
//! while enqueueing one deduplicated rebuild; or, on a true miss, acquire the
//! fenced `feed_build_leases` row and render current database records outside a
//! transaction. Cache writes succeed only if the source revision still equals
//! the rendered revision, the build fence remains live, and no newer cache row
//! exists. Advisory locks are not used because rendering must not retain a
//! pooled connection.
//!
//! Failure behavior: a stale row remains serveable when enqueue or rebuild
//! fails. A cache miss may return a typed temporarily-unavailable result if the
//! single-flight owner cannot produce a first document. A non-owner may poll
//! briefly for that document, then returns `Retry-After` rather than rendering
//! concurrently. No feed request may start browser work or wait for source
//! synchronization.
//!
//! HTTP interaction: fresh responses advertise only their remaining TTL. Stale
//! responses use `max-age=0` plus a bounded `stale-while-revalidate` directive;
//! they must not reset the full 30-minute freshness period.

// TODO(design): define FeedService ports and result DTOs plus the revision-aware
// feed-cache repository before implementing the Axum feed handler.
