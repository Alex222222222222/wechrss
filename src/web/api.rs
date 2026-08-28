//! REST API route and DTO boundary.
//!
//! Documents administrative endpoints for authentication, sources, articles,
//! manual sync, backfill, health/readiness, and job status. It also documents
//! the public/tokenized feed and the optional media route used by archived
//! binary assets.
//!
//! Request DTO validation happens here; use-case sequencing belongs to
//! application services. Access and refresh tokens must never appear in
//! response DTOs, tracing fields, or error messages.
//!
//! The feed handler delegates to `FeedService`, emits its ETag/Last-Modified and
//! freshness metadata, and supports conditional requests without invoking
//! acquisition code. It must not reset a stale row to a fresh 30-minute HTTP
//! lifetime.
//! A true cache miss owned by another live feed-build lease may poll only for a
//! short configured bound; if no document appears, the handler maps the typed
//! result to `503 Service Unavailable` with `Retry-After`.
//!
//! Liveness is process-only. API readiness requires PostgreSQL but remains ready
//! to serve persisted feeds when the browser component is degraded; browser
//! health is reported separately and gates worker claims.

// TODO(design): implement feed routes through FeedService and role-aware health
// responses; do not coordinate renderer/repositories directly in Axum handlers.
