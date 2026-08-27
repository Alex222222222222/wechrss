//! REST API route and DTO boundary.
//!
//! Documents administrative endpoints for authentication, sources, articles,
//! manual sync, backfill, health/readiness, and job status. It also documents
//! the public/tokenized feed and media routes.
//!
//! Request DTO validation happens here; use-case sequencing belongs to
//! application services. Access and refresh tokens must never appear in
//! response DTOs, tracing fields, or error messages.
//!
//! The feed handler reads `feed_cache`, emits ETag/Last-Modified headers, and
//! supports conditional requests without invoking acquisition code.
