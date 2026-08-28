//! Binary article-asset storage abstraction.
//!
//! Defines the future interface for storing, reading, checking, and deleting
//! archived images or other allowed media by checksum/object key.
//!
//! This component is optional in version one because article archival does not
//! require binary asset caching. When enabled, the first implementation may use
//! a persistent local volume. A later
//! implementation can use S3-compatible object storage without changing domain
//! or RSS code. PostgreSQL retains metadata, checksum, MIME type, size, and
//! object key.
//!
//! Asset writes must be idempotent and safe to retry. Orphan cleanup is a
//! separate maintenance concern and must not run as part of an RSS request.
