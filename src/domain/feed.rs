//! Feed-cache domain values.
//!
//! These values describe a rendered RSS document without coupling the domain
//! to SQLx, Axum, or the renderer implementation. A candidate carries the
//! source revision observed by the renderer; persistence must compare that
//! revision with the current source before publishing it.
//!
//! Responsibilities: preserve the immutable cache payload and its freshness,
//! ETag, content hash, and source-revision metadata; provide pure freshness
//! comparisons for unit tests; and keep cache publication inputs explicit.
//!
//! Non-responsibilities: rendering XML, fetching articles, choosing cache TTLs,
//! acquiring a build lease, or writing PostgreSQL rows. The persistence layer
//! owns compare-and-swap publication and lease fencing.

use chrono::{DateTime, Utc};

use super::source::{FeedRevision, SourceId};

/// A rendered RSS document already stored for one source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedCache {
    source_id: SourceId,
    xml_bytes: Vec<u8>,
    etag: String,
    generated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    feed_revision: FeedRevision,
    content_hash: String,
    updated_at: DateTime<Utc>,
}

impl FeedCache {
    /// Reconstructs a cache row from the candidate that was persisted.
    pub(crate) fn from_candidate(candidate: FeedCacheCandidate, updated_at: DateTime<Utc>) -> Self {
        Self {
            source_id: candidate.source_id,
            xml_bytes: candidate.xml_bytes,
            etag: candidate.etag,
            generated_at: candidate.generated_at,
            expires_at: candidate.expires_at,
            feed_revision: candidate.feed_revision,
            content_hash: candidate.content_hash,
            updated_at,
        }
    }

    /// Returns the source owning this cache row.
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// Returns the cached XML bytes.
    pub fn xml_bytes(&self) -> &[u8] {
        &self.xml_bytes
    }

    /// Returns the HTTP entity tag.
    pub fn etag(&self) -> &str {
        &self.etag
    }

    /// Returns when the renderer produced this document.
    pub const fn generated_at(&self) -> DateTime<Utc> {
        self.generated_at
    }

    /// Returns the cache expiry instant.
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    /// Returns the source revision represented by this document.
    pub const fn feed_revision(&self) -> FeedRevision {
        self.feed_revision
    }

    /// Returns the content hash used for ETag/debugging consistency checks.
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    /// Returns when this row was last persisted.
    pub const fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// Reports whether the document is current and unexpired at `now`.
    pub fn is_fresh_at(&self, source_revision: FeedRevision, now: DateTime<Utc>) -> bool {
        self.feed_revision == source_revision && self.expires_at > now
    }
}

/// A cache row together with the database-clocked freshness decision.
///
/// The source revision and freshness flag are read in the same PostgreSQL
/// statement as the cache row. This prevents the feed service from making a
/// freshness decision from a skewed application clock or a separately read
/// source revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedCacheRead {
    cache: FeedCache,
    source_revision: FeedRevision,
    fresh: bool,
}

impl FeedCacheRead {
    /// Reconstructs a database-clocked cache read returned by a repository.
    pub(crate) fn from_parts(cache: FeedCache, source_revision: FeedRevision, fresh: bool) -> Self {
        Self {
            cache,
            source_revision,
            fresh,
        }
    }

    /// Returns the cached document and metadata.
    pub fn cache(&self) -> &FeedCache {
        &self.cache
    }

    /// Returns the source revision observed with this read.
    pub const fn source_revision(&self) -> FeedRevision {
        self.source_revision
    }

    /// Reports the database-clocked current/revision-matching state.
    pub const fn is_fresh(&self) -> bool {
        self.fresh
    }
}

/// A rendered document waiting for fenced compare-and-swap publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedCacheCandidate {
    source_id: SourceId,
    xml_bytes: Vec<u8>,
    etag: String,
    generated_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    feed_revision: FeedRevision,
    content_hash: String,
}

impl FeedCacheCandidate {
    /// Creates a candidate from one renderer snapshot.
    pub fn from_parts(
        source_id: SourceId,
        xml_bytes: Vec<u8>,
        etag: String,
        generated_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        feed_revision: FeedRevision,
        content_hash: String,
    ) -> Self {
        Self {
            source_id,
            xml_bytes,
            etag,
            generated_at,
            expires_at,
            feed_revision,
            content_hash,
        }
    }

    /// Returns the source owning this candidate.
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// Returns the candidate XML bytes.
    pub fn xml_bytes(&self) -> &[u8] {
        &self.xml_bytes
    }

    /// Returns the candidate ETag.
    pub fn etag(&self) -> &str {
        &self.etag
    }

    /// Returns the renderer timestamp used to protect same-revision races.
    pub const fn generated_at(&self) -> DateTime<Utc> {
        self.generated_at
    }

    /// Returns the candidate expiry instant.
    pub const fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }

    /// Returns the source revision represented by this candidate.
    pub const fn feed_revision(&self) -> FeedRevision {
        self.feed_revision
    }

    /// Returns the candidate content hash.
    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use uuid::Uuid;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    #[test]
    fn feed_revision_is_monotonic_until_the_numeric_limit() {
        let revision = FeedRevision::zero();
        assert_eq!(revision.as_u64(), 0);
        assert_eq!(revision.next().unwrap().as_u64(), 1);
        assert!(FeedRevision::from_u64(u64::MAX).next().is_none());
    }

    #[test]
    fn cache_freshness_requires_matching_revision_and_future_expiry() {
        let source_id = SourceId::from_uuid(Uuid::from_u128(1));
        let cache = FeedCache::from_candidate(
            FeedCacheCandidate::from_parts(
                source_id,
                b"<rss/>".to_vec(),
                "etag".to_owned(),
                at(10),
                at(40),
                FeedRevision::from_u64(2),
                "hash".to_owned(),
            ),
            at(10),
        );

        assert!(cache.is_fresh_at(FeedRevision::from_u64(2), at(20)));
        assert!(!cache.is_fresh_at(FeedRevision::from_u64(1), at(20)));
        assert!(!cache.is_fresh_at(FeedRevision::from_u64(2), at(40)));
    }
}
