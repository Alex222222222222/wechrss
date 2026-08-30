//! Cache-first feed delivery and rebuild enqueueing.
//!
//! This module is the executable part of the feed request boundary. It reads
//! the persisted cache, honors conditional requests, serves stale bytes without
//! waiting for upstream synchronization, and enqueues one deduplicated
//! `feed_rebuild` job when the cache is stale or missing. Rendering and fenced
//! publication remain separate use cases because neither belongs on the RSS
//! request's latency-sensitive path.
//!
//! Responsibilities:
//!
//! - classify a database-clocked cache read as fresh or stale;
//! - return a typed cached, not-modified, or temporarily-unavailable result;
//! - recognize normal HTTP `If-None-Match` forms, including lists, weak
//!   validators, quoted validators, and `*`;
//! - enqueue a rebuild through an application port while allowing a stale
//!   response to succeed if that enqueue fails; and
//! - provide the v1 adapter from that port to the existing PostgreSQL `jobs`
//!   table.
//!
//! Non-responsibilities: feed-token lookup, source/article queries, RSS XML
//! rendering, browser work, source synchronization, asset handling, HTTP
//! header construction, and final job completion. Those concerns remain in
//! their owning modules. In particular, this service never calls Thirtyfour or
//! waits for a synchronization job.
//!
//! Expected interfaces: the web layer supplies a source id and an optional
//! `If-None-Match` value; a [`FeedCacheRepository`] performs one fast read; and
//! a [`FeedRebuildQueue`] maps a source to the canonical active-job dedupe key.
//! `FeedTokenService` can be placed in front of this service without changing
//! its cache or queue contracts.
//!
//! Data flow: a fresh cache is returned immediately, optionally as 304 when
//! its ETag matches. A stale cache is returned immediately and asks the queue
//! for `feed_rebuild:{source_id}`. A miss asks the same queue and returns a
//! bounded retry result; it does not render synchronously. The queue's active
//! uniqueness constraint provides cross-replica single-flight for rebuild
//! requests.
//!
//! Failure behavior: cache storage errors are returned because no response
//! metadata can be trusted. Queue errors are represented as
//! [`FeedRebuildStatus::Unavailable`]; stale content remains available, while
//! a miss returns `Retry-After` information. Queue failures should be logged or
//! metered by the application boundary without exposing database URLs or
//! credentials.
//!
//! PostgreSQL/high-availability considerations: production cache freshness is
//! computed by [`crate::persistence::repositories::feed_cache_repository::PostgresFeedCacheRepository`]
//! using PostgreSQL time, and the
//! v1 queue adapter uses the custom `jobs` table's partial unique index. The
//! service does not use a process-local mutex for distributed coordination.
//! PGMQ is intentionally not required for v1; it is documented as a possible
//! future transport optimization while the `jobs` row remains authoritative.
//!
//! The database-only rebuild orchestration lives in
//! [`super::feed_rebuild_service::FeedRebuildService`], keeping rendering and
//! publication off this latency-sensitive request path.

use std::fmt;

use chrono::{Duration, Utc};
use serde_json::json;
use thiserror::Error;

use crate::{
    domain::{
        feed::FeedCache,
        job::{JobType, NewJob},
        source::SourceId,
    },
    persistence::repositories::{
        feed_cache_repository::{FeedCacheRepository, FeedCacheRepositoryError},
        job_repository::{EnqueueResult, PostgresJobRepository},
    },
};

/// A cache request after the web layer has resolved its feed token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedRequest {
    /// Source whose persisted feed should be returned.
    pub source_id: SourceId,
    /// Raw HTTP `If-None-Match` value, if the client supplied one.
    pub if_none_match: Option<String>,
}

impl FeedRequest {
    /// Creates a cache request with an optional conditional validator.
    pub fn new(source_id: SourceId, if_none_match: Option<String>) -> Self {
        Self {
            source_id,
            if_none_match,
        }
    }
}

/// A cache freshness classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedCacheStatus {
    /// The cache revision matches the source and its expiry is still ahead of
    /// the PostgreSQL read timestamp.
    Fresh,
    /// The cache exists but is expired or represents an older source revision.
    Stale,
}

/// Outcome of the best-effort rebuild enqueue attempted by this service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedRebuildStatus {
    /// No rebuild was needed because the cache was fresh.
    NotNeeded,
    /// This request inserted the active rebuild job.
    Enqueued,
    /// A rebuild job already exists for this source.
    AlreadyActive,
    /// The cache response is still usable, but enqueueing failed.
    Unavailable,
}

/// Feed bytes and metadata returned from a cache lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedDelivery {
    /// The client's validator matches the cached ETag.
    NotModified {
        /// Cached document metadata. The body is omitted from the HTTP
        /// response, but remains available to the caller for headers.
        cache: FeedCache,
        /// Whether the cached row is current and unexpired.
        status: FeedCacheStatus,
        /// Rebuild enqueue result, if the row was stale.
        rebuild: FeedRebuildStatus,
    },
    /// Cached XML bytes should be returned with a 200 response.
    Cached {
        /// Cached XML and HTTP metadata.
        cache: FeedCache,
        /// Whether the cached row is current and unexpired.
        status: FeedCacheStatus,
        /// Rebuild enqueue result, if the row was stale.
        rebuild: FeedRebuildStatus,
    },
    /// No cache row was available and the caller should retry later.
    Unavailable {
        /// Suggested delay before trying the feed again.
        retry_after: Duration,
        /// Rebuild enqueue result for the cache miss.
        rebuild: FeedRebuildStatus,
    },
}

/// Settings for cache-miss and stale-cache response behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedServiceConfig {
    stale_while_revalidate: Duration,
    cache_miss_retry_after: Duration,
}

impl FeedServiceConfig {
    /// Creates validated feed delivery settings.
    pub fn new(
        stale_while_revalidate: Duration,
        cache_miss_retry_after: Duration,
    ) -> Result<Self, FeedServiceConfigError> {
        if stale_while_revalidate < Duration::zero() {
            return Err(FeedServiceConfigError::InvalidStaleWindow);
        }
        if cache_miss_retry_after <= Duration::zero() {
            return Err(FeedServiceConfigError::InvalidMissRetryAfter);
        }
        Ok(Self {
            stale_while_revalidate,
            cache_miss_retry_after,
        })
    }

    /// Returns the stale-while-revalidate window for HTTP header construction.
    pub const fn stale_while_revalidate(self) -> Duration {
        self.stale_while_revalidate
    }

    /// Returns the retry delay used when no cache exists.
    pub const fn cache_miss_retry_after(self) -> Duration {
        self.cache_miss_retry_after
    }
}

impl Default for FeedServiceConfig {
    fn default() -> Self {
        Self {
            stale_while_revalidate: Duration::minutes(1),
            cache_miss_retry_after: Duration::seconds(5),
        }
    }
}

/// Invalid feed delivery timing configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FeedServiceConfigError {
    /// A stale-while-revalidate period cannot be negative.
    #[error("stale-while-revalidate duration must not be negative")]
    InvalidStaleWindow,
    /// A cache miss needs a positive retry delay.
    #[error("cache-miss retry duration must be positive")]
    InvalidMissRetryAfter,
}

/// A rebuild request passed from feed delivery to the durable queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedRebuildRequest {
    /// Source whose normalized records must be rendered.
    pub source_id: SourceId,
}

/// Result of inserting or finding a deduplicated rebuild job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedRebuildEnqueueResult {
    /// A new active job was inserted.
    Enqueued,
    /// An active job already owns the source's dedupe key.
    AlreadyActive,
}

/// Errors from a rebuild queue implementation.
#[derive(Debug, Error)]
pub enum FeedRebuildQueueError {
    /// The durable queue could not accept or inspect the request.
    #[error("feed rebuild queue unavailable: {0}")]
    Unavailable(String),
}

/// Minimal queue port needed by cache-first feed delivery.
#[allow(async_fn_in_trait)]
pub trait FeedRebuildQueue: Send + Sync {
    /// Enqueues one source rebuild under its active dedupe key.
    async fn enqueue_rebuild(
        &self,
        request: FeedRebuildRequest,
    ) -> Result<FeedRebuildEnqueueResult, FeedRebuildQueueError>;
}

/// Job settings used by the v1 PostgreSQL rebuild adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedRebuildJobConfig {
    /// Priority assigned to feed rebuild jobs.
    pub priority: i32,
    /// Retryable failure budget assigned to each rebuild job.
    pub max_attempts: u32,
}

impl FeedRebuildJobConfig {
    /// Creates validated settings for the custom `jobs` table adapter.
    pub fn new(priority: i32, max_attempts: u32) -> Result<Self, FeedRebuildJobConfigError> {
        if max_attempts == 0 {
            return Err(FeedRebuildJobConfigError::InvalidAttemptLimit);
        }
        Ok(Self {
            priority,
            max_attempts,
        })
    }
}

impl Default for FeedRebuildJobConfig {
    fn default() -> Self {
        Self {
            priority: 0,
            max_attempts: 3,
        }
    }
}

/// Invalid durable feed-rebuild job settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FeedRebuildJobConfigError {
    /// The job must be allowed at least one retryable failure.
    #[error("feed rebuild max attempts must be positive")]
    InvalidAttemptLimit,
}

/// PostgreSQL queue adapter that keeps the custom `jobs` table authoritative.
///
/// This adapter deliberately maps only enqueueing. Worker outcome transitions
/// still belong to the shared `UnitOfWork` so article, source, sync-run, cache,
/// and job writes can commit atomically.
#[derive(Clone)]
pub struct PostgresFeedRebuildQueue {
    jobs: PostgresJobRepository,
    config: FeedRebuildJobConfig,
}

impl fmt::Debug for PostgresFeedRebuildQueue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresFeedRebuildQueue")
            .field("jobs", &self.jobs)
            .field("config", &self.config)
            .finish()
    }
}

impl PostgresFeedRebuildQueue {
    /// Creates a feed rebuild queue over the v1 custom PostgreSQL jobs table.
    pub fn new(jobs: PostgresJobRepository, config: FeedRebuildJobConfig) -> Self {
        Self { jobs, config }
    }
}

impl FeedRebuildQueue for PostgresFeedRebuildQueue {
    async fn enqueue_rebuild(
        &self,
        request: FeedRebuildRequest,
    ) -> Result<FeedRebuildEnqueueResult, FeedRebuildQueueError> {
        // The PostgreSQL repository replaces these domain-input timestamps
        // with one statement-local database timestamp before inserting.
        let now = Utc::now();
        let result = self
            .jobs
            .enqueue_immediately(NewJob {
                job_type: JobType::FeedRebuild,
                source_id: Some(request.source_id.as_uuid()),
                priority: self.config.priority,
                run_after: now,
                max_attempts: self.config.max_attempts,
                payload: json!({"source_id": request.source_id.to_string()}),
                dedupe_key: format!("feed_rebuild:{}", request.source_id),
                now,
            })
            .await
            .map_err(|error| FeedRebuildQueueError::Unavailable(error.to_string()))?;

        Ok(match result {
            EnqueueResult::Inserted(_) => FeedRebuildEnqueueResult::Enqueued,
            EnqueueResult::AlreadyActive { .. } => FeedRebuildEnqueueResult::AlreadyActive,
        })
    }
}

/// Errors returned by cache-first feed delivery.
#[derive(Debug, Error)]
pub enum FeedServiceError {
    /// A nil source id cannot identify a cache or rebuild job.
    #[error("source id must not be nil")]
    InvalidSourceId,
    /// The cache repository failed to provide a trustworthy read.
    #[error(transparent)]
    Cache(#[from] FeedCacheRepositoryError),
}

/// Cache-first feed delivery service.
pub struct FeedService<C, Q> {
    cache: C,
    rebuild_queue: Q,
    config: FeedServiceConfig,
}

impl<C, Q> FeedService<C, Q>
where
    C: FeedCacheRepository,
    Q: FeedRebuildQueue,
{
    /// Creates a service over one cache reader and one durable rebuild queue.
    pub fn new(cache: C, rebuild_queue: Q, config: FeedServiceConfig) -> Self {
        Self {
            cache,
            rebuild_queue,
            config,
        }
    }

    /// Returns cached feed bytes or a bounded temporary-unavailable result.
    pub async fn get_feed(&self, request: FeedRequest) -> Result<FeedDelivery, FeedServiceError> {
        validate_source_id(request.source_id)?;
        let Some(read) = self.cache.get(request.source_id).await? else {
            let rebuild = self.enqueue_rebuild(request.source_id).await;
            return Ok(FeedDelivery::Unavailable {
                retry_after: self.config.cache_miss_retry_after(),
                rebuild,
            });
        };

        let status = if read.is_fresh() {
            FeedCacheStatus::Fresh
        } else {
            FeedCacheStatus::Stale
        };
        let rebuild = match status {
            FeedCacheStatus::Fresh => FeedRebuildStatus::NotNeeded,
            FeedCacheStatus::Stale => self.enqueue_rebuild(read.cache().source_id()).await,
        };
        let cache = read.cache().clone();

        if if_none_match_matches(request.if_none_match.as_deref(), cache.etag()) {
            Ok(FeedDelivery::NotModified {
                cache,
                status,
                rebuild,
            })
        } else {
            Ok(FeedDelivery::Cached {
                cache,
                status,
                rebuild,
            })
        }
    }

    /// Returns the configured stale-while-revalidate window for HTTP mapping.
    pub const fn stale_while_revalidate(&self) -> Duration {
        self.config.stale_while_revalidate()
    }

    async fn enqueue_rebuild(&self, source_id: SourceId) -> FeedRebuildStatus {
        match self
            .rebuild_queue
            .enqueue_rebuild(FeedRebuildRequest { source_id })
            .await
        {
            Ok(FeedRebuildEnqueueResult::Enqueued) => FeedRebuildStatus::Enqueued,
            Ok(FeedRebuildEnqueueResult::AlreadyActive) => FeedRebuildStatus::AlreadyActive,
            Err(error) => {
                tracing::warn!(%source_id, error = %error, "unable to enqueue feed rebuild");
                FeedRebuildStatus::Unavailable
            }
        }
    }
}

fn validate_source_id(source_id: SourceId) -> Result<(), FeedServiceError> {
    if source_id.as_uuid().is_nil() {
        Err(FeedServiceError::InvalidSourceId)
    } else {
        Ok(())
    }
}

/// Matches an HTTP `If-None-Match` value against the internal unquoted ETag.
fn if_none_match_matches(header: Option<&str>, etag: &str) -> bool {
    header.is_some_and(|header| {
        header.split(',').any(|candidate| {
            let candidate = candidate.trim();
            if candidate == "*" {
                return true;
            }
            let candidate = candidate.strip_prefix("W/").unwrap_or(candidate).trim();
            candidate.trim_matches('"') == etag
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, TimeZone};
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::*;
    use crate::domain::{
        feed::{FeedCacheCandidate, FeedCacheRead},
        source::FeedRevision,
    };

    #[derive(Clone)]
    struct TestCache {
        read: Arc<Mutex<Option<FeedCacheRead>>>,
        fail: bool,
    }

    impl TestCache {
        fn hit(read: FeedCacheRead) -> Self {
            Self {
                read: Arc::new(Mutex::new(Some(read))),
                fail: false,
            }
        }

        fn miss() -> Self {
            Self {
                read: Arc::new(Mutex::new(None)),
                fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                read: Arc::new(Mutex::new(None)),
                fail: true,
            }
        }
    }

    impl FeedCacheRepository for TestCache {
        async fn get(
            &self,
            _source_id: SourceId,
        ) -> Result<Option<FeedCacheRead>, FeedCacheRepositoryError> {
            if self.fail {
                return Err(FeedCacheRepositoryError::Storage("test failure".to_owned()));
            }
            Ok(self.read.lock().await.clone())
        }
    }

    #[derive(Clone)]
    struct TestQueue {
        requests: Arc<Mutex<Vec<FeedRebuildRequest>>>,
        outcome: Result<FeedRebuildEnqueueResult, String>,
    }

    impl TestQueue {
        fn successful(outcome: FeedRebuildEnqueueResult) -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                outcome: Ok(outcome),
            }
        }

        fn failing() -> Self {
            Self {
                requests: Arc::new(Mutex::new(Vec::new())),
                outcome: Err("test queue failure".to_owned()),
            }
        }

        async fn request_count(&self) -> usize {
            self.requests.lock().await.len()
        }
    }

    impl FeedRebuildQueue for TestQueue {
        async fn enqueue_rebuild(
            &self,
            request: FeedRebuildRequest,
        ) -> Result<FeedRebuildEnqueueResult, FeedRebuildQueueError> {
            self.requests.lock().await.push(request);
            self.outcome
                .clone()
                .map_err(FeedRebuildQueueError::Unavailable)
        }
    }

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn cache_read(fresh: bool) -> FeedCacheRead {
        let source_id = source_id();
        let generated_at = at(10);
        let cache = FeedCache::from_candidate(
            FeedCacheCandidate::from_parts(
                source_id,
                b"<rss/>".to_vec(),
                "sha256:test".to_owned(),
                generated_at,
                at(40),
                FeedRevision::from_u64(1),
                "test-hash".to_owned(),
            ),
            generated_at,
        );
        FeedCacheRead::from_parts(cache, FeedRevision::from_u64(1), fresh)
    }

    #[tokio::test]
    async fn fresh_matching_etag_returns_not_modified_without_enqueueing() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let service = FeedService::new(
            TestCache::hit(cache_read(true)),
            queue.clone(),
            FeedServiceConfig::default(),
        );

        let delivery = service
            .get_feed(FeedRequest::new(
                source_id(),
                Some("W/\"sha256:test\", \"other\"".to_owned()),
            ))
            .await
            .expect("cache read should succeed");

        assert!(matches!(
            delivery,
            FeedDelivery::NotModified {
                status: FeedCacheStatus::Fresh,
                rebuild: FeedRebuildStatus::NotNeeded,
                ..
            }
        ));
        assert_eq!(queue.request_count().await, 0);
    }

    #[tokio::test]
    async fn stale_cache_is_served_and_rebuild_is_enqueued() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let service = FeedService::new(
            TestCache::hit(cache_read(false)),
            queue.clone(),
            FeedServiceConfig::default(),
        );

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("stale cache should remain serveable");

        match delivery {
            FeedDelivery::Cached {
                cache,
                status: FeedCacheStatus::Stale,
                rebuild: FeedRebuildStatus::Enqueued,
            } => assert_eq!(cache.xml_bytes(), b"<rss/>"),
            other => panic!("expected stale cached delivery, got {other:?}"),
        }
        assert_eq!(queue.request_count().await, 1);
    }

    #[tokio::test]
    async fn cache_miss_returns_retry_after_even_when_queue_is_temporarily_unavailable() {
        let queue = TestQueue::failing();
        let config = FeedServiceConfig::new(Duration::minutes(1), Duration::seconds(7))
            .expect("test timing should be valid");
        let service = FeedService::new(TestCache::miss(), queue.clone(), config);

        let delivery = service
            .get_feed(FeedRequest::new(source_id(), None))
            .await
            .expect("queue failure should not become a cache error");

        assert!(matches!(
            delivery,
            FeedDelivery::Unavailable {
                retry_after,
                rebuild: FeedRebuildStatus::Unavailable,
            } if retry_after == Duration::seconds(7)
        ));
        assert_eq!(queue.request_count().await, 1);
    }

    #[tokio::test]
    async fn invalid_source_and_cache_errors_are_not_converted_to_rebuilds() {
        let queue = TestQueue::successful(FeedRebuildEnqueueResult::Enqueued);
        let service = FeedService::new(
            TestCache::failing(),
            queue.clone(),
            FeedServiceConfig::default(),
        );

        assert!(matches!(
            service
                .get_feed(FeedRequest::new(SourceId::from_uuid(Uuid::nil()), None))
                .await,
            Err(FeedServiceError::InvalidSourceId)
        ));
        assert!(matches!(
            service.get_feed(FeedRequest::new(source_id(), None)).await,
            Err(FeedServiceError::Cache(FeedCacheRepositoryError::Storage(
                _
            )))
        ));
        assert_eq!(queue.request_count().await, 0);
    }

    #[test]
    fn validates_feed_delivery_and_job_timing() {
        assert!(matches!(
            FeedServiceConfig::new(Duration::seconds(-1), Duration::seconds(1)),
            Err(FeedServiceConfigError::InvalidStaleWindow)
        ));
        assert!(matches!(
            FeedServiceConfig::new(Duration::zero(), Duration::zero()),
            Err(FeedServiceConfigError::InvalidMissRetryAfter)
        ));
        assert!(matches!(
            FeedRebuildJobConfig::new(0, 0),
            Err(FeedRebuildJobConfigError::InvalidAttemptLimit)
        ));
    }

    #[test]
    fn matches_wildcard_and_weak_quoted_entity_tags() {
        assert!(if_none_match_matches(Some("*"), "etag"));
        assert!(if_none_match_matches(Some("W/\"etag\""), "etag"));
        assert!(if_none_match_matches(Some("\"other\", etag"), "etag"));
        assert!(!if_none_match_matches(Some("\"other\""), "etag"));
    }
}
