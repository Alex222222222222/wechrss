//! Persisted RSS feed-cache repository.
//!
//! Stores one rendered XML document per source, its ETag/content hash,
//! generated time, expiry time, and monotonic source feed revision. Reads and
//! fenced compare-and-swap replacement are implemented below. The durable
//! `feed_build_leases` single-flight boundary coordinates cache misses before
//! rendering and final publication.
//!
//! Freshness is intended to default to 30 minutes. Stale rows should remain
//! serveable for stale-while-revalidate behavior, while cache misses are
//! eventually populated by a feed rebuild use case. This repository never
//! contacts WeChat or the browser. The build-lease operations use PostgreSQL
//! server time in short committed statements, not a connection-scoped
//! advisory lock; rendering therefore happens after the lease statement
//! releases its connection.
//!
//! Final replacement verifies source revision, build owner/token, and the live
//! build lease in one `UnitOfWork`, then releases the lease as part of that same
//! transaction. The currently implemented operations are
//! `acquire_build(source_id, owner, lease_for)`,
//! `heartbeat_build(source_id, owner, token, lease_for)`, and
//! `release_build(source_id, owner, token)`. None accepts a caller wall clock
//! in the PostgreSQL implementation. The standalone `get` operation is for
//! fast RSS reads; `publish_if_current` is intentionally transaction-scoped.

use std::{collections::HashMap, fmt, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use tokio::sync::Mutex;

use crate::{
    domain::{
        feed::{FeedCache, FeedCacheCandidate, FeedCacheRead},
        source::{FeedBuildLease, FeedBuildLeaseToken, FeedRevision, SourceId},
    },
    persistence::repositories::job_repository::{JobRepositoryError, PostgresJobTransaction},
};

/// Errors returned by feed-build lease repositories.
#[derive(Debug, Error)]
pub enum FeedBuildLeaseError {
    /// A nil UUID cannot identify a source.
    #[error("source id must not be nil")]
    InvalidSourceId,
    /// Lease ownership must identify an application instance.
    #[error("feed-build lease owner must not be empty")]
    EmptyOwner,
    /// A lease must be positive and representable by the repository.
    #[error("feed-build lease duration must be positive and fit in milliseconds")]
    InvalidLeaseDuration,
    /// The current owner or fencing token no longer controls the lease.
    #[error("feed-build lease for {source_id} is no longer owned by this claim")]
    LeaseLost { source_id: SourceId },
    /// The database could not complete the operation.
    #[error("feed-build lease storage error: {0}")]
    Storage(String),
}

/// Distributed single-flight operations for one source's feed build.
///
/// Production implementations derive all lease-sensitive timestamps from the
/// database clock. The interface has no caller-supplied wall clock, so a
/// replica with a skewed clock cannot take over a live build or renew an
/// expired one.
#[allow(async_fn_in_trait)]
pub trait FeedBuildLeaseRepository: Send + Sync {
    /// Acquires a lease or returns `None` when another live builder owns it.
    async fn acquire_build(
        &self,
        source_id: SourceId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<FeedBuildLease>, FeedBuildLeaseError>;

    /// Extends a lease only when source, owner, token, and liveness match.
    async fn heartbeat_build(
        &self,
        source_id: SourceId,
        owner: &str,
        token: FeedBuildLeaseToken,
        lease_for: Duration,
    ) -> Result<FeedBuildLease, FeedBuildLeaseError>;

    /// Releases a live lease only for its current owner and fencing token.
    async fn release_build(
        &self,
        source_id: SourceId,
        owner: &str,
        token: FeedBuildLeaseToken,
    ) -> Result<(), FeedBuildLeaseError>;
}

/// PostgreSQL-backed feed-build lease repository.
#[derive(Clone)]
pub struct PostgresFeedBuildLeaseRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresFeedBuildLeaseRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresFeedBuildLeaseRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresFeedBuildLeaseRepository {
    /// Creates a repository backed by the shared PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl FeedBuildLeaseRepository for PostgresFeedBuildLeaseRepository {
    async fn acquire_build(
        &self,
        source_id: SourceId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<FeedBuildLease>, FeedBuildLeaseError> {
        validate_inputs(source_id, owner, lease_for)?;
        let milliseconds = lease_milliseconds(lease_for)?;
        let token = FeedBuildLeaseToken::new();
        let row = sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            INSERT INTO feed_build_leases (
                source_id, lease_owner, lease_token, lease_until,
                heartbeat_at, created_at, updated_at
            )
            SELECT $1, $2, $3,
                   db_clock.now + ($4::double precision * INTERVAL '1 millisecond'),
                   db_clock.now, db_clock.now, db_clock.now
            FROM db_clock
            ON CONFLICT (source_id) DO UPDATE
            SET lease_owner = EXCLUDED.lease_owner,
                lease_token = EXCLUDED.lease_token,
                lease_until = EXCLUDED.lease_until,
                heartbeat_at = EXCLUDED.heartbeat_at,
                updated_at = EXCLUDED.updated_at
            WHERE feed_build_leases.lease_until <= (SELECT now FROM db_clock)
            RETURNING source_id, lease_owner, lease_token, lease_until,
                      heartbeat_at
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(owner)
        .bind(token.as_uuid())
        .bind(milliseconds)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(decode_lease).transpose()
    }

    async fn heartbeat_build(
        &self,
        source_id: SourceId,
        owner: &str,
        token: FeedBuildLeaseToken,
        lease_for: Duration,
    ) -> Result<FeedBuildLease, FeedBuildLeaseError> {
        validate_inputs(source_id, owner, lease_for)?;
        let milliseconds = lease_milliseconds(lease_for)?;
        let row = sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            UPDATE feed_build_leases AS lease
            SET lease_until = db_clock.now + ($4::double precision * INTERVAL '1 millisecond'),
                heartbeat_at = db_clock.now,
                updated_at = db_clock.now
            FROM db_clock
            WHERE lease.source_id = $1
              AND lease.lease_owner = $2
              AND lease.lease_token = $3
              AND lease.lease_until > db_clock.now
            RETURNING lease.source_id, lease.lease_owner, lease.lease_token,
                      lease.lease_until, lease.heartbeat_at
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(owner)
        .bind(token.as_uuid())
        .bind(milliseconds)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(decode_lease)
            .transpose()?
            .ok_or(FeedBuildLeaseError::LeaseLost { source_id })
    }

    async fn release_build(
        &self,
        source_id: SourceId,
        owner: &str,
        token: FeedBuildLeaseToken,
    ) -> Result<(), FeedBuildLeaseError> {
        validate_source_and_owner(source_id, owner)?;
        let released = sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            DELETE FROM feed_build_leases AS lease
            USING db_clock
            WHERE lease.source_id = $1
              AND lease.lease_owner = $2
              AND lease.lease_token = $3
              AND lease.lease_until > db_clock.now
            RETURNING lease.source_id
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(owner)
        .bind(token.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        if released.is_some() {
            Ok(())
        } else {
            Err(FeedBuildLeaseError::LeaseLost { source_id })
        }
    }
}

/// Result of attempting to publish a renderer candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedCachePublishResult {
    /// The candidate became the current cache row.
    Published(FeedCache),
    /// The source changed after the renderer captured its snapshot.
    SourceRevisionChanged {
        /// Revision observed while the publication transaction held the
        /// source row lock.
        current_revision: FeedRevision,
    },
    /// A cache row for the same source/revision was already newer.
    ExistingCacheNewer,
}

/// Errors returned while reading or publishing feed-cache rows.
#[derive(Debug, Error)]
pub enum FeedCacheRepositoryError {
    /// A nil UUID cannot identify a source.
    #[error("source id must not be nil")]
    InvalidSourceId,
    /// Lease ownership must identify an application instance.
    #[error("feed-build lease owner must not be empty")]
    EmptyOwner,
    /// A rendered feed must contain bytes before it is published.
    #[error("feed-cache XML must not be empty")]
    EmptyXml,
    /// An HTTP ETag is required for conditional RSS responses.
    #[error("feed-cache ETag must not be empty")]
    EmptyEtag,
    /// A content hash is required for cache consistency checks.
    #[error("feed-cache content hash must not be empty")]
    EmptyContentHash,
    /// Expiration must follow the renderer timestamp.
    #[error("feed-cache expiry must be after generation")]
    InvalidExpiry,
    /// PostgreSQL BIGINT cannot represent this revision value.
    #[error("feed revision is outside the PostgreSQL BIGINT range")]
    RevisionOutOfRange,
    /// The source was removed before publication.
    #[error("source {source_id} was not found")]
    SourceNotFound { source_id: SourceId },
    /// The build lease is no longer held by the caller.
    #[error("feed-build lease for {source_id} is no longer owned by this claim")]
    LeaseLost { source_id: SourceId },
    /// The backing store could not complete the operation.
    #[error("feed-cache storage error: {0}")]
    Storage(String),
}

/// Fast, transaction-independent reads used by the RSS endpoint.
#[allow(async_fn_in_trait)]
pub trait FeedCacheRepository: Send + Sync {
    /// Reads the current cached bytes and metadata for one source.
    async fn get(
        &self,
        source_id: SourceId,
    ) -> Result<Option<FeedCacheRead>, FeedCacheRepositoryError>;
}

/// PostgreSQL feed-cache reader backed by the shared pool.
#[derive(Clone)]
pub struct PostgresFeedCacheRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresFeedCacheRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresFeedCacheRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresFeedCacheRepository {
    /// Creates a feed-cache reader backed by the configured PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl FeedCacheRepository for PostgresFeedCacheRepository {
    async fn get(
        &self,
        source_id: SourceId,
    ) -> Result<Option<FeedCacheRead>, FeedCacheRepositoryError> {
        validate_source_id_for_cache(source_id)?;
        sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            SELECT cache.source_id, cache.xml_bytes, cache.etag,
                   cache.generated_at, cache.expires_at, cache.feed_revision,
                   cache.content_hash, cache.updated_at,
                   source.feed_revision AS source_feed_revision,
                   cache.expires_at > db_clock.now
                       AND cache.feed_revision = source.feed_revision AS fresh
            FROM feed_cache AS cache
            JOIN sources AS source ON source.id = cache.source_id
            CROSS JOIN db_clock
            WHERE cache.source_id = $1
            "#,
        )
        .bind(source_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(cache_storage_error)?
        .map(decode_cache_read)
        .transpose()
    }
}

/// Operations on feed-cache rows inside the shared application transaction.
#[allow(async_fn_in_trait)]
pub trait FeedCacheTransactionRepository {
    /// Publishes only if the source revision and live build fence still match.
    ///
    /// The method releases the matching build lease before returning a normal
    /// result. Callers must commit the surrounding unit of work to publish the
    /// cache and lease release together.
    async fn publish_if_current(
        &mut self,
        candidate: FeedCacheCandidate,
        owner: &str,
        token: FeedBuildLeaseToken,
    ) -> Result<FeedCachePublishResult, FeedCacheRepositoryError>;
}

/// Transaction-scoped PostgreSQL feed-cache view owned by `UnitOfWork`.
pub struct PostgresFeedCacheTransaction<'borrow, 'pool> {
    job_transaction: &'borrow mut PostgresJobTransaction<'pool>,
}

impl<'borrow, 'pool> PostgresFeedCacheTransaction<'borrow, 'pool> {
    /// Creates a view over the transaction owned by the shared job boundary.
    pub(crate) fn new(job_transaction: &'borrow mut PostgresJobTransaction<'pool>) -> Self {
        Self { job_transaction }
    }
}

impl FeedCacheTransactionRepository for PostgresFeedCacheTransaction<'_, '_> {
    async fn publish_if_current(
        &mut self,
        candidate: FeedCacheCandidate,
        owner: &str,
        token: FeedBuildLeaseToken,
    ) -> Result<FeedCachePublishResult, FeedCacheRepositoryError> {
        let revision = validate_candidate(&candidate, owner)?;
        let transaction = self
            .job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)?;

        let source_id = candidate.source_id();
        let source_row = sqlx::query("SELECT feed_revision FROM sources WHERE id = $1 FOR UPDATE")
            .bind(source_id.as_uuid())
            .fetch_optional(&mut **transaction)
            .await
            .map_err(cache_storage_error)?;
        let Some(source_row) = source_row else {
            return Err(FeedCacheRepositoryError::SourceNotFound { source_id });
        };
        let current_revision = persisted_revision(
            source_row
                .try_get::<i64, _>("feed_revision")
                .map_err(cache_storage_error)?,
        )?;

        let live_lease = sqlx::query(
            r#"
            SELECT source_id
            FROM feed_build_leases
            WHERE source_id = $1
              AND lease_owner = $2
              AND lease_token = $3
              AND lease_until > clock_timestamp()
            FOR UPDATE
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(owner)
        .bind(token.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(cache_storage_error)?;
        if live_lease.is_none() {
            return Err(FeedCacheRepositoryError::LeaseLost { source_id });
        }

        if current_revision.as_u64() != revision as u64 {
            release_build_lease(transaction, source_id, owner, token).await?;
            return Ok(FeedCachePublishResult::SourceRevisionChanged { current_revision });
        }

        let cache_row = sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            INSERT INTO feed_cache (
                source_id, xml_bytes, etag, generated_at, expires_at,
                feed_revision, content_hash, updated_at
            )
            SELECT $1, $2, $3, $4, $5, $6, $7, db_clock.now
            FROM db_clock
            WHERE EXISTS (
                SELECT 1
                FROM feed_build_leases AS lease
                WHERE lease.source_id = $1
                  AND lease.lease_owner = $8
                  AND lease.lease_token = $9
                  AND lease.lease_until > db_clock.now
            )
            ON CONFLICT (source_id) DO UPDATE
            SET xml_bytes = EXCLUDED.xml_bytes,
                etag = EXCLUDED.etag,
                generated_at = EXCLUDED.generated_at,
                expires_at = EXCLUDED.expires_at,
                feed_revision = EXCLUDED.feed_revision,
                content_hash = EXCLUDED.content_hash,
                updated_at = EXCLUDED.updated_at
            WHERE feed_cache.feed_revision < EXCLUDED.feed_revision
               OR (
                   feed_cache.feed_revision = EXCLUDED.feed_revision
                   AND feed_cache.generated_at <= EXCLUDED.generated_at
               )
            RETURNING source_id, xml_bytes, etag, generated_at, expires_at,
                      feed_revision, content_hash, updated_at
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(candidate.xml_bytes())
        .bind(candidate.etag())
        .bind(candidate.generated_at())
        .bind(candidate.expires_at())
        .bind(revision)
        .bind(candidate.content_hash())
        .bind(owner)
        .bind(token.as_uuid())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(cache_storage_error)?;

        if let Some(cache_row) = cache_row {
            let released = release_build_lease(transaction, source_id, owner, token).await?;
            if !released {
                return Err(FeedCacheRepositoryError::LeaseLost { source_id });
            }
            return Ok(FeedCachePublishResult::Published(decode_cache(cache_row)?));
        }

        let lease_is_live = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM feed_build_leases
                WHERE source_id = $1
                  AND lease_owner = $2
                  AND lease_token = $3
                  AND lease_until > clock_timestamp()
            )
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(owner)
        .bind(token.as_uuid())
        .fetch_one(&mut **transaction)
        .await
        .map_err(cache_storage_error)?;
        if !lease_is_live {
            return Err(FeedCacheRepositoryError::LeaseLost { source_id });
        }
        let released = release_build_lease(transaction, source_id, owner, token).await?;
        if !released {
            return Err(FeedCacheRepositoryError::LeaseLost { source_id });
        }
        Ok(FeedCachePublishResult::ExistingCacheNewer)
    }
}

async fn release_build_lease(
    transaction: &mut Transaction<'_, Postgres>,
    source_id: SourceId,
    owner: &str,
    token: FeedBuildLeaseToken,
) -> Result<bool, FeedCacheRepositoryError> {
    let result = sqlx::query(
        r#"
        DELETE FROM feed_build_leases
        WHERE source_id = $1
          AND lease_owner = $2
          AND lease_token = $3
          AND lease_until > clock_timestamp()
        "#,
    )
    .bind(source_id.as_uuid())
    .bind(owner)
    .bind(token.as_uuid())
    .execute(&mut **transaction)
    .await
    .map_err(cache_storage_error)?;
    Ok(result.rows_affected() == 1)
}

fn decode_lease(row: PgRow) -> Result<FeedBuildLease, FeedBuildLeaseError> {
    let source_id = SourceId::from_uuid(row.try_get("source_id").map_err(storage_error)?);
    validate_source_id(source_id)?;
    Ok(FeedBuildLease::from_parts(
        source_id,
        row.try_get("lease_owner").map_err(storage_error)?,
        FeedBuildLeaseToken::from_uuid(row.try_get("lease_token").map_err(storage_error)?),
        row.try_get("lease_until").map_err(storage_error)?,
        row.try_get("heartbeat_at").map_err(storage_error)?,
    ))
}

fn decode_cache(row: PgRow) -> Result<FeedCache, FeedCacheRepositoryError> {
    let source_id = SourceId::from_uuid(row.try_get("source_id").map_err(cache_storage_error)?);
    validate_source_id_for_cache(source_id)?;
    let candidate = FeedCacheCandidate::from_parts(
        source_id,
        row.try_get("xml_bytes").map_err(cache_storage_error)?,
        row.try_get("etag").map_err(cache_storage_error)?,
        row.try_get("generated_at").map_err(cache_storage_error)?,
        row.try_get("expires_at").map_err(cache_storage_error)?,
        persisted_revision(
            row.try_get::<i64, _>("feed_revision")
                .map_err(cache_storage_error)?,
        )?,
        row.try_get("content_hash").map_err(cache_storage_error)?,
    );
    Ok(FeedCache::from_candidate(
        candidate,
        row.try_get("updated_at").map_err(cache_storage_error)?,
    ))
}

fn decode_cache_read(row: PgRow) -> Result<FeedCacheRead, FeedCacheRepositoryError> {
    let source_revision = persisted_revision(
        row.try_get::<i64, _>("source_feed_revision")
            .map_err(cache_storage_error)?,
    )?;
    let fresh = row.try_get("fresh").map_err(cache_storage_error)?;
    Ok(FeedCacheRead::from_parts(
        decode_cache(row)?,
        source_revision,
        fresh,
    ))
}

fn validate_candidate(
    candidate: &FeedCacheCandidate,
    owner: &str,
) -> Result<i64, FeedCacheRepositoryError> {
    validate_source_id_for_cache(candidate.source_id())?;
    if owner.trim().is_empty() {
        return Err(FeedCacheRepositoryError::EmptyOwner);
    }
    if candidate.xml_bytes().is_empty() {
        return Err(FeedCacheRepositoryError::EmptyXml);
    }
    if candidate.etag().trim().is_empty() {
        return Err(FeedCacheRepositoryError::EmptyEtag);
    }
    if candidate.content_hash().trim().is_empty() {
        return Err(FeedCacheRepositoryError::EmptyContentHash);
    }
    if candidate.expires_at() <= candidate.generated_at() {
        return Err(FeedCacheRepositoryError::InvalidExpiry);
    }
    i64::try_from(candidate.feed_revision().as_u64())
        .map_err(|_| FeedCacheRepositoryError::RevisionOutOfRange)
}

fn validate_source_id_for_cache(source_id: SourceId) -> Result<(), FeedCacheRepositoryError> {
    if source_id.as_uuid().is_nil() {
        Err(FeedCacheRepositoryError::InvalidSourceId)
    } else {
        Ok(())
    }
}

fn persisted_revision(value: i64) -> Result<FeedRevision, FeedCacheRepositoryError> {
    u64::try_from(value)
        .map(FeedRevision::from_u64)
        .map_err(|_| FeedCacheRepositoryError::RevisionOutOfRange)
}

fn job_transaction_error(error: JobRepositoryError) -> FeedCacheRepositoryError {
    FeedCacheRepositoryError::Storage(error.to_string())
}

fn cache_storage_error(error: impl fmt::Display) -> FeedCacheRepositoryError {
    FeedCacheRepositoryError::Storage(error.to_string())
}

fn validate_inputs(
    source_id: SourceId,
    owner: &str,
    lease_for: Duration,
) -> Result<(), FeedBuildLeaseError> {
    validate_source_and_owner(source_id, owner)?;
    if lease_for <= Duration::zero() {
        return Err(FeedBuildLeaseError::InvalidLeaseDuration);
    }
    Ok(())
}

fn validate_source_and_owner(source_id: SourceId, owner: &str) -> Result<(), FeedBuildLeaseError> {
    validate_source_id(source_id)?;
    if owner.trim().is_empty() {
        return Err(FeedBuildLeaseError::EmptyOwner);
    }
    Ok(())
}

fn validate_source_id(source_id: SourceId) -> Result<(), FeedBuildLeaseError> {
    if source_id.as_uuid().is_nil() {
        Err(FeedBuildLeaseError::InvalidSourceId)
    } else {
        Ok(())
    }
}

fn lease_milliseconds(lease_for: Duration) -> Result<i64, FeedBuildLeaseError> {
    let milliseconds = lease_for.num_milliseconds();
    if milliseconds <= 0 {
        Err(FeedBuildLeaseError::InvalidLeaseDuration)
    } else {
        Ok(milliseconds)
    }
}

fn storage_error(error: impl fmt::Display) -> FeedBuildLeaseError {
    FeedBuildLeaseError::Storage(error.to_string())
}

#[derive(Debug)]
struct MemoryState {
    now: DateTime<Utc>,
    leases: HashMap<SourceId, FeedBuildLease>,
}

/// Deterministic in-memory feed-build lease repository for unit tests.
///
/// It uses Tokio's non-poisoning mutex and an injectable clock. It models
/// ownership, expiry, takeover, and fencing but provides no cross-process
/// coordination; production code must use [`PostgresFeedBuildLeaseRepository`].
#[derive(Clone)]
pub struct MemoryFeedBuildLeaseRepository {
    state: Arc<Mutex<MemoryState>>,
}

impl fmt::Debug for MemoryFeedBuildLeaseRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryFeedBuildLeaseRepository")
            .finish_non_exhaustive()
    }
}

impl MemoryFeedBuildLeaseRepository {
    /// Creates an empty repository with the supplied deterministic time.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryState {
                now,
                leases: HashMap::new(),
            })),
        }
    }

    /// Advances the test clock used for expiry and takeover decisions.
    pub async fn set_now(&self, now: DateTime<Utc>) {
        self.state.lock().await.now = now;
    }
}

impl FeedBuildLeaseRepository for MemoryFeedBuildLeaseRepository {
    async fn acquire_build(
        &self,
        source_id: SourceId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<FeedBuildLease>, FeedBuildLeaseError> {
        validate_inputs(source_id, owner, lease_for)?;
        let mut state = self.state.lock().await;
        if state
            .leases
            .get(&source_id)
            .is_some_and(|lease| lease.is_live_at(state.now))
        {
            return Ok(None);
        }
        let lease_until = state
            .now
            .checked_add_signed(lease_for)
            .ok_or(FeedBuildLeaseError::InvalidLeaseDuration)?;
        let lease = FeedBuildLease::from_parts(
            source_id,
            owner.to_owned(),
            FeedBuildLeaseToken::new(),
            lease_until,
            state.now,
        );
        state.leases.insert(source_id, lease.clone());
        Ok(Some(lease))
    }

    async fn heartbeat_build(
        &self,
        source_id: SourceId,
        owner: &str,
        token: FeedBuildLeaseToken,
        lease_for: Duration,
    ) -> Result<FeedBuildLease, FeedBuildLeaseError> {
        validate_inputs(source_id, owner, lease_for)?;
        let mut state = self.state.lock().await;
        let now = state.now;
        let current = state
            .leases
            .get(&source_id)
            .filter(|lease| lease.owner() == owner)
            .filter(|lease| lease.token() == token)
            .filter(|lease| lease.is_live_at(now))
            .ok_or(FeedBuildLeaseError::LeaseLost { source_id })?;
        let lease_until = now
            .checked_add_signed(lease_for)
            .ok_or(FeedBuildLeaseError::InvalidLeaseDuration)?;
        let renewed =
            FeedBuildLease::from_parts(source_id, owner.to_owned(), token, lease_until, now);
        debug_assert_eq!(current.source_id(), source_id);
        state.leases.insert(source_id, renewed.clone());
        Ok(renewed)
    }

    async fn release_build(
        &self,
        source_id: SourceId,
        owner: &str,
        token: FeedBuildLeaseToken,
    ) -> Result<(), FeedBuildLeaseError> {
        validate_source_and_owner(source_id, owner)?;
        let mut state = self.state.lock().await;
        let now = state.now;
        let matches_live_lease = state.leases.get(&source_id).is_some_and(|lease| {
            lease.owner() == owner && lease.token() == token && lease.is_live_at(now)
        });
        if matches_live_lease {
            state.leases.remove(&source_id);
            Ok(())
        } else {
            Err(FeedBuildLeaseError::LeaseLost { source_id })
        }
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

    fn source_id() -> SourceId {
        SourceId::from_uuid(Uuid::from_u128(1))
    }

    #[tokio::test]
    async fn serializes_builders_and_fences_expired_takeover() {
        let repository = MemoryFeedBuildLeaseRepository::new(at(0));
        let source_id = source_id();
        let first = repository
            .acquire_build(source_id, "builder-a", Duration::seconds(30))
            .await
            .unwrap()
            .expect("first builder should acquire the source");

        assert!(repository
            .acquire_build(source_id, "builder-b", Duration::seconds(30))
            .await
            .unwrap()
            .is_none());
        let renewed = repository
            .heartbeat_build(source_id, "builder-a", first.token(), Duration::seconds(40))
            .await
            .unwrap();
        assert_eq!(renewed.heartbeat_at(), at(0));
        assert_eq!(renewed.lease_until(), at(40));

        assert!(matches!(
            repository
                .heartbeat_build(source_id, "builder-b", first.token(), Duration::seconds(30),)
                .await,
            Err(FeedBuildLeaseError::LeaseLost { .. })
        ));

        repository.set_now(at(41)).await;
        let takeover = repository
            .acquire_build(source_id, "builder-b", Duration::seconds(30))
            .await
            .unwrap()
            .expect("expired build should be recoverable");
        assert_ne!(takeover.token(), first.token());
        assert_eq!(takeover.owner(), "builder-b");
        assert!(matches!(
            repository
                .release_build(source_id, "builder-a", renewed.token())
                .await,
            Err(FeedBuildLeaseError::LeaseLost { .. })
        ));
        repository
            .release_build(source_id, "builder-b", takeover.token())
            .await
            .unwrap();
        assert!(repository
            .acquire_build(source_id, "builder-a", Duration::seconds(30))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn rejects_invalid_lease_inputs() {
        let repository = MemoryFeedBuildLeaseRepository::new(at(0));
        assert!(matches!(
            repository
                .acquire_build(
                    SourceId::from_uuid(Uuid::nil()),
                    "builder-a",
                    Duration::seconds(30),
                )
                .await,
            Err(FeedBuildLeaseError::InvalidSourceId)
        ));
        assert!(matches!(
            repository
                .acquire_build(source_id(), " ", Duration::seconds(30))
                .await,
            Err(FeedBuildLeaseError::EmptyOwner)
        ));
        assert!(matches!(
            repository
                .acquire_build(source_id(), "builder-a", Duration::zero())
                .await,
            Err(FeedBuildLeaseError::InvalidLeaseDuration)
        ));
    }

    #[test]
    fn rejects_invalid_cache_candidates_before_opening_a_transaction() {
        let source_id = source_id();
        let valid = || {
            FeedCacheCandidate::from_parts(
                source_id,
                b"<rss/>".to_vec(),
                "etag".to_owned(),
                at(10),
                at(40),
                FeedRevision::from_u64(1),
                "hash".to_owned(),
            )
        };

        assert!(matches!(
            validate_candidate(
                &FeedCacheCandidate::from_parts(
                    source_id,
                    Vec::new(),
                    "etag".to_owned(),
                    at(10),
                    at(40),
                    FeedRevision::from_u64(1),
                    "hash".to_owned(),
                ),
                "builder",
            ),
            Err(FeedCacheRepositoryError::EmptyXml)
        ));
        assert!(matches!(
            validate_candidate(
                &FeedCacheCandidate::from_parts(
                    source_id,
                    b"<rss/>".to_vec(),
                    "etag".to_owned(),
                    at(10),
                    at(10),
                    FeedRevision::from_u64(1),
                    "hash".to_owned(),
                ),
                "builder",
            ),
            Err(FeedCacheRepositoryError::InvalidExpiry)
        ));
        assert_eq!(validate_candidate(&valid(), "builder").unwrap(), 1);
        assert!(matches!(
            validate_candidate(&valid(), " "),
            Err(FeedCacheRepositoryError::EmptyOwner)
        ));
        assert!(matches!(
            validate_candidate(
                &FeedCacheCandidate::from_parts(
                    source_id,
                    b"<rss/>".to_vec(),
                    "etag".to_owned(),
                    at(10),
                    at(40),
                    FeedRevision::from_u64(i64::MAX as u64 + 1),
                    "hash".to_owned(),
                ),
                "builder",
            ),
            Err(FeedCacheRepositoryError::RevisionOutOfRange)
        ));
    }
}
