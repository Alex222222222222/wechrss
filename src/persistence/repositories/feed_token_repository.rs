//! PostgreSQL persistence for public feed-token lifecycle.
//!
//! The repository stores one current token digest per source. It never
//! accepts or returns the raw bearer token: [`FeedTokenService`](crate::application::feed_token_service::FeedTokenService)
//! generates and validates that value, then passes only a [`FeedTokenHash`]
//! here. Replacing a row rotates a token and invalidates the previous one;
//! revocation makes lookup return no source without deleting audit timestamps.
//!
//! Responsibilities: resolve an active digest to a source, atomically replace
//! or rotate a source's digest, revoke a source token, and map foreign-key and
//! storage failures into typed errors. Non-responsibilities include generating
//! entropy, exposing raw secrets, deciding feed authorization, rendering RSS,
//! or enqueueing rebuild jobs.
//!
//! The initial schema has one `feed_tokens` row per source with a unique
//! `BYTEA` digest, `created_at`, `rotated_at`, and nullable `revoked_at`.
//! Rotation uses one upsert and server timestamps, so all replicas observe the
//! same current capability. Source deletion cascades the row; an old token
//! therefore cannot resolve to a deleted source. Lookup is a short pool read
//! and does not hold a transaction while the feed cache is read.
//!
//! RSS-cache interaction: resolution returns only [`SourceId`]. The caller
//! must pass that id to the cache-first feed service, which serves fresh or
//! stale cached XML and may enqueue a deduplicated rebuild. Token rotation or
//! revocation does not alter feed bytes or cache revision.

use std::fmt;

use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::domain::{feed_token::FeedTokenHash, source::SourceId};

/// Errors returned by the feed-token repository.
#[derive(Debug, Error)]
pub enum FeedTokenRepositoryError {
    /// A nil UUID cannot identify a source.
    #[error("source id must not be nil")]
    InvalidSourceId,
    /// The source foreign key was missing while issuing a token.
    #[error("source {source_id} was not found")]
    SourceNotFound {
        /// Missing source identifier.
        source_id: SourceId,
    },
    /// The backing PostgreSQL operation failed.
    #[error("feed-token repository storage error: {0}")]
    Storage(String),
}

/// Storage port for active public feed-token capabilities.
#[allow(async_fn_in_trait)]
pub trait FeedTokenRepository: Send + Sync {
    /// Resolves an active digest to its source, if one exists.
    async fn find_source(
        &self,
        token_hash: FeedTokenHash,
    ) -> Result<Option<SourceId>, FeedTokenRepositoryError>;

    /// Creates or replaces the current token for a source.
    async fn replace(
        &self,
        source_id: SourceId,
        token_hash: FeedTokenHash,
    ) -> Result<(), FeedTokenRepositoryError>;

    /// Revokes the current token and reports whether a row was changed.
    async fn revoke(&self, source_id: SourceId) -> Result<bool, FeedTokenRepositoryError>;
}

/// PostgreSQL feed-token repository backed by the shared pool.
#[derive(Clone)]
pub struct PostgresFeedTokenRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresFeedTokenRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresFeedTokenRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresFeedTokenRepository {
    /// Creates a repository backed by the configured PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl FeedTokenRepository for PostgresFeedTokenRepository {
    async fn find_source(
        &self,
        token_hash: FeedTokenHash,
    ) -> Result<Option<SourceId>, FeedTokenRepositoryError> {
        let row = sqlx::query(
            "SELECT source_id FROM feed_tokens WHERE token_hash = $1 AND revoked_at IS NULL",
        )
        .bind(token_hash.as_bytes().to_vec())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(|row| {
            row.try_get("source_id")
                .map(SourceId::from_uuid)
                .map_err(storage_error)
        })
        .transpose()
    }

    async fn replace(
        &self,
        source_id: SourceId,
        token_hash: FeedTokenHash,
    ) -> Result<(), FeedTokenRepositoryError> {
        validate_source_id(source_id)?;
        sqlx::query(
            r#"
            INSERT INTO feed_tokens (
                source_id, token_hash, created_at, rotated_at, revoked_at
            )
            VALUES ($1, $2, clock_timestamp(), clock_timestamp(), NULL)
            ON CONFLICT (source_id) DO UPDATE
            SET token_hash = EXCLUDED.token_hash,
                rotated_at = EXCLUDED.rotated_at,
                revoked_at = NULL
            "#,
        )
        .bind(source_id.as_uuid())
        .bind(token_hash.as_bytes().to_vec())
        .execute(&self.pool)
        .await
        .map_err(|error| {
            let is_missing_source = error
                .as_database_error()
                .is_some_and(|database| database.code().as_deref() == Some("23503"));
            if is_missing_source {
                FeedTokenRepositoryError::SourceNotFound { source_id }
            } else {
                storage_error(error)
            }
        })?;
        Ok(())
    }

    async fn revoke(&self, source_id: SourceId) -> Result<bool, FeedTokenRepositoryError> {
        validate_source_id(source_id)?;
        let result = sqlx::query(
            "UPDATE feed_tokens SET revoked_at = clock_timestamp() WHERE source_id = $1 AND revoked_at IS NULL",
        )
        .bind(source_id.as_uuid())
        .execute(&self.pool)
        .await
        .map_err(storage_error)?;
        Ok(result.rows_affected() == 1)
    }
}

fn validate_source_id(source_id: SourceId) -> Result<(), FeedTokenRepositoryError> {
    if source_id.as_uuid().is_nil() {
        Err(FeedTokenRepositoryError::InvalidSourceId)
    } else {
        Ok(())
    }
}

fn storage_error(error: impl fmt::Display) -> FeedTokenRepositoryError {
    FeedTokenRepositoryError::Storage(error.to_string())
}
