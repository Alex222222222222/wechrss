//! PostgreSQL article persistence and source-scoped RSS reads.
//!
//! This repository is the first executable article boundary. It stores one
//! normalized row per `(source_id, review_id)`, updates list/detail metadata
//! idempotently, and returns whether an upsert changed an RSS-visible field.
//! The source-scoped read is deliberately shaped for the RSS renderer: stable
//! publication ordering and the caller-provided item limit are enforced in
//! SQL, so application code does not need to load an unbounded article set.
//!
//! Responsibilities:
//!
//! - own the `articles` SQL and map rows through domain validation;
//! - enforce composite article identity and source foreign-key ownership;
//! - preserve detail URLs as verified public WeChat destinations;
//! - allocate an observation version before upstream work starts;
//! - refresh observation timestamps without bumping the feed revision for a
//!   feed-invisible no-op; and
//! - lock an existing article observation before a caller performs a
//!   multi-step representation rewrite;
//! - expose transaction-scoped upserts for the shared `UnitOfWork`.
//!
//! Non-responsibilities: fetching WeRead or WeChat pages, sanitizing HTML,
//! downloading assets, rendering RSS XML, or advancing `sources.feed_revision`.
//! The caller must sanitize `content_html` before persistence and must bump the
//! source revision in the same unit of work when the returned result reports a
//! feed-visible change. Asset download and relationship persistence belong to
//! the transaction-scoped asset view; this repository only stores the final
//! article representation.
//!
//! Upsert behavior is idempotent. Existing rows are locked before comparison;
//! observations older than the stored monotonic version are ignored so a
//! delayed replica cannot regress content or metadata. Newer partial
//! observations are merged so missing detail fields cannot erase known URLs or
//! archived HTML, changed feed fields are replaced, and an observation-only
//! refresh updates only `fetched_at` and `updated_at`. A concurrent insert is
//! handled by the unique-key conflict path and then re-read under the same
//! transaction. This avoids using a timestamp as an identity or creating
//! duplicate articles.
//!
//! PostgreSQL/high-availability considerations: all mutations in this module
//! use the `UnitOfWork` transaction owned by PostgreSQL. A pool-backed read is
//! safe across replicas, and the `(source_id, review_id)` primary key provides
//! durable deduplication. The repository does not use a process-local mutex or
//! cache. Feed revision changes and final cache publication must still be
//! coordinated by the application transaction boundary.
//!
//! RSS-cache interaction: article list reads provide normalized values to a
//! future database-only feed rebuild. The repository never writes `feed_cache`
//! itself. A changed upsert should cause the caller to advance the source
//! revision; a cache candidate for that exact revision can then be published
//! through the existing fenced feed-cache view.

use std::fmt;

use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use thiserror::Error;

use crate::domain::article::{
    feed_visible_change, Article, ArticleError, ArticleObservationVersion, ArticleParts,
    ArticleUpsertResult, NewArticle,
};
use crate::domain::source::{SourceId, VerifiedWechatArticleUrl};

use super::job_repository::{JobRepositoryError, PostgresJobTransaction};

const ARTICLE_COLUMNS: &str = "source_id, review_id, title, author, summary, cover_url, original_url, published_at, content_html, content_hash, observation_version, fetched_at, created_at, updated_at";

/// Errors returned by article repositories.
#[derive(Debug, Error)]
pub enum ArticleRepositoryError {
    /// An article value failed domain validation.
    #[error(transparent)]
    Domain(#[from] ArticleError),
    /// The source foreign key was missing during an insert.
    #[error("source {source_id} was not found for article persistence")]
    SourceNotFound {
        /// Missing owning source.
        source_id: SourceId,
    },
    /// A source-scoped article read was given an unusable limit.
    #[error("article list limit must be positive")]
    InvalidLimit,
    /// The database row disappeared during a concurrent upsert retry.
    #[error("article {source_id}/{review_id} disappeared during upsert")]
    MissingAfterConflict {
        /// Article source.
        source_id: SourceId,
        /// Stable article identity.
        review_id: String,
    },
    /// The backing PostgreSQL operation failed.
    #[error("article repository storage error: {0}")]
    Storage(String),
}

/// Pool-backed article reads and observation-version allocation.
#[async_trait::async_trait]
pub trait ArticleRepository: Send + Sync {
    /// Finds one article by its composite source/`review_id` identity.
    async fn find(
        &self,
        source_id: SourceId,
        review_id: &str,
    ) -> Result<Option<Article>, ArticleRepositoryError>;

    /// Returns at most `limit` articles in deterministic RSS order.
    async fn list_for_feed(
        &self,
        source_id: SourceId,
        limit: u32,
    ) -> Result<Vec<Article>, ArticleRepositoryError>;

    /// Reserves a monotonic version before upstream acquisition starts.
    async fn allocate_observation_version(
        &self,
    ) -> Result<ArticleObservationVersion, ArticleRepositoryError>;
}

/// PostgreSQL article reader backed by the shared pool.
#[derive(Clone)]
pub struct PostgresArticleRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresArticleRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresArticleRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresArticleRepository {
    /// Creates an article repository over the configured PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl ArticleRepository for PostgresArticleRepository {
    async fn find(
        &self,
        source_id: SourceId,
        review_id: &str,
    ) -> Result<Option<Article>, ArticleRepositoryError> {
        let review_id = validate_key(source_id, review_id)?;
        sqlx::query(&format!(
            "SELECT {ARTICLE_COLUMNS} FROM articles WHERE source_id = $1 AND review_id = $2"
        ))
        .bind(source_id.as_uuid())
        .bind(review_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(decode_article)
        .transpose()
    }

    async fn list_for_feed(
        &self,
        source_id: SourceId,
        limit: u32,
    ) -> Result<Vec<Article>, ArticleRepositoryError> {
        validate_source_id(source_id)?;
        if limit == 0 {
            return Err(ArticleRepositoryError::InvalidLimit);
        }
        let rows = sqlx::query(&format!(
            "SELECT {ARTICLE_COLUMNS} FROM articles WHERE source_id = $1 ORDER BY published_at DESC, review_id ASC LIMIT $2"
        ))
        .bind(source_id.as_uuid())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.into_iter().map(decode_article).collect()
    }

    async fn allocate_observation_version(
        &self,
    ) -> Result<ArticleObservationVersion, ArticleRepositoryError> {
        let value: i64 =
            sqlx::query_scalar("SELECT nextval('article_observation_version_seq'::regclass)")
                .fetch_one(&self.pool)
                .await
                .map_err(storage_error)?;
        observation_version_from_i64(value)
    }
}

/// Operations on article rows inside the shared application transaction.
#[async_trait::async_trait]
pub trait ArticleTransactionRepository {
    /// Locks one article row for an observation without changing its content.
    ///
    /// Asset-enabled synchronization uses this read-before-write boundary to
    /// decide whether an observation is stale while keeping the previously
    /// published representation available for the final upsert. The lock is
    /// held by the surrounding unit of work until commit or rollback.
    async fn find_for_update(
        &mut self,
        source_id: SourceId,
        review_id: &str,
    ) -> Result<Option<Article>, ArticleRepositoryError>;

    /// Inserts or updates one article and reports RSS-visible change.
    async fn upsert(
        &mut self,
        article: NewArticle,
    ) -> Result<ArticleUpsertResult, ArticleRepositoryError>;
}

/// Transaction-scoped PostgreSQL article view owned by
/// [`crate::persistence::unit_of_work::UnitOfWork`].
pub struct PostgresArticleTransaction<'borrow, 'pool> {
    job_transaction: &'borrow mut PostgresJobTransaction<'pool>,
}

impl<'borrow, 'pool> PostgresArticleTransaction<'borrow, 'pool> {
    /// Creates an article view over the unit-of-work transaction.
    pub(crate) fn new(job_transaction: &'borrow mut PostgresJobTransaction<'pool>) -> Self {
        Self { job_transaction }
    }

    fn transaction(&mut self) -> Result<&mut Transaction<'pool, Postgres>, ArticleRepositoryError> {
        self.job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)
    }
}

#[async_trait::async_trait]
impl ArticleTransactionRepository for PostgresArticleTransaction<'_, '_> {
    async fn find_for_update(
        &mut self,
        source_id: SourceId,
        review_id: &str,
    ) -> Result<Option<Article>, ArticleRepositoryError> {
        let review_id = validate_key(source_id, review_id)?;
        sqlx::query(&format!(
            "SELECT {ARTICLE_COLUMNS} FROM articles WHERE source_id = $1 AND review_id = $2 FOR UPDATE"
        ))
        .bind(source_id.as_uuid())
        .bind(review_id)
        .fetch_optional(&mut **self.transaction()?)
        .await
        .map_err(storage_error)?
        .map(decode_article)
        .transpose()
    }

    async fn upsert(
        &mut self,
        article: NewArticle,
    ) -> Result<ArticleUpsertResult, ArticleRepositoryError> {
        let article = article.normalize()?;
        let source_id = article.source_id;
        let review_id = article.review_id.as_str();
        let observation_version = observation_version_as_i64(article.observation_version)?;
        let transaction = self.transaction()?;

        loop {
            let existing = sqlx::query(&format!(
                "SELECT {ARTICLE_COLUMNS} FROM articles WHERE source_id = $1 AND review_id = $2 FOR UPDATE"
            ))
            .bind(source_id.as_uuid())
            .bind(review_id)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(storage_error)?;

            if let Some(existing) = existing {
                let current = decode_article(existing)?;
                if article.observation_version < current.observation_version() {
                    // A slower replica can finish an older observation after a
                    // newer one has committed. Preserve the newer row rather
                    // than allowing out-of-order acquisition to regress RSS
                    // content. `fetched_at` is deliberately not used for this
                    // comparison because it records completion time.
                    return Ok(ArticleUpsertResult::new(current, false, false));
                }
                let merged = current.merge_observation(&article);
                let changed = feed_visible_change(&current, &merged);
                let row = if changed {
                    update_changed_article(transaction, &merged).await?
                } else {
                    update_observation(transaction, &merged).await?
                };
                return Ok(ArticleUpsertResult::new(
                    decode_article(row)?,
                    changed,
                    false,
                ));
            }

            let row = sqlx::query(&format!(
                "INSERT INTO articles (source_id, review_id, title, author, summary, cover_url, original_url, published_at, content_html, content_hash, observation_version, fetched_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT (source_id, review_id) DO NOTHING RETURNING {ARTICLE_COLUMNS}"
            ))
            .bind(source_id.as_uuid())
            .bind(&article.review_id)
            .bind(&article.title)
            .bind(article.author.as_deref())
            .bind(article.summary.as_deref())
            .bind(article.cover_url.as_deref())
            .bind(article.original_url.as_ref().map(VerifiedWechatArticleUrl::as_str))
            .bind(article.published_at)
            .bind(&article.content_html)
            .bind(article.content_hash.as_deref())
            .bind(observation_version)
            .bind(article.fetched_at)
            .fetch_optional(&mut **transaction)
            .await
            .map_err(|error| map_insert_error(error, source_id))?;

            if let Some(row) = row {
                return Ok(ArticleUpsertResult::new(decode_article(row)?, true, true));
            }

            // A concurrent transaction won the primary-key race. Its row is
            // visible to the next statement in READ COMMITTED and will be
            // handled by the locked comparison path above.
        }
    }
}

async fn update_changed_article(
    transaction: &mut Transaction<'_, Postgres>,
    article: &NewArticle,
) -> Result<PgRow, ArticleRepositoryError> {
    let observation_version = observation_version_as_i64(article.observation_version)?;
    sqlx::query(&format!(
        "UPDATE articles SET title = $3, author = $4, summary = $5, cover_url = $6, original_url = $7, published_at = $8, content_html = $9, content_hash = $10, observation_version = $11, fetched_at = $12, updated_at = clock_timestamp() WHERE source_id = $1 AND review_id = $2 RETURNING {ARTICLE_COLUMNS}"
    ))
    .bind(article.source_id.as_uuid())
    .bind(&article.review_id)
    .bind(&article.title)
    .bind(article.author.as_deref())
    .bind(article.summary.as_deref())
    .bind(article.cover_url.as_deref())
    .bind(article.original_url.as_ref().map(VerifiedWechatArticleUrl::as_str))
    .bind(article.published_at)
    .bind(&article.content_html)
    .bind(article.content_hash.as_deref())
    .bind(observation_version)
    .bind(article.fetched_at)
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)
}

async fn update_observation(
    transaction: &mut Transaction<'_, Postgres>,
    article: &NewArticle,
) -> Result<PgRow, ArticleRepositoryError> {
    let observation_version = observation_version_as_i64(article.observation_version)?;
    sqlx::query(&format!(
        "UPDATE articles SET observation_version = $3, fetched_at = $4, updated_at = clock_timestamp() WHERE source_id = $1 AND review_id = $2 RETURNING {ARTICLE_COLUMNS}"
    ))
    .bind(article.source_id.as_uuid())
    .bind(&article.review_id)
    .bind(observation_version)
    .bind(article.fetched_at)
    .fetch_one(&mut **transaction)
    .await
    .map_err(storage_error)
}

fn decode_article(row: PgRow) -> Result<Article, ArticleRepositoryError> {
    let original_url = row
        .try_get::<Option<String>, _>("original_url")
        .map_err(storage_error)?
        .map(|value| value.parse::<VerifiedWechatArticleUrl>())
        .transpose()
        .map_err(|_| ArticleRepositoryError::Domain(ArticleError::InvalidOriginalUrl))?;

    Article::from_parts(ArticleParts {
        source_id: SourceId::from_uuid(row.try_get("source_id").map_err(storage_error)?),
        review_id: row.try_get("review_id").map_err(storage_error)?,
        title: row.try_get("title").map_err(storage_error)?,
        author: row.try_get("author").map_err(storage_error)?,
        summary: row.try_get("summary").map_err(storage_error)?,
        cover_url: row.try_get("cover_url").map_err(storage_error)?,
        original_url,
        published_at: row.try_get("published_at").map_err(storage_error)?,
        content_html: row.try_get("content_html").map_err(storage_error)?,
        content_hash: row.try_get("content_hash").map_err(storage_error)?,
        observation_version: observation_version_from_i64(
            row.try_get("observation_version").map_err(storage_error)?,
        )?,
        fetched_at: row.try_get("fetched_at").map_err(storage_error)?,
        created_at: row.try_get("created_at").map_err(storage_error)?,
        updated_at: row.try_get("updated_at").map_err(storage_error)?,
    })
    .map_err(ArticleRepositoryError::Domain)
}

fn observation_version_from_i64(
    value: i64,
) -> Result<ArticleObservationVersion, ArticleRepositoryError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .map(ArticleObservationVersion::from_u64)
        .ok_or(ArticleRepositoryError::Domain(
            ArticleError::InvalidObservationVersion,
        ))
}

fn observation_version_as_i64(
    value: ArticleObservationVersion,
) -> Result<i64, ArticleRepositoryError> {
    i64::try_from(value.as_u64())
        .map_err(|_| ArticleRepositoryError::Domain(ArticleError::InvalidObservationVersion))
}

fn validate_key(source_id: SourceId, review_id: &str) -> Result<String, ArticleRepositoryError> {
    validate_source_id(source_id)?;
    let review_id = review_id.trim();
    if review_id.is_empty() {
        return Err(ArticleRepositoryError::Domain(ArticleError::EmptyReviewId));
    }
    Ok(review_id.to_owned())
}

fn validate_source_id(source_id: SourceId) -> Result<(), ArticleRepositoryError> {
    if source_id.as_uuid().is_nil() {
        Err(ArticleRepositoryError::Domain(
            ArticleError::InvalidSourceId,
        ))
    } else {
        Ok(())
    }
}

fn map_insert_error(error: sqlx::Error, source_id: SourceId) -> ArticleRepositoryError {
    if let sqlx::Error::Database(database_error) = &error {
        if database_error.constraint() == Some("articles_source_id_fkey") {
            return ArticleRepositoryError::SourceNotFound { source_id };
        }
    }
    storage_error(error)
}

fn job_transaction_error(error: JobRepositoryError) -> ArticleRepositoryError {
    ArticleRepositoryError::Storage(error.to_string())
}

fn storage_error(error: impl fmt::Display) -> ArticleRepositoryError {
    ArticleRepositoryError::Storage(error.to_string())
}
