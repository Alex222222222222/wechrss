//! PostgreSQL synchronization-run persistence.
//!
//! This repository stores one durable audit row for each source-sync
//! execution. It is intentionally not a queue: job ownership, retries, and
//! leases remain in `jobs`, while this module records what a worker observed
//! and how the attempt ended.
//!
//! Responsibilities:
//!
//! - insert a validated `running` row tied to a source and optional job, with
//!   safe retries for the same run identity;
//! - finish exactly one running row with typed outcome, counters, and safe
//!   failure details;
//! - expose source-scoped history in deterministic newest-first order; and
//! - decode all persisted enum/counter values through domain validation.
//!
//! Non-responsibilities: classifying raw browser errors, retry/backoff policy,
//! source scheduling gates, feed revision mutation, RSS rendering, or secret
//! storage. The application chooses the outcome and must pass a diagnostic
//! summary that contains no credentials, URLs with secrets, or raw payloads.
//!
//! Transaction behavior: start and finish operations use the transaction
//! borrowed from `UnitOfWork`. A successful synchronization can therefore
//! finish its run atomically with article changes, source feed revision, cache
//! publication, and fenced job completion. A failure/deferred record can be
//! committed in a short unit of work after upstream work has stopped.
//!
//! PostgreSQL/high-availability considerations: the run primary key prevents
//! duplicate audit rows for one run id, while a retry with the same source,
//! job, and start timestamp returns the existing row. Reusing an id for a
//! different run is rejected. `finish` locks the row before applying the
//! domain transition. A stale worker cannot finish a run already finished by
//! another path. Job lease/fencing verification remains a future UnitOfWork
//! operation and is not replaced by this run row.
//!
//! RSS-cache interaction: `feed_revision` is optional metadata recording the
//! revision published by the same final transaction. This repository never
//! decides whether a feed is stale and never writes `feed_cache` directly.

use std::fmt;

use sqlx::{postgres::PgRow, PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::{
    source::{FeedRevision, SourceId},
    sync::{
        NewSyncRun, SyncError, SyncFailure, SyncFailureClass, SyncOutcome, SyncRun,
        SyncRunCompletion, SyncRunParts, SyncStats,
    },
};

use super::job_repository::{JobRepositoryError, PostgresJobTransaction};

const SYNC_RUN_COLUMNS: &str = "id, source_id, job_id, outcome, articles_seen, articles_created, articles_updated, articles_failed, archived_articles, archived_assets, failure_class, failure_message, feed_revision, started_at, finished_at, created_at, updated_at";

/// Errors returned by synchronization-run repositories.
#[derive(Debug, Error)]
pub enum SyncRunRepositoryError {
    /// A sync-run domain value failed validation.
    #[error(transparent)]
    Domain(#[from] SyncError),
    /// The run's owning source does not exist.
    #[error("source {source_id} was not found for sync run persistence")]
    SourceNotFound {
        /// Missing source identifier.
        source_id: SourceId,
    },
    /// The optional correlated queue job does not exist.
    #[error("job {job_id} was not found for sync run persistence")]
    JobNotFound {
        /// Missing job identifier.
        job_id: Uuid,
    },
    /// The requested run does not exist.
    #[error("sync run {run_id} was not found")]
    NotFound {
        /// Missing run identifier.
        run_id: Uuid,
    },
    /// A start retry reused a run id for a different run identity.
    #[error("sync run {run_id} already exists with a different identity")]
    StartConflict {
        /// Conflicting run identifier.
        run_id: Uuid,
    },
    /// A source history read was given an unusable limit.
    #[error("sync run history limit must be positive")]
    InvalidLimit,
    /// A persisted counter is outside the domain's u32 range.
    #[error("sync run counter {field} is outside the supported range: {value}")]
    InvalidCounter {
        /// Counter column name.
        field: &'static str,
        /// Persisted value.
        value: i64,
    },
    /// A persisted feed revision is outside the domain range.
    #[error("sync run feed revision is invalid: {value}")]
    InvalidFeedRevision {
        /// Persisted revision.
        value: i64,
    },
    /// PostgreSQL could not complete the operation.
    #[error("sync run repository storage error: {0}")]
    Storage(String),
}

/// Pool-backed synchronization-run reads.
#[allow(async_fn_in_trait)]
pub trait SyncRunRepository: Send + Sync {
    /// Finds one run by its durable identifier.
    async fn find(&self, run_id: Uuid) -> Result<Option<SyncRun>, SyncRunRepositoryError>;

    /// Returns source history in newest-started-first order.
    async fn list_for_source(
        &self,
        source_id: SourceId,
        limit: u32,
    ) -> Result<Vec<SyncRun>, SyncRunRepositoryError>;
}

/// PostgreSQL synchronization-run reader backed by the shared pool.
#[derive(Clone)]
pub struct PostgresSyncRunRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresSyncRunRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresSyncRunRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresSyncRunRepository {
    /// Creates a synchronization-run reader over the configured pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl SyncRunRepository for PostgresSyncRunRepository {
    async fn find(&self, run_id: Uuid) -> Result<Option<SyncRun>, SyncRunRepositoryError> {
        if run_id.is_nil() {
            return Err(SyncError::InvalidId.into());
        }
        sqlx::query(&format!(
            "SELECT {SYNC_RUN_COLUMNS} FROM sync_runs WHERE id = $1"
        ))
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?
        .map(decode_sync_run)
        .transpose()
    }

    async fn list_for_source(
        &self,
        source_id: SourceId,
        limit: u32,
    ) -> Result<Vec<SyncRun>, SyncRunRepositoryError> {
        validate_source_id(source_id)?;
        if limit == 0 {
            return Err(SyncRunRepositoryError::InvalidLimit);
        }
        let rows = sqlx::query(&format!(
            "SELECT {SYNC_RUN_COLUMNS} FROM sync_runs WHERE source_id = $1 ORDER BY started_at DESC, id DESC LIMIT $2"
        ))
        .bind(source_id.as_uuid())
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(storage_error)?;
        rows.into_iter().map(decode_sync_run).collect()
    }
}

/// Operations on synchronization-run rows inside `UnitOfWork`.
#[allow(async_fn_in_trait)]
pub trait SyncRunTransactionRepository {
    /// Inserts a new running record.
    async fn start(&mut self, spec: NewSyncRun) -> Result<SyncRun, SyncRunRepositoryError>;

    /// Finishes an existing running record exactly once.
    async fn finish(
        &mut self,
        run_id: Uuid,
        completion: SyncRunCompletion,
    ) -> Result<SyncRun, SyncRunRepositoryError>;
}

/// Transaction-scoped PostgreSQL synchronization-run view owned by `UnitOfWork`.
pub struct PostgresSyncRunTransaction<'borrow, 'pool> {
    job_transaction: &'borrow mut PostgresJobTransaction<'pool>,
}

impl<'borrow, 'pool> PostgresSyncRunTransaction<'borrow, 'pool> {
    /// Creates a run view over the unit-of-work transaction.
    pub(crate) fn new(job_transaction: &'borrow mut PostgresJobTransaction<'pool>) -> Self {
        Self { job_transaction }
    }

    fn transaction(&mut self) -> Result<&mut Transaction<'pool, Postgres>, SyncRunRepositoryError> {
        self.job_transaction
            .transaction_mut()
            .map_err(job_transaction_error)
    }
}

impl SyncRunTransactionRepository for PostgresSyncRunTransaction<'_, '_> {
    async fn start(&mut self, spec: NewSyncRun) -> Result<SyncRun, SyncRunRepositoryError> {
        let run = SyncRun::start(spec)?;
        let transaction = self.transaction()?;
        let inserted = sqlx::query(&format!(
            "INSERT INTO sync_runs (id, source_id, job_id, outcome, articles_seen, articles_created, articles_updated, articles_failed, archived_articles, archived_assets, failure_class, failure_message, feed_revision, started_at, finished_at, created_at, updated_at) VALUES ($1, $2, $3, $4, 0, 0, 0, 0, 0, 0, NULL, NULL, NULL, $5, NULL, clock_timestamp(), clock_timestamp()) ON CONFLICT (id) DO NOTHING RETURNING {SYNC_RUN_COLUMNS}"
        ))
        .bind(run.id())
        .bind(run.source_id().as_uuid())
        .bind(run.job_id())
        .bind(run.outcome().as_str())
        .bind(run.started_at())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|error| map_insert_error(error, run.source_id(), run.job_id()))
        ?;

        if let Some(row) = inserted {
            return decode_sync_run(row);
        }

        let existing_row = sqlx::query(&format!(
            "SELECT {SYNC_RUN_COLUMNS} FROM sync_runs WHERE id = $1 FOR UPDATE"
        ))
        .bind(run.id())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .ok_or(SyncRunRepositoryError::NotFound { run_id: run.id() })?;
        let existing = decode_sync_run(existing_row)?;
        // PostgreSQL timestamps have microsecond precision. Comparing the
        // start timestamp in SQL avoids rejecting a valid retry when the
        // caller supplied sub-microsecond nanoseconds.
        let same_started_at =
            sqlx::query_scalar::<_, bool>("SELECT started_at = $2 FROM sync_runs WHERE id = $1")
                .bind(run.id())
                .bind(run.started_at())
                .fetch_one(&mut **transaction)
                .await
                .map_err(storage_error)?;
        if same_started_at
            && existing.source_id() == run.source_id()
            && existing.job_id() == run.job_id()
        {
            return Ok(existing);
        }
        Err(SyncRunRepositoryError::StartConflict { run_id: run.id() })
    }

    async fn finish(
        &mut self,
        run_id: Uuid,
        completion: SyncRunCompletion,
    ) -> Result<SyncRun, SyncRunRepositoryError> {
        if run_id.is_nil() {
            return Err(SyncError::InvalidId.into());
        }
        let transaction = self.transaction()?;
        let row = sqlx::query(&format!(
            "SELECT {SYNC_RUN_COLUMNS} FROM sync_runs WHERE id = $1 FOR UPDATE"
        ))
        .bind(run_id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(storage_error)?
        .ok_or(SyncRunRepositoryError::NotFound { run_id })?;
        let run = decode_sync_run(row)?;
        let finished = run.finish(completion)?;
        let stats = finished.stats();
        let (failure_class, failure_message) = finished
            .failure()
            .map(|failure| (Some(failure.class().as_str()), Some(failure.message())))
            .unwrap_or((None, None));
        let feed_revision = finished
            .feed_revision()
            .map(feed_revision_as_i64)
            .transpose()?;
        let row = sqlx::query(&format!(
            "UPDATE sync_runs SET outcome = $2, articles_seen = $3, articles_created = $4, articles_updated = $5, articles_failed = $6, archived_articles = $7, archived_assets = $8, failure_class = $9, failure_message = $10, feed_revision = $11, finished_at = $12, updated_at = clock_timestamp() WHERE id = $1 RETURNING {SYNC_RUN_COLUMNS}"
        ))
        .bind(finished.id())
        .bind(finished.outcome().as_str())
        .bind(i64::from(stats.articles_seen))
        .bind(i64::from(stats.articles_created))
        .bind(i64::from(stats.articles_updated))
        .bind(i64::from(stats.articles_failed))
        .bind(i64::from(stats.archived_articles))
        .bind(i64::from(stats.archived_assets))
        .bind(failure_class)
        .bind(failure_message)
        .bind(feed_revision)
        .bind(finished.finished_at())
        .fetch_one(&mut **transaction)
        .await
        .map_err(storage_error)?;
        decode_sync_run(row)
    }
}

fn decode_sync_run(row: PgRow) -> Result<SyncRun, SyncRunRepositoryError> {
    let failure = match (
        row.try_get::<Option<String>, _>("failure_class")
            .map_err(storage_error)?,
        row.try_get::<Option<String>, _>("failure_message")
            .map_err(storage_error)?,
    ) {
        (Some(class), Some(message)) => {
            Some(SyncFailure::new(SyncFailureClass::parse(&class)?, message)?)
        }
        (None, None) => None,
        _ => return Err(SyncRunRepositoryError::Domain(SyncError::InvalidOutcome)),
    };

    SyncRun::from_parts(SyncRunParts {
        id: row.try_get("id").map_err(storage_error)?,
        source_id: SourceId::from_uuid(row.try_get("source_id").map_err(storage_error)?),
        job_id: row.try_get("job_id").map_err(storage_error)?,
        outcome: SyncOutcome::parse(&row.try_get::<String, _>("outcome").map_err(storage_error)?)?,
        stats: SyncStats {
            articles_seen: counter(&row, "articles_seen")?,
            articles_created: counter(&row, "articles_created")?,
            articles_updated: counter(&row, "articles_updated")?,
            articles_failed: counter(&row, "articles_failed")?,
            archived_articles: counter(&row, "archived_articles")?,
            archived_assets: counter(&row, "archived_assets")?,
        },
        failure,
        feed_revision: row
            .try_get::<Option<i64>, _>("feed_revision")
            .map_err(storage_error)?
            .map(feed_revision_from_i64)
            .transpose()?,
        started_at: row.try_get("started_at").map_err(storage_error)?,
        finished_at: row.try_get("finished_at").map_err(storage_error)?,
        created_at: row.try_get("created_at").map_err(storage_error)?,
        updated_at: row.try_get("updated_at").map_err(storage_error)?,
    })
    .map_err(SyncRunRepositoryError::Domain)
}

fn counter(row: &PgRow, field: &'static str) -> Result<u32, SyncRunRepositoryError> {
    let value: i64 = row.try_get(field).map_err(storage_error)?;
    u32::try_from(value).map_err(|_| SyncRunRepositoryError::InvalidCounter { field, value })
}

fn feed_revision_from_i64(value: i64) -> Result<FeedRevision, SyncRunRepositoryError> {
    u64::try_from(value)
        .map(FeedRevision::from_u64)
        .map_err(|_| SyncRunRepositoryError::InvalidFeedRevision { value })
}

fn feed_revision_as_i64(value: FeedRevision) -> Result<i64, SyncRunRepositoryError> {
    i64::try_from(value.as_u64())
        .map_err(|_| SyncRunRepositoryError::InvalidFeedRevision { value: i64::MAX })
}

fn validate_source_id(source_id: SourceId) -> Result<(), SyncRunRepositoryError> {
    if source_id.as_uuid().is_nil() {
        Err(SyncRunRepositoryError::Domain(SyncError::InvalidSourceId))
    } else {
        Ok(())
    }
}

fn map_insert_error(
    error: sqlx::Error,
    source_id: SourceId,
    job_id: Option<Uuid>,
) -> SyncRunRepositoryError {
    if let sqlx::Error::Database(database_error) = &error {
        match database_error.constraint() {
            Some("sync_runs_source_id_fkey") => {
                return SyncRunRepositoryError::SourceNotFound { source_id };
            }
            Some("sync_runs_job_id_fkey") => {
                if let Some(job_id) = job_id {
                    return SyncRunRepositoryError::JobNotFound { job_id };
                }
            }
            _ => {}
        }
    }
    storage_error(error)
}

fn job_transaction_error(error: JobRepositoryError) -> SyncRunRepositoryError {
    SyncRunRepositoryError::Storage(error.to_string())
}

fn storage_error(error: impl fmt::Display) -> SyncRunRepositoryError {
    SyncRunRepositoryError::Storage(error.to_string())
}
