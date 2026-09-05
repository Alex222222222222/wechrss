//! Shared PostgreSQL unit-of-work boundary.
//!
//! Purpose: make article/archive mutations, source revision and schedule
//! changes, sync-run persistence, feed-cache replacement, and fenced job
//! completion atomic without exposing SQLx transactions to application code.
//!
//! `UnitOfWorkFactory::begin` creates one short-lived SQLx transaction. Its
//! returned handle exposes transaction-scoped job, source, article, asset,
//! sync-run, and feed-cache views today. Only the unit of work can commit; dropping it
//! or returning an error rolls all component writes back. Repository views
//! borrow the unit of work and therefore cannot outlive or independently commit
//! their transaction.
//!
//! Minimum executable contract:
//!
//! - `UnitOfWorkFactory::begin()` creates the transaction;
//! - `jobs()` remains the interim compatibility view;
//! - `job_outcomes()` borrows only the transaction-scoped worker-outcome port;
//! - `job_enqueue()` borrows only the transaction-scoped enqueue port;
//! - `source()` borrows the transaction-scoped source mutation view;
//! - `articles()` borrows the transaction-scoped article mutation view;
//! - `assets(policy)` borrows the transaction-scoped asset storage view;
//! - `feed_cache()` borrows the transaction-scoped feed-cache publication view;
//! - `database_now()` samples the PostgreSQL clock for persisted ordering
//!   timestamps;
//! - `commit(self)` is the only successful exit for a completed unit of work;
//! - `rollback(self)` is available for explicit cleanup in tests or callers
//!   that need to await rollback; and
//! - archive acquisition and URL rewriting use `assets(policy)` while
//!   borrowing this same transaction; and
//! - article upserts return feed-visible change information so the caller can
//!   decide whether to bump the source revision in this same transaction.
//!
//! Retry, deferral, cancellation, and failure outcomes use the same boundary
//! because they record sync results or alter source scheduling gates/cooldowns.
//! Queue-only claim, heartbeat, and read operations remain short independent
//! transactions. Enqueueing can be independent for external requests, or can
//! use `job_enqueue()` when another aggregate (such as a newly created source)
//! must be published atomically with its job. Expired-lease recovery is a
//! dedicated atomic persistence operation because exhausting a failure budget
//! may also update the source cooldown.
//!
//! Synchronization data flow:
//!
//! 1. perform browser/network acquisition and normalization without a database
//!    transaction;
//! 2. keep the job lease alive through a separate pool connection;
//! 3. begin a unit of work and, when assets are present, preflight their
//!    checksums/raw-byte candidates before source/article row locks;
//! 4. verify the job owner, fencing token, and live lease, then verify the
//!    expected base feed revision and persist idempotent article/archive
//!    changes, and advance to the candidate revision when feed-visible data
//!    changed;
//! 5. persist an already-rendered cache payload only for that exact revision,
//!    verify/release the feed-build lease, update the sync run and source
//!    schedule/gate, and mark the job successful;
//! 6. commit once.
//!
//! Non-responsibilities: browser calls, sleeping, rendering large documents
//! while locks are held, heartbeat task ownership, retry policy, or HTTP error
//! mapping. A unit of work must use bounded lock/statement timeouts and should
//! be short enough that a worker can safely heartbeat independently.
//!
//! High availability: the job view already participates in the shared
//! transaction, so future fenced job outcomes can commit together with business
//! writes. If ownership is lost, the unit of work rolls back instead of
//! publishing writes from a stale worker. Cache compare-and-swap and feed-build
//! fencing checks happen inside this transaction through the feed-cache view.

use std::fmt;

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use thiserror::Error;

use crate::{
    archive::asset_store::{AssetCachePolicy, AssetInput},
    domain::job::{Job, NewJob},
};

use super::repositories::{
    article_repository::PostgresArticleTransaction,
    asset_repository::{prepare_asset_batch, AssetRepositoryError, PostgresAssetTransaction},
    feed_cache_repository::PostgresFeedCacheTransaction,
    job_repository::PostgresJobTransaction,
    job_repository::{
        EnqueueResult, JobEnqueueTransaction, JobOutcome, JobOutcomeTransaction, JobRepositoryError,
    },
    source_repository::PostgresSourceTransaction,
    sync_run_repository::PostgresSyncRunTransaction,
};

/// Errors raised while opening or completing a unit of work.
#[derive(Debug, Error)]
pub enum UnitOfWorkError {
    /// SQLx could not start, commit, or roll back the transaction.
    #[error("unit of work transaction error: {0}")]
    Transaction(#[source] sqlx::Error),
    /// Asset digest/collision preflight could not complete before persistence.
    #[error("asset preflight error: {0}")]
    AssetPreparation(#[source] AssetRepositoryError),
    /// SQLx could not sample the authoritative PostgreSQL clock.
    #[error("unit of work database clock error: {0}")]
    DatabaseClock(#[source] sqlx::Error),
}

/// Factory for short-lived, shared persistence transactions.
#[derive(Clone)]
pub struct UnitOfWorkFactory {
    pool: PgPool,
}

impl fmt::Debug for UnitOfWorkFactory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnitOfWorkFactory")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl UnitOfWorkFactory {
    /// Creates a factory backed by the configured PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Begins a transaction whose repository views share one commit boundary.
    pub async fn begin(&self) -> Result<UnitOfWork<'_>, UnitOfWorkError> {
        self.begin_with_assets(&[]).await
    }

    /// Begins a transaction after preflighting asset digests and raw-byte
    /// deduplication. This must be called before source/article row locks are
    /// acquired when the unit of work will store asset inputs.
    pub async fn begin_with_assets(
        &self,
        inputs: &[AssetInput],
    ) -> Result<UnitOfWork<'_>, UnitOfWorkError> {
        tracing::trace!("beginning PostgreSQL unit of work");
        let mut jobs = PostgresJobTransaction::begin(&self.pool)
            .await
            .map_err(UnitOfWorkError::Transaction)?;
        let asset_preparation = prepare_asset_batch(
            jobs.transaction_mut().map_err(|error| {
                UnitOfWorkError::AssetPreparation(AssetRepositoryError::Storage(error.to_string()))
            })?,
            inputs,
        )
        .await
        .map_err(UnitOfWorkError::AssetPreparation)?;
        let result = Ok(UnitOfWork {
            jobs,
            asset_preparation: Some(asset_preparation),
        });
        if let Err(error) = &result {
            tracing::warn!(error = %error, "unable to begin PostgreSQL unit of work");
        }
        result
    }

    /// Samples PostgreSQL's wall clock without opening a long-lived transaction.
    ///
    /// Application services use this for persisted timestamps that participate
    /// in cross-replica ordering. The query intentionally uses
    /// `clock_timestamp()` rather than a process-local clock.
    pub async fn database_now(&self) -> Result<DateTime<Utc>, UnitOfWorkError> {
        let result = sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await
            .map_err(UnitOfWorkError::DatabaseClock);
        if let Err(error) = &result {
            tracing::warn!(error = %error, "unable to read PostgreSQL authoritative clock");
        }
        result
    }
}

/// A short-lived transaction-scoped persistence unit.
pub struct UnitOfWork<'a> {
    jobs: PostgresJobTransaction<'a>,
    asset_preparation: Option<super::repositories::asset_repository::AssetBatchPreparation>,
}

/// Outcome-only view over the job transaction owned by [`UnitOfWork`].
///
/// This wrapper intentionally exposes no queue, recovery, or commit operation.
/// A worker can apply one fenced [`JobOutcome`] through it, then the enclosing
/// unit of work can persist related state and commit everything together.
pub struct JobOutcomeView<'u, 'a> {
    transaction: &'u mut PostgresJobTransaction<'a>,
}

/// Enqueue-only view over the job transaction owned by [`UnitOfWork`].
///
/// Source creation uses this capability to persist its initial sync job in the
/// same transaction as the source row. The wrapper intentionally does not
/// expose worker claims, outcomes, recovery, or commit.
pub struct JobEnqueueView<'u, 'a> {
    transaction: &'u mut PostgresJobTransaction<'a>,
}

#[async_trait::async_trait]
impl JobEnqueueTransaction for JobEnqueueView<'_, '_> {
    async fn enqueue_job(&mut self, spec: NewJob) -> Result<EnqueueResult, JobRepositoryError> {
        JobEnqueueTransaction::enqueue_job(&mut *self.transaction, spec).await
    }
}

#[async_trait::async_trait]
impl JobOutcomeTransaction for JobOutcomeView<'_, '_> {
    async fn apply_outcome(&mut self, outcome: JobOutcome) -> Result<Job, JobRepositoryError> {
        JobOutcomeTransaction::apply_outcome(&mut *self.transaction, outcome).await
    }
}

#[async_trait::async_trait]
impl JobOutcomeTransaction for UnitOfWork<'_> {
    async fn apply_outcome(&mut self, outcome: JobOutcome) -> Result<Job, JobRepositoryError> {
        self.job_outcomes().apply_outcome(outcome).await
    }
}

impl<'a> UnitOfWork<'a> {
    /// Borrows the job repository view without exposing an independent commit.
    pub fn jobs(&mut self) -> &mut PostgresJobTransaction<'a> {
        &mut self.jobs
    }

    /// Borrows the outcome-only job view without exposing a commit operation.
    ///
    /// The returned opaque view can apply a fenced [`JobOutcome`] while this
    /// unit of work remains open. Callers should persist related business
    /// changes through the other views and then call [`Self::commit`] once.
    pub fn job_outcomes(&mut self) -> JobOutcomeView<'_, 'a> {
        JobOutcomeView {
            transaction: &mut self.jobs,
        }
    }

    /// Borrows the enqueue-only job view without exposing a commit operation.
    pub fn job_enqueue(&mut self) -> JobEnqueueView<'_, 'a> {
        JobEnqueueView {
            transaction: &mut self.jobs,
        }
    }

    /// Borrows the transaction-scoped article mutation view.
    pub fn articles(&mut self) -> PostgresArticleTransaction<'_, 'a> {
        PostgresArticleTransaction::new(&mut self.jobs)
    }

    /// Borrows the transaction-scoped binary asset mutation view.
    pub fn assets(&mut self, policy: AssetCachePolicy) -> PostgresAssetTransaction<'_, 'a> {
        PostgresAssetTransaction::new(&mut self.jobs, self.asset_preparation.clone(), policy)
    }

    /// Borrows the transaction-scoped feed-cache publication view.
    pub fn feed_cache(&mut self) -> PostgresFeedCacheTransaction<'_, 'a> {
        PostgresFeedCacheTransaction::new(&mut self.jobs)
    }

    /// Borrows the transaction-scoped source mutation view.
    pub fn source(&mut self) -> PostgresSourceTransaction<'_, 'a> {
        PostgresSourceTransaction::new(&mut self.jobs)
    }

    /// Borrows the transaction-scoped synchronization-run view.
    pub fn sync_runs(&mut self) -> PostgresSyncRunTransaction<'_, 'a> {
        PostgresSyncRunTransaction::new(&mut self.jobs)
    }

    /// Commits all mutations made through this unit of work.
    pub async fn commit(self) -> Result<(), UnitOfWorkError> {
        let result = self
            .jobs
            .commit_inner()
            .await
            .map_err(UnitOfWorkError::Transaction);
        match &result {
            Ok(()) => tracing::trace!("committed PostgreSQL unit of work"),
            Err(error) => {
                tracing::warn!(error = %error, "failed to commit PostgreSQL unit of work")
            }
        }
        result
    }

    /// Explicitly rolls back all mutations made through this unit of work.
    pub async fn rollback(self) -> Result<(), UnitOfWorkError> {
        let result = self
            .jobs
            .rollback_inner()
            .await
            .map_err(UnitOfWorkError::Transaction);
        match &result {
            Ok(()) => tracing::trace!("rolled back PostgreSQL unit of work"),
            Err(error) => {
                tracing::warn!(error = %error, "failed to roll back PostgreSQL unit of work")
            }
        }
        result
    }
}

impl fmt::Debug for UnitOfWork<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnitOfWork")
            .field("jobs", &self.jobs)
            .finish()
    }
}

// TODO(design): add the remaining verify-fence and business-coupled source,
// article, sync-run, and archive commands behind this boundary; and prevent
// SyncService from receiving a job-only commit API.
