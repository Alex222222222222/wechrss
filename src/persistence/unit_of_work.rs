//! Shared PostgreSQL unit-of-work boundary.
//!
//! Purpose: make article/archive mutations, source revision and schedule
//! changes, sync-run persistence, feed-cache replacement, and fenced job
//! completion atomic without exposing SQLx transactions to application code.
//!
//! `UnitOfWorkFactory::begin` creates one short-lived SQLx transaction. Its
//! returned handle exposes transaction-scoped job, source, article, sync-run,
//! and feed-cache views today. Only the unit of work can commit; dropping it
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
//! - `feed_cache()` borrows the transaction-scoped feed-cache publication view;
//! - `database_now()` samples the PostgreSQL clock for persisted ordering
//!   timestamps;
//! - `commit(self)` is the only successful exit for a completed unit of work;
//! - `rollback(self)` is available for explicit cleanup in tests or callers
//!   that need to await rollback; and
//! - future `verify_fence` and archive commands will be added to views that
//!   borrow this same transaction; and
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
//! 3. begin a unit of work and verify the job owner, fencing token, and live
//!    lease;
//! 4. verify the expected base feed revision, persist idempotent article/archive
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

use crate::domain::job::{Job, NewJob};

use super::repositories::{
    article_repository::PostgresArticleTransaction,
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
        let jobs = PostgresJobTransaction::begin(&self.pool)
            .await
            .map_err(UnitOfWorkError::Transaction)?;
        Ok(UnitOfWork { jobs })
    }

    /// Samples PostgreSQL's wall clock without opening a long-lived transaction.
    ///
    /// Application services use this for persisted timestamps that participate
    /// in cross-replica ordering. The query intentionally uses
    /// `clock_timestamp()` rather than a process-local clock.
    pub async fn database_now(&self) -> Result<DateTime<Utc>, UnitOfWorkError> {
        sqlx::query_scalar("SELECT clock_timestamp()")
            .fetch_one(&self.pool)
            .await
            .map_err(UnitOfWorkError::DatabaseClock)
    }
}

/// A short-lived transaction-scoped persistence unit.
pub struct UnitOfWork<'a> {
    jobs: PostgresJobTransaction<'a>,
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
        self.jobs
            .commit_inner()
            .await
            .map_err(UnitOfWorkError::Transaction)
    }

    /// Explicitly rolls back all mutations made through this unit of work.
    pub async fn rollback(self) -> Result<(), UnitOfWorkError> {
        self.jobs
            .rollback_inner()
            .await
            .map_err(UnitOfWorkError::Transaction)
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
