//! Shared PostgreSQL unit-of-work boundary.
//!
//! Purpose: make article/archive mutations, source revision and schedule
//! changes, sync-run persistence, feed-cache replacement, and fenced job
//! completion atomic without exposing SQLx transactions to application code.
//!
//! `UnitOfWorkFactory::begin` creates one short-lived SQLx transaction. Its
//! returned handle exposes transaction-scoped job, source, article, and
//! feed-cache views today; sync-run views will be added as their repository
//! contracts become executable. Only the unit of work can commit; dropping it
//! or returning an error rolls all component writes back. Repository views
//! borrow the unit of work and therefore cannot outlive or independently commit
//! their transaction.
//!
//! Minimum executable contract:
//!
//! - `UnitOfWorkFactory::begin()` creates the transaction;
//! - `jobs()` borrows the transaction-scoped job repository view;
//! - `source()` borrows the transaction-scoped source mutation view;
//! - `articles()` borrows the transaction-scoped article mutation view;
//! - `feed_cache()` borrows the transaction-scoped feed-cache publication view;
//! - `commit(self)` is the only successful exit for a completed unit of work;
//! - `rollback(self)` is available for explicit cleanup in tests or callers
//!   that need to await rollback; and
//! - future `verify_fence`, sync-run, and archive commands will be added to
//!   views that borrow this same transaction; and
//! - article upserts return feed-visible change information so the caller can
//!   decide whether to bump the source revision in this same transaction.
//!
//! Retry, deferral, cancellation, and failure outcomes use the same boundary
//! because they record sync results or alter source scheduling gates/cooldowns.
//! Queue-only enqueue, claim, heartbeat, and read operations remain short
//! independent transactions. Expired-lease recovery is a dedicated atomic
//! persistence operation because exhausting a failure budget may also update the
//! source cooldown.
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

use sqlx::PgPool;
use thiserror::Error;

use super::repositories::{
    article_repository::PostgresArticleTransaction,
    feed_cache_repository::PostgresFeedCacheTransaction, job_repository::PostgresJobTransaction,
    source_repository::PostgresSourceTransaction,
};

/// Errors raised while opening or completing a unit of work.
#[derive(Debug, Error)]
pub enum UnitOfWorkError {
    /// SQLx could not start, commit, or roll back the transaction.
    #[error("unit of work transaction error: {0}")]
    Transaction(#[source] sqlx::Error),
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
}

/// A short-lived transaction-scoped persistence unit.
pub struct UnitOfWork<'a> {
    jobs: PostgresJobTransaction<'a>,
}

impl<'a> UnitOfWork<'a> {
    /// Borrows the job repository view without exposing an independent commit.
    pub fn jobs(&mut self) -> &mut PostgresJobTransaction<'a> {
        &mut self.jobs
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

// TODO(design): add the sync-run view; move verify-fence and business-coupled
// job outcomes behind this boundary; and prevent SyncService from receiving a
// job-only commit API.
