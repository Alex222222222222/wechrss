//! Atomic source-scheduling repository.
//!
//! Purpose: provide the one cross-table operation needed by every scheduler
//! replica without exposing a race-prone due-source list to application code.
//! [`SchedulerRepository::enqueue_due_sources`] opens a short transaction,
//! derives eligibility time from PostgreSQL, locks a bounded batch of enabled,
//! `ready`, due sources with `FOR UPDATE SKIP LOCKED`, excludes sources with an
//! active source-sync job, inserts canonical `source_sync:{source_id}` jobs,
//! and records their scheduling reservations.
//!
//! PostgreSQL partial uniqueness remains the final deduplication defense, but
//! it is not the primary loop-control mechanism. Disabled sources and sources
//! blocked for authentication or risk control are never selected. Retry-wait
//! and deferred jobs remain active and therefore exclude another source-sync
//! insertion.
//!
//! Failure behavior: the source reservation and job insertion commit together
//! or not at all. Duplicate conflicts caused by concurrent manual sync requests
//! are normal idempotent outcomes. This repository never calculates quiet
//! hours, runs browser work, or decides a source's terminal scheduling gate.
//!
//! High availability: source rows are locked in PostgreSQL, so independent
//! application replicas claim disjoint batches. `SKIP LOCKED` prevents one slow
//! transaction from making another scheduler wait. A committed reservation is
//! durable across process restarts and is only a short loop-control guard; the
//! active job's partial unique index remains authoritative.

use std::fmt;

use chrono::Duration;
use serde_json::json;
use sqlx::{PgPool, Row};
use thiserror::Error;
use uuid::Uuid;

use crate::domain::source::SourceId;

/// A source-sync job inserted together with its scheduler reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnqueuedSource {
    source_id: SourceId,
    job_id: Uuid,
    reserved_until: chrono::DateTime<chrono::Utc>,
}

impl EnqueuedSource {
    /// Returns the source whose synchronization was enqueued.
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// Returns the durable job identifier.
    pub const fn job_id(&self) -> Uuid {
        self.job_id
    }

    /// Returns the PostgreSQL-derived reservation expiry.
    pub const fn reserved_until(&self) -> chrono::DateTime<chrono::Utc> {
        self.reserved_until
    }
}

/// Errors returned by the atomic scheduler repository.
#[derive(Debug, Error)]
pub enum SchedulerRepositoryError {
    /// An oversized batch cannot be sent to PostgreSQL as a limit.
    #[error("scheduler batch limit is too large: {value}")]
    InvalidLimit {
        /// The requested batch size.
        value: usize,
    },
    /// A reservation must be representable as a positive millisecond interval.
    #[error("scheduler reservation must be a positive whole number of milliseconds")]
    InvalidReservation,
    /// A persisted source retry limit cannot be represented by the job domain.
    #[error("source {source_id} has invalid max_attempts value {value}")]
    InvalidMaxAttempts {
        /// Source containing the invalid value.
        source_id: SourceId,
        /// Value read from PostgreSQL.
        value: i64,
    },
    /// PostgreSQL could not complete the transaction.
    #[error("scheduler repository storage error: {0}")]
    Storage(#[source] sqlx::Error),
}

/// Cross-replica scheduler operation.
#[allow(async_fn_in_trait)]
pub trait SchedulerRepository: Send + Sync {
    /// Enqueues up to `limit` eligible sources and reserves each source for a
    /// short period. A zero limit is a successful no-op.
    async fn enqueue_due_sources(
        &self,
        limit: usize,
        reservation_for: Duration,
    ) -> Result<Vec<EnqueuedSource>, SchedulerRepositoryError>;
}

/// PostgreSQL implementation of the atomic due-source operation.
#[derive(Clone)]
pub struct PostgresSchedulerRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresSchedulerRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresSchedulerRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresSchedulerRepository {
    /// Creates a scheduler repository using an existing configured pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl SchedulerRepository for PostgresSchedulerRepository {
    async fn enqueue_due_sources(
        &self,
        limit: usize,
        reservation_for: Duration,
    ) -> Result<Vec<EnqueuedSource>, SchedulerRepositoryError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let limit = i64::try_from(limit)
            .map_err(|_| SchedulerRepositoryError::InvalidLimit { value: limit })?;
        let reservation_milliseconds = validate_reservation(reservation_for)?;

        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(SchedulerRepositoryError::Storage)?;
        let source_rows = sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            SELECT source.id, source.priority, source.max_attempts, db_clock.now
            FROM sources AS source
            CROSS JOIN db_clock
            WHERE source.enabled
              AND source.scheduling_gate = 'ready'
              AND source.next_fetch_at <= db_clock.now
              AND (
                  source.failure_cooldown_until IS NULL
                  OR source.failure_cooldown_until <= db_clock.now
              )
              AND (
                  source.schedule_reserved_until IS NULL
                  OR source.schedule_reserved_until <= db_clock.now
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM jobs AS active_job
                  WHERE active_job.dedupe_key = 'source_sync:' || source.id::text
                    AND active_job.status IN ('queued', 'running', 'retry_wait', 'deferred')
              )
            ORDER BY source.priority DESC, source.next_fetch_at ASC, source.id ASC
            LIMIT $1
            FOR UPDATE OF source SKIP LOCKED
            "#,
        )
        .bind(limit)
        .fetch_all(&mut *transaction)
        .await
        .map_err(SchedulerRepositoryError::Storage)?;

        let mut enqueued = Vec::with_capacity(source_rows.len());
        for source_row in source_rows {
            let source_id = SourceId::from_uuid(
                source_row
                    .try_get("id")
                    .map_err(SchedulerRepositoryError::Storage)?,
            );
            let priority: i32 = source_row
                .try_get("priority")
                .map_err(SchedulerRepositoryError::Storage)?;
            let max_attempts: i64 = source_row
                .try_get("max_attempts")
                .map_err(SchedulerRepositoryError::Storage)?;
            let max_attempts = u32::try_from(max_attempts).map_err(|_| {
                SchedulerRepositoryError::InvalidMaxAttempts {
                    source_id,
                    value: max_attempts,
                }
            })?;
            let db_now: chrono::DateTime<chrono::Utc> = source_row
                .try_get("now")
                .map_err(SchedulerRepositoryError::Storage)?;
            let job_id = Uuid::new_v4();
            let dedupe_key = format!("source_sync:{source_id}");
            let inserted_job_id = sqlx::query(
                r#"
                INSERT INTO jobs (
                    id, job_type, source_id, status, priority, run_after,
                    claim_count, failure_count, max_attempts, lease_owner,
                    lease_token, lease_until, heartbeat_at, started_at,
                    finished_at, last_error, payload_json, dedupe_key,
                    created_at, updated_at
                )
                VALUES (
                    $1, 'source_sync', $2, 'queued', $3, $4,
                    0, 0, $5, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, $6, $7, $4, $4
                )
                ON CONFLICT (dedupe_key)
                    WHERE status IN ('queued', 'running', 'retry_wait', 'deferred')
                DO NOTHING
                RETURNING id
                "#,
            )
            .bind(job_id)
            .bind(source_id.as_uuid())
            .bind(priority)
            .bind(db_now)
            .bind(i64::from(max_attempts))
            .bind(json!({ "source_id": source_id }))
            .bind(&dedupe_key)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(SchedulerRepositoryError::Storage)?;

            let Some(inserted_job_id) = inserted_job_id else {
                continue;
            };
            let inserted_job_id: Uuid = inserted_job_id
                .try_get("id")
                .map_err(SchedulerRepositoryError::Storage)?;

            let reserved_until: chrono::DateTime<chrono::Utc> = sqlx::query(
                r#"
                UPDATE sources
                SET schedule_reserved_until = $2
                    + ($3::double precision * INTERVAL '1 millisecond'),
                    updated_at = $2
                WHERE id = $1
                RETURNING schedule_reserved_until
                "#,
            )
            .bind(source_id.as_uuid())
            .bind(db_now)
            .bind(reservation_milliseconds)
            .fetch_one(&mut *transaction)
            .await
            .map_err(SchedulerRepositoryError::Storage)?
            .try_get("schedule_reserved_until")
            .map_err(SchedulerRepositoryError::Storage)?;

            enqueued.push(EnqueuedSource {
                source_id,
                job_id: inserted_job_id,
                reserved_until,
            });
        }

        transaction
            .commit()
            .await
            .map_err(SchedulerRepositoryError::Storage)?;
        Ok(enqueued)
    }
}

fn validate_reservation(reservation_for: Duration) -> Result<i64, SchedulerRepositoryError> {
    let milliseconds = reservation_for.num_milliseconds();
    if milliseconds <= 0 {
        Err(SchedulerRepositoryError::InvalidReservation)
    } else {
        Ok(milliseconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn zero_limit_does_not_require_a_valid_reservation() {
        let repository = PostgresSchedulerRepository {
            pool: PgPool::connect_lazy("postgres://unused").expect("valid lazy URL"),
        };

        assert!(repository
            .enqueue_due_sources(0, Duration::zero())
            .await
            .expect("zero limit is a no-op")
            .is_empty());
    }

    #[test]
    fn rejects_non_positive_reservations_before_opening_a_transaction() {
        assert!(matches!(
            validate_reservation(Duration::zero()),
            Err(SchedulerRepositoryError::InvalidReservation)
        ));
        assert!(matches!(
            validate_reservation(Duration::milliseconds(-1)),
            Err(SchedulerRepositoryError::InvalidReservation)
        ));
    }
}
