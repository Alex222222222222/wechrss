//! Distributed WeRead account-lease repository.
//!
//! Purpose: serialize authenticated use of one WeRead account across all
//! application replicas. Source job leases prevent duplicate source work but do
//! not prevent two different sources from using the same account concurrently.
//!
//! This module implements the storage-neutral [`AccountLeaseStore`] port.
//! Expected operations are `acquire(account_id, owner, lease_for)`,
//! `heartbeat(account_id, owner, token, lease_for)`, and
//! `release(account_id, owner, token)`. Acquisition returns a fresh fencing
//! token and succeeds only when no lease exists or the prior lease has expired.
//! Every mutation compares account ID, owner, token, and lease expiry.
//! The concrete production interface omits caller-provided `now`: SQL derives
//! one statement-local PostgreSQL timestamp for acquisition, heartbeat,
//! release, expiry, and takeover. An injectable clock belongs only to the memory
//! test implementation. The old `AccountLeaseRepository` name is retained
//! here only as a compatibility re-export of that port.
//!
//! Authenticated article-list, detail-URL recovery, login exchange, and
//! credential refresh operations hold this lease and heartbeat it through a
//! separate pool connection. Lease loss cancels the account operation before
//! another upstream request. Public WeChat article extraction neither acquires
//! this lease nor receives credentials.
//!
//! PostgreSQL owns cross-replica exclusion. Local `BrowserPool` capacity remains
//! a separate process-level resource limit and must not be treated as this
//! distributed lock. The first version may have a single account, but that
//! account still has a stable durable identifier rather than an implicit global
//! mutex.
//!
//! Failure behavior mirrors job fencing: stale release or heartbeat requests
//! return a typed ownership error, and expired leases are recoverable. Secret
//! credential values are never stored in the lease row or error text.

use std::{collections::HashMap, fmt, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use sqlx::{postgres::PgRow, PgPool, Row};
use tokio::sync::Mutex;

pub use crate::acquisition::browser_pool::{AccountLeaseError, AccountLeaseStore};
use crate::domain::credentials::{AccountLease, AccountLeaseToken, WeReadAccountId};

/// Compatibility re-export for callers that used the old persistence-local
/// name. New code should depend on the storage-neutral [`AccountLeaseStore`]
/// port from the acquisition boundary.
pub use crate::acquisition::browser_pool::AccountLeaseStore as AccountLeaseRepository;

/// PostgreSQL-backed account lease repository.
#[derive(Clone)]
pub struct PostgresAccountLeaseRepository {
    pool: PgPool,
}

impl fmt::Debug for PostgresAccountLeaseRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresAccountLeaseRepository")
            .field("pool", &"<postgres pool>")
            .finish()
    }
}

impl PostgresAccountLeaseRepository {
    /// Creates a repository backed by the shared PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl AccountLeaseStore for PostgresAccountLeaseRepository {
    async fn acquire(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<AccountLease>, AccountLeaseError> {
        validate_inputs(account_id, owner, lease_for)?;
        let milliseconds = lease_milliseconds(lease_for)?;
        let token = AccountLeaseToken::new();
        let row = sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            INSERT INTO account_leases (
                account_id, lease_owner, lease_token, lease_until, heartbeat_at
            )
            SELECT $1, $2, $3,
                   db_clock.now + ($4::double precision * INTERVAL '1 millisecond'),
                   db_clock.now
            FROM db_clock
            ON CONFLICT (account_id) DO UPDATE
            SET lease_owner = EXCLUDED.lease_owner,
                lease_token = EXCLUDED.lease_token,
                lease_until = EXCLUDED.lease_until,
                heartbeat_at = EXCLUDED.heartbeat_at
            WHERE account_leases.lease_until <= (SELECT now FROM db_clock)
            RETURNING account_id, lease_owner, lease_token, lease_until, heartbeat_at
            "#,
        )
        .bind(account_id.as_uuid())
        .bind(owner)
        .bind(token.as_uuid())
        .bind(milliseconds)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(decode_lease).transpose()
    }

    async fn heartbeat(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        token: AccountLeaseToken,
        lease_for: Duration,
    ) -> Result<AccountLease, AccountLeaseError> {
        validate_inputs(account_id, owner, lease_for)?;
        let milliseconds = lease_milliseconds(lease_for)?;
        let row = sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            UPDATE account_leases AS lease
            SET lease_until = db_clock.now + ($4::double precision * INTERVAL '1 millisecond'),
                heartbeat_at = db_clock.now
            FROM db_clock
            WHERE lease.account_id = $1
              AND lease.lease_owner = $2
              AND lease.lease_token = $3
              AND lease.lease_until > db_clock.now
            RETURNING lease.account_id, lease.lease_owner, lease.lease_token,
                      lease.lease_until, lease.heartbeat_at
            "#,
        )
        .bind(account_id.as_uuid())
        .bind(owner)
        .bind(token.as_uuid())
        .bind(milliseconds)
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        row.map(decode_lease)
            .transpose()?
            .ok_or(AccountLeaseError::LeaseLost { account_id })
    }

    async fn release(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        token: AccountLeaseToken,
    ) -> Result<(), AccountLeaseError> {
        validate_account_and_owner(account_id, owner)?;
        let released = sqlx::query(
            r#"
            WITH db_clock AS MATERIALIZED (
                SELECT clock_timestamp() AS now
            )
            DELETE FROM account_leases AS lease
            USING db_clock
            WHERE lease.account_id = $1
              AND lease.lease_owner = $2
              AND lease.lease_token = $3
              AND lease.lease_until > db_clock.now
            RETURNING lease.account_id
            "#,
        )
        .bind(account_id.as_uuid())
        .bind(owner)
        .bind(token.as_uuid())
        .fetch_optional(&self.pool)
        .await
        .map_err(storage_error)?;

        if released.is_some() {
            Ok(())
        } else {
            Err(AccountLeaseError::LeaseLost { account_id })
        }
    }
}

fn decode_lease(row: PgRow) -> Result<AccountLease, AccountLeaseError> {
    let account_id = WeReadAccountId::from_uuid(row.try_get("account_id").map_err(storage_error)?);
    validate_account_id(account_id)?;
    Ok(AccountLease::from_parts(
        account_id,
        row.try_get("lease_owner").map_err(storage_error)?,
        AccountLeaseToken::from_uuid(row.try_get("lease_token").map_err(storage_error)?),
        row.try_get("lease_until").map_err(storage_error)?,
        row.try_get("heartbeat_at").map_err(storage_error)?,
    ))
}

fn validate_inputs(
    account_id: WeReadAccountId,
    owner: &str,
    lease_for: Duration,
) -> Result<(), AccountLeaseError> {
    validate_account_and_owner(account_id, owner)?;
    if lease_for <= Duration::zero() {
        return Err(AccountLeaseError::InvalidLeaseDuration);
    }
    Ok(())
}

fn validate_account_and_owner(
    account_id: WeReadAccountId,
    owner: &str,
) -> Result<(), AccountLeaseError> {
    validate_account_id(account_id)?;
    if owner.trim().is_empty() {
        return Err(AccountLeaseError::EmptyOwner);
    }
    Ok(())
}

fn validate_account_id(account_id: WeReadAccountId) -> Result<(), AccountLeaseError> {
    if account_id.as_uuid().is_nil() {
        Err(AccountLeaseError::InvalidAccountId)
    } else {
        Ok(())
    }
}

fn lease_milliseconds(lease_for: Duration) -> Result<i64, AccountLeaseError> {
    let milliseconds = lease_for.num_milliseconds();
    if milliseconds <= 0 {
        Err(AccountLeaseError::InvalidLeaseDuration)
    } else {
        Ok(milliseconds)
    }
}

fn storage_error(error: impl fmt::Display) -> AccountLeaseError {
    AccountLeaseError::Backend(error.to_string())
}

#[derive(Debug)]
struct MemoryState {
    now: DateTime<Utc>,
    leases: HashMap<WeReadAccountId, AccountLease>,
}

/// Deterministic in-memory account lease repository for unit tests.
///
/// It uses Tokio's non-poisoning mutex and an injectable clock. It models
/// ownership, expiry, takeover, and fencing but provides no cross-process
/// coordination; production code must use [`PostgresAccountLeaseRepository`].
#[derive(Clone)]
pub struct MemoryAccountLeaseRepository {
    state: Arc<Mutex<MemoryState>>,
}

impl fmt::Debug for MemoryAccountLeaseRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryAccountLeaseRepository")
            .finish_non_exhaustive()
    }
}

impl MemoryAccountLeaseRepository {
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

impl AccountLeaseStore for MemoryAccountLeaseRepository {
    async fn acquire(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<AccountLease>, AccountLeaseError> {
        validate_inputs(account_id, owner, lease_for)?;
        let mut state = self.state.lock().await;
        if state
            .leases
            .get(&account_id)
            .is_some_and(|lease| lease.is_live_at(state.now))
        {
            return Ok(None);
        }
        let lease_until = state
            .now
            .checked_add_signed(lease_for)
            .ok_or(AccountLeaseError::InvalidLeaseDuration)?;
        let lease = AccountLease::from_parts(
            account_id,
            owner.to_owned(),
            AccountLeaseToken::new(),
            lease_until,
            state.now,
        );
        state.leases.insert(account_id, lease.clone());
        Ok(Some(lease))
    }

    async fn heartbeat(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        token: AccountLeaseToken,
        lease_for: Duration,
    ) -> Result<AccountLease, AccountLeaseError> {
        validate_inputs(account_id, owner, lease_for)?;
        let mut state = self.state.lock().await;
        let now = state.now;
        let current = state
            .leases
            .get(&account_id)
            .filter(|lease| lease.owner() == owner)
            .filter(|lease| lease.token() == token)
            .filter(|lease| lease.is_live_at(now))
            .ok_or(AccountLeaseError::LeaseLost { account_id })?;
        let lease_until = now
            .checked_add_signed(lease_for)
            .ok_or(AccountLeaseError::InvalidLeaseDuration)?;
        let renewed =
            AccountLease::from_parts(account_id, owner.to_owned(), token, lease_until, now);
        debug_assert_eq!(current.account_id(), account_id);
        state.leases.insert(account_id, renewed.clone());
        Ok(renewed)
    }

    async fn release(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        token: AccountLeaseToken,
    ) -> Result<(), AccountLeaseError> {
        validate_account_and_owner(account_id, owner)?;
        let mut state = self.state.lock().await;
        let now = state.now;
        let matches_live_lease = state.leases.get(&account_id).is_some_and(|lease| {
            lease.owner() == owner && lease.token() == token && lease.is_live_at(now)
        });
        if matches_live_lease {
            state.leases.remove(&account_id);
            Ok(())
        } else {
            Err(AccountLeaseError::LeaseLost { account_id })
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

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    #[tokio::test]
    async fn serializes_live_owners_and_fences_expired_takeover() {
        let repository = MemoryAccountLeaseRepository::new(at(0));
        let account_id = account_id();
        let first = repository
            .acquire(account_id, "worker-a", Duration::seconds(30))
            .await
            .unwrap()
            .expect("first owner should acquire the account");

        assert!(repository
            .acquire(account_id, "worker-b", Duration::seconds(30))
            .await
            .unwrap()
            .is_none());
        let renewed = repository
            .heartbeat(account_id, "worker-a", first.token(), Duration::seconds(40))
            .await
            .unwrap();
        assert_eq!(renewed.heartbeat_at(), at(0));
        assert_eq!(renewed.lease_until(), at(40));

        assert!(matches!(
            repository
                .heartbeat(account_id, "worker-b", first.token(), Duration::seconds(30),)
                .await,
            Err(AccountLeaseError::LeaseLost { .. })
        ));

        repository.set_now(at(41)).await;
        let takeover = repository
            .acquire(account_id, "worker-b", Duration::seconds(30))
            .await
            .unwrap()
            .expect("expired ownership should be recoverable");
        assert_ne!(takeover.token(), first.token());
        assert_eq!(takeover.owner(), "worker-b");
        assert!(matches!(
            repository
                .release(account_id, "worker-a", renewed.token())
                .await,
            Err(AccountLeaseError::LeaseLost { .. })
        ));
        repository
            .release(account_id, "worker-b", takeover.token())
            .await
            .unwrap();
        assert!(repository
            .acquire(account_id, "worker-a", Duration::seconds(30))
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn rejects_invalid_lease_inputs() {
        let repository = MemoryAccountLeaseRepository::new(at(0));
        assert!(matches!(
            repository
                .acquire(
                    WeReadAccountId::from_uuid(Uuid::nil()),
                    "worker-a",
                    Duration::seconds(30),
                )
                .await,
            Err(AccountLeaseError::InvalidAccountId)
        ));
        assert!(matches!(
            repository
                .acquire(account_id(), " ", Duration::seconds(30))
                .await,
            Err(AccountLeaseError::EmptyOwner)
        ));
        assert!(matches!(
            repository
                .acquire(account_id(), "worker-a", Duration::zero())
                .await,
            Err(AccountLeaseError::InvalidLeaseDuration)
        ));
    }
}
