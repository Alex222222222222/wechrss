//! Browser-session capacity and ownership policy.
//!
//! This module owns the process-local browser capacity limit and the durable
//! account-lease capability required by authenticated work. It exposes two
//! non-interchangeable session types through [`super::webdriver`]: a clean
//! public session for article pages and an authenticated session tied to one
//! fenced WeRead account lease.
//!
//! Public sessions are obtained from a Tokio semaphore and carry no account
//! identity, credentials, cookies, or lease guard. Authenticated sessions are
//! obtained only after an [`AccountLeaseGuard`] is acquired. A failed heartbeat
//! marks that guard unusable, so the authenticated adapter can stop before its
//! next upstream request. The guard is intentionally non-cloneable; callers
//! must explicitly release it when work finishes.
//!
//! High availability: the semaphore limits only one process. PostgreSQL job
//! leases prevent duplicate source work, while the repository-backed account
//! lease serializes authenticated use of one account across replicas. The
//! browser pool is not a distributed lock and must not replace either durable
//! lease.
//!
//! Non-responsibilities: WebDriver commands, navigation, article parsing,
//! source scheduling, credential persistence, or RSS caching. Browser-side
//! timezone enforcement and health checks remain TODOs in
//! [`super::webdriver`]; pacing and scroll orchestration belong to
//! [`super::pacing`] and [`super::article_page`].

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use chrono::Duration;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::domain::credentials::{AccountLease, AccountLeaseToken, WeReadAccountId};

use super::webdriver::{AuthenticatedBrowserSession, PublicBrowserSession};

/// Errors exposed by the storage-neutral account-lease port.
#[derive(Debug, Error)]
pub enum AccountLeaseError {
    /// A nil UUID cannot identify a configured account.
    #[error("account id must not be nil")]
    InvalidAccountId,
    /// Lease ownership must identify an application instance.
    #[error("account lease owner must not be empty")]
    EmptyOwner,
    /// A lease must be positive and representable by the lease backend.
    #[error("account lease duration must be positive and fit in milliseconds")]
    InvalidLeaseDuration,
    /// The current owner or fencing token no longer controls the lease.
    #[error("account lease for {account_id} is no longer owned by this claim")]
    LeaseLost { account_id: WeReadAccountId },
    /// The lease backend could not complete the operation.
    #[error("account lease backend error: {0}")]
    Backend(String),
}

/// Storage-neutral account-lease operations required by authenticated work.
///
/// PostgreSQL, the deterministic test repository, and a future alternative
/// lease backend can implement this port. Production implementations must use
/// their authoritative clock for expiry-sensitive operations; callers never
/// provide a wall-clock timestamp.
#[allow(async_fn_in_trait)]
pub trait AccountLeaseStore: Send + Sync {
    /// Acquires a lease or returns `None` when another live owner holds it.
    async fn acquire(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<AccountLease>, AccountLeaseError>;

    /// Extends a lease only when owner, token, and lease liveness all match.
    async fn heartbeat(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        token: AccountLeaseToken,
        lease_for: Duration,
    ) -> Result<AccountLease, AccountLeaseError>;

    /// Releases a live lease only for its current owner and fencing token.
    async fn release(
        &self,
        account_id: WeReadAccountId,
        owner: &str,
        token: AccountLeaseToken,
    ) -> Result<(), AccountLeaseError>;
}

/// Errors raised while acquiring local browser capacity or an account lease.
#[derive(Debug, Error)]
pub enum BrowserPoolError {
    /// The configured capacity cannot be zero.
    #[error("browser pool capacity must be greater than zero")]
    InvalidCapacity,
    /// The semaphore was closed before a session could be acquired.
    #[error("browser pool is closed")]
    Closed,
    /// The distributed account lease could not be acquired.
    #[error(transparent)]
    AccountLease(#[from] AccountLeaseError),
}

/// A fenced, non-cloneable lease for one authenticated WeRead account.
///
/// The repository controls ownership and expiry. This guard only carries the
/// returned account ID, owner, fencing token, and a local cancellation bit.
/// `heartbeat` must be called before its lease expires. Any heartbeat error
/// cancels the guard, including a transient storage error, because the caller
/// can no longer prove that it owns the account.
pub struct AccountLeaseGuard<R> {
    repository: R,
    lease: AccountLease,
    cancelled: Arc<AtomicBool>,
}

impl<R> AccountLeaseGuard<R>
where
    R: AccountLeaseStore,
{
    /// Acquires an account lease and returns `None` when another owner is live.
    pub async fn acquire(
        repository: R,
        account_id: WeReadAccountId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<Self>, AccountLeaseError> {
        let Some(lease) = repository.acquire(account_id, owner, lease_for).await? else {
            return Ok(None);
        };
        Ok(Some(Self {
            repository,
            lease,
            cancelled: Arc::new(AtomicBool::new(false)),
        }))
    }

    /// Returns the stable account identity protected by this guard.
    pub const fn account_id(&self) -> WeReadAccountId {
        self.lease.account_id()
    }

    /// Returns the application instance that owns the lease.
    pub fn owner(&self) -> &str {
        self.lease.owner()
    }

    /// Returns the fencing token for this lease incarnation.
    pub const fn token(&self) -> AccountLeaseToken {
        self.lease.token()
    }

    /// Reports whether a failed heartbeat has made the guard unusable.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Fails if the lease was already lost or a heartbeat previously failed.
    pub fn ensure_usable(&self) -> Result<(), AccountLeaseError> {
        if self.is_cancelled() {
            Err(AccountLeaseError::LeaseLost {
                account_id: self.account_id(),
            })
        } else {
            Ok(())
        }
    }

    /// Extends the lease using the repository's authoritative clock.
    ///
    /// A failed heartbeat permanently cancels this guard. The authenticated
    /// adapter must call [`Self::ensure_usable`] before each request and stop
    /// when it reports lease loss.
    pub async fn heartbeat(&mut self, lease_for: Duration) -> Result<(), AccountLeaseError> {
        self.ensure_usable()?;
        let account_id = self.account_id();
        let owner = self.owner().to_owned();
        let token = self.token();
        match self
            .repository
            .heartbeat(account_id, &owner, token, lease_for)
            .await
        {
            Ok(lease) => {
                self.lease = lease;
                Ok(())
            }
            Err(error) => {
                self.cancelled.store(true, Ordering::Release);
                Err(error)
            }
        }
    }

    /// Releases this lease through the repository.
    ///
    /// This is explicit because Rust `Drop` cannot await a database call. If a
    /// guard is dropped without release, its durable lease remains protected
    /// by its expiry and can be recovered by another worker after expiration.
    pub async fn release(self) -> Result<(), AccountLeaseError> {
        let Self {
            repository,
            lease,
            cancelled,
        } = self;
        cancelled.store(true, Ordering::Release);
        repository
            .release(lease.account_id(), lease.owner(), lease.token())
            .await
    }

    pub(crate) fn into_session(
        self,
        permit: OwnedSemaphorePermit,
    ) -> AuthenticatedBrowserSession<R> {
        AuthenticatedBrowserSession::from_permit(permit, self)
    }
}

impl<R> std::fmt::Debug for AccountLeaseGuard<R>
where
    R: AccountLeaseStore,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AccountLeaseGuard")
            .field("account_id", &self.account_id())
            .field("owner", &self.owner())
            .field("token", &"<fencing token>")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// Process-local limit for browser sessions.
#[derive(Clone, Debug)]
pub struct BrowserPool {
    permits: Arc<Semaphore>,
    capacity: usize,
}

impl BrowserPool {
    /// Creates a pool with a positive maximum number of sessions.
    pub fn new(capacity: usize) -> Result<Self, BrowserPoolError> {
        if capacity == 0 {
            return Err(BrowserPoolError::InvalidCapacity);
        }
        Ok(Self {
            permits: Arc::new(Semaphore::new(capacity)),
            capacity,
        })
    }

    /// Returns the configured process-local session capacity.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Acquires a clean, unauthenticated public browser capability.
    pub async fn open_public(&self) -> Result<PublicBrowserSession, BrowserPoolError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BrowserPoolError::Closed)?;
        Ok(PublicBrowserSession::from_permit(permit))
    }

    /// Acquires local browser capacity and a distributed account lease.
    ///
    /// Local capacity is acquired before the account lease. This prevents a
    /// worker from holding a durable account lock while blocked on its own
    /// browser pool. If account acquisition fails or another owner is live,
    /// the local permit is released with the returned result.
    pub async fn open_authenticated<R>(
        &self,
        repository: R,
        account_id: WeReadAccountId,
        owner: &str,
        lease_for: Duration,
    ) -> Result<Option<AuthenticatedBrowserSession<R>>, BrowserPoolError>
    where
        R: AccountLeaseStore,
    {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| BrowserPoolError::Closed)?;
        let Some(guard) =
            AccountLeaseGuard::acquire(repository, account_id, owner, lease_for).await?
        else {
            return Ok(None);
        };
        Ok(Some(guard.into_session(permit)))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use uuid::Uuid;

    use super::*;
    use crate::persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository;

    fn at(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    #[test]
    fn rejects_zero_capacity() {
        assert!(matches!(
            BrowserPool::new(0),
            Err(BrowserPoolError::InvalidCapacity)
        ));
    }

    #[tokio::test]
    async fn public_session_holds_one_permit_until_drop() {
        let pool = BrowserPool::new(1).unwrap();
        let first = pool.open_public().await.unwrap();
        assert_eq!(pool.capacity(), 1);

        let blocked =
            tokio::time::timeout(std::time::Duration::from_millis(10), pool.open_public()).await;
        assert!(
            blocked.is_err(),
            "the first session should consume capacity"
        );

        drop(first);
        let second = pool.open_public().await.unwrap();
        assert_ne!(second.session_id(), Uuid::nil());
    }

    #[tokio::test]
    async fn failed_heartbeat_cancels_authenticated_capability() {
        let repository = MemoryAccountLeaseRepository::new(at(0));
        let mut guard = AccountLeaseGuard::acquire(
            repository.clone(),
            account_id(),
            "worker-a",
            Duration::seconds(10),
        )
        .await
        .unwrap()
        .unwrap();
        repository.set_now(at(11)).await;

        assert!(matches!(
            guard.heartbeat(Duration::seconds(10)).await,
            Err(AccountLeaseError::LeaseLost { .. })
        ));
        assert!(guard.is_cancelled());
        assert!(matches!(
            guard.ensure_usable(),
            Err(AccountLeaseError::LeaseLost { .. })
        ));
    }
}
