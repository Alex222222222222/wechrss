//! WeRead credential domain model.
//!
//! This module describes access tokens, refresh tokens, device identity,
//! profile metadata, account labels, and credential lifecycle state.
//!
//! Responsibilities: distinguish basic configured credentials from refreshable
//! credentials, provide a stable non-secret `WeReadAccountId`, and document
//! secret-handling and distributed account-lease invariants.
//!
//! Non-responsibilities: QR polling, credential exchange, encryption key
//! management, database persistence, or exposing login state over HTTP.
//!
//! Security: secret fields must be wrapped in secrecy-aware types, excluded
//! from logs and API serialization, and encrypted before PostgreSQL storage.

//! High availability: authenticated account use is fenced by a durable account
//! lease. The lease stores only account identity and ownership metadata, never
//! access/refresh tokens. Version one may expose one account while retaining the
//! explicit identifier required for cross-replica serialization.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identity of one configured WeRead account.
///
/// This identifier is deliberately separate from credential material and is
/// safe to use in job payloads, source relationships, logs, and lease rows.
/// The account record that owns the credentials will be added by a later
/// persistence slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WeReadAccountId(Uuid);

impl WeReadAccountId {
    /// Wraps the UUID assigned to an account.
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the durable UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for WeReadAccountId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Fencing token for one incarnation of an account lease.
///
/// A new token is generated for every acquisition, including takeover after
/// expiry. Workers must retain this token and present it for heartbeat and
/// release so a stale replica cannot mutate a later owner’s lease.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccountLeaseToken(Uuid);

impl AccountLeaseToken {
    /// Generates a token for a new lease incarnation.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps a token read from PostgreSQL.
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the UUID persisted with the lease.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for AccountLeaseToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of a currently held account lease.
///
/// Lease snapshots contain no access or refresh token. They are only the
/// capability required to heartbeat or release a distributed account lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountLease {
    account_id: WeReadAccountId,
    owner: String,
    token: AccountLeaseToken,
    lease_until: DateTime<Utc>,
    heartbeat_at: DateTime<Utc>,
}

impl AccountLease {
    /// Reconstructs a lease returned by a trusted repository.
    pub(crate) fn from_parts(
        account_id: WeReadAccountId,
        owner: String,
        token: AccountLeaseToken,
        lease_until: DateTime<Utc>,
        heartbeat_at: DateTime<Utc>,
    ) -> Self {
        Self {
            account_id,
            owner,
            token,
            lease_until,
            heartbeat_at,
        }
    }

    /// Returns the leased account identity.
    pub const fn account_id(&self) -> WeReadAccountId {
        self.account_id
    }

    /// Returns the owning application instance.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the fencing token for this lease incarnation.
    pub const fn token(&self) -> AccountLeaseToken {
        self.token
    }

    /// Returns the expiry timestamp assigned by the repository clock.
    pub const fn lease_until(&self) -> DateTime<Utc> {
        self.lease_until
    }

    /// Returns the last heartbeat or acquisition timestamp.
    pub const fn heartbeat_at(&self) -> DateTime<Utc> {
        self.heartbeat_at
    }

    /// Reports whether the lease is live at an explicitly supplied instant.
    pub fn is_live_at(&self, now: DateTime<Utc>) -> bool {
        self.lease_until > now
    }
}
