//! Source domain model.
//!
//! A source represents one subscribed WeChat public account. It contains the
//! normalized `book_id`, display name, originating article URL, enabled state,
//! sync interval, RSS item limit, stable WeRead account relationship, monotonic
//! feed revision, scheduling timestamps, and scheduling gate.
//!
//! Responsibilities: document source identity, validation bounds, lifecycle
//! state, and the relationship between a source mutation and feed-cache
//! invalidation.
//!
//! Non-responsibilities: source persistence, URL resolution, job insertion,
//! browser access, and HTTP form validation.
//!
//! High availability: source scheduling data is persisted in PostgreSQL and
//! must not rely on one process's memory. Changes should enqueue or invalidate
//! work through application services.
//!
//! The scheduling gate is one of `ready`, `authentication_required`, or
//! `risk_controlled`; `enabled=false` is the operator-controlled pause. Only
//! enabled, ready, due sources are automatically enqueued. Feed-visible changes
//! increment the source revision atomically with their persistence.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Stable identity of one subscribed source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceId(Uuid);

impl SourceId {
    /// Wraps the UUID assigned to a source.
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the durable UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Fencing token for one feed-build lease incarnation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FeedBuildLeaseToken(Uuid);

impl FeedBuildLeaseToken {
    /// Generates a token for a new feed-build lease incarnation.
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

impl Default for FeedBuildLeaseToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of a currently held per-source feed-build lease.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedBuildLease {
    source_id: SourceId,
    owner: String,
    token: FeedBuildLeaseToken,
    lease_until: DateTime<Utc>,
    heartbeat_at: DateTime<Utc>,
}

impl FeedBuildLease {
    /// Reconstructs a lease returned by a trusted repository.
    pub(crate) fn from_parts(
        source_id: SourceId,
        owner: String,
        token: FeedBuildLeaseToken,
        lease_until: DateTime<Utc>,
        heartbeat_at: DateTime<Utc>,
    ) -> Self {
        Self {
            source_id,
            owner,
            token,
            lease_until,
            heartbeat_at,
        }
    }

    /// Returns the source whose cache is being built.
    pub const fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// Returns the owning application instance.
    pub fn owner(&self) -> &str {
        &self.owner
    }

    /// Returns the fencing token for this lease incarnation.
    pub const fn token(&self) -> FeedBuildLeaseToken {
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

// TODO(design): define the complete Source aggregate, SchedulingGate, failure
// cooldown/reservation fields, and monotonic FeedRevision value types.
