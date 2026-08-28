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

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::credentials::WeReadAccountId;

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

/// Monotonic revision of the feed-visible state for one source.
///
/// The value is persisted as a non-negative PostgreSQL `BIGINT`. A revision
/// changes only when the normalized source/article data can change RSS output;
/// idempotent retries must reuse the current value. It is intentionally kept
/// separate from timestamps because timestamps cannot fence two concurrent
/// rebuilds reliably.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FeedRevision(u64);

impl FeedRevision {
    /// Returns the initial revision for a newly created source.
    pub const fn zero() -> Self {
        Self(0)
    }

    /// Wraps a persisted or otherwise trusted revision value.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric value used by domain comparisons and persistence.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Advances the revision, returning `None` only at the numeric limit.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Scheduling gate persisted with a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulingGate {
    /// Automatic synchronization is allowed when the source is due.
    Ready,
    /// Authentication must be repaired before automatic synchronization.
    AuthenticationRequired,
    /// An operator must clear a risk-control condition before resuming work.
    RiskControlled,
}

impl SchedulingGate {
    /// Returns whether the source can be selected by the scheduler.
    pub const fn is_automatically_eligible(self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Returns the stable value stored in PostgreSQL.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::AuthenticationRequired => "authentication_required",
            Self::RiskControlled => "risk_controlled",
        }
    }
}

/// Normalized source aggregate used by source and scheduler application ports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    id: SourceId,
    book_id: String,
    display_name: String,
    article_url: String,
    enabled: bool,
    sync_interval: Duration,
    rss_item_limit: u32,
    account_id: Option<WeReadAccountId>,
    scheduling_gate: SchedulingGate,
    feed_revision: FeedRevision,
    next_fetch_at: DateTime<Utc>,
    failure_cooldown_until: Option<DateTime<Utc>>,
    schedule_reserved_until: Option<DateTime<Utc>>,
}

impl Source {
    /// Reconstructs a source returned by a trusted repository.
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub(crate) fn from_parts(
        id: SourceId,
        book_id: String,
        display_name: String,
        article_url: String,
        enabled: bool,
        sync_interval: Duration,
        rss_item_limit: u32,
        account_id: Option<WeReadAccountId>,
        scheduling_gate: SchedulingGate,
        feed_revision: FeedRevision,
        next_fetch_at: DateTime<Utc>,
        failure_cooldown_until: Option<DateTime<Utc>>,
        schedule_reserved_until: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            id,
            book_id,
            display_name,
            article_url,
            enabled,
            sync_interval,
            rss_item_limit,
            account_id,
            scheduling_gate,
            feed_revision,
            next_fetch_at,
            failure_cooldown_until,
            schedule_reserved_until,
        }
    }

    /// Returns the stable source identity.
    pub const fn id(&self) -> SourceId {
        self.id
    }

    /// Returns the normalized WeRead book identifier.
    pub fn book_id(&self) -> &str {
        &self.book_id
    }

    /// Returns the display name.
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Returns the source article URL used for identity acquisition.
    pub fn article_url(&self) -> &str {
        &self.article_url
    }

    /// Returns whether automatic synchronization is enabled.
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// Returns the configured interval between successful source fetches.
    pub const fn sync_interval(&self) -> Duration {
        self.sync_interval
    }

    /// Returns the maximum number of articles rendered into the feed.
    pub const fn rss_item_limit(&self) -> u32 {
        self.rss_item_limit
    }

    /// Returns the stable account used for authenticated list acquisition.
    pub const fn account_id(&self) -> Option<WeReadAccountId> {
        self.account_id
    }

    /// Returns the persisted scheduling gate.
    pub const fn scheduling_gate(&self) -> SchedulingGate {
        self.scheduling_gate
    }

    /// Returns the feed-visible revision.
    pub const fn feed_revision(&self) -> FeedRevision {
        self.feed_revision
    }

    /// Returns the next automatic synchronization eligibility time.
    pub const fn next_fetch_at(&self) -> DateTime<Utc> {
        self.next_fetch_at
    }

    /// Returns the optional cooldown expiry after ordinary failures.
    pub const fn failure_cooldown_until(&self) -> Option<DateTime<Utc>> {
        self.failure_cooldown_until
    }

    /// Returns the scheduler's short reservation expiry.
    pub const fn schedule_reserved_until(&self) -> Option<DateTime<Utc>> {
        self.schedule_reserved_until
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_ready_sources_are_automatically_eligible() {
        assert!(SchedulingGate::Ready.is_automatically_eligible());
        assert!(!SchedulingGate::AuthenticationRequired.is_automatically_eligible());
        assert!(!SchedulingGate::RiskControlled.is_automatically_eligible());
    }

    #[test]
    fn scheduling_gate_values_are_stable_database_values() {
        assert_eq!(SchedulingGate::Ready.as_str(), "ready");
        assert_eq!(
            SchedulingGate::AuthenticationRequired.as_str(),
            "authentication_required"
        );
        assert_eq!(SchedulingGate::RiskControlled.as_str(), "risk_controlled");
    }
}
