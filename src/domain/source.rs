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

// TODO(design): define SourceId, WeReadAccountId, SchedulingGate, failure
// cooldown/reservation fields, and monotonic FeedRevision value types.
