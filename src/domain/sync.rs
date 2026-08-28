//! Synchronization result and status model.
//!
//! This module describes sync runs, received/new article counts, archive and
//! optional asset outcomes, timestamps, and typed error classifications.
//!
//! Responsibilities: distinguish successful, running, authentication-expired,
//! risk-control, blocked, retryable, deferred, and failed outcomes. Risk-control
//! is an operator-actionable stop state, not a reason to refresh or retry
//! forever. Deferred is a non-failure quiet-hours outcome and does not consume
//! the retry failure budget.
//!
//! Non-responsibilities: executing syncs, writing `sync_runs`, scheduling the
//! next job, or rebuilding RSS XML.
//!
//! Cache interactions: a successful content mutation should result in a feed
//! cache rebuild before the corresponding job is committed as successful.

// TODO(design): define typed DeferredUntil, scheduling-gate effects, and the
// final UnitOfWork command produced by each sync outcome.
