//! Synchronization-run persistence repository.
//!
//! Stores one durable record of a source synchronization attempt, including
//! counts, timing, outcome classification, operator-actionable state, and a
//! secret-safe error summary. Raw credentials, database URLs, and unrestricted
//! upstream payloads must not be persisted as diagnostics.
//!
//! The transaction-scoped view participates in the shared `UnitOfWork` so the
//! successful run result, source schedule/gate, feed cache revision, and fenced
//! job completion become visible together. Failure and deferral records may use
//! a separate short unit of work after upstream activity stops.
//!
//! Non-responsibilities: classifying arbitrary errors, calculating retries,
//! acquiring account leases, rendering RSS, or deciding scheduler eligibility.

// TODO(design): define sync-run insert/update contracts and include the
// transaction-scoped repository view in UnitOfWork.
