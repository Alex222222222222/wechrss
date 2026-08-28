//! Source persistence repository.
//!
//! Stores source identity and feed revision today. The eventual repository will
//! also store display configuration, enabled state, scheduling timestamps,
//! scheduling gate, failure cooldown, stable WeRead account relationship, and
//! opaque feed-token metadata. It will enforce unique `book_id` values.
//!
//! The cross-table due-source reservation and job insertion operation belongs
//! to `scheduler_repository`; this repository supplies its transaction-scoped
//! source updates but must not encourage application code to compose a
//! race-prone due-source list with unrelated job inserts.
//!
//! Source changes must participate in cache invalidation and job deduplication
//! transactions. This repository does not resolve article URLs or execute
//! synchronization.

// TODO(design): add the remaining source configuration fields, scheduling
// gates/account relationships, and transaction-scoped schedule/reservation
// updates around the implemented feed-revision row.
