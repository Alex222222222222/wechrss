//! Source persistence repository.
//!
//! Stores source scheduling state and feed revision today. The eventual
//! repository will also store display configuration, stable WeRead account
//! relationship, and opaque feed-token metadata. It will enforce unique
//! `book_id` values.
//!
//! The cross-table due-source reservation and job insertion operation belongs
//! to `scheduler_repository`; this repository supplies its transaction-scoped
//! source updates but must not encourage application code to compose a
//! race-prone due-source list with unrelated job inserts.
//!
//! Source changes must participate in cache invalidation and job deduplication
//! transactions. This repository does not resolve article URLs or execute
//! synchronization.

// TODO(design): add source CRUD, remaining configuration fields, stable account
// relationships, feed-token metadata, and transaction-scoped source schedule
// updates around the implemented scheduling/revision rows.
