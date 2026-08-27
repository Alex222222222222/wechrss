//! PostgreSQL persistence boundary.
//!
//! Owns connection pools, migrations in the future, transaction conventions,
//! row-to-domain mapping, and repository construction. SQL must remain inside
//! this subsystem; application services should depend on repository traits.
//!
//! PostgreSQL is the source of truth for sources, articles, archive metadata,
//! credentials, jobs, sync runs, and feed cache. Transactions must group
//! article/archive changes, cache rebuilds, source status, and job completion
//! where practical.
//!
//! The first implemented persistence slices are the PostgreSQL pool factory in
//! [`postgres`] and the job repository contract/test double in
//! [`repositories::job_repository`]. Production SQL queries remain future
//! work.

pub mod postgres;
pub mod repositories;
