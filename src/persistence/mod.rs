//! PostgreSQL persistence boundary.
//!
//! Owns connection pools, migrations, transaction conventions,
//! row-to-domain mapping, and repository construction. SQL must remain inside
//! this subsystem; application services should depend on repository traits.
//!
//! PostgreSQL is the source of truth for sources, articles, archive metadata,
//! credentials, jobs, sync runs, and feed cache. Transactions must group
//! article/archive changes, cache rebuilds, source status, and job completion
//! through [`unit_of_work`]. Browser/network work never runs inside that
//! transaction.
//!
//! The first implemented persistence slices are the PostgreSQL pool/migration
//! helpers in [`postgres`], the shared transaction owner in [`unit_of_work`],
//! the job repository contract plus SQLx and memory implementations in
//! [`repositories::job_repository`], and the PostgreSQL/memory account lease
//! repository in [`repositories::account_lease_repository`] plus the durable
//! feed-build lease and revision-aware feed-cache repository in
//! [`repositories::feed_cache_repository`]. The PostgreSQL scheduler repository
//! in [`repositories::scheduler_repository`] now atomically enqueues due source
//! jobs and records source reservations. Source CRUD, article, sync-run,
//! credential, and archive repository SQL remain future work.

pub mod postgres;
pub mod repositories;
pub mod unit_of_work;
