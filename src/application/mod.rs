//! Application orchestration layer.
//!
//! Application services coordinate domain values with repository and
//! acquisition interfaces. They own use-case sequencing and transaction
//! boundaries but do not embed SQL, CSS selectors, or HTTP response details.
//!
//! The scheduler only creates durable jobs. Workers execute claimed jobs.
//! Successful synchronization updates articles and the source feed cache;
//! administrative mutations invalidate or enqueue cache rebuilds.
//! `FeedService` owns cached-feed delivery decisions. Final cross-repository
//! writes use the persistence `UnitOfWork`; acquisition and waits occur before
//! that short transaction begins. `FeedTokenService` owns the hash-only public
//! feed capability lifecycle; web handlers will compose it with `FeedService`.

pub mod archive_service;
pub mod auth_service;
pub mod feed_service;
pub mod feed_token_service;
pub mod job_service;
pub mod scheduler;
pub mod source_service;
pub mod sync_service;
pub mod worker;
