//! Application orchestration layer.
//!
//! Application services coordinate domain values with repository and
//! acquisition interfaces. They own use-case sequencing and transaction
//! boundaries but do not embed SQL, CSS selectors, or HTTP response details.
//!
//! The scheduler only creates durable jobs. Workers execute claimed jobs.
//! Successful synchronization updates articles and the source feed cache;
//! administrative mutations invalidate or enqueue cache rebuilds.

pub mod archive_service;
pub mod auth_service;
pub mod job_service;
pub mod scheduler;
pub mod source_service;
pub mod sync_service;
