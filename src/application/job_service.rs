//! Durable job lifecycle orchestration.
//!
//! Owns enqueue requests, active-job deduplication, claim/lease coordination,
//! heartbeat calls, retry backoff, cancellation, completion, and expired-lease
//! recovery through repository interfaces.
//! It also owns non-failure deferral: when an eligibility boundary such as
//! quiet hours begins, a running upstream job moves to `deferred` until the next
//! allowed instant without consuming its failure budget.
//!
//! Multiple instances may run these operations concurrently. PostgreSQL row
//! locks with `SKIP LOCKED` select independent jobs, while lease expiry
//! recovers work from crashed instances.
//! Claim calls receive the set of job types allowed at the current time. During
//! quiet hours, local feed rebuilds remain claimable while upstream source and
//! article work does not churn through claims.
//!
//! Non-responsibilities: executing source synchronization or deciding browser
//! selectors. Job handlers report typed outcomes back to this service.
//! Queue-only operations use the general queue port. Handler outcomes use the
//! shared `UnitOfWork` because retry exhaustion, deferral, cancellation, or
//! success may also alter source/sync/cache state. Expired recovery uses its own
//! atomic cross-table persistence operation. Production lease decisions use
//! PostgreSQL time rather than an application `now` argument.

// TODO(design): add allowed-kind claim filters and the queue/outcome/recovery
// port split before implementing a worker loop. The durable deferred state,
// separate counters, and PostgreSQL-owned lease time are implemented by the
// current job domain/repository slice.
