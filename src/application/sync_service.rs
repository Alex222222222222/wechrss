//! Source synchronization orchestration.
//!
//! This service executes a claimed source-sync job. It acquires the distributed
//! WeRead account lease for authenticated article-list and URL-recovery work,
//! then releases that account session before fetching public article pages in
//! clean, ephemeral browser sessions without credentials.
//!
//! Browser acquisition, waits, and normalization happen outside database
//! transactions while an independent heartbeat task maintains the job and, when
//! needed, account leases. After acquisition, the service renders a candidate
//! feed outside the transaction by merging normalized changes with current RSS
//! input. A short persistence `UnitOfWork` verifies the job fencing token and
//! expected base revision, upserts records, advances the source feed revision,
//! stores the matching candidate, records the sync result and next schedule,
//! and completes the job atomically. Revision conflict discards the candidate
//! and retries from a fresh snapshot.
//!
//! Authentication expiry permits exactly one refresh and one retry. Risk
//! control and verification states stop the workflow and update source status.
//! All writes must be idempotent so expired leases and worker crashes are safe.
//!
//! The service checks quiet hours before beginning upstream work and between
//! each request/page operation. It delegates all waits and scroll decisions to
//! the acquisition pacing policy. If quiet hours begin mid-job, the current
//! bounded operation may finish, then the job exits with a non-failure
//! `deferred` outcome whose `run_after` is the next allowed instant.
//! Credentials are scoped to the account/list acquisition step; the article
//! page adapter must not receive or require them.
//!
//! Non-responsibilities: polling due sources, implementing WebDriver commands,
//! storing raw secrets, or serving RSS requests.

// TODO(design): define the SyncService ports, heartbeat cancellation contract,
// account-lease scope, deferred outcome, and final UnitOfWork command before
// adding browser or repository behavior.
