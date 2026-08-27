//! Source synchronization orchestration.
//!
//! This service executes a claimed source-sync job: acquire a browser session,
//! load account context, list articles, fetch rendered pages, normalize and
//! archive content, upsert records, rebuild the source feed cache, and finish
//! the job transactionally.
//!
//! Authentication expiry permits exactly one refresh and one retry. Risk
//! control and verification states stop the workflow and update source status.
//! All writes must be idempotent so expired leases and worker crashes are safe.
//!
//! The service checks quiet hours before beginning upstream work and between
//! each request/page operation. It delegates all waits and scroll decisions to
//! the acquisition pacing policy. If quiet hours begin mid-job, the current
//! bounded operation may finish, then the job exits with a resumable outcome.
//!
//! Non-responsibilities: polling due sources, implementing WebDriver commands,
//! storing raw secrets, or serving RSS requests.
