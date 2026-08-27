//! Source synchronization orchestration.
//!
//! This service executes a claimed source-sync job: acquire a browser session,
//! load the authenticated account context, list article references, fetch each
//! public article page without passing credentials, normalize and archive
//! content, upsert records, rebuild the source feed cache, and finish the job
//! transactionally.
//!
//! Authentication expiry permits exactly one refresh and one retry. Risk
//! control and verification states stop the workflow and update source status.
//! All writes must be idempotent so expired leases and worker crashes are safe.
//!
//! The service checks quiet hours before beginning upstream work and between
//! each request/page operation. It delegates all waits and scroll decisions to
//! the acquisition pacing policy. If quiet hours begin mid-job, the current
//! bounded operation may finish, then the job exits with a resumable outcome.
//! Credentials are scoped to the account/list acquisition step; the article
//! page adapter must not receive or require them.
//!
//! Non-responsibilities: polling due sources, implementing WebDriver commands,
//! storing raw secrets, or serving RSS requests.
