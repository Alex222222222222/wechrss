//! Article archival orchestration.
//!
//! Coordinates sanitization, content hashing, asset persistence, asset
//! deduplication, and URL rewriting. It receives extracted article content and
//! returns normalized archive records suitable for PostgreSQL and RSS.
//!
//! Non-responsibilities: browser navigation, source scheduling, XML rendering,
//! or deciding whether an upstream error is a job retry.
//!
//! Asset failures must be represented explicitly. The policy should allow a
//! partially archived article only when the application records which assets
//! failed and does not claim that the archive is complete.
