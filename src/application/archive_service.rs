//! Article archival orchestration.
//!
//! Coordinates sanitization and content hashing, and optionally performs asset
//! persistence, asset deduplication, and URL rewriting. It receives extracted
//! article content and returns normalized archive records suitable for
//! PostgreSQL and RSS. Version one must work without downloading binary
//! article assets; approved external asset URLs may remain in the sanitized
//! HTML.
//!
//! Non-responsibilities: browser navigation, source scheduling, XML rendering,
//! or deciding whether an upstream error is a job retry.
//!
//! Asset failures must be represented explicitly when optional asset archiving
//! is enabled. The policy should allow a partially archived article only when
//! the application records which assets failed and does not claim that the
//! archive is complete. When asset archiving is disabled, no asset-download
//! failure should be created because no asset download is attempted.
