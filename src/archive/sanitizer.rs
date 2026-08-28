//! HTML sanitization policy.
//!
//! Defines the future allowlist for elements, attributes, styles, URL schemes,
//! and media references accepted into the archive. Scripts, event handlers,
//! unsafe URLs, frames, and active browser behavior must be removed.
//!
//! The sanitizer returns normalized HTML plus any approved external asset
//! references. Asset references may be passed to the optional asset-archive
//! pipeline, or left external for the version-one path. The sanitizer itself
//! does not download assets, write PostgreSQL rows, or render RSS.

//! Sanitization failures are content failures and must be observable in sync
//! results. The policy must prevent archived HTML from becoming an XSS vector
//! when served by the UI or embedded in RSS readers.
