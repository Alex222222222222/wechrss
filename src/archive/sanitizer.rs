//! HTML sanitization policy.
//!
//! Defines the future allowlist for elements, attributes, styles, URL schemes,
//! and media references accepted into the archive. Scripts, event handlers,
//! unsafe URLs, frames, and active browser behavior must be removed.
//!
//! The sanitizer returns normalized HTML plus the external asset references
//! that the archive pipeline must process. It does not download assets, write
//! PostgreSQL rows, or render RSS.

//! Sanitization failures are content failures and must be observable in sync
//! results. The policy must prevent archived HTML from becoming an XSS vector
//! when served by the UI or embedded in RSS readers.
