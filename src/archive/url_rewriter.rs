//! Archived-content URL rewriting.
//!
//! Maps approved external asset URLs to stable local media URLs after assets
//! are stored. It must preserve the original article link separately from
//! rewritten asset links.
//!
//! This module is optional in version one. If binary asset caching is disabled,
//! the archive pipeline leaves approved external asset URLs in the sanitized
//! HTML and does not invoke this rewriter.
//!
//! Responsibilities include deterministic rewriting, unresolved-asset
//! reporting, and protection against rewriting URLs outside the extracted
//! content scope. It does not sanitize HTML or serve media HTTP responses.

//! When enabled, feed-cache rebuilds consume the rewritten representation so
//! RSS clients do not need to contact the original article page for archived
//! assets. The default version-one path does not invoke this module.
