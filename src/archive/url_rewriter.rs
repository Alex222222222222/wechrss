//! Archived-content URL rewriting.
//!
//! Maps approved external asset URLs to stable local media URLs after assets
//! are stored. It must preserve the original article link separately from
//! rewritten asset links.
//!
//! Responsibilities include deterministic rewriting, unresolved-asset
//! reporting, and protection against rewriting URLs outside the extracted
//! content scope. It does not sanitize HTML or serve media HTTP responses.

//! Feed-cache rebuilds consume the rewritten representation so RSS clients do
//! not need to contact the original article page for archived assets.
