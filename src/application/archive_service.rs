//! Article archival orchestration.
//!
//! ArchiveService is the application boundary between extracted article HTML
//! and article persistence. It delegates HTML policy to the pure archive
//! sanitizer, computes a stable SHA-256 content hash over the normalized HTML,
//! and returns approved external image references for the optional asset path.
//! Version one does not download or persist binary assets; the returned HTML
//! therefore keeps approved external URLs in its src attributes.
//!
//! Responsibilities:
//!
//! - apply the one archive sanitization policy to browser-extracted HTML;
//! - treat the sanitizer's normalized output as the only hashable/persistable
//!   representation;
//! - produce a deterministic lowercase SHA-256 hash when non-empty content is
//!   available; and
//! - preserve the sanitizer's first-seen, deduplicated external image URLs for
//!   a later asset-store implementation.
//!
//! Non-responsibilities: browser navigation, source scheduling, XML rendering,
//! database writes, asset downloads, URL rewriting, or deciding whether an
//! upstream error is a job retry. The caller owns article identity and passes
//! the returned HTML/hash into the article upsert in its final UnitOfWork.
//!
//! Data flow is intentionally one-way: raw page fragment -> sanitizer ->
//! normalized HTML + external asset references -> SHA-256 -> archive result.
//! An empty sanitized result has no content hash, which lets a partial list
//! observation remain distinguishable from an article whose content was
//! successfully archived. Repeating the same input produces byte-identical
//! HTML, asset ordering, and hash, so retrying a leased job is idempotent.
//!
//! PostgreSQL/high-availability behavior belongs to the caller. This service
//! has no database or process-global state and does not coordinate replicas;
//! the article repository later compares the observation version and reports
//! whether the normalized content changed before the source feed revision is
//! advanced. RSS cache publication remains outside this service's scope.
//!
//! Optional asset archiving can consume external_assets after this service
//! returns. It must use bounded, SSRF-safe downloads and idempotent checksum
//! writes; it must not be added to the RSS request path. The current service
//! never performs that network operation, so disabled asset mode cannot create
//! an asset-download failure.

use sha2::{Digest, Sha256};
use url::Url;

use crate::archive::sanitizer::{HtmlSanitizer, SanitizedHtml};

/// Normalized article content prepared for an idempotent article upsert.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArchivedContent {
    html: String,
    content_hash: Option<String>,
    external_assets: Vec<Url>,
}

impl ArchivedContent {
    fn from_sanitized(sanitized: SanitizedHtml) -> Self {
        let html = sanitized.html().to_owned();
        let content_hash = (!html.is_empty()).then(|| sha256_hex(html.as_bytes()));

        Self {
            html,
            content_hash,
            external_assets: sanitized.external_assets().to_vec(),
        }
    }

    /// Returns the normalized, sanitized HTML to persist in the article row.
    pub fn html(&self) -> &str {
        &self.html
    }

    /// Returns the lowercase SHA-256 hash of the normalized HTML, or None when
    /// sanitization produced no content.
    pub fn content_hash(&self) -> Option<&str> {
        self.content_hash.as_deref()
    }

    /// Returns deduplicated approved external image URLs in first-seen order.
    pub fn external_assets(&self) -> &[Url] {
        &self.external_assets
    }
}

/// Stateless article archive boundary for version-one content.
#[derive(Debug, Clone, Copy)]
pub struct ArchiveService {
    sanitizer: HtmlSanitizer,
}

impl ArchiveService {
    /// Creates the default archive service.
    pub const fn new() -> Self {
        Self {
            sanitizer: HtmlSanitizer,
        }
    }

    /// Sanitizes one extracted HTML fragment and computes its content hash.
    pub fn archive(&self, raw_html: &str) -> ArchivedContent {
        ArchivedContent::from_sanitized(self.sanitizer.sanitize(raw_html))
    }
}

impl Default for ArchiveService {
    fn default() -> Self {
        Self::new()
    }
}

fn sha256_hex(value: &[u8]) -> String {
    format!("{:x}", Sha256::digest(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archives_normalized_content_with_a_deterministic_hash_and_assets() {
        let service = ArchiveService::new();
        let first = service.archive(
            r#"<p onclick="bad()">Hello</p><img src="https://cdn.example/loading.gif" data-src="https://cdn.example/article.jpg">"#,
        );
        let second = service.archive(
            r#"<p onclick="bad()">Hello</p><img src="https://cdn.example/loading.gif" data-src="https://cdn.example/article.jpg">"#,
        );

        assert_eq!(
            first.html(),
            "<p>Hello</p><img src=\"https://cdn.example/article.jpg\" />"
        );
        assert_eq!(first, second);
        assert_eq!(
            first.content_hash(),
            Some("23ecc0e6fa2feadce103aa677631a05c0c1a85254475ae47a025d70daadfc9cd")
        );
        assert_eq!(first.external_assets().len(), 1);
    }

    #[test]
    fn empty_or_fully_removed_content_has_no_hash() {
        let service = ArchiveService::default();

        let empty = service.archive("");
        let removed =
            service.archive("<script>secret()</script><img src=\"data:image/png;base64,bad\">");

        assert_eq!(empty.html(), "");
        assert_eq!(empty.content_hash(), None);
        assert_eq!(removed.html(), "");
        assert_eq!(removed.content_hash(), None);
        assert!(removed.external_assets().is_empty());
    }

    #[test]
    fn changing_normalized_content_changes_the_hash() {
        let service = ArchiveService::default();

        let first = service.archive("<p>one</p>");
        let second = service.archive("<p>two</p>");

        assert_ne!(first.content_hash(), second.content_hash());
    }
}
