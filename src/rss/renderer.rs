//! Pure RSS XML renderer and cache-payload builder.
//!
//! The renderer consumes a source-scoped, already-normalized article snapshot
//! and returns a revision-carrying FeedCacheCandidate. It does not read the
//! database, contact WeChat, sanitize HTML, fetch assets, or decide cache
//! freshness. The application supplies the source revision and the exact
//! expiration instant that the cache publication should use.
//!
//! Responsibilities:
//!
//! - validate the small set of renderer-owned input invariants;
//! - produce deterministic RSS 2.0 XML with content:encoded;
//! - preserve stable review_id values as non-permalink GUIDs;
//! - delegate XML serialization/escaping to the well-tested `rss` crate; and
//! - compute a content hash and deterministic ETag from the exact XML bytes.
//!
//! Non-responsibilities: article upserts, source revision compare-and-swap,
//! feed-cache writes, browser work, URL navigation, HTML sanitization, and
//! binary asset storage. content_html must already be sanitized by
//! ArchiveService; optional local asset URLs may already have been rewritten
//! before they reach this module. The `rss` crate emits description and
//! content fields as CDATA and safely splits embedded `]]>` delimiters while
//! serializing them.
//!
//! Determinism: articles are ordered by publication time descending and then
//! review_id ascending. Input ordering therefore cannot create needless ETag
//! changes. Duplicate or empty review IDs are rejected because they would
//! violate stable RSS identity.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use rss::{extension::dublincore::DublinCoreExtension, Channel, Guid, Item};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::domain::{
    feed::FeedCacheCandidate,
    source::{FeedRevision, SourceId},
};

/// A normalized article row prepared for RSS rendering.
///
/// The HTML is expected to have passed through the archive sanitizer. This
/// type deliberately keeps the original URL optional because list acquisition
/// can discover an article before its URL has been backfilled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderArticle {
    /// Stable upstream article identity used as the RSS GUID.
    pub review_id: String,
    /// Human-readable article title.
    pub title: String,
    /// Optional author displayed as dc:creator.
    pub author: Option<String>,
    /// Optional plain-text or normalized summary.
    pub summary: Option<String>,
    /// Original public WeChat URL, when known.
    pub original_url: Option<String>,
    /// Publication timestamp used for ordering and RSS pubDate.
    pub published_at: DateTime<Utc>,
    /// Sanitized, optionally asset-rewritten article HTML.
    pub content_html: String,
}

/// All normalized values needed for one feed render.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderFeedInput {
    /// Source owning the feed and candidate.
    pub source_id: SourceId,
    /// Feed title.
    pub title: String,
    /// Public feed URL used for the RSS channel link.
    pub feed_url: String,
    /// Feed-level description.
    pub description: String,
    /// Source revision represented by this snapshot.
    pub source_revision: FeedRevision,
    /// Timestamp assigned by the application for this render.
    pub generated_at: DateTime<Utc>,
    /// Timestamp at which the resulting cache candidate expires.
    pub expires_at: DateTime<Utc>,
    /// Normalized articles included in the feed.
    pub articles: Vec<RenderArticle>,
}

/// A rendered RSS document ready for fenced cache publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedFeed {
    candidate: FeedCacheCandidate,
}

impl RenderedFeed {
    /// Returns the candidate that can be published through the feed-cache CAS
    /// repository.
    pub fn candidate(&self) -> &FeedCacheCandidate {
        &self.candidate
    }

    /// Consumes the rendered result and returns its cache candidate.
    pub fn into_candidate(self) -> FeedCacheCandidate {
        self.candidate
    }
}

/// Errors raised before a feed candidate can be constructed.
#[derive(Debug, Error)]
pub enum RenderError {
    /// A source UUID must identify a real source.
    #[error("feed source id must not be nil")]
    InvalidSourceId,
    /// A required feed metadata value was empty.
    #[error("feed {field} must not be empty")]
    EmptyField {
        /// Name of the missing field.
        field: &'static str,
    },
    /// A review ID appeared more than once in the same feed snapshot.
    #[error("duplicate article review_id {review_id:?}")]
    DuplicateReviewId {
        /// Duplicated stable article identity.
        review_id: String,
    },
    /// The cache candidate would violate its time invariant.
    #[error("feed expiry must be later than its generation time")]
    InvalidExpiry,
    /// A string contains a character that XML 1.0 cannot represent.
    #[error("feed {field} contains an invalid XML character")]
    InvalidXmlCharacter {
        /// Field containing the invalid character.
        field: &'static str,
    },
    /// The RSS crate failed to serialize the channel.
    #[error("RSS XML rendering failed")]
    Serialization(#[source] rss::Error),
}

/// Stateless renderer for normalized RSS input.
#[derive(Debug, Default, Clone, Copy)]
pub struct RssRenderer;

impl RssRenderer {
    /// Renders one deterministic cache candidate.
    pub fn render(&self, mut input: RenderFeedInput) -> Result<RenderedFeed, RenderError> {
        let source_id = input.source_id;
        let article_count = input.articles.len();
        tracing::debug!(
            source_id = %source_id,
            article_count,
            source_revision = %input.source_revision,
            "rendering RSS feed"
        );
        let result = (|| {
            validate_input(&input)?;
            input.articles.sort_by(|left, right| {
                right
                    .published_at
                    .cmp(&left.published_at)
                    .then_with(|| left.review_id.cmp(&right.review_id))
            });

            let mut channel = Channel::default();
            channel.set_title(input.title);
            channel.set_link(input.feed_url);
            channel.set_description(input.description);
            channel.set_items(input.articles.iter().map(to_rss_item).collect::<Vec<_>>());
            let xml_bytes = channel
                .write_to(Vec::new())
                .map_err(RenderError::Serialization)?;
            let digest = Sha256::digest(&xml_bytes);
            let content_hash = format!("{digest:x}");
            let etag = format!("sha256:{content_hash}");

            Ok(RenderedFeed {
                candidate: FeedCacheCandidate::from_parts(
                    input.source_id,
                    xml_bytes,
                    etag,
                    input.generated_at,
                    input.expires_at,
                    input.source_revision,
                    content_hash,
                ),
            })
        })();
        match &result {
            Ok(rendered) => tracing::debug!(
                source_id = %source_id,
                article_count,
                output_bytes = rendered.candidate.xml_bytes().len(),
                "RSS feed rendered"
            ),
            Err(error) => tracing::warn!(
                source_id = %source_id,
                article_count,
                error = %error,
                "RSS feed rendering failed"
            ),
        }
        result
    }
}

fn to_rss_item(article: &RenderArticle) -> Item {
    let mut item = Item::default();
    item.set_title(article.title.clone());

    let mut guid = Guid::default();
    guid.set_value(article.review_id.clone());
    guid.set_permalink(false);
    item.set_guid(guid);

    if let Some(author) = non_empty(article.author.as_deref()) {
        let mut dublin_core = DublinCoreExtension::default();
        dublin_core.set_creators(vec![author.to_owned()]);
        item.set_dublin_core_ext(dublin_core);
    }
    if let Some(summary) = non_empty(article.summary.as_deref()) {
        item.set_description(summary.to_owned());
    }
    if let Some(original_url) = non_empty(article.original_url.as_deref()) {
        item.set_link(original_url.to_owned());
    }
    item.set_pub_date(article.published_at.to_rfc2822());
    item.set_content(article.content_html.clone());
    item
}

fn validate_input(input: &RenderFeedInput) -> Result<(), RenderError> {
    if input.source_id.as_uuid().is_nil() {
        return Err(RenderError::InvalidSourceId);
    }
    validate_required_text("title", &input.title)?;
    validate_required_text("feed_url", &input.feed_url)?;
    validate_xml_text("title", &input.title)?;
    validate_xml_text("feed_url", &input.feed_url)?;
    validate_xml_text("description", &input.description)?;
    if input.expires_at <= input.generated_at {
        return Err(RenderError::InvalidExpiry);
    }

    let mut review_ids = HashSet::with_capacity(input.articles.len());
    for article in &input.articles {
        validate_required_text("article review_id", &article.review_id)?;
        validate_required_text("article title", &article.title)?;
        validate_xml_text("article review_id", &article.review_id)?;
        validate_xml_text("article title", &article.title)?;
        validate_xml_text("article content_html", &article.content_html)?;
        if let Some(author) = article.author.as_deref() {
            validate_xml_text("article author", author)?;
        }
        if let Some(summary) = article.summary.as_deref() {
            validate_xml_text("article summary", summary)?;
        }
        if let Some(original_url) = article.original_url.as_deref() {
            validate_xml_text("article original_url", original_url)?;
        }
        if !review_ids.insert(article.review_id.as_str()) {
            return Err(RenderError::DuplicateReviewId {
                review_id: article.review_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_required_text(field: &'static str, value: &str) -> Result<(), RenderError> {
    if value.trim().is_empty() {
        Err(RenderError::EmptyField { field })
    } else {
        Ok(())
    }
}

fn validate_xml_text(field: &'static str, value: &str) -> Result<(), RenderError> {
    if value.chars().any(|character| {
        !matches!(
            character as u32,
            0x9 | 0xA | 0xD | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x10000..=0x10FFFF
        )
    }) {
        Err(RenderError::InvalidXmlCharacter { field })
    } else {
        Ok(())
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rss::Channel;
    use uuid::Uuid;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("test timestamp should be valid")
    }

    fn input(articles: Vec<RenderArticle>) -> RenderFeedInput {
        RenderFeedInput {
            source_id: SourceId::from_uuid(Uuid::from_u128(1)),
            title: "Example & Feed".to_owned(),
            feed_url: "https://rss.example.test/feed?token=a&b=c".to_owned(),
            description: "A <safe> description".to_owned(),
            source_revision: FeedRevision::from_u64(4),
            generated_at: timestamp(100),
            expires_at: timestamp(200),
            articles,
        }
    }

    fn article(review_id: &str, published_at: i64, title: &str) -> RenderArticle {
        RenderArticle {
            review_id: review_id.to_owned(),
            title: title.to_owned(),
            author: Some("Author & Co".to_owned()),
            summary: Some("Summary <text>".to_owned()),
            original_url: Some("https://mp.weixin.qq.com/s/example?a=1&b=2".to_owned()),
            published_at: timestamp(published_at),
            content_html: "<p>Body & <strong>markup</strong></p>".to_owned(),
        }
    }

    #[test]
    fn renders_escaped_rss_with_stable_guid_and_content() {
        let rendered = RssRenderer
            .render(input(vec![article("review-1", 110, "Title <one>")]))
            .expect("render should succeed");
        let xml = String::from_utf8(rendered.candidate().xml_bytes().to_vec())
            .expect("renderer should emit UTF-8");

        assert!(xml.contains("xmlns:content=\"http://purl.org/rss/1.0/modules/content/\""));
        assert!(xml.contains("<guid isPermaLink=\"false\">review-1</guid>"));
        assert!(xml.contains("<title>Title &lt;one&gt;</title>"));
        assert!(xml.contains(
            "<content:encoded><![CDATA[<p>Body & <strong>markup</strong></p>]]></content:encoded>"
        ));
        assert!(xml.contains("<link>https://mp.weixin.qq.com/s/example?a=1&amp;b=2</link>"));
        assert!(rendered.candidate().etag().starts_with("sha256:"));
        assert_eq!(rendered.candidate().content_hash().len(), 64);
        let channel = Channel::read_from(xml.as_bytes()).expect("rendered XML should parse");
        let item = channel
            .items()
            .first()
            .expect("one item should be rendered");
        assert_eq!(
            item.content(),
            Some("<p>Body & <strong>markup</strong></p>")
        );
        assert_eq!(item.guid().map(Guid::value), Some("review-1"));
    }

    #[test]
    fn splits_cdata_terminators_without_changing_article_content() {
        let mut article = article("review-1", 110, "Title");
        article.content_html = "<p>before ]]> after</p>".to_owned();
        let rendered = RssRenderer
            .render(input(vec![article]))
            .expect("CDATA terminators should be normalized");
        let xml = rendered.candidate().xml_bytes();
        let channel = Channel::read_from(xml).expect("normalized XML should parse");
        assert_eq!(
            channel.items()[0].content(),
            Some("<p>before ]]> after</p>")
        );
    }

    #[test]
    fn render_order_and_hash_do_not_depend_on_input_order() {
        let first = RssRenderer
            .render(input(vec![
                article("review-b", 110, "Second"),
                article("review-a", 120, "First"),
            ]))
            .expect("first render should succeed");
        let second = RssRenderer
            .render(input(vec![
                article("review-a", 120, "First"),
                article("review-b", 110, "Second"),
            ]))
            .expect("second render should succeed");

        assert_eq!(first.candidate(), second.candidate());
        let xml = String::from_utf8(first.candidate().xml_bytes().to_vec())
            .expect("renderer should emit UTF-8");
        assert!(
            xml.find("<guid isPermaLink=\"false\">review-a</guid>")
                < xml.find("<guid isPermaLink=\"false\">review-b</guid>")
        );
    }

    #[test]
    fn rejects_duplicate_ids_invalid_expiry_and_invalid_xml() {
        let duplicate = RssRenderer.render(input(vec![
            article("review-1", 110, "One"),
            article("review-1", 100, "Two"),
        ]));
        assert!(matches!(
            duplicate,
            Err(RenderError::DuplicateReviewId { .. })
        ));

        let mut expired = input(Vec::new());
        expired.expires_at = expired.generated_at;
        assert!(matches!(
            RssRenderer.render(expired),
            Err(RenderError::InvalidExpiry)
        ));

        let mut invalid_xml = input(Vec::new());
        invalid_xml.title.push('\u{0}');
        assert!(matches!(
            RssRenderer.render(invalid_xml),
            Err(RenderError::InvalidXmlCharacter { field: "title" })
        ));
    }

    #[test]
    fn rejects_missing_required_metadata() {
        let mut missing_title = input(Vec::new());
        missing_title.title = "  ".to_owned();
        assert!(matches!(
            RssRenderer.render(missing_title),
            Err(RenderError::EmptyField { field: "title" })
        ));

        let missing_review_id = input(vec![article("", 110, "Title")]);
        assert!(matches!(
            RssRenderer.render(missing_review_id),
            Err(RenderError::EmptyField {
                field: "article review_id"
            })
        ));
    }
}
