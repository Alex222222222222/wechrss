//! WeChat public-account identity resolution.
//!
//! A short `mp.weixin.qq.com/s/...` URL does not necessarily contain the
//! public-account identifier required by WeRead. This module resolves that
//! identity from the URL or from the already-rendered page source, producing
//! the canonical `MP_WXS_<bid>` book identifier used by [`SourceService`].
//!
//! Responsibilities:
//!
//! - validate that all source and redirect URLs are the exact public WeChat
//!   article host;
//! - decode the Base64 `__biz`/`biz` value into the numeric public-account ID;
//! - extract identity from narrow script variables, canonical metadata, and
//!   embedded article links;
//! - extract optional display metadata without treating article text as
//!   identity; and
//! - classify structurally recognized verification pages before returning a
//!   missing-identity error.
//!
//! Non-responsibilities: source persistence, job creation, WeRead API calls,
//! credential handling, HTML sanitization, or RSS rendering. The returned
//! content is metadata only; the public article page still goes through the
//! separate unauthenticated article-page fetcher.
//!
//! Data flow:
//!
//! 1. callers validate the operator URL into [`VerifiedWechatArticleUrl`];
//! 2. [`resolve_from_url`] handles long URLs without network access;
//! 3. for short URLs, the caller opens a clean public browser session and
//!    [`WebDriverIdentityResolver`] captures the final URL and page source;
//! 4. [`extract_identity_from_html`] tries the final URL before progressively
//!    narrower page-source fallbacks; and
//! 5. the application converts the result into a `NewSource` and persists it
//!    in its normal source/job transaction. A caller that already has a
//!    `book_id` may omit the article URL entirely.
//!
//! Failure behavior is explicit: malformed Base64 is not silently converted
//! to a made-up book ID, an unsafe redirect is rejected, a structurally
//! recognized verification page returns [`IdentityError::VerificationRequired`],
//! and a valid page with no identity returns [`IdentityError::MissingIdentity`].
//! Verification detection deliberately does not scan all body text: an
//! article is allowed to discuss “当前环境异常” without being classified as a
//! challenge page.
//!
//! PostgreSQL/high-availability considerations: this module has no database
//! state and is safe to run on any replica. The resulting `book_id` remains
//! subject to the source repository's unique constraint, so concurrent source
//! creation still resolves through the durable database transaction.
//!
use base64::{engine::general_purpose::STANDARD, Engine as _};
use percent_encoding::percent_decode_str;
use regex::Regex;
use scraper::{Html, Selector};
use thiserror::Error;
use url::Url;

use crate::domain::source::{SourceError, VerifiedWechatArticleUrl};

use super::{
    browser_pool::BrowserPool,
    webdriver::{PublicBrowserSession, WebDriverError, WebDriverFactory},
};

/// Application boundary for resolving the public-account identity behind an
/// article URL.
#[async_trait::async_trait]
pub trait ArticleIdentityResolver: Send + Sync {
    /// Resolves the canonical WeRead book ID and optional display metadata.
    async fn resolve(
        &self,
        article_url: VerifiedWechatArticleUrl,
    ) -> Result<ArticleIdentity, IdentityError>;
}

/// Resolver used by callers that only accept long URLs containing `__biz` or
/// `biz`. Short links are deliberately reported as unresolved instead of
/// silently being stored without a stable identity.
#[derive(Debug, Clone, Copy, Default)]
pub struct UrlArticleIdentityResolver;

#[async_trait::async_trait]
impl ArticleIdentityResolver for UrlArticleIdentityResolver {
    async fn resolve(
        &self,
        article_url: VerifiedWechatArticleUrl,
    ) -> Result<ArticleIdentity, IdentityError> {
        let result = resolve_from_url(article_url);
        match &result {
            Ok(identity) => tracing::debug!(
                book_id = %identity.book_id(),
                method = identity.method().as_str(),
                "resolved article identity from URL"
            ),
            Err(error) => {
                tracing::debug!(error = %error, "article identity was not present in URL")
            }
        }
        result
    }
}

/// Resolver that first handles long URLs locally and then uses a clean public
/// browser session for short `/s/...` URLs.
#[derive(Clone)]
pub struct BrowserArticleIdentityResolver {
    factory: WebDriverFactory,
    browser_pool: BrowserPool,
}

impl BrowserArticleIdentityResolver {
    /// Creates a resolver over the process-wide browser capacity and profile.
    pub fn new(factory: WebDriverFactory, browser_pool: BrowserPool) -> Self {
        Self {
            factory,
            browser_pool,
        }
    }
}

#[async_trait::async_trait]
impl ArticleIdentityResolver for BrowserArticleIdentityResolver {
    async fn resolve(
        &self,
        article_url: VerifiedWechatArticleUrl,
    ) -> Result<ArticleIdentity, IdentityError> {
        tracing::debug!("resolving article identity");
        match resolve_from_url(article_url.clone()) {
            Ok(identity) => {
                tracing::debug!(
                    book_id = %identity.book_id(),
                    method = identity.method().as_str(),
                    "resolved article identity without browser"
                );
                Ok(identity)
            }
            Err(IdentityError::MissingIdentity) => {
                tracing::debug!("article identity requires a browser redirect or page inspection");
                let session =
                    self.factory
                        .open_public(&self.browser_pool)
                        .await
                        .map_err(|error| {
                            tracing::warn!(
                                error_kind = error.kind(),
                                "unable to open browser for article identity resolution"
                            );
                            IdentityError::Browser(error.safe_message())
                        })?;
                let result = WebDriverIdentityResolver
                    .resolve(article_url, session)
                    .await;
                match &result {
                    Ok(identity) => tracing::info!(
                        book_id = %identity.book_id(),
                        method = identity.method().as_str(),
                        "resolved article identity with browser"
                    ),
                    Err(error) => {
                        tracing::warn!(error = %error, "browser article identity resolution failed")
                    }
                }
                result
            }
            Err(error) => {
                tracing::warn!(error = %error, "article identity resolution failed");
                Err(error)
            }
        }
    }
}

/// The fallback that produced an identity value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityMethod {
    /// The caller supplied a long URL with a `__biz` or `biz` query value.
    UrlQuery,
    /// The browser redirect expanded a short URL to one with an identity.
    RedirectUrl,
    /// A narrow `biz` or `__biz` JavaScript assignment was found.
    HtmlBiz,
    /// A `msg_link` JavaScript assignment contained an identity-bearing URL.
    HtmlMessageLink,
    /// An OpenGraph/Twitter/canonical link contained an identity-bearing URL.
    HtmlCanonical,
    /// A full public article URL was embedded in page source.
    HtmlEmbeddedUrl,
}

impl IdentityMethod {
    /// Returns a stable diagnostic label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UrlQuery => "url-query",
            Self::RedirectUrl => "redirect-url",
            Self::HtmlBiz => "html-biz",
            Self::HtmlMessageLink => "html-msg-link",
            Self::HtmlCanonical => "html-canonical",
            Self::HtmlEmbeddedUrl => "html-embedded-url",
        }
    }
}

/// Identity and display metadata recovered from one public article page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArticleIdentity {
    /// The URL originally submitted by the operator.
    article_url: VerifiedWechatArticleUrl,
    /// The URL observed after navigation and redirect validation.
    resolved_url: VerifiedWechatArticleUrl,
    /// The original Base64 public-account identity.
    biz: String,
    /// The decoded numeric public-account identifier.
    bid: String,
    /// WeRead's public-account book identifier.
    book_id: String,
    /// Optional article title suitable as a display-name fallback.
    title: Option<String>,
    /// Optional public-account name.
    account_name: Option<String>,
    /// How the identity was recovered.
    method: IdentityMethod,
}

impl ArticleIdentity {
    /// Returns the originally submitted URL.
    pub fn article_url(&self) -> &VerifiedWechatArticleUrl {
        &self.article_url
    }

    /// Returns the validated final URL.
    pub fn resolved_url(&self) -> &VerifiedWechatArticleUrl {
        &self.resolved_url
    }

    /// Returns the Base64 public-account identity.
    pub fn biz(&self) -> &str {
        &self.biz
    }

    /// Returns the decoded numeric public-account identifier.
    pub fn bid(&self) -> &str {
        &self.bid
    }

    /// Returns the normalized WeRead book identifier.
    pub fn book_id(&self) -> &str {
        &self.book_id
    }

    /// Returns the optional page title.
    pub fn title(&self) -> Option<&str> {
        self.title.as_deref()
    }

    /// Returns the optional public-account display name.
    pub fn account_name(&self) -> Option<&str> {
        self.account_name.as_deref()
    }

    /// Returns the identity extraction method.
    pub const fn method(&self) -> IdentityMethod {
        self.method
    }
}

/// Failures raised while resolving a public-account identity.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum IdentityError {
    /// The submitted URL violated the domain URL policy.
    #[error(transparent)]
    InvalidArticleUrl(#[from] SourceError),
    /// The `biz` value was present but was not padded/encoded UTF-8 digits.
    #[error("biz is not valid Base64 containing a numeric public-account ID")]
    InvalidBiz,
    /// The page contained no usable identity-bearing value.
    #[error("WeChat article page did not contain a public-account identity")]
    MissingIdentity,
    /// Navigation ended outside the exact public article host.
    #[error("WeChat article navigation ended at an unsafe URL")]
    UnsafeRedirect,
    /// A structural verification-page marker was found.
    #[error("WeChat requires environment verification before identity can be resolved")]
    VerificationRequired,
    /// WebDriver failed while loading or capturing the public page.
    #[error("identity browser operation failed: {0}")]
    Browser(String),
}

/// Decodes a WeChat `__biz` value into its numeric public-account ID.
///
/// WeChat commonly omits Base64 padding and URL-encodes `+`, `/`, or `=`.
/// Percent decoding is performed here, while query parsing is handled by
/// [`Url`]. The decoded value must contain digits only; arbitrary decoded text
/// is not a valid source identity.
pub fn decode_biz(value: &str) -> Result<String, IdentityError> {
    let value = percent_decode_str(value.trim())
        .decode_utf8()
        .map_err(|_| IdentityError::InvalidBiz)?;
    let value = value.trim();
    if value.is_empty() {
        return Err(IdentityError::InvalidBiz);
    }
    let padded = format!("{value}{}", "=".repeat((4 - value.len() % 4) % 4));
    let decoded = STANDARD
        .decode(padded.as_bytes())
        .map_err(|_| IdentityError::InvalidBiz)?;
    let decoded = std::str::from_utf8(&decoded)
        .map_err(|_| IdentityError::InvalidBiz)?
        .trim();
    if decoded.is_empty() || !decoded.chars().all(|character| character.is_ascii_digit()) {
        return Err(IdentityError::InvalidBiz);
    }
    Ok(decoded.to_owned())
}

/// Resolves a long URL without opening a browser.
pub fn resolve_from_url(
    article_url: VerifiedWechatArticleUrl,
) -> Result<ArticleIdentity, IdentityError> {
    if !is_public_article_url(&article_url) {
        return Err(IdentityError::InvalidArticleUrl(
            SourceError::InvalidArticleUrl,
        ));
    }
    let biz = biz_from_verified_url(&article_url).ok_or(IdentityError::MissingIdentity)?;
    identity_from_biz(
        article_url.clone(),
        article_url,
        biz,
        IdentityMethod::UrlQuery,
        None,
        None,
    )
}

/// Extracts an identity from already captured, browser-rendered page source.
///
/// `article_url` is the original validated destination and `resolved_url` is
/// the browser-observed validated final URL. Supplying validated URL values
/// keeps redirect checks at the browser boundary and prevents metadata links
/// from changing the navigation security policy.
pub fn extract_identity_from_html(
    article_url: VerifiedWechatArticleUrl,
    resolved_url: VerifiedWechatArticleUrl,
    html_text: &str,
) -> Result<Option<ArticleIdentity>, IdentityError> {
    if !is_public_article_url(&article_url) {
        return Err(IdentityError::InvalidArticleUrl(
            SourceError::InvalidArticleUrl,
        ));
    }
    if !is_public_article_url(&resolved_url) {
        return Err(IdentityError::UnsafeRedirect);
    }
    if is_structural_verification_page(html_text) {
        return Err(IdentityError::VerificationRequired);
    }

    let mut candidates = Vec::new();
    if let Some(biz) = biz_from_verified_url(&resolved_url) {
        let method = if resolved_url == article_url {
            IdentityMethod::UrlQuery
        } else {
            IdentityMethod::RedirectUrl
        };
        candidates.push((biz, method));
    }

    let document = Html::parse_document(html_text);
    let script_biz = Regex::new(
        r#"(?is)(?:^|[^\w$])(?:(?:window\.)?(?:biz|__biz)|(?:var\s+)(?:biz|__biz))\s*=\s*['\"]([^'\"]+)['\"]"#,
    )
    .expect("identity biz regex is static and valid");
    if let Some(capture) = script_biz.captures(html_text) {
        candidates.push((clean_embedded_string(&capture[1]), IdentityMethod::HtmlBiz));
    }

    let message_link = Regex::new(
        r#"(?is)(?:^|[^\w$])(?:(?:window\.)?msg_link|(?:var\s+msg_link))\s*=\s*['\"](.+?)['\"]\s*;?"#,
    )
    .expect("identity message-link regex is static and valid");
    if let Some(capture) = message_link.captures(html_text) {
        if let Some(biz) = biz_from_string_url(&clean_embedded_string(&capture[1])) {
            candidates.push((biz, IdentityMethod::HtmlMessageLink));
        }
    }

    for selector in [
        r#"meta[property="og:url"]"#,
        r#"meta[name="twitter:url"]"#,
        r#"link[rel~="canonical"]"#,
    ] {
        let selector = Selector::parse(selector).expect("identity selector is static and valid");
        if let Some(value) = document.select(&selector).find_map(|element| {
            let value = element
                .value()
                .attr("content")
                .or_else(|| element.value().attr("href"))?;
            biz_from_string_url(&clean_embedded_string(value))
        }) {
            candidates.push((value, IdentityMethod::HtmlCanonical));
            break;
        }
    }

    if candidates.is_empty() {
        let embedded_url = Regex::new(r#"(?is)https?://mp\.weixin\.qq\.com/s(?:[/?])[^"'< >\s]+"#)
            .expect("identity embedded-url regex is static and valid");
        if let Some(capture) = embedded_url.find(html_text) {
            if let Some(biz) = biz_from_string_url(&clean_embedded_string(capture.as_str())) {
                candidates.push((biz, IdentityMethod::HtmlEmbeddedUrl));
            }
        }
    }

    if candidates.is_empty() {
        return Ok(None);
    }

    let (biz, method, bid) = candidates
        .into_iter()
        .find_map(|(biz, method)| decode_biz(&biz).ok().map(|bid| (biz, method, bid)))
        .ok_or(IdentityError::InvalidBiz)?;

    let title = first_metadata_value(
        &document,
        &[
            ("meta[property=\"og:title\"]", "content"),
            ("meta[name=\"twitter:title\"]", "content"),
        ],
    )
    .or_else(|| {
        let selector = Selector::parse("title").expect("title selector is static and valid");
        document
            .select(&selector)
            .next()
            .map(|element| clean_text(&element.text().collect::<String>()))
    })
    .filter(|value| !value.is_empty());

    let account_name = first_metadata_value(
        &document,
        &[("meta[property=\"og:article:author\"]", "content")],
    )
    .or_else(|| {
        ["#js_name", "#js_wx_follow_nickname"]
            .into_iter()
            .find_map(|selector| {
                let selector = Selector::parse(selector).expect("account selector is static");
                document
                    .select(&selector)
                    .next()
                    .map(|element| clean_text(&element.text().collect::<String>()))
                    .filter(|value| !value.is_empty())
            })
    })
    .or_else(|| {
        let nickname =
            Regex::new(r#"(?is)(?:^|[^\w$])(?:var\s+)?nickname\s*=\s*['\"](.+?)['\"]\s*;?"#)
                .expect("identity nickname regex is static and valid");
        nickname
            .captures(html_text)
            .map(|capture| clean_text(&clean_embedded_string(&capture[1])))
            .filter(|value| !value.is_empty())
    });

    Ok(Some(ArticleIdentity {
        article_url,
        resolved_url,
        biz,
        bid: bid.clone(),
        book_id: format!("MP_WXS_{bid}"),
        title,
        account_name,
        method,
    }))
}

/// Resolves identity using a clean, unauthenticated browser session.
///
/// The session is consumed and closed on both success and failure. It accepts
/// only a [`PublicBrowserSession`], so WeRead credentials cannot cross into
/// source identity discovery.
#[derive(Debug, Clone, Copy, Default)]
pub struct WebDriverIdentityResolver;

impl WebDriverIdentityResolver {
    /// Navigates to and resolves one public article identity.
    pub async fn resolve(
        &self,
        article_url: VerifiedWechatArticleUrl,
        mut session: PublicBrowserSession,
    ) -> Result<ArticleIdentity, IdentityError> {
        let session_id = session.session_id();
        tracing::debug!(session_id = %session_id, "starting browser article identity resolution");
        let result = self.resolve_without_cleanup(article_url, &session).await;
        let cleanup_result = session.close_client().await;
        match (result, cleanup_result) {
            (Ok(identity), Ok(())) => {
                tracing::info!(session_id = %session_id, book_id = %identity.book_id(), "browser article identity resolution completed");
                Ok(identity)
            }
            (Ok(_), Err(error)) => {
                tracing::warn!(session_id = %session_id, error = %error, "browser identity session cleanup failed");
                Err(IdentityError::Browser(format!(
                    "browser cleanup failed: {error}"
                )))
            }
            (Err(error), Ok(())) => {
                tracing::warn!(session_id = %session_id, error = %error, "browser article identity resolution failed");
                Err(error)
            }
            (Err(error), Err(cleanup_error)) => {
                tracing::warn!(
                    session_id = %session_id,
                    error = %cleanup_error,
                    "browser cleanup failed after identity resolution error"
                );
                Err(error)
            }
        }
    }

    async fn resolve_without_cleanup(
        &self,
        article_url: VerifiedWechatArticleUrl,
        session: &PublicBrowserSession,
    ) -> Result<ArticleIdentity, IdentityError> {
        session
            .goto(article_url.as_str())
            .await
            .map_err(map_browser_error)?;
        let final_url = session.current_url().await.map_err(map_browser_error)?;
        let resolved_url = VerifiedWechatArticleUrl::parse(final_url.as_str())
            .map_err(|_| IdentityError::UnsafeRedirect)?;
        let html = session.source().await.map_err(map_browser_error)?;
        extract_identity_from_html(article_url, resolved_url, &html)?
            .ok_or(IdentityError::MissingIdentity)
    }
}

fn identity_from_biz(
    article_url: VerifiedWechatArticleUrl,
    resolved_url: VerifiedWechatArticleUrl,
    biz: String,
    method: IdentityMethod,
    title: Option<String>,
    account_name: Option<String>,
) -> Result<ArticleIdentity, IdentityError> {
    let bid = decode_biz(&biz)?;
    Ok(ArticleIdentity {
        article_url,
        resolved_url,
        biz,
        book_id: format!("MP_WXS_{bid}"),
        bid,
        title,
        account_name,
        method,
    })
}

fn biz_from_verified_url(url: &VerifiedWechatArticleUrl) -> Option<String> {
    Url::parse(url.as_str()).ok().and_then(|url| {
        if !is_public_article_path(url.path()) {
            return None;
        }
        url.query_pairs()
            .find(|(key, _)| key == "__biz" || key == "biz")
            .map(|(_, value)| value.into_owned())
            .filter(|value| !value.trim().is_empty())
    })
}

fn biz_from_string_url(value: &str) -> Option<String> {
    let url = Url::parse(value).ok()?;
    if url.scheme() != "https"
        || url.host_str() != Some("mp.weixin.qq.com")
        || !is_public_article_path(url.path())
    {
        return None;
    }
    url.query_pairs()
        .find(|(key, _)| key == "__biz" || key == "biz")
        .map(|(_, value)| value.into_owned())
        .filter(|value| !value.trim().is_empty())
}

fn is_public_article_path(path: &str) -> bool {
    path == "/s" || path.starts_with("/s/")
}

fn is_public_article_url(url: &VerifiedWechatArticleUrl) -> bool {
    Url::parse(url.as_str()).is_ok_and(|url| is_public_article_path(url.path()))
}

fn first_metadata_value(document: &Html, selectors: &[(&str, &str)]) -> Option<String> {
    selectors.iter().find_map(|(selector, attribute)| {
        let selector = Selector::parse(selector).expect("metadata selector is static");
        document
            .select(&selector)
            .next()
            .and_then(|element| element.value().attr(attribute))
            .map(clean_text)
            .filter(|value| !value.is_empty())
    })
}

fn clean_embedded_string(value: &str) -> String {
    value
        .replace("&amp;", "&")
        .replace("&#38;", "&")
        .replace("&#x26;", "&")
        .replace(r"\/", "/")
        .replace(r"\x26", "&")
        .replace(r"\u0026", "&")
        .replace(r"\x3d", "=")
        .replace(r"\u003d", "=")
        .replace(r"\x3f", "?")
        .replace(r"\u003f", "?")
}

fn clean_text(value: &str) -> String {
    clean_embedded_string(value)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_owned()
}

fn is_structural_verification_page(html_text: &str) -> bool {
    let document = Html::parse_document(html_text);
    let verify_selector = Selector::parse("#js_verify").expect("verification selector is static");
    if document.select(&verify_selector).next().is_some() {
        return true;
    }

    let title_selector =
        Selector::parse("h2.weui-msg__title").expect("verification title selector is static");
    document.select(&title_selector).any(|element| {
        let text = element.text().collect::<String>();
        ["环境异常", "访问过于频繁", "操作频繁", "完成验证"]
            .iter()
            .any(|marker| text.contains(marker))
    })
}

fn map_browser_error(error: WebDriverError) -> IdentityError {
    IdentityError::Browser(error.safe_message())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn article_url(value: &str) -> VerifiedWechatArticleUrl {
        value.parse().expect("test URL should be valid")
    }

    #[test]
    fn decodes_padded_unpadded_and_percent_encoded_biz_values() {
        assert_eq!(decode_biz("MTIzNDU="), Ok("12345".to_owned()));
        assert_eq!(decode_biz("MTIzNDU"), Ok("12345".to_owned()));
        assert_eq!(decode_biz("MTIzNDU%3D"), Ok("12345".to_owned()));
    }

    #[test]
    fn rejects_non_numeric_or_malformed_biz_values() {
        for value in ["", "not-base64", "bG9naW4=", "////"] {
            assert_eq!(decode_biz(value), Err(IdentityError::InvalidBiz));
        }
    }

    #[test]
    fn does_not_treat_a_longer_javascript_identifier_as_biz() {
        let url = article_url("https://mp.weixin.qq.com/s/short");
        let html = r#"<script>var notbiz = "MTIzNDU=";</script>"#;
        assert_eq!(extract_identity_from_html(url.clone(), url, html), Ok(None));
    }

    #[test]
    fn resolves_a_long_url_without_browser_access() {
        let identity = resolve_from_url(article_url(
            "https://mp.weixin.qq.com/s/example?__biz=MTIzNDU%3D&mid=1",
        ))
        .expect("long URL should resolve");
        assert_eq!(identity.book_id(), "MP_WXS_12345");
        assert_eq!(identity.method(), IdentityMethod::UrlQuery);
        assert_eq!(identity.resolved_url(), identity.article_url());
    }

    #[test]
    fn extracts_script_identity_and_display_metadata() {
        let url = article_url("https://mp.weixin.qq.com/s/example");
        let html = r#"
            <html><head>
              <meta property="og:title" content="示例文章 &amp; 标题">
              <meta property="og:article:author" content="示例公众号">
              <script>window.biz = "MTIzNDU=";</script>
            </head><body></body></html>
        "#;
        let identity = extract_identity_from_html(url.clone(), url, html)
            .expect("HTML should parse")
            .expect("script identity should be found");
        assert_eq!(identity.book_id(), "MP_WXS_12345");
        assert_eq!(identity.title(), Some("示例文章 & 标题"));
        assert_eq!(identity.account_name(), Some("示例公众号"));
        assert_eq!(identity.method(), IdentityMethod::HtmlBiz);
    }

    #[test]
    fn falls_back_to_message_link_then_canonical_metadata() {
        let url = article_url("https://mp.weixin.qq.com/s/short");
        let message_html = r#"
            <script>var msg_link = "https:\/\/mp.weixin.qq.com\/s?__biz=MTIzNDU%3D";</script>
            <title>Message title</title>
        "#;
        let message_identity = extract_identity_from_html(url.clone(), url.clone(), message_html)
            .expect("message link should parse")
            .expect("message link identity should be found");
        assert_eq!(message_identity.method(), IdentityMethod::HtmlMessageLink);

        let canonical_html = r#"
            <link rel="canonical" href="https://mp.weixin.qq.com/s/example?biz=MTIzNDU%3D">
            <title>Canonical title</title>
        "#;
        let canonical_identity = extract_identity_from_html(url.clone(), url, canonical_html)
            .expect("canonical link should parse")
            .expect("canonical identity should be found");
        assert_eq!(canonical_identity.method(), IdentityMethod::HtmlCanonical);
        assert_eq!(canonical_identity.title(), Some("Canonical title"));
    }

    #[test]
    fn prefers_a_valid_redirect_identity_and_supports_embedded_urls() {
        let source = article_url("https://mp.weixin.qq.com/s/short");
        let resolved = article_url("https://mp.weixin.qq.com/s/long?__biz=MTIzNDU%3D");
        let identity = extract_identity_from_html(source.clone(), resolved, "<body />")
            .expect("redirect URL should parse")
            .expect("redirect identity should be found");
        assert_eq!(identity.method(), IdentityMethod::RedirectUrl);

        let html = r#"<script>const link = "https://mp.weixin.qq.com/s/example?__biz=MTIzNDU%3D";</script>"#;
        let embedded = extract_identity_from_html(source.clone(), source, html)
            .expect("embedded URL should parse")
            .expect("embedded identity should be found");
        assert_eq!(embedded.method(), IdentityMethod::HtmlEmbeddedUrl);
    }

    #[test]
    fn ignores_untrusted_embedded_links_and_reports_missing_identity() {
        let url = article_url("https://mp.weixin.qq.com/s/short");
        let html = r#"<a href="https://evil.example/s?__biz=MTIzNDU%3D">not trusted</a>"#;
        assert_eq!(extract_identity_from_html(url.clone(), url, html), Ok(None));
    }

    #[test]
    fn verification_detection_is_structural_not_arbitrary_body_text() {
        let url = article_url("https://mp.weixin.qq.com/s/short");
        let article = r#"
            <script>window.biz = "MTIzNDU=";</script>
            <div id="js_content"><p>本文讨论“当前环境异常”这句话。</p></div>
        "#;
        assert!(extract_identity_from_html(url.clone(), url.clone(), article).is_ok());

        let verification = r#"
            <div id="js_verify"><h2 class="weui-msg__title">当前环境异常</h2></div>
        "#;
        assert_eq!(
            extract_identity_from_html(url.clone(), url, verification),
            Err(IdentityError::VerificationRequired)
        );
    }

    #[test]
    fn rejects_unsafe_source_urls_before_identity_resolution() {
        assert_eq!(
            "https://evil.example/s/example?__biz=MTIzNDU%3D".parse::<VerifiedWechatArticleUrl>(),
            Err(SourceError::InvalidArticleUrl)
        );
    }

    #[test]
    fn rejects_non_article_resolved_urls_before_html_identity_fallbacks() {
        let source = article_url("https://mp.weixin.qq.com/s/short");
        let resolved = article_url("https://mp.weixin.qq.com/cgi-bin/appmsg");
        let html = r#"<script>window.biz = "MTIzNDU=";</script>"#;

        assert_eq!(
            extract_identity_from_html(source, resolved, html),
            Err(IdentityError::UnsafeRedirect)
        );
    }
}
