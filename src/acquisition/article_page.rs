//! Rendered public article extraction.
//!
//! This module defines the unauthenticated article-page port. It accepts only
//! [`VerifiedWechatArticleUrl`] and [`super::webdriver::PublicBrowserSession`],
//! so fetching public content cannot accidentally depend on WeRead login
//! credentials or an authenticated browser capability.
//!
//! The concrete adapter navigates in a clean profile, revalidates the final
//! URL after navigation, optionally applies the shared pacing/scroll policy,
//! and returns normalized metadata plus body HTML. Pacing is opt-in on the
//! constructor so callers can keep unit tests and local diagnostics fast;
//! production composition should always pass the configured controller. The
//! controller's page-operation deadline covers waits, navigation, scrolling,
//! and source capture. This module does not sanitize HTML, archive binary
//! assets, persist articles, or render RSS. Asset archiving remains optional
//! for version one; the normalized page may retain approved external asset URLs
//! when it is disabled.
//!
//! Browser and extraction failures are typed at this boundary so application
//! code can distinguish retryable browser problems from invalid or unavailable
//! article content. The fetch operation consumes the public session, attempts
//! asynchronous browser cleanup on both success and failure, and therefore
//! releases its pool permit when the operation completes or is cancelled.
//!
//! TODO(implementation): add richer extraction fallbacks and content-specific
//! verification-page classification. Browser-visible timezone validation is
//! owned by [`super::webdriver`]. The public navigation, final URL validation,
//! bounded pacing/scroll execution, and common WeChat selectors are implemented
//! below.

use std::{future::Future, time::Duration};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Tz;
use scraper::{ElementRef, Html, Selector};
use thiserror::Error;

use crate::domain::{pacing::DelayKind, source::VerifiedWechatArticleUrl};

use super::{
    pacing::PacingController,
    webdriver::{PublicBrowserSession, WebDriverError},
};

/// Normalized data extracted from one public article page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedArticlePage {
    /// Canonical URL observed and verified after navigation.
    pub canonical_url: VerifiedWechatArticleUrl,
    /// Article title suitable for RSS metadata.
    pub title: String,
    /// Optional displayed author name.
    pub author: Option<String>,
    /// Optional summary or digest.
    pub summary: Option<String>,
    /// Publication time normalized to UTC.
    pub published_at: DateTime<Utc>,
    /// Sanitization input; it is not safe to publish without archive processing.
    pub content_html: String,
    /// Optional cover URL. Binary downloading is intentionally outside this port.
    pub cover_url: Option<String>,
}

/// Failures from public article-page acquisition and extraction.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ArticlePageError {
    /// WebDriver, browser-side, or network failure.
    #[error("article browser operation failed: {0}")]
    Browser(String),
    /// The page loaded but did not contain a valid article representation.
    #[error("article extraction failed: {0}")]
    InvalidExtraction(String),
    /// Navigation ended outside the exact public WeChat article host.
    #[error("article navigation ended at an unsafe URL")]
    UnsafeRedirect,
    /// WeChat returned an environment or CAPTCHA verification page.
    #[error("WeChat requires environment verification before the article can be fetched")]
    VerificationRequired,
    /// The configured page-operation deadline expired.
    #[error("article page operation exceeded its configured deadline")]
    OperationTimedOut,
}

/// Thirtyfour-backed public article fetcher.
#[derive(Debug, Clone)]
pub struct WebDriverArticlePageFetcher {
    timezone: Tz,
    pacing: Option<PacingController>,
}

impl WebDriverArticlePageFetcher {
    /// Creates a fetcher using the configured timezone for local publication
    /// timestamps that are converted to UTC.
    pub const fn new(timezone: Tz) -> Self {
        Self {
            timezone,
            pacing: None,
        }
    }

    /// Adds a shared pacing controller to waits, scrolling, and the page
    /// operation deadline.
    pub fn with_pacing(mut self, pacing: PacingController) -> Self {
        self.pacing = Some(pacing);
        self
    }
}

impl ArticlePageFetcher for WebDriverArticlePageFetcher {
    async fn fetch(
        &self,
        url: VerifiedWechatArticleUrl,
        mut session: PublicBrowserSession,
    ) -> Result<ExtractedArticlePage, ArticlePageError> {
        let result = match &self.pacing {
            Some(pacing) => {
                within_page_deadline(
                    pacing.max_page_operation(),
                    self.fetch_without_cleanup(url, &mut session, Some(pacing)),
                )
                .await
            }
            None => self.fetch_without_cleanup(url, &mut session, None).await,
        };
        let cleanup_result = session.close_client().await;
        match (result, cleanup_result) {
            (Ok(page), Ok(())) => Ok(page),
            (Ok(_), Err(error)) => Err(ArticlePageError::Browser(format!(
                "browser cleanup failed: {error}"
            ))),
            (Err(error), Ok(())) => Err(error),
            (Err(error), Err(cleanup_error)) => {
                tracing::warn!(
                    error = %cleanup_error,
                    "browser cleanup failed after article fetch error"
                );
                Err(error)
            }
        }
    }
}

async fn within_page_deadline<T, F>(deadline: Duration, operation: F) -> Result<T, ArticlePageError>
where
    F: Future<Output = Result<T, ArticlePageError>>,
{
    tokio::time::timeout(deadline, operation)
        .await
        .map_err(|_| ArticlePageError::OperationTimedOut)?
}

impl WebDriverArticlePageFetcher {
    async fn fetch_without_cleanup(
        &self,
        url: VerifiedWechatArticleUrl,
        session: &mut PublicBrowserSession,
        pacing: Option<&PacingController>,
    ) -> Result<ExtractedArticlePage, ArticlePageError> {
        if let Some(pacing) = pacing {
            pacing.wait(DelayKind::PageNavigation).await;
        }
        session
            .goto(url.as_str())
            .await
            .map_err(map_webdriver_error)?;
        let final_url = session.current_url().await.map_err(map_webdriver_error)?;
        let canonical_url = verify_final_url(final_url)?;
        if let Some(pacing) = pacing {
            let viewport_height = session
                .viewport_height()
                .await
                .map_err(map_webdriver_error)?;
            for step in pacing.scroll_plan(viewport_height).await {
                pacing.wait(DelayKind::PageAction).await;
                session
                    .scroll_by(step.distance)
                    .await
                    .map_err(map_webdriver_error)?;
                if !step.settle.is_zero() {
                    tokio::time::sleep(step.settle).await;
                }
            }
            pacing.wait(DelayKind::PageAction).await;
        }
        let html = session.source().await.map_err(map_webdriver_error)?;
        parse_article_html(&html, canonical_url, self.timezone)
    }
}

fn map_webdriver_error(error: WebDriverError) -> ArticlePageError {
    ArticlePageError::Browser(error.to_string())
}

fn verify_final_url(final_url: url::Url) -> Result<VerifiedWechatArticleUrl, ArticlePageError> {
    VerifiedWechatArticleUrl::parse(final_url.as_str())
        .map_err(|_| ArticlePageError::UnsafeRedirect)
}

fn parse_article_html(
    html: &str,
    canonical_url: VerifiedWechatArticleUrl,
    timezone: Tz,
) -> Result<ExtractedArticlePage, ArticlePageError> {
    let document = Html::parse_document(html);
    if is_verification_page(&document) {
        return Err(ArticlePageError::VerificationRequired);
    }
    let title = first_text(
        &document,
        &["#activity-name", "h1.rich_media_title", "#js_title"],
    )
    .or_else(|| first_attribute(&document, "meta[property='og:title']", "content"))
    .or_else(|| first_text(&document, &["title"]))
    .ok_or_else(|| ArticlePageError::InvalidExtraction("article title is missing".into()))?;
    let content = first_element(&document, "#js_content")
        .or_else(|| first_element(&document, ".rich_media_content"))
        .or_else(|| first_element(&document, "#js_article"))
        .ok_or_else(|| ArticlePageError::InvalidExtraction("article body is missing".into()))?;
    let content_html = content.inner_html().trim().to_owned();
    if content_html.is_empty() {
        return Err(ArticlePageError::InvalidExtraction(
            "article body is empty".into(),
        ));
    }

    let published_text = first_text(&document, &["#publish_time"]).ok_or_else(|| {
        ArticlePageError::InvalidExtraction("article publication time is missing".into())
    })?;
    let published_at = parse_publication_time(&published_text, timezone)?;

    Ok(ExtractedArticlePage {
        canonical_url,
        title,
        author: first_text(
            &document,
            &["#js_name", "#js_author_name", "span.rich_media_meta_text"],
        )
        .or_else(|| first_attribute(&document, "meta[property='og:article:author']", "content")),
        summary: first_attribute(&document, "meta[name='description']", "content")
            .or_else(|| first_attribute(&document, "meta[property='og:description']", "content")),
        published_at,
        content_html,
        cover_url: first_attribute(&document, "meta[property='og:image']", "content")
            .or_else(|| first_attribute(&document, "meta[property='twitter:image']", "content")),
    })
}

fn is_verification_page(document: &Html) -> bool {
    first_element(document, "#js_verify").is_some()
        || first_text(document, &["h2.weui-msg__title"])
            .is_some_and(|title| title.contains("环境异常") || title.contains("验证"))
}

fn first_element<'document>(
    document: &'document Html,
    selector: &str,
) -> Option<ElementRef<'document>> {
    let selector = Selector::parse(selector).expect("static article selector must be valid");
    document.select(&selector).next()
}

fn first_text(document: &Html, selectors: &[&str]) -> Option<String> {
    selectors
        .iter()
        .find_map(|selector| first_element(document, selector).map(element_text))
        .filter(|value| !value.is_empty())
}

fn first_attribute(document: &Html, selector: &str, attribute: &str) -> Option<String> {
    first_element(document, selector)
        .and_then(|element| element.value().attr(attribute))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn element_text(element: ElementRef<'_>) -> String {
    element
        .text()
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_publication_time(value: &str, timezone: Tz) -> Result<DateTime<Utc>, ArticlePageError> {
    let value = value.trim();
    let parsed = [
        NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S").ok(),
        NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M").ok(),
        NaiveDateTime::parse_from_str(value, "%Y年%m月%d日 %H:%M:%S").ok(),
        NaiveDateTime::parse_from_str(value, "%Y年%m月%d日 %H:%M").ok(),
    ]
    .into_iter()
    .flatten()
    .next()
    .or_else(|| {
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .ok()
            .map(|date| date.and_hms_opt(0, 0, 0).expect("midnight is valid"))
    })
    .ok_or_else(|| {
        ArticlePageError::InvalidExtraction(format!("article publication time is invalid: {value}"))
    })?;

    timezone
        .from_local_datetime(&parsed)
        .single()
        .map(|date_time| date_time.with_timezone(&Utc))
        .ok_or_else(|| {
            ArticlePageError::InvalidExtraction(
                "article publication time is ambiguous in configured timezone".into(),
            )
        })
}

/// Port for fetching rendered article content without credentials.
#[allow(async_fn_in_trait)]
pub trait ArticlePageFetcher: Send + Sync {
    /// Fetches one verified public URL and consumes the clean browser session.
    ///
    /// Consuming the capability makes one-session-per-operation ownership
    /// explicit: its local pool permit cannot be retained accidentally after
    /// a fetch, and dropping a cancelled future also drops the session.
    async fn fetch(
        &self,
        url: VerifiedWechatArticleUrl,
        session: PublicBrowserSession,
    ) -> Result<ExtractedArticlePage, ArticlePageError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acquisition::browser_pool::BrowserPool;
    use crate::domain::pacing::{DelayDistribution, PacingPolicy};

    fn article_url() -> VerifiedWechatArticleUrl {
        "https://mp.weixin.qq.com/s/example"
            .parse()
            .expect("test URL should be valid")
    }

    fn zero_delay_pacing() -> PacingController {
        let zero = DelayDistribution::new(0.0, 0.0, 0.0, 0.0).unwrap();
        let policy = PacingPolicy::new(
            zero,
            zero,
            zero,
            zero,
            2,
            1_000,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        PacingController::from_seed(policy, 7)
    }

    struct FakeFetcher;

    impl ArticlePageFetcher for FakeFetcher {
        async fn fetch(
            &self,
            url: VerifiedWechatArticleUrl,
            _session: PublicBrowserSession,
        ) -> Result<ExtractedArticlePage, ArticlePageError> {
            Ok(ExtractedArticlePage {
                canonical_url: url,
                title: "title".to_owned(),
                author: None,
                summary: None,
                published_at: Utc::now(),
                content_html: "<p>body</p>".to_owned(),
                cover_url: None,
            })
        }
    }

    struct BlockingFetcher;

    impl ArticlePageFetcher for BlockingFetcher {
        async fn fetch(
            &self,
            _url: VerifiedWechatArticleUrl,
            _session: PublicBrowserSession,
        ) -> Result<ExtractedArticlePage, ArticlePageError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn port_accepts_only_public_session_capability() {
        let url = article_url();
        let pool = BrowserPool::new(1).unwrap();
        let session = pool.open_public().await.unwrap();
        let page = FakeFetcher.fetch(url.clone(), session).await.unwrap();

        assert_eq!(page.canonical_url, url);
        assert_eq!(page.content_html, "<p>body</p>");
    }

    #[tokio::test]
    async fn webdriver_fetch_reports_browser_failure_and_leaves_failed_session_closed() {
        let pool = BrowserPool::new(1).unwrap();
        let session = pool.open_public().await.unwrap();

        let result = WebDriverArticlePageFetcher::new(chrono_tz::Asia::Shanghai)
            .fetch(article_url(), session)
            .await;

        assert!(matches!(result, Err(ArticlePageError::Browser(message))
            if message.contains("not connected to WebDriver")));
        let replacement = pool.open_public().await.unwrap();
        replacement.close().await.unwrap();
    }

    #[tokio::test]
    async fn paced_fetch_releases_capacity_when_browser_operation_fails() {
        let pool = BrowserPool::new(1).unwrap();
        let session = pool.open_public().await.unwrap();

        let result = WebDriverArticlePageFetcher::new(chrono_tz::Asia::Shanghai)
            .with_pacing(zero_delay_pacing())
            .fetch(article_url(), session)
            .await;

        assert!(matches!(result, Err(ArticlePageError::Browser(message))
            if message.contains("not connected to WebDriver")));
        let replacement =
            tokio::time::timeout(std::time::Duration::from_millis(10), pool.open_public())
                .await
                .expect("paced fetch should release its pool permit")
                .unwrap();
        replacement.close().await.unwrap();
    }

    #[tokio::test]
    async fn cancelling_a_fetch_releases_public_session_capacity() {
        let pool = BrowserPool::new(1).unwrap();
        let session = pool.open_public().await.unwrap();

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            BlockingFetcher.fetch(article_url(), session),
        )
        .await;
        assert!(result.is_err(), "the blocking fetch should be cancelled");

        let replacement =
            tokio::time::timeout(std::time::Duration::from_millis(10), pool.open_public())
                .await
                .expect("cancelling the fetch should release the pool permit")
                .unwrap();
        replacement.close().await.unwrap();
    }

    #[tokio::test]
    async fn page_deadline_converts_a_stuck_operation_to_a_typed_error() {
        let result = within_page_deadline(
            std::time::Duration::from_millis(1),
            std::future::pending::<Result<(), ArticlePageError>>(),
        )
        .await;

        assert_eq!(result, Err(ArticlePageError::OperationTimedOut));
    }

    #[test]
    fn parses_common_wechat_article_markup() {
        let html = r#"
            <html><head>
              <title>Fallback title</title>
              <meta name="description" content="A short summary">
              <meta property="og:image" content="https://mmbiz.qpic.cn/example.jpg">
            </head><body>
              <h1 class="rich_media_title">  A rendered title  </h1>
              <span id="js_name">Example Account</span>
              <span id="publish_time">2026-08-29 10:20</span>
              <div id="js_content"><p>Hello <strong>world</strong>.</p></div>
            </body></html>
        "#;

        let page = parse_article_html(html, article_url(), chrono_tz::Asia::Shanghai).unwrap();
        assert_eq!(page.title, "A rendered title");
        assert_eq!(page.author.as_deref(), Some("Example Account"));
        assert_eq!(page.summary.as_deref(), Some("A short summary"));
        assert_eq!(
            page.cover_url.as_deref(),
            Some("https://mmbiz.qpic.cn/example.jpg")
        );
        assert_eq!(page.published_at.to_rfc3339(), "2026-08-29T02:20:00+00:00");
        assert!(page.content_html.contains("<strong>world</strong>"));
    }

    #[test]
    fn accepts_alternate_rendered_article_selectors_and_metadata() {
        let html = r#"
            <html><head>
              <meta property="og:article:author" content="Meta Author">
              <meta property="twitter:image" content="https://mmbiz.qpic.cn/cover.png">
            </head><body>
              <div id="activity-name">  Alternate title  </div>
              <span id="publish_time">2026-08-29 10:20</span>
              <article id="js_article"><p>Alternate body</p></article>
            </body></html>
        "#;

        let page = parse_article_html(html, article_url(), chrono_tz::Asia::Shanghai).unwrap();
        assert_eq!(page.title, "Alternate title");
        assert_eq!(page.author.as_deref(), Some("Meta Author"));
        assert_eq!(
            page.cover_url.as_deref(),
            Some("https://mmbiz.qpic.cn/cover.png")
        );
        assert!(page.content_html.contains("Alternate body"));
    }

    #[test]
    fn falls_back_to_rich_media_content_when_js_content_is_missing() {
        let html = r#"
            <h1 class="rich_media_title">Rich media title</h1>
            <span id="publish_time">2026-08-29 10:20</span>
            <div class="rich_media_content"><p>Rich media body</p></div>
        "#;

        let page = parse_article_html(html, article_url(), chrono_tz::Asia::Shanghai).unwrap();
        assert!(page.content_html.contains("Rich media body"));
    }

    #[test]
    fn does_not_classify_article_text_as_verification() {
        let html = r#"
            <html><body>
              <h1 class="rich_media_title">A quoted phrase</h1>
              <span id="publish_time">2026-08-29 10:20</span>
              <div id="js_content">
                <p>这篇文章讨论“当前环境异常，完成验证后即可继续访问”这句话。</p>
              </div>
            </body></html>
        "#;

        let page = parse_article_html(html, article_url(), chrono_tz::Asia::Shanghai).unwrap();
        assert!(page.content_html.contains("当前环境异常"));
    }

    #[test]
    fn does_not_treat_an_unstructured_body_phrase_as_verification() {
        let html = "<html><body><p>当前环境异常</p></body></html>";

        assert!(matches!(
            parse_article_html(html, article_url(), chrono_tz::Asia::Shanghai),
            Err(ArticlePageError::InvalidExtraction(message))
                if message.contains("article title is missing")
        ));
    }

    #[test]
    fn rejects_missing_body_and_invalid_publication_time() {
        let missing_body = r#"
            <h1 class="rich_media_title">Title</h1>
            <span id="publish_time">2026-08-29 10:20</span>
        "#;
        assert_eq!(
            parse_article_html(missing_body, article_url(), chrono_tz::Asia::Shanghai),
            Err(ArticlePageError::InvalidExtraction(
                "article body is missing".into()
            ))
        );

        let invalid_time = r#"
            <h1 class="rich_media_title">Title</h1>
            <span id="publish_time">not-a-date</span>
            <div id="js_content"><p>Body</p></div>
        "#;
        assert!(matches!(
            parse_article_html(invalid_time, article_url(), chrono_tz::Asia::Shanghai),
            Err(ArticlePageError::InvalidExtraction(message))
                if message.contains("publication time is invalid")
        ));
    }

    #[test]
    fn accepts_date_only_publication_time_at_timezone_midnight() {
        let html = r#"
            <h1 class="rich_media_title">Title</h1>
            <span id="publish_time">2026-08-29</span>
            <div id="js_content"><p>Body</p></div>
        "#;
        let page = parse_article_html(html, article_url(), chrono_tz::Asia::Shanghai).unwrap();
        assert_eq!(page.published_at.to_rfc3339(), "2026-08-28T16:00:00+00:00");
    }

    #[test]
    fn rejects_a_redirect_to_a_non_wechat_host() {
        let redirected_url = url::Url::parse("https://example.com/article").unwrap();
        assert_eq!(
            verify_final_url(redirected_url),
            Err(ArticlePageError::UnsafeRedirect)
        );
    }

    #[test]
    fn preserves_image_only_article_content() {
        let html = r#"
            <h1 class="rich_media_title">Image article</h1>
            <span id="publish_time">2026-08-29 10:20</span>
            <div id="js_content"><img src="https://mmbiz.qpic.cn/example.jpg"></div>
        "#;
        let page = parse_article_html(html, article_url(), chrono_tz::Asia::Shanghai).unwrap();
        assert!(page.content_html.contains("mmbiz.qpic.cn/example.jpg"));
    }
}
