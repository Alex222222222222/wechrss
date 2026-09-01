//! WeRead account and article-list protocol adapter boundary.
//!
//! This module defines the authenticated protocol port for QR/login state,
//! refresh-token lifecycle, article-list responses, detail-URL recovery, and
//! current/legacy response-shape parsing. It does not fetch rendered article
//! content: that is the separate public operation in [`super::article_page`]
//! and intentionally needs no credentials.
//!
//! A caller must obtain the one-request capability from
//! [`super::webdriver::AuthenticatedBrowserSession::prepare_request`] before
//! issuing protocol requests. That capability performs a server-clock lease
//! heartbeat, so an expired lease cannot reach the adapter. Lease loss is
//! terminal for the current operation and must not trigger token rotation.
//! Authentication expiry may be retried once by the application orchestration
//! layer, while risk-control responses remain terminal.
//!
//! The pure [`parse_article_list_payload`] boundary is executable without a
//! browser or network. It accepts the current `data` envelope and the legacy
//! `reviews[].subReviews[]` envelope, normalizes nested review records, and
//! rejects unsupported or unsafe values before they reach persistence. The
//! concrete browser adapter below keeps the authenticated WebDriver capability
//! private and parses only the response body needed by source synchronization.
//! QR exchange remains outside this protocol adapter; credential refresh is
//! handled by the application authentication lifecycle.

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use thiserror::Error;
use url::Url;

use crate::domain::{credentials::WeReadAccountId, source::VerifiedWechatArticleUrl};

use super::{
    browser_pool::{AccountLeaseError, AccountLeaseStore},
    pacing::PacingController,
    webdriver::AuthenticatedRequest,
};

const WEREAD_HOST: &str = "i.weread.qq.com";
const WEREAD_ARTICLE_LIST_PATH: &str = "/web/mp/articles";

/// One normalized article-list entry returned by the WeRead adapter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeReadArticleReference {
    /// Stable upstream review ID used for article idempotency.
    pub review_id: String,
    /// Recovered public article URL, when the list response includes one.
    pub article_url: Option<VerifiedWechatArticleUrl>,
    /// Optional upstream title hint.
    pub title: Option<String>,
    /// Optional plain-text summary from the list response.
    pub summary: Option<String>,
    /// Optional public-account author name.
    pub author: Option<String>,
    /// Optional cover URL from the list response.
    pub cover_url: Option<String>,
    /// Optional publication timestamp from the list response.
    pub published_at: Option<DateTime<Utc>>,
}

impl WeReadArticleReference {
    /// Constructs a normalized reference and rejects an empty stable identity.
    pub fn new(
        review_id: impl Into<String>,
        article_url: Option<VerifiedWechatArticleUrl>,
        title: Option<String>,
    ) -> Result<Self, WeReadAdapterError> {
        let review_id = review_id.into().trim().to_owned();
        if review_id.is_empty() {
            return Err(WeReadAdapterError::InvalidReviewId);
        }
        Ok(Self {
            review_id,
            article_url,
            title,
            summary: None,
            author: None,
            cover_url: None,
            published_at: None,
        })
    }
}

/// Errors exposed by authenticated WeRead protocol adapters.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum WeReadAdapterError {
    /// The account lease was lost before a request could be issued.
    #[error("WeRead account lease lost for {account_id}")]
    LeaseLost { account_id: WeReadAccountId },
    /// The account lease backend could not prove request ownership.
    #[error("WeRead account lease backend error: {0}")]
    LeaseBackend(String),
    /// The account session expired according to a WeRead business response.
    #[error("WeRead authentication expired (code {code})")]
    AuthenticationExpired {
        /// Stable upstream business error code.
        code: i64,
    },
    /// WeRead rejected the request for risk-control or rate-limit reasons.
    #[error("WeRead request was risk-controlled (code {code})")]
    RiskControlled {
        /// Stable upstream business error code.
        code: i64,
    },
    /// The upstream response did not match a supported shape.
    #[error("WeRead protocol error: {0}")]
    Protocol(String),
    /// The authenticated browser transport failed before a valid response
    /// could be parsed.
    #[error("WeRead browser operation failed: {0}")]
    Browser(String),
    /// A response omitted the stable identity needed for idempotent storage.
    #[error("WeRead article review_id must not be empty")]
    InvalidReviewId,
    /// An upstream URL could not be reduced to a verified public WeChat URL.
    #[error("WeRead article URL is not a verified public WeChat URL")]
    InvalidArticleUrl,
}

/// Validation failure for the authenticated WeRead article-list endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WeReadEndpointError {
    /// The endpoint is not the HTTPS WeRead article-list API.
    #[error(
        "WeRead article-list endpoint must be HTTPS i.weread.qq.com/web/mp/articles without credentials, fragments, or a non-default port"
    )]
    Invalid,
}

impl From<AccountLeaseError> for WeReadAdapterError {
    fn from(error: AccountLeaseError) -> Self {
        match error {
            AccountLeaseError::LeaseLost { account_id } => Self::LeaseLost { account_id },
            other => Self::LeaseBackend(other.to_string()),
        }
    }
}

/// Parses a WeRead article-list response into validated references.
///
/// Current mobile responses use `{"data": [...]}`. Older deployments use
/// `{"reviews": [{"subReviews": [{"review": {...}}]}]}`. Entries without a
/// stable `reviewId` or title are ignored because they cannot become useful
/// articles. Present URLs are always converted to
/// [`VerifiedWechatArticleUrl`]; an invalid URL fails the complete response so
/// an unsafe value cannot be passed to a later browser operation.
pub fn parse_article_list_payload(
    payload: &Value,
) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
    let object = payload
        .as_object()
        .ok_or_else(|| WeReadAdapterError::Protocol("response must be a JSON object".to_owned()))?;
    if let Some(error) = response_error(object)? {
        return Err(error);
    }

    if let Some(data) = object.get("data") {
        let entries = data
            .as_array()
            .ok_or_else(|| WeReadAdapterError::Protocol("data must be an array".to_owned()))?;
        return parse_entries(entries, None);
    }

    let Some(reviews) = object.get("reviews") else {
        return Err(WeReadAdapterError::Protocol(
            "response has no supported article-list envelope".to_owned(),
        ));
    };
    let reviews = reviews
        .as_array()
        .ok_or_else(|| WeReadAdapterError::Protocol("reviews must be an array".to_owned()))?;
    let mut result = Vec::new();
    for group in reviews.iter().filter_map(Value::as_object) {
        let group_time = group.get("createTime").and_then(unix_timestamp);
        let Some(sub_reviews) = group.get("subReviews") else {
            continue;
        };
        let sub_reviews = sub_reviews.as_array().ok_or_else(|| {
            WeReadAdapterError::Protocol("subReviews must be an array".to_owned())
        })?;
        result.extend(parse_entries(sub_reviews, group_time)?);
    }
    Ok(result)
}

fn parse_entries(
    entries: &[Value],
    group_time: Option<DateTime<Utc>>,
) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
    entries
        .iter()
        .filter_map(Value::as_object)
        .map(|entry| parse_entry(entry, group_time))
        .filter_map(|result| match result {
            Ok(Some(reference)) => Some(Ok(reference)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn parse_entry(
    entry: &Map<String, Value>,
    group_time: Option<DateTime<Utc>>,
) -> Result<Option<WeReadArticleReference>, WeReadAdapterError> {
    let review = entry
        .get("review")
        .and_then(Value::as_object)
        .unwrap_or(entry);
    let empty = Map::new();
    let mp_info = review
        .get("mpInfo")
        .and_then(Value::as_object)
        .unwrap_or(&empty);

    let Some(review_id) = first_text(&[review, entry], &["reviewId"]) else {
        return Ok(None);
    };
    let Some(title) = first_text(&[review, mp_info], &["title"]) else {
        return Ok(None);
    };

    let article_url = article_url(&[mp_info, review])?;
    let summary = first_text(&[mp_info, review], &["content"]);
    let author = first_text(&[mp_info], &["mp_name", "mpName"]);
    let cover_url = first_text(&[mp_info], &["pic_url", "picUrl"]);
    let published_at = first_timestamp(&[mp_info, review], &["time", "createTime"]).or(group_time);

    Ok(Some(WeReadArticleReference {
        review_id,
        article_url,
        title: Some(title),
        summary,
        author,
        cover_url,
        published_at,
    }))
}

fn response_error(
    object: &Map<String, Value>,
) -> Result<Option<WeReadAdapterError>, WeReadAdapterError> {
    let Some(value) = object.get("errcode").or_else(|| object.get("errCode")) else {
        return Ok(None);
    };
    let Some(code) = integer_value(value) else {
        return Err(WeReadAdapterError::Protocol(
            "error code must be an integer".to_owned(),
        ));
    };
    if code == 0 {
        return Ok(None);
    }
    Ok(Some(match code {
        -2012 => WeReadAdapterError::AuthenticationExpired { code },
        -2041 | -2010 => WeReadAdapterError::RiskControlled { code },
        _ => WeReadAdapterError::Protocol(format!("upstream error code {code}")),
    }))
}

fn article_url(
    objects: &[&Map<String, Value>],
) -> Result<Option<VerifiedWechatArticleUrl>, WeReadAdapterError> {
    let Some(value) = first_text(objects, &["doc_url", "docUrl", "url"]) else {
        let Some(original) = first_text(objects, &["originalId"]) else {
            return Ok(None);
        };
        return verify_article_url(&original);
    };
    verify_article_url(&value)
}

fn verify_article_url(value: &str) -> Result<Option<VerifiedWechatArticleUrl>, WeReadAdapterError> {
    let value = value.trim();
    let candidate = if value.starts_with("http://") || value.starts_with("https://") {
        value.to_owned()
    } else if value.starts_with("/s") {
        format!("https://mp.weixin.qq.com{value}")
    } else if value.starts_with('?') || value.contains("__biz=") {
        format!(
            "https://mp.weixin.qq.com/s?{}",
            value.trim_start_matches('?')
        )
    } else {
        let mut url = url::Url::parse("https://mp.weixin.qq.com/")
            .expect("static WeChat article URL must parse");
        url.path_segments_mut()
            .expect("static WeChat article URL must have path segments")
            .push("s")
            .push(value);
        url.to_string()
    };
    let article_url = VerifiedWechatArticleUrl::parse(&candidate)
        .map_err(|_| WeReadAdapterError::InvalidArticleUrl)?;
    let is_article_path = url::Url::parse(article_url.as_str())
        .is_ok_and(|url| url.path() == "/s" || url.path().starts_with("/s/"));
    if !is_article_path {
        return Err(WeReadAdapterError::InvalidArticleUrl);
    }
    Ok(Some(article_url))
}

fn first_text(objects: &[&Map<String, Value>], keys: &[&str]) -> Option<String> {
    objects.iter().find_map(|object| {
        keys.iter().find_map(|key| {
            let value = object.get(*key)?;
            match value {
                Value::String(value) => {
                    let value = value.trim();
                    (!value.is_empty()).then(|| value.to_owned())
                }
                Value::Number(value) => Some(value.to_string()),
                _ => None,
            }
        })
    })
}

fn first_timestamp(objects: &[&Map<String, Value>], keys: &[&str]) -> Option<DateTime<Utc>> {
    objects.iter().find_map(|object| {
        keys.iter()
            .find_map(|key| object.get(*key).and_then(unix_timestamp))
    })
}

fn unix_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    integer_value(value).and_then(|seconds| DateTime::from_timestamp(seconds, 0))
}

fn integer_value(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str()?.trim().parse().ok())
}

/// Thirtyfour-backed authenticated article-list adapter.
#[derive(Debug, Clone)]
pub struct BrowserWeReadAdapter {
    endpoint: Url,
    pacing: Option<PacingController>,
}

impl BrowserWeReadAdapter {
    /// Creates an adapter for the validated WeRead article-list endpoint.
    pub fn new(mut endpoint: Url) -> Result<Self, WeReadEndpointError> {
        validate_article_list_endpoint(&endpoint)?;
        if endpoint.port() == Some(443) {
            endpoint
                .set_port(None)
                .map_err(|_| WeReadEndpointError::Invalid)?;
        }
        Ok(Self {
            endpoint,
            pacing: None,
        })
    }

    /// Adds the shared pacing controller used before authenticated requests.
    pub fn with_pacing(mut self, pacing: PacingController) -> Self {
        self.pacing = Some(pacing);
        self
    }

    /// Returns the configured endpoint without exposing browser credentials.
    pub fn endpoint(&self) -> &Url {
        &self.endpoint
    }
}

impl<R> WeReadAdapter<R> for BrowserWeReadAdapter
where
    R: AccountLeaseStore,
{
    async fn list_articles(
        &self,
        book_id: &str,
        mut request: AuthenticatedRequest<'_, R>,
    ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
        let endpoint = article_list_endpoint(&self.endpoint, book_id)?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        if let Some(pacing) = &self.pacing {
            pacing.wait(crate::domain::pacing::DelayKind::Request).await;
        }
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        request
            .goto(endpoint.as_str())
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.to_string()))?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        let body = request
            .body_text()
            .await
            .map_err(|error| WeReadAdapterError::Browser(error.to_string()))?;
        request.ensure_usable().map_err(WeReadAdapterError::from)?;
        parse_article_list_body(&body)
    }
}

fn validate_article_list_endpoint(endpoint: &Url) -> Result<(), WeReadEndpointError> {
    if endpoint.scheme() != "https"
        || endpoint.host_str() != Some(WEREAD_HOST)
        || endpoint.path() != WEREAD_ARTICLE_LIST_PATH
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
        || endpoint.port().is_some_and(|port| port != 443)
    {
        return Err(WeReadEndpointError::Invalid);
    }
    Ok(())
}

fn article_list_endpoint(endpoint: &Url, book_id: &str) -> Result<Url, WeReadAdapterError> {
    let book_id = book_id.trim();
    if book_id.is_empty() {
        return Err(WeReadAdapterError::Protocol(
            "book_id must not be empty".to_owned(),
        ));
    }
    let mut endpoint = endpoint.clone();
    let configured_query = endpoint
        .query_pairs()
        .filter(|(key, _)| key != "bookId" && key != "offset")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    endpoint.set_query(None);
    {
        let mut query = endpoint.query_pairs_mut();
        for (key, value) in configured_query {
            query.append_pair(&key, &value);
        }
        query
            .append_pair("bookId", book_id)
            .append_pair("offset", "0");
    }
    Ok(endpoint)
}

fn parse_article_list_body(body: &str) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
    let payload = serde_json::from_str::<Value>(body)
        .map_err(|error| WeReadAdapterError::Protocol(format!("response was not JSON: {error}")))?;
    parse_article_list_payload(&payload)
}

/// Port for authenticated WeRead account/list operations.
#[allow(async_fn_in_trait)]
pub trait WeReadAdapter<R>: Send + Sync
where
    R: AccountLeaseStore,
{
    /// Lists normalized article references using a freshly heartbeated request.
    async fn list_articles(
        &self,
        book_id: &str,
        request: AuthenticatedRequest<'_, R>,
    ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use crate::{
        acquisition::browser_pool::BrowserPool,
        persistence::repositories::account_lease_repository::MemoryAccountLeaseRepository,
    };

    fn account_id() -> WeReadAccountId {
        WeReadAccountId::from_uuid(Uuid::from_u128(1))
    }

    struct FakeAdapter;

    impl<R> WeReadAdapter<R> for FakeAdapter
    where
        R: AccountLeaseStore,
    {
        async fn list_articles(
            &self,
            book_id: &str,
            request: AuthenticatedRequest<'_, R>,
        ) -> Result<Vec<WeReadArticleReference>, WeReadAdapterError> {
            assert_eq!(book_id, "book-1");
            let _account_id = request.account_id();
            Ok(vec![
                WeReadArticleReference::new("review-1", None, None).unwrap()
            ])
        }
    }

    #[test]
    fn rejects_missing_stable_article_identity() {
        assert_eq!(
            WeReadArticleReference::new("  ", None, None),
            Err(WeReadAdapterError::InvalidReviewId)
        );
    }

    #[test]
    fn trims_stable_article_identity() {
        let reference = WeReadArticleReference::new(" review-1 ", None, None).unwrap();
        assert_eq!(reference.review_id, "review-1");
        assert!(reference.published_at.is_none());
    }

    #[test]
    fn parses_current_response_and_normalizes_nested_metadata() {
        let payload = json!({
            "data": [
                {
                    "reviewId": " review-current ",
                    "title": " Current title ",
                    "createTime": "1710000000",
                    "mpInfo": {
                        "content": " summary ",
                        "mpName": " Account ",
                        "picUrl": "https://mp.weixin.qq.com/logo.png",
                        "docUrl": "https://mp.weixin.qq.com/s/current"
                    }
                },
                null,
                {"reviewId": "without-title"}
            ]
        });

        let articles = parse_article_list_payload(&payload).expect("current response should parse");
        assert_eq!(articles.len(), 1);
        let article = &articles[0];
        assert_eq!(article.review_id, "review-current");
        assert_eq!(article.title.as_deref(), Some("Current title"));
        assert_eq!(article.summary.as_deref(), Some("summary"));
        assert_eq!(article.author.as_deref(), Some("Account"));
        assert_eq!(
            article.cover_url.as_deref(),
            Some("https://mp.weixin.qq.com/logo.png")
        );
        assert_eq!(
            article.published_at,
            DateTime::from_timestamp(1_710_000_000, 0)
        );
        assert_eq!(
            article
                .article_url
                .as_ref()
                .map(VerifiedWechatArticleUrl::as_str),
            Some("https://mp.weixin.qq.com/s/current")
        );
    }

    #[test]
    fn parses_legacy_response_and_uses_group_time_as_fallback() {
        let payload = json!({
            "reviews": [{
                "createTime": 1700000000,
                "subReviews": [
                    {
                        "review": {
                            "reviewId": "legacy-1",
                            "mpInfo": {
                                "title": "Legacy title",
                                "originalId": "legacy-token"
                            }
                        }
                    }
                ]
            }]
        });

        let articles = parse_article_list_payload(&payload).expect("legacy response should parse");
        assert_eq!(articles.len(), 1);
        assert_eq!(
            articles[0].published_at,
            DateTime::from_timestamp(1_700_000_000, 0)
        );
        assert_eq!(
            articles[0]
                .article_url
                .as_ref()
                .map(VerifiedWechatArticleUrl::as_str),
            Some("https://mp.weixin.qq.com/s/legacy-token")
        );
    }

    #[test]
    fn supports_article_url_variants_but_rejects_unsafe_hosts() {
        for (value, expected) in [
            ("/s/path-token", "https://mp.weixin.qq.com/s/path-token"),
            (
                "?__biz=MzA==&mid=1",
                "https://mp.weixin.qq.com/s?__biz=MzA==&mid=1",
            ),
            (
                "token with spaces",
                "https://mp.weixin.qq.com/s/token%20with%20spaces",
            ),
        ] {
            let payload = json!({
                "data": [{
                    "reviewId": "review",
                    "title": "title",
                    "mpInfo": {"originalId": value}
                }]
            });
            let articles = parse_article_list_payload(&payload).expect("URL variant should parse");
            assert_eq!(articles[0].article_url.as_ref().unwrap().as_str(), expected);
        }

        let unsafe_payload = json!({
            "data": [{
                "reviewId": "review",
                "title": "title",
                "mpInfo": {"docUrl": "https://example.com/not-wechat"}
            }]
        });
        assert_eq!(
            parse_article_list_payload(&unsafe_payload),
            Err(WeReadAdapterError::InvalidArticleUrl)
        );

        for value in [
            "https://mp.weixin.qq.com/cgi-bin/appmsg",
            "https://mp.weixin.qq.com/script",
            "/script",
        ] {
            let payload = json!({
                "data": [{
                    "reviewId": "review",
                    "title": "title",
                    "mpInfo": {"docUrl": value}
                }]
            });
            assert_eq!(
                parse_article_list_payload(&payload),
                Err(WeReadAdapterError::InvalidArticleUrl),
                "non-article path should be rejected: {value}"
            );
        }
    }

    #[test]
    fn classifies_business_errors_without_exposing_response_text() {
        assert_eq!(
            parse_article_list_payload(&json!({"errCode": "-2012", "errMsg": "secret"})),
            Err(WeReadAdapterError::AuthenticationExpired { code: -2012 })
        );
        assert_eq!(
            parse_article_list_payload(&json!({"errcode": -2041})),
            Err(WeReadAdapterError::RiskControlled { code: -2041 })
        );
        assert_eq!(
            parse_article_list_payload(&json!({"errcode": 1234})),
            Err(WeReadAdapterError::Protocol(
                "upstream error code 1234".to_owned()
            ))
        );
    }

    #[test]
    fn rejects_malformed_envelopes_instead_of_returning_an_empty_success() {
        assert_eq!(
            parse_article_list_payload(&json!([])),
            Err(WeReadAdapterError::Protocol(
                "response must be a JSON object".to_owned()
            ))
        );
        assert_eq!(
            parse_article_list_payload(&json!({"data": {}})),
            Err(WeReadAdapterError::Protocol(
                "data must be an array".to_owned()
            ))
        );
        assert_eq!(
            parse_article_list_payload(&json!({"reviews": [{"subReviews": {}}]})),
            Err(WeReadAdapterError::Protocol(
                "subReviews must be an array".to_owned()
            ))
        );
    }

    #[test]
    fn maps_non_json_browser_bodies_to_protocol_errors() {
        assert!(matches!(
            parse_article_list_body("<html>login required</html>"),
            Err(WeReadAdapterError::Protocol(message)) if message.starts_with("response was not JSON:")
        ));
    }

    #[test]
    fn appends_book_identity_without_discarding_configured_endpoint_query() {
        let adapter = BrowserWeReadAdapter::new(
            "https://i.weread.qq.com/web/mp/articles?count=100"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let endpoint = article_list_endpoint(adapter.endpoint(), "book/with spaces").unwrap();
        assert_eq!(
            endpoint.as_str(),
            "https://i.weread.qq.com/web/mp/articles?count=100&bookId=book%2Fwith+spaces&offset=0"
        );
    }

    #[test]
    fn replaces_configured_book_identity_and_offset() {
        let endpoint: Url =
            "https://i.weread.qq.com/web/mp/articles?bookId=wrong&offset=99&count=100"
                .parse()
                .unwrap();
        let endpoint = article_list_endpoint(&endpoint, "right").unwrap();

        assert_eq!(
            endpoint.as_str(),
            "https://i.weread.qq.com/web/mp/articles?count=100&bookId=right&offset=0"
        );
    }

    #[test]
    fn rejects_an_empty_book_identity_before_touching_the_browser() {
        let endpoint: Url = "https://i.weread.qq.com/web/mp/articles".parse().unwrap();
        assert_eq!(
            article_list_endpoint(&endpoint, "  "),
            Err(WeReadAdapterError::Protocol(
                "book_id must not be empty".to_owned()
            ))
        );
    }

    #[test]
    fn rejects_untrusted_article_list_endpoints_at_construction() {
        for value in [
            "http://i.weread.qq.com/web/mp/articles",
            "https://example.test/web/mp/articles",
            "https://i.weread.qq.com/web/mp/other",
            "https://i.weread.qq.com:8443/web/mp/articles",
            "https://user:password@i.weread.qq.com/web/mp/articles",
            "https://i.weread.qq.com/web/mp/articles#fragment",
        ] {
            let endpoint = value.parse().expect("test URL should parse");
            assert!(
                matches!(
                    BrowserWeReadAdapter::new(endpoint),
                    Err(WeReadEndpointError::Invalid)
                ),
                "unsafe endpoint should be rejected: {value}"
            );
        }
    }

    #[test]
    fn normalizes_the_default_https_port_for_the_article_list_endpoint() {
        let adapter = BrowserWeReadAdapter::new(
            "https://i.weread.qq.com:443/web/mp/articles"
                .parse()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            adapter.endpoint().as_str(),
            "https://i.weread.qq.com/web/mp/articles"
        );
    }

    #[tokio::test]
    async fn request_pacing_waits_before_authenticated_navigation() {
        use crate::domain::pacing::{DelayDistribution, PacingPolicy};

        let delay = DelayDistribution::new(10.0, 0.0, 10.0, 10.0).unwrap();
        let policy = PacingPolicy::new(
            delay,
            delay,
            delay,
            delay,
            1,
            1,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
        let pacing = PacingController::from_seed(policy, 1);
        let started = tokio::time::Instant::now();
        pacing.wait(crate::domain::pacing::DelayKind::Request).await;
        assert!(started.elapsed() >= std::time::Duration::from_millis(8));
    }

    #[tokio::test]
    async fn adapter_receives_only_an_authenticated_session() {
        let repository = MemoryAccountLeaseRepository::new(Utc::now());
        let pool = BrowserPool::new(1).unwrap();
        let mut session = pool
            .open_authenticated(
                repository,
                account_id(),
                "worker-a",
                chrono::Duration::seconds(30),
            )
            .await
            .unwrap()
            .unwrap();

        let request = session
            .prepare_request(chrono::Duration::seconds(30))
            .await
            .unwrap();
        let entries = FakeAdapter.list_articles("book-1", request).await.unwrap();
        assert_eq!(entries[0].review_id, "review-1");
    }
}
