//! REST API route and DTO boundary.
//!
//! Documents administrative endpoints for authentication, sources, articles,
//! manual sync, backfill, health/readiness, and job status. It also documents
//! the public/tokenized feed and the optional media route used by archived
//! binary assets.
//!
//! Request DTO validation happens here; use-case sequencing belongs to
//! application services. Access and refresh tokens must never appear in
//! response DTOs, tracing fields, or error messages.
//!
//! The feed handler delegates to `FeedService`, emits its ETag/Last-Modified and
//! freshness metadata, and supports conditional requests without invoking
//! acquisition code. It must not reset a stale row to a fresh 30-minute HTTP
//! lifetime.
//! A true cache miss owned by another live feed-build lease may poll only for a
//! short configured bound; if no document appears, the handler maps the typed
//! result to `503 Service Unavailable` with `Retry-After`.
//!
//! Liveness is process-only. API readiness requires PostgreSQL but remains ready
//! to serve persisted feeds when the browser component is degraded; browser
//! health is reported separately and gates worker claims.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::Response,
    routing::get,
    Router,
};
use chrono::Duration;

use crate::{
    application::{
        feed_service::{
            FeedCacheStatus, FeedDelivery, FeedRebuildQueue, FeedService, FeedServiceError,
        },
        feed_token_service::{FeedTokenService, FeedTokenServiceError},
    },
    domain::feed::FeedCache,
    persistence::repositories::{
        feed_cache_repository::FeedCacheRepository, feed_token_repository::FeedTokenRepository,
    },
};

/// Builds the public, tokenized RSS route.
///
/// Token resolution and cache delivery remain separate application calls. A
/// syntactically invalid, unknown, or revoked token always receives the same
/// `404` response, while storage failures receive `503` without exposing
/// repository details. The route never renders XML or starts browser work.
pub fn feed_router<R, C, Q>(
    token_service: FeedTokenService<R>,
    feed_service: FeedService<C, Q>,
) -> Router
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
{
    let state = Arc::new(FeedApiState {
        token_service,
        feed_service,
    });
    Router::new()
        // Axum does not allow a literal suffix in the same segment as a path
        // parameter, so the handler validates the exact `.xml` shape below.
        .route("/feeds/{*feed_path}", get(feed::<R, C, Q>))
        .with_state(state)
}

struct FeedApiState<R, C, Q> {
    token_service: FeedTokenService<R>,
    feed_service: FeedService<C, Q>,
}

async fn feed<R, C, Q>(
    State(state): State<Arc<FeedApiState<R, C, Q>>>,
    Path(feed_path): Path<String>,
    headers: HeaderMap,
) -> Response
where
    R: FeedTokenRepository + 'static,
    C: FeedCacheRepository + 'static,
    Q: FeedRebuildQueue + 'static,
{
    let Some(feed_token) = feed_path
        .strip_suffix(".xml")
        .filter(|token| !token.is_empty() && !token.contains('/'))
    else {
        return empty_response(StatusCode::NOT_FOUND);
    };

    let source_id = match state.token_service.resolve(feed_token).await {
        Ok(Some(source_id)) => source_id,
        Ok(None) | Err(FeedTokenServiceError::InvalidToken) => {
            return empty_response(StatusCode::NOT_FOUND);
        }
        Err(FeedTokenServiceError::Repository(_)) => {
            return empty_response(StatusCode::SERVICE_UNAVAILABLE);
        }
    };

    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let request = crate::application::feed_service::FeedRequest::new(source_id, if_none_match);

    match state.feed_service.get_feed(request).await {
        Ok(FeedDelivery::Cached { cache, status, .. }) => cached_response(
            StatusCode::OK,
            cache,
            status,
            state.feed_service.stale_while_revalidate(),
        ),
        Ok(FeedDelivery::NotModified { cache, status, .. }) => cached_response(
            StatusCode::NOT_MODIFIED,
            cache,
            status,
            state.feed_service.stale_while_revalidate(),
        ),
        Ok(FeedDelivery::Unavailable { retry_after, .. }) => {
            let mut response = empty_response(StatusCode::SERVICE_UNAVAILABLE);
            let value = HeaderValue::try_from(retry_after_seconds(retry_after).to_string())
                .expect("numeric Retry-After values are valid headers");
            response.headers_mut().insert(header::RETRY_AFTER, value);
            response
        }
        Err(FeedServiceError::InvalidSourceId | FeedServiceError::Cache(_)) => {
            empty_response(StatusCode::SERVICE_UNAVAILABLE)
        }
    }
}

fn cached_response(
    status_code: StatusCode,
    cache: FeedCache,
    status: FeedCacheStatus,
    stale_while_revalidate: Duration,
) -> Response {
    let etag = match HeaderValue::try_from(format!("\"{}\"", cache.etag())) {
        Ok(value) => value,
        Err(_) => return empty_response(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let last_modified = cache
        .updated_at()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let cache_control = match status {
        // The cache repository currently exposes freshness as a boolean. A
        // zero max-age is conservative until it also returns the exact
        // database-clocked remaining lifetime.
        FeedCacheStatus::Fresh => "public, max-age=0, must-revalidate".to_owned(),
        FeedCacheStatus::Stale => format!(
            "public, max-age=0, stale-while-revalidate={}",
            retry_after_seconds(stale_while_revalidate)
        ),
    };
    let mut response = Response::new(Body::from(if status_code == StatusCode::OK {
        cache.xml_bytes().to_vec()
    } else {
        Vec::new()
    }));
    *response.status_mut() = status_code;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/rss+xml; charset=utf-8"),
    );
    headers.insert(header::ETAG, etag);
    if let Ok(value) = HeaderValue::try_from(last_modified) {
        headers.insert(header::LAST_MODIFIED, value);
    }
    if let Ok(value) = HeaderValue::try_from(cache_control) {
        headers.insert(header::CACHE_CONTROL, value);
    }
    response
}

fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

fn retry_after_seconds(delay: Duration) -> u64 {
    delay
        .to_std()
        .map(|delay| {
            delay
                .as_secs()
                .saturating_add(u64::from(delay.subsec_nanos() != 0))
                .max(1)
        })
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn retry_after_rounds_up_and_never_returns_zero() {
        assert_eq!(retry_after_seconds(Duration::zero()), 1);
        assert_eq!(retry_after_seconds(Duration::milliseconds(1_001)), 2);
        assert_eq!(retry_after_seconds(Duration::seconds(-1)), 1);
    }

    #[test]
    fn http_date_is_rendered_in_a_header_safe_rfc_style() {
        let timestamp = DateTime::<Utc>::from_timestamp(0, 0).expect("epoch should be valid");
        assert_eq!(
            timestamp.format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
            "Thu, 01 Jan 1970 00:00:00 GMT"
        );
    }
}
