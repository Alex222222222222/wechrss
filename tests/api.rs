//! PostgreSQL-backed integration coverage for the public feed HTTP boundary.

use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use chrono::{Duration, Utc};
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;
use wechrss::{
    application::{
        feed_service::{
            FeedRebuildJobConfig, FeedService, FeedServiceConfig, PostgresFeedRebuildQueue,
        },
        feed_token_service::FeedTokenService,
    },
    domain::source::{FeedRevision, SourceId},
    persistence::{
        repositories::{
            feed_cache_repository::{
                FeedBuildLeaseRepository, FeedCachePublishResult, FeedCacheTransactionRepository,
                PostgresFeedBuildLeaseRepository, PostgresFeedCacheRepository,
            },
            feed_token_repository::PostgresFeedTokenRepository,
            job_repository::PostgresJobRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
    rss::renderer::{RenderArticle, RenderFeedInput, RssRenderer},
    web::api::feed_router,
};

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn feed_route_serves_xml_and_honors_conditional_requests(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    publish_cache(&pool, source_id).await;
    let token = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()))
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let app = router(&pool);
    let path = format!("/feeds/{}.xml", token.as_str());

    let response = app
        .clone()
        .oneshot(get_request(&path, None))
        .await
        .expect("feed request should complete");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/rss+xml; charset=utf-8")
    );
    assert_eq!(
        response
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("public, max-age=0, must-revalidate")
    );
    assert!(response.headers().contains_key(header::LAST_MODIFIED));
    let etag = response
        .headers()
        .get(header::ETAG)
        .expect("feed response should include an ETag")
        .to_str()
        .expect("ETag should be valid text")
        .to_owned();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("feed body should be readable");
    assert!(String::from_utf8_lossy(&body).contains("API integration article"));

    let response = app
        .oneshot(get_request(&path, Some(&etag)))
        .await
        .expect("conditional feed request should complete");
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("304 body should be readable")
            .len(),
        0
    );
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn feed_route_does_not_enumerate_invalid_unknown_or_revoked_tokens(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    let token_service = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()));
    let token = token_service
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let unknown = wechrss::domain::feed_token::FeedToken::generate();
    let app = router(&pool);

    let invalid_status = status(&app, "/feeds/not-a-token.xml").await;
    let unknown_status = status(&app, &format!("/feeds/{}.xml", unknown.as_str())).await;
    token_service
        .revoke(source_id)
        .await
        .expect("token should be revoked");
    let revoked_status = status(&app, &format!("/feeds/{}.xml", token.as_str())).await;

    assert_eq!(invalid_status, StatusCode::NOT_FOUND);
    assert_eq!(unknown_status, StatusCode::NOT_FOUND);
    assert_eq!(revoked_status, StatusCode::NOT_FOUND);
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn feed_route_rejects_non_xml_and_nested_paths(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    let token = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()))
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let app = router(&pool);
    let raw_token = token.as_str();

    assert_eq!(
        status(&app, &format!("/feeds/{raw_token}")).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(&app, &format!("/feeds/{raw_token}.json")).await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        status(&app, &format!("/feeds/{raw_token}.xml/extra")).await,
        StatusCode::NOT_FOUND
    );
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn feed_route_returns_retry_after_for_a_cache_miss(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    let token = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()))
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let app = router(&pool);
    let path = format!("/feeds/{}.xml", token.as_str());

    let response = app
        .oneshot(get_request(&path, None))
        .await
        .expect("cache-miss request should complete");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response
            .headers()
            .get(header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok()),
        Some("5")
    );
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("cache-miss body should be readable")
            .len(),
        0
    );

    let queued: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE source_id = $1 AND job_type = 'feed_rebuild'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("rebuild job should be queryable");
    assert_eq!(queued, 1);
}

fn router(pool: &PgPool) -> axum::Router {
    let token_service = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()));
    let queue = PostgresFeedRebuildQueue::new(
        PostgresJobRepository::new(pool.clone()),
        FeedRebuildJobConfig::default(),
    );
    let feed_service = FeedService::new(
        PostgresFeedCacheRepository::new(pool.clone()),
        queue,
        FeedServiceConfig::default(),
    );
    feed_router(token_service, feed_service)
}

fn get_request(path: &str, etag: Option<&str>) -> Request<Body> {
    let mut request = Request::builder().uri(path);
    if let Some(etag) = etag {
        request = request.header(header::IF_NONE_MATCH, etag);
    }
    request
        .body(Body::empty())
        .expect("request should be valid")
}

async fn status(app: &axum::Router, path: &str) -> StatusCode {
    app.clone()
        .oneshot(get_request(path, None))
        .await
        .expect("request should complete")
        .status()
}

async fn insert_source(pool: &PgPool) -> SourceId {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    sqlx::query(
        "INSERT INTO sources (id, book_id, display_name, article_url, feed_revision) VALUES ($1, $2, $3, $4, 1)",
    )
    .bind(source_id.as_uuid())
    .bind(format!("api-book-{source_id}"))
    .bind("API integration source")
    .bind("https://mp.weixin.qq.com/s/api-test")
    .execute(pool)
    .await
    .expect("source should be insertable");
    source_id
}

async fn publish_cache(pool: &PgPool, source_id: SourceId) {
    let generated_at = Utc::now() - Duration::seconds(1);
    let candidate = RssRenderer
        .render(RenderFeedInput {
            source_id,
            title: "API integration feed".to_owned(),
            feed_url: "https://rss.example.test/api-feed".to_owned(),
            description: "API integration feed".to_owned(),
            source_revision: FeedRevision::from_u64(1),
            generated_at,
            expires_at: generated_at + Duration::minutes(30),
            articles: vec![RenderArticle {
                review_id: "api-review-1".to_owned(),
                title: "API integration article".to_owned(),
                author: Some("Test author".to_owned()),
                summary: Some("Test summary".to_owned()),
                original_url: Some("https://mp.weixin.qq.com/s/api-article".to_owned()),
                published_at: generated_at,
                content_html: "<p>Test article body</p>".to_owned(),
            }],
        })
        .expect("candidate should render")
        .into_candidate();
    let leases = PostgresFeedBuildLeaseRepository::new(pool.clone());
    let lease = leases
        .acquire_build(source_id, "api-builder", Duration::minutes(5))
        .await
        .expect("build lease should be acquired")
        .expect("source should not have another builder");
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .feed_cache()
        .publish_if_current(candidate, lease.owner(), lease.token())
        .await
        .expect("cache should publish");
    assert!(matches!(result, FeedCachePublishResult::Published(_)));
    unit_of_work
        .commit()
        .await
        .expect("cache publication should commit");
}
