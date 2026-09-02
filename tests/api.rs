//! PostgreSQL-backed integration coverage for the public feed HTTP boundary.

use axum::{
    body::{to_bytes, Body},
    extract::ConnectInfo,
    http::{header, Request, StatusCode},
    Extension,
};
use chrono::{Duration, Utc};
use chrono_tz::UTC;
use secrecy::SecretString;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;
use werrss::{
    application::{
        feed_service::{
            FeedRebuildJobConfig, FeedService, FeedServiceConfig, PostgresFeedRebuildQueue,
        },
        feed_token_service::FeedTokenService,
        source_service::SourceService,
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
            source_repository::PostgresSourceRepository,
            sync_run_repository::PostgresSyncRunRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
    rss::renderer::{RenderArticle, RenderFeedInput, RssRenderer},
    web::{admin::admin_router, api::feed_router, auth::AdminAuthenticator},
};

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn admin_login_requires_authentication_and_csrf_for_mutations(pool: PgPool) {
    let app = admin_app(&pool);
    let login = app
        .clone()
        .oneshot(json_request(
            "/api/admin/login",
            serde_json::json!({"username": "admin", "password": "wrong"}),
        ))
        .await
        .expect("login request should complete");
    assert_eq!(login.status(), StatusCode::UNAUTHORIZED);
    assert!(!login.headers().contains_key(header::SET_COOKIE));

    let page = app
        .clone()
        .oneshot(get_request("/admin", None))
        .await
        .expect("admin page request should complete");
    assert_eq!(page.status(), StatusCode::SEE_OTHER);
    assert_eq!(page.headers()[header::LOCATION], "/admin/login");

    let login = app
        .clone()
        .oneshot(json_request(
            "/api/admin/login",
            serde_json::json!({"username": "admin", "password": "correct horse"}),
        ))
        .await
        .expect("valid login request should complete");
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login.headers()[header::SET_COOKIE]
        .to_str()
        .expect("session cookie should be valid")
        .split(';')
        .next()
        .expect("cookie should contain a value")
        .to_owned();
    let login_body: serde_json::Value = serde_json::from_slice(
        &to_bytes(login.into_body(), usize::MAX)
            .await
            .expect("login body should be readable"),
    )
    .expect("login body should be JSON");
    let csrf = login_body["csrf_token"]
        .as_str()
        .expect("CSRF should be returned");

    let page = app
        .clone()
        .oneshot(admin_request("/admin", &cookie, None, None))
        .await
        .expect("authenticated admin page request should complete");
    assert_eq!(page.status(), StatusCode::OK);
    let page_body = String::from_utf8(
        to_bytes(page.into_body(), usize::MAX)
            .await
            .expect("admin page body should be readable")
            .to_vec(),
    )
    .expect("admin page should be UTF-8");
    assert!(page_body.contains("Werrss admin"));
    assert!(!page_body.contains("correct horse"));

    let sources = app
        .clone()
        .oneshot(admin_request("/api/admin/sources", &cookie, None, None))
        .await
        .expect("source list request should complete");
    assert_eq!(sources.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(sources.into_body(), usize::MAX).await.unwrap(),
        )
        .unwrap(),
        serde_json::json!([])
    );

    let missing_csrf = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            None,
            Some(serde_json::json!({
                "book_id": "book-csrf", "display_name": "CSRF", "article_url": "https://mp.weixin.qq.com/s/csrf"
            })),
        ))
        .await
        .expect("mutation request should complete");
    assert_eq!(missing_csrf.status(), StatusCode::FORBIDDEN);

    let created = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "book_id": "book-admin", "display_name": "Admin source", "article_url": "https://mp.weixin.qq.com/s/admin"
            })),
        ))
        .await
        .expect("source creation request should complete");
    assert_eq!(created.status(), StatusCode::CREATED);
    let created_body: serde_json::Value =
        serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap()).unwrap();
    let source_id = created_body["id"].as_str().unwrap().to_owned();

    let disabled = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/enabled"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({"enabled": false})),
        ))
        .await
        .expect("source enable mutation should complete");
    assert_eq!(disabled.status(), StatusCode::OK);
    let gated = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/gate"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({"gate": "risk_controlled"})),
        ))
        .await
        .expect("source gate mutation should complete");
    assert_eq!(gated.status(), StatusCode::OK);

    let feed_token = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/feed-token"),
            &cookie,
            Some(csrf),
            Some(serde_json::json!({})),
        ))
        .await
        .expect("feed token request should complete");
    assert_eq!(feed_token.status(), StatusCode::OK);
    let feed_path = serde_json::from_slice::<serde_json::Value>(
        &to_bytes(feed_token.into_body(), usize::MAX).await.unwrap(),
    )
    .unwrap()["feed_path"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(feed_path.starts_with("/feeds/"));
    assert!(feed_path.ends_with(".xml"));

    let missing_feed_token = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{}/feed-token", Uuid::from_u128(99_999)),
            &cookie,
            Some(csrf),
            None,
        ))
        .await
        .expect("missing source token request should complete");
    assert_eq!(missing_feed_token.status(), StatusCode::NOT_FOUND);

    let duplicate = app
        .clone()
        .oneshot(admin_request(
            "/api/admin/sources",
            &cookie,
            Some(csrf),
            Some(serde_json::json!({
                "book_id": "book-admin", "display_name": "Duplicate", "article_url": "https://mp.weixin.qq.com/s/duplicate"
            })),
        ))
        .await
        .expect("duplicate source request should complete");
    assert_eq!(duplicate.status(), StatusCode::CONFLICT);

    let invalid_history = app
        .clone()
        .oneshot(admin_request(
            &format!("/api/admin/sources/{source_id}/sync-runs?limit=0"),
            &cookie,
            None,
            None,
        ))
        .await
        .expect("invalid history request should complete");
    assert_eq!(invalid_history.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
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

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn health_routes_report_liveness_and_database_readiness(pool: PgPool) {
    let app = router(&pool);

    let liveness = app
        .clone()
        .oneshot(get_request("/api/health", None))
        .await
        .expect("liveness request should complete");
    assert_eq!(liveness.status(), StatusCode::OK);
    assert_eq!(liveness.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(
        to_bytes(liveness.into_body(), usize::MAX)
            .await
            .expect("liveness body should be readable"),
        r#"{"status":"ok"}"#
    );

    let readiness = app
        .oneshot(get_request("/api/ready", None))
        .await
        .expect("readiness request should complete");
    assert_eq!(readiness.status(), StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(readiness.into_body(), usize::MAX)
                .await
                .expect("readiness body should be readable"),
        )
        .expect("readiness body should be JSON"),
        serde_json::json!({
            "status": "ready",
            "database": "ready",
            "timezone": "UTC"
        })
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn readiness_returns_service_unavailable_when_database_is_closed(pool: PgPool) {
    let app = router(&pool);
    pool.close().await;

    let response = app
        .oneshot(get_request("/api/ready", None))
        .await
        .expect("readiness request should complete even when the database is closed");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("readiness body should be readable"),
        )
        .expect("readiness body should be JSON"),
        serde_json::json!({
            "status": "not_ready",
            "database": "unavailable",
            "timezone": "UTC"
        })
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn feed_route_does_not_enumerate_invalid_unknown_or_revoked_tokens(pool: PgPool) {
    let source_id = insert_source(&pool).await;
    let token_service = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()));
    let token = token_service
        .issue(source_id)
        .await
        .expect("feed token should be issued");
    let unknown = werrss::domain::feed_token::FeedToken::generate();
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

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
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

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
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
    feed_router(token_service, feed_service, pool.clone(), UTC)
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

fn json_request(path: &str, value: serde_json::Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(value.to_string()))
        .expect("JSON request should be valid")
}

fn admin_request(
    path: &str,
    cookie: &str,
    csrf: Option<&str>,
    body: Option<serde_json::Value>,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(if body.is_some() { "POST" } else { "GET" })
        .uri(path)
        .header(header::COOKIE, cookie);
    if let Some(csrf) = csrf {
        builder = builder.header("x-csrf-token", csrf);
    }
    if let Some(body) = body {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
        builder
            .body(Body::from(body.to_string()))
            .expect("admin JSON request should be valid")
    } else {
        builder
            .body(Body::empty())
            .expect("admin request should be valid")
    }
}

fn admin_app(pool: &PgPool) -> axum::Router {
    let auth = AdminAuthenticator::new(
        "admin".to_owned(),
        SecretString::new("correct horse".to_owned().into_boxed_str()),
        SecretString::new("independent signing key".to_owned().into_boxed_str()),
    )
    .expect("test admin auth should be valid");
    admin_router(
        auth,
        SourceService::new(
            PostgresSourceRepository::new(pool.clone()),
            UnitOfWorkFactory::new(pool.clone()),
        ),
        FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone())),
        PostgresSyncRunRepository::new(pool.clone()),
    )
    .layer(Extension(ConnectInfo(std::net::SocketAddr::from((
        [127, 0, 0, 1],
        43_210,
    )))))
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
