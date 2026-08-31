//! PostgreSQL integration coverage for executable runtime composition.

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use sqlx::PgPool;
use tower::ServiceExt;
use wechrss::{application::runtime_supervisor::RuntimeSupervisor, config::AppConfig};

fn config() -> AppConfig {
    AppConfig::from_env_iter([
        (
            "DATABASE_URL".to_owned(),
            "postgres://user:pass@db/feed".to_owned(),
        ),
        (
            "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
            "runtime-integration-key".to_owned(),
        ),
        ("APP_ROLES".to_owned(), "api".to_owned()),
        (
            "APP_INSTANCE_ID".to_owned(),
            "runtime-integration".to_owned(),
        ),
    ])
    .expect("test configuration should be valid")
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn supervisor_wires_the_public_route_to_real_postgres_adapters(pool: PgPool) {
    let supervisor = RuntimeSupervisor::new(config(), pool).expect("supervisor should compose");
    let router = supervisor
        .api_router()
        .expect("API router construction should succeed")
        .expect("API role should provide a router");
    // A canonical token reaches the PostgreSQL lookup; malformed tokens are
    // rejected before the real adapter is called.
    let unknown_token = "A".repeat(43);

    let response = router
        .oneshot(
            Request::builder()
                .uri(format!("/feeds/{unknown_token}.xml"))
                .body(Body::empty())
                .expect("request should be valid"),
        )
        .await
        .expect("route should complete");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should be readable")
            .len(),
        0
    );
}
