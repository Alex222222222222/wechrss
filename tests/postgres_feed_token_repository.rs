use chrono::{Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;
use werrss::{
    application::feed_token_service::{FeedTokenService, FeedTokenServiceError},
    domain::source::{NewSource, SourceId, VerifiedWechatArticleUrl},
    persistence::{
        repositories::feed_token_repository::{
            FeedTokenRepositoryError, PostgresFeedTokenRepository,
        },
        repositories::source_repository::SourceTransactionRepository,
        unit_of_work::UnitOfWorkFactory,
    },
};

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_feed_tokens_issue_rotate_revoke_and_cascade(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    insert_source(&pool, source_id).await;
    let service = FeedTokenService::new(PostgresFeedTokenRepository::new(pool.clone()));

    let first = service
        .issue(source_id)
        .await
        .expect("initial token issue should succeed");
    assert_eq!(
        service.resolve(first.as_str()).await.unwrap(),
        Some(source_id)
    );

    let stored_length: i32 =
        sqlx::query_scalar("SELECT octet_length(token_hash) FROM feed_tokens WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("token digest should be persisted");
    assert_eq!(stored_length, 32);

    let second = service
        .issue(source_id)
        .await
        .expect("rotation should succeed");
    assert_ne!(first, second);
    assert_eq!(service.resolve(first.as_str()).await.unwrap(), None);
    assert_eq!(
        service.resolve(second.as_str()).await.unwrap(),
        Some(source_id)
    );

    assert!(service.revoke(source_id).await.unwrap());
    assert_eq!(service.resolve(second.as_str()).await.unwrap(), None);
    assert!(!service.revoke(source_id).await.unwrap());

    let third = service
        .issue(source_id)
        .await
        .expect("a revoked source should be issuable again");
    assert_eq!(
        service.resolve(third.as_str()).await.unwrap(),
        Some(source_id)
    );

    sqlx::query("DELETE FROM sources WHERE id = $1")
        .bind(source_id.as_uuid())
        .execute(&pool)
        .await
        .expect("source deletion should succeed");
    assert_eq!(service.resolve(third.as_str()).await.unwrap(), None);
    let remaining: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM feed_tokens WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("feed-token row should be queryable");
    assert_eq!(remaining, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_feed_tokens_reject_missing_and_nil_sources_without_lookup_leaks(pool: PgPool) {
    let repository = PostgresFeedTokenRepository::new(pool);
    let service = FeedTokenService::new(repository);
    let missing = SourceId::from_uuid(Uuid::new_v4());

    let missing_result = service.issue(missing).await;
    assert!(matches!(
        missing_result,
        Err(FeedTokenServiceError::Repository(
            FeedTokenRepositoryError::SourceNotFound { source_id }
        )) if source_id == missing
    ));
    assert!(matches!(
        service.issue(SourceId::from_uuid(Uuid::nil())).await,
        Err(FeedTokenServiceError::Repository(
            FeedTokenRepositoryError::InvalidSourceId
        ))
    ));
    assert!(matches!(
        service.resolve("not-a-canonical-token").await,
        Err(FeedTokenServiceError::InvalidToken)
    ));
}

async fn insert_source(pool: &PgPool, source_id: SourceId) {
    let source = NewSource {
        id: source_id,
        book_id: format!("book-{source_id}"),
        display_name: "Token integration source".to_owned(),
        article_url: Some(
            "https://mp.weixin.qq.com/s/token-test"
                .parse::<VerifiedWechatArticleUrl>()
                .expect("test URL should be valid"),
        ),
        enabled: true,
        sync_interval: Duration::hours(1),
        rss_item_limit: 20,
        account_id: None,
        scheduling_gate: werrss::domain::source::SchedulingGate::Ready,
        next_fetch_at: Utc::now(),
        priority: 0,
        max_attempts: 3,
    };
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut work = factory.begin().await.expect("unit of work should begin");
    work.source()
        .insert(source)
        .await
        .expect("source should be inserted");
    work.commit().await.expect("source should commit");
}
