use chrono::Duration;
use sqlx::PgPool;
use uuid::Uuid;
use wechrss::{
    domain::source::SourceId,
    persistence::repositories::feed_cache_repository::{
        FeedBuildLeaseError, FeedBuildLeaseRepository, PostgresFeedBuildLeaseRepository,
    },
};

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn postgres_feed_build_lease_serializes_builders_and_supports_fenced_takeover(pool: PgPool) {
    let repository = PostgresFeedBuildLeaseRepository::new(pool.clone());
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    sqlx::query("INSERT INTO sources (id) VALUES ($1)")
        .bind(source_id.as_uuid())
        .execute(&pool)
        .await
        .expect("test source should be insertable");

    let (builder_a, builder_b) = tokio::join!(
        repository.acquire_build(source_id, "builder-a", Duration::seconds(30)),
        repository.acquire_build(source_id, "builder-b", Duration::seconds(30)),
    );
    let first = match (
        builder_a.expect("builder A acquisition should succeed"),
        builder_b.expect("builder B acquisition should succeed"),
    ) {
        (Some(lease), None) | (None, Some(lease)) => lease,
        result => panic!("exactly one builder should acquire the lease: {result:?}"),
    };
    assert_eq!(first.source_id(), source_id);
    assert!(first.lease_until() > first.heartbeat_at());

    assert!(matches!(
        repository
            .heartbeat_build(
                source_id,
                "another-builder",
                first.token(),
                Duration::seconds(30),
            )
            .await,
        Err(FeedBuildLeaseError::LeaseLost { source_id: lost }) if lost == source_id
    ));
    let renewed = repository
        .heartbeat_build(
            source_id,
            first.owner(),
            first.token(),
            Duration::seconds(45),
        )
        .await
        .expect("current builder should heartbeat");
    assert_eq!(renewed.token(), first.token());
    assert!(renewed.lease_until() > renewed.heartbeat_at());

    sqlx::query(
        "UPDATE feed_build_leases SET lease_until = clock_timestamp() - interval '1 second' WHERE source_id = $1",
    )
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .expect("test should be able to expire the build lease");

    assert!(matches!(
        repository
            .release_build(source_id, first.owner(), renewed.token())
            .await,
        Err(FeedBuildLeaseError::LeaseLost { source_id: lost }) if lost == source_id
    ));
    let takeover = repository
        .acquire_build(source_id, "takeover-builder", Duration::seconds(30))
        .await
        .expect("expired acquisition should succeed")
        .expect("expired build lease should be taken over");
    assert_eq!(takeover.owner(), "takeover-builder");
    assert_ne!(takeover.token(), first.token());
    repository
        .release_build(source_id, "takeover-builder", takeover.token())
        .await
        .expect("current builder should release");
}
