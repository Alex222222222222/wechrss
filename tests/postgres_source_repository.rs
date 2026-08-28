use chrono::{Duration, TimeZone, Utc};
use sqlx::PgPool;
use uuid::Uuid;
use wechrss::{
    domain::source::{FeedRevision, NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
    persistence::{
        repositories::source_repository::{
            PostgresSourceRepository, SourceRepository, SourceRepositoryError,
            SourceTransactionRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn postgres_source_can_be_created_read_and_lookup_by_book_id(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let source = source_spec(source_id, "book-create");
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let created = unit_of_work
        .source()
        .insert(source)
        .await
        .expect("source should be inserted");
    unit_of_work
        .commit()
        .await
        .expect("source insertion should commit");

    assert_eq!(created.id(), source_id);
    assert_eq!(created.book_id(), "book-create");
    assert_eq!(created.feed_revision(), FeedRevision::zero());
    assert_eq!(created.scheduling_gate(), SchedulingGate::Ready);
    assert_eq!(created.priority(), 10);
    assert_eq!(created.max_attempts(), 3);

    let repository = PostgresSourceRepository::new(pool.clone());
    let by_id = repository
        .find(source_id)
        .await
        .expect("source lookup should succeed")
        .expect("created source should be present");
    assert_eq!(by_id, created);
    let by_book_id = repository
        .find_by_book_id(" book-create ")
        .await
        .expect("book lookup should succeed")
        .expect("book should identify the source");
    assert_eq!(by_book_id.id(), source_id);

    let mut duplicate_work = factory.begin().await.expect("unit of work should begin");
    let duplicate = duplicate_work
        .source()
        .insert(source_spec(
            SourceId::from_uuid(Uuid::new_v4()),
            "book-create",
        ))
        .await
        .expect_err("duplicate book id should be rejected");
    assert!(matches!(
        duplicate,
        SourceRepositoryError::BookIdConflict { ref book_id } if book_id == "book-create"
    ));
    duplicate_work
        .rollback()
        .await
        .expect("duplicate transaction should roll back");
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn postgres_source_mutations_share_revision_and_schedule_transaction(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_spec(source_id, "book-mutate")).await;

    let next_fetch_at = Utc
        .with_ymd_and_hms(2030, 1, 2, 3, 4, 5)
        .single()
        .expect("test timestamp should be valid");
    let cooldown_until = next_fetch_at + Duration::minutes(10);
    let reservation_until = next_fetch_at + Duration::minutes(20);
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let disabled = unit_of_work
        .source()
        .set_enabled(source_id, false)
        .await
        .expect("enabled state should update");
    assert!(!disabled.enabled());
    let gated = unit_of_work
        .source()
        .set_scheduling_gate(source_id, SchedulingGate::AuthenticationRequired)
        .await
        .expect("scheduling gate should update");
    assert_eq!(
        gated.scheduling_gate(),
        SchedulingGate::AuthenticationRequired
    );
    let scheduled = unit_of_work
        .source()
        .update_schedule(
            source_id,
            next_fetch_at,
            Some(cooldown_until),
            Some(reservation_until),
        )
        .await
        .expect("schedule should update");
    assert_eq!(scheduled.next_fetch_at(), next_fetch_at);
    let revision = unit_of_work
        .source()
        .bump_feed_revision(source_id, FeedRevision::zero())
        .await
        .expect("revision should advance");
    assert_eq!(revision, FeedRevision::from_u64(1));
    unit_of_work
        .commit()
        .await
        .expect("source mutations should commit together");

    let persisted = PostgresSourceRepository::new(pool)
        .find(source_id)
        .await
        .expect("source lookup should succeed")
        .expect("source should remain present");
    assert!(!persisted.enabled());
    assert_eq!(
        persisted.scheduling_gate(),
        SchedulingGate::AuthenticationRequired
    );
    assert_eq!(persisted.next_fetch_at(), next_fetch_at);
    assert_eq!(persisted.failure_cooldown_until(), Some(cooldown_until));
    assert_eq!(persisted.schedule_reserved_until(), Some(reservation_until));
    assert_eq!(persisted.feed_revision(), FeedRevision::from_u64(1));
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn postgres_source_revision_compare_and_swap_and_rollback_are_enforced(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_spec(source_id, "book-cas")).await;

    let mut first_update = factory.begin().await.expect("unit of work should begin");
    assert_eq!(
        first_update
            .source()
            .bump_feed_revision(source_id, FeedRevision::zero())
            .await
            .expect("first revision update should succeed"),
        FeedRevision::from_u64(1)
    );
    first_update
        .commit()
        .await
        .expect("first revision update should commit");

    let mut stale_update = factory.begin().await.expect("unit of work should begin");
    let conflict = stale_update
        .source()
        .bump_feed_revision(source_id, FeedRevision::zero())
        .await
        .expect_err("stale revision should be rejected");
    assert!(matches!(
        conflict,
        SourceRepositoryError::RevisionConflict {
            source_id: conflicted,
            expected,
            actual,
        } if conflicted == source_id
            && expected == FeedRevision::zero()
            && actual == FeedRevision::from_u64(1)
    ));
    stale_update
        .rollback()
        .await
        .expect("stale transaction should roll back");

    let rollback_id = SourceId::from_uuid(Uuid::new_v4());
    let mut rollback_work = factory.begin().await.expect("unit of work should begin");
    rollback_work
        .source()
        .insert(source_spec(rollback_id, "book-rollback"))
        .await
        .expect("source should insert before rollback");
    rollback_work
        .rollback()
        .await
        .expect("explicit rollback should succeed");
    assert!(PostgresSourceRepository::new(pool)
        .find(rollback_id)
        .await
        .expect("source lookup should succeed")
        .is_none());
}

async fn create_source(factory: &UnitOfWorkFactory, source: NewSource) {
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    unit_of_work
        .source()
        .insert(source)
        .await
        .expect("source should insert");
    unit_of_work.commit().await.expect("source should commit");
}

fn source_spec(source_id: SourceId, book_id: &str) -> NewSource {
    NewSource {
        id: source_id,
        book_id: book_id.to_owned(),
        display_name: "Test source".to_owned(),
        article_url: "https://mp.weixin.qq.com/s/test"
            .parse::<VerifiedWechatArticleUrl>()
            .expect("test URL should be valid"),
        enabled: true,
        sync_interval: Duration::hours(1),
        rss_item_limit: 20,
        account_id: None,
        scheduling_gate: SchedulingGate::Ready,
        next_fetch_at: Utc::now(),
        priority: 10,
        max_attempts: 3,
    }
}
