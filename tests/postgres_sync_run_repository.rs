use chrono::{TimeZone, Utc};
use sqlx::PgPool;
use uuid::Uuid;
use wechrss::{
    domain::{
        source::{NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
        sync::{
            NewSyncRun, SyncFailure, SyncFailureClass, SyncOutcome, SyncRunCompletion, SyncStats,
        },
    },
    persistence::{
        repositories::{
            source_repository::SourceTransactionRepository,
            sync_run_repository::{
                PostgresSyncRunRepository, SyncRunRepository, SyncRunRepositoryError,
                SyncRunTransactionRepository,
            },
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn sync_run_start_finish_and_history_round_trip(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let run_id = Uuid::new_v4();

    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let started = unit_of_work
        .sync_runs()
        .start(NewSyncRun {
            id: run_id,
            source_id,
            job_id: None,
            started_at: at(10),
        })
        .await
        .expect("sync run should start");
    assert_eq!(started.outcome(), SyncOutcome::Running);
    assert_eq!(started.finished_at(), None);

    let finished = unit_of_work
        .sync_runs()
        .finish(
            run_id,
            SyncRunCompletion {
                outcome: SyncOutcome::Succeeded,
                finished_at: at(20),
                stats: SyncStats {
                    articles_seen: 3,
                    articles_created: 1,
                    articles_updated: 1,
                    articles_failed: 1,
                    archived_articles: 2,
                    archived_assets: 0,
                },
                failure: None,
                feed_revision: Some(wechrss::domain::source::FeedRevision::from_u64(2)),
            },
        )
        .await
        .expect("sync run should finish");
    assert_eq!(finished.outcome(), SyncOutcome::Succeeded);
    assert_eq!(finished.stats().articles_seen, 3);
    unit_of_work.commit().await.expect("sync run should commit");

    let repository = PostgresSyncRunRepository::new(pool);
    let persisted = repository
        .find(run_id)
        .await
        .expect("sync run lookup should succeed")
        .expect("sync run should exist");
    assert_eq!(persisted.outcome(), SyncOutcome::Succeeded);
    assert_eq!(persisted.feed_revision().unwrap().as_u64(), 2);
    assert_eq!(persisted.stats().articles_created, 1);
    assert_eq!(
        repository
            .list_for_source(source_id, 10)
            .await
            .expect("sync run history should succeed")
            .len(),
        1
    );
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn sync_run_start_is_idempotent_after_a_committed_retry(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let run_id = Uuid::new_v4();
    let spec = NewSyncRun {
        id: run_id,
        source_id,
        job_id: None,
        started_at: at(10),
    };

    let mut first_attempt = factory.begin().await.expect("unit of work should begin");
    let first = first_attempt
        .sync_runs()
        .start(spec)
        .await
        .expect("first sync run start should succeed");
    first_attempt
        .commit()
        .await
        .expect("first sync run start should commit");

    let mut retry = factory
        .begin()
        .await
        .expect("retry unit of work should begin");
    let second = retry
        .sync_runs()
        .start(spec)
        .await
        .expect("retrying the same sync run should return the existing row");
    assert_eq!(second, first);
    retry
        .commit()
        .await
        .expect("idempotent retry should commit");

    assert_eq!(
        PostgresSyncRunRepository::new(pool)
            .list_for_source(source_id, 10)
            .await
            .expect("sync run history should succeed")
            .len(),
        1
    );
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn sync_run_start_rejects_reusing_id_for_another_source(pool: PgPool) {
    let first_source_id = SourceId::from_uuid(Uuid::new_v4());
    let second_source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, first_source_id).await;
    create_source(&factory, second_source_id).await;
    let run_id = Uuid::new_v4();

    let mut first_attempt = factory.begin().await.expect("unit of work should begin");
    first_attempt
        .sync_runs()
        .start(NewSyncRun {
            id: run_id,
            source_id: first_source_id,
            job_id: None,
            started_at: at(10),
        })
        .await
        .expect("first sync run start should succeed");
    first_attempt
        .commit()
        .await
        .expect("first sync run start should commit");

    let mut conflicting_attempt = factory
        .begin()
        .await
        .expect("conflicting unit of work should begin");
    let error = conflicting_attempt
        .sync_runs()
        .start(NewSyncRun {
            id: run_id,
            source_id: second_source_id,
            job_id: None,
            started_at: at(10),
        })
        .await
        .expect_err("reusing a run id for another source should fail");
    assert!(matches!(
        error,
        SyncRunRepositoryError::StartConflict { run_id: actual } if actual == run_id
    ));
    conflicting_attempt
        .rollback()
        .await
        .expect("conflicting transaction should roll back");
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn sync_run_failure_is_classified_and_cannot_finish_twice(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let run_id = Uuid::new_v4();
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    unit_of_work
        .sync_runs()
        .start(NewSyncRun {
            id: run_id,
            source_id,
            job_id: None,
            started_at: at(10),
        })
        .await
        .expect("sync run should start");

    let completion = SyncRunCompletion {
        outcome: SyncOutcome::AuthenticationRequired,
        finished_at: at(20),
        stats: SyncStats::default(),
        failure: Some(
            SyncFailure::new(SyncFailureClass::AuthenticationExpired, "login expired")
                .expect("failure should be valid"),
        ),
        feed_revision: None,
    };
    let finished = unit_of_work
        .sync_runs()
        .finish(run_id, completion.clone())
        .await
        .expect("sync run should finish");
    assert_eq!(finished.outcome(), SyncOutcome::AuthenticationRequired);
    assert_eq!(finished.failure().unwrap().message(), "login expired");

    let second_finish = unit_of_work.sync_runs().finish(run_id, completion).await;
    assert!(matches!(
        second_finish,
        Err(SyncRunRepositoryError::Domain(
            wechrss::domain::sync::SyncError::AlreadyFinished
        ))
    ));
    unit_of_work
        .commit()
        .await
        .expect("first completion should remain committed");
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn sync_run_rejects_missing_source_and_invalid_history_limit(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .sync_runs()
        .start(NewSyncRun {
            id: Uuid::new_v4(),
            source_id,
            job_id: None,
            started_at: at(10),
        })
        .await;
    assert!(matches!(
        result,
        Err(SyncRunRepositoryError::SourceNotFound { source_id: actual })
            if actual == source_id
    ));
    unit_of_work
        .rollback()
        .await
        .expect("failed insert should roll back");

    let repository = PostgresSyncRunRepository::new(pool);
    assert!(matches!(
        repository.list_for_source(source_id, 0).await,
        Err(SyncRunRepositoryError::InvalidLimit)
    ));
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn sync_run_completion_rolls_back_with_the_unit_of_work(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let run_id = Uuid::new_v4();

    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    unit_of_work
        .sync_runs()
        .start(NewSyncRun {
            id: run_id,
            source_id,
            job_id: None,
            started_at: at(10),
        })
        .await
        .expect("sync run should start");
    unit_of_work
        .sync_runs()
        .finish(
            run_id,
            SyncRunCompletion {
                outcome: SyncOutcome::Succeeded,
                finished_at: at(20),
                stats: SyncStats::default(),
                failure: None,
                feed_revision: None,
            },
        )
        .await
        .expect("sync run should finish");
    unit_of_work
        .rollback()
        .await
        .expect("sync run transaction should roll back");

    assert!(PostgresSyncRunRepository::new(pool)
        .find(run_id)
        .await
        .expect("sync run lookup should succeed")
        .is_none());
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn sync_run_schema_rejects_unknown_failure_classes(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;

    let error = sqlx::query(
        "INSERT INTO sync_runs (id, source_id, outcome, articles_seen, articles_created, articles_updated, articles_failed, archived_articles, archived_assets, failure_class, failure_message, started_at, finished_at) VALUES ($1, $2, 'retryable_failure', 0, 0, 0, 0, 0, 0, 'unknown', 'test failure', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(Uuid::new_v4())
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .expect_err("unknown failure class should violate the schema");

    assert!(matches!(
        error,
        sqlx::Error::Database(database_error)
            if database_error.constraint() == Some("sync_runs_failure_class_check")
    ));
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn sync_run_schema_rejects_incompatible_failure_classes(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;

    let error = sqlx::query(
        "INSERT INTO sync_runs (id, source_id, outcome, articles_seen, articles_created, articles_updated, articles_failed, archived_articles, archived_assets, failure_class, failure_message, started_at, finished_at) VALUES ($1, $2, 'authentication_required', 0, 0, 0, 0, 0, 0, 'permanent', 'test failure', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
    )
    .bind(Uuid::new_v4())
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .expect_err("incompatible failure class should violate the schema");

    assert!(matches!(
        error,
        sqlx::Error::Database(database_error)
            if database_error.constraint() == Some("sync_runs_failure_class_outcome_check")
    ));
}

async fn create_source(factory: &UnitOfWorkFactory, source_id: SourceId) {
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    unit_of_work
        .source()
        .insert(NewSource {
            id: source_id,
            book_id: format!("book-{source_id}"),
            display_name: "Test source".to_owned(),
            article_url: "https://mp.weixin.qq.com/s/source"
                .parse::<VerifiedWechatArticleUrl>()
                .expect("source URL should be valid"),
            enabled: true,
            sync_interval: chrono::Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: at(0),
            priority: 0,
            max_attempts: 3,
        })
        .await
        .expect("source should be inserted");
    unit_of_work.commit().await.expect("source should commit");
}

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("timestamp should be valid")
}
