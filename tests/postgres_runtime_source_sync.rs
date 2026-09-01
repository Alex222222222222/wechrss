//! PostgreSQL integration coverage for the scheduler/runtime source-sync seam.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use chrono::Duration;
use sqlx::PgPool;
use uuid::Uuid;
use wechrss::{
    application::{
        runtime::RuntimeComponent,
        runtime_supervisor::{RuntimeJobHandler, RuntimeSupervisor},
        worker::{JobExecution, JobHandler},
    },
    config::AppConfig,
    domain::source::SourceId,
    persistence::repositories::{
        job_repository::{JobLease, JobQueue, PostgresJobRepository},
        scheduler_repository::{PostgresSchedulerRepository, SchedulerPass, SchedulerRepository},
    },
};

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
        ("APP_ROLES".to_owned(), "all".to_owned()),
        (
            "APP_INSTANCE_ID".to_owned(),
            "runtime-source-sync-integration".to_owned(),
        ),
        ("WORKER_CONCURRENCY".to_owned(), "16".to_owned()),
        (
            "RSS_FEED_URL".to_owned(),
            "https://feeds.example.test/wechrss.xml".to_owned(),
        ),
        (
            "BROWSER_AUTHENTICATED_PROFILE".to_owned(),
            "/profiles/weread".to_owned(),
        ),
        (
            "WEREAD_ACCOUNT_ID".to_owned(),
            "00000000-0000-0000-0000-000000000001".to_owned(),
        ),
    ])
    .expect("test configuration should be valid")
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn scheduler_output_is_claimable_by_the_configured_runtime_dispatch(pool: PgPool) {
    let source_id = insert_due_source(&pool).await;
    let supervisor = RuntimeSupervisor::new(config(), pool.clone())
        .expect("configured scheduler and worker should compose");
    let RuntimeComponent::Worker(worker_plan) = supervisor
        .plan()
        .component(wechrss::config::AppRole::Worker)
        .expect("worker plan should exist")
    else {
        panic!("all roles should include a worker plan")
    };
    assert!(worker_plan.source_sync_enabled());

    let scheduled = PostgresSchedulerRepository::new(pool.clone())
        .enqueue_due_sources(1, Duration::minutes(5), None)
        .await
        .expect("scheduler should enqueue the due source");
    let SchedulerPass::Enqueued(scheduled) = scheduled else {
        panic!("scheduler should not be in quiet hours")
    };
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].source_id(), source_id);

    let lease = PostgresJobRepository::new(pool)
        .claim_next(
            "runtime-worker",
            chrono::Utc::now(),
            Duration::minutes(10),
            worker_plan.worker_config().allowed_job_types(),
        )
        .await
        .expect("runtime dispatch should claim the scheduled job")
        .expect("scheduled source-sync work should be claimable");
    assert_eq!(
        lease.job.job_type(),
        wechrss::domain::job::JobType::SourceSync
    );

    let source_calls = Arc::new(AtomicUsize::new(0));
    let handler = RuntimeJobHandler::new(
        RecordingHandler::new(Arc::new(AtomicUsize::new(0))),
        Some(RecordingHandler::new(source_calls.clone())),
    );
    assert_eq!(
        handler.execute(&lease, chrono::Utc::now()).await,
        JobExecution::Succeeded
    );
    assert_eq!(source_calls.load(Ordering::Relaxed), 1);
}

#[derive(Clone)]
struct RecordingHandler {
    calls: Arc<AtomicUsize>,
}

impl RecordingHandler {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self { calls }
    }
}

impl JobHandler for RecordingHandler {
    async fn execute(
        &self,
        _lease: &JobLease,
        _now: chrono::DateTime<chrono::Utc>,
    ) -> JobExecution {
        self.calls.fetch_add(1, Ordering::Relaxed);
        JobExecution::Succeeded
    }
}

async fn insert_due_source(pool: &PgPool) -> SourceId {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    sqlx::query(
        "INSERT INTO sources (id, book_id, display_name, article_url, enabled, scheduling_gate, next_fetch_at, priority) VALUES ($1, $2, $3, $4, true, 'ready', clock_timestamp() - interval '1 second', 1)",
    )
    .bind(source_id.as_uuid())
    .bind(format!("runtime-book-{source_id}"))
    .bind("Runtime source")
    .bind("https://mp.weixin.qq.com/s/runtime-test")
    .execute(pool)
    .await
    .expect("due source should be insertable");
    source_id
}
