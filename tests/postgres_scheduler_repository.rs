use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use sqlx::PgPool;
use uuid::Uuid;
use werrss::{
    domain::credentials::WeReadAccountId,
    domain::pacing::QuietHours,
    domain::source::{SchedulingGate, SourceId},
    persistence::repositories::scheduler_repository::{
        PostgresSchedulerRepository, SchedulerPass, SchedulerRepository, SchedulerRepositoryError,
    },
};

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_scheduler_claims_disjoint_batches_across_replicas(pool: PgPool) {
    let source_ids = [
        insert_source(&pool, true, SchedulingGate::Ready, 4).await,
        insert_source(&pool, true, SchedulingGate::Ready, 3).await,
        insert_source(&pool, true, SchedulingGate::Ready, 2).await,
        insert_source(&pool, true, SchedulingGate::Ready, 1).await,
    ];
    let repository_a = PostgresSchedulerRepository::new(pool.clone());
    let repository_b = PostgresSchedulerRepository::new(pool.clone());

    let (batch_a, batch_b) = tokio::join!(
        repository_a.enqueue_due_sources(2, Duration::minutes(5), None),
        repository_b.enqueue_due_sources(2, Duration::minutes(5), None),
    );
    let SchedulerPass::Enqueued(batch_a) = batch_a.expect("first scheduler replica should succeed")
    else {
        panic!("scheduler should not be quiet in this test");
    };
    let SchedulerPass::Enqueued(batch_b) =
        batch_b.expect("second scheduler replica should succeed")
    else {
        panic!("scheduler should not be quiet in this test");
    };

    assert_eq!(batch_a.len(), 2);
    assert_eq!(batch_b.len(), 2);
    let claimed: HashSet<_> = batch_a
        .iter()
        .chain(batch_b.iter())
        .map(|source| source.source_id().as_uuid())
        .collect();
    let expected: HashSet<_> = source_ids.iter().map(|source| source.as_uuid()).collect();
    assert_eq!(claimed, expected);
    assert!(batch_a
        .iter()
        .all(|source| source.reserved_until() > chrono::Utc::now()));
    assert!(batch_a
        .iter()
        .chain(batch_b.iter())
        .all(|source| source.job_id() != Uuid::nil()));

    let active_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE status IN ('queued', 'running', 'retry_wait', 'deferred')",
    )
    .fetch_one(&pool)
    .await
    .expect("active job count should be queryable");
    assert_eq!(active_jobs, 4);
    let distinct_dedupe_keys: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT dedupe_key) FROM jobs WHERE status IN ('queued', 'running', 'retry_wait', 'deferred')",
    )
    .fetch_one(&pool)
    .await
    .expect("dedupe key count should be queryable");
    assert_eq!(distinct_dedupe_keys, 4);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_scheduler_has_a_due_time_leading_partial_index(pool: PgPool) {
    let index_definition: String = sqlx::query_scalar(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'sources_due_idx'",
    )
    .fetch_one(&pool)
    .await
    .expect("source due index should exist");

    assert!(index_definition.contains("(next_fetch_at, priority DESC, id)"));
    assert!(index_definition.contains("WHERE (enabled AND (scheduling_gate = 'ready'::text))"));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_scheduler_filters_gates_and_active_jobs(pool: PgPool) {
    let eligible = insert_source(&pool, true, SchedulingGate::Ready, 10).await;
    let disabled = insert_source(&pool, false, SchedulingGate::Ready, 9).await;
    let auth_blocked = insert_source(&pool, true, SchedulingGate::AuthenticationRequired, 8).await;
    let risk_blocked = insert_source(&pool, true, SchedulingGate::RiskControlled, 7).await;
    let future = insert_source(&pool, true, SchedulingGate::Ready, 6).await;
    let cooldown = insert_source(&pool, true, SchedulingGate::Ready, 5).await;
    let reserved = insert_source(&pool, true, SchedulingGate::Ready, 4).await;
    let active = insert_source(&pool, true, SchedulingGate::Ready, 3).await;
    set_source_time_state(&pool, future, "next_fetch_at", "future").await;
    set_source_time_state(&pool, cooldown, "failure_cooldown_until", "future").await;
    set_source_time_state(&pool, reserved, "schedule_reserved_until", "future").await;
    enqueue_active_job(&pool, active).await;

    let enqueued = PostgresSchedulerRepository::new(pool.clone())
        .enqueue_due_sources(20, Duration::minutes(5), None)
        .await
        .expect("scheduler should succeed");
    let SchedulerPass::Enqueued(enqueued) = enqueued else {
        panic!("scheduler should not be quiet in this test");
    };
    assert_eq!(enqueued.len(), 1);
    assert_eq!(enqueued[0].source_id(), eligible);

    for source_id in [
        disabled,
        auth_blocked,
        risk_blocked,
        future,
        cooldown,
        reserved,
        active,
    ] {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .expect("source job count should be queryable");
        assert_eq!(count, if source_id == active { 1 } else { 0 });
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_scheduler_reservation_and_active_dedupe_survive_repeated_passes(pool: PgPool) {
    let source_id = insert_source(&pool, true, SchedulingGate::Ready, 1).await;
    let repository = PostgresSchedulerRepository::new(pool.clone());

    let first = repository
        .enqueue_due_sources(1, Duration::minutes(5), None)
        .await
        .expect("first scheduling pass should succeed");
    let SchedulerPass::Enqueued(first) = first else {
        panic!("scheduler should not be quiet in this test");
    };
    assert_eq!(first.len(), 1);
    assert!(repository
        .enqueue_due_sources(1, Duration::minutes(5), None)
        .await
        .expect("second scheduling pass should succeed")
        .eq(&SchedulerPass::Enqueued(Vec::new())));

    sqlx::query(
        "UPDATE sources SET schedule_reserved_until = clock_timestamp() - interval '1 second' WHERE id = $1",
    )
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .expect("test should be able to expire the reservation");
    assert!(repository
        .enqueue_due_sources(1, Duration::minutes(5), None)
        .await
        .expect("active job should still deduplicate")
        .eq(&SchedulerPass::Enqueued(Vec::new())));

    sqlx::query(
        "UPDATE jobs SET status = 'succeeded', claim_count = 1, started_at = clock_timestamp() - interval '1 second', finished_at = clock_timestamp(), updated_at = clock_timestamp() WHERE source_id = $1",
    )
    .bind(source_id.as_uuid())
    .execute(&pool)
    .await
    .expect("test should be able to make the job terminal");
    let rescheduled = repository
        .enqueue_due_sources(1, Duration::minutes(5), None)
        .await
        .expect("terminal jobs should allow a future scheduling pass");
    let SchedulerPass::Enqueued(rescheduled) = rescheduled else {
        panic!("scheduler should not be quiet in this test");
    };
    assert_eq!(rescheduled.len(), 1);
    assert_ne!(rescheduled[0].job_id(), first[0].job_id());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_scheduler_uses_database_clock_for_quiet_hours(pool: PgPool) {
    let database_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .expect("database clock should be queryable");
    let quiet_hours = QuietHours::new(
        chrono_tz::UTC,
        (database_now - chrono::Duration::hours(1)).time(),
        (database_now + chrono::Duration::hours(1)).time(),
    )
    .expect("the test quiet window should have distinct endpoints");
    insert_source(&pool, true, SchedulingGate::Ready, 1).await;

    let result = PostgresSchedulerRepository::new(pool.clone())
        .enqueue_due_sources(1, Duration::minutes(5), Some(quiet_hours))
        .await
        .expect("quiet-hours scheduling should succeed");
    assert_eq!(result, SchedulerPass::SkippedQuietHours);

    let job_count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .expect("job count should be queryable");
    assert_eq!(job_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_scheduler_rolls_back_prior_inserts_when_a_source_is_invalid(pool: PgPool) {
    let valid = insert_source(&pool, true, SchedulingGate::Ready, 2).await;
    let invalid = insert_source(&pool, true, SchedulingGate::Ready, 1).await;
    sqlx::query("UPDATE sources SET max_attempts = 4294967296 WHERE id = $1")
        .bind(invalid.as_uuid())
        .execute(&pool)
        .await
        .expect("test should be able to create an invalid domain value");

    let error = PostgresSchedulerRepository::new(pool.clone())
        .enqueue_due_sources(2, Duration::minutes(5), None)
        .await
        .expect_err("invalid persisted source value should fail the transaction");
    assert!(matches!(
        error,
        SchedulerRepositoryError::InvalidMaxAttempts { source_id, value }
            if source_id == invalid && value == 4_294_967_296
    ));
    let job_count: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs")
        .fetch_one(&pool)
        .await
        .expect("job count should be queryable");
    assert_eq!(job_count, 0);
    let reservation_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sources WHERE id IN ($1, $2) AND schedule_reserved_until IS NOT NULL",
    )
    .bind(valid.as_uuid())
    .bind(invalid.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("reservation count should be queryable");
    assert_eq!(reservation_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn postgres_scheduler_enqueues_due_credential_refreshes_and_deduplicates(pool: PgPool) {
    let due = WeReadAccountId::from_uuid(Uuid::new_v4());
    let disabled = WeReadAccountId::from_uuid(Uuid::new_v4());
    let future = WeReadAccountId::from_uuid(Uuid::new_v4());
    insert_account(&pool, due, "clock_timestamp() + interval '1 minute'", false).await;
    insert_account(
        &pool,
        disabled,
        "clock_timestamp() - interval '1 minute'",
        true,
    )
    .await;
    insert_account(
        &pool,
        future,
        "clock_timestamp() + interval '1 hour'",
        false,
    )
    .await;

    let repository = PostgresSchedulerRepository::new(pool.clone());
    let scheduled = repository
        .enqueue_due_credential_refreshes(10, Duration::minutes(5), 4)
        .await
        .expect("credential refresh scheduling should succeed");
    assert_eq!(scheduled.len(), 1);
    assert_eq!(scheduled[0].account_id(), due);
    assert_ne!(scheduled[0].job_id(), Uuid::nil());

    let job: (String, Option<Uuid>, i64, serde_json::Value) = sqlx::query_as(
        "SELECT job_type, source_id, max_attempts, payload_json FROM jobs WHERE id = $1",
    )
    .bind(scheduled[0].job_id())
    .fetch_one(&pool)
    .await
    .expect("refresh job should be queryable");
    assert_eq!(job.0, "credential_refresh");
    assert_eq!(job.1, None);
    assert_eq!(job.2, 4);
    assert_eq!(job.3["account_id"], due.as_uuid().to_string());

    assert!(repository
        .enqueue_due_credential_refreshes(10, Duration::minutes(5), 4)
        .await
        .expect("repeated credential scheduling should succeed")
        .is_empty());
    let active_job_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE dedupe_key = $1 AND status IN ('queued', 'running', 'retry_wait', 'deferred')",
    )
    .bind(format!("credential_refresh:{due}"))
    .fetch_one(&pool)
    .await
    .expect("active refresh job count should be queryable");
    assert_eq!(active_job_count, 1);
}

async fn insert_source(
    pool: &PgPool,
    enabled: bool,
    scheduling_gate: SchedulingGate,
    priority: i32,
) -> SourceId {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    sqlx::query(
        "INSERT INTO sources (id, book_id, display_name, article_url, enabled, scheduling_gate, next_fetch_at, priority) VALUES ($1, $2, $3, $4, $5, $6, clock_timestamp() - interval '1 second', $7)",
    )
    .bind(source_id.as_uuid())
    .bind(format!("book-{source_id}"))
    .bind("Test source")
    .bind("https://mp.weixin.qq.com/s/test")
    .bind(enabled)
    .bind(scheduling_gate.as_str())
    .bind(priority)
    .execute(pool)
    .await
    .expect("test source should be insertable");
    source_id
}

async fn insert_account(
    pool: &PgPool,
    account_id: WeReadAccountId,
    expires_at: &str,
    disabled: bool,
) {
    let query = format!(
        "INSERT INTO weread_accounts (account_id, display_name, credentials_ciphertext, access_expires_at, disabled) VALUES ($1, $2, $3, {expires_at}, $4)"
    );
    sqlx::query(&query)
        .bind(account_id.as_uuid())
        .bind(format!("account-{account_id}"))
        .bind(b"encrypted-placeholder".as_slice())
        .bind(disabled)
        .execute(pool)
        .await
        .expect("test account should be insertable");
}

async fn set_source_time_state(pool: &PgPool, source_id: SourceId, column: &str, state: &str) {
    let expression = match state {
        "future" => "clock_timestamp() + interval '1 hour'",
        _ => unreachable!("test only supports future state"),
    };
    let query = format!("UPDATE sources SET {column} = {expression} WHERE id = $1");
    sqlx::query(&query)
        .bind(source_id.as_uuid())
        .execute(pool)
        .await
        .expect("test source time state should be mutable");
}

async fn enqueue_active_job(pool: &PgPool, source_id: SourceId) {
    sqlx::query(
        r#"
        INSERT INTO jobs (
            id, job_type, source_id, status, priority, run_after,
            claim_count, failure_count, max_attempts, payload_json,
            dedupe_key, created_at, updated_at
        )
        VALUES (
            $1, 'source_sync', $2, 'queued', 0, clock_timestamp(),
            0, 0, 3, '{}'::jsonb, $3, clock_timestamp(), clock_timestamp()
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(source_id.as_uuid())
    .bind(format!("source_sync:{source_id}"))
    .execute(pool)
    .await
    .expect("active source job should be insertable");
}
