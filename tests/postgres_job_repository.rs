use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;
use wechrss::{
    domain::job::{JobStatus, JobType, LeaseToken, NewJob},
    persistence::repositories::job_repository::{
        EnqueueResult, JobRepository, JobRepositoryError, JobRepositoryTransaction,
        PostgresJobRepository,
    },
    persistence::unit_of_work::UnitOfWorkFactory,
};

const OWNER_A: &str = "integration-worker-a";
const OWNER_B: &str = "integration-worker-b";

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}

fn spec(key: String, max_attempts: u32, run_after: i64) -> NewJob {
    NewJob {
        job_type: JobType::SourceSync,
        source_id: Some(Uuid::nil()),
        priority: 10,
        run_after: at(run_after),
        max_attempts,
        payload: json!({"source_id": Uuid::nil()}),
        dedupe_key: key,
        now: at(0),
    }
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn postgres_repository_enforces_claims_fencing_recovery_and_transactions(pool: PgPool) {
    let repository_a = PostgresJobRepository::new(pool.clone());
    let repository_b = PostgresJobRepository::new(pool.clone());
    let prefix = format!("integration:{}:", Uuid::new_v4());

    let active_key = format!("{prefix}active");
    let inserted = repository_a
        .enqueue(spec(active_key.clone(), 2, 0))
        .await
        .expect("first enqueue should succeed");
    let active_id = match inserted {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { .. } => panic!("first enqueue should insert"),
    };
    assert!(matches!(
        repository_b.enqueue(spec(active_key, 2, 0)).await,
        Ok(EnqueueResult::AlreadyActive { job_id }) if job_id == active_id
    ));

    let (claimed_a, claimed_b) = tokio::join!(
        repository_a.claim_next(OWNER_A, at(100), Duration::seconds(30)),
        repository_b.claim_next(OWNER_B, at(100), Duration::seconds(30)),
    );
    let mut claims = [
        claimed_a.expect("worker A claim should succeed"),
        claimed_b.expect("worker B claim should succeed"),
    ]
    .into_iter()
    .flatten();
    let lease = claims
        .next()
        .expect("exactly one worker should claim the row");
    assert!(
        claims.next().is_none(),
        "SKIP LOCKED must prevent a double claim"
    );
    assert_eq!(lease.job.id(), active_id);
    let lease_owner = lease
        .job
        .lease_owner()
        .expect("a claimed job should have a lease owner")
        .to_owned();

    let wrong_token_result = repository_b
        .succeed(active_id, &lease_owner, LeaseToken::new(), at(101))
        .await;
    assert!(
        matches!(
            wrong_token_result.as_ref(),
            Err(JobRepositoryError::Domain(
                wechrss::domain::job::JobError::LeaseTokenMismatch
            ))
        ),
        "unexpected stale-token result: {wrong_token_result:?}"
    );
    let renewed = repository_a
        .heartbeat(
            active_id,
            &lease_owner,
            lease.token,
            at(110),
            Duration::seconds(30),
        )
        .await
        .expect("current owner should renew its lease");
    assert!(
        renewed
            .lease_until()
            .is_some_and(|lease_until| lease_until > Utc::now() + Duration::seconds(20)),
        "PostgreSQL should calculate the renewed lease from database time"
    );

    let waiting = repository_a
        .retry(
            active_id,
            &lease_owner,
            lease.token,
            at(120),
            at(200),
            "temporary failure",
        )
        .await
        .expect("current owner should schedule a retry");
    assert_eq!(waiting.status(), JobStatus::RetryWait);
    let retry_lease = repository_b
        .claim_next(OWNER_B, at(200), Duration::seconds(30))
        .await
        .expect("retry claim should succeed")
        .expect("retry-wait job should become claimable");
    let exhausted = repository_b
        .retry(
            active_id,
            OWNER_B,
            retry_lease.token,
            at(210),
            at(300),
            "second failure",
        )
        .await
        .expect("last retry should become terminal");
    assert_eq!(exhausted.status(), JobStatus::Failed);

    let recovery_key = format!("{prefix}recovery");
    let recovery_id = match repository_a
        .enqueue(spec(recovery_key, 2, 0))
        .await
        .expect("recovery job should enqueue")
    {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { .. } => panic!("recovery job should insert"),
    };
    let recovery_lease = repository_a
        .claim_next(OWNER_A, at(400), Duration::seconds(30))
        .await
        .expect("recovery job should claim")
        .expect("recovery job should be due");
    assert_eq!(recovery_lease.job.id(), recovery_id);
    sqlx::query(
        "UPDATE jobs SET lease_until = clock_timestamp() - interval '1 second' WHERE id = $1",
    )
    .bind(recovery_id)
    .execute(&pool)
    .await
    .expect("test should be able to expire the recovery lease");
    let recovered = repository_b
        .recover_expired(at(430), 10)
        .await
        .expect("expired lease recovery should succeed");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status(), JobStatus::Queued);
    assert_eq!(
        repository_a
            .find(recovery_id)
            .await
            .expect("recovery job lookup should succeed")
            .expect("recovery job should remain stored")
            .status(),
        JobStatus::Queued
    );

    let rollback_key = format!("{prefix}rollback");
    let rollback_id = {
        let mut transaction = repository_a
            .begin()
            .await
            .expect("transaction should begin");
        let result = transaction
            .enqueue(spec(rollback_key, 1, 0))
            .await
            .expect("transactional enqueue should succeed");
        match result {
            EnqueueResult::Inserted(job) => job.id(),
            EnqueueResult::AlreadyActive { .. } => panic!("rollback job should insert"),
        }
    };
    assert!(repository_a
        .find(rollback_id)
        .await
        .expect("rolled-back lookup should succeed")
        .is_none());

    let unit_of_work_factory = UnitOfWorkFactory::new(pool.clone());
    let committed_id = {
        let mut unit = unit_of_work_factory
            .begin()
            .await
            .expect("unit of work should begin");
        let result = unit
            .jobs()
            .enqueue(spec(format!("{prefix}unit-commit"), 1, 0))
            .await
            .expect("unit-of-work enqueue should succeed");
        let id = match result {
            EnqueueResult::Inserted(job) => job.id(),
            EnqueueResult::AlreadyActive { .. } => panic!("unit-of-work job should insert"),
        };
        assert!(repository_a
            .find(id)
            .await
            .expect("uncommitted job lookup should succeed")
            .is_none());
        unit.commit()
            .await
            .expect("unit-of-work commit should succeed");
        id
    };
    assert!(repository_a
        .find(committed_id)
        .await
        .expect("committed job lookup should succeed")
        .is_some());

    let rolled_back_id = {
        let mut unit = unit_of_work_factory
            .begin()
            .await
            .expect("second unit of work should begin");
        let result = unit
            .jobs()
            .enqueue(spec(format!("{prefix}unit-rollback"), 1, 0))
            .await
            .expect("second unit-of-work enqueue should succeed");
        let id = match result {
            EnqueueResult::Inserted(job) => job.id(),
            EnqueueResult::AlreadyActive { .. } => panic!("unit-of-work job should insert"),
        };
        unit.rollback()
            .await
            .expect("unit-of-work rollback should succeed");
        id
    };
    assert!(repository_a
        .find(rolled_back_id)
        .await
        .expect("rolled-back unit-of-work lookup should succeed")
        .is_none());
}
