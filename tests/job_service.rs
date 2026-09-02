//! Integration coverage for the worker-facing job application boundary.

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::json;
use uuid::Uuid;
use werrss::{
    application::job_service::{JobService, JobServiceConfig, JobServiceError},
    domain::job::{JobError, JobStatus, JobType, NewJob},
    persistence::repositories::job_repository::{
        EnqueueResult, JobQueue, JobRepository, JobRepositoryError, JobRepositoryTransaction,
        MemoryJobRepository,
    },
};

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}

fn job(key: &str) -> NewJob {
    NewJob {
        job_type: JobType::SourceSync,
        source_id: Some(Uuid::from_u128(1)),
        priority: 1,
        run_after: at(0),
        max_attempts: 2,
        payload: json!({"source_id": "1"}),
        dedupe_key: key.to_owned(),
        now: at(0),
    }
}

fn service(repository: MemoryJobRepository, owner: &str) -> JobService<MemoryJobRepository> {
    JobService::new(
        repository,
        JobServiceConfig::new(owner, Duration::seconds(10), 1)
            .expect("test worker configuration should be valid"),
    )
}

#[tokio::test]
async fn replicas_share_deduplication_and_fencing() {
    let repository = MemoryJobRepository::new();
    let worker_a = service(repository.clone(), "worker-a");
    let worker_b = service(repository.clone(), "worker-b");

    assert!(matches!(
        worker_a.enqueue(job("same-work")).await,
        Ok(EnqueueResult::Inserted(_))
    ));
    assert!(matches!(
        worker_b.enqueue(job("same-work")).await,
        Ok(EnqueueResult::AlreadyActive { .. })
    ));

    let lease = worker_a
        .claim_next(at(1), &[JobType::SourceSync])
        .await
        .expect("claim should succeed")
        .expect("job should be claimable");
    let error = worker_b
        .heartbeat(&lease, at(2))
        .await
        .expect_err("a different owner must not heartbeat the lease");
    assert!(matches!(
        error,
        JobServiceError::Repository(JobRepositoryError::Domain(JobError::LeaseOwnerMismatch))
    ));
}

#[tokio::test]
async fn outcome_transaction_can_roll_back_a_retry_without_losing_the_lease_state() {
    let repository = MemoryJobRepository::new();
    let worker = service(repository.clone(), "worker-a");
    worker
        .enqueue(job("rollback"))
        .await
        .expect("enqueue should succeed");
    let lease = worker
        .claim_next(at(1), &[JobType::SourceSync])
        .await
        .expect("claim should succeed")
        .expect("job should be claimable");

    {
        let mut transaction = repository.begin().await.expect("transaction should open");
        let retried = worker
            .retry(&mut transaction, &lease, at(2), at(20), "temporary")
            .await
            .expect("retry should succeed before rollback");
        assert_eq!(retried.status(), JobStatus::RetryWait);
    }

    let still_running = repository
        .find(lease.job.id())
        .await
        .expect("job read should succeed")
        .expect("job should remain");
    assert_eq!(still_running.status(), JobStatus::Running);
    assert_eq!(still_running.failure_count(), 0);

    let mut transaction = repository.begin().await.expect("transaction should open");
    let retried = worker
        .retry(&mut transaction, &lease, at(2), at(20), "temporary")
        .await
        .expect("retry should succeed");
    transaction
        .commit()
        .await
        .expect("transaction should commit");
    assert_eq!(retried.status(), JobStatus::RetryWait);
    assert_eq!(retried.failure_count(), 1);
}

#[tokio::test]
async fn disallowed_claims_are_not_taken_and_unclaimed_jobs_can_be_cancelled() {
    let repository = MemoryJobRepository::new();
    let worker = service(repository.clone(), "worker-a");
    let inserted = worker
        .enqueue(job("cancel"))
        .await
        .expect("enqueue should succeed");
    let job_id = match inserted {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { .. } => panic!("test job should be inserted"),
    };

    assert!(
        worker
            .claim_next(at(1), &[JobType::FeedRebuild])
            .await
            .expect("claim should succeed")
            .is_none(),
        "source-sync work must not be claimed under a feed-rebuild filter"
    );

    let mut transaction = repository.begin().await.expect("transaction should open");
    let cancelled = worker
        .cancel(&mut transaction, job_id, at(2), "operator stopped it")
        .await
        .expect("cancellation should succeed");
    transaction
        .commit()
        .await
        .expect("transaction should commit");

    assert_eq!(cancelled.status(), JobStatus::Failed);
    assert_eq!(cancelled.last_error(), Some("operator stopped it"));
}
