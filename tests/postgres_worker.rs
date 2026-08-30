use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;
use wechrss::{
    application::{
        job_service::{JobService, JobServiceConfig},
        worker::{JobExecution, JobHandler, Worker, WorkerConfig, WorkerRun},
    },
    domain::job::{JobStatus, JobType, NewJob},
    persistence::{
        repositories::job_repository::{EnqueueResult, JobLease, JobQueue, PostgresJobRepository},
        unit_of_work::UnitOfWorkFactory,
    },
};

struct FixedHandler {
    outcome: JobExecution,
}

impl JobHandler for FixedHandler {
    async fn execute(&self, _lease: &JobLease) -> JobExecution {
        self.outcome.clone()
    }
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn worker_commits_success_through_the_shared_unit_of_work(pool: PgPool) {
    let queue = PostgresJobRepository::new(pool.clone());
    let job_id = enqueue_job(&queue, "shared-uow-success").await;
    let worker = worker(
        queue.clone(),
        UnitOfWorkFactory::new(pool),
        JobExecution::Succeeded,
    );

    let result = worker
        .run_once(timestamp(1))
        .await
        .expect("worker should complete the job");
    let WorkerRun::Completed { job, outcome } = result else {
        panic!("a due job should be completed")
    };

    assert_eq!(job.id(), job_id);
    assert_eq!(outcome, JobExecution::Succeeded);
    assert_eq!(job.status(), JobStatus::Succeeded);
    assert_eq!(
        queue
            .find(job_id)
            .await
            .expect("job lookup should succeed")
            .expect("completed job should remain")
            .status(),
        JobStatus::Succeeded
    );
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn worker_defers_through_the_shared_unit_of_work_without_spending_failure_budget(
    pool: PgPool,
) {
    let queue = PostgresJobRepository::new(pool.clone());
    let job_id = enqueue_job(&queue, "shared-uow-deferred").await;
    let resume_at = timestamp(4_000_000_000);
    let worker = worker(
        queue.clone(),
        UnitOfWorkFactory::new(pool),
        JobExecution::Deferred { resume_at },
    );

    let result = worker
        .run_once(timestamp(1))
        .await
        .expect("worker should defer the job");
    let WorkerRun::Completed { job, outcome } = result else {
        panic!("a due job should be completed")
    };

    assert_eq!(job.id(), job_id);
    assert_eq!(outcome, JobExecution::Deferred { resume_at });
    assert_eq!(job.status(), JobStatus::Deferred);
    assert_eq!(job.failure_count(), 0);
    assert_eq!(job.run_after(), resume_at);
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn worker_retries_through_the_shared_unit_of_work_and_records_failure_state(pool: PgPool) {
    let queue = PostgresJobRepository::new(pool.clone());
    let job_id = enqueue_job(&queue, "shared-uow-retry").await;
    let retry_at = timestamp(4_000_000_000);
    let worker = worker(
        queue.clone(),
        UnitOfWorkFactory::new(pool),
        JobExecution::Retry {
            retry_at,
            error: "temporary upstream failure".to_owned(),
        },
    );

    let result = worker
        .run_once(timestamp(1))
        .await
        .expect("worker should retry the job");
    let WorkerRun::Completed { job, outcome } = result else {
        panic!("a due job should be completed")
    };

    assert_eq!(job.id(), job_id);
    assert_eq!(
        outcome,
        JobExecution::Retry {
            retry_at,
            error: "temporary upstream failure".to_owned(),
        }
    );
    assert_eq!(job.status(), JobStatus::RetryWait);
    assert_eq!(job.claim_count(), 1);
    assert_eq!(job.failure_count(), 1);
    assert_eq!(job.last_error(), Some("temporary upstream failure"));
    assert_eq!(job.run_after(), retry_at);
    assert!(job.lease_owner().is_none());

    let persisted = queue
        .find(job_id)
        .await
        .expect("job lookup should succeed")
        .expect("retried job should remain");
    assert_eq!(persisted.status(), JobStatus::RetryWait);
    assert_eq!(persisted.failure_count(), 1);
}

fn worker(
    queue: PostgresJobRepository,
    outcomes: UnitOfWorkFactory,
    outcome: JobExecution,
) -> Worker<PostgresJobRepository, UnitOfWorkFactory, FixedHandler> {
    Worker::new(
        JobService::new(
            queue,
            JobServiceConfig::new("worker-a", Duration::seconds(30), 1)
                .expect("job service configuration should be valid"),
        ),
        outcomes,
        FixedHandler { outcome },
        WorkerConfig::new(vec![JobType::SourceSync], std::time::Duration::from_secs(1))
            .expect("worker configuration should be valid"),
    )
    .expect("worker should be valid")
}

async fn enqueue_job(queue: &PostgresJobRepository, key: &str) -> Uuid {
    let job = queue
        .enqueue(NewJob {
            job_type: JobType::SourceSync,
            source_id: None,
            priority: 1,
            run_after: timestamp(0),
            max_attempts: 3,
            payload: json!({"test": key}),
            dedupe_key: format!("integration:{key}:{}", Uuid::new_v4()),
            now: timestamp(0),
        })
        .await
        .expect("job should enqueue");
    match job {
        EnqueueResult::Inserted(job) => job.id(),
        EnqueueResult::AlreadyActive { .. } => panic!("unique integration key should insert"),
    }
}

fn timestamp(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}
