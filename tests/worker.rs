//! Integration-style tests for one-pass worker execution.

use std::{sync::Arc, time::Duration as StdDuration};

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::json;
use tokio::sync::Mutex;
use wechrss::{
    application::{
        job_service::{JobService, JobServiceConfig},
        worker::{JobExecution, JobHandler, Worker, WorkerConfig, WorkerRun},
    },
    domain::job::{JobStatus, JobType, NewJob},
    persistence::repositories::job_repository::{JobLease, JobQueue, MemoryJobRepository},
};

fn at(seconds: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}

fn job(key: &str, job_type: JobType) -> NewJob {
    NewJob {
        job_type,
        source_id: None,
        priority: 1,
        run_after: at(0),
        max_attempts: 2,
        payload: json!({"key": key}),
        dedupe_key: key.to_owned(),
        now: at(0),
    }
}

fn worker_config(types: Vec<JobType>) -> WorkerConfig {
    WorkerConfig::new(types, StdDuration::from_millis(5))
        .expect("worker configuration should be valid")
}

#[derive(Clone)]
struct RecordingHandler {
    seen: Arc<Mutex<Vec<JobType>>>,
    outcome: JobExecution,
}

impl JobHandler for RecordingHandler {
    async fn execute(&self, lease: &JobLease) -> JobExecution {
        self.seen.lock().await.push(lease.job.job_type());
        self.outcome.clone()
    }
}

fn build_worker(
    repository: MemoryJobRepository,
    handler: RecordingHandler,
    types: Vec<JobType>,
) -> Worker<MemoryJobRepository, MemoryJobRepository, RecordingHandler> {
    Worker::new(
        JobService::new(
            repository.clone(),
            JobServiceConfig::new("integration-worker", Duration::seconds(1), 2)
                .expect("job service configuration should be valid"),
        ),
        repository,
        handler,
        worker_config(types),
    )
    .expect("worker configuration should match the job lease")
}

#[tokio::test]
async fn allowed_kind_filter_leaves_disallowed_job_queued() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("source", JobType::SourceSync))
        .await
        .expect("source job should enqueue");
    let seen = Arc::new(Mutex::new(Vec::new()));
    let worker = build_worker(
        repository.clone(),
        RecordingHandler {
            seen: seen.clone(),
            outcome: JobExecution::Succeeded,
        },
        vec![JobType::FeedRebuild],
    );

    assert_eq!(
        worker.run_once(at(1)).await.expect("worker should idle"),
        WorkerRun::Idle
    );
    assert!(seen.lock().await.is_empty());
    let lease = repository
        .claim_next(
            "test-reader",
            at(1),
            Duration::seconds(1),
            &[JobType::SourceSync],
        )
        .await
        .expect("queue read should succeed")
        .expect("disallowed job should remain claimable");
    assert_eq!(lease.job.status(), JobStatus::Running);
}

#[tokio::test]
async fn retry_outcome_increments_failure_count_without_running_a_second_job() {
    let repository = MemoryJobRepository::new();
    repository
        .enqueue(job("retry", JobType::SourceSync))
        .await
        .expect("job should enqueue");
    let worker = build_worker(
        repository.clone(),
        RecordingHandler {
            seen: Arc::new(Mutex::new(Vec::new())),
            outcome: JobExecution::Retry {
                retry_at: at(20),
                error: "temporary upstream failure".to_owned(),
            },
        },
        vec![JobType::SourceSync],
    );

    let first = worker
        .run_once(at(1))
        .await
        .expect("worker should complete");
    let WorkerRun::Completed { job, outcome } = first else {
        panic!("queued job should not be idle")
    };
    assert!(matches!(outcome, JobExecution::Retry { .. }));
    assert_eq!(job.status(), JobStatus::RetryWait);
    assert_eq!(job.failure_count(), 1);
    assert_eq!(
        worker.run_once(at(2)).await.expect("worker should wait"),
        WorkerRun::Idle
    );
}
