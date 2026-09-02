//! PostgreSQL integration coverage for source-sync job finalization.

use std::collections::HashMap;

use chrono::{Duration, TimeZone, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;
use werrss::{
    acquisition::{
        article_page::{ArticlePageError, ExtractedArticlePage},
        weread::{WeReadAdapterError, WeReadArticleReference},
    },
    application::{
        source_sync_handler::{
            SourceSyncAcquirer, SourceSyncJobHandler, SourceSyncJobHandlerConfig,
            SourceSyncJobHandlerDependencies,
        },
        sync_service::SyncAcquisitionError,
        worker::{JobExecution, JobHandler},
    },
    domain::{
        job::{JobType, NewJob},
        pacing::QuietHours,
        source::{NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
        sync::SyncOutcome,
    },
    persistence::{
        repositories::{
            article_repository::PostgresArticleRepository,
            job_repository::{JobLease, JobQueue, PostgresJobRepository},
            source_repository::{PostgresSourceRepository, SourceTransactionRepository},
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

struct FakeAcquirer {
    references: Vec<WeReadArticleReference>,
    pages: HashMap<String, ExtractedArticlePage>,
    timeout_articles: Vec<String>,
    blocked_articles: Vec<String>,
    authentication_error: bool,
}

impl FakeAcquirer {
    fn successful(reference: WeReadArticleReference, page: ExtractedArticlePage) -> Self {
        let mut pages = HashMap::new();
        pages.insert(reference.review_id.clone(), page);
        Self {
            references: vec![reference],
            pages,
            timeout_articles: Vec::new(),
            blocked_articles: Vec::new(),
            authentication_error: false,
        }
    }
}

impl SourceSyncAcquirer for FakeAcquirer {
    async fn list_article_references(
        &self,
        _source: &werrss::domain::source::Source,
    ) -> Result<Vec<WeReadArticleReference>, SyncAcquisitionError> {
        if self.authentication_error {
            Err(SyncAcquisitionError::WeRead(
                WeReadAdapterError::AuthenticationExpired { code: 401 },
            ))
        } else {
            Ok(self.references.clone())
        }
    }

    async fn fetch_article(
        &self,
        reference: &WeReadArticleReference,
    ) -> Result<ExtractedArticlePage, SyncAcquisitionError> {
        if self
            .blocked_articles
            .iter()
            .any(|id| id == &reference.review_id)
        {
            return Err(SyncAcquisitionError::ArticlePage(
                ArticlePageError::VerificationRequired,
            ));
        }
        if self
            .timeout_articles
            .iter()
            .any(|id| id == &reference.review_id)
        {
            return Err(SyncAcquisitionError::ArticlePage(
                ArticlePageError::OperationTimedOut,
            ));
        }
        self.pages
            .get(&reference.review_id)
            .cloned()
            .ok_or_else(|| {
                SyncAcquisitionError::WeRead(WeReadAdapterError::Protocol(
                    "test article is missing".to_owned(),
                ))
            })
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_commits_article_run_schedule_and_feed_rebuild(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;

    let reference = reference("review-success", "https://mp.weixin.qq.com/s/success");
    let page = page("https://mp.weixin.qq.com/s/success", "Fetched title");
    let handler = handler(
        pool.clone(),
        FakeAcquirer::successful(reference, page),
        SourceSyncJobHandlerConfig::new(Duration::minutes(1), Duration::minutes(5)).unwrap(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let job_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_status, "succeeded");
    let article_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM articles WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(article_count, 1);
    let revision: i64 = sqlx::query_scalar("SELECT feed_revision FROM sources WHERE id = $1")
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(revision, 1);
    let rebuild_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE source_id = $1 AND job_type = 'feed_rebuild' AND status = 'queued'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(rebuild_count, 1);
    let run = sqlx::query(
        "SELECT outcome, articles_seen, articles_created, articles_updated, articles_failed, archived_articles, archived_assets FROM sync_runs WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(run.get::<String, _>("outcome"), "succeeded");
    assert_eq!(run.get::<i64, _>("articles_seen"), 1);
    assert_eq!(run.get::<i64, _>("articles_created"), 1);
    assert_eq!(run.get::<i64, _>("articles_updated"), 0);
    assert_eq!(run.get::<i64, _>("articles_failed"), 0);
    assert_eq!(run.get::<i64, _>("archived_articles"), 1);
    assert_eq!(run.get::<i64, _>("archived_assets"), 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_defers_before_upstream_work_during_quiet_hours(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;

    let database_now: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .unwrap();
    let (quiet_start, _) = database_now
        .time()
        .overflowing_sub_signed(Duration::hours(1));
    let (quiet_end, _) = database_now
        .time()
        .overflowing_add_signed(Duration::hours(1));
    let quiet_hours = QuietHours::new(chrono_tz::UTC, quiet_start, quiet_end).unwrap();
    let handler = handler(
        pool.clone(),
        FakeAcquirer {
            authentication_error: true,
            ..FakeAcquirer::successful(
                reference("quiet", "https://mp.weixin.qq.com/s/quiet"),
                page("https://mp.weixin.qq.com/s/quiet", "Should not fetch"),
            )
        },
        SourceSyncJobHandlerConfig::new(Duration::minutes(1), Duration::minutes(5))
            .unwrap()
            .with_quiet_hours(Some(quiet_hours)),
    );

    let outcome = handler
        .execute(
            &lease,
            Utc.timestamp_opt(1_700_000_000, 0).single().unwrap(),
        )
        .await;
    assert!(matches!(outcome, JobExecution::Deferred { .. }));

    let run_count: i64 = sqlx::query_scalar("SELECT count(*) FROM sync_runs WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        run_count, 0,
        "quiet-hours deferral must precede run creation"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn source_sync_retry_commits_cooldown_and_retryable_run_atomically(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let reference = reference("review-timeout", "https://mp.weixin.qq.com/s/timeout");
    let mut acquirer = FakeAcquirer::successful(
        reference,
        page("https://mp.weixin.qq.com/s/timeout", "Timeout"),
    );
    acquirer.timeout_articles.push("review-timeout".to_owned());
    let handler = handler(
        pool.clone(),
        acquirer,
        SourceSyncJobHandlerConfig::new(Duration::minutes(2), Duration::minutes(5)).unwrap(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let job = sqlx::query("SELECT status, failure_count FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job.get::<String, _>("status"), "retry_wait");
    assert_eq!(job.get::<i64, _>("failure_count"), 1);
    let source = sqlx::query(
        "SELECT scheduling_gate, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(source.get::<String, _>("scheduling_gate"), "ready");
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("failure_cooldown_until")
        .is_some());
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("schedule_reserved_until")
        .is_none());
    let outcome: String = sqlx::query_scalar("SELECT outcome FROM sync_runs WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outcome, SyncOutcome::RetryableFailure.as_str());
    let article_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM articles WHERE source_id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(article_count, 0);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn authentication_failure_gates_the_source_and_does_not_retry_the_job(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let handler = handler(
        pool.clone(),
        FakeAcquirer {
            references: Vec::new(),
            pages: HashMap::new(),
            timeout_articles: Vec::new(),
            blocked_articles: Vec::new(),
            authentication_error: true,
        },
        SourceSyncJobHandlerConfig::default(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let source_gate: String =
        sqlx::query_scalar("SELECT scheduling_gate FROM sources WHERE id = $1")
            .bind(source_id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(source_gate, "authentication_required");
    let job_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_status, "failed");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn blocked_public_acquisition_gates_the_source_and_does_not_retry_the_job(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    let job_id = enqueue_source_sync(&pool, source_id).await;
    let lease = claim(&pool, Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()).await;
    let reference = reference("review-blocked", "https://mp.weixin.qq.com/s/blocked");
    let mut acquirer = FakeAcquirer::successful(
        reference,
        page("https://mp.weixin.qq.com/s/blocked", "Blocked"),
    );
    acquirer.blocked_articles.push("review-blocked".to_owned());
    let handler = handler(
        pool.clone(),
        acquirer,
        SourceSyncJobHandlerConfig::default(),
    );

    assert_eq!(
        handler.execute(&lease, Utc::now()).await,
        JobExecution::Committed
    );

    let source = sqlx::query(
        "SELECT scheduling_gate, failure_cooldown_until, schedule_reserved_until FROM sources WHERE id = $1",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        source.get::<String, _>("scheduling_gate"),
        "risk_controlled"
    );
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("failure_cooldown_until")
        .is_none());
    assert!(source
        .get::<Option<chrono::DateTime<Utc>>, _>("schedule_reserved_until")
        .is_none());

    let job_status: String = sqlx::query_scalar("SELECT status FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(job_status, "failed");
    let outcome: String = sqlx::query_scalar("SELECT outcome FROM sync_runs WHERE job_id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(outcome, SyncOutcome::Blocked.as_str());
}

fn handler(
    pool: PgPool,
    acquirer: FakeAcquirer,
    config: SourceSyncJobHandlerConfig,
) -> SourceSyncJobHandler<PostgresSourceRepository, PostgresArticleRepository, FakeAcquirer> {
    SourceSyncJobHandler::new(
        SourceSyncJobHandlerDependencies {
            sources: PostgresSourceRepository::new(pool.clone()),
            articles: PostgresArticleRepository::new(pool.clone()),
            unit_of_work: UnitOfWorkFactory::new(pool),
            acquirer,
            sync_service: werrss::application::sync_service::SyncService::new(),
        },
        config,
    )
}

async fn create_source(factory: &UnitOfWorkFactory, source_id: SourceId) {
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .source()
        .insert(NewSource {
            id: source_id,
            book_id: format!("book-{source_id}"),
            display_name: "Test source".to_owned(),
            article_url: "https://mp.weixin.qq.com/s/source".parse().unwrap(),
            enabled: true,
            sync_interval: Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: Utc::now(),
            priority: 10,
            max_attempts: 3,
        })
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
}

async fn enqueue_source_sync(pool: &PgPool, source_id: SourceId) -> Uuid {
    let repository = PostgresJobRepository::new(pool.clone());
    let job = repository
        .enqueue_immediately(NewJob {
            job_type: JobType::SourceSync,
            source_id: Some(source_id.as_uuid()),
            priority: 10,
            run_after: Utc::now(),
            max_attempts: 3,
            payload: serde_json::json!({"source_id": source_id.to_string()}),
            dedupe_key: format!("source_sync:{source_id}"),
            now: Utc::now(),
        })
        .await
        .unwrap();
    match job {
        werrss::persistence::repositories::job_repository::EnqueueResult::Inserted(job) => job.id(),
        werrss::persistence::repositories::job_repository::EnqueueResult::AlreadyActive {
            job_id,
        } => job_id,
    }
}

async fn claim(pool: &PgPool, now: chrono::DateTime<Utc>) -> JobLease {
    PostgresJobRepository::new(pool.clone())
        .claim_next(
            "source-sync-test",
            now,
            Duration::minutes(5),
            &[JobType::SourceSync],
        )
        .await
        .unwrap()
        .expect("source-sync job should be claimable")
}

fn reference(review_id: &str, url: &str) -> WeReadArticleReference {
    WeReadArticleReference {
        review_id: review_id.to_owned(),
        article_url: Some(url.parse::<VerifiedWechatArticleUrl>().unwrap()),
        title: Some("List title".to_owned()),
        summary: None,
        author: None,
        cover_url: None,
        published_at: Some(Utc.timestamp_opt(1_700_000_000, 0).single().unwrap()),
    }
}

fn page(url: &str, title: &str) -> ExtractedArticlePage {
    ExtractedArticlePage {
        canonical_url: url.parse().unwrap(),
        title: title.to_owned(),
        author: Some("Author".to_owned()),
        summary: Some("Summary".to_owned()),
        published_at: None,
        content_html: "<p>content</p>".to_owned(),
        cover_url: None,
    }
}
