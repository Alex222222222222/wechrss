use chrono::{Duration, TimeZone, Utc};
use sqlx::PgPool;
use uuid::Uuid;
use wechrss::{
    domain::{
        article::{ArticleObservationVersion, NewArticle},
        source::{NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
    },
    persistence::{
        repositories::{
            article_repository::{
                ArticleRepository, ArticleRepositoryError, ArticleTransactionRepository,
                PostgresArticleRepository,
            },
            source_repository::{SourceRepository, SourceTransactionRepository},
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn postgres_articles_upsert_idempotently_and_list_in_feed_order(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;

    let first = upsert_article(&factory, article(source_id, "review-1", " First", 20)).await;
    assert!(first.feed_visible_change());
    assert_eq!(first.article().title(), "First");

    let mut observed_again = article(source_id, " review-1 ", "First", 20);
    observed_again.observation_version = ArticleObservationVersion::from_u64(2);
    observed_again.fetched_at = at(200);
    let no_change = upsert_article(&factory, observed_again).await;
    assert!(!no_change.feed_visible_change());
    assert_eq!(no_change.article().review_id(), "review-1");
    assert_eq!(no_change.article().fetched_at(), at(200));

    let partial = upsert_article(
        &factory,
        NewArticle {
            author: None,
            summary: None,
            cover_url: None,
            original_url: None,
            content_html: String::new(),
            content_hash: None,
            observation_version: ArticleObservationVersion::from_u64(3),
            fetched_at: at(300),
            ..article(source_id, "review-1", "First", 20)
        },
    )
    .await;
    assert!(!partial.feed_visible_change());
    assert_eq!(partial.article().fetched_at(), at(300));
    assert_eq!(partial.article().content_html(), "<p>content</p>");
    assert_eq!(
        partial.article().original_url().unwrap().as_str(),
        "https://mp.weixin.qq.com/s/review-1"
    );

    let changed = upsert_article(
        &factory,
        NewArticle {
            title: "Updated".to_owned(),
            content_html: "<p>new content</p>".to_owned(),
            content_hash: Some("hash-2".to_owned()),
            observation_version: ArticleObservationVersion::from_u64(4),
            fetched_at: at(400),
            ..article(source_id, "review-1", "First", 20)
        },
    )
    .await;
    assert!(changed.feed_visible_change());
    assert_eq!(changed.article().title(), "Updated");

    let stale = upsert_article(
        &factory,
        NewArticle {
            title: "Stale update".to_owned(),
            content_html: "<p>stale content</p>".to_owned(),
            content_hash: Some("stale-hash".to_owned()),
            observation_version: ArticleObservationVersion::from_u64(3),
            // This older acquisition completed after the newer version.
            fetched_at: at(500),
            ..article(source_id, "review-1", "First", 20)
        },
    )
    .await;
    assert!(!stale.feed_visible_change());
    assert_eq!(stale.article().title(), "Updated");
    assert_eq!(stale.article().content_html(), "<p>new content</p>");
    assert_eq!(stale.article().fetched_at(), at(400));

    upsert_article(&factory, article(source_id, "review-2", "Second", 10)).await;
    let repository = PostgresArticleRepository::new(pool);
    let listed = repository
        .list_for_feed(source_id, 10)
        .await
        .expect("article list should succeed");
    assert_eq!(
        listed
            .iter()
            .map(|article| article.review_id())
            .collect::<Vec<_>>(),
        vec!["review-1", "review-2"]
    );
    assert_eq!(listed[0].content_html(), "<p>new content</p>");
    assert_eq!(
        listed[0].original_url().unwrap().as_str(),
        "https://mp.weixin.qq.com/s/review-1"
    );

    let found = repository
        .find(source_id, " review-1 ")
        .await
        .expect("article lookup should succeed")
        .expect("article should exist");
    assert_eq!(found.title(), "Updated");
    assert!(matches!(
        repository.list_for_feed(source_id, 0).await,
        Err(ArticleRepositoryError::InvalidLimit)
    ));
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn postgres_allocates_observation_versions_from_a_shared_sequence(pool: PgPool) {
    let repository = PostgresArticleRepository::new(pool);
    let first = repository
        .allocate_observation_version()
        .await
        .expect("first observation version should be allocated");
    let second = repository
        .allocate_observation_version()
        .await
        .expect("second observation version should be allocated");

    assert_eq!(first.as_u64() + 1, second.as_u64());
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn article_change_and_source_revision_commit_as_one_unit(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;

    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .articles()
        .upsert(article(source_id, "review-atomic", "Atomic", 30))
        .await
        .expect("article should be inserted");
    assert!(result.feed_visible_change());
    let revision = unit_of_work
        .source()
        .bump_feed_revision(source_id, wechrss::domain::source::FeedRevision::zero())
        .await
        .expect("source revision should advance");
    assert_eq!(revision.as_u64(), 1);
    unit_of_work
        .commit()
        .await
        .expect("article and revision should commit");

    let article_repository = PostgresArticleRepository::new(pool.clone());
    assert!(article_repository
        .find(source_id, "review-atomic")
        .await
        .expect("article lookup should succeed")
        .is_some());
    let source =
        wechrss::persistence::repositories::source_repository::PostgresSourceRepository::new(pool)
            .find(source_id)
            .await
            .expect("source lookup should succeed")
            .expect("source should exist");
    assert_eq!(source.feed_revision().as_u64(), 1);
}

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn article_upsert_rolls_back_with_the_unit_of_work(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    let factory = UnitOfWorkFactory::new(pool.clone());
    create_source(&factory, source_id).await;
    upsert_article(
        &factory,
        article(source_id, "review-rollback", "Original", 10),
    )
    .await;

    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    unit_of_work
        .articles()
        .upsert(NewArticle {
            title: "Uncommitted".to_owned(),
            ..article(source_id, "review-rollback", "Original", 10)
        })
        .await
        .expect("article update should succeed");
    unit_of_work
        .rollback()
        .await
        .expect("rollback should succeed");

    let persisted = PostgresArticleRepository::new(pool)
        .find(source_id, "review-rollback")
        .await
        .expect("article lookup should succeed")
        .expect("original article should remain");
    assert_eq!(persisted.title(), "Original");
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
            sync_interval: Duration::hours(1),
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

async fn upsert_article(
    factory: &UnitOfWorkFactory,
    article: NewArticle,
) -> wechrss::domain::article::ArticleUpsertResult {
    let mut unit_of_work = factory.begin().await.expect("unit of work should begin");
    let result = unit_of_work
        .articles()
        .upsert(article)
        .await
        .expect("article upsert should succeed");
    unit_of_work
        .commit()
        .await
        .expect("article upsert should commit");
    result
}

fn article(source_id: SourceId, review_id: &str, title: &str, published_at: i64) -> NewArticle {
    NewArticle {
        source_id,
        review_id: review_id.to_owned(),
        title: title.to_owned(),
        author: Some("Author".to_owned()),
        summary: Some("Summary".to_owned()),
        cover_url: Some("https://cdn.example.test/cover.jpg".to_owned()),
        original_url: Some(
            format!("https://mp.weixin.qq.com/s/{}", review_id.trim())
                .parse()
                .expect("article URL should be valid"),
        ),
        published_at: at(published_at),
        content_html: "<p>content</p>".to_owned(),
        content_hash: Some("hash-1".to_owned()),
        observation_version: ArticleObservationVersion::from_u64(1),
        fetched_at: at(100),
    }
}

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}
