//! PostgreSQL integration coverage for the database-backed asset cache.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Utc;
use sqlx::{PgPool, Row};
use uuid::Uuid;
use werrss::{
    archive::asset_store::{AssetCachePolicy, AssetInput, AssetRead},
    domain::{
        article::{ArticleObservationVersion, NewArticle},
        source::{NewSource, SchedulingGate, SourceId, VerifiedWechatArticleUrl},
    },
    persistence::{
        repositories::{
            article_repository::ArticleTransactionRepository,
            asset_repository::{
                AssetRepositoryError, AssetTransactionRepository, PostgresAssetStore,
            },
            source_repository::SourceTransactionRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
};

const FIRST_BYTES: &[u8] = b"first-image";
const SECOND_BYTES: &[u8] = b"second-img";

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn stores_asset_bytes_and_updates_last_accessed_on_read(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["read-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "read-article",
        asset_input("https://cdn.example/read.png", FIRST_BYTES, 0),
    )
    .await;

    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '1 hour' WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let read = PostgresAssetStore::new(pool.clone(), policy)
        .read_and_touch(stored.id())
        .await
        .unwrap()
        .expect("referenced asset should be readable");
    let AssetRead::Available {
        media_type,
        checksum,
        bytes,
    } = read
    else {
        panic!("stored asset should have binary data");
    };
    assert_eq!(media_type, "image/png");
    assert_eq!(checksum, stored.checksum());
    assert_eq!(bytes, FIRST_BYTES);

    let recently_accessed: bool = sqlx::query_scalar(
        "SELECT last_accessed_at > clock_timestamp() - interval '1 minute' FROM asset_blobs WHERE id = $1",
    )
    .bind(stored.blob_id())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(recently_accessed, "a successful read should touch the blob");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn asset_read_waits_for_eviction_lock_and_keeps_bytes_available(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["read-lock-article"]).await;
    let policy = test_policy(0, Duration::from_secs(1), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "read-lock-article",
        asset_input("https://cdn.example/read-lock.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query(
        "UPDATE asset_blobs
         SET last_accessed_at = clock_timestamp() - interval '1 day'
         WHERE id = $1",
    )
    .bind(stored.blob_id())
    .execute(&pool)
    .await
    .unwrap();

    let mut lock_connection = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *lock_connection)
        .await
        .unwrap();

    let asset_id = stored.id();
    let read_task = tokio::spawn({
        let pool = pool.clone();
        async move {
            PostgresAssetStore::new(pool, policy)
                .read_and_touch(asset_id)
                .await
        }
    });
    wait_for_capacity_waiter(&pool).await;
    assert!(
        !read_task.is_finished(),
        "asset read must not finish while maintenance holds the capacity lock"
    );

    sqlx::query("SELECT pg_advisory_unlock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *lock_connection)
        .await
        .unwrap();
    let read = tokio::time::timeout(Duration::from_secs(2), read_task)
        .await
        .expect("asset read should finish after the capacity lock is released")
        .unwrap()
        .unwrap()
        .expect("referenced asset should be readable");
    assert!(matches!(
        read,
        AssetRead::Available { ref bytes, .. } if bytes == FIRST_BYTES
    ));

    let maintenance = PostgresAssetStore::new(pool.clone(), policy)
        .maintenance()
        .await
        .unwrap();
    assert_eq!(
        maintenance.stale_blobs, 0,
        "a read that waited for the eviction decision must refresh last_accessed_at"
    );
    assert!(matches!(
        PostgresAssetStore::new(pool, policy)
            .read_and_touch(asset_id)
            .await
            .unwrap(),
        Some(AssetRead::Available { .. })
    ));
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn reuses_one_record_when_the_same_url_is_seen_again(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["url-first", "url-second"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "url-first",
        asset_input("https://cdn.example/shared.png", FIRST_BYTES, 0),
    )
    .await;
    let second = store_asset(
        &pool,
        policy,
        source_id,
        "url-second",
        asset_input("https://cdn.example/shared.png", FIRST_BYTES, 0),
    )
    .await;

    assert_eq!(second.id(), first.id());
    assert_eq!(second.blob_id(), first.blob_id());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM article_assets")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn deduplicates_same_bytes_within_one_asset_batch(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["batch-dedup"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let inputs = vec![
        asset_input("https://cdn.example/batch-one.png", FIRST_BYTES, 0),
        asset_input("https://cdn.example/batch-two.png", FIRST_BYTES, 1),
    ];

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin_with_assets(&inputs).await.unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "batch-dedup", &inputs)
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].blob_id(), stored[1].blob_id());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn replacing_a_full_article_observation_removes_stale_asset_links(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["replace-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "replace-article",
        asset_input("https://cdn.example/old.png", FIRST_BYTES, 0),
    )
    .await;

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    let replacement = unit_of_work
        .assets(policy)
        .replace_for_article(source_id, "replace-article", &[])
        .await
        .unwrap();
    assert!(replacement.is_empty());
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM article_assets
             WHERE source_id = $1 AND review_id = 'replace-article'",
        )
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "relationship replacement must not delete repair metadata"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn clearing_article_asset_links_keeps_repair_metadata(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["clear-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "clear-article",
        asset_input("https://cdn.example/clear.png", FIRST_BYTES, 0),
    )
    .await;

    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .assets(policy)
        .clear_for_article(source_id, "clear-article")
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM article_assets
             WHERE source_id = $1 AND review_id = 'clear-article'",
        )
        .bind(source_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1,
        "clearing a relationship must not delete repair metadata"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn failed_article_asset_replacement_preserves_existing_asset_link(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["failed-replacement"]).await;
    let seed_policy = test_policy(0, Duration::from_secs(30), 1024);
    let existing = store_asset(
        &pool,
        seed_policy,
        source_id,
        "failed-replacement",
        asset_input("https://cdn.example/existing.png", FIRST_BYTES, 0),
    )
    .await;

    let replacement_policy = test_policy(5, Duration::from_secs(30), 1024);
    let replacement = asset_input("https://cdn.example/replacement.png", SECOND_BYTES, 0);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&replacement))
        .await
        .unwrap();
    let result = unit_of_work
        .assets(replacement_policy)
        .replace_for_article(
            source_id,
            "failed-replacement",
            std::slice::from_ref(&replacement),
        )
        .await;
    assert_eq!(
        result,
        Err(AssetRepositoryError::CapacityExceeded {
            requested_bytes: SECOND_BYTES.len() as u64,
            max_bytes: 5,
        })
    );
    unit_of_work.commit().await.unwrap();

    let attached_id: Uuid = sqlx::query_scalar(
        "SELECT asset_record_id
         FROM article_assets
         WHERE source_id = $1 AND review_id = 'failed-replacement'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(attached_id, existing.id());
    assert_eq!(
        sqlx::query_scalar::<_, Vec<u8>>("SELECT data FROM asset_blobs WHERE id = $1")
            .bind(existing.blob_id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        FIRST_BYTES
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn same_byte_reuse_waits_for_the_capacity_decision_lock(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["lock-first", "lock-second"]).await;
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "lock-first",
        asset_input("https://cdn.example/locked.png", FIRST_BYTES, 0),
    )
    .await;

    let mut lock_connection = pool.acquire().await.unwrap();
    sqlx::query("SELECT pg_advisory_lock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *lock_connection)
        .await
        .unwrap();

    let task = tokio::spawn({
        let pool = pool.clone();
        async move {
            store_asset(
                &pool,
                policy,
                source_id,
                "lock-second",
                asset_input("https://cdn.example/locked.png", FIRST_BYTES, 0),
            )
            .await
        }
    });
    let lock_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let waiting_for_capacity: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks AS locks
                 JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
                 WHERE locks.locktype = 'advisory'
                   AND NOT locks.granted
                   AND locks.objsubid = 1
                   AND activity.datname = current_database()
                   AND locks.classid = (
                       (hashtextextended('asset-capacity', 0) >> 32)
                       & 4294967295
                   )::oid
                   AND locks.objid = (
                       hashtextextended('asset-capacity', 0) & 4294967295
                   )::oid
             )",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        if waiting_for_capacity {
            break;
        }
        assert!(
            !task.is_finished(),
            "same-byte reuse completed without joining the capacity lock"
        );
        assert!(
            Instant::now() < lock_deadline,
            "reuse did not request the lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    sqlx::query("SELECT pg_advisory_unlock(hashtextextended('asset-capacity', 0))")
        .execute(&mut *lock_connection)
        .await
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("same-byte reuse should finish after the lock is released")
        .unwrap();
    assert_eq!(second.id(), first.id());
    assert_eq!(second.blob_id(), first.blob_id());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn shares_one_blob_when_different_urls_have_identical_bytes(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["bytes-first", "bytes-second"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "bytes-first",
        asset_input("https://cdn.example/one.png", FIRST_BYTES, 0),
    )
    .await;
    let second = store_asset(
        &pool,
        policy,
        source_id,
        "bytes-second",
        asset_input("https://cdn.example/two.png", FIRST_BYTES, 0),
    )
    .await;

    assert_ne!(
        second.id(),
        first.id(),
        "different URLs need separate records"
    );
    assert_eq!(second.blob_id(), first.blob_id());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn creates_a_new_url_version_when_the_bytes_change(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["version-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "version-article",
        asset_input("https://cdn.example/version.png", FIRST_BYTES, 0),
    )
    .await;
    let second = store_asset(
        &pool,
        policy,
        source_id,
        "version-article",
        asset_input("https://cdn.example/version.png", SECOND_BYTES, 0),
    )
    .await;

    assert_ne!(second.id(), first.id());
    assert_ne!(second.blob_id(), first.blob_id());
    let versions = sqlx::query(
        "SELECT version FROM asset_records WHERE source_url = 'https://cdn.example/version.png' ORDER BY version",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        versions
            .iter()
            .map(|row| row.get::<i64, _>("version"))
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let attached_id: Uuid = sqlx::query_scalar(
        "SELECT asset_record_id FROM article_assets WHERE source_id = $1 AND review_id = 'version-article'",
    )
    .bind(source_id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(attached_id, second.id());
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn evicts_oldest_binary_bytes_but_retains_url_metadata(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["evict-first", "evict-second"]).await;
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let first = store_asset(
        &pool,
        policy,
        source_id,
        "evict-first",
        asset_input("https://cdn.example/evict-first.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '1 hour' WHERE id = $1")
        .bind(first.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let second = store_asset(
        &pool,
        policy,
        source_id,
        "evict-second",
        asset_input("https://cdn.example/evict-second.png", SECOND_BYTES, 0),
    )
    .await;

    assert_eq!(
        PostgresAssetStore::new(pool.clone(), policy)
            .read_and_touch(first.id())
            .await
            .unwrap(),
        Some(AssetRead::Missing {
            source_url: "https://cdn.example/evict-first.png".parse().unwrap(),
        })
    );
    assert_eq!(
        PostgresAssetStore::new(pool.clone(), policy)
            .read_and_touch(second.id())
            .await
            .unwrap()
            .and_then(|read| match read {
                AssetRead::Available { bytes, .. } => Some(bytes),
                AssetRead::Missing { .. } => None,
            }),
        Some(SECOND_BYTES.to_vec())
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        2,
        "eviction must not remove URL/version metadata"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT fetch_status FROM asset_records WHERE id = $1",)
            .bind(first.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "missing"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn serializes_concurrent_capacity_decisions(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["concurrent-first", "concurrent-second"]).await;
    let policy = test_policy(15, Duration::from_secs(30), 1024);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first = store_asset_after_barrier(
        &pool,
        policy,
        source_id,
        "concurrent-first",
        asset_input("https://cdn.example/concurrent-first.png", FIRST_BYTES, 0),
        barrier.clone(),
    );
    let second = store_asset_after_barrier(
        &pool,
        policy,
        source_id,
        "concurrent-second",
        asset_input("https://cdn.example/concurrent-second.png", SECOND_BYTES, 0),
        barrier,
    );
    let (_first, _second) = tokio::join!(first, second);

    let present_bytes: i64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(byte_size), 0)::bigint FROM asset_blobs WHERE data IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        present_bytes <= 15,
        "cache bytes must remain within the cap"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn serializes_multi_asset_batches_before_acquiring_url_locks(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["batch-first", "batch-second"]).await;
    let policy = test_policy(1024, Duration::from_secs(30), 1024);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let first_batch = store_assets_after_barrier(
        &pool,
        policy,
        source_id,
        "batch-first",
        vec![
            asset_input("https://cdn.example/batch-one.png", FIRST_BYTES, 0),
            asset_input("https://cdn.example/batch-two.png", SECOND_BYTES, 1),
        ],
        barrier.clone(),
    );
    let second_batch = store_assets_after_barrier(
        &pool,
        policy,
        source_id,
        "batch-second",
        vec![
            asset_input("https://cdn.example/batch-two.png", SECOND_BYTES, 0),
            asset_input("https://cdn.example/batch-one.png", FIRST_BYTES, 1),
        ],
        barrier,
    );

    let (first_batch, second_batch) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(first_batch, second_batch)
    })
    .await
    .expect("reverse-order asset batches must not deadlock");
    assert_eq!(first_batch.len(), 2);
    assert_eq!(second_batch.len(), 2);
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_evicts_idle_bytes_but_retains_the_repair_record(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["stale-article"]).await;
    let policy = test_policy(0, Duration::from_secs(1), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "stale-article",
        asset_input("https://cdn.example/stale.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("UPDATE asset_blobs SET last_accessed_at = clock_timestamp() - interval '1 day' WHERE id = $1")
        .bind(stored.blob_id())
        .execute(&pool)
        .await
        .unwrap();

    let result = PostgresAssetStore::new(pool.clone(), policy)
        .maintenance()
        .await
        .unwrap();

    assert_eq!(result.stale_blobs, 1);
    assert_eq!(
        PostgresAssetStore::new(pool.clone(), policy)
            .read_and_touch(stored.id())
            .await
            .unwrap(),
        Some(AssetRead::Missing {
            source_url: "https://cdn.example/stale.png".parse().unwrap(),
        })
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT fetch_status FROM asset_records WHERE id = $1",)
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        "missing"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records WHERE id = $1")
            .bind(stored.id())
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_and_asset_reuse_follow_the_same_lock_order(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(
        &pool,
        source_id,
        &["maintenance-article", "maintenance-writer"],
    )
    .await;
    let policy = test_policy(1024, Duration::from_secs(1), 1024);
    let stored = store_asset(
        &pool,
        policy,
        source_id,
        "maintenance-article",
        asset_input("https://cdn.example/maintenance.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query(
        "UPDATE asset_blobs
         SET last_accessed_at = clock_timestamp() - interval '1 day'
         WHERE id = $1",
    )
    .bind(stored.blob_id())
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "CREATE FUNCTION test_hold_asset_blob_update() RETURNS trigger
         LANGUAGE plpgsql AS $$
         BEGIN
             PERFORM pg_advisory_lock(2147483645::bigint);
             PERFORM pg_sleep(1);
             PERFORM pg_advisory_unlock(2147483645::bigint);
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "CREATE TRIGGER test_hold_asset_blob_update
         BEFORE UPDATE OF data ON asset_blobs
         FOR EACH ROW EXECUTE FUNCTION test_hold_asset_blob_update()",
    )
    .execute(&pool)
    .await
    .unwrap();

    let maintenance = tokio::spawn({
        let pool = pool.clone();
        async move { PostgresAssetStore::new(pool, policy).maintenance().await }
    });
    wait_for_asset_update_trigger(&pool).await;
    let writer = tokio::spawn({
        let pool = pool.clone();
        async move {
            store_asset(
                &pool,
                policy,
                source_id,
                "maintenance-writer",
                asset_input("https://cdn.example/maintenance.png", FIRST_BYTES, 0),
            )
            .await
        }
    });

    let (maintenance, writer) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(maintenance, writer)
    })
    .await
    .expect("maintenance and asset reuse must not deadlock");
    assert!(
        maintenance
            .expect("maintenance task should not panic")
            .is_ok(),
        "maintenance should complete successfully"
    );
    writer.expect("asset writer task should not panic");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn maintenance_removes_records_and_blobs_after_article_deletion(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["orphan-article"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    store_asset(
        &pool,
        policy,
        source_id,
        "orphan-article",
        asset_input("https://cdn.example/orphan.png", FIRST_BYTES, 0),
    )
    .await;
    sqlx::query("DELETE FROM articles WHERE source_id = $1 AND review_id = 'orphan-article'")
        .bind(source_id.as_uuid())
        .execute(&pool)
        .await
        .unwrap();

    let result = PostgresAssetStore::new(pool.clone(), policy)
        .maintenance()
        .await
        .unwrap();

    assert_eq!(result.orphan_records, 1);
    assert_eq!(result.orphan_blobs, 1);
    assert!(result.changed());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rejects_an_overlarge_asset_without_writing_cache_rows(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["too-large"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 4);
    let input = asset_input("https://cdn.example/too-large.png", FIRST_BYTES, 0);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "too-large", std::slice::from_ref(&input))
        .await;
    assert_eq!(
        result,
        Err(AssetRepositoryError::AssetTooLarge {
            bytes: FIRST_BYTES.len() as u64,
            max_bytes: 4,
        })
    );
    drop(unit_of_work);

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rejects_asset_bytes_mutated_after_preflight_without_writing_cache_rows(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["mutated-input"]).await;
    let policy = test_policy(0, Duration::from_secs(30), 1024);
    let mut input = asset_input("https://cdn.example/mutated.png", FIRST_BYTES, 0);
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();

    input.bytes = b"mutated-after-preflight".to_vec();
    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "mutated-input", std::slice::from_ref(&input))
        .await;

    assert_eq!(result, Err(AssetRepositoryError::ChecksumMismatch));
    drop(unit_of_work);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a mutated input must not persist bytes under a stale checksum"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rolls_back_a_partial_asset_batch_when_a_later_asset_cannot_fit(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["partial-batch"]).await;
    let policy = test_policy(2, Duration::from_secs(30), 1024);
    let inputs = vec![
        asset_input("https://cdn.example/partial-first.png", b"a", 0),
        asset_input("https://cdn.example/partial-second.png", b"bbb", 1),
    ];
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin_with_assets(&inputs).await.unwrap();
    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "partial-batch", &inputs)
        .await;
    assert!(matches!(
        result,
        Err(AssetRepositoryError::CapacityExceeded {
            requested_bytes: 3,
            max_bytes: 2,
        })
    ));
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a failed batch must not leave a binary blob behind"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a failed batch must not leave URL metadata behind"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM article_assets")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a failed batch must not leave an article relationship behind"
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn rejects_a_batch_that_would_evict_its_own_previous_asset(pool: PgPool) {
    let source_id = SourceId::from_uuid(Uuid::new_v4());
    create_source_and_articles(&pool, source_id, &["same-batch-capacity"]).await;
    let policy = test_policy(5, Duration::from_secs(30), 1024);
    let inputs = vec![
        asset_input("https://cdn.example/same-batch-first.png", b"one", 0),
        asset_input("https://cdn.example/same-batch-second.png", b"two", 1),
    ];
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin_with_assets(&inputs).await.unwrap();
    let result = unit_of_work
        .assets(policy)
        .store_for_article(source_id, "same-batch-capacity", &inputs)
        .await;
    assert_eq!(
        result,
        Err(AssetRepositoryError::CapacityExceeded {
            requested_bytes: 3,
            max_bytes: 5,
        })
    );
    unit_of_work.commit().await.unwrap();

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_blobs")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a rejected batch must not leave a blob that was evicted during insertion"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM asset_records")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a rejected batch must not leave URL metadata"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM article_assets")
            .fetch_one(&pool)
            .await
            .unwrap(),
        0,
        "a rejected batch must not leave article relationships"
    );
}

fn test_policy(
    max_cache_size_bytes: u64,
    max_age: Duration,
    max_asset_size_bytes: u64,
) -> AssetCachePolicy {
    AssetCachePolicy::new(
        max_cache_size_bytes,
        max_age,
        max_asset_size_bytes,
        10,
        1024 * 1024,
        Duration::from_secs(10),
        Duration::from_secs(2),
        2,
    )
    .unwrap()
}

fn asset_input(url: &str, bytes: &[u8], occurrence: u32) -> AssetInput {
    AssetInput::new(
        url.parse().unwrap(),
        url.parse().unwrap(),
        "image/png".to_owned(),
        bytes.to_vec(),
        occurrence,
        "https://mp.weixin.qq.com/s/article".parse().unwrap(),
        Some("https://mp.weixin.qq.com".to_owned()),
        Some("asset-test".to_owned()),
    )
}

async fn store_asset(
    pool: &PgPool,
    policy: AssetCachePolicy,
    source_id: SourceId,
    review_id: &str,
    input: AssetInput,
) -> werrss::archive::asset_store::StoredAsset {
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, review_id, &[input])
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
    stored.into_iter().next().unwrap()
}

async fn store_asset_after_barrier(
    pool: &PgPool,
    policy: AssetCachePolicy,
    source_id: SourceId,
    review_id: &str,
    input: AssetInput,
    barrier: Arc<tokio::sync::Barrier>,
) -> werrss::archive::asset_store::StoredAsset {
    let factory = UnitOfWorkFactory::new(pool.clone());
    barrier.wait().await;
    let mut unit_of_work = factory
        .begin_with_assets(std::slice::from_ref(&input))
        .await
        .unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, review_id, &[input])
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
    stored.into_iter().next().unwrap()
}

async fn store_assets_after_barrier(
    pool: &PgPool,
    policy: AssetCachePolicy,
    source_id: SourceId,
    review_id: &str,
    inputs: Vec<AssetInput>,
    barrier: Arc<tokio::sync::Barrier>,
) -> Vec<werrss::archive::asset_store::StoredAsset> {
    let factory = UnitOfWorkFactory::new(pool.clone());
    barrier.wait().await;
    let mut unit_of_work = factory.begin_with_assets(&inputs).await.unwrap();
    let stored = unit_of_work
        .assets(policy)
        .store_for_article(source_id, review_id, &inputs)
        .await
        .unwrap();
    unit_of_work.commit().await.unwrap();
    stored
}

async fn wait_for_asset_update_trigger(pool: &PgPool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let held: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks
                 WHERE locktype = 'advisory'
                   AND granted
                   AND objsubid = 1
                   AND classid = 0::oid
                   AND objid = 2147483645::oid
             )",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if held {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "maintenance did not reach the asset update trigger"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_capacity_waiter(pool: &PgPool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let waiting: bool = sqlx::query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_locks AS locks
                 JOIN pg_stat_activity AS activity ON activity.pid = locks.pid
                 WHERE locks.locktype = 'advisory'
                   AND NOT locks.granted
                   AND locks.objsubid = 1
                   AND activity.datname = current_database()
                   AND locks.classid = (
                       (hashtextextended('asset-capacity', 0) >> 32)
                       & 4294967295
                   )::oid
                   AND locks.objid = (
                       hashtextextended('asset-capacity', 0) & 4294967295
                   )::oid
             )",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        if waiting {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "asset read did not request the capacity lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn create_source_and_articles(pool: &PgPool, source_id: SourceId, review_ids: &[&str]) {
    let factory = UnitOfWorkFactory::new(pool.clone());
    let mut unit_of_work = factory.begin().await.unwrap();
    unit_of_work
        .source()
        .insert(NewSource {
            id: source_id,
            book_id: format!("asset-book-{source_id}"),
            display_name: "Asset test source".to_owned(),
            article_url: Some("https://mp.weixin.qq.com/s/source".parse().unwrap()),
            enabled: true,
            sync_interval: chrono::Duration::hours(1),
            rss_item_limit: 20,
            account_id: None,
            scheduling_gate: SchedulingGate::Ready,
            next_fetch_at: Utc::now(),
            priority: 0,
            max_attempts: 3,
        })
        .await
        .unwrap();

    for (index, review_id) in review_ids.iter().enumerate() {
        let article_url =
            VerifiedWechatArticleUrl::parse(&format!("https://mp.weixin.qq.com/s/{review_id}"))
                .unwrap();
        unit_of_work
            .articles()
            .upsert(NewArticle {
                source_id,
                review_id: (*review_id).to_owned(),
                title: format!("Asset test article {index}"),
                author: None,
                summary: None,
                cover_url: None,
                original_url: Some(article_url),
                published_at: Utc::now(),
                content_html: "<p>asset test article</p>".to_owned(),
                content_hash: Some(format!("asset-test-hash-{index}")),
                observation_version: ArticleObservationVersion::from_u64(index as u64 + 1),
                fetched_at: Utc::now(),
            })
            .await
            .unwrap();
    }
    unit_of_work.commit().await.unwrap();
}
