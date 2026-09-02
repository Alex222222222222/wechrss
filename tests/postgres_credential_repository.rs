use chrono::{TimeZone, Utc};
use secrecy::SecretString;
use sqlx::PgPool;
use tokio::time::{sleep, timeout, Duration};
use uuid::Uuid;
use werrss::persistence::repositories::credential_repository::{
    CredentialReplacement, CredentialRepository, CredentialRepositoryError,
    PostgresCredentialRepository,
};
use werrss::{
    application::auth_service::{
        AuthRefreshOutcome, AuthService, AuthServiceConfig, AuthServiceDependencies,
        CredentialProvision, CredentialRefresher, RefreshedCredentials, RingCredentialCipher,
    },
    application::source_sync_acquirer::{
        CredentialRepositoryAccountSelector, WeReadAccountSelector,
    },
    domain::credentials::{WeReadAccountId, WeReadCredentials},
    persistence::repositories::account_lease_repository::{
        AccountLeaseStore, PostgresAccountLeaseRepository,
    },
};

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .expect("test timestamp should be valid")
}

fn account_id() -> WeReadAccountId {
    WeReadAccountId::from_uuid(Uuid::new_v4())
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn account_selection_ignores_expired_and_disabled_accounts(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let expired = account_id();
    let disabled = account_id();
    let usable = account_id();
    repository
        .insert(
            expired,
            "expired",
            b"expired-ciphertext",
            Utc::now() - chrono::Duration::seconds(1),
        )
        .await
        .expect("expired account should be inserted");
    repository
        .insert(
            disabled,
            "disabled",
            b"disabled-ciphertext",
            Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .expect("disabled account should be inserted");
    sqlx::query("UPDATE weread_accounts SET disabled = TRUE WHERE account_id = $1")
        .bind(disabled.as_uuid())
        .execute(&pool)
        .await
        .expect("account should be disabled");
    repository
        .insert(
            usable,
            "usable",
            b"usable-ciphertext",
            Utc::now() + chrono::Duration::hours(1),
        )
        .await
        .expect("usable account should be inserted");

    let selector = CredentialRepositoryAccountSelector::new(repository);
    assert_eq!(selector.select_account(None).await.unwrap(), Some(usable));
    assert_eq!(selector.select_account(Some(expired)).await.unwrap(), None);
    assert_eq!(selector.select_account(Some(disabled)).await.unwrap(), None);
    assert_eq!(
        selector.select_account(Some(usable)).await.unwrap(),
        Some(usable)
    );
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn encrypted_account_rows_support_versioned_replacement(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let account_id = account_id();
    let inserted = repository
        .insert(account_id, "primary", b"ciphertext-v1", at(3_600))
        .await
        .expect("account should be inserted");
    assert_eq!(inserted.account().credential_version(), 1);
    assert_eq!(inserted.ciphertext(), b"ciphertext-v1");

    let raw: Vec<u8> = sqlx::query_scalar(
        "SELECT credentials_ciphertext FROM weread_accounts WHERE account_id = $1",
    )
    .bind(account_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("ciphertext should be stored");
    assert_eq!(raw, b"ciphertext-v1");

    let lease = leases
        .acquire(account_id, "credential-test", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("test should own the account lease");
    let replaced = repository
        .replace(CredentialReplacement {
            account_id,
            display_name: "primary".to_owned(),
            expected_version: 1,
            ciphertext: b"ciphertext-v2".to_vec(),
            access_expires_at: at(7_200),
            lease_owner: "credential-test".to_owned(),
            lease_token: lease.token(),
        })
        .await
        .expect("the expected version should replace credentials");
    assert_eq!(replaced.account().credential_version(), 2);
    assert_eq!(replaced.account().access_expires_at(), at(7_200));
    assert!(matches!(
        repository
            .replace(CredentialReplacement {
                account_id,
                display_name: "primary".to_owned(),
                expected_version: 1,
                ciphertext: b"stale".to_vec(),
                access_expires_at: at(8_000),
                lease_owner: "credential-test".to_owned(),
                lease_token: lease.token(),
            })
            .await,
        Err(CredentialRepositoryError::Conflict { account_id: lost }) if lost == account_id
    ));
    leases
        .release(account_id, "credential-test", lease.token())
        .await
        .expect("test lease should be released");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn credential_replacement_rejects_a_stale_account_lease(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let account_id = account_id();
    repository
        .insert(account_id, "primary", b"ciphertext-v1", at(3_600))
        .await
        .expect("account should be inserted");

    let stale = leases
        .acquire(account_id, "old-worker", chrono::Duration::minutes(5))
        .await
        .expect("old lease acquisition should succeed")
        .expect("old worker should own the lease");
    sqlx::query("UPDATE account_leases SET lease_until = clock_timestamp() - interval '1 second' WHERE account_id = $1")
        .bind(account_id.as_uuid())
        .execute(&pool)
        .await
        .expect("test should expire the old lease");
    let current = leases
        .acquire(account_id, "new-worker", chrono::Duration::minutes(5))
        .await
        .expect("takeover should succeed")
        .expect("new worker should take over the expired lease");

    assert!(matches!(
        repository
            .replace(CredentialReplacement {
                account_id,
                display_name: "primary".to_owned(),
                expected_version: 1,
                ciphertext: b"stale-worker-write".to_vec(),
                access_expires_at: at(8_000),
                lease_owner: "old-worker".to_owned(),
                lease_token: stale.token(),
            })
            .await,
        Err(CredentialRepositoryError::Conflict { account_id: lost }) if lost == account_id
    ));
    let updated = repository
        .replace(CredentialReplacement {
            account_id,
            display_name: "primary".to_owned(),
            expected_version: 1,
            ciphertext: b"current-worker-write".to_vec(),
            access_expires_at: at(8_000),
            lease_owner: "new-worker".to_owned(),
            lease_token: current.token(),
        })
        .await
        .expect("current lease should fence the replacement");
    assert_eq!(updated.ciphertext(), b"current-worker-write");
    leases
        .release(account_id, "new-worker", current.token())
        .await
        .expect("current lease should be released");
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn credential_replacement_waits_for_the_live_lease_row_lock(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let leases = PostgresAccountLeaseRepository::new(pool.clone());
    let account_id = account_id();
    repository
        .insert(account_id, "primary", b"ciphertext-v1", at(3_600))
        .await
        .expect("account should be inserted");
    let lease = leases
        .acquire(account_id, "credential-test", chrono::Duration::minutes(5))
        .await
        .expect("lease acquisition should succeed")
        .expect("test should own the account lease");

    let mut lock_holder = pool
        .begin()
        .await
        .expect("lock-holder transaction should begin");
    sqlx::query("SELECT account_id FROM account_leases WHERE account_id = $1 FOR UPDATE")
        .bind(account_id.as_uuid())
        .fetch_one(&mut *lock_holder)
        .await
        .expect("test should hold the account lease row lock");

    let replacement = CredentialReplacement {
        account_id,
        display_name: "primary".to_owned(),
        expected_version: 1,
        ciphertext: b"ciphertext-v2".to_vec(),
        access_expires_at: at(7_200),
        lease_owner: "credential-test".to_owned(),
        lease_token: lease.token(),
    };
    let replacement_task = tokio::spawn(async move { repository.replace(replacement).await });
    timeout(Duration::from_secs(5), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%WITH live_lease AS MATERIALIZED%' AND cardinality(pg_blocking_pids(pid)) > 0)",
            )
            .fetch_one(&pool)
            .await
            .expect("database should expose the replacement lock wait");
            if blocked {
                break;
            }
            assert!(
                !replacement_task.is_finished(),
                "replacement completed before waiting on the lease row lock"
            );
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("replacement should reach the lease row lock within the test timeout");

    lock_holder
        .commit()
        .await
        .expect("lock-holder transaction should commit");
    let replaced = replacement_task
        .await
        .expect("replacement task should finish")
        .expect("replacement should succeed after the lease lock is released");
    assert_eq!(replaced.account().credential_version(), 2);
    leases
        .release(account_id, "credential-test", lease.token())
        .await
        .expect("test lease should be released");
}

#[derive(Clone)]
struct TestRefresher;

#[async_trait::async_trait]
impl CredentialRefresher for TestRefresher {
    async fn refresh(
        &self,
        _account_id: WeReadAccountId,
        refresh_token: &str,
    ) -> Result<RefreshedCredentials, werrss::application::auth_service::CredentialRefreshError>
    {
        assert_eq!(refresh_token, "refresh-v1");
        RefreshedCredentials::new(
            "access-v2",
            Some("refresh-v2".to_owned()),
            Utc::now() + chrono::Duration::hours(1),
        )
        .map_err(|_| werrss::application::auth_service::CredentialRefreshError::InvalidResponse)
    }
}

#[sqlx::test(migrator = "werrss::persistence::postgres::MIGRATOR")]
async fn auth_refresh_round_trips_through_postgres_without_plaintext_storage(pool: PgPool) {
    let repository = PostgresCredentialRepository::new(pool.clone());
    let account_id = account_id();
    let service = AuthService::new(
        AuthServiceDependencies {
            accounts: repository.clone(),
            leases: PostgresAccountLeaseRepository::new(pool.clone()),
            refresher: TestRefresher,
            cipher: RingCredentialCipher::new(&SecretString::new("integration-key".into()))
                .expect("cipher should accept a non-empty key"),
        },
        AuthServiceConfig::new(
            chrono::Duration::seconds(30),
            chrono::Duration::seconds(30),
            chrono::Duration::seconds(5),
        )
        .expect("test auth policy should be valid"),
    );
    service
        .provision(CredentialProvision {
            account_id,
            display_name: "primary".to_owned(),
            credentials: WeReadCredentials::new("access-v1", "refresh-v1", at(110), at(100))
                .expect("credentials should be valid"),
        })
        .await
        .expect("provisioning should commit");

    let outcome = service
        .refresh_if_needed(account_id, "integration-worker")
        .await
        .expect("expired credentials should refresh");
    let AuthRefreshOutcome::Refreshed(account) = outcome else {
        panic!("expected refresh outcome");
    };
    assert_eq!(account.credential_version(), 2);
    let raw: Vec<u8> = sqlx::query_scalar(
        "SELECT credentials_ciphertext FROM weread_accounts WHERE account_id = $1",
    )
    .bind(account_id.as_uuid())
    .fetch_one(&pool)
    .await
    .expect("refreshed ciphertext should be stored");
    assert!(!raw
        .windows("access-v2".len())
        .any(|window| window == b"access-v2"));
    assert!(!raw
        .windows("refresh-v2".len())
        .any(|window| window == b"refresh-v2"));
    let reloaded = repository
        .find(account_id)
        .await
        .expect("reloading account should work")
        .expect("account should exist");
    assert!(reloaded.account().access_expires_at() > Utc::now() + chrono::Duration::minutes(30));
}
