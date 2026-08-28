use chrono::Duration;
use sqlx::PgPool;
use uuid::Uuid;
use wechrss::{
    domain::credentials::WeReadAccountId,
    persistence::repositories::account_lease_repository::{
        AccountLeaseError, AccountLeaseRepository, PostgresAccountLeaseRepository,
    },
};

#[sqlx::test(migrator = "wechrss::persistence::postgres::MIGRATOR")]
async fn postgres_account_lease_serializes_owners_and_supports_fenced_takeover(pool: PgPool) {
    let repository = PostgresAccountLeaseRepository::new(pool.clone());
    let account_id = WeReadAccountId::from_uuid(Uuid::new_v4());

    let first = repository
        .acquire(account_id, "worker-a", Duration::seconds(30))
        .await
        .expect("first acquisition should succeed")
        .expect("first owner should acquire the account");
    assert_eq!(first.account_id(), account_id);
    assert_eq!(first.owner(), "worker-a");
    assert!(first.lease_until() > first.heartbeat_at());

    assert!(repository
        .acquire(account_id, "worker-b", Duration::seconds(30))
        .await
        .expect("live acquisition should be checked")
        .is_none());
    assert!(matches!(
        repository
            .heartbeat(
                account_id,
                "worker-b",
                first.token(),
                Duration::seconds(30),
            )
            .await,
        Err(AccountLeaseError::LeaseLost { account_id: lost }) if lost == account_id
    ));

    let renewed = repository
        .heartbeat(account_id, "worker-a", first.token(), Duration::seconds(45))
        .await
        .expect("current owner should heartbeat");
    assert_eq!(renewed.token(), first.token());
    assert!(renewed.lease_until() > renewed.heartbeat_at());

    sqlx::query(
        "UPDATE account_leases SET lease_until = clock_timestamp() - interval '1 second' WHERE account_id = $1",
    )
    .bind(account_id.as_uuid())
    .execute(&pool)
    .await
    .expect("test should be able to expire the lease");

    assert!(matches!(
        repository
            .release(account_id, "worker-a", renewed.token())
            .await,
        Err(AccountLeaseError::LeaseLost { account_id: lost }) if lost == account_id
    ));
    let takeover = repository
        .acquire(account_id, "worker-b", Duration::seconds(30))
        .await
        .expect("expired acquisition should succeed")
        .expect("expired lease should be taken over");
    assert_eq!(takeover.owner(), "worker-b");
    assert_ne!(takeover.token(), first.token());
    repository
        .release(account_id, "worker-b", takeover.token())
        .await
        .expect("current owner should release");
}
