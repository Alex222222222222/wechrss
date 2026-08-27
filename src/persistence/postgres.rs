//! PostgreSQL pool and transaction policy.
//!
//! Defines the SQLx pool configuration, connectivity checks, embedded schema
//! migrations, isolation expectations, and graceful shutdown behavior.
//!
//! High availability responsibilities include connection-pool sizing,
//! transaction timeouts, row-lock behavior, and safe use of `FOR UPDATE SKIP
//! LOCKED` for jobs. It does not define individual repository queries.
//!
//! Pool construction applies the validated minimum and maximum connection
//! counts from `AppConfig` to SQLx `PoolOptions`. PostgreSQL SSL mode, CA/client
//! certificates, private keys, passwords, and other connection options are not
//! separate application settings: they remain in `DATABASE_URL` and are passed
//! unchanged to SQLx.
//!
//! Readiness should verify PostgreSQL connectivity independently from liveness.
//! Credentials must be encrypted before they reach this layer.

use secrecy::ExposeSecret;
use sqlx::{
    migrate::{MigrateError, Migrator},
    postgres::PgPoolOptions,
    PgPool,
};

use crate::config::AppConfig;

/// Embedded migrations compiled from the repository's `migrations` directory.
///
/// The integration-test harness references this same value so tests exercise
/// the migration set used by application startup.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Builds the SQLx PostgreSQL pool options from validated application settings.
///
/// This function does not open a connection. Keeping option construction
/// separate makes pool sizing and future timeout settings testable without a
/// running PostgreSQL instance. The values come from [`AppConfig`], whose
/// loader has already rejected an unusable range.
pub fn pool_options(config: &AppConfig) -> PgPoolOptions {
    PgPoolOptions::new()
        .min_connections(config.database_pool_min_connections)
        .max_connections(config.database_pool_max_connections)
}

/// Opens the PostgreSQL pool using the configured URL and pool limits.
///
/// SQLx parses the complete URL, including PostgreSQL SSL and certificate
/// query parameters. The URL is never reconstructed or logged here, so those
/// settings reach SQLx unchanged and remain protected by [`SecretString`]'s
/// secret wrapper in [`AppConfig`]. No migrations or readiness checks are
/// performed by this constructor; those belong to application startup and
/// health-check orchestration.
pub async fn connect_pool(config: &AppConfig) -> Result<PgPool, sqlx::Error> {
    pool_options(config)
        .connect(config.database_url.expose_secret())
        .await
}

/// Applies pending checked-in PostgreSQL schema migrations.
///
/// SQLx records applied migration versions and checksums in its
/// `_sqlx_migrations` table, so repeated calls are idempotent and edits to an
/// already-applied migration are rejected. The helper is intentionally explicit
/// rather than called by [`connect_pool`], allowing deployment policy to decide
/// whether migrations run automatically during startup or as a separately
/// authorized release step.
pub async fn migrate(pool: &PgPool) -> Result<(), MigrateError> {
    MIGRATOR.run(pool).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(min_connections: &str, max_connections: &str) -> AppConfig {
        AppConfig::from_env_iter([
            (
                "DATABASE_URL".to_owned(),
                "postgres://user:pass@db/feed".to_owned(),
            ),
            (
                "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
                "test-key".to_owned(),
            ),
            (
                "DATABASE_POOL_MIN_CONNECTIONS".to_owned(),
                min_connections.to_owned(),
            ),
            (
                "DATABASE_POOL_MAX_CONNECTIONS".to_owned(),
                max_connections.to_owned(),
            ),
        ])
        .expect("test configuration should be valid")
    }

    #[test]
    fn applies_configured_pool_bounds() {
        let config = test_config("2", "12");
        let options = pool_options(&config);

        assert_eq!(options.get_min_connections(), 2);
        assert_eq!(options.get_max_connections(), 12);
    }

    #[test]
    fn leaves_database_url_for_sqlx_connection_parsing() {
        let config = AppConfig::from_env_iter([
            (
                "DATABASE_URL".to_owned(),
                "postgresql://user:p%40ss@db/feed?sslmode=require&sslrootcert=%2Fetc%2Fpostgres%2Fca.pem"
                    .to_owned(),
            ),
            (
                "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
                "test-key".to_owned(),
            ),
        ])
        .expect("test configuration should be valid");

        assert_eq!(
            config.database_url.expose_secret(),
            "postgresql://user:p%40ss@db/feed?sslmode=require&sslrootcert=%2Fetc%2Fpostgres%2Fca.pem"
        );
    }
}
