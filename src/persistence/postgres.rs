//! PostgreSQL pool and transaction policy.
//!
//! Defines the future SQLx pool configuration, connectivity checks, migration
//! startup policy, isolation expectations, and graceful shutdown behavior.
//!
//! High availability responsibilities include connection-pool sizing,
//! transaction timeouts, row-lock behavior, and safe use of `FOR UPDATE SKIP
//! LOCKED` for jobs. It does not define individual repository queries.
//!
//! Pool construction will apply the validated minimum and maximum connection
//! counts from `AppConfig` to SQLx `PoolOptions`. PostgreSQL SSL mode, CA/client
//! certificates, private keys, passwords, and other connection options are not
//! separate application settings: they remain in `DATABASE_URL` and are passed
//! unchanged to SQLx.
//!
//! Readiness should verify PostgreSQL connectivity independently from liveness.
//! Credentials must be encrypted before they reach this layer.
