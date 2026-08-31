//! Side-effectful process supervision for the executable runtime roles.
//!
//! [`super::runtime::RuntimePlan`] validates role selection without opening
//! resources. This module is the next boundary: it opens the shared
//! PostgreSQL pool, builds only the selected adapters, and supervises the API,
//! scheduler, and currently executable feed-rebuild worker loops.
//!
//! Source synchronization, authenticated WeRead work, browser health, and
//! administrative routes are intentionally not constructed here until their
//! handlers and readiness contracts are complete. A worker therefore claims
//! only [`super::runtime::EXECUTABLE_WORKER_JOB_TYPES`].

use std::time::Duration as StdDuration;

use axum::Router;
use chrono::Duration;
use sqlx::PgPool;
use thiserror::Error;
use tokio::{net::TcpListener, sync::watch, task::JoinSet};

use crate::{
    config::{AppConfig, AppRole},
    persistence::{
        postgres::{connect_pool, migrate},
        repositories::{
            article_repository::PostgresArticleRepository,
            feed_cache_repository::{
                PostgresFeedBuildLeaseRepository, PostgresFeedCacheRepository,
            },
            feed_token_repository::PostgresFeedTokenRepository,
            job_repository::PostgresJobRepository,
            scheduler_repository::PostgresSchedulerRepository,
            source_repository::PostgresSourceRepository,
        },
        unit_of_work::UnitOfWorkFactory,
    },
    web::api::feed_router,
};

use super::{
    feed_rebuild_handler::{
        FeedRebuildJobHandler, FeedRebuildJobHandlerConfig, FeedRebuildJobHandlerConfigError,
    },
    feed_rebuild_service::{FeedRebuildConfig, FeedRebuildDependencies, FeedRebuildService},
    feed_service::{
        FeedRebuildJobConfig, FeedService, FeedServiceConfig, PostgresFeedRebuildQueue,
    },
    feed_token_service::FeedTokenService,
    job_service::JobService,
    runtime::{RuntimeComponent, RuntimePlan, RuntimePlanError},
    scheduler::Scheduler,
    worker::Worker,
};

type FeedRebuildWorker = Worker<
    PostgresJobRepository,
    UnitOfWorkFactory,
    FeedRebuildJobHandler<
        PostgresSourceRepository,
        PostgresArticleRepository,
        PostgresFeedBuildLeaseRepository,
        UnitOfWorkFactory,
    >,
>;

/// A runtime supervisor with validated role selection and an open database
/// pool. The pool is shared by every selected role and is never included in
/// debug output or error messages.
pub struct RuntimeSupervisor {
    config: AppConfig,
    plan: RuntimePlan,
    pool: PgPool,
}

impl RuntimeSupervisor {
    /// Opens the configured PostgreSQL pool, applies pending migrations, and
    /// prepares the selected runtime roles.
    pub async fn from_config(config: AppConfig) -> Result<Self, RuntimeSupervisorError> {
        let plan = Self::validated_plan(&config)?;
        let pool = connect_pool(&config)
            .await
            .map_err(RuntimeSupervisorError::DatabaseConnection)?;
        migrate(&pool)
            .await
            .map_err(RuntimeSupervisorError::Migration)?;
        Ok(Self { config, plan, pool })
    }

    /// Creates a supervisor from an already-open pool.
    ///
    /// This constructor is useful for tests and for deployments that run
    /// migrations as a separately authorized step.
    pub fn new(config: AppConfig, pool: PgPool) -> Result<Self, RuntimeSupervisorError> {
        let plan = Self::validated_plan(&config)?;
        Ok(Self { config, plan, pool })
    }

    fn validated_plan(config: &AppConfig) -> Result<RuntimePlan, RuntimeSupervisorError> {
        let plan = RuntimePlan::from_config(config).map_err(RuntimeSupervisorError::Plan)?;
        if plan.component(AppRole::Scheduler).is_some() {
            return Err(RuntimeSupervisorError::SchedulerNotImplemented);
        }
        if matches!(
            plan.component(AppRole::Api),
            Some(RuntimeComponent::Api(api)) if api.admin_enabled()
        ) {
            return Err(RuntimeSupervisorError::AdminRoutesNotImplemented);
        }
        if plan.component(AppRole::Worker).is_some() && config.rss_feed_url.is_none() {
            return Err(RuntimeSupervisorError::FeedUrlNotConfigured);
        }
        Ok(plan)
    }

    /// Returns the validated, side-effect-free role plan used by this
    /// supervisor.
    pub fn plan(&self) -> &RuntimePlan {
        &self.plan
    }

    /// Builds the public feed router when the API role is selected.
    ///
    /// The returned router shares the supervisor's pool but does not bind a
    /// listener. This keeps route integration tests independent of a port and
    /// lets embedders provide their own listener policy.
    pub fn api_router(&self) -> Result<Option<Router>, RuntimeSupervisorError> {
        if !self.config.roles.contains(AppRole::Api) {
            return Ok(None);
        }
        Ok(Some(self.build_api_router()?))
    }

    /// Runs selected roles until the supplied shutdown watch becomes true.
    ///
    /// The supervisor treats an unexpected role-task exit as an error and
    /// requests graceful shutdown from every remaining task. A normal
    /// shutdown waits for all API, scheduler, and worker tasks to finish.
    pub async fn run_until_shutdown(
        self,
        shutdown: watch::Receiver<bool>,
    ) -> Result<(), RuntimeSupervisorError> {
        if *shutdown.borrow() {
            return Ok(());
        }

        let (role_shutdown_tx, role_shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();

        for component in self.plan.components().iter().cloned() {
            match component {
                RuntimeComponent::Api(api) => {
                    let listener = bind_listener(&api).await?;
                    let router = self.build_api_router()?;
                    let role_shutdown = role_shutdown_rx.clone();
                    tasks.spawn(async move {
                        axum::serve(listener, router)
                            .with_graceful_shutdown(wait_for_shutdown(role_shutdown))
                            .await
                            .map_err(RuntimeSupervisorError::HttpServe)
                    });
                }
                RuntimeComponent::Scheduler(scheduler) => {
                    let scheduler_config = scheduler.scheduler_config();
                    let loop_config = scheduler.loop_config();
                    let repository = PostgresSchedulerRepository::new(self.pool.clone());
                    let scheduler =
                        Scheduler::new(repository, scheduler_config, self.config.quiet_hours);
                    let role_shutdown = role_shutdown_rx.clone();
                    tasks.spawn(async move {
                        scheduler
                            .run_until_shutdown(role_shutdown, loop_config)
                            .await;
                        Ok(())
                    });
                }
                RuntimeComponent::Worker(worker) => {
                    for worker_index in 0..worker.concurrency() {
                        let loop_config = worker.loop_config();
                        let worker = self.build_worker(&worker, worker_index)?;
                        let role_shutdown = role_shutdown_rx.clone();
                        tasks.spawn(async move {
                            worker.run_until_shutdown(role_shutdown, loop_config).await;
                            Ok(())
                        });
                    }
                }
            }
        }

        let mut shutdown = shutdown;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = role_shutdown_tx.send(true);
                        drain_tasks(tasks).await?;
                        return Ok(());
                    }
                }
                joined = tasks.join_next() => {
                    let result = match joined {
                        Some(Ok(result)) => result,
                        Some(Err(error)) => {
                            let _ = role_shutdown_tx.send(true);
                            drain_tasks(tasks).await?;
                            return Err(RuntimeSupervisorError::TaskJoin(error));
                        }
                        None => return Err(RuntimeSupervisorError::NoRuntimeTasks),
                    };
                    let error = result.err().unwrap_or(RuntimeSupervisorError::UnexpectedTaskExit);
                    let _ = role_shutdown_tx.send(true);
                    drain_tasks(tasks).await?;
                    return Err(error);
                }
            }
        }
    }

    /// Runs selected roles until the operating system requests shutdown.
    pub async fn run_until_signal(self) -> Result<(), RuntimeSupervisorError> {
        let (shutdown_tx, shutdown) = watch::channel(false);
        let runtime = tokio::spawn(self.run_until_shutdown(shutdown));
        tokio::pin!(runtime);
        let signal = tokio::signal::ctrl_c();
        tokio::pin!(signal);

        tokio::select! {
            result = &mut runtime => result.map_err(RuntimeSupervisorError::TaskJoin)?,
            signal_result = &mut signal => {
                let signal_result = signal_result.map_err(RuntimeSupervisorError::Signal);
                let _ = shutdown_tx.send(true);
                let runtime_result = runtime.await.map_err(RuntimeSupervisorError::TaskJoin)?;
                match signal_result {
                    Ok(()) => runtime_result,
                    Err(error) => Err(error),
                }
            }
        }
    }

    fn build_api_router(&self) -> Result<Router, RuntimeSupervisorError> {
        let stale_window = chrono_duration(
            self.config.rss_stale_while_revalidate,
            "RSS_STALE_WHILE_REVALIDATE_SECONDS",
        )?;
        let miss_retry =
            chrono_duration(self.config.rss_cache_miss_wait, "RSS_CACHE_MISS_WAIT_MS")?;
        let feed_config = FeedServiceConfig::new(stale_window, miss_retry)
            .map_err(RuntimeSupervisorError::FeedServiceConfig)?;
        let queue_config = FeedRebuildJobConfig::new(0, self.config.job_max_attempts)
            .map_err(RuntimeSupervisorError::FeedRebuildJobConfig)?;
        let queue = PostgresFeedRebuildQueue::new(
            PostgresJobRepository::new(self.pool.clone()),
            queue_config,
        );
        let feed_service = FeedService::new(
            PostgresFeedCacheRepository::new(self.pool.clone()),
            queue,
            feed_config,
        );
        Ok(feed_router(
            FeedTokenService::new(PostgresFeedTokenRepository::new(self.pool.clone())),
            feed_service,
        ))
    }

    fn build_worker(
        &self,
        plan: &super::runtime::WorkerRuntimePlan,
        worker_index: u32,
    ) -> Result<FeedRebuildWorker, RuntimeSupervisorError> {
        let lease_for = chrono_duration(self.config.feed_build_lease, "FEED_BUILD_LEASE_SECONDS")?;
        let cache_ttl = chrono_duration(self.config.rss_cache_ttl, "RSS_CACHE_TTL_SECONDS")?;
        let retry_after = chrono_duration(self.config.job_poll_interval, "JOB_POLL_SECONDS")?;
        let rebuild_config = self.feed_rebuild_config(lease_for, cache_ttl)?;
        let owner = format!("{}-feed-worker-{worker_index}", self.config.instance_id);
        let rebuild_service = FeedRebuildService::new(
            FeedRebuildDependencies::new(
                PostgresSourceRepository::new(self.pool.clone()),
                PostgresArticleRepository::new(self.pool.clone()),
                PostgresFeedBuildLeaseRepository::new(self.pool.clone()),
                UnitOfWorkFactory::new(self.pool.clone()),
            ),
            rebuild_config,
            owner,
        )
        .map_err(RuntimeSupervisorError::FeedRebuild)?;
        let handler_config = FeedRebuildJobHandlerConfig::new(retry_after)
            .map_err(RuntimeSupervisorError::FeedRebuildHandlerConfig)?;
        let handler = FeedRebuildJobHandler::new(rebuild_service, handler_config);
        Worker::new(
            JobService::new(
                PostgresJobRepository::new(self.pool.clone()),
                plan.job_service_config().clone(),
            ),
            UnitOfWorkFactory::new(self.pool.clone()),
            handler,
            plan.worker_config().clone(),
        )
        .map_err(RuntimeSupervisorError::WorkerConfig)
    }

    fn feed_rebuild_config(
        &self,
        lease_for: Duration,
        cache_ttl: Duration,
    ) -> Result<FeedRebuildConfig, RuntimeSupervisorError> {
        let feed_url = self
            .config
            .rss_feed_url
            .as_ref()
            .ok_or(RuntimeSupervisorError::FeedUrlNotConfigured)?;
        FeedRebuildConfig::new(
            lease_for,
            cache_ttl,
            feed_url.as_str(),
            "WeChat article feed",
        )
        .map_err(RuntimeSupervisorError::FeedRebuildConfig)
    }
}

async fn bind_listener(
    api: &super::runtime::ApiRuntimePlan,
) -> Result<TcpListener, RuntimeSupervisorError> {
    let address = format!("{}:{}", api.bind(), api.port());
    TcpListener::bind(&address)
        .await
        .map_err(|error| RuntimeSupervisorError::HttpBind { address, error })
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.wait_for(|value| *value).await;
}

async fn drain_tasks(
    mut tasks: JoinSet<Result<(), RuntimeSupervisorError>>,
) -> Result<(), RuntimeSupervisorError> {
    while let Some(result) = tasks.join_next().await {
        result.map_err(RuntimeSupervisorError::TaskJoin)??;
    }
    Ok(())
}

fn chrono_duration(
    value: StdDuration,
    field: &'static str,
) -> Result<Duration, RuntimeSupervisorError> {
    Duration::from_std(value).map_err(|_| RuntimeSupervisorError::InvalidDuration { field })
}

/// Errors raised while constructing or supervising runtime roles.
#[derive(Debug, Error)]
pub enum RuntimeSupervisorError {
    /// Environment configuration could not be loaded.
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
    /// Role configuration was invalid.
    #[error(transparent)]
    Plan(#[from] RuntimePlanError),
    /// Administrative handlers are not yet complete, so startup must fail
    /// rather than silently ignoring an enabled setting.
    #[error("ADMIN_ENABLED is not supported until administrative routes are implemented")]
    AdminRoutesNotImplemented,
    /// The scheduler currently creates source-sync jobs without an executable
    /// source-sync worker handler.
    #[error("scheduler role is not supported until source synchronization is implemented")]
    SchedulerNotImplemented,
    /// Feed workers must not publish a placeholder channel URL.
    #[error("RSS_FEED_URL is required when the worker role is enabled")]
    FeedUrlNotConfigured,
    /// PostgreSQL could not be reached.
    #[error("database connection failed")]
    DatabaseConnection(#[source] sqlx::Error),
    /// A checked-in migration could not be applied.
    #[error("database migration failed")]
    Migration(#[source] sqlx::migrate::MigrateError),
    /// A configured duration could not be represented by chrono.
    #[error("{field} is outside the supported runtime duration range")]
    InvalidDuration { field: &'static str },
    /// Feed delivery timing was invalid after conversion.
    #[error(transparent)]
    FeedServiceConfig(#[from] super::feed_service::FeedServiceConfigError),
    /// Feed rebuild queue settings were invalid.
    #[error(transparent)]
    FeedRebuildJobConfig(#[from] super::feed_service::FeedRebuildJobConfigError),
    /// Feed rebuild settings were invalid after conversion.
    #[error(transparent)]
    FeedRebuildConfig(#[from] super::feed_rebuild_service::FeedRebuildConfigError),
    /// Feed rebuild handler settings were invalid after conversion.
    #[error(transparent)]
    FeedRebuildHandlerConfig(#[from] FeedRebuildJobHandlerConfigError),
    /// Feed rebuild service construction failed.
    #[error(transparent)]
    FeedRebuild(#[from] super::feed_rebuild_service::FeedRebuildError),
    /// Worker construction failed.
    #[error(transparent)]
    WorkerConfig(#[from] super::worker::WorkerConfigError),
    /// The API listener could not bind.
    #[error("HTTP listener could not bind to {address}")]
    HttpBind {
        /// Address selected by the API plan.
        address: String,
        /// Underlying bind failure.
        #[source]
        error: std::io::Error,
    },
    /// The API server stopped because it could not continue serving.
    #[error("HTTP server failed")]
    HttpServe(#[source] std::io::Error),
    /// The operating system signal handler failed.
    #[error("shutdown signal handler failed")]
    Signal(#[source] std::io::Error),
    /// A role task panicked or was cancelled.
    #[error("runtime role task failed to join")]
    TaskJoin(#[source] tokio::task::JoinError),
    /// A role exited before shutdown was requested.
    #[error("runtime role exited unexpectedly")]
    UnexpectedTaskExit,
    /// No role task remained to supervise.
    #[error("runtime started without any role tasks")]
    NoRuntimeTasks,
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use super::*;
    use crate::config::AppConfig;

    fn config(roles: &str) -> AppConfig {
        AppConfig::from_env_iter([
            (
                "DATABASE_URL".to_owned(),
                "postgres://user:pass@db/feed".to_owned(),
            ),
            (
                "CREDENTIAL_ENCRYPTION_KEY".to_owned(),
                "runtime-test-key".to_owned(),
            ),
            ("APP_ROLES".to_owned(), roles.to_owned()),
            (
                "APP_INSTANCE_ID".to_owned(),
                "runtime-supervisor-test".to_owned(),
            ),
            ("WORKER_CONCURRENCY".to_owned(), "2".to_owned()),
            (
                "RSS_FEED_URL".to_owned(),
                "https://feeds.example.test/wechrss.xml".to_owned(),
            ),
        ])
        .expect("test configuration should be valid")
    }

    fn lazy_pool() -> PgPool {
        PgPoolOptions::new()
            .connect_lazy("postgres://unused")
            .expect("lazy test pool URL should be valid")
    }

    #[tokio::test]
    async fn api_router_is_composed_without_opening_an_extra_connection() {
        let supervisor = RuntimeSupervisor::new(config("api"), lazy_pool()).unwrap();

        assert!(supervisor.api_router().unwrap().is_some());
        assert!(supervisor.plan().component(AppRole::Worker).is_none());
    }

    #[tokio::test]
    async fn worker_dependencies_are_constructible_before_the_first_job_exists() {
        let supervisor = RuntimeSupervisor::new(config("worker"), lazy_pool()).unwrap();
        let RuntimeComponent::Worker(plan) = supervisor
            .plan()
            .component(AppRole::Worker)
            .expect("worker plan should exist")
        else {
            panic!("worker role should produce a worker plan")
        };

        supervisor
            .build_worker(plan, 0)
            .expect("feed rebuild worker should be constructible");
    }

    #[tokio::test]
    async fn scheduler_fails_closed_until_source_sync_handler_exists() {
        assert!(matches!(
            RuntimeSupervisor::new(config("scheduler"), lazy_pool()),
            Err(RuntimeSupervisorError::SchedulerNotImplemented)
        ));
        assert!(matches!(
            RuntimeSupervisor::new(config("all"), lazy_pool()),
            Err(RuntimeSupervisorError::SchedulerNotImplemented)
        ));
    }

    #[tokio::test]
    async fn worker_requires_and_uses_the_configured_feed_url() {
        let mut missing_url_config = config("worker");
        missing_url_config.rss_feed_url = None;
        assert!(matches!(
            RuntimeSupervisor::new(missing_url_config, lazy_pool()),
            Err(RuntimeSupervisorError::FeedUrlNotConfigured)
        ));

        let supervisor = RuntimeSupervisor::new(config("worker"), lazy_pool()).unwrap();
        let rebuild_config = supervisor
            .feed_rebuild_config(Duration::minutes(10), Duration::minutes(30))
            .expect("configured feed URL should produce rebuild settings");
        assert_eq!(
            rebuild_config.feed_url(),
            "https://feeds.example.test/wechrss.xml"
        );
    }

    #[tokio::test]
    async fn enabling_unimplemented_administration_fails_closed() {
        let mut config = config("api");
        config.admin_enabled = true;

        assert!(matches!(
            RuntimeSupervisor::new(config, lazy_pool()),
            Err(RuntimeSupervisorError::AdminRoutesNotImplemented)
        ));
    }

    #[tokio::test]
    async fn shutdown_already_requested_does_not_bind_or_touch_the_database() {
        let supervisor = RuntimeSupervisor::new(config("api"), lazy_pool()).unwrap();
        let (_shutdown_tx, shutdown) = watch::channel(true);

        supervisor
            .run_until_shutdown(shutdown)
            .await
            .expect("pre-requested shutdown should be clean");
    }
}
