//! Werrss process entry point.
//!
//! Startup loads environment configuration, connects to PostgreSQL, applies
//! pending SQLx migrations, and supervises only the selected executable roles.

use werrss::{application::runtime_supervisor::RuntimeSupervisor, config::AppConfig, logging};

#[tokio::main]
async fn main() {
    logging::init_from_env();
    let result = async {
        let config = AppConfig::from_env()?;
        tracing::info!(
            roles = ?config.roles,
            log_level = %config.log_level,
            http_bind = %config.http_bind,
            http_port = config.http_port,
            "starting werrss runtime"
        );
        let supervisor = RuntimeSupervisor::from_config(config).await?;
        supervisor.run_until_signal().await
    }
    .await;

    if let Err(error) = result {
        tracing::error!(error = %error, "werrss runtime terminated with an error");
        std::process::exit(1);
    }
}
