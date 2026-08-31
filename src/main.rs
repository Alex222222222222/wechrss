//! WechRss process entry point.
//!
//! Startup loads environment configuration, connects to PostgreSQL, applies
//! pending SQLx migrations, and supervises only the selected executable roles.

use wechrss::{application::runtime_supervisor::RuntimeSupervisor, config::AppConfig};

#[tokio::main]
async fn main() {
    let result = async {
        let config = AppConfig::from_env()?;
        let supervisor = RuntimeSupervisor::from_config(config).await?;
        supervisor.run_until_signal().await
    }
    .await;

    if let Err(error) = result {
        eprintln!("wech-rss failed to start: {error}");
        std::process::exit(1);
    }
}
