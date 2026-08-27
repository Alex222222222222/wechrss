//! Typed environment-only runtime configuration.
//!
//! This is the second implemented slice of the Rust service. Version one reads
//! configuration from process environment variables only. There is deliberately
//! no application configuration file and no command-line override layer.
//!
//! The raw environment representation is private. Callers receive a validated
//! [`AppConfig`] containing parsed URLs, durations, browser settings, pacing,
//! quiet hours, and secret wrappers. Required connection and encryption values
//! fail startup; optional operational values have documented defaults.
//!
//! Environment names are grouped by concern:
//!
//! ```text
//! DATABASE_URL
//! DATABASE_POOL_MIN_CONNECTIONS / DATABASE_POOL_MAX_CONNECTIONS
//! WEBDRIVER_URL / BROWSER_ENGINE
//! APP_INSTANCE_ID / HTTP_BIND / HTTP_PORT
//! APP_TIMEZONE / QUIET_HOURS_START / QUIET_HOURS_END
//! JOB_POLL_SECONDS / JOB_LEASE_SECONDS / JOB_HEARTBEAT_SECONDS /
//! JOB_MAX_ATTEMPTS
//! RSS_CACHE_TTL_SECONDS
//! PACING_* / SCROLL_*
//! ARCHIVE_BACKEND / ARCHIVE_LOCAL_PATH
//! ADMIN_PASSWORD / CREDENTIAL_ENCRYPTION_KEY
//! ```
//!
//! `APP_INSTANCE_ID` is optional for local use; when omitted, a random UUID is
//! generated for the process so replicas do not share lease ownership. The
//! lease must exceed the heartbeat interval plus the maximum page-operation
//! duration. Pacing delays and page-operation duration are also capped by the
//! loader before they can be converted to runtime durations.
//!
//! PostgreSQL SSL mode, CA certificates, client certificates, private keys,
//! passwords, and other connection options belong in `DATABASE_URL` (including
//! its query parameters) and are passed through to SQLx. This module should not
//! introduce separate PostgreSQL SSL or certificate environment variables.
//!
//! Kubernetes ConfigMaps and Secrets may inject these values, but the
//! application still consumes them through its environment. Diagnostics must
//! expose variable names and validation failures, never secret contents.

use std::{env, str::FromStr, time::Duration};

use chrono::NaiveTime;
use chrono_tz::Tz;
use secrecy::SecretString;
use serde::Deserialize;
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::domain::pacing::{DelayDistribution, PacingError, PacingPolicy, QuietHours};

const MAX_CONFIGURED_DELAY_MS: f64 = 300_000.0;
const MAX_PAGE_OPERATION_SECONDS: u64 = 3_600;

/// Browser implementation selected for a WebDriver sidecar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrowserEngine {
    /// Chromium controlled through ChromeDriver.
    Chromium,
    /// Firefox controlled through GeckoDriver.
    Firefox,
}

impl FromStr for BrowserEngine {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "chromium" | "chrome" => Ok(Self::Chromium),
            "firefox" | "firefox-esr" => Ok(Self::Firefox),
            _ => Err(ConfigError::InvalidValue {
                variable: "BROWSER_ENGINE",
                reason: "expected chromium or firefox",
            }),
        }
    }
}

/// Errors returned while loading or validating environment configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// `envy` could not deserialize an environment value into its raw type.
    #[error("invalid environment configuration: {0}")]
    Environment(#[from] envy::Error),
    /// A required environment variable is absent or empty.
    #[error("environment variable {variable} is required")]
    Missing { variable: &'static str },
    /// An environment variable has a semantically invalid value.
    #[error("environment variable {variable} is invalid: {reason}")]
    InvalidValue {
        variable: &'static str,
        reason: &'static str,
    },
    /// Quiet-hours configuration is only partially supplied.
    #[error("QUIET_HOURS_START and QUIET_HOURS_END must be set together")]
    IncompleteQuietHours,
    /// A pacing or scroll policy failed domain validation.
    #[error("invalid pacing policy: {0}")]
    Pacing(#[from] PacingError),
}

/// Validated configuration used by future runtime components.
#[derive(Debug)]
pub struct AppConfig {
    /// PostgreSQL URL, retained as a secret because it may contain a password.
    pub database_url: SecretString,
    /// Minimum number of PostgreSQL connections kept in the pool.
    pub database_pool_min_connections: u32,
    /// Maximum number of PostgreSQL connections allowed in the pool.
    pub database_pool_max_connections: u32,
    /// Internal WebDriver endpoint.
    pub webdriver_url: Url,
    /// Browser implementation expected behind the WebDriver endpoint.
    pub browser_engine: BrowserEngine,
    /// Stable identity used in PostgreSQL job leases.
    pub instance_id: String,
    /// HTTP bind address or hostname.
    pub http_bind: String,
    /// HTTP listen port.
    pub http_port: u16,
    /// Timezone used by quiet-hours policy and browser setup.
    pub timezone: Tz,
    /// Optional local-time window during which upstream work is paused.
    pub quiet_hours: Option<QuietHours>,
    /// Poll interval for the job enqueue/recovery loops.
    pub job_poll_interval: Duration,
    /// Duration for which a claimed job lease remains valid.
    pub job_lease: Duration,
    /// Maximum interval between worker lease heartbeats.
    pub job_heartbeat: Duration,
    /// Maximum attempts for retryable jobs.
    pub job_max_attempts: u32,
    /// Freshness period for persisted RSS XML.
    pub rss_cache_ttl: Duration,
    /// Shared request/page/scroll pacing policy.
    pub pacing: PacingPolicy,
    /// Archive backend name, currently intended to be `local` or `s3`.
    pub archive_backend: String,
    /// Local archive root used by the local backend.
    pub archive_local_path: String,
    /// Optional administrator password.
    pub admin_password: Option<SecretString>,
    /// Credential-encryption key.
    pub credential_encryption_key: SecretString,
}

impl AppConfig {
    /// Loads, parses, and validates the real process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_env_iter(env::vars())
    }

    /// Loads configuration from key/value pairs.
    ///
    /// This public test seam avoids mutating process-global environment state.
    /// Production callers should normally use [`Self::from_env`].
    pub fn from_env_iter<I>(variables: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let raw: RawConfig = envy::from_iter(variables)?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawConfig) -> Result<Self, ConfigError> {
        let database_url = required(raw.database_url, "DATABASE_URL")?;
        let credential_encryption_key =
            required(raw.credential_encryption_key, "CREDENTIAL_ENCRYPTION_KEY")?;
        let parsed_database_url =
            Url::parse(&database_url).map_err(|_| ConfigError::InvalidValue {
                variable: "DATABASE_URL",
                reason: "expected a valid PostgreSQL URL",
            })?;
        if !matches!(parsed_database_url.scheme(), "postgres" | "postgresql") {
            return Err(ConfigError::InvalidValue {
                variable: "DATABASE_URL",
                reason: "expected a postgres or postgresql URL scheme",
            });
        }

        let database_pool_min_connections = raw.database_pool_min_connections.unwrap_or(1);
        let database_pool_max_connections = raw.database_pool_max_connections.unwrap_or(10);
        if database_pool_max_connections == 0 {
            return Err(ConfigError::InvalidValue {
                variable: "DATABASE_POOL_MAX_CONNECTIONS",
                reason: "must be greater than zero",
            });
        }
        if database_pool_min_connections > database_pool_max_connections {
            return Err(ConfigError::InvalidValue {
                variable: "DATABASE_POOL_MIN_CONNECTIONS",
                reason: "must not exceed DATABASE_POOL_MAX_CONNECTIONS",
            });
        }

        let webdriver_url = raw
            .webdriver_url
            .unwrap_or_else(|| "http://webdriver:4444".to_owned())
            .parse::<Url>()
            .map_err(|_| ConfigError::InvalidValue {
                variable: "WEBDRIVER_URL",
                reason: "expected a valid http or https URL",
            })?;
        if !matches!(webdriver_url.scheme(), "http" | "https") || webdriver_url.host().is_none() {
            return Err(ConfigError::InvalidValue {
                variable: "WEBDRIVER_URL",
                reason: "expected a URL with an http/https scheme and host",
            });
        }

        let timezone_name = raw.app_timezone.unwrap_or_else(|| "UTC".to_owned());
        let timezone = timezone_name
            .parse::<Tz>()
            .map_err(|_| ConfigError::InvalidValue {
                variable: "APP_TIMEZONE",
                reason: "expected an IANA timezone name",
            })?;

        let quiet_hours = match (raw.quiet_hours_start, raw.quiet_hours_end) {
            (None, None) => None,
            (Some(_), None) | (None, Some(_)) => return Err(ConfigError::IncompleteQuietHours),
            (Some(start), Some(end)) => Some(QuietHours::new(
                timezone,
                parse_time(&start, "QUIET_HOURS_START")?,
                parse_time(&end, "QUIET_HOURS_END")?,
            )?),
        };

        let browser_engine = raw
            .browser_engine
            .unwrap_or_else(|| "chromium".to_owned())
            .parse()?;
        let http_port = raw.http_port.unwrap_or(8080);
        if http_port == 0 {
            return Err(ConfigError::InvalidValue {
                variable: "HTTP_PORT",
                reason: "must be greater than zero",
            });
        }

        let job_poll_seconds =
            positive_u64(raw.job_poll_seconds.unwrap_or(30), "JOB_POLL_SECONDS")?;
        let job_lease_seconds =
            positive_u64(raw.job_lease_seconds.unwrap_or(600), "JOB_LEASE_SECONDS")?;
        let job_heartbeat_seconds = positive_u64(
            raw.job_heartbeat_seconds.unwrap_or(60),
            "JOB_HEARTBEAT_SECONDS",
        )?;
        if job_heartbeat_seconds >= job_lease_seconds {
            return Err(ConfigError::InvalidValue {
                variable: "JOB_LEASE_SECONDS",
                reason: "must be greater than JOB_HEARTBEAT_SECONDS",
            });
        }
        let job_max_attempts = raw.job_max_attempts.unwrap_or(3);
        if job_max_attempts == 0 {
            return Err(ConfigError::InvalidValue {
                variable: "JOB_MAX_ATTEMPTS",
                reason: "must be greater than zero",
            });
        }
        let rss_cache_seconds = positive_u64(
            raw.rss_cache_ttl_seconds.unwrap_or(1_800),
            "RSS_CACHE_TTL_SECONDS",
        )?;

        let scroll_max_operation_seconds = raw.scroll_max_operation_seconds.unwrap_or(30);
        if scroll_max_operation_seconds > MAX_PAGE_OPERATION_SECONDS {
            return Err(ConfigError::InvalidValue {
                variable: "SCROLL_MAX_OPERATION_SECONDS",
                reason: "exceeds the maximum supported page-operation duration",
            });
        }

        let required_lease_seconds = job_heartbeat_seconds
            .checked_add(scroll_max_operation_seconds)
            .ok_or(ConfigError::InvalidValue {
                variable: "JOB_LEASE_SECONDS",
                reason: "is too large to validate against worker timing",
            })?;
        if job_lease_seconds <= required_lease_seconds {
            return Err(ConfigError::InvalidValue {
                variable: "JOB_LEASE_SECONDS",
                reason: "must exceed heartbeat interval plus maximum page-operation duration",
            });
        }

        let pacing = PacingPolicy::new(
            distribution(
                raw.pacing_request_mean_ms,
                raw.pacing_request_stddev_ms,
                raw.pacing_request_min_ms,
                raw.pacing_request_max_ms,
                (2_000.0, 250.0, 1_000.0, 4_000.0),
            )?,
            distribution(
                raw.pacing_page_navigation_mean_ms,
                raw.pacing_page_navigation_stddev_ms,
                raw.pacing_page_navigation_min_ms,
                raw.pacing_page_navigation_max_ms,
                (3_000.0, 500.0, 1_500.0, 7_000.0),
            )?,
            distribution(
                raw.pacing_page_action_mean_ms,
                raw.pacing_page_action_stddev_ms,
                raw.pacing_page_action_min_ms,
                raw.pacing_page_action_max_ms,
                (1_000.0, 200.0, 500.0, 3_000.0),
            )?,
            distribution(
                raw.pacing_scroll_settle_mean_ms,
                raw.pacing_scroll_settle_stddev_ms,
                raw.pacing_scroll_settle_min_ms,
                raw.pacing_scroll_settle_max_ms,
                (1_000.0, 200.0, 500.0, 3_000.0),
            )?,
            raw.scroll_max_steps.unwrap_or(4),
            raw.scroll_max_pixels.unwrap_or(4_000),
            Duration::from_secs(scroll_max_operation_seconds),
        )?;

        let http_bind = raw.http_bind.unwrap_or_else(|| "0.0.0.0".to_owned());
        if http_bind.trim().is_empty() {
            return Err(ConfigError::InvalidValue {
                variable: "HTTP_BIND",
                reason: "must not be empty",
            });
        }

        let archive_backend = raw.archive_backend.unwrap_or_else(|| "local".to_owned());
        if !matches!(archive_backend.as_str(), "local" | "s3") {
            return Err(ConfigError::InvalidValue {
                variable: "ARCHIVE_BACKEND",
                reason: "expected local or s3",
            });
        }
        let archive_local_path = raw
            .archive_local_path
            .unwrap_or_else(|| "./data/archive".to_owned())
            .trim()
            .to_owned();
        if archive_backend == "local" && archive_local_path.is_empty() {
            return Err(ConfigError::InvalidValue {
                variable: "ARCHIVE_LOCAL_PATH",
                reason: "must not be empty when ARCHIVE_BACKEND is local",
            });
        }

        Ok(Self {
            database_url: SecretString::new(database_url.into_boxed_str()),
            database_pool_min_connections,
            database_pool_max_connections,
            webdriver_url,
            browser_engine,
            instance_id: raw
                .app_instance_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("wechrss-{}", Uuid::new_v4())),
            http_bind,
            http_port,
            timezone,
            quiet_hours,
            job_poll_interval: Duration::from_secs(job_poll_seconds),
            job_lease: Duration::from_secs(job_lease_seconds),
            job_heartbeat: Duration::from_secs(job_heartbeat_seconds),
            job_max_attempts,
            rss_cache_ttl: Duration::from_secs(rss_cache_seconds),
            pacing,
            archive_backend,
            archive_local_path,
            admin_password: raw
                .admin_password
                .filter(|value| !value.is_empty())
                .map(|value| SecretString::new(value.into_boxed_str())),
            credential_encryption_key: SecretString::new(
                credential_encryption_key.into_boxed_str(),
            ),
        })
    }
}

#[derive(Debug, Deserialize, Default)]
struct RawConfig {
    database_url: Option<String>,
    database_pool_min_connections: Option<u32>,
    database_pool_max_connections: Option<u32>,
    webdriver_url: Option<String>,
    browser_engine: Option<String>,
    app_instance_id: Option<String>,
    http_bind: Option<String>,
    http_port: Option<u16>,
    app_timezone: Option<String>,
    quiet_hours_start: Option<String>,
    quiet_hours_end: Option<String>,
    job_poll_seconds: Option<u64>,
    job_lease_seconds: Option<u64>,
    job_heartbeat_seconds: Option<u64>,
    job_max_attempts: Option<u32>,
    rss_cache_ttl_seconds: Option<u64>,
    pacing_request_mean_ms: Option<f64>,
    pacing_request_stddev_ms: Option<f64>,
    pacing_request_min_ms: Option<f64>,
    pacing_request_max_ms: Option<f64>,
    pacing_page_navigation_mean_ms: Option<f64>,
    pacing_page_navigation_stddev_ms: Option<f64>,
    pacing_page_navigation_min_ms: Option<f64>,
    pacing_page_navigation_max_ms: Option<f64>,
    pacing_page_action_mean_ms: Option<f64>,
    pacing_page_action_stddev_ms: Option<f64>,
    pacing_page_action_min_ms: Option<f64>,
    pacing_page_action_max_ms: Option<f64>,
    pacing_scroll_settle_mean_ms: Option<f64>,
    pacing_scroll_settle_stddev_ms: Option<f64>,
    pacing_scroll_settle_min_ms: Option<f64>,
    pacing_scroll_settle_max_ms: Option<f64>,
    scroll_max_steps: Option<u32>,
    scroll_max_pixels: Option<u32>,
    scroll_max_operation_seconds: Option<u64>,
    archive_backend: Option<String>,
    archive_local_path: Option<String>,
    admin_password: Option<String>,
    credential_encryption_key: Option<String>,
}

fn required(value: Option<String>, variable: &'static str) -> Result<String, ConfigError> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or(ConfigError::Missing { variable })
}

fn parse_time(value: &str, variable: &'static str) -> Result<NaiveTime, ConfigError> {
    NaiveTime::parse_from_str(value.trim(), "%H:%M").map_err(|_| ConfigError::InvalidValue {
        variable,
        reason: "expected HH:MM",
    })
}

fn positive_u64(value: u64, variable: &'static str) -> Result<u64, ConfigError> {
    if value == 0 {
        Err(ConfigError::InvalidValue {
            variable,
            reason: "must be greater than zero",
        })
    } else {
        Ok(value)
    }
}

fn distribution(
    mean_ms: Option<f64>,
    stddev_ms: Option<f64>,
    min_ms: Option<f64>,
    max_ms: Option<f64>,
    defaults: (f64, f64, f64, f64),
) -> Result<DelayDistribution, ConfigError> {
    let mean_ms = bounded_delay(mean_ms, "PACING_DELAY_MEAN_MS")?;
    let stddev_ms = bounded_delay(stddev_ms, "PACING_DELAY_STDDEV_MS")?;
    let min_ms = bounded_delay(min_ms, "PACING_DELAY_MIN_MS")?;
    let max_ms = bounded_delay(max_ms, "PACING_DELAY_MAX_MS")?;
    let (default_mean, default_stddev, default_min, default_max) = defaults;
    DelayDistribution::new(
        mean_ms.unwrap_or(default_mean),
        stddev_ms.unwrap_or(default_stddev),
        min_ms.unwrap_or(default_min),
        max_ms.unwrap_or(default_max),
    )
    .map_err(ConfigError::Pacing)
}

fn bounded_delay(value: Option<f64>, variable: &'static str) -> Result<Option<f64>, ConfigError> {
    let Some(value) = value else {
        return Ok(None);
    };
    if !value.is_finite() || value < 0.0 {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "must be finite and non-negative",
        });
    }
    if value > MAX_CONFIGURED_DELAY_MS {
        return Err(ConfigError::InvalidValue {
            variable,
            reason: "exceeds the maximum supported delay",
        });
    }
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_environment() -> Vec<(String, String)> {
        vec![
            ("DATABASE_URL", "postgres://user:password@db/wechrss"),
            ("CREDENTIAL_ENCRYPTION_KEY", "test-encryption-key"),
            ("WEBDRIVER_URL", "http://webdriver:4444"),
            ("DATABASE_POOL_MIN_CONNECTIONS", "2"),
            ("DATABASE_POOL_MAX_CONNECTIONS", "12"),
            ("APP_TIMEZONE", "Asia/Shanghai"),
            ("QUIET_HOURS_START", "23:00"),
            ("QUIET_HOURS_END", "07:00"),
            ("HTTP_PORT", "8088"),
            ("JOB_LEASE_SECONDS", "120"),
            ("RSS_CACHE_TTL_SECONDS", "1800"),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
    }

    fn replace_environment(
        environment: Vec<(String, String)>,
        variable: &str,
        value: &str,
    ) -> Vec<(String, String)> {
        environment
            .into_iter()
            .map(|(key, current)| {
                if key == variable {
                    (key, value.to_owned())
                } else {
                    (key, current)
                }
            })
            .collect()
    }

    #[test]
    fn loads_defaults_and_explicit_values_from_environment_pairs() {
        let config = AppConfig::from_env_iter(valid_environment()).unwrap();

        assert_eq!(config.browser_engine, BrowserEngine::Chromium);
        assert_eq!(config.database_pool_min_connections, 2);
        assert_eq!(config.database_pool_max_connections, 12);
        assert_eq!(config.http_bind, "0.0.0.0");
        assert_eq!(config.http_port, 8088);
        assert_eq!(config.timezone, chrono_tz::Asia::Shanghai);
        assert_eq!(config.job_lease, Duration::from_secs(120));
        assert_eq!(config.job_heartbeat, Duration::from_secs(60));
        assert_eq!(config.rss_cache_ttl, Duration::from_secs(1_800));
        assert!(config.quiet_hours.unwrap().is_quiet_at(
            "2026-08-27T15:00:00Z"
                .parse::<chrono::DateTime<chrono::Utc>>()
                .unwrap()
        ));
    }

    #[test]
    fn does_not_require_quiet_hours_when_both_values_are_absent() {
        let environment = valid_environment()
            .into_iter()
            .filter(|(key, _)| key != "QUIET_HOURS_START" && key != "QUIET_HOURS_END")
            .collect::<Vec<_>>();

        assert!(AppConfig::from_env_iter(environment)
            .unwrap()
            .quiet_hours
            .is_none());
    }

    #[test]
    fn rejects_missing_required_secrets() {
        let environment = valid_environment()
            .into_iter()
            .filter(|(key, _)| key != "CREDENTIAL_ENCRYPTION_KEY")
            .collect::<Vec<_>>();

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::Missing {
                variable: "CREDENTIAL_ENCRYPTION_KEY"
            })
        ));
    }

    #[test]
    fn rejects_partial_quiet_hours() {
        let environment = valid_environment()
            .into_iter()
            .filter(|(key, _)| key != "QUIET_HOURS_END")
            .collect::<Vec<_>>();

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::IncompleteQuietHours)
        ));
    }

    #[test]
    fn rejects_invalid_timezone_and_webdriver_url() {
        let environment = valid_environment()
            .into_iter()
            .map(|(key, value)| {
                if key == "APP_TIMEZONE" {
                    (key, "Not/A-Timezone".to_owned())
                } else {
                    (key, value)
                }
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "APP_TIMEZONE",
                ..
            })
        ));

        let environment = valid_environment()
            .into_iter()
            .map(|(key, value)| {
                if key == "WEBDRIVER_URL" {
                    (key, "ftp://webdriver:4444".to_owned())
                } else {
                    (key, value)
                }
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "WEBDRIVER_URL",
                ..
            })
        ));
    }

    #[test]
    fn rejects_non_postgres_database_urls() {
        let environment = replace_environment(
            valid_environment(),
            "DATABASE_URL",
            "http://db.example/wechrss",
        );

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "DATABASE_URL",
                ..
            })
        ));
    }

    #[test]
    fn rejects_invalid_database_pool_ranges() {
        let environment =
            replace_environment(valid_environment(), "DATABASE_POOL_MIN_CONNECTIONS", "13");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "DATABASE_POOL_MIN_CONNECTIONS",
                ..
            })
        ));

        let environment =
            replace_environment(valid_environment(), "DATABASE_POOL_MAX_CONNECTIONS", "0");
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "DATABASE_POOL_MAX_CONNECTIONS",
                ..
            })
        ));
    }

    #[test]
    fn rejects_empty_local_archive_paths() {
        let mut environment = valid_environment();
        environment.push(("ARCHIVE_LOCAL_PATH".to_owned(), " ".to_owned()));

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "ARCHIVE_LOCAL_PATH",
                ..
            })
        ));
    }

    #[test]
    fn rejects_zero_intervals_and_unknown_browser_engine() {
        let mut environment = valid_environment();
        environment.push(("JOB_POLL_SECONDS".to_owned(), "0".to_owned()));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "JOB_POLL_SECONDS",
                ..
            })
        ));

        let mut environment = valid_environment();
        environment.push(("BROWSER_ENGINE".to_owned(), "webkit".to_owned()));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "BROWSER_ENGINE",
                ..
            })
        ));
    }

    #[test]
    fn generates_distinct_instance_ids_when_not_configured() {
        let first = AppConfig::from_env_iter(valid_environment()).unwrap();
        let second = AppConfig::from_env_iter(valid_environment()).unwrap();

        assert_ne!(first.instance_id, second.instance_id);
        assert!(first.instance_id.starts_with("wechrss-"));
        assert!(second.instance_id.starts_with("wechrss-"));
    }

    #[test]
    fn rejects_a_lease_that_cannot_cover_heartbeat_and_page_operation() {
        let mut environment = replace_environment(valid_environment(), "JOB_LEASE_SECONDS", "40");
        environment.extend([
            ("JOB_HEARTBEAT_SECONDS".to_owned(), "20".to_owned()),
            ("SCROLL_MAX_OPERATION_SECONDS".to_owned(), "20".to_owned()),
        ]);

        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "JOB_LEASE_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn rejects_oversized_delay_and_page_operation_values() {
        let mut environment = valid_environment();
        environment.push((
            "PACING_REQUEST_MEAN_MS".to_owned(),
            "100000000000000000000".to_owned(),
        ));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "PACING_DELAY_MEAN_MS",
                ..
            })
        ));

        let mut environment = valid_environment();
        environment.push(("SCROLL_MAX_OPERATION_SECONDS".to_owned(), "3601".to_owned()));
        assert!(matches!(
            AppConfig::from_env_iter(environment),
            Err(ConfigError::InvalidValue {
                variable: "SCROLL_MAX_OPERATION_SECONDS",
                ..
            })
        ));
    }

    #[test]
    fn accepts_firefox_and_custom_pacing_bounds() {
        let mut environment = valid_environment();
        environment.extend([
            ("BROWSER_ENGINE".to_owned(), "firefox".to_owned()),
            ("PACING_REQUEST_MEAN_MS".to_owned(), "2500".to_owned()),
            ("PACING_REQUEST_STDDEV_MS".to_owned(), "0".to_owned()),
            ("PACING_REQUEST_MIN_MS".to_owned(), "2000".to_owned()),
            ("PACING_REQUEST_MAX_MS".to_owned(), "3000".to_owned()),
            ("SCROLL_MAX_STEPS".to_owned(), "6".to_owned()),
            ("SCROLL_MAX_PIXELS".to_owned(), "5000".to_owned()),
        ]);

        let config = AppConfig::from_env_iter(environment).unwrap();
        assert_eq!(config.browser_engine, BrowserEngine::Firefox);
        assert_eq!(config.pacing.max_scroll_steps, 6);
        assert_eq!(config.pacing.max_scroll_pixels, 5_000);
        assert_eq!(config.pacing.request.mean_ms, 2_500.0);
    }
}
