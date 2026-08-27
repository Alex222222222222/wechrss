//! Runtime configuration boundary.
//!
//! Purpose: define the future typed configuration loaded from environment
//! variables. Version one intentionally has no application config file and no
//! command-line configuration layer.
//!
//! Responsibilities include PostgreSQL URLs, WebDriver endpoint and browser
//! mode, job lease/retry timing, RSS cache TTL, archive storage settings,
//! authentication settings, logging, HTTP bind configuration, pacing
//! distribution parameters, scroll limits, and quiet-hours timezone/window.
//!
//! This module must not open connections, start tasks, read credentials from
//! the database, or decide application behavior. It should validate values and
//! provide safe defaults only.
//!
//! High availability: instance identity must be configurable or generated once
//! per process so job leases identify the owning replica. Cache TTL defaults to
//! 30 minutes and must be represented as a typed duration.
//!
//! The upstream timezone is an explicit IANA name, not a numeric offset. All
//! replicas and the browser sidecar must use the same value. Pacing settings
//! should include separate bounded policies for request, page, and scroll
//! operations, with safe upper limits to prevent unbounded job duration.
//!
//! The future loader should read and validate environment variables such as
//! `DATABASE_URL`, `WEBDRIVER_URL`, `APP_INSTANCE_ID`, `APP_TIMEZONE`,
//! `QUIET_HOURS_START`, `QUIET_HOURS_END`, `RSS_CACHE_TTL_SECONDS`, job lease
//! settings, pacing distribution settings, scroll limits, archive storage
//! settings, and HTTP/authentication settings. Kubernetes ConfigMaps and
//! Secrets may inject these values, but the application still receives them
//! only through its environment.
//!
//! Future implementation notes: secrets should remain wrapped in `secrecy`
//! types, missing required values should fail startup, and configuration errors
//! should be reported before server startup. The loader should expose a
//! redacted diagnostic view rather than dumping the process environment.
