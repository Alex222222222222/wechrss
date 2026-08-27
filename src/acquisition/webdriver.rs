//! Fantoccini/WebDriver adapter.
//!
//! Encapsulates connecting to Chromium/ChromeDriver or Firefox/GeckoDriver,
//! creating and closing sessions, navigation, waits, DOM/script evaluation,
//! page-source capture, cookies, and browser-context HTTP requests.
//!
//! The future adapter should expose a small `BrowserSession` interface rather
//! than leaking Fantoccini types into application code. WebDriver is an
//! internal dependency and endpoint; it must not be exposed publicly.
//!
//! Non-responsibilities: article parsing, PostgreSQL, job leases, or login API
//! responses. Browser timeouts and session loss become typed acquisition
//! errors with retry classification.
//!
//! The sidecar image must include `tzdata` and set `TZ` to the configured IANA
//! timezone. Future browser-session construction should verify the browser-
//! visible timezone and fail readiness or the session setup if it disagrees
//! with application configuration.
