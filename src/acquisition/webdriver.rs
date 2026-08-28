//! Fantoccini/WebDriver adapter.
//!
//! Encapsulates connecting to Chromium/ChromeDriver or Firefox/GeckoDriver,
//! creating and closing sessions, navigation, waits, DOM/script evaluation,
//! page-source capture, cookies, and browser-context HTTP requests.
//!
//! The future adapter should expose small, capability-specific authenticated
//! and public session interfaces rather than leaking Fantoccini types or one
//! overly powerful `BrowserSession` into application code. WebDriver is an
//! internal dependency and endpoint; it must not be exposed publicly.
//! `PublicBrowserSession` owns a newly created profile/session and has no cookie,
//! storage-import, credential, or account-lease API.
//! `AuthenticatedBrowserSession` carries one account ID and live lease guard; it
//! cannot be converted into the public capability.
//!
//! Non-responsibilities: article parsing, PostgreSQL, job leases, or login API
//! responses. Browser timeouts and session loss become typed acquisition
//! errors with retry classification.
//!
//! The sidecar image must include `tzdata` and set `TZ` to the configured IANA
//! timezone. Future browser-session construction should verify the browser-
//! visible timezone and fail worker readiness or session setup if it disagrees
//! with application configuration. Browser failure alone must not make an API
//! process unable to serve persisted RSS cache bytes.

// TODO(design): implement the non-convertible session capabilities,
// fresh-profile public sessions, redirect inspection, account-guard
// cancellation, and role-aware browser health reporting.
