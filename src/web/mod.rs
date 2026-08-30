//! HTTP and minimal administrative UI boundary.
//!
//! Defines the Axum router, middleware, REST resources, feed delivery,
//! optional media delivery, and server-rendered UI integration. It translates
//! domain errors into HTTP responses but must not contain SQL, browser
//! selectors, or synchronization algorithms. The media route is only needed
//! when optional binary asset archiving is enabled.
//!
//! The public feed route reads persisted XML cache bytes and returns them
//! immediately.
//! Stale-cache requests may enqueue a deduplicated rebuild but never wait for
//! browser work. Feed freshness and rebuild orchestration belong to
//! `application::feed_service`, not Axum handlers.
//!
//! API readiness remains available for cached RSS when browser workers are
//! degraded. Administrative routes are registered only when complete
//! authentication/session configuration is explicitly enabled.

pub mod api;
pub mod auth;
pub mod ui;
