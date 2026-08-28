//! HTTP and minimal administrative UI boundary.
//!
//! Defines the future Axum router, middleware, REST resources, feed delivery,
//! optional media delivery, and server-rendered UI integration. It translates
//! domain errors into HTTP responses but must not contain SQL, browser
//! selectors, or synchronization algorithms. The media route is only needed
//! when optional binary asset archiving is enabled.
//!
//! Feed requests read persisted XML cache bytes and return them immediately.
//! Stale-cache requests may enqueue a deduplicated rebuild but never wait for
//! browser work.

pub mod api;
pub mod auth;
pub mod ui;
