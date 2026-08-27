//! Upstream acquisition adapters.
//!
//! This subsystem is the only place that knows how WeChat and WeRead are
//! reached. It exposes typed, storage-independent results to application
//! services and hides Fantoccini, WebDriver URLs, browser capabilities, page
//! selectors, and protocol response envelopes.
//!
//! Browser access is private to the application network. Browser failures,
//! verification pages, authentication expiry, and risk-control responses must
//! be mapped to typed errors from the shared error taxonomy.
//!
//! WeRead credentials are scoped to authenticated account and article-list
//! operations. Public article content is fetched by [`article_page`] without
//! credentials, so a content-page fetch must not trigger login or depend on a
//! persisted account session.

pub mod article_page;
pub mod browser_pool;
pub mod identity;
pub mod pacing;
pub mod webdriver;
pub mod weread;
