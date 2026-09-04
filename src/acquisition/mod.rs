//! Upstream acquisition adapters.
//!
//! This subsystem is the only place that knows how WeChat and WeRead are
//! reached. It exposes typed, storage-independent results to application
//! services and hides Thirtyfour, WebDriver URLs, browser capabilities, page
//! selectors, and protocol response envelopes.
//!
//! Browser access is private to the application network. Browser failures,
//! verification pages, authentication expiry, and risk-control responses must
//! be mapped to typed errors from the shared error taxonomy.
//!
//! WeRead credentials are scoped to authenticated account, article-list, and
//! content-fallback operations. Public article content is attempted through
//! [`article_page`] without credentials first; a failed public fetch may use
//! the same account session to retrieve WeRead's authenticated MP content.
//! Authenticated and public browser sessions are separate capabilities. Public
//! sessions use clean ephemeral profiles and accept only verified WeChat URLs.
//!
//! The capability ports, process-local browser capacity boundary, public
//! Thirtyfour navigation, identity resolution, common article extraction,
//! bounded public page pacing/scroll execution, optional browser-visible
//! timezone validation, the admin-enrolled cookie-backed WeRead article-list
//! transport, and authenticated request pacing are executable.
//! QR login is implemented through the application login-attempt port;
//! credential refresh remains a separate application service. Browser sidecar health
//! probing is composed by `application::browser_health`. Source creation
//! composes the public identity resolver with the admin API; known book IDs
//! can bypass browser resolution.

pub mod article_page;
pub mod browser_pool;
pub mod identity;
pub mod pacing;
pub mod webdriver;
pub mod weread;
pub mod weread_qr;
