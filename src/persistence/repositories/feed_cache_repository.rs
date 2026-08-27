//! Persisted RSS feed-cache repository.
//!
//! Stores one rendered XML document per source, its ETag/content hash,
//! generated time, expiry time, and source/article revision. It provides fast
//! reads for RSS clients and atomic cache replacement after a successful fetch.
//!
//! Freshness defaults to 30 minutes. Stale rows remain serveable for
//! stale-while-revalidate behavior; cache misses may be populated by a feed
//! rebuild use case. This repository never contacts WeChat or the browser.
