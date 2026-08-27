//! WeChat article identity resolution.
//!
//! Converts a pasted `mp.weixin.qq.com` article URL into canonical URL,
//! `biz`, numeric `bid`, `book_id`, account name, and optional title. It handles
//! query-based IDs, rendered page variables, canonical links, and page HTML.
//!
//! Resolution uses the browser abstraction and returns a domain-level result;
//! it does not insert sources or create jobs. Invalid hosts, malformed Base64,
//! missing identity, and verification pages are distinct failures.

//! A successful identity result is later used by `SourceService` to enforce
//! unique source identity and invalidate/rebuild the correct feed cache.
