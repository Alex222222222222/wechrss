//! WeChat article identity resolution.
//!
//! Converts a pasted `mp.weixin.qq.com` article URL into canonical URL,
//! `biz`, numeric `bid`, `book_id`, account name, and optional title. It handles
//! query-based IDs, rendered page variables, canonical links, and page HTML.
//!
//! Resolution uses the browser abstraction and returns a domain-level result;
//! it does not insert sources or create jobs. Invalid hosts, malformed Base64,
//! missing identity, and verification pages are distinct failures.
//! The same host/scheme validation primitive is reused for article URLs returned
//! by upstream listing/detail responses, not only for operator-pasted URLs.

//! A successful identity result is later used by `SourceService` to enforce
//! unique source identity and invalidate/rebuild the correct feed cache.

// The shared `VerifiedWechatArticleUrl` value object currently lives in
// `domain::source` so persistence and acquisition use the same validated
// representation without making the domain depend on browser code. The
// remaining implementation work is URL identity resolution and redirect
// revalidation after every navigation.
