//! Rendered public article extraction.
//!
//! Loads a public WeChat article in a browser session and extracts canonical
//! URL, title, author, publication time, summary, body HTML, and referenced
//! assets. Public article content does not require WeRead login or any account
//! credentials; callers must not pass credentials into this adapter.
//!
//! This module documents selectors and extraction fallbacks only; it does not
//! sanitize or persist the body. It must distinguish an unavailable article,
//! verification page, malformed content, and ordinary browser/network failure.
//!
//! Page extraction delegates waits and bounded scrolls to
//! `acquisition::pacing` so lazy-loaded content can settle without embedding
//! timing or behavior simulation in selectors.
//!
//! The resulting content is passed to `ArchiveService`, which sanitizes it,
//! optionally stores assets and rewrites URLs, and causes the source feed cache
//! to update.

//! Article-list acquisition and account/session management belong to
//! [`super::weread`]. This module only consumes an article URL and uses an
//! unauthenticated browser context for the public content page.
