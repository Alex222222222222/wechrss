//! Rendered public article extraction.
//!
//! Loads a WeChat article in a browser session and extracts canonical URL,
//! title, author, publication time, summary, body HTML, and referenced assets.
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
//! stores assets, rewrites URLs, and causes the source feed cache to update.
