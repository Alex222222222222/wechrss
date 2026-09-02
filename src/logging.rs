//! Process-wide logging initialization.
//!
//! Libraries and application components emit [`tracing`] events. The binary
//! installs this subscriber after configuration has been validated so the
//! configured level controls every runtime role consistently.

use tracing::level_filters::LevelFilter;

/// Installs the human-readable process logger.
///
/// Each line contains a system timestamp, level, event target (normally the
/// Rust module path), and event details. ANSI escapes are disabled because the
/// output is intended for container logs and log aggregation.
pub fn init(level: LevelFilter) {
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(true)
        .with_level(true)
        .with_timer(tracing_subscriber::fmt::time())
        .with_ansi(false)
        .compact()
        .init();
}
