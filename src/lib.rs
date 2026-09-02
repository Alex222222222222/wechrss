//! Public module map for the Werrss Rust implementation.
//!
//! This crate is being implemented incrementally. The module declarations
//! establish dependency direction; configuration, pacing, durable job and
//! feed-cache/source/article/sync-run persistence slices, and the shared
//! transaction boundary are implemented while the remaining modules document
//! their future contracts.

pub mod acquisition;
pub mod application;
pub mod archive;
pub mod config;
pub mod domain;
pub mod error;
pub mod logging;
pub mod persistence;
pub mod rss;
pub mod web;
