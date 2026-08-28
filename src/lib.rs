//! Public module map for the WechRss Rust implementation.
//!
//! This crate is being implemented incrementally. The module declarations
//! establish dependency direction; configuration, pacing, the first durable
//! job-persistence slice, and the shared job transaction boundary are
//! implemented while the remaining modules document their future contracts.

pub mod acquisition;
pub mod application;
pub mod archive;
pub mod config;
pub mod domain;
pub mod error;
pub mod persistence;
pub mod rss;
pub mod web;
