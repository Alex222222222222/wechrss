//! Domain layer containing storage-independent WechRss business concepts.
//!
//! These modules define the vocabulary shared by application services,
//! acquisition adapters, repositories, and the RSS renderer. They must remain
//! independent of Axum, Fantoccini, SQLx, HTML parsers, and deployment details.
//!
//! The domain owns invariants such as stable `review_id` article identity,
//! explicit job state transitions, bounded retry semantics, and risk-control
//! states that require operator attention. It does not perform I/O or mutate
//! the RSS cache itself.

pub mod article;
pub mod credentials;
pub mod job;
pub mod pacing;
pub mod source;
pub mod sync;
