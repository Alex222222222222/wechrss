//! WeRead credential domain model.
//!
//! This module describes access tokens, refresh tokens, device identity,
//! profile metadata, account labels, and credential lifecycle state.
//!
//! Responsibilities: distinguish basic configured credentials from refreshable
//! credentials and document secret-handling invariants.
//!
//! Non-responsibilities: QR polling, credential exchange, encryption key
//! management, database persistence, or exposing login state over HTTP.
//!
//! Security: secret fields must be wrapped in secrecy-aware types, excluded
//! from logs and API serialization, and encrypted before PostgreSQL storage.
