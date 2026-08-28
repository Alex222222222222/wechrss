//! WeRead credential domain model.
//!
//! This module describes access tokens, refresh tokens, device identity,
//! profile metadata, account labels, and credential lifecycle state.
//!
//! Responsibilities: distinguish basic configured credentials from refreshable
//! credentials, provide a stable non-secret `WeReadAccountId`, and document
//! secret-handling and distributed account-lease invariants.
//!
//! Non-responsibilities: QR polling, credential exchange, encryption key
//! management, database persistence, or exposing login state over HTTP.
//!
//! Security: secret fields must be wrapped in secrecy-aware types, excluded
//! from logs and API serialization, and encrypted before PostgreSQL storage.

//! High availability: authenticated account use is fenced by a durable account
//! lease. The lease stores only account identity and ownership metadata, never
//! access/refresh tokens. Version one may expose one account while retaining the
//! explicit identifier required for cross-replica serialization.

// TODO(design): define stable account identity and account-lease domain values
// before implementing credential persistence or AuthService.
