//! Authentication and credential lifecycle use cases.
//!
//! Coordinates QR login state, browser/account session setup, refresh-token
//! renewal, credential encryption, and operator-visible login status.
//! Authenticated login and refresh operations acquire the same distributed
//! account lease used by source synchronization, preventing two replicas from
//! rotating or using one account concurrently.
//!
//! The service must never return access or refresh tokens through API status
//! responses or logs. Refresh is bounded and may be triggered only by a clear
//! authentication-expiry result, never by risk-control responses.
//!
//! Non-responsibilities: generic admin authorization, PostgreSQL SQL, or
//! browser-driver implementation details.

// TODO(design): add stable account identity and account-lease dependencies to
// the future AuthService interface.
