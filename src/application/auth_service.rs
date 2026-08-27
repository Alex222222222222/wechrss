//! Authentication and credential lifecycle use cases.
//!
//! Coordinates QR login state, browser/account session setup, refresh-token
//! renewal, credential encryption, and operator-visible login status.
//!
//! The service must never return access or refresh tokens through API status
//! responses or logs. Refresh is bounded and may be triggered only by a clear
//! authentication-expiry result, never by risk-control responses.
//!
//! Non-responsibilities: generic admin authorization, PostgreSQL SQL, or
//! browser-driver implementation details.
