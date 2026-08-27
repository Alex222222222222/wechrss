//! Administrative authentication and session middleware.
//!
//! Defines the future protection for management endpoints, CSRF/session policy
//! for the minimal UI, opaque feed-token boundaries, and safe login-status
//! serialization.
//!
//! This module delegates QR login and credential refresh to `AuthService`. It
//! does not store upstream tokens, perform browser login, or implement
//! password/credential encryption.

//! Public feed access must be intentionally separate from administrative
//! access so RSS readers do not need dashboard credentials.
