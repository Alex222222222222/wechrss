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
//!
//! Safe configuration rule: administration is disabled unless explicitly
//! enabled. Enabling it requires both an administrator password and independent
//! session-signing key; incomplete settings fail startup. Disabled management,
//! QR-login, and credential mutation routes are not registered, rather than
//! becoming anonymous. Session cookies and CSRF behavior follow the policies in
//! `ARCHITECTURE.md`.

// TODO(design): add admin-enabled route construction, required session signing,
// secure cookie/CSRF middleware, and login rate limiting.
