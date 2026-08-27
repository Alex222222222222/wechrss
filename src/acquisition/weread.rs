//! WeRead account and article-list protocol adapter.
//!
//! Encapsulates the upstream login/session context, QR exchange, refresh-token
//! lifecycle, article-list responses, article-detail URL recovery, and current
//! plus legacy response-shape parsing.
//!
//! The adapter is browser-driven by default in this design: protocol requests
//! requiring browser cookies or session state execute through the browser
//! abstraction. It must expose normalized results and typed errors rather than
//! raw JSON to application services.
//!
//! Authentication expiry allows one refresh/retry at the application layer.
//! Risk-control responses terminate work and are never used to rotate tokens.
//!
//! Every upstream protocol request passes through the shared pacing and quiet-
//! hours gate. This keeps rate policy centralized and makes it impossible for
//! a protocol-specific method to silently use a tighter interval.
