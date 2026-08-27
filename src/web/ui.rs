//! Minimal administrative UI integration.
//!
//! Describes the future pages or templates for source management, QR login,
//! sync history, archived article inspection, feed-link copying, and operator
//! error states.
//!
//! The UI should call the same application services/API contracts as other
//! clients and must not embed database or browser logic. It must avoid
//! rendering secrets and should display risk-control states as actions for the
//! operator rather than silently retrying them.
