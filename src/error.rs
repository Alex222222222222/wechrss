//! Error taxonomy shared by the module boundaries.
//!
//! Purpose: provide typed categories for validation, acquisition, browser,
//! authentication, persistence, archive, RSS, job, and web failures.
//!
//! Responsibilities include preserving whether an error is retryable,
//! authentication-expiring, risk-control, deferred, fencing/revision-conflicted,
//! permanent, or operator-actionable.
//! This classification drives job transitions and must not be inferred from
//! arbitrary error strings in application code.
//!
//! Non-responsibilities: this module does not log, retry, update PostgreSQL,
//! or translate errors into HTTP responses. The web boundary will perform the
//! final presentation mapping.
//!
//! Future implementation notes: use `thiserror` for stable boundary errors and
//! `anyhow` only when adding context at orchestration edges.

// TODO(design): define errors for job/account lease loss, non-failure deferral,
// UnitOfWork revision conflict, cache single-flight contention, and disabled
// administration before implementing the corresponding services.
