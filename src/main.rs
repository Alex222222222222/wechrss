//! Binary entry point reserved for the future WechRss server.
//!
//! The binary is intentionally non-functional in this architecture phase. It
//! will eventually load configuration, construct PostgreSQL repositories and
//! role-appropriate browser adapters, then start only the configured Axum,
//! scheduler, worker, and recovery components. Those actions do not belong in
//! the documentation-only skeleton.

fn main() {
    // TODO(design): construct only configured APP_ROLES. API readiness must not
    // depend on browser health; worker readiness and job claiming may.
    // Implementation gate: do not start runtime composition until target
    // configuration, corrected jobs, and UnitOfWork are executable and tested.
    // Runtime construction will be added in a later implementation phase.
}
