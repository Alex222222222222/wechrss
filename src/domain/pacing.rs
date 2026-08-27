//! Request-pacing and quiet-hours policy values.
//!
//! This module describes validated, storage-independent policy types for
//! bounded waits, truncated-normal sampling parameters, scroll limits, IANA
//! timezone selection, and local quiet-hour windows.
//!
//! Pacing exists to reduce upstream request pressure and give lazy page content
//! time to settle. It must not be represented as an anti-detection or control-
//! bypass feature. Quiet hours prevent new upstream work during a configured
//! local-time window while allowing RSS reads and cached responses to continue.
//!
//! Responsibilities: validation invariants, deterministic test configuration,
//! and daylight-saving-aware quiet-window semantics. Non-responsibilities:
//! sleeping, random-number generation, browser scrolling, job claiming, or
//! reading environment variables.
