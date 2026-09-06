//! Sole physical-state authority for the epistemic-graph workspace.
//!
//! `StorageKernelV1` alone opens and identifies durable stores, owns the closed
//! physical owner-table registry, issues scoped read, snapshot and write
//! capabilities, and validates adoption, backup, restore and recovery. No
//! server handler, domain crate, provider, sidecar, or wrapper may open a
//! database or become a second physical authority.
