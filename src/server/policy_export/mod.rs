//! CA-16 — row-level-security marking predicate export (feature `policy_export`, off by
//! default).
//!
//! Reserved module for lane CA-16, which projects the engine's existing row-visibility
//! authority (`eg_core::isolation`) into an externally consumable policy predicate.
//! Empty on purpose: this file and its feature declaration were landed by CA-17's single
//! feature-stub commit so that CA-16 edits only this directory and never the root
//! `Cargo.toml` or [`crate::server`]'s module list, which a dozen concurrent eg lanes
//! share.
//!
//! The feature name keeps the underscore of its module path rather than the usual
//! hyphenated Cargo spelling, so feature and module are the same identifier.
