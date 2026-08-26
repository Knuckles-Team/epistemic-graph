//! CA-15 — OpenLineage transport (feature `lineage-transport`, off by default).
//!
//! Reserved module for lane CA-15, which gives the OpenLineage run events already
//! emitted by [`super::lineage`] a real transport to an external collector. Empty on
//! purpose: this file and its feature declaration were landed by CA-17's single
//! feature-stub commit so that CA-15 edits only this file and never the root
//! `Cargo.toml`, which a dozen concurrent eg lanes share.
//!
//! CA-15 owns transport, not event construction: the OpenLineage event model already
//! exists in [`super::lineage`] behind the `lake` feature this one implies.
