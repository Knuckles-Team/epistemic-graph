//! CA-14 — OpenSearch federation-search adapter (feature `federation-opensearch`, off by
//! default).
//!
//! Reserved module for lane CA-14, which adds an OpenSearch peer adapter to the existing
//! super-cluster federated-search fan-out in [`super`] (feature `federation-search`).
//! Empty on purpose: this file and its feature declaration were landed by CA-17's single
//! feature-stub commit so that CA-14 edits only this file and never the root
//! `Cargo.toml`, which a dozen concurrent eg lanes share.
