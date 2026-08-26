//! CA-11 — CDC → Kafka sink (feature `cdc-kafka`, off by default).
//!
//! Reserved module for the Debezium-compatible `ChangeEnvelope` sink that lane CA-11
//! implements on top of the existing engine-side CDC stream ([`crate::server::cdc`],
//! feature `streaming`). Empty on purpose: this file and its feature declaration were
//! landed by CA-17's single feature-stub commit so that CA-11 edits only this directory
//! and never the root `Cargo.toml` or [`crate::server`]'s module list, which a dozen
//! concurrent eg lanes share.
