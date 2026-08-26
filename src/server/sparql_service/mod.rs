//! CA-12 — SPARQL service publication to an external Fuseki endpoint (feature
//! `sparql-fuseki`, off by default).
//!
//! Reserved module for lane CA-12. Empty on purpose: this file and its feature
//! declaration were landed by CA-17's single feature-stub commit so that CA-12 edits only
//! this directory and never the root `Cargo.toml` or [`crate::server`]'s module list,
//! which a dozen concurrent eg lanes share.
//!
//! Distinct from the pre-existing `sparql-service` **feature**, which gates the inbound
//! `SERVICE <ep> { … }` federation client in [`crate::server::sparql_http`]. That feature
//! makes this engine a SPARQL *client* of a remote endpoint; `sparql-fuseki` is about
//! publishing this engine's graphs *to* one.
