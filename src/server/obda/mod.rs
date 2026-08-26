//! CA-13 — OBDA wire method and Python client seam (feature `obda-wire`, off by default).
//!
//! Reserved module for lane CA-13, which exposes the existing OBDA engine (feature
//! `obda`, `eg_rdf::obda` + the `Method::SparqlVirtual` handler in
//! [`crate::server::handlers`]) over the wire protocol. Empty on purpose: this file and
//! its feature declaration were landed by CA-17's single feature-stub commit so that
//! CA-13 edits only this directory and never the root `Cargo.toml` or
//! [`crate::server`]'s module list, which a dozen concurrent eg lanes share.
//!
//! CA-13 owns wiring, not reimplementation: the mapping/rewrite engine already exists
//! behind the `obda` feature this one implies.
