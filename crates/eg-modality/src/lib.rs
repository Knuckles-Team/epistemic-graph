//! # eg-modality — the `ModalityContract` seam (CONCEPT:E4)
//!
//! A tiny leaf crate — deps = `eg-types` ONLY, deliberately BELOW `eg-plan`/`eg-core`
//! in the workspace DAG (`eg-types -> eg-core -> eg-compute -> epistemic-graph`, per
//! `AGENTS.md`'s Module Structure section). Every existing modality leaf crate
//! (`eg-tensor`, `eg-geo`, `eg-tsdb`, `eg-rdf`, …) sits at or near the bottom of that
//! DAG too; if `eg-modality` depended on `eg-plan` (to reuse its REAL `RowSet`
//! type), every modality crate implementing `ModalityContract` would transitively
//! pull `eg-plan` -> `eg-core`, INVERTING the DAG for crates that must stay
//! Pi-lean/dependency-free. So this crate defines its OWN small, DAG-safe dup of the
//! shapes it needs (`RowSetShape`, `StagedWrite`) rather than re-exporting the real
//! ones — see `rowset.rs`/`txn.rs` module docs for the full rationale.
//!
//! ## What's here
//!
//! * [`ModalityContract`] — the trait (4 core methods + 4 default-empty methods;
//!   see `contract.rs` module docs for the v1-scoping rationale).
//! * [`RowSetShape`] — the DAG-safe `{id, score}` dup of `eg_plan::rowset::Row`.
//! * [`StagedWrite`]/[`WriteKind`] — the transaction-staging shape, plus
//!   [`encode_staged`]/[`decode_staged`] helpers.
//! * [`Provenance`] — a generalized derivation record, drafted against
//!   `eg-rdf::owl::Justification`.
//! * [`EvidenceSpan`] — the X1 (multimodal-evidence) seam: a located-evidence enum,
//!   default-unused, so E3's `KnowledgeSet::evidence_refs` can later carry it.
//! * [`ConformanceTestable`] + [`modality_conformance_tests!`] — the conformance
//!   harness: implement the trait once per pilot, invoke the macro once, get the
//!   full test battery (round-trip / provenance non-panic / txn-stage-rollback
//!   symmetry / cdc-topic-iff-declared) for free.
//!
//! ## Retrofit status (v1: pilot only, NOT all 19 crates)
//!
//! Implemented (behind each crate's own opt-in `contract` feature, default OFF):
//! `eg-tensor::Tensor`, `eg-geo::Geometry`.
//!
//! **Documented retrofit order for the rest** (see this crate's `README.md` for the
//! full rationale per step): `eg-tsdb`/`eg-stream` next (both already have a
//! staging-shaped concept — `StagedSeries`/the CEP window — that maps directly onto
//! `txn_stage`) -> `eg-rdf` (the reference non-trivial `provenance()`, mapping
//! `owl::Justification`) -> `eg-epistemic` (the reference "does everything"
//! implementation once the shape has proven itself on 4 real modalities) -> the
//! remaining leaf/mid-tier crates (`eg-ann`, `eg-text`, `eg-shacl`, `eg-shex`,
//! `eg-lake`, `eg-kvcache`, …), lowest-friction (pure-serde leaves) first.
//!
//! This crate itself does NOT retrofit anything — it only defines the seam. Adding
//! it to a crate's `Cargo.toml`/feature list and implementing the trait is each
//! pilot's own, separate, additive change (see `eg-tensor`/`eg-geo`'s `contract`
//! feature for the pattern to repeat).

mod contract;
mod evidence;
mod provenance;
mod rowset;
mod txn;

pub use contract::{ConformanceTestable, ModalityContract};
pub use evidence::EvidenceSpan;
pub use provenance::Provenance;
pub use rowset::RowSetShape;
pub use txn::{decode_staged, encode_staged, StagedWrite, WriteKind};
