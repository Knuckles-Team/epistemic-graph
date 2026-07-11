//! # eg-alignment — the shared alignment graph (CONCEPT:EG-P1-3)
//!
//! First-class document + media runtime modalities need a place where a
//! document span, an image region, an audio interval, a video frame/shot, an
//! extracted entity, a claim, and a source blob can all point at each other —
//! "this claim is supported by this image region, which co-occurs with this
//! document span, which was derived from this source blob." That place is
//! [`graph::AlignmentGraph`]:
//!
//! * [`graph::AlignmentNode`] — `Evidence(eg_modality::EvidenceSpan)` (covers
//!   document/image/audio/video/table/code/trace locations) plus bare
//!   `Entity`/`Claim`/`Blob` id references for the non-artifact-located
//!   endpoints.
//! * [`graph::AlignmentRelation`] / [`graph::AlignmentLink`] — the typed,
//!   directed edges between two nodes (`DerivedFrom`, `CoOccursWith`,
//!   `MentionsEntity`, `SupportsClaim`, `ContradictsClaim`).
//! * [`graph::AlignmentGraph`] — the graph itself: `add_node`/`add_edge`,
//!   `edges_from`/`neighbors`/`path_exists` for traversal, and
//!   `resolve_evidence` to resolve an `Evidence` node through an
//!   [`resolver::EvidenceResolver`].
//! * [`resolver::EvidenceResolver`] — the evidence-resolver seam: resolves an
//!   `EvidenceSpan` back to its [`resolver::ResolvedArtifact`] (a text excerpt or
//!   a blob reference). [`resolver::InMemoryResolver`] is the trivial,
//!   dependency-free implementation this crate ships; a real resolver backed by
//!   the engine's blob CAS + real codecs is a documented follow-up (see module
//!   docs).
//!
//! ## Dependency footprint
//!
//! The default build depends on `eg-modality` ONLY (for `EvidenceSpan`) — no
//! concrete artifact crate (`eg-document`/`eg-image`/`eg-audio`/`eg-video`), and
//! certainly no heavy codec/OCR dependency. The one place this crate exercises a
//! REAL cross-modal alignment end-to-end (a document span -> an image region ->
//! a claim, resolved through `EvidenceResolver`) is `tests/integration.rs`, which
//! pulls the concrete modality crates as dev-dependencies only — this does not
//! change what a consumer of the published `eg-alignment` library links.

pub mod graph;
pub mod resolver;

pub use graph::{AlignmentGraph, AlignmentLink, AlignmentNode, AlignmentNodeId, AlignmentRelation};
pub use resolver::{artifact_id, EvidenceResolver, InMemoryResolver, ResolvedArtifact};
