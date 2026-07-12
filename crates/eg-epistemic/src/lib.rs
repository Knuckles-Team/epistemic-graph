//! CONCEPT:EG-KG.epistemic.epistemic-substrate — the engine-native epistemic layer.
//!
//! Turns the graph's stored confidence/bitemporal/provenance fields into a
//! first-class epistemic model: **claims**, **evidence**, **sources**, a computed
//! **belief state**, **contradiction/support/attack** relations, and a
//! **justification** (proof-tree) explanation of *why* a belief holds.
//!
//! **No new persistence.** Claims/Evidence/Sources are ordinary `type`-tagged nodes
//! over the existing [`eg_types::NodeData`] (`confidence`/`valid_from`/`tx_from`);
//! Support/Contradict/Attack are ordinary edges over [`eg_types::EdgeData`]
//! (`relationship_type`/`confidence`/`provenance`) — the same "typed node/edge by
//! convention" pattern mining already uses for `:AssociationRule`. This crate holds
//! only VIEW/compute types over that data — nothing here is a new stored struct.
//!
//! The core operation is [`propagate::propagate_confidence`]: a bounded, cycle-guarded
//! BFS over the support/contradiction/attack topology whose numeric core is the
//! existing conjugate Bayesian update (`eg_compute::probabilistic::bayesian_update`
//! over a `Beta` belief). The derived [`BeliefState::confidence`] is **never** written
//! back onto `NodeData.confidence` (which carries the Ebbinghaus decay) unless a caller
//! runs an explicit, logged "materialize belief" op — so the stored-vs-derived signals
//! never double-count.

mod adapter;
mod model;
mod propagate;
#[cfg(feature = "epistemic-tms")]
mod tms;
// EPI-P3-3 — calibrated probabilistic + causal reasoning: a linear-Gaussian SCM
// supporting genuine do-calculus interventions, Pearl-style point counterfactuals,
// and calibrated intervals. Independent of `epistemic-tms`/`contract` — its own
// opt-in feature (no new dependency, see `src/causal.rs` + the crate `Cargo.toml`).
#[cfg(feature = "epistemic-causal")]
mod causal;
// EPI-P3-3 — provenance-aware retrieval ranking: orders retrieval candidates by
// evidence quality/provenance (reusing `model::Calibration`) in addition to
// similarity. Same feature gate as `causal` (no new dependency either — see
// `src/ranking.rs`).
#[cfg(feature = "epistemic-causal")]
mod ranking;
// EPI-P3-4 — policy-aware proof redaction + selective disclosure over the
// `explain_belief` proof tree, reusing `eg-core`'s per-agent RLS access check. Behind
// its own feature (which pulls in `eg-core/security`) — see `src/redact.rs`.
#[cfg(feature = "epistemic-redaction")]
mod redact;
// X-6 / EPI-P3-2 — the live dependency-driven recompute/truth-maintenance engine
// built on top of the `tms` module above. Same feature gate: it depends directly
// on `tms::retract`/`RetractionResult`.
#[cfg(feature = "epistemic-tms")]
mod recompute;
// EPI-P3-5 — the bitemporal reasoning + why/why-not/what-changed/what-would-invalidate
// query surface: the Phase-3 acceptance query. Composes `propagate`/`tms`/`recompute`;
// depends on `tms::grounded_extension`/`remove_nodes`, same feature gate as those.
#[cfg(feature = "epistemic-tms")]
mod query;

// ModalityContract retrofit (CONCEPT:E4): `impl ModalityContract for
// BeliefState` — the reference "does everything" implementation (overrides
// provenance/policy_labels/analytics_ops beyond the 4 core methods). Behind the
// crate's own opt-in `contract` feature (default OFF). See module docs.
#[cfg(feature = "contract")]
mod contract;
// X-1 (CONCEPT:EG-X1) — the multimodal evidence-graph spine + citation resolver:
// lets an `:Evidence` node carry a located `eg_modality::EvidenceSpan` locus (plus
// its `AssetOccurrence`/`Blob` identity chain) and resolves a claim's evidence
// neighbourhood into a citation list. Behind its own opt-in `evidence-graph`
// feature (default OFF, reuses the SAME optional `eg-modality` dep `contract`
// already pulls — no new dependency). See `src/evidence.rs` module docs.
#[cfg(feature = "evidence-graph")]
mod evidence;

pub use adapter::BeliefGraph;
pub use model::{
    classify_policy_labels, classify_relationship, AuthorityPolicy, BeliefState, BiTemporalRecord,
    Calibration, EdgeKind, JustRule, JustificationGraph, ProofNode, TimeAxis,
};
pub use propagate::{belief_distribution, explain_belief, propagate_confidence};

// EPI-P3-3 — causal reasoning primitives, see `causal` module docs.
#[cfg(feature = "epistemic-causal")]
pub use causal::{CausalEstimate, CausalGraph, StructuralEquation};
// EPI-P3-3 — provenance-aware retrieval ranking, see `ranking` module docs.
#[cfg(feature = "epistemic-causal")]
pub use ranking::{evidence_quality, rank, RankWeights, RankedResult, RetrievalCandidate};

// X2 — paraconsistent justification-based TMS + Dung-style abstract argumentation
// semantics (grounded/preferred/stable extensions, bipolar "supported attack" closure,
// dependency-directed retraction) over the same `BeliefGraph`. Opt-in and heavier than
// the default confidence-propagation core above — see `tms` module docs.
#[cfg(feature = "epistemic-tms")]
pub use tms::{
    arguments, augmented_attackers, grounded_extension, is_credulously_accepted,
    is_skeptically_accepted, preferred_extensions, retract, stable_extensions, RetractionResult,
    MAX_PREFERRED_ARGUMENTS, MAX_SEARCH_NODES,
};

// X-6 / EPI-P3-2 — dependency-driven truth maintenance: track derived
// materializations + their dependency set, invalidate/retract them on a change
// event (base fact deleted/updated, permission/policy change, model/ontology
// retirement) found via the reverse dependency index, recompute without ever
// resting at `Stale`, and wire the TMS's own dependency-directed retraction
// straight into the same index. See `recompute` module docs.
#[cfg(feature = "epistemic-tms")]
pub use recompute::{
    register_from_provenance, ChangeEvent, Materialization, MaterializationStatus, TruthMaintenance,
};

// EPI-P3-4 — policy-aware proof redaction + selective disclosure (see `src/redact.rs`
// module docs for the three disclosure levels and how they reuse `eg-core::isolation`).
#[cfg(feature = "epistemic-redaction")]
pub use redact::{
    explain_belief_redacted, explain_belief_redacted_capped, DisclosureLevel, ExistenceSignal,
    RedactedJustificationGraph, RedactedProofNode,
};

// EPI-P3-5 — bitemporal reasoning + the why/why-not/what-changed/what-would-invalidate
// query family, culminating in `epistemic_status`: the Phase-3 acceptance query "what
// do we believe, why, on exactly which evidence, under whose authority, at what time,
// with what uncertainty, and what would invalidate it", answerable in ONE typed call.
// See `query` module docs.
#[cfg(feature = "epistemic-tms")]
pub use query::{
    epistemic_status, is_believed, what_changed, what_evidence_would_change_this, why, why_not,
    ChangedBelief, EpistemicStatus, MinimalFlipSet, WhyNot, WhyNotReason, BELIEF_THRESHOLD,
    MAX_FLIP_CANDIDATES,
};

// X-1 (CONCEPT:EG-X1) — the multimodal evidence-graph spine + citation resolver,
// see `evidence` module docs.
#[cfg(feature = "evidence-graph")]
pub use evidence::{
    evidence_citations, justification_citations, resolve_locus, EvidenceCitation,
    EvidenceLocusRecord, NODE_TYPE_ASSET_OCCURRENCE, NODE_TYPE_BLOB, NODE_TYPE_SOURCE_OBJECT,
};
