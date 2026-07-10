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

// ModalityContract retrofit (CONCEPT:E4): `impl ModalityContract for
// BeliefState` — the reference "does everything" implementation (overrides
// provenance/policy_labels/analytics_ops beyond the 4 core methods). Behind the
// crate's own opt-in `contract` feature (default OFF). See module docs.
#[cfg(feature = "contract")]
mod contract;

pub use adapter::BeliefGraph;
pub use model::{
    classify_relationship, AuthorityPolicy, BeliefState, EdgeKind, JustRule, JustificationGraph,
    ProofNode, TimeAxis,
};
pub use propagate::{explain_belief, propagate_confidence};

// X2 — paraconsistent justification-based TMS + Dung-style abstract argumentation
// semantics (grounded/preferred/stable extensions, bipolar "supported attack" closure,
// dependency-directed retraction) over the same `BeliefGraph`. Opt-in and heavier than
// the default confidence-propagation core above — see `tms` module docs.
#[cfg(feature = "epistemic-tms")]
pub use tms::{
    arguments, augmented_attackers, grounded_extension, is_credulously_accepted,
    is_skeptically_accepted, preferred_extensions, retract, stable_extensions,
    RetractionResult, MAX_PREFERRED_ARGUMENTS, MAX_SEARCH_NODES,
};
