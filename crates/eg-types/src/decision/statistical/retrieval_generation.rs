//! Corpus re-embedding as a governed generation swap (EH-397).
//!
//! Stored vectors are never mutated in place, and an embedding space is fixed
//! for the life of a store. A re-embed with a new (fine-tuned) model is
//! therefore a NEW graph generation: AU trains the model under a capacity
//! lease, re-ingests the corpus into a shadow graph whose store declares the
//! new space, and the engine builds that graph's own ANN generation like any
//! other. Before a logical graph may resolve to the shadow, the engine
//! dual-serves the judged retrieval runs of the log against both generations
//! (AU supplies each run's query embedded in BOTH spaces -- the engine has no
//! model) and issues a receipt:
//!
//! * coverage: the shadow embeds at least as many rows as the active one;
//! * quality: a paired sign test of the reciprocal rank of the best cited
//!   unit, shadow against active, over independently judged runs only;
//! * score stability: the PSI of the top-1 similarity distribution. A large
//!   shift means every score threshold calibrated on the old space is void, so
//!   it gates activation (`max_score_psi_q16`).
//!
//! Activation moves the logical graph's pointer to the shadow only with the
//! passing receipt that measured it against the CURRENTLY active generation;
//! rollback returns to the generation before, which is never deleted by it.

use serde::{Deserialize, Serialize};

use super::retrieval::MAX_QUERY_DIMENSIONS;
use crate::contract::BoundedVec;

/// Format identity of a generation receipt.
pub const GENERATION_RECEIPT_SCHEMA_VERSION: u16 = 1;
/// Most judged runs one evaluation replays.
pub const MAX_GENERATION_EVAL_ITEMS: usize = 1_024;
/// Largest top-k a generation evaluation probes.
pub const MAX_GENERATION_TOP_K: u16 = 256;

/// One judged run, its query embedded in both spaces (`q16`, `x / 2^16`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GenerationEvalItem {
    pub record_id: String,
    pub active_q16: BoundedVec<i32, MAX_QUERY_DIMENSIONS>,
    pub shadow_q16: BoundedVec<i32, MAX_QUERY_DIMENSIONS>,
}

/// Evaluate a shadow generation against the active one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GenerationEvalRequest {
    /// The logical graph callers name.
    pub logical: String,
    /// The generation the logical graph resolves to now.
    pub active_graph: String,
    pub shadow_graph: String,
    pub items: BoundedVec<GenerationEvalItem, MAX_GENERATION_EVAL_ITEMS>,
    pub top_k: u16,
    pub min_eval_items: u32,
    /// Largest admissible top-1 score PSI, on `Q16` (0.25 = 16384).
    pub max_score_psi_q16: i64,
}

/// The receipt a generation activation must present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GenerationReceipt {
    pub schema_version: u16,
    pub logical: String,
    pub active_graph: String,
    pub active_space: String,
    pub shadow_graph: String,
    pub shadow_space: String,
    pub active_embedded: u64,
    pub shadow_embedded: u64,
    pub n_eval: u64,
    pub wins: u64,
    pub losses: u64,
    pub ties: u64,
    pub active_mrr_q16: i64,
    pub shadow_mrr_q16: i64,
    pub win_rate_lower_q16: i64,
    /// `None` when this build cannot compute it (no `ann`); then never passed.
    #[serde(default)]
    pub score_psi_q16: Option<i64>,
    pub passed: bool,
}

/// A stored receipt and the digest it is pinned by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GenerationEvaluated {
    pub receipt_digest: String,
    pub receipt: GenerationReceipt,
}
