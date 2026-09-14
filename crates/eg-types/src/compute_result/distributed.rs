//! Result of a cross-shard Pregel/GAS computation (CONCEPT:EG-KG.storage.feature).

use serde::{Deserialize, Serialize};

/// A scored result row `(vertex_id, value)` — PageRank score, or a numeric label for
/// CC (component representative hashed to f64 is lossy, so CC/BFS use the i64 form
/// below). PageRank uses this; CC/BFS use [`LabelRows`].
pub type ScoreRows = Vec<(String, f64)>;
/// A labeled result row `(vertex_id, label)` for CC (component id) / BFS (hop level).
pub type LabelRows = Vec<(String, i64)>;

/// The result of a distributed computation, in a wire-ready form. Serialized to
/// `ResultPayload::Raw` by the handler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DistResult {
    /// PageRank: `(id, score)` rows, sorted by id (deterministic).
    Scores(ScoreRows),
    /// Connected components: `(id, component_repr_index)` rows, sorted by id. The
    /// component label is the index of the component's representative in the sorted
    /// vertex list (a stable small int), so it round-trips losslessly.
    Labels(LabelRows),
}

impl DistResult {
    /// The number of result rows.
    pub fn len(&self) -> usize {
        match self {
            DistResult::Scores(v) => v.len(),
            DistResult::Labels(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
