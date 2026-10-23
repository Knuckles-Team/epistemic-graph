//! The disclosed natural-language query result on the wire
//! (EG-FEDERATED-QUERY-R058.2.2.1).
//!
//! `Method::NlQueryExplain` answers with the exact UQL text the configured planner
//! produced and the engine executed, alongside the SAME `[node id, score | nil]` rows
//! `Method::NlQuery` returns for that text. A caller can therefore show the generated
//! query next to its rows instead of trusting an opaque natural-language answer.

use serde::{Deserialize, Serialize};

/// The executed UQL and its rows, the served form of `eg_plan::nl::NlQueryResult` in
/// executing mode.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NlQueryExplainResult {
    /// The exact UQL text the planner produced; the engine parsed and ran this text.
    pub uql: String,
    /// The `[node id, score | nil]` rows of that UQL, identical to `NlQuery`'s rows.
    pub rows: Vec<(String, Option<f32>)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-FEDERATED-QUERY-R058.2.2.1
    #[test]
    fn explain_result_encodes_uql_and_rows_by_name() {
        let result = NlQueryExplainResult {
            uql: "MATCH (:Doc) |> LIMIT 2".into(),
            rows: vec![("d1".into(), Some(0.5)), ("d2".into(), None)],
        };
        let bytes = rmp_serde::to_vec_named(&result).unwrap();
        let value: serde_json::Value = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(
            value,
            serde_json::json!({"uql": "MATCH (:Doc) |> LIMIT 2", "rows": [["d1", 0.5], ["d2", null]]})
        );
        let back: NlQueryExplainResult = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back, result);
    }
}
