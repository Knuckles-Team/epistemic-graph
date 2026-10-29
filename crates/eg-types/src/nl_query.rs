//! The disclosed natural-language query result on the wire
//! (MEDULLA-FEDERATED-QUERY-R058.2.2.1).
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

/// The unexecuted natural-language candidate and the pins for the exact
/// UQL/empty-parameter request. A caller may use this receipt to obtain its
/// approved foreign execution and then dispatch that exact UQL via `Method::Uql`;
/// the NL planner is never run again for that execution. Plan-only does not read
/// graph data or establish source authority; those checks remain on the later
/// exact-UQL admission path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NlQueryPlanResult {
    /// The exact UQL produced by the server-owned planner.
    pub uql: String,
    /// SHA-256 of the parsed plan for the exact UQL.
    pub plan_digest: crate::contract::Digest256,
    /// SHA-256 of the exact UQL and typed empty parameter map.
    pub query_digest: crate::contract::Digest256,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-FEDERATED-QUERY-R058.2.2.1
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

    #[test]
    fn plan_result_encodes_exact_uql_and_server_pins() {
        let digest = crate::contract::Digest256::sha256(b"pin");
        let result = NlQueryPlanResult {
            uql: "MATCH (:Doc) |> LIMIT 1".into(),
            plan_digest: digest,
            query_digest: digest,
        };
        let bytes = rmp_serde::to_vec_named(&result).unwrap();
        let back: NlQueryPlanResult = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(back, result);
    }
}
