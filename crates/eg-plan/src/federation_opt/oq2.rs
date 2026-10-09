//! FO-15 target capability rows for the OQ-2 external platforms.
//!
//! These are design targets, not runtime advertisements. A source must have a
//! verified registration probe and a bound driver before its effective
//! `SourceCapabilities` can claim pushdown. Until then the source builder fails
//! closed, including through the optimizer and the naive oracle path.

use eg_types::wire::{ForeignCypherBackend, ForeignSourceSpec};

/// How a source is permitted to participate in a query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Oq2ReadMode {
    Interactive,
    CompletedBatchOnly,
}

/// The FO-15 rows from federation design §3. `true` describes a future
/// provider-side operation, not an operation the current executable can use.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Oq2TargetCapabilities {
    pub read_mode: Oq2ReadMode,
    pub projection: bool,
    pub exact_filters: bool,
    pub limit_offset: bool,
    pub order: bool,
    pub aggregates: bool,
    pub same_source_joins: bool,
    pub batched_key_lookup: bool,
    pub full_fetch: bool,
    pub statistics_probe: bool,
}

const INTERACTIVE: Oq2TargetCapabilities = Oq2TargetCapabilities {
    read_mode: Oq2ReadMode::Interactive,
    projection: true,
    exact_filters: true,
    limit_offset: true,
    order: true,
    aggregates: true,
    same_source_joins: true,
    batched_key_lookup: true,
    full_fetch: true,
    statistics_probe: true,
};

const COMPLETED_BATCH: Oq2TargetCapabilities = Oq2TargetCapabilities {
    read_mode: Oq2ReadMode::CompletedBatchOnly,
    projection: false,
    exact_filters: false,
    limit_offset: false,
    order: false,
    aggregates: false,
    same_source_joins: false,
    batched_key_lookup: false,
    full_fetch: true,
    statistics_probe: false,
};

/// Return the target row for an OQ-2 spec. AGE/Neo4j/FalkorDB share the
/// Cypher design row, while their transports remain explicit in the wire spec.
pub fn target_capabilities(spec: &ForeignSourceSpec) -> Option<Oq2TargetCapabilities> {
    match spec {
        ForeignSourceSpec::Trino { .. } | ForeignSourceSpec::Cypher { .. } => Some(INTERACTIVE),
        ForeignSourceSpec::SparkBatch { .. } => Some(COMPLETED_BATCH),
        _ => None,
    }
}

/// Stable, credential-free kind label for traces and unsupported-driver errors.
pub(crate) fn kind(spec: &ForeignSourceSpec) -> Option<&'static str> {
    match spec {
        ForeignSourceSpec::Trino { .. } => Some("trino"),
        ForeignSourceSpec::Cypher {
            backend: ForeignCypherBackend::Neo4j,
            ..
        } => Some("neo4j"),
        ForeignSourceSpec::Cypher {
            backend: ForeignCypherBackend::Age,
            ..
        } => Some("age"),
        ForeignSourceSpec::Cypher {
            backend: ForeignCypherBackend::FalkorDb,
            ..
        } => Some("falkordb"),
        ForeignSourceSpec::SparkBatch { .. } => Some("spark-batch"),
        ForeignSourceSpec::Api { .. } => Some("api"),
        ForeignSourceSpec::Mcp { .. } => Some("mcp"),
        ForeignSourceSpec::A2a { .. } => Some("a2a"),
        ForeignSourceSpec::GraphQl { .. } => Some("graphql"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sources() -> Vec<(ForeignSourceSpec, &'static str)> {
        let trino = ForeignSourceSpec::Trino {
            endpoint: "https://trino.example.invalid".into(),
            catalog: "lake".into(),
            schema: "public".into(),
            query: "SELECT id FROM objects".into(),
            id_field: "id".into(),
            score_field: None,
        };
        let cypher = |backend| ForeignSourceSpec::Cypher {
            backend,
            endpoint: "localhost:7687".into(),
            graph: "objects".into(),
            query: "MATCH (n) RETURN n.id AS id".into(),
            id_field: "id".into(),
            score_field: None,
        };
        vec![
            (trino, "trino"),
            (cypher(ForeignCypherBackend::Neo4j), "neo4j"),
            (cypher(ForeignCypherBackend::Age), "age"),
            (cypher(ForeignCypherBackend::FalkorDb), "falkordb"),
            (
                ForeignSourceSpec::SparkBatch {
                    artifact_ref: "sealed:fixture:1".into(),
                    id_field: "id".into(),
                    score_field: None,
                },
                "spark-batch",
            ),
        ]
    }

    #[test]
    fn oq2_kind_rows_are_explicit_and_wire_round_trip() {
        for (spec, label) in sources() {
            let wire = rmp_serde::to_vec_named(&spec).unwrap();
            let round_trip: ForeignSourceSpec = rmp_serde::from_slice(&wire).unwrap();
            assert_eq!(round_trip, spec);
            assert_eq!(kind(&spec), Some(label));
            let row = target_capabilities(&spec).unwrap();
            if label == "spark-batch" {
                assert_eq!(row.read_mode, Oq2ReadMode::CompletedBatchOnly);
                assert!(!row.batched_key_lookup);
                assert!(!row.limit_offset);
            } else {
                assert_eq!(row.read_mode, Oq2ReadMode::Interactive);
                assert!(row.projection && row.exact_filters && row.batched_key_lookup);
            }
        }
    }

    #[test]
    fn unbound_oq2_kinds_fail_closed_in_both_execution_paths() {
        use super::super::remote;
        for (spec, label) in sources() {
            let naive_error = crate::federation::source_for(&spec).fetch().unwrap_err();
            assert!(naive_error.contains(label), "{naive_error}");
            let optimized = remote::resolve(&spec, None).unwrap();
            let caps = optimized.capabilities();
            assert_eq!(
                caps,
                super::super::capability::SourceCapabilities::fetch_only()
            );
            let optimized_error = optimized
                .fetch(&super::super::capability::RemoteRequest::full())
                .unwrap_err();
            assert!(optimized_error.contains(label), "{optimized_error}");
            assert!(!naive_error.contains("example.invalid"));
        }
    }
}
