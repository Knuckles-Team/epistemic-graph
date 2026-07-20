//! Versioned wire contract for the native governed `KnowledgeBatch` stream.
//!
//! This module deliberately contains only serde data.  The Arrow-backed adapter
//! and query-family runtimes live in `eg-plan` and the facade respectively, while
//! this bottom-of-DAG crate gives every client one stable pull protocol.  A caller
//! sends the same `KnowledgeStreamRequestV1` for the first and subsequent pulls;
//! `cursor = None` opens the deterministic snapshot and a returned cursor resumes
//! it.  There is no server-side cursor identity that can outlive its verified
//! request authority or placement epoch.

use serde::{Deserialize, Serialize};

pub const KNOWLEDGE_STREAM_SCHEMA_VERSION: u16 = 1;

/// The seven result-producing families sharing the native batch currency.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeResultFamily {
    Graph,
    Sql,
    Rdf,
    Vector,
    TimeSeries,
    Job,
    CrossModal,
}

impl KnowledgeResultFamily {
    pub const ALL: [Self; 7] = [
        Self::Graph,
        Self::Sql,
        Self::Rdf,
        Self::Vector,
        Self::TimeSeries,
        Self::Job,
        Self::CrossModal,
    ];
}

/// Result encoding requested from the one native pull method.
///
/// `ArrowIpcV1` is the sole engine-native result contract.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeStreamProjection {
    #[default]
    ArrowIpcV1,
}

/// Typed query submitted to the shared result adapter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case", deny_unknown_fields)]
pub enum KnowledgeStreamQuery {
    Graph {
        /// Empty means every visible node.
        #[serde(default)]
        label: String,
        /// Total source-row cap; zero means no caller cap.
        #[serde(default)]
        limit: usize,
    },
    Sql {
        query: String,
        #[serde(default, with = "serde_bytes")]
        params_msgpack: Vec<u8>,
    },
    Rdf {
        query: String,
        #[serde(default)]
        base_iri: String,
        #[serde(default)]
        type_convention: String,
    },
    Vector {
        #[serde(default)]
        keywords: Vec<String>,
        #[serde(default)]
        query_embedding: Vec<f32>,
        k: usize,
    },
    TimeSeries {
        series_id: String,
        from: i64,
        to: i64,
    },
    Job {
        job_id: String,
    },
    CrossModal {
        /// UQL text parsed into the same plan used by `UnifiedQuery`.
        text: String,
    },
}

impl KnowledgeStreamQuery {
    pub fn family(&self) -> KnowledgeResultFamily {
        match self {
            Self::Graph { .. } => KnowledgeResultFamily::Graph,
            Self::Sql { .. } => KnowledgeResultFamily::Sql,
            Self::Rdf { .. } => KnowledgeResultFamily::Rdf,
            Self::Vector { .. } => KnowledgeResultFamily::Vector,
            Self::TimeSeries { .. } => KnowledgeResultFamily::TimeSeries,
            Self::Job { .. } => KnowledgeResultFamily::Job,
            Self::CrossModal { .. } => KnowledgeResultFamily::CrossModal,
        }
    }
}

/// Authority-, placement-, query-, snapshot-, and schema-bound resume position.
///
/// Every reference is an opaque `eg:<namespace>:<digest>` value.  The facade
/// validates them by constructing `eg_modality::OpaqueRef`s before resuming.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeStreamCursorV1 {
    pub schema_version: u16,
    pub family: KnowledgeResultFamily,
    /// Keyed integrity reference over every other cursor field, including the
    /// position. It is re-derived from verified request authority on resume.
    pub integrity_ref: String,
    pub tenant_ref: String,
    pub access_policy_ref: String,
    pub placement_ref: String,
    pub snapshot_ref: String,
    pub query_ref: String,
    pub derivation_ref: String,
    pub evidence_set_ref: String,
    pub batch_size: u32,
    pub row_offset: u64,
    pub batch_index: u64,
    pub exhausted: bool,
}

/// One pull from the shared served stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeStreamRequestV1 {
    pub schema_version: u16,
    pub query: KnowledgeStreamQuery,
    /// Clamped by the server to its safe bound.
    pub batch_size: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<KnowledgeStreamCursorV1>,
    #[serde(default)]
    pub projection: KnowledgeStreamProjection,
}

/// One bounded native Arrow batch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KnowledgeStreamBatchV1 {
    pub schema_version: u16,
    pub family: KnowledgeResultFamily,
    pub projection: KnowledgeStreamProjection,
    pub cursor: KnowledgeStreamCursorV1,
    /// Arrow IPC stream bytes for `ArrowIpcV1`.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_family_has_one_typed_query_and_shared_wire_shape() {
        let queries = [
            KnowledgeStreamQuery::Graph {
                label: String::new(),
                limit: 0,
            },
            KnowledgeStreamQuery::Sql {
                query: "SELECT 1".to_string(),
                params_msgpack: Vec::new(),
            },
            KnowledgeStreamQuery::Rdf {
                query: "SELECT * WHERE { ?s ?p ?o }".to_string(),
                base_iri: String::new(),
                type_convention: String::new(),
            },
            KnowledgeStreamQuery::Vector {
                keywords: vec!["term".to_string()],
                query_embedding: Vec::new(),
                k: 1,
            },
            KnowledgeStreamQuery::TimeSeries {
                series_id: "series".to_string(),
                from: 0,
                to: 1,
            },
            KnowledgeStreamQuery::Job {
                job_id: "job".to_string(),
            },
            KnowledgeStreamQuery::CrossModal {
                text: "MATCH (n) |> LIMIT 1".to_string(),
            },
        ];
        assert_eq!(
            queries
                .iter()
                .map(KnowledgeStreamQuery::family)
                .collect::<Vec<_>>(),
            KnowledgeResultFamily::ALL
        );
        for query in queries {
            let request = KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query,
                batch_size: 32,
                cursor: None,
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            };
            let encoded = rmp_serde::to_vec_named(&request).unwrap();
            let decoded: KnowledgeStreamRequestV1 = rmp_serde::from_slice(&encoded).unwrap();
            assert_eq!(decoded, request);
        }
    }

    #[test]
    fn native_projection_is_arrow_ipc_v1() {
        let projection = KnowledgeStreamProjection::ArrowIpcV1;
        let encoded = rmp_serde::to_vec_named(&projection).unwrap();
        let decoded: KnowledgeStreamProjection = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded, projection);
        assert_eq!(KNOWLEDGE_STREAM_SCHEMA_VERSION, 1);
    }

    #[test]
    fn retired_projection_and_query_parameter_are_rejected() {
        let projection = rmp_serde::to_vec_named("compatibility_msgpack_v1").unwrap();
        assert!(
            rmp_serde::from_slice::<KnowledgeStreamProjection>(&projection).is_err(),
            "the retired compatibility projection must not decode"
        );

        let query = serde_json::json!({
            "family": "cross_modal",
            "text": "MATCH (n) |> LIMIT 1",
            "reorder_filter_selectivity": 0.5
        });
        assert!(
            serde_json::from_value::<KnowledgeStreamQuery>(query).is_err(),
            "retired query parameters must fail closed"
        );
    }
}
