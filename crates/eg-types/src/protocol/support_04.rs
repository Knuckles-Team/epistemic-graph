use super::*;

/// Wire mirror of `eg_epistemic::RankedResult` (EPI-P3-3) — one ranked candidate:
/// the final blended score plus its components, kept separate for explainability.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RankedResultWire {
    pub id: String,
    pub score: f64,
    pub similarity: f64,
    pub evidence_quality: f64,
}

/// Materialized result of a `Method::RankByProvenance` run, highest score first.
/// Returned via `ResultPayload::raw`.
#[cfg(feature = "epistemic")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankByProvenanceResult {
    pub ranked: Vec<RankedResultWire>,
}

/// Graph type for multi-tenant registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GraphType {
    Agent,
    Team,
    Global,
    Commons,
}

/// Channel type for dynamic communication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ChannelType {
    /// 1:1 direct messaging between two agents.
    PeerToPeer,
    /// Many-to-many group channel.
    Group,
}

// ── Response ────────────────────────────────────────────────────────────

/// Untagged result payload for efficient serialization without JSON overhead.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResultPayload {
    Bool(bool),
    Count(u64),
    Float(f64),
    String(String),
    Ids(Vec<String>),
    NodeList(Vec<(String, serde_json::Value)>),
    EdgeList(Vec<(String, String, Vec<u8>)>),
    /// A typed result serialized STRAIGHT to MessagePack (Phase C-D — compact
    /// result encoding). Skips building a `serde_json::Value` tree on the server —
    /// the dominant allocator for large algorithm results (PageRank/centrality/
    /// communities over the whole graph). On the wire it is a MessagePack `bin`.
    /// The Python client decodes any
    /// top-level `bytes` result with a second `unpackb`, recovering the exact same
    /// structure the `Json` path produced. Opaque-byte methods are explicitly
    /// identified by method at the client boundary and never decoded a second time.
    Raw(
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        Vec<u8>,
    ),
    Json(serde_json::Value),
}

impl ResultPayload {
    /// Encode a typed value straight to MessagePack as a [`ResultPayload::Raw`],
    /// bypassing the `serde_json::Value` tree (the dominant allocator for large
    /// algorithm results). The compact encoding is the ONE wire contract — clients
    /// decode a top-level `bytes` result with a second `unpackb`; there is no
    /// alternate encoding flag. (Phase C-D)
    pub fn raw<T: Serialize + ?Sized>(value: &T) -> Result<Self, String> {
        rmp_serde::to_vec_named(value)
            .map(ResultPayload::Raw)
            .map_err(|error| format!("result serialization failed: {error}"))
    }
}

/// Input accepted by [`Response::ok`].
///
/// Compact encoding is deliberately fallible: a serializer failure becomes an
/// error response and can never masquerade as a successful empty byte string.
#[doc(hidden)]
pub trait IntoResponsePayload {
    fn into_response_payload(self) -> Result<ResultPayload, String>;
}

impl IntoResponsePayload for ResultPayload {
    fn into_response_payload(self) -> Result<ResultPayload, String> {
        Ok(self)
    }
}

impl IntoResponsePayload for Result<ResultPayload, String> {
    fn into_response_payload(self) -> Result<ResultPayload, String> {
        self
    }
}

/// Response envelope sent back to the Python client.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// Correlation ID matching the request.
    pub id: u64,
    /// Result payload on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<ResultPayload>,
    /// Stable error code on failure; structured detail is carried by OperationResult.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Response {
    /// Create a successful response.
    pub fn ok(id: u64, result: impl IntoResponsePayload) -> Self {
        match result.into_response_payload() {
            Ok(result) => Response {
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => Response::err(id, error),
        }
    }

    /// Create an error response.
    pub fn err(id: u64, error: impl Into<String>) -> Self {
        Response {
            id,
            result: None,
            error: Some(error.into()),
        }
    }

    /// Schema-generated placement redirect used when this node cannot serve the
    /// graph's current `(group, epoch)`.
    pub fn stale_route(
        id: u64,
        graph: &str,
        group: u64,
        epoch: u64,
        leader: Option<u64>,
        _reason: impl Into<String>,
    ) -> Self {
        use crate::epistemic_operations::{
            OperationRedirect, OperationRedirectKind, OperationResult,
            OperationResultSchemaVersion, OperationResultStatus,
        };

        let detail = OperationResult {
            schema_version: OperationResultSchemaVersion::V1,
            operation_id: format!("request:{id}"),
            status: OperationResultStatus::Redirected,
            result_kind: None,
            result_ref: None,
            error: None,
            redirect: Some(OperationRedirect {
                kind: OperationRedirectKind::Placement,
                target_ref: graph.to_string(),
                group,
                epoch,
                fencing_token: group,
                leader_ref: leader.map(|node| format!("node:{node}")),
            }),
        };
        match ResultPayload::raw(&detail) {
            Ok(result) => Response {
                id,
                result: Some(result),
                error: Some("OPERATION_REDIRECTED".to_string()),
            },
            Err(error) => Response::err(id, error),
        }
    }
}
