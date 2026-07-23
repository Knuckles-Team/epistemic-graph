//! Shared served `KnowledgeBatch` result plane.
//!
//! `dispatch_graph_op` calls this handler only after graph ACL, lazy-materialization,
//! and authoritative placement checks have succeeded.  Each family then reuses its
//! existing RLS-aware execution path and is projected through exactly one of the
//! seven `eg_plan::result_stream` adapters.  The wire returns one bounded Arrow
//! batch per request; the cursor binds authority, placement, query, complete source
//! snapshot, schema, and batch size, so a changed result or policy fails closed.

use std::sync::Arc;

use eg_modality::{OpaqueRef, ProtocolError};
use eg_plan::{
    cross_modal_result_stream, graph_result_stream, job_result_stream, rdf_result_stream,
    sql_result_stream, time_series_result_stream, vector_result_stream, KnowledgeBatch,
    KnowledgeBatchRow, KnowledgeStreamContext, KnowledgeStreamCursor, ServedResultFamily,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
#[cfg(test)]
use crate::knowledge_stream::KnowledgeStreamProjection;
use crate::knowledge_stream::{
    KnowledgeResultFamily, KnowledgeStreamBatchV1, KnowledgeStreamCursorV1, KnowledgeStreamQuery,
    KnowledgeStreamRequestV1, KNOWLEDGE_STREAM_SCHEMA_VERSION,
};
use crate::protocol::{Method, Response, ResultPayload};
use eg_types::acl::RequestContextClaims;

use super::super::access::{CarrierAuthority, GraphReadAuthority};
use super::super::state::ServerState;

const SCORE_COLUMN: &str = "score";
const MAX_KNOWLEDGE_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAX_KNOWLEDGE_RESULT_ITEMS: usize = 1_000_000;
type HmacSha256 = Hmac<Sha256>;

/// Privacy-safe authority derived only after the request-context MAC, tenant,
/// audience, policy version, scopes, roles, and delegation have been verified.
/// Raw claims never enter the stream cursor or a result row.
#[derive(Clone)]
pub(crate) struct KnowledgeStreamAuthority {
    tenant_ref: OpaqueRef,
    access_policy_ref: OpaqueRef,
    reference_key: [u8; 32],
}

impl KnowledgeStreamAuthority {
    pub(crate) fn from_verified(
        server_secret: &str,
        claims: &RequestContextClaims,
    ) -> Result<Self, String> {
        if server_secret.is_empty()
            || claims.tenant.is_empty()
            || claims.audience.is_empty()
            || claims.policy_version.is_empty()
            || claims.principal.is_empty()
            || claims.agent_id.is_empty()
        {
            return Err("verified KnowledgeStream authority is incomplete".to_string());
        }
        let tenant_ref = keyed_ref(
            server_secret,
            "tenant",
            &[("tenant", claims.tenant.as_str())],
        )?;

        let mut roles = claims.roles.iter().map(String::as_str).collect::<Vec<_>>();
        roles.sort_unstable();
        let mut scopes = claims.scopes.iter().map(String::as_str).collect::<Vec<_>>();
        scopes.sort_unstable();
        let mut policy_values = vec![
            ("tenant", claims.tenant.as_str()),
            ("audience", claims.audience.as_str()),
            ("policy", claims.policy_version.as_str()),
            ("principal", claims.principal.as_str()),
            ("agent", claims.agent_id.as_str()),
        ];
        policy_values.extend(roles.into_iter().map(|value| ("role", value)));
        policy_values.extend(scopes.into_iter().map(|value| ("scope", value)));
        policy_values.extend(
            claims
                .delegation
                .iter()
                .map(String::as_str)
                .map(|value| ("delegation", value)),
        );
        let access_policy_ref = keyed_ref(server_secret, "access-policy", &policy_values)?;
        let reference_key = keyed_bytes(server_secret, "stream-reference-key", &[])?;

        Ok(Self {
            tenant_ref,
            access_policy_ref,
            reference_key,
        })
    }
}

fn keyed_bytes(secret: &str, domain: &str, values: &[(&str, &str)]) -> Result<[u8; 32], String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| "failed to derive KnowledgeStream authority".to_string())?;
    mac.update(domain.as_bytes());
    for (field, value) in values {
        mac.update(&(field.len() as u64).to_be_bytes());
        mac.update(field.as_bytes());
        mac.update(&(value.len() as u64).to_be_bytes());
        mac.update(value.as_bytes());
    }
    Ok(mac.finalize().into_bytes().into())
}

fn keyed_ref(secret: &str, namespace: &str, values: &[(&str, &str)]) -> Result<OpaqueRef, String> {
    OpaqueRef::scoped(
        namespace,
        &hex::encode(keyed_bytes(secret, namespace, values)?),
    )
    .map_err(|_| "failed to derive KnowledgeStream authority".to_string())
}

fn keyed_opaque(
    authority: &KnowledgeStreamAuthority,
    namespace: &str,
    input: &[u8],
) -> Result<OpaqueRef, String> {
    let mut mac = HmacSha256::new_from_slice(&authority.reference_key)
        .map_err(|_| "failed to derive KnowledgeStream reference".to_string())?;
    mac.update(namespace.as_bytes());
    mac.update(&(input.len() as u64).to_be_bytes());
    mac.update(input);
    OpaqueRef::scoped(namespace, &hex::encode(mac.finalize().into_bytes()))
        .map_err(|error: ProtocolError| error.to_string())
}

struct FamilyExecution {
    rows: Vec<KnowledgeBatchRow>,
    source_result: ResultPayload,
}

/// Route the one native stream method. A non-stream method is returned unchanged.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: Arc<GraphCore>,
    method: Method,
    caller: &str,
    carrier: &CarrierAuthority,
    authority: &KnowledgeStreamAuthority,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    read_authority: &GraphReadAuthority,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    let Method::KnowledgeStream { request } = method else {
        return Err(method);
    };
    if request.schema_version != KNOWLEDGE_STREAM_SCHEMA_VERSION {
        return Ok(Response::err(
            req_id,
            "unsupported KnowledgeStream schema version",
        ));
    }
    if request.batch_size == 0 {
        return Ok(Response::err(
            req_id,
            "KnowledgeStream batch_size must be non-zero",
        ));
    }

    let execution = match execute_family(
        state,
        req_id,
        graph_name,
        &core,
        caller,
        carrier,
        placement_epoch,
        fencing_token,
        authority,
        &request.query,
        read_authority,
        #[cfg(feature = "security")]
        rls,
    )
    .await
    {
        Ok(value) => value,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let response = serve_execution(
        graph_name,
        authority,
        placement_epoch,
        fencing_token,
        request,
        execution,
    );
    Ok(match response {
        Ok(batch) => Response::ok(req_id, ResultPayload::raw(&batch)),
        Err(error) => Response::err(req_id, error),
    })
}

async fn execute_family(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    caller: &str,
    carrier: &CarrierAuthority,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    authority: &KnowledgeStreamAuthority,
    query: &KnowledgeStreamQuery,
    read_authority: &GraphReadAuthority,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<FamilyExecution, String> {
    match query {
        KnowledgeStreamQuery::Graph { label, limit } => {
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let mut view = core.analysis_snapshot();
            #[cfg(feature = "security")]
            rls.filter_view(caller, &mut view);
            let mut entries = view.node_properties.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            if !label.is_empty() {
                entries.retain(|(_, properties)| node_has_label(properties.as_slice(), label));
            }
            if *limit != 0 {
                entries.truncate(*limit);
            }
            let source_result = ResultPayload::NodeList(
                entries
                    .iter()
                    .map(|(id, properties)| {
                        let value = eg_types::msgpack::decode_property_value(properties.as_slice())
                            .unwrap_or(serde_json::Value::Null);
                        (id.clone(), value)
                    })
                    .collect(),
            );
            let rows = entries
                .iter()
                .map(|(id, properties)| {
                    let value = eg_types::msgpack::decode_property_value(properties.as_slice())
                        .unwrap_or(serde_json::Value::Null);
                    native_row(
                        authority,
                        "graph_row",
                        id.as_bytes(),
                        normalized_confidence(
                            value.get("confidence").and_then(serde_json::Value::as_f64),
                        ),
                        None,
                    )
                })
                .collect();
            Ok(FamilyExecution {
                rows,
                source_result,
            })
        }
        KnowledgeStreamQuery::Sql {
            query,
            params_msgpack,
        } => {
            if crate::server::access::sql_is_write(query) {
                return Err("KnowledgeStream SQL accepts read-only statements".to_string());
            }
            let response = super::query::try_handle(
                state,
                req_id,
                graph_name,
                core.clone(),
                Method::Sql {
                    query: query.clone(),
                    params_msgpack: params_msgpack.clone(),
                },
                Some(read_authority),
                caller,
                #[cfg(feature = "security")]
                rls,
            )
            .await
            .map_err(|_| "SQL surface unavailable".to_string())?;
            let source_result = response_result(response)?;
            let raw = raw_payload(&source_result, "SQL")?;
            let result: crate::protocol::QueryResult =
                decode_knowledge_result(raw).map_err(|_| "invalid SQL result shape".to_string())?;
            let confidence_index = result.columns.iter().position(|name| name == "confidence");
            let rows = result
                .rows
                .iter()
                .map(|encoded| {
                    let cells: Vec<serde_json::Value> =
                        decode_knowledge_result(encoded).unwrap_or_default();
                    let confidence = normalized_confidence(
                        confidence_index
                            .and_then(|index| cells.get(index))
                            .and_then(serde_json::Value::as_f64),
                    );
                    native_row(authority, "sql_row", encoded, confidence, None)
                })
                .collect();
            Ok(FamilyExecution {
                rows,
                source_result,
            })
        }
        KnowledgeStreamQuery::Rdf {
            query,
            base_iri,
            type_convention,
        } => {
            #[cfg(feature = "sparql")]
            {
                let response = super::rdf::try_handle(
                    state,
                    req_id,
                    graph_name,
                    core.clone(),
                    Method::Sparql {
                        query: query.clone(),
                        base_iri: base_iri.clone(),
                        type_convention: type_convention.clone(),
                    },
                    Some(read_authority),
                    caller,
                    #[cfg(feature = "security")]
                    rls,
                )
                .await
                .map_err(|_| "RDF surface unavailable".to_string())?;
                let source_result = response_result(response)?;
                let raw = raw_payload(&source_result, "RDF")?;
                let result: crate::protocol::SparqlResult = decode_knowledge_result(raw)
                    .map_err(|_| "invalid RDF result shape".to_string())?;
                let rows = result
                    .rows
                    .iter()
                    .map(|binding| {
                        let encoded = rmp_serde::to_vec_named(binding).unwrap_or_default();
                        native_row(authority, "rdf_binding", &encoded, 1.0, None)
                    })
                    .collect();
                Ok(FamilyExecution {
                    rows,
                    source_result,
                })
            }
            #[cfg(not(feature = "sparql"))]
            {
                let _ = (query, base_iri, type_convention, caller);
                Err("RDF KnowledgeStream requires the sparql feature".to_string())
            }
        }
        KnowledgeStreamQuery::Vector {
            keywords,
            query_embedding,
            k,
        } => {
            let response = super::graph_ops::try_handle(
                state,
                req_id,
                Some(caller),
                read_authority,
                core.clone(),
                Method::Discover {
                    keywords: keywords.clone(),
                    query_embedding: query_embedding.clone(),
                    k: *k,
                },
            )
            .await;
            let mut source_result = response_result(response)?;
            let visible = visible_node_ids(
                core,
                caller,
                #[cfg(feature = "security")]
                rls,
            );
            let values = match &mut source_result {
                ResultPayload::Json(serde_json::Value::Array(values)) => values,
                _ => return Err("invalid vector result shape".to_string()),
            };
            values.retain(|value| {
                value
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|id| visible.contains(id))
            });
            let rows = values
                .iter()
                .map(|value| {
                    let id = value
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default();
                    let score = value
                        .get("score")
                        .and_then(serde_json::Value::as_f64)
                        .map(|value| value as f32);
                    native_row(authority, "vector_hit", id.as_bytes(), 1.0, score)
                })
                .collect();
            Ok(FamilyExecution {
                rows,
                source_result,
            })
        }
        KnowledgeStreamQuery::TimeSeries {
            series_id,
            from,
            to,
        } => {
            #[cfg(feature = "tsdb")]
            {
                let response = super::timeseries::try_handle(
                    state,
                    req_id,
                    carrier,
                    graph_name,
                    placement_epoch,
                    fencing_token,
                    Method::TsRange {
                        series_id: series_id.clone(),
                        from: *from,
                        to: *to,
                    },
                )
                .await
                .map_err(|_| "time-series surface unavailable".to_string())?;
                let source_result = response_result(response)?;
                let raw = raw_payload(&source_result, "time-series")?;
                let points: Vec<(i64, Vec<f64>)> = decode_knowledge_result(raw)
                    .map_err(|_| "invalid time-series result shape".to_string())?;
                let rows = points
                    .iter()
                    .map(|(timestamp, values)| {
                        let identity =
                            rmp_serde::to_vec_named(&(series_id, timestamp)).unwrap_or_default();
                        let score = values
                            .first()
                            .copied()
                            .filter(|value| value.is_finite())
                            .map(|value| value as f32);
                        native_row(authority, "time_series_point", &identity, 1.0, score)
                    })
                    .collect();
                Ok(FamilyExecution {
                    rows,
                    source_result,
                })
            }
            #[cfg(not(feature = "tsdb"))]
            {
                let _ = (series_id, from, to, caller, placement_epoch, fencing_token);
                Err("time-series KnowledgeStream requires the tsdb feature".to_string())
            }
        }
        KnowledgeStreamQuery::Job { job_id } => {
            #[cfg(feature = "jobs")]
            {
                let persist_dir = state.read().await.persist_dir.clone();
                let (_job, result) =
                    super::jobs::knowledge_stream_result(&persist_dir, graph_name, job_id)?;
                let rows = result
                    .rows
                    .iter()
                    .map(|row| {
                        let encoded = rmp_serde::to_vec_named(row).unwrap_or_default();
                        let confidence = normalized_confidence(
                            row.get("confidence").and_then(serde_json::Value::as_f64),
                        );
                        let score = row
                            .get("score")
                            .or_else(|| row.get("support"))
                            .and_then(serde_json::Value::as_f64)
                            .map(|value| value as f32);
                        native_row(authority, "job_result", &encoded, confidence, score)
                    })
                    .collect();
                let source_result = ResultPayload::raw(&result);
                Ok(FamilyExecution {
                    rows,
                    source_result,
                })
            }
            #[cfg(not(feature = "jobs"))]
            {
                let _ = job_id;
                Err("job KnowledgeStream requires the jobs feature".to_string())
            }
        }
        KnowledgeStreamQuery::CrossModal { text } => {
            let response = super::query::try_handle(
                state,
                req_id,
                graph_name,
                core.clone(),
                Method::UnifiedQueryText { text: text.clone() },
                Some(read_authority),
                caller,
                #[cfg(feature = "security")]
                rls,
            )
            .await
            .map_err(|_| "cross-modal surface unavailable".to_string())?;
            let source_result = response_result(response)?;
            let raw = raw_payload(&source_result, "cross-modal")?;
            let result: Vec<(String, Option<f32>)> = decode_knowledge_result(raw)
                .map_err(|_| "invalid cross-modal result shape".to_string())?;
            let rows = result
                .iter()
                .map(|(id, score)| {
                    native_row(authority, "cross_modal_row", id.as_bytes(), 1.0, *score)
                })
                .collect();
            Ok(FamilyExecution {
                rows,
                source_result,
            })
        }
    }
}

fn serve_execution(
    graph_name: &str,
    authority: &KnowledgeStreamAuthority,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    request: KnowledgeStreamRequestV1,
    execution: FamilyExecution,
) -> Result<KnowledgeStreamBatchV1, String> {
    let family = request.query.family();
    let query_bytes = rmp_serde::to_vec_named(&request.query)
        .map_err(|_| "KnowledgeStream query encoding failed")?;
    let snapshot_bytes = rmp_serde::to_vec_named(&execution.source_result)
        .map_err(|_| "KnowledgeStream source result encoding failed")?;
    let context = stream_context(
        graph_name,
        authority,
        family,
        placement_epoch,
        fencing_token,
        &query_bytes,
        &snapshot_bytes,
    )?;
    let score_names = vec![SCORE_COLUMN.to_string()];
    let batch_size = usize::try_from(request.batch_size)
        .unwrap_or(65_536)
        .clamp(1, 65_536);
    let mut stream = match family {
        KnowledgeResultFamily::Graph => {
            graph_result_stream(context, score_names.clone(), execution.rows, batch_size)
        }
        KnowledgeResultFamily::Sql => {
            sql_result_stream(context, score_names.clone(), execution.rows, batch_size)
        }
        KnowledgeResultFamily::Rdf => {
            rdf_result_stream(context, score_names.clone(), execution.rows, batch_size)
        }
        KnowledgeResultFamily::Vector => {
            vector_result_stream(context, score_names.clone(), execution.rows, batch_size)
        }
        KnowledgeResultFamily::TimeSeries => {
            time_series_result_stream(context, score_names.clone(), execution.rows, batch_size)
        }
        KnowledgeResultFamily::Job => {
            job_result_stream(context, score_names.clone(), execution.rows, batch_size)
        }
        KnowledgeResultFamily::CrossModal => {
            cross_modal_result_stream(context, score_names.clone(), execution.rows, batch_size)
        }
    }
    .map_err(|error| error.to_string())?;

    if let Some(cursor) = request.cursor.as_ref() {
        let native_cursor = native_cursor(authority, cursor)?;
        stream = stream
            .resume_from(&native_cursor)
            .map_err(|error| error.to_string())?;
    }

    let envelope = stream.next_batch().map_err(|error| error.to_string())?;
    let (cursor, payload) = match envelope {
        Some(envelope) => {
            let payload = envelope
                .batch
                .to_arrow_ipc_stream()
                .map_err(|_| "KnowledgeStream Arrow encoding failed")?;
            (envelope.cursor, payload)
        }
        None => {
            let payload = KnowledgeBatch {
                rows: Vec::new(),
                score_names,
            }
            .to_arrow_ipc_stream()
            .map_err(|_| "KnowledgeStream Arrow encoding failed")?;
            (stream.cursor(), payload)
        }
    };
    Ok(KnowledgeStreamBatchV1 {
        schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
        family,
        projection: request.projection,
        cursor: wire_cursor(authority, &cursor)?,
        payload,
    })
}

fn stream_context(
    graph_name: &str,
    authority: &KnowledgeStreamAuthority,
    family: KnowledgeResultFamily,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    query: &[u8],
    snapshot_result: &[u8],
) -> Result<KnowledgeStreamContext, String> {
    let family_name = family_name(family);
    let placement = rmp_serde::to_vec_named(&(placement_epoch, fencing_token))
        .map_err(|_| "KnowledgeStream placement encoding failed")?;
    Ok(KnowledgeStreamContext {
        tenant_ref: authority.tenant_ref.clone(),
        access_policy_ref: authority.access_policy_ref.clone(),
        placement_ref: keyed_opaque(authority, "placement", &placement)?,
        snapshot_ref: keyed_opaque(
            authority,
            "snapshot",
            &[graph_name.as_bytes(), snapshot_result].concat(),
        )?,
        query_ref: keyed_opaque(authority, "query", query)?,
        derivation_ref: keyed_opaque(
            authority,
            "derivation",
            &[family_name.as_bytes(), env!("CARGO_PKG_VERSION").as_bytes()].concat(),
        )?,
        evidence_set_ref: keyed_opaque(
            authority,
            "evidenceset",
            &[graph_name.as_bytes(), query, snapshot_result].concat(),
        )?,
    })
}

fn native_row(
    authority: &KnowledgeStreamAuthority,
    kind: &str,
    identity: &[u8],
    confidence: f64,
    score: Option<f32>,
) -> KnowledgeBatchRow {
    KnowledgeBatchRow {
        // A cryptographic construction failure yields an invalid empty id; the
        // shared adapter then rejects the row rather than panicking or leaking the
        // unkeyed source identity.
        id: keyed_opaque(authority, "result", identity)
            .map(|reference| reference.as_str().to_string())
            .unwrap_or_default(),
        kind: kind.to_string(),
        scores: vec![(
            SCORE_COLUMN.to_string(),
            score.filter(|value| value.is_finite()),
        )],
        confidence: normalized_confidence(Some(confidence)),
        ..KnowledgeBatchRow::default()
    }
}

fn normalized_confidence(value: Option<f64>) -> f64 {
    value
        .filter(|value| value.is_finite())
        .unwrap_or(1.0)
        .clamp(0.0, 1.0)
}

fn response_result(response: Response) -> Result<ResultPayload, String> {
    if let Some(error) = response.error {
        return Err(error);
    }
    response
        .result
        .ok_or_else(|| "query family returned no result".to_string())
}

fn decode_knowledge_result<T>(bytes: &[u8]) -> Result<T, eg_types::msgpack::MsgpackValidationError>
where
    T: serde::de::DeserializeOwned,
{
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_KNOWLEDGE_RESULT_BYTES,
            MAX_KNOWLEDGE_RESULT_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
}

fn raw_payload<'a>(payload: &'a ResultPayload, family: &str) -> Result<&'a [u8], String> {
    match payload {
        ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes) => Ok(bytes),
        _ => Err(format!("invalid {family} result encoding")),
    }
}

fn node_has_label(properties: &[u8], label: &str) -> bool {
    let Ok(value) = eg_types::msgpack::decode_property_value(properties) else {
        return false;
    };
    ["type", "node_type", "label"]
        .iter()
        .any(|key| value.get(key).and_then(serde_json::Value::as_str) == Some(label))
        || value
            .get("labels")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|labels| labels.iter().any(|value| value.as_str() == Some(label)))
}

fn visible_node_ids(
    core: &Arc<GraphCore>,
    caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> std::collections::HashSet<String> {
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut view = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut view);
    view.node_properties.into_keys().collect()
}

fn native_cursor(
    authority: &KnowledgeStreamAuthority,
    cursor: &KnowledgeStreamCursorV1,
) -> Result<KnowledgeStreamCursor, String> {
    if cursor.schema_version != KNOWLEDGE_STREAM_SCHEMA_VERSION {
        return Err("unsupported KnowledgeStream cursor version".to_string());
    }
    let supplied_integrity = OpaqueRef::new(cursor.integrity_ref.clone())
        .map_err(|_| "invalid KnowledgeStream cursor integrity".to_string())?;
    if supplied_integrity != cursor_integrity(authority, cursor)? {
        return Err("KnowledgeStream cursor integrity mismatch".to_string());
    }
    Ok(KnowledgeStreamCursor {
        family: served_family(cursor.family),
        tenant_ref: OpaqueRef::new(cursor.tenant_ref.clone()).map_err(|e| e.to_string())?,
        access_policy_ref: OpaqueRef::new(cursor.access_policy_ref.clone())
            .map_err(|e| e.to_string())?,
        placement_ref: OpaqueRef::new(cursor.placement_ref.clone()).map_err(|e| e.to_string())?,
        snapshot_ref: OpaqueRef::new(cursor.snapshot_ref.clone()).map_err(|e| e.to_string())?,
        query_ref: OpaqueRef::new(cursor.query_ref.clone()).map_err(|e| e.to_string())?,
        derivation_ref: OpaqueRef::new(cursor.derivation_ref.clone()).map_err(|e| e.to_string())?,
        evidence_set_ref: OpaqueRef::new(cursor.evidence_set_ref.clone())
            .map_err(|e| e.to_string())?,
        batch_size: cursor.batch_size,
        row_offset: cursor.row_offset,
        batch_index: cursor.batch_index,
        exhausted: cursor.exhausted,
    })
}

fn wire_cursor(
    authority: &KnowledgeStreamAuthority,
    cursor: &KnowledgeStreamCursor,
) -> Result<KnowledgeStreamCursorV1, String> {
    let mut wire = KnowledgeStreamCursorV1 {
        schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
        family: wire_family(cursor.family),
        integrity_ref: String::new(),
        tenant_ref: cursor.tenant_ref.as_str().to_string(),
        access_policy_ref: cursor.access_policy_ref.as_str().to_string(),
        placement_ref: cursor.placement_ref.as_str().to_string(),
        snapshot_ref: cursor.snapshot_ref.as_str().to_string(),
        query_ref: cursor.query_ref.as_str().to_string(),
        derivation_ref: cursor.derivation_ref.as_str().to_string(),
        evidence_set_ref: cursor.evidence_set_ref.as_str().to_string(),
        batch_size: cursor.batch_size,
        row_offset: cursor.row_offset,
        batch_index: cursor.batch_index,
        exhausted: cursor.exhausted,
    };
    wire.integrity_ref = cursor_integrity(authority, &wire)?.as_str().to_string();
    Ok(wire)
}

fn cursor_integrity(
    authority: &KnowledgeStreamAuthority,
    cursor: &KnowledgeStreamCursorV1,
) -> Result<OpaqueRef, String> {
    let bytes = rmp_serde::to_vec_named(&(
        cursor.schema_version,
        cursor.family,
        cursor.tenant_ref.as_str(),
        cursor.access_policy_ref.as_str(),
        cursor.placement_ref.as_str(),
        cursor.snapshot_ref.as_str(),
        cursor.query_ref.as_str(),
        cursor.derivation_ref.as_str(),
        cursor.evidence_set_ref.as_str(),
        cursor.batch_size,
        cursor.row_offset,
        cursor.batch_index,
        cursor.exhausted,
    ))
    .map_err(|_| "KnowledgeStream cursor integrity encoding failed")?;
    keyed_opaque(authority, "cursor", &bytes)
}

fn served_family(family: KnowledgeResultFamily) -> ServedResultFamily {
    match family {
        KnowledgeResultFamily::Graph => ServedResultFamily::Graph,
        KnowledgeResultFamily::Sql => ServedResultFamily::Sql,
        KnowledgeResultFamily::Rdf => ServedResultFamily::Rdf,
        KnowledgeResultFamily::Vector => ServedResultFamily::Vector,
        KnowledgeResultFamily::TimeSeries => ServedResultFamily::TimeSeries,
        KnowledgeResultFamily::Job => ServedResultFamily::Job,
        KnowledgeResultFamily::CrossModal => ServedResultFamily::CrossModal,
    }
}

fn wire_family(family: ServedResultFamily) -> KnowledgeResultFamily {
    match family {
        ServedResultFamily::Graph => KnowledgeResultFamily::Graph,
        ServedResultFamily::Sql => KnowledgeResultFamily::Sql,
        ServedResultFamily::Rdf => KnowledgeResultFamily::Rdf,
        ServedResultFamily::Vector => KnowledgeResultFamily::Vector,
        ServedResultFamily::TimeSeries => KnowledgeResultFamily::TimeSeries,
        ServedResultFamily::Job => KnowledgeResultFamily::Job,
        ServedResultFamily::CrossModal => KnowledgeResultFamily::CrossModal,
    }
}

fn family_name(family: KnowledgeResultFamily) -> &'static str {
    match family {
        KnowledgeResultFamily::Graph => "graph",
        KnowledgeResultFamily::Sql => "sql",
        KnowledgeResultFamily::Rdf => "rdf",
        KnowledgeResultFamily::Vector => "vector",
        KnowledgeResultFamily::TimeSeries => "time_series",
        KnowledgeResultFamily::Job => "job",
        KnowledgeResultFamily::CrossModal => "cross_modal",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn authority(policy: u8) -> KnowledgeStreamAuthority {
        KnowledgeStreamAuthority {
            tenant_ref: OpaqueRef::scoped("tenant", &format!("{:064x}", 1)).unwrap(),
            access_policy_ref: OpaqueRef::scoped("access-policy", &format!("{policy:064x}"))
                .unwrap(),
            reference_key: [policy; 32],
        }
    }

    #[test]
    fn verified_authority_exposes_only_keyed_opaque_references() {
        let claims = RequestContextClaims {
            principal: "originating-subject".to_string(),
            tenant: "tenant-display-name".to_string(),
            audience: "engine".to_string(),
            agent_id: "effective-agent".to_string(),
            roles: vec!["reader".to_string()],
            scopes: vec!["query:stream".to_string()],
            policy_version: "policy-display-name".to_string(),
            delegation: vec![
                "originating-subject".to_string(),
                "effective-agent".to_string(),
            ],
        };
        let authority = KnowledgeStreamAuthority::from_verified("server-secret", &claims).unwrap();
        for reference in [&authority.tenant_ref, &authority.access_policy_ref] {
            assert!(OpaqueRef::new(reference.as_str().to_string()).is_ok());
            assert!(!reference.as_str().contains("tenant-display-name"));
            assert!(!reference.as_str().contains("originating-subject"));
            assert!(!reference.as_str().contains("effective-agent"));
            assert!(!reference.as_str().contains("engine"));
            assert!(!reference.as_str().contains("policy-display-name"));
        }
    }

    fn execution(
        authority: &KnowledgeStreamAuthority,
        family: KnowledgeResultFamily,
        count: usize,
    ) -> FamilyExecution {
        FamilyExecution {
            rows: (0..count)
                .map(|index| {
                    native_row(
                        authority,
                        family_name(family),
                        &index.to_le_bytes(),
                        1.0,
                        None,
                    )
                })
                .collect(),
            source_result: ResultPayload::Count(count as u64),
        }
    }

    fn query(family: KnowledgeResultFamily) -> KnowledgeStreamQuery {
        match family {
            KnowledgeResultFamily::Graph => KnowledgeStreamQuery::Graph {
                label: String::new(),
                limit: 0,
            },
            KnowledgeResultFamily::Sql => KnowledgeStreamQuery::Sql {
                query: "SELECT 1".to_string(),
                params_msgpack: Vec::new(),
            },
            KnowledgeResultFamily::Rdf => KnowledgeStreamQuery::Rdf {
                query: "SELECT * WHERE { ?s ?p ?o }".to_string(),
                base_iri: String::new(),
                type_convention: String::new(),
            },
            KnowledgeResultFamily::Vector => KnowledgeStreamQuery::Vector {
                keywords: Vec::new(),
                query_embedding: Vec::new(),
                k: 1,
            },
            KnowledgeResultFamily::TimeSeries => KnowledgeStreamQuery::TimeSeries {
                series_id: "series".to_string(),
                from: 0,
                to: 1,
            },
            KnowledgeResultFamily::Job => KnowledgeStreamQuery::Job {
                job_id: "job".to_string(),
            },
            KnowledgeResultFamily::CrossModal => KnowledgeStreamQuery::CrossModal {
                text: "MATCH (n) |> LIMIT 1".to_string(),
            },
        }
    }

    #[test]
    fn all_seven_families_use_bounded_batches_and_resumable_cursor() {
        for family in KnowledgeResultFamily::ALL {
            let authority = authority(2);
            let first = serve_execution(
                "tenant-graph",
                &authority,
                4,
                Some(9),
                KnowledgeStreamRequestV1 {
                    schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                    query: query(family),
                    batch_size: 2,
                    cursor: None,
                    projection: KnowledgeStreamProjection::ArrowIpcV1,
                },
                execution(&authority, family, 5),
            )
            .unwrap();
            assert_eq!(first.family, family);
            assert_eq!(first.cursor.row_offset, 2);
            assert!(!first.cursor.exhausted);
            assert!(!first.payload.is_empty());

            let second = serve_execution(
                "tenant-graph",
                &authority,
                4,
                Some(9),
                KnowledgeStreamRequestV1 {
                    schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                    query: query(family),
                    batch_size: 2,
                    cursor: Some(first.cursor),
                    projection: KnowledgeStreamProjection::ArrowIpcV1,
                },
                execution(&authority, family, 5),
            )
            .unwrap();
            assert_eq!(second.cursor.row_offset, 4);
            assert_eq!(second.cursor.batch_index, 2);
        }
    }

    #[test]
    fn cursor_cannot_cross_authority_placement_or_changed_snapshot() {
        let original_authority = authority(2);
        let first = serve_execution(
            "tenant-graph",
            &original_authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: None,
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&original_authority, KnowledgeResultFamily::Graph, 3),
        )
        .unwrap();
        let resumed = |authority: &KnowledgeStreamAuthority, epoch: u64, count: usize| {
            serve_execution(
                "tenant-graph",
                authority,
                epoch,
                Some(9),
                KnowledgeStreamRequestV1 {
                    schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                    query: query(KnowledgeResultFamily::Graph),
                    batch_size: 2,
                    cursor: Some(first.cursor.clone()),
                    projection: KnowledgeStreamProjection::ArrowIpcV1,
                },
                execution(authority, KnowledgeResultFamily::Graph, count),
            )
        };
        assert!(resumed(&authority(3), 4, 3).is_err());
        assert!(resumed(&original_authority, 5, 3).is_err());
        assert!(resumed(&original_authority, 4, 4).is_err());

        let mut tampered = first.cursor.clone();
        tampered.row_offset = 0;
        tampered.batch_index = 0;
        assert!(serve_execution(
            "tenant-graph",
            &original_authority,
            4,
            Some(9),
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Graph),
                batch_size: 2,
                cursor: Some(tampered),
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&original_authority, KnowledgeResultFamily::Graph, 3),
        )
        .is_err());
    }

    #[test]
    fn native_projection_is_bounded_and_resumable() {
        let authority = authority(2);
        let response = serve_execution(
            "tenant-graph",
            &authority,
            0,
            None,
            KnowledgeStreamRequestV1 {
                schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
                query: query(KnowledgeResultFamily::Sql),
                batch_size: 2,
                cursor: None,
                projection: KnowledgeStreamProjection::ArrowIpcV1,
            },
            execution(&authority, KnowledgeResultFamily::Sql, 5),
        )
        .unwrap();
        assert!(!response.cursor.exhausted);
        assert_eq!(response.cursor.row_offset, 2);
        assert_eq!(response.projection, KnowledgeStreamProjection::ArrowIpcV1);
    }
}
