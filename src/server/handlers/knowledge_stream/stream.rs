use super::*;

pub(super) fn serve_execution(
    graph_name: &str,
    authority: &KnowledgeStreamAuthority,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    request: KnowledgeStreamRequestV1,
    execution: FamilyExecution,
) -> Result<KnowledgeStreamBatchV1, String> {
    // `try_handle` requires a bound lease for the served path.  The
    // conditional helper is intentionally also used here so the page adapter
    // itself brackets cursor resume, source snapshotting, and publication;
    // direct pure-adapter tests use an unbound authority and exercise the
    // deterministic wire contract without pretending to be an authorization
    // entry point.
    authority.validate_if_bound(true)?;
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
        authority.validate_if_bound(true)?;
        let native_cursor = native_cursor(authority, cursor)?;
        stream = stream
            .resume_from(&native_cursor)
            .map_err(|error| error.to_string())?;
        authority.validate_if_bound(false)?;
    }

    authority.validate_if_bound(true)?;
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
    // Drop the freshly encoded payload if revocation raced page production.
    // The caller therefore receives a typed denial, never a stale batch or
    // even its cursor metadata.
    authority.validate_if_bound(false)?;
    Ok(KnowledgeStreamBatchV1 {
        schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
        family,
        projection: request.projection,
        cursor: wire_cursor(authority, &cursor)?,
        payload,
    })
}

pub(super) fn stream_context(
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

pub(super) fn native_row(
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

pub(super) fn normalized_confidence(value: Option<f64>) -> f64 {
    value
        .filter(|value| value.is_finite())
        .unwrap_or(1.0)
        .clamp(0.0, 1.0)
}

pub(super) fn response_result(response: Response) -> Result<ResultPayload, String> {
    if let Some(error) = response.error {
        return Err(error);
    }
    response
        .result
        .ok_or_else(|| "query family returned no result".to_string())
}

pub(super) fn decode_knowledge_result<T>(
    bytes: &[u8],
) -> Result<T, eg_types::msgpack::MsgpackValidationError>
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

pub(super) fn raw_payload<'a>(
    payload: &'a ResultPayload,
    family: &str,
) -> Result<&'a [u8], String> {
    match payload {
        ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes) => Ok(bytes),
        _ => Err(format!("invalid {family} result encoding")),
    }
}

pub(super) fn node_has_label(properties: &[u8], label: &str) -> bool {
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

pub(super) fn visible_node_ids(
    core: &Arc<GraphCore>,
    authority: &KnowledgeStreamAuthority,
) -> Result<std::collections::HashSet<String>, String> {
    Ok(filtered_snapshot(core, authority)?
        .node_properties
        .into_keys()
        .collect())
}

pub(super) fn filtered_snapshot(
    core: &Arc<GraphCore>,
    authority: &KnowledgeStreamAuthority,
) -> Result<crate::graph::GraphView, String> {
    let mut view = core.analysis_snapshot();
    authority.filter_view(&mut view)?;
    Ok(view)
}

pub(super) fn native_cursor(
    authority: &KnowledgeStreamAuthority,
    cursor: &KnowledgeStreamCursorV1,
) -> Result<KnowledgeStreamCursor, String> {
    if cursor.schema_version != KNOWLEDGE_STREAM_SCHEMA_VERSION {
        return Err("unsupported KnowledgeStream cursor version".to_string());
    }
    if !cursor_identity_matches_authority(authority, cursor) {
        return Err("KnowledgeStream cursor authority mismatch".to_string());
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

pub(super) fn wire_cursor(
    authority: &KnowledgeStreamAuthority,
    cursor: &KnowledgeStreamCursor,
) -> Result<KnowledgeStreamCursorV1, String> {
    if cursor.tenant_ref != authority.tenant_ref
        || cursor.access_policy_ref != authority.access_policy_ref
    {
        return Err("KnowledgeStream cursor authority mismatch".to_string());
    }
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

fn cursor_identity_matches_authority(
    authority: &KnowledgeStreamAuthority,
    cursor: &KnowledgeStreamCursorV1,
) -> bool {
    cursor.tenant_ref == authority.tenant_ref.as_str()
        && cursor.access_policy_ref == authority.access_policy_ref.as_str()
}

pub(super) fn cursor_integrity(
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

pub(super) fn served_family(family: KnowledgeResultFamily) -> ServedResultFamily {
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

pub(super) fn wire_family(family: ServedResultFamily) -> KnowledgeResultFamily {
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

pub(super) fn family_name(family: KnowledgeResultFamily) -> &'static str {
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
