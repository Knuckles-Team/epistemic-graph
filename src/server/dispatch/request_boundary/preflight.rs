use super::*;

fn preflight_nested_msgpack(bytes: &[u8]) -> Result<(), &'static str> {
    crate::server::transport::validate_nested_msgpack(
        bytes,
        MAX_NESTED_MSGPACK_BYTES,
        MAX_NESTED_MSGPACK_ITEMS,
    )
}

fn preflight_optional_nested_msgpack(bytes: &[u8]) -> Result<(), &'static str> {
    if bytes.is_empty() {
        Ok(())
    } else {
        preflight_nested_msgpack(bytes)
    }
}

/// Node/edge property blobs on the graph-write surface.
///
/// Each group below returns `None` for a method it does not own, so the caller
/// can chain them; the method variants are disjoint, so the split cannot change
/// which blob is validated or in what order.
fn preflight_graph_write_msgpack(method: &Method) -> Option<Result<(), &'static str>> {
    match method {
        Method::AddNode {
            properties_msgpack, ..
        }
        | Method::CreateNodeIfAbsent {
            properties_msgpack, ..
        }
        | Method::AddEdge {
            properties_msgpack, ..
        }
        | Method::SupersedeEdge {
            properties_msgpack, ..
        }
        | Method::TxnAddNode {
            properties_msgpack, ..
        }
        | Method::TxnAddEdge {
            properties_msgpack, ..
        } => Some(preflight_nested_msgpack(properties_msgpack)),
        Method::CompareAndSetNodeFields {
            conditions_msgpack,
            updates_msgpack,
            ..
        }
        | Method::TxnCas {
            conditions_msgpack,
            updates_msgpack,
            ..
        } => Some(
            preflight_nested_msgpack(conditions_msgpack)
                .and_then(|()| preflight_nested_msgpack(updates_msgpack)),
        ),
        Method::ClaimNext {
            updates_msgpack, ..
        } => Some(preflight_nested_msgpack(updates_msgpack)),
        _ => None,
    }
}

/// Agent-memory, scene and trajectory blobs.
fn preflight_memory_msgpack(method: &Method) -> Option<Result<(), &'static str>> {
    match method {
        Method::CreateSummaryNode { props_msgpack, .. }
        | Method::StartTrajectory { props_msgpack } => {
            Some(preflight_nested_msgpack(props_msgpack))
        }
        Method::Consolidate {
            semantic_props_msgpack,
            ..
        } => Some(preflight_nested_msgpack(semantic_props_msgpack)),
        Method::AddSceneObject { pose_msgpack, .. } | Method::SetPose { pose_msgpack, .. } => {
            Some(preflight_nested_msgpack(pose_msgpack))
        }
        Method::AppendStep { action_msgpack, .. } => Some(preflight_nested_msgpack(action_msgpack)),
        Method::ObserveScreen { obs_msgpack } => Some(preflight_nested_msgpack(obs_msgpack)),
        _ => None,
    }
}

/// Batch, ingestion, SQL and time-series blobs.
fn preflight_batch_msgpack(method: &Method) -> Option<Result<(), &'static str>> {
    match method {
        Method::BatchUpdate { operations_msgpack } => {
            Some(preflight_nested_msgpack(operations_msgpack))
        }
        Method::MultiGraphBatchUpdate { batches_msgpack } => {
            Some(preflight_nested_msgpack(batches_msgpack))
        }
        Method::FromMsgpack { msgpack } | Method::Reconcile { msgpack, .. } => {
            Some(preflight_nested_msgpack(msgpack))
        }
        Method::ParseFiles { files_msgpack } | Method::IndexRepository { files_msgpack } => {
            Some(preflight_nested_msgpack(files_msgpack))
        }
        Method::Sql { params_msgpack, .. } => {
            Some(preflight_optional_nested_msgpack(params_msgpack))
        }
        Method::TxnAddMeasurement { points, .. } => Some(preflight_nested_msgpack(points)),
        Method::TsAppend { points_msgpack, .. } => Some(preflight_nested_msgpack(points_msgpack)),
        Method::TsAsofJoin {
            left_ts_msgpack, ..
        } => Some(preflight_nested_msgpack(left_ts_msgpack)),
        _ => None,
    }
}

/// A served-modality ingest carries the bundle blob directly; the stream form
/// bounds its cardinality FIRST, then validates every bundle in order.
#[cfg(feature = "modality-serving")]
fn preflight_served_modality_msgpack(
    op: &eg_types::modality::ServedModalityOp,
) -> Result<(), &'static str> {
    match op {
        eg_types::modality::ServedModalityOp::Ingest { bundle_msgpack, .. } => {
            preflight_nested_msgpack(bundle_msgpack)
        }
        eg_types::modality::ServedModalityOp::IngestStream { items, .. } => {
            if !(2..=64).contains(&items.len()) {
                return Err("served modality ingest stream cardinality is outside bounds");
            }
            for item in items {
                preflight_nested_msgpack(&item.bundle_msgpack)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Feature-gated surfaces plus the change-envelope cardinality bound.
fn preflight_feature_surface_msgpack(method: &Method) -> Option<Result<(), &'static str>> {
    match method {
        #[cfg(feature = "streaming")]
        Method::RegisterContinuousQuery { spec_msgpack, .. } => {
            Some(preflight_nested_msgpack(spec_msgpack))
        }
        #[cfg(feature = "streaming")]
        Method::RegisterTrigger { action_msgpack, .. } => {
            Some(preflight_optional_nested_msgpack(action_msgpack))
        }
        #[cfg(feature = "streaming")]
        Method::CepSubscribe {
            pattern_msgpack, ..
        } => Some(preflight_nested_msgpack(pattern_msgpack)),
        #[cfg(feature = "knowledge-batch")]
        Method::KnowledgeStream { request } => Some(match &request.query {
            crate::knowledge_stream::KnowledgeStreamQuery::Sql { params_msgpack, .. } => {
                preflight_optional_nested_msgpack(params_msgpack)
            }
            _ => Ok(()),
        }),
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { op } => Some(preflight_served_modality_msgpack(op)),
        // ChangeEnvelope validates every feature/evidence/outbox/mutation blob
        // with the shared eg-types preflight as part of its schema validation.
        Method::ApplyChangeEnvelope { .. } => Some(Ok(())),
        // The batch bounds its cardinality up front (the per-envelope nested-blob
        // validation stays each envelope's own `validate()` responsibility).
        Method::ApplyChangeEnvelopes { envelopes } => Some(
            if envelopes.len() > crate::change_envelope::MAX_ENVELOPES_PER_BATCH {
                Err("change envelope batch exceeds the resource limit")
            } else {
                Ok(())
            },
        ),
        _ => None,
    }
}

/// Validate every MessagePack-typed binary field reachable from a request before
/// routing. Raw source/blob/KV/broker/WASM bytes are intentionally excluded: they
/// are opaque binary, not nested MessagePack. Operation handlers still enforce
/// their narrower schema/count limits after this allocation-safety gate.
pub(crate) fn preflight_request_msgpack(method: &Method) -> Result<(), &'static str> {
    preflight_graph_write_msgpack(method)
        .or_else(|| preflight_memory_msgpack(method))
        .or_else(|| preflight_batch_msgpack(method))
        .or_else(|| preflight_feature_surface_msgpack(method))
        .unwrap_or(Ok(()))
}

// AST ingestion is intentionally content-based: callers send bounded source bytes
// identified by portable repository-relative names.  The engine never resolves a
// caller-provided host path.  These limits also keep a small, well-formed request
// from declaring enormous MessagePack collections and exhausting the process before
// the ordinary transport-frame cap can help.
#[cfg(feature = "ast")]
const DEFAULT_AST_MAX_FILES: usize = 4_096;
#[cfg(feature = "ast")]
const HARD_AST_MAX_FILES: usize = 100_000;
#[cfg(feature = "ast")]
const DEFAULT_AST_MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
#[cfg(feature = "ast")]
const HARD_AST_MAX_SOURCE_BYTES: usize = 64 * 1024 * 1024;
#[cfg(feature = "ast")]
const DEFAULT_AST_MAX_TOTAL_BYTES: usize = 32 * 1024 * 1024;
#[cfg(feature = "ast")]
const HARD_AST_MAX_TOTAL_BYTES: usize = 256 * 1024 * 1024;
#[cfg(feature = "ast")]
const MAX_AST_LOGICAL_PATH_BYTES: usize = 4_096;

#[cfg(feature = "ast")]
#[derive(Clone, Copy, Debug)]
pub(crate) struct AstInputLimits {
    pub(crate) max_files: usize,
    pub(crate) max_source_bytes: usize,
    pub(crate) max_total_bytes: usize,
}

#[cfg(feature = "ast")]
fn bounded_ast_limit(name: &str, default: usize, hard: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
        .min(hard)
}

#[cfg(feature = "ast")]
pub(crate) fn ast_input_limits() -> AstInputLimits {
    static LIMITS: OnceLock<AstInputLimits> = OnceLock::new();
    *LIMITS.get_or_init(|| AstInputLimits {
        max_files: bounded_ast_limit(
            "EPISTEMIC_GRAPH_AST_MAX_FILES",
            DEFAULT_AST_MAX_FILES,
            HARD_AST_MAX_FILES,
        ),
        max_source_bytes: bounded_ast_limit(
            "EPISTEMIC_GRAPH_AST_MAX_SOURCE_BYTES",
            DEFAULT_AST_MAX_SOURCE_BYTES,
            HARD_AST_MAX_SOURCE_BYTES,
        ),
        max_total_bytes: bounded_ast_limit(
            "EPISTEMIC_GRAPH_AST_MAX_TOTAL_BYTES",
            DEFAULT_AST_MAX_TOTAL_BYTES,
            HARD_AST_MAX_TOTAL_BYTES,
        ),
    })
}

#[cfg(feature = "ast")]
pub(crate) fn validate_ast_logical_path(path: &str) -> Result<(), String> {
    if path.is_empty() || path.len() > MAX_AST_LOGICAL_PATH_BYTES {
        return Err("AST_INPUT_INVALID: source name must be a bounded logical path".to_string());
    }
    if path.starts_with('/')
        || path.starts_with('~')
        || path.contains('\\')
        || path.contains(':')
        || path.chars().any(|ch| ch.is_control())
    {
        return Err(
            "AST_INPUT_INVALID: source names must be portable repository-relative paths"
                .to_string(),
        );
    }
    if path
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(
            "AST_INPUT_INVALID: source names must not contain empty or traversal segments"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(feature = "ast")]
fn read_msgpack_u8(input: &[u8], cursor: &mut usize) -> Result<u8, String> {
    let value = input
        .get(*cursor)
        .copied()
        .ok_or_else(|| "AST_INPUT_INVALID: truncated source collection".to_string())?;
    *cursor += 1;
    Ok(value)
}

#[cfg(feature = "ast")]
fn read_msgpack_len_bytes(input: &[u8], cursor: &mut usize, width: usize) -> Result<usize, String> {
    let end = cursor
        .checked_add(width)
        .filter(|end| *end <= input.len())
        .ok_or_else(|| "AST_INPUT_INVALID: truncated source collection".to_string())?;
    let value = match width {
        1 => input[*cursor] as usize,
        2 => u16::from_be_bytes([input[*cursor], input[*cursor + 1]]) as usize,
        4 => u32::from_be_bytes([
            input[*cursor],
            input[*cursor + 1],
            input[*cursor + 2],
            input[*cursor + 3],
        ]) as usize,
        _ => unreachable!("MessagePack length widths are fixed"),
    };
    *cursor = end;
    Ok(value)
}

#[cfg(feature = "ast")]
fn read_msgpack_array_len(input: &[u8], cursor: &mut usize) -> Result<usize, String> {
    let marker = read_msgpack_u8(input, cursor)?;
    match marker {
        0x90..=0x9f => Ok((marker & 0x0f) as usize),
        0xdc => read_msgpack_len_bytes(input, cursor, 2),
        0xdd => read_msgpack_len_bytes(input, cursor, 4),
        _ => Err("AST_INPUT_INVALID: expected a source collection array".to_string()),
    }
}

#[cfg(feature = "ast")]
fn read_msgpack_str_len(input: &[u8], cursor: &mut usize) -> Result<usize, String> {
    let marker = read_msgpack_u8(input, cursor)?;
    match marker {
        0xa0..=0xbf => Ok((marker & 0x1f) as usize),
        0xd9 => read_msgpack_len_bytes(input, cursor, 1),
        0xda => read_msgpack_len_bytes(input, cursor, 2),
        0xdb => read_msgpack_len_bytes(input, cursor, 4),
        _ => Err("AST_INPUT_INVALID: source name must be a string".to_string()),
    }
}

#[cfg(feature = "ast")]
fn read_msgpack_bin_len(input: &[u8], cursor: &mut usize) -> Result<usize, String> {
    match read_msgpack_u8(input, cursor)? {
        0xc4 => read_msgpack_len_bytes(input, cursor, 1),
        0xc5 => read_msgpack_len_bytes(input, cursor, 2),
        0xc6 => read_msgpack_len_bytes(input, cursor, 4),
        _ => Err("AST_INPUT_INVALID: source content must be MessagePack binary".to_string()),
    }
}

#[cfg(feature = "ast")]
fn take_msgpack_slice<'a>(
    input: &'a [u8],
    cursor: &mut usize,
    len: usize,
) -> Result<&'a [u8], String> {
    let end = cursor
        .checked_add(len)
        .filter(|end| *end <= input.len())
        .ok_or_else(|| "AST_INPUT_INVALID: truncated source collection".to_string())?;
    let value = &input[*cursor..end];
    *cursor = end;
    Ok(value)
}

/// Decode exactly `[(logical_path, source_bytes), ...]` without trusting container
/// length hints to preallocate unbounded memory.  This deliberately accepts only
/// the canonical wire shape emitted by the clients.
#[cfg(feature = "ast")]
pub(crate) fn decode_ast_files(
    input: &[u8],
    limits: AstInputLimits,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut cursor = 0usize;
    let count = read_msgpack_array_len(input, &mut cursor)?;
    if count > limits.max_files {
        return Err(format!(
            "AST_INPUT_LIMIT: source collection exceeds {} files",
            limits.max_files
        ));
    }

    let mut files = Vec::with_capacity(count);
    let mut total_bytes = 0usize;
    let mut names = std::collections::HashSet::with_capacity(count);
    for _ in 0..count {
        if read_msgpack_array_len(input, &mut cursor)? != 2 {
            return Err(
                "AST_INPUT_INVALID: each source entry must contain name and bytes".to_string(),
            );
        }
        let name_len = read_msgpack_str_len(input, &mut cursor)?;
        if name_len > MAX_AST_LOGICAL_PATH_BYTES {
            return Err("AST_INPUT_LIMIT: source name is too long".to_string());
        }
        let name_bytes = take_msgpack_slice(input, &mut cursor, name_len)?;
        let name = std::str::from_utf8(name_bytes)
            .map_err(|_| "AST_INPUT_INVALID: source name must be UTF-8".to_string())?;
        validate_ast_logical_path(name)?;
        if !names.insert(name.to_string()) {
            return Err("AST_INPUT_INVALID: duplicate source name".to_string());
        }

        let source_len = read_msgpack_bin_len(input, &mut cursor)?;
        if source_len > limits.max_source_bytes {
            return Err(format!(
                "AST_INPUT_LIMIT: one source exceeds {} bytes",
                limits.max_source_bytes
            ));
        }
        total_bytes = total_bytes
            .checked_add(source_len)
            .filter(|total| *total <= limits.max_total_bytes)
            .ok_or_else(|| {
                format!(
                    "AST_INPUT_LIMIT: source collection exceeds {} bytes",
                    limits.max_total_bytes
                )
            })?;
        let source = take_msgpack_slice(input, &mut cursor, source_len)?.to_vec();
        files.push((name.to_string(), source));
    }
    if cursor != input.len() {
        return Err("AST_INPUT_INVALID: trailing data after source collection".to_string());
    }
    Ok(files)
}
