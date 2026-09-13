#[cfg(feature = "raft")]
use super::change_envelope::multi_graph_batch_update;
#[cfg(feature = "raft")]
use super::consensus::{is_replicated_apply, propose_native_mutation};
use super::router::dispatch_request_method;
#[cfg(all(feature = "sparql-http", feature = "raft"))]
use super::sparql_update::coordinated_sparql_http_update;
use super::*;

#[derive(serde::Deserialize)]
struct ScreenObservationWire {
    session_id: String,
    #[serde(default)]
    frame_seq: u64,
    #[serde(default)]
    prev_frame_id: String,
    #[serde(default)]
    prev_hash: u64,
    #[serde(with = "serde_bytes", default)]
    png: Vec<u8>,
    #[serde(default)]
    elements: Vec<crate::screen::UiElementInput>,
}

/// Session identity and previous-frame lineage. Checked in the ORIGINAL order:
/// the session identifier first, then the previous-frame identifier's own shape,
/// then whether it names an earlier frame of THIS session — an observation
/// invalid on more than one of these keeps reporting the first.
fn validate_screen_session(wire: &ScreenObservationWire) -> Result<(), String> {
    if wire.session_id.is_empty()
        || wire.session_id.len() > MAX_SCREEN_SESSION_ID_BYTES
        || !wire
            .session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("invalid screen observation session identifier".to_string());
    }
    if wire.prev_frame_id.len() > MAX_SCREEN_PREVIOUS_ID_BYTES
        || wire.prev_frame_id.chars().any(char::is_control)
    {
        return Err("invalid previous screen observation identifier".to_string());
    }
    if wire.prev_frame_id.is_empty() {
        return Ok(());
    }
    let prefix = format!("screenobservation:{}:", wire.session_id);
    let previous_sequence = wire
        .prev_frame_id
        .strip_prefix(&prefix)
        .and_then(|suffix| suffix.parse::<u64>().ok())
        .filter(|sequence| *sequence < wire.frame_seq);
    if previous_sequence.is_none() {
        return Err(
            "previous screen observation must belong to the same earlier session frame".to_string(),
        );
    }
    Ok(())
}

fn validate_screen_png_dimensions(width: u32, height: u32) -> Result<(), String> {
    if width == 0
        || height == 0
        || width > MAX_SCREEN_DIMENSION
        || height > MAX_SCREEN_DIMENSION
        || u64::from(width) * u64::from(height) > MAX_SCREEN_PIXELS
    {
        return Err("screen image dimensions exceed the resource limit".to_string());
    }
    Ok(())
}

/// Byte budget first, then the PNG signature/IHDR header, then the declared
/// dimensions read out of that header. An absent image is allowed.
fn validate_screen_png(png: &[u8]) -> Result<(), String> {
    if png.len() > MAX_SCREEN_PNG_BYTES {
        return Err("screen image exceeds the resource limit".to_string());
    }
    if png.is_empty() {
        return Ok(());
    }
    const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";
    if png.len() < 24 || !png.starts_with(PNG_SIGNATURE) || &png[12..16] != b"IHDR" {
        return Err("screen image must be a PNG".to_string());
    }
    let width = u32::from_be_bytes(png[16..20].try_into().unwrap_or([0; 4]));
    let height = u32::from_be_bytes(png[20..24].try_into().unwrap_or([0; 4]));
    validate_screen_png_dimensions(width, height)
}

fn screen_element_text_within_policy(element: &crate::screen::UiElementInput) -> bool {
    !element.role.is_empty()
        && element.role.len() <= MAX_SCREEN_ROLE_BYTES
        && !element.role.chars().any(char::is_control)
        && element.name.len() <= MAX_SCREEN_ELEMENT_NAME_BYTES
        && !element.name.contains('\0')
}

fn screen_element_box_within_policy(element: &crate::screen::UiElementInput) -> bool {
    element.x.unsigned_abs() <= MAX_SCREEN_COORDINATE_ABS as u64
        && element.y.unsigned_abs() <= MAX_SCREEN_COORDINATE_ABS as u64
        && element.w >= 0
        && element.h >= 0
        && element.w <= MAX_SCREEN_COORDINATE_ABS
        && element.h <= MAX_SCREEN_COORDINATE_ABS
}

/// Element cardinality, then each element's own policy, then the RUNNING text
/// budget — the running total is what bounds a request built from many small
/// but individually legal elements.
fn validate_screen_elements(elements: &[crate::screen::UiElementInput]) -> Result<(), String> {
    if elements.len() > MAX_SCREEN_ELEMENTS {
        return Err("screen element count exceeds the resource limit".to_string());
    }
    let mut text_bytes = 0usize;
    for element in elements {
        if !screen_element_text_within_policy(element) || !screen_element_box_within_policy(element)
        {
            return Err("screen element violates the input policy".to_string());
        }
        text_bytes = text_bytes
            .checked_add(element.role.len())
            .and_then(|total| total.checked_add(element.name.len()))
            .ok_or_else(|| "screen element text exceeds the resource limit".to_string())?;
        if text_bytes > MAX_SCREEN_TOTAL_TEXT_BYTES {
            return Err("screen element text exceeds the resource limit".to_string());
        }
    }
    Ok(())
}

pub(super) fn decode_screen_observation(
    obs_msgpack: &[u8],
) -> Result<crate::screen::ScreenObservationInput, String> {
    let wire: ScreenObservationWire = eg_types::msgpack::decode_bounded(
        obs_msgpack,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_SCREEN_OBSERVATION_BYTES,
            MAX_SCREEN_OBSERVATION_ITEMS,
            64,
        ),
    )
    .map_err(|_| "invalid screen observation payload".to_string())?;

    validate_screen_session(&wire)?;
    validate_screen_png(&wire.png)?;
    validate_screen_elements(&wire.elements)?;

    Ok(crate::screen::ScreenObservationInput {
        session_id: wire.session_id,
        frame_seq: wire.frame_seq,
        prev_frame_id: wire.prev_frame_id,
        prev_hash: wire.prev_hash,
        png: wire.png,
        elements: wire.elements,
    })
}

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

/// Bind the generated WorkItem command context to the already verified carrier.
/// The command carries a full GOC-15 `RequestContext` for durable provenance,
/// but it is not a second authority: tenant, graph, agent, audience, policy,
/// and every downstream scope must be derived from (or narrower than) the
/// authenticated envelope before the command can be proposed or committed.
fn validate_submit_context(
    graph: &str,
    context: &crate::epistemic_operations::RequestContext,
    verified_context: &VerifiedRequestContext,
) -> Result<(), String> {
    if context.schema_version != crate::epistemic_operations::RequestContextSchemaVersion::V2 {
        return Err("SubmitWorkItem context schema_version is unsupported".to_string());
    }
    if context.graph != graph {
        return Err("SubmitWorkItem context graph does not match request graph".to_string());
    }
    // The tenant/agent/audience/policy_version identity check is the one comparison
    // every request boundary shares -- both this native `SubmitWorkItem` command
    // binding and the `kg-delegate` context validation
    // (`server::handlers::delegation::validate_request_context`) run the exact same
    // four-field comparison over the same verified-authority carrier before going on
    // to check their own request's time window / scope bounds, which ARE
    // surface-specific and stay local to each caller. `context_matches_verified_authority`
    // is defined once, in `handlers::delegation` (its `pub(crate)` home), and called
    // from both boundaries.
    if !crate::server::handlers::delegation::context_matches_verified_authority(
        context,
        verified_context,
    ) {
        return Err("SubmitWorkItem context does not match verified request authority".to_string());
    }
    if !submit_context_within_carrier_bounds(context, verified_context) {
        return Err("SubmitWorkItem context violates the verified carrier bounds".to_string());
    }
    Ok(())
}

fn submit_context_within_carrier_bounds(
    context: &crate::epistemic_operations::RequestContext,
    verified_context: &VerifiedRequestContext,
) -> bool {
    !context.request_id.trim().is_empty()
        && !context.subject_id.trim().is_empty()
        && !context.trace_id.trim().is_empty()
        && !context
            .scopes
            .iter()
            .any(|scope| scope.trim().is_empty() || !verified_context.allows_action(scope))
        && context.expires_at_ms >= context.issued_at_ms
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
pub(super) fn preflight_request_msgpack(method: &Method) -> Result<(), &'static str> {
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
pub(super) struct AstInputLimits {
    pub(super) max_files: usize,
    pub(super) max_source_bytes: usize,
    pub(super) max_total_bytes: usize,
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
pub(super) fn ast_input_limits() -> AstInputLimits {
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
pub(super) fn validate_ast_logical_path(path: &str) -> Result<(), String> {
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
pub(super) fn decode_ast_files(
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

#[cfg(feature = "redb")]
struct SessionControlSaga {
    backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
    saga: handlers::admin::AdminSaga,
}

fn is_session_control_mutation(method: &Method) -> bool {
    match method {
        Method::CreateChannel { .. }
        | Method::JoinChannel { .. }
        | Method::LeaveChannel { .. }
        | Method::CloseChannel { .. }
        | Method::SendMessage { .. } => true,
        #[cfg(feature = "streaming")]
        Method::RegisterContinuousQuery { .. }
        | Method::DropContinuousQuery { .. }
        | Method::RegisterTrigger { .. }
        | Method::DropTrigger { .. } => true,
        #[cfg(all(feature = "streaming", feature = "stream"))]
        Method::CepSubscribe { .. } | Method::CepUnsubscribe { .. } => true,
        #[cfg(feature = "wasm-udf")]
        Method::RegisterUdf { .. } => true,
        #[cfg(feature = "federation")]
        Method::RegisterForeignSource { .. } => true,
        _ => false,
    }
}

#[cfg(feature = "redb")]
async fn begin_session_control_saga(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    caller: Option<&str>,
    method: &Method,
    attempt_nonce: Option<eg_types::contract::Nonce>,
) -> Result<Option<SessionControlSaga>, String> {
    if !is_session_control_mutation(method) {
        return Ok(None);
    }
    let backend =
        timed_read(state).await.persistence.clone().ok_or_else(|| {
            "session control mutation requires durable redb coordination".to_string()
        })?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "session control mutation requires durable redb coordination".to_string())?;
    let saga = handlers::admin::begin_admin_saga_with_nonce(
        redb,
        request_id,
        caller,
        method,
        crate::mutation_batch::DurabilityDomain::ControlPlane,
        attempt_nonce,
    )?;
    Ok(Some(SessionControlSaga { backend, saga }))
}

#[cfg(not(feature = "redb"))]
async fn begin_session_control_saga(
    _state: &Arc<RwLock<ServerState>>,
    _request_id: u64,
    _caller: Option<&str>,
    method: &Method,
    _attempt_nonce: Option<eg_types::contract::Nonce>,
) -> Result<Option<()>, String> {
    if is_session_control_mutation(method) {
        Err("session control mutation requires the redb MutationBatch coordinator".to_string())
    } else {
        Ok(None)
    }
}

#[cfg(feature = "redb")]
fn finish_session_control_saga(control: SessionControlSaga) -> Result<(), String> {
    let redb = control
        .backend
        .as_redb()
        .ok_or_else(|| "session control mutation lost its redb coordinator".to_string())?;
    handlers::admin::finish_admin_saga(
        redb,
        control.saga.batch,
        control.saga.created_at_ms,
        ResultPayload::Bool(true),
    )?;
    Ok(())
}

#[cfg(not(feature = "redb"))]
fn finish_session_control_saga(_control: ()) -> Result<(), String> {
    Err("session control mutation requires the redb MutationBatch coordinator".to_string())
}

/// Dispatch a single request to the appropriate handler, recording
/// per-operation request counters and latency (CONCEPT:EG-KG.txn.per-graph-write-isolation).
pub async fn dispatch(state: &Arc<RwLock<ServerState>>, req: Request) -> Response {
    dispatch_with_context(state, req, None).await
}

pub(super) fn append_native_resource_ops(ops: &mut Vec<&'static str>, available: bool) {
    if available {
        ops.extend([
            "ReserveWorkItemResources",
            "ReleaseWorkItemResources",
            "ReclaimWorkItemResources",
            "QueryWorkItemReservation",
            "ResourceReservationStatus",
            "UpdateResourceHost",
        ]);
    }
}

pub(super) fn append_native_capacity_ops(ops: &mut Vec<&'static str>, available: bool) {
    if available {
        ops.extend([
            "AcquireCapacity",
            "RenewCapacity",
            "ReleaseCapacity",
            "ReclaimExpiredCapacity",
            "ReconcileCapacity",
            "CapacityStatus",
            "UpdateCapacityCell",
        ]);
    }
}

pub(super) fn append_native_work_item_ops(ops: &mut Vec<&'static str>, available: bool) {
    if available {
        ops.extend(["KgDelegate", "SubmitWorkItem", "SubmitWorkItems"]);
    }
}

/// Dispatch a native transport request whose current envelope was verified
/// before optional QoS admission. This keeps authentication single-pass: a
/// mutation nonce is consumed by the durable MutationBatch ledger and a
/// read-only nonce by the transport replay ledger, while admission plus
/// dispatch share the same immutable verified context.
pub(crate) async fn dispatch_verified_request(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    context: VerifiedRequestContext,
) -> Response {
    dispatch_with_context(state, req, Some(context)).await
}

/// Bridge an already-authenticated auxiliary broker protocol into the same
/// authorization and dispatch path as the primary request transport. The
/// caller supplies only a secret-keyed opaque actor reference; raw protocol
/// usernames are never admitted to request or persistence state.
pub(crate) async fn dispatch_authenticated_broker_actor(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    actor_ref: &str,
) -> Response {
    let request_id = req.id;
    let context = match VerifiedRequestContext::authenticated_broker_actor(actor_ref, request_id) {
        Ok(context) => context,
        Err(error) => {
            crate::metrics::auth_failure();
            return Response::err(request_id, error);
        }
    };
    dispatch_with_context(state, req, Some(context)).await
}

/// Dispatch an engine-owned local query adapter under its fixed, read-only
/// service identity. The caller remains subject to provisioned graph ACL/RBAC.
pub(crate) async fn dispatch_authenticated_local_query(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
) -> Response {
    let context = match VerifiedRequestContext::authenticated_local_query(req.id) {
        Ok(context) => context,
        Err(error) => return Response::err(req.id, error),
    };
    dispatch_with_context(state, req, Some(context)).await
}

pub(super) async fn dispatch_with_context(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    context: Option<VerifiedRequestContext>,
) -> Response {
    // CONCEPT:EG-OS.observability.slow-query-descriptor — slow-query descriptor, captured BEFORE the method is moved
    // into `dispatch_inner`. `None` (zero cost) unless EPISTEMIC_GRAPH_SLOW_QUERY_MS
    // enabled it AND this is a query method.
    let slow = crate::slow_query::describe(&req.method);
    #[cfg(feature = "metrics")]
    let op: &'static str = (&req.method).into();

    // Time the request when EITHER Prometheus metrics OR slow-query logging needs
    // it. When both are off (metrics feature disabled AND the threshold unset) we
    // skip the clock entirely — byte-for-byte the prior `not(metrics)` path.
    let start = (cfg!(feature = "metrics") || slow.is_some()).then(std::time::Instant::now);

    let resp = dispatch_inner(state, req, context).await;

    if let Some(start) = start {
        let elapsed = start.elapsed();
        #[cfg(feature = "metrics")]
        crate::metrics::record_request(op, elapsed.as_secs_f64());
        if let Some(slow) = slow {
            slow.log_if_slow(elapsed);
        }
    }
    resp
}

#[cfg(feature = "cost")]
pub(super) async fn dispatch_resource_stats(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    request: crate::cost::ResourceStatsRequest,
) -> Response {
    // ResourceStats is service-scoped, so construct the same verified graph
    // read authority used by graph reads before scanning the registry.  The
    // cost collector filters tenant + ACL before it increments any aggregate,
    // cursor, or candidate state.
    let isolation = timed_read(state).await.isolation.clone();
    let authority = match GraphReadAuthority::from_verified(verified_context, &isolation) {
        Ok(authority) => authority,
        Err(error) => return Response::err(req_id, error),
    };
    match crate::cost::collect_resource_stats_authorized(
        state,
        &authority,
        verified_context.tenant(),
        request,
    )
    .await
    {
        Ok(snapshot) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::coordination::ResourceStatsPage>(
                snapshot,
            ),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

/// Explicitly erases a dispatch-arm future's concrete (often enormous,
/// datafusion/cypher/sql-plan-carrying) type to `dyn Future + Send` at the
/// match-arm boundary, once, here -- rather than letting `dispatch_inner`'s
/// own generated per-arm state machine hold N structurally distinct
/// concrete future types simultaneously, which is what overflowed rustc's
/// trait-resolution recursion limit (E0275) once this lane's extraction
/// gave the match 53 separate `async fn` calls instead of one inlined body.
/// `Box<dyn Future<Output = Response> + Send>` is trivially `Send` by
/// construction, so this bounds the Send-proof cost per arm to O(1) instead
/// of O(the whole call graph). Same fix as `server::transport.rs`'s spawn
/// site and the pre-existing `dispatch_on_heap` test helper (dispatch.rs /
/// registry_reaper.rs), applied at the dispatch_inner match itself.
pub(super) fn dispatch_boxed<'a>(
    fut: impl std::future::Future<Output = Response> + Send + 'a,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
    Box::pin(fut)
}

fn required_resource_controller_scope(method: &Method) -> Option<&'static str> {
    match method {
        Method::ReserveWorkItemResources { .. }
        | Method::ReleaseWorkItemResources { .. }
        | Method::ReclaimWorkItemResources { .. } => Some("resource:reserve"),
        Method::UpdateResourceHost { .. } => Some("resource:host"),
        _ => None,
    }
}

fn required_capacity_controller_scope(method: &Method) -> Option<&'static str> {
    match method {
        Method::AcquireCapacity { .. }
        | Method::RenewCapacity { .. }
        | Method::ReleaseCapacity { .. }
        | Method::ReclaimExpiredCapacity { .. } => Some("capacity:lease"),
        Method::UpdateCapacityCell { .. } => Some("capacity:admin"),
        _ => None,
    }
}

/// One controller-scope gate. `authority` names the surface in the refusal text
/// so the caller's message is byte-identical to the inline checks this replaced.
fn enforce_controller_scope(
    req: &Request,
    verified_context: &VerifiedRequestContext,
    required_scope: Option<&str>,
    authority: &str,
) -> Result<(), Response> {
    let Some(required_scope) = required_scope else {
        return Ok(());
    };
    if verified_context.allows_action(required_scope) || verified_context.allows_action("kg:admin")
    {
        return Ok(());
    }
    crate::metrics::access_denied();
    Err(Response::err(
        req.id,
        format!(
            "ACCESS_DENIED: {authority} authority requires controller scope '{required_scope}'"
        ),
    ))
}

fn check_resource_and_capacity_controller_scope(
    req: &Request,
    verified_context: &VerifiedRequestContext,
) -> Result<(), Response> {
    // Resource authority is checked BEFORE capacity authority; a request that
    // violates both must keep reporting the resource refusal.
    enforce_controller_scope(
        req,
        verified_context,
        required_resource_controller_scope(&req.method),
        "resource",
    )?;
    enforce_controller_scope(
        req,
        verified_context,
        required_capacity_controller_scope(&req.method),
        "capacity",
    )
}

async fn check_resource_and_capacity_scope(
    req: &Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
) -> Result<(), Response> {
    if !state_machine_authorized && !identity_bootstrap {
        if matches!(
            &req.method,
            Method::MintWorkItemClaimCapability { .. }
                | Method::VerifyWorkItemClaimCapability { .. }
        ) {
            let capability_authorized = verified_context.allows_action("work:claim-capability")
                || verified_context.allows_action("kg:admin");
            if !capability_authorized {
                crate::metrics::access_denied();
                return Err(Response::err(
                    req.id,
                    "ACCESS_DENIED: WorkItem claim capability requires work:claim-capability",
                ));
            }
        }
        check_resource_and_capacity_controller_scope(req, verified_context)?;
    }
    Ok(())
}

/// The tenant a resource/capacity/work-item body names, if any. This is a
/// correlation carried by the request body, NOT an authority claim.
fn requested_tenant_ref(method: &Method) -> Option<&str> {
    match method {
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => Some(request.tenant_ref.as_str()),
        Method::QueryWorkItemReservation { request }
        | Method::ResourceReservationStatus { request } => Some(request.tenant_ref.as_str()),
        Method::UpdateResourceHost { request } => Some(request.tenant_ref.as_str()),
        Method::AcquireCapacity { request } => Some(request.tenant_ref.as_str()),
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            Some(request.tenant_ref.as_str())
        }
        Method::ReclaimExpiredCapacity { request } => Some(request.tenant_ref.as_str()),
        Method::ReconcileCapacity { request } | Method::CapacityStatus { request } => {
            Some(request.tenant_ref.as_str())
        }
        Method::SubmitWorkItem { request } => Some(request.context.tenant_id.as_str()),
        Method::KgDelegate { request } => Some(request.context.tenant_id.as_str()),
        Method::SubmitWorkItems { request } => Some(request.context.tenant_id.as_str()),
        _ => None,
    }
}

/// Only `kg:admin`, or an explicitly privileged aggregate READER on the two
/// reconciliation surfaces, may name a tenant other than the verified one.
fn cross_tenant_access_allowed(method: &Method, verified_context: &VerifiedRequestContext) -> bool {
    verified_context.allows_action("kg:admin")
        || (matches!(
            method,
            Method::QueryWorkItemReservation { .. } | Method::ResourceReservationStatus { .. }
        ) && verified_context.allows_action("resource:read:aggregate"))
        || (matches!(
            method,
            Method::ReconcileCapacity { .. } | Method::CapacityStatus { .. }
        ) && verified_context.allows_action("capacity:read:aggregate"))
}

fn check_cross_tenant_scope(
    req: &Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
) -> Result<(), Response> {
    if state_machine_authorized || identity_bootstrap {
        return Ok(());
    }
    let names_other_tenant =
        requested_tenant_ref(&req.method).is_some_and(|tenant| tenant != verified_context.tenant());
    if !names_other_tenant || cross_tenant_access_allowed(&req.method, verified_context) {
        return Ok(());
    }
    crate::metrics::access_denied();
    Err(Response::err(
        req.id,
        "ACCESS_DENIED: resource tenant must match verified request tenant",
    ))
}

async fn check_scope_and_admin_authority(
    state: &Arc<RwLock<ServerState>>,
    req: &Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
    action: &'static str,
    mutates: bool,
) -> Result<(), Response> {
    check_resource_and_capacity_scope(
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    )
    .await?;
    // The tenant in a resource body is a correlation, not an authority claim.
    // Bind ordinary callers to the verified request tenant before the native
    // backend sees the request.  Only an explicitly privileged aggregate reader
    // (for reconciliation) or `kg:admin` may inspect another tenant's rows.
    check_cross_tenant_scope(
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    )?;
    if !state_machine_authorized
        && !identity_bootstrap
        && !verified_context.allows_method(action, mutates)
    {
        crate::metrics::access_denied();
        return Err(Response::err(
            req.id,
            format!("ACCESS_DENIED: verified request context lacks required scope '{action}'"),
        ));
    }
    {
        if !state_machine_authorized && is_admin_authz_action(action) && !identity_bootstrap {
            let s = timed_read(state).await;
            let result = require_admin_capability(&s.isolation, req.agent_id.as_deref(), action);
            drop(s);
            if let Err(msg) = result {
                return Err(Response::err(req.id, msg));
            }
        }
    }
    Ok(())
}

/// Every carrier context a submit method asserts, in the order the inline
/// checks validated them: the envelope's own context first, then each child's.
/// That order is load-bearing — a batch invalid at both levels must keep
/// reporting the envelope's failure.
fn submit_work_item_contexts(method: &Method) -> Vec<&crate::epistemic_operations::RequestContext> {
    match method {
        Method::SubmitWorkItem { request } => vec![&request.context],
        Method::KgDelegate { request } => vec![&request.context],
        Method::SubmitWorkItems { request } => std::iter::once(&request.context)
            .chain(request.requests.iter().map(|child| &child.context))
            .collect(),
        _ => Vec::new(),
    }
}

fn check_submit_work_item_context(
    req: &Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
) -> Result<(), Response> {
    if state_machine_authorized || identity_bootstrap {
        return Ok(());
    }
    for context in submit_work_item_contexts(&req.method) {
        if let Err(error) = validate_submit_context(&req.graph, context, verified_context) {
            return Err(Response::err(req.id, error));
        }
    }
    Ok(())
}

#[cfg(feature = "raft")]
async fn check_cluster_placement_before_consensus(
    state: &Arc<RwLock<ServerState>>,
    req: &Request,
) -> Result<(), Response> {
    use crate::server::mutation::ClusterMutationRoute;
    if is_replicated_apply() {
        // The command is already committed in the owning group's log. Its
        // domain kernel must apply locally on every replica without proposing
        // the same command again.
        return Ok(());
    }
    let placement = {
        let current = timed_read(state).await;
        current.placement_authority()
    };
    if !matches!(
        placement,
        crate::server::state::PlacementAuthorityKind::Local
    ) {
        match crate::server::mutation::cluster_mutation_route(&req.method) {
            // `SelfRoutedAdmin` owns its OWN `MultiRaft`-presence check
            // (`handlers::raft_admin::try_handle` answers
            // `RAFT_NOT_CONFIGURED`/`CLUSTER_CONFIGURATION_INVALID` itself,
            // matching this exact pair of messages) — it must not be
            // preempted here, exactly like `ReadOnly`/`VolatileControl`.
            ClusterMutationRoute::ReadOnly
            | ClusterMutationRoute::VolatileControl
            | ClusterMutationRoute::SelfRoutedAdmin => {}
            ClusterMutationRoute::ConsensusGraph
            | ClusterMutationRoute::ConsensusNative
            | ClusterMutationRoute::ConsensusFanout
                if placement.missing_error().is_some() =>
            {
                return Err(Response::err(
                    req.id,
                    placement
                        .missing_error()
                        .expect("missing placement authority has a typed error"),
                ));
            }
            ClusterMutationRoute::ConsensusGraph
            | ClusterMutationRoute::ConsensusNative
            | ClusterMutationRoute::ConsensusFanout => {}
        }
    }
    Ok(())
}

#[cfg(feature = "raft")]
async fn route_consensus_before_gateway(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    verified_context: &VerifiedRequestContext,
    identity_bootstrap: bool,
) -> Result<Request, Response> {
    if !is_replicated_apply()
        && matches!(
            timed_read(state).await.placement_authority(),
            crate::server::state::PlacementAuthorityKind::MultiRaft
        )
        && matches!(
            crate::server::mutation::cluster_mutation_route(&req.method),
            crate::server::mutation::ClusterMutationRoute::ConsensusNative
        )
    {
        return Err(propose_native_mutation(
            state,
            &req.graph,
            req.id,
            verified_context,
            identity_bootstrap,
            req.method,
        )
        .await);
    }

    if !is_replicated_apply()
        && matches!(
            crate::server::mutation::cluster_mutation_route(&req.method),
            crate::server::mutation::ClusterMutationRoute::ConsensusFanout
        )
    {
        return Err(match req.method {
            Method::MultiGraphBatchUpdate { batches_msgpack } => {
                multi_graph_batch_update(
                    state,
                    req.id,
                    req.agent_id.as_deref(),
                    verified_context,
                    &batches_msgpack,
                )
                .await
            }
            #[cfg(feature = "sparql-http")]
            Method::ApplyMutation { event_type, query }
                if event_type == crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT =>
            {
                coordinated_sparql_http_update(
                    state,
                    req.id,
                    req.agent_id.as_deref(),
                    verified_context,
                    &req.graph,
                    query,
                )
                .await
            }
            _ => Response::err(req.id, "consensus fanout routing error"),
        });
    }

    Ok(req)
}

async fn compute_identity_bootstrap(
    state: &Arc<RwLock<ServerState>>,
    req: &Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
) -> bool {
    !state_machine_authorized && {
        let state = timed_read(state).await;
        state.isolation.identity_bootstrap_pending()
            && req.graph == "__commons__"
            && matches!(
                &req.method,
                Method::RegisterIdentity {
                    agent_id,
                    role: crate::isolation::AgentRole::System,
                    teams,
                    roles,
                    ..
                } if agent_id == verified_context.agent_id()
                    && teams.is_empty()
                    && roles.is_empty()
            )
            && verified_context.allows_identity_bootstrap()
    }
}

async fn dispatch_preamble_checks(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
) -> Result<(Request, bool), Response> {
    let method_policy = eg_capabilities::policy(&req.method);
    let action = method_policy.authz_action;
    let identity_bootstrap =
        compute_identity_bootstrap(state, &req, verified_context, state_machine_authorized).await;
    // Resource reservation authority is deliberately narrower than the coarse
    // `kg:write` aggregate.  The resolved-profile assertion and host telemetry
    // are controller inputs; an ordinary graph writer must not be able to forge
    // a heavy reservation or overwrite shared physical-host accounting merely by
    // carrying a WorkItem-shaped request.  Replicated apply already carries a
    // verified native authority and bypasses the external gate here.
    check_scope_and_admin_authority(
        state,
        &req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
        action,
        method_policy.mutates,
    )
    .await?;

    if let Err(error) = preflight_request_msgpack(&req.method) {
        return Err(Response::err(req.id, error));
    }

    // BUG-254 / NE-023: reject an unsupported lifecycle type at the first
    // authenticated, authoritative dispatch boundary.  This runs before
    // placement routing, consensus proposal, session-control saga creation,
    // persistence, or registry publication, so a bad CreateGraph request can
    // never enter the multi-minute retry path or leave partial state behind.
    // The transport decoder has a matching closed-enum guard for raw wire
    // callers; this check protects in-process and future-variant callers too.
    if let Method::CreateGraph { graph_type, .. } = &req.method {
        if let Err(error) = validate_graph_create_type(*graph_type) {
            return Err(Response::err(req.id, error));
        }
    }

    check_submit_work_item_context(
        &req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    )?;

    // Cluster writes cross consensus at the authenticated request boundary. The
    // complete mutation inventory is partitioned between graph commands and typed
    // native commands; a command constructor failure is returned before any local
    // saga or store mutation.
    #[cfg(feature = "raft")]
    check_cluster_placement_before_consensus(state, &req).await?;

    #[cfg(feature = "raft")]
    let req =
        route_consensus_before_gateway(state, req, verified_context, identity_bootstrap).await?;

    Ok((req, identity_bootstrap))
}

async fn dispatch_inner(
    state: &Arc<RwLock<ServerState>>,
    mut req: Request,
    context: Option<VerifiedRequestContext>,
) -> Response {
    // External requests verify the current signed context. Mutation nonces are
    // carried into the durable MutationBatch kernel, while read-only nonces are
    // checked by the transport replay ledger before dispatch. In-process broker
    // bridges provide a context only after their protocol-specific credential has
    // verified.
    let verified_context = match context {
        Some(context) => context,
        None => {
            let s = timed_read(state).await;
            match verify_request_with_security_dir(&s.auth_secret, &req, s.persist_dir.as_deref()) {
                Ok(context) => context,
                Err(msg) => {
                    crate::metrics::auth_failure();
                    return Response::err(req.id, msg);
                }
            }
        }
    };
    req.agent_id = Some(verified_context.agent_id().to_string());

    #[cfg(feature = "raft")]
    let state_machine_authorized = is_replicated_apply();
    #[cfg(not(feature = "raft"))]
    let state_machine_authorized = false;

    // ── Scope + admin enforcement (CONCEPT:EG-KG.compute.feature, EG-P0-6) ────────────────
    // A verified context must carry the capability ledger's action scope.
    // Additionally gates EVERY method whose policy declares a system-wide
    // admin `authz_action` (RegisterIdentity/RbacAdmin/ApplyMultisigMutation, the
    // M3 reshard/rebalance/catalog family, Backup/Restore) behind
    // `IsolationLayer::has_admin_capability` -- driven off the ledger's
    // `authz_action` string (`access::is_admin_authz_action`), NEVER a second
    // hardcoded method-name list here. Checked ONCE, before the method match, so
    // every current AND future admin-tier method is covered without a dispatch.rs
    // edit. Runs only when the `server` feature (which pulls in `eg-capabilities`)
    // is active -- always true for this binary.
    let (req, identity_bootstrap) =
        match dispatch_preamble_checks(state, req, &verified_context, state_machine_authorized)
            .await
        {
            Ok(v) => v,
            Err(resp) => return resp,
        };
    let session_control = match begin_session_control_saga(
        state,
        req.id,
        req.agent_id.as_deref(),
        &req.method,
        verified_context.attempt_nonce(),
    )
    .await
    {
        Ok(control) => control,
        Err(error) => return Response::err(req.id, error),
    };
    if let Some(control) = session_control.as_ref() {
        #[cfg(feature = "redb")]
        if let Some(result) = control.saga.replayed.clone() {
            return Response::ok(req.id, result);
        }
        #[cfg(not(feature = "redb"))]
        let _ = control;
    }

    let req_id = req.id;
    let response = dispatch_request_method(
        state,
        req,
        &verified_context,
        state_machine_authorized,
        identity_bootstrap,
    )
    .await;
    finalize_dispatch_response(req_id, response, session_control)
}

/// Runs the session-control saga's completion side-effect after the main
/// dispatch match, exactly as `dispatch_inner`'s own tail did before this
/// lane's extraction (byte-identical logic, only moved to its own `fn` so
/// its 3 nested conditions are no longer counted against `dispatch_inner`).
/// Two versions, mirroring `begin_session_control_saga`/
/// `finish_session_control_saga`'s own existing `redb`-gated split: the
/// session_control TYPE itself differs (`Option<SessionControlSaga>` vs
/// `Option<()>`), not just the function body.
#[cfg(feature = "redb")]
fn finalize_dispatch_response(
    req_id: u64,
    response: Response,
    session_control: Option<SessionControlSaga>,
) -> Response {
    if response.error.is_none() {
        if let Some(control) = session_control {
            if let Err(error) = finish_session_control_saga(control) {
                return Response::err(req_id, error);
            }
        }
    }
    response
}

#[cfg(test)]
mod native_resource_capability_tests {
    use super::append_native_resource_ops;

    #[test]
    fn native_resource_ops_are_advertised_only_when_backend_declares_support() {
        let mut dark = Vec::new();
        append_native_resource_ops(&mut dark, false);
        assert!(dark.is_empty());

        let mut served = Vec::new();
        append_native_resource_ops(&mut served, true);
        assert_eq!(
            served,
            vec![
                "ReserveWorkItemResources",
                "ReleaseWorkItemResources",
                "ReclaimWorkItemResources",
                "QueryWorkItemReservation",
                "ResourceReservationStatus",
                "UpdateResourceHost",
            ]
        );
    }
}

#[cfg(not(feature = "redb"))]
fn finalize_dispatch_response(
    req_id: u64,
    response: Response,
    session_control: Option<()>,
) -> Response {
    if response.error.is_none() {
        if let Some(control) = session_control {
            if let Err(error) = finish_session_control_saga(control) {
                return Response::err(req_id, error);
            }
        }
    }
    response
}
