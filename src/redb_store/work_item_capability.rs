//! Native WorkItem claim-capability authority (RMDD-29, checkpoint 4).
//!
//! This module is deliberately not a generic bearer-token framework.  A
//! capability is an unguessable opaque nonce whose digest indexes a private,
//! encrypted native row.  The row binds the nonce to the authenticated worker,
//! session, graph lifecycle, and the exact live WorkItem lease tuple.  The
//! control row is checked first on every verification; private capability
//! metadata is never consulted for a caller that does not currently own a
//! live lease.

use super::{decode_durable, property_f64, property_string, property_u64, DurableCrypto, NODES};
use crate::epistemic_operations::{
    WorkItemClaimCapabilityDecision, WorkItemClaimCapabilityMintRequest,
    WorkItemClaimCapabilityRequestSchemaVersion, WorkItemClaimCapabilityResult,
    WorkItemClaimCapabilityResultSchemaVersion, WorkItemClaimCapabilityVerifyRequest,
};
use rand::RngCore;
use redb::{ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

/// Private tables are intentionally disjoint from WorkItem/status/list
/// projections, MutationBatch/outbox rows, and CDC/audit records.
pub(crate) const CAPABILITIES: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("work_item_claim_capabilities");
pub(crate) const INVOCATIONS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("work_item_claim_capability_invocations");
/// Native provenance is the only durable proof that a WorkItem has crossed the
/// engine's claim authority.  It is intentionally separate from the public
/// WorkItem row: a caller may submit a ready/submitted row, but it cannot turn a
/// generic row into an active lease or replace a claimed row through AddNode,
/// CAS, row-delta, or cross-modal projection.
pub(crate) const NATIVE_WORK_ITEMS: TableDefinition<(&str, &str), &[u8]> =
    TableDefinition::new("native_work_item_authority");

/// Stable privacy-safe refusal used when a capability operation reaches a
/// replicated state-machine apply without the original authenticated envelope.
/// The apply path must return this before leader/barrier/private-ledger work.
pub(crate) const AUTHORITY_UNAVAILABLE: &str = "authority_unavailable";

/// Private exact-input storage does not exist in this checkpoint.  This
/// test-only boundary counter is wired to the actual private-row accesses
/// below (the `CAPABILITIES` table get/decode in `mint_in_wtx`/
/// `verify_in_wtx`) so a future payload reader placed before capability
/// verification trips a failing test rather than a silent regression. The
/// `NATIVE_WORK_ITEMS`/`NODES` reads performed by `read_live_lease` are the
/// authorization check itself and are deliberately NOT counted here.
#[cfg(test)]
static PRIVATE_WORK_ITEM_BODY_READS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
pub(crate) fn reset_private_work_item_body_reads() {
    PRIVATE_WORK_ITEM_BODY_READS.store(0, Ordering::SeqCst);
}

#[cfg(test)]
pub(crate) fn private_work_item_body_reads() -> usize {
    PRIVATE_WORK_ITEM_BODY_READS.load(Ordering::SeqCst)
}

/// Record one access to the private `CAPABILITIES` row store — the capability
/// material itself, as opposed to the public WorkItem control row or the
/// native claim-provenance row consulted to establish authority. Call this at
/// the exact point the private row is fetched/decoded, never earlier. A
/// no-op outside `#[cfg(test)]` builds so call sites compile unconditionally
/// and impose no production cost.
#[cfg(test)]
fn record_private_work_item_body_read() {
    PRIVATE_WORK_ITEM_BODY_READS.fetch_add(1, Ordering::SeqCst);
}

#[cfg(not(test))]
#[inline(always)]
fn record_private_work_item_body_read() {}

const CAPABILITY_MAGIC: &[u8; 4] = b"WIC1";
const CAPABILITY_NONCE_BYTES: usize = 32;
const CAPABILITY_BYTES: usize = CAPABILITY_MAGIC.len() + CAPABILITY_NONCE_BYTES;
const MAX_CAPABILITY_BYTES: usize = 128;
const MAX_WORK_ITEM_ID_BYTES: usize = 512;
const MAX_AUTHORITY_TEXT_BYTES: usize = 512;
const MAX_CAPABILITY_RECORD_BYTES: usize = 16 * 1024;
const MAX_CAPABILITY_ROWS: usize = 4096;
const MAX_INVOCATION_ROWS: usize = 4096;
const MAX_NATIVE_WORK_ITEM_ROWS: usize = 65_536;

/// The server constructs this only after the signed request context has passed
/// authentication, replay, graph ACL, and scope checks.  It is crate-private so
/// Python/public callers cannot manufacture authority from an owner/lease tuple.
#[derive(Clone, Debug)]
pub struct AuthenticatedAuthority {
    pub(crate) tenant: String,
    pub(crate) audience: String,
    pub(crate) principal: String,
    pub(crate) agent_id: String,
    pub(crate) session: String,
    pub(crate) authority_epoch: u64,
    pub(crate) incarnation_id: String,
    pub(crate) now_ms: u64,
}

#[derive(Clone, Debug)]
struct LiveLease {
    tenant: String,
    agent_id: String,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: String,
    expires_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapabilityRecord {
    schema_version: u8,
    graph: String,
    tenant: String,
    audience: String,
    work_item_id: String,
    principal: String,
    agent_id: String,
    session: String,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
    work_item_fence: String,
    authority_epoch: u64,
    incarnation_id: String,
    expires_at_ms: u64,
    created_at_ms: u64,
    /// The opaque nonce is retained only inside this private native row so an
    /// exact idempotent retry can return the same bytes after restart.
    #[serde(with = "serde_bytes")]
    capability: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvocationRecord {
    schema_version: u8,
    request_digest: String,
    capability_digest: String,
    expires_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeWorkItemRecord {
    schema_version: u8,
    graph: String,
    work_item_id: String,
    tenant: String,
    kind: String,
    claimed_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Refusal {
    NotFound,
    Unauthorized,
    Expired,
    Stale,
    Malformed,
    InputConflict,
    RetentionExhausted,
}

impl Refusal {
    fn decision(self) -> WorkItemClaimCapabilityDecision {
        match self {
            Self::NotFound => WorkItemClaimCapabilityDecision::NotFound,
            Self::Unauthorized => WorkItemClaimCapabilityDecision::Unauthorized,
            Self::Expired => WorkItemClaimCapabilityDecision::Expired,
            Self::Stale => WorkItemClaimCapabilityDecision::Stale,
            Self::Malformed => WorkItemClaimCapabilityDecision::Malformed,
            Self::InputConflict => WorkItemClaimCapabilityDecision::InputConflict,
            Self::RetentionExhausted => WorkItemClaimCapabilityDecision::RetentionExhausted,
        }
    }
}

pub(crate) fn initialize_tables(wtx: &redb::WriteTransaction) -> Result<(), String> {
    wtx.open_table(CAPABILITIES)
        .map_err(|error| error.to_string())?;
    wtx.open_table(INVOCATIONS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(NATIVE_WORK_ITEMS)
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn clear_graph_rows_in_wtx(
    wtx: &redb::WriteTransaction,
    graph: &str,
) -> Result<(), String> {
    let mut native_work_items = wtx
        .open_table(NATIVE_WORK_ITEMS)
        .map_err(|error| error.to_string())?;
    clear_graph_rows_in_wtx_with_native(wtx, graph, &mut native_work_items)
}

/// Clear private capability state when the caller already owns the native
/// provenance table in this transaction.  redb intentionally rejects opening
/// one table twice in a write transaction, so graph clear/restore paths must
/// reuse the existing handle to preserve atomic purge semantics.
pub(crate) fn clear_graph_rows_in_wtx_with_native(
    wtx: &redb::WriteTransaction,
    graph: &str,
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
) -> Result<(), String> {
    let mut capabilities = wtx
        .open_table(CAPABILITIES)
        .map_err(|error| error.to_string())?;
    let mut invocations = wtx
        .open_table(INVOCATIONS)
        .map_err(|error| error.to_string())?;
    let capability_keys = capabilities
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .map(|(key, _)| {
            let (row_graph, digest) = key.value();
            (row_graph.to_string(), digest.to_string())
        })
        .take(MAX_CAPABILITY_ROWS + 1)
        .collect::<Vec<_>>();
    if capability_keys.len() > MAX_CAPABILITY_ROWS {
        return Err("native capability retention bound exceeded".to_string());
    }
    for (row_graph, digest) in capability_keys {
        capabilities
            .remove((row_graph.as_str(), digest.as_str()))
            .map_err(|error| error.to_string())?;
    }
    let invocation_keys = invocations
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .map(|(key, _)| {
            let (row_graph, session) = key.value();
            (row_graph.to_string(), session.to_string())
        })
        .take(MAX_INVOCATION_ROWS + 1)
        .collect::<Vec<_>>();
    if invocation_keys.len() > MAX_INVOCATION_ROWS {
        return Err("native capability invocation retention bound exceeded".to_string());
    }
    for (row_graph, session) in invocation_keys {
        invocations
            .remove((row_graph.as_str(), session.as_str()))
            .map_err(|error| error.to_string())?;
    }
    let native_keys = native_work_items
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
        .filter_map(|row| row.ok())
        .take_while(|(key, _)| key.value().0 == graph)
        .map(|(key, _)| {
            let (row_graph, item) = key.value();
            (row_graph.to_string(), item.to_string())
        })
        .take(MAX_NATIVE_WORK_ITEM_ROWS + 1)
        .collect::<Vec<_>>();
    if native_keys.len() > MAX_NATIVE_WORK_ITEM_ROWS {
        return Err("native WorkItem authority retention bound exceeded".to_string());
    }
    for (row_graph, item) in native_keys {
        native_work_items
            .remove((row_graph.as_str(), item.as_str()))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

/// Record a successful native claim in the same WriteTransaction as the lease
/// transition.  The record is deliberately non-reconstructible from a public
/// owner/lease tuple: capability verification requires this engine-created row.
pub(crate) fn record_native_claim_in_wtx(
    native_work_items: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    work_item_id: &str,
    props: &serde_json::Map<String, serde_json::Value>,
    claimed_at_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let record = NativeWorkItemRecord {
        schema_version: 1,
        graph: graph.to_string(),
        work_item_id: work_item_id.to_string(),
        tenant: property_string(props, "tenant").to_string(),
        kind: property_string(props, "kind").to_string(),
        claimed_at_ms,
    };
    let encoded = encode_private(&record, crypto)?;
    native_work_items
        .insert((graph, work_item_id), encoded.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn native_claim_exists(
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    work_item_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<bool, Refusal> {
    let Some(row) = native_work_items
        .get((graph, work_item_id))
        .map_err(|_| Refusal::Stale)?
    else {
        return Ok(false);
    };
    let record: NativeWorkItemRecord =
        decode_private(row.value(), crypto).map_err(|_| Refusal::Stale)?;
    Ok(record.schema_version == 1 && record.graph == graph && record.work_item_id == work_item_id)
}

const WORK_ITEM_AUTHORITY_KEYS: &[&str] = &[
    "node_type",
    "kind",
    "status",
    "tenant",
    "lease_owner",
    "last_lease_owner",
    "worker_ref",
    "session",
    "idempotency_key",
    "lease_epoch",
    "fencing_token",
    "work_item_fence",
    "lease_expires_at",
    "authority_epoch",
    "incarnation_id",
    "attempt",
    "metadata",
    "execution_payload",
    "operation_payload",
    "result_ref",
    "error_ref",
    "completed_at",
];

fn is_work_item(props: &serde_json::Map<String, serde_json::Value>) -> bool {
    property_string(props, "node_type") == "WorkItem"
}

fn has_active_value(props: &serde_json::Map<String, serde_json::Value>, key: &str) -> bool {
    let Some(value) = props.get(key) else {
        return false;
    };
    if value.is_null() {
        return false;
    }
    match value {
        serde_json::Value::String(value) => !value.trim().is_empty(),
        serde_json::Value::Number(value) => value.as_f64().is_some_and(|number| number > 0.0),
        serde_json::Value::Array(value) => !value.is_empty(),
        serde_json::Value::Object(value) => !value.is_empty(),
        serde_json::Value::Bool(value) => *value,
        serde_json::Value::Null => false,
    }
}

fn validate_submission_properties(
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    if !is_work_item(props) {
        return Ok(());
    }
    let status = property_string(props, "status");
    if !matches!(status, "submitted" | "ready") {
        return Err("native WorkItem authority required for active lease fields".to_string());
    }
    for key in [
        "lease_owner",
        "last_lease_owner",
        "worker_ref",
        "session",
        "lease_epoch",
        "fencing_token",
        "work_item_fence",
        "lease_expires_at",
        "authority_epoch",
        "incarnation_id",
        "attempt",
    ] {
        if has_active_value(props, key) {
            return Err("native WorkItem authority required for active lease fields".to_string());
        }
    }
    Ok(())
}

fn authority_update_keys(props: &serde_json::Map<String, serde_json::Value>) -> Option<&str> {
    props.keys().find_map(|key| {
        WORK_ITEM_AUTHORITY_KEYS
            .contains(&key.as_str())
            .then_some(key.as_str())
    })
}

/// Guard generic graph-row writers before they can manufacture or replace
/// native WorkItem authority.  Submission is intentionally the one exception:
/// a new `submitted`/`ready` WorkItem may be projected by the existing AU
/// submitter.  Once ClaimWorkItem records the private native marker, even
/// otherwise harmless generic CAS/AddNode/Set operations are refused.
pub(crate) fn validate_generic_method(
    graph: &str,
    method: &crate::protocol::Method,
    nodes: &redb::Table<(&str, &str), &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let existing =
        |work_item_id: &str| -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
            let Some(row) = nodes
                .get((graph, work_item_id))
                .map_err(|error| error.to_string())?
            else {
                return Ok(None);
            };
            let bytes = crypto.unseal(row.value())?;
            let props = decode_durable::<serde_json::Map<String, serde_json::Value>>(&bytes)?;
            Ok(Some(props))
        };
    let claimed = |work_item_id: &str| -> Result<bool, String> {
        native_work_items
            .get((graph, work_item_id))
            .map(|row| row.is_some())
            .map_err(|error| error.to_string())
    };

    match method {
        crate::protocol::Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            if claimed(node_id)? || existing(node_id)?.is_some_and(|props| is_work_item(&props)) {
                return Err(
                    "native WorkItem authority required for generic replacement".to_string()
                );
            }
            // Ordinary graph nodes may retain legacy opaque property bytes;
            // only a structurally valid WorkItem map is subject to the native
            // authority guard.  A plausible WorkItem can never bypass this
            // branch because it necessarily decodes as the map below.
            if let Ok(props) =
                decode_durable::<serde_json::Map<String, serde_json::Value>>(properties_msgpack)
            {
                validate_submission_properties(&props)?;
            }
        }
        crate::protocol::Method::RemoveNode { node_id } => {
            if claimed(node_id)? {
                return Err("native WorkItem authority required for generic removal".to_string());
            }
        }
        crate::protocol::Method::CompareAndSetNodeFields {
            node_id,
            updates_msgpack,
            ..
        } => {
            if claimed(node_id)? {
                return Err("native WorkItem authority required for generic update".to_string());
            }
            if existing(node_id)?.is_some_and(|props| is_work_item(&props)) {
                let updates =
                    decode_durable::<serde_json::Map<String, serde_json::Value>>(updates_msgpack)
                        .map_err(|_| "invalid WorkItem update properties".to_string())?;
                if let Some(key) = authority_update_keys(&updates) {
                    return Err(format!(
                        "native WorkItem authority required for protected field '{key}'"
                    ));
                }
            }
        }
        crate::protocol::Method::BatchUpdate { operations_msgpack } => {
            use crate::algorithms::BatchOperation;
            let operations = crate::algorithms::decode_batch_operations(operations_msgpack)?;
            for operation in operations {
                match operation {
                    BatchOperation::AddNode {
                        id,
                        properties_msgpack,
                        ..
                    } => {
                        if claimed(&id)? || existing(&id)?.is_some_and(|props| is_work_item(&props))
                        {
                            return Err(
                                "native WorkItem authority required for generic batch replacement"
                                    .to_string(),
                            );
                        }
                        if let Ok(props) = decode_durable::<
                            serde_json::Map<String, serde_json::Value>,
                        >(&properties_msgpack)
                        {
                            validate_submission_properties(&props)?;
                        }
                    }
                    BatchOperation::RemoveNode { id } if claimed(&id)? => {
                        return Err(
                            "native WorkItem authority required for generic batch removal"
                                .to_string(),
                        )
                    }
                    _ => {}
                }
            }
        }
        crate::protocol::Method::SetPose { node_id, .. } if claimed(node_id)? => {
            return Err("native WorkItem authority required for generic update".to_string());
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn validate_snapshot_nodes(nodes: &[(String, Vec<u8>)]) -> Result<(), String> {
    for (_, bytes) in nodes {
        // Checkpoint images may contain legacy opaque ordinary-node payloads;
        // a decoded WorkItem map is the only shape whose authority fields are
        // meaningful to this guard.
        if let Ok(props) = decode_durable::<serde_json::Map<String, serde_json::Value>>(bytes) {
            validate_submission_properties(&props)?;
        }
    }
    Ok(())
}

/// Atomically mint or replay the capability while the caller's write
/// transaction is held.  The request contains no mutable authority fields.
pub(crate) fn mint_in_wtx(
    wtx: &redb::WriteTransaction,
    graph: &str,
    request: &WorkItemClaimCapabilityMintRequest,
    authority: &AuthenticatedAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<WorkItemClaimCapabilityResult, String> {
    validate_authority(graph, authority)?;
    if request.schema_version != WorkItemClaimCapabilityRequestSchemaVersion::V1
        || !bounded_text(&request.work_item_id, MAX_WORK_ITEM_ID_BYTES)
    {
        return Ok(refusal_result(Refusal::Malformed));
    }
    let native_work_items = wtx
        .open_table(NATIVE_WORK_ITEMS)
        .map_err(|error| error.to_string())?;
    let nodes = wtx.open_table(NODES).map_err(|error| error.to_string())?;
    // Control-row authorization happens before either private capability table
    // is opened/read.  This is the no-private-row-read-before-authorization
    // checkpoint required by RMDD-29.
    let live = match read_live_lease(
        &nodes,
        &native_work_items,
        graph,
        &request.work_item_id,
        authority,
        crypto,
    ) {
        Ok(lease) => lease,
        Err(error) => return Ok(refusal_result(error)),
    };
    drop(nodes);

    let mut capabilities = wtx
        .open_table(CAPABILITIES)
        .map_err(|error| error.to_string())?;
    let mut invocations = wtx
        .open_table(INVOCATIONS)
        .map_err(|error| error.to_string())?;
    prune_expired(
        &mut capabilities,
        &mut invocations,
        graph,
        authority.now_ms,
        crypto,
    )?;

    let request_digest = request_digest(graph, request, authority, &live);
    // Same private-row boundary as verify_in_wtx: these reads happen only
    // after read_live_lease above has already established authority for this
    // mint/replay request.
    record_private_work_item_body_read();
    if let Some(stored) = invocations
        .get((graph, authority.session.as_str()))
        .map_err(|error| error.to_string())?
    {
        let invocation: InvocationRecord = decode_private(stored.value(), crypto)?;
        if invocation.request_digest != request_digest {
            return Ok(refusal_result(Refusal::InputConflict));
        }
        record_private_work_item_body_read();
        let Some(capability) = capabilities
            .get((graph, invocation.capability_digest.as_str()))
            .map_err(|error| error.to_string())?
        else {
            return Ok(refusal_result(Refusal::Stale));
        };
        let record: CapabilityRecord = decode_private(capability.value(), crypto)?;
        if !record_matches_live(&record, graph, &request.work_item_id, authority, &live) {
            return Ok(refusal_result(
                if record.expires_at_ms <= authority.now_ms {
                    Refusal::Expired
                } else {
                    Refusal::Stale
                },
            ));
        }
        return Ok(WorkItemClaimCapabilityResult {
            schema_version: WorkItemClaimCapabilityResultSchemaVersion::V1,
            decision: WorkItemClaimCapabilityDecision::Replayed,
            valid: true,
            capability: Some(record.capability),
        });
    }

    // An exact retry must remain replayable even when the bounded native
    // tables are full.  Retention applies only to a new session/capability;
    // it must never turn a lost-ack retry into a second mint or a refusal.
    if native_row_count(&capabilities, graph)? >= MAX_CAPABILITY_ROWS
        || native_row_count(&invocations, graph)? >= MAX_INVOCATION_ROWS
    {
        return Ok(refusal_result(Refusal::RetentionExhausted));
    }

    let mut nonce = [0_u8; CAPABILITY_NONCE_BYTES];
    rand::rngs::OsRng.fill_bytes(&mut nonce);
    let mut capability = Vec::with_capacity(CAPABILITY_BYTES);
    capability.extend_from_slice(CAPABILITY_MAGIC);
    capability.extend_from_slice(&nonce);
    let capability_digest = digest_capability(&capability);
    let record = CapabilityRecord {
        schema_version: 1,
        graph: graph.to_string(),
        tenant: authority.tenant.clone(),
        audience: authority.audience.clone(),
        work_item_id: request.work_item_id.clone(),
        principal: authority.principal.clone(),
        agent_id: authority.agent_id.clone(),
        session: authority.session.clone(),
        attempt: live.attempt,
        lease_epoch: live.lease_epoch,
        fencing_token: live.fencing_token,
        work_item_fence: live.work_item_fence,
        authority_epoch: authority.authority_epoch,
        incarnation_id: authority.incarnation_id.clone(),
        expires_at_ms: live.expires_at_ms,
        created_at_ms: authority.now_ms,
        capability: capability.clone(),
    };
    let invocation = InvocationRecord {
        schema_version: 1,
        request_digest,
        capability_digest: capability_digest.clone(),
        expires_at_ms: live.expires_at_ms,
    };
    let record_bytes = encode_private(&record, crypto)?;
    let invocation_bytes = encode_private(&invocation, crypto)?;
    capabilities
        .insert((graph, capability_digest.as_str()), record_bytes.as_slice())
        .map_err(|error| error.to_string())?;
    invocations
        .insert(
            (graph, authority.session.as_str()),
            invocation_bytes.as_slice(),
        )
        .map_err(|error| error.to_string())?;
    Ok(WorkItemClaimCapabilityResult {
        schema_version: WorkItemClaimCapabilityResultSchemaVersion::V1,
        decision: WorkItemClaimCapabilityDecision::Minted,
        valid: true,
        capability: Some(capability),
    })
}

/// Verify the opaque capability after the authoritative WorkItem control row
/// has been checked.  No private payload/blob row is touched by this function.
pub(crate) fn verify_in_wtx(
    wtx: &redb::WriteTransaction,
    graph: &str,
    request: &WorkItemClaimCapabilityVerifyRequest,
    authority: &AuthenticatedAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<WorkItemClaimCapabilityResult, String> {
    if validate_authority(graph, authority).is_err() {
        return Ok(verification_refusal_result());
    }
    if request.schema_version != WorkItemClaimCapabilityRequestSchemaVersion::V1
        || !bounded_text(&request.work_item_id, MAX_WORK_ITEM_ID_BYTES)
    {
        return Ok(verification_refusal_result());
    }
    if request.capability.len() > MAX_CAPABILITY_BYTES
        || request.capability.len() != CAPABILITY_BYTES
        || !request.capability.starts_with(CAPABILITY_MAGIC)
    {
        return Ok(verification_refusal_result());
    }
    let native_work_items = wtx
        .open_table(NATIVE_WORK_ITEMS)
        .map_err(|error| error.to_string())?;
    let nodes = wtx.open_table(NODES).map_err(|error| error.to_string())?;
    // The live lease is authoritative.  The private capability table is not
    // consulted until this check succeeds, preventing a forged public tuple or
    // a scope-mismatched caller from probing private rows.
    let live = match read_live_lease(
        &nodes,
        &native_work_items,
        graph,
        &request.work_item_id,
        authority,
        crypto,
    ) {
        Ok(lease) => lease,
        Err(_) => return Ok(verification_refusal_result()),
    };
    drop(nodes);
    let capabilities = wtx
        .open_table(CAPABILITIES)
        .map_err(|error| error.to_string())?;
    let digest = digest_capability(&request.capability);
    // This is the private-row access the zero-private-body-read ordering
    // requirement guards: the capability material itself, fetched only after
    // `read_live_lease` above has already established authority.
    record_private_work_item_body_read();
    let Some(stored) = capabilities
        .get((graph, digest.as_str()))
        .map_err(|error| error.to_string())?
    else {
        return Ok(verification_refusal_result());
    };
    let record: CapabilityRecord = match decode_private(stored.value(), crypto) {
        Ok(record) => record,
        Err(_) => return Ok(verification_refusal_result()),
    };
    if record.expires_at_ms <= authority.now_ms {
        return Ok(verification_refusal_result());
    }
    if !record_matches_live(&record, graph, &request.work_item_id, authority, &live)
        || record.capability != request.capability
    {
        return Ok(verification_refusal_result());
    }
    Ok(WorkItemClaimCapabilityResult {
        schema_version: WorkItemClaimCapabilityResultSchemaVersion::V1,
        decision: WorkItemClaimCapabilityDecision::Verified,
        valid: true,
        capability: None,
    })
}

fn read_live_lease(
    nodes: &redb::Table<(&str, &str), &[u8]>,
    native_work_items: &redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    work_item_id: &str,
    authority: &AuthenticatedAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<LiveLease, Refusal> {
    let row = nodes
        .get((graph, work_item_id))
        .map_err(|_| Refusal::NotFound)?
        .ok_or(Refusal::NotFound)?;
    let bytes = crypto.unseal(row.value()).map_err(|_| Refusal::Malformed)?;
    let props: serde_json::Map<String, serde_json::Value> =
        decode_durable(&bytes).map_err(|_| Refusal::Malformed)?;
    if property_string(&props, "node_type") != "WorkItem" {
        return Err(Refusal::NotFound);
    }
    if !native_claim_exists(native_work_items, graph, work_item_id, crypto)? {
        // A generic NODES row is not a native WorkItem authority, even when it
        // happens to contain a complete plausible lease tuple.
        return Err(Refusal::Stale);
    }
    if property_string(&props, "tenant") != authority.tenant {
        return Err(Refusal::Unauthorized);
    }
    let status = property_string(&props, "status");
    if !matches!(status, "leased" | "running") {
        return Err(Refusal::Stale);
    }
    if property_string(&props, "lease_owner") != authority.agent_id {
        return Err(Refusal::Unauthorized);
    }
    let expiry_s = property_f64(&props, "lease_expires_at");
    if !expiry_s.is_finite() || expiry_s <= 0.0 {
        return Err(Refusal::Malformed);
    }
    let expires_at_ms = (expiry_s * 1000.0).floor() as u64;
    if expires_at_ms <= authority.now_ms {
        return Err(Refusal::Expired);
    }
    let attempt = property_u64(&props, "attempt");
    let lease_epoch = property_u64(&props, "lease_epoch");
    let fencing_token = property_u64(&props, "fencing_token");
    let work_item_fence = property_string(&props, "work_item_fence").to_string();
    if attempt == 0 || lease_epoch == 0 || fencing_token == 0 || work_item_fence.is_empty() {
        return Err(Refusal::Stale);
    }
    Ok(LiveLease {
        tenant: authority.tenant.clone(),
        agent_id: authority.agent_id.clone(),
        attempt,
        lease_epoch,
        fencing_token,
        work_item_fence,
        expires_at_ms,
    })
}

fn record_matches_live(
    record: &CapabilityRecord,
    graph: &str,
    work_item_id: &str,
    authority: &AuthenticatedAuthority,
    live: &LiveLease,
) -> bool {
    record.schema_version == 1
        && record.graph == graph
        && record.tenant == authority.tenant
        && record.audience == authority.audience
        && record.work_item_id == work_item_id
        && record.principal == authority.principal
        && record.agent_id == authority.agent_id
        && record.session == authority.session
        && record.attempt == live.attempt
        && record.lease_epoch == live.lease_epoch
        && record.fencing_token == live.fencing_token
        && record.work_item_fence == live.work_item_fence
        && record.authority_epoch == authority.authority_epoch
        && record.incarnation_id == authority.incarnation_id
        && record.expires_at_ms == live.expires_at_ms
        && record.expires_at_ms > authority.now_ms
}

fn request_digest(
    graph: &str,
    request: &WorkItemClaimCapabilityMintRequest,
    authority: &AuthenticatedAuthority,
    live: &LiveLease,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph.work-item-claim-capability.v1\0");
    for value in [
        graph.as_bytes(),
        authority.tenant.as_bytes(),
        authority.audience.as_bytes(),
        request.work_item_id.as_bytes(),
        authority.principal.as_bytes(),
        authority.agent_id.as_bytes(),
        authority.session.as_bytes(),
        authority.incarnation_id.as_bytes(),
    ] {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value);
    }
    digest.update(authority.authority_epoch.to_be_bytes());
    digest.update(live.attempt.to_be_bytes());
    digest.update(live.lease_epoch.to_be_bytes());
    digest.update(live.fencing_token.to_be_bytes());
    digest.update(live.expires_at_ms.to_be_bytes());
    hex::encode(digest.finalize())
}

fn digest_capability(capability: &[u8]) -> String {
    hex::encode(Sha256::digest(capability))
}

fn encode_private<T: Serialize>(value: &T, crypto: DurableCrypto<'_>) -> Result<Vec<u8>, String> {
    let plain =
        rmp_serde::to_vec_named(value).map_err(|_| "capability record is invalid".to_string())?;
    if plain.len() > MAX_CAPABILITY_RECORD_BYTES {
        return Err("capability record exceeds native bound".to_string());
    }
    Ok(crypto.seal(&plain).into_owned())
}

fn decode_private<T: serde::de::DeserializeOwned>(
    stored: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<T, String> {
    if stored.len() > MAX_CAPABILITY_RECORD_BYTES {
        return Err("capability record exceeds native bound".to_string());
    }
    let plain = crypto.unseal(stored)?;
    decode_durable(&plain).map_err(|_| "capability record is invalid".to_string())
}

fn bounded_text(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max
}

fn validate_authority(graph: &str, authority: &AuthenticatedAuthority) -> Result<(), String> {
    if !bounded_text(graph, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.tenant, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.audience, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.principal, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.agent_id, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.session, MAX_AUTHORITY_TEXT_BYTES)
        || !bounded_text(&authority.incarnation_id, MAX_AUTHORITY_TEXT_BYTES)
    {
        return Err("capability authority is invalid".to_string());
    }
    if authority.authority_epoch == 0 {
        return Err("capability authority is invalid".to_string());
    }
    Ok(())
}

fn native_row_count(
    table: &redb::Table<(&str, &str), &[u8]>,
    graph: &str,
) -> Result<usize, String> {
    let mut count = 0usize;
    for row in table
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
    {
        let (key, _) = row.map_err(|error| error.to_string())?;
        if key.value().0 != graph {
            break;
        }
        count = count.saturating_add(1);
        if count > MAX_CAPABILITY_ROWS {
            break;
        }
    }
    Ok(count)
}

fn prune_expired(
    capabilities: &mut redb::Table<(&str, &str), &[u8]>,
    invocations: &mut redb::Table<(&str, &str), &[u8]>,
    graph: &str,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut capability_keys = Vec::new();
    let mut capability_scanned = 0usize;
    for row in capabilities
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, digest) = key.value();
        if row_graph != graph {
            break;
        }
        capability_scanned = capability_scanned.saturating_add(1);
        if capability_scanned > MAX_CAPABILITY_ROWS {
            return Err("native capability retention bound exceeded".to_string());
        }
        let record = decode_private::<CapabilityRecord>(value.value(), crypto)
            .map_err(|_| "native capability record is invalid".to_string())?;
        if record.expires_at_ms <= now_ms {
            capability_keys.push((row_graph.to_string(), digest.to_string()));
        }
    }
    for (row_graph, digest) in capability_keys {
        capabilities
            .remove((row_graph.as_str(), digest.as_str()))
            .map_err(|error| error.to_string())?;
    }
    let mut invocation_keys = Vec::new();
    let mut invocation_scanned = 0usize;
    for row in invocations
        .range((graph, "")..)
        .map_err(|error| error.to_string())?
    {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (row_graph, session) = key.value();
        if row_graph != graph {
            break;
        }
        invocation_scanned = invocation_scanned.saturating_add(1);
        if invocation_scanned > MAX_INVOCATION_ROWS {
            return Err("native capability invocation retention bound exceeded".to_string());
        }
        let record = decode_private::<InvocationRecord>(value.value(), crypto)
            .map_err(|_| "native capability invocation is invalid".to_string())?;
        if record.expires_at_ms <= now_ms {
            invocation_keys.push((row_graph.to_string(), session.to_string()));
        }
    }
    for (row_graph, session) in invocation_keys {
        invocations
            .remove((row_graph.as_str(), session.as_str()))
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn refusal_result(refusal: Refusal) -> WorkItemClaimCapabilityResult {
    WorkItemClaimCapabilityResult {
        schema_version: WorkItemClaimCapabilityResultSchemaVersion::V1,
        decision: refusal.decision(),
        valid: false,
        capability: None,
    }
}

fn verification_refusal_result() -> WorkItemClaimCapabilityResult {
    // Verification has one externally observable denial.  The detailed
    // refusal remains an internal control-flow value so callers cannot probe
    // tenant, item, lease, expiry, or private-ledger state.
    refusal_result(Refusal::Unauthorized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation_batch::{
        MutationBatch, MutationDomain, MutationOperation, MutationRequestContext, MutationSurface,
        MUTATION_BATCH_VERSION,
    };
    use crate::protocol::Method;
    use redb::{Database, Durability, ReadableDatabase, ReadableTableMetadata};
    use std::path::{Path, PathBuf};

    fn temp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "eg-work-item-capability-{tag}-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ))
    }

    fn open(path: &Path) -> Database {
        let db = Database::create(path).expect("create capability test db");
        let wtx = db.begin_write().expect("begin table initialization");
        super::super::initialize_canonical_tables(&wtx).expect("initialize native tables");
        wtx.commit().expect("commit table initialization");
        db
    }

    fn try_commit_method(db: &Database, method: Method) -> Result<(), String> {
        let mut ops = vec![("graph-a".to_string(), method)];
        let mut raft_log_ops = Vec::new();
        #[cfg(feature = "security")]
        let mut audit_tail = super::super::AuditTailCache::new();
        super::super::commit_ops(
            db,
            &mut ops,
            &mut raft_log_ops,
            Durability::Immediate,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit_tail,
        )
    }

    fn commit_method(db: &Database, method: Method) {
        try_commit_method(db, method).expect("public native graph mutation");
    }

    fn claim_native(db: &Database, id: &str, worker: &str, now_ms: u64, key: &str) {
        let expected_graph_version = super::super::read_mutation_graph_version(db, "graph-a")
            .expect("read mutation graph version")
            .unwrap_or(0);
        let batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: format!("claim-{key}"),
            context: MutationRequestContext {
                request_id: now_ms,
                principal: format!("principal:sha256:{}", "a".repeat(64)),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
            },
            tenant: "tenant-a".to_string(),
            graph: "graph-a".to_string(),
            placement_epoch: 0,
            idempotency_key: format!("claim-key-{key}"),
            expected_graph_version: Some(expected_graph_version),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: MutationDomain::ControlPlane,
                method: Method::ClaimWorkItem {
                    request: crate::epistemic_operations::ClaimWorkItemRequest {
                        schema_version:
                            crate::epistemic_operations::ClaimWorkItemRequestSchemaVersion::V1,
                        tenant_ref: "tenant-a".to_string(),
                        work_item_id: Some(id.to_string()),
                        queue_ref: None,
                        resource_class: None,
                        fairness_group: None,
                        worker_ref: worker.to_string(),
                        now_ms,
                        lease_ms: 100_000,
                        max_tenant_in_flight: 64,
                    },
                },
            }],
            outbox: Vec::new(),
            created_at_ms: now_ms,
        };
        #[cfg(feature = "security")]
        let mut audit_tail = super::super::AuditTailCache::new();
        let committed = super::super::commit_mutation_batch_inner(
            db,
            "graph-a",
            &batch,
            None,
            None,
            None,
            None,
            now_ms,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit_tail,
            true,
            None,
        )
        .expect("native claim");
        assert!(
            !committed.replayed,
            "claim fixture must be a fresh mutation"
        );
    }

    fn seed_work_item(db: &Database, id: &str) {
        commit_method(
            db,
            Method::AddNode {
                node_id: id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "node_type": "WorkItem",
                    "tenant": "tenant-a",
                    "kind": "generic",
                    "status": "ready",
                }))
                .expect("encode WorkItem"),
            },
        );
        claim_native(db, id, "worker-a", 10_000, &format!("seed-{id}"));
    }

    fn authority(now_ms: u64) -> AuthenticatedAuthority {
        AuthenticatedAuthority {
            tenant: "tenant-a".to_string(),
            audience: "graph-os".to_string(),
            principal: format!("principal:sha256:{}", "a".repeat(64)),
            agent_id: "worker-a".to_string(),
            session: "session-a".to_string(),
            authority_epoch: 7,
            incarnation_id: "incarnation-a".to_string(),
            now_ms,
        }
    }

    fn mint(
        db: &Database,
        item: &str,
        authority: &AuthenticatedAuthority,
    ) -> WorkItemClaimCapabilityResult {
        let wtx = db.begin_write().expect("begin capability mint");
        let result = mint_in_wtx(
            &wtx,
            "graph-a",
            &WorkItemClaimCapabilityMintRequest {
                schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                work_item_id: item.to_string(),
            },
            authority,
            DurableCrypto::none(),
        )
        .expect("native mint result");
        wtx.commit().expect("commit capability mint");
        result
    }

    fn verify(
        db: &Database,
        item: &str,
        capability: Vec<u8>,
        authority: &AuthenticatedAuthority,
    ) -> WorkItemClaimCapabilityResult {
        let wtx = db.begin_write().expect("begin capability verify");
        let result = verify_in_wtx(
            &wtx,
            "graph-a",
            &WorkItemClaimCapabilityVerifyRequest {
                schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                work_item_id: item.to_string(),
                capability,
            },
            authority,
            DurableCrypto::none(),
        )
        .expect("native verify result");
        wtx.commit().expect("commit capability verify");
        result
    }

    fn commit_native_result(
        db: &Database,
        item: &str,
        lease_epoch: u64,
        fencing_token: u64,
        now_ms: u64,
        key: &str,
        outcome: &str,
        retryable: bool,
    ) {
        let expected_graph_version = super::super::read_mutation_graph_version(db, "graph-a")
            .expect("read mutation graph version")
            .unwrap_or(0);
        let batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: format!("result-{key}"),
            context: MutationRequestContext {
                request_id: now_ms,
                principal: format!("principal:sha256:{}", "a".repeat(64)),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
            },
            tenant: "tenant-a".to_string(),
            graph: "graph-a".to_string(),
            placement_epoch: 0,
            idempotency_key: format!("result-key-{key}"),
            expected_graph_version: Some(expected_graph_version),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Job,
                domain: MutationDomain::ControlPlane,
                method: Method::CommitWorkItemResult {
                    tenant: "tenant-a".to_string(),
                    work_item_id: item.to_string(),
                    worker_id: "worker-a".to_string(),
                    lease_epoch,
                    fencing_token,
                    idempotency_key: format!("work-result-{key}"),
                    outcome: outcome.to_string(),
                    result_ref: None,
                    error_ref: None,
                    retryable,
                    now_ms,
                },
            }],
            outbox: Vec::new(),
            created_at_ms: now_ms,
        };
        #[cfg(feature = "security")]
        let mut audit_tail = super::super::AuditTailCache::new();
        super::super::commit_mutation_batch_inner(
            db,
            "graph-a",
            &batch,
            None,
            None,
            None,
            None,
            now_ms,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit_tail,
            true,
            None,
        )
        .expect("native WorkItem result");
    }

    fn private_row_counts(db: &Database) -> (usize, usize, usize) {
        let rtx = db.begin_read().expect("begin private row read");
        let capabilities = rtx.open_table(CAPABILITIES).expect("open capabilities");
        let invocations = rtx.open_table(INVOCATIONS).expect("open invocations");
        let native = rtx
            .open_table(NATIVE_WORK_ITEMS)
            .expect("open native provenance");
        (
            capabilities.len().expect("count capabilities") as usize,
            invocations.len().expect("count invocations") as usize,
            native.len().expect("count native provenance") as usize,
        )
    }

    #[test]
    fn generic_work_item_authority_forgery_is_rejected_before_private_state() {
        let path = temp_path("generic-forgery");
        let db = open(&path);
        let forged = serde_json::json!({
            "node_type": "WorkItem",
            "kind": "generic",
            "status": "leased",
            "tenant": "tenant-a",
            "lease_owner": "worker-a",
            "last_lease_owner": "worker-a",
            "worker_ref": "worker-a",
            "session": "session-a",
            "idempotency_key": "forged-key",
            "lease_epoch": 4,
            "fencing_token": 5,
            "work_item_fence": "forged-fence",
            "lease_expires_at": 99_999.0,
            "authority_epoch": 7,
            "incarnation_id": "forged-incarnation",
            "attempt": 2,
            "metadata": {"private": "forged"}
        });
        let error = try_commit_method(
            &db,
            Method::AddNode {
                node_id: "forged".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&forged).expect("encode forged row"),
            },
        )
        .expect_err("generic active WorkItem AddNode must refuse");
        assert!(error.contains("native WorkItem authority"));
        let batch_forgery = serde_json::json!([{
            "op": "add_node",
            "id": "forged-batch",
            "properties": forged,
        }]);
        let error = try_commit_method(
            &db,
            Method::BatchUpdate {
                operations_msgpack: rmp_serde::to_vec_named(&batch_forgery)
                    .expect("encode forged batch"),
            },
        )
        .expect_err("generic active WorkItem BatchUpdate must refuse");
        assert!(error.contains("native WorkItem authority"));
        assert!(
            super::super::read_one_node(&db, "graph-a", "forged-batch", DurableCrypto::none())
                .expect("read forged batch row")
                .is_none()
        );
        assert!(
            super::super::read_one_node(&db, "graph-a", "forged", DurableCrypto::none())
                .expect("read forged row")
                .is_none()
        );
        assert_eq!(private_row_counts(&db), (0, 0, 0));

        seed_work_item(&db, "wi-guard");
        let before = super::super::read_one_node(&db, "graph-a", "wi-guard", DurableCrypto::none())
            .expect("read native WorkItem")
            .expect("native WorkItem exists");
        let error = try_commit_method(
            &db,
            Method::CompareAndSetNodeFields {
                node_id: "wi-guard".to_string(),
                conditions_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "status": "leased"
                }))
                .expect("encode CAS conditions"),
                updates_msgpack: rmp_serde::to_vec_named(&forged).expect("encode forged update"),
            },
        )
        .expect_err("generic authority CAS must refuse");
        assert!(error.contains("native WorkItem authority"));
        let after = super::super::read_one_node(&db, "graph-a", "wi-guard", DurableCrypto::none())
            .expect("read native WorkItem after refusal")
            .expect("native WorkItem remains");
        assert_eq!(
            before, after,
            "refused CAS must not partially mutate the row"
        );

        assert!(try_commit_method(
            &db,
            Method::RemoveNode {
                node_id: "wi-guard".to_string()
            }
        )
        .is_err());
        assert!(try_commit_method(
            &db,
            Method::SetPose {
                node_id: "wi-guard".to_string(),
                pose_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "translation": [1.0, 2.0, 3.0]
                }))
                .expect("encode pose")
            }
        )
        .is_err());
        assert_eq!(private_row_counts(&db).0, 0);
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn graph_clear_purges_capability_invocation_and_native_provenance_rows_atomically() {
        let path = temp_path("graph-clear");
        let db = open(&path);
        commit_method(
            &db,
            Method::AddNode {
                node_id: "wi-clear".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "node_type": "WorkItem",
                    "tenant": "tenant-a",
                    "kind": "generic",
                    "status": "ready",
                }))
                .expect("encode replacement WorkItem"),
            },
        );
        claim_native(&db, "wi-clear", "worker-a", 10_000, "replacement");
        let first = mint(&db, "wi-clear", &authority(10_000));
        assert_eq!(first.decision, WorkItemClaimCapabilityDecision::Minted);
        assert_eq!(private_row_counts(&db), (1, 1, 1));

        commit_method(&db, Method::ClearGraph);
        assert_eq!(private_row_counts(&db), (0, 0, 0));
        assert!(
            super::super::read_one_node(&db, "graph-a", "wi-clear", DurableCrypto::none())
                .expect("read cleared WorkItem")
                .is_none()
        );

        // Reusing the graph/item id after a clear requires a new native claim;
        // the old opaque bytes cannot be replayed against the replacement row.
        commit_method(
            &db,
            Method::AddNode {
                node_id: "wi-clear".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "node_type": "WorkItem",
                    "tenant": "tenant-a",
                    "kind": "generic",
                    "status": "ready",
                }))
                .expect("encode restored WorkItem"),
            },
        );
        claim_native(&db, "wi-clear", "worker-a", 10_000, "restored");
        let second = mint(&db, "wi-clear", &authority(10_000));
        assert_eq!(second.decision, WorkItemClaimCapabilityDecision::Minted);
        assert_ne!(second.capability, first.capability);
        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn checkpoint_replacement_purges_private_capability_state_and_requires_new_claim() {
        let path = temp_path("checkpoint-replacement");
        let db = open(&path);
        seed_work_item(&db, "wi-checkpoint");
        let owner = authority(10_000);
        let first = mint(&db, "wi-checkpoint", &owner);
        assert_eq!(first.decision, WorkItemClaimCapabilityDecision::Minted);
        let old_capability = first.capability.clone().expect("checkpoint capability");
        assert_eq!(private_row_counts(&db), (1, 1, 1));

        // A checkpoint is an untrusted public graph image.  Convert the
        // leased image to a fresh ready submission before restoring it; the
        // restore path must never preserve an old lease or its private rows.
        let dump = super::super::read_graph_dump(&db, "graph-a", DurableCrypto::none())
            .expect("read graph checkpoint")
            .expect("graph checkpoint identity");
        let invalid = super::super::GraphDump {
            graph: dump.graph.clone(),
            name: dump.name.clone(),
            graph_type: dump.graph_type,
            incarnation_id: "incarnation:checkpoint-invalid".to_string(),
            source_snapshot_version: dump.source_snapshot_version.saturating_add(1),
            integrity_policy: dump.integrity_policy.clone(),
            nodes: dump.nodes.clone(),
            edges: dump.edges.clone(),
            ledger: dump.ledger.clone(),
            semantic: dump.semantic.clone(),
        };
        let error = super::super::apply_checkpoint(
            &db,
            &mut Vec::new(),
            vec![invalid],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint cannot manufacture an active WorkItem image");
        assert!(error.contains("native WorkItem authority"));
        assert_eq!(
            private_row_counts(&db),
            (1, 1, 1),
            "rejected checkpoint must not purge private state"
        );
        let nodes = dump
            .nodes
            .into_iter()
            .map(|(id, bytes)| {
                let mut props: serde_json::Map<String, serde_json::Value> =
                    decode_durable(&bytes).expect("decode checkpoint WorkItem");
                props.insert("status".into(), serde_json::Value::String("ready".into()));
                for key in [
                    "lease_owner",
                    "last_lease_owner",
                    "worker_ref",
                    "session",
                    "idempotency_key",
                    "lease_epoch",
                    "fencing_token",
                    "work_item_fence",
                    "lease_expires_at",
                    "authority_epoch",
                    "incarnation_id",
                    "attempt",
                ] {
                    props.remove(key);
                }
                (
                    id,
                    rmp_serde::to_vec_named(&props).expect("encode restored WorkItem"),
                )
            })
            .collect();
        let restored_incarnation = "incarnation:checkpoint-replacement";
        let restored = super::super::GraphDump {
            graph: dump.graph,
            name: dump.name,
            graph_type: dump.graph_type,
            incarnation_id: restored_incarnation.to_string(),
            source_snapshot_version: dump.source_snapshot_version.saturating_add(1),
            integrity_policy: dump.integrity_policy,
            nodes,
            edges: dump.edges,
            ledger: dump.ledger,
            semantic: dump.semantic,
        };
        super::super::apply_checkpoint(&db, &mut Vec::new(), vec![restored], DurableCrypto::none())
            .expect("checkpoint replacement commits atomically");
        assert_eq!(private_row_counts(&db), (0, 0, 0));
        assert_eq!(
            verify(&db, "wi-checkpoint", old_capability, &owner).decision,
            WorkItemClaimCapabilityDecision::Unauthorized,
            "a capability cannot survive graph replacement"
        );

        let mut replacement_owner = authority(30_000);
        replacement_owner.incarnation_id = restored_incarnation.to_string();
        claim_native(
            &db,
            "wi-checkpoint",
            "worker-a",
            30_000,
            "checkpoint-reclaim",
        );
        let second = mint(&db, "wi-checkpoint", &replacement_owner);
        assert_eq!(second.decision, WorkItemClaimCapabilityDecision::Minted);
        assert_ne!(second.capability, first.capability);
        assert_eq!(private_row_counts(&db), (1, 1, 1));

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn crossmodal_clear_and_delete_purge_private_capability_state_atomically() {
        let path = temp_path("crossmodal-lifecycle");
        let db = open(&path);
        commit_method(
            &db,
            Method::AddNode {
                node_id: "wi-crossmodal".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "node_type": "WorkItem",
                    "tenant": "tenant-a",
                    "kind": "generic",
                    "status": "ready",
                }))
                .expect("encode replacement WorkItem"),
            },
        );
        claim_native(
            &db,
            "wi-crossmodal",
            "worker-a",
            10_000,
            "crossmodal-delete",
        );
        assert_eq!(
            mint(&db, "wi-crossmodal", &authority(10_000)).decision,
            WorkItemClaimCapabilityDecision::Minted
        );
        let before_blob_ref = private_row_counts(&db);
        let blob_refs = [("wi-crossmodal".to_string(), "sha256:forged".to_string())];
        let error = {
            #[cfg(feature = "security")]
            let mut audit_tail = super::super::AuditTailCache::new();
            super::super::commit_crossmodal(
                &db,
                "graph-a",
                &[],
                &[],
                &blob_refs,
                &[],
                DurableCrypto::none(),
                #[cfg(feature = "security")]
                &mut audit_tail,
            )
        }
        .expect_err("generic crossmodal blob mutation cannot touch a WorkItem");
        assert!(error.contains("native WorkItem authority"));
        assert_eq!(private_row_counts(&db), before_blob_ref);
        let props =
            super::super::read_one_node(&db, "graph-a", "wi-crossmodal", DurableCrypto::none())
                .expect("read WorkItem after refused blob mutation")
                .expect("WorkItem remains after refused blob mutation");
        let props: serde_json::Map<String, serde_json::Value> =
            decode_durable(&props).expect("decode WorkItem after refused blob mutation");
        assert!(!props.contains_key("__blob__"));
        #[cfg(feature = "security")]
        let mut audit_tail = super::super::AuditTailCache::new();
        super::super::commit_crossmodal(
            &db,
            "graph-a",
            &[Method::ClearGraph],
            &[],
            &[],
            &[],
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut audit_tail,
        )
        .expect("crossmodal ClearGraph commits");
        assert_eq!(private_row_counts(&db), (0, 0, 0));

        commit_method(
            &db,
            Method::AddNode {
                node_id: "wi-crossmodal".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "node_type": "WorkItem",
                    "tenant": "tenant-a",
                    "kind": "generic",
                    "status": "ready",
                }))
                .expect("encode replacement WorkItem"),
            },
        );
        claim_native(
            &db,
            "wi-crossmodal",
            "worker-a",
            10_000,
            "crossmodal-delete-recreated",
        );
        assert_eq!(
            mint(&db, "wi-crossmodal", &authority(10_000)).decision,
            WorkItemClaimCapabilityDecision::Minted
        );
        #[cfg(feature = "security")]
        let mut delete_audit_tail = super::super::AuditTailCache::new();
        super::super::commit_crossmodal(
            &db,
            "graph-a",
            &[Method::DeleteGraph {
                graph_name: "graph-a".to_string(),
            }],
            &[],
            &[],
            &[],
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut delete_audit_tail,
        )
        .expect("crossmodal DeleteGraph commits");
        assert_eq!(private_row_counts(&db), (0, 0, 0));

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn expired_private_rows_are_pruned_with_a_bounded_scan_before_new_mint() {
        let path = temp_path("cleanup");
        let db = open(&path);
        seed_work_item(&db, "wi-expired");
        let owner = authority(10_000);
        let first = mint(&db, "wi-expired", &owner);
        let old_capability = first.capability.expect("initial capability");
        assert_eq!(private_row_counts(&db), (1, 1, 1));

        commit_method(
            &db,
            Method::AddNode {
                node_id: "wi-fresh".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "node_type": "WorkItem",
                    "tenant": "tenant-a",
                    "kind": "generic",
                    "status": "ready",
                }))
                .expect("encode fresh WorkItem"),
            },
        );
        claim_native(&db, "wi-fresh", "worker-a", 200_000, "cleanup-fresh");
        let mut fresh_owner = authority(200_000);
        fresh_owner.session = "session-b".to_string();
        let fresh = mint(&db, "wi-fresh", &fresh_owner);
        assert_eq!(fresh.decision, WorkItemClaimCapabilityDecision::Minted);
        assert_eq!(
            private_row_counts(&db),
            (1, 1, 2),
            "expired capability and invocation rows are pruned before mint"
        );
        assert_eq!(
            verify(&db, "wi-expired", old_capability, &owner).decision,
            WorkItemClaimCapabilityDecision::Unauthorized
        );

        drop(db);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn capability_is_opaque_context_bound_restart_persistent_and_fail_closed() {
        let path = temp_path("lifecycle");
        let capability = {
            let db = open(&path);
            seed_work_item(&db, "wi-1");
            seed_work_item(&db, "wi-2");
            let owner = authority(10_000);
            let minted = mint(&db, "wi-1", &owner);
            assert_eq!(minted.decision, WorkItemClaimCapabilityDecision::Minted);
            assert!(minted.valid);
            let capability = minted.capability.expect("opaque capability bytes");
            assert_eq!(capability.len(), CAPABILITY_BYTES);
            assert_eq!(&capability[..CAPABILITY_MAGIC.len()], CAPABILITY_MAGIC);
            assert_ne!(
                &capability[CAPABILITY_MAGIC.len()..],
                &[0_u8; CAPABILITY_NONCE_BYTES]
            );

            let verified = verify(&db, "wi-1", capability.clone(), &owner);
            assert_eq!(verified.decision, WorkItemClaimCapabilityDecision::Verified);
            assert!(verified.valid);
            assert!(
                verified.capability.is_none(),
                "verify never re-exports the nonce"
            );

            // Lost-ack retry: same authenticated session and live lease returns
            // exactly the persisted opaque bytes, not a second nonce.
            let replayed = mint(&db, "wi-1", &authority(10_001));
            assert_eq!(replayed.decision, WorkItemClaimCapabilityDecision::Replayed);
            assert_eq!(replayed.capability, Some(capability.clone()));

            // A changed input on the same idempotency session is rejected, even
            // though the second WorkItem is a valid live lease.
            let conflict = mint(&db, "wi-2", &owner);
            assert_eq!(
                conflict.decision,
                WorkItemClaimCapabilityDecision::InputConflict
            );

            // Scope/principal/session/owner/graph mismatches never verify the
            // copied opaque bytes or disclose private-row state.
            let assert_denied = |result: WorkItemClaimCapabilityResult| {
                assert_eq!(
                    result.decision,
                    WorkItemClaimCapabilityDecision::Unauthorized,
                    "all foreign/invalid verification outcomes are indistinguishable"
                );
                assert!(!result.valid);
                assert!(result.capability.is_none());
            };

            // A caller who does not currently own the live public lease for
            // this WorkItem (wrong tenant/owner, an unclaimed/unknown item,
            // or capability bytes that fail the cheap bounds/magic check
            // before any table lookup) is refused by `read_live_lease` alone.
            // The private CAPABILITIES/INVOCATIONS tables must never be
            // opened for these — this is the ordering RMDD-29 requires and
            // the counter below fails the test if that ordering regresses.
            reset_private_work_item_body_reads();
            let malformed = verify(&db, "wi-1", vec![0_u8; MAX_CAPABILITY_BYTES + 1], &owner);
            assert_denied(malformed);
            assert_denied(verify(&db, "wi-1", vec![1, 2, 3], &owner));
            assert_denied(verify(&db, "unknown-work-item", capability.clone(), &owner));
            let mut wrong_tenant = owner.clone();
            wrong_tenant.tenant = "tenant-b".to_string();
            assert_denied(verify(&db, "wi-1", capability.clone(), &wrong_tenant));
            let mut wrong_owner = owner.clone();
            wrong_owner.agent_id = "worker-b".to_string();
            assert_denied(verify(&db, "wi-1", capability.clone(), &wrong_owner));
            assert_eq!(
                private_work_item_body_reads(),
                0,
                "a caller who does not own the live public lease must be refused \
                 before the private capability row is ever opened"
            );

            // A caller who DOES own the live public lease (tenant + lease
            // owner match) but presents a capability minted for a different
            // audience/principal/session/authority-epoch/incarnation still
            // cannot verify — but this final check is inherently a private
            // record comparison (those fields are not stored on the public
            // WorkItem row at all), so it legitimately touches the private
            // row once base lease ownership is already established. The
            // outcome remains the same normalized denial either way.
            let mut wrong_audience = owner.clone();
            wrong_audience.audience = "other-audience".to_string();
            assert_denied(verify(&db, "wi-1", capability.clone(), &wrong_audience));
            let mut wrong_principal = owner.clone();
            wrong_principal.principal = format!("principal:sha256:{}", "b".repeat(64));
            assert_denied(verify(&db, "wi-1", capability.clone(), &wrong_principal));
            let mut wrong_session = owner.clone();
            wrong_session.session = "session-b".to_string();
            assert_denied(verify(&db, "wi-1", capability.clone(), &wrong_session));
            let mut wrong_epoch = owner.clone();
            wrong_epoch.authority_epoch = 8;
            assert_denied(verify(&db, "wi-1", capability.clone(), &wrong_epoch));
            let mut wrong_incarnation = owner.clone();
            wrong_incarnation.incarnation_id = "incarnation-b".to_string();
            assert_denied(verify(&db, "wi-1", capability.clone(), &wrong_incarnation));

            // Authoritative expiry, terminal state, and a reclaimed lease all
            // invalidate the previously minted capability.
            let expired = verify(&db, "wi-1", capability.clone(), &authority(110_000));
            assert_denied(expired);
            // A native retryable failure returns the row to ready, then the
            // native claim authority advances both lease epoch and fence.
            commit_native_result(&db, "wi-1", 1, 1, 10_001, "retry", "failed", true);
            claim_native(&db, "wi-1", "worker-a", 20_000, "reclaim");
            assert_denied(verify(&db, "wi-1", capability.clone(), &owner));
            // The new lease can be completed only through the native result
            // operation; generic CAS is no longer an authority path.
            commit_native_result(&db, "wi-1", 2, 2, 20_001, "terminal", "succeeded", false);
            assert_denied(verify(&db, "wi-1", capability.clone(), &owner));

            capability
        };

        // Capability and invocation rows are native durable state, not public
        // WorkItem metadata; reopening the same database still returns the
        // exact replay bytes and remains fail-closed after the lease changed.
        let db = open(&path);
        let replay_after_restart = mint(&db, "wi-1", &authority(10_001));
        assert_ne!(
            replay_after_restart.decision,
            WorkItemClaimCapabilityDecision::Minted
        );
        assert_ne!(
            replay_after_restart.capability,
            Some(capability.clone()),
            "the changed lease must not replay a stale capability"
        );
        let _ = std::fs::remove_file(path);
    }
}
