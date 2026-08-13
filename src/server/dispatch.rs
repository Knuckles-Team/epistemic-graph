//! The request dispatch shell: authentication, service-level methods, and the
//! graph-operation routing chain. Per-domain mutation kernels own their atomic
//! durability, audit, CDC, and projection publication.

use std::sync::Arc;
#[cfg(feature = "ast")]
use std::sync::OnceLock;
use std::time::Instant as DispatchLockInstant;
use tokio::sync::RwLock;
use tracing::info;

use super::access::{
    check_graph_access, is_admin_authz_action, require_admin_capability, requires_write,
    CarrierAuthority, GraphReadAuthority,
};
use super::auth::{
    verify_multisig_mutation_signatures, verify_register_identity_signature,
    verify_request_with_security_dir, VerifiedRequestContext,
};
// Only the ast-gated ParseFiles handler offloads to the blocking pool here; the
// graph-op off-lock sites live in handlers/graph_ops.rs.
#[cfg(feature = "ast")]
use super::compute::compute_off_lock;
use super::handlers;
use super::persistence::PersistenceBackend;
use super::state::ServerState;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Request, Response, ResultPayload};

// ── D-EIMG-2: the process-wide dispatch lock, instrumented at its chokepoint ──
//
// Every dispatched method acquires this one `Arc<RwLock<ServerState>>` before it can do
// anything. It is the single global serialization point in the engine, and it was
// completely uninstrumented: `epistemic_graph_write_lock_wait_seconds` covers only the
// PER-GRAPH topology lock inside the write coalescer, so nothing measured the wait here.
//
// These two helpers are the ONLY sanctioned way to take that lock inside this module, so
// that instrumentation cannot be bypassed by a future call site the way it would be if
// each of the ~50 acquisitions timed itself inline (and inline timing would also rot the
// moment someone adds acquisition 51). The timing window is exactly enqueue → guard
// acquired; the guard is returned unchanged, so borrow lifetimes at every call site are
// identical to a bare `timed_read(state).await`.
//
// Cost on the fast path is one `Instant::now()` pair per acquisition — the same
// instrumentation the write coalescer already pays per batch.

/// Acquire the process-wide `ServerState` read lock, recording the wait (D-EIMG-2).
pub(crate) async fn timed_read(
    state: &Arc<RwLock<ServerState>>,
) -> tokio::sync::RwLockReadGuard<'_, ServerState> {
    let started = DispatchLockInstant::now();
    let guard = state.read().await;
    crate::metrics::observe_dispatch_lock_wait("read", started.elapsed().as_secs_f64());
    guard
}

#[cfg(test)]
mod resource_status_privacy_tests {
    use super::*;
    use crate::epistemic_operations::{
        ResourceReservationHostCapacitySnapshot, ResourceReservationHostSnapshot,
        ResourceReservationHostSnapshotTargetKind, ResourceReservationStatusRequest,
        ResourceReservationStatusRequestSchemaVersion, ResourceReservationStatusResult,
        ResourceReservationStatusResultSchemaVersion, ResourceReservationSummary,
        ResourceReservationSummaryState,
    };

    fn request(host_ref: &str) -> ResourceReservationStatusRequest {
        ResourceReservationStatusRequest {
            schema_version: ResourceReservationStatusRequestSchemaVersion::V1,
            tenant_ref: "tenant-a".to_string(),
            work_item_id: None,
            reservation_id: None,
            host_ref: Some(host_ref.to_string()),
            owner_id: None,
            fence: None,
            attempt: None,
            lease_epoch: None,
            fencing_token: None,
            input_fingerprint: None,
            fairness_group: None,
            limit: 10,
            cursor: None,
            now_ms: 10,
        }
    }

    fn result(summary_host: &str) -> ResourceReservationStatusResult {
        ResourceReservationStatusResult {
            schema_version: ResourceReservationStatusResultSchemaVersion::V1,
            complete: true,
            next_cursor: None,
            host_snapshot: Some(ResourceReservationHostSnapshot {
                host_ref: "host-secret".to_string(),
                revision: 4,
                capacity: ResourceReservationHostCapacitySnapshot {
                    cpu_weight: 8,
                    memory_mib: 8_192,
                    disk_mib: 10_000,
                    process_slots: 4,
                },
                observed: ResourceReservationHostCapacitySnapshot {
                    cpu_weight: 1,
                    memory_mib: 1_024,
                    disk_mib: 100,
                    process_slots: 1,
                },
                heartbeat_at_ms: 9,
                heartbeat_ttl_ms: 120_000,
                draining: false,
                quarantined: false,
                labels: vec!["private".to_string()],
                target_kind: ResourceReservationHostSnapshotTargetKind::Local,
                target_alias: None,
                disk_used_mib: 100,
                disk_capacity_mib: 10_000,
                held_cpu_weight: 7,
                held_memory_mib: 700,
                held_disk_mib: 70,
                held_process_slots: 1,
                disk_policies: Vec::new(),
            }),
            host_ref: Some("host-secret".to_string()),
            host_revision: 4,
            held_cpu_weight: 7,
            held_memory_mib: 700,
            held_disk_mib: 70,
            held_process_slots: 1,
            fairness_debt: 7,
            reservations: vec![ResourceReservationSummary {
                reservation_id: "reservation-1".to_string(),
                work_item_id: "work-1".to_string(),
                attempt: 1,
                host_ref: summary_host.to_string(),
                profile_name: "light-check".to_string(),
                fairness_group: "default".to_string(),
                state: ResourceReservationSummaryState::Reserved,
                revision: 1,
                expires_at_ms: 100,
                held_cpu_weight: 2,
                held_memory_mib: 200,
                held_disk_mib: 20,
                held_process_slots: 1,
                tombstone: false,
            }],
            orphan_count: 0,
            superseded_count: 0,
        }
    }

    fn decode(payload: ResultPayload) -> ResourceReservationStatusResult {
        let ResultPayload::Raw(bytes) = payload else {
            panic!("status redaction must remain a typed raw result");
        };
        rmp_serde::from_slice(&bytes).expect("status result")
    }

    #[test]
    fn ordinary_reader_cannot_probe_unrelated_host_telemetry() {
        let redacted = decode(redact_resource_status_result(
            result("other-host"),
            &request("host-secret"),
            false,
        ));
        assert!(redacted.host_snapshot.is_none());
        assert!(redacted.host_ref.is_none());
        assert_eq!(redacted.host_revision, 0);
        assert_eq!(redacted.held_cpu_weight, 0);
        assert_eq!(redacted.held_memory_mib, 0);
        assert_eq!(redacted.held_disk_mib, 0);
        assert_eq!(redacted.held_process_slots, 0);
    }

    #[test]
    fn aggregate_reader_keeps_shared_host_totals_and_ordinary_relation_is_redacted() {
        let aggregate = decode(redact_resource_status_result(
            result("other-host"),
            &request("host-secret"),
            true,
        ));
        assert!(aggregate.host_snapshot.is_some());
        assert_eq!(aggregate.held_cpu_weight, 7);
        assert_eq!(
            aggregate
                .host_snapshot
                .as_ref()
                .expect("host snapshot")
                .held_cpu_weight,
            7
        );

        let related = decode(redact_resource_status_result(
            result("host-secret"),
            &request("host-secret"),
            false,
        ));
        assert!(related.host_snapshot.is_none());
        assert_eq!(related.held_cpu_weight, 0);
        assert_eq!(related.host_ref.as_deref(), Some("host-secret"));
    }
}

/// Acquire the process-wide `ServerState` write lock, recording the wait (D-EIMG-2).
pub(crate) async fn timed_write(
    state: &Arc<RwLock<ServerState>>,
) -> tokio::sync::RwLockWriteGuard<'_, ServerState> {
    let started = DispatchLockInstant::now();
    let guard = state.write().await;
    crate::metrics::observe_dispatch_lock_wait("write", started.elapsed().as_secs_f64());
    guard
}

// Nested MessagePack values ride inside outer `bin` fields, which are opaque to
// the transport's top-level grammar scan. Keep a second, tighter budget here so
// every request-controlled inner decoder receives a preflight before serde sees
// an attacker-controlled collection size hint.
const MAX_NESTED_MSGPACK_BYTES: usize = 64 * 1024 * 1024;
const MAX_NESTED_MSGPACK_ITEMS: usize = 1_000_000;
const MAX_SCREEN_OBSERVATION_BYTES: usize = 20 * 1024 * 1024;
const MAX_SCREEN_OBSERVATION_ITEMS: usize = 140_128;
const MAX_SCREEN_PNG_BYTES: usize = 16 * 1024 * 1024;
const MAX_SCREEN_ELEMENTS: usize = 10_000;
const MAX_SCREEN_SESSION_ID_BYTES: usize = 256;
const MAX_SCREEN_PREVIOUS_ID_BYTES: usize = 512;
const MAX_SCREEN_ROLE_BYTES: usize = 256;
const MAX_SCREEN_ELEMENT_NAME_BYTES: usize = 4_096;
const MAX_SCREEN_TOTAL_TEXT_BYTES: usize = 4 * 1024 * 1024;
const MAX_SCREEN_DIMENSION: u32 = 32_768;
const MAX_SCREEN_PIXELS: u64 = 100_000_000;
const MAX_SCREEN_COORDINATE_ABS: i64 = 10_000_000;

/// Keep the physical host ledger shared across tenants while making status
/// responses tenant-safe.  A controller with the explicit aggregate-read
/// capability may reconcile global capacity; an ordinary resource reader gets
/// host telemetry only when one of its returned reservations proves a relation
/// to that host, and never receives aggregate held totals.
fn redact_resource_status_result(
    mut result: crate::epistemic_operations::ResourceReservationStatusResult,
    request: &crate::epistemic_operations::ResourceReservationStatusRequest,
    aggregate_allowed: bool,
) -> ResultPayload {
    if !aggregate_allowed {
        let host_visible = request.host_ref.as_deref().is_some_and(|host_ref| {
            result
                .reservations
                .iter()
                .any(|reservation| reservation.host_ref == host_ref)
        });
        result.held_cpu_weight = 0;
        result.held_memory_mib = 0;
        result.held_disk_mib = 0;
        result.held_process_slots = 0;
        if !host_visible {
            result.host_snapshot = None;
            result.host_ref = None;
            result.host_revision = 0;
        } else {
            // A tenant-visible reservation proves only a relation to this
            // physical host. It does not authorize probing private inventory
            // labels/aliases or shared capacity/telemetry.  Do not construct
            // a zeroed pseudo-snapshot: heartbeat TTL and target identity have
            // schema invariants, and an invalid redacted object is worse than
            // an omitted one. Aggregate reconciliation is the explicit
            // controller capability below.
            result.host_snapshot = None;
        }
    }
    ResultPayload::raw(&result)
}

// ── Fleet server registry (CONCEPT:EG-KG.sharding.server-registry, W2.5) ──────────
// `RegisterServer.name` mirrors au's `_SERVER_NAME` bound
// (`agent_utilities/knowledge_graph/core/engine_mcp_discovery.py`) so the SAME
// name is a valid node-id suffix on both the au config-sync path and this
// engine-native push-registration path.
const MAX_REGISTER_SERVER_NAME_BYTES: usize = 128;
// `url` is an opaque endpoint reference (never a raw credentialed URL -- callers
// pass the same kind of privacy-safe reference au's `persistence_reference`
// produces), bounded generously for a reference string.
const MAX_REGISTER_SERVER_URL_BYTES: usize = 2_048;
// `resources_json` is non-sensitive opaque metadata (mirrors au's
// `_mcp_persistence_resources`), bounded well under the msgpack node-property cap.
const MAX_REGISTER_SERVER_RESOURCES_BYTES: usize = 16 * 1024;
// Lease bounds: at least 1 second, at most 24 hours -- a caller renews well inside
// this window (the stale-lease reaper never waits longer than the registered TTL).
const MIN_REGISTER_SERVER_TTL_SECS: u64 = 1;
const MAX_REGISTER_SERVER_TTL_SECS: u64 = 24 * 60 * 60;

#[cfg(feature = "raft")]
#[derive(Clone, Copy)]
struct ReplicatedApplyScope {
    committed_at_ms: u64,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    identity_bootstrap: bool,
}

#[cfg(feature = "raft")]
tokio::task_local! {
    static REPLICATED_APPLY: ReplicatedApplyScope;
}

#[cfg(feature = "raft")]
pub(crate) fn is_replicated_apply() -> bool {
    REPLICATED_APPLY.try_with(|_| ()).is_ok()
}

/// One authoritative clock for a replicated native command. Followers must not
/// sample their local wall clocks while applying the same committed entry.
pub(crate) fn authoritative_now_ms() -> u64 {
    #[cfg(feature = "raft")]
    if let Ok(value) = REPLICATED_APPLY.try_with(|scope| scope.committed_at_ms) {
        return value;
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) fn authoritative_now_secs() -> u64 {
    authoritative_now_ms() / 1_000
}

/// Civil (proleptic Gregorian) `(year, month, day)` from a days-since-1970-01-01
/// count -- Howard Hinnant's `civil_from_days`, the SAME proven, dependency-free
/// algorithm `eg-rdf`'s `sparql::civil_from_days` already uses for XSD `dateTime`
/// formatting (deliberately re-derived here rather than imported: the facade
/// does not otherwise depend on `eg-rdf` internals, and this is a small, fully
/// self-contained pure function).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Render `unix_secs` as `%Y-%m-%dT%H:%M:%SZ` -- the SAME format au's
/// `engine_ingestion.ingest_mcp_server`/`engine_mcp_discovery.check_server_freshness`
/// already read/write for a `:Server` node's `timestamp` field, so an
/// engine-registered server stays readable by the existing au freshness check.
fn format_iso8601_seconds(unix_secs: u64) -> String {
    let secs = unix_secs as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// `RegisterServer.name` validity -- mirrors au's `_SERVER_NAME` regex
/// (`^[A-Za-z0-9_.-]{1,128}$`) byte-for-byte so the same name is valid on both
/// the au config-sync path and this engine-native push-registration path.
fn valid_register_server_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_REGISTER_SERVER_NAME_BYTES
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// `Method::RegisterServer`'s handler (CONCEPT:EG-KG.sharding.server-registry, W2.5):
/// validate, compute the server-authoritative lease fields, build the `:Server`
/// property blob (preserving `registered_at_ms` across a renewal -- a heartbeat
/// is just a repeat call with the same `name`), and delegate to the ordinary
/// graph gateway via a translated `Method::AddNode` against `__commons__` --
/// see the `Method::RegisterServer` doc comment in `protocol.rs` and
/// `server::mutation::NON_GATEWAY_COORDINATED`'s `RegisterServer` entry. Never
/// trusts a caller-supplied timestamp: every lease field is derived from
/// [`authoritative_now_ms`].
// Mirrors `build_envelope_v2_bytes` (protocol.rs): a wire-marshaling function
// over genuinely-required distinct fields, with no natural grouping that
// wouldn't just be a single-use wrapper struct.
#[allow(clippy::too_many_arguments)]
async fn handle_register_server(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    name: String,
    url: String,
    resources_json: String,
    ttl_secs: u64,
) -> Response {
    if !valid_register_server_name(&name) {
        return Response::err(
            req_id,
            "RegisterServer.name must be a bounded logical name (^[A-Za-z0-9_.-]{1,128}$)",
        );
    }
    if url.is_empty() || url.len() > MAX_REGISTER_SERVER_URL_BYTES {
        return Response::err(req_id, "RegisterServer.url exceeds resource limits");
    }
    if resources_json.len() > MAX_REGISTER_SERVER_RESOURCES_BYTES {
        return Response::err(
            req_id,
            "RegisterServer.resources_json exceeds resource limits",
        );
    }
    let resources = if resources_json.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        match serde_json::from_str::<serde_json::Value>(&resources_json) {
            Ok(value @ serde_json::Value::Object(_)) => value,
            _ => {
                return Response::err(
                    req_id,
                    "RegisterServer.resources_json must be a JSON object",
                )
            }
        }
    };
    if !(MIN_REGISTER_SERVER_TTL_SECS..=MAX_REGISTER_SERVER_TTL_SECS).contains(&ttl_secs) {
        return Response::err(
            req_id,
            format!(
                "RegisterServer.ttl_secs must be between {MIN_REGISTER_SERVER_TTL_SECS} and \
                 {MAX_REGISTER_SERVER_TTL_SECS}"
            ),
        );
    }

    let node_id = format!("srv:{name}");
    let now_ms = authoritative_now_ms();
    let lease_expires_at_ms = now_ms.saturating_add(ttl_secs.saturating_mul(1_000));

    // Preserve `registered_at_ms` across a renewal by peeking at any existing row
    // -- read-only, off the always-resident `__commons__` core, never a
    // durability-relevant read (a race with a concurrent first-registration at
    // worst repeats `now_ms`, never loses data).
    let registered_at_ms = {
        let s = timed_read(state).await;
        s.registry
            .get("__commons__")
            .and_then(|entry| entry.core.get_node_properties(&node_id))
            .and_then(|blob| eg_types::msgpack::decode_property_value(&blob).ok())
            .and_then(|value| value.get("registered_at_ms").and_then(|v| v.as_u64()))
            .unwrap_or(now_ms)
    };

    let properties = serde_json::json!({
        "node_type": "Server",
        "name": name,
        "url": url,
        "resources": resources,
        "timestamp": format_iso8601_seconds(now_ms / 1_000),
        "ttl_secs": ttl_secs,
        "registered_at_ms": registered_at_ms,
        "last_heartbeat_ms": now_ms,
        "lease_expires_at_ms": lease_expires_at_ms,
    });
    let properties_msgpack = match rmp_serde::to_vec_named(&properties) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Response::err(
                req_id,
                format!("RegisterServer payload encode failed: {error}"),
            )
        }
    };

    dispatch_graph_op(
        state,
        "__commons__",
        req_id,
        caller,
        verified_context,
        Method::AddNode {
            node_id,
            properties_msgpack,
        },
    )
    .await
}

#[cfg(feature = "raft")]
pub(crate) fn replicated_placement_authority() -> Option<(u64, Option<u64>)> {
    REPLICATED_APPLY
        .try_with(|scope| (scope.placement_epoch, scope.fencing_token))
        .ok()
}

#[cfg(feature = "raft")]
fn replicated_identity_bootstrap_authorized() -> bool {
    REPLICATED_APPLY
        .try_with(|scope| scope.identity_bootstrap)
        .unwrap_or(false)
}

#[cfg(feature = "raft")]
fn capability_authority_unavailable(method: &Method) -> bool {
    matches!(
        method,
        Method::MintWorkItemClaimCapability { .. } | Method::VerifyWorkItemClaimCapability { .. }
    )
}

#[cfg(not(feature = "raft"))]
fn replicated_identity_bootstrap_authorized() -> bool {
    false
}

/// Apply a committed bounded native command through its existing domain kernel.
/// Authentication/authorization has already happened before proposal; the
/// reconstructed context contains only one-way tenant/principal scopes so no raw
/// identity enters the Raft log or snapshot.
#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_native(
    state: &Arc<RwLock<ServerState>>,
    graph: String,
    request_id: u64,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    method: Method,
) -> Response {
    if capability_authority_unavailable(&method) {
        // RaftMutationContext intentionally contains only one-way routing
        // identity.  It is not an authenticated principal/session envelope,
        // so never reconstruct capability authority on a follower/replay.
        return Response::err(
            request_id,
            crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE,
        );
    }
    let context = match VerifiedRequestContext::replicated_mutation(authority) {
        Ok(context) => context,
        Err(error) => return Response::err(request_id, error),
    };
    let request = Request {
        id: request_id,
        graph,
        auth_token: String::new(),
        agent_id: None,
        method,
    };
    REPLICATED_APPLY
        .scope(
            ReplicatedApplyScope {
                committed_at_ms,
                placement_epoch: authority.placement_epoch,
                fencing_token: authority.fencing_token,
                identity_bootstrap: authority.identity_bootstrap,
            },
            dispatch_with_context(state, request, Some(context)),
        )
        .await
}

#[cfg(feature = "raft")]
fn replicated_apply_scope(
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
) -> ReplicatedApplyScope {
    ReplicatedApplyScope {
        committed_at_ms,
        placement_epoch: authority.placement_epoch,
        fencing_token: authority.fencing_token,
        identity_bootstrap: authority.identity_bootstrap,
    }
}

/// The identifying fields of a replicated transaction participant, bundled so
/// [`apply_replicated_transaction_participant`] stays under the clippy
/// argument-count ceiling.
#[cfg(feature = "raft")]
pub(crate) struct ReplicatedParticipantRef<'a> {
    pub(crate) coordinator_id: &'a str,
    pub(crate) participant_id: u64,
    pub(crate) plan: Option<&'a [u8]>,
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_transaction_participant(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    applying_group: crate::raft::GroupId,
    phase: crate::raft::TransactionParticipantPhase,
    participant: ReplicatedParticipantRef<'_>,
) -> Result<bool, String> {
    let ReplicatedParticipantRef {
        coordinator_id,
        participant_id,
        plan,
    } = participant;
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            match phase {
                crate::raft::TransactionParticipantPhase::Prepare => {
                    handlers::txn::apply_consensus_participant_prepare(
                        state,
                        applying_group,
                        authority.placement_epoch,
                        authority.fencing_token,
                        coordinator_id,
                        participant_id,
                        plan.ok_or_else(|| "participant prepare is missing its plan".to_string())?,
                    )
                    .await
                }
                crate::raft::TransactionParticipantPhase::Commit => {
                    handlers::txn::apply_consensus_participant_commit(
                        state,
                        request_id,
                        applying_group,
                        authority,
                        handlers::txn::ConsensusParticipantCommitRef {
                            coordinator_id,
                            participant_id,
                            plan_bytes: plan.ok_or_else(|| {
                                "participant commit is missing its plan".to_string()
                            })?,
                        },
                    )
                    .await
                }
                crate::raft::TransactionParticipantPhase::Abort => {
                    handlers::txn::apply_consensus_participant_abort(
                        state,
                        coordinator_id,
                        participant_id,
                    )
                    .await
                }
            }
        })
        .await
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_transaction_prepare(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    txn_id: &str,
) -> Response {
    REPLICATED_APPLY
        .scope(
            replicated_apply_scope(committed_at_ms, authority),
            handlers::txn::prepare_consensus_commit(
                state,
                request_id,
                Some(&authority.principal_fingerprint),
                txn_id,
            ),
        )
        .await
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_transaction_decision(
    state: &Arc<RwLock<ServerState>>,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    coordinator_id: &str,
    commit: bool,
) -> Result<bool, String> {
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            handlers::txn::apply_consensus_transaction_decision(
                state,
                coordinator_id,
                &authority.principal_fingerprint,
                commit,
            )
            .await
        })
        .await
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_replicated_transaction_finalize(
    state: &Arc<RwLock<ServerState>>,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    coordinator_id: &str,
    commit: bool,
) -> Result<bool, String> {
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            handlers::txn::apply_consensus_transaction_finalize(
                state,
                coordinator_id,
                &authority.principal_fingerprint,
                commit,
            )
            .await
        })
        .await
}

#[cfg(all(feature = "raft", feature = "jobs"))]
pub(crate) async fn apply_replicated_job_publication_commit(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    applying_group: crate::raft::GroupId,
    coordinator_id: &str,
    plan: &[u8],
) -> Result<bool, String> {
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            handlers::jobs::apply_consensus_job_publication_commit(
                state,
                request_id,
                authority,
                applying_group,
                coordinator_id,
                plan,
            )
            .await
        })
        .await
}

#[cfg(all(feature = "raft", feature = "jobs"))]
pub(crate) async fn apply_replicated_job_publication_finalize(
    state: &Arc<RwLock<ServerState>>,
    committed_at_ms: u64,
    authority: &crate::raft::RaftMutationContext,
    coordinator_id: &str,
    receipt: &[u8],
) -> Result<ResultPayload, String> {
    REPLICATED_APPLY
        .scope(replicated_apply_scope(committed_at_ms, authority), async {
            handlers::jobs::apply_consensus_job_publication_finalize(
                state,
                committed_at_ms,
                coordinator_id,
                receipt,
            )
            .await
        })
        .await
}

#[cfg(feature = "raft")]
fn native_route_target(
    request_graph: &str,
    tenant_scope: &str,
    method: &Method,
    command: &crate::raft::NativeMutationCommand,
) -> String {
    use crate::raft::NativeMutationCommand;
    match command {
        NativeMutationCommand::GraphState { .. } | NativeMutationCommand::Multisig { .. } => {
            request_graph.to_string()
        }
        NativeMutationCommand::GraphLifecycle { .. } => match method {
            Method::CreateGraph { graph_name, .. } | Method::DeleteGraph { graph_name } => {
                graph_name.clone()
            }
            _ => request_graph.to_string(),
        },
        NativeMutationCommand::ClusterAdmin { .. } => {
            crate::raft::placement::PLACEMENT_GRAPH.to_string()
        }
        NativeMutationCommand::ChangeEnvelope { .. } => request_graph.to_string(),
        #[cfg(feature = "modality-serving")]
        NativeMutationCommand::ServedModality { .. } => request_graph.to_string(),
        NativeMutationCommand::Transaction { .. } => {
            crate::raft::placement::PLACEMENT_GRAPH.to_string()
        }
        NativeMutationCommand::TransactionParticipant { .. }
        | NativeMutationCommand::TransactionDecision { .. }
        | NativeMutationCommand::TransactionFinalize { .. } => request_graph.to_string(),
        #[cfg(feature = "jobs")]
        NativeMutationCommand::JobPublicationCommit { .. }
        | NativeMutationCommand::JobPublicationFinalize { .. } => request_graph.to_string(),
        NativeMutationCommand::WorkItem { .. } => request_graph.to_string(),
        #[cfg(feature = "blob")]
        NativeMutationCommand::Blob { .. } => {
            crate::server::mutation_batch::opaque_coordinator_key(
                "raft-native-blob",
                tenant_scope,
                "control",
            )
        }
        #[cfg(feature = "kv")]
        NativeMutationCommand::KeyValue { .. } => {
            crate::server::mutation_batch::opaque_coordinator_key(
                "raft-native-kv",
                tenant_scope,
                "control",
            )
        }
        #[cfg(feature = "tsdb")]
        NativeMutationCommand::TimeSeries { .. } => {
            crate::server::mutation_batch::opaque_coordinator_key(
                "raft-native-timeseries",
                tenant_scope,
                "control",
            )
        }
        #[cfg(feature = "jobs")]
        NativeMutationCommand::AnalyticsJob { .. } => {
            crate::server::mutation_batch::opaque_coordinator_key(
                "raft-native-jobs",
                tenant_scope,
                "control",
            )
        }
        #[cfg(feature = "sqlite-file")]
        NativeMutationCommand::SqliteCatalog { .. } => {
            crate::server::mutation_batch::opaque_coordinator_key(
                "raft-native-sqlite",
                tenant_scope,
                "catalog",
            )
        }
        NativeMutationCommand::SessionControl { .. } => {
            crate::raft::placement::PLACEMENT_GRAPH.to_string()
        }
        // Identity/RBAC state is process-global authority. Every such command must
        // therefore share one Raft order; tenant-hashed groups could apply
        // concurrent add/remove transitions in different orders on each replica.
        // `__commons__` also preserves the exact bootstrap route asserted before
        // proposal and revalidated by `RaftRequest::validate`.
        NativeMutationCommand::Identity { .. } => "__commons__".to_string(),
    }
}

#[cfg(all(test, feature = "raft"))]
mod consensus_admin_route_tests {
    use super::*;

    #[test]
    fn coordinator_ledgers_are_ordered_by_the_placement_group() {
        let auth_material = "consensus-admin-route-test";
        for method in [
            Method::BeginTxn {
                graph: Some("caller-graph".to_string()),
                isolation: None,
            },
            Method::Commit {
                txn_id: "opaque-txn".to_string(),
            },
            Method::CreateChannel {
                channel_id: "opaque-channel".to_string(),
                channel_type: crate::protocol::ChannelType::PeerToPeer,
                creator: "opaque-creator".to_string(),
                initial_members: Vec::new(),
            },
        ] {
            let command = crate::raft::NativeMutationCommand::from_public_method(
                method.clone(),
                auth_material,
            )
            .expect("method has a typed consensus command");
            assert_eq!(
                native_route_target("caller-graph", "tenant-scope", &method, &command),
                crate::raft::placement::PLACEMENT_GRAPH
            );
        }
    }

    #[test]
    fn identity_and_rbac_commands_share_the_commons_consensus_order() {
        let method = Method::RbacAdmin {
            op: crate::acl::RbacAdminOp::List,
        };
        let command = crate::raft::NativeMutationCommand::from_public_method(
            method.clone(),
            "consensus-identity-route-test",
        )
        .unwrap();
        assert_eq!(
            native_route_target("caller-graph", "tenant-scope", &method, &command),
            "__commons__"
        );
    }

    #[test]
    fn capability_consensus_paths_refuse_without_the_original_auth_envelope() {
        use crate::epistemic_operations::{
            WorkItemClaimCapabilityMintRequest, WorkItemClaimCapabilityRequestSchemaVersion,
            WorkItemClaimCapabilityVerifyRequest,
        };

        let methods = [
            Method::MintWorkItemClaimCapability {
                request: WorkItemClaimCapabilityMintRequest {
                    schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                    work_item_id: "work-item".to_string(),
                },
            },
            Method::VerifyWorkItemClaimCapability {
                request: WorkItemClaimCapabilityVerifyRequest {
                    schema_version: WorkItemClaimCapabilityRequestSchemaVersion::V1,
                    work_item_id: "work-item".to_string(),
                    capability: vec![0; 36],
                },
            },
        ];
        for method in methods {
            assert!(capability_authority_unavailable(&method));
            assert_eq!(
                crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE,
                "authority_unavailable"
            );
        }
    }
}

#[cfg(feature = "raft")]
async fn propose_native_mutation(
    state: &Arc<RwLock<ServerState>>,
    request_graph: &str,
    request_id: u64,
    verified_context: &VerifiedRequestContext,
    identity_bootstrap: bool,
    method: Method,
) -> Response {
    if capability_authority_unavailable(&method) {
        // The proposal payload carries no raw authenticated principal/session
        // envelope.  Refuse before CarrierAuthority, command construction,
        // leader routing, barriers, or any private-store mutation.
        return Response::err(
            request_id,
            crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE,
        );
    }
    let authority = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(error) => return Response::err(request_id, error),
    };
    let method = match sanitize_native_proposal(request_graph, verified_context, &authority, method)
    {
        Ok(method) => method,
        Err(error) => return Response::err(request_id, error),
    };
    let is_transaction_commit = matches!(&method, Method::Commit { .. });
    #[cfg(feature = "jobs")]
    let is_job_publication = matches!(
        &method,
        Method::AnalyticsJob {
            op: eg_types::jobs::JobOp::WorkerPublish { .. }
        }
    );
    let server_secret = timed_read(state).await.auth_secret.clone();
    let command = match crate::raft::NativeMutationCommand::from_public_method(
        method.clone(),
        &server_secret,
    ) {
        Ok(command) => command,
        Err(_) => {
            return Response::err(
                request_id,
                "CLUSTER_MUTATION_UNAVAILABLE: no bounded native command exists",
            )
        }
    };
    let graph_name =
        native_route_target(request_graph, authority.tenant_scope(), &method, &command);
    let (multi, graph_type) = {
        let current = timed_read(state).await;
        let graph_type = match &method {
            Method::CreateGraph { graph_type, .. } => *graph_type,
            _ => current
                .registry
                .get(&graph_name)
                .map(|entry| entry.graph_type)
                .unwrap_or(crate::protocol::GraphType::Global),
        };
        (current.multi_raft.clone(), graph_type)
    };
    let Some(multi) = multi else {
        return Response::err(
            request_id,
            "CLUSTER_CONFIGURATION_INVALID: MultiRaft placement authority is required",
        );
    };
    let Some(routed) = multi.handle_for_graph(&graph_name).await else {
        let route = multi.route_graph(&graph_name).await;
        return Response::stale_route(
            request_id,
            &graph_name,
            route.group,
            route.epoch,
            None,
            "authoritative native placement group is not running on this node",
        );
    };
    let committed_at_ms = authoritative_now_ms();
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "raft-native",
        &graph_name,
        request_id,
        &method,
    );
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        request_id,
        authority.tenant_scope(),
        authority.actor_scope().to_string(),
        identity_bootstrap,
        routed.epoch,
        routed.placed.then_some(routed.group_id),
        committed_at_ms,
    ) {
        Ok(mutation) => mutation,
        Err(error) => return Response::err(request_id, error),
    };
    let request = crate::raft::RaftRequest {
        graph_fname: crate::persist::sanitize(&graph_name),
        graph_name: graph_name.clone(),
        graph_type,
        command: crate::raft::ReplicatedMutation::Native { command },
        committed_at_ms,
        mutation,
    };
    match routed.handle.client_write(request).await {
        Ok(response) => {
            if let Some(error) = response.native_error {
                Response::err(request_id, error)
            } else if let Some(result) = response.native_result {
                #[cfg(feature = "jobs")]
                if is_job_publication {
                    match result {
                        ResultPayload::Raw(prepared) => {
                            return execute_consensus_job_publication(
                                request_id,
                                &authority,
                                &server_secret,
                                multi,
                                routed,
                                &graph_name,
                                graph_type,
                                &prepared,
                            )
                            .await;
                        }
                        terminal => return Response::ok(request_id, terminal),
                    }
                }
                if is_transaction_commit {
                    match result {
                        ResultPayload::Raw(prepared) => {
                            return execute_consensus_transaction(
                                state,
                                request_id,
                                &authority,
                                &server_secret,
                                multi,
                                routed,
                                &graph_name,
                                graph_type,
                                &prepared,
                            )
                            .await;
                        }
                        terminal => return Response::ok(request_id, terminal),
                    }
                }
                Response::ok(request_id, result)
            } else {
                Response::err(
                    request_id,
                    "replicated native command returned no deterministic result",
                )
            }
        }
        Err(error) => {
            let leader = routed.handle.current_leader().await;
            Response::stale_route(
                request_id,
                &graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                error,
            )
        }
    }
}

#[cfg(all(feature = "raft", feature = "jobs"))]
#[allow(clippy::too_many_arguments)]
async fn submit_consensus_job_publication_command(
    multi: &Arc<crate::raft::multi::MultiRaft>,
    authority: &CarrierAuthority,
    request_id: u64,
    coordinator_id: &str,
    operation: &str,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    command: crate::raft::NativeMutationCommand,
) -> Result<ResultPayload, String> {
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "raft-job-publication-command",
        coordinator_id,
        operation,
    );
    let committed_at_ms = authoritative_now_ms();
    let mutation = crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        request_id,
        authority.tenant_scope(),
        authority.actor_scope().to_string(),
        false,
        placement_epoch,
        fencing_token,
        committed_at_ms,
    )?;
    let request = crate::raft::RaftRequest {
        graph_fname: crate::persist::sanitize(graph_name),
        graph_name: graph_name.to_string(),
        graph_type,
        command: crate::raft::ReplicatedMutation::Native { command },
        committed_at_ms,
        mutation,
    };
    let response = multi.client_write_group(group_id, request).await?;
    if let Some(error) = response.native_error {
        return Err(error);
    }
    response
        .native_result
        .ok_or_else(|| "job publication command returned no result".to_string())
}

#[cfg(all(feature = "raft", feature = "jobs"))]
#[allow(clippy::too_many_arguments)]
async fn execute_consensus_job_publication(
    request_id: u64,
    authority: &CarrierAuthority,
    server_secret: &str,
    multi: Arc<crate::raft::multi::MultiRaft>,
    control: crate::raft::multi::RoutedRaftHandle,
    control_graph: &str,
    control_graph_type: crate::protocol::GraphType,
    prepared_bytes: &[u8],
) -> Response {
    let prepared = match handlers::jobs::decode_prepared_job_publication(prepared_bytes) {
        Ok(prepared) => prepared,
        Err(error) => return Response::err(request_id, error),
    };
    let target_route = multi.route_graph(&prepared.target_graph).await;
    let target_fence = target_route.placed.then_some(target_route.fencing_token());
    let (commit_plan, finalize_receipt) = match handlers::jobs::build_job_publication_commands(
        prepared.clone(),
        target_route.group,
        target_route.epoch,
        target_fence,
    ) {
        Ok(plans) => plans,
        Err(error) => return Response::err(request_id, error),
    };
    let commit = match crate::raft::NativeMutationCommand::job_publication_commit(
        prepared.coordinator_id.clone(),
        &commit_plan,
        server_secret,
    ) {
        Ok(command) => command,
        Err(error) => return Response::err(request_id, error),
    };
    match submit_consensus_job_publication_command(
        &multi,
        authority,
        request_id,
        &prepared.coordinator_id,
        "target-commit",
        &prepared.target_graph,
        prepared.target_graph_type,
        target_route.group,
        target_route.epoch,
        target_fence,
        commit,
    )
    .await
    {
        Ok(ResultPayload::Bool(true)) => {}
        Ok(ResultPayload::Bool(false)) => {
            return Response::err(request_id, "job publication target rejected commit")
        }
        Ok(_) => {
            return Response::err(request_id, "job publication target returned invalid result")
        }
        Err(error) => {
            return Response::err(
                request_id,
                format!("job publication target commit failed: {error}"),
            )
        }
    }

    let finalize = match crate::raft::NativeMutationCommand::job_publication_finalize(
        prepared.coordinator_id.clone(),
        &finalize_receipt,
        server_secret,
    ) {
        Ok(command) => command,
        Err(error) => return Response::err(request_id, error),
    };
    let control_fence = control.placed.then_some(control.fencing_token());
    match submit_consensus_job_publication_command(
        &multi,
        authority,
        request_id,
        &prepared.coordinator_id,
        "scheduler-finalize",
        control_graph,
        control_graph_type,
        control.group_id,
        control.epoch,
        control_fence,
        finalize,
    )
    .await
    {
        Ok(result) => Response::ok(request_id, result),
        Err(error) => Response::err(
            request_id,
            format!("job publication finalization failed: {error}"),
        ),
    }
}

#[cfg(feature = "raft")]
#[allow(clippy::too_many_arguments)]
async fn submit_consensus_transaction_command(
    multi: &Arc<crate::raft::multi::MultiRaft>,
    authority: &CarrierAuthority,
    request_id: u64,
    coordinator_id: &str,
    operation: &str,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    graph_type: crate::protocol::GraphType,
    command: crate::raft::NativeMutationCommand,
) -> Result<bool, String> {
    let route_key = crate::server::mutation_batch::opaque_coordinator_key(
        "raft-consensus-transaction-route",
        coordinator_id,
        operation,
    );
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "raft-consensus-transaction-command",
        coordinator_id,
        operation,
    );
    let committed_at_ms = authoritative_now_ms();
    let mutation = crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        request_id,
        authority.tenant_scope(),
        authority.actor_scope().to_string(),
        false,
        placement_epoch,
        fencing_token,
        committed_at_ms,
    )?;
    let request = crate::raft::RaftRequest {
        graph_fname: crate::persist::sanitize(&route_key),
        graph_name: route_key,
        graph_type,
        command: crate::raft::ReplicatedMutation::Native { command },
        committed_at_ms,
        mutation,
    };
    let response = multi.client_write_group(group_id, request).await?;
    if let Some(error) = response.native_error {
        return Err(error);
    }
    match response.native_result {
        Some(ResultPayload::Bool(value)) => Ok(value),
        _ => Err("consensus transaction command returned an invalid result".to_string()),
    }
}

#[cfg(feature = "raft")]
#[allow(clippy::too_many_arguments)]
async fn submit_consensus_transaction_decision(
    multi: &Arc<crate::raft::multi::MultiRaft>,
    authority: &CarrierAuthority,
    request_id: u64,
    coordinator_id: &str,
    control_group: crate::raft::GroupId,
    control_epoch: u64,
    control_fence: Option<u64>,
    graph_type: crate::protocol::GraphType,
    commit: bool,
) -> Result<bool, String> {
    submit_consensus_transaction_command(
        multi,
        authority,
        request_id,
        coordinator_id,
        if commit {
            "decision-commit"
        } else {
            "decision-abort"
        },
        control_group,
        control_epoch,
        control_fence,
        graph_type,
        crate::raft::NativeMutationCommand::TransactionDecision {
            coordinator_id: coordinator_id.to_string(),
            commit,
        },
    )
    .await
}

#[cfg(feature = "raft")]
#[allow(clippy::too_many_arguments)]
async fn submit_consensus_transaction_finalize(
    multi: &Arc<crate::raft::multi::MultiRaft>,
    authority: &CarrierAuthority,
    request_id: u64,
    coordinator_id: &str,
    control_group: crate::raft::GroupId,
    control_epoch: u64,
    control_fence: Option<u64>,
    graph_type: crate::protocol::GraphType,
    commit: bool,
) -> Result<bool, String> {
    submit_consensus_transaction_command(
        multi,
        authority,
        request_id,
        coordinator_id,
        if commit {
            "finalize-commit"
        } else {
            "finalize-abort"
        },
        control_group,
        control_epoch,
        control_fence,
        graph_type,
        crate::raft::NativeMutationCommand::TransactionFinalize {
            coordinator_id: coordinator_id.to_string(),
            commit,
        },
    )
    .await
}

#[cfg(feature = "raft")]
#[allow(clippy::too_many_arguments)]
async fn abort_consensus_transaction(
    multi: &Arc<crate::raft::multi::MultiRaft>,
    authority: &CarrierAuthority,
    request_id: u64,
    server_secret: &str,
    coordinator_id: &str,
    participants: &[crate::server::handlers::txn::ConsensusTransactionParticipant],
    control_group: crate::raft::GroupId,
    control_epoch: u64,
    control_fence: Option<u64>,
    control_graph_type: crate::protocol::GraphType,
) -> Result<bool, String> {
    let decided = submit_consensus_transaction_decision(
        multi,
        authority,
        request_id,
        coordinator_id,
        control_group,
        control_epoch,
        control_fence,
        control_graph_type,
        false,
    )
    .await?;
    if decided {
        return Err("consensus transaction abort received a commit decision".to_string());
    }
    // Abort every participant in the frozen fanout, including a participant whose
    // PREPARE reply was lost after its command committed. The abort command is
    // idempotent when no durable intent exists.
    for participant in participants {
        let command = crate::raft::NativeMutationCommand::transaction_participant(
            crate::raft::TransactionParticipantPhase::Abort,
            coordinator_id.to_string(),
            participant.participant_id,
            None,
            server_secret,
        )?;
        let operation = format!("abort-{}", participant.participant_id);
        if !submit_consensus_transaction_command(
            multi,
            authority,
            request_id,
            coordinator_id,
            &operation,
            participant.group_id,
            participant.placement_epoch,
            participant.fencing_token,
            participant.graph_type,
            command,
        )
        .await?
        {
            return Err("consensus participant abort was not applied".to_string());
        }
    }
    submit_consensus_transaction_finalize(
        multi,
        authority,
        request_id,
        coordinator_id,
        control_group,
        control_epoch,
        control_fence,
        control_graph_type,
        false,
    )
    .await
}

#[cfg(feature = "raft")]
#[allow(clippy::too_many_arguments)]
async fn execute_consensus_transaction(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    authority: &CarrierAuthority,
    server_secret: &str,
    multi: Arc<crate::raft::multi::MultiRaft>,
    control: crate::raft::multi::RoutedRaftHandle,
    _control_graph: &str,
    control_graph_type: crate::protocol::GraphType,
    prepared_bytes: &[u8],
) -> Response {
    let fanout =
        match handlers::txn::build_consensus_transaction_fanout(state, prepared_bytes).await {
            Ok(fanout) => fanout,
            Err(error) => return Response::err(request_id, error),
        };
    let control_fence = control.placed.then_some(control.group_id);
    for participant in &fanout.participants {
        let command = match crate::raft::NativeMutationCommand::transaction_participant(
            crate::raft::TransactionParticipantPhase::Prepare,
            fanout.coordinator_id.clone(),
            participant.participant_id,
            Some(&participant.sealed_plan_source),
            server_secret,
        ) {
            Ok(command) => command,
            Err(error) => return Response::err(request_id, error),
        };
        let operation = format!("prepare-{}", participant.participant_id);
        match submit_consensus_transaction_command(
            &multi,
            authority,
            request_id,
            &fanout.coordinator_id,
            &operation,
            participant.group_id,
            participant.placement_epoch,
            participant.fencing_token,
            participant.graph_type,
            command,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                return match abort_consensus_transaction(
                    &multi,
                    authority,
                    request_id,
                    server_secret,
                    &fanout.coordinator_id,
                    &fanout.participants,
                    control.group_id,
                    control.epoch,
                    control_fence,
                    control_graph_type,
                )
                .await
                {
                    Ok(false) => Response::ok(request_id, ResultPayload::Bool(false)),
                    Ok(true) => Response::err(request_id, "consensus abort finalized as commit"),
                    Err(error) => Response::err(request_id, error),
                };
            }
            Err(error) => {
                let cleanup = abort_consensus_transaction(
                    &multi,
                    authority,
                    request_id,
                    server_secret,
                    &fanout.coordinator_id,
                    &fanout.participants,
                    control.group_id,
                    control.epoch,
                    control_fence,
                    control_graph_type,
                )
                .await;
                return match cleanup {
                    Ok(false) => Response::err(
                        request_id,
                        format!("consensus participant prepare failed: {error}"),
                    ),
                    Ok(true) => Response::err(request_id, "consensus abort finalized as commit"),
                    Err(cleanup_error) => Response::err(
                        request_id,
                        format!(
                            "consensus participant prepare failed: {error}; abort failed: {cleanup_error}"
                        ),
                    ),
                };
            }
        }
    }

    match submit_consensus_transaction_decision(
        &multi,
        authority,
        request_id,
        &fanout.coordinator_id,
        control.group_id,
        control.epoch,
        control_fence,
        control_graph_type,
        true,
    )
    .await
    {
        Ok(true) => {}
        Ok(false) => return Response::err(request_id, "consensus transaction was durably aborted"),
        Err(error) => {
            // A prior retry may already have decided ABORT. Conversely, if the
            // COMMIT reply was merely lost, the abort decision will conflict and
            // preserve the durable COMMIT. Either way this cleanup cannot reverse a
            // recorded outcome.
            return match abort_consensus_transaction(
                &multi,
                authority,
                request_id,
                server_secret,
                &fanout.coordinator_id,
                &fanout.participants,
                control.group_id,
                control.epoch,
                control_fence,
                control_graph_type,
            )
            .await
            {
                Ok(false) => Response::err(
                    request_id,
                    format!("consensus transaction was durably aborted: {error}"),
                ),
                Ok(true) => Response::err(request_id, "consensus abort finalized as commit"),
                Err(cleanup_error) => Response::err(
                    request_id,
                    format!(
                        "consensus decision failed: {error}; resolution failed: {cleanup_error}"
                    ),
                ),
            };
        }
    }

    for participant in &fanout.participants {
        let command = match crate::raft::NativeMutationCommand::transaction_participant(
            crate::raft::TransactionParticipantPhase::Commit,
            fanout.coordinator_id.clone(),
            participant.participant_id,
            Some(&participant.sealed_plan_source),
            server_secret,
        ) {
            Ok(command) => command,
            Err(error) => return Response::err(request_id, error),
        };
        let operation = format!("commit-{}", participant.participant_id);
        match submit_consensus_transaction_command(
            &multi,
            authority,
            request_id,
            &fanout.coordinator_id,
            &operation,
            participant.group_id,
            participant.placement_epoch,
            participant.fencing_token,
            participant.graph_type,
            command,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => {
                return Response::err(
                    request_id,
                    "decided consensus participant did not commit; retry will resume",
                )
            }
            Err(error) => {
                return Response::err(
                    request_id,
                    format!("decided consensus participant commit failed: {error}"),
                )
            }
        }
    }

    match submit_consensus_transaction_finalize(
        &multi,
        authority,
        request_id,
        &fanout.coordinator_id,
        control.group_id,
        control.epoch,
        control_fence,
        control_graph_type,
        true,
    )
    .await
    {
        Ok(true) => Response::ok(request_id, ResultPayload::Bool(true)),
        Ok(false) => Response::err(request_id, "consensus transaction finalized as abort"),
        Err(error) => Response::err(request_id, error),
    }
}

#[cfg(feature = "raft")]
fn sanitize_native_proposal(
    request_graph: &str,
    verified_context: &VerifiedRequestContext,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Method, String> {
    if capability_authority_unavailable(&method) {
        return Err(crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE.to_string());
    }
    match method {
        // Transaction control is ordered by the placement group, not by the
        // transaction's data graph. Freeze the caller's original request graph
        // into the method before routing changes `RaftRequest.graph_name`, or a
        // body-less BeginTxn would accidentally target the placement graph.
        Method::BeginTxn { graph, isolation } => Ok(Method::BeginTxn {
            graph: Some(graph.unwrap_or_else(|| request_graph.to_string())),
            isolation,
        }),
        Method::RegisterIdentity {
            agent_id,
            role,
            teams,
            signature,
            roles,
        } => {
            verify_register_identity_signature(
                verified_context,
                request_graph,
                &agent_id,
                &role,
                &teams,
                &roles,
                &signature,
            )?;
            Ok(Method::RegisterIdentity {
                agent_id,
                role,
                teams,
                signature,
                roles,
            })
        }
        Method::ApplyMultisigMutation {
            signatures,
            threshold,
            mutation_type,
            query,
        } => {
            verify_multisig_mutation_signatures(
                verified_context,
                request_graph,
                &signatures,
                threshold,
                &mutation_type,
                &query,
            )?;
            Ok(Method::ApplyMultisigMutation {
                signatures,
                threshold,
                mutation_type,
                query,
            })
        }
        Method::ReserveWorkItemResources { request } => {
            if request.tenant_ref != verified_context.tenant() && !authority.is_admin() {
                return Err(
                    "ACCESS_DENIED: replicated resource tenant is not the verified tenant"
                        .to_string(),
                );
            }
            Ok(Method::ReserveWorkItemResources { request })
        }
        Method::ReleaseWorkItemResources { request } => {
            if request.tenant_ref != verified_context.tenant() && !authority.is_admin() {
                return Err(
                    "ACCESS_DENIED: replicated resource tenant is not the verified tenant"
                        .to_string(),
                );
            }
            Ok(Method::ReleaseWorkItemResources { request })
        }
        Method::ReclaimWorkItemResources { request } => {
            if request.tenant_ref != verified_context.tenant() && !authority.is_admin() {
                return Err(
                    "ACCESS_DENIED: replicated resource tenant is not the verified tenant"
                        .to_string(),
                );
            }
            Ok(Method::ReclaimWorkItemResources { request })
        }
        Method::UpdateResourceHost { request } => {
            if request.tenant_ref != verified_context.tenant() && !authority.is_admin() {
                return Err(
                    "ACCESS_DENIED: replicated resource host tenant is not the verified tenant"
                        .to_string(),
                );
            }
            Ok(Method::UpdateResourceHost { request })
        }
        Method::CreateChannel {
            channel_id,
            channel_type,
            creator,
            initial_members,
        } => {
            if creator != authority.agent_id() {
                return Err("ACCESS_DENIED: channel creator must be caller".to_string());
            }
            Ok(Method::CreateChannel {
                channel_id,
                channel_type,
                creator: authority.actor_scope().to_string(),
                initial_members: initial_members
                    .into_iter()
                    .map(|member| crate::server::mutation_batch::principal_fingerprint(&member))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        Method::JoinChannel {
            channel_id,
            agent_id,
        } => {
            if agent_id != authority.agent_id() {
                return Err("ACCESS_DENIED: channel join actor must be caller".to_string());
            }
            Ok(Method::JoinChannel {
                channel_id,
                agent_id: authority.actor_scope().to_string(),
            })
        }
        Method::LeaveChannel {
            channel_id,
            agent_id,
        } => {
            if agent_id != authority.agent_id() {
                return Err("ACCESS_DENIED: channel leave actor must be caller".to_string());
            }
            Ok(Method::LeaveChannel {
                channel_id,
                agent_id: authority.actor_scope().to_string(),
            })
        }
        Method::SendMessage {
            channel_id,
            sender,
            payload,
        } => {
            if sender != authority.agent_id() {
                return Err("ACCESS_DENIED: message sender must be caller".to_string());
            }
            Ok(Method::SendMessage {
                channel_id,
                sender: authority.actor_scope().to_string(),
                payload,
            })
        }
        other => Ok(other),
    }
}

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

fn decode_screen_observation(
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
    if !wire.prev_frame_id.is_empty() {
        let prefix = format!("screenobservation:{}:", wire.session_id);
        let previous_sequence = wire
            .prev_frame_id
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.parse::<u64>().ok())
            .filter(|sequence| *sequence < wire.frame_seq);
        if previous_sequence.is_none() {
            return Err(
                "previous screen observation must belong to the same earlier session frame"
                    .to_string(),
            );
        }
    }

    if wire.png.len() > MAX_SCREEN_PNG_BYTES {
        return Err("screen image exceeds the resource limit".to_string());
    }
    if !wire.png.is_empty() {
        const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";
        if wire.png.len() < 24
            || !wire.png.starts_with(PNG_SIGNATURE)
            || &wire.png[12..16] != b"IHDR"
        {
            return Err("screen image must be a PNG".to_string());
        }
        let width = u32::from_be_bytes(wire.png[16..20].try_into().unwrap_or([0; 4]));
        let height = u32::from_be_bytes(wire.png[20..24].try_into().unwrap_or([0; 4]));
        if width == 0
            || height == 0
            || width > MAX_SCREEN_DIMENSION
            || height > MAX_SCREEN_DIMENSION
            || u64::from(width) * u64::from(height) > MAX_SCREEN_PIXELS
        {
            return Err("screen image dimensions exceed the resource limit".to_string());
        }
    }

    if wire.elements.len() > MAX_SCREEN_ELEMENTS {
        return Err("screen element count exceeds the resource limit".to_string());
    }
    let mut text_bytes = 0usize;
    for element in &wire.elements {
        if element.role.is_empty()
            || element.role.len() > MAX_SCREEN_ROLE_BYTES
            || element.role.chars().any(char::is_control)
            || element.name.len() > MAX_SCREEN_ELEMENT_NAME_BYTES
            || element.name.contains('\0')
            || element.x.unsigned_abs() > MAX_SCREEN_COORDINATE_ABS as u64
            || element.y.unsigned_abs() > MAX_SCREEN_COORDINATE_ABS as u64
            || element.w < 0
            || element.h < 0
            || element.w > MAX_SCREEN_COORDINATE_ABS
            || element.h > MAX_SCREEN_COORDINATE_ABS
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
    super::transport::validate_nested_msgpack(
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

/// Validate every MessagePack-typed binary field reachable from a request before
/// routing. Raw source/blob/KV/broker/WASM bytes are intentionally excluded: they
/// are opaque binary, not nested MessagePack. Operation handlers still enforce
/// their narrower schema/count limits after this allocation-safety gate.
fn preflight_request_msgpack(method: &Method) -> Result<(), &'static str> {
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
        } => preflight_nested_msgpack(properties_msgpack),
        Method::CompareAndSetNodeFields {
            conditions_msgpack,
            updates_msgpack,
            ..
        }
        | Method::TxnCas {
            conditions_msgpack,
            updates_msgpack,
            ..
        } => {
            preflight_nested_msgpack(conditions_msgpack)?;
            preflight_nested_msgpack(updates_msgpack)
        }
        Method::ClaimNext {
            updates_msgpack, ..
        } => preflight_nested_msgpack(updates_msgpack),
        Method::CreateSummaryNode { props_msgpack, .. }
        | Method::StartTrajectory { props_msgpack } => preflight_nested_msgpack(props_msgpack),
        Method::Consolidate {
            semantic_props_msgpack,
            ..
        } => preflight_nested_msgpack(semantic_props_msgpack),
        Method::AddSceneObject { pose_msgpack, .. } | Method::SetPose { pose_msgpack, .. } => {
            preflight_nested_msgpack(pose_msgpack)
        }
        Method::AppendStep { action_msgpack, .. } => preflight_nested_msgpack(action_msgpack),
        Method::BatchUpdate { operations_msgpack } => preflight_nested_msgpack(operations_msgpack),
        Method::MultiGraphBatchUpdate { batches_msgpack } => {
            preflight_nested_msgpack(batches_msgpack)
        }
        Method::FromMsgpack { msgpack } | Method::Reconcile { msgpack, .. } => {
            preflight_nested_msgpack(msgpack)
        }
        Method::ParseFiles { files_msgpack } | Method::IndexRepository { files_msgpack } => {
            preflight_nested_msgpack(files_msgpack)
        }
        Method::ObserveScreen { obs_msgpack } => preflight_nested_msgpack(obs_msgpack),
        Method::Sql { params_msgpack, .. } => preflight_optional_nested_msgpack(params_msgpack),
        Method::TxnAddMeasurement { points, .. } => preflight_nested_msgpack(points),
        Method::TsAppend { points_msgpack, .. } => preflight_nested_msgpack(points_msgpack),
        Method::TsAsofJoin {
            left_ts_msgpack, ..
        } => preflight_nested_msgpack(left_ts_msgpack),
        #[cfg(feature = "streaming")]
        Method::RegisterContinuousQuery { spec_msgpack, .. } => {
            preflight_nested_msgpack(spec_msgpack)
        }
        #[cfg(feature = "streaming")]
        Method::RegisterTrigger { action_msgpack, .. } => {
            preflight_optional_nested_msgpack(action_msgpack)
        }
        #[cfg(feature = "streaming")]
        Method::CepSubscribe {
            pattern_msgpack, ..
        } => preflight_nested_msgpack(pattern_msgpack),
        #[cfg(feature = "knowledge-batch")]
        Method::KnowledgeStream { request } => {
            if let crate::knowledge_stream::KnowledgeStreamQuery::Sql { params_msgpack, .. } =
                &request.query
            {
                preflight_optional_nested_msgpack(params_msgpack)?;
            }
            Ok(())
        }
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { op } => match op {
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
        },
        // ChangeEnvelope validates every feature/evidence/outbox/mutation blob
        // with the shared eg-types preflight as part of its schema validation.
        Method::ApplyChangeEnvelope { .. } => Ok(()),
        // The batch bounds its cardinality up front (the per-envelope nested-blob
        // validation stays each envelope's own `validate()` responsibility).
        Method::ApplyChangeEnvelopes { envelopes } => {
            if envelopes.len() > crate::change_envelope::MAX_ENVELOPES_PER_BATCH {
                return Err("change envelope batch exceeds the resource limit");
            }
            Ok(())
        }
        _ => Ok(()),
    }
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
struct AstInputLimits {
    max_files: usize,
    max_source_bytes: usize,
    max_total_bytes: usize,
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
fn ast_input_limits() -> AstInputLimits {
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
fn validate_ast_logical_path(path: &str) -> Result<(), String> {
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
fn decode_ast_files(
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
    let saga = handlers::admin::begin_admin_saga(
        redb,
        request_id,
        caller,
        method,
        crate::mutation_batch::MutationDomain::ControlPlane,
    )?;
    Ok(Some(SessionControlSaga { backend, saga }))
}

#[cfg(not(feature = "redb"))]
async fn begin_session_control_saga(
    _state: &Arc<RwLock<ServerState>>,
    _request_id: u64,
    _caller: Option<&str>,
    method: &Method,
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

fn append_native_resource_ops(ops: &mut Vec<&'static str>, available: bool) {
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

/// Dispatch a native transport request whose current envelope was verified
/// before optional QoS admission. This keeps authentication single-pass: the
/// durable replay nonce is consumed exactly once, and admission plus dispatch
/// share the same immutable verified context.
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

async fn dispatch_with_context(
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

async fn dispatch_inner(
    state: &Arc<RwLock<ServerState>>,
    mut req: Request,
    context: Option<VerifiedRequestContext>,
) -> Response {
    // External requests verify the current signed context and durably consume
    // their replay nonce before dispatch. In-process broker bridges provide a
    // context only after their protocol-specific credential has verified.
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
    let method_policy = eg_capabilities::policy(&req.method);
    let action = method_policy.authz_action;
    let identity_bootstrap = !state_machine_authorized && {
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
    };
    // Resource reservation authority is deliberately narrower than the coarse
    // `kg:write` aggregate.  The resolved-profile assertion and host telemetry
    // are controller inputs; an ordinary graph writer must not be able to forge
    // a heavy reservation or overwrite shared physical-host accounting merely by
    // carrying a WorkItem-shaped request.  Replicated apply already carries a
    // verified native authority and bypasses the external gate here.
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
                return Response::err(
                    req.id,
                    "ACCESS_DENIED: WorkItem claim capability requires work:claim-capability",
                );
            }
        }
        let required_resource_scope = match &req.method {
            Method::ReserveWorkItemResources { .. }
            | Method::ReleaseWorkItemResources { .. }
            | Method::ReclaimWorkItemResources { .. } => Some("resource:reserve"),
            Method::UpdateResourceHost { .. } => Some("resource:host"),
            _ => None,
        };
        if let Some(required_scope) = required_resource_scope {
            let controller_authorized = verified_context.allows_action(required_scope)
                || verified_context.allows_action("kg:admin");
            if !controller_authorized {
                crate::metrics::access_denied();
                return Response::err(
                    req.id,
                    format!(
                        "ACCESS_DENIED: resource authority requires controller scope '{required_scope}'"
                    ),
                );
            }
        }
    }
    // The tenant in a resource body is a correlation, not an authority claim.
    // Bind ordinary callers to the verified request tenant before the native
    // backend sees the request.  Only an explicitly privileged aggregate reader
    // (for reconciliation) or `kg:admin` may inspect another tenant's rows.
    if !state_machine_authorized && !identity_bootstrap {
        let requested_tenant = match &req.method {
            Method::ReserveWorkItemResources { request }
            | Method::ReleaseWorkItemResources { request }
            | Method::ReclaimWorkItemResources { request } => Some(request.tenant_ref.as_str()),
            Method::QueryWorkItemReservation { request }
            | Method::ResourceReservationStatus { request } => Some(request.tenant_ref.as_str()),
            Method::UpdateResourceHost { request } => Some(request.tenant_ref.as_str()),
            _ => None,
        };
        if requested_tenant.is_some_and(|tenant| tenant != verified_context.tenant()) {
            let cross_tenant_allowed = verified_context.allows_action("kg:admin")
                || (matches!(
                    &req.method,
                    Method::QueryWorkItemReservation { .. }
                        | Method::ResourceReservationStatus { .. }
                ) && verified_context.allows_action("resource:read:aggregate"));
            if !cross_tenant_allowed {
                crate::metrics::access_denied();
                return Response::err(
                    req.id,
                    "ACCESS_DENIED: resource tenant must match verified request tenant",
                );
            }
        }
    }
    if !state_machine_authorized
        && !identity_bootstrap
        && !verified_context.allows_method(action, method_policy.mutates)
    {
        crate::metrics::access_denied();
        return Response::err(
            req.id,
            format!("ACCESS_DENIED: verified request context lacks required scope '{action}'"),
        );
    }
    {
        if !state_machine_authorized && is_admin_authz_action(action) && !identity_bootstrap {
            let s = timed_read(state).await;
            let result = require_admin_capability(&s.isolation, req.agent_id.as_deref(), action);
            drop(s);
            if let Err(msg) = result {
                return Response::err(req.id, msg);
            }
        }
    }

    if let Err(error) = preflight_request_msgpack(&req.method) {
        return Response::err(req.id, error);
    }

    // Cluster writes cross consensus at the authenticated request boundary. The
    // complete mutation inventory is partitioned between graph commands and typed
    // native commands; a command constructor failure is returned before any local
    // saga or store mutation.
    #[cfg(feature = "raft")]
    {
        use crate::server::mutation::ClusterMutationRoute;
        if is_replicated_apply() {
            // The command is already committed in the owning group's log. Its
            // domain kernel must apply locally on every replica without proposing
            // the same command again.
        } else {
            let (standalone_raft, multi_raft) = {
                let current = timed_read(state).await;
                (current.raft.is_some(), current.multi_raft.is_some())
            };
            if standalone_raft || multi_raft {
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
                        if !multi_raft =>
                    {
                        return Response::err(
                            req.id,
                            "CLUSTER_CONFIGURATION_INVALID: MultiRaft placement authority is required",
                        );
                    }
                    ClusterMutationRoute::ConsensusGraph
                    | ClusterMutationRoute::ConsensusNative
                    | ClusterMutationRoute::ConsensusFanout => {}
                }
            }
        }
    }

    #[cfg(feature = "raft")]
    if !is_replicated_apply()
        && matches!(
            crate::server::mutation::cluster_mutation_route(&req.method),
            crate::server::mutation::ClusterMutationRoute::ConsensusNative
        )
    {
        return propose_native_mutation(
            state,
            &req.graph,
            req.id,
            &verified_context,
            identity_bootstrap,
            req.method,
        )
        .await;
    }

    #[cfg(feature = "raft")]
    if !is_replicated_apply()
        && matches!(
            crate::server::mutation::cluster_mutation_route(&req.method),
            crate::server::mutation::ClusterMutationRoute::ConsensusFanout
        )
    {
        return match req.method {
            Method::MultiGraphBatchUpdate { batches_msgpack } => {
                multi_graph_batch_update(
                    state,
                    req.id,
                    req.agent_id.as_deref(),
                    &verified_context,
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
                    &verified_context,
                    &req.graph,
                    query,
                )
                .await
            }
            _ => Response::err(req.id, "consensus fanout routing error"),
        };
    }

    let session_control =
        match begin_session_control_saga(state, req.id, req.agent_id.as_deref(), &req.method).await
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

    let response = match req.method {
        // ── Service-level ────────────────────────────────────────────
        Method::Ping => Response::ok(req.id, ResultPayload::String("pong".to_string())),

        Method::Health => {
            let (lifecycle, native_resource_ops_available) = {
                let state = timed_read(state).await;
                let manifests = state.registry.materialization_manifests();
                let complete = manifests
                    .iter()
                    .filter(|manifest| manifest.valid)
                    .count();
                let partial = manifests
                    .iter()
                    .filter(|manifest| {
                        manifest.phase == crate::registry::MaterializationPhase::Partial
                    })
                    .count();
                let failed = manifests
                    .iter()
                    .filter(|manifest| {
                        manifest.phase == crate::registry::MaterializationPhase::Failed
                    })
                    .count();
                (
                    serde_json::json!({
                        "catalog_graphs": state.registry.catalog_len(),
                        "resident_graphs": state.registry.resident_len(),
                        "complete_graphs": complete,
                        "partial_graphs": partial,
                        "failed_graphs": failed,
                        "all_resident_materializations_valid": partial == 0 && failed == 0,
                    }),
                    state.persistence.as_ref().is_some_and(|backend| {
                        backend.supports_native_resource_reservations()
                    }),
                )
            };
            let uptime_s = 0; // you can capture start time in ServerState
            let mem_bytes = 0;
            let served_ops = vec![
                "ParseFiles",
                "IndexRepository",
                "ObserveScreen",
                "Discover",
                "ApplyChangeEnvelope",
                "ApplyChangeEnvelopes",
                "GetChangeEnvelope",
                "GetContentVersion",
                "GetChangeCursor",
                "KnowledgeStream",
            ];
            let mut served_ops = served_ops;
            append_native_resource_ops(&mut served_ops, native_resource_ops_available);
            #[cfg(feature = "modality-serving")]
            let served_ops = {
                let mut served_ops = served_ops;
                served_ops.push("ServedModality");
                served_ops
            };
            // ``version`` + ``ops`` let clients negotiate capabilities (e.g. only
            // use ``ParseFiles`` against an engine that advertises it) and fall
            // back gracefully against an older binary. (CONCEPT:EG-KG.query.dispatch-routing)
            Response::ok(
                req.id,
                ResultPayload::Json(serde_json::json!({
                    "status": "ok",
                    "uptime_s": uptime_s,
                    "mem_bytes": mem_bytes,
                    "version": env!("CARGO_PKG_VERSION"),
                    "graph_lifecycle": lifecycle,
                    "ops": served_ops
                })),
            )
        }

        // L36: cooperative cancellation of an in-flight request by its `req_id` (CONCEPT:EG-KG.query.streaming-spillable-collect).
        // Service-level (no graph resolution needed — the registry is keyed by req_id,
        // process-wide) so it works regardless of which graph the target request is
        // running against. `false` covers every "nothing to cancel" case uniformly
        // (already finished / never cancellable / unknown id) — never an error.
        #[cfg(feature = "query")]
        Method::CancelRequest { target_req_id } => Response::ok(
            req.id,
            ResultPayload::Bool(super::request_cancel::cancel(target_req_id)),
        ),

        Method::ParseFile { file_path, source } => {
            #[cfg(feature = "ast")]
            let input_check = validate_ast_logical_path(&file_path).and_then(|()| {
                let limits = ast_input_limits();
                if source.len() > limits.max_source_bytes
                    || source.len() > limits.max_total_bytes
                {
                    Err("AST_INPUT_LIMIT: source content exceeds the configured limit".to_string())
                } else {
                    Ok(())
                }
            });
            #[cfg(feature = "ast")]
            match input_check.and_then(|()| crate::parser::tree_sitter::parse_file(&file_path, &source)) {
                Ok(result) => match serde_json::to_value(&result) {
                    Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req.id, format!("Serialization error: {}", e)),
                },
                Err(e) => Response::err(req.id, e),
            }
            #[cfg(not(feature = "ast"))]
            {
                let _ = (file_path, source);
                Response::err(req.id, "AST feature not enabled".to_string())
            }
        }

        Method::ParseFiles { files_msgpack } => {
            #[cfg(feature = "ast")]
            {
                let owned = match decode_ast_files(&files_msgpack, ast_input_limits()) {
                    Ok(files) => files,
                    Err(error) => return Response::err(req.id, error),
                };
                // Parse on the blocking pool, NOT the async reactor: parse_files is
                // CPU-bound (rayon tree-sitter over every file) and a large batch
                // would otherwise stall the runtime thread, blocking unrelated
                // requests until it finishes. (CONCEPT:EG-KG.compute.off-reactor-dispatch — work off-reactor, A4)
                let results = match compute_off_lock(req.id, move || {
                    crate::parser::tree_sitter::parse_files(&owned)
                })
                .await
                {
                    Ok(r) => r,
                    Err(resp) => return resp,
                };
                match serde_json::to_value(&results) {
                    Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req.id, format!("Serialization error: {}", e)),
                }
            }
            #[cfg(not(feature = "ast"))]
            {
                let _ = files_msgpack;
                Response::err(req.id, "AST feature not enabled".to_string())
            }
        }

        Method::IndexRepository { files_msgpack } => {
            #[cfg(feature = "ast")]
            {
                // Same canonical blob shape as ParseFiles, but parsed AND
                // cross-file-resolved into one IndexResult.
                let owned = match decode_ast_files(&files_msgpack, ast_input_limits()) {
                    Ok(files) => files,
                    Err(error) => return Response::err(req.id, error),
                };
                // Off-reactor like ParseFiles: parse (rayon) + resolution are
                // CPU-bound over the whole batch. (CONCEPT:EG-KG.compute.turn-each-project)
                let result = match compute_off_lock(req.id, move || {
                    crate::parser::resolve::index_repository(&owned)
                })
                .await
                {
                    Ok(r) => r,
                    Err(resp) => return resp,
                };
                match serde_json::to_value(&result) {
                    Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req.id, format!("Serialization error: {}", e)),
                }
            }
            #[cfg(not(feature = "ast"))]
            {
                let _ = files_msgpack;
                Response::err(req.id, "AST feature not enabled".to_string())
            }
        }

        Method::ObserveScreen { obs_msgpack } => {
            // MessagePack map → a captured desktop frame. png rides as a bin field;
            // elements are the AT-SPI accessibles. (CONCEPT:AU-KG.ontology.owl-screen-bridge)
            let input = match decode_screen_observation(&obs_msgpack) {
                Ok(input) => input,
                Err(error) => return Response::err(req.id, error),
            };
            // Inline: PNG hashing + node/edge build over the element set is
            // microsecond-cheap (no AST parse), so it doesn't need the blocking pool.
            let result = crate::screen::observe_screen(&input);
            match serde_json::to_value(&result) {
                Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
                Err(e) => Response::err(req.id, format!("Serialization error: {}", e)),
            }
        }

        Method::Shutdown => {
            info!("Shutdown requested via protocol");
            Response::ok(req.id, ResultPayload::String("shutting_down".to_string()))
        }

        // ── Cost / efficiency (CONCEPT:EG-KG.compute.lane-v, Lane V) ──────────────
        #[cfg(feature = "cost")]
        Method::ResourceStats => {
            match crate::cost::collect_resource_stats(state).await {
                Ok(snapshot) => match serde_json::to_value(&snapshot) {
                    Ok(val) => Response::ok(req.id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req.id, format!("ResourceStats serialization: {e}")),
                },
                Err(error) => Response::err(req.id, error),
            }
        }

        // ── Multi-tenant graph management ────────────────────────────
        Method::CreateGraph {
            graph_name,
            graph_type,
        } => {
            // Lifecycle shares the same per-graph serialization lane as ordinary
            // MutationBatch/txn writes.  The durable identity must land before the
            // registry publishes this incarnation.
            let _mutation_guard =
                crate::server::mutation_batch::lock_graph(&graph_name).await;
            let (backend, already_exists) = {
                let s = timed_read(state).await;
                (s.persistence.clone(), s.registry.exists(&graph_name))
            };
            let Some(backend) = backend else {
                return Response::err(req.id, "graph creation requires durable persistence");
            };
            let created_result = ResultPayload::Json(serde_json::json!({
                "created": graph_name.clone()
            }));
            let incarnation_id = crate::server::mutation_batch::lifecycle_batch_id(
                "create",
                &graph_name,
                req.id,
            );
            if already_exists {
                match crate::server::mutation_batch::lifecycle_was_committed(
                    &backend,
                    "create",
                    &graph_name,
                    req.id,
                )
                .await
                {
                    Ok(true) => return Response::ok(req.id, created_result),
                    Ok(false) => {}
                    Err(e) => {
                        return Response::err(
                            req.id,
                            format!("durable graph-create reconciliation failed: {e}"),
                        )
                    }
                }
                return Response::err(req.id, format!("Graph '{}' already exists", graph_name));
            }
            if let Err(e) = crate::server::mutation_batch::commit_lifecycle(
                &backend,
                "create",
                req.id,
                req.agent_id.as_deref(),
                &graph_name,
                Method::CreateGraph {
                    graph_name: graph_name.clone(),
                    graph_type,
                },
                &created_result,
            )
            .await
            {
                return Response::err(
                    req.id,
                    format!("durable graph registration failed: {e}"),
                );
            }
            let graph_fname = crate::persist::sanitize(&graph_name);
            let committed_version = match backend
                .read_mutation_graph_version(&graph_fname)
                .await
            {
                Ok(Some(version)) if version > 0 => version,
                Ok(Some(_)) => {
                    return Response::err(
                        req.id,
                        "durable graph registration published an invalid zero version",
                    )
                }
                Ok(None) => {
                    return Response::err(
                        req.id,
                        "durable graph registration published no authoritative version",
                    )
                }
                Err(error) => {
                    return Response::err(
                        req.id,
                        format!("durable graph version read failed: {error}"),
                    )
                }
            };

            let mut s = timed_write(state).await;
            // Bounded hot-context cache admission (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3): a
            // new graph is about to be resident, so make room for it FIRST — evict
            // the coldest resident graph if the finite cap is already reached.
            #[cfg(feature = "redb")]
            {
                let cap = crate::server::persistence::cold_offload::max_resident_graphs();
                let tracker = s.cold_tracker.clone();
                crate::server::persistence::cold_offload::admit_capacity(
                    &mut s,
                    &tracker,
                    &graph_name,
                    cap,
                ); // `s`: &mut ServerState via RwLockWriteGuard's DerefMut
            }
            // The creator (when identified) becomes the graph owner, which is
            // what peer-deny / manager-access checks resolve against.
            match s
                .registry
                .create_graph_with_incarnation(
                    &graph_name,
                    graph_type,
                    req.agent_id.clone(),
                    incarnation_id,
                    committed_version,
                )
            {
                Ok(()) => {
                    crate::metrics::set_graph_size(&graph_name, 0, 0);
                    Response::ok(req.id, created_result)
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::DeleteGraph { ref graph_name } => {
            // Fence gateway/txn writes for this graph across durable purge and RAM
            // teardown.  A retry after a crash at that boundary reconciles from the
            // durable batch record.
            let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
            let (backend, exists) = {
                let s = timed_read(state).await;
                let record = s.registry.catalog_record(graph_name);
                if !state_machine_authorized {
                    if let Some(record) = record.as_ref() {
                    if let Err(denied) = check_graph_access(
                        &s.isolation,
                        req.agent_id.as_deref(),
                        graph_name,
                        record.graph_type,
                        record.owner.as_deref(),
                        AccessLevel::Write,
                    ) {
                        return Response::err(req.id, denied);
                    }
                }
                }
                (s.persistence.clone(), record.is_some())
            };
            let Some(backend) = backend else {
                return Response::err(req.id, "graph deletion requires durable persistence");
            };
            let deleted_result = ResultPayload::Json(serde_json::json!({
                "deleted": graph_name
            }));
            if !exists {
                match crate::server::mutation_batch::lifecycle_was_committed(
                    &backend,
                    "delete",
                    graph_name,
                    req.id,
                )
                .await
                {
                    Ok(true) => return Response::ok(req.id, deleted_result),
                    Ok(false) => {}
                    Err(e) => {
                        return Response::err(
                            req.id,
                            format!("durable graph-delete reconciliation failed: {e}"),
                        )
                    }
                }
                return Response::err(req.id, format!("Graph '{}' not found", graph_name));
            }
            if let Err(e) = crate::server::mutation_batch::commit_lifecycle(
                &backend,
                "delete",
                req.id,
                req.agent_id.as_deref(),
                graph_name,
                Method::DeleteGraph {
                    graph_name: graph_name.clone(),
                },
                &deleted_result,
            )
            .await
            {
                return Response::err(req.id, format!("durable graph purge failed: {e}"));
            }

            let mut s = timed_write(state).await;
            if !state_machine_authorized {
                if let Some(entry) = s.registry.catalog_record(graph_name) {
                if let Err(denied) = check_graph_access(
                    &s.isolation,
                    req.agent_id.as_deref(),
                    graph_name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Write,
                ) {
                    return Response::err(req.id, denied);
                }
            }
            }
            match s.registry.delete_graph(graph_name) {
                Ok(()) => {
                    crate::metrics::drop_graph(graph_name);
                    // In-memory teardown (CONCEPT:EG-KG.backend.many-repeated-create-delete) — distinct from the durable
                    // purge below. The registry entry (the live GraphCore) is gone, but
                    // per-graph state keyed by NAME elsewhere in ServerState would
                    // survive and shadow a same-name recreate. Drop it so the recreate
                    // starts truly clean every cycle:
                    //  • the write-coalescer's cached writer — its worker owns an
                    //    `Arc<GraphCore>` of THIS (deleted) incarnation; left cached,
                    //    `writer_for` returns it on recreate (it is name-keyed and
                    //    ignores the new core) and routes the new tenant's writes into
                    //    the orphaned core — silently dropping them in RAM. THIS is the
                    //    tenant-churn corruption.
                    //  • the per-graph in-flight semaphore (no data, but bounds an
                    //    unbounded entry leak across many churn cycles).
                    s.write_coalescer.remove(graph_name);
                    // The routed-write-coalescer registry does not hold a stale
                    // `Arc<GraphCore>` per writer (each queued job carries its own —
                    // see its module docs), so this is resource hygiene only, not a
                    // tenant-churn correctness fix like the line above.
                    s.routed_write_coalescer.remove(graph_name);
                    s.per_graph_inflight.remove(graph_name);
                    // Cold-tenant tracker (CONCEPT:EG-KG.backend.r6-feature, R6): forget this graph's access
                    // timestamp + offload mark so they don't leak across a same-name recreate.
                    #[cfg(feature = "redb")]
                    s.cold_tracker.forget(graph_name);
                    Response::ok(req.id, deleted_result)
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::ListGraphs => {
            let s = timed_read(state).await;
            let read_authority = match GraphReadAuthority::from_verified(
                &verified_context,
                &s.isolation,
            ) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            let graphs: Vec<serde_json::Value> = s
                .registry
                .list()
                .iter()
                .filter(|(name, _)| {
                    s.registry.get(name).is_some_and(|entry| {
                        check_graph_access(
                            &s.isolation,
                            read_authority.actor(),
                            name,
                            entry.graph_type,
                            entry.owner.as_deref(),
                            AccessLevel::Read,
                        )
                        .is_ok()
                    })
                })
                .map(|(name, gt)| {
                    let readiness = s.registry.materialization_manifest(name);
                    let indexes = s.registry.get(name).map(|entry| {
                        entry
                            .core
                            .indexes()
                            .server_manifests()
                            .into_iter()
                            .map(|(kind, manifest)| {
                                let kind = match kind {
                                    crate::index::IndexKind::Text => "text",
                                    crate::index::IndexKind::Temporal => "temporal",
                                    crate::index::IndexKind::DerivedOwl => "derived_owl",
                                    crate::index::IndexKind::Spatial => "spatial",
                                    crate::index::IndexKind::Label => "label",
                                    crate::index::IndexKind::Property => "property",
                                    crate::index::IndexKind::Ontology => "ontology",
                                    crate::index::IndexKind::Vector => "vector",
                                };
                                let validity = match manifest.validity {
                                    crate::index::IndexValidity::Building => "building",
                                    crate::index::IndexValidity::Valid => "valid",
                                    crate::index::IndexValidity::Stale => "stale",
                                    crate::index::IndexValidity::Failed => "failed",
                                };
                                serde_json::json!({
                                    "kind": kind,
                                    "source_snapshot_version": manifest.source_snapshot_version,
                                    "build_version": manifest.build_version,
                                    "completeness_cursor": {
                                        "nodes": manifest.completeness.nodes,
                                        "edges": manifest.completeness.edges,
                                        "complete": manifest.completeness.complete,
                                    },
                                    "validity": validity,
                                })
                            })
                            .collect::<Vec<_>>()
                    });
                    serde_json::json!({
                        "name": name,
                        "type": gt,
                        "materialization": readiness.as_ref().map(|value| match value.phase {
                            crate::registry::MaterializationPhase::CatalogOnly => "catalog_only",
                            crate::registry::MaterializationPhase::Partial => "partial",
                            crate::registry::MaterializationPhase::Complete => "complete",
                            crate::registry::MaterializationPhase::Failed => "failed",
                        }),
                        "source_snapshot_version": readiness.as_ref().and_then(|value| value.source_snapshot_version),
                        "completeness_cursor": readiness.as_ref().and_then(|value| value.completeness_cursor.as_ref()).map(|cursor| {
                            serde_json::json!({
                                "node_offset": cursor.node_offset,
                                "edge_offset": cursor.edge_offset,
                            })
                        }),
                        "valid": readiness.as_ref().is_some_and(|value| value.valid),
                        "index_manifests": indexes.unwrap_or_default(),
                    })
                })
                .collect();
            Response::ok(req.id, ResultPayload::Json(serde_json::json!(graphs)))
        }

        // ── M3 catalog-driven resharding admin (CONCEPT:EG-KG.backend.m3-admin-dispatch) ──────
        // The wire surface that drives online resharding (EG-032), the tenant catalog
        // (EG-031) and the rebalance planner (EG-035) + its execution (EG-039). All
        // self-routing service-level ops handled here (not the per-graph chain), so they
        // reach the concrete redb backend via `as_redb`. A non-redb build returns a clean
        // "not available" error from the handler.
        Method::Reshard { .. }
        | Method::CatalogAssign { .. }
        | Method::CatalogReassign { .. }
        | Method::CatalogRemove { .. }
        | Method::CatalogList
        | Method::RebalancePlan { .. }
        | Method::RebalanceExecute { .. }
        // ── Online backup / restore + PITR (CONCEPT:EG-KG.sharding.reshard-on-restore) ──────────
        // Routed through the SAME admin handler: self-routing service-level DR ops that
        // reach the concrete redb backend via `as_redb`. Non-redb builds return a clean
        // "not available" error from the handler.
        | Method::Backup { .. }
        | Method::Restore { .. } => {
            match handlers::admin::try_handle(
                state,
                req.id,
                req.agent_id.as_deref(),
                req.method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is an admin method.
                Err(_) => Response::err(req.id, "admin dispatch routing error"),
            }
        }

        // ── Placement-catalog wire RPC (CONCEPT:EG-KG.sharding.placement-route-rpc, DIST-P2-4) ──
        // Self-routing, NOT graph-scoped (the catalog is cluster-wide, like the M3 admin
        // block above) — exposes the DIST-P2-1 `PlacementCatalog` over the wire so an
        // external caller (`epistemic_graph.client`'s `placement` namespace, AU's
        // `placement_catalog.py`) can consume it instead of guessing independently. A
        // A single-node engine returns an authoritative unplaced route. A configured
        // Raft node without MultiRaft is an invalid cluster and fails closed.
        // The admin mutation (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc, DIST-P2-5) is
        // routed through the SAME handler, self-routing exactly like the M3 admin
        // block above -- see `handlers::placement::try_handle`'s per-variant arms.
        Method::PlacementRoute { .. } | Method::PlacementAdmin { .. } => {
            match handlers::placement::try_handle(state, req.id, req.method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a placement method.
                Err(_) => Response::err(req.id, "placement dispatch routing error"),
            }
        }

        // ── Raft cluster membership admin (CONCEPT:EG-KG.storage.kg-kg-2 — cluster_deployment.md §5
        // item 2) ── Self-routing, NOT graph-scoped (cluster-wide, like the M3 admin
        // block above): attaches/promotes a node against `MultiRaft` directly. Gated
        // `admin:cluster` by the SAME scope+admin enforcement every other admin-tier
        // method goes through above (`eg_capabilities::policy`), not a second check
        // here.
        Method::RaftAddLearner { .. } | Method::RaftChangeMembership { .. } => {
            match handlers::raft_admin::try_handle(state, req.id, req.method).await {
                Ok(resp) => resp,
                // Unreachable: both variants matched above are raft-admin methods.
                Err(_) => Response::err(req.id, "raft-admin dispatch routing error"),
            }
        }

        // ── Cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1) ──
        // Self-routing, NOT graph-scoped (cluster-wide, like the raft-admin block
        // above): `ClusterMembers` answers from ANY node's local `NodeInfoStore` +
        // live `MultiRaft` membership (no leader redirect, unlike `PlacementRoute` —
        // ADR-1's client resolves via any healthy seed); `NodeInfoUpsert` is the
        // internal per-node self-report `raft::node::start` issues, reaching this
        // arm only via the replicated-apply re-entry (its live proposal is
        // intercepted earlier by the `ConsensusNative` branch above).
        Method::ClusterMembers | Method::NodeInfoUpsert { .. } => {
            match handlers::topology::try_handle(state, req.id, req.method).await {
                Ok(resp) => resp,
                // Unreachable: both variants matched above are topology methods.
                Err(_) => Response::err(req.id, "cluster topology dispatch routing error"),
            }
        }

        // ── Fleet server registry (CONCEPT:EG-KG.sharding.server-registry, W2.5) ──────
        // Self-routing, like `ClusterMembers`/`NodeInfoUpsert` above, but for the
        // OPPOSITE reason: those are cluster-wide and NOT graph nodes, while this
        // writes a REAL `:Server` graph node into `__commons__` -- self-routes
        // here (rather than resolving `req.graph`) because a fleet server's
        // registration is a fleet-wide singleton concept, never tenant-scoped,
        // exactly like `ApplyMultisigMutation` self-routes before translating
        // into `Method::ApplyMutation` against `req.graph`. See
        // `handle_register_server`'s doc comment.
        Method::RegisterServer {
            name,
            url,
            resources_json,
            ttl_secs,
        } => {
            handle_register_server(
                state,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                name,
                url,
                resources_json,
                ttl_secs,
            )
            .await
        }

        // ── Channel operations ───────────────────────────────────────
        Method::CreateChannel {
            channel_id,
            channel_type,
            creator,
            initial_members,
        } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            if creator != carrier.agent_id() {
                return Response::err(req.id, "ACCESS_DENIED: channel creator must be caller");
            }
            let mut s = timed_write(state).await;
            match s
                .channels
                .create_channel_scoped(
                    &channel_id,
                    carrier.tenant_scope(),
                    channel_type,
                    carrier.agent_id(),
                    initial_members,
                )
            {
                Ok(()) => Response::ok(
                    req.id,
                    ResultPayload::Json(serde_json::json!({"channel": channel_id})),
                ),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::JoinChannel {
            channel_id,
            agent_id,
        } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            if agent_id != carrier.agent_id() {
                return Response::err(req.id, "ACCESS_DENIED: channel join actor must be caller");
            }
            let mut s = timed_write(state).await;
            if let Err(error) = s
                .channels
                .authorize_tenant(&channel_id, carrier.tenant_scope())
            {
                return Response::err(req.id, error);
            }
            match s.channels.join_channel(&channel_id, carrier.agent_id()) {
                Ok(()) => Response::ok(req.id, ResultPayload::String("joined".to_string())),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::LeaveChannel {
            channel_id,
            agent_id,
        } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            if agent_id != carrier.agent_id() {
                return Response::err(req.id, "ACCESS_DENIED: channel leave actor must be caller");
            }
            let mut s = timed_write(state).await;
            if let Err(error) = s.channels.authorize_member(
                &channel_id,
                carrier.tenant_scope(),
                carrier.agent_id(),
            ) {
                return Response::err(req.id, error);
            }
            match s.channels.leave_channel(&channel_id, carrier.agent_id()) {
                Ok(imprint) => {
                    let val = match imprint {
                        Some(imp) => {
                            serde_json::to_value(&imp).unwrap_or(serde_json::json!("closed"))
                        }
                        None => serde_json::json!("left"),
                    };
                    Response::ok(req.id, ResultPayload::Json(val))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::CloseChannel {
            channel_id,
            summary_embedding,
            topic_metadata,
        } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            let mut s = timed_write(state).await;
            if let Err(error) = s.channels.authorize_creator(
                &channel_id,
                carrier.tenant_scope(),
                carrier.agent_id(),
            ) {
                return Response::err(req.id, error);
            }
            match s
                .channels
                .close_channel(&channel_id, summary_embedding, topic_metadata)
            {
                Ok(imprint) => {
                    let val = match imprint {
                        Some(imp) => {
                            serde_json::to_value(&imp).unwrap_or(serde_json::json!("closed"))
                        }
                        None => serde_json::json!("closed"),
                    };
                    Response::ok(req.id, ResultPayload::Json(val))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::SendMessage {
            channel_id,
            sender,
            payload,
        } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            if sender != carrier.agent_id() {
                return Response::err(req.id, "ACCESS_DENIED: channel sender must be caller");
            }
            let mut s = timed_write(state).await;
            if let Err(error) = s.channels.authorize_member(
                &channel_id,
                carrier.tenant_scope(),
                carrier.agent_id(),
            ) {
                return Response::err(req.id, error);
            }
            match s
                .channels
                .send_message(&channel_id, carrier.agent_id(), &payload)
            {
                Ok(()) => Response::ok(req.id, ResultPayload::String("sent".to_string())),
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::GetChannelMessages { channel_id, limit } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            let s = timed_read(state).await;
            if let Err(error) = s.channels.authorize_member(
                &channel_id,
                carrier.tenant_scope(),
                carrier.agent_id(),
            ) {
                return Response::err(req.id, error);
            }
            match s.channels.get_messages(&channel_id, limit) {
                Ok(msgs) => {
                    let val: Vec<serde_json::Value> = msgs.iter().map(|m| {
                        serde_json::json!({"sender": m.sender, "payload": m.payload, "timestamp": m.timestamp})
                    }).collect();
                    Response::ok(req.id, ResultPayload::Json(serde_json::json!(val)))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        Method::ListChannels => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            let s = timed_read(state).await;
            let channels: Vec<serde_json::Value> = s.channels.list_channels_for(
                carrier.tenant_scope(),
                carrier.agent_id(),
            ).iter().map(|(id, ct, members)| {
                serde_json::json!({"id": id, "type": ct, "members": members})
            }).collect();
            Response::ok(req.id, ResultPayload::Json(serde_json::json!(channels)))
        }

        Method::GetChannelMembers { channel_id } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            let s = timed_read(state).await;
            if let Err(error) = s.channels.authorize_member(
                &channel_id,
                carrier.tenant_scope(),
                carrier.agent_id(),
            ) {
                return Response::err(req.id, error);
            }
            match s.channels.get_members(&channel_id) {
                Ok(members) => {
                    Response::ok(req.id, ResultPayload::Json(serde_json::json!(members)))
                }
                Err(e) => Response::err(req.id, e),
            }
        }

        // ── Zero-Trust Consensus ─────────────────────────────────────────
        Method::RegisterIdentity {
            agent_id,
            role,
            teams,
            signature,
            roles,
        } => {
            if !state_machine_authorized {
                if let Err(message) = verify_register_identity_signature(
                    &verified_context,
                    &req.graph,
                    &agent_id,
                    &role,
                    &teams,
                    &roles,
                    &signature,
                ) {
                    crate::metrics::auth_failure();
                    return Response::err(req.id, message);
                }
            }
            let mut s = timed_write(state).await;
            let identity = crate::isolation::AgentIdentity {
                agent_id: agent_id.clone(),
                role,
                teams,
                roles,
            };
            let result = if identity_bootstrap
                || (state_machine_authorized && replicated_identity_bootstrap_authorized())
            {
                s.isolation.try_bootstrap_system_identity(identity)
            } else {
                s.isolation.try_register_agent(identity)
            };
            if let Err(message) = result {
                return Response::err(req.id, message);
            }
            info!("RegisterIdentity committed");
            Response::ok(req.id, ResultPayload::String("registered".to_string()))
        }

        // ── RBAC policy administration (CONCEPT:EG-KG.compute.feature) ──────────────────
        // Gated at the handler; a non-security build has no arm and falls to the
        // dispatch "not available in this build" catch-all (mirrors EG-090).
        #[cfg(feature = "security")]
        Method::RbacAdmin { op } => {
            use crate::acl::RbacAdminOp;
            let mut s = timed_write(state).await;
            match op {
                RbacAdminOp::AddRole(role) => {
                    match s.isolation.try_add_role(role) {
                        Ok(()) => Response::ok(
                            req.id,
                            ResultPayload::String("role_added".to_string()),
                        ),
                        Err(message) => Response::err(req.id, message),
                    }
                }
                RbacAdminOp::RemoveRole(name) => {
                    match s.isolation.try_remove_role(&name) {
                        Ok(()) => Response::ok(
                            req.id,
                            ResultPayload::String("role_removed".to_string()),
                        ),
                        Err(message) => Response::err(req.id, message),
                    }
                }
                RbacAdminOp::AddGrant(grant) => {
                    match s.isolation.try_add_grant(grant) {
                        Ok(()) => Response::ok(
                            req.id,
                            ResultPayload::String("grant_added".to_string()),
                        ),
                        Err(message) => Response::err(req.id, message),
                    }
                }
                RbacAdminOp::RemoveGrant(grant) => {
                    match s.isolation.try_remove_grant(&grant) {
                        Ok(removed) => Response::ok(
                            req.id,
                            ResultPayload::Json(serde_json::json!({ "removed": removed })),
                        ),
                        Err(message) => Response::err(req.id, message),
                    }
                }
                RbacAdminOp::List => {
                    let policy = s.isolation.rbac();
                    let roles: Vec<_> = policy.roles().cloned().collect();
                    Response::ok(
                        req.id,
                        ResultPayload::Json(serde_json::json!({
                            "roles": roles,
                            "grants": policy.grants(),
                        })),
                    )
                }
            }
        }

        Method::ApplyMultisigMutation {
            signatures,
            threshold,
            mutation_type,
            query,
        } => {
            if !state_machine_authorized {
                if let Err(message) = verify_multisig_mutation_signatures(
                    &verified_context,
                    &req.graph,
                    &signatures,
                    threshold,
                    &mutation_type,
                    &query,
                ) {
                    crate::metrics::auth_failure();
                    return Response::err(req.id, message);
                }
            }
            // Delegate mutation application to the target graph
            dispatch_graph_op(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                Method::ApplyMutation {
                    event_type: mutation_type,
                    query,
                },
            )
            .await
        }

        // ── Durable analytics-job plane (CONCEPT:INT-P2-1, feature `jobs`) ──────────
        // NOT graph-scoped (own `jobs.redb`, keyed by `job_id`) — self-routes here,
        // BEFORE the per-graph `dispatch_graph_op` chain, exactly like `TsAppend`/
        // `Kv*`/`CreateChannel` above. See `handlers/jobs.rs` module docs.
        #[cfg(feature = "jobs")]
        Method::AnalyticsJob { op } => {
            // Worker fencing identity and durable actor attribution come from the
            // authenticated context, never from the unsigned request envelope's
            // display/agent field.
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            handlers::jobs::handle(
                state,
                req.id,
                &carrier,
                verified_context.allows_analytics_worker(),
                op,
            )
            .await
        }

        // ── Native statechart engine (CONCEPT:INT-P2-2, feature `statechart`) ───────
        // NOT graph-scoped (own `statecharts.redb`, keyed by def_id/instance_id) —
        // self-routes here, BEFORE the per-graph `dispatch_graph_op` chain, exactly
        // like `AnalyticsJob` above. See `handlers/statechart.rs` module docs.
        #[cfg(feature = "statechart")]
        Method::Statechart { op } => {
            // Durable owner attribution comes from the authenticated context, never
            // from the unsigned request envelope's display/agent field.
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            handlers::statechart::handle(state, req.id, &carrier, op).await
        }

        // ── Agent-facing quantum control plane (Q8, CONCEPT:EG-KG.compute.quantum-agent-api,
        // feature `quantum-agent-api`) ──────────────────────────────────────────
        // NOT graph-scoped (pure compute -- reads no persisted graph state, writes
        // nothing durable) — self-routes here, BEFORE the per-graph `dispatch_graph_op`
        // chain, exactly like `AnalyticsJob`/`Statechart` above. See
        // `handlers::quantum`'s module docs for the full reachability/exactness/audit
        // contract this closes (program doc: "no job-plane, no wire protocol Method,
        // and no KG concept mapping" — the wire protocol Method half ends here).
        #[cfg(feature = "quantum-agent-api")]
        Method::Quantum { op } => handlers::quantum::handle(req.id, op).await,
        // ── Native visualization render surface (D-VZ-1 lanes V4/V6, feature
        // `viz-static-export`) ──────────────────────────────────────────────
        // NOT graph-scoped (a render builds a FRESH ephemeral per-request
        // ColumnStore, never reads a live GraphCore) — self-routes here, BEFORE
        // the per-graph `dispatch_graph_op` chain, exactly like
        // `AnalyticsJob`/`Statechart` above. See `handlers/viz.rs` module docs.
        // Gated on `viz-static-export` (not bare `viz`, which `eg-types` alone
        // already gates the wire `Method::Viz` variant on) — a deliberate,
        // documented deviation: the handler needs a real ColumnStore + export
        // backend to do anything, which only exist at that tier.
        #[cfg(feature = "viz-static-export")]
        Method::Viz { op } => {
            // No durable owner-scoped state to attribute a render to; the carrier
            // is still resolved for parity with the sibling self-routed handlers
            // (see `handlers::viz::handle`'s doc).
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            handlers::viz::handle(state, req.id, &carrier, op).await
        }

        // ── Transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid — multi-op OCC ACID) ──────
        // Stateful + self-routing: a Txn* op targets the graph the txn was opened
        // against (resolved from `open_txns`), NOT necessarily `req.graph`, and
        // BeginTxn carries its own graph. So they are handled here (with `state`)
        // BEFORE the graph-op path — never through `dispatch_graph_op`, whose
        // coalescer/registry-lookup assumes a single `req.graph` target. For
        // BeginTxn the request envelope's `graph` is the default target when the
        // body omits one.
        Method::BeginTxn { .. }
        | Method::TxnAddNode { .. }
        | Method::TxnRemoveNode { .. }
        | Method::TxnAddEdge { .. }
        | Method::TxnRemoveEdge { .. }
        | Method::TxnCas { .. }
        | Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. }
        | Method::Commit { .. }
        | Method::Rollback { .. } => {
            // BeginTxn defaults its target to the request envelope's graph.
            let method = match req.method {
                Method::BeginTxn {
                    graph: None,
                    isolation,
                } => Method::BeginTxn {
                    graph: Some(req.graph.clone()),
                    isolation,
                },
                m => m,
            };
            match handlers::txn::try_handle(
                state,
                req.id,
                verified_context.agent_id(),
                &verified_context,
                method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a txn method.
                Err(_) => Response::err(req.id, "txn dispatch routing error"),
            }
        }

        // Extended cross-modal STAGING (CONCEPT:EG-KG.compute.eg-187, closing EG-360/361/362 at RPC) — the tsdb-measurement,
        // OWL-axiom and SPARQL-CONSTRUCT stage methods. `handlers::txn::try_handle` handles
        // them (feature-gated), but they carry their OWN `graph` (like `TxnAddEmbedding`),
        // so they route straight there — NO `BeginTxn` graph-default rewrite. Without these
        // arms the variants fell through to the graph-op "not available" catch-all, so an
        // in-txn measurement/axiom/CONSTRUCT staged fine over pgwire (EG-372, which calls the
        // stage fns directly) but ERRORED over the native RPC surface — a "seamless" leak
        // (docs/north_star.md). Each is `cfg`-gated to match its protocol variant, so a slim
        // build without the feature keeps the prior catch-all behavior.
        #[cfg(feature = "tsdb")]
        Method::TxnAddMeasurement { .. } => {
            match handlers::txn::try_handle(
                state,
                req.id,
                verified_context.agent_id(),
                &verified_context,
                req.method,
            )
                .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req.id, "txn dispatch routing error"),
            }
        }
        #[cfg(feature = "owl")]
        Method::TxnAxiom { .. } => {
            match handlers::txn::try_handle(
                state,
                req.id,
                verified_context.agent_id(),
                &verified_context,
                req.method,
            )
                .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req.id, "txn dispatch routing error"),
            }
        }
        #[cfg(feature = "sparql")]
        Method::TxnConstruct { .. } => {
            match handlers::txn::try_handle(
                state,
                req.id,
                verified_context.agent_id(),
                &verified_context,
                req.method,
            )
                .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req.id, "txn dispatch routing error"),
            }
        }
        // Planner-writeback staging (CONCEPT:EG-KG.query.plan-dag, D7) — carries its OWN
        // `graph` (like `TxnConstruct`), so it routes straight to the txn handler with NO
        // BeginTxn graph-default rewrite. `query`-gated to match its protocol variant.
        #[cfg(feature = "query")]
        Method::TxnPlanWriteback { .. } => {
            match handlers::txn::try_handle(
                state,
                req.id,
                verified_context.agent_id(),
                &verified_context,
                req.method,
            )
                .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req.id, "txn dispatch routing error"),
            }
        }
        // Materialize-belief staging (CONCEPT:EG-KG.epistemic.epistemic-substrate, D5) —
        // carries its OWN `graph` (like `TxnPlanWriteback`), so it routes straight to the
        // txn handler with NO BeginTxn graph-default rewrite. `epistemic`-gated to match
        // its protocol variant.
        #[cfg(feature = "epistemic")]
        Method::TxnMaterializeBelief { .. } => {
            match handlers::txn::try_handle(
                state,
                req.id,
                verified_context.agent_id(),
                &verified_context,
                req.method,
            )
                .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req.id, "txn dispatch routing error"),
            }
        }

        // ── Blob (CONCEPT:EG-KG.storage.blob-namespace) ──────────────────────────────────
        // Content-addressed, NOT graph-scoped: a blob is keyed by digest and may be
        // referenced across graphs, so route at the top level (like txn) before the
        // per-graph chain. The variants only exist with the `blob` feature; without
        // it they aren't in the enum and a slim build can't reach this arm.
        #[cfg(feature = "blob")]
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobFetchBegin { .. }
        | Method::BlobChunkGet { .. }
        | Method::BlobFetchEnd { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            match handlers::blob::try_handle(
                state,
                req.id,
                &carrier,
                req.method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a blob method.
                Err(_) => Response::err(req.id, "blob dispatch routing error"),
            }
        }

        // ── Key→Value (CONCEPT:EG-KG.storage.namespaced-kv-surface) ───────────────────────────────
        // Namespaced KV, NOT graph-scoped: a pair is keyed by (namespace, key) and
        // lives off the node/edge graph, so route at the top level (like blob/txn)
        // before the per-graph chain. The variants only exist with the `kv` feature;
        // without it they aren't in the enum and a slim build can't reach this arm.
        #[cfg(feature = "kv")]
        Method::KvGet { .. }
        | Method::KvPut { .. }
        | Method::KvDelete { .. }
        | Method::KvScan { .. }
        | Method::KvCas { .. } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            match crate::server::kv::try_handle(
                state,
                req.id,
                &carrier,
                req.method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a kv method.
                Err(_) => Response::err(req.id, "kv dispatch routing error"),
            }
        }

        // ── SQLite `.db` file import/export (CONCEPT:EG-KG.query.eg-feature/EG-332) ──
        // File-scoped, NOT graph-scoped: both ops target a filesystem `path` and move
        // rows through the verified caller's owner-scoped user-table store (behind `query`), so they
        // self-route here (like the Blob*/Kv* ops) BEFORE the per-graph chain. Gated
        // `sqlite-file` (which pulls the bundled C sqlite kept OUT of pi); a build
        // without it never has the variants in the enum, so this arm can't be reached.
        #[cfg(feature = "sqlite-file")]
        Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            match handlers::sqlite_file::try_handle(
                state,
                req.id,
                &carrier,
                req.method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: both variants matched above are sqlite-file methods.
                Err(_) => Response::err(req.id, "sqlite-file dispatch routing error"),
            }
        }

        // ── Streaming / CDC / subscriptions (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230) ───
        // The reactive READ + REGISTER surface over the CDC hub on `state` (the WRITE
        // side — emitting changes — lives in the dispatch_graph_op write-side-effect
        // block). These are NOT graph-mutating (CdcRead/Watch/FiredTriggers tail a
        // cursor; Register*/Drop* manage hub registrations), so they self-route here
        // BEFORE the per-graph chain, like tsdb/blob. Gated `streaming`: in a slim
        // build the arm is absent and the variants fall to the graph_ops not-built
        // catch-all (never a panic, never a mis-route).
        #[cfg(feature = "streaming")]
        Method::CdcRead { .. }
        | Method::RegisterContinuousQuery { .. }
        | Method::ReadContinuousQuery { .. }
        | Method::DropContinuousQuery { .. }
        | Method::Watch { .. }
        | Method::RegisterTrigger { .. }
        | Method::DropTrigger { .. }
        | Method::ListTriggers { .. }
        | Method::FiredTriggers { .. } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            let read_authority = {
                let s = timed_read(state).await;
                match GraphReadAuthority::from_verified(&verified_context, &s.isolation) {
                    Ok(authority) => authority,
                    Err(denied) => return Response::err(req.id, denied),
                }
            };
            match handlers::streaming::try_handle(
                state,
                req.id,
                &carrier,
                &read_authority,
                req.method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a streaming method.
                Err(_) => Response::err(req.id, "streaming dispatch routing error"),
            }
        }

        // ── Live CEP standing queries (CONCEPT:EG-KG.query.protocol-types) ───────────────
        // The PUSH half of the event-stream + CEP modality: register a CEP pattern once
        // (CepSubscribe), then long-poll the matches it detects as CDC changes flow
        // (CepPoll). The engine is fed by the CDC hub (the write side lives in the
        // dispatch write-side-effect block via `CepSurface::feed_change`); this is the
        // register + poll surface over it. NOT graph-mutating, so it self-routes here
        // BEFORE the per-graph chain (like the streaming/tsdb/blob surfaces). Gated
        // `all(streaming, stream)`: the CDC feed AND the live NFA engine. A build missing
        // either (e.g. `pi` — streaming, no stream) omits this arm; the `Cep*` variants
        // (gated `streaming`) then fall to the graph_ops not-available catch-all.
        #[cfg(all(feature = "streaming", feature = "stream"))]
        Method::CepSubscribe { .. } | Method::CepPoll { .. } | Method::CepUnsubscribe { .. } => {
            let carrier = match CarrierAuthority::from_verified(&verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req.id, denied),
            };
            match crate::server::cep::try_handle(state, req.id, &carrier, req.method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a CEP method.
                Err(_) => Response::err(req.id, "cep dispatch routing error"),
            }
        }

        // ── Distributed OWL reasoning (CONCEPT:EG-KG.ontology.concept-13) ─────────────
        // Cross-shard: reasons over the UNION of several graphs, so it self-routes
        // here (with `state` to gather each shard's snapshot) BEFORE the per-graph
        // chain — never through `dispatch_graph_op`, which targets a single `req.graph`.
        // Gated `owl`: in a build without it the variant isn't in the enum.
        #[cfg(feature = "owl")]
        Method::OwlReasonDistributed { .. } => {
            let read_authority = {
                let s = timed_read(state).await;
                match GraphReadAuthority::from_verified(&verified_context, &s.isolation) {
                    Ok(authority) => authority,
                    Err(denied) => return Response::err(req.id, denied),
                }
            };
            match handlers::rdf::try_handle_distributed(
                state,
                req.id,
                &read_authority,
                req.method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: the only variant routed here is OwlReasonDistributed.
                Err(_) => Response::err(req.id, "owl distributed dispatch routing error"),
            }
        }

        // ── Graph operations (dispatch to target graph) ──────────────
        Method::ApplyChangeEnvelope { envelope } => {
            let claims = verified_context.claims();
            let batch_context = &envelope.mutation.context;
            if envelope.mutation.graph != req.graph
                || envelope.mutation.tenant != claims.tenant
                || batch_context.request_id != req.id
                || batch_context.principal != verified_context.principal_persistence_id()
                || envelope.mutation.idempotency_key != verified_context.idempotency_key()
                || batch_context.policy_fingerprint.as_deref()
                    != Some(claims.policy_version.as_str())
            {
                return Response::err(
                    req.id,
                    "ApplyChangeEnvelope context does not match the verified request authority",
                );
            }
            dispatch_graph_op(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                Method::ApplyChangeEnvelope { envelope },
            )
            .await
        }
        Method::ApplyChangeEnvelopes { envelopes } => {
            dispatch_change_envelopes(
                state,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                envelopes,
            )
            .await
        }
        #[cfg(feature = "modality-serving")]
        Method::ServedModality { op } => {
            let auth_secret = timed_read(state).await.auth_secret.clone();
            let authority = match handlers::modality::ModalityAuthority::from_verified(
                &auth_secret,
                verified_context.claims(),
            ) {
                Ok(authority) => authority,
                Err(error) => return Response::err(req.id, error),
            };
            dispatch_served_modality(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                op,
                authority,
            )
            .await
        }
        #[cfg(feature = "knowledge-batch")]
        Method::KnowledgeStream { request } => {
            let auth_secret = timed_read(state).await.auth_secret.clone();
            let authority = match handlers::knowledge_stream::KnowledgeStreamAuthority::from_verified(
                &auth_secret,
                verified_context.claims(),
            ) {
                Ok(authority) => authority,
                Err(error) => return Response::err(req.id, error),
            };
            dispatch_knowledge_stream(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                request,
                authority,
            )
            .await
        }
        Method::GetChangeEnvelope {
            envelope_id,
            tenant,
        } => {
            if tenant != verified_context.claims().tenant {
                return Response::err(
                    req.id,
                    "ChangeEnvelope reads require the verified tenant context",
                );
            }
            dispatch_graph_op(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                Method::GetChangeEnvelope {
                    envelope_id,
                    tenant,
                },
            )
            .await
        }
        Method::GetContentVersion { object_id, tenant } => {
            if tenant != verified_context.claims().tenant {
                return Response::err(
                    req.id,
                    "content-version reads require the verified tenant context",
                );
            }
            dispatch_graph_op(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                Method::GetContentVersion { object_id, tenant },
            )
            .await
        }
        Method::GetChangeCursor {
            source,
            partition,
            tenant,
        } => {
            if tenant != verified_context.claims().tenant {
                return Response::err(
                    req.id,
                    "change-cursor reads require the verified tenant context",
                );
            }
            dispatch_graph_op(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                Method::GetChangeCursor {
                    source,
                    partition,
                    tenant,
                },
            )
            .await
        }
        // Natural-language query (CONCEPT:EG-KG.query.core-query-input/EG-080): the graph rides the METHOD
        // (the `/nl` HTTP facade path has no request envelope), so route to the method's
        // `graph`, falling back to the request envelope's graph when it is empty. The
        // handler (behind `nl-query`) turns NL→UQL and runs the deterministic
        // `UnifiedQueryText` pipeline; a build without `nl-query` reaches the graph_ops
        // "not available" catch-all like any other feature-off method.
        Method::NlQuery { ref graph, .. } => {
            let target = if graph.is_empty() {
                req.graph.clone()
            } else {
                graph.clone()
            };
            dispatch_graph_op(
                state,
                &target,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                req.method,
            )
            .await
        }
        // Batched CROSS-GRAPH write (CONCEPT:EG-KG.storage.multi-graph-batch-write) — the
        // graphs ride the METHOD (one round-trip, many graphs), so like the txn/ts
        // self-routing ops it is handled HERE, BEFORE the single-`req.graph`
        // graph-op path. Each sub-batch fans through the normal per-graph write
        // path CONCURRENTLY, so N distinct graphs commit across N of the K shard
        // writers in parallel.
        Method::MultiGraphBatchUpdate { batches_msgpack } => {
            multi_graph_batch_update(
                state,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
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
                &verified_context,
                &req.graph,
                query,
            )
            .await
        }
        _ => {
            dispatch_graph_op(
                state,
                &req.graph,
                req.id,
                req.agent_id.as_deref(),
                &verified_context,
                req.method,
            )
            .await
        }
    };
    if response.error.is_none() {
        if let Some(control) = session_control {
            if let Err(error) = finish_session_control_saga(control) {
                return Response::err(req.id, error);
            }
        }
    }
    response
}

#[cfg(all(feature = "redb", feature = "security"))]
const MAX_PRIVATE_COORDINATOR_PLAN_BYTES: usize = 256 * 1024 * 1024;

#[cfg(all(feature = "redb", feature = "security"))]
fn seal_private_coordinator_plan<T: serde::Serialize>(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    plan: &T,
) -> Result<(String, Vec<u8>), String> {
    let cipher = backend.transaction_recovery_cipher().ok_or_else(|| {
        format!(
            "coordinator recovery requires {} to be configured",
            crate::crypto::ENCRYPTION_KEY_ENV
        )
    })?;
    seal_private_coordinator_plan_with_cipher(&cipher, plan)
}

#[cfg(all(feature = "redb", feature = "security"))]
fn seal_private_coordinator_plan_with_cipher<T: serde::Serialize>(
    cipher: &crate::crypto::ValueCipher,
    plan: &T,
) -> Result<(String, Vec<u8>), String> {
    use sha2::{Digest, Sha256};
    let plaintext = rmp_serde::to_vec_named(plan).map_err(|error| error.to_string())?;
    if plaintext.is_empty() || plaintext.len() > MAX_PRIVATE_COORDINATOR_PLAN_BYTES {
        return Err("coordinator recovery plan exceeds resource limits".to_string());
    }
    let digest = hex::encode(Sha256::digest(&plaintext));
    Ok((digest, cipher.seal(&plaintext)))
}

#[cfg(all(feature = "redb", feature = "security"))]
fn private_coordinator_plan_digest(
    batch: &crate::mutation_batch::MutationBatch,
    event_type: &str,
) -> Result<String, String> {
    let [operation] = batch.operations.as_slice() else {
        return Err("coordinator parent has an invalid operation inventory".to_string());
    };
    let Method::ApplyMutation {
        event_type: observed,
        query,
    } = &operation.method
    else {
        return Err("coordinator parent has an invalid operation".to_string());
    };
    let digest = query
        .strip_prefix("sha256:")
        .filter(|digest| digest.len() == 64)
        .ok_or_else(|| "coordinator parent has an invalid plan digest".to_string())?;
    if observed != event_type
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err("coordinator parent has an invalid plan binding".to_string());
    }
    Ok(digest.to_string())
}

#[cfg(all(feature = "redb", feature = "security"))]
fn open_private_coordinator_plan<T: serde::de::DeserializeOwned>(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch: &crate::mutation_batch::MutationBatch,
    event_type: &str,
    encrypted: &[u8],
) -> Result<T, String> {
    let cipher = backend.transaction_recovery_cipher().ok_or_else(|| {
        format!(
            "coordinator recovery requires {} to be configured",
            crate::crypto::ENCRYPTION_KEY_ENV
        )
    })?;
    open_private_coordinator_plan_with_cipher(&cipher, batch, event_type, encrypted)
}

#[cfg(all(feature = "redb", feature = "security"))]
fn open_private_coordinator_plan_with_cipher<T: serde::de::DeserializeOwned>(
    cipher: &crate::crypto::ValueCipher,
    batch: &crate::mutation_batch::MutationBatch,
    event_type: &str,
    encrypted: &[u8],
) -> Result<T, String> {
    use sha2::{Digest, Sha256};
    if !crate::crypto::is_sealed(encrypted) {
        return Err("coordinator recovery plan is not authenticated ciphertext".to_string());
    }
    let plaintext = cipher
        .unseal(encrypted)
        .map_err(|_| "coordinator recovery plan authentication failed".to_string())?;
    if plaintext.is_empty() || plaintext.len() > MAX_PRIVATE_COORDINATOR_PLAN_BYTES {
        return Err("coordinator recovery plan exceeds resource limits".to_string());
    }
    let observed = hex::encode(Sha256::digest(&plaintext));
    if observed != private_coordinator_plan_digest(batch, event_type)? {
        return Err("coordinator recovery plan digest mismatch".to_string());
    }
    eg_types::msgpack::decode_bounded(
        &plaintext,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_PRIVATE_COORDINATOR_PLAN_BYTES,
            4_000_000,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "coordinator recovery plan is invalid".to_string())
}

#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
const SPARQL_RECOVERY_EVENT: &str = "sparql_http_recovery_plan_v1";
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
const SPARQL_COMPENSATION_EVENT: &str = "sparql_http_compensation_v1";

#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SparqlRecoveryPlanV1 {
    schema_version: u8,
    graphs: Vec<crate::server::sparql_http::PlannedGraphUpdate>,
}

#[cfg(all(feature = "redb", feature = "security", feature = "sparql-http"))]
fn coordinator_result_is_compensated(result: &ResultPayload) -> bool {
    matches!(result, ResultPayload::Json(value)
        if value.get("outcome").and_then(serde_json::Value::as_str) == Some("compensated"))
}

#[cfg(all(
    feature = "sparql-http",
    feature = "redb",
    feature = "security",
    feature = "raft"
))]
async fn clear_coordinated_graph_decision(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    coordinator_id: &str,
) -> Result<(), String> {
    backend.xshard_decision_clear(coordinator_id).await
}

#[cfg(all(
    feature = "sparql-http",
    feature = "redb",
    feature = "security",
    not(feature = "raft")
))]
async fn clear_coordinated_graph_decision(
    _backend: &crate::server::persistence::redb_backend::RedbBackend,
    _coordinator_id: &str,
) -> Result<(), String> {
    Ok(())
}

/// Coordinate one signed SPARQL HTTP update over detached graph images. The
/// complete before/after plan is authenticated ciphertext bound to a digest-only
/// parent before lifecycle or graph state changes. Clustered graph spans use the
/// retained-decision cross-shard 2PC authority; local spans use its deterministic
/// child MutationBatches. A durable compensation marker makes restart choose one
/// direction forever, so a crash cannot alternate roll-forward and rollback.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn coordinated_sparql_http_update(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    default_graph: &str,
    query: String,
) -> Response {
    let verified_actor = verified_context.agent_id().trim();
    if verified_actor.is_empty() {
        return Response::err(req_id, "ACCESS_DENIED: SPARQL update has no verified actor");
    }
    let coordinator_caller = Some(verified_actor);
    let parent_method = Method::ApplyMutation {
        event_type: crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT.to_string(),
        query: query.clone(),
    };
    let backend = timed_read(state).await.persistence.clone();
    let Some(backend) = backend else {
        return Response::err(req_id, "SPARQL HTTP update requires durable persistence");
    };
    let Some(redb) = backend.as_redb() else {
        return Response::err(req_id, "SPARQL HTTP update requires durable redb");
    };
    let parent_id = crate::server::mutation_batch::opaque_request_key(
        "sparql-http-parent",
        default_graph,
        req_id,
        &parent_method,
    );
    let compensation_id = crate::server::mutation_batch::opaque_coordinator_key(
        "sparql-http-compensation",
        default_graph,
        &parent_id,
    );
    let resumed =
        match handlers::admin::resume_named_admin_saga(redb, &parent_id, coordinator_caller) {
            Ok(value) => value,
            Err(error) => return Response::err(req_id, error),
        };
    let (saga, plan) = if let Some(saga) = resumed {
        if let Some(result) = saga.replayed.clone() {
            if let Err(error) = clear_coordinated_graph_decision(redb, &parent_id).await {
                return Response::err(req_id, error);
            }
            if let Err(error) = clear_coordinated_graph_decision(redb, &compensation_id).await {
                return Response::err(req_id, error);
            }
            return if coordinator_result_is_compensated(&result) {
                Response::err(req_id, "SPARQL update was durably compensated")
            } else {
                Response::ok(req_id, result)
            };
        }
        let encrypted = match eg_mutation_store::read_private_payload(
            redb.admin_mutation_store(),
            &parent_id,
        ) {
            Ok(Some(value)) => value,
            Ok(None) => return Response::err(req_id, "SPARQL recovery plan is missing"),
            Err(error) => return Response::err(req_id, error),
        };
        let plan = match open_private_coordinator_plan(
            redb,
            &saga.batch,
            SPARQL_RECOVERY_EVENT,
            &encrypted,
        ) {
            Ok(value) => value,
            Err(error) => return Response::err(req_id, error),
        };
        (saga, plan)
    } else {
        let mut graphs = match crate::server::sparql_http::update_graphs(&query, default_graph) {
            Ok(graphs) => graphs,
            Err(error) => return Response::err(req_id, error),
        };
        if crate::server::sparql_http::update_uses_variable_graph(&query) {
            graphs.extend(
                state
                    .read()
                    .await
                    .registry
                    .list()
                    .into_iter()
                    .map(|(name, _)| name),
            );
            graphs.sort();
            graphs.dedup();
        }
        let missing = {
            let current = timed_read(state).await;
            for graph in &graphs {
                if let Some(entry) = current.registry.get(graph) {
                    if let Err(error) = check_graph_access(
                        &current.isolation,
                        coordinator_caller,
                        graph,
                        entry.graph_type,
                        entry.owner.as_deref(),
                        AccessLevel::Write,
                    ) {
                        return Response::err(req_id, error);
                    }
                }
            }
            graphs
                .iter()
                .filter(|graph| !current.registry.exists(graph))
                .cloned()
                .collect::<Vec<_>>()
        };
        if !missing.is_empty() && !verified_context.allows_method("graph:admin", true) {
            return Response::err(
                req_id,
                "ACCESS_DENIED: SPARQL graph creation requires graph:admin",
            );
        }
        let planned =
            match crate::server::sparql_http::plan_update(state, &query, default_graph, &graphs)
                .await
            {
                Ok(planned) => planned,
                Err(error) => return Response::err(req_id, error),
            };
        let plan = SparqlRecoveryPlanV1 {
            schema_version: 1,
            graphs: planned,
        };
        let (digest, encrypted) = match seal_private_coordinator_plan(redb, &plan) {
            Ok(value) => value,
            Err(error) => return Response::err(req_id, error),
        };
        let saga = match handlers::admin::begin_named_admin_saga_with_private_payload(
            redb,
            req_id,
            coordinator_caller,
            handlers::admin::AdminSagaPayload {
                domain: crate::mutation_batch::MutationDomain::MultiGraph,
                batch_id: &parent_id,
                event_type: SPARQL_RECOVERY_EVENT,
                payload_digest: &digest,
                encrypted_payload: &encrypted,
            },
        ) {
            Ok(saga) => saga,
            Err(error) => return Response::err(req_id, error),
        };
        (saga, plan)
    };
    if plan.schema_version != 1 {
        return Response::err(req_id, "unsupported SPARQL recovery plan");
    }

    let mut compensation_saga = match handlers::admin::resume_named_admin_saga(
        redb,
        &compensation_id,
        coordinator_caller,
    ) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error),
    };
    if compensation_saga.is_none() {
        for update in plan.graphs.iter().filter(|update| !update.existed_before) {
            if timed_read(state).await.registry.exists(&update.graph) {
                continue;
            }
            let request = Request {
                id: req_id,
                graph: "__commons__".to_string(),
                auth_token: String::new(),
                agent_id: Some(verified_actor.to_string()),
                method: Method::CreateGraph {
                    graph_name: update.graph.clone(),
                    graph_type: update.graph_type,
                },
            };
            let response = Box::pin(dispatch_with_context(
                state,
                request,
                Some(verified_context.clone()),
            ))
            .await;
            if let Some(error) = response.error {
                return Response::err(req_id, error);
            }
        }
        let methods = plan
            .graphs
            .iter()
            .map(|update| {
                (
                    update.graph.clone(),
                    update.graph_type,
                    vec![Method::FromMsgpack {
                        msgpack: update.after_msgpack.clone(),
                    }],
                )
            })
            .collect();
        let forward = handlers::txn::commit_coordinated_graph_methods(
            state,
            req_id,
            coordinator_caller,
            &parent_id,
            methods,
        )
        .await;
        if forward.error.is_none() && !matches!(forward.result, Some(ResultPayload::Bool(false))) {
            let result = ResultPayload::Json(serde_json::json!({
                "outcome": "committed",
                "updated_graphs": plan.graphs.len(),
                "created_graphs": plan.graphs.iter().filter(|graph| !graph.existed_before).count(),
            }));
            return match handlers::admin::finish_admin_saga(
                redb,
                saga.batch,
                saga.created_at_ms,
                result,
            ) {
                Ok(result) => {
                    if let Err(error) = clear_coordinated_graph_decision(redb, &parent_id).await {
                        Response::err(req_id, error)
                    } else {
                        Response::ok(req_id, result)
                    }
                }
                Err(error) => Response::err(req_id, error),
            };
        }
        let marker = SparqlRecoveryPlanV1 {
            schema_version: 1,
            graphs: Vec::new(),
        };
        let (digest, encrypted) = match seal_private_coordinator_plan(redb, &marker) {
            Ok(value) => value,
            Err(error) => return Response::err(req_id, error),
        };
        let marker = match handlers::admin::begin_named_admin_saga_with_private_payload(
            redb,
            req_id,
            coordinator_caller,
            handlers::admin::AdminSagaPayload {
                domain: crate::mutation_batch::MutationDomain::MultiGraph,
                batch_id: &compensation_id,
                event_type: SPARQL_COMPENSATION_EVENT,
                payload_digest: &digest,
                encrypted_payload: &encrypted,
            },
        ) {
            Ok(marker) => marker,
            Err(error) => return Response::err(req_id, error),
        };
        compensation_saga = Some(marker);
    }

    let rollback_methods = plan
        .graphs
        .iter()
        .filter(|update| update.existed_before)
        .map(|update| {
            (
                update.graph.clone(),
                update.graph_type,
                vec![Method::FromMsgpack {
                    msgpack: update.before_msgpack.clone(),
                }],
            )
        })
        .collect::<Vec<_>>();
    if !rollback_methods.is_empty() {
        let rollback = handlers::txn::commit_coordinated_graph_methods(
            state,
            req_id,
            coordinator_caller,
            &compensation_id,
            rollback_methods,
        )
        .await;
        if let Some(error) = rollback.error {
            return Response::err(req_id, format!("SPARQL compensation pending: {error}"));
        }
        if matches!(rollback.result, Some(ResultPayload::Bool(false))) {
            return Response::err(req_id, "SPARQL compensation decision aborted");
        }
    }
    for update in plan
        .graphs
        .iter()
        .rev()
        .filter(|update| !update.existed_before)
    {
        if !timed_read(state).await.registry.exists(&update.graph) {
            continue;
        }
        let request = Request {
            id: req_id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(verified_actor.to_string()),
            method: Method::DeleteGraph {
                graph_name: update.graph.clone(),
            },
        };
        let response = Box::pin(dispatch_with_context(
            state,
            request,
            Some(verified_context.clone()),
        ))
        .await;
        if let Some(error) = response.error {
            return Response::err(req_id, format!("SPARQL compensation pending: {error}"));
        }
    }
    let result = ResultPayload::Json(serde_json::json!({
        "outcome": "compensated",
        "updated_graphs": 0,
        "created_graphs": 0,
    }));
    if let Some(marker) = compensation_saga {
        if let Err(error) = handlers::admin::finish_admin_saga(
            redb,
            marker.batch,
            marker.created_at_ms,
            ResultPayload::Bool(true),
        ) {
            return Response::err(req_id, error);
        }
    }
    match handlers::admin::finish_admin_saga(redb, saga.batch, saga.created_at_ms, result) {
        Ok(_) => {
            if let Err(error) = clear_coordinated_graph_decision(redb, &parent_id).await {
                return Response::err(req_id, error);
            }
            if let Err(error) = clear_coordinated_graph_decision(redb, &compensation_id).await {
                return Response::err(req_id, error);
            }
            Response::err(req_id, "SPARQL update was durably compensated")
        }
        Err(error) => Response::err(req_id, error),
    }
}

#[cfg(all(
    feature = "sparql-http",
    not(all(feature = "redb", feature = "security"))
))]
async fn coordinated_sparql_http_update(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _verified_context: &VerifiedRequestContext,
    _default_graph: &str,
    _query: String,
) -> Response {
    Response::err(
        req_id,
        "SPARQL HTTP update requires a build with durable redb support",
    )
}

#[cfg(all(test, feature = "redb", feature = "security", feature = "sparql-http"))]
mod coordinator_restart_tests {
    use super::*;
    use crate::mutation_batch::{MutationBatchStatus, MutationDomain, MutationSurface};

    fn parent_batch(
        id: &str,
        digest: &str,
        event_type: &str,
    ) -> crate::mutation_batch::MutationBatch {
        crate::server::mutation_batch::compile_opaque_digest(
            crate::server::mutation_batch::CompileBatch {
                batch_id: id,
                request_id: 41,
                principal: Some("system"),
                tenant: "native",
                graph: "cluster-admin",
                placement_epoch: 0,
                idempotency_key: id,
                expected_graph_version: Some(0),
                fencing_token: None,
                created_at_ms: 1,
                default_surface: MutationSurface::Other,
                authoritative_state: None,
            },
            digest,
            MutationSurface::Other,
            MutationDomain::MultiGraph,
            event_type,
        )
        .unwrap()
    }

    #[test]
    fn encrypted_sparql_preimages_survive_process_restart_and_tamper_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordinator.redb");
        let cipher = crate::crypto::ValueCipher::from_key_material(b"restart-test-key");
        let plan = SparqlRecoveryPlanV1 {
            schema_version: 1,
            graphs: vec![crate::server::sparql_http::PlannedGraphUpdate {
                graph: "graph-opaque".to_string(),
                graph_type: crate::protocol::GraphType::Global,
                existed_before: true,
                before_msgpack: vec![0x91, 0x01],
                after_msgpack: vec![0x91, 0x02],
            }],
        };
        let (digest, encrypted) =
            seal_private_coordinator_plan_with_cipher(&cipher, &plan).unwrap();
        let batch = parent_batch("sparql-parent", &digest, SPARQL_RECOVERY_EVENT);
        {
            let db = redb::Database::create(&path).unwrap();
            eg_mutation_store::initialize(&db).unwrap();
            assert!(matches!(
                eg_mutation_store::prepare_saga_with_private_payload(
                    &db,
                    &batch,
                    1,
                    Some(&encrypted),
                )
                .unwrap(),
                eg_mutation_store::SagaBegin::Execute
            ));
        }
        let db = redb::Database::open(&path).unwrap();
        let record = eg_mutation_store::read_record(&db, &batch.batch_id)
            .unwrap()
            .unwrap();
        assert_eq!(record.status, MutationBatchStatus::Prepared);
        let recovered = eg_mutation_store::read_private_payload(&db, &batch.batch_id)
            .unwrap()
            .unwrap();
        let opened: SparqlRecoveryPlanV1 = open_private_coordinator_plan_with_cipher(
            &cipher,
            &record.batch,
            SPARQL_RECOVERY_EVENT,
            &recovered,
        )
        .unwrap();
        assert_eq!(opened.graphs[0].before_msgpack, vec![0x91, 0x01]);
        let mut tampered = recovered;
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        assert!(
            open_private_coordinator_plan_with_cipher::<SparqlRecoveryPlanV1>(
                &cipher,
                &record.batch,
                SPARQL_RECOVERY_EVENT,
                &tampered,
            )
            .is_err()
        );
    }

    #[test]
    fn durable_compensation_marker_fixes_restart_direction_and_erases_its_plan() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compensation.redb");
        let cipher = crate::crypto::ValueCipher::from_key_material(b"compensation-test-key");
        let parent_plan = SparqlRecoveryPlanV1 {
            schema_version: 1,
            graphs: Vec::new(),
        };
        let (parent_digest, parent_encrypted) =
            seal_private_coordinator_plan_with_cipher(&cipher, &parent_plan).unwrap();
        let parent = parent_batch("sparql-parent", &parent_digest, SPARQL_RECOVERY_EVENT);
        let (marker_digest, marker_encrypted) =
            seal_private_coordinator_plan_with_cipher(&cipher, &parent_plan).unwrap();
        let marker = parent_batch(
            "sparql-compensation",
            &marker_digest,
            SPARQL_COMPENSATION_EVENT,
        );
        {
            let db = redb::Database::create(&path).unwrap();
            eg_mutation_store::initialize(&db).unwrap();
            eg_mutation_store::prepare_saga_with_private_payload(
                &db,
                &parent,
                1,
                Some(&parent_encrypted),
            )
            .unwrap();
            eg_mutation_store::prepare_saga_with_private_payload(
                &db,
                &marker,
                2,
                Some(&marker_encrypted),
            )
            .unwrap();
            let result = rmp_serde::to_vec_named(&ResultPayload::Bool(true)).unwrap();
            eg_mutation_store::commit_saga(&db, &marker, result, 3).unwrap();
        }
        let db = redb::Database::open(&path).unwrap();
        assert_eq!(
            eg_mutation_store::read_record(&db, &parent.batch_id)
                .unwrap()
                .unwrap()
                .status,
            MutationBatchStatus::Prepared
        );
        assert_eq!(
            eg_mutation_store::read_record(&db, &marker.batch_id)
                .unwrap()
                .unwrap()
                .status,
            MutationBatchStatus::Committed
        );
        assert!(
            eg_mutation_store::read_private_payload(&db, &parent.batch_id)
                .unwrap()
                .is_some()
        );
        assert!(
            eg_mutation_store::read_private_payload(&db, &marker.batch_id)
                .unwrap()
                .is_none()
        );
    }
}

/// Batch envelope coordinator (CONCEPT:EG-KG.ingest.batched-change-envelopes). Validates
/// each envelope's context against the verified request authority, groups envelopes
/// by their `mutation.graph`, and routes each graph's envelopes to `dispatch_graph_op`
/// (which resolves that graph's Write ACL + placement and commits the group in ONE
/// coalesced transaction). Per-graph groups are independent (partial success across
/// graphs); within a graph the commit is atomic. Per-envelope results are reassembled
/// into REQUEST order under `{"results": [...]}` so a caller can advance a watermark
/// through the contiguous success prefix.
#[cfg(not(feature = "redb"))]
async fn dispatch_change_envelopes(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _verified_context: &VerifiedRequestContext,
    _envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> Response {
    Response::err(
        req_id,
        "batch change-envelope commit requires a build with durable redb support",
    )
}

#[cfg(feature = "redb")]
async fn dispatch_change_envelopes(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> Response {
    let total = envelopes.len();
    if total == 0 {
        return Response::ok(
            req_id,
            ResultPayload::Json(serde_json::json!({ "results": [] })),
        );
    }
    if total > crate::change_envelope::MAX_ENVELOPES_PER_BATCH {
        return Response::err(
            req_id,
            format!(
                "CHANGE_BATCH_TOO_LARGE: {total} envelopes exceed the {} cap",
                crate::change_envelope::MAX_ENVELOPES_PER_BATCH
            ),
        );
    }
    // Per-envelope authority binding — mirror the single `ApplyChangeEnvelope` arm,
    // minus the two batch-varying fields: the idempotency_key (per envelope, enforced
    // by the mutation-store idempotency table) and the graph (per envelope, ACL-checked
    // per group by `dispatch_graph_op`).
    let claims = verified_context.claims();
    let principal = verified_context.principal_persistence_id();
    for envelope in &envelopes {
        let ctx = &envelope.mutation.context;
        if envelope.mutation.tenant != claims.tenant
            || ctx.request_id != req_id
            || ctx.principal != principal
            || ctx.policy_fingerprint.as_deref() != Some(claims.policy_version.as_str())
        {
            return Response::err(
                req_id,
                "ApplyChangeEnvelopes context does not match the verified request authority",
            );
        }
    }

    // Group envelope indices by graph, preserving first-seen graph order and the
    // per-graph envelope order.
    let mut graph_order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<
        String,
        Vec<(usize, crate::change_envelope::ChangeEnvelope)>,
    > = std::collections::HashMap::new();
    for (index, envelope) in envelopes.into_iter().enumerate() {
        let graph = envelope.mutation.graph.clone();
        groups
            .entry(graph.clone())
            .or_insert_with(|| {
                graph_order.push(graph.clone());
                Vec::new()
            })
            .push((index, envelope));
    }

    let mut per_index: Vec<serde_json::Value> = vec![serde_json::Value::Null; total];
    for graph in graph_order {
        let group = groups.remove(&graph).expect("grouped graph is present");
        let indices: Vec<usize> = group.iter().map(|(index, _)| *index).collect();
        let group_envelopes: Vec<crate::change_envelope::ChangeEnvelope> =
            group.into_iter().map(|(_, envelope)| envelope).collect();
        let resp = dispatch_graph_op(
            state,
            &graph,
            req_id,
            caller,
            verified_context,
            Method::ApplyChangeEnvelopes {
                envelopes: group_envelopes,
            },
        )
        .await;
        if let Some(err) = resp.error {
            // A transport/ACL/placement failure for the whole group (distinct from the
            // per-envelope atomic-batch abort, which returns Ok with conflict entries).
            for index in &indices {
                per_index[*index] = serde_json::json!({ "status": "conflict", "error": err });
            }
        } else if let Some(ResultPayload::Json(value)) = resp.result {
            let group_results = value
                .get("results")
                .and_then(|results| results.as_array())
                .cloned()
                .unwrap_or_default();
            for (position, index) in indices.iter().enumerate() {
                per_index[*index] = group_results.get(position).cloned().unwrap_or_else(|| {
                    serde_json::json!({
                        "status": "conflict",
                        "error": "missing per-envelope result in batch response",
                    })
                });
            }
        } else {
            for index in &indices {
                per_index[*index] = serde_json::json!({
                    "status": "conflict",
                    "error": "empty batch response",
                });
            }
        }
    }

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({ "results": per_index })),
    )
}

/// Apply a batched cross-graph write (CONCEPT:EG-KG.storage.multi-graph-batch-write).
///
/// `batches_msgpack` decodes to `Vec<(graph_name, operations_msgpack)>` where each
/// inner blob is exactly a [`Method::BatchUpdate`] payload. Every sub-batch is
/// dispatched through the ordinary per-graph write path
/// ([`dispatch_graph_op`]) CONCURRENTLY on the async runtime, so distinct graphs
/// take DISTINCT per-graph write locks and commit across the K redb shard writers
/// in parallel — the client pays ONE round-trip instead of N that each re-acquire
/// a lock. Reuses the existing `BatchUpdate` primitive, so persistence /
/// Raft / CDC / access-control all apply per sub-batch exactly as a normal batch.
///
/// The reply is `{"results": {graph: <batch_result>}, "errors": {graph: msg}}`;
/// one graph's failure never aborts the others (partial-success contract).
#[cfg(feature = "redb")]
async fn multi_graph_batch_update(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    batches_msgpack: &[u8],
) -> Response {
    let batches = match decode_multi_graph_batches(batches_msgpack) {
        Ok(batches) => batches,
        Err(error) => return Response::err(req_id, error),
    };
    let backend = {
        let s = timed_read(state).await;
        s.persistence.clone()
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "multi-graph batch requires durable persistence");
    };
    let Some(redb) = backend.as_redb() else {
        return Response::err(req_id, "multi-graph batch requires durable redb");
    };
    #[cfg(feature = "raft")]
    let clustered = timed_read(state).await.multi_raft.is_some();
    #[cfg(not(feature = "raft"))]
    let clustered = false;
    let saga = if clustered {
        None
    } else {
        let method = Method::MultiGraphBatchUpdate {
            batches_msgpack: batches_msgpack.to_vec(),
        };
        let saga = match handlers::admin::begin_admin_saga(
            redb,
            req_id,
            caller,
            &method,
            crate::mutation_batch::MutationDomain::MultiGraph,
        ) {
            Ok(saga) => saga,
            Err(error) => return Response::err(req_id, error),
        };
        if let Some(result) = saga.replayed.clone() {
            return Response::ok(req_id, result);
        }
        Some(saga)
    };
    let mut results = serde_json::Map::new();
    let mut errors = serde_json::Map::new();
    if batches.is_empty() {
        let result = ResultPayload::Json(serde_json::json!({"results": results, "errors": errors}));
        return if let Some(saga) = saga {
            match handlers::admin::finish_admin_saga(redb, saga.batch, saga.created_at_ms, result) {
                Ok(result) => Response::ok(req_id, result),
                Err(error) => Response::err(req_id, error),
            }
        } else {
            Response::ok(req_id, result)
        };
    }

    // Fan each sub-batch onto its own task so distinct graphs apply concurrently.
    // The Arc<RwLock<ServerState>> is cheaply cloned; dispatch_graph_op takes the
    // registry read-lock only briefly then releases it before the per-graph write
    // lock, so the writes overlap across shard writers.
    let caller_owned = caller.map(str::to_string);
    let mut set = tokio::task::JoinSet::new();
    for (graph, ops) in batches {
        let state = Arc::clone(state);
        let caller_owned = caller_owned.clone();
        let verified_context = verified_context.clone();
        set.spawn(async move {
            let resp = dispatch_graph_op(
                &state,
                &graph,
                req_id,
                caller_owned.as_deref(),
                &verified_context,
                Method::BatchUpdate {
                    operations_msgpack: ops.into_vec(),
                },
            )
            .await;
            (graph, resp)
        });
    }

    while let Some(joined) = set.join_next().await {
        match joined {
            Ok((graph, resp)) => {
                if let Some(err) = resp.error {
                    errors.insert(graph, serde_json::Value::String(err));
                } else if let Some(ResultPayload::Json(v)) = resp.result {
                    results.insert(graph, v);
                } else {
                    results.insert(graph, serde_json::Value::Null);
                }
            }
            Err(join_err) => {
                // A panicked/cancelled sub-batch task — surface it, don't abort.
                let _ = join_err;
                errors.insert(
                    format!("__join_error_{}", errors.len()),
                    serde_json::Value::String("sub-batch execution failed".to_string()),
                );
            }
        }
    }

    let result = ResultPayload::Json(serde_json::json!({"results": results, "errors": errors}));
    if let Some(saga) = saga {
        match handlers::admin::finish_admin_saga(redb, saga.batch, saga.created_at_ms, result) {
            Ok(result) => Response::ok(req_id, result),
            Err(error) => Response::err(req_id, error),
        }
    } else {
        Response::ok(req_id, result)
    }
}

#[cfg(not(feature = "redb"))]
async fn multi_graph_batch_update(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _verified_context: &VerifiedRequestContext,
    _batches_msgpack: &[u8],
) -> Response {
    Response::err(
        req_id,
        "multi-graph batch requires a build with durable redb support",
    )
}

const MAX_MULTI_GRAPH_BATCHES: usize = 256;
const MAX_MULTI_GRAPH_NAME_BYTES: usize = 512;
const MAX_MULTI_GRAPH_OPERATIONS_BYTES: usize = 32 * 1024 * 1024;
const MAX_MULTI_GRAPH_TOTAL_OPERATIONS_BYTES: usize = 64 * 1024 * 1024;
const MAX_MULTI_GRAPH_OPERATION_ITEMS: usize = 500_000;

fn decode_multi_graph_batches(
    batches_msgpack: &[u8],
) -> Result<Vec<(String, serde_bytes::ByteBuf)>, String> {
    // The outer request preflight protects this decoder on the served path. Keep
    // the check local as well so direct unit/library callers cannot bypass it.
    let batches: Vec<(String, serde_bytes::ByteBuf)> = eg_types::msgpack::decode_bounded(
        batches_msgpack,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_NESTED_MSGPACK_BYTES,
            MAX_NESTED_MSGPACK_ITEMS,
            64,
        ),
    )
    .map_err(|_| "invalid multi-graph batch payload".to_string())?;
    if batches.len() > MAX_MULTI_GRAPH_BATCHES {
        return Err("multi-graph batch count exceeds the resource limit".to_string());
    }
    let mut names = std::collections::HashSet::with_capacity(batches.len());
    let mut total_operations_bytes = 0usize;
    for (graph, operations) in &batches {
        if graph.trim().is_empty()
            || graph.len() > MAX_MULTI_GRAPH_NAME_BYTES
            || graph.chars().any(char::is_control)
        {
            return Err("multi-graph batch contains an invalid graph identifier".to_string());
        }
        if !names.insert(graph.as_str()) {
            return Err("multi-graph batch contains a duplicate graph identifier".to_string());
        }
        total_operations_bytes = total_operations_bytes
            .checked_add(operations.len())
            .ok_or_else(|| "multi-graph batch exceeds the resource limit".to_string())?;
        if total_operations_bytes > MAX_MULTI_GRAPH_TOTAL_OPERATIONS_BYTES {
            return Err("multi-graph batch exceeds the resource limit".to_string());
        }
        super::transport::validate_nested_msgpack(
            operations,
            MAX_MULTI_GRAPH_OPERATIONS_BYTES,
            MAX_MULTI_GRAPH_OPERATION_ITEMS,
        )
        .map_err(str::to_string)?;
    }
    Ok(batches)
}

fn change_envelope_result(
    committed: &eg_types::ChangeEnvelopeCommit,
    projection_pending: bool,
) -> serde_json::Value {
    let mut result = serde_json::to_value(committed).unwrap_or_else(|_| {
        serde_json::json!({
            "envelope_id": committed.envelope_id,
            "batch_id": committed.batch_id,
            "replayed": committed.replayed,
        })
    });
    if let Some(object) = result.as_object_mut() {
        object.insert(
            "projection_pending".to_string(),
            serde_json::Value::Bool(projection_pending),
        );
    }
    result
}

/// Derive the native authority epoch from the registry-published graph
/// incarnation.  The caller cannot provide either value; a delete/recreate
/// therefore fences every capability from the retired incarnation.
fn work_item_capability_authority_epoch(incarnation_id: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(incarnation_id.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("fixed digest width")).max(1)
}

/// Dispatch a graph-level operation to the target named graph, enforcing the
/// isolation ACL (`isolation.rs::check_access`) when rules are registered.
async fn dispatch_graph_op(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    dispatch_graph_op_inner(
        state,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        method,
        #[cfg(feature = "modality-serving")]
        None,
        #[cfg(feature = "knowledge-batch")]
        None,
    )
    .await
}

#[cfg(feature = "modality-serving")]
async fn dispatch_served_modality(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    op: eg_types::ServedModalityOp,
    authority: handlers::modality::ModalityAuthority,
) -> Response {
    dispatch_graph_op_inner(
        state,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        Method::ServedModality { op },
        Some(authority),
        #[cfg(feature = "knowledge-batch")]
        None,
    )
    .await
}

#[cfg(feature = "knowledge-batch")]
async fn dispatch_knowledge_stream(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    request: crate::knowledge_stream::KnowledgeStreamRequestV1,
    authority: handlers::knowledge_stream::KnowledgeStreamAuthority,
) -> Response {
    dispatch_graph_op_inner(
        state,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        Method::KnowledgeStream { request },
        #[cfg(feature = "modality-serving")]
        None,
        Some(authority),
    )
    .await
}

/// Leader-only preparation for a replicated modality mutation. The source-bearing
/// public Method is consumed by the native decoder here and never enters
/// `RaftRequest`; consensus receives only the HMAC-authenticated encrypted runtime
/// node plus a digest-only receipt and compact ApplyOutcome.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
#[allow(clippy::too_many_arguments)]
async fn replicate_served_modality(
    state: &Arc<RwLock<ServerState>>,
    handle: crate::raft::RaftHandle,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    req_id: u64,
    tenant_scope: &str,
    principal_fingerprint: &str,
    core: &Arc<crate::graph::GraphCore>,
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    method: Method,
    authority: &handlers::modality::ModalityAuthority,
) -> Response {
    use sha2::{Digest, Sha256};

    let leader = handle.current_leader().await;
    if leader != Some(handle.node_id) {
        return Response::stale_route(
            req_id,
            graph_name,
            group_id,
            placement_epoch,
            leader,
            "served modality mutations require the current placement leader",
        );
    }

    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    let safe_method = crate::server::mutation::durable_receipt_method(&method);
    let Method::ServedModality { op } = method else {
        return Response::err(
            req_id,
            "served modality replication received the wrong method",
        );
    };
    let receipt_query = match &safe_method {
        Method::ApplyMutation { event_type, query } if event_type == "served_modality_v1" => {
            query.clone()
        }
        _ => return Response::err(req_id, "served modality receipt construction failed"),
    };
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "raft-modality",
        graph_name,
        req_id,
        &safe_method,
    );
    let graph_fname = crate::persist::sanitize(graph_name);

    // A client retry with the same request id repairs RAM from the committed image
    // and returns the exact stored ApplyOutcome. It never re-decodes source bytes or
    // emits a second Raft entry/outbox/audit/CDC event.
    match persistence
        .read_mutation_batch(&graph_fname, &batch_id)
        .await
    {
        Ok(Some(record)) => {
            let encoded = match rmp_serde::to_vec_named(&safe_method) {
                Ok(encoded) => encoded,
                Err(error) => return Response::err(req_id, error.to_string()),
            };
            let expected_operation = format!("sha256:{}", hex::encode(Sha256::digest(encoded)));
            let operation_matches = record.batch.operations.len() == 1
                && matches!(
                    &record.batch.operations[0].method,
                    Method::ApplyMutation { event_type, query }
                        if event_type == "authoritative_state_operation"
                            && query == &expected_operation
                );
            if record.status != crate::mutation_batch::MutationBatchStatus::Committed
                || record.batch.batch_id != batch_id
                || record.batch.tenant != tenant_scope
                || record.batch.graph != graph_name
                || record.batch.placement_epoch != placement_epoch
                || record.batch.fencing_token != fencing_token
                || record.batch.context.principal != principal_fingerprint
                || !operation_matches
            {
                return Response::err(
                    req_id,
                    "replicated modality receipt conflicts with request authority",
                );
            }
            let result = match record.result_msgpack.as_deref() {
                Some(encoded) => match eg_types::msgpack::decode_bounded::<ResultPayload>(
                    encoded,
                    eg_types::msgpack::MsgpackLimits::new(
                        MAX_NESTED_MSGPACK_BYTES,
                        MAX_NESTED_MSGPACK_ITEMS,
                        eg_types::msgpack::DEFAULT_MAX_DEPTH,
                    ),
                ) {
                    Ok(result) => {
                        let valid = matches!(
                            &result,
                            ResultPayload::Raw(outcome)
                                if eg_types::msgpack::decode_bounded::<eg_modality::ApplyOutcome>(
                                    outcome,
                                    eg_types::msgpack::MsgpackLimits::new(
                                        MAX_NESTED_MSGPACK_BYTES,
                                        MAX_NESTED_MSGPACK_ITEMS,
                                        eg_types::msgpack::DEFAULT_MAX_DEPTH,
                                    ),
                                )
                                .is_ok()
                        );
                        if !valid {
                            return Response::err(
                                req_id,
                                "replicated modality receipt has an invalid result",
                            );
                        }
                        result
                    }
                    Err(_) => {
                        return Response::err(
                            req_id,
                            "replicated modality receipt has an invalid result",
                        )
                    }
                },
                None => {
                    return Response::err(
                        req_id,
                        "replicated modality receipt has no terminal result",
                    )
                }
            };
            return match persistence
                .read_authoritative_graph_snapshot(&graph_fname)
                .await
            {
                Ok(Some((snapshot, version))) => {
                    match core.install_committed_snapshot(snapshot, version) {
                        Ok(()) => Response::ok(req_id, result),
                        Err(error) => Response::err(req_id, error),
                    }
                }
                Ok(None) => Response::err(req_id, "committed modality image is missing"),
                Err(error) => Response::err(req_id, error),
            };
        }
        Ok(None) => {}
        Err(error) => return Response::err(req_id, error),
    }

    let (base_snapshot, source_version) = match persistence
        .read_authoritative_graph_snapshot(&graph_fname)
        .await
    {
        Ok(Some(value)) => value,
        Ok(None) => {
            let version = match persistence.read_mutation_graph_version(&graph_fname).await {
                Ok(value) => value.unwrap_or_else(|| core.version()),
                Err(error) => return Response::err(req_id, error),
            };
            (core.snapshot(), version)
        }
        Err(error) => return Response::err(req_id, error),
    };
    let staged = match crate::graph::GraphCore::from_snapshot(base_snapshot, source_version) {
        Ok(staged) => staged,
        Err(error) => return Response::err(req_id, error),
    };
    let Some((operation, modality)) = crate::raft::SanitizedModalityMutation::from_served(&op)
    else {
        return Response::err(req_id, "served modality mutation category is invalid");
    };
    let payload = match handlers::modality::handle(&staged, authority, op) {
        Ok(payload) => payload,
        Err(error) => return Response::err(req_id, error),
    };
    let node_id = authority.node_id(modality);
    let Some(sealed_runtime_state) = staged.get_node_properties(&node_id) else {
        return Response::err(req_id, "served modality produced no encrypted state");
    };
    if !crate::crypto::is_sealed(&sealed_runtime_state) {
        return Response::err(req_id, "served modality produced unsealed state");
    }
    let result_msgpack = match rmp_serde::to_vec_named(&payload) {
        Ok(value) => value,
        Err(error) => return Response::err(req_id, error.to_string()),
    };
    let server_secret = timed_read(state).await.auth_secret.clone();
    let command = match crate::raft::SanitizedModalityRaftCommand::new(
        &server_secret,
        modality,
        operation,
        node_id,
        sealed_runtime_state,
        receipt_query,
        result_msgpack,
    ) {
        Ok(command) => command,
        Err(error) => return Response::err(req_id, error),
    };
    let created_at_ms = authoritative_now_ms();
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        req_id,
        tenant_scope,
        principal_fingerprint.to_string(),
        false,
        placement_epoch,
        fencing_token,
        created_at_ms,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(req_id, error),
    };
    let request = crate::raft::RaftRequest {
        graph_fname,
        graph_name: graph_name.to_string(),
        graph_type,
        command: crate::raft::ReplicatedMutation::served_modality(command),
        committed_at_ms: created_at_ms,
        mutation,
    };
    match handle.client_write(request).await {
        Ok(response) if response.applied => Response::ok(req_id, payload),
        Ok(_) => Response::err(req_id, "replicated modality state was not applied"),
        Err(error) => {
            let leader = handle.current_leader().await;
            Response::stale_route(req_id, graph_name, group_id, placement_epoch, leader, error)
        }
    }
}

/// The caller/isolation-scoping fields shared by every `dispatch_graph_op_inner`
/// entry point, bundled so the function stays under the clippy argument-count
/// ceiling once the feature-gated authority parameters are unified in.
struct GraphOpContext<'a> {
    graph_name: &'a str,
    req_id: u64,
    caller: Option<&'a str>,
    verified_context: &'a VerifiedRequestContext,
}

async fn dispatch_graph_op_inner(
    state: &Arc<RwLock<ServerState>>,
    ctx: GraphOpContext<'_>,
    mut method: Method,
    #[cfg(feature = "modality-serving")] modality_authority: Option<
        handlers::modality::ModalityAuthority,
    >,
    #[cfg(feature = "knowledge-batch")] knowledge_stream_authority: Option<
        handlers::knowledge_stream::KnowledgeStreamAuthority,
    >,
) -> Response {
    let GraphOpContext {
        graph_name,
        req_id,
        caller,
        verified_context,
    } = ctx;
    #[cfg(feature = "cypher")]
    if let Err(error) = handlers::query::validate_cypher_mode(&method) {
        return Response::err(req_id, error);
    }
    #[cfg(feature = "redb")]
    let mut s = timed_read(state).await;
    #[cfg(not(feature = "redb"))]
    let s = timed_read(state).await;
    // Cold-path lazy open (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3): the common case
    // (already resident) never pays for this — only a registry MISS escalates to a
    // write lock. The graph may be catalog-known but not yet materialized (a
    // lazy-startup boot scan, or a graph the bounded hot-context cache evicted
    // back to catalog-only); `lazy_open` is a no-op for a genuinely unknown name,
    // so the "not found" error below is unchanged for that case.
    #[cfg(feature = "redb")]
    if s.registry.get(graph_name).is_none() {
        drop(s);
        let cap = crate::server::persistence::cold_offload::max_resident_graphs();
        let page_size = crate::server::persistence::cold_offload::lazy_open_page_size();
        crate::server::persistence::cold_offload::lazy_open(state, graph_name, cap, page_size)
            .await;
        s = timed_read(state).await;
    }
    let entry = match s.registry.get(graph_name) {
        Some(e) => e,
        None => return Response::err(req_id, format!("Graph '{}' not found", graph_name)),
    };
    if let Some(manifest) = s.registry.materialization_manifest(graph_name) {
        if !manifest.valid {
            let phase = match manifest.phase {
                crate::registry::MaterializationPhase::CatalogOnly => "catalog_only",
                crate::registry::MaterializationPhase::Partial => "partial",
                crate::registry::MaterializationPhase::Complete => "complete",
                crate::registry::MaterializationPhase::Failed => "failed",
            };
            return Response::err(
                req_id,
                serde_json::json!({
                    "code": "PARTIAL_MATERIALIZATION",
                    "phase": phase,
                    "source_snapshot_version": manifest.source_snapshot_version,
                    "completeness_cursor": manifest.completeness_cursor.as_ref().map(|cursor| serde_json::json!({
                        "node_offset": cursor.node_offset,
                        "edge_offset": cursor.edge_offset,
                    })),
                    "retryable": manifest.phase != crate::registry::MaterializationPhase::Failed,
                })
                .to_string(),
            );
        }
    }

    // TsAppend used to self-route before this boundary, which accidentally classified
    // it as neither a graph read nor write. It is now graph-scoped and requires the
    // same Write ACL as every other mutation; all other Ts methods require Read.
    let access = if requires_write(&method) || matches!(&method, Method::TsAppend { .. }) {
        AccessLevel::Write
    } else {
        AccessLevel::Read
    };
    #[cfg(feature = "raft")]
    let state_machine_authorized = is_replicated_apply();
    #[cfg(not(feature = "raft"))]
    let state_machine_authorized = false;
    if !state_machine_authorized {
        if let Err(denied) = check_graph_access(
            &s.isolation,
            caller,
            graph_name,
            entry.graph_type,
            entry.owner.as_deref(),
            access,
        ) {
            return Response::err(req_id, denied);
        }
    }

    // Universal served-data authority (CONCEPT:EG-P0-4): derive the row actor
    // from the cryptographically verified RequestContext while the authoritative
    // IsolationLayer is under the registry lock. Every graph read below either
    // consumes this authority's detached projection or an existing handler that
    // receives the same IsolationLayer. Mutation documents can contain read phases
    // (GraphQL staged CONSTRUCT/UQL), so write requests carry the same verified
    // tenant/actor projection rather than gaining access to the raw committed core.
    let read_authority = match GraphReadAuthority::from_verified(verified_context, &s.isolation) {
        Ok(authority) => Some(authority),
        Err(denied) => return Response::err(req_id, denied),
    };
    let verified_actor = match read_authority
        .as_ref()
        .expect("GraphReadAuthority is constructed above")
        .verified_actor()
    {
        Ok(actor) => actor,
        Err(denied) => return Response::err(req_id, denied),
    };
    // Consumed only by the query/cypher/graphql/rdf/knowledge-stream gateway arms
    // below (each independently feature-gated); a slim build with none of them
    // enabled still needs this binding to compile.
    let _ = verified_actor;
    let tenant_scope = read_authority
        .as_ref()
        .and_then(GraphReadAuthority::carrier)
        .expect("GraphReadAuthority always carries verified tenant authority")
        .tenant_scope()
        .to_string();

    // Mutation-gateway authz context (CONCEPT:EG-P0-2): for a `mutation::
    // GATEWAY_ROUTED` method, `commit_mutation` re-derives its OWN authz decision
    // from `(isolation, graph_type, owner)` rather than trusting the check just
    // above — captured here, before `s`/`entry` drop below, ONLY for the routed
    // set (an `IsolationLayer` clone is not free, so this is skipped entirely for
    // the other ~330 methods).
    let gateway_authz_ctx: Option<crate::server::mutation::GatewayAuthzCtx> =
        if crate::server::mutation::is_gateway_routed(&method) {
            Some((s.isolation.clone(), entry.graph_type, entry.owner.clone()))
        } else {
            None
        };

    let core = entry.core.clone();
    let graph_incarnation_id = entry.incarnation_id.clone();
    let materialization_manifest = s.registry.materialization_handle(graph_name);
    // Clone the authoritative durable backend under the registry lock. Mutation
    // paths below fail closed when it is absent and await its commit barrier.
    let persistence = s.persistence.clone();
    // Change-Data-Capture hub (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230): clone the handle under the same
    // lock so a successful durable mutation can emit an ordered change into this
    // graph's feed AFTER it applies. `None` ⇒ a non-streaming build ⇒ no emit, the
    // write path is byte-for-byte unchanged.
    #[cfg(feature = "streaming")]
    let cdc = s.cdc.clone();
    // Per-graph write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): clone the registry handle.
    // `write_coalescer` backs `dispatch::try_coalesce_write`'s pre-gateway
    // fallback (unreachable for the four coalescable methods since the
    // mutation gateway intercepts them first — see below); `routed_write_coalescer`
    // is the LIVE registry the gateway actually batches through
    // (`mutation::commit_coalescable_mutation`), which queues the WHOLE
    // prepare→durable-commit→RAM-publish sequence rather than just the RAM
    // apply. Both are cheap Arc clones under the same registry lock.
    let write_coalescer = s.write_coalescer.clone();
    let routed_write_coalescer = s.routed_write_coalescer.clone();
    // A clustered graph operation requires the MultiRaft placement authority. The
    // former standalone group-0 handle is detected only to reject an incomplete
    // cluster configuration; it is never used as a write-routing fallback.
    #[cfg(feature = "raft")]
    let standalone_raft_configured = !is_replicated_apply() && s.raft.is_some();
    #[cfg(feature = "raft")]
    let multi_raft = if is_replicated_apply() {
        None
    } else {
        s.multi_raft.clone()
    };
    #[cfg(feature = "raft")]
    let graph_type = entry.graph_type;
    // Cold-tenant access tracking (CONCEPT:EG-KG.backend.r6-feature, R6): clone the tracker under the same
    // registry lock so this graph's access recency is recorded after the lock is released
    // (a `touch` is one cheap map upsert, off the graph lock). The periodic cold-offload
    // sweep reads it to hibernate IDLE graphs; a recently-touched graph is never selected.
    // `redb`-only — whole-graph offload is a durable-tier capability (CONCEPT:EG-KG.sharding.eg-r6).
    #[cfg(feature = "redb")]
    let cold_tracker = s.cold_tracker.clone();
    // Per-agent Row-Level Security (CONCEPT:EG-KG.sharding.row-level-security): clone the isolation policy
    // under the same registry lock so the read-only query handler can filter its
    // off-lock snapshot down to the rows the caller may see. Only the read/query
    // surfaces need it (writes are already graph-ACL-gated above); cheap clone of a
    // small identity map, shared by Arc into the handler. `has_rules()==false` ⇒ the
    // filter is a no-op, single-tenant unchanged.
    #[cfg(feature = "security")]
    let rls = std::sync::Arc::new(s.isolation.clone());
    // Referenced by the read-query routing below only when a query/cypher/rdf surface
    // is compiled; keep it used in a security-but-no-query-surface build.
    #[cfg(all(
        feature = "security",
        not(any(feature = "query", feature = "cypher", feature = "rdf"))
    ))]
    let _ = &rls;
    // CONCEPT:EG-KG.mining.tsdb-typed-absent — clone the committed tsdb store handle under
    // the same registry lock so a plan-sourced mining `Op::TsScan` leg (`handlers::mining`,
    // both the gateway-routed `Mine*` methods and `MineClassifyFit`) can bind the REAL store
    // instead of the old hardcoded `None`. Gated on `mining` too: it is unused otherwise.
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    let tsdb_store = s.tsdb_store.clone();
    drop(s); // Release registry lock before graph lock.

    #[cfg(feature = "raft")]
    if standalone_raft_configured && multi_raft.is_none() {
        return Response::err(
            req_id,
            "CLUSTER_CONFIGURATION_INVALID: MultiRaft placement authority is required",
        );
    }

    // Record this graph's access for the cold-offload sweep (CONCEPT:EG-KG.backend.r6-feature, R6) — both
    // reads and writes touch, so a graph being actively used is never offloaded.
    #[cfg(feature = "redb")]
    cold_tracker.touch(graph_name);

    // Mandatory placement resolution for ordinary graph operations. MultiRaft is
    // the sole clustered authority. Resolving here also fences reads away from a
    // node that no longer runs the graph's current group.
    #[cfg(feature = "raft")]
    let routed_raft = if let Some(multi) = multi_raft.as_ref() {
        match multi.handle_for_graph(graph_name).await {
            Some(routed) => Some(routed),
            None => {
                let route = multi.route_graph(graph_name).await;
                return Response::stale_route(
                    req_id,
                    graph_name,
                    route.group,
                    route.epoch,
                    None,
                    "authoritative placement group is not running on this node",
                );
            }
        }
    } else {
        None
    };

    // Resource lifecycle timestamps are authority inputs, not caller clocks.
    // Bind one leader/replicated-apply timestamp before dispatching either the
    // native MutationBatch or an authority read; followers replay the timestamp
    // carried by the committed Raft scope through `authoritative_now_ms()`.
    if crate::server::mutation_batch::is_resource_reservation_method(&method) {
        let now_ms = authoritative_now_ms();
        match &mut method {
            Method::ReserveWorkItemResources { request }
            | Method::ReleaseWorkItemResources { request }
            | Method::ReclaimWorkItemResources { request } => request.now_ms = now_ms,
            Method::QueryWorkItemReservation { request }
            | Method::ResourceReservationStatus { request } => request.now_ms = now_ms,
            Method::UpdateResourceHost { request } => request.now_ms = now_ms,
            _ => unreachable!("resource method classifier and timestamp binding diverged"),
        }
    }

    // ChangeEnvelope is a first-class persistence operation, not a sequence of
    // direct graph calls. It executes only after graph ACL and placement
    // resolution, and before generic replicated mutation paths can decompose it.
    let method = match method {
        Method::ApplyChangeEnvelope { envelope } => {
            #[cfg(feature = "raft")]
            if let Some(routed) = routed_raft.as_ref() {
                let leader = routed.handle.current_leader().await;
                if leader != Some(routed.handle.node_id) {
                    return Response::stale_route(
                        req_id,
                        graph_name,
                        routed.group_id,
                        routed.epoch,
                        leader,
                        "ChangeEnvelope commits require the current placement leader",
                    );
                }
                if envelope.mutation.placement_epoch != routed.epoch
                    || envelope.mutation.fencing_token != Some(routed.group_id)
                {
                    return Response::stale_route(
                        req_id,
                        graph_name,
                        routed.group_id,
                        routed.epoch,
                        leader,
                        "ChangeEnvelope placement epoch or fencing token is stale",
                    );
                }
            } else {
                if envelope.mutation.placement_epoch != 0
                    || envelope.mutation.fencing_token.is_some()
                {
                    return Response::err(
                        req_id,
                        "ChangeEnvelope carries a placement fence but no routed placement is active",
                    );
                }
            }
            #[cfg(not(feature = "raft"))]
            if envelope.mutation.placement_epoch != 0 || envelope.mutation.fencing_token.is_some() {
                return Response::err(
                    req_id,
                    "ChangeEnvelope carries a placement fence in a single-node build",
                );
            }

            let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
            if let Some(expected) = envelope.mutation.expected_graph_version {
                if expected != core.version() {
                    return Response::err(
                        req_id,
                        format!(
                            "STALE_GRAPH_VERSION: expected {expected}, current {}",
                            core.version()
                        ),
                    );
                }
            }
            let committed_at_ms = authoritative_now_ms().max(envelope.mutation.created_at_ms);
            let Some(backend) = persistence.as_ref() else {
                return Response::err(
                    req_id,
                    "ApplyChangeEnvelope requires a configured persistence backend",
                );
            };
            let fname = crate::persist::sanitize(graph_name);

            // A clustered ChangeEnvelope remains one log entry and one native
            // transaction on every replica. The leader-selected timestamp travels
            // in the entry so replay and follower records are byte-stable.
            #[cfg(feature = "raft")]
            {
                if let Some(routed) = routed_raft.as_ref() {
                    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
                        envelope.mutation.batch_id.clone(),
                        envelope.mutation.context.request_id,
                        &tenant_scope,
                        envelope.mutation.context.principal.clone(),
                        false,
                        envelope.mutation.placement_epoch,
                        envelope.mutation.fencing_token,
                        envelope.mutation.created_at_ms,
                    ) {
                        Ok(context) => context,
                        Err(error) => return Response::err(req_id, error),
                    };
                    let server_secret = timed_read(state).await.auth_secret.clone();
                    let command = match crate::raft::ReplicatedMutation::change_envelope(
                        &envelope,
                        &server_secret,
                    ) {
                        Ok(command) => command,
                        Err(error) => return Response::err(req_id, error),
                    };
                    let request = crate::raft::RaftRequest {
                        graph_fname: fname.clone(),
                        graph_name: graph_name.to_string(),
                        graph_type,
                        command,
                        committed_at_ms,
                        mutation,
                    };
                    return match routed.handle.client_write(request).await {
                        Ok(response) if response.native_error.is_some() => {
                            Response::err(req_id, response.native_error.unwrap_or_default())
                        }
                        Ok(response) => match response.change_envelope_commit {
                            Some(committed) => {
                                let mut result =
                                    change_envelope_result(&committed, response.projection_pending);
                                if let Some(object) = result.as_object_mut() {
                                    object.insert("replicated".to_string(), true.into());
                                    object.insert("group".to_string(), routed.group_id.into());
                                    object.insert("epoch".to_string(), routed.epoch.into());
                                    object.insert(
                                        "fencing_token".to_string(),
                                        routed.fencing_token().into(),
                                    );
                                }
                                Response::ok(req_id, ResultPayload::Json(result))
                            }
                            None => Response::err(
                                req_id,
                                "replicated ChangeEnvelope returned no commit receipt",
                            ),
                        },
                        Err(error) => {
                            let leader = routed.handle.current_leader().await;
                            Response::stale_route(
                                req_id,
                                graph_name,
                                routed.group_id,
                                routed.epoch,
                                leader,
                                error,
                            )
                        }
                    };
                }
            }

            let committed = match backend
                .commit_change_envelope(&fname, &envelope, committed_at_ms)
                .await
            {
                Ok(committed) => committed,
                Err(error) => {
                    return Response::err(
                        req_id,
                        format!("ApplyChangeEnvelope atomic commit failed: {error}"),
                    )
                }
            };
            let projection_error = if committed.replayed {
                None
            } else {
                crate::server::mutation_batch::publish_change_envelope_projection(&core, &envelope)
                    .err()
            };
            let result = change_envelope_result(&committed, projection_error.is_some());
            return Response::ok(req_id, ResultPayload::Json(result));
        }
        // Batch envelope commit for ONE graph (the top-level `dispatch_change_envelopes`
        // groups by graph and routes each group here). Every envelope targets
        // `graph_name`; they land in ONE coalesced redb transaction — the atomic
        // graph-batch. A single failing envelope aborts the whole group and every
        // envelope in it reports the batch outcome honestly. Per-envelope results are
        // returned in group order under `{"results": [...]}`.
        Method::ApplyChangeEnvelopes { envelopes } => {
            // Under an active cluster placement the batch is not offered: raft keeps
            // each envelope one log entry (K=1 serializes anyway), so the client falls
            // back to per-record `ApplyChangeEnvelope`. Single-node is where the
            // one-transaction batching win lands, and prod is single-node.
            #[cfg(feature = "raft")]
            if routed_raft.is_some() {
                return Response::err(
                    req_id,
                    "CHANGE_BATCH_UNAVAILABLE_UNDER_PLACEMENT: use per-envelope ApplyChangeEnvelope",
                );
            }
            let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
            for envelope in &envelopes {
                if envelope.mutation.placement_epoch != 0
                    || envelope.mutation.fencing_token.is_some()
                {
                    return Response::err(
                        req_id,
                        "ChangeEnvelope carries a placement fence in a single-node build",
                    );
                }
            }
            let committed_at_ms = envelopes.iter().fold(authoritative_now_ms(), |acc, e| {
                acc.max(e.mutation.created_at_ms)
            });
            let Some(backend) = persistence.as_ref() else {
                return Response::err(
                    req_id,
                    "ApplyChangeEnvelopes requires a configured persistence backend",
                );
            };
            let fname = crate::persist::sanitize(graph_name);
            let mut results: Vec<serde_json::Value> = Vec::with_capacity(envelopes.len());
            match backend
                .commit_change_envelopes(&fname, &envelopes, committed_at_ms)
                .await
            {
                Ok(commits) => {
                    for (envelope, committed) in envelopes.iter().zip(commits.iter()) {
                        let projection_error = if committed.replayed {
                            None
                        } else {
                            crate::server::mutation_batch::publish_change_envelope_projection(
                                &core, envelope,
                            )
                            .err()
                        };
                        let mut entry =
                            change_envelope_result(committed, projection_error.is_some());
                        if let Some(object) = entry.as_object_mut() {
                            object.insert(
                                "status".to_string(),
                                serde_json::Value::String(
                                    if committed.replayed {
                                        "idempotent_skip"
                                    } else {
                                        "applied"
                                    }
                                    .to_string(),
                                ),
                            );
                        }
                        results.push(entry);
                    }
                }
                Err((failing_index, error)) => {
                    // The whole graph-batch aborted atomically — nothing committed.
                    // Report the batch outcome per envelope honestly: the offender
                    // carries its own error; the siblings carry the abort cause.
                    for (index, envelope) in envelopes.iter().enumerate() {
                        let this_error = if index == failing_index {
                            error.clone()
                        } else {
                            format!(
                                "ABORTED_ATOMIC_GRAPH_BATCH: sibling envelope {failing_index} failed ({error})"
                            )
                        };
                        results.push(serde_json::json!({
                            "status": "conflict",
                            "envelope_id": envelope.envelope_id,
                            "error": this_error,
                        }));
                    }
                }
            }
            return Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({ "results": results })),
            );
        }
        Method::GetChangeEnvelope {
            envelope_id,
            tenant,
        } => {
            let Some(backend) = persistence.as_ref() else {
                return Response::err(req_id, "ChangeEnvelope persistence is unavailable");
            };
            let fname = crate::persist::sanitize(graph_name);
            return match backend.read_change_envelope(&fname, &envelope_id).await {
                Ok(Some(record))
                    if record.envelope.mutation.tenant == tenant
                        && record.envelope.mutation.graph == graph_name =>
                {
                    Response::ok(req_id, ResultPayload::raw(&record))
                }
                Ok(Some(_)) => Response::err(req_id, "ACCESS_DENIED: envelope tenant mismatch"),
                Ok(None) => Response::ok(
                    req_id,
                    ResultPayload::raw(
                        &Option::<crate::change_envelope::ChangeEnvelopeRecord>::None,
                    ),
                ),
                Err(error) => Response::err(req_id, format!("ChangeEnvelope read failed: {error}")),
            };
        }
        Method::GetContentVersion { object_id, tenant } => {
            let Some(backend) = persistence.as_ref() else {
                return Response::err(req_id, "content-version persistence is unavailable");
            };
            let fname = crate::persist::sanitize(graph_name);
            return match backend
                .read_content_version(&fname, &tenant, &object_id)
                .await
            {
                Ok(version) => Response::ok(req_id, ResultPayload::raw(&version)),
                Err(error) => {
                    Response::err(req_id, format!("content-version read failed: {error}"))
                }
            };
        }
        Method::GetChangeCursor {
            source,
            partition,
            tenant,
        } => {
            let Some(backend) = persistence.as_ref() else {
                return Response::err(req_id, "change-cursor persistence is unavailable");
            };
            let fname = crate::persist::sanitize(graph_name);
            return match backend
                .read_change_cursor(&fname, &tenant, &source, &partition)
                .await
            {
                Ok(cursor) => Response::ok(req_id, ResultPayload::raw(&cursor)),
                Err(error) => Response::err(req_id, format!("change-cursor read failed: {error}")),
            };
        }
        other => other,
    };

    // Reservation reads are authority reads, not GraphCore snapshots.  Under
    // placement they are served only by the current group leader; followers
    // fail closed with the normal redirect instead of returning stale holds.
    if crate::server::mutation_batch::is_resource_reservation_query_method(&method) {
        #[cfg(feature = "raft")]
        if let Some(routed) = routed_raft.as_ref() {
            let leader = routed.handle.current_leader().await;
            if leader != Some(routed.handle.node_id) {
                return Response::stale_route(
                    req_id,
                    graph_name,
                    routed.group_id,
                    routed.epoch,
                    leader,
                    "native reservation reads require the current placement leader",
                );
            }
            if let Some(multi) = multi_raft.as_ref() {
                if let Err(error) = multi.read_barrier_group(routed.group_id).await {
                    return Response::err(
                        req_id,
                        format!(
                            "native reservation read linearizability barrier failed: {error:?}"
                        ),
                    );
                }
            }
        }
        let Some(backend) = persistence.as_ref() else {
            return Response::err(req_id, "native reservation persistence is unavailable");
        };
        let fname = crate::persist::sanitize(graph_name);
        let crate::protocol::Method::QueryWorkItemReservation { request } = &method else {
            if let crate::protocol::Method::ResourceReservationStatus { request } = &method {
                return match backend
                    .read_resource_reservation_status(&fname, request)
                    .await
                {
                    Ok(result) => Response::ok(
                        req_id,
                        redact_resource_status_result(
                            result,
                            request,
                            verified_context.allows_action("resource:read:aggregate")
                                || verified_context.allows_action("kg:admin"),
                        ),
                    ),
                    Err(error) => Response::err(
                        req_id,
                        format!("native reservation status read failed: {error}"),
                    ),
                };
            }
            unreachable!("resource query classifier and dispatch method diverged");
        };
        return match backend.read_resource_reservation(&fname, request).await {
            Ok(result) => Response::ok(req_id, ResultPayload::raw(&result)),
            Err(error) => Response::err(req_id, format!("native reservation read failed: {error}")),
        };
    }

    // Native WorkItem claim capabilities use a dedicated private ledger and
    // never enter MutationBatch/result/outbox/CDC projections.  The verified
    // request context supplies all authority fields; the public method carries
    // only an item id (mint) or opaque bytes (verify).
    if matches!(
        &method,
        Method::MintWorkItemClaimCapability { .. } | Method::VerifyWorkItemClaimCapability { .. }
    ) {
        #[cfg(feature = "raft")]
        if is_replicated_apply() {
            // A replicated apply has only the bounded Raft routing context;
            // capability authority requires the original cryptographically
            // verified principal and session envelope.
            return Response::err(
                req_id,
                crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE,
            );
        }
        #[cfg(feature = "raft")]
        if let Some(routed) = routed_raft.as_ref() {
            let leader = routed.handle.current_leader().await;
            if leader != Some(routed.handle.node_id) {
                return Response::stale_route(
                    req_id,
                    graph_name,
                    routed.group_id,
                    routed.epoch,
                    leader,
                    "WorkItem claim capabilities require the current placement leader",
                );
            }
            if let Some(multi) = multi_raft.as_ref() {
                if let Err(error) = multi.read_barrier_group(routed.group_id).await {
                    return Response::err(
                        req_id,
                        format!(
                            "WorkItem claim capability linearizability barrier failed: {error:?}"
                        ),
                    );
                }
            }
        }
        let Some(backend) = persistence.as_ref() else {
            return Response::err(
                req_id,
                "native WorkItem claim capability persistence is unavailable",
            );
        };
        #[cfg(feature = "redb")]
        {
            let Some(redb) = backend.as_redb() else {
                return Response::err(
                    req_id,
                    "native WorkItem claim capability persistence is unavailable",
                );
            };
            let authority = crate::redb_store::work_item_capability::AuthenticatedAuthority {
                tenant: verified_context.tenant().to_string(),
                audience: verified_context.claims().audience.clone(),
                principal: verified_context.principal_persistence_id(),
                agent_id: verified_context.agent_id().to_string(),
                session: verified_context.idempotency_key().to_string(),
                authority_epoch: work_item_capability_authority_epoch(&graph_incarnation_id),
                incarnation_id: graph_incarnation_id.clone(),
                now_ms: authoritative_now_ms(),
            };
            let fname = crate::persist::sanitize(graph_name);
            let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
            return match method {
                Method::MintWorkItemClaimCapability { request } => redb
                    .mint_work_item_claim_capability(&fname, request, authority)
                    .await
                    .map(|result| Response::ok(req_id, ResultPayload::raw(&result)))
                    .unwrap_or_else(|error| {
                        Response::err(
                            req_id,
                            format!("WorkItem claim capability mint failed: {error}"),
                        )
                    }),
                Method::VerifyWorkItemClaimCapability { request } => redb
                    .verify_work_item_claim_capability(&fname, request, authority)
                    .await
                    .map(|result| Response::ok(req_id, ResultPayload::raw(&result)))
                    .unwrap_or_else(|error| {
                        Response::err(
                            req_id,
                            format!("WorkItem claim capability verify failed: {error}"),
                        )
                    }),
                _ => unreachable!("capability classifier and dispatch diverged"),
            };
        }
        #[cfg(not(feature = "redb"))]
        {
            let _ = backend;
            return Response::err(
                req_id,
                "native WorkItem claim capability requires redb persistence",
            );
        }
    }

    // ── Native development-lane hold/quota authority (RMDD-28) ──────────────────
    // `redb_store::development_lane` deliberately stops at the redb transaction
    // boundary -- no MutationBatch/result/outbox/CDC projection -- exactly the
    // WorkItem claim-capability posture above. The 6 write methods
    // (Reserve/Renew/Observe/Finish/Cleanup/UpdateQuota) commit through the
    // writer-thread `Cmd` channel (the kernel's own self-contained
    // begin_write()/commit()); the exact-query/status reads are MVCC snapshot
    // reads, same posture as the native reservation-ledger reads above. Every
    // request carries its own tenant/owner/fencing authority (CAS'd against the
    // live WorkItem row inside the kernel), not a server-derived
    // AuthenticatedAuthority, so — unlike claim capability — there is no
    // verified-context authority struct to build here.
    if crate::server::mutation_batch::is_development_lane_method(&method) {
        #[cfg(feature = "raft")]
        if let Some(routed) = routed_raft.as_ref() {
            let leader = routed.handle.current_leader().await;
            if leader != Some(routed.handle.node_id) {
                return Response::stale_route(
                    req_id,
                    graph_name,
                    routed.group_id,
                    routed.epoch,
                    leader,
                    "development-lane operations require the current placement leader",
                );
            }
            if let Some(multi) = multi_raft.as_ref() {
                if let Err(error) = multi.read_barrier_group(routed.group_id).await {
                    return Response::err(
                        req_id,
                        format!("development-lane linearizability barrier failed: {error:?}"),
                    );
                }
            }
        }
        let Some(backend) = persistence.as_ref() else {
            return Response::err(req_id, "native development-lane persistence is unavailable");
        };
        let fname = crate::persist::sanitize(graph_name);
        let now_ms = authoritative_now_ms();
        let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
        return match method {
            Method::QueryDevelopmentLane { ref request } => backend
                .read_development_lane(&fname, request, now_ms)
                .await
                .map(|result| Response::ok(req_id, ResultPayload::raw(&result)))
                .unwrap_or_else(|error| {
                    Response::err(req_id, format!("development-lane query failed: {error}"))
                }),
            Method::DevelopmentLaneStatus { ref request } => backend
                .read_development_lane_status(&fname, request, now_ms)
                .await
                .map(|result| Response::ok(req_id, ResultPayload::raw(&result)))
                .unwrap_or_else(|error| {
                    Response::err(
                        req_id,
                        format!("development-lane status read failed: {error}"),
                    )
                }),
            _ => backend
                .commit_development_lane(&fname, method, now_ms)
                .await
                .map(|bytes| Response::ok(req_id, ResultPayload::Raw(bytes)))
                .unwrap_or_else(|error| {
                    Response::err(req_id, format!("development-lane commit failed: {error}"))
                }),
        };
    }

    // Engine-native WorkItem transitions are result-producing durable CAS
    // operations. They must execute at the current placement leader (a generic
    // Raft acknowledgement cannot carry the selected work-item result), and their
    // redb MutationBatch atomically persists the transition/result/outbox before
    // the in-memory graph projection is refreshed.
    if crate::server::mutation_batch::is_work_item_mutation_method(&method) {
        #[cfg(feature = "raft")]
        let (placement_epoch, placement_fence) = if let Some(routed) = routed_raft.as_ref() {
            let leader = routed.handle.current_leader().await;
            if leader != Some(routed.handle.node_id) {
                return Response::stale_route(
                    req_id,
                    graph_name,
                    routed.group_id,
                    routed.epoch,
                    leader,
                    "WorkItem transitions require the current placement leader",
                );
            }
            (routed.epoch, Some(routed.group_id))
        } else {
            (0, None)
        };
        #[cfg(not(feature = "raft"))]
        let (placement_epoch, placement_fence) = (0, None);

        return match crate::server::mutation_batch::commit_work_item(
            persistence.as_ref(),
            &core,
            req_id,
            caller,
            graph_name,
            placement_epoch,
            placement_fence,
            method,
        )
        .await
        {
            Ok(result) => Response::ok(req_id, result),
            Err(error) => Response::err(req_id, format!("WorkItem mutation failed: {error}")),
        };
    }

    // `TsAppend` is not yet a Raft state-machine command, so it cannot rely on the
    // durable-mutation barrier below. Still fence it to the current placement
    // leader: accepting a follower-local append would create a divergent
    // `series.redb` projection and acknowledge a write on the wrong replica.
    #[cfg(all(feature = "raft", feature = "tsdb"))]
    if matches!(&method, Method::TsAppend { .. }) {
        if let Some(routed) = routed_raft.as_ref() {
            let leader = routed.handle.current_leader().await;
            if leader != Some(routed.handle.node_id) {
                return Response::stale_route(
                    req_id,
                    graph_name,
                    routed.group_id,
                    routed.epoch,
                    leader,
                    "time-series writes require the current placement leader",
                );
            }
        }
    }

    #[cfg(all(feature = "raft", feature = "tsdb"))]
    let (ts_placement_epoch, ts_fencing_token) = if let Some(routed) = routed_raft.as_ref() {
        (routed.epoch, Some(routed.group_id))
    } else {
        (0, None)
    };
    #[cfg(all(not(feature = "raft"), feature = "tsdb"))]
    let (ts_placement_epoch, ts_fencing_token) = (0, None);

    // One native KnowledgeBatch pull surface for every query family. This point is
    // deliberately after graph ACL, lazy-materialization and placement resolution,
    // and before any family-specific direct handler, so a cursor cannot bypass the
    // same RequestContext/RLS/placement boundary as its underlying query.
    #[cfg(feature = "knowledge-batch")]
    if matches!(&method, Method::KnowledgeStream { .. }) {
        let Some(authority) = knowledge_stream_authority.as_ref() else {
            return Response::err(
                req_id,
                "KnowledgeStream authority was not derived from verified context",
            );
        };
        let carrier = match CarrierAuthority::from_verified(verified_context) {
            Ok(authority) => authority,
            Err(denied) => return Response::err(req_id, denied),
        };
        #[cfg(feature = "raft")]
        let (stream_placement_epoch, stream_fencing_token) =
            if let Some(routed) = routed_raft.as_ref() {
                (routed.epoch, Some(routed.group_id))
            } else {
                (0, None)
            };
        #[cfg(not(feature = "raft"))]
        let (stream_placement_epoch, stream_fencing_token) = (0, None);
        return match handlers::knowledge_stream::try_handle(
            state,
            req_id,
            graph_name,
            core.clone(),
            method,
            verified_actor,
            &carrier,
            authority,
            stream_placement_epoch,
            stream_fencing_token,
            read_authority
                .as_ref()
                .expect("KnowledgeStream is classified as a graph read"),
            #[cfg(feature = "security")]
            &rls,
        )
        .await
        {
            Ok(response) => response,
            Err(_) => Response::err(req_id, "KnowledgeStream dispatch routing error"),
        };
    }

    // Time-series operations now run only after graph ACL + placement policy. The
    // handler derives the canonical `(tenant, graph, series)` storage key from this
    // already-authorized graph context.
    #[cfg(feature = "tsdb")]
    if matches!(
        &method,
        Method::TsAppend { .. }
            | Method::TsRange { .. }
            | Method::TsAsofJoin { .. }
            | Method::TsWindow { .. }
            | Method::TsGapFill { .. }
    ) {
        let carrier = match CarrierAuthority::from_verified(verified_context) {
            Ok(authority) => authority,
            Err(denied) => return Response::err(req_id, denied),
        };
        return match handlers::timeseries::try_handle(
            state,
            req_id,
            &carrier,
            graph_name,
            ts_placement_epoch,
            ts_fencing_token,
            method,
        )
        .await
        {
            Ok(resp) => resp,
            Err(_) => Response::err(req_id, "timeseries dispatch routing error"),
        };
    }

    // Tamper-evident audit verification (CONCEPT:EG-KG.sharding.row-level-security): a read-only walk of the
    // target graph's durable hash-chained audit log. Routed to the redb backend's
    // owner thread (which flushes pending first). Handled here — AFTER the registry
    // lock is released — so blocking on the writer-thread reply never holds the lock.
    #[cfg(feature = "security")]
    if matches!(method, Method::AuditVerify) {
        let fname = crate::persist::sanitize(graph_name);
        return match persistence.as_ref().and_then(|p| p.as_redb()) {
            Some(redb) => match redb.audit_verify_blocking(&fname) {
                Ok(report) => Response::ok(req_id, ResultPayload::raw(&report)),
                Err(e) => Response::err(req_id, format!("AuditVerify error: {e}")),
            },
            None => Response::err(
                req_id,
                "AuditVerify requires a durable redb backend (no persist dir configured)"
                    .to_string(),
            ),
        };
    }

    // Provenance-anchor inclusion proof (CONCEPT:EG-KG.sharding.row-level-security, provenance anchoring): the
    // `AuditVerify` extension that reaches an anchored NODE's content, not just
    // mutation ordering. Same routing shape as `AuditVerify` immediately above —
    // the redb backend's owner thread (flushes pending first), handled after the
    // registry lock is released.
    #[cfg(feature = "security")]
    if let Method::AuditProveInclusion {
        node_id,
        anchor_seq,
    } = &method
    {
        let fname = crate::persist::sanitize(graph_name);
        return match persistence.as_ref().and_then(|p| p.as_redb()) {
            Some(redb) => match redb.audit_prove_inclusion_blocking(&fname, node_id, *anchor_seq) {
                Ok(report) => Response::ok(req_id, ResultPayload::raw(&report)),
                Err(e) => Response::err(req_id, format!("AuditProveInclusion error: {e}")),
            },
            None => Response::err(
                req_id,
                "AuditProveInclusion requires a durable redb backend (no persist dir configured)"
                    .to_string(),
            ),
        };
    }

    // Concrete governed modality service. This is deliberately after graph ACL,
    // lazy materialization, placement resolution and verified-context authority
    // derivation, but before the generic replicated-mutation branch: the mutation gateway
    // stages a complete graph image and commits the encrypted runtime snapshot
    // through the authoritative MutationBatch boundary before publishing RAM.
    #[cfg(feature = "modality-serving")]
    if let Method::ServedModality { op } = &method {
        if op.mutates() && persistence.is_none() {
            return Response::err(
                req_id,
                "served modality operations require authoritative redb persistence",
            );
        }
        let authority = match modality_authority.as_ref() {
            Some(authority) => authority.clone(),
            None => {
                return Response::err(
                    req_id,
                    "served modality authority was not derived from verified context",
                )
            }
        };
        let (isolation, graph_type, owner) = gateway_authz_ctx
            .as_ref()
            .expect("ServedModality must be registered in the mutation gateway");
        #[cfg(feature = "raft")]
        if op.mutates() {
            let backend = persistence
                .as_ref()
                .expect("mutating ServedModality requires persistence");
            let principal_fingerprint = verified_context.principal_persistence_id();
            if let Some(routed) = routed_raft.as_ref() {
                return replicate_served_modality(
                    state,
                    routed.handle.clone(),
                    routed.group_id,
                    routed.epoch,
                    Some(routed.group_id),
                    graph_name,
                    *graph_type,
                    req_id,
                    &tenant_scope,
                    &principal_fingerprint,
                    &core,
                    backend,
                    method,
                    &authority,
                )
                .await;
            }
        }
        let plan = crate::server::mutation::MutationPlan::for_method(&method);
        let ctx = crate::server::mutation::MutationCtx {
            req_id,
            caller,
            tenant_scope: &tenant_scope,
            graph_name,
            graph_type: *graph_type,
            owner: owner.as_deref(),
            isolation,
            core: &core,
            persistence: persistence.as_ref(),
            #[cfg(feature = "streaming")]
            cdc: cdc.as_ref(),
            materialization_manifest: materialization_manifest.as_ref(),
            write_coalescer: None,
        };
        let op_apply = op.clone();
        return crate::server::mutation::commit_conditional_mutation(
            &ctx,
            &plan,
            &method,
            op.mutates(),
            move |staged_core| handlers::modality::handle(staged_core, &authority, op_apply),
        )
        .await;
    }

    // ── Raft write-routing barrier (CONCEPT:AU-KG.ingest.source-sync-canonical) ──────────────────────
    // When a cluster is active, a durable mutation goes through Raft consensus
    // (the leader's `client_write`) BEFORE it is applied+acked: the entry is
    // replicated to a quorum and then APPLIED on every node by the Raft state
    // machine. So we replace the local gateway call with `client_write` and return
    // its outcome — deterministic staging, the state-backed MutationBatch commit,
    // and RAM publication happen inside the Raft state machine, not here. A
    // follower returns a ForwardToLeader error which we surface so the client
    // retries against the leader. This branch is the ONLY behavioral difference vs
    // single-node, and it is taken only for durable mutations with Raft active.
    #[cfg(feature = "raft")]
    if let Some(routed) = routed_raft {
        if crate::mutation_apply::is_durable_mutation(&method) {
            let created_at_ms = authoritative_now_ms();
            let batch_id = crate::server::mutation_batch::opaque_request_key(
                "raft-rpc", graph_name, req_id, &method,
            );
            let mutation = match crate::raft::RaftMutationContext::from_verified_request(
                batch_id,
                req_id,
                &tenant_scope,
                verified_context.principal_persistence_id(),
                false,
                routed.epoch,
                Some(routed.group_id),
                created_at_ms,
            ) {
                Ok(context) => context,
                Err(error) => return Response::err(req_id, error),
            };
            let server_secret = timed_read(state).await.auth_secret.clone();
            let command = match crate::raft::ReplicatedMutation::graph(method, &server_secret) {
                Ok(command) => command,
                Err(error) => return Response::err(req_id, error),
            };
            let req = crate::raft::RaftRequest {
                graph_fname: crate::persist::sanitize(graph_name),
                graph_name: graph_name.to_string(),
                graph_type,
                committed_at_ms: created_at_ms,
                mutation,
                command,
            };
            return match routed.handle.client_write(req).await {
                Ok(response) => {
                    if let Some(error) = response.native_error {
                        Response::err(req_id, error)
                    } else {
                        Response::ok(
                            req_id,
                            ResultPayload::Json(serde_json::json!({
                                "replicated": true,
                                "group": routed.group_id,
                                "epoch": routed.epoch,
                                "fencing_token": routed.fencing_token(),
                            })),
                        )
                    }
                }
                Err(e) => {
                    let leader = routed.handle.current_leader().await;
                    Response::stale_route(
                        req_id,
                        graph_name,
                        routed.group_id,
                        routed.epoch,
                        leader,
                        e,
                    )
                }
            };
        }
    }

    crate::metrics::graph_op(graph_name);

    let response = 'dispatch: {
        // Mutation-gateway routing (CONCEPT:EG-P0-2): the primary CRUD + agent-
        // memory writes (`mutation::GATEWAY_ROUTED`) are routed through the
        // single `commit_mutation` gateway — policy-driven authz, durability,
        // audit, CDC, and TMS in ONE call. Native stores use their own explicit
        // MutationBatch kernels. There is no second post-dispatch durability tail.
        let method = match handlers::graph_ops::try_handle_gateway(
            req_id,
            caller,
            &tenant_scope,
            graph_name,
            &core,
            materialization_manifest.as_ref(),
            read_authority.as_ref(),
            persistence.as_ref(),
            #[cfg(feature = "streaming")]
            cdc.as_ref(),
            Some(&routed_write_coalescer),
            gateway_authz_ctx.as_ref(),
            #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
            tsdb_store.as_ref(),
            method,
        )
        .await
        {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Per-graph write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): the five high-frequency
        // single-op writes are batched onto this graph's writer so M concurrent
        // writers cost ⌈M/batch⌉ topology-lock acquisitions instead of M. The shell
        // below still owns dirty/durability/gauge off the returned Response, so durability
        // and checkpoint semantics are unchanged — only WHERE the lock is taken
        // moved. On a full queue / disabled coalescer the method is handed back and
        // flows through the inline path unchanged.
        let method =
            match try_coalesce_write(req_id, &write_coalescer, graph_name, &core, method).await {
                Ok(resp) => break 'dispatch resp,
                Err(m) => m,
            };
        // Pure-compute domains (stateless: no graph core / lock) route first; a
        // method that isn't theirs is handed back via Err and falls through to the
        // graph-op match below. (CONCEPT:EG-KG.query.dispatch-routing — thin routing; logic in handlers/.)
        // Feature-gated: in a slim build the line is absent and the method flows
        // straight through to graph_ops (whose catch-all reports "not available").
        #[cfg(feature = "finance")]
        let method = match handlers::finance::try_handle(req_id, method) {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        #[cfg(feature = "datascience")]
        let method = match handlers::datascience::try_handle(req_id, method) {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Data-mining domain (CONCEPT:EG-KG.mining.frequent-itemset-mining): GRAPH-SCOPED
        // (unlike finance/datascience), so it takes the graph core — the graph-derived
        // transaction source reads node neighborhoods and write-back materializes
        // `:AssociationRule` nodes into it. A method whose feature is off falls through
        // to the graph_ops not-available catch-all.
        #[cfg(feature = "mining")]
        let method = match handlers::mining::try_handle(
            req_id,
            core.clone(),
            read_authority.as_ref(),
            #[cfg(all(feature = "query", feature = "tsdb"))]
            graph_name,
            #[cfg(all(feature = "query", feature = "tsdb"))]
            tsdb_store.as_ref(),
            method,
        ) {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Graph-learning domain (CONCEPT:EG-KG.graphlearn.link-predictor): GRAPH-SCOPED
        // like mining — the KAN link-predictor reads the live subgraph and write-back
        // materializes `:PredictedEdge`/`:EdgeFunction` nodes into the core. A method
        // whose feature is off falls through to the graph_ops not-available catch-all.
        #[cfg(feature = "graphlearn")]
        let method = match handlers::graphlearn::try_handle(req_id, core.clone(), method) {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): the READ verbs
        // (Evaluate/Compare) route here with the graph core; Train/Serve/Predict are
        // GATEWAY_ROUTED (writeback) and never reach this fallback. A build without
        // `ml-pipeline` omits this line.
        #[cfg(feature = "ml-pipeline")]
        let method = match handlers::pipeline::try_handle(
            req_id,
            core.clone(),
            read_authority.as_ref(),
            method,
        ) {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Read-only query surface — SQL (CONCEPT:EG-KG.query.read-only-sql-query, DataFusion behind
        // `query`) AND Cypher (CONCEPT:EG-KG.query.dep-free-behind, dep-free behind `cypher`) AND GraphQL
        // (CONCEPT:EG-KG.query.sparql-completeness, pure-Rust eg-graphql behind `graphql`): borrows the graph
        // core for an off-lock snapshot, runs on the blocking pool. Gated on ANY of the
        // three features so CypherQuery still routes in a cypher-only (no-DataFusion) Pi
        // build and GraphQl routes in a graphql build; the handler's per-method arm
        // falls through (Err) when ITS feature is off, so Sql/CypherQuery/GraphQl then
        // reach the graph_ops not-available catch-all. GraphQL — like SQL/Cypher/SPARQL
        // — runs UNDER the SAME RLS-aware result-cache compose (`caller`/`&rls` threaded
        // in, the cache key folds the caller's RLS context, the snapshot is RLS-filtered
        // to the caller) so a GraphQL read NEVER leaks across agents. Slim builds with
        // NONE of the three omit this line.
        //
        // Runtime-conditional query gateway (CONCEPT:EG-P0-2, L11): `Sql`/
        // `CypherQuery`/`GraphQl` are `mutation::GATEWAY_ROUTED`, but their execution
        // is `async` and needs `state`/`rls`, so they are routed HERE (not at the
        // graph-ops `try_handle_gateway`, which hands them back). The SAME runtime
        // parse `access::requires_write` uses decides whether THIS statement mutates:
        // a SQL write / Cypher `CREATE|SET|DELETE` / GraphQL `mutation` → the full
        // Write-authz commit; a `SELECT` / read-only Cypher / GraphQL `query` → a
        // Read-authz passthrough with no durability/audit/CDC. Every OTHER query
        // method (`UnifiedQuery`/`Explain*`/`Txn*Query`) is a pure read handled by
        // the unchanged direct call in the `else` arm.
        #[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
        let method = if crate::server::mutation::is_query_gateway_method(&method)
            && !crate::server::mutation::is_query_native_coordinator(&method)
        {
            let mutates_now = requires_write(&method);
            let plan = crate::server::mutation::MutationPlan::for_method(&method);
            let (iso, gtype, owner) = gateway_authz_ctx
                .as_ref()
                .expect("is_gateway_routed query method must have a captured GatewayAuthzCtx");
            let ctx = crate::server::mutation::MutationCtx {
                req_id,
                caller,
                tenant_scope: &tenant_scope,
                graph_name,
                graph_type: *gtype,
                owner: owner.as_deref(),
                isolation: iso,
                core: &core,
                persistence: persistence.as_ref(),
                #[cfg(feature = "streaming")]
                cdc: cdc.as_ref(),
                materialization_manifest: materialization_manifest.as_ref(),
                write_coalescer: None,
            };
            let method_apply = method.clone();
            let query_read_authority = read_authority.clone();
            #[cfg(feature = "security")]
            let rls_apply = rls.clone();
            let resp = crate::server::mutation::commit_conditional_mutation_async(
                &ctx,
                &plan,
                &method,
                mutates_now,
                move |staged_core| async move {
                    match handlers::query::try_handle(
                        state,
                        handlers::TryHandleContext {
                            req_id,
                            graph_name,
                            read_authority: query_read_authority.as_ref(),
                            caller: verified_actor,
                        },
                        staged_core,
                        method_apply,
                        #[cfg(feature = "security")]
                        &rls_apply,
                    )
                    .await
                    {
                        Ok(r) => match r.error {
                            Some(e) => Err(e),
                            None => Ok(r
                                .result
                                .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                        },
                        // Unreachable for a real routed query method (its name only
                        // exists when the surface is compiled); kept total.
                        Err(_) => Err("query surface not available in this build".to_string()),
                    }
                },
            )
            .await;
            break 'dispatch resp;
        } else {
            match handlers::query::try_handle(
                state,
                handlers::TryHandleContext {
                    req_id,
                    graph_name,
                    read_authority: read_authority.as_ref(),
                    caller: verified_actor,
                },
                core.clone(),
                method,
                #[cfg(feature = "security")]
                &rls,
            )
            .await
            {
                Ok(r) => break 'dispatch r,
                Err(m) => m,
            }
        };
        // Native RDF/SPARQL surface (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql/218, features `rdf`/`sparql`):
        // AddTriples (durable — the shell below records it like any write),
        // GetRdf + Sparql (read-only, off-lock snapshot). Graph-scoped, so the
        // handler takes the graph core + name. Multi-valued literals are embedded
        // losslessly in that graph image. Gated on `rdf`; a method whose feature is
        // off falls through (Err) to the graph_ops not-available catch-all.
        //
        // Native-RDF write gateway (CONCEPT:EG-P0-2, L11): `AddTriples`/
        // `RemoveTriples`/`DropNamedGraph` are `mutation::GATEWAY_ROUTED` (GraphRedb-
        // durable, audited), routed HERE (not at `try_handle_gateway`) because their
        // handler is async and also performs RDF policy validation. They always
        // mutate (`mutates_now = true`), so `commit_conditional_mutation_async` runs
        // the full Write-authz + durable audit-chain commit; the read-only
        // RDF methods (`GetRdf`/`Sparql`/`ShaclValidate`/…) take the unchanged direct
        // call in the `else` arm.
        #[cfg(feature = "rdf")]
        let method = if crate::server::mutation::is_rdf_gateway_method(&method) {
            let plan = crate::server::mutation::MutationPlan::for_method(&method);
            let (iso, gtype, owner) = gateway_authz_ctx
                .as_ref()
                .expect("is_gateway_routed rdf method must have a captured GatewayAuthzCtx");
            let ctx = crate::server::mutation::MutationCtx {
                req_id,
                caller,
                tenant_scope: &tenant_scope,
                graph_name,
                graph_type: *gtype,
                owner: owner.as_deref(),
                isolation: iso,
                core: &core,
                persistence: persistence.as_ref(),
                #[cfg(feature = "streaming")]
                cdc: cdc.as_ref(),
                materialization_manifest: materialization_manifest.as_ref(),
                write_coalescer: None,
            };
            let method_apply = method.clone();
            let rdf_read_authority = read_authority.clone();
            #[cfg(feature = "security")]
            let rls_apply = rls.clone();
            let resp = crate::server::mutation::commit_conditional_mutation_async(
                &ctx,
                &plan,
                &method,
                true,
                move |staged_core| async move {
                    match handlers::rdf::try_handle(
                        state,
                        handlers::TryHandleContext {
                            req_id,
                            graph_name,
                            read_authority: rdf_read_authority.as_ref(),
                            caller: verified_actor,
                        },
                        staged_core,
                        method_apply,
                        #[cfg(feature = "security")]
                        &rls_apply,
                    )
                    .await
                    {
                        Ok(r) => match r.error {
                            Some(e) => Err(e),
                            None => Ok(r
                                .result
                                .unwrap_or(ResultPayload::Json(serde_json::Value::Null))),
                        },
                        Err(_) => Err("rdf surface not available in this build".to_string()),
                    }
                },
            )
            .await;
            break 'dispatch resp;
        } else {
            match handlers::rdf::try_handle(
                state,
                handlers::TryHandleContext {
                    req_id,
                    graph_name,
                    read_authority: read_authority.as_ref(),
                    caller: verified_actor,
                },
                core.clone(),
                method,
                #[cfg(feature = "security")]
                &rls,
            )
            .await
            {
                Ok(r) => break 'dispatch r,
                Err(m) => m,
            }
        };
        // WASM-sandboxed UDF surface (CONCEPT:EG-KG.query.rowset-execution, feature `wasm-udf`):
        // RegisterUdf compiles+caches, RunUdf runs sandboxed (fuel+memory+no host
        // caps) — both off-reactor. Process-global (not graph-scoped), so it takes
        // `state` for the UdfRegistry. A method whose feature is off falls through.
        #[cfg(feature = "wasm-udf")]
        let method = match handlers::wasm_udf::try_handle(state, req_id, method).await {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Query federation (CONCEPT:EG-KG.query.query-federation, feature `federation`):
        // RegisterForeignSource records a named foreign source on ServerState. The
        // `Op::ForeignScan` op itself runs through the unified-query handler above
        // (inline spec). Process-global, so it takes `state`. A method whose feature
        // is off falls through to the graph_ops not-available catch-all.
        #[cfg(feature = "federation")]
        let method = match handlers::federation::try_handle(state, req_id, method).await {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Distributed graph compute (CONCEPT:EG-KG.storage.feature, feature `compute-dist`):
        // DistributedCompute + the matview lifecycle. Cross-shard, so it takes
        // `state` (it gathers each shard graph's snapshot from the registry).
        #[cfg(any(feature = "compute-dist", feature = "matview"))]
        let method = match handlers::dist_compute::try_handle(
            state,
            req_id,
            caller,
            read_authority.as_ref(),
            method,
        )
        .await
        {
            Ok(r) => break 'dispatch r,
            Err(m) => m,
        };
        // Terminal handler: graph-targeted ops (borrow the core; cross-graph ops
        // re-enter the registry via `state`). Owns the catch-all, returns a Response.
        // Bind then `break` (not a tail expr) so the `'dispatch` label stays used
        // even when both compute-routing lines above are feature-gated out.
        let Some(read_authority) = read_authority.as_ref() else {
            break 'dispatch Response::err(
                req_id,
                "mutation escaped the universal mutation gateway before terminal dispatch",
            );
        };
        let resp = handlers::graph_ops::try_handle(
            state,
            req_id,
            caller,
            read_authority,
            core.clone(),
            method,
        )
        .await;
        break 'dispatch resp;
    };

    // Refresh the per-graph size gauges after mutations — both petgraph
    // counts are O(1), so this adds no meaningful write-path cost.
    #[cfg(feature = "metrics")]
    if matches!(access, AccessLevel::Write) {
        let topo = core.topo.read();
        crate::metrics::set_graph_size(
            graph_name,
            topo.graph.node_count() as i64,
            topo.graph.edge_count() as i64,
        );
    }

    // Non-gateway writes still rely on the dispatch shell for their single
    // projection publication. Gateway writes already publish exactly once in
    // `commit_finalize`; marking them here as well would advance the resident OCC
    // version past the authoritative MutationBatch version.
    if matches!(access, AccessLevel::Write)
        && response.error.is_none()
        && gateway_authz_ctx.is_none()
    {
        core.mark_dirty();
    }

    // W0.4 semantic-ANN warm-on-demand (CONCEPT:EG-KG.storage.semantic-index-directory): a graph created, or
    // one whose embedding count crosses `ANN_BUILD_THRESHOLD`, AFTER the
    // boot-time warm task's one-shot snapshot never gets a trigger from it
    // otherwise. Every write that adds embeddings (`AddEmbedding`, and every
    // mining/graph-learning writeback) flows through this SAME dispatch tail, so
    // one hook here — spawned, never inline on the request path — covers them
    // all. Cheap no-op below the threshold or once already warm/warming.
    #[cfg(feature = "ann")]
    if matches!(access, AccessLevel::Write) && response.error.is_none() {
        crate::server::ann_warm::maybe_warm_after_write(state, graph_name, &core).await;
    }

    response
}

/// Route the five high-frequency single-op writes through the per-graph write
/// coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer). On success returns the same `Response` the inline
/// handler would have produced (so the dispatch shell's dirty/durability/gauge logic runs
/// identically against it). Returns `Err(method)` — handing the method back
/// untouched — for any method that isn't coalescable, or when the coalescer is
/// disabled / its bounded queue is full (backpressure → inline fallback) / the
/// worker is gone, so behavior is never lost, only the lock-acquisition path
/// changes.
async fn try_coalesce_write(
    req_id: u64,
    coalescer: &crate::write_coalescer::WriteCoalescerRegistry,
    graph_name: &str,
    core: &Arc<crate::graph::GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    use crate::write_coalescer::{WriteOp, WriteOutcome};
    use tokio::sync::oneshot;

    // Build this op's reply channel; the op carries the sender, we await the receiver
    // regardless of whether it was batched or applied inline on fallback.
    let (reply, reply_rx) = oneshot::channel::<WriteOutcome>();

    // Map the method → a WriteOp (consuming its args). For CompareAndSetNodeFields,
    // decode the two msgpack blobs FIRST: a decode failure is a CAS failure
    // (Bool(false)) that does NOT touch the graph — exactly the inline handler's
    // contract — so short-circuit without enqueuing (the shell then durably commits the
    // method, matching inline). Non-coalescable methods are handed straight back.
    let op = match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => WriteOp::AddNode {
            node_id,
            properties_msgpack,
            reply,
        },
        Method::RemoveNode { node_id } => WriteOp::RemoveNode { node_id, reply },
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => WriteOp::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
            reply,
        },
        Method::RemoveEdge {
            source_id,
            target_id,
        } => WriteOp::RemoveEdge {
            source_id,
            target_id,
            reply,
        },
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            let conditions = match eg_types::msgpack::decode_property_object(&conditions_msgpack) {
                Ok(m) => m,
                Err(_) => return Ok(Response::ok(req_id, ResultPayload::Bool(false))),
            };
            let updates = match eg_types::msgpack::decode_property_object(&updates_msgpack) {
                Ok(m) => m,
                Err(_) => return Ok(Response::ok(req_id, ResultPayload::Bool(false))),
            };
            WriteOp::CompareAndSet {
                node_id,
                conditions,
                updates,
                reply,
            }
        }
        other => return Err(other),
    };

    // Lazily get/create this graph's writer (automatic per new graph/connector).
    let writer = coalescer.writer_for(graph_name, core);

    // Enqueue; on a full/closed queue (backpressure) apply this single op inline
    // under its own txn — same engine effect, just not batched — so a saturated
    // worker never drops or stalls a write.
    if let Err(op) = writer.try_enqueue(op) {
        writer.apply_one_inline(core, graph_name, op);
    }

    // Await the outcome (from the batch worker or the inline fallback) and rebuild
    // the exact Response the inline handler would have returned.
    let outcome = reply_rx.await.unwrap_or(WriteOutcome::WriterGone);
    let resp = match outcome {
        WriteOutcome::Ok => Response::ok(req_id, ResultPayload::String("ok".to_string())),
        WriteOutcome::Cas(b) => Response::ok(req_id, ResultPayload::Bool(b)),
        WriteOutcome::Err(e) => Response::err(req_id, e),
        WriteOutcome::WriterGone => Response::err(req_id, "write worker unavailable"),
    };
    Ok(resp)
}

// ── Agent-memory / scene / trajectory dispatch round-trip (CONCEPT:EG-KG.memory.eg-batch-decay-caller) ────
//
// Drive the EG-318 Methods through the SAME `dispatch` entrypoint a wire request
// hits (auth → routing → access-classify → handler → GraphCore), proving each wire
// op reaches its eg-core primitive and returns the expected payload — the served
// surface, not the library unit. Runs on a bare `--features server` build (the
// state builder gates every optional field behind its own feature).
#[cfg(all(test, feature = "ast"))]
mod ast_input_hardening_tests {
    use super::*;

    fn limits() -> AstInputLimits {
        AstInputLimits {
            max_files: 2,
            max_source_bytes: 8,
            max_total_bytes: 12,
        }
    }

    fn pack(files: Vec<(String, serde_bytes::ByteBuf)>) -> Vec<u8> {
        rmp_serde::to_vec(&files).expect("encode AST source fixture")
    }

    #[test]
    fn accepts_canonical_bounded_relative_sources() {
        let encoded = pack(vec![(
            "src/lib.rs".to_string(),
            serde_bytes::ByteBuf::from(b"fn x(){}".to_vec()),
        )]);
        let decoded = decode_ast_files(&encoded, limits()).expect("valid source collection");
        assert_eq!(
            decoded,
            vec![("src/lib.rs".to_string(), b"fn x(){}".to_vec())]
        );
    }

    #[test]
    fn rejects_host_paths_traversal_duplicates_and_declared_bombs() {
        for name in [
            "/private/source.rs",
            "../source.rs",
            "C:\\source.rs",
            "a/./b.rs",
        ] {
            let encoded = pack(vec![(
                name.to_string(),
                serde_bytes::ByteBuf::from(vec![1]),
            )]);
            assert!(
                decode_ast_files(&encoded, limits()).is_err(),
                "accepted {name}"
            );
        }

        let duplicate = pack(vec![
            ("a.rs".to_string(), serde_bytes::ByteBuf::from(vec![1])),
            ("a.rs".to_string(), serde_bytes::ByteBuf::from(vec![2])),
        ]);
        assert!(decode_ast_files(&duplicate, limits()).is_err());

        // array32 with a huge declared count and no entries: rejection happens
        // before allocation or element decoding.
        let declared_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_ast_files(&declared_bomb, limits()).is_err());
    }

    #[test]
    fn rejects_per_source_and_aggregate_overflow() {
        let one_too_large = pack(vec![(
            "a.rs".to_string(),
            serde_bytes::ByteBuf::from(vec![0; 9]),
        )]);
        assert!(decode_ast_files(&one_too_large, limits()).is_err());

        let aggregate = pack(vec![
            ("a.rs".to_string(), serde_bytes::ByteBuf::from(vec![0; 7])),
            ("b.rs".to_string(), serde_bytes::ByteBuf::from(vec![0; 7])),
        ]);
        assert!(decode_ast_files(&aggregate, limits()).is_err());
    }
}

#[cfg(test)]
mod nested_payload_security_tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Element<'a> {
        role: &'a str,
        name: &'a str,
        x: i64,
        y: i64,
        w: i64,
        h: i64,
    }

    #[derive(Serialize)]
    struct ScreenWire<'a> {
        session_id: &'a str,
        frame_seq: u64,
        prev_frame_id: &'a str,
        prev_hash: u64,
        png: serde_bytes::ByteBuf,
        elements: Vec<Element<'a>>,
    }

    fn png(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
        bytes.extend_from_slice(&13u32.to_be_bytes());
        bytes.extend_from_slice(b"IHDR");
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&[8, 2, 0, 0, 0]);
        bytes
    }

    fn screen_blob(session: &str, frame_seq: u64, previous: &str, width: u32) -> Vec<u8> {
        rmp_serde::to_vec_named(&ScreenWire {
            session_id: session,
            frame_seq,
            prev_frame_id: previous,
            prev_hash: 0,
            png: serde_bytes::ByteBuf::from(png(width, 1080)),
            elements: vec![Element {
                role: "button",
                name: "Save",
                x: 1,
                y: 2,
                w: 10,
                h: 10,
            }],
        })
        .unwrap()
    }

    #[test]
    fn screen_observation_is_bounded_and_session_local() {
        let valid = screen_blob("session-1", 2, "screenobservation:session-1:1", 1920);
        assert!(decode_screen_observation(&valid).is_ok());

        let cross_session = screen_blob("session-1", 2, "screenobservation:session-2:1", 1920);
        assert!(decode_screen_observation(&cross_session).is_err());

        let oversized_dimensions = screen_blob("session-1", 0, "", 40_000);
        assert!(decode_screen_observation(&oversized_dimensions).is_err());
        assert!(decode_screen_observation(&[0xdd, 0xff, 0xff, 0xff, 0xff]).is_err());
    }

    #[test]
    fn multi_graph_batch_rejects_duplicate_graphs_and_inner_bombs() {
        let empty_ops = serde_bytes::ByteBuf::from(vec![0x90]);
        let valid = rmp_serde::to_vec_named(&vec![("graph-a", empty_ops.clone())]).unwrap();
        assert_eq!(decode_multi_graph_batches(&valid).unwrap().len(), 1);

        let duplicate = rmp_serde::to_vec_named(&vec![
            ("graph-a", empty_ops.clone()),
            ("graph-a", empty_ops),
        ])
        .unwrap();
        assert!(decode_multi_graph_batches(&duplicate).is_err());

        let inner_bomb = rmp_serde::to_vec_named(&vec![(
            "graph-a",
            serde_bytes::ByteBuf::from(vec![0xdd, 0xff, 0xff, 0xff, 0xff]),
        )])
        .unwrap();
        assert!(decode_multi_graph_batches(&inner_bomb).is_err());
    }

    #[test]
    fn request_preflight_scans_binary_fields_but_not_opaque_payloads() {
        let bomb = vec![0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(preflight_request_msgpack(&Method::AddNode {
            node_id: "node".to_string(),
            properties_msgpack: bomb,
        })
        .is_err());
        assert!(preflight_request_msgpack(&Method::Sql {
            query: "SELECT 1".to_string(),
            params_msgpack: Vec::new(),
        })
        .is_ok());
    }
}

#[cfg(all(test, feature = "redb"))]
mod eg318_dispatch_tests {
    use super::*;
    use crate::acl::{AgentIdentity, AgentRole};
    use crate::channels::ChannelManager;
    use crate::durability::DurabilityPolicy;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request};
    use crate::registry::GraphRegistry;
    use crate::server::auth::sign_current_test_request;
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;
    use dashmap::DashMap;
    use std::ops::Deref;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    const SECRET: &str = "eg318-test-secret";

    struct DurableTestState {
        state: Option<Arc<RwLock<ServerState>>>,
        dir: PathBuf,
    }

    impl Deref for DurableTestState {
        type Target = Arc<RwLock<ServerState>>;

        fn deref(&self) -> &Self::Target {
            self.state.as_ref().expect("test state remains live")
        }
    }

    impl Drop for DurableTestState {
        fn drop(&mut self) {
            // Close redb before deleting its test directory.
            drop(self.state.take());
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn state_min() -> DurableTestState {
        let dir = std::env::temp_dir().join(format!(
            "eg318-dispatch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos()
        ));
        let dir_string = dir.to_string_lossy().to_string();
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(
            RedbBackend::open(dir_string.clone(), DurabilityPolicy::Each, 64)
                .expect("open authoritative test backend"),
        );
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "system".to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        let state = Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation,
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir_string),
            persistence: Some(persistence),
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }));
        DurableTestState {
            state: Some(state),
            dir,
        }
    }

    fn req(id: u64, method: Method) -> Request {
        sign_current_test_request(
            SECRET,
            Request {
                id,
                graph: "__commons__".into(),
                auth_token: String::new(),
                agent_id: Some("system".to_string()),
                method,
            },
        )
    }

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// BUG-044-class: keep the full (`--features full`) dispatcher's state machine
    /// behind one heap indirection. `dispatch()` bottoms out in `dispatch_inner`,
    /// a single very large async fn whose generated future is enormous; nesting it
    /// (this module's tests awaiting `dispatch` inside a `#[tokio::test]` future) can
    /// exhaust the test harness thread's stack before the first request is even
    /// polled, aborting the WHOLE test binary with SIGABRT and hiding every other
    /// test's result. Same fix as `server::mod::tests::dispatch_on_heap` (8e00e0b),
    /// `result_cache_dispatch_tests` (ae64cfd), and `redb_backend`'s tests (92586a7).
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-220 — CreateSummaryNode over the wire → SummaryChildren
    /// reads back the linked children.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_create_summary_then_read_children() {
        let state = state_min();
        for (i, id) in ["e1", "e2"].iter().enumerate() {
            let r = dispatch_on_heap(
                &state,
                req(
                    100 + i as u64,
                    Method::AddNode {
                        node_id: (*id).into(),
                        properties_msgpack: blob(serde_json::json!({"type": "Episodic"})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "AddNode: {:?}", r.error);
        }
        let created = dispatch_on_heap(
            &state,
            req(
                1,
                Method::CreateSummaryNode {
                    level: 1,
                    child_ids: vec!["e1".into(), "e2".into()],
                    props_msgpack: blob(serde_json::json!({})),
                },
            ),
        )
        .await;
        let sid = match created.result {
            Some(ResultPayload::String(s)) => s,
            other => panic!("CreateSummaryNode: {:?} / {:?}", other, created.error),
        };
        let children = dispatch_on_heap(
            &state,
            req(
                2,
                Method::SummaryChildren {
                    node_id: sid.clone(),
                },
            ),
        )
        .await;
        match children.result {
            Some(ResultPayload::Ids(ids)) => assert_eq!(ids, vec!["e1", "e2"]),
            other => panic!("SummaryChildren: {:?} / {:?}", other, children.error),
        }
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-221 — Consolidate over the wire returns the deterministic
    /// semantic node id.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_consolidate_returns_semantic_id() {
        let state = state_min();
        for (i, id) in ["a", "b"].iter().enumerate() {
            let _ = dispatch_on_heap(
                &state,
                req(
                    200 + i as u64,
                    Method::AddNode {
                        node_id: (*id).into(),
                        properties_msgpack: blob(serde_json::json!({"type": "Episodic"})),
                    },
                ),
            )
            .await;
        }
        let r = dispatch_on_heap(
            &state,
            req(
                3,
                Method::Consolidate {
                    episodic_ids: vec!["a".into(), "b".into()],
                    semantic_props_msgpack: blob(serde_json::json!({"summary": "s"})),
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::String(s)) => assert!(s.starts_with("semantic:")),
            other => panic!("Consolidate: {:?} / {:?}", other, r.error),
        }
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-222 — Maintain (decay + evict) over the wire returns the
    /// `(decayed, pruned_ids)` tuple.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_maintain_decays_and_evicts() {
        let state = state_min();
        // A low-importance node in the working set gets evicted below threshold.
        let _ = dispatch_on_heap(
            &state,
            req(
                300,
                Method::AddNode {
                    node_id: "low".into(),
                    properties_msgpack: blob(serde_json::json!({"importance": 0.1})),
                },
            ),
        )
        .await;
        let r = dispatch_on_heap(
            &state,
            req(
                4,
                Method::Maintain {
                    ids: vec!["low".into()],
                    now_ms: 1_000,
                    half_life_ms: 604_800_000,
                    evict_threshold: 0.5,
                    delete: false,
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("Maintain: {:?} / {:?}", other, r.error),
        };
        let (_decayed, pruned): (usize, Vec<String>) = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(pruned, vec!["low"]);
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-087 — AddSceneObject over the wire → WorldTransform reads
    /// back the composed world pose.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_scene_object_then_world_transform() {
        let state = state_min();
        let pose = serde_json::json!({"translation": {"x": 5.0, "y": 0.0, "z": 0.0}});
        let created = dispatch_on_heap(
            &state,
            req(
                5,
                Method::AddSceneObject {
                    pose_msgpack: blob(pose),
                    parent: None,
                },
            ),
        )
        .await;
        let oid = match created.result {
            Some(ResultPayload::String(s)) => s,
            other => panic!("AddSceneObject: {:?} / {:?}", other, created.error),
        };
        let wt = dispatch_on_heap(&state, req(6, Method::WorldTransform { node_id: oid })).await;
        match wt.result {
            Some(ResultPayload::Json(v)) => {
                let tx = v["translation"]["x"].as_f64().unwrap();
                assert!((tx - 5.0).abs() < 1e-9, "world x = {tx}");
            }
            other => panic!("WorldTransform: {:?} / {:?}", other, wt.error),
        }
    }

    /// CONCEPT:EG-KG.memory.eg-batch-decay-caller/EG-099 — StartTrajectory + AppendStep over the wire →
    /// DiscountedReturn computes `Σ gamma^t · reward`.
    #[tokio::test(flavor = "multi_thread")]
    async fn eg318_trajectory_append_then_discounted_return() {
        let state = state_min();
        let started = dispatch_on_heap(
            &state,
            req(
                7,
                Method::StartTrajectory {
                    props_msgpack: blob(serde_json::json!({})),
                },
            ),
        )
        .await;
        let tid = match started.result {
            Some(ResultPayload::String(s)) => s,
            other => panic!("StartTrajectory: {:?} / {:?}", other, started.error),
        };
        for (i, reward) in [2.0f64, 4.0].into_iter().enumerate() {
            let r = dispatch_on_heap(
                &state,
                req(
                    8 + i as u64,
                    Method::AppendStep {
                        traj_id: tid.clone(),
                        action_msgpack: blob(serde_json::json!("go")),
                        reward,
                        state_ref: None,
                        next_state_ref: None,
                        t: i as u64,
                    },
                ),
            )
            .await;
            // Raw(Option<String>) — Some(step id) since the trajectory exists.
            match r.result {
                Some(ResultPayload::Raw(b)) => {
                    let step: Option<String> = rmp_serde::from_slice(&b).unwrap();
                    assert!(step.is_some(), "AppendStep should return a step id");
                }
                other => panic!("AppendStep: {:?} / {:?}", other, r.error),
            }
        }
        let dr = dispatch_on_heap(
            &state,
            req(
                20,
                Method::DiscountedReturn {
                    traj_id: tid,
                    gamma: 0.5,
                },
            ),
        )
        .await;
        match dr.result {
            // 2.0 + 0.5^1 * 4.0 = 4.0
            Some(ResultPayload::Float(f)) => assert!((f - 4.0).abs() < 1e-9, "return = {f}"),
            other => panic!("DiscountedReturn: {:?} / {:?}", other, dr.error),
        }
    }

    /// Public Ts* calls must share the ordinary graph ACL boundary, and identical
    /// local series ids in two tenants must never collide in series.redb.
    #[cfg(feature = "tsdb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn timeseries_is_graph_authorized_and_tenant_scoped() {
        let state = state_min();
        let path = std::env::temp_dir().join(format!(
            "eg-ts-policy-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mut s = state.write().await;
            s.tsdb_store = Some(Arc::new(eg_tsdb::store::SeriesStore::open(&path).unwrap()));
            // RBAC (`feature = "security"`) is the mandatory current access decision
            // for a non-System identity — `check_access` ignores `graph_owner`
            // entirely under this feature and evaluates ONLY `identity.roles`
            // against the RBAC policy (no pre-RBAC ACL fall-through, no "owner
            // always wins" shortcut). So each agent needs an explicit grant on
            // their own private graph, or every Ts* call below default-denies.
            #[cfg(feature = "security")]
            {
                use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
                s.isolation.add_role(Role::new("owner-acme-private"));
                s.isolation.add_role(Role::new("owner-other-private"));
                let grant = |role: &str, graph: &str, action: RbacAction| Grant {
                    role: role.to_string(),
                    resource: ResourceSelector::Graph(graph.to_string()),
                    action,
                    effect: GrantEffect::Allow,
                };
                for action in [RbacAction::Read, RbacAction::Write] {
                    s.isolation
                        .add_grant(grant("owner-acme-private", "acme:private", action));
                    s.isolation
                        .add_grant(grant("owner-other-private", "other:private", action));
                }
            }
            s.isolation.register_agent(AgentIdentity {
                agent_id: "alice".into(),
                role: AgentRole::Agent,
                teams: vec![],
                #[cfg(feature = "security")]
                roles: vec!["owner-acme-private".into()],
                #[cfg(not(feature = "security"))]
                roles: vec![],
            });
            s.isolation.register_agent(AgentIdentity {
                agent_id: "bob".into(),
                role: AgentRole::Agent,
                teams: vec![],
                #[cfg(feature = "security")]
                roles: vec!["owner-other-private".into()],
                #[cfg(not(feature = "security"))]
                roles: vec![],
            });
            let _ = s.registry.create_graph(
                "acme:private",
                crate::protocol::GraphType::Agent,
                Some("alice".into()),
            );
            let _ = s.registry.create_graph(
                "other:private",
                crate::protocol::GraphType::Agent,
                Some("bob".into()),
            );
        }
        let request = |id: u64, graph: &str, agent: &str, method: Method| {
            sign_current_test_request(
                SECRET,
                Request {
                    id,
                    graph: graph.into(),
                    auth_token: String::new(),
                    agent_id: Some(agent.into()),
                    method,
                },
            )
        };
        let append = |value: f64| Method::TsAppend {
            series_id: "cpu".into(),
            n_fields: 1,
            bucket_ns: 1_000,
            field_names: vec!["value".into()],
            points_msgpack: rmp_serde::to_vec(&vec![(1i64, vec![value])]).unwrap(),
        };
        assert!(
            dispatch_on_heap(&state, request(1, "acme:private", "alice", append(10.0)))
                .await
                .error
                .is_none()
        );
        assert!(
            dispatch_on_heap(&state, request(2, "other:private", "bob", append(20.0)))
                .await
                .error
                .is_none()
        );

        let denied = dispatch_on_heap(
            &state,
            request(
                3,
                "other:private",
                "alice",
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10,
                },
            ),
        )
        .await;
        assert!(
            denied.error.is_some(),
            "cross-tenant series read must be denied"
        );

        let own = dispatch_on_heap(
            &state,
            request(
                4,
                "acme:private",
                "alice",
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10,
                },
            ),
        )
        .await;
        let points: Vec<(i64, Vec<f64>)> = match own.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected scoped TsRange result, got {other:?}"),
        };
        assert_eq!(points, vec![(1, vec![10.0])]);
        drop(state);
        let _ = std::fs::remove_file(path);
    }
}

// ── Admin-scope enforcement dispatch round-trip (CONCEPT:EG-KG.compute.feature, EG-P0-6) ──────
//
// Drives `Method::RegisterIdentity` and `Method::RbacAdmin` through the SAME
// `dispatch` entrypoint a wire request hits, proving the admin-scope gate added at
// the top of `dispatch_inner` actually rejects a caller without admin capability
// and allows one that has it — both the `System`-role bypass and an explicit RBAC
// `Admin` grant. Runs on a bare `--features server,security` build.
#[cfg(all(test, feature = "security"))]
mod admin_scope_tests {
    use super::*;
    use crate::acl::{
        AgentIdentity, Grant, GrantEffect, RbacAction, RbacAdminOp, ResourceSelector, Role,
    };
    use crate::channels::ChannelManager;
    use crate::isolation::{AgentRole, IsolationLayer};
    use crate::protocol::{Method, Request};
    use crate::registry::GraphRegistry;
    use crate::server::auth::sign_current_test_request;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    const SECRET: &str = "admin-scope-test-secret";

    /// BUG-044-class: see `eg318_dispatch_tests::dispatch_on_heap` above for why every
    /// `dispatch()` call in a test needs one heap indirection to avoid overflowing the
    /// harness thread's stack and SIGABRTing the whole test binary.
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    fn state_min() -> Arc<RwLock<ServerState>> {
        let mut isolation = IsolationLayer::new();
        for (agent_id, role) in [("root", AgentRole::System), ("alice", AgentRole::Agent)] {
            isolation.register_agent(AgentIdentity {
                agent_id: agent_id.to_string(),
                role,
                teams: Vec::new(),
                roles: Vec::new(),
            });
        }
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation,
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    fn req_as(id: u64, agent_id: Option<&str>, method: Method) -> Request {
        sign_current_test_request(
            SECRET,
            Request {
                id,
                graph: "__commons__".into(),
                auth_token: String::new(),
                agent_id: Some(agent_id.unwrap_or("system").to_string()),
                method,
            },
        )
    }

    async fn register_identity(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        caller: Option<&str>,
        agent_id: &str,
        role: AgentRole,
    ) -> Response {
        dispatch_on_heap(
            state,
            req_as(
                id,
                caller,
                Method::RegisterIdentity {
                    agent_id: agent_id.into(),
                    role,
                    teams: vec![],
                    signature: String::new(),
                    roles: vec![],
                },
            ),
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_identity_policy_accepts_only_signer_backed_current_bootstrap() {
        let state = state_min();
        state.write().await.isolation = IsolationLayer::new();
        let r = register_identity(&state, 1, Some("root"), "root", AgentRole::System).await;
        assert!(r.error.is_none(), "current bootstrap failed: {:?}", r.error);
        assert!(state.read().await.isolation.has_admin_capability("root"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn identity_bootstrap_is_atomic_under_concurrent_requests() {
        let state = state_min();
        state.write().await.isolation = IsolationLayer::new();
        // `register_identity`'s embedded `Method::RegisterIdentity.signature` is checked
        // against `#[cfg(test)]`'s hardcoded `signer_registry()` allowlist
        // (`["system", "root", "alice", "priv"]`, src/server/auth.rs) BEFORE anything
        // concurrency-related runs — an untrusted signer name fails closed at
        // `verify_register_identity_signature` regardless of which request wins the
        // race. "first"/"second" were never registered there, so both calls failed
        // identically (0 successes) for a reason that has nothing to do with the
        // atomicity this test exists to prove. "root"/"alice" are two DISTINCT
        // already-trusted test signers, matching every other test in this module.
        let (first, second) = tokio::join!(
            register_identity(&state, 11, Some("root"), "root", AgentRole::System),
            register_identity(&state, 12, Some("alice"), "alice", AgentRole::System),
        );
        assert_eq!(
            [first, second]
                .into_iter()
                .filter(|response| response.error.is_none())
                .count(),
            1
        );
        assert!(!state.read().await.isolation.identity_bootstrap_pending());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn removing_all_identities_does_not_reopen_bootstrap() {
        let state = state_min();
        state.write().await.isolation = IsolationLayer::new();
        let first = register_identity(&state, 21, Some("root"), "root", AgentRole::System).await;
        assert!(first.error.is_none());
        assert!(state
            .write()
            .await
            .isolation
            .try_unregister_agent("root")
            .unwrap());
        assert!(!state.read().await.isolation.identity_bootstrap_pending());

        let second =
            register_identity(&state, 22, Some("second"), "second", AgentRole::System).await;
        assert!(second.error.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_identity_policy_rejects_delegated_or_non_system_bootstrap() {
        let state = state_min();
        state.write().await.isolation = IsolationLayer::new();
        let delegated =
            register_identity(&state, 2, Some("system"), "root", AgentRole::System).await;
        assert!(delegated.error.is_some());

        let non_system =
            register_identity(&state, 3, Some("alice"), "alice", AgentRole::Agent).await;
        assert!(non_system.error.is_some());
        assert!(!state.read().await.isolation.has_rules());
    }

    /// Once ANY identity exists, a plain `Agent`-role caller with NO admin
    /// capability is REJECTED trying to register another identity — the core
    /// EG-P0-6 guarantee (an admin method without the capability is rejected).
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_method_rejected_without_capability() {
        let state = state_min();
        // alice (no roles, no grants, not System) tries to register "bob".
        let r = register_identity(&state, 3, Some("alice"), "bob", AgentRole::Agent).await;
        assert!(r.error.is_some(), "expected ACCESS_DENIED, got {:?}", r);
        let msg = r.error.unwrap();
        assert!(
            msg.contains("ACCESS_DENIED") && msg.contains("admin capability"),
            "unexpected denial message: {msg}"
        );
    }

    /// A `System`-role caller (root) always holds admin capability — WITH the
    /// capability, the same admin method is allowed.
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_method_allowed_for_system_role() {
        let state = state_min();
        let r = register_identity(&state, 2, Some("root"), "bob", AgentRole::Agent).await;
        assert!(
            r.error.is_none(),
            "root (System) must be allowed: {:?}",
            r.error
        );
    }

    /// A non-System agent with an EXPLICIT RBAC `Admin` grant (over
    /// `ResourceSelector::All`) also holds admin capability — proving the gate
    /// really reads the RBAC evaluator, not just a `System`-role special case.
    #[tokio::test(flavor = "multi_thread")]
    async fn admin_method_allowed_with_explicit_rbac_admin_grant() {
        let state = state_min();
        // Give "auditor-admin" the RBAC role "sysadmin" via RbacAdmin (itself an
        // admin action -- root, System, is allowed to call it).
        let add_role = dispatch_on_heap(
            &state,
            req_as(
                2,
                Some("root"),
                Method::RbacAdmin {
                    op: RbacAdminOp::AddRole(Role::new("sysadmin")),
                },
            ),
        )
        .await;
        assert!(add_role.error.is_none(), "AddRole: {:?}", add_role.error);

        let add_grant = dispatch_on_heap(
            &state,
            req_as(
                3,
                Some("root"),
                Method::RbacAdmin {
                    op: RbacAdminOp::AddGrant(Grant {
                        role: "sysadmin".into(),
                        resource: ResourceSelector::All,
                        action: RbacAction::Admin,
                        effect: GrantEffect::Allow,
                    }),
                },
            ),
        )
        .await;
        assert!(add_grant.error.is_none(), "AddGrant: {:?}", add_grant.error);

        // Register "priv" holding the "sysadmin" role.
        let r = dispatch_on_heap(
            &state,
            req_as(
                4,
                Some("root"),
                Method::RegisterIdentity {
                    agent_id: "priv".into(),
                    role: AgentRole::Agent,
                    teams: vec![],
                    signature: String::new(),
                    roles: vec!["sysadmin".into()],
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "register priv: {:?}", r.error);

        // "priv" (Agent role, but RBAC-granted Admin) now registers "carol" — must
        // be ALLOWED even though priv is not System.
        let r = register_identity(&state, 5, Some("priv"), "carol", AgentRole::Agent).await;
        assert!(
            r.error.is_none(),
            "an agent with an explicit RBAC Admin grant must be allowed: {:?}",
            r.error
        );
    }
}

// ── Blob substrate dispatch round-trip (CONCEPT:EG-KG.storage.blob-namespace) ─────────────────────
//
// Drives the Blob* methods through the SAME `dispatch` entrypoint a wire request
// hits (auth → routing → handler → CAS), proving streamed round-trip integrity +
// dedup + bounded memory + GC over the real protocol — not just the store unit.
#[cfg(all(test, feature = "blob"))]
mod blob_dispatch_tests {
    use super::*;
    use crate::acl::{AgentIdentity, AgentRole};
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request};
    use crate::registry::GraphRegistry;
    use crate::server::auth::sign_current_test_request;
    use crate::server::blob::{BlobCursors, RedbChunkStore};
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "blob-test-secret";

    /// BUG-044-class: see `eg318_dispatch_tests::dispatch_on_heap` above for why every
    /// `dispatch()` call in a test needs one heap indirection to avoid overflowing the
    /// harness thread's stack and SIGABRTing the whole test binary.
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    fn state_with_blob(dir: &str) -> Arc<RwLock<ServerState>> {
        let store = Arc::new(RedbChunkStore::open(dir).unwrap());
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "system".to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation,
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir.to_string()),
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            blob: Some(Arc::new(BlobCursors::new(store))),
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        sign_current_test_request(
            SECRET,
            Request {
                id,
                graph: "__commons__".into(),
                auth_token: String::new(),
                agent_id: Some("system".to_string()),
                method,
            },
        )
    }

    fn peak_rss_mb() -> u64 {
        let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                if let Some(kb) = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    return kb / 1024;
                }
            }
        }
        0
    }

    /// Upload `data` chunk-by-chunk via dispatch (never resident whole), commit,
    /// return the blob digest.
    async fn upload(
        state: &Arc<RwLock<ServerState>>,
        next_id: &mut u64,
        data: &[u8],
        chunk_size: usize,
    ) -> String {
        let begin = dispatch_on_heap(
            state,
            req(
                *next_id,
                Method::BlobBegin {
                    chunk_size: chunk_size as u32,
                },
            ),
        )
        .await;
        *next_id += 1;
        let cursor = match begin.result {
            Some(ResultPayload::Count(c)) => c,
            other => panic!("BlobBegin: {:?} / {:?}", other, begin.error),
        };
        for part in data.chunks(chunk_size) {
            let r = dispatch_on_heap(
                state,
                req(
                    *next_id,
                    Method::BlobChunkPut {
                        cursor,
                        data: part.to_vec(),
                    },
                ),
            )
            .await;
            *next_id += 1;
            assert!(r.error.is_none(), "BlobChunkPut: {:?}", r.error);
        }
        let commit = dispatch_on_heap(state, req(*next_id, Method::BlobCommit { cursor })).await;
        *next_id += 1;
        match commit.result {
            Some(ResultPayload::String(d)) => d,
            other => panic!("BlobCommit: {:?} / {:?}", other, commit.error),
        }
    }

    /// Stream `digest` back down chunk-by-chunk via dispatch, reassemble.
    async fn download(
        state: &Arc<RwLock<ServerState>>,
        next_id: &mut u64,
        digest: &str,
    ) -> Vec<u8> {
        let begin = dispatch_on_heap(
            state,
            req(
                *next_id,
                Method::BlobFetchBegin {
                    digest: digest.into(),
                },
            ),
        )
        .await;
        *next_id += 1;
        let (cursor, n): (u64, u32) = match begin.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("BlobFetchBegin: {:?} / {:?}", other, begin.error),
        };
        let mut out = Vec::new();
        for idx in 0..n {
            let r =
                dispatch_on_heap(state, req(*next_id, Method::BlobChunkGet { cursor, idx })).await;
            *next_id += 1;
            match r.result {
                // The chunk travels as a `Raw` MessagePack `bin` (serde_bytes) so the
                // Python client recovers raw bytes via its second `unpackb`; decode
                // that here to reassemble the original content.
                Some(ResultPayload::Raw(packed)) => {
                    let bytes: serde_bytes::ByteBuf =
                        rmp_serde::from_slice(&packed).expect("BlobChunkGet Raw decode");
                    out.extend(bytes.into_vec());
                }
                other => panic!("BlobChunkGet: {:?} / {:?}", other, r.error),
            }
        }
        let _ = dispatch_on_heap(state, req(*next_id, Method::BlobFetchEnd { cursor })).await;
        *next_id += 1;
        out
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn roundtrip_dedup_bounded_memory_and_gc() {
        // Held for the whole test. This test drives every Blob* method through the
        // real `dispatch()` entrypoint (auth → routing → handler), which resolves
        // process-global env-configured state on the request path; a concurrent
        // `crypto::tests::EnvGuard`-protected test transiently mutating the shared
        // `EPISTEMIC_GRAPH_ENCRYPTION_KEY`/`_TXN_RECOVERY_KEY` env vars elsewhere in
        // the crate can otherwise land mid-flight of this test's dispatch calls. See
        // `crate::crypto::acquire_test_env_lock`'s doc for the full mechanism.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock();
        let dir = std::env::temp_dir().join(format!("eg-blob-dispatch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // Baseline BEFORE any of this test's own allocation, so the bounded-memory
        // assertion below measures the DELTA this test's own streamed upload adds to
        // `VmHWM`, not the absolute process-wide high-water mark. `VmHWM` is a
        // PROCESS-WIDE counter (`/proc/self/status`), and `cargo test`'s default
        // parallel run shares one process across every concurrently-running test —
        // some OTHER, unrelated, memory-heavier test (e.g. a large redb/persistence
        // fixture) can already have pushed the process's peak well past this test's
        // own 16 MB + 512 MB budget before this test's first allocation, failing an
        // assertion this test's own behavior never violated. This was a real,
        // reproducible parallel-run flake (observed: 1593MB absolute peak against a
        // 528MB budget, entirely attributable to concurrently-running sibling tests).
        // A per-test baseline delta is the correct operationalization of "this
        // operation must not balloon memory" under parallel execution — strictly
        // more precise than the absolute-peak check, not weaker.
        let baseline_rss_mb = peak_rss_mb();
        let state = state_with_blob(&dir.to_string_lossy());
        let mut id = 1u64;

        // 16 MB blob streamed as 2 MiB chunks. NON-dedupable content (offset-seeded)
        // so real chunks are stored; the file is never held whole in this test
        // either — each chunk is generated, dispatched, then dropped.
        let chunk_size = 2 * 1024 * 1024usize;
        let n_chunks = 8u64;
        let mut full = Vec::new(); // only kept to verify the round-trip equals source
        {
            // Upload streaming: build+dispatch one chunk at a time.
            let begin = dispatch_on_heap(
                &state,
                req(
                    id,
                    Method::BlobBegin {
                        chunk_size: chunk_size as u32,
                    },
                ),
            )
            .await;
            id += 1;
            let cursor = match begin.result {
                Some(ResultPayload::Count(c)) => c,
                o => panic!("begin {:?}", o),
            };
            for c in 0..n_chunks {
                let mut buf = vec![0u8; chunk_size];
                let mut x = (c + 1).wrapping_mul(0x9E3779B97F4A7C15) | 1;
                for b in buf.iter_mut() {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    *b = (x & 0xFF) as u8;
                }
                full.extend_from_slice(&buf);
                let r =
                    dispatch_on_heap(&state, req(id, Method::BlobChunkPut { cursor, data: buf }))
                        .await;
                id += 1;
                assert!(r.error.is_none());
            }
            let commit = dispatch_on_heap(&state, req(id, Method::BlobCommit { cursor })).await;
            id += 1;
            let digest = match commit.result {
                Some(ResultPayload::String(d)) => d,
                o => panic!("commit {:?}", o),
            };

            // Round-trip integrity.
            let got = download(&state, &mut id, &digest).await;
            assert_eq!(got.len(), full.len());
            assert_eq!(got, full);

            // Bounded memory: the whole 16 MB blob was streamed through dispatch,
            // and the RSS this test's OWN work adds must stay well under buffering
            // the whole object on both sides. We keep ONE copy (`full`) for the
            // integrity assert, so allow total + a fixed floor; a regression that
            // buffers the file in the cursor/handler would blow past this. Measured
            // as a delta off the pre-test baseline (see above) so a concurrently
            // running, unrelated, memory-heavier sibling test cannot fail this
            // assertion on THIS test's behalf.
            let total_mb = (n_chunks * chunk_size as u64) / (1024 * 1024);
            let peak = peak_rss_mb().saturating_sub(baseline_rss_mb);
            assert!(
                peak < total_mb + 512,
                "peak RSS delta {peak}MB (baseline {baseline_rss_mb}MB) should stay \
                 bounded for a {total_mb}MB streamed blob"
            );

            // Reference the blob (a :Media node points at it).
            let r = dispatch_on_heap(
                &state,
                req(
                    id,
                    Method::BlobRef {
                        digest: digest.clone(),
                    },
                ),
            )
            .await;
            id += 1;
            assert!(matches!(r.result, Some(ResultPayload::Count(1))));

            // Dedup: re-upload identical content → same digest, ZERO new chunks.
            let store = state.read().await.blob.as_ref().unwrap().store.clone();
            let chunks_before = store.chunk_count().unwrap();
            let digest2 = upload(&state, &mut id, &full, chunk_size).await;
            let chunks_after = store.chunk_count().unwrap();
            assert_eq!(digest, digest2, "identical content ⇒ identical digest");
            assert_eq!(chunks_before, chunks_after, "dedup: no new chunks");

            // GC keeps a referenced blob, reclaims an unreferenced one. digest is
            // referenced (count 1); digest2 == digest so still 1 reference total.
            let gc = dispatch_on_heap(&state, req(id, Method::BlobGc)).await;
            id += 1;
            let (blobs, _chunks): (u64, u64) = match gc.result {
                Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
                o => panic!("gc {:?}", o),
            };
            assert_eq!(blobs, 0, "referenced blob is kept");
            // Still fetchable after GC.
            assert_eq!(download(&state, &mut id, &digest).await, full);

            // Drop the reference → GC reclaims the blob + all its chunks.
            let r = dispatch_on_heap(
                &state,
                req(
                    id,
                    Method::BlobUnref {
                        digest: digest.clone(),
                    },
                ),
            )
            .await;
            id += 1;
            assert!(matches!(r.result, Some(ResultPayload::Count(0))));
            let gc = dispatch_on_heap(&state, req(id, Method::BlobGc)).await;
            id += 1;
            let (blobs, chunks): (u64, u64) = match gc.result {
                Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
                o => panic!("gc {:?}", o),
            };
            assert_eq!(blobs, 1, "unreferenced blob reclaimed");
            assert_eq!(chunks, n_chunks, "all its orphan chunks reclaimed");
            assert_eq!(store.chunk_count().unwrap(), 0);
            // Fetching a reclaimed blob now fails.
            let r = dispatch_on_heap(&state, req(id, Method::BlobFetchBegin { digest })).await;
            assert!(r.error.is_some());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
