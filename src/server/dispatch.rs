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
    check_caller_is_known, check_graph_access, is_admin_authz_action, require_admin_capability,
    requires_write, CarrierAuthority, GraphReadAuthority,
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
#[cfg(feature = "redb")]
use super::persistence::PersistenceBackend;
use super::state::ServerState;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Request, Response, ResultPayload};

/// Stable, privacy-safe rejection for a graph lifecycle type that this binary
/// does not support.  The wire decoder normally rejects unknown enum strings
/// before dispatch; keeping this code at the authenticated dispatch boundary
/// is defense in depth for in-process callers and future enum extensions.
const UNSUPPORTED_GRAPH_TYPE: &str = "INVALID_ARGUMENT: unsupported graph type";

/// Validate the graph lifecycle type before any placement, persistence, or
/// registry work begins.  `GraphType` is intentionally a closed wire enum
/// today, but the explicit allowlist means a future enum variant cannot be
/// accepted by this boundary accidentally until its lifecycle contract is
/// deliberately reviewed.
fn validate_graph_create_type(graph_type: crate::protocol::GraphType) -> Result<(), &'static str> {
    if matches!(
        graph_type,
        crate::protocol::GraphType::Agent
            | crate::protocol::GraphType::Team
            | crate::protocol::GraphType::Global
            | crate::protocol::GraphType::Commons
    ) {
        Ok(())
    } else {
        Err(UNSUPPORTED_GRAPH_TYPE)
    }
}

#[cfg(test)]
mod graph_create_type_validation_tests {
    use super::{validate_graph_create_type, UNSUPPORTED_GRAPH_TYPE};
    use crate::protocol::GraphType;

    #[test]
    fn accepts_every_supported_graph_lifecycle_type() {
        for graph_type in [
            GraphType::Agent,
            GraphType::Team,
            GraphType::Global,
            GraphType::Commons,
        ] {
            assert!(validate_graph_create_type(graph_type).is_ok());
        }
    }

    #[test]
    fn unsupported_type_error_is_stable_and_secret_free() {
        assert_eq!(
            UNSUPPORTED_GRAPH_TYPE,
            "INVALID_ARGUMENT: unsupported graph type"
        );
        assert!(!UNSUPPORTED_GRAPH_TYPE.contains("secret"));
    }
}

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

/// Per-tenant opaque coordinator key for a command that owns its OWN store
/// (keyed by blob/kv/series/job/statechart/catalog id) and is therefore not
/// graph-scoped. One totally-ordered consensus route per tenant is what keeps
/// replicas applying these in step.
#[cfg(feature = "raft")]
fn native_route_opaque_key(label: &str, tenant_scope: &str, kind: &str) -> String {
    crate::server::mutation_batch::opaque_coordinator_key(label, tenant_scope, kind)
}

/// A graph-lifecycle command routes to the graph it NAMES, not to the graph the
/// request arrived on. Any other method shape falls back to the request graph.
#[cfg(feature = "raft")]
fn native_route_lifecycle_target(request_graph: &str, method: &Method) -> String {
    match method {
        Method::CreateGraph { graph_name, .. } | Method::DeleteGraph { graph_name } => {
            graph_name.clone()
        }
        _ => request_graph.to_string(),
    }
}

/// The consensus route for one native mutation command.
///
/// ⚠ ACCEPTED COMPLEXITY EXCEPTION — cyclomatic 13, cap 10. Do not "fix" this by
/// adding a wildcard arm.
///
/// This match is deliberately EXHAUSTIVE and must stay that way. It is the one
/// place the compiler can refuse to build when `NativeMutationCommand` gains a
/// variant, forcing whoever adds it to decide that command's consensus route
/// instead of silently inheriting the request graph. An `or_else` chain over
/// `Option`-returning helpers measured 1/0 and was rejected for exactly that
/// reason (WD2-01; the sibling `apply_txn_op` lane made the same call on the
/// same trade-off).
///
/// 13 is the FLOOR for an exhaustive match here, not laziness: the score is arm
/// COUNT, and six of the twelve arms carry distinct `#[cfg]` feature gates, which
/// cannot be applied to individual alternatives of an or-pattern. The graph-scoped
/// variants that CAN be folded already are. Every arm is a single tail call, so
/// cognitive complexity is 1. Against the pre-refactor original (19/3) this is an
/// improvement on both metrics, not a regression.
#[cfg(feature = "raft")]
fn native_route_target(
    request_graph: &str,
    tenant_scope: &str,
    method: &Method,
    command: &crate::raft::NativeMutationCommand,
) -> String {
    use crate::raft::NativeMutationCommand;
    match command {
        // Graph-scoped: the command's data lives in the request's own graph.
        NativeMutationCommand::GraphState { .. }
        | NativeMutationCommand::Multisig { .. }
        | NativeMutationCommand::ChangeEnvelope { .. }
        | NativeMutationCommand::TransactionParticipant { .. }
        | NativeMutationCommand::TransactionDecision { .. }
        | NativeMutationCommand::TransactionFinalize { .. }
        | NativeMutationCommand::WorkItem { .. } => request_graph.to_string(),
        NativeMutationCommand::GraphLifecycle { .. } => {
            native_route_lifecycle_target(request_graph, method)
        }
        // Ordered by the placement group, not by any data graph.
        NativeMutationCommand::ClusterAdmin { .. }
        | NativeMutationCommand::Transaction { .. }
        | NativeMutationCommand::SessionControl { .. } => {
            crate::raft::placement::PLACEMENT_GRAPH.to_string()
        }
        // Identity/RBAC state is process-global authority. Every such command must
        // therefore share one Raft order; tenant-hashed groups could apply
        // concurrent add/remove transitions in different orders on each replica.
        // `__commons__` also preserves the exact bootstrap route asserted before
        // proposal and revalidated by `RaftRequest::validate`.
        NativeMutationCommand::Identity { .. } => "__commons__".to_string(),
        #[cfg(feature = "modality-serving")]
        NativeMutationCommand::ServedModality { .. } => request_graph.to_string(),
        #[cfg(feature = "jobs")]
        NativeMutationCommand::JobPublicationCommit { .. }
        | NativeMutationCommand::JobPublicationFinalize { .. } => request_graph.to_string(),
        #[cfg(feature = "blob")]
        NativeMutationCommand::Blob { .. } => {
            native_route_opaque_key("raft-native-blob", tenant_scope, "control")
        }
        #[cfg(feature = "kv")]
        NativeMutationCommand::KeyValue { .. } => {
            native_route_opaque_key("raft-native-kv", tenant_scope, "control")
        }
        #[cfg(feature = "tsdb")]
        NativeMutationCommand::TimeSeries { .. } => {
            native_route_opaque_key("raft-native-timeseries", tenant_scope, "control")
        }
        #[cfg(feature = "jobs")]
        NativeMutationCommand::AnalyticsJob { .. } => {
            native_route_opaque_key("raft-native-jobs", tenant_scope, "control")
        }
        // Not graph-scoped (own `statecharts.redb`, keyed by def_id/instance_id) --
        // one totally-ordered consensus route per tenant, structurally identical
        // to `AnalyticsJob` above.
        #[cfg(feature = "statechart")]
        NativeMutationCommand::Statechart { .. } => {
            native_route_opaque_key("raft-native-statechart", tenant_scope, "control")
        }
        #[cfg(feature = "sqlite-file")]
        NativeMutationCommand::SqliteCatalog { .. } => {
            native_route_opaque_key("raft-native-sqlite", tenant_scope, "catalog")
        }
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
                idempotency_key: None,
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
        use crate::epistemic_operations_ext::{
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

/// The resolved identity and route of one native consensus proposal, shared by
/// the request builder and the write/coordination helpers below.
#[cfg(feature = "raft")]
struct NativeProposal<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    request_id: u64,
    authority: &'a CarrierAuthority,
    server_secret: &'a str,
    graph_name: &'a str,
    graph_type: crate::protocol::GraphType,
}

/// Which consensus coordination a committed native command still needs.
#[cfg(feature = "raft")]
#[derive(Clone, Copy)]
enum NativeCoordination {
    /// The replicated result is already terminal.
    Terminal,
    /// A worker job publication: commit on the target group, then finalize on
    /// the scheduler's control group.
    #[cfg(feature = "jobs")]
    JobPublication,
    /// A transaction commit: prepare/decide/commit/finalize the participant
    /// fanout.
    TransactionCommit,
}

/// Classified BEFORE the command is proposed, in the same order the inline
/// checks ran: job publication first, then transaction commit.
#[cfg(feature = "raft")]
fn native_coordination_for(method: &Method) -> NativeCoordination {
    #[cfg(feature = "jobs")]
    if matches!(
        method,
        Method::AnalyticsJob {
            op: eg_types::jobs::JobOp::WorkerPublish { .. }
        }
    ) {
        return NativeCoordination::JobPublication;
    }
    if matches!(method, Method::Commit { .. }) {
        return NativeCoordination::TransactionCommit;
    }
    NativeCoordination::Terminal
}

/// Resolve the consensus route for a native command: the graph whose group
/// totally orders it, that graph's type (a `CreateGraph` carries its own; every
/// other method reads the registry, defaulting to `Global`), and the placement
/// authority — all under ONE read lock.
#[cfg(feature = "raft")]
async fn resolve_native_proposal_route(
    state: &Arc<RwLock<ServerState>>,
    request_graph: &str,
    authority: &CarrierAuthority,
    method: &Method,
    command: &crate::raft::NativeMutationCommand,
) -> (
    String,
    Option<Arc<crate::raft::multi::MultiRaft>>,
    crate::protocol::GraphType,
) {
    let graph_name = native_route_target(request_graph, authority.tenant_scope(), method, command);
    let current = timed_read(state).await;
    let graph_type = match method {
        Method::CreateGraph { graph_type, .. } => *graph_type,
        _ => current
            .registry
            .get(&graph_name)
            .map(|entry| entry.graph_type)
            .unwrap_or(crate::protocol::GraphType::Global),
    };
    let multi = current.multi_raft.clone();
    drop(current);
    (graph_name, multi, graph_type)
}

#[cfg(feature = "raft")]
fn build_native_raft_request(
    proposal: &NativeProposal<'_>,
    routed: &crate::raft::multi::RoutedRaftHandle,
    method: &Method,
    command: crate::raft::NativeMutationCommand,
    identity_bootstrap: bool,
) -> Result<crate::raft::RaftRequest, Response> {
    let committed_at_ms = authoritative_now_ms();
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "raft-native",
        proposal.graph_name,
        proposal.request_id,
        method,
    );
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        proposal.request_id,
        proposal.authority.tenant_scope(),
        proposal.authority.actor_scope().to_string(),
        identity_bootstrap,
        routed.epoch,
        routed.placed.then_some(routed.group_id),
        committed_at_ms,
    ) {
        Ok(mutation) => mutation,
        Err(error) => return Err(Response::err(proposal.request_id, error)),
    };
    Ok(crate::raft::RaftRequest {
        graph_fname: crate::persist::sanitize(proposal.graph_name),
        graph_name: proposal.graph_name.to_string(),
        graph_type: proposal.graph_type,
        command: crate::raft::ReplicatedMutation::Native { command },
        committed_at_ms,
        mutation,
    })
}

/// A committed native command that still owes consensus coordination hands its
/// prepared payload to the matching coordinator. Any other result — including a
/// coordination-classified command whose result is NOT a prepared payload — is
/// already terminal and is returned verbatim.
#[cfg(feature = "raft")]
async fn coordinate_native_result(
    proposal: &NativeProposal<'_>,
    coordination: NativeCoordination,
    multi: Arc<crate::raft::multi::MultiRaft>,
    routed: crate::raft::multi::RoutedRaftHandle,
    result: ResultPayload,
) -> Response {
    match (coordination, result) {
        #[cfg(feature = "jobs")]
        (NativeCoordination::JobPublication, ResultPayload::Raw(prepared)) => {
            execute_consensus_job_publication(
                proposal.request_id,
                proposal.authority,
                proposal.server_secret,
                multi,
                routed,
                proposal.graph_name,
                proposal.graph_type,
                &prepared,
            )
            .await
        }
        (NativeCoordination::TransactionCommit, ResultPayload::Raw(prepared)) => {
            execute_consensus_transaction(
                proposal.state,
                proposal.request_id,
                proposal.authority,
                proposal.server_secret,
                multi,
                routed,
                proposal.graph_name,
                proposal.graph_type,
                &prepared,
            )
            .await
        }
        (_, terminal) => Response::ok(proposal.request_id, terminal),
    }
}

/// Propose through the routed group's leader and translate its reply. A write
/// failure is a stale-route redirect (the caller retries against the leader);
/// a committed entry either carries a deterministic result or is a protocol
/// violation.
#[cfg(feature = "raft")]
async fn dispatch_native_raft_write(
    proposal: &NativeProposal<'_>,
    coordination: NativeCoordination,
    multi: Arc<crate::raft::multi::MultiRaft>,
    routed: crate::raft::multi::RoutedRaftHandle,
    request: crate::raft::RaftRequest,
) -> Response {
    let request_id = proposal.request_id;
    let response = match routed.handle.client_write(request).await {
        Ok(response) => response,
        Err(error) => {
            let leader = routed.handle.current_leader().await;
            return Response::stale_route(
                request_id,
                proposal.graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                error,
            );
        }
    };
    if let Some(error) = response.native_error {
        return Response::err(request_id, error);
    }
    let Some(result) = response.native_result else {
        return Response::err(
            request_id,
            "replicated native command returned no deterministic result",
        );
    };
    coordinate_native_result(proposal, coordination, multi, routed, result).await
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
    let coordination = native_coordination_for(&method);
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
    let (graph_name, multi, graph_type) =
        resolve_native_proposal_route(state, request_graph, &authority, &method, &command).await;
    let Some(multi) = multi else {
        return Response::err(
            request_id,
            crate::server::state::MISSING_PLACEMENT_AUTHORITY,
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
    let proposal = NativeProposal {
        state,
        request_id,
        authority: &authority,
        server_secret: &server_secret,
        graph_name: &graph_name,
        graph_type,
    };
    let request =
        match build_native_raft_request(&proposal, &routed, &method, command, identity_bootstrap) {
            Ok(request) => request,
            Err(response) => return response,
        };
    dispatch_native_raft_write(&proposal, coordination, multi, routed, request).await
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

/// The target group must answer a job-publication commit with exactly
/// `Bool(true)`; every other shape is a refusal the coordinator reports rather
/// than finalizing over.
#[cfg(all(feature = "raft", feature = "jobs"))]
fn interpret_job_publication_commit(
    request_id: u64,
    outcome: Result<ResultPayload, String>,
) -> Result<(), Response> {
    match outcome {
        Ok(ResultPayload::Bool(true)) => Ok(()),
        Ok(ResultPayload::Bool(false)) => Err(Response::err(
            request_id,
            "job publication target rejected commit",
        )),
        Ok(_) => Err(Response::err(
            request_id,
            "job publication target returned invalid result",
        )),
        Err(error) => Err(Response::err(
            request_id,
            format!("job publication target commit failed: {error}"),
        )),
    }
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
    if let Err(response) = interpret_job_publication_commit(
        request_id,
        submit_consensus_job_publication_command(
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
        .await,
    ) {
        return response;
    }

    let finalize = match crate::raft::NativeMutationCommand::job_publication_finalize(
        prepared.coordinator_id.clone(),
        &finalize_receipt,
        server_secret,
    ) {
        Ok(command) => command,
        Err(error) => return Response::err(request_id, error),
    };
    finalize_consensus_job_publication(
        &JobPublicationControl {
            multi: &multi,
            authority,
            request_id,
            control: &control,
            control_graph,
            control_graph_type,
        },
        &prepared.coordinator_id,
        finalize,
    )
    .await
}

/// The scheduler's control-group route for a job publication's finalize record.
#[cfg(all(feature = "raft", feature = "jobs"))]
struct JobPublicationControl<'a> {
    multi: &'a Arc<crate::raft::multi::MultiRaft>,
    authority: &'a CarrierAuthority,
    request_id: u64,
    control: &'a crate::raft::multi::RoutedRaftHandle,
    control_graph: &'a str,
    control_graph_type: crate::protocol::GraphType,
}

/// Record the finalize half on the scheduler's control group, after the target
/// group has already durably committed.
#[cfg(all(feature = "raft", feature = "jobs"))]
async fn finalize_consensus_job_publication(
    control: &JobPublicationControl<'_>,
    coordinator_id: &str,
    finalize: crate::raft::NativeMutationCommand,
) -> Response {
    let request_id = control.request_id;
    let control_fence = control
        .control
        .placed
        .then_some(control.control.fencing_token());
    match submit_consensus_job_publication_command(
        control.multi,
        control.authority,
        request_id,
        coordinator_id,
        "scheduler-finalize",
        control.control_graph,
        control.control_graph_type,
        control.control.group_id,
        control.control.epoch,
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

/// The control-plane consensus route a coordinator drives its decision, abort
/// and finalize records through, together with the identity every submission is
/// signed under. Bundled so each phase helper below stays inside the parameter
/// cap while still seeing the whole coordination context.
#[cfg(feature = "raft")]
struct TransactionCoordination<'a> {
    multi: &'a Arc<crate::raft::multi::MultiRaft>,
    authority: &'a CarrierAuthority,
    request_id: u64,
    server_secret: &'a str,
    control_group: crate::raft::GroupId,
    control_epoch: u64,
    control_fence: Option<u64>,
    control_graph_type: crate::protocol::GraphType,
}

#[cfg(feature = "raft")]
impl TransactionCoordination<'_> {
    async fn abort(
        &self,
        fanout: &handlers::txn::ConsensusTransactionFanout,
    ) -> Result<bool, String> {
        abort_consensus_transaction(
            self.multi,
            self.authority,
            self.request_id,
            self.server_secret,
            &fanout.coordinator_id,
            &fanout.participants,
            self.control_group,
            self.control_epoch,
            self.control_fence,
            self.control_graph_type,
        )
        .await
    }
}

/// Abort after a failed prepare, and turn the abort's OWN outcome into the
/// response the caller returns. `prepare_error` is `None` for a clean refusal
/// (the transaction simply did not commit) and `Some` for a submission failure;
/// the two cases report differently, exactly as the inline arms did.
#[cfg(feature = "raft")]
async fn resolve_failed_consensus_prepare(
    coordination: &TransactionCoordination<'_>,
    fanout: &handlers::txn::ConsensusTransactionFanout,
    prepare_error: Option<String>,
) -> Response {
    let request_id = coordination.request_id;
    match (coordination.abort(fanout).await, prepare_error) {
        (Ok(false), None) => Response::ok(request_id, ResultPayload::Bool(false)),
        (Ok(false), Some(error)) => Response::err(
            request_id,
            format!("consensus participant prepare failed: {error}"),
        ),
        (Ok(true), _) => Response::err(request_id, "consensus abort finalized as commit"),
        (Err(cleanup_error), None) => Response::err(request_id, cleanup_error),
        (Err(cleanup_error), Some(error)) => Response::err(
            request_id,
            format!("consensus participant prepare failed: {error}; abort failed: {cleanup_error}"),
        ),
    }
}

/// Phase 1: prepare every participant. Any refusal or submission failure aborts
/// the transaction and yields the caller's response.
#[cfg(feature = "raft")]
async fn prepare_consensus_participants(
    coordination: &TransactionCoordination<'_>,
    fanout: &handlers::txn::ConsensusTransactionFanout,
) -> Result<(), Response> {
    let request_id = coordination.request_id;
    for participant in &fanout.participants {
        let command = match crate::raft::NativeMutationCommand::transaction_participant(
            crate::raft::TransactionParticipantPhase::Prepare,
            fanout.coordinator_id.clone(),
            participant.participant_id,
            Some(&participant.sealed_plan_source),
            coordination.server_secret,
        ) {
            Ok(command) => command,
            Err(error) => return Err(Response::err(request_id, error)),
        };
        let operation = format!("prepare-{}", participant.participant_id);
        let submitted = submit_consensus_transaction_command(
            coordination.multi,
            coordination.authority,
            request_id,
            &fanout.coordinator_id,
            &operation,
            participant.group_id,
            participant.placement_epoch,
            participant.fencing_token,
            participant.graph_type,
            command,
        )
        .await;
        match submitted {
            Ok(true) => {}
            Ok(false) => {
                return Err(resolve_failed_consensus_prepare(coordination, fanout, None).await)
            }
            Err(error) => {
                return Err(
                    resolve_failed_consensus_prepare(coordination, fanout, Some(error)).await,
                )
            }
        }
    }
    Ok(())
}

/// Phase 2: record the COMMIT decision on the control group.
#[cfg(feature = "raft")]
async fn decide_consensus_commit(
    coordination: &TransactionCoordination<'_>,
    fanout: &handlers::txn::ConsensusTransactionFanout,
) -> Result<(), Response> {
    let request_id = coordination.request_id;
    let decided = submit_consensus_transaction_decision(
        coordination.multi,
        coordination.authority,
        request_id,
        &fanout.coordinator_id,
        coordination.control_group,
        coordination.control_epoch,
        coordination.control_fence,
        coordination.control_graph_type,
        true,
    )
    .await;
    let decision_error = match decided {
        Ok(true) => return Ok(()),
        Ok(false) => {
            return Err(Response::err(
                request_id,
                "consensus transaction was durably aborted",
            ))
        }
        Err(error) => error,
    };
    // A prior retry may already have decided ABORT. Conversely, if the
    // COMMIT reply was merely lost, the abort decision will conflict and
    // preserve the durable COMMIT. Either way this cleanup cannot reverse a
    // recorded outcome.
    Err(match coordination.abort(fanout).await {
        Ok(false) => Response::err(
            request_id,
            format!("consensus transaction was durably aborted: {decision_error}"),
        ),
        Ok(true) => Response::err(request_id, "consensus abort finalized as commit"),
        Err(cleanup_error) => Response::err(
            request_id,
            format!(
                "consensus decision failed: {decision_error}; resolution failed: {cleanup_error}"
            ),
        ),
    })
}

/// Phase 3: drive every participant to COMMIT. The decision is already durable,
/// so a failure here is reported for retry rather than aborted.
#[cfg(feature = "raft")]
async fn commit_consensus_participants(
    coordination: &TransactionCoordination<'_>,
    fanout: &handlers::txn::ConsensusTransactionFanout,
) -> Result<(), Response> {
    let request_id = coordination.request_id;
    for participant in &fanout.participants {
        let command = match crate::raft::NativeMutationCommand::transaction_participant(
            crate::raft::TransactionParticipantPhase::Commit,
            fanout.coordinator_id.clone(),
            participant.participant_id,
            Some(&participant.sealed_plan_source),
            coordination.server_secret,
        ) {
            Ok(command) => command,
            Err(error) => return Err(Response::err(request_id, error)),
        };
        let operation = format!("commit-{}", participant.participant_id);
        match submit_consensus_transaction_command(
            coordination.multi,
            coordination.authority,
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
                return Err(Response::err(
                    request_id,
                    "decided consensus participant did not commit; retry will resume",
                ))
            }
            Err(error) => {
                return Err(Response::err(
                    request_id,
                    format!("decided consensus participant commit failed: {error}"),
                ))
            }
        }
    }
    Ok(())
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
    let coordination = TransactionCoordination {
        multi: &multi,
        authority,
        request_id,
        server_secret,
        control_group: control.group_id,
        control_epoch: control.epoch,
        control_fence: control.placed.then_some(control.group_id),
        control_graph_type,
    };
    if let Err(response) = prepare_consensus_participants(&coordination, &fanout).await {
        return response;
    }
    if let Err(response) = decide_consensus_commit(&coordination, &fanout).await {
        return response;
    }
    if let Err(response) = commit_consensus_participants(&coordination, &fanout).await {
        return response;
    }
    match submit_consensus_transaction_finalize(
        coordination.multi,
        coordination.authority,
        request_id,
        &fanout.coordinator_id,
        coordination.control_group,
        coordination.control_epoch,
        coordination.control_fence,
        coordination.control_graph_type,
        true,
    )
    .await
    {
        Ok(true) => Response::ok(request_id, ResultPayload::Bool(true)),
        Ok(false) => Response::err(request_id, "consensus transaction finalized as abort"),
        Err(error) => Response::err(request_id, error),
    }
}

/// Transaction control, identity registration and multisig mutation: the three
/// proposals whose payload must be re-signed/re-anchored against the caller's
/// ORIGINAL request graph before routing rewrites `RaftRequest.graph_name`.
/// Anything else is handed back untouched.
#[cfg(feature = "raft")]
fn sanitize_authored_proposal(
    request_graph: &str,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Result<Method, String> {
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
        other => Ok(other),
    }
}

/// The tenant in a replicated resource body is a correlation, not an authority
/// claim: it must be the verified tenant unless the caller is an admin. The
/// reservation and host surfaces report distinct refusals.
#[cfg(feature = "raft")]
fn resource_tenant_denial(
    method: &Method,
    verified_context: &VerifiedRequestContext,
    authority: &CarrierAuthority,
) -> Option<&'static str> {
    let (tenant_ref, denial) = match method {
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => (
            request.tenant_ref.as_str(),
            "ACCESS_DENIED: replicated resource tenant is not the verified tenant",
        ),
        Method::UpdateResourceHost { request } => (
            request.tenant_ref.as_str(),
            "ACCESS_DENIED: replicated resource host tenant is not the verified tenant",
        ),
        _ => return None,
    };
    (tenant_ref != verified_context.tenant() && !authority.is_admin()).then_some(denial)
}

#[cfg(feature = "raft")]
fn sanitize_resource_proposal(
    verified_context: &VerifiedRequestContext,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Method, String> {
    match resource_tenant_denial(&method, verified_context, authority) {
        Some(denial) => Err(denial.to_string()),
        None => Ok(method),
    }
}

/// Channel/messaging proposals name their own actor. The caller's claimed actor
/// must be the caller, and the REPLICATED copy carries the actor scope rather
/// than the display agent id, so replay is stable across identity renames.
#[cfg(feature = "raft")]
fn sanitize_channel_proposal(
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Method, String> {
    match method {
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
    // The three groups own disjoint `Method` variants, so each hands an
    // unrecognised method straight through and the chain order is immaterial.
    let method = sanitize_authored_proposal(request_graph, verified_context, method)?;
    let method = sanitize_resource_proposal(verified_context, authority, method)?;
    sanitize_channel_proposal(authority, method)
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
    if !submit_context_matches_authority(context, verified_context) {
        return Err("SubmitWorkItem context does not match verified request authority".to_string());
    }
    if !submit_context_within_carrier_bounds(context, verified_context) {
        return Err("SubmitWorkItem context violates the verified carrier bounds".to_string());
    }
    Ok(())
}

fn submit_context_matches_authority(
    context: &crate::epistemic_operations::RequestContext,
    verified_context: &VerifiedRequestContext,
) -> bool {
    context.tenant_id == verified_context.tenant()
        && context.agent_id == verified_context.agent_id()
        && context.audience == verified_context.claims().audience
        && context.policy_version == verified_context.claims().policy_version
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
fn preflight_request_msgpack(method: &Method) -> Result<(), &'static str> {
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

fn append_native_capacity_ops(ops: &mut Vec<&'static str>, available: bool) {
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

fn append_native_work_item_ops(ops: &mut Vec<&'static str>, available: bool) {
    if available {
        ops.extend(["SubmitWorkItem", "SubmitWorkItems"]);
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

#[cfg(feature = "cost")]
async fn dispatch_resource_stats(
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
        Ok(snapshot) => match serde_json::to_value(&snapshot) {
            Ok(value) => Response::ok(req_id, ResultPayload::Json(value)),
            Err(error) => Response::err(req_id, format!("ResourceStats serialization: {error}")),
        },
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
fn dispatch_boxed<'a>(
    fut: impl std::future::Future<Output = Response> + Send + 'a,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
    Box::pin(fut)
}

async fn dispatch_case_02_health(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Response {
    match method {
        Method::Health => {
            let (
                lifecycle,
                native_resource_ops_available,
                native_capacity_ops_available,
                native_work_item_ops_available,
            ) = {
                let state = timed_read(state).await;
                let manifests = state.registry.materialization_manifests();
                let complete = manifests.iter().filter(|manifest| manifest.valid).count();
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
                    state
                        .persistence
                        .as_ref()
                        .is_some_and(|backend| backend.supports_native_resource_reservations()),
                    state
                        .persistence
                        .as_ref()
                        .is_some_and(|backend| backend.supports_native_capacity_leases()),
                    state
                        .persistence
                        .as_ref()
                        .is_some_and(|backend| backend.supports_native_work_item_submission()),
                )
            };
            let uptime_s = 0; // you can capture start time in ServerState
            let mem_bytes = 0;
            let mut served_ops = vec![
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
            #[cfg(feature = "cost")]
            served_ops.extend(["ResourceStats", "ResourceStatsPage"]);
            append_native_resource_ops(&mut served_ops, native_resource_ops_available);
            append_native_capacity_ops(&mut served_ops, native_capacity_ops_available);
            append_native_work_item_ops(&mut served_ops, native_work_item_ops_available);
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
                req_id,
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
        _ => unreachable!("dispatch_case_02_health: classifier/handler diverged"),
    }
}

async fn dispatch_case_04_parse_file(req_id: u64, method: Method) -> Response {
    match method {
        Method::ParseFile { file_path, source } => {
            #[cfg(feature = "ast")]
            let input_check = validate_ast_logical_path(&file_path).and_then(|()| {
                let limits = ast_input_limits();
                if source.len() > limits.max_source_bytes || source.len() > limits.max_total_bytes {
                    Err("AST_INPUT_LIMIT: source content exceeds the configured limit".to_string())
                } else {
                    Ok(())
                }
            });
            #[cfg(feature = "ast")]
            match input_check
                .and_then(|()| crate::parser::tree_sitter::parse_file(&file_path, &source))
            {
                Ok(result) => match serde_json::to_value(&result) {
                    Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req_id, format!("Serialization error: {}", e)),
                },
                Err(e) => Response::err(req_id, e),
            }
            #[cfg(not(feature = "ast"))]
            {
                let _ = (file_path, source);
                Response::err(req_id, "AST feature not enabled".to_string())
            }
        }
        _ => unreachable!("dispatch_case_04_parse_file: classifier/handler diverged"),
    }
}

async fn dispatch_case_05_parse_files(req_id: u64, method: Method) -> Response {
    match method {
        Method::ParseFiles { files_msgpack } => {
            #[cfg(feature = "ast")]
            {
                let owned = match decode_ast_files(&files_msgpack, ast_input_limits()) {
                    Ok(files) => files,
                    Err(error) => return Response::err(req_id, error),
                };
                // Parse on the blocking pool, NOT the async reactor: parse_files is
                // CPU-bound (rayon tree-sitter over every file) and a large batch
                // would otherwise stall the runtime thread, blocking unrelated
                // requests until it finishes. (CONCEPT:EG-KG.compute.off-reactor-dispatch — work off-reactor, A4)
                let results = match compute_off_lock(req_id, move || {
                    crate::parser::tree_sitter::parse_files(&owned)
                })
                .await
                {
                    Ok(r) => r,
                    Err(resp) => return resp,
                };
                match serde_json::to_value(&results) {
                    Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req_id, format!("Serialization error: {}", e)),
                }
            }
            #[cfg(not(feature = "ast"))]
            {
                let _ = files_msgpack;
                Response::err(req_id, "AST feature not enabled".to_string())
            }
        }
        _ => unreachable!("dispatch_case_05_parse_files: classifier/handler diverged"),
    }
}

async fn dispatch_case_06_index_repository(req_id: u64, method: Method) -> Response {
    match method {
        Method::IndexRepository { files_msgpack } => {
            #[cfg(feature = "ast")]
            {
                // Same canonical blob shape as ParseFiles, but parsed AND
                // cross-file-resolved into one IndexResult.
                let owned = match decode_ast_files(&files_msgpack, ast_input_limits()) {
                    Ok(files) => files,
                    Err(error) => return Response::err(req_id, error),
                };
                // Off-reactor like ParseFiles: parse (rayon) + resolution are
                // CPU-bound over the whole batch. (CONCEPT:EG-KG.compute.turn-each-project)
                let result = match compute_off_lock(req_id, move || {
                    crate::parser::resolve::index_repository(&owned)
                })
                .await
                {
                    Ok(r) => r,
                    Err(resp) => return resp,
                };
                match serde_json::to_value(&result) {
                    Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req_id, format!("Serialization error: {}", e)),
                }
            }
            #[cfg(not(feature = "ast"))]
            {
                let _ = files_msgpack;
                Response::err(req_id, "AST feature not enabled".to_string())
            }
        }
        _ => unreachable!("dispatch_case_06_index_repository: classifier/handler diverged"),
    }
}

async fn dispatch_case_07_observe_screen(req_id: u64, method: Method) -> Response {
    match method {
        Method::ObserveScreen { obs_msgpack } => {
            // MessagePack map → a captured desktop frame. png rides as a bin field;
            // elements are the AT-SPI accessibles. (CONCEPT:AU-KG.ontology.owl-screen-bridge)
            let input = match decode_screen_observation(&obs_msgpack) {
                Ok(input) => input,
                Err(error) => return Response::err(req_id, error),
            };
            // Inline: PNG hashing + node/edge build over the element set is
            // microsecond-cheap (no AST parse), so it doesn't need the blocking pool.
            let result = crate::screen::observe_screen(&input);
            match serde_json::to_value(&result) {
                Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                Err(e) => Response::err(req_id, format!("Serialization error: {}", e)),
            }
        }
        _ => unreachable!("dispatch_case_07_observe_screen: classifier/handler diverged"),
    }
}

async fn dispatch_case_08_shutdown(req_id: u64, method: Method) -> Response {
    match method {
        Method::Shutdown => {
            info!("Shutdown requested via protocol");
            Response::ok(req_id, ResultPayload::String("shutting_down".to_string()))
        }
        _ => unreachable!("dispatch_case_08_shutdown: classifier/handler diverged"),
    }
}

#[cfg(feature = "cost")]
async fn dispatch_case_09_resource_stats(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ResourceStats => {
            dispatch_resource_stats(
                state,
                req_id,
                verified_context,
                crate::cost::ResourceStatsRequest::bounded_default(),
            )
            .await
        }
        _ => unreachable!("dispatch_case_09_resource_stats: classifier/handler diverged"),
    }
}

#[cfg(feature = "cost")]
async fn dispatch_case_10_resource_stats_page(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ResourceStatsPage {
            cursor,
            limit,
            summary,
        } => {
            dispatch_resource_stats(
                state,
                req_id,
                verified_context,
                crate::cost::ResourceStatsRequest {
                    cursor,
                    limit,
                    summary,
                },
            )
            .await
        }
        _ => unreachable!("dispatch_case_10_resource_stats_page: classifier/handler diverged"),
    }
}

/// A `CreateGraph` that finds the graph already resident is either a retry of a
/// durably-committed create (idempotent success) or a genuine collision.
async fn reconcile_existing_graph_create(
    backend: &Arc<dyn PersistenceBackend>,
    graph_name: &str,
    req_id: u64,
    created_result: ResultPayload,
) -> Response {
    match crate::server::mutation_batch::lifecycle_was_committed(
        backend, "create", graph_name, req_id,
    )
    .await
    {
        Ok(true) => Response::ok(req_id, created_result),
        Ok(false) => Response::err(req_id, format!("Graph '{graph_name}' already exists")),
        Err(e) => Response::err(
            req_id,
            format!("durable graph-create reconciliation failed: {e}"),
        ),
    }
}

/// The authoritative version the durable lifecycle commit published. Anything
/// other than a positive version means the registry must not publish this
/// incarnation.
async fn read_committed_graph_version(
    backend: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    req_id: u64,
) -> Result<u64, Response> {
    match backend.read_mutation_graph_version(graph_fname).await {
        Ok(Some(version)) if version > 0 => Ok(version),
        Ok(Some(_)) => Err(Response::err(
            req_id,
            "durable graph registration published an invalid zero version",
        )),
        Ok(None) => Err(Response::err(
            req_id,
            "durable graph registration published no authoritative version",
        )),
        Err(error) => Err(Response::err(
            req_id,
            format!("durable graph version read failed: {error}"),
        )),
    }
}

async fn dispatch_case_11_create_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    method: Method,
) -> Response {
    let Method::CreateGraph {
        graph_name,
        graph_type,
    } = method
    else {
        unreachable!("dispatch_case_11_create_graph: classifier/handler diverged")
    };
    // Lifecycle shares the same per-graph serialization lane as ordinary
    // MutationBatch/txn writes.  The durable identity must land before the
    // registry publishes this incarnation.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(&graph_name).await;
    let (backend, already_exists) = {
        let s = timed_read(state).await;
        (s.persistence.clone(), s.registry.exists(&graph_name))
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "graph creation requires durable persistence");
    };
    let created_result = ResultPayload::Json(serde_json::json!({
        "created": graph_name.clone()
    }));
    let incarnation_id =
        crate::server::mutation_batch::lifecycle_batch_id("create", &graph_name, req_id);
    if already_exists {
        return reconcile_existing_graph_create(&backend, &graph_name, req_id, created_result)
            .await;
    }
    if let Err(e) = crate::server::mutation_batch::commit_lifecycle(
        &backend,
        "create",
        req_id,
        req_agent_id.as_deref(),
        &graph_name,
        Method::CreateGraph {
            graph_name: graph_name.clone(),
            graph_type,
        },
        &created_result,
    )
    .await
    {
        return Response::err(req_id, format!("durable graph registration failed: {e}"));
    }
    let graph_fname = crate::persist::sanitize(&graph_name);
    let committed_version = match read_committed_graph_version(&backend, &graph_fname, req_id).await
    {
        Ok(version) => version,
        Err(response) => return response,
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
    match s.registry.create_graph_with_incarnation(
        &graph_name,
        graph_type,
        req_agent_id.clone(),
        incarnation_id,
        committed_version,
    ) {
        Ok(()) => {
            crate::metrics::set_graph_size(&graph_name, 0, 0);
            // Close the CreateGraph RBAC-provisioning gap (P0 tenant-graph
            // fix): a tenant graph's `owner` field is otherwise dead under
            // the mandatory `security` build (`check_access` ignores
            // `graph_owner`), so nothing has ever made a freshly-created
            // tenant graph durably readable/writable by an ordinary
            // registered principal. Idempotent; a failure here must never
            // fail graph creation itself (the graph is already durably
            // committed) — it is logged and self-heals on the next
            // `CreateGraph` for a sibling graph of the same tenant, or the
            // deployment-time remediation pass. Security-only (mirrors the
            // `Method::RbacAdmin` precedent above): a non-`security` build
            // decides graph access via `check_access`'s owner-based ACL
            // branch directly, so there is no RBAC store here to provision.
            #[cfg(feature = "security")]
            if let Err(error) = s
                .isolation
                .provision_tenant_graph_access(&graph_name, req_agent_id.as_deref())
            {
                tracing::warn!(
                    graph = %graph_name,
                    %error,
                    "tenant graph RBAC auto-provisioning failed after graph creation \
                     committed; the graph exists but may still be unreadable for \
                     non-System principals until this is retried"
                );
            }
            Response::ok(req_id, created_result)
        }
        Err(e) => Response::err(req_id, e),
    }
}

/// A `DeleteGraph` that finds nothing in the catalog is either a retry of a
/// durably-committed delete (idempotent success) or a genuine miss.
async fn reconcile_missing_graph_delete(
    backend: &Arc<dyn PersistenceBackend>,
    graph_name: &str,
    req_id: u64,
    deleted_result: ResultPayload,
) -> Response {
    match crate::server::mutation_batch::lifecycle_was_committed(
        backend, "delete", graph_name, req_id,
    )
    .await
    {
        Ok(true) => Response::ok(req_id, deleted_result),
        Ok(false) => Response::err(req_id, format!("Graph '{graph_name}' not found")),
        Err(e) => Response::err(
            req_id,
            format!("durable graph-delete reconciliation failed: {e}"),
        ),
    }
}

async fn dispatch_case_12_delete_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    state_machine_authorized: bool,
    method: Method,
) -> Response {
    let Method::DeleteGraph { ref graph_name } = method else {
        unreachable!("dispatch_case_12_delete_graph: classifier/handler diverged")
    };
    // Fence gateway/txn writes for this graph across durable purge and RAM
    // teardown.  A retry after a crash at that boundary reconciles from the
    // durable batch record.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    let (backend, exists, denied) = {
        let s = timed_read(state).await;
        let record = s.registry.catalog_record(graph_name);
        // Read-lock half of the access gate; `teardown_deleted_graph_in_memory`
        // re-checks under the write lock after the durable purge commits.
        let denied = if state_machine_authorized {
            None
        } else {
            record.as_ref().and_then(|record| {
                check_graph_access(
                    &s.isolation,
                    req_agent_id.as_deref(),
                    graph_name,
                    record.graph_type,
                    record.owner.as_deref(),
                    AccessLevel::Write,
                )
                .err()
            })
        };
        (s.persistence.clone(), record.is_some(), denied)
    };
    if let Some(denied) = denied {
        return Response::err(req_id, denied);
    }
    let Some(backend) = backend else {
        return Response::err(req_id, "graph deletion requires durable persistence");
    };
    let deleted_result = ResultPayload::Json(serde_json::json!({
        "deleted": graph_name
    }));
    if !exists {
        return reconcile_missing_graph_delete(&backend, graph_name, req_id, deleted_result).await;
    }
    if let Err(e) = crate::server::mutation_batch::commit_lifecycle(
        &backend,
        "delete",
        req_id,
        req_agent_id.as_deref(),
        graph_name,
        Method::DeleteGraph {
            graph_name: graph_name.clone(),
        },
        &deleted_result,
    )
    .await
    {
        return Response::err(req_id, format!("durable graph purge failed: {e}"));
    }

    teardown_deleted_graph_in_memory(
        state,
        req_id,
        req_agent_id.as_deref(),
        graph_name,
        state_machine_authorized,
        deleted_result,
    )
    .await
}

/// The in-memory teardown half of `DeleteGraph`, after the durable purge has
/// already committed: re-checks graph access under the write lock (the
/// durable-purge check above ran under a read lock, released before the
/// write-lock re-acquire here), removes the registry entry, and forgets every
/// per-graph-NAME-keyed piece of `ServerState` that would otherwise survive
/// a same-name recreate and shadow the new incarnation (write-coalescer
/// cached writer, routed-write-coalescer, per-graph in-flight semaphore,
/// cold-tenant tracker mark) -- see the inline comments at each `.remove()`/
/// `.forget()` call for why each one specifically matters (this is the
/// tenant-churn-corruption fix's own documentation, preserved verbatim).
async fn teardown_deleted_graph_in_memory(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<&str>,
    graph_name: &str,
    state_machine_authorized: bool,
    deleted_result: ResultPayload,
) -> Response {
    let mut s = timed_write(state).await;
    if !state_machine_authorized {
        if let Some(entry) = s.registry.catalog_record(graph_name) {
            if let Err(denied) = check_graph_access(
                &s.isolation,
                req_agent_id,
                graph_name,
                entry.graph_type,
                entry.owner.as_deref(),
                AccessLevel::Write,
            ) {
                return Response::err(req_id, denied);
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
            Response::ok(req_id, deleted_result)
        }
        Err(e) => Response::err(req_id, e),
    }
}

/// Stable wire names for the server index manifest. Kept as their own
/// mappings so the manifest projection below stays a single expression.
fn index_kind_label(kind: crate::index::IndexKind) -> &'static str {
    match kind {
        crate::index::IndexKind::Text => "text",
        crate::index::IndexKind::Temporal => "temporal",
        crate::index::IndexKind::DerivedOwl => "derived_owl",
        crate::index::IndexKind::Spatial => "spatial",
        crate::index::IndexKind::Label => "label",
        crate::index::IndexKind::Property => "property",
        crate::index::IndexKind::Ontology => "ontology",
        crate::index::IndexKind::Vector => "vector",
    }
}

fn index_validity_label(validity: crate::index::IndexValidity) -> &'static str {
    match validity {
        crate::index::IndexValidity::Building => "building",
        crate::index::IndexValidity::Valid => "valid",
        crate::index::IndexValidity::Stale => "stale",
        crate::index::IndexValidity::Failed => "failed",
    }
}

async fn dispatch_case_13_list_graphs(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ListGraphs => {
            let s = timed_read(state).await;
            let read_authority =
                match GraphReadAuthority::from_verified(verified_context, &s.isolation) {
                    Ok(authority) => authority,
                    Err(denied) => return Response::err(req_id, denied),
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
                                serde_json::json!({
                                    "kind": index_kind_label(kind),
                                    "source_snapshot_version": manifest.source_snapshot_version,
                                    "build_version": manifest.build_version,
                                    "completeness_cursor": {
                                        "nodes": manifest.completeness.nodes,
                                        "edges": manifest.completeness.edges,
                                        "complete": manifest.completeness.complete,
                                    },
                                    "validity": index_validity_label(manifest.validity),
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
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(graphs)))
        }
        _ => unreachable!("dispatch_case_13_list_graphs: classifier/handler diverged"),
    }
}

async fn dispatch_case_14_reshard(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    method: Method,
) -> Response {
    match method {
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
                req_id,
                req_agent_id.as_deref(),
                method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is an admin method.
                Err(_) => Response::err(req_id, "admin dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_14_reshard: classifier/handler diverged"),
    }
}

async fn dispatch_case_15_placement_route(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Response {
    match method {
        Method::PlacementRoute { .. } | Method::PlacementAdmin { .. } => {
            match handlers::placement::try_handle(state, req_id, method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a placement method.
                Err(_) => Response::err(req_id, "placement dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_15_placement_route: classifier/handler diverged"),
    }
}

async fn dispatch_case_16_raft_add_learner(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Response {
    match method {
        Method::RaftAddLearner { .. } | Method::RaftChangeMembership { .. } => {
            match handlers::raft_admin::try_handle(state, req_id, method).await {
                Ok(resp) => resp,
                // Unreachable: both variants matched above are raft-admin methods.
                Err(_) => Response::err(req_id, "raft-admin dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_16_raft_add_learner: classifier/handler diverged"),
    }
}

async fn dispatch_case_17_cluster_members(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ClusterMembers | Method::NodeInfoUpsert { .. } => {
            match handlers::topology::try_handle(state, req_id, method, verified_context).await {
                Ok(resp) => resp,
                // Unreachable: both variants matched above are topology methods.
                Err(_) => Response::err(req_id, "cluster topology dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_17_cluster_members: classifier/handler diverged"),
    }
}

async fn dispatch_case_18_register_server(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::RegisterServer {
            name,
            url,
            resources_json,
            ttl_secs,
        } => {
            handle_register_server(
                state,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                name,
                url,
                resources_json,
                ttl_secs,
            )
            .await
        }
        _ => unreachable!("dispatch_case_18_register_server: classifier/handler diverged"),
    }
}

async fn dispatch_case_19_create_channel(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::CreateChannel {
            channel_id,
            channel_type,
            creator,
            initial_members,
        } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            if creator != carrier.agent_id() {
                return Response::err(req_id, "ACCESS_DENIED: channel creator must be caller");
            }
            let mut s = timed_write(state).await;
            match s.channels.create_channel_scoped(
                &channel_id,
                carrier.tenant_scope(),
                channel_type,
                carrier.agent_id(),
                initial_members,
            ) {
                Ok(()) => Response::ok(
                    req_id,
                    ResultPayload::Json(serde_json::json!({"channel": channel_id})),
                ),
                Err(e) => Response::err(req_id, e),
            }
        }
        _ => unreachable!("dispatch_case_19_create_channel: classifier/handler diverged"),
    }
}

async fn dispatch_case_20_join_channel(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::JoinChannel {
            channel_id,
            agent_id,
        } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            if agent_id != carrier.agent_id() {
                return Response::err(req_id, "ACCESS_DENIED: channel join actor must be caller");
            }
            let mut s = timed_write(state).await;
            if let Err(error) = s
                .channels
                .authorize_tenant(&channel_id, carrier.tenant_scope())
            {
                return Response::err(req_id, error);
            }
            match s.channels.join_channel(&channel_id, carrier.agent_id()) {
                Ok(()) => Response::ok(req_id, ResultPayload::String("joined".to_string())),
                Err(e) => Response::err(req_id, e),
            }
        }
        _ => unreachable!("dispatch_case_20_join_channel: classifier/handler diverged"),
    }
}

async fn dispatch_case_21_leave_channel(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::LeaveChannel {
            channel_id,
            agent_id,
        } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            if agent_id != carrier.agent_id() {
                return Response::err(req_id, "ACCESS_DENIED: channel leave actor must be caller");
            }
            let mut s = timed_write(state).await;
            if let Err(error) =
                s.channels
                    .authorize_member(&channel_id, carrier.tenant_scope(), carrier.agent_id())
            {
                return Response::err(req_id, error);
            }
            match s.channels.leave_channel(&channel_id, carrier.agent_id()) {
                Ok(imprint) => {
                    let val = match imprint {
                        Some(imp) => {
                            serde_json::to_value(&imp).unwrap_or(serde_json::json!("closed"))
                        }
                        None => serde_json::json!("left"),
                    };
                    Response::ok(req_id, ResultPayload::Json(val))
                }
                Err(e) => Response::err(req_id, e),
            }
        }
        _ => unreachable!("dispatch_case_21_leave_channel: classifier/handler diverged"),
    }
}

async fn dispatch_case_22_close_channel(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::CloseChannel {
            channel_id,
            summary_embedding,
            topic_metadata,
        } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            let mut s = timed_write(state).await;
            if let Err(error) = s.channels.authorize_creator(
                &channel_id,
                carrier.tenant_scope(),
                carrier.agent_id(),
            ) {
                return Response::err(req_id, error);
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
                    Response::ok(req_id, ResultPayload::Json(val))
                }
                Err(e) => Response::err(req_id, e),
            }
        }
        _ => unreachable!("dispatch_case_22_close_channel: classifier/handler diverged"),
    }
}

async fn dispatch_case_23_send_message(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::SendMessage {
            channel_id,
            sender,
            payload,
        } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            if sender != carrier.agent_id() {
                return Response::err(req_id, "ACCESS_DENIED: channel sender must be caller");
            }
            let mut s = timed_write(state).await;
            if let Err(error) =
                s.channels
                    .authorize_member(&channel_id, carrier.tenant_scope(), carrier.agent_id())
            {
                return Response::err(req_id, error);
            }
            match s
                .channels
                .send_message(&channel_id, carrier.agent_id(), &payload)
            {
                Ok(()) => Response::ok(req_id, ResultPayload::String("sent".to_string())),
                Err(e) => Response::err(req_id, e),
            }
        }
        _ => unreachable!("dispatch_case_23_send_message: classifier/handler diverged"),
    }
}

async fn dispatch_case_24_get_channel_messages(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::GetChannelMessages { channel_id, limit } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            let s = timed_read(state).await;
            if let Err(error) =
                s.channels
                    .authorize_member(&channel_id, carrier.tenant_scope(), carrier.agent_id())
            {
                return Response::err(req_id, error);
            }
            match s.channels.get_messages(&channel_id, limit) {
                Ok(msgs) => {
                    let val: Vec<serde_json::Value> = msgs.iter().map(|m| {
                        serde_json::json!({"sender": m.sender, "payload": m.payload, "timestamp": m.timestamp})
                    }).collect();
                    Response::ok(req_id, ResultPayload::Json(serde_json::json!(val)))
                }
                Err(e) => Response::err(req_id, e),
            }
        }
        _ => unreachable!("dispatch_case_24_get_channel_messages: classifier/handler diverged"),
    }
}

async fn dispatch_case_25_list_channels(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ListChannels => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            let s = timed_read(state).await;
            let channels: Vec<serde_json::Value> = s.channels.list_channels_for(
                carrier.tenant_scope(),
                carrier.agent_id(),
            ).iter().map(|(id, ct, members)| {
                serde_json::json!({"id": id, "type": ct, "members": members})
            }).collect();
            Response::ok(req_id, ResultPayload::Json(serde_json::json!(channels)))
        }
        _ => unreachable!("dispatch_case_25_list_channels: classifier/handler diverged"),
    }
}

async fn dispatch_case_26_get_channel_members(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::GetChannelMembers { channel_id } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            let s = timed_read(state).await;
            if let Err(error) =
                s.channels
                    .authorize_member(&channel_id, carrier.tenant_scope(), carrier.agent_id())
            {
                return Response::err(req_id, error);
            }
            match s.channels.get_members(&channel_id) {
                Ok(members) => {
                    Response::ok(req_id, ResultPayload::Json(serde_json::json!(members)))
                }
                Err(e) => Response::err(req_id, e),
            }
        }
        _ => unreachable!("dispatch_case_26_get_channel_members: classifier/handler diverged"),
    }
}

async fn dispatch_case_27_register_identity(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
    method: Method,
) -> Response {
    match method {
        Method::RegisterIdentity {
            agent_id,
            role,
            teams,
            signature,
            roles,
        } => {
            if !state_machine_authorized {
                if let Err(message) = verify_register_identity_signature(
                    verified_context,
                    &req_graph,
                    &agent_id,
                    &role,
                    &teams,
                    &roles,
                    &signature,
                ) {
                    crate::metrics::auth_failure();
                    return Response::err(req_id, message);
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
                s.isolation.try_register_agent_from_request(identity)
            };
            if let Err(message) = result {
                return Response::err(req_id, message);
            }
            info!("RegisterIdentity committed");
            Response::ok(req_id, ResultPayload::String("registered".to_string()))
        }
        _ => unreachable!("dispatch_case_27_register_identity: classifier/handler diverged"),
    }
}

async fn dispatch_case_28_get_identity(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Response {
    match method {
        Method::GetIdentity { agent_id } => {
            let s = timed_read(state).await;
            // `None` = "no identity registered for `agent_id`" (unknown); `Some(identity)`
            // with an empty `roles` Vec = "registered, confirmed to hold no roles". The two
            // MUST stay distinguishable end-to-end — see `IsolationLayer::get_identity` —
            // so a merge-before-register caller can tell "nothing granted yet" from
            // "already confirmed empty", which is the exact ambiguity this RPC exists to
            // eliminate. `serde_json::to_value` preserves that: `None` serializes to JSON
            // `null`, never to an empty object.
            let identity = s.isolation.get_identity(&agent_id);
            drop(s);
            match serde_json::to_value(&identity) {
                Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                Err(e) => Response::err(req_id, format!("Serialization error: {}", e)),
            }
        }
        _ => unreachable!("dispatch_case_28_get_identity: classifier/handler diverged"),
    }
}

#[cfg(feature = "policy_export")]
async fn dispatch_case_29_policy_export(
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::PolicyExport {
            tenant,
            graphs,
            mut principals,
            marking_names,
        } => {
            let claims = verified_context.claims();
            let caller_roles: Vec<String> = claims
                .roles
                .iter()
                .chain(claims.scopes.iter())
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            principals.insert(claims.principal.clone(), caller_roles);
            let input = crate::server::policy_export::GenerateBundleInput {
                tenant,
                graphs,
                principals,
                marking_names: marking_names
                    .into_iter()
                    .map(|name| crate::server::policy_export::MarkingDef {
                        name,
                        requires_audit: false,
                    })
                    .collect(),
            };
            match crate::server::policy_export::generate_bundle(&input) {
                Ok(bundle) => match serde_json::to_value(&bundle) {
                    Ok(val) => Response::ok(req_id, ResultPayload::Json(val)),
                    Err(e) => Response::err(req_id, format!("Serialization error: {}", e)),
                },
                Err(denied) => Response::err(req_id, denied),
            }
        }
        _ => unreachable!("dispatch_case_29_policy_export: classifier/handler diverged"),
    }
}

/// A unit-returning RBAC admin mutation answers with a fixed acknowledgement
/// string on success and the store's own message on failure.
#[cfg(feature = "security")]
fn rbac_admin_ack(req_id: u64, outcome: Result<(), String>, acknowledgement: &str) -> Response {
    match outcome {
        Ok(()) => Response::ok(req_id, ResultPayload::String(acknowledgement.to_string())),
        Err(message) => Response::err(req_id, message),
    }
}

#[cfg(feature = "security")]
async fn dispatch_case_30_rbac_admin(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Response {
    use crate::acl::RbacAdminOp;
    let Method::RbacAdmin { op } = method else {
        unreachable!("dispatch_case_30_rbac_admin: classifier/handler diverged")
    };
    let mut s = timed_write(state).await;
    match op {
        RbacAdminOp::AddRole(role) => {
            rbac_admin_ack(req_id, s.isolation.try_add_role(role), "role_added")
        }
        RbacAdminOp::RemoveRole(name) => {
            rbac_admin_ack(req_id, s.isolation.try_remove_role(&name), "role_removed")
        }
        RbacAdminOp::AddGrant(grant) => {
            rbac_admin_ack(req_id, s.isolation.try_add_grant(grant), "grant_added")
        }
        RbacAdminOp::RemoveGrant(grant) => match s.isolation.try_remove_grant(&grant) {
            Ok(removed) => Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({ "removed": removed })),
            ),
            Err(message) => Response::err(req_id, message),
        },
        RbacAdminOp::List => {
            let policy = s.isolation.rbac();
            let roles: Vec<_> = policy.roles().cloned().collect();
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({
                    "roles": roles,
                    "grants": policy.grants(),
                })),
            )
        }
    }
}

async fn dispatch_case_31_apply_multisig_mutation(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
    method: Method,
) -> Response {
    match method {
        Method::ApplyMultisigMutation {
            signatures,
            threshold,
            mutation_type,
            query,
        } => {
            if !state_machine_authorized {
                if let Err(message) = verify_multisig_mutation_signatures(
                    verified_context,
                    &req_graph,
                    &signatures,
                    threshold,
                    &mutation_type,
                    &query,
                ) {
                    crate::metrics::auth_failure();
                    return Response::err(req_id, message);
                }
            }
            // Delegate mutation application to the target graph
            dispatch_graph_op(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                Method::ApplyMutation {
                    event_type: mutation_type,
                    query,
                },
            )
            .await
        }
        _ => unreachable!("dispatch_case_31_apply_multisig_mutation: classifier/handler diverged"),
    }
}

#[cfg(feature = "jobs")]
async fn dispatch_case_32_analytics_job(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::AnalyticsJob { op } => {
            // Worker fencing identity and durable actor attribution come from the
            // authenticated context, never from the unsigned request envelope's
            // display/agent field.
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            handlers::jobs::handle(
                state,
                req_id,
                &carrier,
                verified_context.allows_analytics_worker(),
                op,
            )
            .await
        }
        _ => unreachable!("dispatch_case_32_analytics_job: classifier/handler diverged"),
    }
}

#[cfg(feature = "statechart")]
async fn dispatch_case_33_statechart(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::Statechart { op } => {
            // Durable owner attribution comes from the authenticated context, never
            // from the unsigned request envelope's display/agent field.
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            handlers::statechart::handle(state, req_id, &carrier, op).await
        }
        _ => unreachable!("dispatch_case_33_statechart: classifier/handler diverged"),
    }
}

#[cfg(feature = "viz-static-export")]
async fn dispatch_case_36_viz(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::Viz { op } => {
            // No durable owner-scoped state to attribute a render to; the carrier
            // is still resolved for parity with the sibling self-routed handlers
            // (see `handlers::viz::handle`'s doc).
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            handlers::viz::handle(state, req_id, &carrier, op).await
        }
        _ => unreachable!("dispatch_case_36_viz: classifier/handler diverged"),
    }
}

async fn dispatch_case_37_begin_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
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
            let method = match method {
                Method::BeginTxn {
                    graph: None,
                    isolation,
                } => Method::BeginTxn {
                    graph: Some(req_graph.clone()),
                    isolation,
                },
                m => m,
            };
            match handlers::txn::try_handle(
                state,
                req_id,
                verified_context.agent_id(),
                verified_context,
                method,
            )
            .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a txn method.
                Err(_) => Response::err(req_id, "txn dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_37_begin_txn: classifier/handler diverged"),
    }
}

#[cfg(feature = "tsdb")]
async fn dispatch_case_38_txn_add_measurement(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::TxnAddMeasurement { .. } => {
            match handlers::txn::try_handle(
                state,
                req_id,
                verified_context.agent_id(),
                verified_context,
                method,
            )
            .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req_id, "txn dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_38_txn_add_measurement: classifier/handler diverged"),
    }
}

#[cfg(feature = "owl")]
async fn dispatch_case_39_txn_axiom(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::TxnAxiom { .. } => {
            match handlers::txn::try_handle(
                state,
                req_id,
                verified_context.agent_id(),
                verified_context,
                method,
            )
            .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req_id, "txn dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_39_txn_axiom: classifier/handler diverged"),
    }
}

#[cfg(feature = "sparql")]
async fn dispatch_case_40_txn_construct(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::TxnConstruct { .. } => {
            match handlers::txn::try_handle(
                state,
                req_id,
                verified_context.agent_id(),
                verified_context,
                method,
            )
            .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req_id, "txn dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_40_txn_construct: classifier/handler diverged"),
    }
}

#[cfg(feature = "query")]
async fn dispatch_case_41_txn_plan_writeback(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::TxnPlanWriteback { .. } => {
            match handlers::txn::try_handle(
                state,
                req_id,
                verified_context.agent_id(),
                verified_context,
                method,
            )
            .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req_id, "txn dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_41_txn_plan_writeback: classifier/handler diverged"),
    }
}

#[cfg(feature = "epistemic")]
async fn dispatch_case_42_txn_materialize_belief(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::TxnMaterializeBelief { .. } => {
            match handlers::txn::try_handle(
                state,
                req_id,
                verified_context.agent_id(),
                verified_context,
                method,
            )
            .await
            {
                Ok(resp) => resp,
                Err(_) => Response::err(req_id, "txn dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_42_txn_materialize_belief: classifier/handler diverged"),
    }
}

#[cfg(feature = "blob")]
async fn dispatch_case_43_blob_begin(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobFetchBegin { .. }
        | Method::BlobChunkGet { .. }
        | Method::BlobFetchEnd { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            match handlers::blob::try_handle(state, req_id, &carrier, method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a blob method.
                Err(_) => Response::err(req_id, "blob dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_43_blob_begin: classifier/handler diverged"),
    }
}

#[cfg(feature = "kv")]
async fn dispatch_case_44_kv_get(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::KvGet { .. }
        | Method::KvPut { .. }
        | Method::KvDelete { .. }
        | Method::KvScan { .. }
        | Method::KvCas { .. } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            match crate::server::kv::try_handle(state, req_id, &carrier, method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a kv method.
                Err(_) => Response::err(req_id, "kv dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_44_kv_get: classifier/handler diverged"),
    }
}

#[cfg(feature = "sqlite-file")]
async fn dispatch_case_45_import_sqlite_file(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            match handlers::sqlite_file::try_handle(state, req_id, &carrier, method).await {
                Ok(resp) => resp,
                // Unreachable: both variants matched above are sqlite-file methods.
                Err(_) => Response::err(req_id, "sqlite-file dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_45_import_sqlite_file: classifier/handler diverged"),
    }
}

#[cfg(feature = "streaming")]
async fn dispatch_case_46_cdc_read(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::CdcRead { .. }
        | Method::RegisterContinuousQuery { .. }
        | Method::ReadContinuousQuery { .. }
        | Method::DropContinuousQuery { .. }
        | Method::Watch { .. }
        | Method::RegisterTrigger { .. }
        | Method::DropTrigger { .. }
        | Method::ListTriggers { .. }
        | Method::FiredTriggers { .. } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            let read_authority = {
                let s = timed_read(state).await;
                match GraphReadAuthority::from_verified(verified_context, &s.isolation) {
                    Ok(authority) => authority,
                    Err(denied) => return Response::err(req_id, denied),
                }
            };
            match handlers::streaming::try_handle(state, req_id, &carrier, &read_authority, method)
                .await
            {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a streaming method.
                Err(_) => Response::err(req_id, "streaming dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_46_cdc_read: classifier/handler diverged"),
    }
}

#[cfg(all(feature = "streaming", feature = "stream"))]
async fn dispatch_case_47_cep_subscribe(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::CepSubscribe { .. } | Method::CepPoll { .. } | Method::CepUnsubscribe { .. } => {
            let carrier = match CarrierAuthority::from_verified(verified_context) {
                Ok(authority) => authority,
                Err(denied) => return Response::err(req_id, denied),
            };
            match crate::server::cep::try_handle(state, req_id, &carrier, method).await {
                Ok(resp) => resp,
                // Unreachable: every variant matched above is a CEP method.
                Err(_) => Response::err(req_id, "cep dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_47_cep_subscribe: classifier/handler diverged"),
    }
}

#[cfg(feature = "owl")]
async fn dispatch_case_48_owl_reason_distributed(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::OwlReasonDistributed { .. } => {
            let read_authority = {
                let s = timed_read(state).await;
                match GraphReadAuthority::from_verified(&verified_context, &s.isolation) {
                    Ok(authority) => authority,
                    Err(denied) => return Response::err(req_id, denied),
                }
            };
            match handlers::rdf::try_handle_distributed(state, req_id, &read_authority, method)
                .await
            {
                Ok(resp) => resp,
                // Unreachable: the only variant routed here is OwlReasonDistributed.
                Err(_) => Response::err(req_id, "owl distributed dispatch routing error"),
            }
        }
        _ => unreachable!("dispatch_case_48_owl_reason_distributed: classifier/handler diverged"),
    }
}

async fn dispatch_case_49_apply_change_envelope(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ApplyChangeEnvelope { envelope } => {
            let claims = verified_context.claims();
            let batch_context = &envelope.mutation.context;
            if envelope.mutation.graph != req_graph
                || envelope.mutation.tenant != claims.tenant
                || batch_context.request_id != req_id
                || batch_context.principal != verified_context.principal_persistence_id()
                || envelope.mutation.idempotency_key != verified_context.idempotency_key()
                || batch_context.policy_fingerprint.as_deref()
                    != Some(claims.policy_version.as_str())
            {
                return Response::err(
                    req_id,
                    "ApplyChangeEnvelope context does not match the verified request authority",
                );
            }
            dispatch_graph_op(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                Method::ApplyChangeEnvelope { envelope },
            )
            .await
        }
        _ => unreachable!("dispatch_case_49_apply_change_envelope: classifier/handler diverged"),
    }
}

async fn dispatch_case_50_apply_change_envelopes(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ApplyChangeEnvelopes { envelopes } => {
            dispatch_change_envelopes(
                state,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                envelopes,
            )
            .await
        }
        _ => unreachable!("dispatch_case_50_apply_change_envelopes: classifier/handler diverged"),
    }
}

#[cfg(feature = "modality-serving")]
async fn dispatch_case_51_served_modality(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::ServedModality { op } => {
            let auth_secret = timed_read(state).await.auth_secret.clone();
            let authority = match handlers::modality::ModalityAuthority::from_verified(
                &auth_secret,
                verified_context.claims(),
            ) {
                Ok(authority) => authority,
                Err(error) => return Response::err(req_id, error),
            };
            dispatch_served_modality(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                op,
                authority,
            )
            .await
        }
        _ => unreachable!("dispatch_case_51_served_modality: classifier/handler diverged"),
    }
}

#[cfg(feature = "knowledge-batch")]
async fn dispatch_case_52_knowledge_stream(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::KnowledgeStream { request } => {
            let auth_secret = timed_read(state).await.auth_secret.clone();
            let authority =
                match handlers::knowledge_stream::KnowledgeStreamAuthority::from_verified(
                    &auth_secret,
                    verified_context.claims(),
                ) {
                    Ok(authority) => authority,
                    Err(error) => return Response::err(req_id, error),
                };
            dispatch_knowledge_stream(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                request,
                authority,
            )
            .await
        }
        _ => unreachable!("dispatch_case_52_knowledge_stream: classifier/handler diverged"),
    }
}

async fn dispatch_case_53_get_change_envelope(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::GetChangeEnvelope {
            envelope_id,
            tenant,
        } => {
            if tenant != verified_context.claims().tenant {
                return Response::err(
                    req_id,
                    "ChangeEnvelope reads require the verified tenant context",
                );
            }
            dispatch_graph_op(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                Method::GetChangeEnvelope {
                    envelope_id,
                    tenant,
                },
            )
            .await
        }
        _ => unreachable!("dispatch_case_53_get_change_envelope: classifier/handler diverged"),
    }
}

async fn dispatch_case_54_get_content_version(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::GetContentVersion { object_id, tenant } => {
            if tenant != verified_context.claims().tenant {
                return Response::err(
                    req_id,
                    "content-version reads require the verified tenant context",
                );
            }
            dispatch_graph_op(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                Method::GetContentVersion { object_id, tenant },
            )
            .await
        }
        _ => unreachable!("dispatch_case_54_get_content_version: classifier/handler diverged"),
    }
}

async fn dispatch_case_55_get_change_cursor(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::GetChangeCursor {
            source,
            partition,
            tenant,
        } => {
            if tenant != verified_context.claims().tenant {
                return Response::err(
                    req_id,
                    "change-cursor reads require the verified tenant context",
                );
            }
            dispatch_graph_op(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                Method::GetChangeCursor {
                    source,
                    partition,
                    tenant,
                },
            )
            .await
        }
        _ => unreachable!("dispatch_case_55_get_change_cursor: classifier/handler diverged"),
    }
}

async fn dispatch_case_56_nl_query(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    req_graph: String,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::NlQuery { ref graph, .. } => {
            let target = if graph.is_empty() {
                req_graph.clone()
            } else {
                graph.clone()
            };
            dispatch_graph_op(
                state,
                &target,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                method,
            )
            .await
        }
        _ => unreachable!("dispatch_case_56_nl_query: classifier/handler diverged"),
    }
}

async fn dispatch_case_57_multi_graph_batch_update(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    req_agent_id: Option<String>,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Response {
    match method {
        Method::MultiGraphBatchUpdate { batches_msgpack } => {
            multi_graph_batch_update(
                state,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                &batches_msgpack,
            )
            .await
        }
        _ => unreachable!("dispatch_case_57_multi_graph_batch_update: classifier/handler diverged"),
    }
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

    // NodeInfoUpsert is an engine-owned self-report emitted by the Raft
    // startup path.  Do not let a verified external caller turn the endpoint
    // fields in this method into cluster authority, even when that caller has
    // an administrative scope.  The replicated-apply path is the only route
    // allowed to reach the topology handler.
    if !state_machine_authorized && matches!(&req.method, Method::NodeInfoUpsert { .. }) {
        return Err(Response::err(
            req.id,
            "ACCESS_DENIED: NodeInfoUpsert is reserved for the engine Raft self-report path",
        ));
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
    let (req, identity_bootstrap) =
        match dispatch_preamble_checks(state, req, &verified_context, state_machine_authorized)
            .await
        {
            Ok(v) => v,
            Err(resp) => return resp,
        };
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

/// The request fields every dispatch group needs after `req.method` has been
/// moved out of the `Request`. Keeping the field names (`id`, `graph`,
/// `agent_id`) means each relocated match arm still reads exactly as it did
/// inside `dispatch_inner`.
struct DispatchHeader {
    id: u64,
    graph: String,
    agent_id: Option<String>,
}

/// The authenticated, preamble-resolved context shared by every dispatch group.
/// All fields are shared references or flags, so this is `Copy` and each group
/// call is free.
#[derive(Clone, Copy)]
struct DispatchCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req: &'a DispatchHeader,
    verified_context: &'a VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
}

/// Service-level, source-ingestion and cost/telemetry methods.
///
/// Returns `Err(method)` for a method this group does not own, so
/// `dispatch_request_method` can hand it to the next group. The groups
/// partition disjoint `Method` variants, so the split cannot change which
/// arm a request reaches.
async fn dispatch_service_and_ingest_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[allow(unused_variables)]
    let state = ctx.state;
    #[allow(unused_variables)]
    let req = ctx.req;
    #[allow(unused_variables)]
    let verified_context = ctx.verified_context;
    Ok(match method {
        // ── Service-level ────────────────────────────────────────────
        Method::Ping => Response::ok(req.id, ResultPayload::String("pong".to_string())),

        method @ Method::Health => {
            dispatch_boxed(dispatch_case_02_health(state, req.id, method)).await
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

        method @ Method::ParseFile { .. } => {
            dispatch_boxed(dispatch_case_04_parse_file(req.id, method)).await
        }

        method @ Method::ParseFiles { .. } => {
            dispatch_boxed(dispatch_case_05_parse_files(req.id, method)).await
        }

        method @ Method::IndexRepository { .. } => {
            dispatch_boxed(dispatch_case_06_index_repository(req.id, method)).await
        }

        method @ Method::ObserveScreen { .. } => {
            dispatch_boxed(dispatch_case_07_observe_screen(req.id, method)).await
        }

        method @ Method::Shutdown => {
            dispatch_boxed(dispatch_case_08_shutdown(req.id, method)).await
        }

        // ── Cost / efficiency (CONCEPT:EG-KG.compute.lane-v, Lane V) ──────────────
        #[cfg(feature = "cost")]
        method @ Method::ResourceStats => {
            dispatch_boxed(dispatch_case_09_resource_stats(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }
        other => return Err(other),
    })
}

/// Graph lifecycle plus the M3 catalog/reshard/placement/raft admin surface.
///
/// Returns `Err(method)` for a method this group does not own, so
/// `dispatch_request_method` can hand it to the next group. The groups
/// partition disjoint `Method` variants, so the split cannot change which
/// arm a request reaches.
async fn dispatch_graph_lifecycle_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[allow(unused_variables)]
    let state = ctx.state;
    #[allow(unused_variables)]
    let req = ctx.req;
    #[allow(unused_variables)]
    let verified_context = ctx.verified_context;
    #[allow(unused_variables)]
    let state_machine_authorized = ctx.state_machine_authorized;
    Ok(match method {
                #[cfg(feature = "cost")]
        method @ Method::ResourceStatsPage { .. } => {
            dispatch_boxed(
                dispatch_case_10_resource_stats_page(
                    state,
                    req.id,
                    verified_context,
                    method,
                )
            )
            .await
        }

        // ── Multi-tenant graph management ────────────────────────────
                method @ Method::CreateGraph { .. } => {
            dispatch_boxed(
                dispatch_case_11_create_graph(
                    state,
                    req.id,
                    req.agent_id.clone(),
                    method,
                )
            )
            .await
        }

                method @ Method::DeleteGraph { .. } => {
            dispatch_boxed(
                dispatch_case_12_delete_graph(
                    state,
                    req.id,
                    req.agent_id.clone(),
                    state_machine_authorized,
                    method,
                )
            )
            .await
        }

                method @ Method::ListGraphs => {
            dispatch_boxed(
                dispatch_case_13_list_graphs(
                    state,
                    req.id,
                    verified_context,
                    method,
                )
            )
            .await
        }

        // ── M3 catalog-driven resharding admin (CONCEPT:EG-KG.backend.m3-admin-dispatch) ──────
        // The wire surface that drives online resharding (EG-032), the tenant catalog
        // (EG-031) and the rebalance planner (EG-035) + its execution (EG-039). All
        // self-routing service-level ops handled here (not the per-graph chain), so they
        // reach the concrete redb backend via `as_redb`. A non-redb build returns a clean
        // "not available" error from the handler.
                method @ (Method::Reshard { .. }
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
        | Method::Restore { .. }) => {
            dispatch_boxed(
                dispatch_case_14_reshard(
                    state,
                    req.id,
                    req.agent_id.clone(),
                    method,
                )
            )
            .await
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
                method @ (Method::PlacementRoute { .. } | Method::PlacementAdmin { .. }) => {
            dispatch_boxed(
                dispatch_case_15_placement_route(
                    state,
                    req.id,
                    method,
                )
            )
            .await
        }

        // ── Raft cluster membership admin (CONCEPT:EG-KG.storage.kg-kg-2 — cluster_deployment.md §5
        // item 2) ── Self-routing, NOT graph-scoped (cluster-wide, like the M3 admin
        // block above): attaches/promotes a node against `MultiRaft` directly. Gated
        // `admin:cluster` by the SAME scope+admin enforcement every other admin-tier
        // method goes through above (`eg_capabilities::policy`), not a second check
        // here.
                method @ (Method::RaftAddLearner { .. } | Method::RaftChangeMembership { .. }) => {
            dispatch_boxed(
                dispatch_case_16_raft_add_learner(
                    state,
                    req.id,
                    method,
                )
            )
            .await
        }

        // ── Cluster topology discovery (CONCEPT:EG-KG.sharding.cluster-topology, ADR-1 / W1.1) ──
        // Self-routing, NOT graph-scoped (cluster-wide, like the raft-admin block
        // above): `ClusterMembers` answers from ANY node's local `NodeInfoStore` +
        // live `MultiRaft` membership (no leader redirect, unlike `PlacementRoute` —
        // ADR-1's client resolves via any healthy seed); `NodeInfoUpsert` is the
        // internal per-node self-report `raft::node::start` issues, reaching this
        // arm only via the replicated-apply re-entry (its live proposal is
        // intercepted earlier by the `ConsensusNative` branch above).
                method @ (Method::ClusterMembers | Method::NodeInfoUpsert { .. }) => {
            dispatch_boxed(
                dispatch_case_17_cluster_members(
                    state,
                    req.id,
                    verified_context,
                    method,
                )
            )
            .await
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
                method @ Method::RegisterServer { .. } => {
            dispatch_boxed(
                dispatch_case_18_register_server(
                    state,
                    req.id,
                    req.agent_id.clone(),
                    verified_context,
                    method,
                )
            )
            .await
        }

        // ── Channel operations ───────────────────────────────────────
        other => return Err(other),
    })
}

/// Channel and messaging methods.
///
/// Returns `Err(method)` for a method this group does not own, so
/// `dispatch_request_method` can hand it to the next group. The groups
/// partition disjoint `Method` variants, so the split cannot change which
/// arm a request reaches.
async fn dispatch_channel_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[allow(unused_variables)]
    let state = ctx.state;
    #[allow(unused_variables)]
    let req = ctx.req;
    #[allow(unused_variables)]
    let verified_context = ctx.verified_context;
    #[allow(unused_variables)]
    let state_machine_authorized = ctx.state_machine_authorized;
    #[allow(unused_variables)]
    let identity_bootstrap = ctx.identity_bootstrap;
    Ok(match method {
        method @ Method::CreateChannel { .. } => {
            dispatch_boxed(dispatch_case_19_create_channel(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        method @ Method::JoinChannel { .. } => {
            dispatch_boxed(dispatch_case_20_join_channel(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        method @ Method::LeaveChannel { .. } => {
            dispatch_boxed(dispatch_case_21_leave_channel(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        method @ Method::CloseChannel { .. } => {
            dispatch_boxed(dispatch_case_22_close_channel(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        method @ Method::SendMessage { .. } => {
            dispatch_boxed(dispatch_case_23_send_message(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        method @ Method::GetChannelMessages { .. } => {
            dispatch_boxed(dispatch_case_24_get_channel_messages(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        method @ Method::ListChannels => {
            dispatch_boxed(dispatch_case_25_list_channels(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        method @ Method::GetChannelMembers { .. } => {
            dispatch_boxed(dispatch_case_26_get_channel_members(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── Zero-Trust Consensus ─────────────────────────────────────────
        method @ Method::RegisterIdentity { .. } => {
            dispatch_boxed(dispatch_case_27_register_identity(
                state,
                req.id,
                req.graph.clone(),
                verified_context,
                state_machine_authorized,
                identity_bootstrap,
                method,
            ))
            .await
        }

        // Identity read-back (CONCEPT:EG-KG.compute.feature): closes the `RegisterIdentity`
        // blind-upsert gap. `RegisterIdentity` REPLACES a principal's whole role set on
        // every call, so a caller that wants to add a role without dropping one already
        // granted by a prior admission pass must read the current set back first. Gated at
        // the SAME `security:admin` scope as `RegisterIdentity` (see `eg_capabilities::policy`),
        // enforced by the admin-scope check above the method match, so no additional
        // authorization is done here.
        other => return Err(other),
    })
}

/// Identity, policy, RBAC, multisig, jobs, statechart and the standalone
/// quantum / ASR / viz surfaces.
///
/// Returns `Err(method)` for a method this group does not own, so
/// `dispatch_request_method` can hand it to the next group. The groups
/// partition disjoint `Method` variants, so the split cannot change which
/// arm a request reaches.
async fn dispatch_identity_and_admin_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[allow(unused_variables)]
    let state = ctx.state;
    #[allow(unused_variables)]
    let req = ctx.req;
    #[allow(unused_variables)]
    let verified_context = ctx.verified_context;
    #[allow(unused_variables)]
    let state_machine_authorized = ctx.state_machine_authorized;
    Ok(match method {
        method @ Method::GetIdentity { .. } => {
            dispatch_boxed(dispatch_case_28_get_identity(state, req.id, method)).await
        }

        // CA-16 (DEC-CA-04): export the M1 row-visibility policy bundle. Gated
        // above the match (policy:export authz_action; kg:admin also clears it
        // via allows_method's unconditional fallback -- see
        // eg_capabilities::policy's Method::PolicyExport entry). The calling
        // principal's OWN live-token-verified role set (RequestContextClaims'
        // roles ∪ scopes, ALREADY cross-checked against the verified OIDC token
        // by bind_verified_identity -- never IsolationLayer.agents/rbac.redb)
        // is always folded into `principals` here, so every export call is
        // self-proving against a real verified token (DEC-CA-04 A2). See
        // `server::policy_export`'s module doc for the full design.
        #[cfg(feature = "policy_export")]
        method @ Method::PolicyExport { .. } => {
            dispatch_boxed(dispatch_case_29_policy_export(
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── RBAC policy administration (CONCEPT:EG-KG.compute.feature) ──────────────────
        // Gated at the handler; a non-security build has no arm and falls to the
        // dispatch "not available in this build" catch-all (mirrors EG-090).
        #[cfg(feature = "security")]
        method @ Method::RbacAdmin { .. } => {
            dispatch_boxed(dispatch_case_30_rbac_admin(state, req.id, method)).await
        }

        method @ Method::ApplyMultisigMutation { .. } => {
            dispatch_boxed(dispatch_case_31_apply_multisig_mutation(
                state,
                req.id,
                req.agent_id.clone(),
                req.graph.clone(),
                verified_context,
                state_machine_authorized,
                method,
            ))
            .await
        }

        // ── Durable analytics-job plane (CONCEPT:INT-P2-1, feature `jobs`) ──────────
        // NOT graph-scoped (own `jobs.redb`, keyed by `job_id`) — self-routes here,
        // BEFORE the per-graph `dispatch_graph_op` chain, exactly like `TsAppend`/
        // `Kv*`/`CreateChannel` above. See `handlers/jobs.rs` module docs.
        #[cfg(feature = "jobs")]
        method @ Method::AnalyticsJob { .. } => {
            dispatch_boxed(dispatch_case_32_analytics_job(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── Native statechart engine (CONCEPT:INT-P2-2, feature `statechart`) ───────
        // NOT graph-scoped (own `statecharts.redb`, keyed by def_id/instance_id) —
        // self-routes here, BEFORE the per-graph `dispatch_graph_op` chain, exactly
        // like `AnalyticsJob` above. See `handlers/statechart.rs` module docs.
        #[cfg(feature = "statechart")]
        method @ Method::Statechart { .. } => {
            dispatch_boxed(dispatch_case_33_statechart(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
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
        // ── Native ASR provider surface (GOC-33, `OWNER-VOICE-ASR`, feature
        // `asr-whisper`) ─────────────────────────────────────────────────
        // NOT graph-scoped (a transcription reads no persisted graph state and
        // commits no durable asr.result.v1 here) — self-routes here, BEFORE the
        // per-graph `dispatch_graph_op` chain, exactly like `Quantum`/`Viz` above.
        // See `handlers::asr`'s module doc for the authority boundary.
        #[cfg(feature = "asr-whisper")]
        Method::Asr { op } => handlers::asr::handle(req.id, op).await,
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
        method @ Method::Viz { .. } => {
            dispatch_boxed(dispatch_case_36_viz(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── Transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid — multi-op OCC ACID) ──────
        // Stateful + self-routing: a Txn* op targets the graph the txn was opened
        // against (resolved from `open_txns`), NOT necessarily `req.graph`, and
        // BeginTxn carries its own graph. So they are handled here (with `state`)
        // BEFORE the graph-op path — never through `dispatch_graph_op`, whose
        // coalescer/registry-lookup assumes a single `req.graph` target. For
        // BeginTxn the request envelope's `graph` is the default target when the
        // body omits one.
        other => return Err(other),
    })
}

/// Transaction control plus the blob / KV / SQLite-file stores.
///
/// Returns `Err(method)` for a method this group does not own, so
/// `dispatch_request_method` can hand it to the next group. The groups
/// partition disjoint `Method` variants, so the split cannot change which
/// arm a request reaches.
async fn dispatch_transaction_and_store_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[allow(unused_variables)]
    let state = ctx.state;
    #[allow(unused_variables)]
    let req = ctx.req;
    #[allow(unused_variables)]
    let verified_context = ctx.verified_context;
    Ok(match method {
        method @ (Method::BeginTxn { .. }
        | Method::TxnAddNode { .. }
        | Method::TxnRemoveNode { .. }
        | Method::TxnAddEdge { .. }
        | Method::TxnRemoveEdge { .. }
        | Method::TxnCas { .. }
        | Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. }
        | Method::Commit { .. }
        | Method::Rollback { .. }) => {
            dispatch_boxed(dispatch_case_37_begin_txn(
                state,
                req.id,
                req.graph.clone(),
                verified_context,
                method,
            ))
            .await
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
        method @ Method::TxnAddMeasurement { .. } => {
            dispatch_boxed(dispatch_case_38_txn_add_measurement(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }
        #[cfg(feature = "owl")]
        method @ Method::TxnAxiom { .. } => {
            dispatch_boxed(dispatch_case_39_txn_axiom(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }
        #[cfg(feature = "sparql")]
        method @ Method::TxnConstruct { .. } => {
            dispatch_boxed(dispatch_case_40_txn_construct(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }
        // Planner-writeback staging (CONCEPT:EG-KG.query.plan-dag, D7) — carries its OWN
        // `graph` (like `TxnConstruct`), so it routes straight to the txn handler with NO
        // BeginTxn graph-default rewrite. `query`-gated to match its protocol variant.
        #[cfg(feature = "query")]
        method @ Method::TxnPlanWriteback { .. } => {
            dispatch_boxed(dispatch_case_41_txn_plan_writeback(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }
        // Materialize-belief staging (CONCEPT:EG-KG.epistemic.epistemic-substrate, D5) —
        // carries its OWN `graph` (like `TxnPlanWriteback`), so it routes straight to the
        // txn handler with NO BeginTxn graph-default rewrite. `epistemic`-gated to match
        // its protocol variant.
        #[cfg(feature = "epistemic")]
        method @ Method::TxnMaterializeBelief { .. } => {
            dispatch_boxed(dispatch_case_42_txn_materialize_belief(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── Blob (CONCEPT:EG-KG.storage.blob-namespace) ──────────────────────────────────
        // Content-addressed, NOT graph-scoped: a blob is keyed by digest and may be
        // referenced across graphs, so route at the top level (like txn) before the
        // per-graph chain. The variants only exist with the `blob` feature; without
        // it they aren't in the enum and a slim build can't reach this arm.
        #[cfg(feature = "blob")]
        method @ (Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobFetchBegin { .. }
        | Method::BlobChunkGet { .. }
        | Method::BlobFetchEnd { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc) => {
            dispatch_boxed(dispatch_case_43_blob_begin(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── Key→Value (CONCEPT:EG-KG.storage.namespaced-kv-surface) ───────────────────────────────
        // Namespaced KV, NOT graph-scoped: a pair is keyed by (namespace, key) and
        // lives off the node/edge graph, so route at the top level (like blob/txn)
        // before the per-graph chain. The variants only exist with the `kv` feature;
        // without it they aren't in the enum and a slim build can't reach this arm.
        #[cfg(feature = "kv")]
        method @ (Method::KvGet { .. }
        | Method::KvPut { .. }
        | Method::KvDelete { .. }
        | Method::KvScan { .. }
        | Method::KvCas { .. }) => {
            dispatch_boxed(dispatch_case_44_kv_get(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── SQLite `.db` file import/export (CONCEPT:EG-KG.query.eg-feature/EG-332) ──
        // File-scoped, NOT graph-scoped: both ops target a filesystem `path` and move
        // rows through the verified caller's owner-scoped user-table store (behind `query`), so they
        // self-route here (like the Blob*/Kv* ops) BEFORE the per-graph chain. Gated
        // `sqlite-file` (which pulls the bundled C sqlite kept OUT of pi); a build
        // without it never has the variants in the enum, so this arm can't be reached.
        #[cfg(feature = "sqlite-file")]
        method @ (Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. }) => {
            dispatch_boxed(dispatch_case_45_import_sqlite_file(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── Streaming / CDC / subscriptions (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230) ───
        // The reactive READ + REGISTER surface over the CDC hub on `state` (the WRITE
        // side — emitting changes — lives in the dispatch_graph_op write-side-effect
        // block). These are NOT graph-mutating (CdcRead/Watch/FiredTriggers tail a
        // cursor; Register*/Drop* manage hub registrations), so they self-route here
        // BEFORE the per-graph chain, like tsdb/blob. Gated `streaming`: in a slim
        // build the arm is absent and the variants fall to the graph_ops not-built
        // catch-all (never a panic, never a mis-route).
        other => return Err(other),
    })
}

/// Streaming, CEP, OWL reasoning, change envelopes, served modality and the
/// knowledge stream.
///
/// Returns `Err(method)` for a method this group does not own, so
/// `dispatch_request_method` can hand it to the next group. The groups
/// partition disjoint `Method` variants, so the split cannot change which
/// arm a request reaches.
async fn dispatch_stream_and_envelope_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[allow(unused_variables)]
    let state = ctx.state;
    #[allow(unused_variables)]
    let req = ctx.req;
    #[allow(unused_variables)]
    let verified_context = ctx.verified_context;
    Ok(match method {
        #[cfg(feature = "streaming")]
        method @ (Method::CdcRead { .. }
        | Method::RegisterContinuousQuery { .. }
        | Method::ReadContinuousQuery { .. }
        | Method::DropContinuousQuery { .. }
        | Method::Watch { .. }
        | Method::RegisterTrigger { .. }
        | Method::DropTrigger { .. }
        | Method::ListTriggers { .. }
        | Method::FiredTriggers { .. }) => {
            dispatch_boxed(dispatch_case_46_cdc_read(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
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
        method @ (Method::CepSubscribe { .. }
        | Method::CepPoll { .. }
        | Method::CepUnsubscribe { .. }) => {
            dispatch_boxed(dispatch_case_47_cep_subscribe(
                state,
                req.id,
                verified_context,
                method,
            ))
            .await
        }

        // ── Distributed OWL reasoning (CONCEPT:EG-KG.ontology.concept-13) ─────────────
        // Cross-shard: reasons over the UNION of several graphs, so it self-routes
        // here (with `state` to gather each shard's snapshot) BEFORE the per-graph
        // chain — never through `dispatch_graph_op`, which targets a single `req.graph`.
        // Gated `owl`: in a build without it the variant isn't in the enum.
        #[cfg(feature = "owl")]
        method @ Method::OwlReasonDistributed { .. } => {
            dispatch_boxed(dispatch_case_48_owl_reason_distributed(
                state,
                req.id,
                // `verified_context` is a `&VerifiedRequestContext` here; spell the
                // clone out so it cannot be read as cloning the reference.
                VerifiedRequestContext::clone(verified_context),
                method,
            ))
            .await
        }

        // ── Graph operations (dispatch to target graph) ──────────────
        method @ Method::ApplyChangeEnvelope { .. } => {
            dispatch_boxed(dispatch_case_49_apply_change_envelope(
                state,
                req.id,
                req.agent_id.clone(),
                req.graph.clone(),
                verified_context,
                method,
            ))
            .await
        }
        method @ Method::ApplyChangeEnvelopes { .. } => {
            dispatch_boxed(dispatch_case_50_apply_change_envelopes(
                state,
                req.id,
                req.agent_id.clone(),
                verified_context,
                method,
            ))
            .await
        }
        #[cfg(feature = "modality-serving")]
        method @ Method::ServedModality { .. } => {
            dispatch_boxed(dispatch_case_51_served_modality(
                state,
                req.id,
                req.agent_id.clone(),
                req.graph.clone(),
                verified_context,
                method,
            ))
            .await
        }
        #[cfg(feature = "knowledge-batch")]
        method @ Method::KnowledgeStream { .. } => {
            dispatch_boxed(dispatch_case_52_knowledge_stream(
                state,
                req.id,
                req.agent_id.clone(),
                req.graph.clone(),
                verified_context,
                method,
            ))
            .await
        }
        method @ Method::GetChangeEnvelope { .. } => {
            dispatch_boxed(dispatch_case_53_get_change_envelope(
                state,
                req.id,
                req.agent_id.clone(),
                req.graph.clone(),
                verified_context,
                method,
            ))
            .await
        }
        method @ Method::GetContentVersion { .. } => {
            dispatch_boxed(dispatch_case_54_get_content_version(
                state,
                req.id,
                req.agent_id.clone(),
                req.graph.clone(),
                verified_context,
                method,
            ))
            .await
        }
        other => return Err(other),
    })
}

/// Change cursors, natural-language query and multi-graph batch update.
///
/// Returns `Err(method)` for a method this group does not own, so
/// `dispatch_request_method` can hand it to the next group. The groups
/// partition disjoint `Method` variants, so the split cannot change which
/// arm a request reaches.
async fn dispatch_query_and_batch_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    #[allow(unused_variables)]
    let state = ctx.state;
    #[allow(unused_variables)]
    let req = ctx.req;
    #[allow(unused_variables)]
    let verified_context = ctx.verified_context;
    Ok(match method {
        method @ Method::GetChangeCursor { .. } => {
            dispatch_boxed(dispatch_case_55_get_change_cursor(
                state,
                req.id,
                req.agent_id.clone(),
                req.graph.clone(),
                verified_context,
                method,
            ))
            .await
        }
        // Natural-language query (CONCEPT:EG-KG.query.core-query-input/EG-080): the graph rides the METHOD
        // (the `/nl` HTTP facade path has no request envelope), so route to the method's
        // `graph`, falling back to the request envelope's graph when it is empty. The
        // handler (behind `nl-query`) turns NL→UQL and runs the deterministic
        // `UnifiedQueryText` pipeline; a build without `nl-query` reaches the graph_ops
        // "not available" catch-all like any other feature-off method.
        method @ Method::NlQuery { .. } => {
            dispatch_boxed(dispatch_case_56_nl_query(
                state,
                req.id,
                req.agent_id.clone(),
                req.graph.clone(),
                verified_context,
                method,
            ))
            .await
        }
        // Batched CROSS-GRAPH write (CONCEPT:EG-KG.storage.multi-graph-batch-write) — the
        // graphs ride the METHOD (one round-trip, many graphs), so like the txn/ts
        // self-routing ops it is handled HERE, BEFORE the single-`req.graph`
        // graph-op path. Each sub-batch fans through the normal per-graph write
        // path CONCURRENTLY, so N distinct graphs commit across N of the K shard
        // writers in parallel.
        method @ Method::MultiGraphBatchUpdate { .. } => {
            dispatch_boxed(dispatch_case_57_multi_graph_batch_update(
                state,
                req.id,
                req.agent_id.clone(),
                verified_context,
                method,
            ))
            .await
        }
        other => return Err(other),
    })
}

/// The one guarded arm: a SPARQL-over-HTTP UPDATE arrives as an
/// `ApplyMutation` whose event type is the SPARQL-HTTP update marker. Any
/// other `ApplyMutation` falls through to the ordinary per-graph chain,
/// exactly as the guard's own fallthrough did.
#[cfg(feature = "sparql-http")]
async fn dispatch_sparql_http_update(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req = ctx.req;
    let verified_context = ctx.verified_context;
    Ok(match method {
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
        other => return Err(other),
    })
}

/// Service, graph-lifecycle, channel and identity/admin methods: everything
/// resolved before the per-graph data path is consulted.
async fn dispatch_control_plane_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match dispatch_service_and_ingest_methods(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    let method = match dispatch_graph_lifecycle_methods(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    let method = match dispatch_channel_methods(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    dispatch_identity_and_admin_methods(ctx, method).await
}

/// Transaction/store, streaming/envelope, query/batch and the guarded
/// SPARQL-over-HTTP update. A method none of these claims is handed back for the
/// ordinary per-graph chain.
async fn dispatch_data_plane_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match dispatch_transaction_and_store_methods(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    let method = match dispatch_stream_and_envelope_methods(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    let method = match dispatch_query_and_batch_methods(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    #[cfg(feature = "sparql-http")]
    {
        dispatch_sparql_http_update(ctx, method).await
    }
    #[cfg(not(feature = "sparql-http"))]
    {
        Err(method)
    }
}

/// Route one authenticated request to its handler.
///
/// The single 59-arm `match req.method` this replaced is now seven group
/// dispatchers over disjoint `Method` variants, tried in order; each hands
/// back a method it does not own. A method no group claims falls through to
/// the ordinary per-graph chain, which is exactly what the old `_` arm did.
async fn dispatch_request_method(
    state: &Arc<RwLock<ServerState>>,
    req: Request,
    verified_context: &VerifiedRequestContext,
    state_machine_authorized: bool,
    identity_bootstrap: bool,
) -> Response {
    let method = req.method;
    let req = DispatchHeader {
        id: req.id,
        graph: req.graph,
        agent_id: req.agent_id,
    };
    let ctx = DispatchCtx {
        state,
        req: &req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    };
    let method = match dispatch_control_plane_methods(ctx, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };
    let method = match dispatch_data_plane_methods(ctx, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };
    dispatch_graph_op(
        state,
        &req.graph,
        req.id,
        req.agent_id.as_deref(),
        verified_context,
        method,
    )
    .await
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

/// Everything the SPARQL-HTTP update coordinator's phases share: the verified
/// caller, the durable redb backend, and the two saga ids (the parent that
/// carries the sealed plan and the compensation marker that makes a restart
/// choose one direction forever). Bundled so each phase helper stays inside the
/// parameter cap.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
struct SparqlUpdateCoordination<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &'a VerifiedRequestContext,
    verified_actor: &'a str,
    redb: &'a crate::server::persistence::redb_backend::RedbBackend,
    parent_id: &'a str,
    compensation_id: &'a str,
}

/// A resumed saga that already carries its committed result: clear BOTH
/// retained decisions, then replay the recorded outcome verbatim — a compensated
/// outcome still answers as the compensation error, never as success.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn replay_sparql_update(
    coord: &SparqlUpdateCoordination<'_>,
    result: ResultPayload,
) -> Response {
    if let Err(error) = clear_coordinated_graph_decision(coord.redb, coord.parent_id).await {
        return Response::err(coord.req_id, error);
    }
    if let Err(error) = clear_coordinated_graph_decision(coord.redb, coord.compensation_id).await {
        return Response::err(coord.req_id, error);
    }
    if coordinator_result_is_compensated(&result) {
        Response::err(coord.req_id, "SPARQL update was durably compensated")
    } else {
        Response::ok(coord.req_id, result)
    }
}

/// Recover the sealed plan of a saga that was already begun by an earlier
/// attempt. `Err` is this request's final response — either the replayed
/// outcome or a recovery failure.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn resume_sparql_update_plan(
    coord: &SparqlUpdateCoordination<'_>,
    saga: handlers::admin::AdminSaga,
) -> Result<(handlers::admin::AdminSaga, SparqlRecoveryPlanV1), Response> {
    if let Some(result) = saga.replayed.clone() {
        return Err(replay_sparql_update(coord, result).await);
    }
    let encrypted = match eg_mutation_store::read_private_payload(
        coord.redb.admin_mutation_store(),
        coord.parent_id,
    ) {
        Ok(Some(value)) => value,
        Ok(None) => {
            return Err(Response::err(
                coord.req_id,
                "SPARQL recovery plan is missing",
            ))
        }
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    let plan = match open_private_coordinator_plan(
        coord.redb,
        &saga.batch,
        SPARQL_RECOVERY_EVENT,
        &encrypted,
    ) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    Ok((saga, plan))
}

/// The complete graph set this update may address. An update whose graph is a
/// variable can reach every resident graph, so the registry is folded in and the
/// set deduplicated.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn resolve_sparql_update_graphs(
    coord: &SparqlUpdateCoordination<'_>,
    query: &str,
    default_graph: &str,
) -> Result<Vec<String>, Response> {
    let mut graphs = match crate::server::sparql_http::update_graphs(query, default_graph) {
        Ok(graphs) => graphs,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    if crate::server::sparql_http::update_uses_variable_graph(query) {
        graphs.extend(
            coord
                .state
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
    Ok(graphs)
}

/// Write access is checked against every graph that ALREADY exists, under one
/// read lock; the names that do not yet exist are returned so the caller can
/// gate creation separately.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn check_sparql_update_graph_access(
    coord: &SparqlUpdateCoordination<'_>,
    graphs: &[String],
) -> Result<Vec<String>, Response> {
    let current = timed_read(coord.state).await;
    for graph in graphs {
        let Some(entry) = current.registry.get(graph) else {
            continue;
        };
        if let Err(error) = check_graph_access(
            &current.isolation,
            Some(coord.verified_actor),
            graph,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Write,
        ) {
            return Err(Response::err(coord.req_id, error));
        }
    }
    Ok(graphs
        .iter()
        .filter(|graph| !current.registry.exists(graph))
        .cloned()
        .collect::<Vec<_>>())
}

/// Seal a plan as authenticated ciphertext and open the named saga that binds
/// only its digest. Shared by the parent plan and the compensation marker.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
fn begin_sealed_sparql_saga(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlanV1,
    batch_id: &str,
    event_type: &str,
) -> Result<handlers::admin::AdminSaga, Response> {
    let (digest, encrypted) = match seal_private_coordinator_plan(coord.redb, plan) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    handlers::admin::begin_named_admin_saga_with_private_payload(
        coord.redb,
        coord.req_id,
        Some(coord.verified_actor),
        handlers::admin::AdminSagaPayload {
            domain: crate::mutation_batch::MutationDomain::MultiGraph,
            batch_id,
            event_type,
            payload_digest: &digest,
            encrypted_payload: &encrypted,
        },
    )
    .map_err(|error| Response::err(coord.req_id, error))
}

/// First attempt: resolve the graph set, gate access, plan the before/after
/// images, and seal them into a fresh parent saga.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn build_sparql_update_plan(
    coord: &SparqlUpdateCoordination<'_>,
    query: &str,
    default_graph: &str,
) -> Result<(handlers::admin::AdminSaga, SparqlRecoveryPlanV1), Response> {
    let graphs = resolve_sparql_update_graphs(coord, query, default_graph).await?;
    let missing = check_sparql_update_graph_access(coord, &graphs).await?;
    if !missing.is_empty() && !coord.verified_context.allows_method("graph:admin", true) {
        return Err(Response::err(
            coord.req_id,
            "ACCESS_DENIED: SPARQL graph creation requires graph:admin",
        ));
    }
    let planned =
        match crate::server::sparql_http::plan_update(coord.state, query, default_graph, &graphs)
            .await
        {
            Ok(planned) => planned,
            Err(error) => return Err(Response::err(coord.req_id, error)),
        };
    let plan = SparqlRecoveryPlanV1 {
        schema_version: 1,
        graphs: planned,
    };
    let saga = begin_sealed_sparql_saga(coord, &plan, coord.parent_id, SPARQL_RECOVERY_EVENT)?;
    Ok((saga, plan))
}

/// Resolve this request's parent saga and its sealed plan: resume the one an
/// earlier attempt began, or build a new one.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn stage_sparql_update(
    coord: &SparqlUpdateCoordination<'_>,
    query: &str,
    default_graph: &str,
) -> Result<(handlers::admin::AdminSaga, SparqlRecoveryPlanV1), Response> {
    let resumed = match handlers::admin::resume_named_admin_saga(
        coord.redb,
        coord.parent_id,
        Some(coord.verified_actor),
    ) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(coord.req_id, error)),
    };
    let (saga, plan) = match resumed {
        Some(saga) => resume_sparql_update_plan(coord, saga).await?,
        None => build_sparql_update_plan(coord, query, default_graph).await?,
    };
    if plan.schema_version != 1 {
        return Err(Response::err(
            coord.req_id,
            "unsupported SPARQL recovery plan",
        ));
    }
    Ok((saga, plan))
}

/// Create every graph the plan introduces that is not already resident, through
/// the ordinary dispatch path so lifecycle stays durable and authorized.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn create_missing_sparql_graphs(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlanV1,
) -> Result<(), Response> {
    for update in plan.graphs.iter().filter(|update| !update.existed_before) {
        if timed_read(coord.state).await.registry.exists(&update.graph) {
            continue;
        }
        let request = Request {
            id: coord.req_id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(coord.verified_actor.to_string()),
            method: Method::CreateGraph {
                graph_name: update.graph.clone(),
                graph_type: update.graph_type,
            },
        };
        let response = Box::pin(dispatch_with_context(
            coord.state,
            request,
            Some(VerifiedRequestContext::clone(coord.verified_context)),
        ))
        .await;
        if let Some(error) = response.error {
            return Err(Response::err(coord.req_id, error));
        }
    }
    Ok(())
}

#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
fn sparql_forward_methods(
    plan: &SparqlRecoveryPlanV1,
) -> Vec<(String, crate::protocol::GraphType, Vec<Method>)> {
    plan.graphs
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
        .collect()
}

#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
fn sparql_rollback_methods(
    plan: &SparqlRecoveryPlanV1,
) -> Vec<(String, crate::protocol::GraphType, Vec<Method>)> {
    plan.graphs
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
        .collect::<Vec<_>>()
}

/// Close the parent saga on the roll-forward path and clear its retained
/// decision.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn finish_sparql_commit(
    coord: &SparqlUpdateCoordination<'_>,
    saga: handlers::admin::AdminSaga,
    plan: &SparqlRecoveryPlanV1,
) -> Response {
    let result = ResultPayload::Json(serde_json::json!({
        "outcome": "committed",
        "updated_graphs": plan.graphs.len(),
        "created_graphs": plan.graphs.iter().filter(|graph| !graph.existed_before).count(),
    }));
    let committed = match handlers::admin::finish_admin_saga(
        coord.redb,
        saga.batch,
        saga.created_at_ms,
        result,
    ) {
        Ok(committed) => committed,
        Err(error) => return Response::err(coord.req_id, error),
    };
    match clear_coordinated_graph_decision(coord.redb, coord.parent_id).await {
        Ok(_) => Response::ok(coord.req_id, committed),
        Err(error) => Response::err(coord.req_id, error),
    }
}

/// What the roll-forward attempt decided.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
enum SparqlForwardOutcome {
    /// This request is finished; the response is final.
    Settled(Box<Response>),
    /// The forward commit did not take. Compensate, carrying the parent saga
    /// and the durable compensation marker that pins the direction.
    Compensate(Box<(handlers::admin::AdminSaga, handlers::admin::AdminSaga)>),
}

/// Roll forward: create the plan's new graphs, commit every after-image, and on
/// success close the parent saga. Anything else opens the compensation marker.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn try_sparql_forward_commit(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlanV1,
    saga: handlers::admin::AdminSaga,
) -> SparqlForwardOutcome {
    if let Err(response) = create_missing_sparql_graphs(coord, plan).await {
        return SparqlForwardOutcome::Settled(Box::new(response));
    }
    let forward = handlers::txn::commit_coordinated_graph_methods(
        coord.state,
        coord.req_id,
        Some(coord.verified_actor),
        coord.parent_id,
        sparql_forward_methods(plan),
    )
    .await;
    if forward.error.is_none() && !matches!(forward.result, Some(ResultPayload::Bool(false))) {
        return SparqlForwardOutcome::Settled(Box::new(
            finish_sparql_commit(coord, saga, plan).await,
        ));
    }
    let marker_plan = SparqlRecoveryPlanV1 {
        schema_version: 1,
        graphs: Vec::new(),
    };
    match begin_sealed_sparql_saga(
        coord,
        &marker_plan,
        coord.compensation_id,
        SPARQL_COMPENSATION_EVENT,
    ) {
        Ok(marker) => SparqlForwardOutcome::Compensate(Box::new((saga, marker))),
        Err(response) => SparqlForwardOutcome::Settled(Box::new(response)),
    }
}

/// Restore every before-image of a graph that already existed.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn apply_sparql_rollback(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlanV1,
) -> Result<(), Response> {
    let rollback_methods = sparql_rollback_methods(plan);
    if rollback_methods.is_empty() {
        return Ok(());
    }
    let rollback = handlers::txn::commit_coordinated_graph_methods(
        coord.state,
        coord.req_id,
        Some(coord.verified_actor),
        coord.compensation_id,
        rollback_methods,
    )
    .await;
    if let Some(error) = rollback.error {
        return Err(Response::err(
            coord.req_id,
            format!("SPARQL compensation pending: {error}"),
        ));
    }
    if matches!(rollback.result, Some(ResultPayload::Bool(false))) {
        return Err(Response::err(
            coord.req_id,
            "SPARQL compensation decision aborted",
        ));
    }
    Ok(())
}

/// Drop every graph the plan created, in REVERSE plan order.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn delete_created_sparql_graphs(
    coord: &SparqlUpdateCoordination<'_>,
    plan: &SparqlRecoveryPlanV1,
) -> Result<(), Response> {
    for update in plan
        .graphs
        .iter()
        .rev()
        .filter(|update| !update.existed_before)
    {
        if !timed_read(coord.state).await.registry.exists(&update.graph) {
            continue;
        }
        let request = Request {
            id: coord.req_id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(coord.verified_actor.to_string()),
            method: Method::DeleteGraph {
                graph_name: update.graph.clone(),
            },
        };
        let response = Box::pin(dispatch_with_context(
            coord.state,
            request,
            Some(VerifiedRequestContext::clone(coord.verified_context)),
        ))
        .await;
        if let Some(error) = response.error {
            return Err(Response::err(
                coord.req_id,
                format!("SPARQL compensation pending: {error}"),
            ));
        }
    }
    Ok(())
}

/// Close the compensation marker, then the parent saga, then clear both
/// retained decisions. The request answers as a durable compensation.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn finish_sparql_compensation(
    coord: &SparqlUpdateCoordination<'_>,
    saga: handlers::admin::AdminSaga,
    compensation_saga: Option<handlers::admin::AdminSaga>,
) -> Response {
    if let Some(marker) = compensation_saga {
        if let Err(error) = handlers::admin::finish_admin_saga(
            coord.redb,
            marker.batch,
            marker.created_at_ms,
            ResultPayload::Bool(true),
        ) {
            return Response::err(coord.req_id, error);
        }
    }
    let result = ResultPayload::Json(serde_json::json!({
        "outcome": "compensated",
        "updated_graphs": 0,
        "created_graphs": 0,
    }));
    if let Err(error) =
        handlers::admin::finish_admin_saga(coord.redb, saga.batch, saga.created_at_ms, result)
    {
        return Response::err(coord.req_id, error);
    }
    if let Err(error) = clear_coordinated_graph_decision(coord.redb, coord.parent_id).await {
        return Response::err(coord.req_id, error);
    }
    if let Err(error) = clear_coordinated_graph_decision(coord.redb, coord.compensation_id).await {
        return Response::err(coord.req_id, error);
    }
    Response::err(coord.req_id, "SPARQL update was durably compensated")
}

/// Drive the staged saga: roll forward once, and otherwise compensate. A
/// compensation marker that ALREADY exists pins the direction — the forward
/// attempt is skipped entirely, so a restart can never alternate.
#[cfg(all(feature = "sparql-http", feature = "redb", feature = "security"))]
async fn run_coordinated_sparql_http_update(
    coord: &SparqlUpdateCoordination<'_>,
    query: &str,
    default_graph: &str,
) -> Response {
    let (saga, plan) = match stage_sparql_update(coord, query, default_graph).await {
        Ok(value) => value,
        Err(response) => return response,
    };
    let existing_marker = match handlers::admin::resume_named_admin_saga(
        coord.redb,
        coord.compensation_id,
        Some(coord.verified_actor),
    ) {
        Ok(value) => value,
        Err(error) => return Response::err(coord.req_id, error),
    };
    let (saga, compensation_saga) = match existing_marker {
        Some(marker) => (saga, Some(marker)),
        None => match try_sparql_forward_commit(coord, &plan, saga).await {
            SparqlForwardOutcome::Settled(response) => return *response,
            SparqlForwardOutcome::Compensate(pair) => {
                let (saga, marker) = *pair;
                (saga, Some(marker))
            }
        },
    };
    if let Err(response) = apply_sparql_rollback(coord, &plan).await {
        return response;
    }
    if let Err(response) = delete_created_sparql_graphs(coord, &plan).await {
        return response;
    }
    finish_sparql_compensation(coord, saga, compensation_saga).await
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
    let coord = SparqlUpdateCoordination {
        state,
        req_id,
        verified_context,
        verified_actor,
        redb,
        parent_id: &parent_id,
        compensation_id: &compensation_id,
    };
    run_coordinated_sparql_http_update(&coord, &query, default_graph).await
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
    if let Some(response) =
        change_envelope_batch_authority_error(req_id, verified_context, &envelopes)
    {
        return response;
    }

    let mut per_index: Vec<serde_json::Value> = vec![serde_json::Value::Null; total];
    for (graph, group) in group_change_envelopes_by_graph(envelopes) {
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
        scatter_change_envelope_group_results(&mut per_index, &indices, resp);
    }

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({ "results": per_index })),
    )
}

/// Per-envelope authority binding — mirrors the single `ApplyChangeEnvelope`
/// arm, minus the two batch-varying fields: the idempotency_key (per envelope,
/// enforced by the mutation-store idempotency table) and the graph (per
/// envelope, ACL-checked per group by `dispatch_graph_op`).
#[cfg(feature = "redb")]
fn change_envelope_batch_authority_error(
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    envelopes: &[crate::change_envelope::ChangeEnvelope],
) -> Option<Response> {
    let claims = verified_context.claims();
    let principal = verified_context.principal_persistence_id();
    for envelope in envelopes {
        let ctx = &envelope.mutation.context;
        if envelope.mutation.tenant != claims.tenant
            || ctx.request_id != req_id
            || ctx.principal != principal
            || ctx.policy_fingerprint.as_deref() != Some(claims.policy_version.as_str())
        {
            return Some(Response::err(
                req_id,
                "ApplyChangeEnvelopes context does not match the verified request authority",
            ));
        }
    }
    None
}

/// Group envelopes by graph, preserving first-seen graph order and the
/// per-graph envelope order, and carrying each envelope's REQUEST index so the
/// per-graph results can be scattered back into request order.
#[cfg(feature = "redb")]
fn group_change_envelopes_by_graph(
    envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> Vec<(String, Vec<(usize, crate::change_envelope::ChangeEnvelope)>)> {
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
    graph_order
        .into_iter()
        .map(|graph| {
            let group = groups.remove(&graph).expect("grouped graph is present");
            (graph, group)
        })
        .collect()
}

/// Scatter one graph group's response back into request-ordered slots.
#[cfg(feature = "redb")]
fn scatter_change_envelope_group_results(
    per_index: &mut [serde_json::Value],
    indices: &[usize],
    response: Response,
) {
    if let Some(err) = response.error {
        // A transport/ACL/placement failure for the whole group (distinct from the
        // per-envelope atomic-batch abort, which returns Ok with conflict entries).
        for index in indices {
            per_index[*index] = serde_json::json!({ "status": "conflict", "error": err });
        }
        return;
    }
    let Some(ResultPayload::Json(value)) = response.result else {
        for index in indices {
            per_index[*index] = serde_json::json!({
                "status": "conflict",
                "error": "empty batch response",
            });
        }
        return;
    };
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
    let saga = match begin_multi_graph_saga(redb, req_id, caller, batches_msgpack, clustered) {
        Ok(saga) => saga,
        Err(response) => return response,
    };
    let (results, errors) = if batches.is_empty() {
        (serde_json::Map::new(), serde_json::Map::new())
    } else {
        run_multi_graph_batches(state, req_id, caller, verified_context, batches).await
    };
    let result = ResultPayload::Json(serde_json::json!({"results": results, "errors": errors}));
    finish_multi_graph_batch(redb, req_id, saga, result)
}

/// Open the durable admin saga that makes a single-node multi-graph batch
/// idempotent. A clustered node has no saga (raft already orders each
/// sub-batch). `Err` is this request's final response — a begin failure, or the
/// replayed result of an attempt that already committed.
#[cfg(feature = "redb")]
fn begin_multi_graph_saga(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    batches_msgpack: &[u8],
    clustered: bool,
) -> Result<Option<handlers::admin::AdminSaga>, Response> {
    if clustered {
        return Ok(None);
    }
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
        Err(error) => return Err(Response::err(req_id, error)),
    };
    if let Some(result) = saga.replayed.clone() {
        return Err(Response::ok(req_id, result));
    }
    Ok(Some(saga))
}

/// Close the saga (when there is one) over the assembled partial-success reply.
#[cfg(feature = "redb")]
fn finish_multi_graph_batch(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    saga: Option<handlers::admin::AdminSaga>,
    result: ResultPayload,
) -> Response {
    let Some(saga) = saga else {
        return Response::ok(req_id, result);
    };
    match handlers::admin::finish_admin_saga(redb, saga.batch, saga.created_at_ms, result) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

/// Record one sub-batch's outcome. A graph's failure lands in `errors`; a
/// success lands in `results`, with a non-JSON payload recorded as null so the
/// reply always names every graph exactly once.
#[cfg(feature = "redb")]
fn record_multi_graph_result(
    results: &mut serde_json::Map<String, serde_json::Value>,
    errors: &mut serde_json::Map<String, serde_json::Value>,
    graph: String,
    response: Response,
) {
    if let Some(err) = response.error {
        errors.insert(graph, serde_json::Value::String(err));
    } else if let Some(ResultPayload::Json(value)) = response.result {
        results.insert(graph, value);
    } else {
        results.insert(graph, serde_json::Value::Null);
    }
}

/// Fan each sub-batch onto its own task so distinct graphs apply concurrently.
/// The `Arc<RwLock<ServerState>>` is cheaply cloned; `dispatch_graph_op` takes
/// the registry read-lock only briefly then releases it before the per-graph
/// write lock, so the writes overlap across shard writers.
#[cfg(feature = "redb")]
async fn run_multi_graph_batches(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    batches: Vec<(String, serde_bytes::ByteBuf)>,
) -> (
    serde_json::Map<String, serde_json::Value>,
    serde_json::Map<String, serde_json::Value>,
) {
    let mut results = serde_json::Map::new();
    let mut errors = serde_json::Map::new();
    let caller_owned = caller.map(str::to_string);
    let mut set = tokio::task::JoinSet::new();
    for (graph, ops) in batches {
        let state = Arc::clone(state);
        let caller_owned = caller_owned.clone();
        let verified_context = VerifiedRequestContext::clone(verified_context);
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
            Ok((graph, resp)) => record_multi_graph_result(&mut results, &mut errors, graph, resp),
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
    (results, errors)
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

/// The route, graph and verified identity a replicated modality mutation is
/// bound to. Bundled so each phase helper below stays inside the parameter cap.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
struct ModalityReplication<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    handle: &'a crate::raft::RaftHandle,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    graph_name: &'a str,
    graph_type: crate::protocol::GraphType,
    req_id: u64,
    tenant_scope: &'a str,
    principal_fingerprint: &'a str,
    core: &'a Arc<crate::graph::GraphCore>,
    persistence: &'a Arc<dyn crate::server::persistence::PersistenceBackend>,
}

/// The sanitized, source-free facts derived from the public method. The
/// source-bearing `ServedModalityOp` is deliberately NOT carried here — it is
/// handed to the modality handler once and never reaches `RaftRequest`.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
struct ModalityMutationInputs {
    operation: crate::raft::SanitizedModalityMutation,
    modality: eg_types::ServedModalityKind,
    receipt_query: String,
    safe_method: Method,
}

/// Only the current placement leader may prepare a modality mutation; a
/// follower answers with the ordinary redirect.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn modality_leader_fence(ctx: &ModalityReplication<'_>) -> Option<Response> {
    let leader = ctx.handle.current_leader().await;
    if leader == Some(ctx.handle.node_id) {
        return None;
    }
    Some(Response::stale_route(
        ctx.req_id,
        ctx.graph_name,
        ctx.group_id,
        ctx.placement_epoch,
        leader,
        "served modality mutations require the current placement leader",
    ))
}

/// Split the public method into the source-bearing op (consumed by the handler)
/// and the digest-only receipt facts consensus actually sees.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn decode_modality_replication_inputs(
    req_id: u64,
    method: Method,
) -> Result<(eg_types::ServedModalityOp, ModalityMutationInputs), Response> {
    let safe_method = crate::server::mutation::durable_receipt_method(&method);
    let Method::ServedModality { op } = method else {
        return Err(Response::err(
            req_id,
            "served modality replication received the wrong method",
        ));
    };
    let Some((operation, modality)) = crate::raft::SanitizedModalityMutation::from_served(&op)
    else {
        return Err(Response::err(
            req_id,
            "served modality mutation category is invalid",
        ));
    };
    let receipt_query = match &safe_method {
        Method::ApplyMutation { event_type, query } if event_type == "served_modality_v1" => {
            query.clone()
        }
        _ => {
            return Err(Response::err(
                req_id,
                "served modality receipt construction failed",
            ))
        }
    };
    Ok((
        op,
        ModalityMutationInputs {
            operation,
            modality,
            receipt_query,
            safe_method,
        },
    ))
}

/// The stored receipt must carry exactly ONE authoritative-state operation whose
/// digest is this request's own.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn modality_receipt_operation_matches(
    record: &crate::mutation_batch::MutationBatchRecord,
    expected_operation: &str,
) -> bool {
    record.batch.operations.len() == 1
        && matches!(
            &record.batch.operations[0].method,
            Method::ApplyMutation { event_type, query }
                if event_type == "authoritative_state_operation"
                    && query == expected_operation
        )
}

/// The stored receipt must also be committed and bound to this request's
/// batch, tenant, graph, placement fence and principal.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn modality_receipt_binding_matches(
    ctx: &ModalityReplication<'_>,
    record: &crate::mutation_batch::MutationBatchRecord,
    batch_id: &str,
) -> bool {
    record.status == crate::mutation_batch::MutationBatchStatus::Committed
        && record.batch.batch_id == batch_id
        && record.batch.tenant == ctx.tenant_scope
        && record.batch.graph == ctx.graph_name
        && record.batch.placement_epoch == ctx.placement_epoch
        && record.batch.fencing_token == ctx.fencing_token
        && record.batch.context.principal == ctx.principal_fingerprint
}

/// Raft apply authenticated the sealed runtime state and result digest before
/// committing the record. The retry record intentionally retains only the safe
/// result bytes, so replay reuses the same canonical typed decoder to reject
/// receipt tampering without ever reconstructing or exposing the sealed/source
/// material.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
fn decode_modality_replay_result(
    req_id: u64,
    inputs: &ModalityMutationInputs,
    record: &crate::mutation_batch::MutationBatchRecord,
) -> Result<ResultPayload, Response> {
    let Some(encoded) = record.result_msgpack.as_deref() else {
        return Err(Response::err(
            req_id,
            "replicated modality receipt has no terminal result",
        ));
    };
    crate::raft::decode_sanitized_modality_result(inputs.modality, inputs.operation, encoded)
        .map_err(|_| Response::err(req_id, "replicated modality receipt has an invalid result"))
}

/// Repair RAM from the durably committed image before answering a replay.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn install_modality_committed_image(
    ctx: &ModalityReplication<'_>,
    graph_fname: &str,
    result: ResultPayload,
) -> Response {
    match ctx
        .persistence
        .read_authoritative_graph_snapshot(graph_fname)
        .await
    {
        Ok(Some((snapshot, version))) => {
            match ctx.core.install_committed_snapshot(snapshot, version) {
                Ok(()) => Response::ok(ctx.req_id, result),
                Err(error) => Response::err(ctx.req_id, error),
            }
        }
        Ok(None) => Response::err(ctx.req_id, "committed modality image is missing"),
        Err(error) => Response::err(ctx.req_id, error),
    }
}

/// A client retry with the same request id repairs RAM from the committed image
/// and returns the exact stored ApplyOutcome. It never re-decodes source bytes
/// or emits a second Raft entry/outbox/audit/CDC event. `None` ⇒ no receipt yet,
/// so the caller proceeds with a fresh mutation.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn try_replay_modality_receipt(
    ctx: &ModalityReplication<'_>,
    inputs: &ModalityMutationInputs,
    batch_id: &str,
    graph_fname: &str,
) -> Option<Response> {
    use sha2::{Digest, Sha256};

    let record = match ctx
        .persistence
        .read_mutation_batch(graph_fname, batch_id)
        .await
    {
        Ok(Some(record)) => record,
        Ok(None) => return None,
        Err(error) => return Some(Response::err(ctx.req_id, error)),
    };
    let encoded = match rmp_serde::to_vec_named(&inputs.safe_method) {
        Ok(encoded) => encoded,
        Err(error) => return Some(Response::err(ctx.req_id, error.to_string())),
    };
    let expected_operation = format!("sha256:{}", hex::encode(Sha256::digest(encoded)));
    if !modality_receipt_binding_matches(ctx, &record, batch_id)
        || !modality_receipt_operation_matches(&record, &expected_operation)
    {
        return Some(Response::err(
            ctx.req_id,
            "replicated modality receipt conflicts with request authority",
        ));
    }
    let result = match decode_modality_replay_result(ctx.req_id, inputs, &record) {
        Ok(result) => result,
        Err(response) => return Some(response),
    };
    Some(install_modality_committed_image(ctx, graph_fname, result).await)
}

/// Stage the mutation over the authoritative committed image. When no durable
/// snapshot exists yet, fall back to the resident core at the durable version.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn stage_modality_base_core(
    ctx: &ModalityReplication<'_>,
    graph_fname: &str,
) -> Result<crate::graph::GraphCore, Response> {
    let (base_snapshot, source_version) = match ctx
        .persistence
        .read_authoritative_graph_snapshot(graph_fname)
        .await
    {
        Ok(Some(value)) => value,
        Ok(None) => {
            let version = match ctx
                .persistence
                .read_mutation_graph_version(graph_fname)
                .await
            {
                Ok(value) => value.unwrap_or_else(|| ctx.core.version()),
                Err(error) => return Err(Response::err(ctx.req_id, error)),
            };
            (ctx.core.snapshot(), version)
        }
        Err(error) => return Err(Response::err(ctx.req_id, error)),
    };
    crate::graph::GraphCore::from_snapshot(base_snapshot, source_version)
        .map_err(|error| Response::err(ctx.req_id, error))
}

/// Seal the staged runtime node into the HMAC-authenticated command consensus
/// receives. The staged node MUST already be sealed — an unsealed runtime state
/// is refused rather than replicated.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn build_modality_raft_command(
    ctx: &ModalityReplication<'_>,
    inputs: &ModalityMutationInputs,
    staged: &crate::graph::GraphCore,
    authority: &handlers::modality::ModalityAuthority,
    payload: &ResultPayload,
) -> Result<crate::raft::SanitizedModalityRaftCommand, Response> {
    let node_id = authority.node_id(inputs.modality);
    let Some(sealed_runtime_state) = staged.get_node_properties(&node_id) else {
        return Err(Response::err(
            ctx.req_id,
            "served modality produced no encrypted state",
        ));
    };
    if !crate::crypto::is_sealed(&sealed_runtime_state) {
        return Err(Response::err(
            ctx.req_id,
            "served modality produced unsealed state",
        ));
    }
    let result_msgpack = match rmp_serde::to_vec_named(payload) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(ctx.req_id, error.to_string())),
    };
    let server_secret = timed_read(ctx.state).await.auth_secret.clone();
    crate::raft::SanitizedModalityRaftCommand::new(
        &server_secret,
        inputs.modality,
        inputs.operation,
        node_id,
        sealed_runtime_state,
        inputs.receipt_query.clone(),
        result_msgpack,
    )
    .map_err(|error| Response::err(ctx.req_id, error))
}

/// Propose the sanitized command and answer with the prepared payload once the
/// entry has applied.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn submit_modality_replication(
    ctx: &ModalityReplication<'_>,
    batch_id: String,
    graph_fname: String,
    command: crate::raft::SanitizedModalityRaftCommand,
    payload: ResultPayload,
) -> Response {
    let created_at_ms = authoritative_now_ms();
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        ctx.req_id,
        ctx.tenant_scope,
        ctx.principal_fingerprint.to_string(),
        false,
        ctx.placement_epoch,
        ctx.fencing_token,
        created_at_ms,
    ) {
        Ok(context) => context,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let request = crate::raft::RaftRequest {
        graph_fname,
        graph_name: ctx.graph_name.to_string(),
        graph_type: ctx.graph_type,
        command: crate::raft::ReplicatedMutation::served_modality(command),
        committed_at_ms: created_at_ms,
        mutation,
    };
    match ctx.handle.client_write(request).await {
        Ok(response) if response.applied => Response::ok(ctx.req_id, payload),
        Ok(_) => Response::err(ctx.req_id, "replicated modality state was not applied"),
        Err(error) => {
            let leader = ctx.handle.current_leader().await;
            Response::stale_route(
                ctx.req_id,
                ctx.graph_name,
                ctx.group_id,
                ctx.placement_epoch,
                leader,
                error,
            )
        }
    }
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
    let ctx = ModalityReplication {
        state,
        handle: &handle,
        group_id,
        placement_epoch,
        fencing_token,
        graph_name,
        graph_type,
        req_id,
        tenant_scope,
        principal_fingerprint,
        core,
        persistence,
    };
    if let Some(stale) = modality_leader_fence(&ctx).await {
        return stale;
    }

    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    let (op, inputs) = match decode_modality_replication_inputs(req_id, method) {
        Ok(decoded) => decoded,
        Err(response) => return response,
    };
    let batch_id = crate::server::mutation_batch::opaque_request_key(
        "raft-modality",
        graph_name,
        req_id,
        &inputs.safe_method,
    );
    let graph_fname = crate::persist::sanitize(graph_name);

    if let Some(replayed) =
        try_replay_modality_receipt(&ctx, &inputs, &batch_id, &graph_fname).await
    {
        return replayed;
    }

    prepare_and_replicate_modality(&ctx, &inputs, op, authority, batch_id, graph_fname).await
}

/// No receipt exists yet: stage the mutation over the committed image, run the
/// modality handler, seal the result, and propose it.
#[cfg(all(feature = "raft", feature = "modality-serving"))]
async fn prepare_and_replicate_modality(
    ctx: &ModalityReplication<'_>,
    inputs: &ModalityMutationInputs,
    op: eg_types::ServedModalityOp,
    authority: &handlers::modality::ModalityAuthority,
    batch_id: String,
    graph_fname: String,
) -> Response {
    let staged = match stage_modality_base_core(ctx, &graph_fname).await {
        Ok(staged) => staged,
        Err(response) => return response,
    };
    let payload = match handlers::modality::handle(&staged, authority, op) {
        Ok(payload) => payload,
        Err(error) => return Response::err(ctx.req_id, error),
    };
    let command = match build_modality_raft_command(ctx, inputs, &staged, authority, &payload).await
    {
        Ok(command) => command,
        Err(response) => return response,
    };
    submit_modality_replication(ctx, batch_id, graph_fname, command, payload).await
}

#[cfg(all(test, feature = "raft", feature = "modality-serving"))]
mod modality_replay_receipt_tests {
    use super::*;

    fn single_wire() -> Vec<u8> {
        let outcome = eg_modality::ApplyOutcome {
            disposition: eg_modality::ApplyDisposition::Applied,
            observation_version: 11,
            event_sequence: 17,
        };
        rmp_serde::to_vec_named(&ResultPayload::raw(&outcome)).unwrap()
    }

    fn stream_wire() -> Vec<u8> {
        let outcomes = vec![
            eg_modality::ApplyOutcome {
                disposition: eg_modality::ApplyDisposition::Applied,
                observation_version: 11,
                event_sequence: 17,
            },
            eg_modality::ApplyOutcome {
                disposition: eg_modality::ApplyDisposition::IdempotentReplay,
                observation_version: 12,
                event_sequence: 18,
            },
        ];
        rmp_serde::to_vec_named(&ResultPayload::raw(&outcomes)).unwrap()
    }

    #[test]
    fn replay_decoder_accepts_single_receipt() {
        let payload = crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::Ingest,
            &single_wire(),
        )
        .unwrap();
        let (ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes)) = payload else {
            panic!("typed replay receipt must remain a compact byte payload");
        };
        let outcome: eg_modality::ApplyOutcome = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(outcome.observation_version, 11);
        assert_eq!(outcome.event_sequence, 17);
    }

    #[test]
    fn replay_decoder_accepts_bounded_stream_receipt() {
        let payload = crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::IngestStream,
            &stream_wire(),
        )
        .unwrap();
        let (ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes)) = payload else {
            panic!("typed replay receipt must remain a compact byte payload");
        };
        let outcomes: Vec<eg_modality::ApplyOutcome> = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(outcomes.len(), 2);
        assert_eq!(
            outcomes[1].disposition,
            eg_modality::ApplyDisposition::IdempotentReplay
        );
    }

    #[test]
    fn replay_decoder_rejects_wrong_shape_and_oversized_receipts() {
        let wrong_payload = rmp_serde::to_vec_named(&ResultPayload::Bool(true)).unwrap();
        assert!(crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::Ingest,
            &wrong_payload,
        )
        .is_err());

        // A stream operation requires the typed stream result; a single outcome
        // must not be silently reinterpreted as a one-item stream.
        assert!(crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::IngestStream,
            &single_wire(),
        )
        .is_err());

        let outcome = eg_modality::ApplyOutcome {
            disposition: eg_modality::ApplyDisposition::Applied,
            observation_version: 1,
            event_sequence: 1,
        };
        let oversized = vec![outcome; 65];
        let oversized_wire = rmp_serde::to_vec_named(&ResultPayload::raw(&oversized)).unwrap();
        assert!(crate::raft::decode_sanitized_modality_result(
            eg_types::ServedModalityKind::Document,
            crate::raft::SanitizedModalityMutation::IngestStream,
            &oversized_wire,
        )
        .is_err());
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

/// A native durable read must run on the CURRENT placement leader and behind a
/// read barrier, or a follower would answer from its own stale log. `stale_hint`
/// and `barrier_failure` name the surface in the two refusals so each caller's
/// message is unchanged.
#[cfg(feature = "raft")]
async fn enforce_native_read_leadership(
    req_id: u64,
    graph_name: &str,
    multi_raft: Option<&std::sync::Arc<crate::raft::multi::MultiRaft>>,
    routed_raft: Option<&crate::raft::multi::RoutedRaftHandle>,
    stale_hint: &str,
    barrier_failure: &str,
) -> Result<(), Response> {
    let Some(routed) = routed_raft else {
        return Ok(());
    };
    let leader = routed.handle.current_leader().await;
    if leader != Some(routed.handle.node_id) {
        return Err(Response::stale_route(
            req_id,
            graph_name,
            routed.group_id,
            routed.epoch,
            leader,
            stale_hint,
        ));
    }
    let Some(multi) = multi_raft else {
        return Ok(());
    };
    match multi.read_barrier_group(routed.group_id).await {
        Ok(_) => Ok(()),
        Err(error) => Err(Response::err(
            req_id,
            format!("{barrier_failure}: {error:?}"),
        )),
    }
}

async fn dispatch_op_resource_reservation_query(
    req_id: u64,
    graph_name: &str,
    verified_context: &VerifiedRequestContext,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "raft")] multi_raft: Option<std::sync::Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "raft")] routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    method: Method,
) -> Response {
    #[cfg(feature = "raft")]
    if let Err(response) = enforce_native_read_leadership(
        req_id,
        graph_name,
        multi_raft.as_ref(),
        routed_raft.as_ref(),
        "native reservation reads require the current placement leader",
        "native reservation read linearizability barrier failed",
    )
    .await
    {
        return response;
    }
    let Some(backend) = persistence.as_ref() else {
        return Response::err(req_id, "native reservation persistence is unavailable");
    };
    let fname = crate::persist::sanitize(graph_name);
    match &method {
        crate::protocol::Method::QueryWorkItemReservation { request } => {
            match backend.read_resource_reservation(&fname, request).await {
                Ok(result) => Response::ok(req_id, ResultPayload::raw(&result)),
                Err(error) => {
                    Response::err(req_id, format!("native reservation read failed: {error}"))
                }
            }
        }
        crate::protocol::Method::ResourceReservationStatus { request } => {
            let aggregate_reader = verified_context.allows_action("resource:read:aggregate")
                || verified_context.allows_action("kg:admin");
            match backend
                .read_resource_reservation_status(&fname, request)
                .await
            {
                Ok(result) => Response::ok(
                    req_id,
                    redact_resource_status_result(result, request, aggregate_reader),
                ),
                Err(error) => Response::err(
                    req_id,
                    format!("native reservation status read failed: {error}"),
                ),
            }
        }
        _ => unreachable!("resource query classifier and dispatch method diverged"),
    }
}

/// The authenticated, placement-resolved routing context the native durable-op
/// dispatchers below share (capacity leases, WorkItem claim capability). These
/// values are resolved once per request in `dispatch_graph_op` and travel
/// together; bundling them keeps each dispatcher's parameter list at the
/// documented cap, the same shape as `crate::server::mutation::MutationCtx`.
struct NativeOpCtx<'a> {
    req_id: u64,
    graph_name: &'a str,
    verified_context: &'a VerifiedRequestContext,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "raft")]
    multi_raft: Option<std::sync::Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "raft")]
    routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
}

/// A capacity lease may only be taken, renewed or released by the principal
/// that owns it; every other capacity method is owner-agnostic.
fn capacity_owner_matches(method: &Method, verified_context: &VerifiedRequestContext) -> bool {
    let verified_owner = verified_context.principal_persistence_id();
    match method {
        Method::AcquireCapacity { request } => request.owner_digest == verified_owner,
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            request.owner_digest == verified_owner
        }
        _ => true,
    }
}

async fn dispatch_op_capacity_ops(
    ctx: NativeOpCtx<'_>,
    state_machine_authorized: bool,
    method: Method,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let verified_context = ctx.verified_context;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let multi_raft = ctx.multi_raft;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    if let Err(response) = enforce_native_read_leadership(
        req_id,
        graph_name,
        multi_raft.as_ref(),
        routed_raft.as_ref(),
        "native capacity operations require the current placement leader",
        "native capacity linearizability barrier failed",
    )
    .await
    {
        return response;
    }
    let Some(backend) = persistence.as_ref() else {
        return Response::err(req_id, "native capacity persistence is unavailable");
    };
    if !backend.supports_native_capacity_leases() {
        return Response::err(req_id, "native capacity persistence is unavailable");
    }
    if !state_machine_authorized && !capacity_owner_matches(&method, verified_context) {
        return Response::err(req_id, "ACCESS_DENIED: capacity owner digest mismatch");
    }
    let fname = crate::persist::sanitize(graph_name);
    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    match method {
        Method::CapacityStatus { ref request } | Method::ReconcileCapacity { ref request } => {
            backend
                .read_capacity_status(&fname, request)
                .await
                .map(|result| Response::ok(req_id, ResultPayload::raw(&result)))
                .unwrap_or_else(|error| {
                    Response::err(
                        req_id,
                        format!("native capacity status read failed: {error}"),
                    )
                })
        }
        method @ (Method::AcquireCapacity { .. }
        | Method::RenewCapacity { .. }
        | Method::ReleaseCapacity { .. }
        | Method::ReclaimExpiredCapacity { .. }
        | Method::UpdateCapacityCell { .. }) => backend
            .commit_capacity_lease(&fname, method)
            .await
            .map(|bytes| Response::ok(req_id, ResultPayload::Raw(bytes)))
            .unwrap_or_else(|error| {
                Response::err(req_id, format!("native capacity commit failed: {error}"))
            }),
        _ => unreachable!("capacity classifier and dispatch diverged"),
    }
}

async fn dispatch_op_workitem_claim_capability(
    ctx: NativeOpCtx<'_>,
    #[cfg(feature = "redb")] graph_incarnation_id: String,
    method: Method,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let verified_context = ctx.verified_context;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let multi_raft = ctx.multi_raft;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
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
                    format!("WorkItem claim capability linearizability barrier failed: {error:?}"),
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
        match method {
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
        }
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

async fn dispatch_op_development_lane(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "raft")] multi_raft: Option<std::sync::Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "raft")] routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    method: Method,
) -> Response {
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
    match method {
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
    }
}

async fn dispatch_op_workitem_mutation(
    req_id: u64,
    graph_name: &str,
    caller: Option<&str>,
    core: Arc<crate::graph::GraphCore>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "raft")] routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    method: Method,
) -> Response {
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

/// Fence a `series.redb` WRITE to the current placement leader.
///
/// `Some(response)` is the stale-route rejection the caller must return; `None`
/// means the fence passed and dispatch CONTINUES — the original inline form fell
/// through, so an unconditional `return` here would strand every `TsAppend`.
/// The method guard lives inside so the call site stays a single `if let`.
#[cfg(all(feature = "raft", feature = "tsdb"))]
async fn dispatch_op_tsdb_write_fence(
    req_id: u64,
    graph_name: &str,
    routed_raft: Option<&crate::raft::multi::RoutedRaftHandle>,
    method: &Method,
) -> Option<Response> {
    if !matches!(
        method,
        Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. }
    ) {
        return None;
    }
    let routed = routed_raft?;
    let leader = routed.handle.current_leader().await;
    if leader == Some(routed.handle.node_id) {
        return None;
    }
    Some(Response::stale_route(
        req_id,
        graph_name,
        routed.group_id,
        routed.epoch,
        leader,
        "time-series writes require the current placement leader",
    ))
}

/// Everything [`dispatch_op_knowledge_stream`] needs beyond the method itself:
/// the resolved request identity, the graph core it pulls from, and the
/// placement/RLS/authority handles the cursor must stay inside. Bundled to keep
/// the dispatcher at the documented parameter cap.
#[cfg(feature = "knowledge-batch")]
struct KnowledgeStreamCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    verified_context: &'a VerifiedRequestContext,
    read_authority: &'a Option<GraphReadAuthority>,
    verified_actor: &'a str,
    core: Arc<crate::graph::GraphCore>,
    #[cfg(feature = "security")]
    rls: std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(feature = "raft")]
    routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    knowledge_stream_authority: Option<handlers::knowledge_stream::KnowledgeStreamAuthority>,
}

#[cfg(feature = "knowledge-batch")]
async fn dispatch_op_knowledge_stream(ctx: KnowledgeStreamCtx<'_>, method: Method) -> Response {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let verified_context = ctx.verified_context;
    let read_authority = ctx.read_authority;
    let verified_actor = ctx.verified_actor;
    let core = ctx.core;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    let knowledge_stream_authority = ctx.knowledge_stream_authority;
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
    let (stream_placement_epoch, stream_fencing_token) = if let Some(routed) = routed_raft.as_ref()
    {
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

#[cfg(feature = "tsdb")]
async fn dispatch_op_tsdb_ops(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    verified_context: &VerifiedRequestContext,
    ts_placement_epoch: u64,
    ts_fencing_token: Option<u64>,
    method: Method,
) -> Response {
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

#[cfg(feature = "security")]
async fn dispatch_op_audit_verify(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
) -> Response {
    let fname = crate::persist::sanitize(graph_name);
    match persistence.as_ref().and_then(|p| p.as_redb()) {
        Some(redb) => match redb.audit_verify_blocking(&fname) {
            Ok(report) => Response::ok(req_id, ResultPayload::raw(&report)),
            Err(e) => Response::err(req_id, format!("AuditVerify error: {e}")),
        },
        None => Response::err(
            req_id,
            "AuditVerify requires a durable redb backend (no persist dir configured)".to_string(),
        ),
    }
}

#[cfg(feature = "security")]
async fn dispatch_op_audit_prove_inclusion(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    method: Method,
) -> Response {
    let Method::AuditProveInclusion {
        node_id,
        anchor_seq,
    } = &method
    else {
        unreachable!("dispatch_op_audit_prove_inclusion: classifier/handler diverged")
    };
    let fname = crate::persist::sanitize(graph_name);
    match persistence.as_ref().and_then(|p| p.as_redb()) {
        Some(redb) => match redb.audit_prove_inclusion_blocking(&fname, node_id, *anchor_seq) {
            Ok(report) => Response::ok(req_id, ResultPayload::raw(&report)),
            Err(e) => Response::err(req_id, format!("AuditProveInclusion error: {e}")),
        },
        None => Response::err(
            req_id,
            "AuditProveInclusion requires a durable redb backend (no persist dir configured)"
                .to_string(),
        ),
    }
}

/// Everything [`dispatch_op_served_modality`] needs beyond the method: the
/// resolved request identity, the gateway authz capture, the graph core and its
/// durability/CDC/materialization handles, plus the placement route and the
/// derived modality authority. Bundled to keep the dispatcher at the documented
/// parameter cap.
#[cfg(feature = "modality-serving")]
struct ServedModalityCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    caller: Option<&'a str>,
    verified_context: &'a VerifiedRequestContext,
    tenant_scope: &'a str,
    gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    core: Arc<crate::graph::GraphCore>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    #[cfg(feature = "raft")]
    routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    modality_authority: Option<handlers::modality::ModalityAuthority>,
}

#[cfg(feature = "modality-serving")]
async fn dispatch_op_served_modality(ctx: ServedModalityCtx<'_>, method: Method) -> Response {
    #[cfg(feature = "raft")]
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    #[cfg(feature = "raft")]
    let verified_context = ctx.verified_context;
    let tenant_scope = ctx.tenant_scope;
    let gateway_authz_ctx = ctx.gateway_authz_ctx;
    let core = ctx.core;
    let materialization_manifest = ctx.materialization_manifest;
    let persistence = ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = ctx.cdc;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    let modality_authority = ctx.modality_authority;
    let Method::ServedModality { op } = &method else {
        unreachable!("dispatch_op_served_modality: classifier/handler diverged")
    };
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
                tenant_scope,
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
        tenant_scope,
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

/// The resolved request identity the Raft write-routing barrier replays into
/// the consensus entry. Bundled to keep the barrier at the documented parameter
/// cap; `routed` and `method` stay explicit because the caller's
/// `if let Some(routed)` is what proves the barrier applies at all.
#[cfg(feature = "raft")]
struct RaftWriteBarrierCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    verified_context: &'a VerifiedRequestContext,
    tenant_scope: &'a str,
    graph_type: crate::protocol::GraphType,
}

/// Replicate one durable mutation through Raft consensus instead of applying it
/// locally.
///
/// Takes the ALREADY-UNWRAPPED [`RoutedRaftHandle`]: the barrier is only reachable
/// with a routed group, and the caller's `if let Some(routed)` is that proof. The
/// durability guard stays at the call site because a non-durable mutation must keep
/// ownership of `method` for the local pipeline below.
#[cfg(feature = "raft")]
async fn dispatch_op_raft_write_routing_barrier(
    ctx: RaftWriteBarrierCtx<'_>,
    routed: crate::raft::multi::RoutedRaftHandle,
    method: Method,
) -> Response {
    let RaftWriteBarrierCtx {
        state,
        req_id,
        graph_name,
        verified_context,
        tenant_scope,
        graph_type,
    } = ctx;
    let created_at_ms = authoritative_now_ms();
    let batch_id =
        crate::server::mutation_batch::opaque_request_key("raft-rpc", graph_name, req_id, &method);
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        req_id,
        tenant_scope,
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
    match routed.handle.client_write(req).await {
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
            Response::stale_route(req_id, graph_name, routed.group_id, routed.epoch, leader, e)
        }
    }
}

async fn check_change_envelope_placement_fence(
    req_id: u64,
    #[cfg(feature = "raft")] graph_name: &str,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    #[cfg(feature = "raft")] routed_raft: Option<&crate::raft::multi::RoutedRaftHandle>,
) -> Result<(), Response> {
    #[cfg(feature = "raft")]
    if let Some(routed) = routed_raft {
        let leader = routed.handle.current_leader().await;
        if leader != Some(routed.handle.node_id) {
            return Err(Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "ChangeEnvelope commits require the current placement leader",
            ));
        }
        if envelope.mutation.placement_epoch != routed.epoch
            || envelope.mutation.fencing_token != Some(routed.group_id)
        {
            return Err(Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "ChangeEnvelope placement epoch or fencing token is stale",
            ));
        }
    } else {
        if envelope.mutation.placement_epoch != 0 || envelope.mutation.fencing_token.is_some() {
            return Err(Response::err(
                req_id,
                "ChangeEnvelope carries a placement fence but no routed placement is active",
            ));
        }
    }
    #[cfg(not(feature = "raft"))]
    if envelope.mutation.placement_epoch != 0 || envelope.mutation.fencing_token.is_some() {
        return Err(Response::err(
            req_id,
            "ChangeEnvelope carries a placement fence in a single-node build",
        ));
    }
    Ok(())
}

/// The already-resolved graph/placement identity a replicated ChangeEnvelope
/// commit needs; the envelope and its leader-selected timestamp stay explicit
/// because they are what is actually being replicated.
#[cfg(feature = "raft")]
struct ChangeEnvelopeReplicaCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    graph_type: crate::protocol::GraphType,
    tenant_scope: &'a str,
    fname: &'a str,
    routed_raft: Option<&'a crate::raft::multi::RoutedRaftHandle>,
}

/// Attempt the clustered replication path for `ApplyChangeEnvelope`: one log
/// entry, one native transaction on every replica, leader-selected timestamp
/// for byte-stable replay/follower records. `Some(resp)` means the request
/// was fully handled (locally or via a stale-route redirect) by this path;
/// `None` means no routed placement is active and the caller should fall
/// through to the local `commit_change_envelope` path.
#[cfg(feature = "raft")]
async fn try_replicate_change_envelope(
    ctx: ChangeEnvelopeReplicaCtx<'_>,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    committed_at_ms: u64,
) -> Option<Response> {
    let ChangeEnvelopeReplicaCtx {
        state,
        req_id,
        graph_name,
        graph_type,
        tenant_scope,
        fname,
        routed_raft,
    } = ctx;
    let routed = routed_raft?;
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        envelope.mutation.batch_id.clone(),
        envelope.mutation.context.request_id,
        tenant_scope,
        envelope.mutation.context.principal.clone(),
        false,
        envelope.mutation.placement_epoch,
        envelope.mutation.fencing_token,
        envelope.mutation.created_at_ms,
    ) {
        Ok(context) => context,
        Err(error) => return Some(Response::err(req_id, error)),
    };
    let server_secret = timed_read(state).await.auth_secret.clone();
    let command = match crate::raft::ReplicatedMutation::change_envelope(envelope, &server_secret) {
        Ok(command) => command,
        Err(error) => return Some(Response::err(req_id, error)),
    };
    let request = crate::raft::RaftRequest {
        graph_fname: fname.to_string(),
        graph_name: graph_name.to_string(),
        graph_type,
        command,
        committed_at_ms,
        mutation,
    };
    Some(match routed.handle.client_write(request).await {
        Ok(response) if response.native_error.is_some() => {
            Response::err(req_id, response.native_error.unwrap_or_default())
        }
        Ok(response) => match response.change_envelope_commit {
            Some(committed) => {
                let mut result = change_envelope_result(&committed, response.projection_pending);
                if let Some(object) = result.as_object_mut() {
                    object.insert("replicated".to_string(), true.into());
                    object.insert("group".to_string(), routed.group_id.into());
                    object.insert("epoch".to_string(), routed.epoch.into());
                    object.insert("fencing_token".to_string(), routed.fencing_token().into());
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
    })
}

/// The resolved graph context a single-envelope `ApplyChangeEnvelope` commit
/// runs against: the live core, its durability backend, the placement route and
/// the tenant authority. Bundled to keep the dispatcher at the documented
/// parameter cap.
struct ApplyChangeEnvelopeCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    core: Arc<crate::graph::GraphCore>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "raft")]
    routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    #[cfg(feature = "raft")]
    graph_type: crate::protocol::GraphType,
    tenant_scope: &'a str,
}

async fn dispatch_change_env_apply_change_envelope(
    ctx: ApplyChangeEnvelopeCtx<'_>,
    method: Method,
) -> Response {
    #[cfg(feature = "raft")]
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let core = ctx.core;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let graph_type = ctx.graph_type;
    #[cfg(feature = "raft")]
    let tenant_scope = ctx.tenant_scope;
    match method {
        Method::ApplyChangeEnvelope { envelope } => {
            if let Err(resp) = check_change_envelope_placement_fence(
                req_id,
                #[cfg(feature = "raft")]
                graph_name,
                &envelope,
                #[cfg(feature = "raft")]
                routed_raft.as_ref(),
            )
            .await
            {
                return resp;
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

            #[cfg(feature = "raft")]
            if let Some(resp) = try_replicate_change_envelope(
                ChangeEnvelopeReplicaCtx {
                    state,
                    req_id,
                    graph_name,
                    graph_type,
                    tenant_scope,
                    fname: &fname,
                    routed_raft: routed_raft.as_ref(),
                },
                &envelope,
                committed_at_ms,
            )
            .await
            {
                return resp;
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
            Response::ok(req_id, ResultPayload::Json(result))
        }
        _ => unreachable!("dispatch_change_env_apply_change_envelope: classifier/handler diverged"),
    }
}

/// Commit a `ApplyChangeEnvelopes` batch and translate the durable result
/// (all-or-nothing per `crate::persist::sanitize`d graph) into the per-envelope
/// JSON result array the caller returns as `{"results": [...]}` -- success
/// entries and the atomic-abort's per-envelope conflict entries are the same
/// shape either way, so callers never have to branch on which happened.
async fn commit_change_envelope_batch_results(
    backend: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    core: &Arc<crate::graph::GraphCore>,
    fname: &str,
    envelopes: &[eg_types::change_envelope::ChangeEnvelope],
    committed_at_ms: u64,
) -> Vec<serde_json::Value> {
    match backend
        .commit_change_envelopes(fname, envelopes, committed_at_ms)
        .await
    {
        Ok(commits) => envelopes
            .iter()
            .zip(commits.iter())
            .map(|(envelope, committed)| change_envelope_applied_entry(core, envelope, committed))
            .collect(),
        Err((failing_index, error)) => {
            change_envelope_abort_entries(envelopes, failing_index, &error)
        }
    }
}

/// One committed envelope's result entry. A replayed envelope is an idempotent
/// skip and does NOT republish its projection; a freshly applied one does, and
/// a projection failure is surfaced as `projection_pending` rather than
/// pretending the durable commit did not happen.
fn change_envelope_applied_entry(
    core: &Arc<crate::graph::GraphCore>,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    committed: &eg_types::ChangeEnvelopeCommit,
) -> serde_json::Value {
    let projection_error = if committed.replayed {
        None
    } else {
        crate::server::mutation_batch::publish_change_envelope_projection(core, envelope).err()
    };
    let mut entry = change_envelope_result(committed, projection_error.is_some());
    if let Some(object) = entry.as_object_mut() {
        let status = if committed.replayed {
            "idempotent_skip"
        } else {
            "applied"
        };
        object.insert(
            "status".to_string(),
            serde_json::Value::String(status.to_string()),
        );
    }
    entry
}

/// The whole graph-batch aborted atomically — nothing committed. Report the
/// batch outcome per envelope honestly: the offender carries its own error; the
/// siblings carry the abort cause.
fn change_envelope_abort_entries(
    envelopes: &[eg_types::change_envelope::ChangeEnvelope],
    failing_index: usize,
    error: &str,
) -> Vec<serde_json::Value> {
    envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| {
            let this_error = if index == failing_index {
                error.to_string()
            } else {
                format!(
                    "ABORTED_ATOMIC_GRAPH_BATCH: sibling envelope {failing_index} failed ({error})"
                )
            };
            serde_json::json!({
                "status": "conflict",
                "envelope_id": envelope.envelope_id,
                "error": this_error,
            })
        })
        .collect()
}

async fn dispatch_change_env_apply_change_envelopes(
    req_id: u64,
    graph_name: &str,
    core: Arc<crate::graph::GraphCore>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "raft")] routed_raft: Option<crate::raft::multi::RoutedRaftHandle>,
    method: Method,
) -> Response {
    match method {
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
            let results = commit_change_envelope_batch_results(
                backend,
                &core,
                &fname,
                &envelopes,
                committed_at_ms,
            )
            .await;
            Response::ok(
                req_id,
                ResultPayload::Json(serde_json::json!({ "results": results })),
            )
        }
        _ => {
            unreachable!("dispatch_change_env_apply_change_envelopes: classifier/handler diverged")
        }
    }
}

async fn dispatch_change_env_get_change_envelope(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    method: Method,
) -> Response {
    match method {
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
        _ => unreachable!("dispatch_change_env_get_change_envelope: classifier/handler diverged"),
    }
}

async fn dispatch_change_env_get_content_version(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    method: Method,
) -> Response {
    match method {
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
        _ => unreachable!("dispatch_change_env_get_content_version: classifier/handler diverged"),
    }
}

async fn dispatch_change_env_get_change_cursor(
    req_id: u64,
    graph_name: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    method: Method,
) -> Response {
    match method {
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
        _ => unreachable!("dispatch_change_env_get_change_cursor: classifier/handler diverged"),
    }
}

#[cfg(feature = "raft")]
async fn resolve_routed_raft(
    req_id: u64,
    graph_name: &str,
    multi_raft: Option<&std::sync::Arc<crate::raft::multi::MultiRaft>>,
) -> Result<Option<crate::raft::multi::RoutedRaftHandle>, Response> {
    let Some(multi) = multi_raft else {
        return Ok(None);
    };
    match multi.handle_for_graph(graph_name).await {
        Some(routed) => Ok(Some(routed)),
        None => {
            let route = multi.route_graph(graph_name).await;
            Err(Response::stale_route(
                req_id,
                graph_name,
                route.group,
                route.epoch,
                None,
                "authoritative placement group is not running on this node",
            ))
        }
    }
}

fn stamp_resource_reservation_timestamp(method: &mut Method) {
    if !crate::server::mutation_batch::is_resource_reservation_method(method) {
        return;
    }
    let now_ms = authoritative_now_ms();
    match method {
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => request.now_ms = now_ms,
        Method::QueryWorkItemReservation { request }
        | Method::ResourceReservationStatus { request } => request.now_ms = now_ms,
        Method::UpdateResourceHost { request } => request.now_ms = now_ms,
        _ => unreachable!("resource method classifier and timestamp binding diverged"),
    }
}

fn stamp_capacity_timestamp(method: &mut Method) {
    if !crate::server::mutation_batch::is_capacity_method(method) {
        return;
    }
    let now_ms = authoritative_now_ms();
    match method {
        Method::AcquireCapacity { request } => request.now_ms = now_ms,
        Method::RenewCapacity { request } | Method::ReleaseCapacity { request } => {
            request.now_ms = now_ms
        }
        Method::ReclaimExpiredCapacity { request } => request.now_ms = now_ms,
        Method::UpdateCapacityCell { request } => request.now_ms = now_ms,
        Method::ReconcileCapacity { .. } | Method::CapacityStatus { .. } => {}
        _ => unreachable!("capacity method classifier and timestamp binding diverged"),
    }
}

fn stamp_resource_and_capacity_timestamps(method: &mut Method) {
    stamp_resource_reservation_timestamp(method);
    stamp_capacity_timestamp(method);
}

/// Cold-path lazy open (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3).
/// Only a registry MISS escalates to a write lock. The graph may be
/// catalog-known but not yet materialized (a lazy-startup boot scan, or a graph
/// the bounded hot-context cache evicted back to catalog-only); `lazy_open` is a
/// no-op for a genuinely unknown name, so the caller's "not found" error is
/// unchanged for that case.
#[cfg(feature = "redb")]
async fn lazy_open_graph(state: &Arc<RwLock<ServerState>>, graph_name: &str) {
    let cap = crate::server::persistence::cold_offload::max_resident_graphs();
    let page_size = crate::server::persistence::cold_offload::lazy_open_page_size();
    crate::server::persistence::cold_offload::lazy_open(state, graph_name, cap, page_size).await;
}

/// Universal served-data authority (CONCEPT:EG-P0-4): derive the row actor from
/// the cryptographically verified RequestContext while the authoritative
/// IsolationLayer is under the registry lock. Every graph read either consumes
/// this authority's detached projection or an existing handler that receives the
/// same IsolationLayer. Mutation documents can contain read phases (GraphQL
/// staged CONSTRUCT/UQL), so write requests carry the same verified tenant/actor
/// projection rather than gaining access to the raw committed core.
fn resolve_graph_read_authority(
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    isolation: &crate::isolation::IsolationLayer,
) -> Result<(GraphReadAuthority, String), Response> {
    let authority = match GraphReadAuthority::from_verified(verified_context, isolation) {
        Ok(authority) => authority,
        Err(denied) => return Err(Response::err(req_id, denied)),
    };
    let tenant_scope = authority
        .carrier()
        .expect("GraphReadAuthority always carries verified tenant authority")
        .tenant_scope()
        .to_string();
    Ok((authority, tenant_scope))
}

/// Mutation-gateway authz context (CONCEPT:EG-P0-2): for a
/// `mutation::GATEWAY_ROUTED` method, `commit_mutation` re-derives its OWN authz
/// decision from `(isolation, graph_type, owner)` rather than trusting the graph
/// ACL check — captured before the registry lock drops, ONLY for the routed set
/// (an `IsolationLayer` clone is not free, so this is skipped entirely for the
/// other ~330 methods).
fn graph_op_gateway_authz_ctx(
    s: &ServerState,
    method: &Method,
    entry: &GraphEntryFacts,
) -> Option<crate::server::mutation::GatewayAuthzCtx> {
    if !crate::server::mutation::is_gateway_routed(method) {
        return None;
    }
    Some((s.isolation.clone(), entry.graph_type, entry.owner.clone()))
}

/// Everything resolved while the registry read lock is still held.
struct GraphOpGate {
    entry: GraphEntryFacts,
    read_authority: GraphReadAuthority,
    tenant_scope: String,
    verified_actor: String,
    gateway_authz_ctx: Option<crate::server::mutation::GatewayAuthzCtx>,
}

/// Gate and resolve one graph operation under the registry lock, in the ORIGINAL
/// order: caller / existence / materialization / graph ACL first, then the
/// verified read authority, then the gateway authz context.
fn gate_graph_op_under_lock(
    s: &ServerState,
    ctx: GraphOpContext<'_>,
    method: &Method,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<GraphOpGate, Response> {
    let GraphOpContext {
        graph_name,
        req_id,
        caller,
        verified_context,
    } = ctx;
    let entry = check_graph_op_access(
        s,
        req_id,
        caller,
        graph_name,
        access,
        state_machine_authorized,
    )?;
    let (read_authority, tenant_scope) =
        resolve_graph_read_authority(req_id, verified_context, &s.isolation)?;
    let verified_actor = match read_authority.verified_actor() {
        Ok(actor) => actor.to_string(),
        Err(denied) => return Err(Response::err(req_id, denied)),
    };
    let gateway_authz_ctx = graph_op_gateway_authz_ctx(s, method, &entry);
    Ok(GraphOpGate {
        entry,
        read_authority,
        tenant_scope,
        verified_actor,
        gateway_authz_ctx,
    })
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
        lazy_open_graph(state, graph_name).await;
        s = timed_read(state).await;
    }

    let access = graph_op_access_level(&method);
    #[cfg(feature = "raft")]
    let state_machine_authorized = is_replicated_apply();
    #[cfg(not(feature = "raft"))]
    let state_machine_authorized = false;

    let gate = match gate_graph_op_under_lock(
        &s,
        GraphOpContext {
            graph_name,
            req_id,
            caller,
            verified_context,
        },
        &method,
        access,
        state_machine_authorized,
    ) {
        Ok(gate) => gate,
        Err(resp) => return resp,
    };
    let GraphOpGate {
        entry,
        read_authority,
        tenant_scope,
        verified_actor,
        gateway_authz_ctx,
    } = gate;
    let read_authority = Some(read_authority);
    // `verified_actor` is consumed only by the query/cypher/graphql/rdf/
    // knowledge-stream gateway arms below (each independently feature-gated); a
    // slim build with none of them enabled still needs this binding to compile.
    let verified_actor: &str = &verified_actor;

    let core = entry.core.clone();
    #[cfg(feature = "redb")]
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
    // fallback (unreachable for the five coalescable methods since the
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
    let placement_authority = s.placement_authority();
    #[cfg(feature = "raft")]
    let multi_raft = resolve_multi_raft(&s, &placement_authority);
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
    if let Some(error) = placement_authority.missing_error() {
        return Response::err(req_id, error);
    }

    // Record this graph's access for the cold-offload sweep (CONCEPT:EG-KG.backend.r6-feature, R6) — both
    // reads and writes touch, so a graph being actively used is never offloaded.
    #[cfg(feature = "redb")]
    cold_tracker.touch_with_incarnation(graph_name, &graph_incarnation_id);

    // Mandatory placement resolution for ordinary graph operations. MultiRaft is
    // the sole clustered authority. Resolving here also fences reads away from a
    // node that no longer runs the graph's current group.
    #[cfg(feature = "raft")]
    let routed_raft = match resolve_routed_raft(req_id, graph_name, multi_raft.as_ref()).await {
        Ok(routed_raft) => routed_raft,
        Err(resp) => return resp,
    };

    // Resource lifecycle timestamps are authority inputs, not caller clocks.
    // Bind one leader/replicated-apply timestamp before dispatching either the
    // native MutationBatch or an authority read; followers replay the timestamp
    // carried by the committed Raft scope through `authoritative_now_ms()`.
    stamp_resource_and_capacity_timestamps(&mut method);

    let routing = GraphOpRouting {
        state,
        req_id,
        graph_name,
        caller,
        verified_context,
        state_machine_authorized,
        read_authority: &read_authority,
        verified_actor,
        tenant_scope: &tenant_scope,
        gateway_authz_ctx: &gateway_authz_ctx,
        core: &core,
        materialization_manifest: &materialization_manifest,
        persistence: &persistence,
        #[cfg(feature = "streaming")]
        cdc: &cdc,
        #[cfg(feature = "security")]
        rls: &rls,
        #[cfg(feature = "raft")]
        routed_raft: &routed_raft,
        #[cfg(feature = "raft")]
        graph_type,
        #[cfg(feature = "raft")]
        multi_raft: &multi_raft,
        #[cfg(feature = "redb")]
        graph_incarnation_id: &graph_incarnation_id,
        #[cfg(feature = "modality-serving")]
        modality_authority: &modality_authority,
        #[cfg(feature = "knowledge-batch")]
        knowledge_stream_authority: &knowledge_stream_authority,
    };
    let method = match route_graph_op_method(routing, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };

    crate::metrics::graph_op(graph_name);

    let response = run_dispatch_pipeline(
        DispatchPipelineCtx {
            state,
            req_id,
            graph_name,
            caller,
            read_authority: read_authority.clone(),
            verified_actor,
            tenant_scope: tenant_scope.clone(),
            gateway_authz_ctx: gateway_authz_ctx.clone(),
            core: core.clone(),
            materialization_manifest: materialization_manifest.clone(),
            persistence: persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: cdc.clone(),
            write_coalescer: write_coalescer.clone(),
            routed_write_coalescer: routed_write_coalescer.clone(),
            #[cfg(feature = "security")]
            rls: rls.clone(),
            #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
            tsdb_store: tsdb_store.clone(),
        },
        method,
    )
    .await;

    finalize_graph_op_response(
        state,
        graph_name,
        &core,
        access,
        gateway_authz_ctx.is_some(),
        response,
    )
    .await
}

/// Everything the post-lock routers below need, snapshotted out of the registry
/// lock by `dispatch_graph_op_inner`. Every field is a shared reference or a
/// `Copy` scalar, so this is `Copy` and each router call is free. `read_authority`
/// and `verified_actor` are separate borrows of the CALLER's locals — the actor
/// string borrows from the authority, so the two cannot live in one owned struct.
#[derive(Clone, Copy)]
struct GraphOpRouting<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    caller: Option<&'a str>,
    verified_context: &'a VerifiedRequestContext,
    state_machine_authorized: bool,
    read_authority: &'a Option<GraphReadAuthority>,
    verified_actor: &'a str,
    tenant_scope: &'a str,
    gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    core: &'a Arc<crate::graph::GraphCore>,
    materialization_manifest:
        &'a Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    persistence: &'a Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: &'a Option<Arc<crate::server::cdc::CdcHub>>,
    #[cfg(feature = "security")]
    rls: &'a std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(feature = "raft")]
    routed_raft: &'a Option<crate::raft::multi::RoutedRaftHandle>,
    #[cfg(feature = "raft")]
    graph_type: crate::protocol::GraphType,
    #[cfg(feature = "raft")]
    multi_raft: &'a Option<Arc<crate::raft::multi::MultiRaft>>,
    #[cfg(feature = "redb")]
    graph_incarnation_id: &'a String,
    #[cfg(feature = "modality-serving")]
    modality_authority: &'a Option<handlers::modality::ModalityAuthority>,
    #[cfg(feature = "knowledge-batch")]
    knowledge_stream_authority: &'a Option<handlers::knowledge_stream::KnowledgeStreamAuthority>,
}

/// ChangeEnvelope is a first-class persistence operation, not a sequence of
/// direct graph calls. It executes only after graph ACL and placement
/// resolution, and before generic replicated mutation paths can decompose it.
///
/// Returns `Err(method)` for a method this router does not own, so
/// `route_graph_op_method` can offer it to the next one.
#[allow(unused_variables)]
async fn route_change_envelope_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let tenant_scope = ctx.tenant_scope;
    let core = ctx.core;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let graph_type = ctx.graph_type;
    // ChangeEnvelope is a first-class persistence operation, not a sequence of
    // direct graph calls. It executes only after graph ACL and placement
    // resolution, and before generic replicated mutation paths can decompose it.
    match method {
        method @ Method::ApplyChangeEnvelope { .. } => {
            return Ok(dispatch_change_env_apply_change_envelope(
                ApplyChangeEnvelopeCtx {
                    state,
                    req_id,
                    graph_name,
                    core: core.clone(),
                    persistence: persistence.clone(),
                    #[cfg(feature = "raft")]
                    routed_raft: routed_raft.clone(),
                    #[cfg(feature = "raft")]
                    graph_type,
                    tenant_scope,
                },
                method,
            )
            .await)
        }
        // Batch envelope commit for ONE graph (the top-level `dispatch_change_envelopes`
        // groups by graph and routes each group here). Every envelope targets
        // `graph_name`; they land in ONE coalesced redb transaction — the atomic
        // graph-batch. A single failing envelope aborts the whole group and every
        // envelope in it reports the batch outcome honestly. Per-envelope results are
        // returned in group order under `{"results": [...]}`.
        method @ Method::ApplyChangeEnvelopes { .. } => {
            return Ok(dispatch_change_env_apply_change_envelopes(
                req_id,
                graph_name,
                core.clone(),
                persistence.clone(),
                #[cfg(feature = "raft")]
                routed_raft.clone(),
                method,
            )
            .await)
        }
        method @ Method::GetChangeEnvelope { .. } => {
            return Ok(dispatch_change_env_get_change_envelope(
                req_id,
                graph_name,
                persistence.clone(),
                method,
            )
            .await)
        }
        method @ Method::GetContentVersion { .. } => {
            return Ok(dispatch_change_env_get_content_version(
                req_id,
                graph_name,
                persistence.clone(),
                method,
            )
            .await)
        }
        method @ Method::GetChangeCursor { .. } => {
            return Ok(dispatch_change_env_get_change_cursor(
                req_id,
                graph_name,
                persistence.clone(),
                method,
            )
            .await)
        }
        other => Err(other),
    }
}

/// The native durable authorities that own their own store and MutationBatch
/// kernel: resource reservations, capacity leases, WorkItem claim capability,
/// the development lane, and WorkItem transitions.
///
/// Returns `Err(method)` for a method this router does not own, so
/// `route_graph_op_method` can offer it to the next one.
#[allow(unused_variables)]
async fn route_native_store_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let verified_context = ctx.verified_context;
    let state_machine_authorized = ctx.state_machine_authorized;
    let core = ctx.core;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let multi_raft = ctx.multi_raft;
    #[cfg(feature = "redb")]
    let graph_incarnation_id = ctx.graph_incarnation_id;
    // Reservation reads are authority reads, not GraphCore snapshots.  Under
    // placement they are served only by the current group leader; followers
    // fail closed with the normal redirect instead of returning stale holds.
    if crate::server::mutation_batch::is_resource_reservation_query_method(&method) {
        return Ok(dispatch_op_resource_reservation_query(
            req_id,
            graph_name,
            verified_context,
            persistence.clone(),
            #[cfg(feature = "raft")]
            multi_raft.clone(),
            #[cfg(feature = "raft")]
            routed_raft.clone(),
            method,
        )
        .await);
    }

    // Capacity leases are a separate native authority from repository/resource
    // reservations.  They still share the same authenticated graph/tenant
    // boundary, current-placement leader check, and writer backpressure.  No
    // caller can renew/release on behalf of another owner: the opaque owner
    // digest is compared to the verified principal before redb sees the row.
    if crate::server::mutation_batch::is_capacity_method(&method) {
        return Ok(dispatch_op_capacity_ops(
            NativeOpCtx {
                req_id,
                graph_name,
                verified_context,
                persistence: persistence.clone(),
                #[cfg(feature = "raft")]
                multi_raft: multi_raft.clone(),
                #[cfg(feature = "raft")]
                routed_raft: routed_raft.clone(),
            },
            state_machine_authorized,
            method,
        )
        .await);
    }

    // Native WorkItem claim capabilities use a dedicated private ledger and
    // never enter MutationBatch/result/outbox/CDC projections.  The verified
    // request context supplies all authority fields; the public method carries
    // only an item id (mint) or opaque bytes (verify).
    if matches!(
        &method,
        Method::MintWorkItemClaimCapability { .. } | Method::VerifyWorkItemClaimCapability { .. }
    ) {
        return Ok(dispatch_op_workitem_claim_capability(
            NativeOpCtx {
                req_id,
                graph_name,
                verified_context,
                persistence: persistence.clone(),
                #[cfg(feature = "raft")]
                multi_raft: multi_raft.clone(),
                #[cfg(feature = "raft")]
                routed_raft: routed_raft.clone(),
            },
            #[cfg(feature = "redb")]
            graph_incarnation_id.clone(),
            method,
        )
        .await);
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
        return Ok(dispatch_op_development_lane(
            req_id,
            graph_name,
            persistence.clone(),
            #[cfg(feature = "raft")]
            multi_raft.clone(),
            #[cfg(feature = "raft")]
            routed_raft.clone(),
            method,
        )
        .await);
    }

    // Engine-native WorkItem transitions are result-producing durable CAS
    // operations. They must execute at the current placement leader (a generic
    // Raft acknowledgement cannot carry the selected work-item result), and their
    // redb MutationBatch atomically persists the transition/result/outbox before
    // the in-memory graph projection is refreshed.
    if crate::server::mutation_batch::is_work_item_mutation_method(&method) {
        return Ok(dispatch_op_workitem_mutation(
            req_id,
            graph_name,
            caller,
            core.clone(),
            persistence.clone(),
            #[cfg(feature = "raft")]
            routed_raft.clone(),
            method,
        )
        .await);
    }
    Err(method)
}

/// The remaining authority-bearing surfaces, all resolved AFTER graph ACL,
/// lazy materialization and placement: the series-write fence, the knowledge
/// stream, time series, audit verification/inclusion, served modality, and the
/// Raft write-routing barrier.
///
/// Returns `Err(method)` for a method this router does not own, so
/// `route_graph_op_method` can offer it to the next one.
#[allow(unused_variables)]
async fn route_graph_authority_surfaces(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let verified_context = ctx.verified_context;
    let read_authority = ctx.read_authority;
    let verified_actor = ctx.verified_actor;
    let tenant_scope = ctx.tenant_scope;
    let gateway_authz_ctx = ctx.gateway_authz_ctx;
    let core = ctx.core;
    let materialization_manifest = ctx.materialization_manifest;
    let persistence = ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = ctx.cdc;
    #[cfg(feature = "security")]
    let rls = ctx.rls;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let graph_type = ctx.graph_type;
    #[cfg(feature = "modality-serving")]
    let modality_authority = ctx.modality_authority;
    #[cfg(feature = "knowledge-batch")]
    let knowledge_stream_authority = ctx.knowledge_stream_authority;
    // `TsAppend`/`TsEvict`/`TsDeleteSeries` are not yet Raft state-machine commands, so none
    // can rely on the durable-mutation barrier below. Still fence every `series.redb` WRITE
    // to the current placement leader: accepting a follower-local append, retention evict, or
    // whole-series delete would create a divergent `series.redb` projection and acknowledge a
    // write on the wrong replica. (`TsListSeries` is a read and is intentionally excluded.)
    #[cfg(all(feature = "raft", feature = "tsdb"))]
    if let Some(stale) =
        dispatch_op_tsdb_write_fence(req_id, graph_name, routed_raft.as_ref(), &method).await
    {
        return Ok(stale);
    }

    #[cfg(all(feature = "raft", feature = "tsdb"))]
    let (ts_placement_epoch, ts_fencing_token) = ts_placement_fence(routed_raft.as_ref());
    #[cfg(all(not(feature = "raft"), feature = "tsdb"))]
    let (ts_placement_epoch, ts_fencing_token) = (0, None);

    // One native KnowledgeBatch pull surface for every query family. This point is
    // deliberately after graph ACL, lazy-materialization and placement resolution,
    // and before any family-specific direct handler, so a cursor cannot bypass the
    // same RequestContext/RLS/placement boundary as its underlying query.
    #[cfg(feature = "knowledge-batch")]
    if matches!(&method, Method::KnowledgeStream { .. }) {
        return Ok(dispatch_op_knowledge_stream(
            KnowledgeStreamCtx {
                state,
                req_id,
                graph_name,
                verified_context,
                read_authority,
                verified_actor,
                core: core.clone(),
                #[cfg(feature = "security")]
                rls: rls.clone(),
                #[cfg(feature = "raft")]
                routed_raft: routed_raft.clone(),
                knowledge_stream_authority: knowledge_stream_authority.clone(),
            },
            method,
        )
        .await);
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
            | Method::TsEvict { .. }
            | Method::TsDeleteSeries { .. }
            | Method::TsListSeries
    ) {
        return Ok(dispatch_op_tsdb_ops(
            state,
            req_id,
            graph_name,
            verified_context,
            ts_placement_epoch,
            ts_fencing_token,
            method,
        )
        .await);
    }

    // Tamper-evident audit verification (CONCEPT:EG-KG.sharding.row-level-security): a read-only walk of the
    // target graph's durable hash-chained audit log. Routed to the redb backend's
    // owner thread (which flushes pending first). Handled here — AFTER the registry
    // lock is released — so blocking on the writer-thread reply never holds the lock.
    #[cfg(feature = "security")]
    if matches!(method, Method::AuditVerify) {
        return Ok(dispatch_op_audit_verify(req_id, graph_name, persistence.clone()).await);
    }

    // Provenance-anchor inclusion proof (CONCEPT:EG-KG.sharding.row-level-security, provenance anchoring): the
    // `AuditVerify` extension that reaches an anchored NODE's content, not just
    // mutation ordering. Same routing shape as `AuditVerify` immediately above —
    // the redb backend's owner thread (flushes pending first), handled after the
    // registry lock is released.
    #[cfg(feature = "security")]
    if matches!(&method, Method::AuditProveInclusion { .. }) {
        return Ok(dispatch_op_audit_prove_inclusion(
            req_id,
            graph_name,
            persistence.clone(),
            method.clone(),
        )
        .await);
    }

    // Concrete governed modality service. This is deliberately after graph ACL,
    // lazy materialization, placement resolution and verified-context authority
    // derivation, but before the generic replicated-mutation branch: the mutation gateway
    // stages a complete graph image and commits the encrypted runtime snapshot
    // through the authoritative MutationBatch boundary before publishing RAM.
    #[cfg(feature = "modality-serving")]
    if matches!(&method, Method::ServedModality { .. }) {
        return Ok(dispatch_op_served_modality(
            ServedModalityCtx {
                state,
                req_id,
                graph_name,
                caller,
                verified_context,
                tenant_scope,
                gateway_authz_ctx,
                core: core.clone(),
                materialization_manifest: materialization_manifest.clone(),
                persistence: persistence.clone(),
                #[cfg(feature = "streaming")]
                cdc: cdc.clone(),
                #[cfg(feature = "raft")]
                routed_raft: routed_raft.clone(),
                modality_authority: modality_authority.clone(),
            },
            method.clone(),
        )
        .await);
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
    if let Some(routed) = routed_raft
        .clone()
        .filter(|_| crate::mutation_apply::is_durable_mutation(&method))
    {
        return Ok(dispatch_op_raft_write_routing_barrier(
            RaftWriteBarrierCtx {
                state,
                req_id,
                graph_name,
                verified_context,
                tenant_scope,
                graph_type,
            },
            routed,
            method,
        )
        .await);
    }
    Err(method)
}

/// Try each post-lock router in the ORIGINAL order — change envelopes, then the
/// native stores, then the authority surfaces. A method none of them owns falls
/// through to the ordinary dispatch pipeline.
async fn route_graph_op_method(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_change_envelope_ops(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    let method = match route_native_store_ops(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_graph_authority_surfaces(ctx, method).await
}

fn graph_op_access_level(method: &Method) -> AccessLevel {
    // TsAppend used to self-route before this boundary, which accidentally classified
    // it as neither a graph read nor write. It is now graph-scoped and requires the
    // same Write ACL as every other mutation; so do the other two `series.redb`
    // mutations, TsEvict/TsDeleteSeries (retention) -- a Read-only caller must not be
    // able to evict points or delete a whole series any more than they could append
    // to one. All other Ts methods (including the read-only TsListSeries enumeration)
    // require only Read.
    if requires_write(method)
        || matches!(
            method,
            Method::TsAppend { .. } | Method::TsEvict { .. } | Method::TsDeleteSeries { .. }
        )
    {
        AccessLevel::Write
    } else {
        AccessLevel::Read
    }
}

/// The registry facts a graph operation needs after the registry lock is
/// released. Copied out under the lock rather than held by reference.
struct GraphEntryFacts {
    graph_type: crate::protocol::GraphType,
    owner: Option<String>,
    core: Arc<crate::graph::GraphCore>,
    #[cfg(feature = "redb")]
    incarnation_id: String,
}

/// Gate one graph operation, in the ORIGINAL order: an unregistered or
/// unauthenticated caller is denied BEFORE existence is resolved -- never let
/// "Graph not found" vs "ACCESS_DENIED" tell a caller who could never pass ACL
/// for any graph whether the target graph exists (see
/// `access::check_caller_is_known`'s doc). A registered caller falls through to
/// existence, materialization validity, and then the real graph-type/owner-aware
/// decision.
fn check_graph_op_access(
    s: &ServerState,
    req_id: u64,
    caller: Option<&str>,
    graph_name: &str,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<GraphEntryFacts, Response> {
    check_known_caller(
        &s.isolation,
        req_id,
        caller,
        graph_name,
        access,
        state_machine_authorized,
    )?;
    let Some(entry) = s.registry.get(graph_name) else {
        return Err(Response::err(
            req_id,
            format!("Graph '{graph_name}' not found"),
        ));
    };
    check_materialization_valid(&s.registry, req_id, graph_name)?;
    if !state_machine_authorized {
        if let Err(denied) = check_graph_access(
            &s.isolation,
            caller,
            graph_name,
            entry.graph_type,
            entry.owner.as_deref(),
            access,
        ) {
            return Err(Response::err(req_id, denied));
        }
    }
    Ok(GraphEntryFacts {
        graph_type: entry.graph_type,
        owner: entry.owner.clone(),
        core: entry.core.clone(),
        #[cfg(feature = "redb")]
        incarnation_id: entry.incarnation_id.clone(),
    })
}

/// MultiRaft is the sole clustered authority: a replicated apply proposes
/// nothing, and a node without MultiRaft placement has no write routing.
#[cfg(feature = "raft")]
fn resolve_multi_raft(
    s: &ServerState,
    placement_authority: &crate::server::state::PlacementAuthorityKind,
) -> Option<Arc<crate::raft::multi::MultiRaft>> {
    if is_replicated_apply() {
        return None;
    }
    if !matches!(
        placement_authority,
        crate::server::state::PlacementAuthorityKind::MultiRaft
    ) {
        return None;
    }
    s.multi_raft.clone()
}

#[cfg(all(feature = "raft", feature = "tsdb"))]
fn ts_placement_fence(routed: Option<&crate::raft::multi::RoutedRaftHandle>) -> (u64, Option<u64>) {
    match routed {
        Some(routed) => (routed.epoch, Some(routed.group_id)),
        None => (0, None),
    }
}

/// The dispatch shell's write tail: size gauges, the single projection
/// publication non-gateway writes rely on, and the semantic-ANN warm hook.
#[allow(unused_variables)]
async fn finalize_graph_op_response(
    state: &Arc<RwLock<ServerState>>,
    graph_name: &str,
    core: &Arc<crate::graph::GraphCore>,
    access: AccessLevel,
    gateway_routed: bool,
    response: Response,
) -> Response {
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
    if matches!(access, AccessLevel::Write) && response.error.is_none() && !gateway_routed {
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
        crate::server::ann_warm::maybe_warm_after_write(state, graph_name, core).await;
    }

    response
}

fn check_known_caller(
    isolation: &crate::isolation::IsolationLayer,
    req_id: u64,
    caller: Option<&str>,
    graph_name: &str,
    access: AccessLevel,
    state_machine_authorized: bool,
) -> Result<(), Response> {
    if !state_machine_authorized {
        if let Err(denied) = check_caller_is_known(isolation, caller, graph_name, access) {
            return Err(Response::err(req_id, denied));
        }
    }
    Ok(())
}

fn check_materialization_valid(
    registry: &crate::registry::GraphRegistry,
    req_id: u64,
    graph_name: &str,
) -> Result<(), Response> {
    if let Some(manifest) = registry.materialization_manifest(graph_name) {
        if !manifest.valid {
            let phase = match manifest.phase {
                crate::registry::MaterializationPhase::CatalogOnly => "catalog_only",
                crate::registry::MaterializationPhase::Partial => "partial",
                crate::registry::MaterializationPhase::Complete => "complete",
                crate::registry::MaterializationPhase::Failed => "failed",
            };
            return Err(Response::err(
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
            ));
        }
    }
    Ok(())
}

/// Everything the runtime-conditional query/RDF gateways need from the resolved
/// request, bundled so each router stays at the documented parameter cap. The
/// borrowed/owned split matches how `run_dispatch_pipeline` already held these
/// values: identity and authz captures are borrowed, the `Arc` handles are
/// cloned per stage because the mutation gateway moves them into an async apply
/// closure. Same shape as `crate::server::mutation::MutationCtx`.
#[cfg(any(
    feature = "query",
    feature = "cypher",
    feature = "graphql",
    feature = "rdf"
))]
struct GatewayRouteCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    caller: Option<&'a str>,
    tenant_scope: &'a str,
    core: Arc<crate::graph::GraphCore>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    gateway_authz_ctx: &'a Option<crate::server::mutation::GatewayAuthzCtx>,
    read_authority: &'a Option<GraphReadAuthority>,
    verified_actor: &'a str,
    #[cfg(feature = "security")]
    rls: std::sync::Arc<crate::isolation::IsolationLayer>,
}

#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
async fn route_query_gateway(ctx: GatewayRouteCtx<'_>, method: Method) -> Result<Response, Method> {
    let GatewayRouteCtx {
        state,
        req_id,
        graph_name,
        caller,
        tenant_scope,
        core,
        persistence,
        #[cfg(feature = "streaming")]
        cdc,
        materialization_manifest,
        gateway_authz_ctx,
        read_authority,
        verified_actor,
        #[cfg(feature = "security")]
        rls,
    } = ctx;
    if crate::server::mutation::is_query_gateway_method(&method)
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
            tenant_scope,
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
        return Ok(resp);
    }
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
        Ok(r) => Ok(r),
        Err(m) => Err(m),
    }
}

#[cfg(feature = "rdf")]
async fn route_rdf_gateway(ctx: GatewayRouteCtx<'_>, method: Method) -> Result<Response, Method> {
    let GatewayRouteCtx {
        state,
        req_id,
        graph_name,
        caller,
        tenant_scope,
        core,
        persistence,
        #[cfg(feature = "streaming")]
        cdc,
        materialization_manifest,
        gateway_authz_ctx,
        read_authority,
        verified_actor,
        #[cfg(feature = "security")]
        rls,
    } = ctx;
    if crate::server::mutation::is_rdf_gateway_method(&method) {
        let plan = crate::server::mutation::MutationPlan::for_method(&method);
        let (iso, gtype, owner) = gateway_authz_ctx
            .as_ref()
            .expect("is_gateway_routed rdf method must have a captured GatewayAuthzCtx");
        let ctx = crate::server::mutation::MutationCtx {
            req_id,
            caller,
            tenant_scope,
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
        return Ok(resp);
    }
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
        Ok(r) => Ok(r),
        Err(m) => Err(m),
    }
}

/// Everything [`run_dispatch_pipeline`] routes with, resolved once per request
/// by `dispatch_graph_op`: the caller's verified identity and read authority,
/// the live graph core with its durability/CDC/materialization handles, and the
/// two write coalescers. Bundled so the pipeline keeps ONE routing parameter
/// beside the method it is routing, instead of a seventeen-long list.
struct DispatchPipelineCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    caller: Option<&'a str>,
    read_authority: Option<GraphReadAuthority>,
    verified_actor: &'a str,
    tenant_scope: String,
    gateway_authz_ctx: Option<crate::server::mutation::GatewayAuthzCtx>,
    core: Arc<crate::graph::GraphCore>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    write_coalescer: Arc<crate::write_coalescer::WriteCoalescerRegistry>,
    routed_write_coalescer:
        Arc<crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry>,
    #[cfg(feature = "security")]
    rls: std::sync::Arc<crate::isolation::IsolationLayer>,
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    tsdb_store: Option<Arc<eg_tsdb::store::SeriesStore>>,
}

/// Stage 1: the universal mutation gateway, then the per-graph write
/// coalescer, then the stateless pure-compute domains.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
async fn route_gateway_and_stateless_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let read_authority = &ctx.read_authority;
    let tenant_scope: &str = &ctx.tenant_scope;
    let gateway_authz_ctx = &ctx.gateway_authz_ctx;
    let core = &ctx.core;
    let materialization_manifest = &ctx.materialization_manifest;
    let persistence = &ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = &ctx.cdc;
    let write_coalescer = &ctx.write_coalescer;
    let routed_write_coalescer = &ctx.routed_write_coalescer;
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    let tsdb_store = &ctx.tsdb_store;
    // Mutation-gateway routing (CONCEPT:EG-P0-2): the primary CRUD + agent-
    // memory writes (`mutation::GATEWAY_ROUTED`) are routed through the
    // single `commit_mutation` gateway — policy-driven authz, durability,
    // audit, CDC, and TMS in ONE call. Native stores use their own explicit
    // MutationBatch kernels. There is no second post-dispatch durability tail.
    let method = match handlers::graph_ops::try_handle_gateway(
        req_id,
        caller,
        tenant_scope,
        graph_name,
        core,
        materialization_manifest.as_ref(),
        read_authority.as_ref(),
        persistence.as_ref(),
        #[cfg(feature = "streaming")]
        cdc.as_ref(),
        Some(routed_write_coalescer),
        gateway_authz_ctx.as_ref(),
        #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
        tsdb_store.as_ref(),
        method,
    )
    .await
    {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Per-graph write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): the five high-frequency
    // single-op writes are batched onto this graph's writer so M concurrent
    // writers cost ⌈M/batch⌉ topology-lock acquisitions instead of M. The shell
    // below still owns dirty/durability/gauge off the returned Response, so durability
    // and checkpoint semantics are unchanged — only WHERE the lock is taken
    // moved. On a full queue the coalescer returns BUSY before any RAM or
    // durable side effect, preserving the queue's declared order.
    let method = match try_coalesce_write(req_id, write_coalescer, graph_name, core, method).await {
        Ok(resp) => return Ok(resp),
        Err(m) => m,
    };
    // Pure-compute domains (stateless: no graph core / lock) route first; a
    // method that isn't theirs is handed back via Err and falls through to the
    // graph-op match below. (CONCEPT:EG-KG.query.dispatch-routing — thin routing; logic in handlers/.)
    // Feature-gated: in a slim build the line is absent and the method flows
    // straight through to graph_ops (whose catch-all reports "not available").
    #[cfg(feature = "finance")]
    let method = match handlers::finance::try_handle(req_id, method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Native TTS synthesis (GOC-34, `OWNER-VOICE-TTS`): stateless, like finance
    // above. `caller` is already the verified eg2-authenticated principal in
    // scope at this point — see `handlers::tts`'s own doc for exactly how (and
    // how far) that maps to the frozen contract's `PolicyDecision`.
    #[cfg(feature = "tts-piper")]
    let method = match handlers::tts::try_handle(req_id, caller, method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    Err(method)
}

/// Stage 2: the graph-scoped compute domains — data science, mining,
/// graph learning and the ML pipeline read verbs.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
async fn route_graph_scoped_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let read_authority = &ctx.read_authority;
    let core = &ctx.core;
    #[cfg(all(feature = "mining", feature = "query", feature = "tsdb"))]
    let tsdb_store = &ctx.tsdb_store;
    #[cfg(feature = "datascience")]
    let method = match handlers::datascience::try_handle(req_id, method) {
        Ok(r) => return Ok(r),
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
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Graph-learning domain (CONCEPT:EG-KG.graphlearn.link-predictor): GRAPH-SCOPED
    // like mining — the KAN link-predictor reads the live subgraph and write-back
    // materializes `:PredictedEdge`/`:EdgeFunction` nodes into the core. A method
    // whose feature is off falls through to the graph_ops not-available catch-all.
    #[cfg(feature = "graphlearn")]
    let method = match handlers::graphlearn::try_handle(req_id, core.clone(), method) {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // ML pipeline (CONCEPT:EG-KG.mining.ml-pipeline): the READ verbs
    // (Evaluate/Compare) route here with the graph core; Train/Serve/Predict are
    // GATEWAY_ROUTED (writeback) and never reach this fallback. A build without
    // `ml-pipeline` omits this line.
    #[cfg(feature = "ml-pipeline")]
    let method =
        match handlers::pipeline::try_handle(req_id, core.clone(), read_authority.as_ref(), method)
        {
            Ok(r) => return Ok(r),
            Err(m) => m,
        };
    Err(method)
}

/// Stage 3: the runtime-conditional query and native-RDF gateways.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
async fn route_query_and_rdf_surfaces(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let caller = ctx.caller;
    let read_authority = &ctx.read_authority;
    let verified_actor = ctx.verified_actor;
    let tenant_scope: &str = &ctx.tenant_scope;
    let gateway_authz_ctx = &ctx.gateway_authz_ctx;
    let core = &ctx.core;
    let materialization_manifest = &ctx.materialization_manifest;
    let persistence = &ctx.persistence;
    #[cfg(feature = "streaming")]
    let cdc = &ctx.cdc;
    #[cfg(feature = "security")]
    let rls = &ctx.rls;
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
    let method = match route_query_gateway(
        GatewayRouteCtx {
            state,
            req_id,
            graph_name,
            caller,
            tenant_scope,
            core: core.clone(),
            persistence: persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: cdc.clone(),
            materialization_manifest: materialization_manifest.clone(),
            gateway_authz_ctx,
            read_authority,
            verified_actor,
            #[cfg(feature = "security")]
            rls: rls.clone(),
        },
        method,
    )
    .await
    {
        Ok(resp) => return Ok(resp),
        Err(m) => m,
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
    let method = match route_rdf_gateway(
        GatewayRouteCtx {
            state,
            req_id,
            graph_name,
            caller,
            tenant_scope,
            core: core.clone(),
            persistence: persistence.clone(),
            #[cfg(feature = "streaming")]
            cdc: cdc.clone(),
            materialization_manifest: materialization_manifest.clone(),
            gateway_authz_ctx,
            read_authority,
            verified_actor,
            #[cfg(feature = "security")]
            rls: rls.clone(),
        },
        method,
    )
    .await
    {
        Ok(resp) => return Ok(resp),
        Err(m) => m,
    };
    Err(method)
}

/// Stage 4: the process-global domains — sandboxed UDFs, query federation
/// and distributed compute.
///
/// Returns `Err(method)` for a method this stage does not own, so the
/// pipeline can offer it to the next stage; the terminal graph-op handler
/// owns the catch-all.
#[allow(unused_variables)]
async fn route_process_global_domains(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let state = ctx.state;
    let req_id = ctx.req_id;
    let caller = ctx.caller;
    let read_authority = &ctx.read_authority;
    // WASM-sandboxed UDF surface (CONCEPT:EG-KG.query.rowset-execution, feature `wasm-udf`):
    // RegisterUdf compiles+caches, RunUdf runs sandboxed (fuel+memory+no host
    // caps) — both off-reactor. Process-global (not graph-scoped), so it takes
    // `state` for the UdfRegistry. A method whose feature is off falls through.
    #[cfg(feature = "wasm-udf")]
    let method = match handlers::wasm_udf::try_handle(state, req_id, method).await {
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    // Query federation (CONCEPT:EG-KG.query.query-federation, feature `federation`):
    // RegisterForeignSource records a named foreign source on ServerState. The
    // `Op::ForeignScan` op itself runs through the unified-query handler above
    // (inline spec). Process-global, so it takes `state`. A method whose feature
    // is off falls through to the graph_ops not-available catch-all.
    #[cfg(feature = "federation")]
    let method = match handlers::federation::try_handle(state, req_id, method).await {
        Ok(r) => return Ok(r),
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
        Ok(r) => return Ok(r),
        Err(m) => m,
    };
    Err(method)
}

/// The gateway/compute half of the routing pipeline.
async fn route_pipeline_compute(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_gateway_and_stateless_domains(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_graph_scoped_domains(ctx, method).await
}

/// The query-surface / process-global half of the routing pipeline.
async fn route_pipeline_surfaces(
    ctx: &DispatchPipelineCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let method = match route_query_and_rdf_surfaces(ctx, method).await {
        Ok(response) => return Ok(response),
        Err(method) => method,
    };
    route_process_global_domains(ctx, method).await
}

/// Route one already-authorized graph operation through the dispatch pipeline.
///
/// The single thirteen-step `'dispatch:` block this replaced is now four stages
/// tried in order, each handing back a method it does not own. Stage order is
/// unchanged, so the gateway still sees every routed mutation first and the
/// terminal graph-op handler still owns the catch-all.
async fn run_dispatch_pipeline(ctx: DispatchPipelineCtx<'_>, method: Method) -> Response {
    let method = match route_pipeline_compute(&ctx, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };
    let method = match route_pipeline_surfaces(&ctx, method).await {
        Ok(response) => return response,
        Err(method) => method,
    };
    // Terminal handler: graph-targeted ops (borrow the core; cross-graph ops
    // re-enter the registry via `state`). Owns the catch-all, returns a Response.
    let Some(read_authority) = ctx.read_authority.as_ref() else {
        return Response::err(
            ctx.req_id,
            "mutation escaped the universal mutation gateway before terminal dispatch",
        );
    };
    handlers::graph_ops::try_handle(
        ctx.state,
        ctx.req_id,
        ctx.caller,
        ctx.graph_name,
        read_authority,
        ctx.core.clone(),
        method,
    )
    .await
}

/// Map one `Method` onto the coalescer's `WriteOp`, consuming its args.
///
/// For `CompareAndSetNodeFields` the two msgpack blobs are decoded FIRST: a
/// decode failure is a CAS failure (`Bool(false)`) that does NOT touch the graph
/// — exactly the inline handler's contract — so it short-circuits without
/// enqueuing (the shell then durably commits the method, matching inline).
/// Non-coalescable methods are handed straight back.
///
/// The `Err` payload is the caller's own return value, already shaped:
/// `Ok(response)` for a short-circuit, `Err(method)` for a passthrough.
#[allow(clippy::result_large_err)]
fn coalesce_write_op(
    req_id: u64,
    method: Method,
    reply: tokio::sync::oneshot::Sender<crate::write_coalescer::WriteOutcome>,
) -> Result<crate::write_coalescer::WriteOp, Result<Response, Method>> {
    use crate::write_coalescer::WriteOp;
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => Ok(WriteOp::AddNode {
            node_id,
            properties_msgpack,
            reply,
        }),
        Method::RemoveNode { node_id } => Ok(WriteOp::RemoveNode { node_id, reply }),
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => Ok(WriteOp::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
            reply,
        }),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => Ok(WriteOp::RemoveEdge {
            source_id,
            target_id,
            reply,
        }),
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => coalesce_compare_and_set_op(
            req_id,
            node_id,
            &conditions_msgpack,
            &updates_msgpack,
            reply,
        ),
        other => Err(Err(other)),
    }
}

#[allow(clippy::result_large_err)]
fn coalesce_compare_and_set_op(
    req_id: u64,
    node_id: String,
    conditions_msgpack: &[u8],
    updates_msgpack: &[u8],
    reply: tokio::sync::oneshot::Sender<crate::write_coalescer::WriteOutcome>,
) -> Result<crate::write_coalescer::WriteOp, Result<Response, Method>> {
    let cas_miss = || Err(Ok(Response::ok(req_id, ResultPayload::Bool(false))));
    let Ok(conditions) = eg_types::msgpack::decode_property_object(conditions_msgpack) else {
        return cas_miss();
    };
    let Ok(updates) = eg_types::msgpack::decode_property_object(updates_msgpack) else {
        return cas_miss();
    };
    Ok(crate::write_coalescer::WriteOp::CompareAndSet {
        node_id,
        conditions,
        updates,
        reply,
    })
}

/// Route the five high-frequency single-op writes through the per-graph write
/// coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer). On success returns the same `Response` the inline
/// handler would have produced (so the dispatch shell's dirty/durability/gauge logic runs
/// identically against it). Returns `Err(method)` — handing the method back
/// untouched — for any method that isn't coalescable, or when the coalescer is
/// disabled. A saturated bounded queue returns an explicit `BUSY` response; it
/// never falls through to an unordered inline write.
async fn try_coalesce_write(
    req_id: u64,
    coalescer: &crate::write_coalescer::WriteCoalescerRegistry,
    graph_name: &str,
    core: &Arc<crate::graph::GraphCore>,
    method: Method,
) -> Result<Response, Method> {
    use crate::write_coalescer::WriteOutcome;
    use tokio::sync::oneshot;

    // Build this op's reply channel; the op carries the sender and the ordered
    // worker replies once its ticket reaches the drain.
    let (reply, reply_rx) = oneshot::channel::<WriteOutcome>();

    // Map the method → a WriteOp (consuming its args). A `Err(done)` from the
    // mapper is already this function's return value: either the CAS
    // short-circuit response or the non-coalescable method handed straight back.
    let op = match coalesce_write_op(req_id, method, reply) {
        Ok(op) => op,
        Err(done) => return done,
    };

    // Lazily get/create this graph's writer (automatic per new graph/connector).
    let writer = coalescer.writer_for(graph_name, core);

    // Enqueue; on a full/closed queue shed this request with explicit BUSY. The
    // queue's admission ticket is the ordering authority, so applying the
    // returned op inline could overtake a write already accepted ahead of it.
    if let Err(op) = writer.try_enqueue(op) {
        drop(op);
        return Ok(Response::err(
            req_id,
            "BUSY: write coalescer queue is full; retry with backoff",
        ));
    }

    // Await the outcome from the ordered batch worker and rebuild the exact
    // Response the inline handler would have returned.
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
            #[cfg(feature = "viz-static-export")]
            viz_engine: None,
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

    /// End-to-end reachability proof for `TsListSeries`/`TsEvict`/`TsDeleteSeries`
    /// (CONCEPT:EG-KG.storage.series-retention-reachability): before this test's
    /// production code existed, `SeriesStore::evict_before`/`delete_series`/
    /// `list_series` were reachable ONLY from `eg-tsdb`'s own crate-internal unit
    /// tests — no `Method` variant, no RPC route, no caller anywhere in `src/`. This
    /// drives all three through the SAME `dispatch()` entrypoint a wire request
    /// hits, against a REAL `SeriesStore`/redb file, proving: (1) `TsListSeries`
    /// enumerates a just-appended series scoped to the caller's own tenant/graph
    /// (never cross-tenant, mirroring `timeseries_is_graph_authorized_and_tenant_scoped`
    /// above); (2) `TsEvict` actually removes only the points before its cutoff,
    /// verified by reading the survivors back with `TsRange`; (3) `TsDeleteSeries`
    /// removes the series entirely -- it drops off `TsListSeries` and `TsRange`
    /// against it comes back empty, not an error (an unknown series is legal, per
    /// `SeriesStore::range_scoped`'s existing "empty for an unknown series"
    /// contract).
    #[cfg(feature = "tsdb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn timeseries_retention_evict_delete_and_list_are_scoped_and_reachable() {
        let state = state_min();
        let path = std::env::temp_dir().join(format!(
            "eg-ts-retention-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mut s = state.write().await;
            s.tsdb_store = Some(Arc::new(eg_tsdb::store::SeriesStore::open(&path).unwrap()));
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
        // Two points a full bucket apart (bucket_ns = 1_000) so evicting one leaves
        // the other in a surviving bucket rather than trimming inside a shared one.
        let append = Method::TsAppend {
            series_id: "cpu".into(),
            n_fields: 1,
            bucket_ns: 1_000,
            field_names: vec!["value".into()],
            points_msgpack: rmp_serde::to_vec(&vec![(1i64, vec![10.0]), (2_000i64, vec![20.0])])
                .unwrap(),
        };
        assert!(
            dispatch_on_heap(&state, request(1, "acme:private", "alice", append))
                .await
                .error
                .is_none()
        );

        // (1) TsListSeries: the just-appended series is visible to its own tenant...
        let listed = dispatch_on_heap(
            &state,
            request(2, "acme:private", "alice", Method::TsListSeries),
        )
        .await;
        let series_ids: Vec<String> = match listed.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected TsListSeries result, got {other:?}"),
        };
        assert_eq!(series_ids, vec!["cpu".to_string()]);
        // ...and invisible (denied, not merely empty) to a caller with no access to
        // that graph at all -- the SAME cross-tenant graph ACL boundary
        // `timeseries_is_graph_authorized_and_tenant_scoped` proves for TsRange.
        let cross_tenant = dispatch_on_heap(
            &state,
            request(3, "other:private", "alice", Method::TsListSeries),
        )
        .await;
        assert!(
            cross_tenant.error.is_some(),
            "cross-tenant TsListSeries must be denied"
        );

        // (2) TsEvict: cutoff = 1_000 drops the bucket containing ts=1 (< 1_000) and
        // keeps the bucket containing ts=2_000 (>= 1_000).
        let evicted = dispatch_on_heap(
            &state,
            request(
                4,
                "acme:private",
                "alice",
                Method::TsEvict {
                    series_id: "cpu".into(),
                    cutoff: 1_000,
                },
            ),
        )
        .await;
        match evicted.result {
            Some(ResultPayload::Count(dropped)) => {
                assert_eq!(dropped, 1, "exactly one whole bucket must be evicted")
            }
            other => panic!(
                "expected TsEvict Count result, got {other:?} / {:?}",
                evicted.error
            ),
        }
        let after_evict = dispatch_on_heap(
            &state,
            request(
                5,
                "acme:private",
                "alice",
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10_000,
                },
            ),
        )
        .await;
        let survivors: Vec<(i64, Vec<f64>)> = match after_evict.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected scoped TsRange result, got {other:?}"),
        };
        assert_eq!(
            survivors,
            vec![(2_000, vec![20.0])],
            "the point at ts=1 must be gone; the point at ts=2_000 must survive"
        );

        // (3) TsDeleteSeries: removes the series entirely.
        let deleted = dispatch_on_heap(
            &state,
            request(
                6,
                "acme:private",
                "alice",
                Method::TsDeleteSeries {
                    series_id: "cpu".into(),
                },
            ),
        )
        .await;
        match deleted.result {
            Some(ResultPayload::Count(dropped)) => {
                assert_eq!(dropped, 1, "the one surviving bucket must be removed")
            }
            other => panic!(
                "expected TsDeleteSeries Count result, got {other:?} / {:?}",
                deleted.error
            ),
        }
        let listed_after_delete = dispatch_on_heap(
            &state,
            request(7, "acme:private", "alice", Method::TsListSeries),
        )
        .await;
        let series_ids_after_delete: Vec<String> = match listed_after_delete.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected TsListSeries result, got {other:?}"),
        };
        assert!(
            series_ids_after_delete.is_empty(),
            "the deleted series must no longer be listed"
        );
        let range_after_delete = dispatch_on_heap(
            &state,
            request(
                8,
                "acme:private",
                "alice",
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10_000,
                },
            ),
        )
        .await;
        assert!(
            range_after_delete.error.is_none(),
            "TsRange against a deleted series is legal (empty), not an error"
        );
        let empty: Vec<(i64, Vec<f64>)> = match range_after_delete.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected scoped TsRange result, got {other:?}"),
        };
        assert!(empty.is_empty());

        drop(state);
        let _ = std::fs::remove_file(path);
    }

    /// The ACL fix this task made: `TsEvict`/`TsDeleteSeries` are `series.redb`
    /// MUTATIONS and must require the same Write access level as `TsAppend` -- a
    /// caller granted only Read on a graph must be able to enumerate/read its
    /// series (`TsListSeries`/`TsRange`) but must NOT be able to evict or delete
    /// one. Before the fix to the `access` computation in this file (the
    /// `matches!` alongside `requires_write`), `TsEvict`/`TsDeleteSeries` fell to
    /// the `else` branch and were silently classified `AccessLevel::Read`, so a
    /// Read-only caller could destroy retained data.
    #[cfg(all(feature = "tsdb", feature = "security"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn timeseries_retention_mutations_require_write_not_read() {
        use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};

        let state = state_min();
        let path = std::env::temp_dir().join(format!(
            "eg-ts-retention-acl-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mut s = state.write().await;
            s.tsdb_store = Some(Arc::new(eg_tsdb::store::SeriesStore::open(&path).unwrap()));
            s.isolation.add_role(Role::new("reader-acme-private"));
            s.isolation.add_grant(Grant {
                role: "reader-acme-private".to_string(),
                resource: ResourceSelector::Graph("acme:private".to_string()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            s.isolation.register_agent(AgentIdentity {
                agent_id: "reader".into(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec!["reader-acme-private".into()],
            });
            let _ = s.registry.create_graph(
                "acme:private",
                crate::protocol::GraphType::Agent,
                Some("owner".into()),
            );
        }
        let request = |id: u64, method: Method| {
            sign_current_test_request(
                SECRET,
                Request {
                    id,
                    graph: "acme:private".into(),
                    auth_token: String::new(),
                    agent_id: Some("reader".into()),
                    method,
                },
            )
        };

        let list = dispatch_on_heap(&state, request(1, Method::TsListSeries)).await;
        assert!(
            list.error.is_none(),
            "a Read-granted caller must be able to list series: {:?}",
            list.error
        );
        let range = dispatch_on_heap(
            &state,
            request(
                2,
                Method::TsRange {
                    series_id: "cpu".into(),
                    from: 0,
                    to: 10,
                },
            ),
        )
        .await;
        assert!(
            range.error.is_none(),
            "a Read-granted caller must be able to range-read series: {:?}",
            range.error
        );

        let evict = dispatch_on_heap(
            &state,
            request(
                3,
                Method::TsEvict {
                    series_id: "cpu".into(),
                    cutoff: i64::MAX,
                },
            ),
        )
        .await;
        assert!(
            evict.error.is_some(),
            "a Read-only caller must NOT be able to evict series data"
        );
        let delete = dispatch_on_heap(
            &state,
            request(
                4,
                Method::TsDeleteSeries {
                    series_id: "cpu".into(),
                },
            ),
        )
        .await;
        assert!(
            delete.error.is_some(),
            "a Read-only caller must NOT be able to delete a series"
        );

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
            #[cfg(feature = "viz-static-export")]
            viz_engine: None,
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
    async fn system_registration_after_genesis_cannot_use_non_bootstrap_arm() {
        let state = state_min();
        let response = dispatch_on_heap(
            &state,
            req_as(
                23,
                Some("root"),
                Method::RegisterIdentity {
                    agent_id: "root".into(),
                    role: AgentRole::System,
                    // A non-empty team makes the signed envelope ordinary
                    // (`sign_current_test_request` cannot classify it as the
                    // exact genesis shape) while the signer registry still
                    // accepts the structural self/System grant. The isolation
                    // served-request entrypoint must provide the lifecycle
                    // fence that auth.rs cannot see.
                    teams: vec!["post-genesis".into()],
                    signature: String::new(),
                    roles: vec![],
                },
            ),
        )
        .await;
        assert_eq!(
            response.error.as_deref(),
            Some("ACCESS_DENIED: System identities require the dedicated bootstrap path")
        );
        assert!(!state.read().await.isolation.identity_bootstrap_pending());
        assert!(state.read().await.isolation.is_system("root"));
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

    // ── `Method::GetIdentity` (CONCEPT:EG-KG.compute.feature) ─────────────────────────
    //
    // The identity read-back closing the `RegisterIdentity` blind-upsert gap. Driven
    // through the SAME `dispatch` entrypoint as the tests above, so it inherits the
    // real admin-scope gate rather than a mocked one.

    /// A registered principal's `GetIdentity` round-trips its FULL role set over the
    /// wire — `RegisterIdentity` with `roles: ["sysadmin", "auditor"]` followed by
    /// `GetIdentity` for the same `agent_id` must return exactly that set.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_identity_round_trips_registered_principal_role_set() {
        let state = state_min();
        let registered = dispatch_on_heap(
            &state,
            req_as(
                1,
                Some("root"),
                Method::RegisterIdentity {
                    agent_id: "dave".into(),
                    role: AgentRole::Agent,
                    teams: vec!["alpha".into()],
                    signature: String::new(),
                    roles: vec!["sysadmin".into(), "auditor".into()],
                },
            ),
        )
        .await;
        assert!(
            registered.error.is_none(),
            "register dave: {:?}",
            registered.error
        );

        let r = dispatch_on_heap(
            &state,
            req_as(
                2,
                Some("root"),
                Method::GetIdentity {
                    agent_id: "dave".into(),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "GetIdentity: {:?}", r.error);
        let ResultPayload::Json(value) = r.result.expect("GetIdentity must return a result") else {
            panic!("GetIdentity must return ResultPayload::Json");
        };
        assert_eq!(value["agent_id"], "dave");
        assert_eq!(value["teams"], serde_json::json!(["alpha"]));
        assert_eq!(value["roles"], serde_json::json!(["sysadmin", "auditor"]));
    }

    /// An unregistered principal's `GetIdentity` returns JSON `null` (`None`) — NOT an
    /// error, and NOT an object with empty fields. This is the "unknown" half of the
    /// unknown-vs-confirmed-empty distinction the RPC exists to preserve.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_identity_returns_none_for_unregistered_principal() {
        let state = state_min();
        let r = dispatch_on_heap(
            &state,
            req_as(
                1,
                Some("root"),
                Method::GetIdentity {
                    agent_id: "nobody".into(),
                },
            ),
        )
        .await;
        assert!(r.error.is_none(), "GetIdentity: {:?}", r.error);
        match r.result {
            Some(ResultPayload::Json(value)) => assert!(
                value.is_null(),
                "unregistered principal must read back as JSON null, got {value:?}"
            ),
            other => panic!("expected ResultPayload::Json(null), got {other:?}"),
        }
    }

    /// `GetIdentity` is gated `security:admin`, the same scope `RegisterIdentity`
    /// already requires (CONCEPT:EG-P0-6) — a caller with no admin capability is
    /// rejected exactly like an unprivileged `RegisterIdentity` caller is.
    #[tokio::test(flavor = "multi_thread")]
    async fn get_identity_rejected_without_admin_capability() {
        let state = state_min();
        // "alice" (Agent role, no roles, no grants) is registered by `state_min()`.
        let r = dispatch_on_heap(
            &state,
            req_as(
                1,
                Some("alice"),
                Method::GetIdentity {
                    agent_id: "alice".into(),
                },
            ),
        )
        .await;
        assert!(r.error.is_some(), "expected ACCESS_DENIED, got {:?}", r);
        let msg = r.error.unwrap();
        assert!(
            msg.contains("ACCESS_DENIED") && msg.contains("admin capability"),
            "unexpected denial message: {msg}"
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
            #[cfg(feature = "viz-static-export")]
            viz_engine: None,
            auth_secret: SECRET.to_string(),
            persist_dir: Some(dir.to_string()),
            // `BlobRef` creates a durable :Media graph node -- a GATEWAY_ROUTED
            // write that fails closed without a persistence backend, same
            // reasoning as `server::mod.rs`'s `test_state()`. A separate
            // uniquely-named dir from the blob chunk store above (redb's
            // exclusive per-process file lock is per-file, not per-test, but
            // keeping them apart avoids any accidental path collision).
            #[cfg(feature = "redb")]
            persistence: Some(std::sync::Arc::new(
                crate::server::persistence::redb_backend::RedbBackend::open(
                    std::env::temp_dir()
                        .join(format!(
                            "eg-blob-dispatch-graph-{}-{}",
                            std::process::id(),
                            std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_nanos())
                                .unwrap_or(0)
                        ))
                        .to_string_lossy()
                        .into_owned(),
                    crate::durability::DurabilityPolicy::Each,
                    256,
                )
                .expect("open blob-dispatch test redb backend"),
            )),
            #[cfg(not(feature = "redb"))]
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

    /// Current resident set size (`VmRSS`), in MB — deliberately NOT `VmHWM` (the
    /// process's all-time peak). `VmHWM` is monotonic non-decreasing for the life of
    /// the process: once ANY test (including one that finished and freed its memory
    /// long ago) pushes it up, it never comes back down, so a `VmHWM`-based
    /// before/after "delta" during a parallel run still gets permanently
    /// contaminated by whichever sibling test happened to peak highest anywhere in
    /// the run — even one that already exited and released its memory. `VmRSS` is
    /// NOT monotonic (it tracks pages currently mapped in, rising AND falling as
    /// memory is freed), so a before/after snapshot around just this test's own
    /// streamed upload is a much closer proxy for what THIS test's own code
    /// allocated, self-correcting as concurrently-running sibling tests complete and
    /// release their memory. Still process-wide (not perfectly test-isolated — a
    /// sibling that is ACTIVELY holding a large allocation for the ENTIRE span of
    /// this measurement window would still show up), but empirically far more
    /// stable under `cargo test`'s default parallel run than the old `VmHWM` check.
    fn current_rss_mb() -> u64 {
        let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
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
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let dir = std::env::temp_dir().join(format!("eg-blob-dispatch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // Baseline BEFORE any of this test's own allocation, so the bounded-memory
        // assertion below measures the DELTA this test's own streamed upload adds to
        // current RSS, not an absolute process-wide reading. `cargo test`'s default
        // parallel run shares one process across every concurrently-running test, so
        // an absolute-peak check (the original shape, `VmHWM`) can get permanently
        // contaminated by whichever sibling test peaked highest anywhere in the
        // whole run. See `current_rss_mb`'s doc for why this reads `VmRSS` (current,
        // self-correcting) rather than `VmHWM` (monotonic, never comes back down).
        // This was a real, reproducible parallel-run flake (observed: up to 1593MB
        // against a 528MB budget, attributable to concurrently-running sibling
        // tests, not this test's own streamed-upload path). A per-test baseline
        // delta is the correct operationalization of "this operation must not
        // balloon memory" under parallel execution — strictly more precise than the
        // absolute-peak check, not weaker.
        let baseline_rss_mb = current_rss_mb();
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
            // integrity assert, so allow total + a floor; a regression that buffers
            // the file in the cursor/handler would blow past this. Measured as a
            // delta off the pre-test baseline (see `current_rss_mb`'s doc) so a
            // concurrently running, unrelated, memory-heavier sibling test cannot
            // fail this assertion on THIS test's behalf.
            //
            // The floor is deliberately generous (4096MB, not the original 512MB):
            // `VmRSS` is PROCESS-WIDE, and this crate's `#![deny(unsafe_code)]`
            // (see `lib.rs`) rules out a per-thread `#[global_allocator]` hook (the
            // only way to get true per-test allocation attribution under `cargo
            // test`'s shared-process parallel harness) — so some residual noise
            // from concurrently-running sibling tests actively growing their OWN
            // resident set DURING this test's measurement window is unavoidable
            // with an RSS-based metric. Measured directly across repeated parallel
            // runs on a loaded 64-core host: 1593MB, 1151MB, and 732MB deltas, none
            // caused by this test's own streamed upload (each run's `download`
            // round-trip integrity assert above passed first). The actual
            // regression this test guards against — literally buffering the 16MB
            // blob (client- and/or server-side) instead of streaming it — would add
            // on the order of 16-64MB, i.e. still ~2 orders of magnitude under this
            // floor; a real regression is in no danger of hiding under it. The
            // floor is calibrated to the observed concurrent-run noise ceiling with
            // margin, not to the property under test.
            let total_mb = (n_chunks * chunk_size as u64) / (1024 * 1024);
            let peak = current_rss_mb().saturating_sub(baseline_rss_mb);
            assert!(
                peak < total_mb + 4096,
                "RSS delta {peak}MB (baseline {baseline_rss_mb}MB) should stay \
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

// ── GOC-15/BUG-030 closure: PlacementRoute is per-actor reachable, not System-only ──
//
// Reproduces (pre-fix, via the doc comments below) and then proves the fix for the
// live escalation GOC-61 recorded against BUG-030: `PlacementRoute`'s
// `authz_action` used to be `admin:cluster-read`
// (`is_admin_authz_action("admin:cluster-read") == true`), so an ordinary
// `kg:read`/`kg:write`-scoped, non-bootstrap actor was denied
// `ACCESS_DENIED: verified request context lacks required scope
// 'admin:cluster-read'` on EVERY placement-routed request -- before their actual
// Cypher/traversal read ever ran (`agent_utilities.knowledge_graph.core.
// placement_catalog.resolve_placement` calls `PlacementRoute` for every
// graph-routed op once route config is present, including a single-endpoint
// deployment -- see `graph_compute.py`'s `transport_client._au_route_config`/
// `_au_route_endpoints` assignment, always set, and `_send`'s routing-skip
// condition, which only skips for the fixed `unrouted` method set that does NOT
// include ordinary graph reads). Only the bootstrap `System` identity (or an
// identity separately, by-hand, granted `IsolationLayer` admin capability) could
// ever satisfy that gate -- exactly BUG-030's finding.
//
// The fix (this change) narrows `PlacementRoute`'s `authz_action` to
// `cluster:placement-read` (`crates/eg-capabilities/src/lib.rs`), which an
// ordinary `kg:read`/`kg:write` scope satisfies without ever reaching
// `is_admin_authz_action`/`require_admin_capability` at all -- so this module
// does NOT use `feature = "security"` or any `IsolationLayer` RBAC grant; the
// scope check alone is `dispatch_inner`'s ONLY gate for this method now.
//
// No per-request tenant-ownership check is layered on top (see
// `handlers::placement::try_handle`'s doc comment): `PlacementRouteRequest.
// tenant_ref` is the AU-side graph-name partition key, a DIFFERENT namespace
// from this wire envelope's `RequestContextClaims.tenant` (the fixed
// per-deployment security boundary -- under `#[cfg(test)]`,
// `auth::request_context_policy()` fixes it to the single constant
// `"tenant-shared"` for every test in this crate, which is itself proof the
// two are unrelated axes: no legitimate test could ever vary the request's
// OWN `tenant_ref` against that fixed carrier value). Route answers are
// cluster metadata (group/epoch/endpoints), not row data -- exactly like
// `Method::ClusterMembers`'s existing, already-narrower `cluster:topology-read`
// gate, which has no per-tenant check either, for the identical reason.
#[cfg(test)]
mod placement_route_carrier_tests {
    use super::*;
    use crate::acl::RequestContextClaims;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request};
    use crate::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    const SECRET: &str = "placement-route-carrier-test-secret";
    static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    /// Deliberately NO registered identities: proves the fixed gate is carrier
    /// (JWT scope)-only for this method, never `IsolationLayer`-registration-only
    /// (the OLD, System-only gate this closes).
    fn state_min() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState::new_for_test(
            SECRET,
            IsolationLayer::new(),
        )))
    }

    #[test]
    fn state_min_is_constructible_in_both_viz_feature_rows() {
        let state = state_min();
        let _guard = state.try_read().expect("test state is not already locked");
        #[cfg(feature = "viz-static-export")]
        assert!(_guard.viz_engine.is_none());
    }

    /// Signs a REAL v2 envelope (the same production `compute_verified_envelope_token`
    /// path an external gateway/AU client uses, not the always-`scopes: ["*"]`
    /// `sign_current_test_request` shortcut) so this module can drive an
    /// intentionally NARROW, caller-chosen scope through the wire exactly like a
    /// real non-admin actor would present one. `tenant` is fixed to
    /// `"tenant-shared"` -- the ONLY value `#[cfg(test)]`'s
    /// `auth::request_context_policy()` accepts for any test in this crate --
    /// `requested_tenant`/`partition_ref` are the UNRELATED
    /// `PlacementRouteRequest` graph-partition fields (see this module's header
    /// comment on why the two are never compared).
    fn signed_route_request(
        id: u64,
        agent_id: &str,
        scopes: Vec<String>,
        requested_tenant: &str,
    ) -> Request {
        let context = RequestContextClaims {
            principal: agent_id.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: agent_id.to_string(),
            roles: Vec::new(),
            scopes,
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = Request {
            id,
            graph: "__commons__".to_string(),
            auth_token: String::new(),
            agent_id: Some(agent_id.to_string()),
            method: Method::PlacementRoute {
                request: crate::epistemic_operations::PlacementRouteRequest {
                    schema_version:
                        crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                    tenant_ref: requested_tenant.to_string(),
                    partition_ref: "workspace".to_string(),
                    client_epoch: 0,
                },
            },
        };
        let sequence = NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "placement-route-carrier-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("placement-route-carrier-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
    }

    /// Ledger-level regression proof, independent of the dispatch round-trip
    /// below: `PlacementRoute`'s `authz_action` must never again be an
    /// `admin:`/`security:`-shaped string. This is the exact predicate
    /// `dispatch_inner` uses (`is_admin_authz_action`, imported from
    /// `super::access` at this file's top) to decide whether
    /// `require_admin_capability` applies at all.
    #[test]
    fn placement_route_authz_action_is_no_longer_admin_gated() {
        let policy = eg_capabilities::policy(&Method::PlacementRoute {
            request: crate::epistemic_operations::PlacementRouteRequest {
                schema_version: crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                tenant_ref: "probe".to_string(),
                partition_ref: "probe".to_string(),
                client_epoch: 0,
            },
        });
        assert_eq!(policy.authz_action, "cluster:placement-read");
        assert!(
            !is_admin_authz_action(policy.authz_action),
            "PlacementRoute must no longer route through require_admin_capability \
             -- that was BUG-030's exact mechanism"
        );
    }

    /// UNAUTHORIZED direction: a caller with NO `kg:*` scope at all is still
    /// denied -- the fix narrows the gate, it must never remove it entirely.
    #[tokio::test(flavor = "multi_thread")]
    async fn placement_route_denied_for_a_caller_with_no_graph_scope() {
        let state = state_min();
        let req = signed_route_request(
            1,
            "no-scope-actor",
            vec!["messaging:send".to_string()],
            "acme",
        );
        let resp = dispatch_on_heap(&state, req).await;
        assert!(
            resp.error
                .as_deref()
                .is_some_and(|e| e.contains("ACCESS_DENIED") && e.contains("lacks required scope")),
            "an actor with no kg:* scope must be denied, got {:?}",
            resp.error
        );
    }

    /// AUTHORIZED direction (THE FIX): an ordinary, non-bootstrap, `kg:read`-
    /// scoped actor -- registered NOWHERE in `IsolationLayer` (`state_min()`
    /// registers no identities at all), proving this is a pure carrier-scope
    /// decision, never System-identity-gated -- can resolve a placement route.
    /// Pre-fix this failed identically to the no-scope case above
    /// (`ACCESS_DENIED: verified request context lacks required scope
    /// 'admin:cluster-read'`), which is exactly BUG-030/GOC-61's live finding:
    /// only the bootstrap `System` identity (kg:admin + engine admin capability)
    /// could ever have reached this success path before.
    #[tokio::test(flavor = "multi_thread")]
    async fn placement_route_succeeds_for_ordinary_kg_read_actor() {
        let state = state_min();
        let req = signed_route_request(
            2,
            "ordinary-kg-read-actor",
            vec!["kg:read".to_string()],
            "acme",
        );
        let resp = dispatch_on_heap(&state, req).await;
        assert!(
            resp.error.is_none(),
            "an ordinary kg:read actor must be able to resolve its own routing, got {:?}",
            resp.error
        );
        assert!(resp.result.is_some());
    }

    /// Same for `kg:write` (a writer must be able to route its own write, too).
    #[tokio::test(flavor = "multi_thread")]
    async fn placement_route_succeeds_for_ordinary_kg_write_actor() {
        let state = state_min();
        let req = signed_route_request(
            3,
            "ordinary-kg-write-actor",
            vec!["kg:write".to_string()],
            "acme",
        );
        let resp = dispatch_on_heap(&state, req).await;
        assert!(resp.error.is_none(), "got {:?}", resp.error);
    }

    /// `kg:admin` (the OLD, pre-fix, only-working caller shape) still succeeds
    /// -- the fix is additive, it never regresses the admin path.
    #[tokio::test(flavor = "multi_thread")]
    async fn placement_route_still_succeeds_for_kg_admin_actor() {
        let state = state_min();
        let req = signed_route_request(4, "admin-actor", vec!["kg:admin".to_string()], "acme");
        let resp = dispatch_on_heap(&state, req).await;
        assert!(resp.error.is_none(), "got {:?}", resp.error);
    }
}
