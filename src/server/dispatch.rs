//! The request dispatch shell: authentication, service-level methods, and the
//! graph-operation routing chain. Per-domain mutation kernels own their atomic
//! durability, audit, CDC, and projection publication.

use std::ops::ControlFlow;
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
// NOT `#[cfg(feature = "redb")]`: `server::persistence` is an unconditional module
// (server/mod.rs) and three ungated signatures below -- `reconcile_existing_graph_create`,
// `read_committed_graph_version`, `reconcile_missing_graph_delete` -- name this trait
// unqualified. Gating the import broke every build without `redb`
// (`--no-default-features --features server`) with E0405 while the default full build,
// which enables `redb`, stayed green. See plans/complex/lane-reports/WD10-R-DISPATCH.md.
use super::persistence::PersistenceBackend;
use super::state::ServerState;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Request, Response, ResultPayload};

/// Stable, privacy-safe rejection for a graph lifecycle type that this binary
/// does not support.  The wire decoder normally rejects unknown enum strings
/// before dispatch; keeping this code at the authenticated dispatch boundary
/// is defense in depth for in-process callers and future enum extensions.
const UNSUPPORTED_GRAPH_TYPE: &str = "INVALID_ARGUMENT: unsupported graph type";

/// The identity registry is stored on the control graph, not on the caller's
/// selected data graph.  Keep this boundary exact: accepting an empty or
/// alternate graph here would make the signed request's graph claim
/// meaningless for an identity read-back.
const IDENTITY_GRAPH: &str = "__commons__";
const IDENTITY_GRAPH_SCOPE_ERROR: &str =
    "INVALID_ARGUMENT: GetIdentity requires the __commons__ graph";

fn validate_get_identity_graph(request_graph: &str) -> Result<(), &'static str> {
    if request_graph == IDENTITY_GRAPH {
        Ok(())
    } else {
        Err(IDENTITY_GRAPH_SCOPE_ERROR)
    }
}

#[cfg(test)]
mod identity_graph_scope_tests {
    use super::{validate_get_identity_graph, IDENTITY_GRAPH_SCOPE_ERROR};

    #[test]
    fn accepts_only_the_identity_registry_graph() {
        assert!(validate_get_identity_graph("__commons__").is_ok());
        for graph in ["", "agent:planner", "__commons__ ", "__COMMONS__"] {
            assert_eq!(
                validate_get_identity_graph(graph),
                Err(IDENTITY_GRAPH_SCOPE_ERROR)
            );
        }
    }

    #[test]
    fn scope_error_is_typed_and_does_not_echo_request_graph() {
        assert!(IDENTITY_GRAPH_SCOPE_ERROR.starts_with("INVALID_ARGUMENT:"));
        assert!(!IDENTITY_GRAPH_SCOPE_ERROR.contains("agent:planner"));
    }
}

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
        let redacted = decode(
            redact_resource_status_result(result("other-host"), &request("host-secret"), false)
                .unwrap(),
        );
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
        let aggregate = decode(
            redact_resource_status_result(result("other-host"), &request("host-secret"), true)
                .unwrap(),
        );
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

        let related = decode(
            redact_resource_status_result(result("host-secret"), &request("host-secret"), false)
                .unwrap(),
        );
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
) -> Result<ResultPayload, String> {
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
    ResultPayload::of::<eg_types::result_contract::coordination::ResourceReservationStatus>(result)
}

// ── Fleet server registry (CONCEPT:EG-KG.sharding.server-registry, W2.5) ──────────
// `RegisterServer.name` is bounded by `eg_types::result_contract::cluster::
// is_valid_server_name`, the one definition the registry, its cursors and the
// fleet catalog's discovery records share.
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

mod change_envelope;
mod consensus;
mod graph_pipeline;
mod request_boundary;
mod router;

/// Create one engine-owned global graph through the same durable lifecycle as
/// the public `CreateGraph` method.
///
/// Internal projection workers use this only for reserved graph names after
/// validating their own closed namespace.  Keeping the lifecycle entry here
/// avoids direct `GraphRegistry::create_graph` publication, which would bypass
/// the authoritative registration batch and restart identity.
pub(crate) async fn ensure_internal_global_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    graph_name: &str,
    idempotency_key: &str,
) -> Result<(), String> {
    if state.read().await.registry.exists(graph_name) {
        return Ok(());
    }
    let create_identity = eg_types::contract::Digest256::framed(
        b"eg/internal-graph-lifecycle/v1",
        &[idempotency_key.as_bytes(), graph_name.as_bytes()],
    )?;
    let response = router::create_graph(
        state,
        req_id,
        Some(verified.agent_id().to_string()),
        verified.attempt_nonce(),
        format!("internal-create:{}", create_identity.to_hex()),
        graph_name.to_string(),
        crate::protocol::GraphType::Global,
    )
    .await;
    if let Some(error) = response.error {
        // A concurrent creator may have won after the existence probe.  The
        // graph's durable lifecycle is sufficient; never interpret any other
        // create failure as success.
        if state.read().await.registry.exists(graph_name) {
            return Ok(());
        }
        return Err(error);
    }
    if state.read().await.registry.exists(graph_name) {
        Ok(())
    } else {
        Err(format!(
            "internal graph lifecycle acknowledged without publishing '{graph_name}'"
        ))
    }
}
mod sparql_update;

#[cfg(all(feature = "raft", feature = "jobs"))]
pub(crate) use consensus::{
    apply_replicated_job_publication_commit, apply_replicated_job_publication_finalize,
};
#[cfg(feature = "raft")]
pub(crate) async fn propose_native_mutation(
    state: &Arc<RwLock<ServerState>>,
    request_graph: &str,
    request_id: u64,
    verified_context: &VerifiedRequestContext,
    identity_bootstrap: bool,
    method: Method,
) -> Response {
    consensus::propose_native_mutation(
        state,
        request_graph,
        request_id,
        verified_context,
        identity_bootstrap,
        method,
    )
    .await
}

#[cfg(feature = "raft")]
pub(crate) use consensus::{
    apply_replicated_native, apply_replicated_transaction_decision,
    apply_replicated_transaction_finalize, apply_replicated_transaction_participant,
    apply_replicated_transaction_prepare, ReplicatedParticipantRef,
};
pub(crate) use consensus::{authoritative_now_ms, authoritative_now_secs};
#[cfg(feature = "raft")]
pub(crate) use consensus::{is_replicated_apply, replicated_placement_authority};
pub use request_boundary::dispatch;
#[cfg(any(
    feature = "amqp-wire",
    feature = "mqtt-wire",
    feature = "stomp-wire",
    feature = "mssql-wire",
    feature = "redis-wire",
))]
pub(crate) use request_boundary::dispatch_authenticated_broker_actor;
#[cfg(any(feature = "federation-search", feature = "nl-query"))]
pub(crate) use request_boundary::dispatch_authenticated_local_query;
pub(crate) use request_boundary::dispatch_verified_request;
