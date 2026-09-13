use super::graph_pipeline::dispatch_graph_op;
#[cfg(feature = "raft")]
use super::request_boundary::dispatch_with_context;
use super::*;

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
pub(super) async fn handle_register_server(
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
pub(super) fn replicated_identity_bootstrap_authorized() -> bool {
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
pub(super) fn replicated_identity_bootstrap_authorized() -> bool {
    false
}

// ── The non-raft arms, made real ────────────────────────────────────────────
//
// `is_replicated_apply`, `replicated_placement_authority` and
// `propose_native_mutation` were `#[cfg(feature = "raft")]` while
// `graph_pipeline.rs`, `request_boundary.rs` and `dispatch.rs` imported them
// unconditionally, so `--all-features` (which turns `raft` on) compiled and the
// pre-commit hook's own `--no-default-features --features full` did not -- three
// E0432 that only the hook's feature set ever saw. Fixing it at the import would
// only move the cfg outward and leave the call sites to grow their own arms; the
// honest answer is that a build without consensus HAS an answer to each of these
// questions, and it is the same one it would give if the node were not a
// follower.

/// Without consensus there is no replicated apply, so no request is one.
#[cfg(not(feature = "raft"))]
pub(crate) fn is_replicated_apply() -> bool {
    false
}

/// Without consensus there is no replicated placement authority to inherit, so a
/// compiled batch keeps the route its own caller supplied.
#[cfg(not(feature = "raft"))]
pub(crate) fn replicated_placement_authority() -> Option<(u64, Option<u64>)> {
    None
}

/// Without consensus there is nowhere to propose a native mutation TO.
///
/// The only caller reaches this after observing
/// `PlacementAuthorityKind::MultiRaft`, which a build without `raft` cannot
/// serve, so this refuses by name rather than silently applying locally -- a
/// local apply would be a single-node write standing in for a replicated one,
/// which is exactly the divergence the placement authority exists to prevent.
#[cfg(not(feature = "raft"))]
pub(super) async fn propose_native_mutation(
    _state: &Arc<RwLock<ServerState>>,
    _request_graph: &str,
    request_id: u64,
    _verified_context: &VerifiedRequestContext,
    _identity_bootstrap: bool,
    _method: Method,
) -> Response {
    Response::err(
        request_id,
        "consensus routing is not available in this build",
    )
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
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
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
        | NativeMutationCommand::NodeInfo { .. }
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
    attempt_nonce: Option<eg_types::contract::Nonce>,
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
        proposal.attempt_nonce,
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
            // A clustered `Commit` answers the bare committed boolean: the consensus
            // path does not see the caller idempotency key the local path tags with.
            handlers::txn::tag_commit_response(
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
                .await,
                false,
                false,
            )
        }
        (coordination, terminal) => {
            terminal_native_response(proposal.request_id, coordination, terminal)
        }
    }
}

/// A coordination-classified command whose apply already produced its terminal
/// result, declared as that command's result.
#[cfg(feature = "raft")]
fn terminal_native_response(
    request_id: u64,
    coordination: NativeCoordination,
    terminal: ResultPayload,
) -> Response {
    match coordination {
        NativeCoordination::TransactionCommit => {
            handlers::txn::tag_commit_response(Response::ok(request_id, terminal), false, false)
        }
        _ => Response::ok(request_id, terminal),
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
pub(super) async fn propose_native_mutation(
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
        attempt_nonce: verified_context.attempt_nonce(),
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
async fn submit_consensus_job_publication_response(
    multi: &Arc<crate::raft::multi::MultiRaft>,
    authority: &CarrierAuthority,
    request_id: u64,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    coordinator_id: &str,
    operation: &str,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    command: crate::raft::NativeMutationCommand,
) -> Result<crate::raft::RaftResponse, String> {
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "raft-job-publication-command",
        coordinator_id,
        operation,
    );
    let committed_at_ms = authoritative_now_ms();
    let mutation = crate::raft::RaftMutationContext::from_verified_request(
        batch_id,
        request_id,
        attempt_nonce,
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
    response.validate()?;
    if let Some(error) = response.native_error {
        return Err(error);
    }
    Ok(response)
}

#[cfg(all(feature = "raft", feature = "jobs"))]
#[allow(clippy::too_many_arguments)]
async fn submit_consensus_job_publication_command(
    multi: &Arc<crate::raft::multi::MultiRaft>,
    authority: &CarrierAuthority,
    request_id: u64,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    coordinator_id: &str,
    operation: &str,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    command: crate::raft::NativeMutationCommand,
) -> Result<ResultPayload, String> {
    let response = submit_consensus_job_publication_response(
        multi,
        authority,
        request_id,
        attempt_nonce,
        coordinator_id,
        operation,
        graph_name,
        graph_type,
        group_id,
        placement_epoch,
        fencing_token,
        command,
    )
    .await?;
    response
        .native_result
        .ok_or_else(|| "job publication command returned no result".to_string())
}

/// Submit a job-publication target commit and return the durable typed receipt.
/// The caller must validate domain-specific publication bytes; this layer only
/// transports the state-machine's exact MutationBatchCommit without deriving a
/// substitute from `applied` or `native_result`.
#[cfg(all(feature = "raft", feature = "jobs"))]
#[allow(clippy::too_many_arguments)]
async fn submit_consensus_job_publication_commit(
    multi: &Arc<crate::raft::multi::MultiRaft>,
    authority: &CarrierAuthority,
    request_id: u64,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    coordinator_id: &str,
    operation: &str,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    command: crate::raft::NativeMutationCommand,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let response = submit_consensus_job_publication_response(
        multi,
        authority,
        request_id,
        attempt_nonce,
        coordinator_id,
        operation,
        graph_name,
        graph_type,
        group_id,
        placement_epoch,
        fencing_token,
        command,
    )
    .await?;
    response
        .native_commit
        .ok_or_else(|| "job publication commit returned no durable receipt".to_string())
}

/// The target group must return a valid committed MutationBatch receipt. The
/// caller retains the typed value until the jobs domain binds it to the
/// prepared batch immediately before scheduler finalization.
#[cfg(all(feature = "raft", feature = "jobs"))]
fn interpret_job_publication_commit(
    request_id: u64,
    outcome: Result<crate::mutation_batch::MutationBatchCommit, String>,
) -> Result<crate::mutation_batch::MutationBatchCommit, Response> {
    match outcome {
        Ok(commit) => commit.validate().map(|()| commit).map_err(|error| {
            Response::err(
                request_id,
                format!("job publication target returned invalid receipt: {error}"),
            )
        }),
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
    let committed = match interpret_job_publication_commit(
        request_id,
        submit_consensus_job_publication_commit(
            &multi,
            authority,
            request_id,
            authority.attempt_nonce(),
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
        Ok(committed) => committed,
        Err(response) => return response,
    };

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
        &prepared,
        &committed,
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
    prepared: &handlers::jobs::PreparedJobPublication,
    committed: &crate::mutation_batch::MutationBatchCommit,
    coordinator_id: &str,
    finalize: crate::raft::NativeMutationCommand,
) -> Response {
    let request_id = control.request_id;
    if let Err(error) = handlers::jobs::validate_job_publication_commit(prepared, committed) {
        return Response::err(
            request_id,
            format!("job publication finalization lost its target receipt: {error}"),
        );
    }
    let control_fence = control
        .control
        .placed
        .then_some(control.control.fencing_token());
    match submit_consensus_job_publication_command(
        control.multi,
        control.authority,
        request_id,
        None,
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
        None,
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
#[derive(Clone, Copy)]
enum TransactionConsensusPhase {
    Decision,
    Finalize,
}

#[cfg(feature = "raft")]
impl TransactionConsensusPhase {
    fn operation(self, commit: bool) -> &'static str {
        match (self, commit) {
            (Self::Decision, true) => "decision-commit",
            (Self::Decision, false) => "decision-abort",
            (Self::Finalize, true) => "finalize-commit",
            (Self::Finalize, false) => "finalize-abort",
        }
    }

    fn command(self, coordinator_id: &str, commit: bool) -> crate::raft::NativeMutationCommand {
        match self {
            Self::Decision => crate::raft::NativeMutationCommand::TransactionDecision {
                coordinator_id: coordinator_id.to_string(),
                commit,
            },
            Self::Finalize => crate::raft::NativeMutationCommand::TransactionFinalize {
                coordinator_id: coordinator_id.to_string(),
                commit,
            },
        }
    }
}

#[cfg(feature = "raft")]
#[allow(clippy::too_many_arguments)]
async fn submit_consensus_transaction_phase(
    phase: TransactionConsensusPhase,
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
        phase.operation(commit),
        control_group,
        control_epoch,
        control_fence,
        graph_type,
        phase.command(coordinator_id, commit),
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
    let decided = submit_consensus_transaction_phase(
        TransactionConsensusPhase::Decision,
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
    submit_consensus_transaction_phase(
        TransactionConsensusPhase::Finalize,
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
    let decided = submit_consensus_transaction_phase(
        TransactionConsensusPhase::Decision,
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
    match submit_consensus_transaction_phase(
        TransactionConsensusPhase::Finalize,
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
