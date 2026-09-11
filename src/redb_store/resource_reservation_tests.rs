//! Adversarial unit coverage for the native resource authority helpers.
//!
//! These tests intentionally stay below the full server/Raft harness: they exercise
//! the exact predicates used by the one redb transaction, so the race and restart
//! integration suites can layer on top without duplicating policy logic.

use super::*;
use crate::mutation_batch::{IncarnationId, MutationScopeIdentity, ScopeTenantId};
use crate::redb_store::resource::*;
use crate::redb_store::shard::{Shard, ShardWrite};
use eg_storage::{GraphShardOwner, ScopedRead};

fn host() -> DurableResourceHost {
    DurableResourceHost {
        tenant_ref: "tenant-a".to_string(),
        host_ref: "host-1".to_string(),
        revision: 7,
        capacity: ResourceCapacity {
            cpu_weight: 8,
            memory_mib: 8_192,
            disk_mib: 10_000,
            process_slots: 4,
        },
        observed: ResourceCapacity {
            cpu_weight: 3,
            memory_mib: 1_024,
            disk_mib: 100,
            process_slots: 1,
        },
        heartbeat_at_ms: 1_000,
        heartbeat_ttl_ms: 120_000,
        now_ms: 1_000,
        draining: false,
        quarantined: false,
        labels: vec!["linux".to_string(), "rust".to_string()],
        target_kind: "local".to_string(),
        target_alias: None,
        disk_used_mib: 100,
        disk_capacity_mib: 10_000,
        held_cpu_weight: 2,
        held_memory_mib: 2_048,
        held_disk_mib: 200,
        held_process_slots: 1,
    }
}

fn request() -> ResourceReservationRequest {
    ResourceReservationRequest {
        schema_version: crate::epistemic_operations::ResourceReservationRequestSchemaVersion::V1,
        tenant_ref: "tenant-a".to_string(),
        work_item_id: "work-1".to_string(),
        owner_id: "worker-a".to_string(),
        fence: "1".to_string(),
        lease_epoch: 1,
        fencing_token: 1,
        attempt: 1,
        reservation_id: "reservation-1".to_string(),
        input_fingerprint: format!("v1:{}", "0".repeat(64)),
        profile_name: "rust-build".to_string(),
        profile_version: "1".to_string(),
        host_ref: "host-1".to_string(),
        requirement: ResourceRequirement {
            cpu_weight: 2,
            memory_mib: 1_024,
            disk_mib: 200,
            process_slots: 1,
        },
        target_kind: ResourceReservationRequestTargetKind::Local,
        target_alias: None,
        repository_id: "repo".to_string(),
        branch: "main".to_string(),
        concurrency_key: "rust-build".to_string(),
        concurrency_limit: Some(1),
        repository_exclusive: false,
        branch_exclusive: false,
        required_labels: vec!["linux".to_string()],
        anti_affinity: vec!["compiler".to_string()],
        fairness_group: "default".to_string(),
        fairness_cost: 1,
        disk_low_watermark_mib: Some(500),
        disk_high_watermark_mib: Some(800),
        disk_policy_key: "rust-v1".to_string(),
        reserved_at_ms: 1_000,
        expires_at_ms: 61_000,
        idempotency_key: "reserve-invocation-1".to_string(),
        now_ms: 1_000,
        expected_host_revision: Some(7),
        expected_lifecycle_revision: None,
    }
}

fn work_item_props() -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "node_type": "WorkItem",
        "tenant": "tenant-a",
        "status": "running",
        "lease_owner": "worker-a",
        "last_lease_owner": "worker-a",
        "attempt": 1,
        "lease_epoch": 1,
        "fencing_token": 1,
        "lease_expires_at": 61.0,
        "metadata": {
            "repository_work_item": {
                "contract_version": "1",
                "immutable_input_digest": format!("{}", "1".repeat(64)),
                "tenant_id": resource_b64_urlsafe("tenant-a"),
                "repository_id": resource_b64_urlsafe("repo"),
                "owner_id": resource_b64_urlsafe("worker-a"),
                "branch": resource_b64_urlsafe("main"),
                "job_id": resource_b64_urlsafe("job-1"),
                "target_kind": "local",
                "target_alias": null,
                "priority": 0,
                "queue_deadline": null,
                "resource_reservation": {
                    "schema_version": "1",
                    "resolved_profile_authority": "repository_manager:resource_profile_registry:v1",
                    "profile_name": resource_b64_urlsafe("rust-build"),
                    "profile_version": resource_b64_urlsafe("1"),
                    "cpu_weight": 2,
                    "memory_mib": 1_024,
                    "disk_mib": 200,
                    "process_slots": 1,
                    "host_labels": [resource_b64_urlsafe("linux")],
                    "anti_affinity": [resource_b64_urlsafe("compiler")],
                    "preferred_target": {
                        "contract_version": "1",
                        "kind": "local",
                        "alias": null,
                        "capability_labels": [],
                    },
                    "required_target": null,
                    "repository_id": resource_b64_urlsafe("repo"),
                    "concurrency_key": resource_b64_urlsafe("rust-build"),
                    "concurrency_limit": 1,
                    "repository_exclusive": false,
                    "branch_exclusive": false,
                    "fairness_group": resource_b64_urlsafe("default"),
                    "fairness_cost": 1,
                    "disk_policy_key": resource_b64_urlsafe("rust-v1"),
                    "disk_low_watermark_mib": 500,
                    "disk_high_watermark_mib": 800,
                    "branch": resource_b64_urlsafe("main"),
                    "branch_explicit": true,
                    "base_ref": resource_b64_urlsafe("main"),
                    "target_kind": "local",
                    "target_alias": null,
                    "work_item_input_fingerprint": format!("v1:{}", "1".repeat(64)),
                },
            }
        }
    })
    .as_object()
    .expect("object")
    .clone()
}

fn work_item_props_for_request(
    request: &ResourceReservationRequest,
) -> serde_json::Map<String, serde_json::Value> {
    let mut props = work_item_props();
    props.insert("tenant".to_string(), serde_json::json!(request.tenant_ref));
    props.insert("status".to_string(), serde_json::json!("running"));
    props.insert(
        "lease_owner".to_string(),
        serde_json::json!(request.owner_id),
    );
    props.insert(
        "last_lease_owner".to_string(),
        serde_json::json!(request.owner_id),
    );
    props.insert("attempt".to_string(), serde_json::json!(request.attempt));
    props.insert(
        "lease_epoch".to_string(),
        serde_json::json!(request.lease_epoch),
    );
    props.insert(
        "fencing_token".to_string(),
        serde_json::json!(request.fencing_token),
    );
    props.insert(
        "lease_expires_at".to_string(),
        serde_json::json!(request.expires_at_ms as f64 / 1_000.0),
    );
    let repository = props
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|metadata| metadata.get_mut("repository_work_item"))
        .and_then(serde_json::Value::as_object_mut)
        .expect("repository WorkItem metadata");
    repository.insert(
        "tenant_id".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.tenant_ref)),
    );
    repository.insert(
        "repository_id".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.repository_id)),
    );
    repository.insert(
        "owner_id".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.owner_id)),
    );
    repository.insert(
        "branch".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.branch)),
    );
    repository.insert(
        "target_kind".to_string(),
        serde_json::Value::String(resource_request_target_kind(request.target_kind).to_string()),
    );
    repository.insert(
        "target_alias".to_string(),
        request
            .target_alias
            .as_deref()
            .map_or(serde_json::Value::Null, |alias| {
                serde_json::Value::String(resource_b64_urlsafe(alias))
            }),
    );
    let extension = repository
        .get_mut("resource_reservation")
        .and_then(serde_json::Value::as_object_mut)
        .expect("resource reservation extension");
    extension.insert(
        "profile_name".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.profile_name)),
    );
    extension.insert(
        "profile_version".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.profile_version)),
    );
    extension.insert(
        "cpu_weight".to_string(),
        serde_json::json!(request.requirement.cpu_weight),
    );
    extension.insert(
        "memory_mib".to_string(),
        serde_json::json!(request.requirement.memory_mib),
    );
    extension.insert(
        "disk_mib".to_string(),
        serde_json::json!(request.requirement.disk_mib),
    );
    extension.insert(
        "process_slots".to_string(),
        serde_json::json!(request.requirement.process_slots),
    );
    extension.insert(
        "host_labels".to_string(),
        serde_json::Value::Array(
            request
                .required_labels
                .iter()
                .map(|label| serde_json::Value::String(resource_b64_urlsafe(label)))
                .collect(),
        ),
    );
    extension.insert(
        "anti_affinity".to_string(),
        serde_json::Value::Array(
            request
                .anti_affinity
                .iter()
                .map(|tag| serde_json::Value::String(resource_b64_urlsafe(tag)))
                .collect(),
        ),
    );
    extension.insert(
        "repository_id".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.repository_id)),
    );
    extension.insert(
        "concurrency_key".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.concurrency_key)),
    );
    extension.insert(
        "concurrency_limit".to_string(),
        request
            .concurrency_limit
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    extension.insert(
        "repository_exclusive".to_string(),
        serde_json::json!(request.repository_exclusive),
    );
    extension.insert(
        "branch_exclusive".to_string(),
        serde_json::json!(request.branch_exclusive),
    );
    extension.insert(
        "fairness_group".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.fairness_group)),
    );
    extension.insert(
        "fairness_cost".to_string(),
        serde_json::json!(request.fairness_cost),
    );
    extension.insert(
        "disk_policy_key".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.disk_policy_key)),
    );
    extension.insert(
        "disk_low_watermark_mib".to_string(),
        request
            .disk_low_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    extension.insert(
        "disk_high_watermark_mib".to_string(),
        request
            .disk_high_watermark_mib
            .map_or(serde_json::Value::Null, serde_json::Value::from),
    );
    extension.insert(
        "branch".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.branch)),
    );
    extension.insert("branch_explicit".to_string(), serde_json::json!(true));
    extension.insert(
        "base_ref".to_string(),
        serde_json::Value::String(resource_b64_urlsafe(&request.branch)),
    );
    extension.insert(
        "target_kind".to_string(),
        serde_json::Value::String(resource_request_target_kind(request.target_kind).to_string()),
    );
    extension.insert(
        "target_alias".to_string(),
        request
            .target_alias
            .as_deref()
            .map_or(serde_json::Value::Null, |alias| {
                serde_json::Value::String(resource_b64_urlsafe(alias))
            }),
    );
    props
}

/// One admitted maintenance write over `graph-a` plus the shard file's own
/// control member -- the five-step admitted write every fixture in this file
/// plants rows through (`graph_members` -> `admit_maintenance` ->
/// `ShardWrite::open` -> rows -> `finish` -> `commit_drain`).
///
/// The class is the honest label rather than a knob: a fixture row carries no
/// caller operation identity, so it is the ledgered maintenance class. Being
/// ledgered, it is also a real admitted batch, which is why it advances the
/// graph's authoritative version by exactly one -- see
/// [`current_resource_graph_version`].
///
/// `op_id` must be unique per ATTEMPT, so every caller passes a distinct one.
fn resource_maintenance<T>(
    shard: &Shard,
    graph: &str,
    op_id: &str,
    rows: impl FnOnce(&ShardWrite<'_>) -> T,
) -> T {
    let members = shard
        .graph_members(&[graph])
        .expect("bind the fixture graph on the resource fixture shard");
    let (group, batches) = shard
        .admit_maintenance(&members, op_id)
        .expect("admit resource fixture maintenance");
    let write =
        ShardWrite::open(shard, &group, &members, &batches).expect("open resource fixture write");
    let value = rows(&write);
    write.finish().expect("finish resource fixture write");
    shard
        .commit_drain(group, &batches, 1_000)
        .expect("commit resource fixture write");
    value
}

/// One kernel-issued scoped read bounded to one graph's rows, which is how
/// every assertion in this file reaches a shard row after the cut.
fn resource_read<'s>(shard: &'s Shard, graph: &str) -> ScopedRead<'s, GraphShardOwner> {
    let handle = shard
        .graph(graph)
        .expect("bind the fixture graph on the resource fixture shard");
    shard
        .read(&handle)
        .expect("scoped read of the fixture graph's resource rows")
}

/// One host row, read through the graph's own scope rather than through a
/// write transaction opened solely to inspect it.
fn read_host(shard: &Shard, host_ref: &str) -> Option<DurableResourceHost> {
    let read = resource_read(shard, "graph-a");
    let hosts = read
        .scoped_owner_table(RESOURCE_HOSTS)
        .expect("open resource_hosts on graph-a");
    hosts
        .get(("graph-a", host_ref))
        .expect("read resource host row")
        .map(|row| resource_decode(row.value(), DurableCrypto::none()).expect("decode host row"))
}

/// One reservation row, read through the graph's own scope.
fn read_reservation(shard: &Shard, reservation_id: &str) -> Option<DurableResourceReservation> {
    let read = resource_read(shard, "graph-a");
    let reservations = read
        .scoped_owner_table(RESOURCE_RESERVATIONS)
        .expect("open resource_reservations on graph-a");
    reservations
        .get(("graph-a", reservation_id))
        .expect("read resource reservation row")
        .map(|row| {
            resource_decode(row.value(), DurableCrypto::none()).expect("decode reservation row")
        })
}

/// One persisted batch receipt of `graph-a`'s scope, from the kernel ledger
/// that replaced the shard's private `mutation_batches` table.
fn ledger_receipt(shard: &Shard, batch_id: &str) -> Option<eg_types::MutationBatchRecord> {
    let read = resource_read(shard, "graph-a");
    eg_transaction::read_ledger(&read, batch_id).expect("read kernel ledger receipt")
}

/// Every outbox row of one batch of `graph-a`'s scope, from the kernel ledger
/// that replaced the shard's private `mutation_outbox` table.
fn ledger_outbox(shard: &Shard, batch_id: &str) -> Vec<eg_types::MutationOutboxRecord> {
    let read = resource_read(shard, "graph-a");
    eg_transaction::read_outbox(&read, batch_id).expect("read kernel ledger outbox")
}

/// Open (creating if absent) one shard file and plant this file's standard
/// fixture rows on `graph-a` through one admitted maintenance write.
///
/// `Shard::open` materializes the whole declared `OwnerLayout::GraphShard`
/// census, so the hand-written `initialize_canonical_tables` bootstrap the raw
/// path needed has no counterpart here.
fn seed_resource_database(
    path: &std::path::Path,
    work_items: Vec<(String, serde_json::Map<String, serde_json::Value>)>,
    hosts: Vec<DurableResourceHost>,
) -> Shard {
    let shard = Shard::open(path).expect("open resource race shard");
    resource_maintenance(&shard, "graph-a", "resource-fixture-seed", |write| {
        let graph = write.graph("graph-a").expect("graph-a is a group member");
        let mut nodes = graph.open_scoped_table(NODES).unwrap();
        for (id, props) in work_items {
            let bytes = rmp_serde::to_vec_named(&props).unwrap();
            nodes
                .insert(("graph-a", id.as_str()), bytes.as_slice())
                .unwrap();
        }
        drop(nodes);
        let mut host_table = graph.open_scoped_table(RESOURCE_HOSTS).unwrap();
        for host in &hosts {
            resource_put_host(&mut host_table, "graph-a", host, DurableCrypto::none()).unwrap();
        }
    });
    shard
}

fn resource_batch(
    caller_tenant: &str,
    method: Method,
    batch_id: &str,
    idempotency_key: &str,
    expected_version: u64,
) -> MutationBatch {
    // A CALLER identity, not `graph_scope_identity`. The latter builds
    // `(GRAPH_SHARD_TENANT, graph, incarnation)` -- the scope a batch has AFTER
    // `shard::bind_caller_batch` rewrites it. Handing that back to the binder,
    // whose whole contract is to bind a caller batch onto the shard scope, is
    // refused on its first line: `'__shard__' is the graph shard's reserved
    // scope tenant and cannot be a caller tenant`.
    //
    // The parameter was neutered to `_caller_tenant` during the RF-RULING-004
    // migration -- correctly, since the caller tenant no longer separates two
    // racers -- but the identity was switched to the SHARD identity at the same
    // time, which also meant these fixtures stopped exercising the production
    // binding step at all. Restoring a real caller tenant fixes the refusal and
    // puts `bind_caller_batch` back on the path under test.
    let identity = MutationScopeIdentity::graph(
        ScopeTenantId::new(caller_tenant).expect("valid caller tenant"),
        LogicalName::new("graph-a").expect("valid resource-reservation graph name"),
        IncarnationId::new("incarnation:test:resource-reservation").expect("valid incarnation"),
    );
    let mut batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.to_string(),
        envelope: super::fixture_operation_envelope(
            &identity,
            &format!("principal:sha256:{}", "b".repeat(64)),
            77,
            &idempotency_key.to_string(),
        ),
        // Graph scope, not native: `commit_mutation_batch_inner` (via
        // `mutation_batch_graph_name`) fails closed on any batch that is not
        // graph-scoped, so this is the only route these fixtures actually
        // commit through. `DurabilityDomain::ControlPlane` is one of the
        // "either" domains (`may_own_native_scope` AND legal in a graph
        // scope per the graph arm of `validate_operations`), so it is free to
        // take the graph route here. `"graph-a"` is reused verbatim as the
        // graph name -- the exact literal the old flat `graph` field carried
        // -- rather than inventing a new sentinel.
        identity,
        placement_epoch: 0,
        // A graph-scoped batch is OCC-checked for real:
        // `check_occ_version_and_fence` (in `commit_mutation_batch_inner`)
        // requires `expected_version` to equal the live
        // `MUTATION_GRAPH_VERSION["graph-a"]` row at commit time, or the
        // commit fails closed with `STALE_VERSION`. `expected_version` is
        // supplied by the caller, traced from that test's own seed/commit
        // chain (a fresh `seed_resource_database` starts the counter at
        // `INITIAL_GRAPH_VERSION` = 0; every prior non-replayed commit against
        // the same database advances it by exactly one, replays and raw
        // out-of-band table writes do not).
        version_expectation: VersionExpectation::Graph(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Job,
            domain: DurabilityDomain::ControlPlane,
            method,
        }],
        outbox: Vec::new(),
        created_at_ms: 1_000,
    };
    batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a fixture batch reseals its envelope over its final body");
    batch
}

/// Rebuild a committed fixture as a fresh invocation over the same stable
/// operation. The exact batch carries a consumed nonce and must be refused;
/// transport retries mint a new nonce while retaining the idempotency key.
fn fresh_resource_attempt(batch: &MutationBatch, request_id: u64) -> MutationBatch {
    let mut retry = batch.clone();
    retry.envelope = super::fixture_operation_envelope(
        &retry.identity,
        &format!("principal:sha256:{}", "b".repeat(64)),
        request_id,
        retry.idempotency_key(),
    );
    retry
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a resource retry reseals its final body");
    retry
}

fn commit_resource_batch_at(
    shard: &Shard,
    batch: &MutationBatch,
    crashpoint: Option<MutationBatchCrashpoint>,
) -> Result<MutationBatchCommit, String> {
    #[cfg(feature = "security")]
    let mut audit = AuditTailCache::new();
    commit_mutation_batch_inner(
        shard,
        BatchCommitInput {
            graph_fname: "graph-a",
            batch,
            change: None,
            authoritative_state_msgpack: None,
            crossmodal: None,
            result_msgpack: None,
            committed_at_ms: 1_000,
            audited: true,
            crashpoint,
        },
        DurableCrypto::none(),
        #[cfg(feature = "security")]
        &mut audit,
    )
}

fn commit_resource_batch(
    shard: &Shard,
    batch: &MutationBatch,
) -> Result<MutationBatchCommit, String> {
    commit_resource_batch_at(shard, batch, None)
}

/// The authoritative version of `graph-a`'s bound shard scope, read fresh
/// through `eg_transaction::version` -- the same value `commit::begin` will
/// compare a batch's `version_expectation` against, resolved inside the
/// commit's own transaction.
///
/// Every admitted batch advances it by exactly one, and under this cut that
/// includes the fixture's own maintenance seeds and every coalesced drain, not
/// only caller batches: a coalesced apply IS an admitted batch. So no test may
/// assume the counter starts a caller's chain at zero -- it starts wherever the
/// seed left it, which is what every version input below is derived from.
/// Used by [`commit_racing_resource_batch`] so a genuine multi-thread race can
/// re-derive the real current version on each retry instead of a caller having
/// to guess which thread's commit lands first.
fn current_resource_graph_version(shard: &Shard) -> u64 {
    let read = resource_read(shard, "graph-a");
    eg_transaction::version(&read).expect("authoritative version of graph-a's shard scope")
}

/// Commit a resource batch that genuinely races another thread for the same
/// authoritative version of `graph-a`'s shard scope.
///
/// Two threads racing to commit pre-built, statically-versioned batches can
/// no longer both succeed once the fixture is graph-scoped: only one commit
/// per live version can land, and the loser now fails closed with a real
/// `STALE_VERSION` (previously native scope carried no OCC counter at all,
/// so both commits landed unconditionally). That STALE_VERSION is exactly
/// the signal a real production retrying caller would act on, so this
/// helper mirrors that: read the live version, build the batch against it,
/// and on `STALE_VERSION` re-read and retry. The eventual business decision
/// (Accepted vs. Idempotent/Capacity/Exclusivity/...) is unaffected -- it is
/// still resolved by whichever attempt's write transaction lands first,
/// exactly as before this migration; only the batch-level version
/// bookkeeping is now real. Returns the exact `MutationBatch` that
/// succeeded, since a caller may need it again (e.g. to prove a subsequent
/// commit of the identical batch replays).
///
/// `caller_tenant` no longer separates two racers: RF-RULING-004 application
/// note 2 makes the mutation scope `(GRAPH_SHARD_TENANT, graph, incarnation)`,
/// so both racers resolve to the SAME scope and therefore to the same
/// `(scope, idempotency_key)` durable idempotency key space. What keeps two
/// racers two commits is now their distinct `idempotency_key`s alone, and a
/// same-key pair from two tenants collapses to one batch -- asserted directly
/// by [`mutation_batch_same_attempt_race_has_one_durable_winner_and_replay`].
fn commit_racing_resource_batch(
    shard: &Shard,
    caller_tenant: &str,
    method: Method,
    batch_id: &str,
    idempotency_key: &str,
) -> (MutationBatch, MutationBatchCommit) {
    loop {
        let expected_version = current_resource_graph_version(shard);
        let batch = resource_batch(
            caller_tenant,
            method.clone(),
            batch_id,
            idempotency_key,
            expected_version,
        );
        match commit_resource_batch(shard, &batch) {
            Ok(commit) => return (batch, commit),
            Err(message) if message.starts_with("STALE_VERSION") => continue,
            Err(message) => panic!("resource batch race commit failed: {message}"),
        }
    }
}

fn batch_resource_result(commit: &MutationBatchCommit) -> ResourceReservationResult {
    let bytes = commit
        .record
        .result_msgpack
        .as_ref()
        .expect("resource mutation stores a typed result");
    let payload: crate::protocol::ResultPayload = rmp_serde::from_slice(bytes).unwrap();
    resource_decode_result_payload(payload).unwrap()
}

fn resolved_request() -> (
    ResourceReservationRequest,
    serde_json::Map<String, serde_json::Value>,
) {
    let mut request = request();
    let props = work_item_props_for_request(&request);
    request.input_fingerprint = resource_recomputed_fingerprint(&props, &request)
        .expect("test WorkItem has a complete resolved projection");
    (request, props)
}

fn batch_host_result(commit: &MutationBatchCommit) -> ResourceHostUpdateResult {
    let bytes = commit
        .record
        .result_msgpack
        .as_ref()
        .expect("host mutation stores a typed result");
    let payload: crate::protocol::ResultPayload = rmp_serde::from_slice(bytes).unwrap();
    let crate::protocol::ResultPayload::Raw(bytes) = payload else {
        panic!("host mutation result must be raw typed payload");
    };
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .expect("decode host mutation result")
}

fn single_reserve_decision(
    suffix: &str,
    request: ResourceReservationRequest,
    props: serde_json::Map<String, serde_json::Value>,
    host: DurableResourceHost,
    concurrency_count: Option<u64>,
    anti_affinity_count: Option<(&str, u64)>,
) -> ResourceReservationResult {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-policy-{suffix}-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let shard = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host],
    );
    if concurrency_count.is_some() || anti_affinity_count.is_some() {
        resource_maintenance(&shard, "graph-a", "resource-policy-counter-seed", |write| {
            let graph = write.graph("graph-a").expect("graph-a is a group member");
            if let Some(count) = concurrency_count {
                let mut concurrency = graph.open_scoped_table(RESOURCE_CONCURRENCY).unwrap();
                let key = resource_concurrency_scope_key(&request.concurrency_key);
                concurrency
                    .insert(("graph-a", key.as_str()), count)
                    .unwrap();
                drop(concurrency);
            }
            if let Some((tag, count)) = anti_affinity_count {
                let mut anti_affinity = graph.open_scoped_table(RESOURCE_ANTI_AFFINITY).unwrap();
                anti_affinity
                    .insert(("graph-a", request.host_ref.as_str(), tag), count)
                    .unwrap();
                drop(anti_affinity);
            }
        });
    }
    let tenant_ref = request.tenant_ref.clone();
    let batch = resource_batch(
        &tenant_ref,
        Method::ReserveWorkItemResources { request },
        &format!("batch-policy-{suffix}"),
        &format!("reserve-policy-{suffix}"),
        // Fresh, single-use shard file: this is the only caller commit ever
        // made against it, but the seed (and the optional counter seed above)
        // are admitted batches too, so the counter is wherever they left it.
        current_resource_graph_version(&shard),
    );
    let result =
        batch_resource_result(&commit_resource_batch(&shard, &batch).expect("policy result"));
    drop(shard);
    let _ = std::fs::remove_file(path);
    result
}

#[test]
fn capacity_accounts_for_live_observed_usage_and_checked_last_slot() {
    let host = host();
    let requirement = ResourceRequirement {
        cpu_weight: 3,
        memory_mib: 5_120,
        disk_mib: 9_701,
        process_slots: 2,
    };
    assert!(!resource_capacity_sum(&host, &requirement));
    let one_slot = ResourceRequirement {
        cpu_weight: 3,
        memory_mib: 5_120,
        disk_mib: 9_700,
        process_slots: 1,
    };
    assert!(resource_capacity_sum(&host, &one_slot));
    let overflow = ResourceRequirement {
        cpu_weight: u64::MAX,
        ..one_slot
    };
    assert!(!resource_capacity_sum(&host, &overflow));
}

#[test]
fn native_transaction_policy_refusals_cover_host_and_accounting_guards() {
    let (base, _) = resolved_request();

    let mut labels_request = base.clone();
    labels_request.required_labels = vec!["windows".to_string()];
    let labels_props = work_item_props_for_request(&labels_request);
    labels_request.input_fingerprint =
        resource_recomputed_fingerprint(&labels_props, &labels_request)
            .expect("labels WorkItem projection");
    let labels =
        single_reserve_decision("labels", labels_request, labels_props, host(), None, None);
    assert_eq!(labels.decision, ResourceReservationResultDecision::Labels);
    assert_eq!(labels.held_cpu_weight, 0);

    let mut quarantined_host = host();
    quarantined_host.quarantined = true;
    let quarantined = single_reserve_decision(
        "quarantine",
        base.clone(),
        work_item_props_for_request(&base),
        quarantined_host,
        None,
        None,
    );
    assert_eq!(
        quarantined.decision,
        ResourceReservationResultDecision::Quarantined
    );

    let mut future_heartbeat_host = host();
    future_heartbeat_host.heartbeat_at_ms = 1_001;
    let future_heartbeat = single_reserve_decision(
        "future-heartbeat",
        base.clone(),
        work_item_props_for_request(&base),
        future_heartbeat_host,
        None,
        None,
    );
    assert_eq!(
        future_heartbeat.decision,
        ResourceReservationResultDecision::StaleHost
    );

    let mut stale_request = base.clone();
    stale_request.now_ms = 2_001;
    let mut stale_host = host();
    stale_host.heartbeat_at_ms = 0;
    stale_host.heartbeat_ttl_ms = 1_000;
    let stale_heartbeat = single_reserve_decision(
        "stale-heartbeat",
        stale_request.clone(),
        work_item_props_for_request(&stale_request),
        stale_host,
        None,
        None,
    );
    assert_eq!(
        stale_heartbeat.decision,
        ResourceReservationResultDecision::StaleHost
    );

    let anti_affinity = single_reserve_decision(
        "anti-affinity",
        base.clone(),
        work_item_props_for_request(&base),
        host(),
        None,
        Some(("compiler", 1)),
    );
    assert_eq!(
        anti_affinity.decision,
        ResourceReservationResultDecision::AntiAffinity
    );

    let concurrency = single_reserve_decision(
        "concurrency",
        base.clone(),
        work_item_props_for_request(&base),
        host(),
        Some(1),
        None,
    );
    assert_eq!(
        concurrency.decision,
        ResourceReservationResultDecision::Concurrency
    );

    let mut observed_host = host();
    observed_host.observed.cpu_weight = observed_host.capacity.cpu_weight;
    let observed = single_reserve_decision(
        "observed-capacity",
        base.clone(),
        work_item_props_for_request(&base),
        observed_host,
        None,
        None,
    );
    assert_eq!(
        observed.decision,
        ResourceReservationResultDecision::Capacity
    );

    let mut remote_host = host();
    remote_host.target_kind = "inventory_alias".to_string();
    remote_host.target_alias = Some("remote-a".to_string());
    let target = single_reserve_decision(
        "target-mismatch",
        base.clone(),
        work_item_props_for_request(&base),
        remote_host,
        None,
        None,
    );
    assert_eq!(target.decision, ResourceReservationResultDecision::Policy);

    // A refusal is a decision only; it must never mutate scheduler debt or
    // expose held accounting.  Keep this assertion over every refusal in the
    // matrix so a newly added policy guard cannot accidentally charge debt.
    for refusal in [
        &labels,
        &quarantined,
        &future_heartbeat,
        &stale_heartbeat,
        &anti_affinity,
        &concurrency,
        &observed,
        &target,
    ] {
        assert_eq!(refusal.fairness_debt, 0, "refusal must not accrue debt");
        assert_eq!(refusal.held_cpu_weight, 0, "refusal must not hold CPU");
        assert_eq!(refusal.held_memory_mib, 0, "refusal must hold no memory");
        assert_eq!(refusal.held_disk_mib, 0, "refusal must hold no disk");
        assert_eq!(refusal.held_process_slots, 0, "refusal must hold no slots");
    }
}

#[test]
fn mutation_batch_same_attempt_race_has_one_durable_winner_and_replay() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-batch-race-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let shard = std::sync::Arc::new(seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    ));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let tenant_a = request.tenant_ref.clone();
    let method_a = Method::ReserveWorkItemResources {
        request: request.clone(),
    };
    let mut invocation_b = request.clone();
    invocation_b.idempotency_key = "reserve-same-attempt-b".to_string();
    let tenant_b = invocation_b.tenant_ref.clone();
    let method_b = Method::ReserveWorkItemResources {
        request: invocation_b,
    };
    // Each thread races the other for the same live authoritative version of
    // `graph-a`'s shard scope, so neither can carry a version_expectation fixed
    // up front -- `commit_racing_resource_batch` reads the live version through
    // `eg_transaction::version` and retries on `STALE_VERSION` exactly as a
    // real caller would. See its doc comment.
    //
    // RF-RULING-004 application note 2: the two racers' caller tenants are no
    // longer part of the mutation scope, so what makes these two DISTINCT
    // commits rather than one replay is their distinct `idempotency_key`s
    // ("...-a" / "...-b") alone -- which is what this case needs, because its
    // subject is one durable winner between two genuine same-attempt
    // invocations. The same-key collapse the rule now implies is asserted at
    // the end of this test rather than left to the comment.
    let mut handles = Vec::new();
    for (tenant, method, batch_id, idempotency_key) in [
        (
            tenant_a,
            method_a,
            "batch-same-attempt-a",
            "reserve-same-attempt-a",
        ),
        (
            tenant_b,
            method_b,
            "batch-same-attempt-b",
            "reserve-same-attempt-b",
        ),
    ] {
        let shard = shard.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            commit_racing_resource_batch(&shard, &tenant, method, batch_id, idempotency_key)
        }));
    }
    let results: Vec<(MutationBatch, MutationBatchCommit)> = handles
        .into_iter()
        .map(|handle| handle.join().expect("resource race worker"))
        .collect();
    assert!(results.iter().all(|(_, commit)| !commit.replayed));
    let decisions: Vec<_> = results
        .iter()
        .map(|(_, commit)| batch_resource_result(commit).decision)
        .collect();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Accepted)
            .count(),
        1
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Idempotent)
            .count(),
        1
    );
    let (batch_a, commit_a) = results
        .iter()
        .find(|(batch, _)| batch.batch_id == "batch-same-attempt-a")
        .expect("batch A result");
    let batch_a_decision = batch_resource_result(commit_a);
    let consumed = commit_resource_batch(&shard, batch_a).expect_err("same attempt is consumed");
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
    let retry_batch = fresh_resource_attempt(batch_a, 78);
    let replay = commit_resource_batch(&shard, &retry_batch).expect("transport replay after race");
    assert!(replay.replayed);
    assert_eq!(
        batch_resource_result(&replay).decision,
        batch_a_decision.decision
    );

    // RF-RULING-004 application note 2, asserted rather than assumed: a second
    // caller in a DIFFERENT tenant, sending the same invocation on the same
    // graph under the same idempotency key, now resolves to the SAME mutation
    // scope -- the durable idempotency key is `(scope, idempotency_key)`, not
    // `(tenant, graph, idempotency_key)`. Before this cut the two tenants were
    // two scopes and this would have committed separately; it now replays the
    // batch the first tenant committed. That narrowing is the accepted
    // consequence of deriving the shard scope from the durable graph name
    // alone, and it is what the "-a"/"-b" key split above exists to sidestep.
    let other_tenant_batch = resource_batch(
        "tenant-b",
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        &batch_a.batch_id,
        batch_a.idempotency_key(),
        match batch_a.version_expectation {
            VersionExpectation::Graph(version) => version,
            other => panic!("a graph-scoped fixture batch cannot expect {other:?}"),
        },
    );
    // Asserted on the BOUND identities, not on the fixtures. `resource_batch`
    // deliberately carries a real CALLER identity (see its doc comment) so these
    // fixtures exercise `bind_caller_batch`, the production step that performs
    // the rewrite -- so the two fixtures legitimately differ by caller tenant
    // here, and the scope-collapse invariant only exists on the far side of the
    // binder. Comparing the pre-binding fixtures asserted the opposite of what
    // the message claims.
    let bound_handle = shard.graph("graph-a").expect("graph-a is bound");
    let bind = |batch: &MutationBatch| {
        crate::redb_store::shard::bind_caller_batch(bound_handle.as_ref(), "graph-a", batch)
            .expect("a caller batch binds onto the graph shard scope")
            .identity
    };
    assert_eq!(
        bind(&other_tenant_batch),
        bind(batch_a),
        "the caller's tenant is not part of a graph-shard mutation scope"
    );
    // ... and the second tenant is REFUSED, not served the first tenant's
    // receipt.
    //
    // The shard SCOPE collapses (asserted just above) but the operation replay
    // identity does not: `bind_caller_batch` rebinds the scope identity and the
    // serving principal while the VERIFIED CALLER deliberately survives in the
    // envelope's authority context (RF-RULING-004 application note 1), and that
    // authority is part of the digest the replay ledger compares. Two different
    // callers presenting one key on one graph are therefore two different
    // operations under one key, and the ledger fails closed.
    //
    // The comment that used to stand here predicted the opposite -- that the
    // scope collapse would make the second tenant REPLAY the first -- and
    // asserted it. That prediction was wrong, and had it been right it would
    // have been a cross-tenant information leak: tenant-b would receive the
    // reservation receipt tenant-a committed, for capacity tenant-a holds.
    // Fail-closed is both the actual and the correct behaviour. Corrected
    // 2026-09-11 by the redb_store absolute-green lane (F1).
    let refused = commit_resource_batch(&shard, &other_tenant_batch)
        .expect_err("a second tenant may not reuse another caller's idempotency key");
    assert!(
        refused.contains("IDEMPOTENCY_CONFLICT"),
        "a cross-caller key collision must fail closed, got: {refused}"
    );
    if let Some(reservation_id) = batch_a_decision.reservation_id.as_deref() {
        assert!(
            !refused.contains(reservation_id),
            "the refusal must not disclose the first tenant's receipt: {refused}"
        );
    }

    drop(shard);
    let shard = Shard::open(&path).expect("reopen race shard");
    let stored_host = read_host(&shard, "host-1").expect("host survives race");
    assert_eq!(
        stored_host.held_cpu_weight,
        host().held_cpu_weight + request.requirement.cpu_weight
    );
    assert_eq!(
        stored_host.held_process_slots,
        host().held_process_slots + request.requirement.process_slots
    );
    drop(shard);
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_distinct_reservation_id_same_attempt_refuses_without_recharge() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-distinct-reservation-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let shard = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    );
    // The seed is itself an admitted maintenance batch, so the chain starts at
    // whatever version it produced, read through `eg_transaction::version`.
    let seeded_version = current_resource_graph_version(&shard);
    let first = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-distinct-reservation-first",
        "reserve-distinct-reservation-first",
        seeded_version,
    );
    let accepted = commit_resource_batch(&shard, &first).expect("first same-attempt reserve");
    assert_eq!(
        batch_resource_result(&accepted).decision,
        ResourceReservationResultDecision::Accepted
    );

    let expected_cpu_weight = request.requirement.cpu_weight;
    let mut conflicting_request = request;
    conflicting_request.reservation_id = "reservation-other".to_string();
    conflicting_request.idempotency_key = "reserve-distinct-reservation-other".to_string();
    conflicting_request.input_fingerprint = resource_recomputed_fingerprint(
        &work_item_props_for_request(&conflicting_request),
        &conflicting_request,
    )
    .expect("conflicting caller supplies a self-consistent request fingerprint");
    let conflicting_tenant_ref = conflicting_request.tenant_ref.clone();
    let conflicting = resource_batch(
        &conflicting_tenant_ref,
        Method::ReserveWorkItemResources {
            request: conflicting_request,
        },
        "batch-distinct-reservation-other",
        "reserve-distinct-reservation-other",
        // `first` above committed one prior batch against this shard,
        // advancing the counter by exactly one.
        seeded_version + 1,
    );
    assert_eq!(current_resource_graph_version(&shard), seeded_version + 1);
    let refused = commit_resource_batch(&shard, &conflicting).expect("distinct reservation result");
    assert_eq!(
        batch_resource_result(&refused).decision,
        ResourceReservationResultDecision::Conflict
    );
    let stored_host =
        read_host(&shard, "host-1").expect("host survives distinct reservation refusal");
    assert_eq!(
        stored_host.held_cpu_weight,
        host().held_cpu_weight + expected_cpu_weight
    );
    drop(shard);
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_distinct_work_items_race_for_last_slot() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-last-slot-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (mut request_a, _) = resolved_request();
    request_a.concurrency_limit = None;
    request_a.anti_affinity.clear();
    let props_a = work_item_props_for_request(&request_a);
    request_a.input_fingerprint = resource_recomputed_fingerprint(&props_a, &request_a)
        .expect("last-slot WorkItem has a complete resolved projection");
    let mut request_b = request_a.clone();
    request_b.work_item_id = "work-2".to_string();
    request_b.owner_id = "worker-b".to_string();
    request_b.fence = "2".to_string();
    request_b.lease_epoch = 2;
    request_b.fencing_token = 2;
    request_b.reservation_id = "reservation-2".to_string();
    request_b.idempotency_key = "reserve-last-slot-b".to_string();
    let props_b = work_item_props_for_request(&request_b);
    request_b.input_fingerprint = resource_recomputed_fingerprint(&props_b, &request_b)
        .expect("second WorkItem has a complete resolved projection");
    let mut constrained_host = host();
    constrained_host.capacity.process_slots =
        constrained_host.observed.process_slots + constrained_host.held_process_slots + 1;
    let shard = std::sync::Arc::new(seed_resource_database(
        &path,
        vec![
            (request_a.work_item_id.clone(), props_a),
            (request_b.work_item_id.clone(), props_b),
        ],
        vec![constrained_host.clone()],
    ));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let tenant_a = request_a.tenant_ref.clone();
    let method_a = Method::ReserveWorkItemResources {
        request: request_a.clone(),
    };
    let tenant_b = request_b.tenant_ref.clone();
    let method_b = Method::ReserveWorkItemResources {
        request: request_b.clone(),
    };
    // Both threads race for the same last capacity slot on the same live
    // authoritative version of `graph-a`'s shard scope, so the
    // version_expectation cannot be fixed up front -- see
    // `commit_racing_resource_batch`'s doc comment.
    //
    // RF-RULING-004 application note 2: the two racers' caller tenants are no
    // longer part of that scope, so they no longer separate the two
    // invocations. This case needs both to commit (its subject is which of two
    // genuine reservations wins the last slot), and what keeps them two
    // commits is their distinct `idempotency_key`s -- "reserve-last-slot-a"
    // and "-b", which `request_b` above already carries.
    let handles = [
        (
            tenant_a,
            method_a,
            "batch-last-slot-a",
            "reserve-last-slot-a",
        ),
        (
            tenant_b,
            method_b,
            "batch-last-slot-b",
            "reserve-last-slot-b",
        ),
    ]
    .into_iter()
    .map(|(tenant, method, batch_id, idempotency_key)| {
        let shard = shard.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            let (_, commit) =
                commit_racing_resource_batch(&shard, &tenant, method, batch_id, idempotency_key);
            batch_resource_result(&commit).decision
        })
    })
    .collect::<Vec<_>>();
    let decisions: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("last-slot worker"))
        .collect();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Accepted)
            .count(),
        1
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Capacity)
            .count(),
        1
    );
    drop(shard);
    let shard = Shard::open(&path).expect("reopen last-slot shard");
    let stored_host = read_host(&shard, "host-1").expect("last-slot host survives");
    assert_eq!(
        stored_host.held_process_slots,
        constrained_host.held_process_slots + 1
    );
    drop(shard);
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_transient_refusal_needs_fresh_invocation_but_acceptance_replays() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-fresh-invocation-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let mut draining = host();
    draining.draining = true;
    let shard = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![draining.clone()],
    );
    // The seed is itself an admitted maintenance batch: read the version it
    // produced rather than assuming a caller's chain starts at zero.
    let seeded_version = current_resource_graph_version(&shard);
    let refused_batch = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-transient-refusal",
        "reserve-transient-refusal",
        seeded_version,
    );
    let refused = commit_resource_batch(&shard, &refused_batch).expect("persist transient refusal");
    assert!(!refused.replayed);
    assert_eq!(
        batch_resource_result(&refused).decision,
        ResourceReservationResultDecision::Drained
    );
    {
        let read = resource_read(&shard, "graph-a");
        let fairness = read
            .scoped_owner_table(RESOURCE_FAIRNESS)
            .expect("open resource_fairness on graph-a");
        let key = resource_fairness_scope_key(&request.tenant_ref, &request.fairness_group);
        let debt = fairness
            .get(("graph-a", key.as_str()))
            .expect("read fairness row")
            .map(|row| {
                resource_decode::<DurableResourceFairness>(row.value(), DurableCrypto::none())
                    .expect("decode fairness row")
            })
            .unwrap_or_default()
            .debt;
        assert_eq!(debt, 0, "refused admission must not accrue fairness debt");
    }

    let host_update = ResourceHostUpdateRequest {
        schema_version: crate::epistemic_operations::ResourceHostUpdateRequestSchemaVersion::V1,
        tenant_ref: draining.tenant_ref.clone(),
        host_ref: draining.host_ref.clone(),
        revision: draining.revision + 1,
        capacity: draining.capacity.clone(),
        observed: draining.observed.clone(),
        heartbeat_at_ms: 2_000,
        heartbeat_ttl_ms: draining.heartbeat_ttl_ms,
        now_ms: 2_000,
        draining: false,
        quarantined: false,
        labels: draining.labels.clone(),
        target_kind: ResourceHostUpdateRequestTargetKind::Local,
        target_alias: None,
        disk_used_mib: draining.disk_used_mib,
        disk_capacity_mib: draining.disk_capacity_mib,
    };
    let update_batch = resource_batch(
        &request.tenant_ref,
        Method::UpdateResourceHost {
            request: host_update,
        },
        "batch-clear-drain",
        "host-clear-drain",
        // `refused_batch` above is the one prior commit since the seed (its
        // decision was a refusal, but the batch itself still committed and
        // still advanced the counter by exactly one).
        seeded_version + 1,
    );
    assert_eq!(current_resource_graph_version(&shard), seeded_version + 1);
    let update = commit_resource_batch(&shard, &update_batch).expect("clear host drain");
    assert!(!update.replayed);
    assert_eq!(
        batch_host_result(&update).reason,
        ResourceHostUpdateResultReason::Accepted
    );
    let consumed = commit_resource_batch(&shard, &refused_batch)
        .expect_err("same refusal attempt is consumed");
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");

    let mut fresh_request = request.clone();
    fresh_request.idempotency_key = "reserve-fresh-after-drain".to_string();
    fresh_request.now_ms = 2_000;
    fresh_request.expected_host_revision = Some(draining.revision + 1);
    let fresh_batch = resource_batch(
        &fresh_request.tenant_ref.clone(),
        Method::ReserveWorkItemResources {
            request: fresh_request,
        },
        "batch-fresh-after-drain",
        "reserve-fresh-after-drain",
        // `refused_batch` then `update_batch` each committed once; the
        // intervening `refused_replay` above is a replay of `refused_batch`
        // and does not advance the counter -- asserted, not assumed, by the
        // equality below.
        seeded_version + 2,
    );
    assert_eq!(
        current_resource_graph_version(&shard),
        seeded_version + 2,
        "a replay must not advance the authoritative version"
    );
    let accepted = commit_resource_batch(&shard, &fresh_batch).expect("fresh reserve invocation");
    assert!(!accepted.replayed);
    assert_eq!(
        batch_resource_result(&accepted).decision,
        ResourceReservationResultDecision::Accepted
    );
    let consumed =
        commit_resource_batch(&shard, &fresh_batch).expect_err("same accepted attempt is consumed");
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
    let fresh_retry = fresh_resource_attempt(&fresh_batch, 79);
    let replay = commit_resource_batch(&shard, &fresh_retry).expect("fresh accepted replay");
    assert!(replay.replayed);
    assert_eq!(
        batch_resource_result(&replay).decision,
        ResourceReservationResultDecision::Accepted
    );
    assert_eq!(
        current_resource_graph_version(&shard),
        seeded_version + 3,
        "only `fresh_batch` advanced the counter past `update_batch`"
    );
    drop(shard);
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_cross_host_repository_and_branch_exclusivity_is_atomic() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-cross-host-exclusive-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (mut request_a, _) = resolved_request();
    request_a.concurrency_limit = None;
    request_a.repository_exclusive = true;
    request_a.branch_exclusive = true;
    let props_a = work_item_props_for_request(&request_a);
    request_a.input_fingerprint = resource_recomputed_fingerprint(&props_a, &request_a)
        .expect("exclusive WorkItem has a complete resolved projection");
    let mut request_b = request_a.clone();
    request_b.work_item_id = "work-2".to_string();
    request_b.owner_id = "worker-b".to_string();
    request_b.fence = "2".to_string();
    request_b.lease_epoch = 2;
    request_b.fencing_token = 2;
    request_b.reservation_id = "reservation-2".to_string();
    request_b.host_ref = "host-2".to_string();
    request_b.idempotency_key = "reserve-exclusive-b".to_string();
    let props_b = work_item_props_for_request(&request_b);
    request_b.input_fingerprint = resource_recomputed_fingerprint(&props_b, &request_b)
        .expect("second exclusive WorkItem has a complete resolved projection");
    let host_two = {
        let mut value = host();
        value.host_ref = "host-2".to_string();
        value
    };
    let shard = seed_resource_database(
        &path,
        vec![
            (request_a.work_item_id.clone(), props_a),
            (request_b.work_item_id.clone(), props_b),
        ],
        vec![host(), host_two.clone()],
    );
    let tenant_a = request_a.tenant_ref.clone();
    let method_a = Method::ReserveWorkItemResources {
        request: request_a.clone(),
    };
    let tenant_b = request_b.tenant_ref.clone();
    let method_b = Method::ReserveWorkItemResources { request: request_b };
    let shard = std::sync::Arc::new(shard);
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    // Both threads race for the same exclusivity slot on the same live
    // authoritative version of `graph-a`'s shard scope -- see
    // `commit_racing_resource_batch`'s doc comment.
    //
    // RF-RULING-004 application note 2: the two racers' caller tenants are no
    // longer part of that scope. This case needs both invocations to reach
    // their own commit (its subject is that exactly one of two hosts is
    // charged), so the two are kept distinct by their `idempotency_key`s --
    // "reserve-exclusive-a" and "-b" -- and not by the tenant they arrive on.
    let handles = [
        (
            tenant_a,
            method_a,
            "batch-exclusive-a",
            "reserve-exclusive-a",
        ),
        (
            tenant_b,
            method_b,
            "batch-exclusive-b",
            "reserve-exclusive-b",
        ),
    ]
    .into_iter()
    .map(|(tenant, method, batch_id, idempotency_key)| {
        let shard = shard.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            let (_, commit) =
                commit_racing_resource_batch(&shard, &tenant, method, batch_id, idempotency_key);
            batch_resource_result(&commit).decision
        })
    })
    .collect::<Vec<_>>();
    let decisions: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().expect("exclusive race worker"))
        .collect();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Accepted)
            .count(),
        1
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == ResourceReservationResultDecision::Exclusivity)
            .count(),
        1
    );
    let host_one = read_host(&shard, "host-1").expect("first host remains present");
    let host_two_after = read_host(&shard, "host-2").expect("second host remains present");
    let first_delta = host_one.held_cpu_weight - host().held_cpu_weight;
    let second_delta = host_two_after.held_cpu_weight - host_two.held_cpu_weight;
    assert_eq!(
        [first_delta, second_delta]
            .into_iter()
            .filter(|delta| *delta == request_a.requirement.cpu_weight)
            .count(),
        1,
        "exactly one host wins the exclusivity race"
    );
    assert_eq!(
        first_delta + second_delta,
        request_a.requirement.cpu_weight,
        "exclusive refusal cannot charge both hosts"
    );
    drop(shard);
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_release_and_superseded_reclaim_replay_tombstones_after_reopen() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-lifecycle-batch-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let shard = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    );
    // The seed is itself an admitted maintenance batch: the caller chain
    // starts at the version it produced, read through `eg_transaction::version`.
    let seeded_version = current_resource_graph_version(&shard);
    let reserve_batch = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-lifecycle-reserve",
        "reserve-lifecycle",
        seeded_version,
    );
    let reserved = commit_resource_batch(&shard, &reserve_batch).expect("reserve lifecycle hold");
    assert_eq!(
        batch_resource_result(&reserved).decision,
        ResourceReservationResultDecision::Accepted
    );

    let mut release_request = request.clone();
    release_request.expected_lifecycle_revision = Some(1);
    release_request.now_ms = 2_000;
    release_request.idempotency_key = "release-lifecycle".to_string();
    let release_batch = resource_batch(
        &release_request.tenant_ref,
        Method::ReleaseWorkItemResources {
            request: release_request.clone(),
        },
        "batch-lifecycle-release",
        "release-lifecycle",
        // `reserve_batch` above committed once.
        seeded_version + 1,
    );
    let released = commit_resource_batch(&shard, &release_batch).expect("release lifecycle hold");
    let released_result = batch_resource_result(&released);
    assert_eq!(
        released_result.decision,
        ResourceReservationResultDecision::Accepted
    );
    assert_eq!(
        released_result.state,
        ResourceReservationResultState::Released
    );
    assert_eq!(released_result.held_cpu_weight, 0);
    assert_eq!(released_result.held_memory_mib, 0);
    assert_eq!(released_result.held_disk_mib, 0);
    assert_eq!(released_result.held_process_slots, 0);
    let consumed = commit_resource_batch(&shard, &release_batch)
        .expect_err("same release attempt is consumed");
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
    let release_retry = fresh_resource_attempt(&release_batch, 78);
    let replay_release = commit_resource_batch(&shard, &release_retry).expect("replay release");
    assert!(replay_release.replayed);
    assert_eq!(
        batch_resource_result(&replay_release).decision,
        ResourceReservationResultDecision::Accepted
    );
    let mut stale_release = release_request.clone();
    stale_release.expected_lifecycle_revision = Some(2);
    let stale_release_batch = resource_batch(
        &stale_release.tenant_ref.clone(),
        Method::ReleaseWorkItemResources {
            request: stale_release,
        },
        "batch-lifecycle-release-stale",
        "release-lifecycle-stale",
        // `reserve_batch` then `release_batch` each committed once; the
        // intervening `replay_release` above is a replay of `release_batch`
        // and does not advance the counter.
        seeded_version + 2,
    );
    assert_eq!(
        current_resource_graph_version(&shard),
        seeded_version + 2,
        "a replay must not advance the authoritative version"
    );
    let stale = commit_resource_batch(&shard, &stale_release_batch).expect("stale release result");
    assert_eq!(
        batch_resource_result(&stale).decision,
        ResourceReservationResultDecision::InputConflict
    );

    let (mut reclaim_request, _) = resolved_request();
    reclaim_request.work_item_id = "work-2".to_string();
    reclaim_request.owner_id = "worker-b".to_string();
    reclaim_request.fence = "2".to_string();
    reclaim_request.lease_epoch = 2;
    reclaim_request.fencing_token = 2;
    reclaim_request.reservation_id = "reservation-2".to_string();
    reclaim_request.idempotency_key = "reserve-reclaim".to_string();
    let reclaim_props = work_item_props_for_request(&reclaim_request);
    reclaim_request.input_fingerprint =
        resource_recomputed_fingerprint(&reclaim_props, &reclaim_request)
            .expect("reclaim WorkItem has a complete resolved projection");
    let reclaim_reserve = resource_batch(
        &reclaim_request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: reclaim_request.clone(),
        },
        "batch-reclaim-reserve",
        "reserve-reclaim",
        // `reserve_batch`, `release_batch` and `stale_release_batch` (still a
        // genuine commit despite its InputConflict decision) each committed
        // once, and the second-WorkItem seed below is a fourth admitted batch:
        // every drain bumps the graph version, so an out-of-band row seed is
        // no longer free the way a raw table write was.
        seeded_version + 4,
    );
    let reserve_two = {
        let props = work_item_props_for_request(&reclaim_request);
        resource_maintenance(
            &shard,
            "graph-a",
            "lifecycle-second-work-item-seed",
            |write| {
                let mut nodes = write
                    .graph("graph-a")
                    .expect("graph-a is a group member")
                    .open_scoped_table(NODES)
                    .unwrap();
                let bytes = rmp_serde::to_vec_named(&props).unwrap();
                nodes
                    .insert(
                        ("graph-a", reclaim_request.work_item_id.as_str()),
                        bytes.as_slice(),
                    )
                    .unwrap();
            },
        );
        commit_resource_batch(&shard, &reclaim_reserve).expect("reserve reclaim hold")
    };
    assert_eq!(
        batch_resource_result(&reserve_two).decision,
        ResourceReservationResultDecision::Accepted
    );
    resource_maintenance(
        &shard,
        "graph-a",
        "lifecycle-advance-work-item-attempt",
        |write| {
            let mut nodes = write
                .graph("graph-a")
                .expect("graph-a is a group member")
                .open_scoped_table(NODES)
                .unwrap();
            let current = nodes
                .get(("graph-a", reclaim_request.work_item_id.as_str()))
                .unwrap()
                .map(|value| {
                    decode_durable::<serde_json::Map<String, serde_json::Value>>(value.value())
                })
                .transpose()
                .unwrap()
                .expect("second WorkItem row");
            let mut current = current;
            current.insert("status".to_string(), serde_json::json!("ready"));
            current.insert("lease_owner".to_string(), serde_json::json!(""));
            current.insert(
                "last_lease_owner".to_string(),
                serde_json::json!("worker-b"),
            );
            current.insert("attempt".to_string(), serde_json::json!(2));
            current.insert("lease_epoch".to_string(), serde_json::json!(3));
            current.insert("fencing_token".to_string(), serde_json::json!(3));
            current.insert("lease_expires_at".to_string(), serde_json::json!(0.0));
            let bytes = rmp_serde::to_vec_named(&current).unwrap();
            nodes
                .insert(
                    ("graph-a", reclaim_request.work_item_id.as_str()),
                    bytes.as_slice(),
                )
                .unwrap();
        },
    );
    let mut reclaim = reclaim_request.clone();
    reclaim.expected_lifecycle_revision = Some(1);
    reclaim.now_ms = reclaim.expires_at_ms;
    reclaim.idempotency_key = "reclaim-lifecycle".to_string();
    let reclaim_batch = resource_batch(
        &reclaim.tenant_ref.clone(),
        Method::ReclaimWorkItemResources { request: reclaim },
        "batch-lifecycle-reclaim",
        "reclaim-lifecycle",
        // `reclaim_reserve` above (via `reserve_two`) committed once more, and
        // the two `NODES` seeds bracketing it are admitted maintenance batches
        // now rather than out-of-band table edits, so each of them advances
        // the counter too: the live version is the only honest input here.
        current_resource_graph_version(&shard),
    );
    let reclaimed = commit_resource_batch(&shard, &reclaim_batch).expect("reclaim superseded hold");
    let reclaimed_result = batch_resource_result(&reclaimed);
    assert_eq!(
        reclaimed_result.decision,
        ResourceReservationResultDecision::Accepted
    );
    assert_eq!(
        reclaimed_result.state,
        ResourceReservationResultState::Superseded
    );
    assert_eq!(reclaimed_result.held_cpu_weight, 0);
    assert_eq!(reclaimed_result.held_memory_mib, 0);
    assert_eq!(reclaimed_result.held_disk_mib, 0);
    assert_eq!(reclaimed_result.held_process_slots, 0);
    let consumed = commit_resource_batch(&shard, &reclaim_batch)
        .expect_err("same reclaim attempt is consumed");
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
    let reclaim_retry = fresh_resource_attempt(&reclaim_batch, 79);
    let replay_reclaim = commit_resource_batch(&shard, &reclaim_retry).expect("replay reclaim");
    assert!(replay_reclaim.replayed);
    assert_eq!(
        batch_resource_result(&replay_reclaim).state,
        ResourceReservationResultState::Superseded
    );
    drop(shard);

    let shard = Shard::open(&path).expect("reopen lifecycle shard");
    let released =
        read_reservation(&shard, "reservation-1").expect("released tombstone survives restart");
    assert_eq!(
        released.record.state,
        ResourceReservationRecordState::Released
    );
    assert_eq!(released.held_cpu_weight, 0);
    assert_eq!(released.held_memory_mib, 0);
    assert_eq!(released.held_disk_mib, 0);
    assert_eq!(released.held_process_slots, 0);
    let superseded =
        read_reservation(&shard, "reservation-2").expect("superseded tombstone survives restart");
    assert_eq!(
        superseded.record.state,
        ResourceReservationRecordState::Superseded
    );
    assert_eq!(superseded.held_cpu_weight, 0);
    assert_eq!(superseded.held_memory_mib, 0);
    assert_eq!(superseded.held_disk_mib, 0);
    assert_eq!(superseded.held_process_slots, 0);
    drop(shard);
    let _ = std::fs::remove_file(path);
}

#[test]
fn mutation_batch_resource_crashpoints_reopen_all_or_nothing_and_replay() {
    for (index, point) in [
        MutationBatchCrashpoint::BeforeRows,
        MutationBatchCrashpoint::AfterRowsBeforeMetadata,
        MutationBatchCrashpoint::BeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        let path = std::env::temp_dir().join(format!(
            "eg-resource-crashpoint-{index}-{}-{}.redb",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        let (request, props) = resolved_request();
        let shard = seed_resource_database(
            &path,
            vec![(request.work_item_id.clone(), props)],
            vec![host()],
        );
        let batch = resource_batch(
            &request.tenant_ref,
            Method::ReserveWorkItemResources {
                request: request.clone(),
            },
            &format!("batch-resource-crash-{index}"),
            &format!("reserve-resource-crash-{index}"),
            // Fresh shard file each loop iteration, seeded once.
            current_resource_graph_version(&shard),
        );
        assert!(commit_resource_batch_at(&shard, &batch, Some(point)).is_err());
        drop(shard);
        let shard = Shard::open(&path).expect("reopen precommit resource shard");
        assert!(
            ledger_receipt(&shard, &batch.batch_id).is_none(),
            "a rolled-back resource mutation must not leave a ledger receipt"
        );
        assert!(
            ledger_outbox(&shard, &batch.batch_id).is_empty(),
            "a rolled-back resource mutation must not leave an outbox row"
        );
        let current = read_host(&shard, "host-1").expect("precommit host survives");
        assert_eq!(current.held_cpu_weight, host().held_cpu_weight);
        assert!(read_reservation(&shard, &request.reservation_id).is_none());
        #[cfg(feature = "security")]
        {
            let audit = verify_audit(&shard, "graph-a").expect("verify rollback audit");
            assert!(audit.ok);
            assert_eq!(
                audit.entries, 0,
                "a rolled-back resource mutation must not leave an audit row"
            );
        }
        drop(shard);
        let _ = std::fs::remove_file(path);
    }

    let path = std::env::temp_dir().join(format!(
        "eg-resource-postcommit-crash-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let shard = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    );
    let batch = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-resource-postcommit",
        "reserve-resource-postcommit",
        // Fresh shard file, seeded once.
        current_resource_graph_version(&shard),
    );
    assert!(commit_resource_batch_at(
        &shard,
        &batch,
        Some(MutationBatchCrashpoint::AfterCommitBeforeAck),
    )
    .is_err());
    drop(shard);
    let shard = Shard::open(&path).expect("reopen postcommit resource shard");
    let receipt = ledger_receipt(&shard, &batch.batch_id).expect("postcommit resource receipt");
    assert_eq!(
        receipt.batch.operations.len(),
        1,
        "one logical resource effect"
    );
    assert!(
        receipt.batch.outbox.is_empty(),
        "resource effect has no explicit outbox intent"
    );
    let before_outbox = ledger_outbox(&shard, &batch.batch_id);
    assert!(
        before_outbox.is_empty(),
        "no physical outbox row without an explicit intent"
    );
    let consumed =
        commit_resource_batch(&shard, &batch).expect_err("same postcommit attempt is consumed");
    assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
    let retry_batch = fresh_resource_attempt(&batch, 78);
    let replay = commit_resource_batch(&shard, &retry_batch).expect("postcommit resource replay");
    assert!(replay.replayed);
    assert_eq!(ledger_outbox(&shard, &batch.batch_id), before_outbox);
    let current = read_host(&shard, "host-1").expect("postcommit host survives");
    assert_eq!(
        current.held_cpu_weight,
        host().held_cpu_weight + request.requirement.cpu_weight
    );
    #[cfg(feature = "security")]
    {
        let audit = verify_audit(&shard, "graph-a").expect("verify resource audit");
        assert!(audit.ok);
        assert!(
            audit.entries > 0,
            "accepted resource mutation must be audited"
        );
    }
    drop(shard);
    let _ = std::fs::remove_file(path);
}

#[test]
fn disk_hysteresis_uses_used_mib_boundaries() {
    assert!(!resource_disk_policy_blocked(
        false,
        799,
        Some(500),
        Some(800)
    ));
    assert!(resource_disk_policy_blocked(
        false,
        800,
        Some(500),
        Some(800)
    ));
    assert!(resource_disk_policy_blocked(
        true,
        501,
        Some(500),
        Some(800)
    ));
    assert!(!resource_disk_policy_blocked(
        true,
        500,
        Some(500),
        Some(800)
    ));
    // With equal watermarks, reopening at the shared boundary must not
    // immediately re-enter the open-state high check in the same evaluation.
    assert!(!resource_disk_policy_blocked(
        true,
        500,
        Some(500),
        Some(500)
    ));
}

#[test]
fn host_refresh_rejects_filesystem_shrink_under_existing_held_disk() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-host-refresh-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        // `Shard::open` materializes the whole declared GraphShard census, so
        // no `initialize_canonical_tables` bootstrap is needed or possible.
        let shard = Shard::open(&path).expect("create test shard");
        resource_maintenance(&shard, "graph-a", "host-refresh-shrink", |write| {
            let graph = write.graph("graph-a").expect("graph-a is a group member");
            let mut nodes = graph.open_scoped_table(NODES).unwrap();
            let mut reservations = graph.open_scoped_table(RESOURCE_RESERVATIONS).unwrap();
            let mut tenant_index = graph
                .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
                .unwrap();
            let mut attempts = graph
                .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
                .unwrap();
            let mut hosts = graph.open_scoped_table(RESOURCE_HOSTS).unwrap();
            let mut exclusivity = graph.open_scoped_table(RESOURCE_EXCLUSIVITY).unwrap();
            let mut fairness = graph.open_scoped_table(RESOURCE_FAIRNESS).unwrap();
            let mut concurrency = graph.open_scoped_table(RESOURCE_CONCURRENCY).unwrap();
            let mut anti_affinity = graph.open_scoped_table(RESOURCE_ANTI_AFFINITY).unwrap();
            let mut disk_policies = graph.open_scoped_table(RESOURCE_DISK_POLICIES).unwrap();
            let crypto = DurableCrypto::none();
            let current = host();
            resource_put_host(&mut hosts, "graph-a", &current, crypto).unwrap();
            let refreshed = ResourceHostUpdateRequest {
                schema_version:
                    crate::epistemic_operations::ResourceHostUpdateRequestSchemaVersion::V1,
                tenant_ref: "tenant-a".to_string(),
                host_ref: "host-1".to_string(),
                revision: current.revision + 1,
                capacity: current.capacity.clone(),
                observed: current.observed.clone(),
                heartbeat_at_ms: 1_000,
                heartbeat_ttl_ms: current.heartbeat_ttl_ms,
                now_ms: 2_000,
                draining: false,
                quarantined: false,
                labels: current.labels.clone(),
                target_kind: ResourceHostUpdateRequestTargetKind::Local,
                target_alias: None,
                disk_used_mib: 9_000,
                disk_capacity_mib: 9_100,
            };
            let result = apply_resource_reservation_rows(
                "graph-a",
                &Method::UpdateResourceHost { request: refreshed },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .unwrap()
            .expect("host update returns a typed refusal");
            assert!(matches!(result, crate::protocol::ResultPayload::Raw(_)));
            let after = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                .unwrap()
                .expect("host remains present");
            assert_eq!(after.revision, current.revision);
            assert_eq!(after.disk_used_mib, current.disk_used_mib);
            assert_eq!(after.disk_capacity_mib, current.disk_capacity_mib);
            drop(nodes);
            drop(reservations);
            drop(tenant_index);
            drop(attempts);
            drop(hosts);
            drop(exclusivity);
            drop(fairness);
            drop(concurrency);
            drop(anti_affinity);
            drop(disk_policies);
        });
        drop(shard);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn host_disk_policy_projection_caps_at_schema_bound() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-policy-bound-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        // `Shard::open` materializes the whole declared GraphShard census.
        let shard = Shard::open(&path).expect("create test shard");
        resource_maintenance(&shard, "graph-a", "host-disk-policy-bound", |write| {
            let graph = write.graph("graph-a").expect("graph-a is a group member");
            let mut nodes = graph.open_scoped_table(NODES).unwrap();
            let mut reservations = graph.open_scoped_table(RESOURCE_RESERVATIONS).unwrap();
            let mut tenant_index = graph
                .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
                .unwrap();
            let mut attempts = graph
                .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
                .unwrap();
            let mut hosts = graph.open_scoped_table(RESOURCE_HOSTS).unwrap();
            let mut exclusivity = graph.open_scoped_table(RESOURCE_EXCLUSIVITY).unwrap();
            let mut fairness = graph.open_scoped_table(RESOURCE_FAIRNESS).unwrap();
            let mut concurrency = graph.open_scoped_table(RESOURCE_CONCURRENCY).unwrap();
            let mut anti_affinity = graph.open_scoped_table(RESOURCE_ANTI_AFFINITY).unwrap();
            let mut disk_policies = graph.open_scoped_table(RESOURCE_DISK_POLICIES).unwrap();
            let crypto = DurableCrypto::none();
            let current = host();
            resource_put_host(&mut hosts, "graph-a", &current, crypto).unwrap();
            let policy = DurableResourceDiskPolicy {
                blocked: false,
                low_watermark_mib: Some(500),
                high_watermark_mib: Some(800),
                revision: 1,
            };
            for index in 0..128 {
                let key = format!("host-1\0policy-{index:03}");
                let bytes = resource_encode(&policy, crypto).unwrap();
                disk_policies
                    .insert(("graph-a", key.as_str()), bytes.as_slice())
                    .unwrap();
            }
            let update = |revision| ResourceHostUpdateRequest {
                schema_version:
                    crate::epistemic_operations::ResourceHostUpdateRequestSchemaVersion::V1,
                tenant_ref: "tenant-a".to_string(),
                host_ref: "host-1".to_string(),
                revision,
                capacity: current.capacity.clone(),
                observed: current.observed.clone(),
                heartbeat_at_ms: 1_000,
                heartbeat_ttl_ms: current.heartbeat_ttl_ms,
                now_ms: 2_000,
                draining: false,
                quarantined: false,
                labels: current.labels.clone(),
                target_kind: ResourceHostUpdateRequestTargetKind::Local,
                target_alias: None,
                disk_used_mib: current.disk_used_mib,
                disk_capacity_mib: current.disk_capacity_mib,
            };
            let mut invalid_ttl = update(8);
            invalid_ttl.heartbeat_ttl_ms = 999;
            let error = apply_resource_reservation_rows(
                "graph-a",
                &Method::UpdateResourceHost {
                    request: invalid_ttl,
                },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .expect_err("heartbeat TTL below the schema minimum must fail closed");
            assert!(error.contains("telemetry bounds"));
            apply_resource_reservation_rows(
                "graph-a",
                &Method::UpdateResourceHost { request: update(8) },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .expect("128 policy rows remain representable")
            .expect("host update result");
            assert_eq!(
                resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                    .unwrap()
                    .unwrap()
                    .revision,
                8
            );

            let key = "host-1\0policy-overflow";
            let bytes = resource_encode(&policy, crypto).unwrap();
            disk_policies
                .insert(("graph-a", key), bytes.as_slice())
                .unwrap();
            let error = apply_resource_reservation_rows(
                "graph-a",
                &Method::UpdateResourceHost { request: update(9) },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .expect_err("129 policy rows exceed the generated snapshot bound");
            assert!(error.contains("disk-policy scan exceeds native bound"));
            assert_eq!(
                resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                    .unwrap()
                    .unwrap()
                    .revision,
                8
            );
            drop(nodes);
            drop(reservations);
            drop(tenant_index);
            drop(attempts);
            drop(hosts);
            drop(exclusivity);
            drop(fairness);
            drop(concurrency);
            drop(anti_affinity);
            drop(disk_policies);
        });
        drop(shard);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn orphan_attempt_index_fails_closed_without_recharging_host() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-orphan-attempt-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        // `Shard::open` materializes the whole declared GraphShard census.
        let shard = Shard::open(&path).expect("create test shard");
        resource_maintenance(&shard, "graph-a", "orphan-attempt-index", |write| {
            let graph = write.graph("graph-a").expect("graph-a is a group member");
            let mut nodes = graph.open_scoped_table(NODES).unwrap();
            let mut reservations = graph.open_scoped_table(RESOURCE_RESERVATIONS).unwrap();
            let mut tenant_index = graph
                .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
                .unwrap();
            let mut attempts = graph
                .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
                .unwrap();
            let mut hosts = graph.open_scoped_table(RESOURCE_HOSTS).unwrap();
            let mut exclusivity = graph.open_scoped_table(RESOURCE_EXCLUSIVITY).unwrap();
            let mut fairness = graph.open_scoped_table(RESOURCE_FAIRNESS).unwrap();
            let mut concurrency = graph.open_scoped_table(RESOURCE_CONCURRENCY).unwrap();
            let mut anti_affinity = graph.open_scoped_table(RESOURCE_ANTI_AFFINITY).unwrap();
            let mut disk_policies = graph.open_scoped_table(RESOURCE_DISK_POLICIES).unwrap();
            let crypto = DurableCrypto::none();
            let props = work_item_props();
            let props_bytes = rmp_serde::to_vec_named(&props).unwrap();
            nodes
                .insert(("graph-a", "work-1"), props_bytes.as_slice())
                .unwrap();
            let current_host = host();
            resource_put_host(&mut hosts, "graph-a", &current_host, crypto).unwrap();

            let mut reserve_request = request();
            reserve_request.reservation_id = "reservation-orphan".to_string();
            reserve_request.expected_lifecycle_revision = Some(0);
            reserve_request.input_fingerprint =
                resource_recomputed_fingerprint(&props, &reserve_request)
                    .expect("test WorkItem has a complete resolved projection");
            attempts
                .insert(
                    (
                        "graph-a",
                        reserve_request.work_item_id.as_str(),
                        reserve_request.attempt,
                    ),
                    reserve_request.reservation_id.as_str(),
                )
                .unwrap();

            let error = apply_resource_reservation_rows(
                "graph-a",
                &Method::ReserveWorkItemResources {
                    request: reserve_request.clone(),
                },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .expect_err("an orphan attempt index is corruption, not an idempotent win");
            assert!(error.contains("attempt index references missing reservation"));
            let after = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                .unwrap()
                .expect("host remains present");
            assert_eq!(after.held_cpu_weight, current_host.held_cpu_weight);
            assert_eq!(after.held_memory_mib, current_host.held_memory_mib);
            assert_eq!(after.held_disk_mib, current_host.held_disk_mib);
            assert_eq!(after.held_process_slots, current_host.held_process_slots);
            assert!(reservations
                .get(("graph-a", reserve_request.reservation_id.as_str()))
                .unwrap()
                .is_none());

            // Repair the deliberately injected orphan in this isolated database,
            // then exercise the real WTX reserve/release path.  The first reserve
            // must be the only operation that charges host capacity and counters.
            attempts
                .remove((
                    "graph-a",
                    reserve_request.work_item_id.as_str(),
                    reserve_request.attempt,
                ))
                .unwrap();
            let accepted = apply_resource_reservation_rows(
                "graph-a",
                &Method::ReserveWorkItemResources {
                    request: reserve_request.clone(),
                },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .unwrap()
            .expect("reserve result");
            let accepted = resource_decode_result_payload(accepted).unwrap();
            assert_eq!(
                accepted.decision,
                ResourceReservationResultDecision::Accepted
            );
            assert_eq!(
                accepted.held_cpu_weight,
                reserve_request.requirement.cpu_weight
            );
            let reserved_host = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                .unwrap()
                .expect("reserved host");
            assert_eq!(
                reserved_host.held_cpu_weight,
                current_host.held_cpu_weight + 2
            );

            for expected in [Some(0), Some(2)] {
                let mut stale_release = reserve_request.clone();
                stale_release.expected_lifecycle_revision = expected;
                stale_release.now_ms = 2_000;
                let stale = apply_resource_reservation_rows(
                    "graph-a",
                    &Method::ReleaseWorkItemResources {
                        request: stale_release,
                    },
                    &mut nodes,
                    &mut reservations,
                    &mut tenant_index,
                    &mut attempts,
                    &mut hosts,
                    &mut exclusivity,
                    &mut fairness,
                    &mut concurrency,
                    &mut anti_affinity,
                    &mut disk_policies,
                    crypto,
                )
                .unwrap()
                .expect("stale lifecycle refusal result");
                assert_eq!(
                    resource_decode_result_payload(stale).unwrap().decision,
                    ResourceReservationResultDecision::Stale
                );
            }

            let mut release_request = reserve_request.clone();
            release_request.now_ms = 2_000;
            release_request.expected_lifecycle_revision = Some(1);
            let released = apply_resource_reservation_rows(
                "graph-a",
                &Method::ReleaseWorkItemResources {
                    request: release_request.clone(),
                },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .unwrap()
            .expect("release result");
            let released = resource_decode_result_payload(released).unwrap();
            assert_eq!(
                released.decision,
                ResourceReservationResultDecision::Accepted
            );
            assert_eq!(released.held_cpu_weight, 0);
            assert_eq!(released.state, ResourceReservationResultState::Released);
            let released_host = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                .unwrap()
                .expect("released host");
            assert_eq!(released_host.held_cpu_weight, current_host.held_cpu_weight);

            // The retained tombstone makes the exact release replay idempotent,
            // while a new reservation identity for the same WorkItem attempt is a
            // conflict with the durable attempt winner.
            let replay = apply_resource_reservation_rows(
                "graph-a",
                &Method::ReleaseWorkItemResources {
                    request: release_request,
                },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .unwrap()
            .expect("release replay result");
            assert_eq!(
                resource_decode_result_payload(replay).unwrap().decision,
                ResourceReservationResultDecision::Idempotent
            );
            let mut changed_precondition = reserve_request.clone();
            changed_precondition.now_ms = 3_000;
            changed_precondition.expected_lifecycle_revision = Some(2);
            let changed_precondition_result = apply_resource_reservation_rows(
                "graph-a",
                &Method::ReleaseWorkItemResources {
                    request: changed_precondition,
                },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .unwrap()
            .expect("changed lifecycle refusal result");
            assert_eq!(
                resource_decode_result_payload(changed_precondition_result)
                    .unwrap()
                    .decision,
                ResourceReservationResultDecision::InputConflict
            );
            let mut changed_id = reserve_request.clone();
            changed_id.reservation_id = "reservation-changed".to_string();
            changed_id.idempotency_key = "reserve-invocation-changed".to_string();
            changed_id.input_fingerprint =
                resource_recomputed_fingerprint(&props, &changed_id).unwrap();
            let conflict = apply_resource_reservation_rows(
                "graph-a",
                &Method::ReserveWorkItemResources {
                    request: changed_id,
                },
                &mut nodes,
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .unwrap()
            .expect("changed-id refusal result");
            assert_eq!(
                resource_decode_result_payload(conflict).unwrap().decision,
                ResourceReservationResultDecision::Conflict
            );
            let after_conflict = resource_load_host(&mut hosts, "graph-a", "host-1", crypto)
                .unwrap()
                .expect("host remains after changed-id refusal");
            assert_eq!(after_conflict.held_cpu_weight, current_host.held_cpu_weight);
            drop(nodes);
            drop(reservations);
            drop(tenant_index);
            drop(attempts);
            drop(hosts);
            drop(exclusivity);
            drop(fairness);
            drop(concurrency);
            drop(anti_affinity);
            drop(disk_policies);
        });
        drop(shard);
    }
    // Reopen the durable store and rebuild the scheduler projection solely
    // from the native tombstone/status readers.  Held totals remain zero after
    // release, while exact lifecycle correlation still returns the record.
    {
        let shard = Shard::open(&path).expect("reopen resource shard");
        let stored_request = request();
        let query = ResourceReservationStatusRequest {
            schema_version:
                crate::epistemic_operations::ResourceReservationStatusRequestSchemaVersion::V1,
            tenant_ref: stored_request.tenant_ref.clone(),
            work_item_id: Some(stored_request.work_item_id.clone()),
            reservation_id: Some("reservation-orphan".to_string()),
            host_ref: Some(stored_request.host_ref.clone()),
            owner_id: Some(stored_request.owner_id.clone()),
            fence: Some(stored_request.fence.clone()),
            attempt: Some(stored_request.attempt),
            lease_epoch: Some(stored_request.lease_epoch),
            fencing_token: Some(stored_request.fencing_token),
            input_fingerprint: None,
            fairness_group: Some(stored_request.fairness_group.clone()),
            limit: 10,
            cursor: None,
            now_ms: 3_000,
        };
        let exact = read_resource_reservation(&shard, "graph-a", &query, DurableCrypto::none())
            .expect("exact tombstone query");
        assert_eq!(
            exact.decision,
            ResourceReservationResultDecision::Idempotent
        );
        assert_eq!(exact.state, ResourceReservationResultState::Released);
        assert_eq!(exact.held_cpu_weight, 0);
        let mut wrong_host_query = query.clone();
        wrong_host_query.host_ref = Some("host-2".to_string());
        assert_eq!(
            read_resource_reservation(&shard, "graph-a", &wrong_host_query, DurableCrypto::none())
                .expect_err("wrong host correlation must fail closed"),
            "resource reservation correlation does not match"
        );
        let status =
            read_resource_reservation_status(&shard, "graph-a", &query, DurableCrypto::none())
                .expect("status projection after restart");
        assert!(status.complete);
        assert_eq!(status.reservations.len(), 1);
        assert_eq!(status.reservations[0].held_cpu_weight, 0);
        assert_eq!(
            status.reservations[0].state,
            ResourceReservationSummaryState::Released
        );
        drop(shard);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn exact_request_record_match_binds_fence_and_all_immutable_policy() {
    let request = request();
    let record = resource_build_record(&request, &host(), 7, 1).unwrap();
    assert!(resource_request_matches_record(&request, &record));
    let mut changed_fence = request.clone();
    changed_fence.fence = "2".to_string();
    assert!(!resource_request_matches_record(&changed_fence, &record));
    let mut changed_input = request.clone();
    changed_input.input_fingerprint = "v1:changed".to_string();
    assert!(!resource_request_matches_record(&changed_input, &record));
    let mut changed_reserved_at = request.clone();
    changed_reserved_at.reserved_at_ms += 1;
    changed_reserved_at.expires_at_ms += 1;
    assert!(!resource_request_matches_record(
        &changed_reserved_at,
        &record
    ));
    let mut changed_host_precondition = request.clone();
    changed_host_precondition.expected_host_revision = Some(8);
    assert!(!resource_request_matches_record(
        &changed_host_precondition,
        &record
    ));
    let mut changed_lifecycle_precondition = request.clone();
    changed_lifecycle_precondition.expected_lifecycle_revision = Some(1);
    // Lifecycle CAS is checked by the release/reclaim transaction against the
    // current row, not treated as immutable reserve identity.
    assert!(resource_request_matches_record(
        &changed_lifecycle_precondition,
        &record
    ));
}

#[test]
fn work_item_validation_rejects_legacy_profile_and_future_fence_reclaim() {
    let request = request();
    let mut props = work_item_props();
    props["metadata"]["repository_work_item"]["resource_reservation"]
        .as_object_mut()
        .expect("resource extension")
        .remove("resolved_profile_authority");
    assert!(matches!(
        resource_validate_work_item(&props, &request, false),
        Err(ResourceReservationResultDecision::Policy)
    ));

    props = work_item_props();
    props["metadata"]["repository_work_item"]["resource_reservation"]["profile_version"] =
        serde_json::Value::String(resource_b64_urlsafe("01"));
    assert!(matches!(
        resource_validate_work_item(&props, &request, false),
        Err(ResourceReservationResultDecision::Policy)
    ));

    props = work_item_props();
    let mut future_fence = request;
    future_fence.lease_epoch = 99;
    future_fence.fencing_token = 99;
    future_fence.fence = "99".to_string();
    assert!(matches!(
        resource_validate_work_item(&props, &future_fence, true),
        Err(ResourceReservationResultDecision::Stale)
    ));
}

#[test]
fn reclaim_proves_strictly_newer_attempt_and_current_query_requires_live_lease() {
    let request = request();
    let mut props = work_item_props();
    props["status"] = serde_json::json!("ready");
    props["attempt"] = serde_json::json!(2);
    props["lease_epoch"] = serde_json::json!(2);
    props["fencing_token"] = serde_json::json!(2);
    let fence = resource_validate_work_item(&props, &request, true)
        .expect("strictly newer attempt is a reclaim supersession proof");
    assert!(fence.is_superseded());

    let record = resource_build_record(&request, &host(), 7, 1).unwrap();
    props = work_item_props();
    props["status"] = serde_json::json!("succeeded");
    assert!(!resource_record_work_item_live(&props, &record, 1_000));
    props["status"] = serde_json::json!("running");
    assert!(resource_record_work_item_live(&props, &record, 1_000));
    props["lease_expires_at"] = serde_json::json!(1.0);
    assert!(!resource_record_work_item_live(&props, &record, 1_000));
}

#[test]
fn bounded_query_validation_rejects_untrusted_optional_fields() {
    let request = ResourceReservationStatusRequest {
        schema_version:
            crate::epistemic_operations::ResourceReservationStatusRequestSchemaVersion::V1,
        tenant_ref: "tenant-a".to_string(),
        work_item_id: Some("work-1".to_string()),
        reservation_id: None,
        host_ref: None,
        owner_id: None,
        fence: None,
        attempt: None,
        lease_epoch: None,
        fencing_token: None,
        input_fingerprint: Some("not-a-fingerprint".to_string()),
        fairness_group: None,
        limit: 1,
        cursor: None,
        now_ms: 1,
    };
    assert!(resource_validate_query_request(&request, true).is_err());
    let mut oversized = request;
    oversized.input_fingerprint = None;
    oversized.work_item_id = Some("x".repeat(MAX_RESOURCE_TEXT + 1));
    assert!(resource_validate_query_request(&oversized, true).is_err());
}

#[test]
fn exact_query_missing_reservation_returns_typed_not_found() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-query-missing-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        // `Shard::open` materializes the whole declared GraphShard census, so
        // an empty shard file needs no bootstrap write at all.
        let shard = Shard::open(&path).expect("create query shard");

        let expected = request();
        let query = ResourceReservationStatusRequest {
            schema_version:
                crate::epistemic_operations::ResourceReservationStatusRequestSchemaVersion::V1,
            tenant_ref: expected.tenant_ref.clone(),
            work_item_id: Some(expected.work_item_id.clone()),
            reservation_id: Some("reservation-not-yet-created".to_string()),
            host_ref: Some(expected.host_ref.clone()),
            owner_id: Some(expected.owner_id.clone()),
            fence: Some(expected.fence.clone()),
            attempt: Some(expected.attempt),
            lease_epoch: Some(expected.lease_epoch),
            fencing_token: Some(expected.fencing_token),
            input_fingerprint: None,
            fairness_group: Some(expected.fairness_group.clone()),
            limit: 1,
            cursor: None,
            now_ms: expected.now_ms,
        };
        let result = read_resource_reservation(&shard, "graph-a", &query, DurableCrypto::none())
            .expect("missing reservation is a typed result");
        assert_eq!(result.decision, ResourceReservationResultDecision::NotFound);
        assert_eq!(result.state, ResourceReservationResultState::Absent);
        assert!(result.record.is_none());
        assert_eq!(result.held_cpu_weight, 0);
        drop(shard);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn resource_scope_text_rejects_delimiter_and_control_collisions() {
    let mut embedded_delimiter = request();
    embedded_delimiter.repository_id = "repo\0branch".to_string();
    assert!(resource_validate_request(&embedded_delimiter).is_err());

    let mut embedded_newline = request();
    embedded_newline.fairness_group = "build\nteam".to_string();
    assert!(resource_validate_request(&embedded_newline).is_err());

    let mut query = ResourceReservationStatusRequest {
        schema_version:
            crate::epistemic_operations::ResourceReservationStatusRequestSchemaVersion::V1,
        tenant_ref: "tenant\0a".to_string(),
        work_item_id: None,
        reservation_id: None,
        host_ref: None,
        owner_id: None,
        fence: None,
        attempt: None,
        lease_epoch: None,
        fencing_token: None,
        input_fingerprint: None,
        fairness_group: None,
        limit: 1,
        cursor: None,
        now_ms: 1,
    };
    assert!(resource_validate_query_request(&query, true).is_err());
    query.tenant_ref = "tenant-a".to_string();
    query.host_ref = Some("host\t1".to_string());
    assert!(resource_validate_query_request(&query, true).is_err());
}

#[test]
fn native_retry_comparison_normalizes_only_authoritative_time() {
    let first = Method::ReserveWorkItemResources { request: request() };
    let mut later_request = request();
    later_request.now_ms = 2_000;
    let later = Method::ReserveWorkItemResources {
        request: later_request,
    };
    assert_eq!(
        native_retry_method_key(&first).unwrap(),
        native_retry_method_key(&later).unwrap()
    );

    let mut changed_request = request();
    changed_request.fence = "2".to_string();
    let changed = Method::ReserveWorkItemResources {
        request: changed_request,
    };
    assert_ne!(
        native_retry_method_key(&first).unwrap(),
        native_retry_method_key(&changed).unwrap()
    );

    let operation = |method| MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Job,
        domain: DurabilityDomain::ControlPlane,
        method,
    };
    let stored = vec![operation(first)];
    let replayed = vec![operation(later)];
    assert!(mutation_operations_retry_match(&stored, &replayed).unwrap());
    let mut changed_host = request();
    changed_host.host_ref = "host-2".to_string();
    assert!(!mutation_operations_retry_match(
        &stored,
        &[operation(Method::ReserveWorkItemResources {
            request: changed_host,
        })]
    )
    .unwrap());

    let identity = MutationScopeIdentity::graph(
        ScopeTenantId::new("tenant-a").expect("valid tenant id"),
        LogicalName::new("graph-a").expect("valid graph name"),
        IncarnationId::new("incarnation:test:resource-reservation").expect("valid incarnation id"),
    );
    let mut stored_batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: "batch-1".to_string(),
        envelope: super::fixture_operation_envelope(
            &identity,
            &format!("principal:sha256:{}", "a".repeat(64)),
            1,
            &"idem-1".to_string(),
        ),
        // This batch is never committed through `commit_mutation_batch_inner`
        // (there is no `Shard` anywhere in this test) -- it only feeds
        // `native_resource_placement_replay_match`, which reads only
        // `placement_epoch`/`fencing_token`/`operations`, never `identity` or
        // `version_expectation`. Both are therefore inert to this test's
        // assertions. Built as graph-scoped (like `resource_batch` above) for
        // consistency with the rest of this file's fixtures now that
        // `commit_mutation_batch_inner` requires a graph scope; `"graph-a"`
        // is reused verbatim as the graph name, matching every other fixture
        // here.
        identity,
        placement_epoch: 1,
        // Inert (see above): no commit path ever reads this. `Graph(0)` is
        // the simplest value that satisfies `batch.validate()`'s structural
        // requirement that a graph-scoped batch carry a `Graph` expectation.
        version_expectation: VersionExpectation::Graph(0),
        fencing_token: Some(1),
        authoritative_state: None,
        operations: stored,
        outbox: Vec::new(),
        created_at_ms: 1,
    };
    stored_batch
        .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
        .expect("a fixture batch reseals its envelope over its final body");
    let mut failover_batch = stored_batch.clone();
    failover_batch.placement_epoch = 2;
    failover_batch.fencing_token = Some(1);
    assert!(native_resource_placement_replay_match(
        &stored_batch,
        &failover_batch,
        true
    ));
    let mut backwards = failover_batch.clone();
    backwards.placement_epoch = 0;
    assert!(!native_resource_placement_replay_match(
        &stored_batch,
        &backwards,
        true
    ));
    let mut fabricated_fence = failover_batch;
    fabricated_fence.fencing_token = Some(99);
    assert!(!native_resource_placement_replay_match(
        &stored_batch,
        &fabricated_fence,
        true
    ));
}

#[test]
fn native_retry_rebuilds_projection_outbox_after_authoritative_time_changes() {
    let operation = |method| MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Job,
        domain: DurabilityDomain::ControlPlane,
        method,
    };
    let stored_method = Method::ReserveWorkItemResources { request: request() };
    let mut later_request = request();
    later_request.now_ms = 2_000;
    let proposed_method = Method::ReserveWorkItemResources {
        request: later_request,
    };
    let stored_operations = vec![operation(stored_method)];
    let proposed_operations = vec![operation(proposed_method)];
    assert!(mutation_operations_retry_match(&stored_operations, &proposed_operations).unwrap());
    let stored_payload =
        crate::redb_store::projection_payload_for_operations(&stored_operations).unwrap();
    let proposed_payload =
        crate::redb_store::projection_payload_for_operations(&proposed_operations).unwrap();
    assert_ne!(
        stored_payload, proposed_payload,
        "the producer binds each outbox to its historical authority timestamp"
    );
    let metadata =
        std::collections::BTreeMap::from([("scope_sha256".to_string(), "digest".to_string())]);
    let stored_outbox = vec![MutationOutboxIntent {
        topic: "engine.projection.rebuild".to_string(),
        key: "batch-1".to_string(),
        payload: stored_payload,
        headers: metadata.clone(),
    }];
    let proposed_outbox = vec![MutationOutboxIntent {
        topic: "engine.projection.rebuild".to_string(),
        key: "batch-1".to_string(),
        payload: proposed_payload,
        headers: metadata,
    }];
    assert!(native_retry_outbox_match(
        &stored_operations,
        &proposed_operations,
        &stored_outbox,
        &proposed_outbox,
        true,
    )
    .unwrap());
    let mut tampered = proposed_outbox;
    tampered[0].payload[0] ^= 1;
    assert!(!native_retry_outbox_match(
        &stored_operations,
        &proposed_operations,
        &stored_outbox,
        &tampered,
        true,
    )
    .unwrap());
}

#[test]
fn target_policy_keeps_default_local_and_remote_preference_distinct() {
    let extension = serde_json::json!({
        "preferred_target": {
            "contract_version": "1",
            "kind": "local",
            "alias": null,
            "capability_labels": []
        }
    });
    let extension = extension.as_object().unwrap();
    assert!(resource_target_selection_matches(extension, &host()).unwrap());

    let mut remote = host();
    remote.target_kind = "inventory_alias".to_string();
    remote.target_alias = Some("remote-a".to_string());
    assert!(!resource_target_selection_matches(extension, &remote).unwrap());
}

#[test]
fn selected_host_identity_must_match_wire_target_pair() {
    let local_request = request();
    assert!(resource_selected_target_matches_request(
        &local_request,
        &host()
    ));

    let mut remote_host = host();
    remote_host.target_kind = "inventory_alias".to_string();
    remote_host.target_alias = Some("remote-a".to_string());
    let mut remote_request = local_request.clone();
    remote_request.target_kind = ResourceReservationRequestTargetKind::InventoryAlias;
    remote_request.target_alias = Some("remote-a".to_string());
    assert!(resource_selected_target_matches_request(
        &remote_request,
        &remote_host
    ));
    assert!(!resource_selected_target_matches_request(
        &local_request,
        &remote_host
    ));
    remote_request.target_alias = Some("remote-b".to_string());
    assert!(!resource_selected_target_matches_request(
        &remote_request,
        &remote_host
    ));
}

#[test]
fn work_item_target_projection_must_match_reservation_request() {
    let props = work_item_props();
    let repository = props
        .get("metadata")
        .and_then(serde_json::Value::as_object)
        .and_then(|metadata| metadata.get("repository_work_item"))
        .and_then(serde_json::Value::as_object)
        .expect("repository WorkItem metadata");
    let extension = repository
        .get("resource_reservation")
        .and_then(serde_json::Value::as_object)
        .expect("native resource extension");
    let request = request();
    assert!(resource_extension_matches(repository, extension, &request).unwrap());

    let mut remote_request = request.clone();
    remote_request.target_kind = ResourceReservationRequestTargetKind::InventoryAlias;
    remote_request.target_alias = Some("remote-a".to_string());
    // The original WorkItem declaration is local, but RM may select a remote
    // host when its preferred/required policy permits it; selection is bound
    // to the host row by `resource_selected_target_matches_request`.
    assert!(resource_extension_matches(repository, extension, &remote_request).unwrap());

    let mut remote_extension = extension.clone();
    remote_extension.insert(
        "target_kind".to_string(),
        serde_json::Value::String("inventory_alias".to_string()),
    );
    remote_extension.insert(
        "target_alias".to_string(),
        serde_json::Value::String(resource_b64_urlsafe("remote-a")),
    );
    assert!(!resource_extension_matches(repository, &remote_extension, &request).unwrap());

    let mut changed_digest = extension.clone();
    changed_digest.insert(
        "work_item_input_fingerprint".to_string(),
        serde_json::Value::String(format!("v1:{}", "2".repeat(64))),
    );
    assert!(!resource_extension_matches(repository, &changed_digest, &request).unwrap());
}

#[test]
fn canonical_opaque_decoder_rejects_noncanonical_chunks_and_accepts_unicode() {
    let encoded = resource_b64_urlsafe("é");
    assert_eq!(resource_b64_value(&encoded, "unicode").unwrap(), "é");
    assert!(resource_b64_value(&format!("{}.bad", encoded), "noncanonical").is_err());
}

#[test]
fn direct_request_validation_rejects_noncanonical_profile_version() {
    let mut request = request();
    request.profile_version = "01".to_string();
    let error = resource_validate_request(&request).expect_err("profile version must be canonical");
    assert!(error.contains("canonical integer"));
}

#[test]
fn terminal_result_replays_exact_record_without_held_capacity() {
    let request = request();
    let mut record = resource_build_record(&request, &host(), 7, 1).unwrap();
    record.state = ResourceReservationRecordState::Released;
    record.tombstone = true;
    let result = resource_decode_result_payload(
        resource_result_payload(
            ResourceReservationResultDecision::Idempotent,
            &request,
            Some(record.clone()),
            Some(&host()),
            9,
            Vec::new(),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(resource_request_matches_record(&request, &record));
    assert!(result.tombstone);
    assert_eq!(result.held_cpu_weight, 0);
    assert_eq!(result.held_memory_mib, 0);
    assert_eq!(result.held_disk_mib, 0);
    assert_eq!(result.held_process_slots, 0);
    assert_eq!(result.fairness_debt, 9);
}

#[test]
fn graph_clear_streams_terminal_history_past_bound_and_preserves_active_holds() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-clear-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        // `Shard::open` materializes the whole declared GraphShard census.
        let shard = Shard::open(&path).expect("create test shard");
        resource_maintenance(&shard, "graph-a", "graph-clear-terminal-history", |write| {
            let graph = write.graph("graph-a").expect("graph-a is a group member");
            let mut reservations = graph.open_scoped_table(RESOURCE_RESERVATIONS).unwrap();
            let mut tenant_index = graph
                .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
                .unwrap();
            let mut attempts = graph
                .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
                .unwrap();
            let mut hosts = graph.open_scoped_table(RESOURCE_HOSTS).unwrap();
            let mut exclusivity = graph.open_scoped_table(RESOURCE_EXCLUSIVITY).unwrap();
            let mut fairness = graph.open_scoped_table(RESOURCE_FAIRNESS).unwrap();
            let mut concurrency = graph.open_scoped_table(RESOURCE_CONCURRENCY).unwrap();
            let mut anti_affinity = graph.open_scoped_table(RESOURCE_ANTI_AFFINITY).unwrap();
            let mut disk_policies = graph.open_scoped_table(RESOURCE_DISK_POLICIES).unwrap();
            let crypto = DurableCrypto::none();
            let base_request = request();
            let base_record = resource_build_record(&base_request, &host(), 7, 1).unwrap();

            for index in 0..=MAX_RESOURCE_CLEAR_SCAN {
                // A max-Unicode prefix followed by another byte sorts after the
                // old `..=\u{10ffff}` sentinel.  The production clear/status
                // ranges are open-ended and must still include this legal ID.
                let reservation_id = if index == MAX_RESOURCE_CLEAR_SCAN {
                    "\u{10ffff}terminal-x".to_string()
                } else {
                    format!("terminal-{index:06}")
                };
                let mut record = base_record.clone();
                record.reservation_id = reservation_id.clone();
                if index == 0 {
                    record.tenant_ref = "\u{10ffff}tenant-x".to_string();
                }
                record.state = ResourceReservationRecordState::Released;
                record.tombstone = true;
                record.revision = index as u64 + 1;
                record.lifecycle_revision = index as u64 + 1;
                let tenant = record.tenant_ref.clone();
                let durable = DurableResourceReservation {
                    record,
                    held_cpu_weight: 0,
                    held_memory_mib: 0,
                    held_disk_mib: 0,
                    held_process_slots: 0,
                    fairness_debt: 1,
                };
                let bytes = resource_encode(&durable, crypto).unwrap();
                reservations
                    .insert(("graph-a", reservation_id.as_str()), bytes.as_slice())
                    .unwrap();
                tenant_index
                    .insert(
                        ("graph-a", tenant.as_str(), reservation_id.as_str()),
                        reservation_id.as_str(),
                    )
                    .unwrap();
            }

            let mut max_host = host();
            max_host.host_ref = "\u{10ffff}host-x".to_string();
            hosts
                .insert(
                    ("graph-a", max_host.host_ref.as_str()),
                    resource_encode(&max_host, crypto).unwrap().as_slice(),
                )
                .unwrap();
            let max_policy_key = "\u{10ffff}policy-x";
            let max_policy = DurableResourceDiskPolicy {
                blocked: false,
                low_watermark_mib: Some(1),
                high_watermark_mib: Some(2),
                revision: 1,
            };
            let max_policy_bytes = resource_encode(&max_policy, crypto).unwrap();
            let max_policy_row = format!("host-a\0{max_policy_key}");
            disk_policies
                .insert(
                    ("graph-a", max_policy_row.as_str()),
                    max_policy_bytes.as_slice(),
                )
                .unwrap();

            let active_id = "active-hold";
            let mut active_request = request();
            active_request.reservation_id = active_id.to_string();
            let active_record = resource_build_record(&active_request, &host(), 7, 1).unwrap();
            let active = DurableResourceReservation {
                record: active_record,
                held_cpu_weight: active_request.requirement.cpu_weight,
                held_memory_mib: active_request.requirement.memory_mib,
                held_disk_mib: active_request.requirement.disk_mib,
                held_process_slots: active_request.requirement.process_slots,
                fairness_debt: active_request.fairness_cost,
            };
            let active_bytes = resource_encode(&active, crypto).unwrap();
            reservations
                .insert(("graph-a", active_id), active_bytes.as_slice())
                .unwrap();
            tenant_index
                .insert(("graph-a", "tenant-a", active_id), active_id)
                .unwrap();

            assert!(clear_resource_rows(
                "graph-a",
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .is_err());
            assert!(reservations.get(("graph-a", active_id)).unwrap().is_some());
            assert!(tenant_index
                .get(("graph-a", "tenant-a", active_id))
                .unwrap()
                .is_some());

            reservations.remove(("graph-a", active_id)).unwrap();
            tenant_index
                .remove(("graph-a", "tenant-a", active_id))
                .unwrap();
            clear_resource_rows(
                "graph-a",
                &mut reservations,
                &mut tenant_index,
                &mut attempts,
                &mut hosts,
                &mut exclusivity,
                &mut fairness,
                &mut concurrency,
                &mut anti_affinity,
                &mut disk_policies,
                crypto,
            )
            .expect("terminal history is cleared in bounded chunks");
            // A per-graph prefix scan is `scope_rows()`: it starts at the least
            // key this scope can own and stops on the first key that leaves it, so
            // the open-ended `range(("graph-a", "")..)` + `take_while` shape the
            // raw table needed is now the capability's own bound.
            assert!(reservations.scope_rows().unwrap().next().is_none());
            assert!(tenant_index.scope_rows().unwrap().next().is_none());
            drop(reservations);
            drop(tenant_index);
            drop(attempts);
            drop(hosts);
            drop(exclusivity);
            drop(fairness);
            drop(concurrency);
            drop(anti_affinity);
            drop(disk_policies);
        });
        drop(shard);
    }
    let _ = std::fs::remove_file(path);
}

/// RMDD-29's native WorkItem-authority migration (`work_item_capability::validate_snapshot_nodes`,
/// invoked from `apply_checkpoint` for every graph in the incoming image) now unconditionally
/// refuses any checkpoint image containing a WorkItem-shaped node whose `status` is not
/// `submitted`/`ready` -- by design, a checkpoint restore always purges native claim state
/// atomically before installing the replacement, so it can never carry forward (or manufacture)
/// an ACTIVE lease, lane-linked or not (see `redb_store::development_lane`'s own
/// `REASON_..._NOT_YET_WIRED`-adjacent conflict for the identical interaction on the
/// development-lane side of this migration). That makes `apply_checkpoint` structurally unable to
/// INSTALL the active/leased WorkItem image (`work_item_props_for_request` always sets
/// `status: "running"`) this test needs as a baseline for its actual subject: proving
/// `apply_checkpoint` refuses to orphan or downgrade an ALREADY-held resource domain. Those
/// refusal assertions are unaffected and still exercise the real, unmodified `apply_checkpoint`
/// guard. This helper installs a checkpoint image directly, replicating exactly the
/// node/meta/version side effects a successful `apply_checkpoint` would have produced for ONE
/// graph, without running the (now submission-only) node-authority guard -- the same bypass
/// `redb_store::development_lane::tests::seed_lane_work_item` already uses for the identical
/// reason.
///
/// The seeded snapshot version is no longer an argument: the graph's
/// authoritative version belongs to the kernel ledger, and this seed is itself
/// one admitted maintenance batch, so it advances that version by exactly one
/// and the resulting value is RETURNED for the caller to compare against.
/// `graph_meta` is file-wide, so it is written through the group's control
/// member; the node/edge/ledger rows are scope-prefixed and go through the
/// graph member.
fn seed_checkpoint_image(
    shard: &Shard,
    graph: &str,
    incarnation_id: &str,
    nodes: &[(String, Vec<u8>)],
) -> u64 {
    resource_maintenance(shard, graph, incarnation_id, |write| {
        let member = write.graph(graph).expect("the seeded graph is a member");
        let mut nodes_table = member
            .open_scoped_table(NODES)
            .expect("open nodes for seed");
        let mut edges_table = member
            .open_scoped_table(EDGES)
            .expect("open edges for seed");
        let mut ledger_table = member
            .open_scoped_table(LEDGER)
            .expect("open ledger for seed");
        clear_graph_rows(graph, &mut nodes_table, &mut edges_table, &mut ledger_table)
            .expect("clear prior graph rows before seed");
        for (id, bytes) in nodes {
            let sealed = DurableCrypto::none().seal(bytes);
            nodes_table
                .insert((graph, id.as_str()), sealed.as_ref())
                .expect("insert seeded node");
        }
        drop(nodes_table);
        drop(edges_table);
        drop(ledger_table);
        let mut meta = write
            .control()
            .open_table(GRAPH_META)
            .expect("open graph_meta for seed");
        let encoded = encode_meta_with_incarnation(graph, GraphType::Global, incarnation_id)
            .expect("encode seeded graph_meta");
        meta.insert(graph, encoded.as_slice())
            .expect("insert seeded graph_meta");
    });
    eg_transaction::version(&resource_read(shard, graph))
        .expect("authoritative version of the seeded graph's shard scope")
}

#[test]
fn checkpoint_replaces_graph_rows_but_preserves_native_resource_domain() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-checkpoint-domain-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    {
        // `Shard::open` materializes the whole declared GraphShard census.
        let shard = Shard::open(&path).expect("create checkpoint shard");
        let current_host = host();
        let mut request = request();
        request.input_fingerprint = format!("v1:{}", "0".repeat(64));
        resource_maintenance(&shard, "graph-a", "checkpoint-domain-seed", |write| {
            let graph = write.graph("graph-a").expect("graph-a is a group member");
            let mut hosts = graph.open_scoped_table(RESOURCE_HOSTS).unwrap();
            resource_put_host(&mut hosts, "graph-a", &current_host, DurableCrypto::none()).unwrap();
            drop(hosts);
            let mut reservations = graph.open_scoped_table(RESOURCE_RESERVATIONS).unwrap();
            let record = resource_build_record(&request, &current_host, current_host.revision, 1)
                .expect("build active checkpoint record");
            let durable = DurableResourceReservation {
                record,
                held_cpu_weight: request.requirement.cpu_weight,
                held_memory_mib: request.requirement.memory_mib,
                held_disk_mib: request.requirement.disk_mib,
                held_process_slots: request.requirement.process_slots,
                fairness_debt: request.fairness_cost,
            };
            resource_put_reservation(
                &mut reservations,
                "graph-a",
                &durable,
                DurableCrypto::none(),
            )
            .unwrap();
            drop(reservations);
            let mut tenant_index = graph
                .open_scoped_table(RESOURCE_RESERVATION_TENANT_INDEX)
                .unwrap();
            tenant_index
                .insert(
                    (
                        "graph-a",
                        request.tenant_ref.as_str(),
                        request.reservation_id.as_str(),
                    ),
                    request.reservation_id.as_str(),
                )
                .unwrap();
            drop(tenant_index);
            let mut attempts = graph
                .open_scoped_table(RESOURCE_RESERVATION_ATTEMPTS)
                .unwrap();
            attempts
                .insert(
                    ("graph-a", request.work_item_id.as_str(), request.attempt),
                    request.reservation_id.as_str(),
                )
                .unwrap();
        });

        let work_item_bytes = rmp_serde::to_vec_named(&work_item_props_for_request(&request))
            .expect("encode linked WorkItem");
        let make_dump =
            |incarnation_id: &str, source_snapshot_version: u64, nodes: Vec<(String, Vec<u8>)>| {
                GraphDump::in_place_core_checkpoint(InPlaceCoreCheckpoint {
                    graph: "graph-a".to_string(),
                    name: "graph-a".to_string(),
                    graph_type: GraphType::Global,
                    incarnation_id: incarnation_id.to_string(),
                    source_snapshot_version,
                    integrity_policy: None,
                    nodes,
                    edges: Vec::new(),
                    ledger: Vec::new(),
                    semantic: Vec::new(),
                })
            };

        // Establish a valid image first.  The incoming snapshot contains the
        // exact live WorkItem linked by the native hold. Installed directly
        // (see `seed_checkpoint_image`'s doc): `apply_checkpoint` itself can no
        // longer install an ACTIVE-status WorkItem row post-RMDD-29.
        // The seeded snapshot version is whatever the kernel ledger's version
        // for this scope became: it is the kernel's counter now, not a row a
        // seed may set, and every admitted batch (this seed included) advances
        // it by exactly one. Every dump below is versioned RELATIVE to it.
        let initial_version = seed_checkpoint_image(
            &shard,
            "graph-a",
            "incarnation:checkpoint-domain-initial",
            &[
                (
                    "old-node".to_string(),
                    rmp_serde::to_vec_named(&serde_json::json!({"old": true})).unwrap(),
                ),
                (request.work_item_id.clone(), work_item_bytes.clone()),
            ],
        );
        let initial = read_graph_dump(&shard, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("checkpoint graph identity");
        assert_eq!(
            initial
                .nodes
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["old-node", "work-1"]
        );
        assert_eq!(initial.source_snapshot_version, initial_version);

        // The incoming replacement omits the linked WorkItem.  It must refuse
        // before clear_graph_rows, leaving both the old graph image and native
        // held authority untouched.
        let error = apply_checkpoint(
            &shard,
            &mut Vec::new(),
            vec![make_dump(
                "incarnation:checkpoint-domain-invalid",
                initial_version + 1,
                vec![(
                    "new-node".to_string(),
                    rmp_serde::to_vec_named(&serde_json::json!({"new": true})).unwrap(),
                )],
            )],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint cannot orphan an active native hold");
        assert_eq!(error, "checkpoint resource domain validation failed");
        assert!(!error.contains("reservation-1"));
        assert!(!error.contains("work-1"));
        let refused = read_graph_dump(&shard, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("graph remains after refused checkpoint");
        assert_eq!(
            refused
                .nodes
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["old-node", "work-1"]
        );

        // A replacement containing the exact linked WorkItem is valid and can
        // replace the ordinary graph rows while preserving the native domain.
        // Installed directly (see `seed_checkpoint_image`'s doc) for the same
        // post-RMDD-29 reason as the initial image above.
        let restored_version = seed_checkpoint_image(
            &shard,
            "graph-a",
            "incarnation:checkpoint-domain-valid",
            &[
                (
                    "new-node".to_string(),
                    rmp_serde::to_vec_named(&serde_json::json!({"new": true})).unwrap(),
                ),
                (request.work_item_id.clone(), work_item_bytes.clone()),
            ],
        );
        let restored = read_graph_dump(&shard, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("valid replacement graph identity");
        assert_eq!(restored.source_snapshot_version, restored_version);
        assert_eq!(
            restored
                .nodes
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["new-node", "work-1"]
        );

        // A replacement image may not move the graph authority backwards,
        // even when it contains an otherwise valid live WorkItem.
        let stale_version = apply_checkpoint(
            &shard,
            &mut Vec::new(),
            vec![make_dump(
                "incarnation:checkpoint-domain-stale-version",
                restored_version - 1,
                vec![(request.work_item_id.clone(), work_item_bytes.clone())],
            )],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint cannot lower the graph snapshot version");
        assert_eq!(stale_version, "checkpoint graph image is stale");

        // A historically valid WorkItem image with an old fence cannot be
        // restored beside the still-held reservation.
        let mut old_fence_props = work_item_props_for_request(&request);
        old_fence_props.insert("fencing_token".to_string(), serde_json::json!(0));
        old_fence_props.insert("lease_epoch".to_string(), serde_json::json!(0));
        let old_fence = apply_checkpoint(
            &shard,
            &mut Vec::new(),
            vec![make_dump(
                "incarnation:checkpoint-domain-old-fence",
                restored_version + 1,
                vec![(
                    request.work_item_id.clone(),
                    rmp_serde::to_vec_named(&old_fence_props).unwrap(),
                )],
            )],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint cannot restore an old WorkItem fence");
        // Post-RMDD-29, `work_item_capability::validate_snapshot_nodes` now refuses this
        // ACTIVE-status (`status: "running"`) node before `validate_checkpoint_resource_links`
        // ever inspects its fence -- a strictly broader refusal than the fence-specific one this
        // assertion originally named (any active-lease WorkItem is refused now, fence-valid or
        // not), so the still-refused invariant this test proves holds a fortiori.
        assert_eq!(
            old_fence,
            "native WorkItem authority required for active lease fields"
        );

        // A lease expiring at or before reservation time cannot keep a held
        // reservation alive.
        let mut expired_props = work_item_props_for_request(&request);
        expired_props.insert("lease_expires_at".to_string(), serde_json::json!(1.0));
        let expired = apply_checkpoint(
            &shard,
            &mut Vec::new(),
            vec![make_dump(
                "incarnation:checkpoint-domain-expired-at-reservation",
                restored_version + 2,
                vec![(
                    request.work_item_id.clone(),
                    rmp_serde::to_vec_named(&expired_props).unwrap(),
                )],
            )],
            DurableCrypto::none(),
        )
        .expect_err("checkpoint rejects a WorkItem lease expired at/before reservation time");
        // Same post-RMDD-29 interaction as `old_fence` above: the ACTIVE-status node is refused
        // by `validate_snapshot_nodes` before the expiry-specific check in
        // `validate_checkpoint_resource_links` ever runs.
        assert_eq!(
            expired,
            "native WorkItem authority required for active lease fields"
        );
        let unchanged = read_graph_dump(&shard, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("graph remains after stale resource checkpoint images");
        // Every `apply_checkpoint` above was REFUSED, and a refused checkpoint
        // rolls its whole transaction back, so the authoritative version is
        // still exactly the one the last successful seed produced.
        assert_eq!(unchanged.source_snapshot_version, restored_version);
        assert_eq!(
            unchanged
                .nodes
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>(),
            vec!["new-node", "work-1"]
        );

        // Checkpoint validity is historical: 2.0s is later than the
        // reservation at 1.0s, even though it is far in the past relative to
        // this test's real wall clock. Later expiry/reclaim is handled only by
        // an explicit authoritative transaction carrying `now_ms`.
        let mut historically_valid_props = work_item_props_for_request(&request);
        historically_valid_props.insert("lease_expires_at".to_string(), serde_json::json!(2.0));
        // Installed directly (see `seed_checkpoint_image`'s doc): `apply_checkpoint` can no
        // longer install this ACTIVE-status image post-RMDD-29, same as the two seeds above.
        let historical_version = seed_checkpoint_image(
            &shard,
            "graph-a",
            "incarnation:checkpoint-domain-historical-expiry",
            &[(
                request.work_item_id.clone(),
                rmp_serde::to_vec_named(&historically_valid_props).unwrap(),
            )],
        );
        let historically_installed = read_graph_dump(&shard, "graph-a", DurableCrypto::none())
            .unwrap()
            .expect("historically valid checkpoint remains installed");
        assert_eq!(
            historically_installed.source_snapshot_version,
            historical_version
        );
        assert_eq!(historical_version, restored_version + 1);

        assert!(read_host(&shard, "host-1").is_some());
        assert!(read_reservation(&shard, "reservation-1").is_some());

        // A graph lifecycle clear cannot silently strand this held domain. The
        // pending clear is rejected atomically and leaves both graph/resource
        // rows untouched until release/reclaim drains the hold.
        let mut pending = vec![("graph-a".to_string(), Method::ClearGraph)];
        let error = apply_checkpoint(&shard, &mut pending, Vec::new(), DurableCrypto::none())
            .expect_err("active resource hold blocks checkpoint clear");
        assert!(error.contains("native reservation rows to be drained"));
        assert_eq!(
            pending.len(),
            1,
            "failed checkpoint preserves caller pending work"
        );
        assert!(matches!(pending[0].1, Method::ClearGraph));
        assert!(read_reservation(&shard, "reservation-1").is_some());
        drop(shard);
    }
    let _ = std::fs::remove_file(path);
}

#[test]
fn delete_graph_with_active_native_hold_is_atomic_and_recreate_is_clean() {
    let path = std::env::temp_dir().join(format!(
        "eg-resource-delete-active-hold-{}-{}.redb",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    let (request, props) = resolved_request();
    let shard = seed_resource_database(
        &path,
        vec![(request.work_item_id.clone(), props)],
        vec![host()],
    );
    // The seed is itself an admitted maintenance batch: read the version it
    // produced through `eg_transaction::version` rather than assuming zero.
    let seeded_version = current_resource_graph_version(&shard);

    let reserve = resource_batch(
        &request.tenant_ref,
        Method::ReserveWorkItemResources {
            request: request.clone(),
        },
        "batch-delete-active-reserve",
        "delete-active-reserve",
        seeded_version,
    );
    let reserved = commit_resource_batch(&shard, &reserve).expect("reserve active hold");
    let reserved_result = batch_resource_result(&reserved);
    assert_eq!(
        reserved_result.decision,
        ResourceReservationResultDecision::Accepted
    );
    assert!(reserved_result.held_cpu_weight > 0);

    let lifecycle_batch =
        |method: Method, batch_id: &str, idempotency_key: &str, expected_version: u64| {
            let mut batch = resource_batch(
                &request.tenant_ref,
                method,
                batch_id,
                idempotency_key,
                expected_version,
            );
            batch.operations[0].surface = MutationSurface::Lifecycle;
            batch.operations[0].domain = DurabilityDomain::Lifecycle;
            batch
                .reseal_envelope(eg_types::contract::Digest256::from_bytes([1_u8; 32]))
                .expect("a lifecycle fixture reseals its final body");
            batch
        };

    // DeleteGraph must fail before its graph/resource row changes become
    // durable while a native hold is still active.  No lifecycle status or
    // projection outbox row may survive the failed transaction either.
    //
    // `reserve` above committed once since the seed.
    let delete_while_held = lifecycle_batch(
        Method::DeleteGraph {
            graph_name: "graph-a".to_string(),
        },
        "batch-delete-active-held",
        "delete-active-held",
        seeded_version + 1,
    );
    let error = commit_resource_batch(&shard, &delete_while_held)
        .expect_err("active native hold blocks DeleteGraph atomically");
    assert_eq!(
        error,
        "resource graph clear requires native reservation rows to be drained"
    );
    assert!(ledger_receipt(&shard, &delete_while_held.batch_id).is_none());
    assert!(ledger_outbox(&shard, &delete_while_held.batch_id).is_empty());
    assert!(read_one_node(
        &shard,
        "graph-a",
        &request.work_item_id,
        DurableCrypto::none()
    )
    .unwrap()
    .is_some());
    {
        let held = read_reservation(&shard, &request.reservation_id)
            .expect("active reservation survives failed DeleteGraph");
        assert_eq!(held.record.state, ResourceReservationRecordState::Reserved);
        assert!(held.held_cpu_weight > 0);
        let held_host =
            read_host(&shard, "host-1").expect("host accounting survives failed DeleteGraph");
        assert_eq!(
            held_host.held_cpu_weight,
            host().held_cpu_weight + request.requirement.cpu_weight
        );
    }

    // Drain the hold through its explicit lifecycle operation, then the same
    // DeleteGraph path may remove the graph and all terminal resource history.
    let mut release_request = request.clone();
    release_request.now_ms = 2_000;
    release_request.expected_lifecycle_revision = Some(1);
    let release_tenant_ref = release_request.tenant_ref.clone();
    let release = resource_batch(
        &release_tenant_ref,
        Method::ReleaseWorkItemResources {
            request: release_request,
        },
        "batch-delete-active-release",
        "delete-active-release",
        // `delete_while_held` above FAILED (a business/route error returned
        // before the group committed), so its whole transaction rolled back
        // and the counter never advanced past `reserve`'s commit.
        seeded_version + 1,
    );
    assert_eq!(
        current_resource_graph_version(&shard),
        seeded_version + 1,
        "a refused commit must not advance the authoritative version"
    );
    let released = commit_resource_batch(&shard, &release).expect("release active hold");
    assert_eq!(
        batch_resource_result(&released).decision,
        ResourceReservationResultDecision::Accepted
    );

    // `release` above committed successfully.
    let delete_after_release = lifecycle_batch(
        Method::DeleteGraph {
            graph_name: "graph-a".to_string(),
        },
        "batch-delete-after-release",
        "delete-after-release",
        seeded_version + 2,
    );
    commit_resource_batch(&shard, &delete_after_release)
        .expect("DeleteGraph succeeds after explicit hold drain");
    assert!(read_one_node(
        &shard,
        "graph-a",
        &request.work_item_id,
        DurableCrypto::none()
    )
    .unwrap()
    .is_none());
    {
        assert!(read_reservation(&shard, &request.reservation_id).is_none());
        assert!(read_host(&shard, "host-1").is_none());
    }

    // Recreate the same graph name.  The fresh lifecycle must not recover the
    // old WorkItem, reservation, or terminal tombstone from the deleted image.
    // `delete_after_release` above committed successfully. Deleting the graph
    // does not reset the scope's authoritative version -- the kernel's version
    // row belongs to the SCOPE BINDING, not to the graph's content rows, and
    // graph deletion clears only the content -- so the counter keeps counting
    // through the delete, which the equality below asserts rather than assumes.
    assert_eq!(current_resource_graph_version(&shard), seeded_version + 3);
    let recreate = lifecycle_batch(
        Method::CreateGraph {
            graph_name: "graph-a".to_string(),
            graph_type: GraphType::Global,
        },
        "batch-recreate-after-delete",
        "recreate-after-delete",
        seeded_version + 3,
    );
    commit_resource_batch(&shard, &recreate).expect("recreate graph after drained delete");
    assert!(read_one_node(
        &shard,
        "graph-a",
        &request.work_item_id,
        DurableCrypto::none()
    )
    .unwrap()
    .is_none());
    let meta = read_all_graph_meta(&shard).unwrap();
    assert!(meta.iter().any(|(graph, _, _, incarnation)| {
        graph == "graph-a" && incarnation == &recreate.batch_id
    }));
    assert!(read_reservation(&shard, &request.reservation_id).is_none());
    assert!(read_host(&shard, "host-1").is_none());

    drop(shard);
    let _ = std::fs::remove_file(path);
}

#[test]
fn rmdd08_reservation_fingerprint_matches_cross_language_golden_vector() {
    // Generated from Repository Manager's frozen
    // _reservation_input_fingerprint using a ResourceProfile version 3 and
    // ResourceRequest.model_dump(mode="json").  Keep this fixture beside the
    // Rust recomputation so integer profile versions, contract markers, target
    // policy markers, and UTC Z deadline spelling cannot drift independently.
    let mut request = request();
    request.work_item_id =
        "workitem:repository_manager:11111111-1111-1111-1111-111111111111".to_string();
    request.attempt = 2;
    request.fence = "7".to_string();
    request.lease_epoch = 7;
    request.fencing_token = 7;
    request.reservation_id = "reservation:golden".to_string();
    request.owner_id = "owner-a".to_string();
    request.tenant_ref = "tenant-a".to_string();
    request.profile_version = "3".to_string();
    request.requirement = ResourceRequirement {
        cpu_weight: 5,
        memory_mib: 3_072,
        disk_mib: 5_000,
        process_slots: 3,
    };
    request.repository_id = "repo-opaque".to_string();
    request.branch = "release/main".to_string();
    request.concurrency_key = "rust-build".to_string();
    request.concurrency_limit = Some(2);
    request.required_labels = vec!["linux".to_string(), "rust".to_string()];
    request.anti_affinity = vec!["compiler".to_string(), "gpu".to_string()];
    request.fairness_group = "build".to_string();
    request.disk_low_watermark_mib = Some(400);
    request.disk_high_watermark_mib = Some(700);
    request.disk_policy_key = "rust-v3".to_string();
    let mut props = work_item_props();
    let repository = props["metadata"]["repository_work_item"]
        .as_object_mut()
        .expect("repository metadata");
    repository["tenant_id"] = serde_json::Value::String(resource_b64_urlsafe("tenant-a"));
    repository["repository_id"] = serde_json::Value::String(resource_b64_urlsafe("repo-opaque"));
    repository["owner_id"] = serde_json::Value::String(resource_b64_urlsafe("owner-a"));
    repository["branch"] = serde_json::Value::String(resource_b64_urlsafe("release/main"));
    repository["job_id"] = serde_json::Value::String(resource_b64_urlsafe(
        "rmjob:22222222-2222-2222-2222-222222222222",
    ));
    repository["priority"] = serde_json::json!(17);
    repository["queue_deadline"] = serde_json::json!("2026-08-09T12:00:00Z");
    let extension = repository["resource_reservation"]
        .as_object_mut()
        .expect("resource extension");
    extension["profile_name"] = serde_json::Value::String(resource_b64_urlsafe("rust-build"));
    extension["profile_version"] = serde_json::Value::String(resource_b64_urlsafe("3"));
    extension["cpu_weight"] = serde_json::json!(5);
    extension["memory_mib"] = serde_json::json!(3_072);
    extension["disk_mib"] = serde_json::json!(5_000);
    extension["process_slots"] = serde_json::json!(3);
    extension["host_labels"] =
        serde_json::json!([resource_b64_urlsafe("linux"), resource_b64_urlsafe("rust")]);
    extension["anti_affinity"] = serde_json::json!([
        resource_b64_urlsafe("compiler"),
        resource_b64_urlsafe("gpu")
    ]);
    extension["repository_id"] = serde_json::Value::String(resource_b64_urlsafe("repo-opaque"));
    extension["concurrency_key"] = serde_json::Value::String(resource_b64_urlsafe("rust-build"));
    extension["concurrency_limit"] = serde_json::json!(2);
    extension["repository_exclusive"] = serde_json::json!(true);
    extension["branch_exclusive"] = serde_json::json!(true);
    extension["fairness_group"] = serde_json::Value::String(resource_b64_urlsafe("build"));
    extension["disk_policy_key"] = serde_json::Value::String(resource_b64_urlsafe("rust-v3"));
    extension["disk_low_watermark_mib"] = serde_json::json!(400);
    extension["disk_high_watermark_mib"] = serde_json::json!(700);
    extension["branch"] = serde_json::Value::String(resource_b64_urlsafe("release/main"));
    extension["base_ref"] = serde_json::Value::String(resource_b64_urlsafe("release/main"));
    let actual = resource_recomputed_fingerprint(&props, &request).unwrap();
    assert_eq!(
        actual,
        "v1:13553590ebbcc2eca94df4968dcc1df773b847ed23cc833ec91350406b4067bb"
    );
}
