//! G9 regression: booting against an existing store must not be silently
//! blocked for minutes (EG-DURABLE-KERNEL-R006.4, EG-DURABLE-KERNEL-R006.5).
//!
//! Pre-G9 the pre-listener boot sequence queued the matview reload and the
//! time-series startup reconciliation behind the connector-pack-projection
//! drain — which held the `ServerState` write lock across the agent-library
//! store open — so on a large store the listeners (the only thing that marks
//! the engine up) waited 6–10+ minutes, with nothing logged for the whole
//! interval. This test reproduces the boot shape on a temp store:
//!
//!  * N pending `connector-pack-import` outbox rows (real imports over the
//!    signed dispatch — each import publishes its row on the Agent Library
//!    outbox, and each pack carries an ontology entry, so its projection
//!    actually runs),
//!  * a tsdb series durable in the authoritative shard with an EMPTY served
//!    store (the legacy-store / crash-window seed), and
//!  * a durable plan-backed matview awaiting its RAM re-hydration,
//!
//! then runs the real wiring: the projection worker spawned BEFORE the
//! transports (as `run_inner` does), the UDS listener, and the post-bind
//! maintenance spawned AFTER the bind. It asserts:
//!
//!  * the listener is bound within a 30s budget and BEFORE the drain
//!    completes (the mandatory ordering regression),
//!  * the backlog drains, the series reconciles and the matview reloads in
//!    the background within a bounded wait,
//!  * a SECOND reconciliation takes the durable-cursor fast path (zero
//!    replay, no duplicates) — the full replay is a once-per-store cost, not
//!    a per-boot one.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use eg_types::connector_pack::{
    ConnectorPackImportRequest, ConnectorPackIndex, ConnectorPackOp, McpCatalogSnapshotBinding,
    PackAnnotations, PackArchiveRef, PackEntry, PackEntryKind, PackHeadRef, PackImportResult,
    PackProducer, PackSection, CONNECTOR_PACK_SCHEMA_VERSION,
};
use eg_types::contract::{BoundedVec, Digest256, ResourceId};
use eg_types::mutation_outbox::{
    MutationOutboxOp, MutationOutboxStatusView, NativeOutboxScope, NativeOutboxStore, OutboxTarget,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use tokio::sync::RwLock;

use crate::protocol::{GraphType, Method, Response, ResultPayload};
use crate::server::auth::{dispatch_test_on_heap, scoped_test_request_on, ScopedTestCaller};
use crate::server::persistence::PersistenceBackend;
use crate::server::state::ServerState;

const SECRET: &str = "g9-boot-critical-fixture-secret"; // sanitizer:ignore
/// The fixed tenant `auth::request_context_policy` expects under `cfg(test)`.
const TENANT: &str = "tenant-shared";
/// The System-role agent the harness isolation registers.
const ADMIN: &str = "g9-boot-critical-admin";
const CONNECTOR: &str = "g9-boot-critical";
const GRAPH: &str = "g9-boot-graph";
const SERIES: &str = "g9-boot-series";
const MV_NAME: &str = "g9-boot-critical-mv";
/// Rows seeded onto the import outbox.
const PENDING_ROWS: usize = 8;
/// The listener must be up within this of the worker spawning.
const BIND_BUDGET: Duration = Duration::from_secs(30);
/// The post-bind work (drain + reconcile + reload) must finish within this.
const DRAIN_BUDGET: Duration = Duration::from_secs(120);
const BUCKET_NS: u64 = 3_600_000_000_000; // 1h — matches DEFAULT_MEASUREMENT_BUCKET_NS.

/// Request ids and envelope nonces, unique across the whole test process:
/// the transport replay ledger is process-wide.
fn next_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// One signed request as `ADMIN`, through the same envelope builder the
/// served-surface fixtures use.
async fn call(state: &Arc<RwLock<ServerState>>, key: &str, method: Method) -> Response {
    let id = next_id();
    let request = scoped_test_request_on(
        SECRET,
        id,
        "__commons__",
        method,
        ScopedTestCaller {
            principal: ADMIN,
            tenant: TENANT,
            scopes: &["*"],
            nonce: &format!("g9-boot-nonce-{id}"),
            idempotency_key: key,
        },
    );
    dispatch_test_on_heap(state, request).await
}

/// Decode a successful typed result.
fn ok<T: DeserializeOwned>(label: &str, response: Response) -> T {
    assert!(
        response.error.is_none(),
        "{label} was refused: {response:?}"
    );
    let value = match response.result {
        Some(ResultPayload::Raw(bytes)) => {
            return rmp_serde::from_slice(&bytes)
                .unwrap_or_else(|error| panic!("{label} result does not decode: {error}"));
        }
        Some(ResultPayload::Count(count)) => serde_json::json!(count),
        Some(ResultPayload::String(text)) => serde_json::json!(text),
        Some(ResultPayload::Bool(flag)) => serde_json::json!(flag),
        Some(ResultPayload::Json(value)) => value,
        other => panic!("{label} answered an unexpected payload shape: {other:?}"),
    };
    serde_json::from_value(value)
        .unwrap_or_else(|error| panic!("{label} result does not decode: {error}"))
}

/// A request-body context shell; the engine re-binds every field of it.
fn context(key: &str) -> eg_types::agent_library::AgentLibraryMutationContext {
    eg_types::agent_library::AgentLibraryMutationContext {
        request_id: 1,
        principal: "bound-by-engine".to_string(),
        caller_principal: "bound-by-engine".to_string(),
        attempt_nonce: eg_types::contract::Nonce::from_bytes([0; 32]),
        tenant_id: TENANT.to_string(),
        actor_scope: "bound-by-engine".to_string(),
        purpose_id: "bound-by-engine".to_string(),
        policy_revision: "bound-by-engine".to_string(),
        policy_digest: format!("sha256:{}", "0".repeat(64)),
        policy_decision_id: "bound-by-engine".to_string(),
        idempotency_key: key.to_string(),
        expected_revision: None,
        trace_id: None,
        created_at_ms: 1,
    }
}

/// Append `bytes` to `archive` as a digest-honest section.
fn section(archive: &mut Vec<u8>, bytes: &[u8]) -> PackSection {
    let section = PackSection {
        offset: archive.len() as u64,
        length: bytes.len() as u64,
        sha256: Digest256::from_bytes(Sha256::digest(bytes).into()),
    };
    archive.extend_from_slice(bytes);
    section
}

/// A complete, digest-honest pack for `row`: a server entry, a tool, and an
/// ontology entry. The ontology is what makes the projection do real work
/// (SHACL + graph commit + text index), so the drain is slow enough to be
/// observable against the listener bind.
fn build_pack(connector: &str, row: usize, ontology: &[u8]) -> (ConnectorPackIndex, Vec<u8>) {
    let mut archive = Vec::new();
    let server_body =
        format!(r#"{{"contract_version":"1","instructions":"Test.","name":"{connector}"}}"#)
            .into_bytes();
    let server = PackEntry {
        kind: PackEntryKind::McpServer,
        uri: format!("mcp-server://{connector}"),
        name: connector.to_string(),
        media_type: "application/json".to_string(),
        body: section(&mut archive, &server_body),
        input_schema: None,
        output_schema: None,
        annotations: PackAnnotations::default(),
        references: BoundedVec::new(Vec::new()).unwrap(),
    };
    let tool_body = format!(r#"{{"description":"G9 row {row}","name":"tool"}}"#).into_bytes();
    let tool_schema = br#"{"properties":{"q":{"type":"string"}},"type":"object"}"#;
    let tool = PackEntry {
        kind: PackEntryKind::Tool,
        uri: format!("tool://{connector}/tool"),
        name: "tool".to_string(),
        media_type: "application/json".to_string(),
        body: section(&mut archive, &tool_body),
        input_schema: Some(section(&mut archive, tool_schema)),
        output_schema: None,
        annotations: PackAnnotations {
            read_only_hint: Some(true),
            ..PackAnnotations::default()
        },
        references: BoundedVec::new(Vec::new()).unwrap(),
    };
    let ontology_entry = PackEntry {
        kind: PackEntryKind::Ontology,
        uri: format!("ontology://{connector}/core.ttl"),
        name: "core.ttl".to_string(),
        media_type: "text/turtle".to_string(),
        body: section(&mut archive, ontology),
        input_schema: None,
        output_schema: None,
        annotations: PackAnnotations::default(),
        references: BoundedVec::new(Vec::new()).unwrap(),
    };
    let mut index = ConnectorPackIndex {
        schema_version: CONNECTOR_PACK_SCHEMA_VERSION,
        connector: ResourceId::new(connector).unwrap(),
        server,
        server_package_version: "1.0.0".to_string(),
        archive: PackArchiveRef {
            blob_digest: String::new(),
            length: archive.len() as u64,
            sha256: Digest256::from_bytes(Sha256::digest(&archive).into()),
        },
        entries: BoundedVec::new(vec![ontology_entry, tool]).unwrap(),
        producer: PackProducer {
            name: "g9-boot-critical".to_string(),
            version: "1".to_string(),
        },
        catalog: McpCatalogSnapshotBinding {
            configuration_revision: 1,
            catalog_generation: 1,
            snapshot_digest: Digest256::from_bytes([1; 32]),
            child_connection_generation: 1,
            authorization_scope_digest: Digest256::from_bytes([2; 32]),
        },
        pack_digest: Digest256::from_bytes([0; 32]),
    };
    index.pack_digest = eg_types::connector_pack::digest::pack_digest(&index).unwrap();
    (index, archive)
}

/// Upload `archive` through the Blob CAS and return its blob digest.
async fn upload(state: &Arc<RwLock<ServerState>>, archive: &[u8]) -> String {
    let tag = format!("g9:{}", hex::encode(Sha256::digest(archive)));
    let cursor: u64 = ok(
        "BlobBegin",
        call(
            state,
            &format!("upload:{tag}:begin"),
            Method::BlobBegin {
                chunk_size: 1 << 20,
            },
        )
        .await,
    );
    let chunk: serde_json::Value = ok(
        "BlobChunkPut",
        call(
            state,
            &format!("upload:{tag}:chunk"),
            Method::BlobChunkPut {
                cursor,
                data: archive.to_vec(),
            },
        )
        .await,
    );
    drop(chunk);
    ok(
        "BlobCommit",
        call(
            state,
            &format!("upload:{tag}:commit"),
            Method::BlobCommit { cursor },
        )
        .await,
    )
}

/// Bind `CONNECTOR` to `ADMIN`'s persistence id.
async fn bind(state: &Arc<RwLock<ServerState>>) {
    let key = format!("bind:{CONNECTOR}");
    let _: serde_json::Value = ok(
        "Bind",
        call(
            state,
            &key,
            Method::ConnectorPack {
                op: Box::new(ConnectorPackOp::Bind {
                    request: eg_types::connector_pack::ConnectorPackBindRequest {
                        context: context(&key),
                        connector: ResourceId::new(CONNECTOR).unwrap(),
                        importer: format!(
                            "principal:sha256:{}",
                            hex::encode(Sha256::digest(ADMIN.as_bytes()))
                        ),
                    },
                }),
            },
        )
        .await,
    );
}

/// Seed `PENDING_ROWS` real imports — each publishes one `connector-pack-
/// import` row on the Agent Library outbox under `TENANT`.
async fn seed_import_backlog(state: &Arc<RwLock<ServerState>>) {
    bind(state).await;
    let mut expected_head: Option<PackHeadRef> = None;
    for row in 0..PENDING_ROWS {
        let ontology =
            format!("_:b{row} <https://example.org/member> <https://example.org/row-{row}> .\n");
        let (mut index, archive) = build_pack(CONNECTOR, row, ontology.as_bytes());
        index.archive.blob_digest = upload(state, &archive).await;
        // The import's idempotency key is the deterministic operation key the
        // handler derives (validate_import_identity): connector + pack digest +
        // the expected head's binding revision (0 for the first import).
        let key = format!(
            "connector-pack:{CONNECTOR}:import:{digest}:{}",
            expected_head
                .as_ref()
                .map_or(0, |head| head.binding_revision),
            digest = index.pack_digest
        );
        let receipt = match ok::<PackImportResult>(
            "Import",
            call(
                state,
                &key,
                Method::ConnectorPack {
                    op: Box::new(ConnectorPackOp::Import {
                        request: Box::new(ConnectorPackImportRequest {
                            context: context(&key),
                            index,
                            expected_head,
                            allow_mass_withdrawal: false,
                        }),
                    }),
                },
            )
            .await,
        ) {
            PackImportResult::Imported { receipt } => *receipt,
            other => panic!("import {row} did not land: {other:?}"),
        };
        expected_head = Some(PackHeadRef {
            binding_revision: receipt.binding_revision,
            pack_digest: receipt.pack_digest,
        });
    }
}

/// The worker's stream position, read the way an operator reads it. Transient
/// `None` while the worker's first sweep is still ensuring the agent-library
/// store (the operator view is unavailable until the owner resolves).
async fn outbox_status(state: &Arc<RwLock<ServerState>>) -> Option<MutationOutboxStatusView> {
    let op = MutationOutboxOp::Status {
        target: OutboxTarget::NativeStore {
            store: NativeOutboxStore::AgentLibrary,
            tenant_id: TENANT.to_string(),
            scope: NativeOutboxScope::Tenant,
        },
        consumer: crate::server::connector_pack_projection::worker::CONSUMER.to_string(),
    };
    let response = call(
        state,
        "outbox:status",
        Method::MutationOutbox { op: Box::new(op) },
    )
    .await;
    (response.error.is_none()).then(|| ok("MutationOutbox", response))
}

/// The canonical authority-scoped series key — the same construction the
/// durable fixture in `redb_backend.rs` uses.
fn scoped_series_key(graph: &str, series: &str, principal: &str) -> String {
    let tenant_scope =
        crate::server::mutation_batch::opaque_coordinator_key("carrier-tenant", "verified", TENANT);
    let actor_scope = format!(
        "principal:sha256:{}",
        hex::encode(Sha256::digest(principal.as_bytes()))
    );
    let owner_scope = crate::server::mutation_batch::opaque_coordinator_key(
        "carrier-owner",
        &tenant_scope,
        &actor_scope,
    );
    let graph_scope = crate::server::mutation_batch::opaque_coordinator_key(
        "timeseries-graph",
        &owner_scope,
        graph,
    );
    eg_tsdb::store::SeriesKey::new(tenant_scope, graph_scope, series).encode()
}

// spec: EG-DURABLE-KERNEL-R006.4
// spec: EG-DURABLE-KERNEL-R006.5
#[tokio::test(flavor = "multi_thread")]
async fn boot_listener_binds_before_the_projection_drain_completes() {
    // Held for the whole test: this opens durable stores, so the ambient
    // encryption environment must stay constant across every open.
    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let dir = tempfile::tempdir().unwrap();
    let persist = dir.path().to_string_lossy().into_owned();

    // ── Seed a temp durable store: N pending import rows, a shard-durable
    // series against an EMPTY served store, and a durable matview. ──
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        crate::server::persistence::redb_backend::RedbBackend::open(persist.clone(), 64).unwrap(),
    );
    let redb = backend.as_redb().expect("redb backend");
    let series_store = Arc::new(
        eg_tsdb::store::SeriesStore::open_in_dir(
            std::path::Path::new(&persist),
            crate::store_authority::process_verifier(),
            crate::store_authority::process_authority().principal(),
            &crate::store_authority::process_authority().proof(),
        )
        .unwrap(),
    );
    let blob_dir = dir.path().join("blob");
    std::fs::create_dir_all(&blob_dir).unwrap();
    let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(ADMIN));
    state.persist_dir = Some(persist.clone());
    state.persistence = Some(backend.clone());
    state.tsdb_store = Some(series_store.clone());
    state.blob = Some(crate::server::handlers::blob::cursors_for_test(
        blob_dir.to_str().unwrap(),
    ));
    let _ = state.registry.create_graph(GRAPH, GraphType::Global, None);
    let state = Arc::new(RwLock::new(state));

    let points: Vec<(i64, Vec<f64>)> = vec![
        (1_000_000_000i64, vec![1.0]),
        (2_000_000_000i64, vec![2.0]),
        (3_000_000_000i64, vec![3.0]),
    ];
    // The series is durable in the AUTHORITATIVE shard only (the crash-window
    // seed): the served store stays empty until the startup reconciliation.
    let measurement: crate::MeasurementBatch = (
        scoped_series_key(GRAPH, SERIES, ADMIN),
        1,
        BUCKET_NS,
        vec!["value".to_string()],
        points.clone(),
    );
    backend
        .commit_crossmodal(GRAPH, &[], &[], &[], std::slice::from_ref(&measurement))
        .await
        .expect("authoritative-shard-only commit must succeed");
    // A durable plan-backed matview awaiting its RAM re-hydration.
    let view = crate::server::matview::PlanMatView {
        name: MV_NAME.to_string(),
        graph: GRAPH.to_string(),
        plan: eg_types::wire::Plan::new(Vec::new()),
    };
    redb.plan_matview_put(MV_NAME, crate::server::matview::encode_def(&view).unwrap())
        .await
        .unwrap();
    // N real imports → N pending connector-pack-import outbox rows.
    seed_import_backlog(&state).await;

    // The seeding dispatches ensured the Agent Library store: drop the cached
    // handle so the worker's first sweep exercises the open-OUTSIDE-the-write-
    // lock path (EG-DURABLE-KERNEL-R006.4).
    let mut guard = state.write().await;
    guard.agent_library = None;
    *guard.agent_library_open.lock() = None;
    drop(guard);

    // ── Boot the real wiring: the worker first (as `run_inner` does), then
    // the UDS listener, then the post-bind maintenance after the bind. ──
    crate::server::connector_pack_projection::spawn(state.clone());
    let socket_path = dir
        .path()
        .join("engine-g9-boot.sock")
        .to_string_lossy()
        .into_owned();
    let shutdown = crate::server::ShutdownCoordinator::new();
    let serve_state = state.clone();
    let serve_path = socket_path.clone();
    tokio::spawn(async move {
        let _ = crate::server::serve_uds(&serve_path, 0o600, serve_state, shutdown).await;
    });

    // (a) The listener is bound within the budget.
    let started = Instant::now();
    let bind_at = loop {
        if std::os::unix::net::UnixStream::connect(&socket_path).is_ok() {
            break Instant::now();
        }
        assert!(
            started.elapsed() <= BIND_BUDGET,
            "the UDS listener did not bind within {BIND_BUDGET:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };

    // (b) The post-bind maintenance — exactly the hook `run_inner` now places
    // after the transport bind.
    crate::server::boot_maintenance::spawn_post_bind_maintenance(state.clone()).await;

    // (c) The drain completes in the background within the budget — and the
    // listener was already up while it did.
    let drain_at = loop {
        if let Some(view) = outbox_status(&state).await {
            let delivered = view.status.delivered;
            let pending = view.status.pending;
            if pending == 0 && delivered == PENDING_ROWS as u64 {
                break Instant::now();
            }
            if delivered > 0 {
                // A mid-flight drain: the strongest ordering form — the listener
                // must be connectable right now.
                assert!(
                    std::os::unix::net::UnixStream::connect(&socket_path).is_ok(),
                    "the listener must be up while the drain is mid-flight"
                );
            }
        }
        assert!(
            started.elapsed() <= DRAIN_BUDGET,
            "the outbox drain did not complete within {DRAIN_BUDGET:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert!(
        bind_at <= drain_at,
        "the listener (up at +{:?}) must be bound BEFORE the drain completes (+{:?}) — \
         the pre-G9 boot waited the whole drain out",
        bind_at - started,
        drain_at - started
    );

    // (c2) The drain did REAL work, not just lease resolution: the final
    // head's ontology is committed into the projection graph's durable schema
    // sources — read straight from the authoritative snapshot, not a helper.
    {
        let graph = crate::server::graph_schema::pack_projection_graph_name(
            TENANT,
            &ResourceId::new(CONNECTOR).unwrap(),
        )
        .unwrap();
        let snapshot = backend
            .read_authoritative_graph_snapshot(&crate::persist::sanitize(&graph))
            .await
            .expect("the projection graph snapshot must read")
            .expect("the projection graph must exist after the drain");
        let source = snapshot
            .0
            .schema_sources
            .dynamic
            .get(&format!("pack:{CONNECTOR}"))
            .expect("the final head's schema source must be committed");
        let triples = eg_rdf::mapping::parse_turtle(source.ontology_ttl.as_deref().unwrap())
            .expect("the projected ontology must parse");
        assert_eq!(
            triples.len(),
            1,
            "exactly the final head's one ontology triple must be projected"
        );
    }

    // (d) The reconciliation and the matview reload complete in the
    // background within the budget.
    let storage_key = scoped_series_key(GRAPH, SERIES, ADMIN);
    let expected: Vec<eg_tsdb::point::Point> = points
        .iter()
        .map(|(ts, values)| eg_tsdb::point::Point {
            ts: *ts,
            values: values.clone(),
        })
        .collect();
    let mut reconciled = false;
    let mut reloaded = false;
    loop {
        if !reconciled {
            let served = series_store.scan_all(&storage_key).unwrap();
            if served == expected {
                reconciled = true;
            }
        }
        if !reloaded {
            reloaded = crate::server::matview::manager().get(MV_NAME).is_some();
        }
        if reconciled && reloaded {
            break;
        }
        assert!(
            started.elapsed() <= DRAIN_BUDGET,
            "the post-bind reconcile/reload did not complete within {DRAIN_BUDGET:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // (e) The durable-cursor fast path (EG-DURABLE-KERNEL-R006.5): a second
    // reconciliation (the NEXT boot) is a true no-op — zero replay, no
    // duplicates — so the full replay is a once-per-store cost, not a
    // per-boot one.
    let report = redb
        .reconcile_time_series_sync(&series_store)
        .expect("second reconciliation must succeed");
    assert_eq!(
        report.series_reconciled, 0,
        "a converged series must not be re-reconciled"
    );
    assert_eq!(
        report.points_replayed, 0,
        "the cursor fast path must replay nothing"
    );
    let served = series_store.scan_all(&storage_key).unwrap();
    assert_eq!(served, expected, "no duplicates may be appended");
}
