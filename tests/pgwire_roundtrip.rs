//! Postgres wire round-trip integration test (CONCEPT:AU-KG.query.raw-python).
//!
//! Starts the real pgwire listener over an in-process `ServerState`, connects with
//! the real `tokio-postgres` client, and proves the shim end-to-end:
//!   * `SELECT id FROM nodes …` over a seeded graph returns the expected rows
//!     (reusing the eg-query DataFusion path — the SAME code `Method::Sql` runs),
//!   * an `INSERT INTO nodes …` (routed through the GraphTxn write path) is visible
//!     to a subsequent `SELECT` on the same connection.
//!
//! It also proves the durability barrier (CONCEPT:EG-KG.query.concept-10): a pgwire
//! INSERT awaits `record_durable` before completion, verified by reading the row
//! back from a fresh backend on the same directory.
//!
//! Only compiled with `--features pgwire` (the listener + the eg-query SQL path);
//! the authoritative durability test additionally needs `--features redb`.

#![cfg(feature = "pgwire")]

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::IsolationLayer;
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::pgwire;
use epistemic_graph::server::txn::TxnIdGen;
use epistemic_graph::server::ServerState;

use epistemic_graph::server::persistence::PersistenceBackend;

fn sql_test_persist_dir() -> String {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
    std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-test");
    std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
    std::env::temp_dir()
        .join(format!(
            "epistemic-graph-pgwire-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ))
        .to_string_lossy()
        .into_owned()
}

/// Grant the `commons-user` RBAC role Read+Write on `__commons__` (mirrors
/// `server::mod::tests::multi_tenant_state`'s identical role) and register `agent_id`
/// as holding it. Under the mandatory-RBAC flip (`feature = "security"`), a pgwire
/// connection's user IS the ACL actor (`server::pgwire::auth`), so every fixture
/// identity that needs to reach `__commons__` must be provisioned this way — there is
/// no more "Commons is open to all" fallback for a non-`System` identity.
#[allow(unused_variables)]
fn grant_commons_access(isolation: &mut IsolationLayer, agent_id: &str) {
    #[cfg(feature = "security")]
    {
        use epistemic_graph::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
        use epistemic_graph::isolation::AgentIdentity;
        use epistemic_graph::isolation::AgentRole;
        isolation.add_role(Role::new("commons-user"));
        isolation.add_grant(Grant {
            role: "commons-user".to_string(),
            resource: ResourceSelector::Graph("__commons__".to_string()),
            action: RbacAction::Read,
            effect: GrantEffect::Allow,
        });
        isolation.add_grant(Grant {
            role: "commons-user".to_string(),
            resource: ResourceSelector::Graph("__commons__".to_string()),
            action: RbacAction::Write,
            effect: GrantEffect::Allow,
        });
        isolation.register_agent(AgentIdentity {
            agent_id: agent_id.to_string(),
            role: AgentRole::Agent,
            teams: Vec::new(),
            roles: vec!["commons-user".to_string()],
        });
    }
}

/// Build a minimal `ServerState` with one seeded node so a wire SELECT has rows to
/// return. `__commons__` is pre-created by the registry.
fn state_with(persistence: Option<Arc<dyn PersistenceBackend>>) -> Arc<RwLock<ServerState>> {
    let registry = GraphRegistry::new();
    // Seed three nodes directly via the graph core (the engine write API). Tagged
    // `_visibility: "public"` since default-deny RLS (`IsolationLayer::can_see_row`)
    // hides any untagged, unowned row from a non-`System` actor — the pgwire
    // connection runs as `tester`, not `System`.
    {
        let core = registry.get("__commons__").unwrap().core.clone();
        for (id, ty, rank) in [("n1", "Agent", 1i64), ("n2", "Agent", 2), ("n3", "Tool", 3)] {
            let blob = rmp_serde::to_vec_named(
                &serde_json::json!({"type": ty, "rank": rank, "_visibility": "public"}),
            )
            .unwrap();
            core.add_node(id.to_string(), blob);
        }
    }
    let mut isolation = IsolationLayer::new();
    grant_commons_access(&mut isolation, "tester");
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry,
        isolation,
        channels: ChannelManager::new(),
        auth_secret: "test".to_string(),
        #[cfg(feature = "kv")]
        kv: None,
        persist_dir: Some(sql_test_persist_dir()),
        persistence,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new()),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(TxnIdGen),
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
        // A real per-test served time-series store (mirrors `server::mod::tests::test_state`
        // / `served_mining_tsdb_scan.rs` / `advanced_crossmodal_roundtrip.rs`'s identical
        // wiring): committing a staged `TxnAddMeasurement` now requires a served store to
        // project into (`project_committed_measurements`), and a `None` store fails closed
        // at COMMIT with "committed measurements require the served time-series store".
        #[cfg(feature = "tsdb")]
        tsdb_store: Some({
            static NEXT_TSDB_SEQ: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(1);
            let path = std::env::temp_dir().join(format!(
                "eg-pgwire-tsdb-test-{}-{}.redb",
                std::process::id(),
                NEXT_TSDB_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::sync::Arc::new(
                eg_tsdb::store::SeriesStore::open(&path).expect("open test series store"),
            )
        }),
        #[cfg(feature = "streaming")]
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: Arc::new(DashMap::new()),
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

/// The cache-only default state (no durable tier) the original round-trip tests use.
/// A real tempdir-backed `RedbBackend` (mirrors `common::tempdir_persistence()`,
/// `server::mod::tests::test_state`, `bolt_wire::tests::durable_state`, and every
/// other `tests/*.rs` fixture that needs one): the authoritative-commit flip means
/// EVERY graph-node write now routes through the universal cross-modal MutationBatch
/// kernel (`commit_cross_modal_txn`), which fails closed ("cross-modal mutation
/// requires durable persistence") against a `persistence: None` state. A keyed
/// backend additionally satisfies the multi-op commit's transaction-recovery-plan
/// seal, which requires `EPISTEMIC_GRAPH_ENCRYPTION_KEY` at `open()` (see
/// `redb_backend::tests::cm_dir`'s identical requirement) — provisioned once, before
/// the first backend opens.
#[cfg(feature = "redb")]
fn default_persistence() -> Option<Arc<dyn PersistenceBackend>> {
    use epistemic_graph::durability::DurabilityPolicy;
    use epistemic_graph::server::persistence::redb_backend::RedbBackend;
    static ENCRYPTION_KEY: std::sync::Once = std::sync::Once::new();
    ENCRYPTION_KEY.call_once(|| {
        std::env::set_var(
            epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
            "pgwire-roundtrip-recovery-key",
        );
    });
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let dir = std::env::temp_dir().join(format!(
        "epistemic-graph-pgwire-persist-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create pgwire persist dir");
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(
            dir.to_string_lossy().into_owned(),
            DurabilityPolicy::Each,
            4096,
        )
        .expect("open tempdir redb backend"),
    );
    Some(backend)
}

/// `redb`-off fallback: no durable gateway exists, so this stays `None` (a build
/// without `redb` has no authoritative cross-modal commit gateway to satisfy).
#[cfg(not(feature = "redb"))]
fn default_persistence() -> Option<Arc<dyn PersistenceBackend>> {
    None
}

fn seeded_state() -> Arc<RwLock<ServerState>> {
    state_with(default_persistence())
}

/// Reopen a `RedbBackend` at `dir` with a short bounded retry. `shutdown()` joins the
/// writer thread, which drops that thread's sole STRONG `Arc<Database>` (the shard
/// only ever keeps a `Weak`, see `redb_backend::Shard::db`'s doc) so the exclusive
/// per-process file lock releases once the thread has actually finished unwinding —
/// a small window after `shutdown()`/`join()` returns where the very next `open()`
/// can still race the OS releasing the lock. Mirrors the identical retry
/// `advanced_crossmodal_roundtrip.rs`'s `encryption_at_rest_wrong_key_fails_eg394`
/// needed for the same race.
#[cfg(feature = "redb")]
fn reopen_redb_with_retry(
    dir: &str,
    policy: epistemic_graph::durability::DurabilityPolicy,
    cache: usize,
) -> epistemic_graph::server::persistence::redb_backend::RedbBackend {
    use epistemic_graph::server::persistence::redb_backend::RedbBackend;
    let mut last_err = None;
    for attempt in 0..50 {
        match RedbBackend::open(dir.to_string(), policy, cache) {
            Ok(backend) => return backend,
            Err(e) => {
                last_err = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(20 * (attempt + 1)));
            }
        }
    }
    panic!("reopen redb backend at {dir} after bounded retry: {last_err:?}");
}

/// Bind the listener on an ephemeral port with mandatory SCRAM authentication.
/// Returns the chosen `127.0.0.1:<port>` address.
async fn spawn_listener(state: Arc<RwLock<ServerState>>) -> String {
    spawn_listener_mode(state, pgwire::PgWireAuthMode::Scram).await
}

/// Bind + serve with an EXPLICIT auth mode (CONCEPT:EG-KG.query.concept-13). Used by the auth
/// tests to pin SCRAM deterministically (no process-global env toggle).
async fn spawn_listener_mode(
    state: Arc<RwLock<ServerState>>,
    mode: pgwire::PgWireAuthMode,
) -> String {
    // Probe a free port, then let `serve` bind it (a tiny race window, fine for a test).
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let addr_s = addr.to_string();
    let serve_addr = addr_s.clone();
    tokio::spawn(async move {
        let _ = pgwire::serve_with_auth(&serve_addr, state, mode).await;
    });
    // Give the listener a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    addr_s
}

/// Like [`spawn_listener`], but hands back the listener task's `JoinHandle` too. A
/// durability test that reopens the SAME on-disk persistence directory mid-test needs
/// this: `shutdown()` alone does not release redb's advisory file lock — the lock is
/// held for as long as ANY `Arc<RedbBackend>` clone survives (see
/// `redb_backend::tests::many_recreate_cycles_keep_inmemory_writes_visible`'s identical
/// note), and the listener task captured its OWN clone via `state.persistence` and
/// runs forever (its `JoinHandle` is normally discarded). The caller must `.abort()`
/// this handle and await it BEFORE reopening, so the task's captured `state` (and thus
/// its `persistence` clone) is actually dropped.
async fn spawn_listener_abortable(
    state: Arc<RwLock<ServerState>>,
) -> (String, tokio::task::JoinHandle<()>) {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let addr_s = addr.to_string();
    let serve_addr = addr_s.clone();
    let handle = tokio::spawn(async move {
        let _ = pgwire::serve_with_auth(&serve_addr, state, pgwire::PgWireAuthMode::Scram).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    (addr_s, handle)
}

/// Connect a real tokio-postgres client with the fixture's derived SCRAM password.
async fn connect(addr: &str) -> tokio_postgres::Client {
    let password = pgwire::derive_pg_password("test", "tester");
    let conn_str = format!(
        "host=127.0.0.1 port={} user=tester password={} dbname=__commons__",
        addr.rsplit(':').next().unwrap(),
        password
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("pgwire connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_select_returns_seeded_rows() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    let rows = client
        .simple_query("SELECT id FROM nodes WHERE rank >= 2 ORDER BY id")
        .await
        .expect("simple_query SELECT");

    // simple_query yields a mix of RowDescription/Row/CommandComplete messages;
    // pull the data rows.
    let ids: Vec<String> = rows
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec!["n2".to_string(), "n3".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_insert_then_select_round_trip() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    // INSERT a new node through the GraphTxn write path (simple-query protocol —
    // the surface the first increment supports; extended Parse/Bind is a follow-up).
    let insert = client
        .simple_query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('n9', 'Agent', 9, 'public')",
        )
        .await
        .expect("INSERT");
    let affected = insert
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .expect("INSERT CommandComplete");
    assert_eq!(affected, 1, "one row inserted");

    // It must now be visible to a SELECT on the same connection.
    let rows = client
        .simple_query("SELECT id FROM nodes WHERE rank = 9")
        .await
        .expect("SELECT after insert");
    let ids: Vec<String> = rows
        .into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec!["n9".to_string()]);
}

// ───────────────────────────────────────────────────────────────────────────
// Extended / prepared protocol (CONCEPT:EG-KG.query.describe) — real driver path.
// `tokio-postgres`'s `prepare`/`query`/`execute` use the extended protocol
// (Parse/Bind/Describe/Execute/Sync), which is what psycopg3/asyncpg/JDBC/sqlx/
// SQLAlchemy use by DEFAULT. These prove a real driver's prepared+parameterized
// round-trip works against the shim, not just raw `psql`.
// ───────────────────────────────────────────────────────────────────────────

/// Extended protocol: `client.prepare(...)` + a parameterized `query` with a
/// typed param (`WHERE rank > $1`) returns the matching rows. Proves Parse/Bind/
/// Describe/Execute + parameter binding through a real driver.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_prepared_parameterized_select() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    // prepare() issues Parse + Describe(statement) + Sync; query() issues Bind +
    // Execute + Sync — the full extended flow.
    let stmt = client
        .prepare("SELECT id FROM nodes WHERE rank > $1 ORDER BY id")
        .await
        .expect("prepare");
    let rows = client
        .query(&stmt, &[&1i64])
        .await
        .expect("parameterized query");
    let ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ids, vec!["n2".to_string(), "n3".to_string()]);

    // A different bound value reuses the SAME prepared statement (new portal).
    let rows2 = client.query(&stmt, &[&2i64]).await.expect("second bind");
    let ids2: Vec<String> = rows2.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ids2, vec!["n3".to_string()]);
}

/// Extended protocol DML: a parameterized `UPDATE … WHERE id = $1` applies, and a
/// follow-up SELECT shows the change. Proves a parameterized write over the real
/// driver path routes through the GraphTxn CAS write path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_parameterized_update_then_select() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    let affected = client
        .execute("UPDATE nodes SET rank = $1 WHERE id = $2", &[&42i64, &"n1"])
        .await
        .expect("parameterized UPDATE");
    assert_eq!(affected, 1, "one row updated");

    let rows = client
        .query("SELECT id FROM nodes WHERE rank = $1", &[&42i64])
        .await
        .expect("SELECT after update");
    let ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ids, vec!["n1".to_string()]);
}

/// Extended protocol DML: a parameterized `DELETE … WHERE id = $1` removes the
/// node; a follow-up SELECT shows it gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_parameterized_delete_then_select() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    let affected = client
        .execute("DELETE FROM nodes WHERE id = $1", &[&"n2"])
        .await
        .expect("parameterized DELETE");
    assert_eq!(affected, 1, "one row deleted");

    let rows = client
        .query("SELECT id FROM nodes WHERE id = $1", &[&"n2"])
        .await
        .expect("SELECT after delete");
    assert_eq!(rows.len(), 0, "deleted node must be gone");
}

/// Multi-row INSERT (CONCEPT:EG-KG.query.follow-up): `INSERT … VALUES (…),(…),(…)` adds three
/// nodes in one statement; a COUNT confirms all landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_then_count() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    let affected = client
        .execute(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES \
             ('m1','Agent',10,'public'),('m2','Agent',11,'public'),('m3','Tool',12,'public')",
            &[],
        )
        .await
        .expect("multi-row INSERT");
    assert_eq!(affected, 3, "three rows inserted");

    let rows = client
        .query("SELECT count(*) AS c FROM nodes WHERE rank >= 10", &[])
        .await
        .expect("COUNT after multi-insert");
    let c: i64 = rows[0].get("c");
    assert_eq!(c, 3, "all three multi-insert rows present");
}

/// `INSERT … RETURNING id` (CONCEPT:EG-KG.query.follow-up): the write returns the affected row
/// as a result set, not just a CommandComplete tag.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_row() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    let rows = client
        .query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('r1','Agent',55,'public') RETURNING id",
            &[],
        )
        .await
        .expect("INSERT RETURNING");
    assert_eq!(rows.len(), 1, "RETURNING yields one row");
    assert_eq!(rows[0].get::<_, String>("id"), "r1".to_string());

    // UPDATE … RETURNING surfaces the post-write value.
    let urows = client
        .query(
            "UPDATE nodes SET rank = $1 WHERE id = $2 RETURNING id, rank",
            &[&77i64, &"r1"],
        )
        .await
        .expect("UPDATE RETURNING");
    assert_eq!(urows.len(), 1);
    assert_eq!(urows[0].get::<_, String>("id"), "r1".to_string());
    assert_eq!(urows[0].get::<_, i64>("rank"), 77i64);
}

/// A recording `PersistenceBackend` that captures every `record_durable` call.
#[derive(Default)]
struct RecordingBackend {
    recorded: std::sync::Mutex<Vec<(String, String)>>,
    durable: std::sync::atomic::AtomicUsize,
}

impl RecordingBackend {
    /// `(graph_fname, node_id)` pairs captured via the durable commit seam.
    fn recorded_pairs(&self) -> Vec<(String, String)> {
        self.recorded.lock().unwrap().clone()
    }
    fn durable_calls(&self) -> usize {
        self.durable.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl PersistenceBackend for RecordingBackend {
    async fn load_all(&self, _state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        Ok(0)
    }
    async fn record_durable(
        &self,
        graph_fname: &str,
        method: &epistemic_graph::protocol::Method,
    ) -> Result<(), String> {
        self.durable
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let epistemic_graph::protocol::Method::AddNode { node_id, .. } = method {
            self.recorded
                .lock()
                .unwrap()
                .push((graph_fname.to_string(), node_id.clone()));
        }
        Ok(())
    }
    // Wire graph-node writes now route through the universal cross-modal
    // MutationBatch kernel (`server::handlers::txn::commit_cross_modal_txn`), not
    // a bare `record_durable` call — the trait's default `commit_mutation_batch_crossmodal`
    // fails closed ("persistence backend does not support atomic cross-modal
    // MutationBatch commits"), which would defeat this backend's whole purpose
    // (proving the durable seam is AWAITED before ack). Override it to record the
    // SAME (graph, node_id) pairs `record_durable` used to, off the batch's staged
    // `AddNode` operations, and hand back a minimal committed record — this is the
    // authoritative-commit-flip seam this mock must speak now.
    async fn commit_mutation_batch_crossmodal(
        &self,
        args: epistemic_graph::server::persistence::CrossModalCommitArgs<'_>,
    ) -> Result<epistemic_graph::mutation_batch::MutationBatchCommit, String> {
        self.durable
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        for method in args.methods {
            if let epistemic_graph::protocol::Method::AddNode { node_id, .. } = method {
                self.recorded
                    .lock()
                    .unwrap()
                    .push((args.graph_fname.to_string(), node_id.clone()));
            }
        }
        Ok(epistemic_graph::mutation_batch::MutationBatchCommit {
            record: epistemic_graph::mutation_batch::MutationBatchRecord {
                batch: args.batch.clone(),
                status: epistemic_graph::mutation_batch::MutationBatchStatus::Committed,
                result_msgpack: args.result_msgpack.map(|b| b.to_vec()),
                committed_at_ms: args.committed_at_ms,
            },
            replayed: false,
        })
    }
    fn shutdown(&self) {}
}

/// A pgwire INSERT awaits the durable backend before reporting completion.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_insert_awaits_durable_backend() {
    let backend = Arc::new(RecordingBackend::default());
    let state = state_with(Some(backend.clone()));
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    let insert = client
        .simple_query("INSERT INTO nodes (id, type, rank) VALUES ('w1', 'Agent', 4)")
        .await
        .expect("INSERT");
    let affected = insert
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .expect("INSERT CommandComplete");
    assert_eq!(affected, 1);

    assert_eq!(
        backend.recorded_pairs(),
        vec![("__commons__".to_string(), "w1".to_string())],
        "the durable commit must receive the pgwire INSERT"
    );
    assert_eq!(
        backend.durable_calls(),
        1,
        "record_durable must be awaited before completion"
    );
}

/// Authoritative durability (CONCEPT:EG-KG.query.concept-10): a pgwire INSERT is
/// commit-before-ack — `record_durable` is awaited before the
/// CommandComplete is sent. We prove the wire write is durable WITHOUT any checkpoint
/// by reading the row back from a SEPARATE redb backend reopened on the same dir: the
/// row is only there because the INSERT's await observed a durable commit, exactly
/// like a normal `Method::AddNode` write. Uses `DurabilityPolicy::Interval` so the ONLY way
/// the row lands is the group-commit barrier firing the awaited writer.
#[cfg(feature = "redb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_insert_authoritative_is_durable_without_checkpoint() {
    use epistemic_graph::durability::DurabilityPolicy;
    use epistemic_graph::server::persistence::redb_backend::RedbBackend;

    let dir = std::env::temp_dir().join(format!("eg-pgwire-durable-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir_s = dir.to_string_lossy().to_string();

    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(
            dir_s.clone(),
            DurabilityPolicy::Interval(std::time::Duration::from_millis(20)),
            64,
        )
        .expect("open redb backend"),
    );
    let state = state_with(Some(backend.clone()));
    let (addr, listener) = spawn_listener_abortable(state).await;
    let client = connect(&addr).await;

    // CommandComplete is returned only after the durable group commit.
    let insert = client
        .simple_query("INSERT INTO nodes (id, type, rank) VALUES ('d1', 'Agent', 7)")
        .await
        .expect("INSERT");
    let affected = insert
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::CommandComplete(n) => Some(*n),
            _ => None,
        })
        .expect("INSERT CommandComplete");
    assert_eq!(affected, 1);

    // The wire write was acked ⇒ (commit-before-ack) it is on disk. Prove it is
    // durable independent of any checkpoint by reopening a FRESH redb backend on the
    // SAME dir and point-reading the node — no bulk registry export was involved.
    // redb's advisory file lock is held for as long as ANY `Arc<RedbBackend>` clone
    // survives, not just the writer thread `shutdown()` stops — the listener task
    // (and its per-connection handler for `client`) each captured their own clone via
    // `state.persistence`, so those must actually be torn down first: close the
    // client connection, abort + await the listener task, THEN shut down our own
    // local clone (the retry loop below absorbs the async race of the per-connection
    // handler noticing the closed socket).
    drop(client);
    listener.abort();
    let _ = listener.await;
    backend.shutdown();
    drop(backend);
    let reopened = reopen_redb_with_retry(
        &dir_s,
        DurabilityPolicy::Interval(std::time::Duration::from_millis(20)),
        64,
    );
    let stored = reopened
        .read_node("__commons__", "d1")
        .await
        .expect("read_node");
    assert!(
        stored.is_some(),
        "pgwire authoritative INSERT must be durable in redb WITHOUT a checkpoint"
    );
    // The stored blob round-trips the inserted properties.
    let props: serde_json::Value =
        rmp_serde::from_slice(&stored.unwrap()).expect("decode stored node");
    assert_eq!(props.get("type").and_then(|v| v.as_str()), Some("Agent"));
    assert_eq!(props.get("rank").and_then(|v| v.as_i64()), Some(7));

    reopened.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Extended-protocol durable-on-ack (CONCEPT:EG-KG.query.describe + KG-2.198): a parameterized
/// `UPDATE … WHERE id = $1` issued over the extended protocol
/// mode is commit-before-ack — `record_durable` (a `CompareAndSetNodeFields` method)
/// is AWAITED before the client's `execute()` returns. We prove the wire write is
/// durable WITHOUT any checkpoint by reopening a FRESH redb backend on the same dir
/// and reading the updated property back. This is the headline guarantee that a real
/// driver's prepared write never acks before it lands on disk.
#[cfg(feature = "redb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_update_authoritative_is_durable_without_checkpoint() {
    use epistemic_graph::durability::DurabilityPolicy;
    use epistemic_graph::server::persistence::redb_backend::RedbBackend;

    let dir = std::env::temp_dir().join(format!("eg-pgwire-ext-durable-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir_s = dir.to_string_lossy().to_string();

    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(
            dir_s.clone(),
            DurabilityPolicy::Interval(std::time::Duration::from_millis(20)),
            64,
        )
        .expect("open redb backend"),
    );
    let state = state_with(Some(backend.clone()));
    let (addr, listener) = spawn_listener_abortable(state).await;
    let client = connect(&addr).await;

    // Seed `n1` durably via a wire INSERT (also commit-before-ack), then UPDATE it
    // over the extended/prepared protocol with a bound parameter.
    client
        .execute(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('n1','Agent',1,'public')",
            &[],
        )
        .await
        .expect("seed INSERT");
    let affected = client
        .execute("UPDATE nodes SET rank = $1 WHERE id = $2", &[&99i64, &"n1"])
        .await
        .expect("extended UPDATE");
    assert_eq!(affected, 1);

    // The UPDATE was acked ⇒ (commit-before-ack) the CAS is on disk. Reopen a fresh
    // backend on the same dir and confirm the updated value WITHOUT any checkpoint.
    // See the identical teardown note in `wire_insert_authoritative_is_durable_without_checkpoint`:
    // redb's file lock survives until every `Arc<RedbBackend>` clone (including the
    // listener task's and its per-connection handler's) is actually dropped.
    drop(client);
    listener.abort();
    let _ = listener.await;
    backend.shutdown();
    drop(backend);
    let reopened = reopen_redb_with_retry(
        &dir_s,
        DurabilityPolicy::Interval(std::time::Duration::from_millis(20)),
        64,
    );
    let stored = reopened
        .read_node("__commons__", "n1")
        .await
        .expect("read_node");
    assert!(
        stored.is_some(),
        "extended-protocol authoritative UPDATE must be durable in redb WITHOUT a checkpoint"
    );
    let props: serde_json::Value =
        rmp_serde::from_slice(&stored.unwrap()).expect("decode stored node");
    assert_eq!(
        props.get("rank").and_then(|v| v.as_i64()),
        Some(99),
        "the UPDATE's new value must be the durably-committed one"
    );

    reopened.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

// ───────────────────────────────────────────────────────────────────────────
// Synthetic catalog (CONCEPT:EG-KG.query.datafusion) — a real driver introspects on connect.
// A tokio-postgres client lists tables via information_schema, reflects the
// `nodes` columns, calls version()/current_schema(), then SELECTs — all succeed.
// ───────────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_introspection_then_select() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    // 1. List tables via information_schema (what SQLAlchemy get_table_names issues).
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_name IN ('nodes','edges') ORDER BY table_name",
            &[],
        )
        .await
        .expect("information_schema.tables");
    let tables: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(tables, vec!["edges".to_string(), "nodes".to_string()]);

    // 2. Reflect the `nodes` columns (what SQLAlchemy get_columns issues).
    let rows = client
        .query(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = 'nodes' ORDER BY column_name",
            &[],
        )
        .await
        .expect("information_schema.columns");
    let cols: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    for must in ["id", "rank", "type"] {
        assert!(
            cols.contains(&must.to_string()),
            "reflect missing {must}: {cols:?}"
        );
    }

    // 3. pg_catalog reflect: list relations under public via pg_class/pg_namespace.
    // Filter to the graph projections (`nodes`/`edges`) — the user-table SQL store is
    // a process-global singleton, so a sibling test's user table can otherwise appear
    // here; this mirrors step 1's `information_schema` query, which also filters.
    let rows = client
        .query(
            "SELECT c.relname FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relkind = 'r' \
             AND c.relname IN ('nodes','edges') ORDER BY c.relname",
            &[],
        )
        .await
        .expect("pg_catalog.pg_class");
    let rels: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(rels, vec!["edges".to_string(), "nodes".to_string()]);

    // 4. Catalog scalar functions a driver probes on connect.
    let v = client
        .query("SELECT version()", &[])
        .await
        .expect("version()");
    assert!(v[0].get::<_, String>(0).starts_with("PostgreSQL"));
    let s = client
        .query("SELECT current_schema()", &[])
        .await
        .expect("current_schema()");
    assert_eq!(s[0].get::<_, String>(0), "public");

    // 5. After introspection, a normal SELECT still works.
    let rows = client
        .query("SELECT id FROM nodes WHERE rank >= 2 ORDER BY id", &[])
        .await
        .expect("post-introspection SELECT");
    let ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ids, vec!["n2".to_string(), "n3".to_string()]);
}

// ───────────────────────────────────────────────────────────────────────────
// pg-wire SCRAM auth bridged to the engine identity (CONCEPT:EG-KG.query.concept-13).
// A SCRAM login succeeds with the derived password, is REJECTED with the wrong
// one, and post-login queries run under the mapped engine AgentIdentity/ACL.
// ───────────────────────────────────────────────────────────────────────────

use epistemic_graph::isolation::{AgentIdentity, AgentRole};
use epistemic_graph::server::pgwire::derive_pg_password;

/// A SCRAM-auth state: a known engine secret + a registered identity so the
/// IsolationLayer has rules (the ACL is enforced once any identity exists). The
/// `worker` agent owns `agent:worker` and is a member of no team; `__commons__`
/// stays open to all authenticated agents.
fn scram_state(secret: &str) -> Arc<RwLock<ServerState>> {
    let mut registry = GraphRegistry::new();
    // Tagged `_visibility: "public"` for the same default-deny-RLS reason as
    // `state_with` above.
    {
        let core = registry.get("__commons__").unwrap().core.clone();
        for (id, ty, rank) in [("n1", "Agent", 1i64), ("n2", "Agent", 2), ("n3", "Tool", 3)] {
            let blob = rmp_serde::to_vec_named(
                &serde_json::json!({"type": ty, "rank": rank, "_visibility": "public"}),
            )
            .unwrap();
            core.add_node(id.to_string(), blob);
        }
    }
    // A private per-agent graph owned by `worker`, plus a peer's private graph.
    registry
        .create_graph(
            "agent:worker",
            epistemic_graph::protocol::GraphType::Agent,
            Some("worker".to_string()),
        )
        .unwrap();
    registry
        .create_graph(
            "agent:peer",
            epistemic_graph::protocol::GraphType::Agent,
            Some("peer".to_string()),
        )
        .unwrap();

    let mut isolation = IsolationLayer::new();
    // `__commons__` stays reachable by every authenticated agent via the
    // `commons-user` role (there is no more pre-RBAC "Commons is open to all"
    // fall-through under `feature = "security"`).
    grant_commons_access(&mut isolation, "worker");
    grant_commons_access(&mut isolation, "peer");
    // Each agent additionally owns its own private `agent:*` graph — granted
    // explicitly (mirrors `server::mod::tests::multi_tenant_state`'s `owner-{w}`
    // pattern) so `scram_identity_drives_acl` can prove `worker` reaches its own
    // graph while still being denied `peer`'s.
    #[cfg(feature = "security")]
    {
        use epistemic_graph::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
        for (owner, graph) in [("worker", "agent:worker"), ("peer", "agent:peer")] {
            let role = format!("owner-{owner}");
            isolation.add_role(Role::new(role.clone()));
            for action in [RbacAction::Read, RbacAction::Write] {
                isolation.add_grant(Grant {
                    role: role.clone(),
                    resource: ResourceSelector::Graph(graph.to_string()),
                    action,
                    effect: GrantEffect::Allow,
                });
            }
        }
    }
    isolation.register_agent(AgentIdentity {
        agent_id: "worker".to_string(),
        role: AgentRole::Agent,
        teams: vec![],
        roles: vec!["commons-user".to_string(), "owner-worker".to_string()],
    });
    isolation.register_agent(AgentIdentity {
        agent_id: "peer".to_string(),
        role: AgentRole::Agent,
        teams: vec![],
        roles: vec!["commons-user".to_string(), "owner-peer".to_string()],
    });

    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry,
        isolation,
        channels: ChannelManager::new(),
        auth_secret: secret.to_string(),
        #[cfg(feature = "kv")]
        kv: None,
        persist_dir: Some(sql_test_persist_dir()),
        persistence: None,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new()),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(TxnIdGen),
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
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: Arc::new(DashMap::new()),
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

/// Connect with explicit user + password (SCRAM negotiated by tokio-postgres).
async fn connect_scram(
    addr: &str,
    user: &str,
    password: &str,
    dbname: &str,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let port = addr.rsplit(':').next().unwrap();
    let conn_str =
        format!("host=127.0.0.1 port={port} user={user} password={password} dbname={dbname}");
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// SCRAM login SUCCEEDS with the derived password and a post-login query runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scram_login_succeeds_with_correct_password() {
    let secret = "dummy-pg-test-secret";
    let state = scram_state(secret);
    let addr = spawn_listener_mode(state, pgwire::PgWireAuthMode::Scram).await;

    let pw = derive_pg_password(secret, "worker");
    let client = connect_scram(&addr, "worker", &pw, "__commons__")
        .await
        .expect("SCRAM login with correct derived password");

    // A query after login runs (against the open __commons__). `rank` is inferred
    // as a flat typed column off the seeded property blobs (the same convention
    // `wire_select_returns_seeded_rows`/`extended_prepared_parameterized_select`
    // rely on elsewhere in this file) — no JSON deep-index accessor is needed. The
    // `props->>'k'` operator form is only wired for the narrow graph-node
    // UPDATE/DELETE selector decoder (`eg_query::sql::classify`), not the general
    // DataFusion-planned SELECT path this query runs on, so using it here panicked
    // with "Operator ->> is not yet supported".
    let rows = client
        .query("SELECT id FROM nodes WHERE rank = 1", &[])
        .await
        .expect("post-login SELECT");
    let ids: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(ids, vec!["n1".to_string()]);
}

/// SCRAM login is REJECTED with the wrong password.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scram_login_rejected_with_wrong_password() {
    let secret = "dummy-pg-test-secret";
    let state = scram_state(secret);
    let addr = spawn_listener_mode(state, pgwire::PgWireAuthMode::Scram).await;

    let err = connect_scram(&addr, "worker", "not-the-derived-password", "__commons__")
        .await
        .expect_err("wrong password must be rejected");
    // The SASL failure surfaces as a db error; inspect the full source chain so the
    // assertion catches the auth-failure SQLSTATE/message regardless of wrapping.
    let chain = error_chain(&err);
    assert!(
        chain.contains("password")
            || chain.contains("auth")
            || chain.contains("sasl")
            || chain.contains("28p01")
            || chain.contains("28000"),
        "expected an auth failure, got chain: {chain}"
    );
}

/// The post-login identity drives the ACL: `worker` reaches its OWN private graph
/// (`agent:worker`) but is DENIED a peer's private graph (`agent:peer`). Proves the
/// pg user → engine AgentIdentity mapping flows into `IsolationLayer::check_access`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scram_identity_drives_acl() {
    let secret = "dummy-pg-test-secret";
    let state = scram_state(secret);
    let addr = spawn_listener_mode(state, pgwire::PgWireAuthMode::Scram).await;

    let pw = derive_pg_password(secret, "worker");
    let client = connect_scram(&addr, "worker", &pw, "__commons__")
        .await
        .expect("SCRAM login");

    // Owner reaches its own private graph.
    client
        .simple_query("SET graph = 'agent:worker'")
        .await
        .expect("SET graph to own");
    client
        .query("SELECT id FROM nodes", &[])
        .await
        .expect("read own private graph");

    // The same connection is DENIED a peer's private graph.
    client
        .simple_query("SET graph = 'agent:peer'")
        .await
        .expect("SET graph to peer (the SET itself is local)");
    let denied = client.query("SELECT id FROM nodes", &[]).await;
    let err = denied.expect_err("peer graph read must be denied");
    let chain = error_chain(&err);
    assert!(
        chain.contains("permission denied") || chain.contains("42501"),
        "expected an ACL denial, got chain: {chain}"
    );
}

/// Render a tokio-postgres error's full source chain lowercased, so an assertion
/// can match a SQLSTATE/message that the top-level `Display` hides behind a generic
/// "db error" wrapper.
fn error_chain(err: &tokio_postgres::Error) -> String {
    use std::error::Error;
    let mut s = err.to_string();
    let mut src = err.source();
    while let Some(e) = src {
        s.push_str(" | ");
        s.push_str(&e.to_string());
        src = e.source();
    }
    s.to_lowercase()
}

// ───────────────────────────────────────────────────────────────────────────
// Mixed-store wire transactions (CONCEPT:EG-KG.compute.kg-transaction-is-pinned) — BEGIN/COMMIT/ROLLBACK over the
// simple-query protocol, on ONE persistent connection (== one EngineBackend).
// ───────────────────────────────────────────────────────────────────────────

/// Pull the first-column values from a `simple_query` result as strings.
fn simple_ids(msgs: Vec<tokio_postgres::SimpleQueryMessage>) -> Vec<String> {
    msgs.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect()
}

/// A unique user-table name (the SQL table store is a process-global file, so a
/// fixed name could collide with a prior run / a sibling test).
fn unique_table() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("kv_{nanos}")
}

/// ROLLBACK discards a buffered node INSERT, and a SELECT inside the txn sees its
/// own uncommitted write first (read-your-own-writes). Proves the `T` in-transaction
/// status (the txn is live and buffering) and clean discard on ROLLBACK.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_txn_rollback_discards_and_ryow() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('tx1', 'Agent', 42, 'public')",
        )
        .await
        .expect("INSERT inside txn");
    // Read-your-own-writes: the uncommitted insert IS visible inside the txn.
    let seen = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE rank = 42")
            .await
            .expect("SELECT inside txn"),
    );
    assert_eq!(
        seen,
        vec!["tx1".to_string()],
        "read-your-own-writes: buffered insert visible inside the txn"
    );

    client.simple_query("ROLLBACK").await.expect("ROLLBACK");

    // After ROLLBACK the buffered insert is gone.
    let after = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE rank = 42")
            .await
            .expect("SELECT after ROLLBACK"),
    );
    assert!(
        after.is_empty(),
        "ROLLBACK discarded the buffered insert, got {after:?}"
    );
}

/// COMMIT applies a buffered node INSERT so it survives the transaction and is seen
/// by a post-COMMIT SELECT.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_txn_commit_persists_node_insert() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('tx2', 'Agent', 43, 'public')",
        )
        .await
        .expect("INSERT inside txn");
    client.simple_query("COMMIT").await.expect("COMMIT");

    let after = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE rank = 43")
            .await
            .expect("SELECT after COMMIT"),
    );
    assert_eq!(
        after,
        vec!["tx2".to_string()],
        "COMMIT applied the buffered node insert"
    );
}

/// A mixed transaction commits BOTH graph-node ops AND user-table ops atomically
/// (sequenced across the two stores).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_txn_mixed_node_and_table_commit() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;
    let table = unique_table();

    client.simple_query("BEGIN").await.expect("BEGIN");
    client
        .simple_query(&format!("CREATE TABLE {table} (k TEXT, v BIGINT)"))
        .await
        .expect("CREATE TABLE inside txn");
    client
        .simple_query(&format!("INSERT INTO {table} (k, v) VALUES ('a', 1)"))
        .await
        .expect("INSERT table inside txn");
    client
        .simple_query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('mx1', 'Agent', 44, 'public')",
        )
        .await
        .expect("INSERT node inside txn");
    client.simple_query("COMMIT").await.expect("COMMIT");

    // Both stores reflect the committed transaction.
    let node = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE rank = 44")
            .await
            .expect("SELECT node after COMMIT"),
    );
    assert_eq!(node, vec!["mx1".to_string()], "node op committed");

    let table_rows = simple_ids(
        client
            .simple_query(&format!("SELECT k FROM {table}"))
            .await
            .expect("SELECT table after COMMIT"),
    );
    assert_eq!(table_rows, vec!["a".to_string()], "table op committed");

    // Clean up: the SQL table store is a process-global singleton, so drop the table
    // to avoid leaking it into other tests / future runs.
    let _ = client.simple_query(&format!("DROP TABLE {table}")).await;
}

/// An error inside an open transaction latches it into the aborted state: every
/// later statement except COMMIT/ROLLBACK is rejected with SQLSTATE 25P02 (the `E`
/// failed-transaction status), and ROLLBACK returns the connection to a usable idle
/// state (`I`) where writes work again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_txn_error_blocks_until_rollback_25p02() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    // A statement that errors inside the txn (unknown table) → aborts the txn.
    let bad = client.simple_query("SELECT id FROM no_such_table").await;
    assert!(bad.is_err(), "the bad statement must error");

    // Every subsequent non-COMMIT/ROLLBACK statement is now rejected with 25P02.
    let blocked = client
        .simple_query("INSERT INTO nodes (id, type, rank) VALUES ('nope', 'Agent', 45)")
        .await
        .expect_err("statement after an in-txn error must be rejected");
    let chain = error_chain(&blocked);
    assert!(
        chain.contains("25p02") || chain.contains("aborted"),
        "expected 25P02 aborted-transaction error, got: {chain}"
    );

    // ROLLBACK ends the block and restores a usable connection.
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");
    client
        .simple_query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('ok', 'Agent', 46, 'public')",
        )
        .await
        .expect("writes work again after ROLLBACK");
    let ok = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE rank = 46")
            .await
            .expect("SELECT after recovery"),
    );
    assert_eq!(
        ok,
        vec!["ok".to_string()],
        "connection usable after ROLLBACK"
    );
    // The aborted statement never landed.
    let nope = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE rank = 45")
            .await
            .expect("SELECT for the blocked row"),
    );
    assert!(nope.is_empty(), "the blocked insert never applied");
}

/// SET graph is rejected while a transaction is open (a txn stays within one graph /
/// redb shard, CONCEPT:EG-KG.sharding.semantic-embedding-store-backed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_txn_set_graph_rejected_while_open() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");
    let denied = client.simple_query("SET graph = 'other'").await;
    let err = denied.expect_err("SET graph inside a txn must be rejected");
    let chain = error_chain(&err);
    assert!(
        chain.contains("transaction"),
        "expected a 'within one graph / transaction' rejection, got: {chain}"
    );
    client.simple_query("ROLLBACK").await.expect("ROLLBACK");
}

// ───────────────────────────────────────────────────────────────────────────
// FEATURE-GAP SEAM SPECS (ignored executable specs) — the audit's cross-modal
// write→read seams that are NOT yet built. Each is written to the INTENDED
// contract and `#[ignore]`d with the precise missing seam, so it flips green when
// the seam lands. They COMPILE against the real tokio-postgres client path.
// ───────────────────────────────────────────────────────────────────────────

/// SEAM (CONCEPT:EG-KG.txn.isolation-ryow-begin-set): inside an open transaction, staged writes are visible to a
/// same-connection *SQL* read (read-your-own-writes — proven by
/// `wire_txn_rollback_discards_and_ryow`) AND to an in-txn cross-modal `UQL …` read over
/// the PGWIRE TEXT protocol — the entrypoint this PR adds onto the committed EG-359 RPC
/// seam. The UQL is genuinely cross-modal: its `MATCH … WHERE rank = 99` reads the staged
/// UPDATE (graph modality, RYOW) and its `RANK BY ~[…]` reads the staged embedding (vector
/// modality, RYOW) — both overlaid onto the committed snapshot by the shared `run_unified`
/// overlay. (`RANK BY` retains only rows carrying an embedding — the documented eg-plan
/// hybrid-metadata-filter invariant — so the row the UPDATE touches is ALSO given a staged
/// embedding, which is what makes the filter→rank return it.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_txn_update_then_cross_modal_read() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");

    // Stage a graph write (UPDATE) AND a vector write (SET EMBEDDING) inside the txn —
    // neither committed yet. The cross-modal UQL below must read BOTH.
    client
        .execute("UPDATE nodes SET rank = $1 WHERE id = $2", &[&99i64, &"n1"])
        .await
        .expect("staged in-txn UPDATE");
    client
        .simple_query("SET EMBEDDING FOR 'n1' = '[1.0, 0.0, 0.0]'")
        .await
        .expect("staged in-txn embedding");

    // GREEN today: a plain-SQL RYOW read sees the staged write (buffered write path).
    let sql_ryow = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE rank = 99")
            .await
            .expect("plain-SQL RYOW inside txn"),
    );
    assert_eq!(
        sql_ryow,
        vec!["n1".to_string()],
        "plain SQL RYOW works today"
    );

    // THE SEAM: a CROSS-MODAL / UQL read issued over the wire ALSO reflects the staged
    // writes — a unified filter→rank over the staged row returns `n1` (RYOW across the
    // graph AND vector modalities before COMMIT).
    let unified = simple_ids(
        client
            .simple_query("UQL MATCH (:Agent) WHERE rank = 99 |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5")
            .await
            .expect("in-txn cross-modal/UQL read over the wire"),
    );
    assert_eq!(
        unified,
        vec!["n1".to_string()],
        "the cross-modal read must see the same-txn staged UPDATE + embedding (read-your-own-writes)"
    );

    client.simple_query("COMMIT").await.expect("COMMIT");
}

/// SEAM (CONCEPT:EG-KG.txn.isolation-ryow-begin-set): a single transaction that stages FIVE modalities over the
/// PGWIRE TEXT protocol — a graph node (`INSERT INTO nodes`), a vector embedding
/// (`SET EMBEDDING FOR`), a timeseries point (`INSERT INTO series`), an OWL/SPARQL axiom
/// (`SPARQL UPDATE INSERT DATA`), and a `SPARQL CONSTRUCT` — then COMMITs them atomically
/// through the shared RPC cross-modal commit (all modalities in ONE redb
/// `WriteTransaction`), and reads the committed result with a cross-modal UQL join over
/// the graph + vector + timeseries legs. This is the pgwire entrypoint for the EG-360/361/
/// 362 staging seam + EG-363 planner reach the RPC transport already proved. (The final
/// read uses `MATCH (:Sensor) |> RANK BY …`. The string-type↔IRI-class bridge for a
/// `REASON <iri>` over the committed axiom is now BUILT — EG-375/376 — and proved over the
/// wire by `wire_reason_iri_bridges_string_typed_node` below under the reasoner-exec build
/// layer; the in-txn tsdb-RYOW overlay is likewise built, EG-374, see `docs/north_star.md`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_txn_mixed_five_modality_commit() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    client.simple_query("BEGIN").await.expect("BEGIN");

    // (1) graph node.
    client
        .simple_query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('mm1', 'Sensor', 7, 'public')",
        )
        .await
        .expect("stage graph node");
    // (2) vector embedding for the new node (a wire verb the seam must add).
    client
        .simple_query("SET EMBEDDING FOR 'mm1' = '[1.0, 0.0, 0.0]'")
        .await
        .expect("stage vector embedding");
    // (3) timeseries point (tsdb staging the seam must add to txn.rs).
    client
        .simple_query("INSERT INTO series (id, ts, value) VALUES ('mm1', 1000, 42.0)")
        .await
        .expect("stage timeseries point");
    // (4) OWL/SPARQL axiom UPDATE (OWL-update-in-txn the seam must add).
    client
        .simple_query(
            "SPARQL UPDATE INSERT DATA { <http://ex/Sensor> \
             <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://ex/Device> }",
        )
        .await
        .expect("stage OWL axiom update");
    // (5) CONSTRUCT-materialized triple (CONSTRUCT-in-txn the seam must add).
    client
        .simple_query(
            "SPARQL CONSTRUCT { ?s <http://ex/kind> <http://ex/Device> } \
             WHERE { ?s a <http://ex/Sensor> }",
        )
        .await
        .expect("stage CONSTRUCT materialization");

    client.simple_query("COMMIT").await.expect("COMMIT");

    // A cross-modal UQL join reads the just-committed txn: the new Sensor node (graph)
    // ranked by its committed embedding (vector). (The in-txn tsdb-RYOW overlay — a
    // `TsScan` reading the txn's OWN staged points — is now built, EG-374; the staged
    // measurement above still commits atomically here.)
    let joined = simple_ids(
        client
            .simple_query("UQL MATCH (:Sensor) |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5")
            .await
            .expect("cross-modal UQL join over the committed five-modality txn"),
    );
    assert_eq!(
        joined,
        vec!["mm1".to_string()],
        "the staged modalities join in one UQL read over the committed txn"
    );
}

/// SEAM (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) — isolation + RYOW: a `BEGIN; SET EMBEDDING; INSERT INTO series;
/// <UQL join over graph+vector+tsdb>; COMMIT` reads its OWN writes inside the txn, while a
/// SECOND connection sees NONE of them until COMMIT (the cross-modal writes are staged, not
/// committed). After COMMIT the second connection reads the committed cross-modal state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_txn_crossmodal_ryow_isolated_until_commit() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let writer = connect(&addr).await;
    let reader = connect(&addr).await;

    // The UQL cross-modal join used by both connections: a Widget node filtered on the
    // graph modality and ranked by the query embedding (graph + vector legs). The
    // timeseries WRITE is exercised below; the in-txn tsdb-RYOW READ overlay (a `TsScan`
    // reading the txn's own staged points) is now built (EG-374) and proved by the
    // hand-built `eg_plan::tsdb_scan_tests::tsdb_in_txn_ryow_staged_overlay`.
    let uql = "UQL MATCH (:Widget) WHERE rank = 5 |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5";

    writer.simple_query("BEGIN").await.expect("BEGIN");
    // Stage a graph node, its embedding, and a measurement — all inside the txn.
    writer
        .simple_query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('vv1', 'Widget', 5, 'public')",
        )
        .await
        .expect("stage graph node");
    writer
        .simple_query("SET EMBEDDING FOR 'vv1' = '[1.0, 0.0, 0.0]'")
        .await
        .expect("stage embedding");
    writer
        .simple_query("INSERT INTO series (id, ts, value) VALUES ('vv1', 1000, 9.0)")
        .await
        .expect("stage measurement");

    // RYOW: the writer's OWN in-txn UQL join sees its staged node + embedding.
    let in_txn = simple_ids(
        writer
            .simple_query(uql)
            .await
            .expect("in-txn cross-modal UQL"),
    );
    assert_eq!(
        in_txn,
        vec!["vv1".to_string()],
        "the writer reads its own staged cross-modal writes (RYOW)"
    );

    // Isolation: a SECOND connection (no open txn) sees NONE of the staged writes.
    let before = simple_ids(
        reader
            .simple_query(uql)
            .await
            .expect("off-txn cross-modal UQL"),
    );
    assert!(
        before.is_empty(),
        "a second connection must see none of the uncommitted cross-modal writes, got {before:?}"
    );

    writer.simple_query("COMMIT").await.expect("COMMIT");

    // After COMMIT the committed cross-modal state is visible to the second connection.
    let after = simple_ids(
        reader
            .simple_query(uql)
            .await
            .expect("post-commit cross-modal UQL"),
    );
    assert_eq!(
        after,
        vec!["vv1".to_string()],
        "after COMMIT the committed node + embedding are visible off-txn"
    );
}

/// SEAM (CONCEPT:EG-KG.query.reason-iri-parses-angle / EG-376) — `REASON <iri>` + the string-type↔IRI-class bridge over
/// the PGWIRE wire. Off-txn (each statement auto-commits as one atomic cross-modal write):
/// INSERT a bare-string-typed `Sensor` node, then stage the OWL axiom `<http://ex/Sensor>
/// ⊑ <http://ex/Device>` via `SPARQL UPDATE`. A `UQL MATCH (:Sensor) |> REASON
/// <http://ex/Device>` read then returns the Sensor node — the bare string `type`
/// `"Sensor"` is BRIDGED to the class IRI `<http://ex/Sensor>` (the target's namespace +
/// the local name) and reasoned into `<http://ex/Device>` through the committed subclass
/// axiom. This is the ORIGINAL intended `REASON <iri>` read the five-modality spec had to
/// avoid before the bridge existed. It needs the reasoner-exec build layer (`owl-plan` →
/// `eg-plan/owl`, which compiles the `Op::Reason` executor the UQL clause lowers to); it
/// is in the `full` build (so the `cargo-test-full` / `clippy --features full` gates cover
/// it) but not the narrower `owl` feature alone.
#[cfg(feature = "owl-plan")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_reason_iri_bridges_string_typed_node() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    // A string-typed Sensor node (auto-commits as one atomic statement).
    client
        .simple_query(
            "INSERT INTO nodes (id, type, rank, _visibility) VALUES ('sen1', 'Sensor', 3, 'public')",
        )
        .await
        .expect("insert string-typed Sensor node");
    // The OWL axiom Sensor ⊑ Device (SPARQL UPDATE auto-commits, lowered to graph triples
    // the reasoner reconstructs as TBox).
    //
    // FIXED (A18 -- was the KNOWN GAP this test used to document): the two axiom class
    // nodes (`<http://ex/Sensor>`, `<http://ex/Device>`) land with an EMPTY property
    // blob (`lower_triples`/`eg_rdf::update::insert_triple` only populate ordinary
    // properties for a subject/object that also carries a literal-valued triple) --
    // untagged and unowned in the ABox sense. Row-level default-deny RLS
    // (`IsolationLayer::can_see_row`) exists to protect ABox rows, but an OWL axiom is
    // SCHEMA (TBox), not a row about anyone -- applying row-level default-deny to it was
    // a category error, and its consequence was exactly this: `REASON <http://ex/Device>`
    // never saw the axiom for the pgwire "tester" actor. `eg_rdf::update::insert_triple`
    // (and `mapping::lower_triples` for bulk RDF load) now stamps BOTH endpoints of a
    // recognized RDFS/OWL schema predicate (`rdfs:subClassOf` here) with
    // `eg_core::isolation::RLS_SCHEMA_KEY`, and `can_see_row` treats a schema-marked row
    // as visible -- narrowly: ONLY rows the RDF/OWL lowering structurally identified as
    // schema, never merely because they are untagged. Graph-level ACL
    // (`check_graph_access`, exercised in the SECOND connection below) is completely
    // unchanged and still gates whether an actor reaches row filtering at all.
    client
        .simple_query(
            "SPARQL UPDATE INSERT DATA { <http://ex/Sensor> \
             <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://ex/Device> }",
        )
        .await
        .expect("stage OWL subclass axiom");

    // REASON <http://ex/Device> over the wire: MATCH (:Sensor) sources the string-typed
    // node; the mid-plan REASON filters to the IRI class's members, and the bridge makes
    // the bare `"Sensor"` a member of `<http://ex/Device>`.
    let ids = simple_ids(
        client
            .simple_query("UQL MATCH (:Sensor) |> REASON <http://ex/Device> |> LIMIT 5")
            .await
            .expect("REASON <iri> read over the wire"),
    );
    assert_eq!(
        ids,
        vec!["sen1".to_string()],
        "REASON <http://ex/Device> includes the string-typed Sensor via the string→IRI bridge"
    );

    // ── A18, the OTHER direction: an actor with NO graph access sees NOTHING, TBox
    // included. `stranger` completes SCRAM login (the shared secret alone proves
    // possession; login never consults `IsolationLayer`), but is never
    // `register_agent`-ed, so `IsolationLayer::check_access` denies it before the
    // read even reaches row-level filtering -- proving the TBox exemption did not
    // widen the GRAPH-level gate this fix leaves untouched.
    let stranger_pw = pgwire::derive_pg_password("test", "stranger");
    let stranger = connect_scram(&addr, "stranger", &stranger_pw, "__commons__")
        .await
        .expect(
            "SCRAM login succeeds for an unregistered user (login proves only the shared secret)",
        );
    let denied = stranger
        .simple_query("UQL MATCH (:Sensor) |> REASON <http://ex/Device> |> LIMIT 5")
        .await;
    let err = denied.expect_err("an unregistered actor must be denied __commons__ graph access");
    let chain = error_chain(&err);
    assert!(
        chain.contains("permission denied") || chain.contains("42501"),
        "expected a graph-level ACL denial, got chain: {chain}"
    );
}

/// BUG A3 (2026-08-12): the `RLS_SCHEMA_KEY` TBox marker was stamped on axiom
/// lowering but never cleared on axiom deletion, so an IRI once used as a
/// schema term stayed schema-visible to every non-`System` actor FOREVER,
/// even after being repurposed as (or simply left as) an ordinary,
/// untagged/unowned ABox node — a permanent, one-way visibility leak.
///
/// Design: derive schema-ness from `GraphCore::schema_refs`, a live reverse
/// index (`iri -> count of live schema-defining triples`) maintained in the
/// SAME mutation as the triple write/delete, so a node is TBox iff that
/// count is `> 0` — definitionally correct rather than eventually correct.
///
/// This test: define the axiom (`<http://ex/Sensor> rdfs:subClassOf
/// <http://ex/Device>`, EXACTLY the fixture `wire_reason_iri_bridges_string_
/// typed_node` above proves is schema-exempt while the axiom is live), then
/// DELETE the axiom, and assert BOTH untagged/unowned class IRIs revert to
/// ordinary ABox default-deny for the SAME non-`System` `tester` actor that
/// could see them before the delete — proving the exemption is genuinely
/// undone, not merely left in whatever state deletion happened to leave it.
#[cfg(feature = "owl-plan")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn axiom_deletion_reverts_schema_node_to_abox_default_deny() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    // Stage the axiom (auto-commits as one atomic SPARQL UPDATE statement) —
    // same class pair as the sibling INSERT-side fixture above.
    client
        .simple_query(
            "SPARQL UPDATE INSERT DATA { <http://ex/Sensor> \
             <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://ex/Device> }",
        )
        .await
        .expect("stage OWL subclass axiom");

    // BEFORE delete: both untagged, unowned class nodes are visible to
    // `tester` (a non-`System` actor) via an ordinary row-level-gated SQL
    // read — the schema exemption is active.
    let sensor_visible_before = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE id = '<http://ex/Sensor>'")
            .await
            .expect("SELECT Sensor class node before delete"),
    );
    let device_visible_before = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE id = '<http://ex/Device>'")
            .await
            .expect("SELECT Device class node before delete"),
    );
    assert_eq!(
        sensor_visible_before,
        vec!["<http://ex/Sensor>".to_string()],
        "the Sensor class node must be schema-visible to a non-System actor \
         while the axiom is live"
    );
    assert_eq!(
        device_visible_before,
        vec!["<http://ex/Device>".to_string()],
        "the Device class node must be schema-visible to a non-System actor \
         while the axiom is live"
    );

    // Delete the ONE axiom triple that made both nodes schema. Neither node
    // is otherwise owned/tagged/public, so nothing else keeps them visible.
    client
        .simple_query(
            "SPARQL UPDATE DELETE DATA { <http://ex/Sensor> \
             <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://ex/Device> }",
        )
        .await
        .expect("delete OWL subclass axiom");

    // AFTER delete: both class nodes revert to ordinary ABox default-deny for
    // the SAME `tester` connection/actor — this is the actual bug. Before the
    // fix, the `_schema` blob marker was write-once and never cleared, so
    // this SELECT still returned the row.
    let sensor_visible_after = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE id = '<http://ex/Sensor>'")
            .await
            .expect("SELECT Sensor class node after delete"),
    );
    let device_visible_after = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE id = '<http://ex/Device>'")
            .await
            .expect("SELECT Device class node after delete"),
    );
    assert!(
        sensor_visible_after.is_empty(),
        "the Sensor class node must revert to ABox default-deny once its only \
         schema-defining triple is deleted, got: {sensor_visible_after:?}"
    );
    assert!(
        device_visible_after.is_empty(),
        "the Device class node must revert to ABox default-deny once its only \
         schema-defining triple is deleted, got: {device_visible_after:?}"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// DIFFERENTIAL WIRE-SURFACE ORACLE (CONCEPT:EG-KG.query.pgwire-leg) — the pgwire leg of the
// query-surface harness. The in-process UQL-vs-builder-vs-oracle legs live in
// `crates/eg-plan/tests/differential_oracle.rs`; this leg REUSES the live pgwire
// listener above to prove the SQL-wire and UQL-wire surfaces agree on the SAME query,
// so a caller reaches the identical result whichever door they speak.
// ───────────────────────────────────────────────────────────────────────────

/// The SAME relational filter expressed as plain SQL and as UQL over the pgwire text
/// protocol returns the IDENTICAL id set. `type='Agent' AND rank>1` (SQL) and
/// `MATCH (:Agent) WHERE rank > 1` (UQL) must both select exactly `n2` — one wire, two
/// surfaces, one answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_sql_and_uql_agree_on_same_filter() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    let mut sql = simple_ids(
        client
            .simple_query("SELECT id FROM nodes WHERE type = 'Agent' AND rank > 1")
            .await
            .expect("SQL filter"),
    );
    sql.sort();
    let mut uql = simple_ids(
        client
            .simple_query("UQL MATCH (:Agent) WHERE rank > 1")
            .await
            .expect("UQL filter"),
    );
    uql.sort();

    assert_eq!(
        sql, uql,
        "the SQL and UQL wire surfaces must return the identical set for the same filter"
    );
    assert_eq!(
        sql,
        vec!["n2".to_string()],
        "only n2 is an Agent with rank > 1"
    );
}

/// The UQL wire surface is DETERMINISTIC: the same cross-modal `MATCH → RANK → LIMIT`
/// issued twice over the wire yields the byte-identical ordered id list (the wire leg of
/// the harness's determinism / snapshot-isolation property).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_uql_is_deterministic() {
    let state = seeded_state();
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    // Give the two Agent nodes embeddings so the RANK leg has something to order.
    client
        .simple_query("SET EMBEDDING FOR 'n1' = '[0.2, 0.9, 0.0]'")
        .await
        .expect("embed n1");
    client
        .simple_query("SET EMBEDDING FOR 'n2' = '[0.98, 0.2, 0.0]'")
        .await
        .expect("embed n2");

    let uql = "UQL MATCH (:Agent) |> RANK BY ~[1.0,0.0,0.0] |> LIMIT 5";
    let a = simple_ids(client.simple_query(uql).await.expect("UQL run 1"));
    let b = simple_ids(client.simple_query(uql).await.expect("UQL run 2"));
    assert_eq!(a, b, "the same UQL over the wire must be deterministic");
    assert_eq!(
        a,
        vec!["n2".to_string(), "n1".to_string()],
        "ranked n2 > n1"
    );
}
