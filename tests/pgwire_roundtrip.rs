//! Postgres wire round-trip integration test (CONCEPT:KG-2.189).
//!
//! Starts the real pgwire listener over an in-process `ServerState`, connects with
//! the real `tokio-postgres` client, and proves the shim end-to-end:
//!   * `SELECT id FROM nodes …` over a seeded graph returns the expected rows
//!     (reusing the eg-query DataFusion path — the SAME code `Method::Sql` runs),
//!   * an `INSERT INTO nodes …` (routed through the GraphTxn write path) is visible
//!     to a subsequent `SELECT` on the same connection.
//!
//! It also proves the durability barrier (CONCEPT:KG-2.190): a pgwire INSERT runs
//! the SAME post-write durable-record block dispatch runs, so the wire write is
//!   * fire-and-forget `record()`'d in the default (non-authoritative) regime
//!     (regression: today's behavior intact, plus the per-op record M8 was missing),
//!   * commit-before-ack `record_durable().await`'d under redb-AUTHORITATIVE mode —
//!     verified by reading the row back from a FRESH redb backend on the same dir
//!     (durable WITHOUT any checkpoint), the gated test below.
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

/// Build a minimal `ServerState` with one seeded node so a wire SELECT has rows to
/// return. `__commons__` is pre-created by the registry. `persistence` /
/// `redb_authoritative` parameterize the durability tier so the durability tests can
/// exercise both regimes (default = `None` / `false` = the cache-only path).
fn state_with(
    persistence: Option<Arc<dyn PersistenceBackend>>,
    redb_authoritative: bool,
) -> Arc<RwLock<ServerState>> {
    let registry = GraphRegistry::new();
    // Seed three nodes directly via the graph core (the engine write API).
    {
        let core = registry.get("__commons__").unwrap().core.clone();
        for (id, ty, rank) in [("n1", "Agent", 1i64), ("n2", "Agent", 2), ("n3", "Tool", 3)] {
            let blob =
                rmp_serde::to_vec_named(&serde_json::json!({"type": ty, "rank": rank})).unwrap();
            core.add_node(id.to_string(), blob);
        }
    }
    Arc::new(RwLock::new(ServerState {
        registry,
        isolation: IsolationLayer::new(),
        channels: ChannelManager::new(),
        auth_secret: "test".to_string(),
        persist_dir: None,
        persistence,
        redb_authoritative,
        max_in_flight: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(
            epistemic_graph::write_coalescer::WriteCoalescerRegistry::from_env(),
        ),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(TxnIdGen::default()),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "raft")]
        raft: None,
    }))
}

/// The cache-only default state (no durable tier) the original round-trip tests use.
fn seeded_state() -> Arc<RwLock<ServerState>> {
    state_with(None, false)
}

/// Bind the listener on an ephemeral port, then start serving it. Returns the
/// chosen `127.0.0.1:<port>` address.
async fn spawn_listener(state: Arc<RwLock<ServerState>>) -> String {
    // Probe a free port, then let `serve` bind it (a tiny race window, fine for a test).
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let addr_s = addr.to_string();
    let serve_addr = addr_s.clone();
    tokio::spawn(async move {
        let _ = pgwire::serve(&serve_addr, state).await;
    });
    // Give the listener a moment to bind.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    addr_s
}

/// Connect a real tokio-postgres client to the shim (trust auth, first increment).
async fn connect(addr: &str) -> tokio_postgres::Client {
    let conn_str = format!("host=127.0.0.1 port={} user=tester dbname=__commons__", {
        addr.rsplit(':').next().unwrap()
    });
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
        .simple_query("INSERT INTO nodes (id, type, rank) VALUES ('n9', 'Agent', 9)")
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

/// A recording `PersistenceBackend` that captures every `record()` / `record_durable`
/// call. Lets the non-authoritative regression test assert a pgwire write is handed
/// to the durable writer (write-behind) WITHOUT a real durable store — and that the
/// authoritative `record_durable` await path is NOT taken when the flag is off.
#[derive(Default)]
struct RecordingBackend {
    recorded: std::sync::Mutex<Vec<(String, String)>>,
    durable: std::sync::atomic::AtomicUsize,
}

impl RecordingBackend {
    /// `(graph_fname, node_id)` pairs captured via the fire-and-forget `record()`.
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
    async fn checkpoint_all(&self, _state: &Arc<RwLock<ServerState>>) -> Result<usize, String> {
        Ok(0)
    }
    fn record(&self, graph_fname: &str, method: &epistemic_graph::protocol::Method) {
        if let epistemic_graph::protocol::Method::AddNode { node_id, .. } = method {
            self.recorded
                .lock()
                .unwrap()
                .push((graph_fname.to_string(), node_id.clone()));
        }
    }
    async fn record_durable(
        &self,
        graph_fname: &str,
        method: &epistemic_graph::protocol::Method,
    ) -> Result<(), String> {
        self.durable
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Mirror the trait default so the captured pair is still observable.
        self.record(graph_fname, method);
        Ok(())
    }
    fn shutdown(&self) {}
}

/// Non-authoritative regression (CONCEPT:KG-2.190): with a durable backend present
/// but `redb_authoritative = false`, a pgwire INSERT must take the write-BEHIND path
/// — fire-and-forget `record()` (NOT the awaited `record_durable`). Proves today's
/// behavior is intact AND the per-op `record()` the M8 path was missing now fires.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_insert_non_authoritative_records_write_behind() {
    let backend = Arc::new(RecordingBackend::default());
    let state = state_with(Some(backend.clone()), false);
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

    // Write-behind: `record()` fired once for the inserted node; the awaited
    // commit-before-ack path was NOT taken (that is authoritative-only).
    assert_eq!(
        backend.recorded_pairs(),
        vec![("__commons__".to_string(), "w1".to_string())],
        "write-behind record() must fire for the pgwire INSERT"
    );
    assert_eq!(
        backend.durable_calls(),
        0,
        "record_durable must NOT be awaited when not authoritative"
    );
}

/// Authoritative durability (CONCEPT:KG-2.190): under `EPISTEMIC_GRAPH_REDB_AUTHORITATIVE`
/// a pgwire INSERT is commit-before-ack — `record_durable` is AWAITED before the
/// CommandComplete is sent. We prove the wire write is durable WITHOUT any checkpoint
/// by reading the row back from a SEPARATE redb backend reopened on the same dir: the
/// row is only there because the INSERT's await observed a durable commit, exactly
/// like a normal `Method::AddNode` write. Uses `FsyncPolicy::Interval` so the ONLY way
/// the row lands is the group-commit barrier firing the awaited writer.
#[cfg(feature = "redb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_insert_authoritative_is_durable_without_checkpoint() {
    use epistemic_graph::server::persistence::redb_backend::RedbBackend;
    use epistemic_graph::wal_service::FsyncPolicy;

    let dir = std::env::temp_dir().join(format!("eg-pgwire-durable-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir_s = dir.to_string_lossy().to_string();

    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(
            dir_s.clone(),
            FsyncPolicy::Interval(std::time::Duration::from_millis(20)),
            64,
        )
        .expect("open redb backend"),
    );
    let state = state_with(Some(backend.clone()), true);
    let addr = spawn_listener(state).await;
    let client = connect(&addr).await;

    // INSERT over the wire. Under authoritative mode the CommandComplete below is
    // only returned AFTER record_durable's group-commit has fsynced this op.
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
    // SAME dir and point-reading the node — no checkpoint_all was ever called.
    // Shut the live backend down first so its owner thread drops the redb `Database`
    // and releases the exclusive file lock (redb forbids two open handles).
    backend.shutdown();
    let reopened = RedbBackend::open(
        dir_s.clone(),
        FsyncPolicy::Interval(std::time::Duration::from_millis(20)),
        64,
    )
    .expect("reopen redb backend");
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
