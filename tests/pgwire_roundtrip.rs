//! Postgres wire round-trip integration test (CONCEPT:KG-2.189).
//!
//! Starts the real pgwire listener over an in-process `ServerState`, connects with
//! the real `tokio-postgres` client, and proves the shim end-to-end:
//!   * `SELECT id FROM nodes …` over a seeded graph returns the expected rows
//!     (reusing the eg-query DataFusion path — the SAME code `Method::Sql` runs),
//!   * an `INSERT INTO nodes …` (routed through the GraphTxn write path) is visible
//!     to a subsequent `SELECT` on the same connection.
//!
//! Only compiled with `--features pgwire` (the listener + the eg-query SQL path).

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

/// Build a minimal, persistence-free `ServerState` with one seeded node so a wire
/// SELECT has rows to return. `__commons__` is pre-created by the registry.
fn seeded_state() -> Arc<RwLock<ServerState>> {
    let registry = GraphRegistry::new();
    // Seed two nodes directly via the graph core (the engine write API).
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
        persistence: None,
        redb_authoritative: false,
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
    }))
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
