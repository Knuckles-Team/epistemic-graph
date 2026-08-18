//! NE-071 (EG-OCC-SWEEP) regression guard: a wire multi-statement `BEGIN
//! ISOLATION LEVEL SERIALIZABLE … COMMIT` transaction MUST reject a commit
//! whose captured predicate read-set was invalidated by a concurrent phantom
//! insert, landed WHILE the transaction was open.
//!
//! This is the exact class of bug the P0 fixed on `main`
//! (`WireSession::txn_begin_version`, captured at the real `BEGIN`) and the
//! residue this track closes: `commit_graph_methods_with_op` /
//! `new_txn_state`'s `begin_version` parameter used to default
//! (`Option<u64>::unwrap_or_else(|| core.version())`) to the CURRENT
//! (commit-time) version whenever a caller passed `None` — silently
//! reproducing the exact defeated-guard shape even after the primary BEGIN
//! fix landed, for any call site that forgot (or a future refactor
//! accidentally caused) to thread the real begin-time version through.
//!
//! Proof shape (mirrors `tests/advanced_crossmodal_roundtrip.rs`'s
//! `concurrent_serializable_phantom_conflict_eg392`, but over the WIRE SQL
//! `BEGIN`/`COMMIT` surface `commit_graph_methods_with_op` actually serves,
//! not the RPC `Method::BeginTxn`/`Commit` surface that test covers):
//!
//!   1. Seed one `Sensor` node so a `label=Sensor` predicate read-set is
//!      non-empty at `BEGIN`.
//!   2. Connection A: `BEGIN ISOLATION LEVEL SERIALIZABLE`, then
//!      `SELECT … WHERE type = 'Sensor'` — this fingerprints the predicate
//!      AT READ TIME, real begin-time state.
//!   3. Connection A buffers an unrelated node INSERT (any durable write is
//!      needed so `COMMIT` reaches `commit_graph_methods_with_op` with
//!      `isolation = Serializable` and a non-empty `predicate_reads`).
//!   4. Connection B (a separate, autocommit connection) inserts a PHANTOM
//!      `Sensor` node and commits immediately — landing WHILE A's
//!      transaction is still open, bumping `core.version()`.
//!   5. Connection A: `COMMIT` — MUST fail (`GraphTxnState::validate`
//!      re-evaluates the `label=Sensor` predicate, sees the phantom, and
//!      conflicts), and A's own buffered node write must never land (a true
//!      rollback).
//!
//! `begin_version_fallback_reintroduction_would_fail_this_test` documents
//! (in prose, since it cannot be enabled in the same binary two ways at
//! once) exactly how this test was manually proven to fail against a
//! reintroduced commit-time `begin_version` — see that function's doc for
//! the exact repro steps used.

#![cfg(all(feature = "pgwire", feature = "redb"))]

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::isolation::{AgentIdentity, AgentRole, IsolationLayer};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::persistence::PersistenceBackend;
use epistemic_graph::server::pgwire;
use epistemic_graph::server::txn::TxnIdGen;
use epistemic_graph::server::ServerState;

const AUTH_SECRET: &str = "occ-begin-version-secret";
const AGENT: &str = "occtester";

fn ensure_env() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
        std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-occ-sweep");
        std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-occ-sweep");
        std::env::set_var(
            epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
            "occ-begin-version-recovery-key",
        );
    });
}

/// A real tempdir-backed `RedbBackend` — the wire graph-node commit path
/// (`commit_cross_modal_txn`) fails closed without durable persistence.
fn persistence_pair() -> (String, Arc<dyn PersistenceBackend>) {
    use epistemic_graph::durability::DurabilityPolicy;
    use epistemic_graph::server::persistence::redb_backend::RedbBackend;
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let dir = std::env::temp_dir().join(format!(
        "eg-occ-begin-version-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create occ test persist dir");
    let dir_s = dir.to_string_lossy().into_owned();
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 4096)
            .expect("open occ test redb backend"),
    );
    (dir_s, backend)
}

/// Build a `ServerState` with one seeded `Sensor` node. `AGENT` is registered
/// `AgentRole::System` — every graph-ACL and row-visibility check trivially
/// passes (`crates/eg-core/src/isolation.rs::can_see_row` / `check_access`),
/// so this test proves transaction OCC semantics, never RBAC/RLS (covered
/// elsewhere).
fn state_with_sensor_seed() -> Arc<RwLock<ServerState>> {
    ensure_env();
    let registry = GraphRegistry::new();
    {
        let core = registry.get("__commons__").unwrap().core.clone();
        let blob = rmp_serde::to_vec_named(&serde_json::json!({"type": "Sensor"})).unwrap();
        core.add_node("s0".to_string(), blob);
    }
    let mut isolation = IsolationLayer::new();
    isolation.register_agent(AgentIdentity {
        agent_id: AGENT.into(),
        role: AgentRole::System,
        teams: Vec::new(),
        roles: Vec::new(),
    });
    let (persist_dir, persistence) = persistence_pair();
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry,
        isolation,
        channels: ChannelManager::new(),
        #[cfg(feature = "viz-static-export")]
        viz_engine: None,
        auth_secret: AUTH_SECRET.to_string(),
        #[cfg(feature = "kv")]
        kv: None,
        persist_dir: Some(persist_dir),
        persistence: Some(persistence),
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(
            epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
        ),
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
        tsdb_store: Some({
            static NEXT_TSDB_SEQ: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(1);
            let path = std::env::temp_dir().join(format!(
                "eg-occ-begin-version-tsdb-{}-{}.redb",
                std::process::id(),
                NEXT_TSDB_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
            std::sync::Arc::new(
                eg_tsdb::store::SeriesStore::open(&path).expect("open occ test series store"),
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

async fn wait_for_listener_ready(addr: &str) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn spawn_listener(state: Arc<RwLock<ServerState>>) -> String {
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);
    let addr_s = addr.to_string();
    let serve_addr = addr_s.clone();
    tokio::spawn(async move {
        let _ = pgwire::serve_with_auth(&serve_addr, state, pgwire::PgWireAuthMode::Scram).await;
    });
    wait_for_listener_ready(&addr_s).await;
    addr_s
}

async fn connect(addr: &str) -> tokio_postgres::Client {
    let password = pgwire::derive_pg_password(AUTH_SECRET, AGENT);
    let conn_str = format!(
        "host=127.0.0.1 port={} user={AGENT} password={password} dbname=__commons__",
        addr.rsplit(':').next().unwrap(),
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, tokio_postgres::NoTls)
        .await
        .expect("pgwire connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

fn simple_ids(msgs: Vec<tokio_postgres::SimpleQueryMessage>) -> Vec<String> {
    msgs.into_iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect()
}

/// THE regression guard (NE-071): a `SERIALIZABLE` wire transaction's captured
/// `label=Sensor` predicate read-set must survive a concurrent phantom insert
/// that lands WHILE the transaction is open, and `COMMIT` must reject on it.
///
/// If `WireSession`'s `begin_version` threading regresses to a commit-time
/// re-derivation (the fallback this track closed, or the primary `BEGIN`
/// capture this sweep depends on), `GraphTxnState::validate`'s coarse guard
/// (`core.version() == begin_version`) trivially short-circuits BEFORE the
/// predicate re-check ever runs, `B`'s phantom goes undetected, and `A`'s
/// `COMMIT` silently SUCCEEDS instead of conflicting — flipping this test's
/// `assert!(commit.is_err(), …)` to a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_serializable_commit_rejects_concurrent_phantom() {
    let state = state_with_sensor_seed();
    let addr = spawn_listener(state).await;

    // Connection A: the SERIALIZABLE transaction under test.
    let a = connect(&addr).await;
    a.simple_query("BEGIN ISOLATION LEVEL SERIALIZABLE")
        .await
        .expect("BEGIN ISOLATION LEVEL SERIALIZABLE");
    // Seeds A's predicate read-set: {s0} labelled Sensor, fingerprinted NOW
    // (real begin-time state, before B's phantom lands).
    let seen = simple_ids(
        a.simple_query("SELECT id FROM nodes WHERE type = 'Sensor'")
            .await
            .expect("SELECT inside SERIALIZABLE txn"),
    );
    assert_eq!(
        seen,
        vec!["s0".to_string()],
        "predicate read-set must see exactly the seeded Sensor at begin"
    );
    // A durable write of A's own, so COMMIT actually reaches
    // `commit_graph_methods_with_op` (a read-only txn has `has_node_ops ==
    // false` and never calls it at all).
    a.simple_query("INSERT INTO nodes (id, type) VALUES ('a_node', 'Widget')")
        .await
        .expect("buffer A's own node insert");

    // Connection B: an UNRELATED, off-txn autocommit connection inserts a
    // PHANTOM Sensor WHILE A's transaction is still open.
    let b = connect(&addr).await;
    b.simple_query("INSERT INTO nodes (id, type) VALUES ('s_phantom', 'Sensor')")
        .await
        .expect("B's phantom Sensor insert must land immediately (autocommit)");

    // A commits AFTER B's phantom landed: serializable validation MUST
    // re-evaluate the `label=Sensor` predicate, see the phantom, and reject.
    let commit = a.simple_query("COMMIT").await;
    assert!(
        commit.is_err(),
        "A's SERIALIZABLE commit must CONFLICT on B's phantom Sensor, but it \
         succeeded — begin_version silently defeated OCC validation \
         (got Ok: {:?})",
        commit.is_ok()
    );

    // A's own buffered node write never landed (true rollback — nothing
    // partially applied).
    let after = simple_ids(
        b.simple_query("SELECT id FROM nodes WHERE type = 'Widget'")
            .await
            .expect("SELECT after A's conflicted commit"),
    );
    assert!(
        after.is_empty(),
        "A's conflicted transaction must not have persisted its buffered \
         write, got {after:?}"
    );
}

/// This test's bug-catching power was PROVEN, not just asserted, before this
/// track closed (NE-071 / EG-OCC-SWEEP definition-of-done requires watching
/// the guard actually fail against a reintroduced bug, then reverting):
///
///   1. In `src/server/wire/mod.rs`, the `has_node_ops` branch of `run_commit`
///      temporarily changed
///      `let begin_version = self.require_txn_begin_version()?;` to
///      `let begin_version = TxnBeginVersion::Autocommit;` — reinstating
///      exactly the commit-time re-derivation the original P0 (and this
///      sweep's fallback fix) closed, for the SAME code path this test
///      exercises.
///   2. `cargo test --features full --test occ_wire_serializable_begin_version
///      wire_serializable_commit_rejects_concurrent_phantom` was run against
///      that change.
///   3. Observed failure: `A's SERIALIZABLE commit must CONFLICT on B's
///      phantom Sensor, but it succeeded` — the `COMMIT` returned `Ok`
///      instead of erroring, exactly the silent-defeat this guard exists to
///      catch.
///   4. The temporary change was reverted (`git diff` clean against the real
///      fix) and the test re-run to confirm it passes again.
///
/// This function carries no executable assertion of its own — it exists so
/// the proof is discoverable in-repo, next to the guard it documents, rather
/// than only in a track report.
#[test]
fn begin_version_fallback_reintroduction_would_fail_this_test() {}
