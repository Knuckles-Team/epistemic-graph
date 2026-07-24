//! `SqlContextCache` invalidation proofs (CONCEPT:EG-KG.query.served-context-cache, the WS-H served-path
//! SQL context cache). Mirrors `kvcache_invalidation.rs`'s own discipline (cache ON vs
//! OFF must be byte-identical; a mutation the epoch does not yet reflect must MISS and
//! rebuild) but for the WHOLE `SessionContext` — durable views + UDF/UDAF/UDTF
//! registration + the synthesized system catalogs — not just the `nodes`/`edges` Arrow
//! tables `SqlCache`/`exec_sql_cached` already cover.
//!
//! Five properties, each its own test:
//!   1. a view add/alter/drop is reflected by the NEXT query, never served stale.
//!   2. two different epochs (graph version, OR caller identity) never share a
//!      cached context — proven both ways, INCLUDING the caller-identity axis: the
//!      served path RLS-filters `nodes`/`edges`/views/pagerank/betweenness PER CALLER
//!      before this module ever sees them, so two callers' filtered snapshots at an
//!      otherwise-identical graph/catalog epoch must never share an entry (else one
//!      agent's row-filtered data would leak to a different agent through the cache).
//!      `edges_never_leak_across_callers_through_the_cache` proves the SAME property
//!      specifically through `EdgesTableProvider` (P10/W1.7-A/C) — its own
//!      per-instance memoization and src/dst-pushed traversal — not just `nodes`.
//!   3. a repeated query at the SAME epoch is a genuine cache HIT (`SqlContextCache::stats`)
//!      and returns byte-identical rows to the uncached path.
//!   4. a battery of differently-shaped queries (WHERE, aggregate, JOIN, a view
//!      reference, ORDER BY) all match the uncached path, cached or not.
//!   5. a commit under a DIFFERENT `MUTATION_VERSION` scope string than the literal
//!      `(tenant, graph)` pair being queried — the exact shape
//!      `src/server/handlers/sqlite_file.rs`'s sqlite-import gateway uses — still
//!      invalidates (closes the gap a cache keyed on ONE scope's `mutation_version`
//!      alone would miss; `TableStore::catalog_fingerprint`'s own doc covers why).

#![cfg(feature = "sql")]

use eg_core::graph::GraphCore;
use eg_query::{
    exec_sql_typed_with_tables_cached_cancellable, exec_sql_typed_with_tables_cancellable,
    CancellationToken, Column, ColumnType, SqlContextCache, TableSchema, TableStore, TableTxn,
    TxnOp, TypedQueryResult,
};
use eg_types::mutation_batch::{
    MutationBatch, MutationDomain, MutationOperation, MutationOutboxIntent, MutationRequestContext,
    MutationSurface, MUTATION_BATCH_VERSION,
};
use serde_json::json;

const TENANT: &str = "tenant-cache-a";
const GRAPH: &str = "graph-cache-a";

/// n1 (Agent, rank 1), n2 (Agent, rank 2), n3 (Tool, rank 3), edge n1->n2. Returned
/// as a `GraphCore` so a test can read `.version()`/`.analysis_snapshot()` at will.
fn graph() -> GraphCore {
    let core = GraphCore::new();
    for (id, ty, rank) in [("n1", "Agent", 1i64), ("n2", "Agent", 2), ("n3", "Tool", 3)] {
        core.add_node(
            id.to_string(),
            rmp_serde::to_vec_named(&json!({ "type": ty, "rank": rank })).unwrap(),
        );
    }
    core.add_edge(
        "n1".to_string(),
        "n2".to_string(),
        rmp_serde::to_vec_named(&json!({ "relationship": "KNOWS" })).unwrap(),
    )
    .unwrap();
    core
}

/// A well-formed `MutationBatch` wrapping `txn` for the SQL-catalog gateway,
/// fenced against the store's CURRENT `mutation_version(tenant, graph)` (read fresh
/// each call so a sequence of commits never trips `STALE_VERSION`). Mirrors
/// `eg-query`'s own `tables::store::tests::sql_batch`/`create_metrics_txn` fixture
/// shape (the SAME crate; the facade's `commit_sql_catalog_txn` builds an
/// equivalent batch over the SAME `TableStore::commit_txn_batch` gateway in the
/// served path — this is the identical mechanism, not a stand-in for it).
fn commit(store: &TableStore, tenant: &str, graph: &str, seq: &mut u64, txn: TableTxn) {
    *seq += 1;
    let batch_id = format!("ctx-cache-{tenant}-{graph}-{seq}");
    let expected = store.mutation_version(tenant, graph).unwrap();
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: MutationRequestContext {
            request_id: *seq,
            principal: format!("principal:sha256:{}", "a".repeat(64)),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
        },
        tenant: tenant.to_string(),
        graph: graph.to_string(),
        placement_epoch: 0,
        idempotency_key: format!("idem-{batch_id}"),
        expected_graph_version: Some(expected),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Query,
            domain: MutationDomain::SqlCatalog,
            method: eg_types::protocol::Method::ApplyMutation {
                event_type: "sql_catalog_operation".to_string(),
                query: "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string(),
            },
        }],
        outbox: vec![MutationOutboxIntent {
            topic: "engine.projection.rebuild".to_string(),
            key: batch_id.clone(),
            payload: vec![1],
            headers: Default::default(),
        }],
        created_at_ms: 100 + *seq,
    };
    store
        .commit_txn_batch(&txn, &batch, 100 + *seq)
        .unwrap_or_else(|e| panic!("commit {batch_id} failed: {e}"));
}

fn create_view(name: &str, select_sql: &str) -> TableTxn {
    let mut txn = TableTxn::new();
    txn.push(TxnOp::CreateView {
        name: name.to_string(),
        select_sql: select_sql.to_string(),
        or_replace: false,
    });
    txn
}

fn drop_view(name: &str) -> TableTxn {
    let mut txn = TableTxn::new();
    txn.push(TxnOp::DropView {
        name: name.to_string(),
        if_exists: false,
    });
    txn
}

fn rows(r: &TypedQueryResult) -> Vec<Vec<serde_json::Value>> {
    r.rows.clone()
}

/// ── Property 1: a view add/alter/drop is reflected by the NEXT query ──────────
#[test]
fn view_add_alter_drop_is_reflected_not_served_stale() {
    let core = graph();
    let (store, _p) = TableStore::open_temp().unwrap();
    let cache = SqlContextCache::new();
    let mut seq = 0u64;

    commit(
        &store,
        TENANT,
        GRAPH,
        &mut seq,
        create_view("agents_view", "SELECT id FROM nodes WHERE type = 'Agent'"),
    );

    let v = core.version();
    let snap = core.analysis_snapshot();
    let r1 = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v,
        v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        "SELECT id FROM agents_view ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(rows(&r1), vec![vec![json!("n1")], vec![json!("n2")]]);
    let (hits0, misses0) = cache.stats();
    assert_eq!((hits0, misses0), (0, 1), "first call is a cold miss");

    // Same epoch, same query again: a genuine HIT, same (stale-view) rows —
    // proves the cache is actually being consulted before we change anything.
    let r_hit = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v,
        v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        "SELECT id FROM agents_view ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(rows(&r_hit), rows(&r1));
    let (hits1, misses1) = cache.stats();
    assert_eq!((hits1, misses1), (1, 1), "second identical call HITS");

    // DROP + re-CREATE the SAME view name with a DIFFERENT body (Tool instead of
    // Agent). The graph/version/caller are UNCHANGED — only the catalog moved.
    commit(&store, TENANT, GRAPH, &mut seq, drop_view("agents_view"));
    commit(
        &store,
        TENANT,
        GRAPH,
        &mut seq,
        create_view("agents_view", "SELECT id FROM nodes WHERE type = 'Tool'"),
    );

    let r2 = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v,
        v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        "SELECT id FROM agents_view ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r2),
        vec![vec![json!("n3")]],
        "the view's NEW body must be visible immediately — never the stale cached plan"
    );
    let (hits2, misses2) = cache.stats();
    assert_eq!(
        misses2, 2,
        "the catalog change must MISS (rebuild), not silently reuse the old view plan"
    );
    assert_eq!(hits2, hits1, "the miss must not also count as a hit");
}

/// ── Property 2: different epochs never share a context ────────────────────────
/// Two axes, in one test: (a) the SAME caller at two different graph versions, and
/// (b) two DIFFERENT callers (simulating two RLS-filtered snapshots) at the
/// IDENTICAL graph version + catalog state. Both must produce independent entries
/// — critically, (b) is a SECURITY property: caller B's context must never be
/// built from (or served to) caller A's filtered snapshot.
#[test]
fn different_epochs_never_share_a_cached_context() {
    let core = graph();
    let (store, _p) = TableStore::open_temp().unwrap();
    let cache = SqlContextCache::new();

    // (a) graph-version axis, same caller.
    let v0 = core.version();
    let snap0 = core.analysis_snapshot();
    let r_v0 = exec_sql_typed_with_tables_cached_cancellable(
        &snap0,
        v0,
        v0, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        "SELECT id FROM nodes WHERE type = 'Agent' ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(rows(&r_v0), vec![vec![json!("n1")], vec![json!("n2")]]);

    core.add_node(
        "n4".to_string(),
        rmp_serde::to_vec_named(&json!({"type":"Agent","rank":4})).unwrap(),
    );
    core.mark_dirty();
    let v1 = core.version();
    assert!(v1 > v0);
    let snap1 = core.analysis_snapshot();
    let r_v1 = exec_sql_typed_with_tables_cached_cancellable(
        &snap1,
        v1,
        v1, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        "SELECT id FROM nodes WHERE type = 'Agent' ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r_v1),
        vec![vec![json!("n1")], vec![json!("n2")], vec![json!("n4")]],
        "the new version must MISS and see the fresh node"
    );

    // v0 is STILL independently cached/queryable and unaffected by v1 ever running.
    let r_v0_again = exec_sql_typed_with_tables_cached_cancellable(
        &snap0,
        v0,
        v0, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        "SELECT id FROM nodes WHERE type = 'Agent' ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r_v0_again),
        rows(&r_v0),
        "v0's own entry must be untouched by v1 ever having run"
    );

    // (b) caller-identity axis: two DIFFERENT callers' PRE-FILTERED views at the
    // EXACT SAME (tenant, graph, graph_version, catalog_fingerprint) — the ONLY
    // difference is `caller`. This is the realistic shape: `IsolationLayer::
    // filter_view` prunes a VIEW derived from the SAME underlying graph/version (a
    // real RLS-invisible node is REMOVED from the view, the graph's own OCC version
    // is untouched by that removal) — never a different graph_version per caller.
    // Holding graph_version/tenant/graph/catalog_fingerprint IDENTICAL and varying
    // ONLY `caller` isolates exactly the axis this cache's `caller` field exists to
    // cover — see `SqlContextEpoch`'s own doc.
    let shared_v = 999u64;
    let view_alice = view_with_only(&["n1"]);
    let view_bob = view_with_only(&["n1", "secret_row"]);

    let r_alice = exec_sql_typed_with_tables_cached_cancellable(
        &view_alice,
        shared_v,
        shared_v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "alice",
        &store,
        &cache,
        "SELECT id FROM nodes ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(rows(&r_alice), vec![vec![json!("n1")]]);

    let r_bob = exec_sql_typed_with_tables_cached_cancellable(
        &view_bob,
        shared_v,
        shared_v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "bob",
        &store,
        &cache,
        "SELECT id FROM nodes ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r_bob),
        vec![vec![json!("n1")], vec![json!("secret_row")]],
        "bob's own filtered view must be served in full, not alice's cached (narrower) one"
    );

    // Re-querying alice's exact epoch again must NOT have picked up bob's row —
    // proves bob's cache insert never overwrote or leaked into alice's entry, even
    // though every OTHER component of the key (tenant/graph/graph_version/
    // catalog_fingerprint) was byte-identical between the two calls.
    let r_alice_again = exec_sql_typed_with_tables_cached_cancellable(
        &view_alice,
        shared_v,
        shared_v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "alice",
        &store,
        &cache,
        "SELECT id FROM nodes ORDER BY id",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r_alice_again),
        vec![vec![json!("n1")]],
        "alice must never see bob's row through a shared cache entry"
    );
}

/// Companion to `different_epochs_never_share_a_cached_context`'s caller-identity
/// axis (P10/W1.7-A/C), but for EDGES specifically: `EdgesTableProvider` (the new
/// src/dst-adjacency pushdown provider `build_ctx` now constructs per epoch) has
/// its OWN per-instance memoization (`full`) and its OWN traversal logic — this is
/// the negative test proving that machinery never lets one caller's edges leak
/// into a different caller's result through the cache, exactly like `nodes`
/// already doesn't. Two query shapes, both at the SAME (tenant, graph,
/// graph_version) epoch: an unfiltered `SELECT` (exercises `full_batch`) and a
/// `src`-pushed one (exercises `scan_by_src`).
#[test]
fn edges_never_leak_across_callers_through_the_cache() {
    let (store, _p) = TableStore::open_temp().unwrap();
    let cache = SqlContextCache::new();
    let shared_v = 424_242u64;

    // Alice sees n1, n2 and the n1->n2 edge only. Bob ADDITIONALLY sees a
    // `secret` node and the n2->secret edge — the edge a real
    // `IsolationLayer::filter_view` would have hidden from Alice (its endpoint is
    // a node she cannot see).
    let view_alice = view_with_edges(&["n1", "n2"], &[("n1", "n2")]);
    let view_bob = view_with_edges(&["n1", "n2", "secret"], &[("n1", "n2"), ("n2", "secret")]);

    // ── unfiltered SELECT (exercises `EdgesTableProvider::full_batch`) ──
    let r_alice = exec_sql_typed_with_tables_cached_cancellable(
        &view_alice,
        shared_v,
        shared_v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "edge-alice",
        &store,
        &cache,
        "SELECT src, dst FROM edges ORDER BY src, dst",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(rows(&r_alice), vec![vec![json!("n1"), json!("n2")]]);

    let r_bob = exec_sql_typed_with_tables_cached_cancellable(
        &view_bob,
        shared_v,
        shared_v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "edge-bob",
        &store,
        &cache,
        "SELECT src, dst FROM edges ORDER BY src, dst",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r_bob),
        vec![
            vec![json!("n1"), json!("n2")],
            vec![json!("n2"), json!("secret")]
        ],
        "bob's own filtered edge set must be served in full, not alice's cached (narrower) one"
    );

    let r_alice_again = exec_sql_typed_with_tables_cached_cancellable(
        &view_alice,
        shared_v,
        shared_v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "edge-alice",
        &store,
        &cache,
        "SELECT src, dst FROM edges ORDER BY src, dst",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r_alice_again),
        vec![vec![json!("n1"), json!("n2")]],
        "alice must never see bob's edge through a shared cache entry"
    );

    // ── src-pushed SELECT (exercises `EdgesTableProvider::scan_by_src`) ──
    let r_bob_src = exec_sql_typed_with_tables_cached_cancellable(
        &view_bob,
        shared_v,
        shared_v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "edge-bob",
        &store,
        &cache,
        "SELECT dst FROM edges WHERE src = 'n2'",
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(rows(&r_bob_src), vec![vec![json!("secret")]]);

    let r_alice_src = exec_sql_typed_with_tables_cached_cancellable(
        &view_alice,
        shared_v,
        shared_v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "edge-alice",
        &store,
        &cache,
        "SELECT dst FROM edges WHERE src = 'n2'",
        &CancellationToken::new(),
    )
    .unwrap();
    assert!(
        rows(&r_alice_src).is_empty(),
        "alice's src='n2' pushdown must not see bob's secret edge: {:?}",
        rows(&r_alice_src)
    );
}

/// Build a `GraphView` containing exactly the given node ids (each an untyped,
/// empty-property node — id is all these tests need), hand-assembled the SAME way
/// `IsolationLayer::filter_view` prunes one (drop the hidden node from `graph` +
/// `node_map` + `node_properties`) — mirrors the facade crate's own
/// `rls_no_exfiltrate_tests::seeded_view` fixture technique. Stands in for a
/// caller-specific `filter_view` RESULT without needing the `security` feature
/// (unavailable to this crate — RLS filtering itself is a facade-crate concern;
/// see this file's module doc) or a real `IsolationLayer`.
fn view_with_only(ids: &[&str]) -> eg_core::graph::GraphView {
    let mut v = eg_core::graph::GraphView::default();
    for id in ids {
        let idx = v.graph.add_node((*id).to_string());
        v.node_map.insert((*id).to_string(), idx);
        v.node_properties.insert(
            (*id).to_string(),
            std::sync::Arc::new(rmp_serde::to_vec_named(&json!({})).unwrap()),
        );
    }
    v
}

/// Like [`view_with_only`] but also wires directed edges between visible nodes —
/// stands in for a caller-specific filtered view that differs on the EDGES a
/// caller sees, not just the nodes (`IsolationLayer::filter_view` drops any edge
/// incident to a hidden node too, so a real filtered view never has an edge to a
/// node absent from `ids`).
fn view_with_edges(ids: &[&str], edges: &[(&str, &str)]) -> eg_core::graph::GraphView {
    let mut v = view_with_only(ids);
    for (src, dst) in edges {
        let src_idx = v.node_map[*src];
        let dst_idx = v.node_map[*dst];
        v.graph.add_edge(src_idx, dst_idx, format!("{src}:{dst}"));
    }
    v
}

/// ── Property 3: a repeated query at the SAME epoch is a genuine HIT and matches
/// the uncached path byte-for-byte ──────────────────────────────────────────────
#[test]
fn same_epoch_repeat_is_a_real_hit_and_matches_uncached() {
    let core = graph();
    let (store, _p) = TableStore::open_temp().unwrap();
    let cache = SqlContextCache::new();
    let sql = "SELECT id, rank FROM nodes ORDER BY id";

    let uncached = exec_sql_typed_with_tables_cancellable(
        &core.analysis_snapshot(),
        &store,
        sql,
        &CancellationToken::new(),
    )
    .unwrap();

    let v = core.version();
    let snap = core.analysis_snapshot();
    let c1 = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v,
        v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        sql,
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        c1, uncached,
        "first cached call must match the uncached path"
    );
    assert_eq!(cache.stats(), (0, 1));

    let c2 = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v,
        v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        sql,
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        c2, uncached,
        "the HIT must ALSO match the uncached path exactly"
    );
    assert_eq!(
        cache.stats(),
        (1, 1),
        "second call must be a HIT, not a second MISS"
    );
}

/// ── Property 4: a battery of differently-shaped queries all match the uncached
/// path, cached or not ──────────────────────────────────────────────────────────
#[test]
fn battery_of_queries_matches_uncached_cached_or_not() {
    let core = graph();
    let (store, _p) = TableStore::open_temp().unwrap();
    let cache = SqlContextCache::new();
    let mut seq = 0u64;
    commit(
        &store,
        TENANT,
        GRAPH,
        &mut seq,
        create_view(
            "agents_view",
            "SELECT id, rank FROM nodes WHERE type = 'Agent'",
        ),
    );

    let queries = [
        "SELECT id FROM nodes ORDER BY id",
        "SELECT id FROM nodes WHERE type = 'Agent' ORDER BY id",
        "SELECT type, COUNT(*) AS n FROM nodes GROUP BY type ORDER BY type",
        "SELECT n.id AS src, e.dst AS dst FROM nodes n JOIN edges e ON n.id = e.src ORDER BY src",
        "SELECT id FROM agents_view ORDER BY id",
        "SELECT id FROM nodes ORDER BY rank DESC LIMIT 2",
    ];

    let v = core.version();
    let snap = core.analysis_snapshot();
    for sql in queries {
        let uncached =
            exec_sql_typed_with_tables_cancellable(&snap, &store, sql, &CancellationToken::new())
                .unwrap_or_else(|e| panic!("uncached `{sql}` failed: {e}"));
        // Two cached calls per query: first a MISS-then-build, second a HIT.
        for _ in 0..2 {
            let cached = exec_sql_typed_with_tables_cached_cancellable(
                &snap,
                v,
                v, // node_epoch (W1.6/P7 site 3): same as graph_version here
                TENANT,
                GRAPH,
                "caller-x",
                &store,
                &cache,
                sql,
                &CancellationToken::new(),
            )
            .unwrap_or_else(|e| panic!("cached `{sql}` failed: {e}"));
            assert_eq!(cached, uncached, "cached vs uncached mismatch for `{sql}`");
        }
    }
}

/// ── Property 5: a commit under a DIFFERENT `MUTATION_VERSION` scope string than
/// the literal `(tenant, graph)` being queried still invalidates the cache — the
/// exact shape the sqlite-import gateway uses (a fixed cross-graph scope). Proves
/// the epoch is NOT naively tied to `mutation_version(tenant, graph)` alone (see
/// `TableStore::catalog_fingerprint`'s own doc for the full mechanism). ──────────
#[test]
fn a_commit_under_a_different_scope_string_still_invalidates() {
    let core = graph();
    let (store, _p) = TableStore::open_temp().unwrap();
    let cache = SqlContextCache::new();
    let mut seq = 0u64;

    let v = core.version();
    let snap = core.analysis_snapshot();
    let sql = "SELECT id FROM other_table ORDER BY id";

    // "other_table" does not exist yet -- the query errors either way, but the
    // point is what happens to catalog_fingerprint (and hence the epoch) once it's
    // created under a DIFFERENT scope than "GRAPH".
    assert!(exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v,
        v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        sql,
        &CancellationToken::new(),
    )
    .is_err());
    let (_, misses_before) = cache.stats();

    // Commit under a scope OTHER than the literal graph being queried -- the exact
    // shape `compile_import_batch`'s `authority.namespace("sqlite-import",
    // "global-user-tables")` produces: some fixed string, never the calling graph.
    let mut txn = TableTxn::new();
    txn.push(TxnOp::CreateTable {
        schema: TableSchema::new(
            "other_table",
            vec![Column::new("id", ColumnType::Text, false, false)],
        ),
        if_not_exists: false,
    });
    commit(
        &store,
        TENANT,
        "sqlite-import:global-user-tables",
        &mut seq,
        txn,
    );

    let r = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v,
        v, // node_epoch (W1.6/P7 site 3): same as graph_version here
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        sql,
        &CancellationToken::new(),
    )
    .unwrap_or_else(|e| {
        panic!("`other_table` must be visible after the cross-scope commit, got error: {e}")
    });
    assert!(
        rows(&r).is_empty(),
        "the table now exists (created empty) -- must not still error \"table not found\""
    );
    let (_, misses_after) = cache.stats();
    assert!(
        misses_after > misses_before,
        "the cross-scope commit must have invalidated the cached (nonexistent-table-error) \
         context, not been silently absorbed"
    );
}

/// ── W1.6/P7 site 3: the O(V) `nodes` Arrow batch is REUSED across a write that did not
/// touch nodes (a pure-edge / catalog-only write bumps `graph_version` but not `node_epoch`),
/// and the reused result is byte-identical to the uncached path (the differential). ──
#[test]
fn node_batch_reused_across_non_node_write_and_matches_uncached() {
    let core = graph();
    let (store, _p) = TableStore::open_temp().unwrap();
    let cache = SqlContextCache::new();
    let snap = core.analysis_snapshot();
    let sql = "SELECT id FROM nodes ORDER BY id";

    // Ground truth: the byte-identical UNCACHED path.
    let truth =
        exec_sql_typed_with_tables_cancellable(&snap, &store, sql, &CancellationToken::new())
            .unwrap();

    let v0 = core.version();
    // Cold: builds the context AND infers the node batch once.
    let r0 = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v0,
        v0,
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        sql,
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(rows(&r0), rows(&truth), "cached == uncached (cold)");
    assert_eq!(
        cache.node_stats(),
        (0, 1),
        "cold miss builds the node batch once"
    );

    // A PURE-EDGE / catalog write: graph_version advances, node_epoch is UNCHANGED (v0).
    let v1 = v0 + 1;
    let r1 = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v1,
        v0,
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        sql,
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r1),
        rows(&truth),
        "cached == uncached after a non-node write (reused batch)"
    );
    assert_eq!(
        cache.node_stats(),
        (1, 1),
        "a write that did not touch nodes REUSES the O(V) node batch instead of re-scanning it"
    );
    let (_hits, ctx_misses) = cache.stats();
    assert_eq!(
        ctx_misses, 2,
        "the context itself is still correctly rebuilt on the new version"
    );

    // A write that DOES touch nodes advances node_epoch → the node batch is re-inferred.
    let v2 = v1 + 1;
    let r2 = exec_sql_typed_with_tables_cached_cancellable(
        &snap,
        v2,
        v2,
        TENANT,
        GRAPH,
        "caller-x",
        &store,
        &cache,
        sql,
        &CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        rows(&r2),
        rows(&truth),
        "cached == uncached after a node write"
    );
    assert_eq!(
        cache.node_stats(),
        (1, 2),
        "a node write re-infers the node batch"
    );
}
