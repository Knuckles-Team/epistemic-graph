//! Embedded API round-trip + durable-reopen tests (CONCEPT:KG-2.216).

use super::*;
use std::path::PathBuf;

fn props(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

fn tmp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "eg-embedded-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// Open → write nodes/edges + a vector → read them back → run an algorithm + a
/// semantic search; durability is on (a persist dir + default options).
#[test]
fn embedded_roundtrip_nodes_edges_vector_algo_search() {
    let dir = tmp_dir("roundtrip");
    let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
    assert!(eng.is_durable(), "a persist dir + durable ⇒ durable engine");

    eng.create_graph("g", GraphType::Global).unwrap();
    eng.add_node("g", "a", props(serde_json::json!({"label": "A"})))
        .unwrap();
    eng.add_node("g", "b", props(serde_json::json!({"label": "B"})))
        .unwrap();
    eng.add_node("g", "c", props(serde_json::json!({"label": "C"})))
        .unwrap();
    eng.add_edge("g", "a", "b", props(serde_json::json!({"w": 1})))
        .unwrap();
    eng.add_edge("g", "b", "c", props(serde_json::json!({"w": 1})))
        .unwrap();

    // Read back.
    assert!(eng.has_node("g", "a").unwrap());
    assert_eq!(eng.node_count("g").unwrap(), 3);
    let pa = eng.get_node_properties("g", "a").unwrap().unwrap();
    let va: serde_json::Value = rmp_serde::from_slice(&pa).unwrap();
    assert_eq!(va["label"], "A");

    // Algorithm: a→b→c is a DAG, topological sort puts a before c.
    let order = eng.topological_sort("g").unwrap();
    let pos = |id: &str| order.iter().position(|n| n == id).unwrap();
    assert!(pos("a") < pos("b") && pos("b") < pos("c"));

    // Semantic search.
    eng.add_embedding("g", "a", vec![1.0, 0.0, 0.0]).unwrap();
    eng.add_embedding("g", "b", vec![0.0, 1.0, 0.0]).unwrap();
    eng.add_embedding("g", "c", vec![0.9, 0.1, 0.0]).unwrap();
    let hits = eng.semantic_search("g", &[1.0, 0.0, 0.0], 2).unwrap();
    assert_eq!(hits.len(), 2);
    // The query is closest to "a" (identical) then "c" (near).
    assert_eq!(hits[0].0, "a");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Durability: write, close, REOPEN — the data is recovered from redb (an embedded
/// write is durable; commit-before-return survives a fresh process/handle).
#[test]
fn embedded_durable_close_and_reopen() {
    let dir = tmp_dir("durable");

    {
        let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
        eng.create_graph("kg", GraphType::Agent).unwrap();
        eng.add_node("kg", "n1", props(serde_json::json!({"v": 42})))
            .unwrap();
        eng.add_node("kg", "n2", props(serde_json::json!({"v": 7})))
            .unwrap();
        eng.add_edge("kg", "n1", "n2", props(serde_json::json!({"rel": "links"})))
            .unwrap();
        eng.add_embedding("kg", "n1", vec![0.1, 0.2, 0.3]).unwrap();
        // Checkpoint persists the semantic vectors too (per-mutation rows already
        // persisted the nodes/edges before each call returned).
        let n = eng.close().unwrap();
        assert!(n >= 1, "at least the kg graph checkpointed");
    }

    // Reopen a fresh handle (simulates a process restart).
    let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
    let names: Vec<String> = eng.list_graphs().into_iter().map(|(n, _)| n).collect();
    assert!(
        names.contains(&"kg".to_string()),
        "graph recovered: {names:?}"
    );

    assert_eq!(eng.node_count("kg").unwrap(), 2);
    let p = eng.get_node_properties("kg", "n1").unwrap().unwrap();
    let v: serde_json::Value = rmp_serde::from_slice(&p).unwrap();
    assert_eq!(v["v"], 42);

    // The edge survived too (an algorithm sees the topology).
    let comps = eng.connected_components("kg").unwrap();
    assert_eq!(comps.len(), 1, "n1 and n2 are connected: {comps:?}");

    // The semantic vector survived the checkpoint.
    let hits = eng.semantic_search("kg", &[0.1, 0.2, 0.3], 1).unwrap();
    assert_eq!(hits.first().map(|h| h.0.as_str()), Some("n1"));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Durability WITHOUT an explicit checkpoint: a per-mutation commit-before-return
/// must survive a reopen on its own (no `close`/`checkpoint` called).
#[test]
fn embedded_per_mutation_durable_without_checkpoint() {
    let dir = tmp_dir("nocp");
    {
        let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
        eng.add_node("__commons__", "x", props(serde_json::json!({"k": "v"})))
            .unwrap();
        // intentionally NO close()/checkpoint() — drop the handle.
    }
    let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
    let p = eng.get_node_properties("__commons__", "x").unwrap();
    assert!(
        p.is_some(),
        "per-mutation commit survived a reopen with no checkpoint"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Tenant DELETE durably PURGES the graph's redb rows (CONCEPT:KG-2.221/2.216) so a
/// recreate of the SAME name after a reopen starts from a clean durable slate — the
/// embedded analogue of the server's `delete_then_recreate_same_name_keeps_new_writes`.
#[test]
fn embedded_delete_purges_durable_rows_across_reopen() {
    let dir = tmp_dir("delete-purge");
    {
        let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
        eng.create_graph("g", GraphType::Global).unwrap();
        eng.add_node("g", "n1", props(serde_json::json!({"v": 1})))
            .unwrap();
        eng.add_node("g", "stale", props(serde_json::json!({"v": 9})))
            .unwrap();
        // Delete the tenant — this must purge its durable rows.
        eng.delete_graph("g").unwrap();
        // Recreate the SAME name; write ONLY n1={v:2} (never "stale").
        eng.create_graph("g", GraphType::Global).unwrap();
        eng.add_node("g", "n1", props(serde_json::json!({"v": 2})))
            .unwrap();
    }

    // Reopen (load_all): the deleted incarnation's "stale" node must NOT resurrect,
    // and n1 must be the NEW write {v:2}, not the stale {v:1}.
    let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
    assert!(
        !eng.has_node("g", "stale").unwrap(),
        "deleted tenant's 'stale' node resurrected from redb on reopen after recreate"
    );
    let p = eng.get_node_properties("g", "n1").unwrap().unwrap();
    let v: serde_json::Value = rmp_serde::from_slice(&p).unwrap();
    assert_eq!(
        v["v"], 2,
        "recreated tenant's n1 must read back as the NEW write {{v:2}}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// In-memory mode: no persist dir ⇒ not durable, but every op still works.
#[test]
fn embedded_in_memory_no_persist() {
    let eng = EmbeddedEngine::open(None::<&std::path::Path>, EmbeddedOptions::durable()).unwrap();
    assert!(!eng.is_durable());
    eng.add_node("g", "a", props(serde_json::json!({})))
        .unwrap();
    assert_eq!(eng.node_count("g").unwrap(), 1);
    assert_eq!(
        eng.checkpoint().unwrap(),
        0,
        "no durable store ⇒ checkpoint no-ops"
    );
}

/// Gated SQL/Cypher over the embedded snapshot, when those features are on.
#[cfg(feature = "cypher")]
#[test]
fn embedded_cypher_query() {
    let eng = EmbeddedEngine::open(None::<&std::path::Path>, EmbeddedOptions::in_memory()).unwrap();
    eng.add_node("g", "a", props(serde_json::json!({"name": "Ada"})))
        .unwrap();
    eng.add_node("g", "b", props(serde_json::json!({"name": "Bob"})))
        .unwrap();
    let res = eng.cypher("g", "MATCH (n) RETURN n").unwrap();
    assert!(res.rows.len() >= 2, "cypher returned the nodes: {res:?}");
}

#[cfg(feature = "query")]
#[test]
fn embedded_sql_query() {
    let eng = EmbeddedEngine::open(None::<&std::path::Path>, EmbeddedOptions::in_memory()).unwrap();
    eng.add_node("g", "a", props(serde_json::json!({"name": "Ada"})))
        .unwrap();
    let res = eng.sql("g", "SELECT id FROM nodes").unwrap();
    assert!(!res.rows.is_empty(), "sql returned rows: {res:?}");
}

/// SQLite-equivalent (CONCEPT:EG-022 / EG-018): open a file → CREATE TABLE → INSERT →
/// SELECT, in-process, no server — and the durable user-table rows survive a reopen of
/// the SAME single-file store.
#[cfg(feature = "query")]
#[test]
fn embedded_sqlite_equivalent_create_insert_select() {
    let dir = tmp_dir("sqlite");
    {
        let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
        eng.sql_exec("kg", "CREATE TABLE prices (sym TEXT, px DOUBLE)")
            .unwrap();
        // INSERT … VALUES durably appends two rows.
        let ins = eng
            .sql_exec(
                "kg",
                "INSERT INTO prices (sym, px) VALUES ('AAPL', 1.5), ('MSFT', 2.5)",
            )
            .unwrap();
        assert_eq!(ins.rows[0][0], serde_json::json!(2), "two rows inserted");
        // SELECT them back, ordered.
        let sel = eng
            .sql_exec("kg", "SELECT sym, px FROM prices ORDER BY sym")
            .unwrap();
        let cols: Vec<&str> = sel.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(cols, vec!["sym", "px"]);
        assert_eq!(sel.rows.len(), 2);
        assert_eq!(sel.rows[0][0], serde_json::json!("AAPL"));
    }
    // Reopen the SAME dir: the durable user-table rows persist (single-file store).
    let eng = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable()).unwrap();
    let sel = eng
        .sql_exec("kg", "SELECT sym FROM prices ORDER BY sym")
        .unwrap();
    assert_eq!(
        sel.rows.len(),
        2,
        "durable user-table rows survive reopen: {:?}",
        sel.rows
    );
    let _ = std::fs::remove_dir_all(&dir);
}
