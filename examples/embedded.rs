//! SQLite/DuckDB-style embedded usage of the epistemic-graph engine
//! (CONCEPT:KG-2.216). No socket, no server, no auth — the engine is a durable
//! library you open in-process.
//!
//! Run with:
//!   cargo run --example embedded --no-default-features --features "embedded redb"
//!   # with the lean Cypher query surface (the Pi tier):
//!   cargo run --example embedded --no-default-features --features "embedded pi"

use epistemic_graph::embedded::{EmbeddedEngine, EmbeddedOptions};
use epistemic_graph::protocol::GraphType;

fn props(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

fn main() -> Result<(), String> {
    // A scratch persist dir — like a SQLite file, but a directory holding graph.redb.
    let dir = std::env::temp_dir().join("eg-embedded-example");
    let _ = std::fs::remove_dir_all(&dir);

    // ── Open a durable embedded engine (commit-before-return) ────────────
    let engine = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable())?;
    println!("opened embedded engine (durable = {})", engine.is_durable());

    // ── Write a tiny knowledge graph ─────────────────────────────────────
    engine.create_graph("kg", GraphType::Global)?;
    engine.add_node(
        "kg",
        "ada",
        props(serde_json::json!({"label": "Person", "name": "Ada"})),
    )?;
    engine.add_node(
        "kg",
        "eng",
        props(serde_json::json!({"label": "Engine", "name": "epistemic-graph"})),
    )?;
    engine.add_edge(
        "kg",
        "ada",
        "eng",
        props(serde_json::json!({"rel": "builds"})),
    )?;

    // Vector for semantic search.
    engine.add_embedding("kg", "ada", vec![1.0, 0.0, 0.0])?;
    engine.add_embedding("kg", "eng", vec![0.0, 1.0, 0.0])?;

    // ── Read back ────────────────────────────────────────────────────────
    println!("nodes in kg: {}", engine.node_count("kg")?);
    let ada = engine.get_node_properties("kg", "ada")?.unwrap();
    let ada: serde_json::Value = rmp_serde::from_slice(&ada).unwrap();
    println!("ada = {ada}");

    // ── Algorithm + semantic search over the same in-process core ────────
    println!("components: {:?}", engine.connected_components("kg")?);
    let hits = engine.semantic_search("kg", &[1.0, 0.0, 0.0], 1)?;
    println!("nearest to [1,0,0]: {hits:?}");

    // ── Gated Cypher query (when built with the `cypher`/`pi` feature) ────
    #[cfg(feature = "cypher")]
    {
        let res = engine.cypher("kg", "MATCH (n) RETURN n")?;
        println!("cypher MATCH (n) returned {} row(s)", res.rows.len());
    }

    // ── SQLite-equivalent SQL user tables (when built with `query`) ──────
    // No server, no socket: CREATE TABLE / INSERT / SELECT in-process, durably,
    // against a single-file user-table store (CONCEPT:EG-022 / EG-018).
    #[cfg(feature = "query")]
    {
        engine.sql_exec("kg", "CREATE TABLE prices (sym TEXT, px DOUBLE)")?;
        engine.sql_exec(
            "kg",
            "INSERT INTO prices (sym, px) VALUES ('AAPL', 1.5), ('MSFT', 2.5)",
        )?;
        let sel = engine.sql_exec("kg", "SELECT sym, px FROM prices ORDER BY sym")?;
        println!(
            "SQL SELECT returned {} row(s): {:?}",
            sel.rows.len(),
            sel.rows
        );
    }

    // ── Durably checkpoint + close, then reopen (proves durability) ──────
    let n = engine.close()?;
    println!("checkpointed {n} graph(s); reopening…");

    let reopened = EmbeddedEngine::open(Some(&dir), EmbeddedOptions::durable())?;
    println!(
        "after reopen, kg has {} node(s) — durable!",
        reopened.node_count("kg")?
    );
    assert_eq!(reopened.node_count("kg")?, 2);

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
