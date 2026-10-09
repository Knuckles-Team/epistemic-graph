use super::*;
use eg_core::graph::GraphCore;
use serde_json::json;

fn provider() -> NodesTableProvider {
    let core = GraphCore::new();
    for (id, team, rank) in [
        ("a", "blue", 1),
        ("b", "red", 2),
        ("c", "blue", 2),
        ("d", "blue", 3),
    ] {
        core.add_node(
            id.into(),
            rmp_serde::to_vec_named(&json!({"team": team, "rank": rank})).unwrap(),
        );
    }
    let snap = core.analysis_snapshot();
    let (schema, batch) = infer_nodes(&snap).unwrap();
    NodesTableProvider::new(schema, batch)
}

/// The index lookup returns exactly the row positions whose column value matches
/// — the proof the pushdown path resolves through the index, not a scan.
#[test]
fn lookup_returns_matching_row_positions() {
    let p = provider();
    let reg = &p.registry;
    // `team` is an indexable Utf8 column; resolve "blue" -> the 3 blue rows.
    let rows = reg.lookup("team", &"blue".to_string()).unwrap();
    assert_eq!(rows.len(), 3, "three nodes are blue");
    // Each returned position's `team` cell must actually be "blue".
    let (idx, _) = reg.schema.column_with_name("team").unwrap();
    let col = reg.batch.column(idx);
    for r in rows {
        assert_eq!(
            PushdownRegistry::cell_key(col.as_ref(), r as usize),
            Some("blue".to_string())
        );
    }
    // A value with no rows -> empty, but Some (column IS indexed).
    assert_eq!(reg.lookup("team", &"green".to_string()), Some(Vec::new()));
}

/// `supports_filters_pushdown`: equality on an indexable column is `Inexact`
/// (pushed + re-checked), everything else `Unsupported` (kept as a Filter).
#[test]
fn supports_filters_classifies_predicates() {
    use datafusion::logical_expr::{col, lit};
    let p = provider();
    let eq = col("team").eq(lit("blue"));
    let like = col("team").like(lit("b%"));
    let gt = col("rank").gt(lit(2_i64));
    let refs: Vec<&Expr> = vec![&eq, &like, &gt];
    let got = p.supports_filters_pushdown(&refs).unwrap();
    assert_eq!(got[0], TableProviderFilterPushDown::Inexact);
    assert_eq!(got[1], TableProviderFilterPushDown::Unsupported);
    assert_eq!(got[2], TableProviderFilterPushDown::Unsupported);
}

/// CONCEPT:EG-KG.query.register-each-user-table: a USER table (not the graph `nodes`) registered through the
/// SAME `NodesTableProvider` pushdown path — a `WHERE col = literal` resolves
/// through the secondary index, and `supports_filters_pushdown` reports the
/// equality `Inexact` (pushed + re-checked). This is the proof the user-table
/// scan uses the index, mirroring the `nodes` provider.
#[test]
fn user_table_pushdown_uses_index() {
    use crate::tables::provider::materialize;
    use crate::tables::schema::{Cell, Column, ColumnType, TableSchema};
    use datafusion::logical_expr::{col, lit};

    let schema = TableSchema::new(
        "prices",
        vec![
            Column::new("symbol", ColumnType::Text, false, false),
            Column::new("px", ColumnType::Double, true, false),
        ],
    );
    let rows = vec![
        vec![Cell::Text("AAPL".into()), Cell::Float(1.0)],
        vec![Cell::Text("MSFT".into()), Cell::Float(2.0)],
        vec![Cell::Text("AAPL".into()), Cell::Float(3.0)],
    ];
    let (arrow_schema, batch) = materialize(&schema, &rows).unwrap();
    let p = NodesTableProvider::new(arrow_schema, batch);

    // The index resolves `symbol = 'AAPL'` to exactly the two matching rows.
    let hits = p.registry.lookup("symbol", &"AAPL".to_string()).unwrap();
    assert_eq!(hits.len(), 2, "two AAPL rows resolved via the index");

    // The equality is pushed (Inexact); a non-equality stays a Filter.
    let eq = col("symbol").eq(lit("AAPL"));
    let gt = col("px").gt(lit(1.5_f64));
    let got = p.supports_filters_pushdown(&[&eq, &gt]).unwrap();
    assert_eq!(got[0], TableProviderFilterPushDown::Inexact);
    assert_eq!(got[1], TableProviderFilterPushDown::Unsupported);
}

/// The bounded cap: with cap=1, a second distinct column overflows and `lookup`
/// returns `None` (caller serves the full batch — still correct via `Inexact`).
#[test]
fn bounded_cap_overflows_to_none() {
    std::env::set_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES", "1");
    std::env::remove_var("EPISTEMIC_GRAPH_INDEXED_PROPERTIES");
    let p = provider();
    let reg = &p.registry;
    assert!(reg.lookup("team", &"blue".to_string()).is_some());
    assert!(
        reg.lookup("rank", &"2".to_string()).is_none(),
        "cap=1 must refuse a second column"
    );
    std::env::remove_var("EPISTEMIC_GRAPH_MAX_INDEXED_PROPERTIES");
}

/// Regression: a node whose OWN JSON properties carry a key literally named
/// `id` (or `props`) must NOT produce a second Arrow `Field` of the same name —
/// `infer_nodes`'s schema must stay unique, and the batch's column count must
/// stay 1:1 aligned with it. Before the `is_reserved_column` guard, this
/// scenario made `Schema::new` emit two `id` fields, which DataFusion rejects
/// with "Schema contains duplicate qualified field name nodes.id" on EVERY
/// query over the table (even `SELECT COUNT(*)`, which touches no columns).
#[test]
fn duplicate_reserved_property_does_not_duplicate_schema_field() {
    let core = GraphCore::new();
    core.add_node(
        "a".into(),
        rmp_serde::to_vec_named(&json!({"id": "a", "label": "Server"})).unwrap(),
    );
    core.add_node(
        "b".into(),
        rmp_serde::to_vec_named(&json!({"id": "b", "label": "Server", "props": "x"})).unwrap(),
    );
    let snap = core.analysis_snapshot();
    let (schema, batch) = infer_nodes(&snap).unwrap();

    // Exactly one `id` field and one `props` field.
    let id_count = schema.fields().iter().filter(|f| f.name() == "id").count();
    let props_count = schema
        .fields()
        .iter()
        .filter(|f| f.name() == "props")
        .count();
    assert_eq!(id_count, 1, "schema must carry exactly one `id` field");
    assert_eq!(
        props_count, 1,
        "schema must carry exactly one `props` field"
    );
    // Batch column count matches schema field count (build_batch stayed in sync).
    assert_eq!(batch.num_columns(), schema.fields().len());
    // `label` (a non-reserved property) still made it into the schema.
    assert!(schema.field_with_name("label").is_ok());
}

/// End-to-end: the exact failure mode from the bug report. Register a graph
/// with a node carrying an `id` property (colliding with the reserved column)
/// and run real SQL through `exec_sql` — both `SELECT COUNT(*)` and
/// `SELECT id FROM nodes LIMIT 1` must succeed with no "duplicate qualified
/// field name" schema error.
#[test]
fn count_and_select_id_succeed_with_colliding_id_property() {
    use crate::sql::exec::exec_sql;

    let core = GraphCore::new();
    core.add_node(
        "srv-1".into(),
        rmp_serde::to_vec_named(&json!({"id": "srv-1", "label": "Server"})).unwrap(),
    );
    core.add_node(
        "srv-2".into(),
        rmp_serde::to_vec_named(&json!({"id": "srv-2", "label": "Server"})).unwrap(),
    );
    let snap = core.analysis_snapshot();

    let count = exec_sql(
        &snap,
        "SELECT COUNT(*) FROM nodes WHERE label='Server'",
        &crate::sql::CancellationToken::new(),
    )
    .expect("COUNT(*) query must not fail with a duplicate-field schema error");
    assert_eq!(count.rows.len(), 1);

    let sel = exec_sql(
        &snap,
        "SELECT id FROM nodes LIMIT 1",
        &crate::sql::CancellationToken::new(),
    )
    .expect("SELECT id query must not fail with a duplicate-field schema error");
    assert_eq!(sel.rows.len(), 1);
}
