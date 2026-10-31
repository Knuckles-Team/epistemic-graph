//! Symbol edit model (MEDULLA-SCM-R013.1).

use medulla_types::source_symbol_edit::{SymbolEdit, SymbolEditKind, SymbolEditOp};

// spec: MEDULLA-SCM-R013.1
#[test]
fn edit_round_trips_and_kinds_are_closed() {
    let kinds = [
        (SymbolEditKind::ReplaceSymbol, "replace_symbol"),
        (SymbolEditKind::ReplaceBody, "replace_body"),
        (SymbolEditKind::InsertBefore, "insert_before"),
        (SymbolEditKind::InsertAfter, "insert_after"),
        (SymbolEditKind::Delete, "delete"),
    ];
    let edit = SymbolEdit {
        repo: "medulla".into(),
        branch: "main".into(),
        base_rev: "c1".into(),
        ops: kinds
            .iter()
            .map(|(kind, _)| SymbolEditOp {
                kind: *kind,
                symbol_id: "src/a.rs::f".into(),
                anchor_span_digest: "ab".repeat(32),
                new_text: "fn f() {}".into(),
            })
            .collect(),
        message: "edit f".into(),
        run_trace_id: "run-1".into(),
    };
    let json = serde_json::to_string(&edit).unwrap();
    for (_, name) in kinds {
        assert!(json.contains(&format!("\"{name}\"")));
    }
    assert_eq!(serde_json::from_str::<SymbolEdit>(&json).unwrap(), edit);
    assert!(serde_json::from_str::<SymbolEdit>(&json.replace("replace_body", "rewrite")).is_err());
}
