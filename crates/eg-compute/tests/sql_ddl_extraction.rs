//! CONCEPT:KG-2.212 — SQL DDL extraction into the database ontology.
#![cfg(feature = "ast")]

use eg_compute::parser::tree_sitter::parse_file;

const SCHEMA: &str = r#"CREATE TABLE users (
   user_id  SERIAL PRIMARY KEY,
   email    TEXT UNIQUE NOT NULL
);
CREATE TABLE sessions (
   token    TEXT PRIMARY KEY,
   user_id  INTEGER NOT NULL REFERENCES users(user_id)
);
CREATE VIEW active_sessions AS
SELECT s.token, u.email
FROM sessions s JOIN users u ON s.user_id = u.user_id;
"#;

#[test]
fn extracts_tables_columns_fks_and_view() {
    let res = parse_file("schema.sql", SCHEMA.as_bytes()).expect("parse");

    let node = |id: &str| res.nodes.iter().find(|n| n.node_id == id);
    // Tables.
    assert_eq!(node("table:users").unwrap().node_type, "DatabaseTable");
    assert_eq!(node("table:sessions").unwrap().node_type, "DatabaseTable");
    // View.
    assert_eq!(
        node("view:active_sessions").unwrap().node_type,
        "DatabaseView"
    );
    // Columns + flags.
    let pk = node("column:users.user_id").unwrap();
    assert_eq!(pk.node_type, "DatabaseColumn");
    assert_eq!(
        pk.properties.get("primary_key").map(String::as_str),
        Some("true")
    );
    let email = node("column:users.email").unwrap();
    assert_eq!(
        email.properties.get("unique").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        email.properties.get("not_null").map(String::as_str),
        Some("true")
    );

    let edge = |s: &str, t: &str, ty: &str| {
        res.edges
            .iter()
            .any(|e| e.source == s && e.target == t && e.edge_type == ty)
    };
    // hasColumn.
    assert!(edge("table:users", "column:users.user_id", "hasColumn"));
    // Inline FK: sessions.user_id -> users.user_id, and table->table.
    assert!(edge(
        "column:sessions.user_id",
        "column:users.user_id",
        "referencesColumn"
    ));
    assert!(edge("table:sessions", "table:users", "referencesTable"));
    // View depends on its source tables.
    assert!(edge("view:active_sessions", "table:sessions", "references"));
    assert!(edge("view:active_sessions", "table:users", "references"));
}

#[test]
fn extracts_table_level_foreign_key() {
    let ddl = r#"CREATE TABLE orders (
       id INTEGER PRIMARY KEY,
       user_id INTEGER NOT NULL,
       FOREIGN KEY (user_id) REFERENCES users(user_id)
    );"#;
    let res = parse_file("orders.sql", ddl.as_bytes()).expect("parse");
    let edge = |s: &str, t: &str, ty: &str| {
        res.edges
            .iter()
            .any(|e| e.source == s && e.target == t && e.edge_type == ty)
    };
    assert!(edge("table:orders", "table:users", "referencesTable"));
    assert!(edge(
        "column:orders.user_id",
        "column:users.user_id",
        "referencesColumn"
    ));
}
