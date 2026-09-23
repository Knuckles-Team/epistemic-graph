use super::{first_child_of_kind, get_node_text, ExtractedEdge, ExtractedNode, ParseResult};
use std::collections::HashMap;
use tree_sitter::Node;

// ── SQL DDL extraction (CONCEPT:AU-KG.ontology.emits-database-ontology-entities) ───────────────────────────────────
// Parses CREATE TABLE / CREATE VIEW + inline and table-level FOREIGN KEY
// constraints from the tree-sitter-sequel grammar into the database ontology
// (:DatabaseTable / :DatabaseColumn / :DatabaseView with hasColumn /
// referencesTable / referencesColumn / references edges). Assimilated from
// Graphify's SQL layer, but written into the durable engine graph natively.

/// Text of an `object_reference`'s `name` field identifier (the bare table/view
/// name; schema-qualified names collapse to their last segment).
fn object_reference_name(node: Node, source: &[u8]) -> Option<String> {
    let name = node.child_by_field_name("name")?;
    Some(get_node_text(name, source).trim().to_string())
}

/// The referenced table + column of an inline/table-level `REFERENCES t(c)`:
/// the `object_reference` after `keyword_references`, then the trailing column
/// identifier. Returns `(table, Option<column>)`.
fn references_target(parent: Node, source: &[u8]) -> Option<(String, Option<String>)> {
    let mut c = parent.walk();
    let children: Vec<Node> = parent.children(&mut c).collect();
    let kw = children
        .iter()
        .position(|ch| ch.kind() == "keyword_references")?;
    let obj = children[kw + 1..]
        .iter()
        .find(|ch| ch.kind() == "object_reference")?;
    let table = object_reference_name(*obj, source)?;
    // The referenced column is the identifier that follows the object_reference.
    let obj_pos = children.iter().position(|ch| ch.id() == obj.id())?;
    let col = children[obj_pos + 1..]
        .iter()
        .find(|ch| ch.kind() == "identifier")
        .map(|ch| get_node_text(*ch, source).trim().to_string());
    Some((table, col))
}

pub(super) fn extract_sql(root: Node, source: &[u8], file_path: &str, result: &mut ParseResult) {
    // Walk every statement; dispatch the two DDL forms we model.
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        match node.kind() {
            "create_table" => extract_create_table(node, source, file_path, result),
            "create_view" => extract_create_view(node, source, file_path, result),
            _ => {}
        }
        let mut c = node.walk();
        for child in node.children(&mut c) {
            stack.push(child);
        }
    }
}

fn extract_create_table(node: Node, source: &[u8], file_path: &str, result: &mut ParseResult) {
    let Some(obj) = first_child_of_kind(node, "object_reference") else {
        return;
    };
    let Some(table) = object_reference_name(obj, source) else {
        return;
    };
    let table_id = format!("table:{}", table);
    let mut props = HashMap::new();
    props.insert("name".to_string(), table.clone());
    props.insert("source_file".to_string(), file_path.to_string());
    props.insert("kind_detail".to_string(), "table".to_string());
    props.insert("language".to_string(), "sql".to_string());
    result.nodes.push(ExtractedNode {
        node_id: table_id.clone(),
        node_type: "DatabaseTable".to_string(),
        properties: props,
    });
    result.symbols_extracted += 1;

    let Some(cols) = first_child_of_kind(node, "column_definitions") else {
        return;
    };
    let mut cc = cols.walk();
    for child in cols.children(&mut cc) {
        match child.kind() {
            "column_definition" => {
                extract_column(child, &table, &table_id, file_path, source, result)
            }
            // Table-level FK constraints live under `constraints`.
            "constraints" => {
                let mut gc = child.walk();
                for con in child.children(&mut gc) {
                    if con.kind() == "constraint" {
                        extract_table_constraint(con, &table, &table_id, source, result);
                    }
                }
            }
            _ => {}
        }
    }
}

struct ColumnFacts {
    name: String,
    id: String,
    data_type: String,
    kinds: Vec<String>,
}

fn column_facts(col: Node, table: &str, source: &[u8]) -> Option<ColumnFacts> {
    let name_node = col.child_by_field_name("name")?;
    let name = get_node_text(name_node, source).trim().to_string();
    let id = format!("column:{}.{}", table, name);
    let data_type = col
        .child_by_field_name("type")
        .map(|t| get_node_text(t, source).trim().to_string())
        .unwrap_or_default();
    let mut cursor = col.walk();
    let kinds = col
        .children(&mut cursor)
        .map(|child| child.kind().to_string())
        .collect();
    Some(ColumnFacts {
        name,
        id,
        data_type,
        kinds,
    })
}

fn column_properties(table: &str, file_path: &str, facts: &ColumnFacts) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("name".to_string(), facts.name.clone());
    props.insert("table".to_string(), table.to_string());
    props.insert("source_file".to_string(), file_path.to_string());
    props.insert("kind_detail".to_string(), "column".to_string());
    props.insert("language".to_string(), "sql".to_string());
    if !facts.data_type.is_empty() {
        props.insert("data_type".to_string(), facts.data_type.clone());
    }
    if facts.kinds.iter().any(|kind| kind == "keyword_primary")
        && facts.kinds.iter().any(|kind| kind == "keyword_key")
    {
        props.insert("primary_key".to_string(), "true".to_string());
    }
    if facts.kinds.iter().any(|kind| kind == "keyword_unique") {
        props.insert("unique".to_string(), "true".to_string());
    }
    if facts.kinds.iter().any(|kind| kind == "keyword_not")
        && facts.kinds.iter().any(|kind| kind == "keyword_null")
    {
        props.insert("not_null".to_string(), "true".to_string());
    }
    props
}

fn append_column(
    result: &mut ParseResult,
    table_id: &str,
    facts: &ColumnFacts,
    properties: HashMap<String, String>,
) {
    result.nodes.push(ExtractedNode {
        node_id: facts.id.clone(),
        node_type: "DatabaseColumn".to_string(),
        properties,
    });
    result.symbols_extracted += 1;
    result.edges.push(ExtractedEdge {
        source: table_id.to_string(),
        target: facts.id.clone(),
        edge_type: "hasColumn".to_string(),
        properties: HashMap::new(),
    });
}

fn extract_column(
    col: Node,
    table: &str,
    table_id: &str,
    file_path: &str,
    source: &[u8],
    result: &mut ParseResult,
) {
    let Some(facts) = column_facts(col, table, source) else {
        return;
    };
    let properties = column_properties(table, file_path, &facts);
    append_column(result, table_id, &facts, properties);

    // Inline `... REFERENCES other(col)` foreign key.
    if facts.kinds.iter().any(|kind| kind == "keyword_references") {
        if let Some((ref_table, ref_col)) = references_target(col, source) {
            emit_fk(&facts.id, table_id, &ref_table, ref_col.as_deref(), result);
        }
    }
}

fn extract_table_constraint(
    con: Node,
    _table: &str,
    table_id: &str,
    source: &[u8],
    result: &mut ParseResult,
) {
    let Some((ref_table, ref_col)) = references_target(con, source) else {
        return;
    };
    // Local columns named in `ordered_columns (column name: (identifier))`.
    let mut local_cols: Vec<String> = Vec::new();
    if let Some(ordered) = first_child_of_kind(con, "ordered_columns") {
        let mut c = ordered.walk();
        for col in ordered.children(&mut c) {
            if col.kind() == "column" {
                if let Some(n) = col.child_by_field_name("name") {
                    local_cols.push(get_node_text(n, source).trim().to_string());
                }
            }
        }
    }
    let table_name = table_id.strip_prefix("table:").unwrap_or(table_id);
    for lc in &local_cols {
        let col_id = format!("column:{}.{}", table_name, lc);
        emit_fk(&col_id, table_id, &ref_table, ref_col.as_deref(), result);
    }
    if local_cols.is_empty() {
        // No explicit local columns parsed — still record the table→table FK.
        let mut p = HashMap::new();
        p.insert("confidence".to_string(), "EXTRACTED".to_string());
        result.edges.push(ExtractedEdge {
            source: table_id.to_string(),
            target: format!("table:{}", ref_table),
            edge_type: "referencesTable".to_string(),
            properties: p,
        });
    }
}

/// Emit the column→column and table→table foreign-key edges.
fn emit_fk(
    col_id: &str,
    table_id: &str,
    ref_table: &str,
    ref_col: Option<&str>,
    result: &mut ParseResult,
) {
    let mut p = HashMap::new();
    p.insert("confidence".to_string(), "EXTRACTED".to_string());
    if let Some(rc) = ref_col {
        result.edges.push(ExtractedEdge {
            source: col_id.to_string(),
            target: format!("column:{}.{}", ref_table, rc),
            edge_type: "referencesColumn".to_string(),
            properties: p.clone(),
        });
    }
    result.edges.push(ExtractedEdge {
        source: table_id.to_string(),
        target: format!("table:{}", ref_table),
        edge_type: "referencesTable".to_string(),
        properties: p,
    });
}

fn extract_create_view(node: Node, source: &[u8], file_path: &str, result: &mut ParseResult) {
    let Some(obj) = first_child_of_kind(node, "object_reference") else {
        return;
    };
    let Some(view) = object_reference_name(obj, source) else {
        return;
    };
    let view_id = format!("view:{}", view);
    let mut props = HashMap::new();
    props.insert("name".to_string(), view.clone());
    props.insert("source_file".to_string(), file_path.to_string());
    props.insert("kind_detail".to_string(), "view".to_string());
    props.insert("language".to_string(), "sql".to_string());
    result.nodes.push(ExtractedNode {
        node_id: view_id.clone(),
        node_type: "DatabaseView".to_string(),
        properties: props,
    });
    result.symbols_extracted += 1;

    // Tables the view depends on: every `relation (object_reference name: ...)`
    // in the SELECT body (skip the view's own name object_reference).
    let mut stack = vec![node];
    while let Some(n) = stack.pop() {
        if n.kind() == "relation" {
            if let Some(rel_obj) = first_child_of_kind(n, "object_reference") {
                if let Some(t) = object_reference_name(rel_obj, source) {
                    result.edges.push(ExtractedEdge {
                        source: view_id.clone(),
                        target: format!("table:{}", t),
                        edge_type: "references".to_string(),
                        properties: HashMap::new(),
                    });
                }
            }
        }
        let mut c = n.walk();
        for child in n.children(&mut c) {
            stack.push(child);
        }
    }
}
