use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tree_sitter::{Language, Node, Parser};

#[derive(Serialize, Deserialize, Debug)]
pub struct SymbolMetadata {
    pub name: String,
    pub symbol_type: String, // Class, Function, etc.
    pub line: usize,
    pub docstring: Option<String>,
    pub args: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExtractedNode {
    pub node_id: String,
    pub node_type: String,
    pub properties: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExtractedEdge {
    pub source: String,
    pub target: String,
    pub edge_type: String,
    pub properties: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ParseResult {
    pub nodes: Vec<ExtractedNode>,
    pub edges: Vec<ExtractedEdge>,
    pub symbols_extracted: usize,
}

/// Resolve a file path to its tree-sitter grammar plus a stable language label
/// (the label is stamped on every extracted symbol so the graph can answer
/// "show me all Java code" and compute per-language metrics). Returns ``None``
/// for paths we don't have a grammar for.
fn lang_for_path(file_path: &str) -> Option<(Language, &'static str)> {
    let ext = file_path
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let pair: (Language, &'static str) = match ext.as_str() {
        "py" | "pyi" => (tree_sitter_python::LANGUAGE.into(), "python"),
        "js" | "jsx" | "mjs" | "cjs" => (tree_sitter_javascript::LANGUAGE.into(), "javascript"),
        "ts" | "mts" | "cts" => (
            tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            "typescript",
        ),
        "tsx" => (tree_sitter_typescript::LANGUAGE_TSX.into(), "typescript"),
        "go" => (tree_sitter_go::LANGUAGE.into(), "go"),
        "rs" => (tree_sitter_rust::LANGUAGE.into(), "rust"),
        "java" => (tree_sitter_java::LANGUAGE.into(), "java"),
        "c" | "h" => (tree_sitter_c::LANGUAGE.into(), "c"),
        "cpp" | "cc" | "cxx" | "hpp" | "hxx" | "hh" | "c++" => {
            (tree_sitter_cpp::LANGUAGE.into(), "cpp")
        }
        "cs" => (tree_sitter_c_sharp::LANGUAGE.into(), "csharp"),
        "sql" | "ddl" => (tree_sitter_sequel::LANGUAGE.into(), "sql"),
        _ => return lang_for_path_extended(&ext),
    };
    Some(pair)
}

/// Extended-language tier (CONCEPT:AU-KG.compute.built-ast-extended), compiled only with `ast-extended`.
/// Without the feature it resolves nothing, so a slim `ast` build stays lean.
#[cfg(feature = "ast-extended")]
fn lang_for_path_extended(ext: &str) -> Option<(Language, &'static str)> {
    Some(match ext {
        "rb" => (tree_sitter_ruby::LANGUAGE.into(), "ruby"),
        "php" => (tree_sitter_php::LANGUAGE_PHP.into(), "php"),
        "sh" | "bash" => (tree_sitter_bash::LANGUAGE.into(), "bash"),
        "scala" | "sc" => (tree_sitter_scala::LANGUAGE.into(), "scala"),
        "lua" => (tree_sitter_lua::LANGUAGE.into(), "lua"),
        _ => return None,
    })
}

#[cfg(not(feature = "ast-extended"))]
fn lang_for_path_extended(_ext: &str) -> Option<(Language, &'static str)> {
    None
}

/// Extensions the parser can ingest — kept in sync with [`lang_for_path`] and
/// mirrored by the Python file-discovery walk.
pub const SUPPORTED_EXTENSIONS: &[&str] = &[
    "py", "pyi", "js", "jsx", "mjs", "cjs", "ts", "mts", "cts", "tsx", "go", "rs", "java", "c",
    "h", "cpp", "cc", "cxx", "hpp", "hxx", "hh",
    "cs", // SQL DDL (CONCEPT:AU-KG.ontology.emits-database-ontology-entities):
    "sql", "ddl", // extended tier (CONCEPT:AU-KG.compute.built-ast-extended):
    "rb", "php", "sh", "bash", "scala", "sc", "lua",
];

pub fn parse_file(file_path: &str, source: &[u8]) -> Result<ParseResult, String> {
    let mut parser = Parser::new();
    let (language, lang_label) = lang_for_path(file_path).ok_or("Unsupported file extension")?;

    parser.set_language(&language).map_err(|e| e.to_string())?;

    let tree = parser.parse(source, None).ok_or("Failed to parse source")?;

    let mut result = ParseResult {
        nodes: Vec::new(),
        edges: Vec::new(),
        symbols_extracted: 0,
    };

    let file_node_id = format!("file:{}", file_path);

    // SQL DDL takes a dedicated extraction path (CONCEPT:AU-KG.ontology.emits-database-ontology-entities): it emits
    // database-ontology entities (tables/columns/views + FK edges), NOT :Code
    // symbols, so it does not flow through the call-graph walker.
    if lang_label == "sql" {
        extract_sql(tree.root_node(), source, file_path, &mut result);
        return Ok(result);
    }

    walk_node(
        tree.root_node(),
        source,
        file_path,
        lang_label,
        &file_node_id,
        "",
        &mut result,
    );

    Ok(result)
}

// ── SQL DDL extraction (CONCEPT:AU-KG.ontology.emits-database-ontology-entities) ───────────────────────────────────
// Parses CREATE TABLE / CREATE VIEW + inline and table-level FOREIGN KEY
// constraints from the tree-sitter-sequel grammar into the database ontology
// (:DatabaseTable / :DatabaseColumn / :DatabaseView with hasColumn /
// referencesTable / referencesColumn / references edges). Assimilated from
// Graphify's SQL layer, but written into the durable engine graph natively.

/// First direct child of the given kind.
fn first_child_of_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut c = node.walk();
    let found = node.children(&mut c).find(|ch| ch.kind() == kind);
    found
}

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

fn extract_sql(root: Node, source: &[u8], file_path: &str, result: &mut ParseResult) {
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

fn extract_column(
    col: Node,
    table: &str,
    table_id: &str,
    file_path: &str,
    source: &[u8],
    result: &mut ParseResult,
) {
    let Some(name_node) = col.child_by_field_name("name") else {
        return;
    };
    let col_name = get_node_text(name_node, source).trim().to_string();
    let col_id = format!("column:{}.{}", table, col_name);
    let col_type = col
        .child_by_field_name("type")
        .map(|t| get_node_text(t, source).trim().to_string())
        .unwrap_or_default();

    // Constraint keyword flags (PRIMARY KEY / UNIQUE / NOT NULL).
    let mut c = col.walk();
    let kinds: Vec<&str> = col.children(&mut c).map(|ch| ch.kind()).collect();
    let mut props = HashMap::new();
    props.insert("name".to_string(), col_name.clone());
    props.insert("table".to_string(), table.to_string());
    props.insert("source_file".to_string(), file_path.to_string());
    props.insert("kind_detail".to_string(), "column".to_string());
    props.insert("language".to_string(), "sql".to_string());
    if !col_type.is_empty() {
        props.insert("data_type".to_string(), col_type);
    }
    if kinds.contains(&"keyword_primary") && kinds.contains(&"keyword_key") {
        props.insert("primary_key".to_string(), "true".to_string());
    }
    if kinds.contains(&"keyword_unique") {
        props.insert("unique".to_string(), "true".to_string());
    }
    if kinds.contains(&"keyword_not") && kinds.contains(&"keyword_null") {
        props.insert("not_null".to_string(), "true".to_string());
    }
    result.nodes.push(ExtractedNode {
        node_id: col_id.clone(),
        node_type: "DatabaseColumn".to_string(),
        properties: props,
    });
    result.symbols_extracted += 1;

    result.edges.push(ExtractedEdge {
        source: table_id.to_string(),
        target: col_id.clone(),
        edge_type: "hasColumn".to_string(),
        properties: HashMap::new(),
    });

    // Inline `... REFERENCES other(col)` foreign key.
    if kinds.contains(&"keyword_references") {
        if let Some((ref_table, ref_col)) = references_target(col, source) {
            emit_fk(&col_id, table_id, &ref_table, ref_col.as_deref(), result);
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

// CONCEPT:EG-KG.storage.nonblocking-checkpoint — Native test-quality metrics. Computed in the Rust compute
// layer (not Python) so "which pytests need work" is a graph fact, not a script.
const MOCK_CALLS: &[&str] = &[
    "Mock",
    "MagicMock",
    "AsyncMock",
    "NonCallableMock",
    "PropertyMock",
    "patch",
    "create_autospec",
];
const RAISES_CALLS: &[&str] = &["raises", "warns", "fail"];

#[derive(Default)]
struct TestMetrics {
    assert_count: usize,
    raises_count: usize,
    mock_count: usize,
    calls: Vec<String>,
}

/// Last identifier of a Python `call` node's function (e.g. `pytest.raises` → `raises`).
fn py_callee_name(call_node: Node, source: &[u8]) -> Option<String> {
    let f = call_node.child_by_field_name("function")?;
    let text = get_node_text(f, source);
    text.rsplit('.').next().map(|s| s.trim().to_string())
}

/// Recursively accumulate assert/raises/mock counts and callee names over a
/// function body subtree.
fn collect_test_metrics(node: Node, source: &[u8], m: &mut TestMetrics) {
    let kind = node.kind();
    if kind == "assert_statement" {
        m.assert_count += 1;
    } else if kind == "call" {
        if let Some(callee) = py_callee_name(node, source) {
            if RAISES_CALLS.contains(&callee.as_str()) {
                m.raises_count += 1;
            }
            if MOCK_CALLS.contains(&callee.as_str()) {
                m.mock_count += 1;
            }
            m.calls.push(callee);
        }
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_test_metrics(child, source, m);
    }
}

// CONCEPT:EG-KG.compute.type-scope-resolved-call — Type/scope-resolved call graph. A call site carries the
// receiver (`self`/`this`, a variable, or a Type for a static call — empty for a
// bare call), the callee name (last dotted/`::` segment), and the argument count.
// The cross-file resolver (`super::resolve`) binds these to the right *method on
// the receiver's class* (scope) and disambiguates same-name overloads by arity,
// instead of the old name-only match. Computed in Rust, shipped already-resolved.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CallSite {
    pub receiver: String,
    pub callee: String,
    pub argc: Option<usize>,
}

/// Split a callee expression's text into (receiver, callee) by its last dotted/
/// `::` segment: `obj.method` → (`obj`,`method`), `a::b::run` → (`b`,`run`),
/// `foo` → (``,`foo`).
fn split_receiver_callee(text: &str) -> (String, String) {
    let norm = text.replace("::", ".");
    let mut segs: Vec<&str> = norm
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let callee = segs.pop().unwrap_or("").to_string();
    let receiver = segs.pop().unwrap_or("").to_string();
    (receiver, callee)
}

/// Count the arguments at a call node (named children of its `arguments` /
/// `argument_list`), or `None` when no argument list is present.
fn count_args(node: Node) -> Option<usize> {
    let args = node.child_by_field_name("arguments").or_else(|| {
        let mut c = node.walk();
        let found = node
            .children(&mut c)
            .find(|ch| matches!(ch.kind(), "argument_list" | "arguments"));
        found
    })?;
    let mut c = args.walk();
    Some(args.children(&mut c).filter(|ch| ch.is_named()).count())
}

/// Collect structured call sites within a function/method body across languages:
/// Python `call`, JS/TS/Go/Rust/C/C++ `call_expression`, Java `method_invocation`.
/// (CONCEPT:EG-KG.compute.type-scope-resolved-call; supersedes the old name-only `collect_calls`.)
fn collect_call_sites(node: Node, source: &[u8], out: &mut Vec<CallSite>) {
    match node.kind() {
        "call" | "call_expression" => {
            if let Some(f) = node.child_by_field_name("function") {
                let (receiver, callee) = split_receiver_callee(&get_node_text(f, source));
                if !callee.is_empty() {
                    out.push(CallSite {
                        receiver,
                        callee,
                        argc: count_args(node),
                    });
                }
            }
        }
        "method_invocation" => {
            // Java `recv.name(args)`: `object` is the receiver, `name` the callee.
            if let Some(n) = node.child_by_field_name("name") {
                let callee = get_node_text(n, source).trim().to_string();
                if !callee.is_empty() {
                    let receiver = node
                        .child_by_field_name("object")
                        .map(|o| split_receiver_callee(&get_node_text(o, source)).1)
                        .unwrap_or_default();
                    out.push(CallSite {
                        receiver,
                        callee,
                        argc: count_args(node),
                    });
                }
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_call_sites(child, source, out);
    }
}

/// Serialize call sites onto a SYMBOL property: `recv:callee:argc` per site,
/// joined by `;` (identifiers/digits only, so the delimiters never clash). The
/// resolver decodes this with [`decode_call_sites`]; it is stripped from the
/// graph nodes after resolution (purely a resolution input).
pub(crate) fn encode_call_sites(sites: &[CallSite]) -> String {
    sites
        .iter()
        .map(|s| {
            let a = s.argc.map(|n| n.to_string()).unwrap_or_default();
            format!("{}:{}:{}", s.receiver, s.callee, a)
        })
        .collect::<Vec<_>>()
        .join(";")
}

/// A decoded call site (the resolver's view of [`encode_call_sites`]).
pub(crate) struct DecodedSite {
    pub receiver: String,
    pub callee: String,
    pub argc: Option<usize>,
}

/// Inverse of [`encode_call_sites`].
pub(crate) fn decode_call_sites(s: &str) -> Vec<DecodedSite> {
    s.split(';')
        .filter(|x| !x.is_empty())
        .filter_map(|site| {
            let mut it = site.splitn(3, ':');
            let receiver = it.next()?.to_string();
            let callee = it.next()?.to_string();
            let argc = it
                .next()
                .and_then(|a| if a.is_empty() { None } else { a.parse().ok() });
            if callee.is_empty() {
                None
            } else {
                Some(DecodedSite {
                    receiver,
                    callee,
                    argc,
                })
            }
        })
        .collect()
}

/// Formal-parameter count for a callable across grammars (the def-side `arity`
/// the resolver matches against a call's `argc`). Python excludes a leading
/// `self`/`cls`; other grammars carry the receiver outside the parameter list
/// (Go `receiver` field, Rust `self_parameter`), so a plain named-child count of
/// the parameter list is the callee-visible arity.
fn param_count(node: Node, source: &[u8], language: &str) -> usize {
    if language == "python" {
        return py_param_count(node, source);
    }
    let params = node.child_by_field_name("parameters").or_else(|| {
        let mut c = node.walk();
        let found = node.children(&mut c).find(|ch| {
            matches!(
                ch.kind(),
                "formal_parameters" | "parameter_list" | "parameters"
            )
        });
        found
    });
    let params = match params {
        Some(p) => p,
        None => return 0,
    };
    let mut c = params.walk();
    params
        .children(&mut c)
        .filter(|ch| {
            ch.is_named()
                && !matches!(
                    ch.kind(),
                    "self_parameter" | "comment" | "line_comment" | "block_comment"
                )
        })
        .count()
}

/// Collect the last segment of every type identifier under a node (e.g. a
/// heritage clause), skipping generic type-argument lists so only the base
/// type/interface names are returned. Best-effort and tolerant of grammar shape.
fn collect_type_names(node: Node, source: &[u8], out: &mut Vec<String>) {
    let k = node.kind();
    if k.ends_with("type_identifier") || k == "identifier" {
        let t = get_node_text(node, source);
        let name = t.rsplit(['.', ':']).next().unwrap_or(&t).trim().to_string();
        if !name.is_empty() && !out.contains(&name) {
            out.push(name);
        }
        return;
    }
    if matches!(k, "type_arguments" | "type_parameters") {
        return;
    }
    let mut c = node.walk();
    for child in node.children(&mut c) {
        collect_type_names(child, source, out);
    }
}

/// Best-effort (inherits, realizes) base/interface names for a class-like node.
/// Conservative and language-scoped: Python bases, Java `extends`/`implements`,
/// TS/JS `extends`/`implements`. Rust trait impls and Go embedding need
/// impl-block / embedding analysis and are deferred (return empty) — the
/// resolver never emits a dangling edge, so an empty result is safe.
fn class_relations(node: Node, source: &[u8], language: &str) -> (Vec<String>, Vec<String>) {
    match language {
        "python" => (py_class_bases(node, source), Vec::new()),
        "java" => {
            let mut inh = Vec::new();
            let mut real = Vec::new();
            if let Some(sc) = node.child_by_field_name("superclass") {
                collect_type_names(sc, source, &mut inh);
            }
            if let Some(ifs) = node.child_by_field_name("interfaces") {
                collect_type_names(ifs, source, &mut real);
            }
            (inh, real)
        }
        "typescript" | "javascript" => {
            let mut inh = Vec::new();
            let mut real = Vec::new();
            let mut c = node.walk();
            for ch in node.children(&mut c) {
                if ch.kind() == "class_heritage" {
                    let mut c2 = ch.walk();
                    for clause in ch.children(&mut c2) {
                        match clause.kind() {
                            "extends_clause" => collect_type_names(clause, source, &mut inh),
                            "implements_clause" => collect_type_names(clause, source, &mut real),
                            _ => {}
                        }
                    }
                }
            }
            (inh, real)
        }
        _ => (Vec::new(), Vec::new()),
    }
}

/// Count a Python function's parameters, excluding a leading `self`/`cls`.
fn py_param_count(node: Node, source: &[u8]) -> usize {
    let params = match node.child_by_field_name("parameters") {
        Some(p) => p,
        None => return 0,
    };
    let mut n = 0usize;
    let mut cursor = params.walk();
    for child in params.children(&mut cursor) {
        match child.kind() {
            "identifier"
            | "typed_parameter"
            | "default_parameter"
            | "typed_default_parameter"
            | "list_splat_pattern"
            | "dictionary_splat_pattern" => {
                let txt = get_node_text(child, source);
                let base = txt.trim_start_matches('*');
                let first = base.split([':', '=']).next().unwrap_or("").trim();
                if first != "self" && first != "cls" && !first.is_empty() {
                    n += 1;
                }
            }
            _ => {}
        }
    }
    n
}

/// Collect decorator source strings for a function (handles `decorated_definition`).
fn py_decorators(node: Node, source: &[u8]) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(parent) = node.parent() {
        if parent.kind() == "decorated_definition" {
            let mut cursor = parent.walk();
            for child in parent.children(&mut cursor) {
                if child.kind() == "decorator" {
                    out.push(get_node_text(child, source).trim().to_string());
                }
            }
        }
    }
    out
}

/// Base-class names of a Python `class_definition` (from the `superclasses` field).
fn py_class_bases(node: Node, source: &[u8]) -> Vec<String> {
    let mut bases = Vec::new();
    if let Some(supers) = node.child_by_field_name("superclasses") {
        let mut cursor = supers.walk();
        for child in supers.children(&mut cursor) {
            match child.kind() {
                "identifier" | "attribute" => {
                    bases.push(get_node_text(child, source));
                }
                "keyword_argument" => {
                    // e.g. metaclass=ABCMeta — record the value side.
                    if let Some(v) = child.child_by_field_name("value") {
                        bases.push(get_node_text(v, source));
                    }
                }
                _ => {}
            }
        }
    }
    bases
}

/// Member method names of a Python class + whether any is `@abstractmethod`.
fn py_class_methods(node: Node, source: &[u8]) -> (Vec<String>, bool) {
    let mut methods = Vec::new();
    let mut has_abstract = false;
    if let Some(body) = node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.children(&mut cursor) {
            let fdef = if child.kind() == "decorated_definition" {
                let mut c2 = child.walk();
                let kids: Vec<Node> = child.children(&mut c2).collect();
                kids.into_iter().find(|n| n.kind() == "function_definition")
            } else if child.kind() == "function_definition" {
                Some(child)
            } else {
                None
            };
            if let Some(f) = fdef {
                if let Some(n) = f.child_by_field_name("name") {
                    methods.push(get_node_text(n, source));
                }
                for d in py_decorators(f, source) {
                    if d.contains("abstractmethod") {
                        has_abstract = true;
                    }
                }
            }
        }
    }
    (methods, has_abstract)
}

/// Extract pytest mark names from decorator strings (`@pytest.mark.skip` → `skip`).
fn py_marks(decorators: &[String]) -> Vec<String> {
    let mut marks = Vec::new();
    for d in decorators {
        if let Some(idx) = d.find(".mark.") {
            let rest = &d[idx + ".mark.".len()..];
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if !name.is_empty() {
                marks.push(name);
            }
        }
    }
    marks
}

/// Semantic kind for a type-like declaration node across grammars, or ``None``.
/// Covers Python/JS/TS/Java/C#/C/C++/Rust/Go type containers so every language's
/// classes, structs, interfaces, enums, traits, etc. surface as ``Class`` symbols
/// (the detail string preserves the precise kind for classification).
fn class_like_kind(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "class_definition"
        | "class_declaration"
        | "class_specifier"
        | "abstract_class_declaration" => "class",
        "interface_declaration" => "interface",
        "struct_specifier" | "struct_item" | "struct_declaration" => "struct",
        "enum_declaration" | "enum_item" | "enum_specifier" => "enum",
        // `trait_item` Rust, `trait_definition` Scala, `trait_declaration` PHP.
        "trait_item" | "trait_definition" | "trait_declaration" => "trait",
        "union_item" | "union_specifier" => "union",
        "record_declaration" | "record_struct_declaration" => "record",
        "namespace_definition" => "namespace",
        // Ruby `class`/`module`; Scala `object_definition` (CONCEPT:AU-KG.compute.built-ast-extended).
        "class" => "class",
        "module" => "module",
        "object_definition" => "object",
        // Go puts struct/interface names on the type_spec under a type_declaration.
        "type_spec" | "type_alias_declaration" => "type",
        _ => return None,
    })
}

/// Semantic kind for a callable declaration node across grammars, or ``None``.
fn function_like_kind(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "function_definition"
        | "function_declaration"
        | "function_item"
        | "generator_function_declaration" => "function",
        // Ruby `method`/`singleton_method` (CONCEPT:AU-KG.compute.built-ast-extended).
        "method_definition" | "method_declaration" | "method" | "singleton_method" => "method",
        "constructor_declaration" => "constructor",
        _ => return None,
    })
}

/// Best-effort symbol name across grammars. Most declarations expose a ``name``
/// field; C/C++ functions nest the identifier under ``declarator`` and Rust
/// ``impl`` blocks use ``type``, so fall back to those.
fn symbol_name(node: Node, source: &[u8]) -> Option<String> {
    if let Some(n) = node.child_by_field_name("name") {
        return Some(get_node_text(n, source));
    }
    if let Some(decl) = node.child_by_field_name("declarator") {
        if let Some(name) = innermost_declarator_name(decl, source) {
            return Some(name);
        }
    }
    if let Some(t) = node.child_by_field_name("type") {
        return Some(get_node_text(t, source));
    }
    None
}

/// Descend a C/C++ declarator chain (pointer/function/array declarators) to the
/// innermost identifier — that's the function/variable name.
fn innermost_declarator_name(node: Node, source: &[u8]) -> Option<String> {
    match node.kind() {
        "identifier"
        | "field_identifier"
        | "type_identifier"
        | "qualified_identifier"
        | "destructor_name"
        | "operator_name" => return Some(get_node_text(node, source)),
        _ => {}
    }
    if let Some(inner) = node.child_by_field_name("declarator") {
        if let Some(name) = innermost_declarator_name(inner, source) {
            return Some(name);
        }
    }
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(name) = innermost_declarator_name(child, source) {
            return Some(name);
        }
    }
    None
}

/// Build a SYMBOL node (id = content hash) + an IMPLEMENTS edge from its file,
/// stamping the common facts (name, symbol_type, kind_detail, language, line,
/// ast_hash, file_path) plus any language-specific ``extra`` properties.
#[allow(clippy::too_many_arguments)]
fn emit_symbol(
    node: Node,
    source: &[u8],
    file_path: &str,
    language: &str,
    symbol_type: &str,
    kind_detail: &str,
    name: String,
    file_node_id: &str,
    extra: HashMap<String, String>,
    result: &mut ParseResult,
) {
    let content_bytes = &source[node.start_byte()..node.end_byte()];
    let mut hasher = Sha256::new();
    hasher.update(content_bytes);
    let content_hash = format!("{:x}", hasher.finalize());
    let symbol_id = format!("symbol:{}", content_hash);

    let mut properties = HashMap::new();
    properties.insert("name".to_string(), name);
    properties.insert("symbol_type".to_string(), symbol_type.to_string());
    properties.insert("kind_detail".to_string(), kind_detail.to_string());
    properties.insert("language".to_string(), language.to_string());
    properties.insert(
        "line".to_string(),
        (node.start_position().row + 1).to_string(),
    );
    properties.insert("ast_hash".to_string(), content_hash);
    properties.insert("file_path".to_string(), file_path.to_string());
    // CONCEPT:EG-KG.compute.model-free-similar-code — model-free similarity signature (MinHash over normalized
    // AST leaf trigrams). The cross-file resolver LSH-bands these into `similar_to`
    // edges; it is a resolution-only input and is stripped from the graph nodes.
    properties.insert(
        "minhash".to_string(),
        encode_minhash(&symbol_minhash(node, source)),
    );
    for (k, v) in extra {
        properties.insert(k, v);
    }

    result.nodes.push(ExtractedNode {
        node_id: symbol_id.clone(),
        node_type: "SYMBOL".to_string(),
        properties,
    });
    result.edges.push(ExtractedEdge {
        source: file_node_id.to_string(),
        target: symbol_id,
        edge_type: "IMPLEMENTS".to_string(),
        properties: HashMap::new(),
    });
    result.symbols_extracted += 1;
}

#[allow(clippy::too_many_arguments)]
fn walk_node(
    node: Node,
    source: &[u8],
    file_path: &str,
    language: &str,
    file_node_id: &str,
    scope: &str,
    result: &mut ParseResult,
) {
    let kind = node.kind();
    // The enclosing-class name children inherit (CONCEPT:EG-KG.compute.type-scope-resolved-call scope tracking);
    // defaults to propagating the current scope unless this node is a named class.
    let mut descend_scope = scope.to_string();

    if let Some(detail) = class_like_kind(kind) {
        if let Some(name) = symbol_name(node, source).filter(|n| !n.is_empty()) {
            let mut extra = HashMap::new();
            // CONCEPT:EG-KG.compute.type-scope-resolved-call — inheritance/realization facts across grammars.
            let (inherits, realizes) = class_relations(node, source, language);
            extra.insert("scope".to_string(), scope.to_string());
            extra.insert("bases".to_string(), inherits.join(","));
            extra.insert("interfaces".to_string(), realizes.join(","));
            descend_scope = name.clone();
            // CONCEPT:EG-KG.storage.nonblocking-checkpoint — Python structural facts for design-pattern detection.
            if language == "python" {
                let (methods, has_abstract) = py_class_methods(node, source);
                let decorators = py_decorators(node, source);
                extra.insert("methods".to_string(), methods.join(","));
                extra.insert(
                    "decorators".to_string(),
                    decorators
                        .iter()
                        .map(|d| d.trim_start_matches('@').to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                );
                extra.insert("is_abstract".to_string(), has_abstract.to_string());
                extra.insert("method_count".to_string(), methods.len().to_string());
            }
            emit_symbol(
                node,
                source,
                file_path,
                language,
                "Class",
                detail,
                name,
                file_node_id,
                extra,
                result,
            );
        }
    } else if let Some(detail) = function_like_kind(kind) {
        if let Some(name) = symbol_name(node, source).filter(|n| !n.is_empty()) {
            let mut extra = HashMap::new();
            extra.insert("scope".to_string(), scope.to_string());
            // Structured call sites (receiver/callee/argc) for type/scope-resolved
            // call edges, plus the bare callee names kept as `calls` for the
            // name-only fallback + COVERS. (CONCEPT:EG-KG.compute.type-scope-resolved-call / KG-2.8)
            let mut sites = Vec::new();
            collect_call_sites(node, source, &mut sites);
            sites.sort();
            sites.dedup();
            sites.truncate(64);
            let mut calls: Vec<String> = sites.iter().map(|s| s.callee.clone()).collect();
            calls.sort();
            calls.dedup();
            calls.truncate(64);
            extra.insert("calls".to_string(), calls.join(","));
            extra.insert("call_sites".to_string(), encode_call_sites(&sites));
            extra.insert(
                "arity".to_string(),
                param_count(node, source, language).to_string(),
            );

            // CONCEPT:EG-KG.storage.nonblocking-checkpoint — native Python test-quality metrics on the symbol.
            if language == "python" {
                let decorators = py_decorators(node, source);
                // CONCEPT:EG-KG.compute.raw-decorator-strings — keep the raw decorator strings so the route
                // pass can detect HTTP route definitions (@app.route/@router.get…).
                extra.insert(
                    "decorators".to_string(),
                    decorators
                        .iter()
                        .map(|d| d.trim_start_matches('@').to_string())
                        .collect::<Vec<_>>()
                        .join("\u{1f}"),
                );
                let marks = py_marks(&decorators);
                let is_skipped = marks
                    .iter()
                    .any(|m| m == "skip" || m == "skipif" || m == "xfail");
                let mock_decos = decorators
                    .iter()
                    .filter(|d| {
                        let l = d.to_lowercase();
                        l.contains("patch") || l.contains("mock")
                    })
                    .count();
                let is_test = name.starts_with("test");

                let mut m = TestMetrics::default();
                collect_test_metrics(node, source, &mut m);

                extra.insert("is_test".to_string(), is_test.to_string());
                extra.insert("assert_count".to_string(), m.assert_count.to_string());
                extra.insert("raises_count".to_string(), m.raises_count.to_string());
                extra.insert(
                    "mock_count".to_string(),
                    (m.mock_count + mock_decos).to_string(),
                );
                extra.insert(
                    "fixture_count".to_string(),
                    py_param_count(node, source).to_string(),
                );
                extra.insert("marks".to_string(), marks.join(","));
                extra.insert("is_skipped".to_string(), is_skipped.to_string());
            }
            emit_symbol(
                node,
                source,
                file_path,
                language,
                "Function",
                detail,
                name,
                file_node_id,
                extra,
                result,
            );
        }
    } else if kind == "call" || kind == "call_expression" {
        if let Some(function_node) = node.child_by_field_name("function") {
            let callee = get_node_text(function_node, source);
            let mut properties = HashMap::new();
            properties.insert("raw".to_string(), callee.clone());
            result.edges.push(ExtractedEdge {
                source: file_node_id.to_string(),
                target: callee,
                edge_type: "calls_raw".to_string(),
                properties,
            });
        }
    } else if kind == "method_invocation" {
        // Java call site: `obj.method(...)` exposes the method via `name`.
        if let Some(name_node) = node.child_by_field_name("name") {
            let callee = get_node_text(name_node, source);
            let mut properties = HashMap::new();
            properties.insert("raw".to_string(), callee.clone());
            result.edges.push(ExtractedEdge {
                source: file_node_id.to_string(),
                target: callee,
                edge_type: "calls_raw".to_string(),
                properties,
            });
        }
    } else if matches!(
        kind,
        "import_statement"
            | "import_from_statement"
            | "import_declaration"
            | "import_spec"
            | "use_declaration"
            | "preproc_include"
    ) {
        // The raw module/path string (no longer an `"import_target"` placeholder):
        // `resolve::resolve_import` maps it to the in-batch file that defines it
        // to build a resolved `depends_on` edge. Unreadable imports emit nothing.
        if let Some(module) = import_module(node, source) {
            let mut properties = HashMap::new();
            properties.insert("raw".to_string(), module.clone());
            result.edges.push(ExtractedEdge {
                source: file_node_id.to_string(),
                target: module,
                edge_type: "depends_on_raw".to_string(),
                properties,
            });
        }
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        walk_node(
            child,
            source,
            file_path,
            language,
            file_node_id,
            &descend_scope,
            result,
        );
    }
}

fn get_node_text(node: Node, source: &[u8]) -> String {
    let bytes = &source[node.start_byte()..node.end_byte()];
    String::from_utf8_lossy(bytes).into_owned()
}

/// Extract the imported module/path string from an import-like node across
/// grammars, or `None` when it can't be read. Python `import a.b` /
/// `from a.b import x`, JS/TS `import … from "src"`, Go `import_spec` path,
/// Rust `use a::b`, Java `import a.b.C`, C/C++ `#include "x"`. The raw string is
/// resolved to a file downstream in [`super::resolve::resolve_import`].
fn import_module(node: Node, source: &[u8]) -> Option<String> {
    match node.kind() {
        // Python `from <module_name> import …`.
        "import_from_statement" => node
            .child_by_field_name("module_name")
            .map(|n| get_node_text(n, source)),
        // `import_statement` is shared but differs structurally: JS/TS expose a
        // `source` field (the string literal), Python a `name` field (dotted_name).
        "import_statement" => node
            .child_by_field_name("source")
            .or_else(|| node.child_by_field_name("name"))
            .map(|n| get_node_text(n, source)),
        // Go `import_spec` carries the path string literal.
        "import_spec" => node
            .child_by_field_name("path")
            .map(|n| get_node_text(n, source)),
        // Rust `use a::b::c;`.
        "use_declaration" => node
            .child_by_field_name("argument")
            .map(|n| get_node_text(n, source)),
        // Java `import a.b.C;` — no named field; take the scoped identifier child.
        // (Go's `import_declaration` wraps `import_spec`s with no such child, so it
        // falls through to `None` here and resolves at the `import_spec` level.)
        "import_declaration" => {
            let mut cursor = node.walk();
            let found = node
                .children(&mut cursor)
                .find(|c| c.kind() == "scoped_identifier" || c.kind() == "identifier")
                .map(|c| get_node_text(c, source));
            found
        }
        // C/C++ `#include "x"` or `<x>`.
        "preproc_include" => node
            .child_by_field_name("path")
            .map(|n| get_node_text(n, source)),
        _ => None,
    }
}

/// Parse many files in one call (CONCEPT:EG-KG.compute.graph-compute-engine batch op). Files are parsed
/// independently and in parallel via rayon (tree-sitter is stateless per call);
/// a file that fails to parse yields an empty [`ParseResult`] in its slot, so
/// the output is 1:1 with — and in the same order as — the input. This is the
/// engine-side primitive behind the `ParseFiles` protocol op: one round-trip
/// instead of N.
// CONCEPT:EG-KG.compute.parse-resolve-span — span AST parse throughput (mirrors the Python-side phase spans).
// `n_files`/`total_bytes` are carried as span fields; a no-op without a subscriber.
#[tracing::instrument(
    skip(files),
    fields(n_files = files.len(), total_bytes = files.iter().map(|(_, b)| b.len()).sum::<usize>())
)]
pub fn parse_files(files: &[(String, Vec<u8>)]) -> Vec<ParseResult> {
    use rayon::prelude::*;
    files
        .par_iter()
        .map(|(path, src)| {
            parse_file(path, src).unwrap_or(ParseResult {
                nodes: Vec::new(),
                edges: Vec::new(),
                symbols_extracted: 0,
            })
        })
        .collect()
}

// ── CONCEPT:EG-KG.compute.model-free-similar-code — model-free code-similarity signature ──────────────────
// A MinHash signature over a symbol's normalized AST-leaf trigrams. Identifiers/
// strings/numbers/types are abstracted to class tokens (so a renamed-variable
// clone still matches) while keywords/operators/punctuation are kept verbatim (so
// structure is preserved). The cross-file resolver LSH-bands these signatures into
// `similar_to` edges — model-free, so code similarity survives the embedder being
// offline (the recurring GB10 502s).

/// Number of MinHash permutations (signature width).
pub(crate) const MINHASH_K: usize = 32;

/// FNV-1a 64-bit hash of a shingle string (stable across processes, unlike the
/// std hasher's randomized state).
fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Map a leaf node to its normalized shingle token: identifiers/strings/numbers/
/// types collapse to a class char; everything else (keywords, operators,
/// punctuation) keeps its literal text. Comments/whitespace yield empty.
fn normalize_leaf(node: Node, source: &[u8]) -> String {
    match node.kind() {
        "comment" | "line_comment" | "block_comment" => String::new(),
        "identifier"
        | "field_identifier"
        | "property_identifier"
        | "shorthand_property_identifier"
        | "namespace_identifier" => "I".to_string(),
        "type_identifier" | "primitive_type" => "T".to_string(),
        "string"
        | "string_literal"
        | "string_content"
        | "raw_string_literal"
        | "interpreted_string_literal"
        | "char_literal"
        | "character_literal" => "S".to_string(),
        "integer" | "float" | "number" | "integer_literal" | "float_literal"
        | "numeric_literal" => "N".to_string(),
        _ => get_node_text(node, source).trim().to_string(),
    }
}

/// Collect a symbol subtree's leaf tokens (childless nodes) in source order.
fn collect_leaf_tokens(node: Node, source: &[u8], out: &mut Vec<String>) {
    let mut cursor = node.walk();
    let mut has_child = false;
    for child in node.children(&mut cursor) {
        has_child = true;
        collect_leaf_tokens(child, source, out);
    }
    if !has_child {
        let t = normalize_leaf(node, source);
        if !t.is_empty() {
            out.push(t);
        }
    }
}

/// MinHash signature of a symbol's normalized AST-leaf trigrams. Empty/tiny
/// symbols fall back to uni-/bi-grams so they still produce a signature.
pub(crate) fn symbol_minhash(node: Node, source: &[u8]) -> [u32; MINHASH_K] {
    let mut tokens: Vec<String> = Vec::new();
    collect_leaf_tokens(node, source, &mut tokens);

    let mut shingles: Vec<u64> = Vec::new();
    if tokens.len() >= 3 {
        for w in tokens.windows(3) {
            shingles.push(fnv1a(&w.join("\u{1}")));
        }
    } else {
        for t in &tokens {
            shingles.push(fnv1a(t));
        }
    }

    let mut sig = [u32::MAX; MINHASH_K];
    for &h in &shingles {
        for (i, slot) in sig.iter_mut().enumerate() {
            // K independent linear permutations of the base hash (top 32 bits).
            let a = ((i as u64) * 2 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let b = ((i as u64) + 1).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
            let v = (h.wrapping_mul(a).wrapping_add(b) >> 32) as u32;
            if v < *slot {
                *slot = v;
            }
        }
    }
    sig
}

/// Hex-encode a MinHash signature for storage as a node property.
pub(crate) fn encode_minhash(sig: &[u32; MINHASH_K]) -> String {
    let mut s = String::with_capacity(MINHASH_K * 8);
    for v in sig {
        s.push_str(&format!("{v:08x}"));
    }
    s
}

/// Decode a hex MinHash signature; `None` if malformed. Returns `None` for the
/// all-empty signature (a symbol with no shingles is similar to nothing).
pub(crate) fn decode_minhash(s: &str) -> Option<[u32; MINHASH_K]> {
    if s.len() != MINHASH_K * 8 {
        return None;
    }
    let mut sig = [0u32; MINHASH_K];
    let mut all_max = true;
    for (i, slot) in sig.iter_mut().enumerate() {
        *slot = u32::from_str_radix(&s[i * 8..i * 8 + 8], 16).ok()?;
        if *slot != u32::MAX {
            all_max = false;
        }
    }
    if all_max {
        None
    } else {
        Some(sig)
    }
}

/// Estimated Jaccard similarity = fraction of matching MinHash positions.
pub(crate) fn minhash_jaccard(a: &[u32; MINHASH_K], b: &[u32; MINHASH_K]) -> f64 {
    let matches = a.iter().zip(b.iter()).filter(|(x, y)| x == y).count();
    matches as f64 / MINHASH_K as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn func_props(src: &str, name: &str) -> HashMap<String, String> {
        let r = parse_file("t.py", src.as_bytes()).unwrap();
        r.nodes
            .into_iter()
            .find(|n| n.properties.get("name").map(|s| s.as_str()) == Some(name))
            .unwrap_or_else(|| panic!("no function {name}"))
            .properties
    }

    #[test]
    fn mock_heavy_test_metrics() {
        let src = r#"
from unittest.mock import patch, MagicMock

@patch("mod.thing")
def test_mock_heavy(mock_thing, db, cache):
    m = MagicMock()
    m.foo()
    assert m.called
"#;
        let p = func_props(src, "test_mock_heavy");
        assert_eq!(p["is_test"], "true");
        assert_eq!(p["assert_count"], "1");
        // MagicMock() call + @patch decorator
        assert_eq!(p["mock_count"], "2");
        assert_eq!(p["fixture_count"], "3");
        assert_eq!(p["is_skipped"], "false");
    }

    #[test]
    fn skipped_and_raises_metrics() {
        let src = r#"
import pytest

@pytest.mark.skip(reason="flaky")
def test_dormant():
    with pytest.raises(ValueError):
        do_thing()
"#;
        let p = func_props(src, "test_dormant");
        assert_eq!(p["is_test"], "true");
        assert_eq!(p["is_skipped"], "true");
        assert_eq!(p["marks"], "skip");
        assert_eq!(p["raises_count"], "1");
        assert_eq!(p["assert_count"], "0");
    }

    #[test]
    fn non_test_function_marked() {
        let src = "def helper(a, b):\n    return a + b\n";
        let p = func_props(src, "helper");
        assert_eq!(p["is_test"], "false");
        assert_eq!(p["fixture_count"], "2");
    }

    fn class_props(src: &str, name: &str) -> HashMap<String, String> {
        let r = parse_file("t.py", src.as_bytes()).unwrap();
        r.nodes
            .into_iter()
            .find(|n| {
                n.properties.get("symbol_type").map(|s| s.as_str()) == Some("Class")
                    && n.properties.get("name").map(|s| s.as_str()) == Some(name)
            })
            .unwrap_or_else(|| panic!("no class {name}"))
            .properties
    }

    #[test]
    fn class_facts_for_pattern_detection() {
        let src = r#"
from abc import ABC, abstractmethod

@final
class Strategy(ABC, Base):
    @abstractmethod
    def run(self): ...
    def __enter__(self): return self
    def __exit__(self, *a): ...
"#;
        let p = class_props(src, "Strategy");
        assert_eq!(p["bases"], "ABC,Base");
        assert_eq!(p["is_abstract"], "true");
        assert_eq!(p["decorators"], "final");
        let methods: Vec<&str> = p["methods"].split(',').collect();
        assert!(methods.contains(&"run"));
        assert!(methods.contains(&"__enter__"));
        assert!(methods.contains(&"__exit__"));
    }

    /// Find a symbol by name in a parsed file, returning its properties.
    fn sym(path: &str, src: &str, name: &str) -> HashMap<String, String> {
        let r =
            parse_file(path, src.as_bytes()).unwrap_or_else(|e| panic!("parse {path} failed: {e}"));
        r.nodes
            .into_iter()
            .find(|n| n.properties.get("name").map(|s| s.as_str()) == Some(name))
            .unwrap_or_else(|| panic!("no symbol {name} in {path}"))
            .properties
    }

    #[test]
    fn java_class_and_method() {
        let src = r#"
public class Widget {
    private int value;
    public int getValue() { return value; }
}
interface Drawable { void draw(); }
"#;
        let c = sym("Widget.java", src, "Widget");
        assert_eq!(c["symbol_type"], "Class");
        assert_eq!(c["kind_detail"], "class");
        assert_eq!(c["language"], "java");
        let m = sym("Widget.java", src, "getValue");
        assert_eq!(m["symbol_type"], "Function");
        assert_eq!(m["kind_detail"], "method");
        let i = sym("Widget.java", src, "Drawable");
        assert_eq!(i["kind_detail"], "interface");
    }

    #[test]
    fn rust_struct_trait_fn() {
        let src = r#"
pub struct Point { x: i32, y: i32 }
pub trait Shape { fn area(&self) -> f64; }
pub fn make_point() -> Point { Point { x: 0, y: 0 } }
"#;
        assert_eq!(sym("m.rs", src, "Point")["kind_detail"], "struct");
        assert_eq!(sym("m.rs", src, "Shape")["kind_detail"], "trait");
        let f = sym("m.rs", src, "make_point");
        assert_eq!(f["symbol_type"], "Function");
        assert_eq!(f["language"], "rust");
    }

    #[test]
    fn go_func_and_struct() {
        let src = r#"
package main
type Server struct { addr string }
func NewServer(a string) *Server { return &Server{addr: a} }
func (s *Server) Start() error { return nil }
"#;
        assert_eq!(sym("s.go", src, "Server")["kind_detail"], "type");
        let f = sym("s.go", src, "NewServer");
        assert_eq!(f["symbol_type"], "Function");
        assert_eq!(f["language"], "go");
        assert_eq!(sym("s.go", src, "Start")["kind_detail"], "method");
    }

    #[test]
    fn c_function_via_declarator() {
        // C function names nest under the declarator (no `name` field).
        let src = "int add(int a, int b) { return a + b; }\nstruct Pt { int x; };\n";
        let f = sym("a.c", src, "add");
        assert_eq!(f["symbol_type"], "Function");
        assert_eq!(f["language"], "c");
        assert_eq!(sym("a.c", src, "Pt")["kind_detail"], "struct");
    }

    #[test]
    fn typescript_interface_and_function() {
        let src = r#"
export interface User { id: number; name: string; }
export function greet(u: User): string { return u.name; }
"#;
        assert_eq!(sym("u.ts", src, "User")["kind_detail"], "interface");
        let f = sym("u.ts", src, "greet");
        assert_eq!(f["symbol_type"], "Function");
        assert_eq!(f["language"], "typescript");
    }

    #[test]
    fn csharp_class_and_method() {
        let src = r#"
namespace App {
    public class Service {
        public int Compute(int n) { return n * 2; }
    }
}
"#;
        assert_eq!(sym("S.cs", src, "Service")["kind_detail"], "class");
        let m = sym("S.cs", src, "Compute");
        assert_eq!(m["kind_detail"], "method");
        assert_eq!(m["language"], "csharp");
    }

    #[test]
    fn python_still_carries_language_and_metrics() {
        // Regression: Python keeps its rich metrics AND gains the language field.
        let p = func_props("def helper(a, b):\n    return a + b\n", "helper");
        assert_eq!(p["language"], "python");
        assert_eq!(p["fixture_count"], "2");
    }

    #[test]
    fn calls_collected_across_languages() {
        // Java: method_invocation inside a method body.
        let java = "class A { int f(){ return g() + this.h(); } int g(){return 1;} }";
        let cj: Vec<String> = sym("A.java", java, "f")["calls"]
            .split(',')
            .map(|s| s.to_string())
            .collect();
        assert!(
            cj.contains(&"g".to_string()) && cj.contains(&"h".to_string()),
            "{cj:?}"
        );

        // Rust: call_expression incl. a path call (helper::run -> run).
        let rust = "fn outer() { inner(); helper::run(); }";
        let cr: Vec<String> = sym("m.rs", rust, "outer")["calls"]
            .split(',')
            .map(|s| s.to_string())
            .collect();
        assert!(
            cr.contains(&"inner".to_string()) && cr.contains(&"run".to_string()),
            "{cr:?}"
        );

        // Go: call_expression.
        let go = "package m\nfunc Outer() { doThing(); fmt.Println(\"x\") }";
        let cg: Vec<String> = sym("m.go", go, "Outer")["calls"]
            .split(',')
            .map(|s| s.to_string())
            .collect();
        assert!(
            cg.contains(&"doThing".to_string()) && cg.contains(&"Println".to_string()),
            "{cg:?}"
        );
    }

    #[cfg(feature = "ast-extended")]
    #[test]
    fn extended_languages_extract_symbols() {
        // Ruby class + method.
        let rb = sym(
            "a.rb",
            "class Widget\n  def run\n    1\n  end\nend\n",
            "Widget",
        );
        assert_eq!(rb["symbol_type"], "Class");
        assert_eq!(rb["language"], "ruby");
        assert_eq!(
            sym(
                "a.rb",
                "class Widget\n  def run\n    1\n  end\nend\n",
                "run"
            )["symbol_type"],
            "Function"
        );
        // PHP function.
        let php = sym(
            "a.php",
            "<?php\nfunction greet($n) { return $n; }\n",
            "greet",
        );
        assert_eq!(php["symbol_type"], "Function");
        assert_eq!(php["language"], "php");
        // Bash function.
        assert_eq!(
            sym("a.sh", "deploy() {\n  echo hi\n}\n", "deploy")["symbol_type"],
            "Function"
        );
        // Scala object + def.
        assert_eq!(
            sym("a.scala", "object App {\n  def run(): Int = 1\n}\n", "run")["symbol_type"],
            "Function"
        );
        // Lua function.
        assert_eq!(
            sym("a.lua", "function compute(x)\n  return x\nend\n", "compute")["symbol_type"],
            "Function"
        );
    }

    #[test]
    fn parse_files_preserves_order_and_is_fault_tolerant() {
        let files: Vec<(String, Vec<u8>)> = vec![
            ("a.py".into(), b"def a():\n    return 1\n".to_vec()),
            // Unsupported extension → parse_file errs → empty slot, no abort.
            ("b.txt".into(), b"not python".to_vec()),
            ("c.py".into(), b"class C:\n    def m(self): ...\n".to_vec()),
        ];
        let results = parse_files(&files);
        assert_eq!(results.len(), 3, "one result per input, order preserved");
        // a.py: function 'a' present.
        assert!(results[0]
            .nodes
            .iter()
            .any(|n| n.properties.get("name").map(|s| s.as_str()) == Some("a")));
        // b.txt: unsupported → empty result, not an error.
        assert!(results[1].nodes.is_empty());
        // c.py: class 'C' present (parity with single-file parse_file).
        assert!(results[2]
            .nodes
            .iter()
            .any(|n| n.properties.get("name").map(|s| s.as_str()) == Some("C")));
    }
}
