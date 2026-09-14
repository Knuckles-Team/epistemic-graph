use super::ast::{
    class_like_kind, class_relations, collect_call_sites, collect_test_metrics, encode_call_sites,
    function_like_kind, join_qualified, param_count, py_class_methods, py_decorators, py_marks,
    py_param_count, qual_segment, symbol_name, CallSite, TestMetrics,
};
use super::{
    encode_minhash, get_node_text, import_module, symbol_minhash, ExtractedEdge, ExtractedNode,
    ParseResult,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};
use tree_sitter::Node;

/// Per-file walk state: the growing [`ParseResult`] plus the occurrence counters
/// that give each declaration a unique id.
///
/// CONCEPT:EG-KG.compute.symbol-occurrence-id — OCCURRENCE identity, separate
/// from CONTENT identity. A SYMBOL's `node_id` used to be
/// `symbol:<sha256 of the declaration bytes>`, which is a content address:
/// byte-identical declarations in different files (or twice in one file)
/// collapsed onto ONE id, so every edge touching such an id was ambiguous about
/// which declaration it meant. Content identity is still wanted — it is exactly
/// what clone detection and `similar_to` are built on — so it stays unchanged as
/// the `ast_hash` property (with `minhash` for near-duplicates). The id now
/// answers a different question: WHICH declaration site is this.
///
/// The id is `symbol:<sha256 of (file_path, symbol_type, qualified_symbol,
/// ordinal)>`, where `ordinal` counts the declarations of that
/// (symbol_type, qualified_symbol) pair already emitted in the SAME file.
/// Chosen over the obvious `file_path + start_byte` because a byte offset
/// changes whenever anything ABOVE the declaration changes — inserting one line
/// at the top of a file would rewrite every id below it, and this corpus is
/// diffed run over run. The (path, qualified name, ordinal) key only moves when
/// a same-named sibling is added or removed before it in the same file, which is
/// rare (overloads, `#[cfg]`-duplicated items). Fields are length-prefixed
/// before hashing, so the encoding is injective.
pub(super) struct WalkState {
    pub(super) result: ParseResult,
    /// (symbol_type, qualified_symbol) -> declarations already emitted in this file.
    occurrences: HashMap<(String, String), u64>,
    file_path: String,
    language: &'static str,
    file_node_id: String,
}

impl WalkState {
    pub(super) fn new(file_path: &str, language: &'static str) -> Self {
        Self {
            result: ParseResult {
                nodes: Vec::new(),
                edges: Vec::new(),
                symbols_extracted: 0,
            },
            occurrences: HashMap::new(),
            file_path: file_path.to_string(),
            language,
            file_node_id: format!("file:{file_path}"),
        }
    }

    /// The 0-based ordinal for the NEXT declaration of this
    /// (symbol_type, qualified_symbol) in this file.
    fn next_ordinal(&mut self, symbol_type: &str, qualified_symbol: &str) -> u64 {
        let slot = self
            .occurrences
            .entry((symbol_type.to_string(), qualified_symbol.to_string()))
            .or_insert(0);
        let ordinal = *slot;
        *slot += 1;
        ordinal
    }
}

/// sha256 over LENGTH-PREFIXED fields. Prefixing makes the encoding injective —
/// plain concatenation would let two different field tuples collide (`("ab","c")`
/// vs `("a","bc")`). Lengths are little-endian u64 so the digest is byte-identical
/// on every platform and every run.
pub(super) fn hash_fields(fields: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for field in fields {
        hasher.update((field.len() as u64).to_le_bytes());
        hasher.update(field.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Build a SYMBOL node (id = per-occurrence identity, see [`WalkState`]) + an
/// IMPLEMENTS edge from its file,
/// stamping the common facts (name, symbol_type, kind_detail, language, line,
/// qualified_symbol, end_line/start_col/end_col/start_byte/end_byte, ast_hash,
/// occurrence_index, file_path) plus any language-specific ``extra`` properties.
#[allow(clippy::too_many_arguments)]
fn emit_symbol(
    node: Node,
    source: &[u8],
    symbol_type: &str,
    kind_detail: &str,
    name: String,
    qualified_symbol: String,
    extra: HashMap<String, String>,
    state: &mut WalkState,
) {
    let (content_hash, symbol_id, ordinal) =
        symbol_identity(node, source, symbol_type, &qualified_symbol, state);
    let properties = symbol_properties(
        SymbolInput {
            node,
            source,
            symbol_type,
            kind_detail,
            name,
            qualified_symbol,
            content_hash,
            ordinal,
            extra,
        },
        state,
    );
    append_symbol_result(state, symbol_id, properties);
}

fn symbol_identity(
    node: Node,
    source: &[u8],
    symbol_type: &str,
    qualified_symbol: &str,
    state: &mut WalkState,
) -> (String, String, u64) {
    // CONTENT identity: the sha256 of the declaration bytes. Two byte-identical
    // declarations SHOULD share it — that is what clone detection reads.
    let content_bytes = &source[node.start_byte()..node.end_byte()];
    let mut hasher = Sha256::new();
    hasher.update(content_bytes);
    let content_hash = format!("{:x}", hasher.finalize());
    // OCCURRENCE identity: unique per declaration site (see `WalkState`).
    let ordinal = state.next_ordinal(symbol_type, qualified_symbol);
    let symbol_id = format!(
        "symbol:{}",
        hash_fields(&[
            state.file_path.as_str(),
            symbol_type,
            qualified_symbol,
            &ordinal.to_string(),
        ])
    );
    (content_hash, symbol_id, ordinal)
}

struct SymbolInput<'tree, 'source> {
    node: Node<'tree>,
    source: &'source [u8],
    symbol_type: &'source str,
    kind_detail: &'source str,
    name: String,
    qualified_symbol: String,
    content_hash: String,
    ordinal: u64,
    extra: HashMap<String, String>,
}

fn symbol_properties(input: SymbolInput<'_, '_>, state: &WalkState) -> HashMap<String, String> {
    let mut properties = base_symbol_properties(&input, state);
    append_symbol_range(&mut properties, input.node);
    append_symbol_identity(&mut properties, &input, state);
    properties.extend(input.extra);
    properties
}

fn base_symbol_properties(
    input: &SymbolInput<'_, '_>,
    state: &WalkState,
) -> HashMap<String, String> {
    let mut properties = HashMap::new();
    properties.insert("name".to_string(), input.name.clone());
    properties.insert("symbol_type".to_string(), input.symbol_type.to_string());
    properties.insert("kind_detail".to_string(), input.kind_detail.to_string());
    properties.insert("language".to_string(), state.language.to_string());
    properties.insert(
        "line".to_string(),
        (input.node.start_position().row + 1).to_string(),
    );
    // CONCEPT:EG-KG.compute.qualified-symbol — lexically qualified name (see the
    // rules above `scope_sep`). `name` stays BARE for existing consumers.
    properties.insert(
        "qualified_symbol".to_string(),
        input.qualified_symbol.clone(),
    );
    properties
}

fn append_symbol_range(properties: &mut HashMap<String, String>, node: Node) {
    // Full declaration range. `line` (1-based start line) is unchanged for
    // compatibility; these are additive. Byte offsets are half-open
    // [start_byte, end_byte); lines are 1-based; columns are 1-based BYTE
    // columns within their line, with `end_col` exclusive.
    properties.insert(
        "end_line".to_string(),
        (node.end_position().row + 1).to_string(),
    );
    properties.insert(
        "start_col".to_string(),
        (node.start_position().column + 1).to_string(),
    );
    properties.insert(
        "end_col".to_string(),
        (node.end_position().column + 1).to_string(),
    );
    properties.insert("start_byte".to_string(), node.start_byte().to_string());
    properties.insert("end_byte".to_string(), node.end_byte().to_string());
}

fn append_symbol_identity(
    properties: &mut HashMap<String, String>,
    input: &SymbolInput<'_, '_>,
    state: &WalkState,
) {
    properties.insert("ast_hash".to_string(), input.content_hash.clone());
    // Which declaration of this qualified name in this file (0-based) — the
    // ordinal the occurrence id was derived from, kept readable for consumers
    // that want to reconstruct or explain the id.
    properties.insert("occurrence_index".to_string(), input.ordinal.to_string());
    properties.insert("file_path".to_string(), state.file_path.clone());
    // CONCEPT:EG-KG.compute.model-free-similar-code — model-free similarity signature (MinHash over normalized
    // AST leaf trigrams). The cross-file resolver LSH-bands these into `similar_to`
    // edges; it is a resolution-only input and is stripped from the graph nodes.
    properties.insert(
        "minhash".to_string(),
        encode_minhash(&symbol_minhash(input.node, input.source)),
    );
}

fn append_symbol_result(
    state: &mut WalkState,
    symbol_id: String,
    properties: HashMap<String, String>,
) {
    state.result.nodes.push(ExtractedNode {
        node_id: symbol_id.clone(),
        node_type: "SYMBOL".to_string(),
        properties,
    });
    state.result.edges.push(ExtractedEdge {
        source: state.file_node_id.clone(),
        target: symbol_id,
        edge_type: "IMPLEMENTS".to_string(),
        properties: HashMap::new(),
    });
    state.result.symbols_extracted += 1;
}

pub(super) fn walk_node(
    node: Node,
    source: &[u8],
    scope: &str,
    qual: &[String],
    state: &mut WalkState,
) {
    // CONCEPT:EG-KG.compute.qualified-symbol — the lexical ancestor chain children
    // inherit. Allocates only at a real container, not at every AST node.
    let language = state.language;
    let descend_qual_owned: Option<Vec<String>> = qual_segment(node, source, language).map(|seg| {
        let mut v = Vec::with_capacity(qual.len() + 1);
        v.extend_from_slice(qual);
        v.push(seg);
        v
    });
    let descend_qual: &[String] = descend_qual_owned.as_deref().unwrap_or(qual);
    let descend_scope = visit_node(node, source, scope, qual, state);

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        walk_node(child, source, &descend_scope, descend_qual, state);
    }
}

fn visit_node(
    node: Node,
    source: &[u8],
    scope: &str,
    qual: &[String],
    state: &mut WalkState,
) -> String {
    if let Some(detail) = class_like_kind(node.kind()) {
        return emit_class_symbol(node, source, detail, scope, qual, state);
    }
    if let Some(detail) = function_like_kind(node.kind()) {
        emit_function_symbol(node, source, detail, scope, qual, state);
    } else {
        emit_raw_edge(node, source, state);
    }
    scope.to_string()
}

fn emit_class_symbol(
    node: Node,
    source: &[u8],
    detail: &str,
    scope: &str,
    qual: &[String],
    state: &mut WalkState,
) -> String {
    let Some(name) = symbol_name(node, source).filter(|n| !n.is_empty()) else {
        return scope.to_string();
    };
    let mut extra = HashMap::new();
    // CONCEPT:EG-KG.compute.type-scope-resolved-call — inheritance/realization facts across grammars.
    let (inherits, realizes) = class_relations(node, source, state.language);
    extra.insert("scope".to_string(), scope.to_string());
    extra.insert("bases".to_string(), inherits.join(","));
    extra.insert("interfaces".to_string(), realizes.join(","));
    // CONCEPT:EG-KG.storage.nonblocking-checkpoint — Python structural facts for design-pattern detection.
    if state.language == "python" {
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
    let qualified = join_qualified(qual, &name, state.language);
    emit_symbol(
        node,
        source,
        "Class",
        detail,
        name.clone(),
        qualified,
        extra,
        state,
    );
    name
}

fn emit_function_symbol(
    node: Node,
    source: &[u8],
    detail: &str,
    scope: &str,
    qual: &[String],
    state: &mut WalkState,
) {
    let Some(name) = symbol_name(node, source).filter(|n| !n.is_empty()) else {
        return;
    };
    let mut extra = HashMap::new();
    extra.insert("scope".to_string(), scope.to_string());
    // Structured call sites (receiver/callee/argc) for type/scope-resolved
    // call edges, plus the bare callee names kept as `calls` for the
    // name-only fallback + COVERS. (CONCEPT:EG-KG.compute.type-scope-resolved-call / KG-2.8)
    let mut sites = BTreeSet::new();
    collect_call_sites(node, source, &mut sites);
    let sites: Vec<CallSite> = sites.into_iter().collect();
    let calls: Vec<String> = sites
        .iter()
        .map(|site| site.callee.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    extra.insert("calls".to_string(), calls.join(","));
    extra.insert("call_sites".to_string(), encode_call_sites(&sites));
    extra.insert(
        "arity".to_string(),
        param_count(node, source, state.language).to_string(),
    );

    if state.language == "python" {
        append_python_function_metrics(node, source, &name, &mut extra);
    }
    let qualified = join_qualified(qual, &name, state.language);
    emit_symbol(
        node, source, "Function", detail, name, qualified, extra, state,
    );
}

/// Add Python test/decorator facts after the language-independent function facts.
fn append_python_function_metrics(
    node: Node,
    source: &[u8],
    name: &str,
    extra: &mut HashMap<String, String>,
) {
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

fn emit_raw_edge(node: Node, source: &[u8], state: &mut WalkState) {
    match node.kind() {
        "call" | "call_expression" => node
            .child_by_field_name("function")
            .map(|function| get_node_text(function, source))
            .map(|callee| append_raw_edge(callee, "calls_raw", state)),
        "method_invocation" => node
            .child_by_field_name("name")
            .map(|name| get_node_text(name, source))
            .map(|callee| append_raw_edge(callee, "calls_raw", state)),
        "import_statement"
        | "import_from_statement"
        | "import_declaration"
        | "import_spec"
        | "use_declaration"
        | "preproc_include" => import_module(node, source)
            .map(|module| append_raw_edge(module, "depends_on_raw", state)),
        _ => None,
    };
}

fn append_raw_edge(target: String, edge_type: &str, state: &mut WalkState) {
    let properties = HashMap::from([("raw".to_string(), target.clone())]);
    state.result.edges.push(ExtractedEdge {
        source: state.file_node_id.clone(),
        target,
        edge_type: edge_type.to_string(),
        properties,
    });
}
