use serde::{Deserialize, Serialize};
use tree_sitter::{Language, Node, Parser};

use eg_types::{
    contract::BoundedVec,
    ingestion_wire::{IndexDiagnostic, IndexFileOutcome, IndexFileStatus},
};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize, Debug)]
pub struct SymbolMetadata {
    pub name: String,
    pub symbol_type: String, // Class, Function, etc.
    pub line: usize,
    pub docstring: Option<String>,
    pub args: Vec<String>,
}

pub use eg_types::ingestion_wire::{ExtractedEdge, ExtractedNode, ParseResult};

#[path = "tree_sitter_ast.rs"]
mod ast;
#[path = "tree_sitter_sql.rs"]
mod sql;
#[path = "tree_sitter_walk.rs"]
mod walk;
// CONCEPT:EH-281 grammar expansion — the extended-language grammar TABLE lives in its
// own file so this one stays small as the tier grows (a table row per language, never
// a new function). See `grammars_extended::lookup` and its module doc.
#[cfg(feature = "ast-extended")]
#[path = "tree_sitter_grammars_extended.rs"]
mod grammars_extended;

#[cfg(test)]
use ast::MAX_SYMBOL_CALL_SITES;
pub(crate) use ast::{decode_call_sites, DecodedSite};
#[cfg(test)]
use walk::hash_fields;

/// A grammar constructor, deferred so [`CORE_LANGUAGES`] can be a plain data
/// table (rather than one `match` arm per extension) with no runtime cost —
/// the closures are non-capturing and coerce to bare `fn` pointers.
type LangCtor = fn() -> Language;

/// `(extensions, grammar constructor, stable language label)` for every
/// core-tier grammar. Kept as data (not a `match`) so adding a language is a
/// table row, and so the extension lookup is a linear scan rather than a
/// cyclomatic-heavy dispatch — this is string matching with an explicit
/// non-exhaustive fallback ([`lang_for_path_extended`]), not an enum, so there
/// is no compile-time exhaustiveness guarantee here to trade away.
const CORE_LANGUAGES: &[(&[&str], LangCtor, &str)] = &[
    (
        &["py", "pyi"],
        || tree_sitter_python::LANGUAGE.into(),
        "python",
    ),
    (
        &["js", "jsx", "mjs", "cjs"],
        || tree_sitter_javascript::LANGUAGE.into(),
        "javascript",
    ),
    (
        &["ts", "mts", "cts"],
        || tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
        "typescript",
    ),
    (
        &["tsx"],
        || tree_sitter_typescript::LANGUAGE_TSX.into(),
        "typescript",
    ),
    (&["go"], || tree_sitter_go::LANGUAGE.into(), "go"),
    (&["rs"], || tree_sitter_rust::LANGUAGE.into(), "rust"),
    (&["java"], || tree_sitter_java::LANGUAGE.into(), "java"),
    (&["c", "h"], || tree_sitter_c::LANGUAGE.into(), "c"),
    (
        &["cpp", "cc", "cxx", "hpp", "hxx", "hh", "c++"],
        || tree_sitter_cpp::LANGUAGE.into(),
        "cpp",
    ),
    // CUDA (CONCEPT:EH-281) reuses the C++ grammar rather than a dedicated
    // crate: CUDA device code is a C++ superset, and crates.io's only
    // maintained `tree-sitter-cuda` is generated at tree-sitter ABI 15, which
    // this crate's ABI-14 core can't load (see the ABI note in Cargo.toml).
    // GPU-specific syntax (`__global__`, kernel-launch `<<<...>>>`) that
    // tree-sitter-cpp doesn't model may parse as an error node, but an
    // ordinary function/type in a `.cu`/`.cuh` file resolves exactly as it
    // would in a `.cpp` file — same grammar, a distinct language label so
    // graph queries can still tell CUDA files apart from C++.
    (&["cu", "cuh"], || tree_sitter_cpp::LANGUAGE.into(), "cuda"),
    (&["cs"], || tree_sitter_c_sharp::LANGUAGE.into(), "csharp"),
    (
        &["sql", "ddl"],
        || tree_sitter_sequel::LANGUAGE.into(),
        "sql",
    ),
];

/// Resolve a file path to its tree-sitter grammar plus a stable language label
/// (the label is stamped on every extracted symbol so the graph can answer
/// "show me all Java code" and compute per-language metrics). Returns ``None``
/// for paths we don't have a grammar for.
fn lang_for_path(file_path: &str) -> Option<(Language, &'static str)> {
    let ext = normalized_extension(file_path);
    if let Some((ctor, label)) = core_language_entry(&ext) {
        return Some((ctor(), label));
    }
    lang_for_path_extended(&ext)
}

fn normalized_extension(file_path: &str) -> String {
    file_path
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
}

/// Look up `ext` in a `(extensions, ctor, label)` table by linear scan — the
/// shape both [`CORE_LANGUAGES`] and the extended tier's table
/// (`grammars_extended`) share, so growing either tier is a table row, never
/// a second copy of this scan or a new dispatch branch.
fn language_table_entry(
    table: &[(&[&str], LangCtor, &'static str)],
    ext: &str,
) -> Option<(LangCtor, &'static str)> {
    for &(exts, ctor, label) in table {
        if exts.contains(&ext) {
            return Some((ctor, label));
        }
    }
    None
}

/// Look up `ext` in [`CORE_LANGUAGES`], returning the matching grammar
/// constructor and label.
fn core_language_entry(ext: &str) -> Option<(LangCtor, &'static str)> {
    language_table_entry(CORE_LANGUAGES, ext)
}

/// Extended-language tier (CONCEPT:AU-KG.compute.built-ast-extended), compiled only with `ast-extended`.
/// Without the feature it resolves nothing, so a slim `ast` build stays lean.
/// The table itself lives in `grammars_extended` (CONCEPT:EH-281).
#[cfg(feature = "ast-extended")]
fn lang_for_path_extended(ext: &str) -> Option<(Language, &'static str)> {
    grammars_extended::lookup(ext)
}

#[cfg(not(feature = "ast-extended"))]
fn lang_for_path_extended(_ext: &str) -> Option<(Language, &'static str)> {
    None
}

/// Extensions the parser can ingest — kept in sync with [`lang_for_path`] and
/// mirrored by the Python file-discovery walk.
pub const SUPPORTED_EXTENSIONS: &[&str] = &[
    "py", "pyi", "js", "jsx", "mjs", "cjs", "ts", "mts", "cts", "tsx", "go", "rs", "java", "c",
    "h", "cpp", "cc", "cxx", "hpp", "hxx", "hh", "cu",
    "cuh", // CUDA (CONCEPT:EH-281), reuses cpp.
    "cs",  // SQL DDL (CONCEPT:AU-KG.ontology.emits-database-ontology-entities):
    "sql", "ddl", // extended tier (CONCEPT:AU-KG.compute.built-ast-extended):
    "rb", "php", "sh", "bash", "scala", "sc", "lua",
    // CONCEPT:EH-281 grammar expansion (extended tier): Kotlin (+ Gradle Kotlin
    // DSL via `.kts`), Objective-C, Zig, Groovy (+ Gradle Groovy DSL via
    // `.gradle` — Gradle needs no grammar of its own), Swift. HTML/CSS/JSON
    // are deliberately NOT listed: see the exclusion note in
    // `grammars_extended`'s module doc.
    "kt", "kts", "m", "mm", "zig", "groovy", "gradle", "swift",
];

const PARSER_CAPABILITY_DIGEST_DOMAIN: &[u8] = b"eg/index-repository-parser-capability/v1\0";

fn sha256_digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

/// Fingerprint the parser capability that classified one path. The normalized
/// extension and resolved language make this feature-sensitive: a previously
/// unsupported file gets a different digest when its grammar is compiled in.
/// The versioned domain is bumped when parser semantics change.
fn parser_capability_digest(file_path: &str) -> String {
    let extension = normalized_extension(file_path);
    let language = lang_for_path(file_path)
        .map(|(_, label)| label)
        .unwrap_or("unsupported");
    let mut digest = Sha256::new();
    digest.update(PARSER_CAPABILITY_DIGEST_DOMAIN);
    digest.update(extension.as_bytes());
    digest.update([0]);
    digest.update(language.as_bytes());
    format!("sha256:{}", hex::encode(digest.finalize()))
}

fn parse_outcome(file_path: &str, source: &[u8]) -> (ParseResult, IndexFileOutcome) {
    let content_digest = sha256_digest(source);
    let parser_capability_digest = parser_capability_digest(file_path);
    let parser_supported = lang_for_path(file_path).is_some();
    match parse_file(file_path, source) {
        Ok(result) => (
            result,
            IndexFileOutcome {
                file_path: file_path.to_string(),
                status: IndexFileStatus::Success,
                content_digest,
                parser_capability_digest,
                diagnostics: BoundedVec::new(Vec::new())
                    .expect("an empty diagnostic list is bounded"),
            },
        ),
        Err(message) => {
            let (status, code) = if parser_supported {
                (IndexFileStatus::Error, "parse_failed")
            } else {
                (IndexFileStatus::Unsupported, "unsupported_extension")
            };
            (
                ParseResult {
                    nodes: Vec::new(),
                    edges: Vec::new(),
                    symbols_extracted: 0,
                },
                IndexFileOutcome {
                    file_path: file_path.to_string(),
                    status,
                    content_digest,
                    parser_capability_digest,
                    diagnostics: BoundedVec::new(vec![IndexDiagnostic {
                        code: code.to_string(),
                        message,
                    }])
                    .expect("one index diagnostic is bounded"),
                },
            )
        }
    }
}

pub fn parse_file(file_path: &str, source: &[u8]) -> Result<ParseResult, String> {
    let mut parser = Parser::new();
    let (language, lang_label) = lang_for_path(file_path).ok_or("Unsupported file extension")?;

    parser.set_language(&language).map_err(|e| e.to_string())?;

    let tree = parser.parse(source, None).ok_or("Failed to parse source")?;

    let mut state = walk::WalkState::new(file_path, lang_label);

    // SQL DDL takes a dedicated extraction path (CONCEPT:AU-KG.ontology.emits-database-ontology-entities): it emits
    // database-ontology entities (tables/columns/views + FK edges), NOT :Code
    // symbols, so it does not flow through the call-graph walker.
    if lang_label == "sql" {
        sql::extract_sql(tree.root_node(), source, file_path, &mut state.result);
        return Ok(state.result);
    }

    walk::walk_node(tree.root_node(), source, "", &[], &mut state);

    Ok(state.result)
}

pub(super) fn get_node_text(node: Node, source: &[u8]) -> String {
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

/// Parse one repository batch while preserving each input's exact disposition.
/// Rayon indexed parallel iteration retains the submitted order.
pub(super) fn parse_files_with_outcomes(
    files: &[(String, Vec<u8>)],
) -> (Vec<ParseResult>, Vec<IndexFileOutcome>) {
    use rayon::prelude::*;
    files
        .par_iter()
        .map(|(path, source)| parse_outcome(path, source))
        .unzip()
}

// ── CONCEPT:EG-KG.compute.model-free-similar-code — model-free code-similarity signature ──────────────────
// A MinHash signature over a symbol's normalized AST-leaf trigrams. Identifiers/
// strings/numbers/types are abstracted to class tokens (so a renamed-variable
// clone still matches) while keywords/operators/punctuation are kept verbatim (so
// structure is preserved). The cross-file resolver LSH-bands these signatures into
// `similar_to` edges — model-free, so code similarity survives the embedder being
// offline or unavailable.

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
    use std::collections::HashMap;

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
    fn cuda_reuses_the_cpp_grammar_under_its_own_label() {
        // CONCEPT:EH-281 — `.cu`/`.cuh` route through tree-sitter-cpp (no
        // dedicated CUDA grammar is ABI-compatible; see the Cargo.toml note),
        // so an ordinary C++-shaped struct/function extracts exactly as it
        // would from a `.cpp` file, just stamped with the `cuda` label.
        let src = "struct Params { int width; int height; };\nvoid scale(int a, int b) { int c = a + b; }\n";
        let s = sym("kernel.cu", src, "Params");
        assert_eq!(s["kind_detail"], "struct");
        assert_eq!(s["language"], "cuda");
        let f = sym("kernel.cuh", src, "scale");
        assert_eq!(f["symbol_type"], "Function");
        assert_eq!(f["language"], "cuda");
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

    #[test]
    fn call_site_cap_retains_exact_lexicographic_prefix() {
        let mut source = "def f():\n".to_string();
        for index in (0..100).rev() {
            source.push_str(&format!("    call_{index:03}()\n"));
        }
        let props = sym("calls.py", &source, "f");
        let calls: Vec<&str> = props["calls"].split(',').collect();
        assert_eq!(calls.len(), MAX_SYMBOL_CALL_SITES);
        assert_eq!(calls.first(), Some(&"call_000"));
        assert_eq!(calls.last(), Some(&"call_063"));
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

    // ── CONCEPT:EG-KG.compute.qualified-symbol ───────────────────────────────

    /// Every symbol in a file, keyed by qualified name (so overloads/duplicated
    /// bare names are distinguishable).
    fn quals(path: &str, src: &str) -> Vec<String> {
        let r =
            parse_file(path, src.as_bytes()).unwrap_or_else(|e| panic!("parse {path} failed: {e}"));
        let mut v: Vec<String> = r
            .nodes
            .iter()
            .filter_map(|n| n.properties.get("qualified_symbol").cloned())
            .collect();
        v.sort();
        v
    }

    #[test]
    fn rust_nested_impl_method_is_qualified() {
        let src = r#"
mod inner {
    pub struct Thing;
    impl Thing {
        pub fn method(&self) -> u32 { 1 }
    }
    impl std::fmt::Debug for Thing {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { Ok(()) }
    }
}
pub fn free() {}
"#;
        let q = quals("m.rs", src);
        assert!(
            q.contains(&"inner::Thing::method".to_string()),
            "inherent impl method must qualify through mod + type: {q:?}"
        );
        assert!(
            q.contains(&"inner::<Thing as std::fmt::Debug>::fmt".to_string()),
            "trait impl must disambiguate with `<Type as Trait>`: {q:?}"
        );
        assert!(
            q.contains(&"inner::Thing".to_string()),
            "the struct itself qualifies through its mod: {q:?}"
        );
        assert!(
            q.contains(&"free".to_string()),
            "a top-level item qualifies to its bare name: {q:?}"
        );
        // `name` stays BARE — existing consumers are untouched.
        assert_eq!(sym("m.rs", src, "method")["name"], "method");
    }

    #[test]
    fn rust_impl_generics_and_references_are_stripped() {
        let src = "struct Wrap<T>(T);\nimpl<T> Wrap<T> { fn get(&self) -> u8 { 0 } }\n";
        assert_eq!(sym("g.rs", src, "get")["qualified_symbol"], "Wrap::get");
    }

    #[test]
    fn python_nested_class_method_is_qualified() {
        let src = "class Outer:\n    class Inner:\n        def method(self):\n            return 1\n\n    def top(self):\n        def helper():\n            return 2\n        return helper()\n";
        let q = quals("p.py", src);
        assert!(
            q.contains(&"Outer.Inner.method".to_string()),
            "nested class method: {q:?}"
        );
        assert!(
            q.contains(&"Outer.top.helper".to_string()),
            "function nested in a method: {q:?}"
        );
        assert!(q.contains(&"Outer.Inner".to_string()), "{q:?}");
        assert_eq!(sym("p.py", src, "method")["name"], "method");
    }

    #[test]
    fn end_range_is_stamped_and_well_formed() {
        let src = "def f(a):\n    b = a + 1\n    return b\n";
        let p = sym("r.py", src, "f");
        let line: usize = p["line"].parse().unwrap();
        let end_line: usize = p["end_line"].parse().unwrap();
        let start_byte: usize = p["start_byte"].parse().unwrap();
        let end_byte: usize = p["end_byte"].parse().unwrap();
        assert_eq!(line, 1);
        assert!(end_line >= line, "end_line {end_line} >= line {line}");
        assert_eq!(end_line, 3, "the def spans three lines");
        assert!(end_byte > start_byte);
        // Byte range is half-open and slices back to the declaration text.
        assert!(src[start_byte..end_byte].starts_with("def f(a):"));
        assert!(p.contains_key("start_col") && p.contains_key("end_col"));
    }

    #[test]
    fn end_line_never_precedes_line_across_a_repo_shaped_batch() {
        let files: Vec<(String, Vec<u8>)> = vec![
            (
                "a.py".into(),
                b"class C:\n    def m(self):\n        pass\n".to_vec(),
            ),
            (
                "b.rs".into(),
                b"mod m {\n    struct S;\n    impl S { fn go(&self) {} }\n}\n".to_vec(),
            ),
            ("c.go".into(), b"package m\nfunc F() {}\n".to_vec()),
            (
                "d.java".into(),
                b"class K { int f() { return 1; } }\n".to_vec(),
            ),
        ];
        let results = parse_files(&files);
        let mut seen = 0usize;
        for r in &results {
            for n in &r.nodes {
                let line: usize = n.properties["line"].parse().unwrap();
                let end_line: usize = n.properties["end_line"].parse().unwrap();
                assert!(end_line >= line, "{:?}", n.properties);
                assert!(
                    !n.properties["qualified_symbol"].is_empty(),
                    "every symbol carries a qualified name: {:?}",
                    n.properties
                );
                seen += 1;
            }
        }
        assert!(seen >= 6, "expected symbols across the batch, got {seen}");
    }

    // ── CONCEPT:EG-KG.compute.symbol-occurrence-id — occurrence vs content identity ──

    /// Every `node_id` in a parse result, in emission order.
    fn ids(r: &ParseResult) -> Vec<String> {
        r.nodes.iter().map(|n| n.node_id.clone()).collect()
    }

    #[test]
    fn identical_declarations_in_two_files_get_distinct_ids_but_one_ast_hash() {
        // The defect: `fn new()` bodies are byte-identical across files, so a
        // content-addressed id collapsed them onto ONE node and every edge that
        // touched it was ambiguous.
        let body = b"impl A {\n    fn new() -> Self {\n        Self\n    }\n}\n";
        let a = parse_file("a.rs", body).unwrap();
        let b = parse_file("b.rs", body).unwrap();
        let (a_new, b_new) = (&a.nodes[0], &b.nodes[0]);
        assert_eq!(a_new.properties["name"], "new");
        assert_ne!(
            a_new.node_id, b_new.node_id,
            "same declaration in two files must be two occurrences"
        );
        // Content identity is unchanged — clone detection still sees them as one.
        assert_eq!(a_new.properties["ast_hash"], b_new.properties["ast_hash"]);
        assert_eq!(a_new.properties["minhash"], b_new.properties["minhash"]);
    }

    #[test]
    fn identical_declarations_in_one_file_get_distinct_ids() {
        // Two `Foo::new` in one file (two `impl` blocks) — same path, same
        // qualified name, so only the ordinal separates them.
        let src = b"impl Foo {\n    fn new() -> Self { Foo }\n}\nimpl Foo {\n    fn new() -> Self { Foo }\n}\n";
        let r = parse_file("dup.rs", src).unwrap();
        let news: Vec<&ExtractedNode> = r
            .nodes
            .iter()
            .filter(|n| n.properties["qualified_symbol"] == "Foo::new")
            .collect();
        assert_eq!(news.len(), 2, "two occurrences expected: {:?}", ids(&r));
        assert_ne!(news[0].node_id, news[1].node_id);
        assert_eq!(news[0].properties["occurrence_index"], "0");
        assert_eq!(news[1].properties["occurrence_index"], "1");
    }

    #[test]
    fn ids_are_unique_across_a_repo_shaped_batch() {
        // The property `IndexResult.nodes` documents: one row per occurrence.
        let files: Vec<(String, Vec<u8>)> = vec![
            (
                "a.py".into(),
                b"class C:\n    def m(self):\n        pass\n".to_vec(),
            ),
            (
                "b.py".into(),
                b"class C:\n    def m(self):\n        pass\n".to_vec(),
            ),
            ("c.rs".into(), b"impl S { fn new() -> S { S } }\n".to_vec()),
            ("d.rs".into(), b"impl S { fn new() -> S { S } }\n".to_vec()),
        ];
        let out = super::super::resolve::index_repository(&files);
        let mut seen = std::collections::HashSet::new();
        for n in &out.nodes {
            assert!(
                seen.insert(n.node_id.clone()),
                "duplicate node id {} ({:?})",
                n.node_id,
                n.properties
            );
        }
        assert!(out.nodes.len() >= 6, "got {} nodes", out.nodes.len());
    }

    #[test]
    fn ids_are_byte_identical_across_runs() {
        // Determinism: the corpus is publication evidence, so two runs over the
        // same bytes must produce the same ids.
        let src = b"class A:\n    def run(self):\n        return helper()\n\ndef helper():\n    return 1\n";
        assert_eq!(
            ids(&parse_file("x.py", src).unwrap()),
            ids(&parse_file("x.py", src).unwrap())
        );
    }

    #[test]
    fn id_survives_an_unrelated_edit_above_the_declaration() {
        // Why (path, qualified name, ordinal) and NOT (path, start_byte): an
        // insertion above a declaration must not renumber it.
        let before = b"def keep():\n    return 1\n";
        let after = b"def added():\n    return 0\n\ndef keep():\n    return 1\n";
        let id_of = |src: &[u8]| -> String {
            parse_file("m.py", src)
                .unwrap()
                .nodes
                .iter()
                .find(|n| n.properties["name"] == "keep")
                .expect("keep")
                .node_id
                .clone()
        };
        let (a, b) = (id_of(before), id_of(after));
        assert_eq!(a, id_of(before));
        assert_eq!(
            a, b,
            "`keep` moved down the file but is the same occurrence"
        );
        // Its byte offset DID move — the fact a byte-keyed id would have tripped on.
        let moved = parse_file("m.py", after).unwrap();
        let keep = moved
            .nodes
            .iter()
            .find(|n| n.properties["name"] == "keep")
            .unwrap();
        assert_ne!(keep.properties["start_byte"], "0");
    }

    #[test]
    fn id_depends_on_the_file_path_and_the_qualified_name() {
        // Both components are load-bearing; neither alone would separate these.
        let src =
            b"class A:\n    def m(self):\n        pass\nclass B:\n    def m(self):\n        pass\n";
        let one = parse_file("p/one.py", src).unwrap();
        let two = parse_file("p/two.py", src).unwrap();
        let id = |r: &ParseResult, qual: &str| {
            r.nodes
                .iter()
                .find(|n| n.properties["qualified_symbol"] == qual)
                .unwrap_or_else(|| panic!("no {qual}"))
                .node_id
                .clone()
        };
        assert_ne!(id(&one, "A.m"), id(&one, "B.m"), "qualified name separates");
        assert_ne!(id(&one, "A.m"), id(&two, "A.m"), "file path separates");
    }

    #[test]
    fn every_core_extension_resolves_to_its_grammar_and_label() {
        let python: LangCtor = || tree_sitter_python::LANGUAGE.into();
        let javascript: LangCtor = || tree_sitter_javascript::LANGUAGE.into();
        let typescript: LangCtor = || tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into();
        let tsx: LangCtor = || tree_sitter_typescript::LANGUAGE_TSX.into();
        let go: LangCtor = || tree_sitter_go::LANGUAGE.into();
        let rust: LangCtor = || tree_sitter_rust::LANGUAGE.into();
        let java: LangCtor = || tree_sitter_java::LANGUAGE.into();
        let c: LangCtor = || tree_sitter_c::LANGUAGE.into();
        let cpp: LangCtor = || tree_sitter_cpp::LANGUAGE.into();
        let csharp: LangCtor = || tree_sitter_c_sharp::LANGUAGE.into();
        let sql: LangCtor = || tree_sitter_sequel::LANGUAGE.into();
        // One row per extension: `(extension, expected grammar, expected label)`.
        let cases: [(&str, LangCtor, &str); 27] = [
            ("py", python, "python"),
            ("pyi", python, "python"),
            ("js", javascript, "javascript"),
            ("jsx", javascript, "javascript"),
            ("mjs", javascript, "javascript"),
            ("cjs", javascript, "javascript"),
            ("ts", typescript, "typescript"),
            ("mts", typescript, "typescript"),
            ("cts", typescript, "typescript"),
            ("tsx", tsx, "typescript"),
            ("go", go, "go"),
            ("rs", rust, "rust"),
            ("java", java, "java"),
            ("c", c, "c"),
            ("h", c, "c"),
            ("cpp", cpp, "cpp"),
            ("cc", cpp, "cpp"),
            ("cxx", cpp, "cpp"),
            ("hpp", cpp, "cpp"),
            ("hxx", cpp, "cpp"),
            ("hh", cpp, "cpp"),
            ("c++", cpp, "cpp"),
            // CUDA (CONCEPT:EH-281) reuses the cpp grammar under its own label.
            ("cu", cpp, "cuda"),
            ("cuh", cpp, "cuda"),
            ("cs", csharp, "csharp"),
            ("sql", sql, "sql"),
            ("ddl", sql, "sql"),
        ];
        for (ext, grammar, label) in cases {
            let upper = ext.to_ascii_uppercase();
            // Extension matching is case-insensitive, so both spellings must resolve.
            for path in [format!("src/file.{ext}"), format!("SRC/FILE.{upper}")] {
                let (language, got_label) =
                    lang_for_path(&path).unwrap_or_else(|| panic!("{path}: no grammar"));
                assert_eq!(language, grammar(), "{path}: wrong grammar");
                assert_eq!(got_label, label, "{path}: wrong label");
                assert!(
                    parse_file(&path, b"").is_ok(),
                    "{path}: grammar did not load"
                );
            }
        }
    }

    #[test]
    fn hash_fields_is_injective_across_field_boundaries() {
        // Length prefixes: plain concatenation would make these two equal.
        assert_ne!(hash_fields(&["ab", "c"]), hash_fields(&["a", "bc"]));
        assert_eq!(hash_fields(&["ab", "c"]), hash_fields(&["ab", "c"]));
    }

    #[test]
    fn qualification_survives_index_repository_resolution() {
        // resolve() strips `call_sites`/`minhash` — it must NOT strip the new facts.
        let files: Vec<(String, Vec<u8>)> = vec![(
            "x.py".into(),
            b"class A:\n    def run(self):\n        return 1\n".to_vec(),
        )];
        let out = super::super::resolve::index_repository(&files);
        let run = out
            .nodes
            .iter()
            .find(|n| n.properties.get("name").map(String::as_str) == Some("run"))
            .expect("run symbol");
        assert_eq!(run.properties["qualified_symbol"], "A.run");
        assert!(run.properties.contains_key("end_line"));
        assert!(!run.properties.contains_key("minhash"));
    }
}
