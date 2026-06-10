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

pub fn parse_file(file_path: &str, source: &[u8]) -> Result<ParseResult, String> {
    let mut parser = Parser::new();
    let language: Language = if file_path.ends_with(".py") {
        tree_sitter_python::LANGUAGE.into()
    } else if file_path.ends_with(".js") {
        tree_sitter_javascript::LANGUAGE.into()
    } else if file_path.ends_with(".ts") {
        tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()
    } else if file_path.ends_with(".tsx") {
        tree_sitter_typescript::LANGUAGE_TSX.into()
    } else if file_path.ends_with(".go") {
        tree_sitter_go::LANGUAGE.into()
    } else {
        return Err("Unsupported file extension".into());
    };

    parser
        .set_language(&language)
        .map_err(|e| e.to_string())?;

    let tree = parser
        .parse(source, None)
        .ok_or("Failed to parse source")?;

    let mut result = ParseResult {
        nodes: Vec::new(),
        edges: Vec::new(),
        symbols_extracted: 0,
    };

    let file_node_id = format!("file:{}", file_path);

    walk_node(
        tree.root_node(),
        source,
        file_path,
        &file_node_id,
        &mut result,
    );

    Ok(result)
}

// CONCEPT:KG-2.8 — Native test-quality metrics. Computed in the Rust compute
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

fn walk_node(
    node: Node,
    source: &[u8],
    file_path: &str,
    file_node_id: &str,
    result: &mut ParseResult,
) {
    let kind = node.kind();

    if kind == "class_definition" || kind == "class_declaration" {
        if let Some(name_node) = node.child_by_field_name("name") {
            let name = get_node_text(name_node, source);
            let content_bytes = &source[node.start_byte()..node.end_byte()];
            let mut hasher = Sha256::new();
            hasher.update(content_bytes);
            let content_hash = format!("{:x}", hasher.finalize());
            let symbol_id = format!("symbol:{}", content_hash);

            let mut properties = HashMap::new();
            properties.insert("name".to_string(), name);
            properties.insert("symbol_type".to_string(), "Class".to_string());
            properties.insert("line".to_string(), (node.start_position().row + 1).to_string());
            properties.insert("ast_hash".to_string(), content_hash);
            properties.insert("file_path".to_string(), file_path.to_string());

            // CONCEPT:KG-2.8 — structural facts for design-pattern detection.
            if file_path.ends_with(".py") {
                let bases = py_class_bases(node, source);
                let (methods, has_abstract) = py_class_methods(node, source);
                let decorators = py_decorators(node, source);
                properties.insert("bases".to_string(), bases.join(","));
                properties.insert("methods".to_string(), methods.join(","));
                properties.insert(
                    "decorators".to_string(),
                    decorators
                        .iter()
                        .map(|d| d.trim_start_matches('@').to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                );
                properties.insert("is_abstract".to_string(), has_abstract.to_string());
                properties.insert("method_count".to_string(), methods.len().to_string());
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
    } else if kind == "function_definition"
        || kind == "function_declaration"
        || kind == "method_definition"
    {
        if let Some(name_node) = node.child_by_field_name("name") {
            let name = get_node_text(name_node, source);
            let content_bytes = &source[node.start_byte()..node.end_byte()];
            let mut hasher = Sha256::new();
            hasher.update(content_bytes);
            let content_hash = format!("{:x}", hasher.finalize());
            let symbol_id = format!("symbol:{}", content_hash);

            let mut properties = HashMap::new();
            properties.insert("name".to_string(), name.clone());
            properties.insert("symbol_type".to_string(), "Function".to_string());
            properties.insert("line".to_string(), (node.start_position().row + 1).to_string());
            properties.insert("ast_hash".to_string(), content_hash);
            properties.insert("file_path".to_string(), file_path.to_string());

            // CONCEPT:KG-2.8 — native Python test-quality metrics on the symbol.
            if file_path.ends_with(".py") {
                let decorators = py_decorators(node, source);
                let marks = py_marks(&decorators);
                let is_skipped =
                    marks.iter().any(|m| m == "skip" || m == "skipif" || m == "xfail");
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

                properties.insert("is_test".to_string(), is_test.to_string());
                properties.insert("assert_count".to_string(), m.assert_count.to_string());
                properties.insert("raises_count".to_string(), m.raises_count.to_string());
                properties.insert(
                    "mock_count".to_string(),
                    (m.mock_count + mock_decos).to_string(),
                );
                properties.insert(
                    "fixture_count".to_string(),
                    py_param_count(node, source).to_string(),
                );
                properties.insert("marks".to_string(), marks.join(","));
                properties.insert("is_skipped".to_string(), is_skipped.to_string());
                // Cap calls list to keep the payload bounded.
                let mut calls = m.calls.clone();
                calls.sort();
                calls.dedup();
                calls.truncate(64);
                properties.insert("calls".to_string(), calls.join(","));
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
    } else if kind == "call" {
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
    } else if kind == "import_statement" || kind == "import_declaration" {
        // Simplified import logic
        let mut properties = HashMap::new();
        properties.insert("raw".to_string(), "import".to_string());
        result.edges.push(ExtractedEdge {
            source: file_node_id.to_string(),
            target: "import_target".to_string(),
            edge_type: "depends_on_raw".to_string(),
            properties,
        });
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        walk_node(child, source, file_path, file_node_id, result);
    }
}

fn get_node_text(node: Node, source: &[u8]) -> String {
    let bytes = &source[node.start_byte()..node.end_byte()];
    String::from_utf8_lossy(bytes).into_owned()
}

/// Parse many files in one call (CONCEPT:KG-2.16 batch op). Files are parsed
/// independently and in parallel via rayon (tree-sitter is stateless per call);
/// a file that fails to parse yields an empty [`ParseResult`] in its slot, so
/// the output is 1:1 with — and in the same order as — the input. This is the
/// engine-side primitive behind the `ParseFiles` protocol op: one round-trip
/// instead of N.
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
