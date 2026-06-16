// CONCEPT:KG-2.8r — Cross-file call/import resolution.
//
// `parse_file`/`parse_files` extract per-symbol callee names (the `calls`
// property) and per-file raw call/import edges, but those targets are bare
// names — not the symbols they refer to. Resolution binds them across a whole
// batch of files in one pass:
//   - a function symbol's `calls` names → the SYMBOL ids that DEFINE them  (`calls`)
//   - a file's import module strings   → the file that defines the module  (`depends_on`)
//
// This is the cross-file step gkg's `gitlab-code-parser` performs, and the
// signal feature-clustering / impact-analysis run over. Resolution is purely
// name/path based and deliberately conservative: an ambiguous cross-file callee
// (same name defined in >1 other file, none preferred by scope) is left
// UNRESOLVED rather than guessed, so we never emit a false call edge.

use super::tree_sitter::{parse_files, ExtractedEdge, ExtractedNode, ParseResult};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Resolved, cross-file symbol graph for a batch of files — the response shape
/// of the `IndexRepository` RPC. Unlike `ParseFiles` (one raw `ParseResult` per
/// file), this is a SINGLE merged graph whose `calls`/`depends_on` edges point
/// at real node ids.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct IndexResult {
    /// Every SYMBOL node across all files (deduplicated by node id).
    pub nodes: Vec<ExtractedNode>,
    /// `IMPLEMENTS` (file→symbol) + resolved `calls` (symbol→symbol) + resolved
    /// `depends_on` (file→file). Raw unresolved `calls_raw`/`depends_on_raw`
    /// edges are dropped — they're superseded here.
    pub edges: Vec<ExtractedEdge>,
    pub symbols_extracted: usize,
    pub files_parsed: usize,
    /// Call sites bound to a definition (numerator of call-resolution coverage).
    pub calls_resolved: usize,
    /// Call sites seen but not bound (external/stdlib/ambiguous) — the remainder.
    pub calls_unresolved: usize,
    /// Import statements bound to an in-batch file.
    pub imports_resolved: usize,
    /// Import statements seen but not bound (external packages, unknown layout).
    pub imports_unresolved: usize,
}

/// A symbol definition site, indexed by bare name for call resolution.
struct Def {
    id: String,
    file_path: String,
}

/// Parse a batch of `(file_path, source_bytes)` and resolve cross-file edges in
/// one pass. The batch IS the resolution scope: a repository (or a delta set)
/// should be shipped together so intra-repo calls/imports resolve.
pub fn index_repository(files: &[(String, Vec<u8>)]) -> IndexResult {
    let results = parse_files(files);
    resolve(files, &results)
}

/// Resolve already-parsed results against the file set they came from. Split out
/// from [`index_repository`] so tests can resolve hand-built `ParseResult`s.
pub fn resolve(files: &[(String, Vec<u8>)], results: &[ParseResult]) -> IndexResult {
    let file_paths: HashSet<&str> = files.iter().map(|(p, _)| p.as_str()).collect();

    let mut out = IndexResult {
        files_parsed: results.len(),
        ..Default::default()
    };

    // name → definitions (functions, methods, classes). Built first so a call in
    // any file can resolve to a def in any other.
    let mut def_index: HashMap<String, Vec<Def>> = HashMap::new();
    // Carry the (file→module) import facts to resolve after node merge.
    let mut import_raw: Vec<(String, String)> = Vec::new();
    let mut edges: Vec<ExtractedEdge> = Vec::new();

    for r in results {
        out.symbols_extracted += r.symbols_extracted;
        for n in &r.nodes {
            if n.node_type == "SYMBOL" {
                if let (Some(name), Some(fp)) =
                    (n.properties.get("name"), n.properties.get("file_path"))
                {
                    let st = n.properties.get("symbol_type").map(String::as_str);
                    if st == Some("Function") || st == Some("Class") {
                        def_index.entry(name.clone()).or_default().push(Def {
                            id: n.node_id.clone(),
                            file_path: fp.clone(),
                        });
                    }
                }
            }
            out.nodes.push(clone_node(n));
        }
        for e in &r.edges {
            match e.edge_type.as_str() {
                "IMPLEMENTS" => edges.push(clone_edge(e)),
                // Raw forms are superseded by the resolved edges built below.
                "calls_raw" => {}
                "depends_on_raw" => {
                    let importer = e.source.strip_prefix("file:").unwrap_or(&e.source);
                    import_raw.push((importer.to_string(), e.target.clone()));
                }
                _ => edges.push(clone_edge(e)),
            }
        }
    }

    // ── Resolve calls: caller symbol → callee definition ──────────────────
    // The `calls` property (set on every function-like symbol) is the per-symbol
    // callee-name list; resolve each name to a definition, preferring same-file.
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for n in &out.nodes {
        if n.properties.get("symbol_type").map(String::as_str) != Some("Function") {
            continue;
        }
        let caller_file = match n.properties.get("file_path") {
            Some(f) => f.as_str(),
            None => continue,
        };
        let calls = match n.properties.get("calls") {
            Some(c) if !c.is_empty() => c,
            _ => continue,
        };
        for callee in calls.split(',').filter(|s| !s.is_empty()) {
            match resolve_callee(callee, caller_file, &def_index) {
                Some(target) => {
                    if seen.insert((n.node_id.clone(), target.clone())) {
                        edges.push(ExtractedEdge {
                            source: n.node_id.clone(),
                            target,
                            edge_type: "calls".to_string(),
                            properties: HashMap::from([("name".to_string(), callee.to_string())]),
                        });
                    }
                    out.calls_resolved += 1;
                }
                None => out.calls_unresolved += 1,
            }
        }
    }

    // ── Resolve imports: importer file → defining file ────────────────────
    let mut seen_dep: HashSet<(String, String)> = HashSet::new();
    for (importer, module) in &import_raw {
        match resolve_import(importer, module, &file_paths) {
            Some(target_file) => {
                let src = format!("file:{importer}");
                let tgt = format!("file:{target_file}");
                if seen_dep.insert((src.clone(), tgt.clone())) {
                    edges.push(ExtractedEdge {
                        source: src,
                        target: tgt,
                        edge_type: "depends_on".to_string(),
                        properties: HashMap::from([("module".to_string(), module.clone())]),
                    });
                }
                out.imports_resolved += 1;
            }
            None => out.imports_unresolved += 1,
        }
    }

    out.edges = edges;
    out
}

/// Pick the definition a bare callee name refers to, or `None` if external or
/// ambiguous. Preference order: a definition in the caller's own file, then a
/// unique definition anywhere in the batch. Two+ cross-file definitions with no
/// same-file match are ambiguous → unresolved (we don't guess).
fn resolve_callee(
    name: &str,
    caller_file: &str,
    index: &HashMap<String, Vec<Def>>,
) -> Option<String> {
    let defs = index.get(name)?;
    if let Some(local) = defs.iter().find(|d| d.file_path == caller_file) {
        return Some(local.id.clone());
    }
    match defs.as_slice() {
        [only] => Some(only.id.clone()),
        _ => None,
    }
}

/// Map an import module string to the in-batch file that defines it, or `None`
/// for external packages / unknown layouts. Handles the dominant conventions:
/// dotted module paths (Python/Java), relative specifiers (JS/TS), and
/// `::`-separated paths (Rust). Matching is suffix-based against the batch's
/// file paths, so it's path-layout tolerant and never invents a target.
fn resolve_import(importer: &str, module: &str, files: &HashSet<&str>) -> Option<String> {
    let m = module
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == '<' || c == '>');
    if m.is_empty() {
        return None;
    }

    // Relative JS/TS specifier (./foo, ../bar/baz): resolve against importer dir.
    if m.starts_with("./") || m.starts_with("../") {
        let base = dir_of(importer);
        let joined = normalize_join(&base, m);
        return match_with_extensions(&joined, files);
    }

    // Dotted (Python `a.b.c`, Java `com.foo.Bar`) or `::` (Rust) module path →
    // slash path, then suffix-match. `crate::`/`self::`/`super::` Rust prefixes
    // and a Python leading dot are stripped to a best-effort relative stem.
    let stem = m
        .trim_start_matches('.')
        .replace("::", "/")
        .replace('.', "/");
    let stem = stem
        .strip_prefix("crate/")
        .or_else(|| stem.strip_prefix("self/"))
        .unwrap_or(&stem)
        .to_string();
    if stem.is_empty() {
        return None;
    }
    match_with_extensions(&stem, files)
}

/// Directory portion of a file path (`a/b/c.py` → `a/b`), empty for a bare name.
fn dir_of(path: &str) -> String {
    match path.rfind('/') {
        Some(i) => path[..i].to_string(),
        None => String::new(),
    }
}

/// Join a relative specifier onto a base dir, collapsing `.`/`..` segments.
fn normalize_join(base: &str, rel: &str) -> String {
    let mut parts: Vec<&str> = if base.is_empty() {
        Vec::new()
    } else {
        base.split('/').collect()
    };
    for seg in rel.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            s => parts.push(s),
        }
    }
    parts.join("/")
}

/// Suffix-match a module stem against the batch files, trying source extensions
/// and package-index files (`__init__.py`, `index.ts`, `mod.rs`). Returns the
/// matched file path. Suffix (not exact) matching tolerates repo-root prefixes
/// that the importer's relative/dotted path omits.
fn match_with_extensions(stem: &str, files: &HashSet<&str>) -> Option<String> {
    const EXTS: &[&str] = &[
        "py", "pyi", "ts", "tsx", "js", "jsx", "mjs", "go", "rs", "java",
    ];
    const INDEX: &[&str] = &["__init__.py", "index.ts", "index.js", "mod.rs"];

    let mut candidates: Vec<String> = Vec::new();
    for ext in EXTS {
        candidates.push(format!("{stem}.{ext}"));
    }
    for idx in INDEX {
        candidates.push(format!("{stem}/{idx}"));
    }

    for cand in &candidates {
        // Exact or path-suffix match (boundary-aware so `auth.py` ≠ `oauth.py`).
        for f in files {
            if *f == cand || f.ends_with(&format!("/{cand}")) {
                return Some((*f).to_string());
            }
        }
    }
    None
}

fn clone_node(n: &ExtractedNode) -> ExtractedNode {
    ExtractedNode {
        node_id: n.node_id.clone(),
        node_type: n.node_type.clone(),
        properties: n.properties.clone(),
    }
}

fn clone_edge(e: &ExtractedEdge) -> ExtractedEdge {
    ExtractedEdge {
        source: e.source.clone(),
        target: e.target.clone(),
        edge_type: e.edge_type.clone(),
        properties: e.properties.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files(pairs: &[(&str, &str)]) -> Vec<(String, Vec<u8>)> {
        pairs
            .iter()
            .map(|(p, s)| (p.to_string(), s.as_bytes().to_vec()))
            .collect()
    }

    /// Helper: collect resolved `calls` edges as (caller_name, callee_name) by
    /// looking the caller symbol id back up in the node set.
    fn call_pairs(r: &IndexResult) -> Vec<(String, String)> {
        let name_of = |id: &str| -> String {
            r.nodes
                .iter()
                .find(|n| n.node_id == id)
                .and_then(|n| n.properties.get("name").cloned())
                .unwrap_or_default()
        };
        r.edges
            .iter()
            .filter(|e| e.edge_type == "calls")
            .map(|e| (name_of(&e.source), name_of(&e.target)))
            .collect()
    }

    #[test]
    fn resolves_same_file_call_to_definition() {
        let r = index_repository(&files(&[(
            "m.py",
            "def helper():\n    return 1\n\ndef caller():\n    return helper()\n",
        )]));
        let pairs = call_pairs(&r);
        assert!(
            pairs.contains(&("caller".to_string(), "helper".to_string())),
            "expected caller→helper, got {pairs:?}"
        );
        assert!(r.calls_resolved >= 1);
    }

    #[test]
    fn resolves_cross_file_unique_definition() {
        let r = index_repository(&files(&[
            ("util.py", "def shared():\n    return 1\n"),
            ("app.py", "def run():\n    return shared()\n"),
        ]));
        let pairs = call_pairs(&r);
        assert!(
            pairs.contains(&("run".to_string(), "shared".to_string())),
            "cross-file unique call should resolve, got {pairs:?}"
        );
    }

    #[test]
    fn ambiguous_cross_file_callee_is_not_guessed() {
        // `dup` defined in TWO other files, called from a third with no same-file
        // definition → ambiguous → must NOT emit a (false) call edge.
        let r = index_repository(&files(&[
            ("a.py", "def dup():\n    return 1\n"),
            ("b.py", "def dup():\n    return 2\n"),
            ("c.py", "def go():\n    return dup()\n"),
        ]));
        let pairs = call_pairs(&r);
        assert!(
            !pairs.iter().any(|(c, _)| c == "go"),
            "ambiguous callee must stay unresolved, got {pairs:?}"
        );
        assert!(r.calls_unresolved >= 1);
    }

    #[test]
    fn same_file_definition_wins_over_other_files() {
        // `dup` exists in a.py AND locally in c.py; the local one must win.
        let r = index_repository(&files(&[
            ("a.py", "def dup():\n    return 1\n"),
            (
                "c.py",
                "def dup():\n    return 2\n\ndef go():\n    return dup()\n",
            ),
        ]));
        let local_dup = r
            .nodes
            .iter()
            .find(|n| {
                n.properties.get("name").map(String::as_str) == Some("dup")
                    && n.properties.get("file_path").map(String::as_str) == Some("c.py")
            })
            .expect("local dup")
            .node_id
            .clone();
        let go = r
            .nodes
            .iter()
            .find(|n| n.properties.get("name").map(String::as_str) == Some("go"))
            .unwrap()
            .node_id
            .clone();
        assert!(
            r.edges
                .iter()
                .any(|e| e.edge_type == "calls" && e.source == go && e.target == local_dup),
            "go→dup must bind the SAME-FILE dup"
        );
    }

    #[test]
    fn resolves_python_from_import_to_dependson() {
        let r = index_repository(&files(&[
            ("pkg/util.py", "def shared():\n    return 1\n"),
            (
                "pkg/app.py",
                "from pkg.util import shared\n\ndef run():\n    return shared()\n",
            ),
        ]));
        assert!(
            r.edges.iter().any(|e| e.edge_type == "depends_on"
                && e.source == "file:pkg/app.py"
                && e.target == "file:pkg/util.py"),
            "from-import should resolve to a depends_on edge; edges={:?}",
            r.edges
        );
        assert!(r.imports_resolved >= 1);
    }

    #[test]
    fn resolves_relative_ts_import() {
        let r = index_repository(&files(&[
            ("src/util.ts", "export function shared(): number { return 1; }\n"),
            (
                "src/app.ts",
                "import { shared } from './util';\nexport function run(): number { return shared(); }\n",
            ),
        ]));
        assert!(
            r.edges.iter().any(|e| e.edge_type == "depends_on"
                && e.source == "file:src/app.ts"
                && e.target == "file:src/util.ts"),
            "relative ts import should resolve; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn external_import_is_left_unresolved() {
        let r = index_repository(&files(&[(
            "app.py",
            "import os\nfrom requests import get\n\ndef run():\n    return get('x')\n",
        )]));
        assert_eq!(
            r.imports_resolved, 0,
            "stdlib/external imports must not bind"
        );
        assert!(r.imports_unresolved >= 1);
    }

    #[test]
    fn implements_edges_and_nodes_survive() {
        let r = index_repository(&files(&[("m.py", "def a():\n    return 1\n")]));
        assert!(r.nodes.iter().any(|n| n.node_type == "SYMBOL"));
        assert!(r.edges.iter().any(|e| e.edge_type == "IMPLEMENTS"));
        // No raw placeholder edges leak through.
        assert!(!r.edges.iter().any(|e| e.edge_type.ends_with("_raw")));
    }
}
