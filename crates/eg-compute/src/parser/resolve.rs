// CONCEPT:KG-2.8r / KG-2.100 — Cross-file, type/scope-resolved call + import graph.
//
// `parse_file`/`parse_files` extract per-symbol call sites (the `call_sites`
// property: receiver/callee/argc triples) and per-file raw import edges, but
// those targets are bare names — not the symbols they refer to. Resolution binds
// them across a whole batch of files in one pass:
//   - a symbol's call sites → the SYMBOL ids that DEFINE them                 (`calls`)
//   - a class's base/interface names → the class SYMBOL ids   (`inherits`/`realizes`)
//   - a file's import module strings → the file that defines the module (`depends_on`)
//
// Call resolution is now **type/scope-aware** (CONCEPT:KG-2.100): a method call
// `obj.run()` / `self.run()` binds to the `run` method of the receiver's class
// (or an inherited one), and same-name overloads disambiguate by argument count —
// instead of the old name-only match. Every resolved call edge carries a
// `strategy` (same_file/scoped/arity/unique) and a `confidence`. Resolution stays
// deliberately conservative: an ambiguous callee with no stronger signal is left
// UNRESOLVED rather than guessed, so we never emit a false edge.

use super::tree_sitter::{
    decode_call_sites, DecodedSite, ExtractedEdge, ExtractedNode, ParseResult,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Resolved, cross-file symbol graph for a batch of files — the response shape
/// of the `IndexRepository` RPC. Unlike `ParseFiles` (one raw `ParseResult` per
/// file), this is a SINGLE merged graph whose `calls`/`inherits`/`realizes`/
/// `depends_on` edges point at real node ids.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct IndexResult {
    /// Every SYMBOL node across all files (deduplicated by node id). The internal
    /// `call_sites` resolution-input property is stripped before return.
    pub nodes: Vec<ExtractedNode>,
    /// `IMPLEMENTS` (file→symbol) + resolved `calls` (symbol→symbol) + `inherits`/
    /// `realizes` (class→class) + resolved `depends_on` (file→file). Raw unresolved
    /// `calls_raw`/`depends_on_raw` edges are dropped — they're superseded here.
    pub edges: Vec<ExtractedEdge>,
    pub symbols_extracted: usize,
    pub files_parsed: usize,
    /// Call sites bound to a definition (numerator of call-resolution coverage).
    pub calls_resolved: usize,
    /// Call sites seen but not bound (external/stdlib/ambiguous) — the remainder.
    pub calls_unresolved: usize,
    /// Of `calls_resolved`, those bound by receiver/class scope (CONCEPT:KG-2.100).
    pub calls_scope_resolved: usize,
    /// Of `calls_resolved`, those disambiguated by argument-count match.
    pub calls_type_resolved: usize,
    /// Class→base `inherits` edges emitted.
    pub inherits_edges: usize,
    /// Class→interface `realizes` edges emitted.
    pub realizes_edges: usize,
    /// Model-free `similar_to` edges emitted (CONCEPT:KG-2.101).
    pub similar_edges: usize,
    /// Import statements bound to an in-batch file.
    pub imports_resolved: usize,
    /// Import statements seen but not bound (external packages, unknown layout).
    pub imports_unresolved: usize,
}

/// A symbol definition site, indexed by bare name for call resolution.
struct Def {
    id: String,
    file_path: String,
    arity: Option<usize>,
}

/// A class/struct/interface definition site, indexed by name for structural and
/// scoped-method resolution.
struct ClassDef {
    id: String,
    file_path: String,
    bases: Vec<String>,
    interfaces: Vec<String>,
}

/// Parse a batch of `(file_path, source_bytes)` and resolve cross-file edges in
/// one pass. The batch IS the resolution scope: a repository (or a delta set)
/// should be shipped together so intra-repo calls/imports resolve.
pub fn index_repository(files: &[(String, Vec<u8>)]) -> IndexResult {
    let results = super::tree_sitter::parse_files(files);
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
    // (class scope, method name) → method node ids, for receiver-scoped calls.
    let mut scoped: HashMap<(String, String), Vec<String>> = HashMap::new();
    // class name → its definitions (for structural edges + scoped lookup).
    let mut class_by_name: HashMap<String, Vec<ClassDef>> = HashMap::new();
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
                    let scope = n.properties.get("scope").cloned().unwrap_or_default();
                    let arity = n.properties.get("arity").and_then(|a| a.parse().ok());
                    if st == Some("Function") || st == Some("Class") {
                        def_index.entry(name.clone()).or_default().push(Def {
                            id: n.node_id.clone(),
                            file_path: fp.clone(),
                            arity,
                        });
                    }
                    if st == Some("Function") && !scope.is_empty() {
                        scoped
                            .entry((scope, name.clone()))
                            .or_default()
                            .push(n.node_id.clone());
                    }
                    if st == Some("Class") {
                        class_by_name
                            .entry(name.clone())
                            .or_default()
                            .push(ClassDef {
                                id: n.node_id.clone(),
                                file_path: fp.clone(),
                                bases: split_csv(n.properties.get("bases")),
                                interfaces: split_csv(n.properties.get("interfaces")),
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

    // class name → its (deduped) base + interface names, for ancestor lookup.
    let mut bases_of: HashMap<String, Vec<String>> = HashMap::new();
    for (name, defs) in &class_by_name {
        let mut all: Vec<String> = Vec::new();
        for d in defs {
            for b in d.bases.iter().chain(d.interfaces.iter()) {
                if !all.contains(b) {
                    all.push(b.clone());
                }
            }
        }
        bases_of.insert(name.clone(), all);
    }

    // ── Resolve calls: caller symbol → callee definition (type/scope-aware) ──
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for idx in 0..out.nodes.len() {
        let (caller_id, caller_file, caller_scope, caller_lang, sites) = {
            let n = &out.nodes[idx];
            if n.properties.get("symbol_type").map(String::as_str) != Some("Function") {
                continue;
            }
            let file = match n.properties.get("file_path") {
                Some(f) => f.clone(),
                None => continue,
            };
            let scope = n.properties.get("scope").cloned().unwrap_or_default();
            let lang = n.properties.get("language").cloned().unwrap_or_default();
            let sites = match n.properties.get("call_sites") {
                Some(cs) if !cs.is_empty() => decode_call_sites(cs),
                // Fallback (no structured sites): name-only, no receiver/arity signal.
                _ => match n.properties.get("calls") {
                    Some(c) if !c.is_empty() => c
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .map(|name| DecodedSite {
                            receiver: String::new(),
                            callee: name.to_string(),
                            argc: None,
                        })
                        .collect(),
                    _ => Vec::new(),
                },
            };
            (n.node_id.clone(), file, scope, lang, sites)
        };
        for site in &sites {
            match resolve_site(
                site,
                &caller_file,
                &caller_scope,
                &caller_lang,
                &def_index,
                &scoped,
                &class_by_name,
                &bases_of,
            ) {
                Some((target, strategy, confidence)) => {
                    if seen.insert((caller_id.clone(), target.clone())) {
                        edges.push(ExtractedEdge {
                            source: caller_id.clone(),
                            target,
                            edge_type: "calls".to_string(),
                            properties: HashMap::from([
                                ("name".to_string(), site.callee.clone()),
                                ("strategy".to_string(), strategy.to_string()),
                                ("confidence".to_string(), format!("{confidence:.2}")),
                            ]),
                        });
                    }
                    out.calls_resolved += 1;
                    match strategy {
                        "scoped" => out.calls_scope_resolved += 1,
                        "arity" => out.calls_type_resolved += 1,
                        _ => {}
                    }
                }
                None => out.calls_unresolved += 1,
            }
        }
    }

    // ── Structural edges: class → base (`inherits`) / interface (`realizes`) ──
    let mut seen_struct: HashSet<(String, String, &'static str)> = HashSet::new();
    for defs in class_by_name.values() {
        for d in defs {
            for (names, etype) in [(&d.bases, "inherits"), (&d.interfaces, "realizes")] {
                for base in names {
                    if let Some(target) = resolve_class(base, &d.file_path, &class_by_name) {
                        if target == d.id {
                            continue;
                        }
                        if seen_struct.insert((d.id.clone(), target.clone(), etype)) {
                            edges.push(ExtractedEdge {
                                source: d.id.clone(),
                                target,
                                edge_type: etype.to_string(),
                                properties: HashMap::from([("name".to_string(), base.clone())]),
                            });
                            if etype == "inherits" {
                                out.inherits_edges += 1;
                            } else {
                                out.realizes_edges += 1;
                            }
                        }
                    }
                }
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

    // ── Model-free similarity: LSH-band the MinHash signatures (CONCEPT:KG-2.101) ──
    out.similar_edges = similarity_edges(&out.nodes, &mut edges);

    // `call_sites`/`minhash` are resolution-only inputs; don't leak them onto nodes.
    for n in &mut out.nodes {
        n.properties.remove("call_sites");
        n.properties.remove("minhash");
    }

    out.edges = edges;
    out
}

/// LSH-band the per-symbol MinHash signatures into `similar_to` edges. Symbols
/// whose signatures collide in any band become candidate pairs; a pair is linked
/// when its estimated Jaccard ≥ [`SIMILAR_THRESHOLD`]. Edges are symmetric
/// (emitted once with sorted endpoints), scored, and capped per node so a big
/// clone family doesn't explode the edge set. Returns the edge count.
fn similarity_edges(nodes: &[ExtractedNode], edges: &mut Vec<ExtractedEdge>) -> usize {
    use super::tree_sitter::{decode_minhash, minhash_jaccard, MINHASH_K};

    // (node_id, signature) for every code symbol with a usable signature.
    let sigs: Vec<(&str, [u32; MINHASH_K])> = nodes
        .iter()
        .filter(|n| n.node_type == "SYMBOL")
        .filter_map(|n| {
            n.properties
                .get("minhash")
                .and_then(|s| decode_minhash(s))
                .map(|sig| (n.node_id.as_str(), sig))
        })
        .collect();
    if sigs.len() < 2 {
        return 0;
    }

    // LSH banding: BANDS bands of ROWS rows (BANDS*ROWS == MINHASH_K).
    const BANDS: usize = 8;
    const ROWS: usize = MINHASH_K / BANDS;
    let mut buckets: HashMap<(usize, u64), Vec<usize>> = HashMap::new();
    for (idx, (_, sig)) in sigs.iter().enumerate() {
        for band in 0..BANDS {
            let mut h: u64 = 0xcbf2_9ce4_8422_2325;
            for row in 0..ROWS {
                h ^= sig[band * ROWS + row] as u64;
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            buckets.entry((band, h)).or_default().push(idx);
        }
    }

    // Candidate pairs (deduped) from co-bucketed symbols.
    let mut candidates: HashSet<(usize, usize)> = HashSet::new();
    for members in buckets.values() {
        if members.len() < 2 || members.len() > 256 {
            continue; // skip degenerate mega-buckets (all-identical tiny symbols)
        }
        for i in 0..members.len() {
            for j in (i + 1)..members.len() {
                let (a, b) = (members[i], members[j]);
                candidates.insert((a.min(b), a.max(b)));
            }
        }
    }

    let mut per_node: HashMap<usize, usize> = HashMap::new();
    let mut count = 0;
    let mut scored: Vec<(usize, usize, f64)> = candidates
        .into_iter()
        .filter_map(|(a, b)| {
            let s = minhash_jaccard(&sigs[a].1, &sigs[b].1);
            (s >= SIMILAR_THRESHOLD).then_some((a, b, s))
        })
        .collect();
    // Strongest links first so the per-node cap keeps the best neighbours.
    scored.sort_by(|x, y| y.2.partial_cmp(&x.2).unwrap_or(std::cmp::Ordering::Equal));

    for (a, b, score) in scored {
        if *per_node.get(&a).unwrap_or(&0) >= SIMILAR_CAP_PER_NODE
            || *per_node.get(&b).unwrap_or(&0) >= SIMILAR_CAP_PER_NODE
        {
            continue;
        }
        *per_node.entry(a).or_default() += 1;
        *per_node.entry(b).or_default() += 1;
        edges.push(ExtractedEdge {
            source: sigs[a].0.to_string(),
            target: sigs[b].0.to_string(),
            edge_type: "similar_to".to_string(),
            properties: HashMap::from([("score".to_string(), format!("{score:.2}"))]),
        });
        count += 1;
    }
    count
}

/// Minimum estimated Jaccard for a `similar_to` edge.
const SIMILAR_THRESHOLD: f64 = 0.5;
/// Max `similar_to` edges per symbol (keeps a clone family bounded).
const SIMILAR_CAP_PER_NODE: usize = 10;

/// Bind one call site to a definition, most-specific signal first, tagging the
/// strategy + confidence. Returns `None` (unresolved) rather than guessing an
/// ambiguous callee. Preference: receiver/class scope → same-file → arity-unique
/// → unique-anywhere.
#[allow(clippy::too_many_arguments)]
fn resolve_site(
    site: &DecodedSite,
    caller_file: &str,
    caller_scope: &str,
    caller_lang: &str,
    def_index: &HashMap<String, Vec<Def>>,
    scoped: &HashMap<(String, String), Vec<String>>,
    class_by_name: &HashMap<String, Vec<ClassDef>>,
    bases_of: &HashMap<String, Vec<String>>,
) -> Option<(String, &'static str, f64)> {
    let callee = site.callee.as_str();
    let recv = site.receiver.as_str();

    // 1. `self`/`this`/`super` receiver (or an implicit-this language's bare call)
    //    → a method of the caller's own class or an inherited one.
    let implicit_this = matches!(caller_lang, "java" | "cpp" | "csharp");
    let self_recv = matches!(recv, "self" | "this" | "super") || (recv.is_empty() && implicit_this);
    if self_recv && !caller_scope.is_empty() {
        if let Some(id) = lookup_method(caller_scope, callee, scoped, bases_of) {
            return Some((id, "scoped", 0.95));
        }
    }
    // 2. Explicit receiver naming a known class (static call / typed receiver)
    //    → a method of that class or an inherited one.
    if !recv.is_empty() && class_by_name.contains_key(recv) {
        if let Some(id) = lookup_method(recv, callee, scoped, bases_of) {
            return Some((id, "scoped", 0.9));
        }
    }

    let defs = def_index.get(callee)?;
    // 3. A definition in the caller's own file.
    if let Some(d) = defs.iter().find(|d| d.file_path == caller_file) {
        return Some((d.id.clone(), "same_file", 0.9));
    }
    // 4. Disambiguate same-name defs by argument count.
    if let Some(argc) = site.argc {
        let matches: Vec<&Def> = defs.iter().filter(|d| d.arity == Some(argc)).collect();
        if let [only] = matches.as_slice() {
            return Some((only.id.clone(), "arity", 0.7));
        }
    }
    // 5. A unique definition anywhere in the batch.
    if let [only] = defs.as_slice() {
        return Some((only.id.clone(), "unique", 0.6));
    }
    None
}

/// Find a method `name` defined on `class` or any of its (transitive) ancestors.
/// Returns the first matching method node id, or `None`.
fn lookup_method(
    class: &str,
    name: &str,
    scoped: &HashMap<(String, String), Vec<String>>,
    bases_of: &HashMap<String, Vec<String>>,
) -> Option<String> {
    for cls in ancestors(class, bases_of) {
        if let Some(ids) = scoped.get(&(cls, name.to_string())) {
            if let Some(first) = ids.first() {
                return Some(first.clone());
            }
        }
    }
    None
}

/// The class itself followed by its transitive base/interface names (bounded,
/// cycle-safe).
fn ancestors(class: &str, bases_of: &HashMap<String, Vec<String>>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut queue = vec![class.to_string()];
    while let Some(c) = queue.pop() {
        if !seen.insert(c.clone()) {
            continue;
        }
        out.push(c.clone());
        if out.len() > 64 {
            break;
        }
        if let Some(bs) = bases_of.get(&c) {
            for b in bs {
                if !seen.contains(b) {
                    queue.push(b.clone());
                }
            }
        }
    }
    out
}

/// Bind a base/interface name to a class definition, preferring the referrer's
/// own file, then a unique definition anywhere. Ambiguous → `None` (no edge).
fn resolve_class(
    name: &str,
    file: &str,
    class_by_name: &HashMap<String, Vec<ClassDef>>,
) -> Option<String> {
    let defs = class_by_name.get(name)?;
    if let Some(local) = defs.iter().find(|d| d.file_path == file) {
        return Some(local.id.clone());
    }
    match defs.as_slice() {
        [only] => Some(only.id.clone()),
        _ => None,
    }
}

/// Split a comma-joined property into its non-empty parts.
fn split_csv(v: Option<&String>) -> Vec<String> {
    v.map(|s| {
        s.split(',')
            .filter(|x| !x.is_empty())
            .map(str::to_string)
            .collect()
    })
    .unwrap_or_default()
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

    /// The `strategy` of the resolved call edge whose caller/callee names match.
    fn strategy_of(r: &IndexResult, caller: &str, callee: &str) -> Option<String> {
        let cid = r
            .nodes
            .iter()
            .find(|n| n.properties.get("name").map(String::as_str) == Some(caller))
            .map(|n| n.node_id.clone())?;
        r.edges
            .iter()
            .find(|e| {
                e.edge_type == "calls"
                    && e.source == cid
                    && e.properties.get("name").map(String::as_str) == Some(callee)
            })
            .and_then(|e| e.properties.get("strategy").cloned())
    }

    fn node_id(r: &IndexResult, name: &str, file: &str) -> String {
        r.nodes
            .iter()
            .find(|n| {
                n.properties.get("name").map(String::as_str) == Some(name)
                    && n.properties.get("file_path").map(String::as_str) == Some(file)
            })
            .unwrap_or_else(|| panic!("no {name} in {file}"))
            .node_id
            .clone()
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
        // definition and no scope/arity disambiguation → must NOT emit a call edge.
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
        let local_dup = node_id(&r, "dup", "c.py");
        let go = node_id(&r, "go", "c.py");
        assert!(
            r.edges
                .iter()
                .any(|e| e.edge_type == "calls" && e.source == go && e.target == local_dup),
            "go→dup must bind the SAME-FILE dup"
        );
    }

    #[test]
    fn scoped_method_resolves_over_same_named_free_function() {
        // Java: `this.work()` inside Worker must bind Worker.work via scope, not
        // the same-named method on Other.
        let r = index_repository(&files(&[(
            "W.java",
            "class Worker {\n  void run() { this.work(); }\n  void work() {}\n}\nclass Other { void work() {} }\n",
        )]));
        assert_eq!(
            strategy_of(&r, "run", "work").as_deref(),
            Some("scoped"),
            "this.work() should resolve by scope; edges={:?}",
            r.edges
        );
        assert!(r.calls_scope_resolved >= 1);
    }

    #[test]
    fn implicit_this_call_resolves_to_own_method_java() {
        // Java implicit-this: a bare `helper()` inside a method is a self call.
        let r = index_repository(&files(&[(
            "A.java",
            "class A {\n  int f() { return helper(); }\n  int helper() { return 1; }\n}\n",
        )]));
        assert_eq!(strategy_of(&r, "f", "helper").as_deref(), Some("scoped"));
    }

    #[test]
    fn overload_disambiguated_by_arity() {
        // Two free functions `make` (arity 1 and 2); a 2-arg call binds the 2-arg
        // def — the name-only resolver would drop both as ambiguous.
        let r = index_repository(&files(&[
            ("a.py", "def make(x):\n    return x\n"),
            ("b.py", "def make(x, y):\n    return x + y\n"),
            ("c.py", "def go():\n    return make(1, 2)\n"),
        ]));
        let two_arg = node_id(&r, "make", "b.py");
        let go = node_id(&r, "go", "c.py");
        assert!(
            r.edges.iter().any(|e| e.edge_type == "calls"
                && e.source == go
                && e.target == two_arg
                && e.properties.get("strategy").map(String::as_str) == Some("arity")),
            "2-arg make() should bind the 2-param def by arity; edges={:?}",
            r.edges
        );
        assert!(r.calls_type_resolved >= 1);
    }

    #[test]
    fn inherited_method_resolves_through_base() {
        // `self.base_op()` in a subclass binds the inherited base method.
        let r = index_repository(&files(&[(
            "h.py",
            "class Base:\n    def base_op(self):\n        return 1\n\nclass Child(Base):\n    def run(self):\n        return self.base_op()\n",
        )]));
        assert_eq!(
            strategy_of(&r, "run", "base_op").as_deref(),
            Some("scoped"),
            "inherited self.base_op() should resolve by scope; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn inherits_edge_emitted_python() {
        let r = index_repository(&files(&[(
            "h.py",
            "class Base:\n    pass\n\nclass Child(Base):\n    pass\n",
        )]));
        assert!(
            r.edges.iter().any(|e| e.edge_type == "inherits"
                && e.source == node_id(&r, "Child", "h.py")
                && e.target == node_id(&r, "Base", "h.py")),
            "Child→Base inherits edge expected; edges={:?}",
            r.edges
        );
        assert!(r.inherits_edges >= 1);
    }

    #[test]
    fn realizes_edge_emitted_java() {
        let r = index_repository(&files(&[(
            "S.java",
            "interface Runnable { void run(); }\nclass Service implements Runnable { public void run() {} }\n",
        )]));
        assert!(
            r.edges.iter().any(|e| e.edge_type == "realizes"
                && e.source == node_id(&r, "Service", "S.java")
                && e.target == node_id(&r, "Runnable", "S.java")),
            "Service→Runnable realizes edge expected; edges={:?}",
            r.edges
        );
        assert!(r.realizes_edges >= 1);
    }

    #[test]
    fn similar_functions_get_similar_to_edge() {
        // Two structurally identical functions with different identifier names →
        // a `similar_to` edge (renamed-variable clone); an unrelated one does not.
        let r = index_repository(&files(&[
            (
                "a.py",
                "def alpha(x):\n    total = 0\n    for i in x:\n        total = total + i\n    return total\n",
            ),
            (
                "b.py",
                "def beta(y):\n    acc = 0\n    for j in y:\n        acc = acc + j\n    return acc\n",
            ),
            ("c.py", "def unrelated():\n    print('hi')\n    return None\n"),
        ]));
        let name_of = |id: &str| -> String {
            r.nodes
                .iter()
                .find(|n| n.node_id == id)
                .and_then(|n| n.properties.get("name").cloned())
                .unwrap_or_default()
        };
        let sim: Vec<(String, String)> = r
            .edges
            .iter()
            .filter(|e| e.edge_type == "similar_to")
            .map(|e| {
                let mut pair = [name_of(&e.source), name_of(&e.target)];
                pair.sort();
                let [a, b] = pair;
                (a, b)
            })
            .collect();
        assert!(r.similar_edges >= 1);
        assert!(
            sim.contains(&("alpha".to_string(), "beta".to_string())),
            "alpha~beta clone expected, got {sim:?}"
        );
        assert!(
            !sim.iter()
                .any(|(a, b)| a == "unrelated" || b == "unrelated"),
            "unrelated function must not be linked, got {sim:?}"
        );
        // Every similar_to edge carries a score; minhash is stripped from nodes.
        assert!(r
            .edges
            .iter()
            .filter(|e| e.edge_type == "similar_to")
            .all(|e| e.properties.contains_key("score")));
        assert!(r
            .nodes
            .iter()
            .all(|n| !n.properties.contains_key("minhash")));
    }

    #[test]
    fn call_sites_property_stripped_from_nodes() {
        let r = index_repository(&files(&[(
            "m.py",
            "def helper():\n    return 1\n\ndef caller():\n    return helper()\n",
        )]));
        assert!(
            r.nodes
                .iter()
                .all(|n| !n.properties.contains_key("call_sites")),
            "internal call_sites property must not leak onto graph nodes"
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
