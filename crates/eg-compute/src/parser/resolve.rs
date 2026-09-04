// CONCEPT:EG-KG.compute.turn-each-project / KG-2.100 — Cross-file, type/scope-resolved call + import graph.
//
// `parse_file`/`parse_files` extract per-symbol call sites (the `call_sites`
// property: receiver/callee/argc triples) and per-file raw import edges, but
// those targets are bare names — not the symbols they refer to. Resolution binds
// them across a whole batch of files in one pass:
//   - a symbol's call sites → the SYMBOL ids that DEFINE them                 (`calls`)
//   - a class's base/interface names → the class SYMBOL ids   (`inherits`/`realizes`)
//   - a file's import module strings → the file that defines the module (`depends_on`)
//
// Call resolution is now **type/scope-aware** (CONCEPT:EG-KG.compute.type-scope-resolved-call): a method call
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
    /// Of `calls_resolved`, those bound by receiver/class scope (CONCEPT:EG-KG.compute.type-scope-resolved-call).
    pub calls_scope_resolved: usize,
    /// Of `calls_resolved`, those disambiguated by argument-count match.
    pub calls_type_resolved: usize,
    /// Class→base `inherits` edges emitted.
    pub inherits_edges: usize,
    /// Class→interface `realizes` edges emitted.
    pub realizes_edges: usize,
    /// Model-free `similar_to` edges emitted (CONCEPT:EG-KG.compute.model-free-similar-code).
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
// CONCEPT:EG-KG.compute.parse-resolve-span — span the parse+resolve indexing pass (AST throughput trace).
#[tracing::instrument(
    skip(files),
    fields(n_files = files.len(), total_bytes = files.iter().map(|(_, b)| b.len()).sum::<usize>())
)]
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

    let mut inputs = ResolutionInputs::default();
    collect_resolution_inputs(results, &mut out, &mut inputs);
    let bases_of = build_bases_of(&inputs.class_by_name);
    let context = ResolutionContext {
        def_index: &inputs.def_index,
        scoped: &inputs.scoped,
        class_by_name: &inputs.class_by_name,
        bases_of: &bases_of,
    };

    // ── Resolve calls: caller symbol → callee definition (type/scope-aware) ──
    let calls = resolve_call_edges(&out.nodes, &context, &mut inputs.edges);
    out.calls_resolved = calls.resolved;
    out.calls_unresolved = calls.unresolved;
    out.calls_scope_resolved = calls.scope_resolved;
    out.calls_type_resolved = calls.type_resolved;

    // ── Structural edges: class → base (`inherits`) / interface (`realizes`) ──
    let structural = resolve_structural_edges(&inputs.class_by_name, &mut inputs.edges);
    out.inherits_edges = structural.inherits;
    out.realizes_edges = structural.realizes;

    // ── Resolve imports: importer file → defining file ────────────────────
    let imports = resolve_import_edges(&inputs.import_raw, &file_paths, &mut inputs.edges);
    out.imports_resolved = imports.resolved;
    out.imports_unresolved = imports.unresolved;

    // ── Model-free similarity: LSH-band the MinHash signatures (CONCEPT:EG-KG.compute.model-free-similar-code) ──
    out.similar_edges = similarity_edges(&out.nodes, &mut inputs.edges);

    // `call_sites`/`minhash` are resolution-only inputs; don't leak them onto nodes.
    for n in &mut out.nodes {
        n.properties.remove("call_sites");
        n.properties.remove("minhash");
    }

    out.edges = inputs.edges;
    out
}

#[derive(Default)]
struct ResolutionInputs {
    // name → definitions (functions, methods, classes). Built first so a call in
    // any file can resolve to a def in any other.
    def_index: HashMap<String, Vec<Def>>,
    // (class scope, method name) → method node ids, for receiver-scoped calls.
    scoped: HashMap<(String, String), Vec<String>>,
    // class name → its definitions (for structural edges + scoped lookup).
    class_by_name: HashMap<String, Vec<ClassDef>>,
    // Carry the (file→module) import facts to resolve after node merge.
    import_raw: Vec<(String, String)>,
    edges: Vec<ExtractedEdge>,
}

struct ResolutionContext<'a> {
    def_index: &'a HashMap<String, Vec<Def>>,
    scoped: &'a HashMap<(String, String), Vec<String>>,
    class_by_name: &'a HashMap<String, Vec<ClassDef>>,
    bases_of: &'a HashMap<String, Vec<String>>,
}

/// Intermediate caller data kept owned while resolution appends edges.
struct CallContext {
    id: String,
    file: String,
    scope: String,
    language: String,
    sites: Vec<DecodedSite>,
}

#[derive(Default)]
struct CallCounts {
    resolved: usize,
    unresolved: usize,
    scope_resolved: usize,
    type_resolved: usize,
}

struct CallResolutionState<'a> {
    seen: HashSet<(String, String)>,
    edges: &'a mut Vec<ExtractedEdge>,
    counts: CallCounts,
}

#[derive(Default)]
struct StructuralCounts {
    inherits: usize,
    realizes: usize,
}

struct StructuralResolutionState<'a> {
    seen: HashSet<(String, String, &'static str)>,
    edges: &'a mut Vec<ExtractedEdge>,
    counts: StructuralCounts,
}

#[derive(Default)]
struct ImportCounts {
    resolved: usize,
    unresolved: usize,
}

fn collect_resolution_inputs(
    results: &[ParseResult],
    out: &mut IndexResult,
    inputs: &mut ResolutionInputs,
) {
    for result in results {
        out.symbols_extracted += result.symbols_extracted;
        collect_result_nodes(result, out, inputs);
        collect_result_edges(result, inputs);
    }
}

fn collect_result_nodes(
    result: &ParseResult,
    out: &mut IndexResult,
    inputs: &mut ResolutionInputs,
) {
    for node in &result.nodes {
        index_node(node, inputs);
        out.nodes.push(clone_node(node));
    }
}

fn index_node(node: &ExtractedNode, inputs: &mut ResolutionInputs) {
    if node.node_type != "SYMBOL" {
        return;
    }
    let Some(name) = node.properties.get("name") else {
        return;
    };
    let Some(file_path) = node.properties.get("file_path") else {
        return;
    };
    index_symbol(node, name, file_path, inputs);
}

fn index_symbol(node: &ExtractedNode, name: &str, file_path: &str, inputs: &mut ResolutionInputs) {
    let symbol_type = node.properties.get("symbol_type").map(String::as_str);
    let scope = node.properties.get("scope").cloned().unwrap_or_default();
    let arity = node.properties.get("arity").and_then(|a| a.parse().ok());
    let definition = Def {
        id: node.node_id.clone(),
        file_path: file_path.to_string(),
        arity,
    };

    match symbol_type {
        Some("Function") => {
            inputs
                .def_index
                .entry(name.to_string())
                .or_default()
                .push(definition);
            if !scope.is_empty() {
                inputs
                    .scoped
                    .entry((scope, name.to_string()))
                    .or_default()
                    .push(node.node_id.clone());
            }
        }
        Some("Class") => {
            inputs
                .def_index
                .entry(name.to_string())
                .or_default()
                .push(definition);
            inputs
                .class_by_name
                .entry(name.to_string())
                .or_default()
                .push(ClassDef {
                    id: node.node_id.clone(),
                    file_path: file_path.to_string(),
                    bases: split_csv(node.properties.get("bases")),
                    interfaces: split_csv(node.properties.get("interfaces")),
                });
        }
        _ => {}
    }
}

fn collect_result_edges(result: &ParseResult, inputs: &mut ResolutionInputs) {
    for edge in &result.edges {
        match edge.edge_type.as_str() {
            "IMPLEMENTS" => inputs.edges.push(clone_edge(edge)),
            // Raw forms are superseded by the resolved edges built below.
            "calls_raw" => {}
            "depends_on_raw" => {
                let importer = edge.source.strip_prefix("file:").unwrap_or(&edge.source);
                inputs
                    .import_raw
                    .push((importer.to_string(), edge.target.clone()));
            }
            _ => inputs.edges.push(clone_edge(edge)),
        }
    }
}

fn build_bases_of(class_by_name: &HashMap<String, Vec<ClassDef>>) -> HashMap<String, Vec<String>> {
    let mut bases_of = HashMap::new();
    for (name, definitions) in class_by_name {
        let mut bases = Vec::new();
        for definition in definitions {
            append_unique_bases(&mut bases, definition);
        }
        bases_of.insert(name.clone(), bases);
    }
    bases_of
}

fn append_unique_bases(bases: &mut Vec<String>, definition: &ClassDef) {
    for base in definition.bases.iter().chain(definition.interfaces.iter()) {
        if !bases.contains(base) {
            bases.push(base.clone());
        }
    }
}

fn resolve_call_edges(
    nodes: &[ExtractedNode],
    resolution: &ResolutionContext<'_>,
    edges: &mut Vec<ExtractedEdge>,
) -> CallCounts {
    let mut state = CallResolutionState {
        seen: HashSet::new(),
        edges,
        counts: CallCounts::default(),
    };
    for node in nodes {
        let Some(caller) = call_context(node) else {
            continue;
        };
        for site in &caller.sites {
            let resolved = resolve_site(
                site,
                &caller.file,
                &caller.scope,
                &caller.language,
                resolution,
            );
            match resolved {
                Some((target, strategy, confidence)) => record_call_resolution(
                    &caller.id, site, target, strategy, confidence, &mut state,
                ),
                None => state.counts.unresolved += 1,
            }
        }
    }
    state.counts
}

fn call_context(node: &ExtractedNode) -> Option<CallContext> {
    if node.properties.get("symbol_type").map(String::as_str) != Some("Function") {
        return None;
    }
    Some(CallContext {
        id: node.node_id.clone(),
        file: node.properties.get("file_path")?.clone(),
        scope: node.properties.get("scope").cloned().unwrap_or_default(),
        language: node.properties.get("language").cloned().unwrap_or_default(),
        sites: decode_node_sites(node),
    })
}

fn decode_node_sites(node: &ExtractedNode) -> Vec<DecodedSite> {
    match node.properties.get("call_sites") {
        Some(call_sites) if !call_sites.is_empty() => decode_call_sites(call_sites),
        _ => fallback_call_sites(node.properties.get("calls")),
    }
}

fn fallback_call_sites(calls: Option<&String>) -> Vec<DecodedSite> {
    calls
        .filter(|calls| !calls.is_empty())
        .map(|calls| {
            calls
                .split(',')
                .filter(|name| !name.is_empty())
                .map(|name| DecodedSite {
                    receiver: String::new(),
                    callee: name.to_string(),
                    argc: None,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn record_call_resolution(
    caller_id: &str,
    site: &DecodedSite,
    target: String,
    strategy: &'static str,
    confidence: f64,
    state: &mut CallResolutionState<'_>,
) {
    if state.seen.insert((caller_id.to_string(), target.clone())) {
        state.edges.push(ExtractedEdge {
            source: caller_id.to_string(),
            target,
            edge_type: "calls".to_string(),
            properties: HashMap::from([
                ("name".to_string(), site.callee.clone()),
                ("strategy".to_string(), strategy.to_string()),
                ("confidence".to_string(), format!("{confidence:.2}")),
            ]),
        });
    }
    state.counts.resolved += 1;
    match strategy {
        "scoped" => state.counts.scope_resolved += 1,
        "arity" => state.counts.type_resolved += 1,
        _ => {}
    }
}

fn resolve_structural_edges(
    class_by_name: &HashMap<String, Vec<ClassDef>>,
    edges: &mut Vec<ExtractedEdge>,
) -> StructuralCounts {
    let mut state = StructuralResolutionState {
        seen: HashSet::new(),
        edges,
        counts: StructuralCounts::default(),
    };
    for definitions in class_by_name.values() {
        for definition in definitions {
            append_named_structural_edges(
                definition,
                &definition.bases,
                "inherits",
                class_by_name,
                &mut state,
            );
            append_named_structural_edges(
                definition,
                &definition.interfaces,
                "realizes",
                class_by_name,
                &mut state,
            );
        }
    }
    state.counts
}

fn append_named_structural_edges(
    definition: &ClassDef,
    names: &[String],
    edge_type: &'static str,
    class_by_name: &HashMap<String, Vec<ClassDef>>,
    state: &mut StructuralResolutionState<'_>,
) {
    for base in names {
        if let Some(target) = resolve_class(base, &definition.file_path, class_by_name) {
            append_structural_edge(definition, base, target, edge_type, state);
        }
    }
}

fn append_structural_edge(
    definition: &ClassDef,
    base: &str,
    target: String,
    edge_type: &'static str,
    state: &mut StructuralResolutionState<'_>,
) {
    if target == definition.id
        || !state
            .seen
            .insert((definition.id.clone(), target.clone(), edge_type))
    {
        return;
    }
    state.edges.push(ExtractedEdge {
        source: definition.id.clone(),
        target,
        edge_type: edge_type.to_string(),
        properties: HashMap::from([("name".to_string(), base.to_string())]),
    });
    match edge_type {
        "inherits" => state.counts.inherits += 1,
        _ => state.counts.realizes += 1,
    }
}

fn resolve_import_edges(
    import_raw: &[(String, String)],
    file_paths: &HashSet<&str>,
    edges: &mut Vec<ExtractedEdge>,
) -> ImportCounts {
    let mut counts = ImportCounts::default();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for (importer, module) in import_raw {
        let Some(target_file) = resolve_import(importer, module, file_paths) else {
            counts.unresolved += 1;
            continue;
        };
        append_import_edge(importer, module, &target_file, &mut seen, edges);
        counts.resolved += 1;
    }
    counts
}

fn append_import_edge(
    importer: &str,
    module: &str,
    target_file: &str,
    seen: &mut HashSet<(String, String)>,
    edges: &mut Vec<ExtractedEdge>,
) {
    let source = format!("file:{importer}");
    let target = format!("file:{target_file}");
    if !seen.insert((source.clone(), target.clone())) {
        return;
    }
    edges.push(ExtractedEdge {
        source,
        target,
        edge_type: "depends_on".to_string(),
        properties: HashMap::from([("module".to_string(), module.to_string())]),
    });
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
    // The comparator must be a TOTAL order: `sort_by` is stable, so ordering by
    // score alone leaves equal-score pairs in `candidates`' `HashSet` iteration
    // order — which is per-process — and the greedy `SIMILAR_CAP_PER_NODE` loop
    // below then admits a different edge set on every run. The pair's `sigs`
    // indices break every tie: `sigs` is built from `nodes` in order, so they
    // are stable across runs. (`buckets` iteration order is irrelevant: it only
    // feeds `candidates`, a `HashSet` whose membership is order-independent —
    // each bucket's member list is built by ascending `idx`, the skip rule reads
    // only `members.len()`, and every pair is normalised to (min, max).)
    scored.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.0.cmp(&y.0))
            .then_with(|| x.1.cmp(&y.1))
    });

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
fn resolve_site(
    site: &DecodedSite,
    caller_file: &str,
    caller_scope: &str,
    caller_lang: &str,
    context: &ResolutionContext<'_>,
) -> Option<(String, &'static str, f64)> {
    let callee = site.callee.as_str();
    let recv = site.receiver.as_str();

    // 1. `self`/`this`/`super` receiver (or an implicit-this language's bare call)
    //    → a method of the caller's own class or an inherited one.
    let implicit_this = matches!(caller_lang, "java" | "cpp" | "csharp");
    let self_recv = matches!(recv, "self" | "this" | "super") || (recv.is_empty() && implicit_this);
    if self_recv && !caller_scope.is_empty() {
        if let Some(id) = lookup_method(caller_scope, callee, context.scoped, context.bases_of) {
            return Some((id, "scoped", 0.95));
        }
    }
    // 2. Explicit receiver naming a known class (static call / typed receiver)
    //    → a method of that class or an inherited one.
    if !recv.is_empty() && context.class_by_name.contains_key(recv) {
        if let Some(id) = lookup_method(recv, callee, context.scoped, context.bases_of) {
            return Some((id, "scoped", 0.9));
        }
    }

    let defs = context.def_index.get(callee)?;
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
/// `::`-separated paths (Rust).
///
/// Importer-relative forms — Rust `crate::`/`self::`/`super::` and Python
/// leading-dot relatives — are **anchored to the importer** rather than stripped
/// to a bare stem. Stripping `crate::reduce` to `reduce` made it suffix-match
/// every `reduce.rs` in the batch, so `eg-viz-export/src/render.rs` could bind to
/// `eg-compute/src/mining/reduce.rs`; `crate::` names the importer's OWN crate
/// root, and that is exactly the information the strip threw away. An anchored
/// path is therefore matched EXACTLY under its anchor: one that does not exist
/// there is left unresolved rather than bound to a same-named file elsewhere.
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
        return match_with_extensions(&joined, files, importer, false).filter(|f| f != importer);
    }

    // The directory a Rust file's module OWNS. A crate/module root — `mod.rs`,
    // `lib.rs`, `main.rs` — owns its own directory; any other `a/b.rs` owns the
    // sibling directory `a/b/` (the non-`mod.rs` layout, 943 of 992 `.rs` files
    // in this workspace). `self::` resolves there; each `super::` climbs one
    // level, so `super::x` from `a/b.rs` is the sibling `a/x`, not `a/../x`.
    let module_dir = |path: &str| -> String {
        let dir = dir_of(path);
        let file = path.rsplit('/').next().unwrap_or(path);
        let Some(stem) = file.strip_suffix(".rs") else {
            return dir;
        };
        if matches!(stem, "mod" | "lib" | "main") {
            dir
        } else if dir.is_empty() {
            stem.to_string()
        } else {
            format!("{dir}/{stem}")
        }
    };

    // (anchor dir, module path relative to it) for an importer-relative form;
    // `None` leaves an absolute/dotted path to the suffix matcher below.
    let mut anchor: Option<(String, String)> = None;
    if let Some(rest) = m
        .strip_prefix("crate::")
        .or_else(|| (m == "crate").then_some(""))
    {
        // Crate root: the path up to and including the importer's last `src`
        // segment (this workspace's layout). No `src` segment — a single-file
        // example/bench crate — falls back to the importer's own directory.
        let segs: Vec<&str> = importer.split('/').collect();
        let root = match segs.iter().rposition(|s| *s == "src") {
            Some(i) => segs[..=i].join("/"),
            None => dir_of(importer),
        };
        anchor = Some((root, rest.to_string()));
    } else if m.starts_with("self::") || m.starts_with("super::") || m == "self" || m == "super" {
        let mut base = module_dir(importer);
        let mut rest = m;
        loop {
            if let Some(r) = rest.strip_prefix("self::") {
                rest = r;
            } else if let Some(r) = rest.strip_prefix("super::") {
                base = dir_of(&base);
                rest = r;
            } else if rest == "self" || rest == "super" {
                if rest == "super" {
                    base = dir_of(&base);
                }
                rest = "";
                break;
            } else {
                break;
            }
        }
        anchor = Some((base, rest.to_string()));
    } else if m.starts_with('.') {
        // Python relative import: the first dot is the importer's own package,
        // each further dot climbs one package.
        let dots = m.len() - m.trim_start_matches('.').len();
        let mut base = dir_of(importer);
        for _ in 1..dots {
            base = dir_of(&base);
        }
        anchor = Some((base, m[dots..].to_string()));
    }

    if let Some((base, rest)) = anchor {
        // A file never depends on itself: `from . import x` inside a package's
        // own `__init__.py` anchors back onto the importer.
        let not_self = |hit: Option<String>| hit.filter(|f| f != importer);
        let join = |b: &str, r: &str| -> String {
            match (b.is_empty(), r.is_empty()) {
                (_, true) => b.to_string(),
                (true, false) => r.to_string(),
                _ => format!("{b}/{r}"),
            }
        };
        let rel = rest.replace("::", "/").replace('.', "/");
        let segs: Vec<&str> = rel.split('/').filter(|s| !s.is_empty()).collect();
        if segs.is_empty() {
            // `from . import x` / bare `crate` — the anchor dir IS the package.
            return not_self(match_with_extensions(&base, files, importer, true));
        }
        // A Rust `use` path ends in the ITEM (`crate::a::b::Thing`), so try the
        // longest module path first and drop one trailing segment at a time.
        for take in (1..=segs.len()).rev() {
            let stem = join(&base, &segs[..take].join("/"));
            if let Some(hit) = not_self(match_with_extensions(&stem, files, importer, true)) {
                return Some(hit);
            }
        }
        return None;
    }

    // Absolute dotted (Python `a.b.c`, Java `com.foo.Bar`) or `::` (Rust) module
    // path -> slash path, then suffix-match against the batch's files.
    let stem = m.replace("::", "/").replace('.', "/");
    if stem.is_empty() {
        return None;
    }
    match_with_extensions(&stem, files, importer, false).filter(|f| f != importer)
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
/// matched file path.
///
/// `exact` (an importer-anchored stem, already a full batch-relative path) admits
/// only the literal path; otherwise a boundary-aware path SUFFIX also matches
/// (`auth.py` != `oauth.py`), tolerating a repo-root prefix that a dotted module
/// path omits.
///
/// Determinism: `files` is a `HashSet`, so "the first file that matches" is a
/// per-process hash order — the same binary on the same input returned different
/// targets on different runs. Every match for a spelling is now collected and the
/// winner chosen by a stated TOTAL order:
///   1. extension / index-file precedence (the outer `candidates` loop, unchanged);
///   2. longest shared leading path-segment prefix with the importer — the file
///      nearest the importer in the tree wins, so an exact match always beats a
///      suffix match in a foreign subtree;
///   3. lexicographically smallest path.
fn match_with_extensions(
    stem: &str,
    files: &HashSet<&str>,
    importer: &str,
    exact: bool,
) -> Option<String> {
    const EXTS: &[&str] = &[
        "py", "pyi", "ts", "tsx", "js", "jsx", "mjs", "go", "rs", "java",
    ];
    const INDEX: &[&str] = &["__init__.py", "index.ts", "index.js", "mod.rs"];

    /// Leading path segments `a` and `b` share.
    fn shared_segments(a: &str, b: &str) -> usize {
        a.split('/')
            .zip(b.split('/'))
            .take_while(|(x, y)| x == y)
            .count()
    }

    let mut candidates: Vec<String> = Vec::new();
    for ext in EXTS {
        candidates.push(format!("{stem}.{ext}"));
    }
    for idx in INDEX {
        candidates.push(format!("{stem}/{idx}"));
    }

    for cand in &candidates {
        if exact {
            // Anchored: a hash lookup, and no cross-subtree fallback at all.
            if let Some(f) = files.get(cand.as_str()) {
                return Some((*f).to_string());
            }
            continue;
        }
        let suffix = format!("/{cand}");
        let mut matches: Vec<&str> = files
            .iter()
            .copied()
            .filter(|f| *f == cand.as_str() || f.ends_with(suffix.as_str()))
            .collect();
        if matches.is_empty() {
            continue;
        }
        matches.sort_unstable_by(|a, b| {
            shared_segments(b, importer)
                .cmp(&shared_segments(a, importer))
                .then_with(|| a.cmp(b))
        });
        return Some(matches[0].to_string());
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

    /// Regression: `similarity_edges` sorted equal-score candidate pairs by score
    /// only. `sort_by` is stable, so tied pairs kept the per-process `HashSet`
    /// iteration order and the greedy `SIMILAR_CAP_PER_NODE` loop admitted a
    /// DIFFERENT edge set on every run (measured: 72,944 vs 72,889 `similar_to`
    /// edges from one binary on one input). A clone family larger than the cap
    /// makes every pair a tie AND forces the cap to reject some of them; two
    /// `index_repository` calls must still produce the identical edge sequence.
    /// (Each `HashMap`/`HashSet` gets a fresh `RandomState`, so the two calls
    /// really do walk the candidate set in different orders.)
    #[test]
    fn tied_similarity_scores_are_ordered_deterministically() {
        // 14 structurally identical functions with distinct names → all C(14,2)
        // pairs score 1.0, and the per-node cap (10) must reject some of them.
        let src: Vec<(String, String)> = (0..14)
            .map(|i| {
                (
                    format!("f{i}.py"),
                    format!(
                        "def fn{i}(seq{i}):\n    acc{i} = 0\n    for item{i} in seq{i}:\n        acc{i} = acc{i} + item{i}\n    return acc{i}\n"
                    ),
                )
            })
            .collect();
        let batch: Vec<(String, Vec<u8>)> = src
            .iter()
            .map(|(p, b)| (p.clone(), b.as_bytes().to_vec()))
            .collect();
        let sim = |r: &IndexResult| -> Vec<(String, String)> {
            r.edges
                .iter()
                .filter(|e| e.edge_type == "similar_to")
                .map(|e| (e.source.clone(), e.target.clone()))
                .collect()
        };
        let a = index_repository(&batch);
        let b = index_repository(&batch);
        let (ea, eb) = (sim(&a), sim(&b));
        assert!(
            ea.len() < 14 * 13 / 2,
            "the per-node cap must actually reject tied pairs, got {} edges",
            ea.len()
        );
        assert_eq!(ea, eb, "similar_to edge sequence must be run-independent");
    }

    /// Regression: `crate::`/`self::`/`super::` were stripped to a bare stem, so
    /// `crate::reduce` suffix-matched EVERY `reduce.rs` in the batch and bound
    /// `eg-viz-export/src/render.rs` to `eg-compute/src/mining/reduce.rs` — and
    /// which one it picked varied per run. Each anchored form must resolve
    /// inside the importer's own crate, with a same-named decoy present.
    #[test]
    fn rust_crate_and_super_imports_resolve_inside_the_importers_crate() {
        let r = index_repository(&files(&[
            ("crates/a/src/lib.rs", "use crate::reduce;\npub fn a() {}\n"),
            ("crates/a/src/reduce.rs", "pub fn go() {}\n"),
            ("crates/a/src/nested/sibling.rs", "pub struct Thing;\n"),
            (
                "crates/b/src/render.rs",
                "use crate::reduce;\npub fn r() {}\n",
            ),
            ("crates/b/src/reduce.rs", "pub fn go() {}\n"),
            (
                "crates/b/src/nested/deep.rs",
                "use super::sibling::Thing;\npub fn d() {}\n",
            ),
            ("crates/b/src/nested/sibling.rs", "pub struct Thing;\n"),
        ]));
        let target = |src: &str, module: &str| -> Option<String> {
            r.edges
                .iter()
                .find(|e| {
                    e.edge_type == "depends_on"
                        && e.source == format!("file:{src}")
                        && e.properties.get("module").map(String::as_str) == Some(module)
                })
                .map(|e| e.target.clone())
        };
        assert_eq!(
            target("crates/b/src/render.rs", "crate::reduce").as_deref(),
            Some("file:crates/b/src/reduce.rs"),
            "crate:: must anchor on the importer's own crate root; edges={:?}",
            r.edges
        );
        assert_eq!(
            target("crates/a/src/lib.rs", "crate::reduce").as_deref(),
            Some("file:crates/a/src/reduce.rs")
        );
        // `crates/b/src/nested/deep.rs` owns module dir `.../nested/deep`, so
        // `super::` is `.../nested` — never crate `a`'s same-named sibling.
        assert_eq!(
            target("crates/b/src/nested/deep.rs", "super::sibling::Thing").as_deref(),
            Some("file:crates/b/src/nested/sibling.rs"),
            "super:: must stay in the importer's parent module; edges={:?}",
            r.edges
        );
    }

    /// The Python leading-dot relative import has the same shape and was broken
    /// the same way: `.helper` was stripped to `helper` and suffix-matched the
    /// whole batch. It must anchor on the importer's own package.
    #[test]
    fn python_relative_import_anchors_on_the_importers_package() {
        let r = index_repository(&files(&[
            ("pkg/helper.py", "def decoy():\n    return 0\n"),
            ("pkg/sub/helper.py", "def real():\n    return 1\n"),
            (
                "pkg/sub/mod_a.py",
                "from .helper import real\n\ndef go():\n    return real()\n",
            ),
            (
                "pkg/sub/mod_b.py",
                "from ..helper import decoy\n\ndef go():\n    return decoy()\n",
            ),
        ]));
        let target = |src: &str, module: &str| -> Option<String> {
            r.edges
                .iter()
                .find(|e| {
                    e.edge_type == "depends_on"
                        && e.source == format!("file:{src}")
                        && e.properties.get("module").map(String::as_str) == Some(module)
                })
                .map(|e| e.target.clone())
        };
        assert_eq!(
            target("pkg/sub/mod_a.py", ".helper").as_deref(),
            Some("file:pkg/sub/helper.py"),
            "single dot = the importer's own package; edges={:?}",
            r.edges
        );
        assert_eq!(
            target("pkg/sub/mod_b.py", "..helper").as_deref(),
            Some("file:pkg/helper.py"),
            "two dots = the parent package; edges={:?}",
            r.edges
        );
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
