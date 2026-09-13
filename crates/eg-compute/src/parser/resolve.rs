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
use std::collections::{HashMap, HashSet};

#[path = "resolve_imports.rs"]
mod imports;
#[cfg(test)]
use imports::family_of;
use imports::{call_family, resolve_import, split_csv};

pub use eg_types::ingestion_wire::IndexResult;

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
    let bases_of = build_bases_of(&inputs.families);
    let context = ResolutionContext {
        families: &inputs.families,
        bases_of: &bases_of,
    };

    // ── Resolve calls: caller symbol → callee definition (type/scope-aware) ──
    let calls = resolve_call_edges(&out.nodes, &context, &mut inputs.edges);
    out.calls_resolved = calls.resolved;
    out.calls_unresolved = calls.unresolved;
    out.calls_scope_resolved = calls.scope_resolved;
    out.calls_type_resolved = calls.type_resolved;

    // ── Structural edges: class → base (`inherits`) / interface (`realizes`) ──
    let structural = resolve_structural_edges(&inputs.families, &mut inputs.edges);
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

/// The symbol indexes of ONE language family (see [`call_family`]). Call and
/// class resolution never reach outside the family of the file being resolved:
/// a Python call site cannot name a Rust `fn`, so a same-named definition in
/// another family is not a weaker candidate, it is not a candidate at all.
#[derive(Default)]
struct LangIndex {
    // name → definitions (functions, methods, classes). Built first so a call in
    // any file can resolve to a def in any other file OF THIS FAMILY.
    def_index: HashMap<String, Vec<Def>>,
    // (class scope, method name) → method node ids, for receiver-scoped calls.
    scoped: HashMap<(String, String), Vec<String>>,
    // class name → its definitions (for structural edges + scoped lookup).
    class_by_name: HashMap<String, Vec<ClassDef>>,
}

#[derive(Default)]
struct ResolutionInputs {
    // Symbol indexes PARTITIONED by language family, so a cross-language bind is
    // unrepresentable rather than merely filtered out at each decision point.
    families: HashMap<&'static str, LangIndex>,
    // Carry the (file→module) import facts to resolve after node merge.
    import_raw: Vec<(String, String)>,
    edges: Vec<ExtractedEdge>,
}

struct ResolutionContext<'a> {
    families: &'a HashMap<&'static str, LangIndex>,
    /// Per family: class name → its base/interface names.
    bases_of: &'a HashMap<&'static str, HashMap<String, Vec<String>>>,
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
    // Index into the DEFINING file's own language family: a definition is only
    // ever a candidate for a call site written in the same language.
    match node.properties.get("symbol_type").map(String::as_str) {
        Some("Function") => index_function(node, name, family_index(inputs, file_path), file_path),
        Some("Class") => index_class(node, name, family_index(inputs, file_path), file_path),
        _ => {}
    }
}

/// The symbol index of `file_path`'s language family, created on first use.
fn family_index<'a>(inputs: &'a mut ResolutionInputs, file_path: &str) -> &'a mut LangIndex {
    inputs.families.entry(call_family(file_path)).or_default()
}

fn index_function(node: &ExtractedNode, name: &str, index: &mut LangIndex, file_path: &str) {
    push_definition(node, name, index, file_path);
    let scope = node.properties.get("scope").cloned().unwrap_or_default();
    if scope.is_empty() {
        return;
    }
    index
        .scoped
        .entry((scope, name.to_string()))
        .or_default()
        .push(node.node_id.clone());
}

fn index_class(node: &ExtractedNode, name: &str, index: &mut LangIndex, file_path: &str) {
    push_definition(node, name, index, file_path);
    index
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

fn push_definition(node: &ExtractedNode, name: &str, index: &mut LangIndex, file_path: &str) {
    index
        .def_index
        .entry(name.to_string())
        .or_default()
        .push(Def {
            id: node.node_id.clone(),
            file_path: file_path.to_string(),
            arity: node.properties.get("arity").and_then(|a| a.parse().ok()),
        });
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

/// Per family: class name → its base/interface names. Family-scoped like every
/// other index — a Python class must not inherit the bases of a same-named Rust
/// struct, and `ancestors` walks this map by bare name.
fn build_bases_of(
    families: &HashMap<&'static str, LangIndex>,
) -> HashMap<&'static str, HashMap<String, Vec<String>>> {
    families
        .iter()
        .map(|(family, index)| (*family, family_bases_of(&index.class_by_name)))
        .collect()
}

fn family_bases_of(class_by_name: &HashMap<String, Vec<ClassDef>>) -> HashMap<String, Vec<String>> {
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
    families: &HashMap<&'static str, LangIndex>,
    edges: &mut Vec<ExtractedEdge>,
) -> StructuralCounts {
    let mut state = StructuralResolutionState {
        seen: HashSet::new(),
        edges,
        counts: StructuralCounts::default(),
    };
    // Emission ORDER, not membership, was per-process: these are `HashMap`s, so
    // `.values()` walked them in `RandomState` order and the emitted
    // `inherits`/`realizes` sequence differed run to run even though the sorted
    // edge SET was stable. Membership was always safe -- `resolve_class` picks
    // same-file-else-unique-or-none, which is order-free -- but a corpus that is
    // diffed byte-for-byte needs the order too. Walk both key levels sorted.
    let mut family_names: Vec<&'static str> = families.keys().copied().collect();
    family_names.sort_unstable();
    for family in family_names {
        append_family_structural_edges(&families[family].class_by_name, &mut state);
    }
    state.counts
}

/// Emit the `inherits`/`realizes` edges of ONE language family. A base name is
/// resolved only against classes of that same family: `resolve_class` is keyed
/// on the bare name, so a Python `class Child(Base)` would otherwise bind a Rust
/// `struct Base` whenever no Python `Base` was in the batch.
fn append_family_structural_edges(
    class_by_name: &HashMap<String, Vec<ClassDef>>,
    state: &mut StructuralResolutionState<'_>,
) {
    let mut class_names: Vec<&String> = class_by_name.keys().collect();
    class_names.sort_unstable();
    for name in class_names {
        for definition in &class_by_name[name] {
            append_named_structural_edges(
                definition,
                &definition.bases,
                "inherits",
                class_by_name,
                state,
            );
            append_named_structural_edges(
                definition,
                &definition.interfaces,
                "realizes",
                class_by_name,
                state,
            );
        }
    }
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

type SimilaritySignature<'a> = (&'a str, [u32; super::tree_sitter::MINHASH_K]);

/// LSH-band the per-symbol MinHash signatures into `similar_to` edges. Symbols
/// whose signatures collide in any band become candidate pairs; a pair is linked
/// when its estimated Jaccard ≥ [`SIMILAR_THRESHOLD`]. Edges are symmetric
/// (emitted once with sorted endpoints), scored, and capped per node so a big
/// clone family doesn't explode the edge set. Returns the edge count.
fn similarity_edges(nodes: &[ExtractedNode], edges: &mut Vec<ExtractedEdge>) -> usize {
    use super::tree_sitter::decode_minhash;

    let sigs: Vec<SimilaritySignature<'_>> = nodes
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
    let scored = similarity_scored_pairs(&sigs);

    let (mut per_node, mut count) = (HashMap::new(), 0);
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

fn similarity_scored_pairs(sigs: &[SimilaritySignature<'_>]) -> Vec<(usize, usize, f64)> {
    use super::tree_sitter::{minhash_jaccard, MINHASH_K};

    // LSH banding: BANDS bands of ROWS rows (BANDS*ROWS == MINHASH_K).
    const BANDS: usize = 8;
    const ROWS: usize = MINHASH_K / BANDS;
    let mut buckets: HashMap<(usize, u64), Vec<usize>> = HashMap::new();
    for (idx, (_, sig)) in sigs.iter().enumerate() {
        for (band, rows) in sig.chunks_exact(ROWS).enumerate().take(BANDS) {
            let h = rows.iter().fold(0xcbf2_9ce4_8422_2325, |h, row| {
                (h ^ *row as u64).wrapping_mul(0x0000_0100_0000_01b3)
            });
            buckets.entry((band, h)).or_default().push(idx);
        }
    }

    // Candidate pairs (deduped) from co-bucketed symbols.
    let mut candidates: HashSet<(usize, usize)> = HashSet::new();
    for members in buckets.values().filter(|m| (2..=256).contains(&m.len())) {
        for (i, &a) in members.iter().enumerate() {
            candidates.extend(members[i + 1..].iter().map(|&b| (a.min(b), a.max(b))));
        }
    }
    let mut scored: Vec<(usize, usize, f64)> = candidates
        .into_iter()
        .filter_map(|(a, b)| {
            let s = minhash_jaccard(&sigs[a].1, &sigs[b].1);
            (s >= SIMILAR_THRESHOLD).then_some((a, b, s))
        })
        .collect();
    // Total tie order keeps the greedy per-node cap deterministic across HashSet iteration.
    scored.sort_by(|x, y| {
        y.2.partial_cmp(&x.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| x.0.cmp(&y.0))
            .then_with(|| x.1.cmp(&y.1))
    });
    scored
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
    // 0. Confine every candidate below to the CALLER's language family. A
    //    same-named definition in another family is not a weaker match, it is a
    //    wrong one -- Python cannot call a Rust `fn` by name. Both maps are keyed
    //    by the same family set, so the second lookup cannot miss when the first
    //    hits; a family with no indexed symbol resolves nothing, which is the
    //    honest answer rather than a foreign guess.
    let family = call_family(caller_file);
    let index = context.families.get(family)?;
    let bases_of = context.bases_of.get(family)?;

    if let Some(resolved) = resolve_scoped_site(site, caller_scope, caller_lang, index, bases_of) {
        return Some(resolved);
    }
    let defs = index.def_index.get(site.callee.as_str())?;
    if let Some(d) = defs.iter().find(|d| d.file_path == caller_file) {
        return Some((d.id.clone(), "same_file", 0.9));
    }
    if let Some(argc) = site.argc {
        let matches: Vec<&Def> = defs.iter().filter(|d| d.arity == Some(argc)).collect();
        if matches.len() == 1 {
            return Some((matches[0].id.clone(), "arity", 0.7));
        }
    }
    (defs.len() == 1).then(|| (defs[0].id.clone(), "unique", 0.6))
}

fn resolve_scoped_site(
    site: &DecodedSite,
    caller_scope: &str,
    caller_lang: &str,
    index: &LangIndex,
    bases_of: &HashMap<String, Vec<String>>,
) -> Option<(String, &'static str, f64)> {
    let (callee, recv) = (site.callee.as_str(), site.receiver.as_str());

    // 1. `self`/`this`/`super` receiver (or an implicit-this language's bare call)
    //    → a method of the caller's own class or an inherited one.
    let self_recv = matches!(recv, "self" | "this" | "super")
        || (recv.is_empty() && matches!(caller_lang, "java" | "cpp" | "csharp"));
    if let Some(id) = (self_recv && !caller_scope.is_empty())
        .then(|| lookup_method(caller_scope, callee, &index.scoped, bases_of))
        .flatten()
    {
        return Some((id, "scoped", 0.95));
    }
    // 2. Explicit receiver naming a known class (static call / typed receiver).
    (!recv.is_empty() && index.class_by_name.contains_key(recv))
        .then(|| lookup_method(recv, callee, &index.scoped, bases_of))
        .flatten()
        .map(|id| (id, "scoped", 0.9))
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
    fn duplicate_content_resolves_per_occurrence_not_per_content() {
        // CONCEPT:EG-KG.compute.symbol-occurrence-id — two files whose helper is
        // byte-identical. Each caller must bind ITS OWN file's helper, and the two
        // helpers must be two nodes.
        let r = index_repository(&files(&[
            (
                "a.py",
                "def helper():\n    return 1\n\ndef go():\n    return helper()\n",
            ),
            (
                "b.py",
                "def helper():\n    return 1\n\ndef run():\n    return helper()\n",
            ),
        ]));
        let a_helper = node_id(&r, "helper", "a.py");
        let b_helper = node_id(&r, "helper", "b.py");
        assert_ne!(a_helper, b_helper, "one id per occurrence");
        let go = node_id(&r, "go", "a.py");
        let run = node_id(&r, "run", "b.py");
        assert!(
            r.edges
                .iter()
                .any(|e| e.edge_type == "calls" && e.source == go && e.target == a_helper),
            "go must bind a.py's helper; edges={:?}",
            r.edges
        );
        assert!(
            r.edges
                .iter()
                .any(|e| e.edge_type == "calls" && e.source == run && e.target == b_helper),
            "run must bind b.py's helper; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn index_result_nodes_carry_no_duplicate_ids() {
        // The documented shape of `IndexResult.nodes`: one row per declaration
        // site, and no two rows share an id.
        let r = index_repository(&files(&[
            ("a.py", "class C:\n    def m(self):\n        pass\n"),
            ("b.py", "class C:\n    def m(self):\n        pass\n"),
        ]));
        let mut seen = HashSet::new();
        for n in &r.nodes {
            assert!(
                seen.insert(n.node_id.as_str()),
                "duplicate id {}",
                n.node_id
            );
        }
        assert_eq!(
            r.nodes.len(),
            4,
            "2 classes + 2 methods: {:?}",
            r.nodes.len()
        );
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

    /// Helper: the `depends_on` target for one (importer, module) pair.
    fn dep_target(r: &IndexResult, src: &str, module: &str) -> Option<String> {
        r.edges
            .iter()
            .find(|e| {
                e.edge_type == "depends_on"
                    && e.source == format!("file:{src}")
                    && e.properties.get("module").map(String::as_str) == Some(module)
            })
            .map(|e| e.target.clone())
    }

    /// Regression: one flat extension list was tried for EVERY stem, so a Python
    /// `import types` (stdlib — no `types.py` anywhere in the batch) fell through
    /// the Python candidates and bound to a Rust `types.rs`, asserting that a
    /// Python module depends on a Rust file. A stdlib/external import has no
    /// intra-batch target; NO edge is the correct answer.
    #[test]
    fn python_import_never_binds_to_a_rust_file() {
        let r = index_repository(&files(&[
            (
                "crates/eg-compute/src/graph_algos/similarity/types.rs",
                "pub struct Pair;\n",
            ),
            ("crates/eg-compute/src/mining/math.rs", "pub fn add() {}\n"),
            ("crates/eg-compute/src/ast/mod.rs", "pub fn walk() {}\n"),
            // Positive control: a real intra-batch Python target, so the test
            // fails if the resolver simply stopped resolving anything.
            ("epistemic_graph/kvcache/store.py", "def put():\n    return 1\n"),
            (
                "epistemic_graph/kvcache/connector.py",
                "import types\nimport math\nimport ast\nfrom epistemic_graph.kvcache.store import put\n\ndef go():\n    return put()\n",
            ),
        ]));
        let crossed: Vec<&ExtractedEdge> = r
            .edges
            .iter()
            .filter(|e| {
                e.edge_type == "depends_on"
                    && e.source.ends_with(".py")
                    && e.target.ends_with(".rs")
            })
            .collect();
        assert!(
            crossed.is_empty(),
            "a Python import must never resolve to a Rust file; got {crossed:?}"
        );
        for module in ["types", "math", "ast"] {
            assert_eq!(
                dep_target(&r, "epistemic_graph/kvcache/connector.py", module),
                None,
                "`import {module}` has no intra-batch Python target -- expected no edge"
            );
        }
        assert_eq!(
            dep_target(
                &r,
                "epistemic_graph/kvcache/connector.py",
                "epistemic_graph.kvcache.store"
            )
            .as_deref(),
            Some("file:epistemic_graph/kvcache/store.py"),
            "the positive control must still resolve; edges={:?}",
            r.edges
        );
    }

    /// The boundary is symmetric: a Rust `use` must not bind to a Python file
    /// that happens to share the stem.
    #[test]
    fn rust_use_never_binds_to_a_python_file() {
        let r = index_repository(&files(&[
            ("pkg/serde.py", "def loads():\n    return 1\n"),
            (
                "crates/a/src/lib.rs",
                "use serde::Deserialize;\npub fn a() {}\n",
            ),
        ]));
        assert_eq!(
            dep_target(&r, "crates/a/src/lib.rs", "serde::Deserialize"),
            None,
            "an external crate must not bind to a same-named .py; edges={:?}",
            r.edges
        );
    }

    /// TS and JS are ONE family, not two: a `.ts` importing `./util` legitimately
    /// resolves `util.js` when that is the only file with the stem. Constraining
    /// by language must not sever that.
    #[test]
    fn ts_import_resolves_a_js_file_in_the_same_family() {
        let r = index_repository(&files(&[
            ("src/util.js", "export function shared() { return 1; }\n"),
            (
                "src/app.ts",
                "import { shared } from './util';\nexport function run(): number { return shared(); }\n",
            ),
        ]));
        assert_eq!(
            dep_target(&r, "src/app.ts", "'./util'").as_deref(),
            Some("file:src/util.js"),
            "TS/JS interoperate -- a .ts must still resolve a .js; edges={:?}",
            r.edges
        );
    }

    /// A Python package directory still resolves through its `__init__.py`, and
    /// a Rust module directory through its `mod.rs` -- each within its own family
    /// and never the other's.
    #[test]
    fn package_index_files_follow_the_family() {
        let r = index_repository(&files(&[
            ("pkg/shared/__init__.py", "def go():\n    return 1\n"),
            ("crates/a/src/shared/mod.rs", "pub fn go() {}\n"),
            (
                "pkg/app.py",
                "from pkg.shared import go\n\ndef run():\n    return go()\n",
            ),
            (
                "crates/a/src/lib.rs",
                "use crate::shared;\npub fn run() {}\n",
            ),
        ]));
        assert_eq!(
            dep_target(&r, "pkg/app.py", "pkg.shared").as_deref(),
            Some("file:pkg/shared/__init__.py"),
            "a Python package must resolve via __init__.py; edges={:?}",
            r.edges
        );
        assert_eq!(
            dep_target(&r, "crates/a/src/lib.rs", "crate::shared").as_deref(),
            Some("file:crates/a/src/shared/mod.rs"),
            "a Rust module dir must resolve via mod.rs; edges={:?}",
            r.edges
        );
    }

    /// True when a resolved `calls` edge runs from `source` to `target`.
    fn has_call(r: &IndexResult, source: &str, target: &str) -> bool {
        r.edges
            .iter()
            .any(|e| e.edge_type == "calls" && e.source == source && e.target == target)
    }

    /// True when ANY resolved edge of `edge_type` points at `target`.
    fn any_edge_into(r: &IndexResult, edge_type: &str, target: &str) -> bool {
        r.edges
            .iter()
            .any(|e| e.edge_type == edge_type && e.target == target)
    }

    // ── Language-family boundary for call/class resolution ────────────────
    // Every one of these carries its POSITIVE control in the SAME batch, so a
    // resolver that went dead (resolving nothing at all) fails them too.

    #[test]
    fn python_call_never_binds_a_rust_function_of_the_same_name() {
        // `only_rust` exists ONLY as a Rust `fn`; before the family constraint
        // the `unique` strategy bound the Python call site straight to it.
        let r = index_repository(&files(&[
            (
                "crates/eg-numeric/src/reductions.rs",
                "pub fn only_rust(a: i32) -> i32 { a }\n",
            ),
            ("pkg/helper.py", "def only_python(a):\n    return a\n"),
            (
                "pkg/caller.py",
                "def go():\n    return only_rust(1) + only_python(2)\n",
            ),
        ]));
        let go = node_id(&r, "go", "pkg/caller.py");
        // Positive control, same batch: the same-language call still resolves.
        assert!(
            has_call(&r, &go, &node_id(&r, "only_python", "pkg/helper.py")),
            "same-language only_python() must still resolve; edges={:?}",
            r.edges
        );
        let rust_fn = node_id(&r, "only_rust", "crates/eg-numeric/src/reductions.rs");
        assert!(
            !any_edge_into(&r, "calls", &rust_fn),
            "a Python call must not bind a Rust fn; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn rust_call_never_binds_a_python_function_of_the_same_name() {
        let r = index_repository(&files(&[
            ("pkg/only_py.py", "def only_python(a):\n    return a\n"),
            (
                "crates/a/src/util.rs",
                "pub fn only_rust(a: i32) -> i32 { a }\n",
            ),
            (
                "crates/a/src/main.rs",
                "fn run() { only_python(1); only_rust(2); }\n",
            ),
        ]));
        let run = node_id(&r, "run", "crates/a/src/main.rs");
        // Positive control: the same-language call still resolves.
        assert!(
            has_call(&r, &run, &node_id(&r, "only_rust", "crates/a/src/util.rs")),
            "same-language only_rust() must still resolve; edges={:?}",
            r.edges
        );
        let py_fn = node_id(&r, "only_python", "pkg/only_py.py");
        assert!(
            !any_edge_into(&r, "calls", &py_fn),
            "a Rust call must not bind a Python def; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn arity_disambiguation_stays_inside_the_language_family() {
        // The 2-arg call matches the RUST definition's arity exactly and no
        // Python one's -- `arity` (1,639 of the 2,436 cross-language edges
        // measured on the epistemic-graph corpus) is precisely that shape.
        // Constrained to Python, arity finds nothing and the unique Python
        // definition wins instead.
        let r = index_repository(&files(&[
            (
                "crates/a/src/lib.rs",
                "pub fn make(a: i32, b: i32) -> i32 { a + b }\n",
            ),
            ("pkg/defs.py", "def make(x):\n    return x\n"),
            ("pkg/call.py", "def go():\n    return make(1, 2)\n"),
        ]));
        let go = node_id(&r, "go", "pkg/call.py");
        assert!(
            !any_edge_into(&r, "calls", &node_id(&r, "make", "crates/a/src/lib.rs")),
            "the 2-arg Rust fn must not absorb a Python call; edges={:?}",
            r.edges
        );
        assert!(
            has_call(&r, &go, &node_id(&r, "make", "pkg/defs.py")),
            "the call must fall back to the unique PYTHON make; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn scoped_method_lookup_stays_inside_the_language_family() {
        // `scoped` is keyed (class name, method name) with no language, so a
        // Python `self.shared()` could bind the method of a same-named JAVA
        // class -- it measured 0 on both corpora only because no such name
        // collision happened to exist, not because it was safe.
        let r = index_repository(&files(&[
            ("A.java", "class Holder { void shared() {} }\n"),
            (
                "m.py",
                "class Holder:\n    def shared(self):\n        return 1\n\n    def run(self):\n        return self.shared()\n",
            ),
        ]));
        let run = node_id(&r, "run", "m.py");
        assert!(
            has_call(&r, &run, &node_id(&r, "shared", "m.py")),
            "self.shared() must bind the PYTHON method; edges={:?}",
            r.edges
        );
        assert!(
            !any_edge_into(&r, "calls", &node_id(&r, "shared", "A.java")),
            "a Python call must not bind a Java method; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn class_resolution_never_crosses_the_language_family() {
        // `resolve_class` feeds `inherits`/`realizes` and is name-keyed the same
        // way. `OnlyRust` exists only as a Rust struct, so a Python subclass of
        // that name must produce NO edge.
        let r = index_repository(&files(&[
            (
                "crates/a/src/lib.rs",
                "pub struct OnlyRust { pub x: i32 }\n",
            ),
            ("pkg/base.py", "class PyBase:\n    pass\n"),
            (
                "pkg/child.py",
                "class Child(OnlyRust):\n    pass\n\nclass Sibling(PyBase):\n    pass\n",
            ),
        ]));
        // Positive control, same batch: the same-language inherits still lands.
        assert!(
            r.edges.iter().any(|e| e.edge_type == "inherits"
                && e.source == node_id(&r, "Sibling", "pkg/child.py")
                && e.target == node_id(&r, "PyBase", "pkg/base.py")),
            "Sibling→PyBase inherits must survive; edges={:?}",
            r.edges
        );
        assert!(
            !any_edge_into(
                &r,
                "inherits",
                &node_id(&r, "OnlyRust", "crates/a/src/lib.rs")
            ),
            "a Python class must not inherit a Rust struct; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn ts_call_resolves_a_js_definition_in_the_same_family() {
        // TS and JS are ONE family for calls, exactly as for imports: a `.ts`
        // file genuinely calls a function defined in a `.js` file.
        let r = index_repository(&files(&[
            ("src/util.js", "export function helper(a) { return a; }\n"),
            (
                "src/main.ts",
                "export function go() { return helper(1); }\n",
            ),
        ]));
        assert!(
            has_call(
                &r,
                &node_id(&r, "go", "src/main.ts"),
                &node_id(&r, "helper", "src/util.js")
            ),
            "a TS call must still resolve a JS definition; edges={:?}",
            r.edges
        );
    }

    #[test]
    fn call_family_agrees_with_the_import_family_names() {
        // One family model, two entry points: whatever `family_of` claims for a
        // path, `call_family` must agree, and every grammar the parser has must
        // land in some family so its own calls can resolve.
        for path in ["a.py", "a.pyi", "a.ts", "a.js", "a.rs", "a.go", "A.java"] {
            assert_eq!(call_family(path), family_of(path).unwrap().name, "{path}");
        }
        assert_eq!(call_family("a.tsx"), call_family("a.mjs"));
        assert_eq!(call_family("a.c"), call_family("a.cpp"));
        assert_ne!(call_family("a.py"), call_family("a.rs"));
        assert_eq!(call_family("Makefile"), "");
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
