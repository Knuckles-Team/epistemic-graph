//! W2-parity — SPARQL 1.1 UPDATE executed over the native property-graph write ops
//! (CONCEPT:EG-017).
//!
//! This is the REAL update path that replaces the naive `INSERT DATA` string-split shim
//! (`graph_ops.rs`). It wires `spargebra::Update` / `GraphUpdateOperation` to the
//! engine's `add_node` / `add_edge` / `remove_*` primitives via the SAME RDF ⇄
//! property-graph mapping the loader uses ([`crate::mapping`]):
//!
//!   * a triple with a **resource object** → a typed edge `s --p--> o` (`{type:p}`);
//!   * a triple with a **literal object** → a typed property cell on `s`;
//!   * `rdf:type` → folded into the node `type` label AND kept as an explicit edge.
//!
//! Crucially the inserts are **incremental + merge-aware**: a single literal insert
//! merges into the subject's existing property blob instead of overwriting it (the
//! loader writes whole-node blobs, which is wrong for one-triple updates). Deletes are
//! surgical: a literal delete drops one property key; a resource delete removes the one
//! matching typed edge and preserves any others between the same pair.
//!
//! Named-graph routing goes through the [`GraphStore`] trait so the executor stays
//! decoupled from the engine registry: the server wires a registry-backed store, while
//! tests use the in-memory [`MapStore`]. `DELETE/INSERT … WHERE` evaluates its WHERE over
//! a [`Dataset`] of the store's graph snapshots (named-graph aware), then instantiates
//! the delete/insert quad patterns per solution.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use eg_core::graph::GraphCore;
use oxrdf::{Graph, Literal, NamedOrBlankNode, Term, Triple};

use crate::guard::{GuardRejection, WriteGuard};
use spargebra::algebra::GraphTarget;
use spargebra::term::{
    GraphName, GraphNamePattern, GroundQuad, GroundQuadPattern, GroundTerm, GroundTermPattern,
    NamedNodePattern, Quad, QuadPattern, TermPattern,
};
use spargebra::{GraphUpdateOperation, Update};

use crate::mapping::literal_to_cell;
use crate::sparql::{Binding, Dataset, Projection, Solution};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// How an UPDATE resolves a SPARQL graph name to a writable [`GraphCore`], so the
/// executor is decoupled from the engine's graph registry. The default graph is `None`;
/// a named graph is `Some(bare-iri)`.
pub trait GraphStore {
    /// The core backing a graph, creating it on demand (writes auto-vivify a graph,
    /// matching the engine's lazy-create behavior). `None` ⇒ the store could not
    /// provide/create it (the op is then skipped or errors per `silent`).
    fn core(&self, graph: Option<&str>) -> Option<Arc<GraphCore>>;

    /// Every named graph as `(bare-iri, core)` — the candidate set a `GRAPH ?g` WHERE
    /// clause ranges over. The default impl returns nothing (single-graph stores).
    fn named(&self) -> Vec<(String, Arc<GraphCore>)> {
        Vec::new()
    }

    /// CLEAR a graph (remove all its triples) — idempotent. Default = clear the core.
    fn clear(&self, graph: Option<&str>) -> Result<(), String> {
        if let Some(c) = self.core(graph) {
            c.clear();
        }
        Ok(())
    }

    /// DROP a graph entirely. Default delegates to [`GraphStore::clear`] (the store may
    /// override to also evict the registry entry).
    fn drop_graph(&self, graph: Option<&str>) -> Result<(), String> {
        self.clear(graph)
    }

    /// CREATE GRAPH — ensure it exists. Default = touch the core.
    fn create(&self, iri: &str) -> Result<(), String> {
        match self.core(Some(iri)) {
            Some(_) => Ok(()),
            None => Err(format!("CREATE GRAPH <{iri}>: store could not create it")),
        }
    }
}

/// What a [`execute`] run changed.
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UpdateReport {
    /// Update operations applied (one per `;`-separated clause).
    pub operations: usize,
    /// Triples inserted.
    pub inserted: usize,
    /// Triples deleted.
    pub deleted: usize,
}

/// Parse a SPARQL 1.1 UPDATE string into the spargebra model.
pub fn parse_update(update_str: &str) -> Result<Update, String> {
    Update::parse(update_str, None).map_err(|e| format!("sparql update parse: {e}"))
}

/// The constant named-graph IRIs an UPDATE references (so a caller — e.g. the `/sparql`
/// endpoint — can pre-create them in its registry before executing). Covers ground data,
/// CREATE/CLEAR/DROP targets, the LOAD destination, and constant insert/delete pattern
/// graph names — the practical "write to a new named graph" cases.
pub fn referenced_named_graphs(update: &Update) -> Vec<String> {
    use spargebra::term::{GraphName, GraphNamePattern};
    let mut out = std::collections::HashSet::new();
    let push_name = |g: &GraphName, out: &mut std::collections::HashSet<String>| {
        if let GraphName::NamedNode(n) = g {
            out.insert(n.as_str().to_string());
        }
    };
    let push_target = |t: &GraphTarget, out: &mut std::collections::HashSet<String>| {
        if let GraphTarget::NamedNode(n) = t {
            out.insert(n.as_str().to_string());
        }
    };
    for op in &update.operations {
        match op {
            GraphUpdateOperation::InsertData { data } => {
                for q in data {
                    push_name(&q.graph_name, &mut out);
                }
            }
            GraphUpdateOperation::DeleteData { data } => {
                for q in data {
                    push_name(&q.graph_name, &mut out);
                }
            }
            GraphUpdateOperation::DeleteInsert { insert, delete, .. } => {
                for qp in insert {
                    if let GraphNamePattern::NamedNode(n) = &qp.graph_name {
                        out.insert(n.as_str().to_string());
                    }
                }
                for gqp in delete {
                    if let GraphNamePattern::NamedNode(n) = &gqp.graph_name {
                        out.insert(n.as_str().to_string());
                    }
                }
            }
            GraphUpdateOperation::Create { graph, .. } => {
                out.insert(graph.as_str().to_string());
            }
            GraphUpdateOperation::Clear { graph, .. }
            | GraphUpdateOperation::Drop { graph, .. } => {
                push_target(graph, &mut out);
            }
            GraphUpdateOperation::Load { destination, .. } => {
                push_name(destination, &mut out);
            }
        }
    }
    out.into_iter().collect()
}

/// Parse + execute a SPARQL 1.1 UPDATE string against `store`. `LOAD` from a remote URL
/// is the one deferred op (it needs an HTTP fetch the engine intentionally does not do
/// from the write path) — it is reported via the returned error unless `silent`.
pub fn execute_str(
    update_str: &str,
    store: &dyn GraphStore,
    proj: &Projection,
) -> Result<UpdateReport, String> {
    let update = parse_update(update_str)?;
    execute(&update, store, proj)
}

/// Execute a parsed SPARQL UPDATE against `store`.
pub fn execute(
    update: &Update,
    store: &dyn GraphStore,
    proj: &Projection,
) -> Result<UpdateReport, String> {
    let mut report = UpdateReport::default();
    for op in &update.operations {
        report.operations += 1;
        match op {
            GraphUpdateOperation::InsertData { data } => {
                for quad in data {
                    apply_quad(store, quad, &mut report, true)?;
                }
            }
            GraphUpdateOperation::DeleteData { data } => {
                for quad in data {
                    apply_ground_quad(store, quad, &mut report, false)?;
                }
            }
            GraphUpdateOperation::DeleteInsert {
                delete,
                insert,
                using: _,
                pattern,
            } => {
                exec_delete_insert(store, proj, delete, insert, pattern, &mut report)?;
            }
            GraphUpdateOperation::Clear { silent, graph } => {
                clear_target(store, graph, *silent)?;
            }
            GraphUpdateOperation::Create { silent, graph } => {
                let r = store.create(graph.as_str());
                if r.is_err() && !*silent {
                    return r.map(|_| report.clone());
                }
            }
            GraphUpdateOperation::Drop { silent, graph } => {
                drop_target(store, graph, *silent)?;
            }
            GraphUpdateOperation::Load {
                silent,
                source,
                destination: _,
            } => {
                if !*silent {
                    return Err(format!(
                        "LOAD <{}>: remote-URL load is not supported from the engine write \
                         path (use AddTriples / source_sync); add SILENT to ignore",
                        source.as_str()
                    ));
                }
            }
        }
    }
    Ok(report)
}

// ── EG-300 constraint-enforced commit (WriteGuard hook) ─────────────────────────

/// The failure of a guarded commit (CONCEPT:EG-300): either the underlying
/// parse/execute failed, or a [`WriteGuard`] refused the change set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateError {
    /// The update failed to parse or execute (the same message the plain path returns).
    Exec(String),
    /// A constraint guard rejected the commit — NO change was applied to the store.
    Rejected(GuardRejection),
}

impl std::fmt::Display for UpdateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateError::Exec(e) => write!(f, "update failed: {e}"),
            UpdateError::Rejected(r) => write!(f, "update rejected by constraint guard: {r}"),
        }
    }
}

impl std::error::Error for UpdateError {}

/// Parse + execute a SPARQL UPDATE string under a [`WriteGuard`] (CONCEPT:EG-300).
pub fn execute_guarded_str(
    update_str: &str,
    store: &dyn GraphStore,
    proj: &Projection,
    guard: &dyn WriteGuard,
) -> Result<UpdateReport, UpdateError> {
    let update = parse_update(update_str).map_err(UpdateError::Exec)?;
    execute_guarded(&update, store, proj, guard)
}

/// Execute a parsed SPARQL UPDATE as a **constraint-enforced transaction** (CONCEPT:EG-300).
///
/// The whole change set is the transaction boundary. When the guard is [`WriteGuard::active`]:
///
///  1. snapshot the store's graphs (`base`);
///  2. replay the update on a throwaway *shadow* store seeded from `base` (so the REAL
///     store is never touched speculatively) → the projected `after`;
///  3. per graph, diff `after` vs `base` into net additions/removals and call
///     [`WriteGuard::check_graph`];
///  4. if any graph is refused → return [`UpdateError::Rejected`] and the real store is
///     left UNCHANGED (nothing was committed);
///  5. otherwise replay the update on the real store and return its [`UpdateReport`].
///
/// When the guard is inactive this is byte-for-byte [`execute`] (zero simulate/diff cost),
/// so an `Off` policy is fully backward-compatible. Deterministic: the shadow replay and
/// the real replay start from the identical `base`, so `DELETE/INSERT … WHERE` binds the
/// same solutions in both.
pub fn execute_guarded(
    update: &Update,
    store: &dyn GraphStore,
    proj: &Projection,
    guard: &dyn WriteGuard,
) -> Result<UpdateReport, UpdateError> {
    if !guard.active() {
        return execute(update, store, proj).map_err(UpdateError::Exec);
    }

    // (1) snapshot base — the default graph plus every named graph currently in the store.
    let base = snapshot_store(store).map_err(UpdateError::Exec)?;

    // (2) seed a shadow store from base and replay the update there (real store untouched).
    let shadow = MapStore::new();
    for (g, triples) in &base {
        if let Some(core) = shadow.core(g.as_deref()) {
            insert_triples(&core, triples).map_err(UpdateError::Exec)?;
        }
    }
    execute(update, &shadow, proj).map_err(UpdateError::Exec)?;
    let after = snapshot_store(&shadow).map_err(UpdateError::Exec)?;

    // (3) per graph, diff and ask the guard (over the union of base + after graph names).
    let mut names: HashSet<Option<String>> = base.keys().cloned().collect();
    names.extend(after.keys().cloned());
    let empty: Vec<Triple> = Vec::new();
    for name in names {
        let b = base.get(&name).unwrap_or(&empty);
        let a = after.get(&name).unwrap_or(&empty);
        let (additions, removals) = triple_diff(b, a);
        if additions.is_empty() && removals.is_empty() {
            continue; // this graph is unchanged — nothing to validate.
        }
        let base_graph = triples_to_graph(b);
        guard
            .check_graph(name.as_deref(), &base_graph, &additions, &removals)
            .map_err(UpdateError::Rejected)?;
    }

    // (4/5) accepted — commit to the real store (same base ⇒ same result).
    execute(update, store, proj).map_err(UpdateError::Exec)
}

/// Snapshot every graph of a store to RDF triples: the default graph (key `None`) plus
/// each named graph. Reuses the same lossless export the whole-graph copy path uses.
fn snapshot_store(
    store: &dyn GraphStore,
) -> Result<HashMap<Option<String>, Vec<Triple>>, String> {
    let mut out: HashMap<Option<String>, Vec<Triple>> = HashMap::new();
    if let Some(core) = store.core(None) {
        out.insert(None, export_graph_triples(&core, "")?);
    }
    for (name, core) in store.named() {
        let triples = export_graph_triples(&core, &name)?;
        out.insert(Some(name), triples);
    }
    Ok(out)
}

/// Net additions (`after \ base`) and removals (`base \ after`) between two triple sets.
fn triple_diff(base: &[Triple], after: &[Triple]) -> (Vec<Triple>, Vec<Triple>) {
    let base_set: HashSet<&Triple> = base.iter().collect();
    let after_set: HashSet<&Triple> = after.iter().collect();
    let additions = after
        .iter()
        .filter(|t| !base_set.contains(*t))
        .cloned()
        .collect();
    let removals = base
        .iter()
        .filter(|t| !after_set.contains(*t))
        .cloned()
        .collect();
    (additions, removals)
}

/// Build an oxrdf [`Graph`] from a slice of triples (the guard's `base` argument).
fn triples_to_graph(triples: &[Triple]) -> Graph {
    let mut g = Graph::new();
    for t in triples {
        g.insert(t);
    }
    g
}

// ── DELETE/INSERT … WHERE ───────────────────────────────────────────────────────

fn exec_delete_insert(
    store: &dyn GraphStore,
    proj: &Projection,
    delete: &[GroundQuadPattern],
    insert: &[QuadPattern],
    pattern: &spargebra::algebra::GraphPattern,
    report: &mut UpdateReport,
) -> Result<(), String> {
    // CONCEPT:EG-134 — ADD / COPY / MOVE. spargebra performs the W3C rewriting at parse
    // time: each desugars to a whole-graph `?s ?p ?o` copy (this `DeleteInsert`) plus a
    // preceding DROP of the destination (COPY/MOVE) and a trailing DROP of the source
    // (MOVE), which the `Clear`/`Drop` arms already execute via `GraphStore::clear`. So
    // COPY/MOVE/ADD need no dedicated `execute` arm — we only recognize the canonical
    // whole-graph-copy shape here and run it as a LOSSLESS graph→graph triple copy
    // (reusing `export_triples` + `insert_triples`) instead of the lossy binding
    // round-trip the generic WHERE path would use (which flattens literal datatypes).
    if let Some((from, to)) = whole_graph_copy(delete, insert, pattern) {
        let (Some(src), Some(dst)) = (store.core(from.as_deref()), store.core(to.as_deref()))
        else {
            // Missing source/destination ⇒ nothing to copy (SILENT-friendly: the DROPs
            // that frame COPY/MOVE are `silent` and no error is raised here either).
            return Ok(());
        };
        let triples = export_graph_triples(&src, from.as_deref().unwrap_or(""))?;
        report.inserted += insert_triples(&dst, &triples)?;
        return Ok(());
    }

    // Snapshot the store's graphs and evaluate the WHERE over them (named-graph aware).
    let default_core = store
        .core(None)
        .ok_or("DELETE/INSERT WHERE: no default graph")?;
    let default_view = default_core.analysis_snapshot();
    let named_cores = store.named();
    let named_views: Vec<(String, eg_core::graph::GraphView)> = named_cores
        .iter()
        .map(|(n, c)| (n.clone(), c.analysis_snapshot()))
        .collect();
    let named_refs: Vec<(String, &eg_core::graph::GraphView)> =
        named_views.iter().map(|(n, v)| (n.clone(), v)).collect();
    let ds = Dataset::new(&default_view, named_refs);

    let solutions = crate::sparql::eval_where(&ds, pattern, proj)?;

    // DELETE first (SPARQL: the delete sees the pre-update graph), then INSERT.
    for sol in &solutions {
        for gqp in delete {
            if let Some((graph, s, p, obj)) = instantiate_ground(gqp, sol) {
                if let Some(core) = store.core(graph.as_deref()) {
                    if delete_triple(&core, &s, &p, &obj) {
                        report.deleted += 1;
                    }
                }
            }
        }
    }
    for sol in &solutions {
        for qp in insert {
            if let Some((graph, s, p, obj)) = instantiate_quad(qp, sol) {
                if let Some(core) = store.core(graph.as_deref()) {
                    if insert_triple(&core, &s, &p, &obj)? {
                        report.inserted += 1;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Recognize the canonical whole-graph copy `spargebra` emits for ADD/COPY/MOVE
/// (CONCEPT:EG-134): an empty DELETE, a single INSERT quad-pattern of three variables
/// `?s ?p ?o` into a constant destination graph, and a WHERE that is exactly the SAME
/// three variables over one source graph (a bare BGP ⇒ the default graph, or a
/// `GRAPH <src> { … }`). Returns `(from, to)` as bare-iri graph names (`None` = the
/// default graph) when the shape matches, else `None` (fall back to the generic path).
fn whole_graph_copy(
    delete: &[GroundQuadPattern],
    insert: &[QuadPattern],
    pattern: &spargebra::algebra::GraphPattern,
) -> Option<(Option<String>, Option<String>)> {
    use spargebra::algebra::GraphPattern as GP;
    if !delete.is_empty() || insert.len() != 1 {
        return None;
    }
    let qp = &insert[0];
    let (TermPattern::Variable(vs), TermPattern::Variable(vo)) = (&qp.subject, &qp.object) else {
        return None;
    };
    let NamedNodePattern::Variable(vp) = &qp.predicate else {
        return None;
    };
    let to = match &qp.graph_name {
        GraphNamePattern::DefaultGraph => None,
        GraphNamePattern::NamedNode(n) => Some(n.as_str().to_string()),
        GraphNamePattern::Variable(_) => return None,
    };
    // WHERE = one bare BGP (source = default) or `GRAPH <src> { BGP }`.
    let (from, bgp) = match pattern {
        GP::Bgp { patterns } => (None, patterns),
        GP::Graph { name, inner } => {
            let NamedNodePattern::NamedNode(src) = name else {
                return None;
            };
            match inner.as_ref() {
                GP::Bgp { patterns } => (Some(src.as_str().to_string()), patterns),
                _ => return None,
            }
        }
        _ => return None,
    };
    let [tp] = bgp.as_slice() else { return None };
    // The WHERE triple must be the SAME three variables the INSERT reuses.
    let same = matches!(&tp.subject, TermPattern::Variable(s) if s == vs)
        && matches!(&tp.predicate, NamedNodePattern::Variable(p) if p == vp)
        && matches!(&tp.object, TermPattern::Variable(o) if o == vo);
    if !same {
        return None;
    }
    Some((from, to))
}

/// Export a graph core back to RDF triples for a whole-graph copy — a thin cfg wrapper
/// over [`crate::mapping::export_triples`] (the multi-valued-literal quad store, present
/// only under `rdf-redb`, is not wired into a copy: extras are a documented edge case).
fn export_graph_triples(core: &GraphCore, graph_name: &str) -> Result<Vec<oxrdf::Triple>, String> {
    #[cfg(feature = "rdf-redb")]
    {
        crate::mapping::export_triples(core, graph_name, None)
    }
    #[cfg(not(feature = "rdf-redb"))]
    {
        crate::mapping::export_triples(core, graph_name)
    }
}

// ── ground-data ops (INSERT DATA / DELETE DATA) ─────────────────────────────────

fn apply_quad(
    store: &dyn GraphStore,
    quad: &Quad,
    report: &mut UpdateReport,
    insert: bool,
) -> Result<(), String> {
    let graph = graph_opt(&quad.graph_name);
    let core = match store.core(graph.as_deref()) {
        Some(c) => c,
        None => return Ok(()),
    };
    let s = nob_id(&quad.subject);
    let p = quad.predicate.as_str().to_string();
    let Some(obj) = obj_from_term(&quad.object) else {
        return Ok(());
    };
    if insert {
        if insert_triple(&core, &s, &p, &obj)? {
            report.inserted += 1;
        }
    } else if delete_triple(&core, &s, &p, &obj) {
        report.deleted += 1;
    }
    Ok(())
}

fn apply_ground_quad(
    store: &dyn GraphStore,
    quad: &GroundQuad,
    report: &mut UpdateReport,
    insert: bool,
) -> Result<(), String> {
    let graph = graph_opt(&quad.graph_name);
    let core = match store.core(graph.as_deref()) {
        Some(c) => c,
        None => return Ok(()),
    };
    let s = format!("<{}>", quad.subject.as_str());
    let p = quad.predicate.as_str().to_string();
    let Some(obj) = obj_from_ground(&quad.object) else {
        return Ok(());
    };
    if insert {
        if insert_triple(&core, &s, &p, &obj)? {
            report.inserted += 1;
        }
    } else if delete_triple(&core, &s, &p, &obj) {
        report.deleted += 1;
    }
    Ok(())
}

fn clear_target(store: &dyn GraphStore, target: &GraphTarget, silent: bool) -> Result<(), String> {
    match target {
        GraphTarget::DefaultGraph => store.clear(None),
        GraphTarget::NamedNode(n) => store.clear(Some(n.as_str())),
        GraphTarget::NamedGraphs | GraphTarget::AllGraphs => {
            if matches!(target, GraphTarget::AllGraphs) {
                store.clear(None)?;
            }
            for (name, _) in store.named() {
                store.clear(Some(&name))?;
            }
            Ok(())
        }
    }
    .or_else(|e| if silent { Ok(()) } else { Err(e) })
}

fn drop_target(store: &dyn GraphStore, target: &GraphTarget, silent: bool) -> Result<(), String> {
    match target {
        GraphTarget::DefaultGraph => store.drop_graph(None),
        GraphTarget::NamedNode(n) => store.drop_graph(Some(n.as_str())),
        GraphTarget::NamedGraphs | GraphTarget::AllGraphs => {
            if matches!(target, GraphTarget::AllGraphs) {
                store.drop_graph(None)?;
            }
            for (name, _) in store.named() {
                store.drop_graph(Some(&name))?;
            }
            Ok(())
        }
    }
    .or_else(|e| if silent { Ok(()) } else { Err(e) })
}

// ── the typed object an update writes/removes ───────────────────────────────────

enum ObjTerm {
    Resource(String),
    Literal(Literal),
}

fn obj_from_term(t: &Term) -> Option<ObjTerm> {
    match t {
        Term::Literal(l) => Some(ObjTerm::Literal(l.clone())),
        Term::NamedNode(n) => Some(ObjTerm::Resource(format!("<{}>", n.as_str()))),
        Term::BlankNode(b) => Some(ObjTerm::Resource(format!("_:{}", b.as_str()))),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn obj_from_ground(t: &GroundTerm) -> Option<ObjTerm> {
    match t {
        GroundTerm::NamedNode(n) => Some(ObjTerm::Resource(format!("<{}>", n.as_str()))),
        GroundTerm::Literal(l) => Some(ObjTerm::Literal(l.clone())),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn nob_id(s: &NamedOrBlankNode) -> String {
    match s {
        NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
        NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
    }
}

fn graph_opt(g: &GraphName) -> Option<String> {
    match g {
        GraphName::DefaultGraph => None,
        GraphName::NamedNode(n) => Some(n.as_str().to_string()),
    }
}

// ── pattern instantiation (DELETE/INSERT WHERE) ─────────────────────────────────

/// Instantiate an INSERT `QuadPattern` with a solution → `(graph, subject_id, pred, obj)`.
fn instantiate_quad(
    qp: &QuadPattern,
    sol: &Solution,
) -> Option<(Option<String>, String, String, ObjTerm)> {
    let graph = resolve_graph_pattern(&qp.graph_name, sol)?;
    let s = subject_from_term_pattern(&qp.subject, sol)?;
    let p = pred_from_pattern(&qp.predicate, sol)?;
    let obj = object_from_term_pattern(&qp.object, sol)?;
    Some((graph, s, p, obj))
}

/// Instantiate a DELETE `GroundQuadPattern` with a solution.
fn instantiate_ground(
    gqp: &GroundQuadPattern,
    sol: &Solution,
) -> Option<(Option<String>, String, String, ObjTerm)> {
    let graph = resolve_graph_pattern(&gqp.graph_name, sol)?;
    let s = subject_from_ground_pattern(&gqp.subject, sol)?;
    let p = pred_from_pattern(&gqp.predicate, sol)?;
    let obj = object_from_ground_pattern(&gqp.object, sol)?;
    Some((graph, s, p, obj))
}

/// Outer `Option` = resolvable; inner = `None` default / `Some(iri)` named.
fn resolve_graph_pattern(g: &GraphNamePattern, sol: &Solution) -> Option<Option<String>> {
    match g {
        GraphNamePattern::DefaultGraph => Some(None),
        GraphNamePattern::NamedNode(n) => Some(Some(n.as_str().to_string())),
        GraphNamePattern::Variable(v) => sol
            .get(v.as_str())
            .map(|b| Some(strip_iri(b.as_str()).to_string())),
    }
}

fn subject_from_term_pattern(t: &TermPattern, sol: &Solution) -> Option<String> {
    match t {
        TermPattern::NamedNode(n) => Some(format!("<{}>", n.as_str())),
        TermPattern::BlankNode(b) => Some(format!("_:{}", b.as_str())),
        TermPattern::Variable(v) => binding_node_id(sol.get(v.as_str())?),
        _ => None,
    }
}

fn subject_from_ground_pattern(t: &GroundTermPattern, sol: &Solution) -> Option<String> {
    match t {
        GroundTermPattern::NamedNode(n) => Some(format!("<{}>", n.as_str())),
        GroundTermPattern::Variable(v) => binding_node_id(sol.get(v.as_str())?),
        _ => None,
    }
}

fn pred_from_pattern(p: &NamedNodePattern, sol: &Solution) -> Option<String> {
    match p {
        NamedNodePattern::NamedNode(n) => Some(n.as_str().to_string()),
        NamedNodePattern::Variable(v) => Some(strip_iri(sol.get(v.as_str())?.as_str()).to_string()),
    }
}

fn object_from_term_pattern(t: &TermPattern, sol: &Solution) -> Option<ObjTerm> {
    match t {
        TermPattern::NamedNode(n) => Some(ObjTerm::Resource(format!("<{}>", n.as_str()))),
        TermPattern::BlankNode(b) => Some(ObjTerm::Resource(format!("_:{}", b.as_str()))),
        TermPattern::Literal(l) => Some(ObjTerm::Literal(l.clone())),
        TermPattern::Variable(v) => binding_to_obj(sol.get(v.as_str())?),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

fn object_from_ground_pattern(t: &GroundTermPattern, sol: &Solution) -> Option<ObjTerm> {
    match t {
        GroundTermPattern::NamedNode(n) => Some(ObjTerm::Resource(format!("<{}>", n.as_str()))),
        GroundTermPattern::Literal(l) => Some(ObjTerm::Literal(l.clone())),
        GroundTermPattern::Variable(v) => binding_to_obj(sol.get(v.as_str())?),
        #[allow(unreachable_patterns)]
        _ => None,
    }
}

/// A bound value usable as a node id (a resource binding); `None` if it is a literal.
fn binding_node_id(b: &Binding) -> Option<String> {
    match b {
        Binding::Node(id) => Some(id.clone()),
        Binding::Literal(_) => None,
    }
}

fn binding_to_obj(b: &Binding) -> Option<ObjTerm> {
    match b {
        Binding::Node(id) => Some(ObjTerm::Resource(id.clone())),
        Binding::Literal(v) => Some(ObjTerm::Literal(Literal::new_simple_literal(v))),
    }
}

fn strip_iri(s: &str) -> &str {
    s.strip_prefix('<')
        .and_then(|x| x.strip_suffix('>'))
        .unwrap_or(s)
}

// ── reusable engine retract / insert ops (CONCEPT:EG-017) ───────────────────────
//
// These are the CLEAN, REUSABLE physical-write primitives the rest of the engine (the
// `RemoveTriples` wire method, the ontology UNLOAD path, the WAL replay) calls — the
// inverse of `mapping::load_triples`, NOT logic buried inside the UPDATE executor.

/// Physically RETRACT a set of RDF triples from a graph core (CONCEPT:EG-017). Surgical:
/// a literal triple drops one property key (matched by lexical value); a resource triple
/// removes the one matching typed edge, preserving any others between the same pair; a
/// folded `rdf:type` also clears the node `type` label. Returns the count removed. This
/// is the reusable retract op behind the `RemoveTriples` wire method + ontology unload.
pub fn remove_triples(core: &GraphCore, triples: &[oxrdf::Triple]) -> usize {
    let mut removed = 0;
    for t in triples {
        let s = subject_id_of(&t.subject);
        let p = t.predicate.as_str();
        if let Some(obj) = obj_from_term(&t.object) {
            if delete_triple(core, &s, p, &obj) {
                removed += 1;
            }
        }
    }
    removed
}

/// Physically INSERT a set of RDF triples into a graph core, merge-aware (the same
/// projection `load_triples` uses, but incremental — it never overwrites a node's other
/// properties). Returns the count of triples that added something new.
pub fn insert_triples(core: &GraphCore, triples: &[oxrdf::Triple]) -> Result<usize, String> {
    let mut inserted = 0;
    for t in triples {
        let s = subject_id_of(&t.subject);
        let p = t.predicate.as_str();
        if let Some(obj) = obj_from_term(&t.object) {
            if insert_triple(core, &s, p, &obj)? {
                inserted += 1;
            }
        }
    }
    Ok(inserted)
}

/// Canonical node id for an oxrdf subject (`<iri>` / `_:b`).
fn subject_id_of(s: &oxrdf::Subject) -> String {
    match s {
        oxrdf::Subject::NamedNode(n) => format!("<{}>", n.as_str()),
        oxrdf::Subject::BlankNode(b) => format!("_:{}", b.as_str()),
        #[allow(unreachable_patterns)]
        _ => String::new(),
    }
}

// ── the native triple-level write ops ───────────────────────────────────────────

/// Insert one triple into `core` (merge-aware). Returns `true` if it actually added
/// something new (an absent edge or a new/changed property), `false` if already present.
fn insert_triple(core: &GraphCore, s: &str, p: &str, obj: &ObjTerm) -> Result<bool, String> {
    match obj {
        ObjTerm::Literal(lit) => {
            ensure_node(core, s);
            Ok(merge_property(core, s, p, literal_to_cell(lit)))
        }
        ObjTerm::Resource(o) => {
            ensure_node(core, s);
            ensure_node(core, o);
            let mut changed = false;
            // rdf:type folds into the node label (matches the loader) AND stays an edge.
            if p == RDF_TYPE {
                if let Some(iri) = o.strip_prefix('<').and_then(|x| x.strip_suffix('>')) {
                    changed |= set_type_property(core, s, iri);
                }
            }
            changed |= add_edge_if_absent(core, s, o, p)?;
            Ok(changed)
        }
    }
}

/// Delete one triple from `core` (surgical). Returns `true` if it removed something.
fn delete_triple(core: &GraphCore, s: &str, p: &str, obj: &ObjTerm) -> bool {
    match obj {
        ObjTerm::Literal(lit) => delete_property(core, s, p, lit.value()),
        ObjTerm::Resource(o) => {
            let mut removed = remove_typed_edge(core, s, o, p);
            if p == RDF_TYPE {
                if let Some(iri) = o.strip_prefix('<').and_then(|x| x.strip_suffix('>')) {
                    removed |= clear_type_property(core, s, iri);
                }
            }
            removed
        }
    }
}

fn read_node_obj(core: &GraphCore, id: &str) -> serde_json::Map<String, serde_json::Value> {
    core.get_node_properties(id)
        .and_then(|b| rmp_serde::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| match v {
            serde_json::Value::Object(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default()
}

fn write_node_obj(core: &GraphCore, id: &str, map: serde_json::Map<String, serde_json::Value>) {
    let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(map)).unwrap_or_default();
    core.add_node(id.to_string(), blob);
}

fn ensure_node(core: &GraphCore, id: &str) {
    if !core.has_node(id) {
        write_node_obj(core, id, serde_json::Map::new());
    }
}

/// Merge a property cell into the node blob. Returns `true` if it added/changed the key.
fn merge_property(core: &GraphCore, id: &str, key: &str, cell: serde_json::Value) -> bool {
    let mut map = read_node_obj(core, id);
    if map.get(key) == Some(&cell) {
        return false;
    }
    map.insert(key.to_string(), cell);
    write_node_obj(core, id, map);
    true
}

/// Remove a literal property `key` whose lexical value matches. Returns `true` if dropped.
fn delete_property(core: &GraphCore, id: &str, key: &str, value: &str) -> bool {
    let mut map = read_node_obj(core, id);
    let matches = map
        .get(key)
        .map(|cell| cell_lexical(cell) == Some(value.to_string()));
    if matches == Some(true) {
        map.remove(key);
        write_node_obj(core, id, map);
        true
    } else {
        false
    }
}

fn set_type_property(core: &GraphCore, id: &str, type_iri: &str) -> bool {
    let mut map = read_node_obj(core, id);
    let val = serde_json::Value::String(type_iri.to_string());
    if map.get("type") == Some(&val) {
        return false;
    }
    map.insert("type".to_string(), val);
    write_node_obj(core, id, map);
    true
}

fn clear_type_property(core: &GraphCore, id: &str, type_iri: &str) -> bool {
    let mut map = read_node_obj(core, id);
    if map.get("type").and_then(|v| v.as_str()) == Some(type_iri) {
        map.remove("type");
        write_node_obj(core, id, map);
        true
    } else {
        false
    }
}

fn edge_rel(blob: &[u8]) -> Option<String> {
    rmp_serde::from_slice::<serde_json::Value>(blob)
        .ok()
        .and_then(|v| v.get("type").and_then(|x| x.as_str()).map(String::from))
}

/// Add a typed edge `s --p--> o` unless one already exists. Returns `true` if added.
fn add_edge_if_absent(core: &GraphCore, s: &str, o: &str, p: &str) -> Result<bool, String> {
    let exists = core
        .get_edge_properties(s, o)
        .iter()
        .any(|b| edge_rel(b).as_deref() == Some(p));
    if exists {
        return Ok(false);
    }
    let blob = rmp_serde::to_vec_named(&serde_json::json!({ "type": p }))
        .map_err(|e| format!("encode edge: {e}"))?;
    core.add_edge(s.to_string(), o.to_string(), blob)?;
    Ok(true)
}

/// Remove the one `s --p--> o` edge, preserving any OTHER typed edges between the pair.
fn remove_typed_edge(core: &GraphCore, s: &str, o: &str, p: &str) -> bool {
    let existing = core.get_edge_properties(s, o);
    let mut survivors = Vec::new();
    let mut removed = false;
    for blob in existing {
        if !removed && edge_rel(&blob).as_deref() == Some(p) {
            removed = true; // drop exactly one matching edge
        } else {
            survivors.push(blob);
        }
    }
    if !removed {
        return false;
    }
    core.remove_edge(s.to_string(), o.to_string());
    for blob in survivors {
        let _ = core.add_edge(s.to_string(), o.to_string(), blob);
    }
    true
}

/// Lexical value of a stored property cell (typed `{value,…}` or a bare scalar).
fn cell_lexical(cell: &serde_json::Value) -> Option<String> {
    match cell {
        serde_json::Value::Object(m) => m.get("value").and_then(|v| v.as_str()).map(String::from),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

// ── an in-memory store for tests + embedded use ─────────────────────────────────

/// A simple [`GraphStore`] backed by in-memory `GraphCore`s, keyed by graph name (the
/// default graph is the empty key). Auto-creates graphs on first write. Used by the
/// eg-rdf tests and available to embedded callers that have no engine registry.
#[derive(Default)]
pub struct MapStore {
    graphs: std::sync::Mutex<std::collections::HashMap<String, Arc<GraphCore>>>,
}

impl MapStore {
    pub fn new() -> Self {
        Self::default()
    }
    fn key(graph: Option<&str>) -> String {
        graph.unwrap_or("").to_string()
    }
    /// Borrow (creating) the core for a graph — handy for tests asserting state.
    pub fn core_of(&self, graph: Option<&str>) -> Arc<GraphCore> {
        self.core(graph).unwrap()
    }
}

impl GraphStore for MapStore {
    fn core(&self, graph: Option<&str>) -> Option<Arc<GraphCore>> {
        let mut g = self.graphs.lock().unwrap();
        Some(
            g.entry(Self::key(graph))
                .or_insert_with(|| Arc::new(GraphCore::new()))
                .clone(),
        )
    }
    fn named(&self) -> Vec<(String, Arc<GraphCore>)> {
        self.graphs
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| !k.is_empty())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    fn clear(&self, graph: Option<&str>) -> Result<(), String> {
        if let Some(c) = self.graphs.lock().unwrap().get(&Self::key(graph)) {
            c.clear();
        }
        Ok(())
    }
    fn drop_graph(&self, graph: Option<&str>) -> Result<(), String> {
        self.graphs.lock().unwrap().remove(&Self::key(graph));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sparql::{run_outcome, QueryOutcome};

    fn count(store: &MapStore, graph: Option<&str>, query: &str) -> usize {
        let view = store.core_of(graph).analysis_snapshot();
        match run_outcome(&view, query, &Projection::raw()).unwrap() {
            QueryOutcome::Solutions(r) => r.solutions.len(),
            _ => panic!("expected solutions"),
        }
    }

    /// INSERT DATA then SELECT sees the triple; DELETE DATA removes it.
    #[test]
    fn insert_then_select_then_delete() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/> INSERT DATA { ex:a ex:knows ex:b . ex:a ex:name \"A\" }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?o WHERE { <http://ex/a> <http://ex/knows> ?o }"
            ),
            1,
            "edge visible after INSERT DATA"
        );
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?n WHERE { <http://ex/a> <http://ex/name> ?n }"
            ),
            1,
            "literal visible after INSERT DATA"
        );

        execute_str(
            "PREFIX ex: <http://ex/> DELETE DATA { ex:a ex:knows ex:b }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?o WHERE { <http://ex/a> <http://ex/knows> ?o }"
            ),
            0,
            "edge gone after DELETE DATA"
        );
        // The literal (a different triple) is untouched — incremental delete is surgical.
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?n WHERE { <http://ex/a> <http://ex/name> ?n }"
            ),
            1,
            "the name literal survives the edge delete"
        );
    }

    /// DELETE/INSERT … WHERE rewrites: rename every `ex:name "A"` to `"B"`.
    #[test]
    fn delete_insert_where_rewrites() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/> INSERT DATA { ex:a ex:name \"A\" . ex:b ex:name \"A\" }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        execute_str(
            "PREFIX ex: <http://ex/>
             DELETE { ?s ex:name \"A\" } INSERT { ?s ex:name \"B\" }
             WHERE  { ?s ex:name \"A\" }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?s WHERE { ?s <http://ex/name> \"A\" }"
            ),
            0,
            "no A left"
        );
        assert_eq!(
            count(
                &store,
                None,
                "SELECT ?s WHERE { ?s <http://ex/name> \"B\" }"
            ),
            2,
            "both renamed to B"
        );
    }

    /// Named-graph isolation: a triple inserted into GRAPH g1 is NOT in the default graph
    /// nor in g2, and a CLEAR of g1 leaves g2 intact.
    #[test]
    fn named_graph_isolation() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b }
               GRAPH <http://g/2> { ex:c ex:p ex:d }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(&store, Some("http://g/1"), "SELECT ?s WHERE { ?s ?p ?o }"),
            1,
            "g1 has its triple"
        );
        assert_eq!(
            count(&store, Some("http://g/2"), "SELECT ?s WHERE { ?s ?p ?o }"),
            1,
            "g2 has its triple"
        );
        assert_eq!(
            count(&store, None, "SELECT ?s WHERE { ?s ?p ?o }"),
            0,
            "default graph stays empty"
        );

        execute_str("CLEAR GRAPH <http://g/1>", &store, &Projection::raw()).unwrap();
        assert_eq!(
            count(&store, Some("http://g/1"), "SELECT ?s WHERE { ?s ?p ?o }"),
            0,
            "g1 cleared"
        );
        assert_eq!(
            count(&store, Some("http://g/2"), "SELECT ?s WHERE { ?s ?p ?o }"),
            1,
            "g2 untouched by CLEAR g1"
        );
    }

    /// ADD merges src into dst WITHOUT clearing dst (CONCEPT:EG-134). Both graphs' triples
    /// survive in dst; the source is left intact.
    #[test]
    fn add_merges_into_destination() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b . ex:a ex:name \"A\" }
               GRAPH <http://g/2> { ex:c ex:p ex:d }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        execute_str(
            "ADD <http://g/1> TO <http://g/2>",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        // dst keeps its own triple AND gains src's edge + literal (merge, no clear).
        assert_eq!(
            count(
                &store,
                Some("http://g/2"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            3,
            "g2 has its own + both of g1's triples"
        );
        // Source unchanged.
        assert_eq!(
            count(
                &store,
                Some("http://g/1"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            2,
            "g1 source left intact by ADD"
        );
    }

    /// COPY replaces dst with src (CONCEPT:EG-134): dst's prior content is dropped first,
    /// then src is copied in; src stays intact.
    #[test]
    fn copy_replaces_destination() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b }
               GRAPH <http://g/2> { ex:c ex:p ex:d . ex:c ex:q ex:e }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        execute_str(
            "COPY <http://g/1> TO <http://g/2>",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        // dst == src exactly (its two prior triples are gone).
        assert_eq!(
            count(
                &store,
                Some("http://g/2"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            1,
            "g2 replaced by g1's single triple"
        );
        assert_eq!(
            count(
                &store,
                Some("http://g/2"),
                "SELECT ?s WHERE { <http://ex/a> <http://ex/p> <http://ex/b> }"
            ),
            1,
            "g2 now holds g1's triple"
        );
        assert_eq!(
            count(
                &store,
                Some("http://g/1"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            1,
            "g1 source unchanged by COPY"
        );
    }

    /// MOVE replaces dst with src AND empties src (CONCEPT:EG-134).
    #[test]
    fn move_replaces_and_empties_source() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b . ex:a ex:name \"A\" }
               GRAPH <http://g/2> { ex:c ex:p ex:d }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        execute_str(
            "MOVE <http://g/1> TO <http://g/2>",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        assert_eq!(
            count(
                &store,
                Some("http://g/2"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            2,
            "g2 replaced by g1's two triples"
        );
        assert_eq!(
            count(
                &store,
                Some("http://g/1"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            0,
            "g1 source emptied by MOVE"
        );
    }

    /// SILENT swallows a missing-source: `MOVE SILENT` from a graph that was never written
    /// is a no-op that returns Ok and leaves the destination empty.
    #[test]
    fn move_silent_missing_source_is_noop() {
        let store = MapStore::new();
        let r = execute_str(
            "MOVE SILENT <http://g/missing> TO <http://g/dst>",
            &store,
            &Projection::raw(),
        );
        assert!(r.is_ok(), "SILENT missing-source move does not error");
        assert_eq!(
            count(
                &store,
                Some("http://g/dst"),
                "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
            ),
            0,
            "destination stays empty"
        );
    }

    /// A cross-named-graph query: with both graphs in the dataset, `GRAPH ?g {}` ranges
    /// over them independently (graph A's triple is not seen under graph B's name).
    #[test]
    fn named_graph_query_scoping() {
        let store = MapStore::new();
        execute_str(
            "PREFIX ex: <http://ex/>
             INSERT DATA {
               GRAPH <http://g/1> { ex:a ex:p ex:b }
               GRAPH <http://g/2> { ex:c ex:p ex:d }
             }",
            &store,
            &Projection::raw(),
        )
        .unwrap();
        // Build a dataset over both named graphs and ask which graph holds ex:a.
        let v1 = store.core_of(Some("http://g/1")).analysis_snapshot();
        let v2 = store.core_of(Some("http://g/2")).analysis_snapshot();
        let default = eg_core::graph::GraphView::default();
        let ds = Dataset::new(
            &default,
            vec![
                ("http://g/1".to_string(), &v1),
                ("http://g/2".to_string(), &v2),
            ],
        );
        let out = crate::sparql::run_outcome_dataset(
            &ds,
            "SELECT ?g WHERE { GRAPH ?g { <http://ex/a> <http://ex/p> <http://ex/b> } }",
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Solutions(r) = out else {
            panic!("expected solutions")
        };
        let graphs: Vec<String> = r
            .solutions
            .iter()
            .filter_map(|s| s.get("g").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            graphs,
            vec!["<http://g/1>".to_string()],
            "only g1 holds ex:a"
        );
    }
}
