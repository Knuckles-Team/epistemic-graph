use std::collections::{HashMap, HashSet};

use oxrdf::{Graph, Triple};
use spargebra::{GraphUpdateOperation, Update};

use crate::guard::{GuardRejection, WriteGuard};
use crate::sparql::Projection;

use super::parse::parse_update;
use super::store::{GraphStore, MapStore};
use super::terms::{apply_resolved_quad, apply_to_target, GroundQuadData};
use super::triples::insert_triples;
use super::where_ops::{exec_delete_insert, export_graph_triples};
use super::UpdateReport;

/// Parse + execute a SPARQL 1.1 UPDATE under the mandatory write guard.
pub fn execute_str(
    update_str: &str,
    store: &dyn GraphStore,
    proj: &Projection,
    guard: &dyn WriteGuard,
) -> Result<UpdateReport, UpdateError> {
    let update = parse_update(update_str).map_err(UpdateError::Exec)?;
    execute(&update, store, proj, guard)
}

/// Apply an already-authorized update to a store. This primitive is private so no
/// external caller can bypass the simulate/diff/guard transaction.
fn apply_update(
    update: &Update,
    store: &dyn GraphStore,
    proj: &Projection,
) -> Result<UpdateReport, String> {
    let mut report = UpdateReport::default();
    for op in &update.operations {
        report.operations += 1;
        apply_one_operation(op, store, proj, &mut report)?;
    }
    Ok(report)
}

/// Apply one update operation against `store`, updating `report`. Extracted from
/// [`apply_update`]'s per-operation match.
fn apply_one_operation(
    op: &GraphUpdateOperation,
    store: &dyn GraphStore,
    proj: &Projection,
    report: &mut UpdateReport,
) -> Result<(), String> {
    match op {
        GraphUpdateOperation::InsertData { data } => {
            for quad in data {
                apply_resolved_quad(store, quad.resolved(), report, true)?;
            }
            Ok(())
        }
        GraphUpdateOperation::DeleteData { data } => {
            for quad in data {
                apply_resolved_quad(store, quad.resolved(), report, false)?;
            }
            Ok(())
        }
        GraphUpdateOperation::DeleteInsert {
            delete,
            insert,
            using: _,
            pattern,
        } => exec_delete_insert(store, proj, delete, insert, pattern, report),
        GraphUpdateOperation::Clear { silent, graph } => {
            apply_to_target(store, graph, *silent, &|g| store.clear(g))
        }
        GraphUpdateOperation::Create { silent, graph } => apply_create(store, graph, *silent),
        GraphUpdateOperation::Drop { silent, graph } => {
            apply_to_target(store, graph, *silent, &|g| store.drop_graph(g))
        }
        GraphUpdateOperation::Load {
            silent,
            source,
            destination: _,
        } => apply_load(*silent, source),
    }
}

/// The `Create` arm of [`apply_one_operation`]'s match: create `graph`, propagating a
/// failure unless `silent`.
fn apply_create(
    store: &dyn GraphStore,
    graph: &oxrdf::NamedNode,
    silent: bool,
) -> Result<(), String> {
    let r = store.create(graph.as_str());
    if r.is_err() && !silent {
        return r;
    }
    Ok(())
}

/// The `Load` arm of [`apply_one_operation`]'s match: remote-URL LOAD is not supported
/// from the engine write path (use AddTriples / source_sync); `SILENT` swallows it.
fn apply_load(silent: bool, source: &oxrdf::NamedNode) -> Result<(), String> {
    if silent {
        return Ok(());
    }
    Err(format!(
        "LOAD <{}>: remote-URL load is not supported from the engine write \
         path (use AddTriples / source_sync); add SILENT to ignore",
        source.as_str()
    ))
}

// ── EG-300 constraint-enforced commit (WriteGuard hook) ─────────────────────────

/// The failure of a guarded commit (CONCEPT:EG-KG.ontology.rdf-update-guard): either the underlying
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

/// Execute a parsed SPARQL UPDATE as a **constraint-enforced transaction** (CONCEPT:EG-KG.ontology.rdf-update-guard).
///
/// The whole change set is the transaction boundary:
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
/// Deterministic: the shadow replay and the real replay start from the identical
/// `base`, so `DELETE/INSERT … WHERE` binds the same solutions in both.
pub fn execute(
    update: &Update,
    store: &dyn GraphStore,
    proj: &Projection,
    guard: &dyn WriteGuard,
) -> Result<UpdateReport, UpdateError> {
    // (1) snapshot base — the default graph plus every named graph currently in the store.
    let base = snapshot_store(store).map_err(UpdateError::Exec)?;

    // (2) seed a shadow store from base and replay the update there (real store untouched).
    let shadow = MapStore::new();
    for (g, triples) in &base {
        if let Some(core) = shadow.core(g.as_deref()) {
            insert_triples(&core, triples).map_err(UpdateError::Exec)?;
        }
    }
    apply_update(update, &shadow, proj).map_err(UpdateError::Exec)?;
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
            continue;
        }
        let base_graph = triples_to_graph(b);
        guard
            .check_graph(name.as_deref(), &base_graph, &additions, &removals)
            .map_err(UpdateError::Rejected)?;
    }

    // (4/5) accepted — commit to the real store (same base ⇒ same result).
    apply_update(update, store, proj).map_err(UpdateError::Exec)
}

/// Snapshot every graph of a store to RDF triples: the default graph (key `None`) plus
/// each named graph. Reuses the same lossless export the whole-graph copy path uses.
fn snapshot_store(store: &dyn GraphStore) -> Result<HashMap<Option<String>, Vec<Triple>>, String> {
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
