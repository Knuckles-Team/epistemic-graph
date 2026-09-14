use eg_core::graph::GraphCore;
use spargebra::term::{
    GraphNamePattern, GroundQuadPattern, NamedNodePattern, QuadPattern, TermPattern, TriplePattern,
    Variable,
};

use crate::sparql::{Dataset, Projection, Solution};

use super::store::GraphStore;
use super::terms::{apply_resolved_quad, instantiate};
use super::triples::insert_triples;
use super::UpdateReport;

// ── DELETE/INSERT … WHERE ───────────────────────────────────────────────────────

pub(super) fn exec_delete_insert(
    store: &dyn GraphStore,
    proj: &Projection,
    delete: &[GroundQuadPattern],
    insert: &[QuadPattern],
    pattern: &spargebra::algebra::GraphPattern,
    report: &mut UpdateReport,
) -> Result<(), String> {
    // CONCEPT:EG-KG.query.sparql-add-copy-move — ADD / COPY / MOVE fast path; see
    // `try_whole_graph_copy`'s docs for why.
    if try_whole_graph_copy(store, delete, insert, pattern, report)?.is_some() {
        return Ok(());
    }

    // Snapshot the store's graphs and evaluate the WHERE over them (named-graph aware).
    let (default_view, named_views) = snapshot_dataset_views(store)?;
    let named_refs: Vec<(String, &eg_core::graph::GraphView)> =
        named_views.iter().map(|(n, v)| (n.clone(), v)).collect();
    let ds = Dataset::new(&default_view, named_refs);

    let solutions = crate::sparql::eval_where(&ds, pattern, proj)?;

    // DELETE first (SPARQL: the delete sees the pre-update graph), then INSERT.
    apply_delete_solutions(store, delete, &solutions, report)?;
    apply_insert_solutions(store, insert, &solutions, report)?;
    Ok(())
}

/// The ADD/COPY/MOVE fast path (CONCEPT:EG-KG.query.sparql-add-copy-move): spargebra performs the W3C rewriting
/// at parse time, so each desugars to a whole-graph `?s ?p ?o` copy (this
/// `DeleteInsert`) plus a preceding DROP of the destination (COPY/MOVE) and a trailing
/// DROP of the source (MOVE), which the `Clear`/`Drop` arms already execute via
/// `GraphStore::clear`. So COPY/MOVE/ADD need no dedicated `execute` arm — this
/// recognizes only the canonical whole-graph-copy shape and runs it as a LOSSLESS
/// graph→graph triple copy (reusing `export_triples` + `insert_triples`) instead of the
/// lossy binding round-trip the generic WHERE path would use (which flattens literal
/// datatypes). Returns `Ok(Some(()))` when handled (a real copy, or a no-op because
/// source/destination is missing — SILENT-friendly), `Ok(None)` to fall through to the
/// generic WHERE path. Extracted from [`exec_delete_insert`].
fn try_whole_graph_copy(
    store: &dyn GraphStore,
    delete: &[GroundQuadPattern],
    insert: &[QuadPattern],
    pattern: &spargebra::algebra::GraphPattern,
    report: &mut UpdateReport,
) -> Result<Option<()>, String> {
    let Some((from, to)) = whole_graph_copy(delete, insert, pattern) else {
        return Ok(None);
    };
    let (Some(src), Some(dst)) = (store.core(from.as_deref()), store.core(to.as_deref())) else {
        // Missing source/destination ⇒ nothing to copy (SILENT-friendly: the DROPs
        // that frame COPY/MOVE are `silent` and no error is raised here either).
        return Ok(Some(()));
    };
    let triples = export_graph_triples(&src, from.as_deref().unwrap_or(""))?;
    report.inserted += insert_triples(&dst, &triples)?;
    Ok(Some(()))
}

/// Snapshot the default graph + every named graph as analysis views, for
/// [`exec_delete_insert`]'s WHERE evaluation.
fn snapshot_dataset_views(
    store: &dyn GraphStore,
) -> Result<
    (
        eg_core::graph::GraphView,
        Vec<(String, eg_core::graph::GraphView)>,
    ),
    String,
> {
    let default_core = store
        .core(None)
        .ok_or("DELETE/INSERT WHERE: no default graph")?;
    let default_view = default_core.analysis_snapshot();
    let named_cores = store.named();
    let named_views: Vec<(String, eg_core::graph::GraphView)> = named_cores
        .iter()
        .map(|(n, c)| (n.clone(), c.analysis_snapshot()))
        .collect();
    Ok((default_view, named_views))
}

/// The DELETE phase of [`exec_delete_insert`]'s WHERE path (SPARQL: the delete sees the
/// pre-update graph).
fn apply_delete_solutions(
    store: &dyn GraphStore,
    delete: &[GroundQuadPattern],
    solutions: &[Solution],
    report: &mut UpdateReport,
) -> Result<(), String> {
    for sol in solutions {
        for gqp in delete {
            let parts = instantiate(
                &gqp.graph_name,
                &gqp.subject,
                &gqp.predicate,
                &gqp.object,
                sol,
            );
            apply_resolved_quad(store, parts, report, false)?;
        }
    }
    Ok(())
}

/// The INSERT phase of [`exec_delete_insert`]'s WHERE path.
fn apply_insert_solutions(
    store: &dyn GraphStore,
    insert: &[QuadPattern],
    solutions: &[Solution],
    report: &mut UpdateReport,
) -> Result<(), String> {
    for sol in solutions {
        for qp in insert {
            let parts = instantiate(&qp.graph_name, &qp.subject, &qp.predicate, &qp.object, sol);
            apply_resolved_quad(store, parts, report, true)?;
        }
    }
    Ok(())
}

/// Recognize the canonical whole-graph copy `spargebra` emits for ADD/COPY/MOVE
/// (CONCEPT:EG-KG.query.sparql-add-copy-move): an empty DELETE, a single INSERT quad-pattern of three variables
/// `?s ?p ?o` into a constant destination graph, and a WHERE that is exactly the SAME
/// three variables over one source graph (a bare BGP ⇒ the default graph, or a
/// `GRAPH <src> { … }`). Returns `(from, to)` as bare-iri graph names (`None` = the
/// default graph) when the shape matches, else `None` (fall back to the generic path).
fn whole_graph_copy(
    delete: &[GroundQuadPattern],
    insert: &[QuadPattern],
    pattern: &spargebra::algebra::GraphPattern,
) -> Option<(Option<String>, Option<String>)> {
    if !delete.is_empty() || insert.len() != 1 {
        return None;
    }
    let (vs, vp, vo, to) = insert_copy_shape(&insert[0])?;
    let (from, tp) = where_copy_shape(pattern)?;
    // The WHERE triple must be the SAME three variables the INSERT reuses.
    let same = matches!(&tp.subject, TermPattern::Variable(s) if s == vs)
        && matches!(&tp.predicate, NamedNodePattern::Variable(p) if p == vp)
        && matches!(&tp.object, TermPattern::Variable(o) if o == vo);
    if !same {
        return None;
    }
    Some((from, to))
}

/// Validate + destructure the single INSERT quad-pattern's shape: the same variable
/// reused as subject/predicate/object, and a constant (or default) destination graph.
/// `None` when it doesn't match. Part of [`whole_graph_copy`]'s shape recognition.
fn insert_copy_shape(
    qp: &QuadPattern,
) -> Option<(&Variable, &Variable, &Variable, Option<String>)> {
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
    Some((vs, vp, vo, to))
}

/// Validate + destructure the WHERE pattern's shape: one bare BGP (source = default
/// graph) or `GRAPH <src> { BGP }`, itself exactly one triple pattern. `None` when it
/// doesn't match. Part of [`whole_graph_copy`]'s shape recognition.
fn where_copy_shape(
    pattern: &spargebra::algebra::GraphPattern,
) -> Option<(Option<String>, &TriplePattern)> {
    use spargebra::algebra::GraphPattern as GP;
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
    Some((from, tp))
}

/// Export a graph core back to RDF triples for a whole-graph copy. Embedded
/// multi-valued literals are part of the graph image and therefore copy with it.
pub(super) fn export_graph_triples(
    core: &GraphCore,
    graph_name: &str,
) -> Result<Vec<oxrdf::Triple>, String> {
    crate::mapping::export_triples(core, graph_name)
}
