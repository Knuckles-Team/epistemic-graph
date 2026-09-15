use std::collections::HashSet;

use oxrdf::Triple;
use spargebra::algebra::GraphTarget;
use spargebra::term::{GraphName, GraphNamePattern};
use spargebra::{GraphUpdateOperation, SparqlParser, Update};

/// Parse a SPARQL 1.1 UPDATE string into the spargebra model.
pub fn parse_update(update_str: &str) -> Result<Update, String> {
    SparqlParser::new()
        .parse_update(update_str)
        .map_err(|e| format!("sparql update parse: {e}"))
}

/// Extract the ground triples an `INSERT DATA { … }` update inserts (CONCEPT:EG-KG.txn.isolation-ryow-begin-set), so
/// a caller that must STAGE (not directly apply) the axioms — e.g. the pgwire cross-modal
/// transaction seam — can lower them to graph-native methods with the SAME RDF ⇄
/// property-graph mapping the loader uses. Only `INSERT DATA` operations contribute
/// (the staging-insert case); other update ops (DELETE DATA, DELETE/INSERT … WHERE,
/// CLEAR/DROP/…) are ignored — a txn that needs them applies directly via [`execute`].
pub fn insert_data_triples(update_str: &str) -> Result<Vec<Triple>, String> {
    let update = parse_update(update_str)?;
    let mut out = Vec::new();
    for op in &update.operations {
        if let GraphUpdateOperation::InsertData { data } = op {
            for q in data {
                out.push(Triple::new(
                    q.subject.clone(),
                    q.predicate.clone(),
                    q.object.clone(),
                ));
            }
        }
    }
    Ok(out)
}

/// The constant named-graph IRIs an UPDATE references (so a caller — e.g. the `/sparql`
/// endpoint — can pre-create them in its registry before executing). Covers ground data,
/// CREATE/CLEAR/DROP targets, the LOAD destination, and constant insert/delete pattern
/// graph names — the practical "write to a new named graph" cases.
pub fn referenced_named_graphs(update: &Update) -> Vec<String> {
    let mut out = HashSet::new();
    for op in &update.operations {
        collect_op_named_graphs(op, &mut out);
    }
    out.into_iter().collect()
}

/// Collect the constant named-graph IRIs one update operation references. Extracted
/// from [`referenced_named_graphs`]'s per-operation match.
fn collect_op_named_graphs(op: &GraphUpdateOperation, out: &mut HashSet<String>) {
    match op {
        GraphUpdateOperation::InsertData { data } => {
            push_named_graph_names(data.iter().map(|q| &q.graph_name), out);
        }
        GraphUpdateOperation::DeleteData { data } => {
            push_named_graph_names(data.iter().map(|q| &q.graph_name), out);
        }
        GraphUpdateOperation::DeleteInsert { insert, delete, .. } => {
            push_pattern_graph_names(insert.iter().map(|qp| &qp.graph_name), out);
            push_pattern_graph_names(delete.iter().map(|gqp| &gqp.graph_name), out);
        }
        GraphUpdateOperation::Create { graph, .. } => {
            out.insert(graph.as_str().to_string());
        }
        GraphUpdateOperation::Clear { graph, .. } | GraphUpdateOperation::Drop { graph, .. } => {
            push_graph_target(graph, out);
        }
        GraphUpdateOperation::Load { destination, .. } => {
            push_named_graph_names(std::iter::once(destination), out);
        }
    }
}

/// Insert every `GraphName::NamedNode` from `names` into `out`.
fn push_named_graph_names<'a>(
    names: impl Iterator<Item = &'a GraphName>,
    out: &mut HashSet<String>,
) {
    for g in names {
        if let GraphName::NamedNode(n) = g {
            out.insert(n.as_str().to_string());
        }
    }
}

/// Insert every `GraphNamePattern::NamedNode` from `names` into `out`.
fn push_pattern_graph_names<'a>(
    names: impl Iterator<Item = &'a GraphNamePattern>,
    out: &mut HashSet<String>,
) {
    for g in names {
        if let GraphNamePattern::NamedNode(n) = g {
            out.insert(n.as_str().to_string());
        }
    }
}

/// Insert `t` into `out` when it is a `GraphTarget::NamedNode`.
fn push_graph_target(t: &GraphTarget, out: &mut HashSet<String>) {
    if let GraphTarget::NamedNode(n) = t {
        out.insert(n.as_str().to_string());
    }
}
