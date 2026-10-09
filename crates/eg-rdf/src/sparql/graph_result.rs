use super::*;

// ── CONSTRUCT / DESCRIBE (CONCEPT:EG-KG.query.named-graph-support) ───────────────────────────────────────

/// Instantiate a CONSTRUCT template against each WHERE solution → the result graph.
/// A pattern whose terms can't all be resolved/built for a given solution is skipped
/// (SPARQL: an unbound or ill-typed template slot yields no triple for that solution).
#[cfg(feature = "rdf")]
pub(super) fn construct_graph(
    template: &[TriplePattern],
    solutions: &[Solution],
) -> Vec<oxrdf::Triple> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for sol in solutions {
        for tp in template {
            if let Some(t) = instantiate_triple(tp, sol) {
                if seen.insert(t.to_string()) {
                    out.push(t);
                }
            }
        }
    }
    out
}

/// Resolve ONE template triple pattern against a solution to a concrete RDF triple.
/// A constant literal keeps its datatype/lang (it is the oxrdf `Literal` verbatim); a
/// variable-bound term carries only its lexical value (the `Binding` model is lexical),
/// so a bound literal becomes a simple literal — the documented projection limitation.
#[cfg(feature = "rdf")]
pub(super) fn instantiate_triple(tp: &TriplePattern, sol: &Solution) -> Option<oxrdf::Triple> {
    use oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};

    let subject: NamedOrBlankNode = match &tp.subject {
        TermPattern::NamedNode(n) => NamedOrBlankNode::NamedNode(n.clone()),
        TermPattern::BlankNode(b) => NamedOrBlankNode::BlankNode(b.clone()),
        TermPattern::Variable(v) => node_str_to_subject(sol.get(v.as_str())?.as_str())?,
        _ => return None,
    };
    let predicate: NamedNode = match &tp.predicate {
        NamedNodePattern::NamedNode(n) => n.clone(),
        NamedNodePattern::Variable(v) => {
            NamedNode::new(strip_iri(sol.get(v.as_str())?.as_str())).ok()?
        }
    };
    let object: Term = match &tp.object {
        TermPattern::NamedNode(n) => Term::NamedNode(n.clone()),
        TermPattern::BlankNode(b) => Term::BlankNode(b.clone()),
        TermPattern::Literal(l) => Term::Literal(l.clone()),
        TermPattern::Variable(v) => binding_to_term(sol.get(v.as_str())?),
        #[allow(unreachable_patterns)]
        _ => return None,
    };
    Some(Triple::new(subject, predicate, object))
}

/// Build the DESCRIBE graph: every resource bound (subject- or object-position) by the
/// WHERE pattern's variables, described by all triples of the active graph that mention
/// it (subject OR object position) — a minimal concise bounded description.
#[cfg(feature = "rdf")]
pub(super) fn describe_resources(
    ctx: &Ctx,
    vars: &[String],
    solutions: &[Solution],
) -> Vec<oxrdf::Triple> {
    let resources = describe_resource_set(vars, solutions);
    // All triples of the active graph (term-string form), filtered to those touching a
    // described resource in subject or object position.
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for (s, p, o, o_is_node) in all_triples_terms(ctx) {
        if !describes(&resources, &s, &o, o_is_node) {
            continue;
        }
        if let Some(t) = build_triple(&s, &p, &o, o_is_node) {
            if seen.insert(t.to_string()) {
                out.push(t);
            }
        }
    }
    out
}

/// The resource set: every binding across the projected vars whose value is a resource
/// term (`<iri>` / `_:b`). A `DESCRIBE <iri>` constant arrives as an `Extend`-bound
/// LITERAL lexically equal to `<iri>`, while a `DESCRIBE ?x` arrives as a `Node`
/// binding — both are captured by the term-form check.
#[cfg(feature = "rdf")]
pub(super) fn describe_resource_set(
    vars: &[String],
    solutions: &[Solution],
) -> std::collections::HashSet<String> {
    let mut resources: std::collections::HashSet<String> = std::collections::HashSet::new();
    for sol in solutions {
        for v in vars {
            let Some(b) = sol.get(v) else { continue };
            let s = b.as_str();
            if s.starts_with('<') || s.starts_with("_:") {
                resources.insert(s.to_string());
            }
        }
    }
    resources
}

/// Whether a triple (in projected term-string form) touches a described resource in
/// subject or object position.
#[cfg(feature = "rdf")]
pub(super) fn describes(
    resources: &std::collections::HashSet<String>,
    s: &str,
    o: &str,
    o_is_node: bool,
) -> bool {
    resources.contains(s) || (o_is_node && resources.contains(o))
}

/// Enumerate the active graph as `(subject, predicate, object, object_is_node)` projected
/// term strings — the same projection the `?s ?p ?o` BGP scan produces.
#[cfg(feature = "rdf")]
pub(super) fn all_triples_terms(ctx: &Ctx) -> Vec<(String, String, String, bool)> {
    let view = ctx.active;
    let proj = ctx.proj;
    let mut out = Vec::new();
    push_edge_triples_terms(view, proj, &mut out);
    push_node_triples_terms(view, proj, &mut out);
    out
}

/// The `edge_properties` half of [`all_triples_terms`]: every relationship edge as a
/// `(subject, predicate, object, true)` term triple.
#[cfg(feature = "rdf")]
pub(super) fn push_edge_triples_terms(
    view: &GraphView,
    proj: &Projection,
    out: &mut Vec<(String, String, String, bool)>,
) {
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) else {
                continue;
            };
            let Some(rel) = v.get("relationship").and_then(|x| x.as_str()) else {
                continue;
            };
            out.push((proj.node_iri(s), proj.pred_iri(rel), proj.node_iri(o), true));
        }
    }
}

/// The `node_properties` half of [`all_triples_terms`]: each node's `rdf:type` triple
/// (if typed) plus one literal triple per remaining scalar property.
#[cfg(feature = "rdf")]
pub(super) fn push_node_triples_terms(
    view: &GraphView,
    proj: &Projection,
    out: &mut Vec<(String, String, String, bool)>,
) {
    for (id, blob) in &view.node_properties {
        let Ok(v) = eg_types::msgpack::decode_property_value(blob.as_slice()) else {
            continue;
        };
        let Some(obj) = v.as_object() else { continue };
        let subj_iri = proj.node_iri(id);
        if let Some(ty) = obj
            .get("type")
            .or_else(|| obj.get("node_type"))
            .and_then(|x| x.as_str())
        {
            if let Some(type_obj) = proj.type_object_iri(ty) {
                out.push((subj_iri.clone(), RDF_TYPE_IRI.to_string(), type_obj, true));
            }
        }
        for (k, cell) in literal_cells(obj) {
            if let Some(lit_val) = cell_lexical(cell) {
                out.push((subj_iri.clone(), proj.pred_iri(k), lit_val, false));
            }
        }
    }
}

/// Parse a projected node-id string (`<iri>` / `_:b`) to an RDF subject; `None` else.
#[cfg(feature = "rdf")]
pub(super) fn node_str_to_subject(id: &str) -> Option<oxrdf::NamedOrBlankNode> {
    use oxrdf::{BlankNode, NamedNode, NamedOrBlankNode};
    if let Some(iri) = id.strip_prefix('<').and_then(|s| s.strip_suffix('>')) {
        Some(NamedOrBlankNode::NamedNode(NamedNode::new(iri).ok()?))
    } else if let Some(b) = id.strip_prefix("_:") {
        Some(NamedOrBlankNode::BlankNode(BlankNode::new(b).ok()?))
    } else {
        None
    }
}

/// Strip surrounding `<…>` from a node-iri term string for `NamedNode::new`.
#[cfg(feature = "rdf")]
pub(super) fn strip_iri(s: &str) -> &str {
    s.strip_prefix('<')
        .and_then(|x| x.strip_suffix('>'))
        .unwrap_or(s)
}

/// A solution binding → an RDF object term. A node binding parses as a resource (or a
/// simple literal if it is not a term form); a literal binding is a simple literal.
#[cfg(feature = "rdf")]
pub(super) fn binding_to_term(b: &Binding) -> oxrdf::Term {
    use oxrdf::{Literal, Term};
    match b {
        Binding::Node(id) => match node_str_to_subject(id) {
            Some(oxrdf::NamedOrBlankNode::NamedNode(n)) => Term::NamedNode(n),
            Some(oxrdf::NamedOrBlankNode::BlankNode(bn)) => Term::BlankNode(bn),
            _ => Term::Literal(Literal::new_simple_literal(b.as_str())),
        },
        Binding::Literal(v) => Term::Literal(Literal::new_simple_literal(v)),
    }
}

/// Build an RDF triple from projected term strings (`object_is_node` distinguishes a
/// resource object from a literal object).
#[cfg(feature = "rdf")]
pub(super) fn build_triple(
    s: &str,
    p: &str,
    o: &str,
    object_is_node: bool,
) -> Option<oxrdf::Triple> {
    use oxrdf::{Literal, NamedNode, Term, Triple};
    let subj = node_str_to_subject(s)?;
    let pred = NamedNode::new(strip_iri(p)).ok()?;
    let obj = if object_is_node {
        match node_str_to_subject(o)? {
            oxrdf::NamedOrBlankNode::NamedNode(n) => Term::NamedNode(n),
            oxrdf::NamedOrBlankNode::BlankNode(b) => Term::BlankNode(b),
        }
    } else {
        Term::Literal(Literal::new_simple_literal(o))
    };
    Some(Triple::new(subj, pred, obj))
}
