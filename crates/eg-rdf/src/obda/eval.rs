//! Established RDF materialization and SPARQL evaluation for virtual graphs.

use super::*;

/// Keep the existing evaluator as the authoritative fallback for every query
/// shape or source that declines the narrow direct-solution capability.
pub(super) fn run_materialized_virtual(
    vg: &VirtualGraph,
    reg: &ObdaSourceRegistry,
    query_str: &str,
    proj: &Projection,
    wanted: &Option<BTreeSet<String>>,
    filters: &FilterContext,
) -> Result<QueryOutcome, String> {
    // (2)+(3) scan the backing source(s) on demand for only the needed columns, applying
    // the pushed-down FILTERs, and materialize the query-relevant triples via the TriplesMaps.
    let triples = vg.materialize(reg, wanted, filters)?;

    // (4) load into a TRANSIENT view and run the existing evaluator (joins/filters/…).
    let view = build_view(triples)?;
    crate::sparql::execute(
        &crate::sparql::Dataset::new(&view, Vec::new()),
        query_str,
        proj,
        None,
    )
}

/// Build a transient [`GraphView`] from materialized triples (nothing persisted).
fn build_view(triples: Vec<Triple>) -> Result<eg_core::graph::GraphView, String> {
    let core = eg_core::graph::GraphCore::new();
    let mut iris = crate::mapping::IriStore::default();
    // RDF graphs are sets of triples. The LPG lowering layer retains repeated
    // literal property cells, so collapse identical foreign rows here before
    // constructing the transient view. Keep first-seen order for stable ties.
    let mut seen = std::collections::HashSet::new();
    let unique = triples
        .into_iter()
        .filter(|triple| seen.insert(triple.clone()));
    crate::mapping::load_triples(&core, &mut iris, "__obda_virtual__", unique)?;
    Ok(core.analysis_snapshot())
}

/// A one-row boolean result table (`ASK` over a virtual graph).
pub(super) fn bool_result(b: bool) -> SparqlResult {
    let mut sol = HashMap::new();
    sol.insert(
        "_ask".to_string(),
        crate::sparql::Binding::Literal(b.to_string()),
    );
    SparqlResult {
        vars: vec!["_ask".to_string()],
        solutions: vec![sol],
    }
}

/// CONCEPT:EG-KG.ontology.foreign-source-seam — the set of predicate IRIs a query's triple patterns reference, or
/// `None` when the pushdown cannot be narrowed (a variable predicate, a property path, or
/// an unrecognized algebra node) — in which case EVERY predicate is materialized so the
/// answer stays complete. This is the projection/predicate pushdown key.
pub(super) fn wanted_predicates(query: &spargebra::Query) -> Option<BTreeSet<String>> {
    let pattern = query_pattern(query);
    let mut preds = BTreeSet::new();
    let mut unrestricted = false;
    collect_predicates(pattern, &mut preds, &mut unrestricted);
    if unrestricted {
        None
    } else {
        Some(preds)
    }
}

pub(super) fn query_pattern(query: &spargebra::Query) -> &spargebra::algebra::GraphPattern {
    use spargebra::Query;
    match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    }
}

/// Walk the algebra collecting constant predicate IRIs from every BGP; set `unrestricted`
/// on a variable predicate, a property path, or an unrecognized node (so materialization
/// falls back to "all predicates" and completeness is preserved).
fn collect_predicates(
    p: &spargebra::algebra::GraphPattern,
    preds: &mut BTreeSet<String>,
    unrestricted: &mut bool,
) {
    use spargebra::algebra::GraphPattern as G;
    use spargebra::term::NamedNodePattern;
    // The trailing `_` arm is only reachable when spargebra's `sep-0006` `Lateral`
    // variant (or a future variant) is compiled in; without it the match is already
    // exhaustive, so silence the resulting unreachable-pattern lint.
    #[allow(unreachable_patterns)]
    match p {
        G::Bgp { patterns } => {
            for tp in patterns {
                match &tp.predicate {
                    NamedNodePattern::NamedNode(n) => {
                        preds.insert(n.as_str().to_string());
                    }
                    // A `?p` predicate can match ANY predicate → cannot narrow.
                    NamedNodePattern::Variable(_) => *unrestricted = true,
                }
            }
        }
        // A property path can traverse arbitrary predicates (closures, alternatives) —
        // conservatively materialize everything.
        G::Path { .. } => *unrestricted = true,
        G::Join { left, right } | G::Union { left, right } | G::Minus { left, right } => {
            collect_predicates(left, preds, unrestricted);
            collect_predicates(right, preds, unrestricted);
        }
        G::LeftJoin { left, right, .. } => {
            collect_predicates(left, preds, unrestricted);
            collect_predicates(right, preds, unrestricted);
        }
        G::Filter { inner, .. }
        | G::Extend { inner, .. }
        | G::OrderBy { inner, .. }
        | G::Project { inner, .. }
        | G::Distinct { inner }
        | G::Reduced { inner }
        | G::Slice { inner, .. }
        | G::Group { inner, .. }
        | G::Graph { inner, .. }
        | G::Service { inner, .. } => collect_predicates(inner, preds, unrestricted),
        G::Values { .. } => {}
        // `Lateral` (feature `sep-0006`) and any future variant we cannot see into.
        _ => *unrestricted = true,
    }
}
