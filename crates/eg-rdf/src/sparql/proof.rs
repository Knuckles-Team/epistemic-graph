//! Proof-carrying SPARQL SELECT results (EH-197).
//!
//! The OWL reasoner returns a proof tree for an entailment; a SPARQL row had nothing.
//! A row of a SELECT holds because some set of ground triples in the queried graph
//! instantiates the query's triple patterns under that row's bindings. This module
//! finds such a set — the row's WITNESS — and returns it with the row, so a caller
//! can check the answer against the data instead of trusting it.
//!
//! The witness is searched over the query's pattern obligations: every BGP triple
//! pattern reachable through `JOIN`/`FILTER`/`PROJECT`/`DISTINCT`/`REDUCED`/`SLICE`/
//! `ORDER BY`/`BIND` is REQUIRED; a pattern under the right side of an `OPTIONAL` is
//! witnessed when a matching triple exists. A row's proof is
//! [`SparqlProofCoverage::Complete`] when every required pattern was instantiated by
//! a triple of the graph. Algebra this witness does not certify — property paths,
//! `UNION`, `MINUS`, `GROUP BY` aggregates, `GRAPH`, `SERVICE`, or a query with its
//! own `FROM` dataset — makes every row [`SparqlProofCoverage::Partial`], and so does
//! a row whose search exhausts [`MAX_WITNESS_STEPS`]; a partial proof is never
//! presented as a complete one.

use eg_types::rdf_report::{
    SparqlObjectKind, SparqlProofCoverage, SparqlRowProof, SparqlWitnessTriple,
};
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;

use super::{
    bnode_var, eval_filter, evaluate_query, match_triple_pattern, merge, parse_query, Binding, Ctx,
    Dataset, Projection, Solution, SparqlResult,
};

/// Most candidate matches one row's witness search tries before giving up.
pub const MAX_WITNESS_STEPS: usize = 100_000;

/// Whether a pattern must be witnessed for the row to hold.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Obligation {
    Required,
    Optional,
}

/// The triple patterns a witness instantiates, the required-scope FILTERs its binding
/// must satisfy, and whether the query's algebra lets a witness certify its rows.
struct Obligations<'q> {
    patterns: Vec<(&'q TriplePattern, Obligation)>,
    filters: Vec<&'q Expression>,
    certifiable: bool,
}

/// A witness binding and the indices of the patterns it instantiated.
type Witness = (Solution, Vec<usize>);

/// Evaluate `query_str` like [`super::execute`] and return, beside the row table, one
/// proof per row in row order.
pub fn execute_explained(
    ds: &Dataset,
    query_str: &str,
    proj: &Projection,
) -> Result<(SparqlResult, Vec<SparqlRowProof>), String> {
    let query = parse_query(query_str)?;
    let table = evaluate_query(ds, &query, proj, None)?.into_table();
    let ctx = Ctx {
        ds,
        active: ds.default,
        proj,
        service: None,
    };
    let obligations = obligations(&query);
    let proofs = explain_rows(&ctx, &obligations, &table.solutions);
    Ok((table, proofs))
}

/// Collect the obligations of a SELECT's WHERE pattern. Any other query form, or a
/// query naming its own dataset, is not certifiable.
fn obligations(query: &Query) -> Obligations<'_> {
    let mut out = Obligations {
        patterns: Vec::new(),
        filters: Vec::new(),
        certifiable: query.dataset().is_none(),
    };
    match query {
        Query::Select { pattern, .. } => collect(pattern, Obligation::Required, &mut out),
        Query::Ask { .. } | Query::Construct { .. } | Query::Describe { .. } => {
            out.certifiable = false;
        }
    }
    out
}

/// Walk the algebra, recording each BGP pattern under the obligation its position
/// imposes; mark the query uncertifiable at the first operator a witness cannot cover.
fn collect<'q>(pattern: &'q GraphPattern, mode: Obligation, out: &mut Obligations<'q>) {
    match pattern {
        GraphPattern::Bgp { patterns } => out.patterns.extend(patterns.iter().map(|p| (p, mode))),
        GraphPattern::Join { left, right } => {
            collect(left, mode, out);
            collect(right, mode, out);
        }
        GraphPattern::LeftJoin { left, right, .. } => {
            collect(left, mode, out);
            collect(right, Obligation::Optional, out);
        }
        GraphPattern::Filter { expr, inner } => {
            if mode == Obligation::Required {
                out.filters.push(expr);
            }
            collect(inner, mode, out);
        }
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Extend { inner, .. } => collect(inner, mode, out),
        // A VALUES table binds without reading the graph: nothing to witness.
        GraphPattern::Values { .. } => {}
        GraphPattern::Path { .. }
        | GraphPattern::Union { .. }
        | GraphPattern::Minus { .. }
        | GraphPattern::Group { .. }
        | GraphPattern::Graph { .. }
        | GraphPattern::Service { .. } => out.certifiable = false,
    }
}

/// One proof per row. Each pattern's candidate matches are computed once and shared
/// by every row's search.
fn explain_rows(ctx: &Ctx, obligations: &Obligations, rows: &[Solution]) -> Vec<SparqlRowProof> {
    let candidates: Vec<Vec<Solution>> = obligations
        .patterns
        .iter()
        .map(|(pattern, _)| match_triple_pattern(ctx, pattern))
        .collect();
    let search = WitnessSearch {
        ctx,
        obligations,
        candidates: &candidates,
    };
    rows.iter()
        .enumerate()
        .map(|(index, row)| search.prove(index, row))
        .collect()
}

/// The shared, read-only inputs of every row's witness search.
struct WitnessSearch<'a, 'q> {
    ctx: &'a Ctx<'a>,
    obligations: &'a Obligations<'q>,
    candidates: &'a [Vec<Solution>],
}

impl WitnessSearch<'_, '_> {
    fn prove(&self, index: usize, row: &Solution) -> SparqlRowProof {
        let mut steps = 0usize;
        let found = self.extend(0, row.clone(), &mut steps);
        let witnesses = found
            .as_ref()
            .map(|witness| self.instantiate(witness))
            .unwrap_or_default();
        let complete = self.obligations.certifiable && found.is_some();
        SparqlRowProof {
            row: index as u64,
            witnesses,
            coverage: if complete {
                SparqlProofCoverage::Complete
            } else {
                SparqlProofCoverage::Partial
            },
        }
    }

    /// Depth-first search for a binding that extends `current` through every
    /// required pattern (and every optional one that has a consistent match) and
    /// satisfies every required-scope FILTER.
    fn extend(&self, index: usize, current: Solution, steps: &mut usize) -> Option<Witness> {
        let Some((_, obligation)) = self.obligations.patterns.get(index) else {
            return self.passes_filters(&current).then(|| (current, Vec::new()));
        };
        for candidate in &self.candidates[index] {
            if *steps >= MAX_WITNESS_STEPS {
                return None;
            }
            *steps += 1;
            let Some(merged) = merge(&current, candidate) else {
                continue;
            };
            if let Some((binding, mut matched)) = self.extend(index + 1, merged, steps) {
                matched.push(index);
                return Some((binding, matched));
            }
        }
        match obligation {
            Obligation::Optional => self.extend(index + 1, current, steps),
            Obligation::Required => None,
        }
    }

    fn passes_filters(&self, binding: &Solution) -> bool {
        self.obligations
            .filters
            .iter()
            .all(|expr| eval_filter(self.ctx, expr, binding))
    }

    /// The ground triples of exactly the patterns the witness matched, in pattern
    /// order — an optional pattern the search skipped contributes nothing, even when
    /// other patterns happen to bind all of its terms.
    fn instantiate(&self, witness: &Witness) -> Vec<SparqlWitnessTriple> {
        let (binding, matched) = witness;
        let mut indices = matched.clone();
        indices.sort_unstable();
        indices
            .into_iter()
            .filter_map(|index| ground(self.obligations.patterns[index].0, binding))
            .collect()
    }
}

/// `pattern` with every term resolved under `binding`, or `None` when a term is
/// unbound.
fn ground(pattern: &TriplePattern, binding: &Solution) -> Option<SparqlWitnessTriple> {
    let (subject, _) = term(&pattern.subject, binding)?;
    let predicate = match &pattern.predicate {
        NamedNodePattern::NamedNode(node) => node.as_str().to_string(),
        NamedNodePattern::Variable(var) => binding.get(var.as_str())?.as_str().to_string(),
    };
    let (object, object_kind) = term(&pattern.object, binding)?;
    Some(SparqlWitnessTriple {
        subject,
        predicate,
        object,
        object_kind,
    })
}

/// One subject/object term resolved under `binding`, in the evaluator's own lexical
/// form (`<iri>` / `_:b` for a resource, the lexical value for a literal).
fn term(pattern: &TermPattern, binding: &Solution) -> Option<(String, SparqlObjectKind)> {
    match pattern {
        TermPattern::NamedNode(node) => {
            Some((format!("<{}>", node.as_str()), SparqlObjectKind::Resource))
        }
        TermPattern::Literal(literal) => {
            Some((literal.value().to_string(), SparqlObjectKind::Literal))
        }
        TermPattern::Variable(var) => bound(binding.get(var.as_str())?),
        TermPattern::BlankNode(node) => bound(binding.get(&bnode_var(node))?),
        #[cfg(feature = "sparql-star")]
        TermPattern::Triple(_) => None,
    }
}

fn bound(value: &Binding) -> Option<(String, SparqlObjectKind)> {
    let kind = match value {
        Binding::Node(_) => SparqlObjectKind::Resource,
        Binding::Literal(_) => SparqlObjectKind::Literal,
    };
    Some((value.as_str().to_string(), kind))
}

#[cfg(test)]
mod tests;
