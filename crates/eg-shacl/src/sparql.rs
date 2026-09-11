//! A minimal, pre-binding-aware SPARQL 1.1 SELECT evaluator over an
//! `eg_rdf::oxrdf::Graph` (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL-SPARQL §3.5) — the engine half of
//! `sh:sparql` constraint evaluation.
//!
//! This is a DIFFERENT engine from eg-rdf's general SPARQL surface
//! (`eg_rdf::sparql`, which compiles onto the property-graph `GraphView`): a
//! `sh:sparql` constraint's query runs directly over the SAME `oxrdf::Graph` the
//! SHACL/ICV engine already holds as its data (and shapes) graph, with `$this`
//! (and, for a property shape, `$PATH`; and `$shapesGraph`/`$currentShape`)
//! genuinely PRE-BOUND rather than textually substituted — `SELECT $this ...`
//! would not even be syntactically valid SPARQL after a naive text substitution
//! of `$this` by an IRI. Every recursive evaluation threads an `init` floor of
//! pre-bound bindings that every base case starts from, so a pre-bound variable
//! is visible even where the query text never mentions it in a triple pattern
//! (`WHERE { FILTER($this = ...) }` has zero triple patterns).
//!
//! Scope, proven against the W3C SHACL-SPARQL test suite (see
//! `tests/w3c_sparql_closed.rs`): BGP / Join / LeftJoin / Filter / Union / Graph / Extend
//! (`BIND`/`AS`) / Project / Distinct / Reduced / OrderBy(pass-through) / Slice,
//! plus the FILTER built-ins a shape's `sh:select` commonly needs (`isIRI`/
//! `isBlank`/`isLiteral`/`isNumeric`/`bound`/`lang`/`langMatches`/`datatype`/`str`/
//! `regex`/`contains`/`strStarts`/`strEnds`/`ucase`/`lcase`/`strlen`, `=`/
//! `sameTerm`/`<`/`<=`/`>`/`>=`/`IN`, `&&`/`||`/`!`). Constructs the SHACL-SPARQL
//! spec explicitly permits an implementation to decline — aggregates/`GROUP BY`,
//! `MINUS`, `VALUES`, non-`SILENT` `SERVICE`, sub-`SELECT`, property paths,
//! `EXISTS`, and arithmetic — are REJECTED (`Err`), never silently mishandled: an
//! `sh:sparql` shape that needs one of those fails the validation run rather than
//! producing a wrong or incomplete report. Rebinding a pre-bound variable via
//! `BIND`/`AS` (e.g. `BIND(true AS $this)`) is likewise rejected — allowing it
//! would let a shape's query silently defeat pre-binding.

use std::collections::HashMap;

use eg_rdf::oxrdf::{Graph, NamedNode, Term};
use spargebra::{Query, SparqlParser};

mod expression;
mod pattern;
mod terms;

/// One SELECT solution: variable name (no `?`/`$` sigil) → bound term.
pub type Solution = HashMap<String, Term>;

/// The pre-bound input variables for one `sh:sparql` constraint evaluation
/// (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL-SPARQL §3.5.2): `$this` always; `$PATH` for a property shape;
/// `$shapesGraph`/`$currentShape` always (a node shape's query may reference
/// either even though it has no `sh:path`).
pub struct PreBindings {
    pub this: Term,
    pub path: Option<Term>,
    pub shapes_graph: Term,
    pub current_shape: Term,
}

/// The names `BIND`/`(expr AS ?var)` may never target — rebinding one of these
/// would let a shape's query silently defeat pre-binding.
pub(super) fn is_protected_var(name: &str) -> bool {
    matches!(name, "this" | "PATH" | "shapesGraph" | "currentShape")
}

/// A stable sentinel graph name bound to `$shapesGraph`, and matched by
/// `GRAPH $shapesGraph { … }` / `GRAPH ?g { … }` to route into the shapes graph
/// (our evaluator's dataset has exactly one named graph). Not a real dereferenced
/// IRI — just an opaque, collision-unlikely token.
pub fn shapes_graph_sentinel() -> Term {
    Term::NamedNode(NamedNode::new_unchecked(
        "urn:eg-shacl:shapes-graph#sentinel",
    ))
}

/// Evaluate a `sh:sparql` `sh:select` query (CONCEPT:EG-KG.ontology.concept-6, W3C SHACL-SPARQL §3.5) with
/// `pre` pre-bound, over `data` (the default/active graph) and `shapes` (reachable
/// via `GRAPH $shapesGraph { … }`). `prefixes` are prepended as `PREFIX` lines
/// before parsing (resolved from `sh:prefixes`/`sh:declare` by
/// [`crate::shapes::ShapesGraph::parse_sparql_constraint`]). Returns one
/// [`Solution`] per result row, each already restricted to the query's own
/// projected variables (a `SELECT $this ?path` result never carries an incidental
/// `?value` binding from inside the `WHERE` clause).
pub fn eval_select(
    query_text: &str,
    prefixes: &[(String, String)],
    data: &Graph,
    shapes: &Graph,
    pre: &PreBindings,
) -> Result<Vec<Solution>, String> {
    let query = parse_prefixed(query_text, prefixes)?;
    let Query::Select { pattern, .. } = query else {
        return Err("sh:sparql: sh:select must be a SPARQL SELECT query".to_string());
    };
    let init = init_solution(pre);
    let ctx = Ctx {
        data,
        shapes,
        shapes_graph_term: &pre.shapes_graph,
        active: data,
    };
    pattern::eval_pattern(&ctx, &pattern, &init)
}

fn parse_prefixed(query_text: &str, prefixes: &[(String, String)]) -> Result<Query, String> {
    let mut text = String::new();
    for (p, ns) in prefixes {
        text.push_str("PREFIX ");
        text.push_str(p);
        text.push_str(": <");
        text.push_str(ns);
        text.push_str(">\n");
    }
    text.push_str(query_text);
    SparqlParser::new()
        .parse_query(&text)
        .map_err(|e| format!("sh:sparql: query parse error: {e}"))
}

fn init_solution(pre: &PreBindings) -> Solution {
    let mut s = Solution::new();
    s.insert("this".to_string(), pre.this.clone());
    if let Some(p) = &pre.path {
        s.insert("PATH".to_string(), p.clone());
    }
    s.insert("shapesGraph".to_string(), pre.shapes_graph.clone());
    s.insert("currentShape".to_string(), pre.current_shape.clone());
    s
}

/// The active evaluation context: `data` and `shapes` are the two graphs of our
/// tiny dataset (default + the one named graph reachable via `$shapesGraph`);
/// `active` is whichever of them the current scan targets.
pub(super) struct Ctx<'a> {
    pub(super) data: &'a Graph,
    pub(super) shapes: &'a Graph,
    pub(super) shapes_graph_term: &'a Term,
    pub(super) active: &'a Graph,
}

impl<'a> Ctx<'a> {
    pub(super) fn with_active(&self, active: &'a Graph) -> Ctx<'a> {
        Ctx {
            data: self.data,
            shapes: self.shapes,
            shapes_graph_term: self.shapes_graph_term,
            active,
        }
    }
}
