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

use std::cmp::Ordering;
use std::collections::HashMap;

use eg_rdf::oxrdf::{Graph, Literal, NamedNode, NamedOrBlankNodeRef, Term};
use regex::RegexBuilder;
use spargebra::algebra::{Expression, Function, GraphPattern};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::{Query, SparqlParser};

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
fn is_protected_var(name: &str) -> bool {
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
    eval_pattern(&ctx, &pattern, &init)
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
struct Ctx<'a> {
    data: &'a Graph,
    shapes: &'a Graph,
    shapes_graph_term: &'a Term,
    active: &'a Graph,
}

impl<'a> Ctx<'a> {
    fn with_active(&self, active: &'a Graph) -> Ctx<'a> {
        Ctx {
            data: self.data,
            shapes: self.shapes,
            shapes_graph_term: self.shapes_graph_term,
            active,
        }
    }
}

// ── Graph pattern evaluation ─────────────────────────────────────────────────

/// Evaluate a WHERE-clause graph pattern, threading the pre-bound `init` floor
/// into every base case (so a pre-bound variable is visible even in a
/// zero-triple-pattern `Bgp`).
fn eval_pattern(
    ctx: &Ctx,
    pattern: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    match pattern {
        GraphPattern::Bgp { patterns } => {
            let mut acc = vec![init.clone()];
            for tp in patterns {
                let matches = match_triple_pattern(ctx.active, tp);
                acc = hash_join(&acc, &matches);
            }
            Ok(acc)
        }
        GraphPattern::Join { left, right } => {
            let l = eval_pattern(ctx, left, init)?;
            let r = eval_pattern(ctx, right, init)?;
            Ok(hash_join(&l, &r))
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            let l = eval_pattern(ctx, left, init)?;
            let r = eval_pattern(ctx, right, init)?;
            let mut out = Vec::new();
            for a in &l {
                let mut matched_any = false;
                for b in &r {
                    let Some(m) = merge(a, b) else { continue };
                    let keep = match expression {
                        Some(e) => eval_filter(ctx, e, &m)?,
                        None => true,
                    };
                    if keep {
                        matched_any = true;
                        out.push(m);
                    }
                }
                if !matched_any {
                    out.push(a.clone());
                }
            }
            Ok(out)
        }
        GraphPattern::Filter { expr, inner } => {
            let rows = eval_pattern(ctx, inner, init)?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                if eval_filter(ctx, expr, &row)? {
                    out.push(row);
                }
            }
            Ok(out)
        }
        GraphPattern::Union { left, right } => {
            let mut l = eval_pattern(ctx, left, init)?;
            let mut r = eval_pattern(ctx, right, init)?;
            l.append(&mut r);
            Ok(l)
        }
        GraphPattern::Graph { name, inner } => eval_graph(ctx, name, inner, init),
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let name = variable.as_str();
            if is_protected_var(name) {
                return Err(format!(
                    "sh:sparql: BIND/AS may not rebind the pre-bound variable ?{name}"
                ));
            }
            let rows = eval_pattern(ctx, inner, init)?;
            let mut out = Vec::with_capacity(rows.len());
            for mut row in rows {
                if let Some(v) = eval_term(ctx, expression, &row)? {
                    row.insert(name.to_string(), v);
                    out.push(row);
                }
                // An expression that evaluates to unbound (Ok(None), e.g. a type
                // error) drops the row's BIND per SPARQL semantics but is not
                // itself a hard failure -- so this branch simply omits the row's
                // extra binding rather than erroring. Matching upstream engines,
                // we still keep the row (BIND never filters; only FILTER does).
                else {
                    out.push(row);
                }
            }
            Ok(out)
        }
        GraphPattern::Project { inner, variables } => {
            let rows = eval_pattern(ctx, inner, init)?;
            let keep: Vec<&str> = variables.iter().map(|v| v.as_str()).collect();
            Ok(rows
                .into_iter()
                .map(|row| {
                    row.into_iter()
                        .filter(|(k, _)| keep.contains(&k.as_str()))
                        .collect()
                })
                .collect())
        }
        GraphPattern::Distinct { inner } => {
            let rows = eval_pattern(ctx, inner, init)?;
            Ok(dedup(rows))
        }
        GraphPattern::Reduced { inner } | GraphPattern::OrderBy { inner, .. } => {
            // ORDER BY does not affect a SHACL result SET (row order is not
            // significant to `sh:sparql`); REDUCED permits, but does not
            // require, duplicate elimination -- passing `inner` through
            // unmodified is a conforming (maximal) implementation of both.
            eval_pattern(ctx, inner, init)
        }
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let rows = eval_pattern(ctx, inner, init)?;
            let iter = rows.into_iter().skip(*start);
            Ok(match length {
                Some(n) => iter.take(*n).collect(),
                None => iter.collect(),
            })
        }
        GraphPattern::Path { .. } => {
            Err("sh:sparql: SPARQL property-path triples are not supported".to_string())
        }
        GraphPattern::Minus { .. } => Err("sh:sparql: MINUS is not supported".to_string()),
        GraphPattern::Values { .. } => Err("sh:sparql: VALUES is not supported".to_string()),
        GraphPattern::Group { .. } => {
            Err("sh:sparql: GROUP BY / aggregates are not supported".to_string())
        }
        GraphPattern::Service { silent: true, .. } => Ok(Vec::new()),
        GraphPattern::Service { .. } => Err("sh:sparql: SERVICE is not supported".to_string()),
        // `GraphPattern::Lateral` exists only behind spargebra's `sep-0006` (which
        // nothing in this workspace currently enables, confirmed by
        // `grep -r sep-0006`) — but, exactly like `TermPattern::Triple`
        // (`bind_term`, `sparql-12`) above, eg-shacl has no Cargo feature of its
        // own correlated with whether some OTHER crate's feature unification ever
        // turns it on. A wildcard erring, not silently mis-evaluating, is the
        // future-proof choice if that ever changes.
        #[allow(unreachable_patterns)]
        _ => Err("sh:sparql: this SPARQL construct is not supported".to_string()),
    }
}

/// `GRAPH <name> { inner }` — our dataset has exactly one named graph, reachable
/// under [`shapes_graph_sentinel`]. A bound `name` that resolves to anything else
/// is simply an unknown named graph (empty result, not an error — matches SPARQL
/// dataset semantics for a `GRAPH` clause naming a graph outside the dataset).
fn eval_graph(
    ctx: &Ctx,
    name: &NamedNodePattern,
    inner: &GraphPattern,
    init: &Solution,
) -> Result<Vec<Solution>, String> {
    let var = match name {
        NamedNodePattern::NamedNode(n) => {
            return if Term::NamedNode(n.clone()) == *ctx.shapes_graph_term {
                eval_pattern(&ctx.with_active(ctx.shapes), inner, init)
            } else {
                Ok(Vec::new())
            };
        }
        NamedNodePattern::Variable(v) => v,
    };
    if let Some(bound) = init.get(var.as_str()) {
        if bound != ctx.shapes_graph_term {
            return Ok(Vec::new());
        }
    }
    let rows = eval_pattern(&ctx.with_active(ctx.shapes), inner, init)?;
    Ok(rows
        .into_iter()
        .map(|mut r| {
            r.insert(var.as_str().to_string(), ctx.shapes_graph_term.clone());
            r
        })
        .collect())
}

fn dedup(rows: Vec<Solution>) -> Vec<Solution> {
    let mut out: Vec<Solution> = Vec::with_capacity(rows.len());
    for row in rows {
        if !out.contains(&row) {
            out.push(row);
        }
    }
    out
}

// ── Triple-pattern matching (BGP leaves) ─────────────────────────────────────

/// Every graph triple whose shape matches `tp`, each yielding the bindings `tp`
/// alone introduces (no knowledge of any OTHER pattern or of `init` — restriction
/// against those happens via the caller's [`hash_join`]).
fn match_triple_pattern(graph: &Graph, tp: &TriplePattern) -> Vec<Solution> {
    let mut out = Vec::new();
    for t in graph.iter() {
        let mut sol = Solution::new();
        let subject: Term = match t.subject {
            NamedOrBlankNodeRef::NamedNode(n) => Term::NamedNode(n.into_owned()),
            NamedOrBlankNodeRef::BlankNode(b) => Term::BlankNode(b.into_owned()),
        };
        if !bind_term(&tp.subject, &subject, &mut sol) {
            continue;
        }
        if !bind_pred(&tp.predicate, t.predicate.into_owned(), &mut sol) {
            continue;
        }
        if !bind_term(&tp.object, &t.object.into_owned(), &mut sol) {
            continue;
        }
        out.push(sol);
    }
    out
}

fn bind_term(pat: &TermPattern, actual: &Term, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::NamedNode(n) => matches!(actual, Term::NamedNode(a) if a == n),
        TermPattern::Literal(l) => matches!(actual, Term::Literal(a) if a == l),
        TermPattern::BlankNode(b) => bind_var(&format!("__bnode_{}", b.as_str()), actual, sol),
        TermPattern::Variable(v) => bind_var(v.as_str(), actual, sol),
        // RDF-star's `TermPattern::Triple` (spargebra `sparql-12`) is out of this
        // evaluator's bounded scope (SHACL/ICV do not need quoted triples) — never
        // named explicitly (eg-shacl requests no `rdf-12`/`sparql-12` feature of
        // its own, so the variant may or may not even exist depending on whether a
        // SIBLING crate elsewhere in the same build turns spargebra's `sparql-12`
        // on; Cargo unifies a dependency's features build-wide, not per-crate).
        // Mirrors eg-rdf's own `sparql.rs` (`instantiate_triple`) handling of the
        // identical situation: a wildcard, `#[allow(unreachable_patterns)]`
        // because whether it is reachable depends on that external unification,
        // not on anything eg-shacl's own Cargo features can express.
        #[allow(unreachable_patterns)]
        _ => false,
    }
}

fn bind_pred(pat: &NamedNodePattern, actual: NamedNode, sol: &mut Solution) -> bool {
    match pat {
        NamedNodePattern::NamedNode(n) => &actual == n,
        NamedNodePattern::Variable(v) => bind_var(v.as_str(), &Term::NamedNode(actual), sol),
    }
}

fn bind_var(name: &str, actual: &Term, sol: &mut Solution) -> bool {
    match sol.get(name) {
        Some(existing) => existing == actual,
        None => {
            sol.insert(name.to_string(), actual.clone());
            true
        }
    }
}

fn merge(a: &Solution, b: &Solution) -> Option<Solution> {
    let mut out = a.clone();
    for (k, v) in b {
        match out.get(k) {
            Some(existing) if existing != v => return None,
            _ => {
                out.insert(k.clone(), v.clone());
            }
        }
    }
    Some(out)
}

fn hash_join(l: &[Solution], r: &[Solution]) -> Vec<Solution> {
    let mut out = Vec::new();
    for a in l {
        for b in r {
            if let Some(m) = merge(a, b) {
                out.push(m);
            }
        }
    }
    out
}

// ── Expression evaluation ────────────────────────────────────────────────────

fn eval_filter(ctx: &Ctx, expr: &Expression, sol: &Solution) -> Result<bool, String> {
    Ok(ebv(eval_term(ctx, expr, sol)?.as_ref()))
}

/// Evaluate an expression to a bound term. `Ok(None)` = unbound / a type error
/// for THIS operand (e.g. `lang()` of a non-literal) — propagated, never a hard
/// `Err`, exactly like a real SPARQL processor's per-solution type errors. `Err`
/// is reserved for a construct this evaluator does not implement at all.
fn eval_term(ctx: &Ctx, e: &Expression, sol: &Solution) -> Result<Option<Term>, String> {
    match e {
        Expression::NamedNode(n) => Ok(Some(Term::NamedNode(n.clone()))),
        Expression::Literal(l) => Ok(Some(Term::Literal(l.clone()))),
        Expression::Variable(v) => Ok(sol.get(v.as_str()).cloned()),
        Expression::Bound(v) => Ok(Some(bool_term(sol.contains_key(v.as_str())))),
        Expression::Not(a) => Ok(Some(bool_term(!ebv(eval_term(ctx, a, sol)?.as_ref())))),
        Expression::And(a, b) => Ok(Some(bool_term(
            ebv(eval_term(ctx, a, sol)?.as_ref()) && ebv(eval_term(ctx, b, sol)?.as_ref()),
        ))),
        Expression::Or(a, b) => Ok(Some(bool_term(
            ebv(eval_term(ctx, a, sol)?.as_ref()) || ebv(eval_term(ctx, b, sol)?.as_ref()),
        ))),
        Expression::Equal(a, b) => {
            let (x, y) = (eval_term(ctx, a, sol)?, eval_term(ctx, b, sol)?);
            Ok(Some(bool_term(term_eq(&x, &y))))
        }
        Expression::SameTerm(a, b) => {
            let (x, y) = (eval_term(ctx, a, sol)?, eval_term(ctx, b, sol)?);
            Ok(Some(bool_term(x == y)))
        }
        Expression::Greater(a, b) => cmp_expr(ctx, a, b, sol, |o| o == Ordering::Greater),
        Expression::GreaterOrEqual(a, b) => cmp_expr(ctx, a, b, sol, |o| o != Ordering::Less),
        Expression::Less(a, b) => cmp_expr(ctx, a, b, sol, |o| o == Ordering::Less),
        Expression::LessOrEqual(a, b) => cmp_expr(ctx, a, b, sol, |o| o != Ordering::Greater),
        Expression::In(a, list) => {
            let av = eval_term(ctx, a, sol)?;
            let mut any = false;
            for item in list {
                if term_eq(&av, &eval_term(ctx, item, sol)?) {
                    any = true;
                    break;
                }
            }
            Ok(Some(bool_term(any)))
        }
        Expression::FunctionCall(func, args) => eval_function(ctx, func, args, sol),
        Expression::Exists(_) => Err("sh:sparql: EXISTS / NOT EXISTS is not supported".to_string()),
        Expression::If(_, _, _) => Err("sh:sparql: IF is not supported".to_string()),
        Expression::Coalesce(_) => Err("sh:sparql: COALESCE is not supported".to_string()),
        Expression::Add(_, _)
        | Expression::Subtract(_, _)
        | Expression::Multiply(_, _)
        | Expression::Divide(_, _)
        | Expression::UnaryPlus(_)
        | Expression::UnaryMinus(_) => Err("sh:sparql: arithmetic is not supported".to_string()),
    }
}

fn cmp_expr(
    ctx: &Ctx,
    a: &Expression,
    b: &Expression,
    sol: &Solution,
    ok: impl Fn(Ordering) -> bool,
) -> Result<Option<Term>, String> {
    let (x, y) = (eval_term(ctx, a, sol)?, eval_term(ctx, b, sol)?);
    let ord = match (
        x.as_ref().and_then(literal_lexical),
        y.as_ref().and_then(literal_lexical),
    ) {
        (Some(lx), Some(ly)) => match (lx.trim().parse::<f64>(), ly.trim().parse::<f64>()) {
            (Ok(nx), Ok(ny)) => nx.partial_cmp(&ny),
            _ => Some(lx.cmp(&ly)),
        },
        _ => None,
    };
    Ok(Some(bool_term(ord.is_some_and(ok))))
}

fn eval_function(
    ctx: &Ctx,
    func: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Result<Option<Term>, String> {
    let arg = |i: usize| -> Result<Option<Term>, String> {
        match args.get(i) {
            Some(e) => eval_term(ctx, e, sol),
            None => Ok(None),
        }
    };
    match func {
        Function::IsIri => Ok(Some(bool_term(matches!(arg(0)?, Some(Term::NamedNode(_)))))),
        Function::IsBlank => Ok(Some(bool_term(matches!(arg(0)?, Some(Term::BlankNode(_)))))),
        Function::IsLiteral => Ok(Some(bool_term(matches!(arg(0)?, Some(Term::Literal(_)))))),
        Function::IsNumeric => Ok(Some(bool_term(
            arg(0)?.as_ref().and_then(numeric_of).is_some(),
        ))),
        Function::Str => Ok(Some(Term::Literal(Literal::new_simple_literal(
            lexical_of(arg(0)?.as_ref()),
        )))),
        Function::Lang => Ok(Some(Term::Literal(Literal::new_simple_literal(
            match arg(0)? {
                Some(Term::Literal(l)) => l.language().unwrap_or("").to_string(),
                _ => String::new(),
            },
        )))),
        Function::Datatype => Ok(match arg(0)? {
            Some(Term::Literal(l)) => Some(Term::NamedNode(l.datatype().into_owned())),
            _ => None,
        }),
        Function::LangMatches => {
            let tag = lexical_of(arg(0)?.as_ref());
            let range = lexical_of(arg(1)?.as_ref());
            Ok(Some(bool_term(lang_range_matches(&range, &tag))))
        }
        Function::Regex => {
            let s = lexical_of(arg(0)?.as_ref());
            let pat = lexical_of(arg(1)?.as_ref());
            let flags = if args.len() > 2 {
                Some(lexical_of(arg(2)?.as_ref()))
            } else {
                None
            };
            Ok(Some(bool_term(pattern_ok(&s, &pat, flags.as_deref()))))
        }
        Function::Contains => str_bool(arg(0)?, arg(1)?, |s, n| s.contains(n)),
        Function::StrStarts => str_bool(arg(0)?, arg(1)?, |s, n| s.starts_with(n)),
        Function::StrEnds => str_bool(arg(0)?, arg(1)?, |s, n| s.ends_with(n)),
        Function::UCase => Ok(Some(Term::Literal(Literal::new_simple_literal(
            lexical_of(arg(0)?.as_ref()).to_uppercase(),
        )))),
        Function::LCase => Ok(Some(Term::Literal(Literal::new_simple_literal(
            lexical_of(arg(0)?.as_ref()).to_lowercase(),
        )))),
        Function::StrLen => Ok(Some(int_term(lexical_of(arg(0)?.as_ref()).chars().count()))),
        other => Err(format!("sh:sparql: unsupported SPARQL function {other:?}")),
    }
}

/// An `xsd:integer`-typed result literal (`STRLEN` is the one numeric-result
/// function in the supported set — everything else here returns a boolean or a
/// string).
fn int_term(n: usize) -> Term {
    Term::Literal(Literal::new_typed_literal(
        n.to_string(),
        NamedNode::new_unchecked(crate::vocab::XSD_INTEGER),
    ))
}

fn str_bool(
    a: Option<Term>,
    b: Option<Term>,
    f: impl Fn(&str, &str) -> bool,
) -> Result<Option<Term>, String> {
    Ok(Some(bool_term(f(
        &lexical_of(a.as_ref()),
        &lexical_of(b.as_ref()),
    ))))
}

// ── Term helpers ──────────────────────────────────────────────────────────

fn bool_term(b: bool) -> Term {
    Term::Literal(Literal::new_typed_literal(
        if b { "true" } else { "false" },
        NamedNode::new_unchecked(crate::vocab::XSD_BOOLEAN),
    ))
}

/// Effective boolean value (SPARQL 1.1 §17.2.2): a boolean literal by its lexical
/// value; a numeric literal by non-zero; a simple (`xsd:string`) or
/// language-tagged (`rdf:langString`) literal by non-empty; anything else (IRI,
/// blank node, other typed literal, or genuinely unbound) is a type error,
/// folded to `false`.
fn ebv(t: Option<&Term>) -> bool {
    match t {
        Some(Term::Literal(l)) => {
            let dt = l.datatype().as_str();
            if dt == crate::vocab::XSD_BOOLEAN {
                l.value() == "true" || l.value() == "1"
            } else if dt == crate::vocab::XSD_STRING || dt == crate::vocab::RDF_LANG_STRING {
                !l.value().is_empty()
            } else if let Some(n) = numeric_of(&Term::Literal(l.clone())) {
                n != 0.0 && !n.is_nan()
            } else {
                false
            }
        }
        _ => false,
    }
}

/// A literal's lexical form as `f64`, else `None` (non-literal, or does not
/// parse). Datatype-agnostic on purpose — the constraints this evaluator
/// supports never need to distinguish `xsd:integer` from `xsd:double`.
fn numeric_of(t: &Term) -> Option<f64> {
    match t {
        Term::Literal(l) => l.value().trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// A literal's lexical value; `None` for any non-literal (an IRI's string form
/// goes through [`lexical_of`], not this — comparisons only compare literals).
fn literal_lexical(t: &Term) -> Option<String> {
    match t {
        Term::Literal(l) => Some(l.value().to_string()),
        _ => None,
    }
}

/// `STR()`-style lexical rendering: a literal's value, an IRI's bare string form,
/// or an empty string for a blank node / genuinely unbound operand.
fn lexical_of(t: Option<&Term>) -> String {
    match t {
        Some(Term::Literal(l)) => l.value().to_string(),
        Some(Term::NamedNode(n)) => n.as_str().to_string(),
        _ => String::new(),
    }
}

/// SPARQL `=` (RDFterm-equal, widened with the same numeric-value coercion the
/// SHACL Core range constraints already use — see `crate::validate::cmp_value`):
/// exact term equality first, then a numeric fallback for two literals.
fn term_eq(a: &Option<Term>, b: &Option<Term>) -> bool {
    let (Some(x), Some(y)) = (a, b) else {
        return false;
    };
    if x == y {
        return true;
    }
    matches!((literal_lexical(x), literal_lexical(y)), (Some(lx), Some(ly))
        if matches!((lx.trim().parse::<f64>(), ly.trim().parse::<f64>()), (Ok(nx), Ok(ny)) if nx == ny))
}

/// Basic language-range match (RFC 4647 §3.3.1 "basic filtering", the
/// `langMatches()` builtin): `en` matches `en`/`en-US`; `*` matches any
/// non-empty tag. Mirrors `crate::validate::lang_range_matches`.
fn lang_range_matches(range: &str, tag: &str) -> bool {
    if range == "*" {
        return !tag.is_empty();
    }
    let range = range.to_ascii_lowercase();
    let tag = tag.to_ascii_lowercase();
    tag == range || tag.strip_prefix(&range).is_some_and(|r| r.starts_with('-'))
}

fn pattern_ok(s: &str, pattern: &str, flags: Option<&str>) -> bool {
    let mut b = RegexBuilder::new(pattern);
    if let Some(f) = flags {
        b.case_insensitive(f.contains('i'));
        b.multi_line(f.contains('m'));
        b.dot_matches_new_line(f.contains('s'));
        b.ignore_whitespace(f.contains('x'));
    }
    match b.build() {
        Ok(re) => re.is_match(s),
        Err(_) => false,
    }
}
