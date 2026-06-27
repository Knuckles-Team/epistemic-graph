//! W2 — Native SPARQL 1.1 evaluation over a `GraphView` (CONCEPT:KG-2.218).
//!
//! DECISION (embed-vs-compile, from the spike): we COMPILE `spargebra`'s parsed
//! algebra down to scans over OUR property-graph `GraphView`, rather than EMBED
//! oxigraph's evaluator over an adapter. Why this side:
//!   * spargebra is parser+algebra ONLY (no store, no async, tiny dep) — it gives
//!     the SPARQL 1.1 grammar + the typed `GraphPattern` algebra for free.
//!   * The evaluator walks that algebra resolving each triple pattern against the
//!     SAME `node_properties` / `edge_properties` / topology the eg-plan executor
//!     reads. So a SPARQL BGP is literally "more scans over the one substrate" — no
//!     second copy of the graph, no oxigraph store to keep in sync.
//!
//! Increment-1 algebra coverage: BGP (triple-pattern match + join on shared vars),
//! FILTER (Bound + comparison + And/Or/Not), OPTIONAL (left-join), UNION, JOIN,
//! PROJECT, DISTINCT, SLICE, and a BASIC fixed-length property path (`p1/p2` seq
//! and a single predicate).
//!
//! Completeness increment (CONCEPT:KG-2.235): aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/
//! `MAX` with `GROUP BY` — the `Group`+`Extend` algebra), the fuller property paths
//! (`p+` / `p*` / `p?`, alternative `a|b`, inverse `^p`, and their nesting), and the
//! `GRAPH ?g { … }` named-graph form (a single dataset here ⇒ `?g` binds the request
//! graph). Sub-SELECT / SERVICE stay deferred (see the findings "SPARQL completeness").
//!
//! Performance note (carried from the spike): this evaluator does a full scan per
//! triple pattern + a materialized join. That is the documented naive-evaluator gap
//! — an SPO/POS index + selectivity join-ordering is W2 follow-on, not a substrate
//! limitation.

use std::collections::HashMap;

use eg_core::graph::GraphView;
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, PropertyPathExpression,
};
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};
use spargebra::Query;

/// One solution: variable name → bound term (in our node-id / literal lexical form).
pub type Solution = HashMap<String, Binding>;

/// A bound value: a graph-node id (`<iri>` / `_:b`) or a literal lexical value.
#[derive(Clone, Debug, PartialEq)]
pub enum Binding {
    Node(String),
    Literal(String),
}

impl Binding {
    pub fn as_str(&self) -> &str {
        match self {
            Binding::Node(s) | Binding::Literal(s) => s,
        }
    }
    pub fn is_node(&self) -> bool {
        matches!(self, Binding::Node(_))
    }
}

/// LPG→RDF projection vocabulary (CONCEPT:KG-2.240). Controls how the live property
/// graph is projected into RDF terms during SPARQL evaluation. The engine stays
/// GENERAL — it hardcodes NO ontology URL; the namespace + class-naming convention
/// are supplied by the caller (e.g. agent-utilities passes its `au:` namespace +
/// CamelCase so the engine projection matches its rdflib materialization).
///
/// [`Projection::raw`] (the default) is the IDENTITY projection: node-type and
/// property keys are emitted verbatim and `rdf:type` is NOT synthesized from the node
/// `type` field (it comes only from explicit typing edges, the prior behavior). When
/// a `base_iri` is set, native LPG keys are projected under it; with `camel_type` the
/// `rdf:type` object local name is CamelCased.
#[derive(Clone, Debug, Default)]
pub struct Projection {
    /// Base namespace IRI for projected node/property/type local names. `None` ⇒
    /// identity (the key is already a complete term, e.g. an RDF-loaded `<iri>`).
    pub base_iri: Option<String>,
    /// CamelCase the `rdf:type` object local name (matches AU's `.title()` mapping).
    pub camel_type: bool,
}

/// The `rdf:type` predicate IRI (bare, no angle brackets — predicates compare bare).
const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

impl Projection {
    /// The identity projection (verbatim keys, no `rdf:type` synthesis).
    pub fn raw() -> Self {
        Self::default()
    }

    /// Build from the wire `(base_iri, type_convention)` pair. An empty `base_iri`
    /// ⇒ identity. `type_convention == "camel"` ⇒ CamelCase the `rdf:type` object.
    pub fn from_wire(base_iri: &str, type_convention: &str) -> Self {
        if base_iri.is_empty() {
            return Self::raw();
        }
        Self {
            base_iri: Some(base_iri.to_string()),
            camel_type: type_convention.eq_ignore_ascii_case("camel"),
        }
    }

    /// Project a graph node KEY to its subject/object binding string. Identity returns
    /// the key verbatim (RDF-loaded ids are already `<iri>`). Namespaced wraps a BARE
    /// local id as `<base + id_with_spaces_underscored>` (matching AU's `_uri`), but
    /// passes through a key that is already a term (`<iri>` / `_:bnode`).
    fn node_iri(&self, key: &str) -> String {
        match &self.base_iri {
            None => key.to_string(),
            Some(base) => {
                if key.starts_with('<') || key.starts_with("_:") {
                    key.to_string()
                } else {
                    format!("<{}{}>", base, key.replace(' ', "_"))
                }
            }
        }
    }

    /// Project a property / edge-relation KEY to its predicate IRI (bare — predicates
    /// compare without angle brackets). Identity returns it verbatim; namespaced
    /// prefixes a BARE key but passes through one that is already an IRI.
    fn pred_iri(&self, key: &str) -> String {
        match &self.base_iri {
            None => key.to_string(),
            Some(base) => {
                if key.contains("://") || key.starts_with("urn:") {
                    key.to_string()
                } else {
                    format!("{base}{key}")
                }
            }
        }
    }

    /// Project the `rdf:type` OBJECT (the node `type` value) to its class binding
    /// string. `None` in identity mode (no synthesis — `rdf:type` is edge-sourced).
    /// Namespaced: `<base + CamelCase(type)>` when `camel_type`, else the verbatim
    /// (space-underscored) local name.
    fn type_object_iri(&self, ty: &str) -> Option<String> {
        let base = self.base_iri.as_ref()?;
        let local = if self.camel_type {
            camel_case(ty)
        } else {
            ty.replace(' ', "_")
        };
        Some(format!("<{base}{local}>"))
    }
}

/// CamelCase a local name to mirror AU's `s.replace(" ", "_").title().replace("_", "")`
/// (e.g. `agent` → `Agent`, `world_model` → `WorldModel`). A letter is uppercased when
/// it follows a non-letter (word start), else lowercased; non-letters are kept then the
/// underscores are removed.
fn camel_case(s: &str) -> String {
    let pre = s.replace(' ', "_");
    let mut out = String::with_capacity(pre.len());
    let mut prev_is_alpha = false;
    for c in pre.chars() {
        if c.is_alphabetic() {
            if prev_is_alpha {
                out.extend(c.to_lowercase());
            } else {
                out.extend(c.to_uppercase());
            }
            prev_is_alpha = true;
        } else {
            out.push(c);
            prev_is_alpha = false;
        }
    }
    out.replace('_', "")
}

/// Extract the lexical value of a node property cell. A typed RDF cell is the JSON
/// object `{value, datatype, lang}` (the `AddTriples` shape); a native-LPG scalar is a
/// bare string / number / bool. Arrays / objects-without-`value` / null ⇒ `None`.
fn cell_lexical(cell: &serde_json::Value) -> Option<String> {
    match cell {
        serde_json::Value::Object(m) => m.get("value").and_then(|v| v.as_str()).map(String::from),
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// A materialized SELECT result: the projected variable order + the solution rows.
#[derive(Debug, Clone)]
pub struct SparqlResult {
    pub vars: Vec<String>,
    pub solutions: Vec<Solution>,
}

impl SparqlResult {
    /// Project to a wire-friendly row table: `vars` columns, each row a
    /// `Vec<Option<String>>` aligned to `vars` (None = unbound). Lets the protocol
    /// return a flat `{columns, rows}` shape matching `Sql`/`Cypher`.
    pub fn to_rows(&self) -> (Vec<String>, Vec<Vec<Option<String>>>) {
        let rows = self
            .solutions
            .iter()
            .map(|s| {
                self.vars
                    .iter()
                    .map(|v| s.get(v).map(|b| b.as_str().to_string()))
                    .collect()
            })
            .collect();
        (self.vars.clone(), rows)
    }
}

/// Parse a SPARQL 1.1 query string into the spargebra algebra.
pub fn parse_query(q: &str) -> Result<Query, String> {
    Query::parse(q, None).map_err(|e| format!("sparql parse: {e}"))
}

/// The bare IRI bound to `?g` (as `<…>`) for a `GRAPH ?g {}` over the single default
/// dataset — the back-compat name a [`Dataset::single`] registers its view under.
pub const DEFAULT_GRAPH_NAME: &str = "urn:eg:graph:default";

/// An RDF dataset over live property-graph views (CONCEPT:EG-017 — true named-graph
/// semantics): a DEFAULT graph plus zero-or-more NAMED graphs, each a `GraphView`.
/// `GRAPH <g> { … }` evaluates against the matching named member (empty if absent);
/// `GRAPH ?g { … }` ranges over the named members binding `?g` to each — instead of
/// collapsing every named-graph form onto the single default graph (the prior behavior).
pub struct Dataset<'a> {
    default: &'a GraphView,
    /// `(bare graph IRI, that graph's view)` — the named graphs of the dataset.
    named: Vec<(String, &'a GraphView)>,
}

impl<'a> Dataset<'a> {
    /// A single-graph dataset: `view` is the default graph AND is exposed as ONE named
    /// graph under [`DEFAULT_GRAPH_NAME`], so a `GRAPH ?g { }` still resolves (`?g`
    /// binds the default name) — the back-compatible single-dataset behavior.
    pub fn single(view: &'a GraphView) -> Self {
        Self {
            default: view,
            named: vec![(DEFAULT_GRAPH_NAME.to_string(), view)],
        }
    }

    /// A multi-graph dataset. `default` is the default graph; `named` is the set of
    /// named graphs keyed by their BARE graph IRI (no angle brackets).
    pub fn new(default: &'a GraphView, named: Vec<(String, &'a GraphView)>) -> Self {
        Self { default, named }
    }

    fn named_view(&self, iri: &str) -> Option<&'a GraphView> {
        self.named
            .iter()
            .find(|(n, _)| n == iri)
            .map(|(_, v)| *v)
    }
}

/// The active evaluation context: the dataset, the graph the current scans resolve
/// against (the default, or a `GRAPH`-scoped named graph), and the LPG→RDF projection.
struct Ctx<'a> {
    ds: &'a Dataset<'a>,
    active: &'a GraphView,
    proj: &'a Projection,
}

impl<'a> Ctx<'a> {
    /// Re-scope the context to a different active graph (entering a `GRAPH` block).
    fn with_active(&self, active: &'a GraphView) -> Ctx<'a> {
        Ctx {
            ds: self.ds,
            active,
            proj: self.proj,
        }
    }
}

/// The outcome of evaluating ANY SPARQL query form (CONCEPT:EG-017).
#[derive(Debug, Clone)]
pub enum QueryOutcome {
    /// `SELECT` — a solution table.
    Solutions(SparqlResult),
    /// `ASK` — a boolean (`true` iff the pattern has ≥1 solution).
    Boolean(bool),
    /// `CONSTRUCT` / `DESCRIBE` — an RDF graph (a set of triples).
    #[cfg(feature = "rdf")]
    Graph(Vec<oxrdf::Triple>),
}

/// Parse + evaluate a SPARQL SELECT over the GraphView in one call, using the IDENTITY
/// LPG→RDF projection (verbatim keys). See [`run_projected`] to supply a vocabulary.
pub fn run(view: &GraphView, query_str: &str) -> Result<SparqlResult, String> {
    run_projected(view, query_str, &Projection::raw())
}

/// Parse + evaluate a SPARQL SELECT, projecting the live property graph into RDF terms
/// under `proj` (CONCEPT:KG-2.240). With [`Projection::raw`] this equals [`run`].
/// Non-SELECT forms are coerced to a row table (see [`run_outcome`] for the typed form).
pub fn run_projected(
    view: &GraphView,
    query_str: &str,
    proj: &Projection,
) -> Result<SparqlResult, String> {
    Ok(outcome_to_result(run_outcome(view, query_str, proj)?))
}

/// Parse + evaluate a SPARQL query of ANY form (SELECT/ASK/CONSTRUCT/DESCRIBE) over a
/// single GraphView, returning the typed [`QueryOutcome`] (CONCEPT:EG-017).
pub fn run_outcome(
    view: &GraphView,
    query_str: &str,
    proj: &Projection,
) -> Result<QueryOutcome, String> {
    let q = parse_query(query_str)?;
    let ds = Dataset::single(view);
    evaluate_outcome(&ds, &q, proj)
}

/// Parse + evaluate a SPARQL query over a multi-graph [`Dataset`] (named-graph aware).
pub fn run_outcome_dataset(
    ds: &Dataset,
    query_str: &str,
    proj: &Projection,
) -> Result<QueryOutcome, String> {
    let q = parse_query(query_str)?;
    evaluate_outcome(ds, &q, proj)
}

/// Evaluate a parsed SELECT query over the GraphView (back-compat SELECT-only API).
pub fn evaluate(
    view: &GraphView,
    query: &Query,
    proj: &Projection,
) -> Result<SparqlResult, String> {
    let ds = Dataset::single(view);
    match evaluate_outcome(&ds, query, proj)? {
        QueryOutcome::Solutions(r) => Ok(r),
        other => Ok(outcome_to_result(other)),
    }
}

/// Evaluate a parsed query of ANY form over a [`Dataset`] under projection `proj`.
///
/// * `SELECT`    → the projected solution table.
/// * `ASK`       → `true` iff the WHERE pattern yields ≥1 solution.
/// * `CONSTRUCT` → the WHERE solutions instantiated against the template triples.
/// * `DESCRIBE`  → the triples describing each bound resource (subject- AND
///   object-position — a minimal concise bounded description over the active graph).
pub fn evaluate_outcome(
    ds: &Dataset,
    query: &Query,
    proj: &Projection,
) -> Result<QueryOutcome, String> {
    let ctx = Ctx {
        ds,
        active: ds.default,
        proj,
    };
    match query {
        Query::Select { pattern, .. } => {
            let solutions = eval_pattern(&ctx, pattern)?;
            let vars = collect_vars(pattern);
            Ok(QueryOutcome::Solutions(SparqlResult { vars, solutions }))
        }
        Query::Ask { pattern, .. } => {
            let solutions = eval_pattern(&ctx, pattern)?;
            Ok(QueryOutcome::Boolean(!solutions.is_empty()))
        }
        #[cfg(feature = "rdf")]
        Query::Construct {
            template, pattern, ..
        } => {
            let solutions = eval_pattern(&ctx, pattern)?;
            Ok(QueryOutcome::Graph(construct_graph(template, &solutions)))
        }
        #[cfg(feature = "rdf")]
        Query::Describe { pattern, .. } => {
            let solutions = eval_pattern(&ctx, pattern)?;
            let vars = collect_vars(pattern);
            Ok(QueryOutcome::Graph(describe_resources(&ctx, &vars, &solutions)))
        }
        #[cfg(not(feature = "rdf"))]
        _ => Err("eg-rdf SPARQL: CONSTRUCT/DESCRIBE need the `rdf` feature".into()),
    }
}

/// Evaluate just a WHERE graph pattern over a dataset → its raw solutions. Used by the
/// SPARQL UPDATE executor (`DELETE/INSERT … WHERE`) and DESCRIBE.
pub fn eval_where(
    ds: &Dataset,
    pattern: &GraphPattern,
    proj: &Projection,
) -> Result<Vec<Solution>, String> {
    let ctx = Ctx {
        ds,
        active: ds.default,
        proj,
    };
    eval_pattern(&ctx, pattern)
}

/// Coerce any [`QueryOutcome`] to the flat wire [`SparqlResult`] row table: SELECT is
/// passed through; ASK becomes a one-cell `?ask` table; a CONSTRUCT/DESCRIBE graph
/// becomes an `?subject ?predicate ?object` table (N-Triples term lex:).
fn outcome_to_result(outcome: QueryOutcome) -> SparqlResult {
    match outcome {
        QueryOutcome::Solutions(r) => r,
        QueryOutcome::Boolean(b) => {
            let mut sol = Solution::new();
            sol.insert("ask".to_string(), Binding::Literal(b.to_string()));
            SparqlResult {
                vars: vec!["ask".to_string()],
                solutions: vec![sol],
            }
        }
        #[cfg(feature = "rdf")]
        QueryOutcome::Graph(triples) => {
            let vars = vec![
                "subject".to_string(),
                "predicate".to_string(),
                "object".to_string(),
            ];
            let solutions = triples
                .iter()
                .map(|t| {
                    let mut sol = Solution::new();
                    sol.insert("subject".to_string(), Binding::Node(t.subject.to_string()));
                    sol.insert(
                        "predicate".to_string(),
                        Binding::Node(t.predicate.to_string()),
                    );
                    sol.insert("object".to_string(), Binding::Literal(t.object.to_string()));
                    sol
                })
                .collect();
            SparqlResult { vars, solutions }
        }
    }
}

// ── CONSTRUCT / DESCRIBE (CONCEPT:EG-017) ───────────────────────────────────────

/// Instantiate a CONSTRUCT template against each WHERE solution → the result graph.
/// A pattern whose terms can't all be resolved/built for a given solution is skipped
/// (SPARQL: an unbound or ill-typed template slot yields no triple for that solution).
#[cfg(feature = "rdf")]
fn construct_graph(template: &[TriplePattern], solutions: &[Solution]) -> Vec<oxrdf::Triple> {
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
fn instantiate_triple(tp: &TriplePattern, sol: &Solution) -> Option<oxrdf::Triple> {
    use oxrdf::{NamedNode, Subject, Term, Triple};

    let subject: Subject = match &tp.subject {
        TermPattern::NamedNode(n) => Subject::NamedNode(n.clone()),
        TermPattern::BlankNode(b) => Subject::BlankNode(b.clone()),
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
fn describe_resources(ctx: &Ctx, vars: &[String], solutions: &[Solution]) -> Vec<oxrdf::Triple> {
    // The resource set: every binding across the projected vars whose value is a
    // resource term (`<iri>` / `_:b`). A `DESCRIBE <iri>` constant arrives as an
    // `Extend`-bound LITERAL lexically equal to `<iri>`, while a `DESCRIBE ?x` arrives
    // as a `Node` binding — both are captured by the term-form check.
    let mut resources: std::collections::HashSet<String> = std::collections::HashSet::new();
    for sol in solutions {
        for v in vars {
            if let Some(b) = sol.get(v) {
                let s = b.as_str();
                if s.starts_with('<') || s.starts_with("_:") {
                    resources.insert(s.to_string());
                }
            }
        }
    }
    // All triples of the active graph (term-string form), filtered to those touching a
    // described resource in subject or object position.
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    for (s, p, o, o_is_node) in all_triples_terms(ctx) {
        if !(resources.contains(&s) || (o_is_node && resources.contains(&o))) {
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

/// Enumerate the active graph as `(subject, predicate, object, object_is_node)` projected
/// term strings — the same projection the `?s ?p ?o` BGP scan produces.
#[cfg(feature = "rdf")]
fn all_triples_terms(ctx: &Ctx) -> Vec<(String, String, String, bool)> {
    let view = ctx.active;
    let proj = ctx.proj;
    let mut out = Vec::new();
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
                if let Some(rel) = v.get("type").and_then(|x| x.as_str()) {
                    out.push((proj.node_iri(s), proj.pred_iri(rel), proj.node_iri(o), true));
                }
            }
        }
    }
    for (id, blob) in &view.node_properties {
        let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) else {
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
        for (k, cell) in obj {
            if k == "type" || k == "node_type" {
                continue;
            }
            if let Some(lit_val) = cell_lexical(cell) {
                out.push((subj_iri.clone(), proj.pred_iri(k), lit_val, false));
            }
        }
    }
    out
}

/// Parse a projected node-id string (`<iri>` / `_:b`) to an RDF subject; `None` else.
#[cfg(feature = "rdf")]
fn node_str_to_subject(id: &str) -> Option<oxrdf::Subject> {
    use oxrdf::{BlankNode, NamedNode, Subject};
    if let Some(iri) = id.strip_prefix('<').and_then(|s| s.strip_suffix('>')) {
        Some(Subject::NamedNode(NamedNode::new(iri).ok()?))
    } else if let Some(b) = id.strip_prefix("_:") {
        Some(Subject::BlankNode(BlankNode::new(b).ok()?))
    } else {
        None
    }
}

/// Strip surrounding `<…>` from a node-iri term string for `NamedNode::new`.
#[cfg(feature = "rdf")]
fn strip_iri(s: &str) -> &str {
    s.strip_prefix('<')
        .and_then(|x| x.strip_suffix('>'))
        .unwrap_or(s)
}

/// A solution binding → an RDF object term. A node binding parses as a resource (or a
/// simple literal if it is not a term form); a literal binding is a simple literal.
#[cfg(feature = "rdf")]
fn binding_to_term(b: &Binding) -> oxrdf::Term {
    use oxrdf::{Literal, Term};
    match b {
        Binding::Node(id) => match node_str_to_subject(id) {
            Some(oxrdf::Subject::NamedNode(n)) => Term::NamedNode(n),
            Some(oxrdf::Subject::BlankNode(bn)) => Term::BlankNode(bn),
            _ => Term::Literal(Literal::new_simple_literal(b.as_str())),
        },
        Binding::Literal(v) => Term::Literal(Literal::new_simple_literal(v)),
    }
}

/// Build an RDF triple from projected term strings (`object_is_node` distinguishes a
/// resource object from a literal object).
#[cfg(feature = "rdf")]
fn build_triple(s: &str, p: &str, o: &str, object_is_node: bool) -> Option<oxrdf::Triple> {
    use oxrdf::{Literal, NamedNode, Term, Triple};
    let subj = node_str_to_subject(s)?;
    let pred = NamedNode::new(strip_iri(p)).ok()?;
    let obj = if object_is_node {
        match node_str_to_subject(o)? {
            oxrdf::Subject::NamedNode(n) => Term::NamedNode(n),
            oxrdf::Subject::BlankNode(b) => Term::BlankNode(b),
        }
    } else {
        Term::Literal(Literal::new_simple_literal(o))
    };
    Some(Triple::new(subj, pred, obj))
}

fn collect_vars(p: &GraphPattern) -> Vec<String> {
    let mut vs = Vec::new();
    p.on_in_scope_variable(|v| {
        let n = v.as_str().to_string();
        if !vs.contains(&n) {
            vs.push(n);
        }
    });
    vs
}

/// The algebra walker.
fn eval_pattern(ctx: &Ctx, p: &GraphPattern) -> Result<Vec<Solution>, String> {
    match p {
        GraphPattern::Bgp { patterns } => Ok(eval_bgp(ctx, patterns)),
        GraphPattern::Path {
            subject,
            path,
            object,
        } => eval_path(ctx, subject, path, object),
        GraphPattern::Filter { expr, inner } => {
            let inner_sols = eval_pattern(ctx, inner)?;
            Ok(inner_sols
                .into_iter()
                .filter(|s| eval_filter(expr, s))
                .collect())
        }
        GraphPattern::Join { left, right } => {
            let l = eval_pattern(ctx, left)?;
            let r = eval_pattern(ctx, right)?;
            Ok(hash_join(&l, &r))
        }
        GraphPattern::LeftJoin {
            left,
            right,
            expression,
        } => {
            // OPTIONAL: keep every left solution; extend with a compatible right
            // (passing the optional FILTER) where one exists.
            let l = eval_pattern(ctx, left)?;
            let r = eval_pattern(ctx, right)?;
            Ok(left_join(&l, &r, expression.as_ref()))
        }
        GraphPattern::Union { left, right } => {
            let mut l = eval_pattern(ctx, left)?;
            let mut r = eval_pattern(ctx, right)?;
            l.append(&mut r);
            Ok(l)
        }
        GraphPattern::Project { inner, .. } => eval_pattern(ctx, inner),
        // GROUP BY + aggregates (CONCEPT:KG-2.235). `Group` produces one solution per
        // group binding the GROUP BY vars + the aggregate-result vars; the wrapping
        // `Extend` (below) re-binds those to the projected names. With no GROUP BY var
        // the whole result is one group (`SELECT (COUNT(*) AS ?n) …`).
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let rows = eval_pattern(ctx, inner)?;
            Ok(eval_group(rows, variables, aggregates))
        }
        // BIND / the aggregate-projection rename. `Extend` binds `variable` to the
        // value of `expression` in each solution. We evaluate the (already-aggregated
        // or scalar) expression and bind it; an unevaluable expression leaves it
        // unbound (SPARQL: an error in Extend yields no binding for that var).
        GraphPattern::Extend {
            inner,
            variable,
            expression,
        } => {
            let rows = eval_pattern(ctx, inner)?;
            Ok(rows
                .into_iter()
                .map(|mut s| {
                    if let Some(val) = expr_str(expression, &s) {
                        s.insert(variable.as_str().to_string(), Binding::Literal(val));
                    }
                    s
                })
                .collect())
        }
        // GRAPH … { … } — true named-graph scoping (CONCEPT:EG-017). A constant graph
        // IRI re-scopes evaluation to THAT named graph (empty if it is not in the
        // dataset). A variable `?g` ranges over EVERY named graph, evaluating the inner
        // pattern against each and binding `?g` to its IRI (the union). This replaces
        // the prior single-dataset collapse onto `DEFAULT_GRAPH_IRI`.
        GraphPattern::Graph { name, inner } => match name {
            NamedNodePattern::NamedNode(n) => match ctx.ds.named_view(n.as_str()) {
                Some(v) => eval_pattern(&ctx.with_active(v), inner),
                None => Ok(Vec::new()),
            },
            NamedNodePattern::Variable(v) => {
                let mut out = Vec::new();
                for (gname, gview) in &ctx.ds.named {
                    let binding = Binding::Node(format!("<{gname}>"));
                    for mut s in eval_pattern(&ctx.with_active(gview), inner)? {
                        match s.get(v.as_str()) {
                            Some(existing) if *existing != binding => continue,
                            _ => {
                                s.insert(v.as_str().to_string(), binding.clone());
                            }
                        }
                        out.push(s);
                    }
                }
                Ok(out)
            }
        },
        GraphPattern::Distinct { inner } => {
            let mut seen = std::collections::HashSet::new();
            Ok(eval_pattern(ctx, inner)?
                .into_iter()
                .filter(|s| seen.insert(canonical_solution(s)))
                .collect())
        }
        GraphPattern::Reduced { inner } => eval_pattern(ctx, inner),
        GraphPattern::Slice {
            inner,
            start,
            length,
        } => {
            let all = eval_pattern(ctx, inner)?;
            let end = length.map(|l| start + l).unwrap_or(all.len());
            Ok(all
                .into_iter()
                .skip(*start)
                .take(end.saturating_sub(*start))
                .collect())
        }
        other => Err(format!("eg-rdf SPARQL: unsupported algebra node {other:?}")),
    }
}

fn canonical_solution(s: &Solution) -> String {
    let mut kv: Vec<_> = s
        .iter()
        .map(|(k, v)| (k.clone(), v.as_str().to_string()))
        .collect();
    kv.sort();
    format!("{kv:?}")
}

/// The IRI `?g` binds to in a `GRAPH ?g {}` over the single-graph dataset (the `<…>`
/// wrapping of [`DEFAULT_GRAPH_NAME`]). Used by the named-graph back-compat test.
#[cfg(test)]
const DEFAULT_GRAPH_IRI: &str = "<urn:eg:graph:default>";

// ── GROUP BY + aggregates (CONCEPT:KG-2.235) ────────────────────────────────────

/// Evaluate `GROUP BY group_vars` + the `aggregates` over `rows`. Returns one
/// solution per distinct group-key, binding each group-by var to its value AND each
/// aggregate-result var (the internal name spargebra assigns) to the computed scalar.
/// The wrapping `Extend` re-binds those internal vars to the user's projected names.
fn eval_group(
    rows: Vec<Solution>,
    group_vars: &[spargebra::term::Variable],
    aggregates: &[(spargebra::term::Variable, AggregateExpression)],
) -> Vec<Solution> {
    use std::collections::BTreeMap;

    // Bucket rows by the tuple of group-by values (a stable string key keeps the
    // result deterministic). With no GROUP BY var, ALL rows fall in one "" group.
    let mut groups: BTreeMap<String, Vec<Solution>> = BTreeMap::new();
    for row in rows {
        let key = group_vars
            .iter()
            .map(|v| {
                row.get(v.as_str())
                    .map(|b| b.as_str().to_string())
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\u{1f}");
        groups.entry(key).or_default().push(row);
    }

    let mut out = Vec::new();
    for (_key, members) in groups {
        let mut sol = Solution::new();
        // Carry the group-by var values (taken from the first member of the group).
        if let Some(first) = members.first() {
            for gv in group_vars {
                if let Some(b) = first.get(gv.as_str()) {
                    sol.insert(gv.as_str().to_string(), b.clone());
                }
            }
        }
        // Compute each aggregate over the group's members.
        for (out_var, agg) in aggregates {
            let value = compute_aggregate(agg, &members);
            sol.insert(out_var.as_str().to_string(), Binding::Literal(value));
        }
        out.push(sol);
    }
    out
}

/// Compute ONE aggregate over a group's member solutions, returning its lexical value.
fn compute_aggregate(agg: &AggregateExpression, members: &[Solution]) -> String {
    match agg {
        // COUNT(*) — count solutions (DISTINCT counts distinct whole solutions).
        AggregateExpression::CountSolutions { distinct } => {
            let n = if *distinct {
                let mut seen = std::collections::HashSet::new();
                members
                    .iter()
                    .filter(|s| seen.insert(canonical_solution(s)))
                    .count()
            } else {
                members.len()
            };
            n.to_string()
        }
        AggregateExpression::FunctionCall {
            name,
            expr,
            distinct,
        } => {
            // The per-row values of the aggregated expression (skipping unbound rows).
            let mut vals: Vec<String> = members.iter().filter_map(|s| expr_str(expr, s)).collect();
            if *distinct {
                let mut seen = std::collections::HashSet::new();
                vals.retain(|v| seen.insert(v.clone()));
            }
            agg_over(name, &vals)
        }
    }
}

/// Apply an aggregate function to the already-collected per-row lexical values.
fn agg_over(func: &AggregateFunction, vals: &[String]) -> String {
    let nums: Vec<f64> = vals.iter().filter_map(|v| v.parse::<f64>().ok()).collect();
    match func {
        AggregateFunction::Count => vals.len().to_string(),
        AggregateFunction::Sum => fmt_num(nums.iter().sum::<f64>()),
        AggregateFunction::Avg => {
            if nums.is_empty() {
                "0".to_string()
            } else {
                fmt_num(nums.iter().sum::<f64>() / nums.len() as f64)
            }
        }
        AggregateFunction::Min => {
            if nums.is_empty() {
                vals.iter().min().cloned().unwrap_or_default()
            } else {
                fmt_num(nums.iter().cloned().fold(f64::INFINITY, f64::min))
            }
        }
        AggregateFunction::Max => {
            if nums.is_empty() {
                vals.iter().max().cloned().unwrap_or_default()
            } else {
                fmt_num(nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max))
            }
        }
        AggregateFunction::GroupConcat { separator } => {
            vals.join(separator.as_deref().unwrap_or(" "))
        }
        AggregateFunction::Sample => vals.first().cloned().unwrap_or_default(),
        AggregateFunction::Custom(_) => String::new(),
    }
}

/// Format an f64 aggregate result without a trailing `.0` for integral values, so a
/// `SUM`/`COUNT` of integers reads as an integer (matching the stored lexical form).
fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

// ── BGP — match each triple pattern, join on shared variables ───────────────────

fn eval_bgp(ctx: &Ctx, patterns: &[TriplePattern]) -> Vec<Solution> {
    let mut acc: Vec<Solution> = vec![Solution::new()];
    for tp in patterns {
        let matches = match_triple_pattern(ctx, tp);
        let mut next = Vec::new();
        for base in &acc {
            for m in &matches {
                if let Some(merged) = merge(base, m) {
                    next.push(merged);
                }
            }
        }
        acc = next;
        if acc.is_empty() {
            break;
        }
    }
    acc
}

/// Property-path evaluation (CONCEPT:KG-2.235). spargebra DESUGARS a sequence path
/// (`p1/p2`) into a BGP with anonymous-bnode intermediates, so a single-predicate
/// path reaching here is handled by the one-triple-pattern matcher. The variable-
/// length / combinator forms (`p+`, `p*`, `p?`, alternative `a|b`, inverse `^p`, and
/// their nesting) are evaluated by [`path_pairs`]: it computes the `(start, end)`
/// resource pairs the path connects, then binds subject/object against them.
fn eval_path(
    ctx: &Ctx,
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
) -> Result<Vec<Solution>, String> {
    // A single named predicate stays the literal/edge triple-pattern matcher (it also
    // matches literal-valued predicates, which the resource-only path engine doesn't).
    if let PropertyPathExpression::NamedNode(n) = path {
        let pred = oxrdf::NamedNode::new(n.as_str()).map_err(|e| e.to_string())?;
        let tp = TriplePattern {
            subject: subject.clone(),
            predicate: NamedNodePattern::NamedNode(pred),
            object: object.clone(),
        };
        return Ok(match_triple_pattern(ctx, &tp));
    }

    // The combinator forms resolve over RESOURCE edges (a property path connects nodes,
    // not literals). Enumerate the connected pairs, then bind the subject/object terms.
    let pairs = path_pairs(ctx, path)?;
    let mut out = Vec::new();
    for (s, o) in pairs {
        let mut sol = Solution::new();
        if !bind_subject(subject, &s, &mut sol) {
            continue;
        }
        if !bind_object_node(object, &o, &mut sol) {
            continue;
        }
        out.push(sol);
    }
    Ok(out)
}

/// All `(start, end)` resource-node id pairs the property `path` connects, over the
/// GraphView's typed edges. Recurses on the path combinators:
///   * `NamedNode(p)`  → every edge typed `p`.
///   * `Reverse(p)`    → the pairs of `p` flipped (`^p`).
///   * `Sequence(a,b)` → join: `a` then `b` (shared midpoint).
///   * `Alternative(a,b)` → the union of both.
///   * `OneOrMore(p)`  → transitive closure (`p+`, ≥1 hop).
///   * `ZeroOrMore(p)` → reflexive-transitive closure (`p*`, incl. identity on EVERY
///     node, per SPARQL `x p* x`).
///   * `ZeroOrOne(p)`  → `p` ∪ identity (`p?`).
fn path_pairs(ctx: &Ctx, path: &PropertyPathExpression) -> Result<Vec<(String, String)>, String> {
    Ok(match path {
        PropertyPathExpression::NamedNode(n) => edge_pairs(ctx, n.as_str()),
        PropertyPathExpression::Reverse(inner) => path_pairs(ctx, inner)?
            .into_iter()
            .map(|(s, o)| (o, s))
            .collect(),
        PropertyPathExpression::Sequence(a, b) => {
            let left = path_pairs(ctx, a)?;
            let right = path_pairs(ctx, b)?;
            let mut out = Vec::new();
            for (s, mid) in &left {
                for (rs, o) in &right {
                    if rs == mid {
                        out.push((s.clone(), o.clone()));
                    }
                }
            }
            dedup_pairs(out)
        }
        PropertyPathExpression::Alternative(a, b) => {
            let mut out = path_pairs(ctx, a)?;
            out.extend(path_pairs(ctx, b)?);
            dedup_pairs(out)
        }
        PropertyPathExpression::OneOrMore(inner) => {
            let base = path_pairs(ctx, inner)?;
            transitive_closure(&base, false, ctx)
        }
        PropertyPathExpression::ZeroOrMore(inner) => {
            let base = path_pairs(ctx, inner)?;
            transitive_closure(&base, true, ctx)
        }
        PropertyPathExpression::ZeroOrOne(inner) => {
            let mut out = path_pairs(ctx, inner)?;
            // identity on every node (`x p? x`).
            for id in ctx.active.node_properties.keys() {
                let iri = ctx.proj.node_iri(id);
                out.push((iri.clone(), iri));
            }
            dedup_pairs(out)
        }
        PropertyPathExpression::NegatedPropertySet(_) => {
            return Err("eg-rdf SPARQL: negated property set `!p` not supported".into());
        }
    })
}

/// Every `(subject, object)` resource pair carrying a typed edge whose projected
/// predicate IRI equals `pred` (the path predicate, already a full IRI from spargebra).
/// Subject/object are projected node IRIs so pairs match query terms + bind consistently.
fn edge_pairs(ctx: &Ctx, pred: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for ((s, o), blobs) in &ctx.active.edge_properties {
        for blob in blobs {
            if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
                if let Some(rel) = v.get("type").and_then(|x| x.as_str()) {
                    if ctx.proj.pred_iri(rel) == pred {
                        out.push((ctx.proj.node_iri(s), ctx.proj.node_iri(o)));
                        break;
                    }
                }
            }
        }
    }
    out
}

/// Transitive closure of `base` edge pairs. `reflexive` adds the identity pair on
/// EVERY graph node (the `p*` semantics: `x p* x` for any `x`, even isolated nodes).
fn transitive_closure(
    base: &[(String, String)],
    reflexive: bool,
    ctx: &Ctx,
) -> Vec<(String, String)> {
    use std::collections::{HashMap, HashSet};
    // adjacency.
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for (s, o) in base {
        adj.entry(s.as_str()).or_default().push(o.as_str());
    }
    let starts: HashSet<&str> = base.iter().map(|(s, _)| s.as_str()).collect();
    let mut out: HashSet<(String, String)> = HashSet::new();
    // BFS reachability (≥1 hop) from each start.
    for &start in &starts {
        let mut stack = vec![start];
        let mut visited: HashSet<&str> = HashSet::new();
        while let Some(cur) = stack.pop() {
            if let Some(next) = adj.get(cur) {
                for &n in next {
                    if visited.insert(n) {
                        out.insert((start.to_string(), n.to_string()));
                        stack.push(n);
                    }
                }
            }
        }
    }
    if reflexive {
        for id in ctx.active.node_properties.keys() {
            let iri = ctx.proj.node_iri(id);
            out.insert((iri.clone(), iri));
        }
    }
    out.into_iter().collect()
}

fn dedup_pairs(v: Vec<(String, String)>) -> Vec<(String, String)> {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    v.into_iter().filter(|p| seen.insert(p.clone())).collect()
}

/// Resolve ONE triple pattern against the GraphView under the LPG→RDF projection
/// `proj`. Predicate may be an IRI or a variable; object an IRI/literal/variable;
/// subject an IRI/bnode/variable. Subject/object resource IRIs, property/edge predicate
/// IRIs, and the synthesized `rdf:type` object are all produced by `proj` so the
/// projected triples match the caller's vocabulary (CONCEPT:KG-2.240).
fn match_triple_pattern(ctx: &Ctx, tp: &TriplePattern) -> Vec<Solution> {
    let view = ctx.active;
    let proj = ctx.proj;
    let mut out = Vec::new();

    // EDGE patterns: object is a resource. Scan edges; project subject/predicate/object.
    for ((s, o), blobs) in &view.edge_properties {
        for blob in blobs {
            let v: serde_json::Value = match rmp_serde::from_slice(blob.as_slice()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let Some(rel) = v.get("type").and_then(|x| x.as_str()) else {
                continue;
            };
            let mut sol = Solution::new();
            if !bind_subject(&tp.subject, &proj.node_iri(s), &mut sol) {
                continue;
            }
            if !bind_predicate_iri(&tp.predicate, &proj.pred_iri(rel), &mut sol) {
                continue;
            }
            if !bind_object_node(&tp.object, &proj.node_iri(o), &mut sol) {
                continue;
            }
            out.push(sol);
        }
    }

    // NODE patterns: scan node property cells.
    for (id, blob) in &view.node_properties {
        let v: serde_json::Value = match rmp_serde::from_slice(blob.as_slice()) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let Some(obj) = v.as_object() else { continue };
        let subj_iri = proj.node_iri(id);

        // `rdf:type` synthesis from the node `type`/`node_type` field. In the IDENTITY
        // projection this is `None` (no synthesis — `rdf:type` comes from explicit
        // typing edges, the prior behavior). Under a namespaced projection it yields
        // `<subj> rdf:type <base + CamelCase(type)>`, matching AU's materialization.
        if let Some(ty) = obj
            .get("type")
            .or_else(|| obj.get("node_type"))
            .and_then(|x| x.as_str())
        {
            if let Some(type_obj) = proj.type_object_iri(ty) {
                let mut sol = Solution::new();
                if bind_subject(&tp.subject, &subj_iri, &mut sol)
                    && bind_predicate_iri(&tp.predicate, RDF_TYPE_IRI, &mut sol)
                    && bind_object_node(&tp.object, &type_obj, &mut sol)
                {
                    out.push(sol);
                }
            }
        }

        // LITERAL patterns: each scalar / typed-cell property → a literal triple.
        for (k, cell) in obj {
            // `type`/`node_type` are emitted as `rdf:type` (above) / engine bookkeeping.
            if k == "type" || k == "node_type" {
                continue;
            }
            let Some(lit_val) = cell_lexical(cell) else {
                continue;
            };
            let mut sol = Solution::new();
            if !bind_subject(&tp.subject, &subj_iri, &mut sol) {
                continue;
            }
            if !bind_predicate_iri(&tp.predicate, &proj.pred_iri(k), &mut sol) {
                continue;
            }
            if !bind_object_literal(&tp.object, &lit_val, &mut sol) {
                continue;
            }
            out.push(sol);
        }
    }

    out
}

/// The variable name a query blank node binds under. A blank node in a QUERY
/// pattern is a non-distinguished variable (SPARQL semantics) — spargebra desugars
/// a sequence property path `p1/p2` into a BGP whose intermediate is exactly such a
/// bnode. We key it in the solution by a reserved name so it joins like any other
/// variable but is dropped from the projected output (it isn't in `collect_vars`).
fn bnode_var(b: &spargebra::term::BlankNode) -> String {
    format!("__bnode__{}", b.as_str())
}

fn bind_subject(pat: &TermPattern, node_id: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::NamedNode(n) => format!("<{}>", n.as_str()) == node_id,
        TermPattern::BlankNode(b) => {
            sol.insert(bnode_var(b), Binding::Node(node_id.to_string()));
            true
        }
        _ => false,
    }
}

fn bind_predicate_iri(pat: &NamedNodePattern, pred_iri: &str, sol: &mut Solution) -> bool {
    match pat {
        NamedNodePattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(pred_iri.to_string()));
            true
        }
        NamedNodePattern::NamedNode(n) => n.as_str() == pred_iri,
    }
}

fn bind_object_node(pat: &TermPattern, node_id: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(v.as_str().to_string(), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::NamedNode(n) => format!("<{}>", n.as_str()) == node_id,
        TermPattern::BlankNode(b) => {
            sol.insert(bnode_var(b), Binding::Node(node_id.to_string()));
            true
        }
        TermPattern::Literal(_) => false, // a literal pattern can't match a resource
    }
}

fn bind_object_literal(pat: &TermPattern, lit_val: &str, sol: &mut Solution) -> bool {
    match pat {
        TermPattern::Variable(v) => {
            sol.insert(
                v.as_str().to_string(),
                Binding::Literal(lit_val.to_string()),
            );
            true
        }
        TermPattern::Literal(l) => l.value() == lit_val,
        _ => false, // a resource pattern can't match a literal
    }
}

/// Merge two solutions if they agree on every shared variable.
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

fn left_join(l: &[Solution], r: &[Solution], filter: Option<&Expression>) -> Vec<Solution> {
    let mut out = Vec::new();
    for a in l {
        let mut matched = false;
        for b in r {
            if let Some(m) = merge(a, b) {
                if filter.map(|e| eval_filter(e, &m)).unwrap_or(true) {
                    out.push(m);
                    matched = true;
                }
            }
        }
        if !matched {
            out.push(a.clone()); // OPTIONAL: keep the un-extended left solution
        }
    }
    out
}

// ── FILTER — a small expression evaluator (the increment's subset) ──────────────

fn eval_filter(expr: &Expression, sol: &Solution) -> bool {
    eval_expr_bool(expr, sol).unwrap_or(false)
}

fn eval_expr_bool(expr: &Expression, sol: &Solution) -> Option<bool> {
    match expr {
        Expression::Bound(v) => Some(sol.contains_key(v.as_str())),
        Expression::Equal(a, b) => Some(expr_str(a, sol)? == expr_str(b, sol)?),
        Expression::Greater(a, b) => Some(num(a, sol)? > num(b, sol)?),
        Expression::GreaterOrEqual(a, b) => Some(num(a, sol)? >= num(b, sol)?),
        Expression::Less(a, b) => Some(num(a, sol)? < num(b, sol)?),
        Expression::LessOrEqual(a, b) => Some(num(a, sol)? <= num(b, sol)?),
        Expression::And(a, b) => Some(eval_expr_bool(a, sol)? && eval_expr_bool(b, sol)?),
        Expression::Or(a, b) => Some(eval_expr_bool(a, sol)? || eval_expr_bool(b, sol)?),
        Expression::Not(a) => Some(!eval_expr_bool(a, sol)?),
        _ => None,
    }
}

fn expr_str(e: &Expression, sol: &Solution) -> Option<String> {
    match e {
        Expression::Variable(v) => sol.get(v.as_str()).map(|b| b.as_str().to_string()),
        Expression::Literal(l) => Some(l.value().to_string()),
        Expression::NamedNode(n) => Some(format!("<{}>", n.as_str())),
        _ => None,
    }
}

fn num(e: &Expression, sol: &Solution) -> Option<f64> {
    expr_str(e, sol)?.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::{load_triples, parse_turtle, IriStore};

    fn loaded_view() -> GraphView {
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer ; ex:knows ex:bob .
ex:bob   a ex:Person ; ex:name "Bob"   ; ex:age "25"^^xsd:integer .
ex:carol a ex:Person ; ex:name "Carol" ; ex:age "40"^^xsd:integer ; ex:knows ex:alice .
"#;
        let core = eg_core::graph::GraphCore::new();
        let mut iris = IriStore::default();
        load_triples(
            &core,
            &mut iris,
            "g",
            parse_turtle(ttl).unwrap(),
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
        core.analysis_snapshot()
    }

    /// W2: a ≥2-pattern BGP + a FILTER returns the right solutions.
    #[test]
    fn bgp_two_patterns_plus_filter() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p a ex:Person .
              ?p ex:name ?name .
              ?p ex:age ?age .
              ?p ex:knows ?other .
              FILTER (?age > 28)
            }"#,
        )
        .unwrap();
        assert!(res.vars.contains(&"name".to_string()));
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        // alice (30, knows bob) and carol (40, knows alice) qualify; bob (25) is
        // filtered out AND has no ex:knows anyway.
        assert_eq!(names, vec!["Alice", "Carol"], "got {names:?}");
    }

    /// W2: OPTIONAL left-join returns an unbound variable for the no-match case.
    #[test]
    fn optional_left_join() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?other WHERE {
              ?p a ex:Person .
              ?p ex:name ?name .
              OPTIONAL { ?p ex:knows ?other }
            }"#,
        )
        .unwrap();
        let mut rows: Vec<(String, Option<String>)> = res
            .solutions
            .iter()
            .map(|s| {
                (
                    s.get("name").unwrap().as_str().to_string(),
                    s.get("other").map(|b| b.as_str().to_string()),
                )
            })
            .collect();
        rows.sort();
        assert_eq!(rows.len(), 3, "got {rows:?}");
        let bob = rows.iter().find(|(n, _)| n == "Bob").unwrap();
        assert_eq!(bob.1, None, "bob has no OPTIONAL knows-target");
        let alice = rows.iter().find(|(n, _)| n == "Alice").unwrap();
        assert!(alice.1.is_some(), "alice DOES know someone");
    }

    /// W2: UNION merges two graph patterns' solutions.
    #[test]
    fn union_merges_branches() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              { ?p ex:name "Alice" . ?p ex:name ?name . }
              UNION
              { ?p ex:name "Bob" . ?p ex:name ?name . }
            }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        names.dedup();
        assert_eq!(names, vec!["Alice", "Bob"], "got {names:?}");
    }

    /// W2: a fixed-length sequence property path `ex:knows/ex:name`.
    #[test]
    fn sequence_property_path() {
        let view = loaded_view();
        // carol knows alice; alice's name is "Alice".
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?n WHERE { ex:carol ex:knows/ex:name ?n . }"#,
        )
        .unwrap();
        let names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("n").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(names, vec!["Alice"], "got {names:?}");
    }

    /// `to_rows` projects unbound OPTIONAL variables as None (wire shape).
    #[test]
    fn to_rows_projects_unbound_as_none() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?other WHERE {
              ?p ex:name ?name .
              OPTIONAL { ?p ex:knows ?other }
            }"#,
        )
        .unwrap();
        let (cols, rows) = res.to_rows();
        assert!(cols.contains(&"name".to_string()) && cols.contains(&"other".to_string()));
        let other_idx = cols.iter().position(|c| c == "other").unwrap();
        assert!(
            rows.iter().any(|r| r[other_idx].is_none()),
            "bob's ?other must be None"
        );
    }

    // ── CONCEPT:KG-2.235 — SPARQL completeness ──────────────────────────────

    /// Pull the single aggregate cell from a 1-row, 1-projected-var result.
    fn agg_cell(res: &SparqlResult, var: &str) -> String {
        assert_eq!(res.solutions.len(), 1, "expected ONE group, got {res:?}");
        res.solutions[0].get(var).unwrap().as_str().to_string()
    }

    /// COUNT(*) over a BGP — the whole result is one group.
    #[test]
    fn aggregate_count_all() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person . }"#,
        )
        .unwrap();
        // alice, bob, carol.
        assert_eq!(agg_cell(&res, "n"), "3");
    }

    /// SUM(?age) over the three people = 30+25+40 = 95.
    #[test]
    fn aggregate_sum() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT (SUM(?age) AS ?total) WHERE { ?p ex:age ?age . }"#,
        )
        .unwrap();
        assert_eq!(agg_cell(&res, "total"), "95");
    }

    /// GROUP BY a constant property + COUNT — every Person shares the same rdf:type,
    /// so a GROUP BY ?type yields one group of 3.
    #[test]
    fn aggregate_group_by_count() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT ?type (COUNT(?p) AS ?n) WHERE { ?p rdf:type ?type . }
            GROUP BY ?type"#,
        )
        .unwrap();
        // one group (ex:Person), count 3.
        assert_eq!(res.solutions.len(), 1, "one group");
        assert_eq!(
            res.solutions[0].get("n").unwrap().as_str(),
            "3",
            "got {res:?}"
        );
    }

    /// `p+` transitive closure: carol knows alice, alice knows bob ⇒
    /// `ex:carol ex:knows+ ?who` reaches BOTH alice and bob.
    #[test]
    fn property_path_one_or_more() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:carol ex:knows+ ?who . }"#,
        )
        .unwrap();
        let mut who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        who.sort();
        assert_eq!(
            who,
            vec![
                "<http://example.org/alice>".to_string(),
                "<http://example.org/bob>".to_string()
            ],
            "got {who:?}"
        );
    }

    /// `^p` inverse path: `ex:alice ^ex:knows ?who` ⇒ whoever knows alice = carol.
    #[test]
    fn property_path_inverse() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:alice ^ex:knows ?who . }"#,
        )
        .unwrap();
        let who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            who,
            vec!["<http://example.org/carol>".to_string()],
            "got {who:?}"
        );
    }

    /// `a|b` alternative path — match either of two predicates (here `knows`
    /// alternated with itself, so the result equals the plain `knows` pairs).
    #[test]
    fn property_path_alternative() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?who WHERE { ex:carol (ex:knows|ex:knows) ?who . }"#,
        )
        .unwrap();
        let who: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("who").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            who,
            vec!["<http://example.org/alice>".to_string()],
            "got {who:?}"
        );
    }

    /// `GRAPH ?g { … }` — over the single dataset, the inner BGP resolves and `?g`
    /// binds the request graph IRI.
    #[test]
    fn graph_named_form_binds_g() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?g ?name WHERE { GRAPH ?g { ex:alice ex:name ?name . } }"#,
        )
        .unwrap();
        assert_eq!(res.solutions.len(), 1, "got {res:?}");
        let s = &res.solutions[0];
        assert_eq!(s.get("name").unwrap().as_str(), "Alice");
        assert_eq!(s.get("g").unwrap().as_str(), DEFAULT_GRAPH_IRI);
    }

    // ── CONCEPT:KG-2.240 — LPG→RDF projection vocabulary ────────────────────────

    /// The AU namespace + CamelCase projection — exactly what agent-utilities passes.
    const AU_NS: &str = "http://agent-utilities.dev/ontology#";
    fn au_proj() -> Projection {
        Projection::from_wire(AU_NS, "camel")
    }

    /// A NATIVE property-graph view (the `add_node`/`add_edge` shape AU writes, NOT the
    /// `AddTriples` RDF shape): node `type`/`name` are bare scalars, the edge is typed
    /// `knows`. `alice` is an `agent`, `bob` a `world_model` (a multi-word type, to
    /// exercise CamelCase). `alice knows bob`.
    fn native_view() -> GraphView {
        let core = eg_core::graph::GraphCore::new();
        let mut txn = core.txn();
        txn.add_node(
            "alice".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"type":"agent","name":"Alice"})).unwrap(),
        );
        txn.add_node(
            "bob".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"type":"world_model","name":"Bob"}))
                .unwrap(),
        );
        txn.add_edge(
            "alice".into(),
            "bob".into(),
            rmp_serde::to_vec_named(&serde_json::json!({"type":"knows"})).unwrap(),
        )
        .unwrap();
        drop(txn);
        core.analysis_snapshot()
    }

    /// `?s rdf:type au:Agent` resolves natively over the LPG — the original failure.
    /// `agent`→`au:Agent` (CamelCase), so only alice matches; `world_model`→
    /// `au:WorldModel`, so bob matches that class instead.
    #[test]
    fn projection_rdf_type_by_class() {
        let view = native_view();
        let proj = au_proj();
        let res = run_projected(
            &view,
            "PREFIX au: <http://agent-utilities.dev/ontology#>\
             SELECT ?s WHERE { ?s a au:Agent }",
            &proj,
        )
        .unwrap();
        let subs: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("s").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            subs,
            vec![format!("<{AU_NS}alice>")],
            "only the agent-typed node is an au:Agent; got {subs:?}"
        );

        // The multi-word type CamelCases: bob is an au:WorldModel.
        let res2 = run_projected(
            &view,
            "PREFIX au: <http://agent-utilities.dev/ontology#>\
             SELECT ?s WHERE { ?s a au:WorldModel }",
            &proj,
        )
        .unwrap();
        let subs2: Vec<String> = res2
            .solutions
            .iter()
            .filter_map(|s| s.get("s").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(subs2, vec![format!("<{AU_NS}bob>")], "got {subs2:?}");
    }

    /// A by-property literal query projects the property key under `au:` and matches the
    /// native scalar value.
    #[test]
    fn projection_by_property_literal() {
        let view = native_view();
        let res = run_projected(
            &view,
            "PREFIX au: <http://agent-utilities.dev/ontology#>\
             SELECT ?s WHERE { ?s au:name \"Alice\" }",
            &au_proj(),
        )
        .unwrap();
        let subs: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("s").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(subs, vec![format!("<{AU_NS}alice>")], "got {subs:?}");
    }

    /// The typed edge projects to `au:knows` between `au:`-namespaced node IRIs.
    #[test]
    fn projection_edge() {
        let view = native_view();
        let res = run_projected(
            &view,
            "PREFIX au: <http://agent-utilities.dev/ontology#>\
             SELECT ?o WHERE { au:alice au:knows ?o }",
            &au_proj(),
        )
        .unwrap();
        let objs: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("o").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(objs, vec![format!("<{AU_NS}bob>")], "got {objs:?}");
    }

    /// The full `?s ?p ?o` projection emits EXACTLY the AU-vocabulary triples that AU's
    /// rdflib `_build_rdf_graph` materializes from the same LPG: a CamelCased `rdf:type`
    /// per node under `au:`, each scalar property under `au:`, and the typed edge under
    /// `au:` between `au:` node IRIs. This is the engine==rdflib vocabulary contract.
    #[test]
    fn projection_full_triple_set_matches_au_convention() {
        let view = native_view();
        let res = run_projected(&view, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }", &au_proj()).unwrap();
        let triples: std::collections::HashSet<(String, String, String)> = res
            .solutions
            .iter()
            .map(|sol| {
                (
                    sol.get("s").unwrap().as_str().to_string(),
                    sol.get("p").unwrap().as_str().to_string(),
                    sol.get("o").unwrap().as_str().to_string(),
                )
            })
            .collect();
        let rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string();
        let expected: std::collections::HashSet<(String, String, String)> = [
            (
                format!("<{AU_NS}alice>"),
                rdf_type.clone(),
                format!("<{AU_NS}Agent>"),
            ),
            (
                format!("<{AU_NS}bob>"),
                rdf_type,
                format!("<{AU_NS}WorldModel>"),
            ),
            (
                format!("<{AU_NS}alice>"),
                format!("{AU_NS}name"),
                "Alice".into(),
            ),
            (
                format!("<{AU_NS}bob>"),
                format!("{AU_NS}name"),
                "Bob".into(),
            ),
            (
                format!("<{AU_NS}alice>"),
                format!("{AU_NS}knows"),
                format!("<{AU_NS}bob>"),
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            triples, expected,
            "projected triple set must match AU's vocabulary"
        );
    }

    /// The IDENTITY projection (the default) does NOT synthesize `rdf:type` from the
    /// node `type` field (it stays edge-sourced) and emits keys verbatim — so existing
    /// callers are byte-for-byte unchanged. Here a native LPG under the raw projection
    /// yields the bare-id `knows` edge + bare scalar literals, and NO `rdf:type`.
    #[test]
    fn identity_projection_unchanged() {
        let view = native_view();
        let res = run(&view, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }").unwrap();
        let preds: std::collections::HashSet<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("p").map(|b| b.as_str().to_string()))
            .collect();
        assert!(
            preds.contains("knows"),
            "raw edge predicate is bare; got {preds:?}"
        );
        assert!(
            preds.contains("name"),
            "raw scalar literal predicate is bare"
        );
        assert!(
            !preds.iter().any(|p| p.contains("rdf-syntax-ns#type")),
            "identity projection synthesizes NO rdf:type; got {preds:?}"
        );
    }

    // ── CONCEPT:EG-017 — ASK / CONSTRUCT / DESCRIBE / named graphs ──────────────

    /// ASK returns true when the pattern matches, false when it does not.
    #[test]
    fn ask_true_and_false() {
        let view = loaded_view();
        let t = run_outcome(
            &view,
            "PREFIX ex: <http://example.org/> ASK { ex:alice ex:name \"Alice\" }",
            &Projection::raw(),
        )
        .unwrap();
        assert!(matches!(t, QueryOutcome::Boolean(true)), "got {t:?}");
        let f = run_outcome(
            &view,
            "PREFIX ex: <http://example.org/> ASK { ex:alice ex:name \"Zelda\" }",
            &Projection::raw(),
        )
        .unwrap();
        assert!(matches!(f, QueryOutcome::Boolean(false)), "got {f:?}");
    }

    /// CONSTRUCT instantiates its template — re-predicate `ex:knows` as `ex:friend`.
    #[test]
    fn construct_returns_expected_triples() {
        let view = loaded_view();
        let out = run_outcome(
            &view,
            r#"PREFIX ex: <http://example.org/>
               CONSTRUCT { ?a ex:friend ?b } WHERE { ?a ex:knows ?b }"#,
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Graph(triples) = out else {
            panic!("expected a graph")
        };
        // carol knows alice; alice knows bob ⇒ two friend triples.
        let mut got: Vec<String> = triples
            .iter()
            .map(|t| {
                format!(
                    "{} {}",
                    t.subject,
                    match &t.object {
                        oxrdf::Term::NamedNode(n) => n.as_str().to_string(),
                        other => other.to_string(),
                    }
                )
            })
            .collect();
        got.sort();
        assert!(triples
            .iter()
            .all(|t| t.predicate.as_str() == "http://example.org/friend"));
        assert_eq!(
            got,
            vec![
                "<http://example.org/alice> http://example.org/bob".to_string(),
                "<http://example.org/carol> http://example.org/alice".to_string(),
            ],
            "got {got:?}"
        );
    }

    /// DESCRIBE returns the triples about the resource (subject- and object-position).
    #[test]
    fn describe_returns_resource_triples() {
        let view = loaded_view();
        let out = run_outcome(
            &view,
            "PREFIX ex: <http://example.org/> DESCRIBE ex:alice",
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Graph(triples) = out else {
            panic!("expected a graph")
        };
        let alice = "<http://example.org/alice>";
        // Every described triple must mention alice in subject or object position.
        assert!(!triples.is_empty(), "alice has a description");
        assert!(
            triples
                .iter()
                .all(|t| t.subject.to_string() == alice || t.object.to_string() == alice),
            "every DESCRIBE triple touches alice; got {triples:?}"
        );
        // Her own properties (name) are in subject position …
        assert!(
            triples
                .iter()
                .any(|t| t.subject.to_string() == alice
                    && t.predicate.as_str() == "http://example.org/name"),
            "alice's name is described"
        );
        // … and carol-knows-alice is in object position (CBD object side).
        assert!(
            triples.iter().any(|t| t.object.to_string() == alice
                && t.predicate.as_str() == "http://example.org/knows"),
            "the inbound knows edge is described"
        );
    }

    /// Query-side named-graph isolation: a triple in graph A is not visible when the
    /// `GRAPH <B>` form scopes to graph B.
    #[test]
    fn named_graph_query_isolation() {
        let core_a = eg_core::graph::GraphCore::new();
        let mut iris = IriStore::default();
        load_triples(
            &core_a,
            &mut iris,
            "a",
            parse_turtle("@prefix ex: <http://ex/> . ex:a ex:p ex:b .").unwrap(),
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
        let core_b = eg_core::graph::GraphCore::new();
        load_triples(
            &core_b,
            &mut iris,
            "b",
            parse_turtle("@prefix ex: <http://ex/> . ex:c ex:p ex:d .").unwrap(),
            #[cfg(feature = "rdf-redb")]
            None,
        )
        .unwrap();
        let va = core_a.analysis_snapshot();
        let vb = core_b.analysis_snapshot();
        let default = GraphView::default();
        let ds = Dataset::new(
            &default,
            vec![("http://g/a".to_string(), &va), ("http://g/b".to_string(), &vb)],
        );
        // ex:a is in graph A only — scoping to B yields nothing.
        let in_b = run_outcome_dataset(
            &ds,
            "SELECT ?o WHERE { GRAPH <http://g/b> { <http://ex/a> <http://ex/p> ?o } }",
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Solutions(rb) = in_b else {
            panic!()
        };
        assert!(rb.solutions.is_empty(), "ex:a not visible in graph B");
        // Scoping to A finds it.
        let in_a = run_outcome_dataset(
            &ds,
            "SELECT ?o WHERE { GRAPH <http://g/a> { <http://ex/a> <http://ex/p> ?o } }",
            &Projection::raw(),
        )
        .unwrap();
        let QueryOutcome::Solutions(ra) = in_a else {
            panic!()
        };
        assert_eq!(ra.solutions.len(), 1, "ex:a visible in graph A");
    }
}
