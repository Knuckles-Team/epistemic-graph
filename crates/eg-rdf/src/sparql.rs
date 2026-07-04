//! W2 — Native SPARQL 1.1 evaluation over a `GraphView` (CONCEPT:EG-KG.ontology.concept-11).
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
//! Completeness increment (CONCEPT:EG-KG.query.sparql-completeness): aggregates (`COUNT`/`SUM`/`AVG`/`MIN`/
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
    AggregateExpression, AggregateFunction, Expression, Function, GraphPattern, OrderExpression,
    PropertyPathExpression,
};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern, Variable};
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

/// LPG→RDF projection vocabulary (CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary). Controls how the live property
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

/// An RDF dataset over live property-graph views (CONCEPT:EG-KG.query.named-graph-support — true named-graph
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
        self.named.iter().find(|(n, _)| n == iri).map(|(_, v)| *v)
    }
}

/// RDF-merge a set of graph views into ONE owned view (CONCEPT:EG-KG.ontology.from-from-named), used to build
/// the `FROM`-scoped default graph: node-property and edge-property maps are unioned
/// (node ids are unique per graph so the first cell wins; edge blob-lists concatenate).
/// Only the SPARQL-scanned maps are populated — the topology (`graph`/`node_map`) is not
/// needed by the pattern matcher, which reads `node_properties`/`edge_properties` only.
fn merge_views<'v>(views: impl Iterator<Item = &'v GraphView>) -> GraphView {
    let mut out = GraphView::default();
    for v in views {
        for (k, cell) in &v.node_properties {
            out.node_properties
                .entry(k.clone())
                .or_insert_with(|| cell.clone());
        }
        for (k, blobs) in &v.edge_properties {
            out.edge_properties
                .entry(k.clone())
                .or_default()
                .extend(blobs.iter().cloned());
        }
    }
    out
}

/// A remote SPARQL endpoint the evaluator can delegate a `SERVICE` clause to
/// (CONCEPT:EG-KG.query.sparql-service-federation-client). This is the SEAM: `eg-rdf` owns the algebra + the SILENT / join
/// semantics but knows NOTHING about HTTP — the facade supplies a `ureq`-backed impl
/// (feature `sparql-service`), keeping the Pi/crate-DAG contract intact (no HTTP dep
/// enters this pure-Rust crate). `select` runs one remote SELECT and returns its
/// solution table; `Err` carries a human-readable failure (routed by SILENT).
pub trait RemoteSparql: Sync {
    /// Evaluate `query` (a complete SPARQL SELECT) against `endpoint`, returning its rows.
    fn select(&self, endpoint: &str, query: &str) -> Result<SparqlResult, String>;
}

/// The active evaluation context: the dataset, the graph the current scans resolve
/// against (the default, or a `GRAPH`-scoped named graph), the LPG→RDF projection, and
/// the OPTIONAL remote-`SERVICE` client (CONCEPT:EG-KG.query.sparql-service-federation-client; `None` ⇒ SERVICE is unavailable).
struct Ctx<'a> {
    ds: &'a Dataset<'a>,
    active: &'a GraphView,
    proj: &'a Projection,
    service: Option<&'a dyn RemoteSparql>,
}

impl<'a> Ctx<'a> {
    /// Re-scope the context to a different active graph (entering a `GRAPH` block).
    fn with_active(&self, active: &'a GraphView) -> Ctx<'a> {
        Ctx {
            ds: self.ds,
            active,
            proj: self.proj,
            service: self.service,
        }
    }
}

/// The outcome of evaluating ANY SPARQL query form (CONCEPT:EG-KG.query.named-graph-support).
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
/// under `proj` (CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary). With [`Projection::raw`] this equals [`run`].
/// Non-SELECT forms are coerced to a row table (see [`run_outcome`] for the typed form).
pub fn run_projected(
    view: &GraphView,
    query_str: &str,
    proj: &Projection,
) -> Result<SparqlResult, String> {
    Ok(outcome_to_result(run_outcome(view, query_str, proj)?))
}

/// Parse + evaluate a SPARQL query of ANY form (SELECT/ASK/CONSTRUCT/DESCRIBE) over a
/// single GraphView, returning the typed [`QueryOutcome`] (CONCEPT:EG-KG.query.named-graph-support).
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

/// Parse + evaluate a SPARQL query over a [`Dataset`] with an OPTIONAL remote-`SERVICE`
/// client bound (CONCEPT:EG-KG.query.sparql-service-federation-client). Identical to [`run_outcome_dataset`] except a
/// `SERVICE <ep> { … }` clause dispatches through `service` (a `None` client makes every
/// non-SILENT SERVICE an error — the fail-closed default). This is the ONE additive entry
/// the facade calls; all existing entry points forward `service = None` (no behavior change).
pub fn run_outcome_dataset_service(
    ds: &Dataset,
    query_str: &str,
    proj: &Projection,
    service: Option<&dyn RemoteSparql>,
) -> Result<QueryOutcome, String> {
    let q = parse_query(query_str)?;
    evaluate_outcome_svc(ds, &q, proj, service)
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
    evaluate_outcome_svc(ds, query, proj, None)
}

/// Service-aware core of [`evaluate_outcome`] (CONCEPT:EG-KG.query.sparql-service-federation-client): identical, but threads an
/// optional remote-`SERVICE` client into the evaluation `Ctx`. The public `evaluate_outcome`
/// forwards `None` (no SERVICE), so no existing caller changes behavior.
fn evaluate_outcome_svc(
    ds: &Dataset,
    query: &Query,
    proj: &Projection,
    service: Option<&dyn RemoteSparql>,
) -> Result<QueryOutcome, String> {
    // FROM / FROM NAMED (CONCEPT:EG-KG.ontology.from-from-named): if the query carries a dataset spec, honor it
    // to scope the active dataset instead of always using the server-registered one.
    // `merged_default` owns the FROM-union view (if any) so it outlives the borrow.
    let merged_default;
    let scoped_ds;
    let ds: &Dataset = match query.dataset() {
        Some(qd) => {
            merged_default = if qd.default.is_empty() {
                None
            } else {
                // Default graph = the RDF-merge of every named `FROM <g>` graph.
                Some(merge_views(
                    qd.default.iter().filter_map(|g| ds.named_view(g.as_str())),
                ))
            };
            let default = merged_default.as_ref().unwrap_or(ds.default);
            // Named graphs = the `FROM NAMED <g>` set (all of them if none given).
            let named = match &qd.named {
                Some(names) => names
                    .iter()
                    .filter_map(|g| {
                        ds.named_view(g.as_str())
                            .map(|v| (g.as_str().to_string(), v))
                    })
                    .collect(),
                None => ds.named.clone(),
            };
            scoped_ds = Dataset { default, named };
            &scoped_ds
        }
        None => ds,
    };
    let ctx = Ctx {
        ds,
        active: ds.default,
        proj,
        service,
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
            Ok(QueryOutcome::Graph(describe_resources(
                &ctx, &vars, &solutions,
            )))
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
        // UPDATE/DESCRIBE WHERE never spans a remote SERVICE (CONCEPT:EG-KG.query.sparql-service-federation-client).
        service: None,
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

// ── CONSTRUCT / DESCRIBE (CONCEPT:EG-KG.query.named-graph-support) ───────────────────────────────────────

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
                .filter(|s| eval_filter(ctx, expr, s))
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
            Ok(left_join(ctx, &l, &r, expression.as_ref()))
        }
        GraphPattern::Union { left, right } => {
            let mut l = eval_pattern(ctx, left)?;
            let mut r = eval_pattern(ctx, right)?;
            l.append(&mut r);
            Ok(l)
        }
        // Sub-SELECT (CONCEPT:EG-KG.ontology.sub-select): evaluate the inner pattern, then RESTRICT each
        // solution to the projected `variables` so inner-only bindings can't leak out and
        // corrupt an outer join. Top-level SELECT output is unchanged — the result columns
        // already derive from the projected set — so this is a pure correctness fix that
        // makes a nested `{ SELECT … }` join on its projected vars only.
        GraphPattern::Project { inner, variables } => {
            let projected: std::collections::HashSet<&str> =
                variables.iter().map(|v| v.as_str()).collect();
            Ok(eval_pattern(ctx, inner)?
                .into_iter()
                .map(|s| {
                    s.into_iter()
                        .filter(|(k, _)| projected.contains(k.as_str()))
                        .collect()
                })
                .collect())
        }
        // GROUP BY + aggregates (CONCEPT:EG-KG.query.sparql-completeness). `Group` produces one solution per
        // group binding the GROUP BY vars + the aggregate-result vars; the wrapping
        // `Extend` (below) re-binds those to the projected names. With no GROUP BY var
        // the whole result is one group (`SELECT (COUNT(*) AS ?n) …`).
        GraphPattern::Group {
            inner,
            variables,
            aggregates,
        } => {
            let rows = eval_pattern(ctx, inner)?;
            Ok(eval_group(ctx, rows, variables, aggregates))
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
                    if let Some(val) = expr_str(ctx, expression, &s) {
                        s.insert(variable.as_str().to_string(), Binding::Literal(val));
                    }
                    s
                })
                .collect())
        }
        // GRAPH … { … } — true named-graph scoping (CONCEPT:EG-KG.query.named-graph-support). A constant graph
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
        // MINUS (CONCEPT:EG-KG.ontology.minus): set-difference. Keep each LEFT solution that is NOT
        // compatible with ANY right solution. SPARQL MINUS compatibility is agreement on
        // the SHARED bound variables; a left solution whose domain is DISJOINT from a
        // right solution is NOT removed by it (so a right pattern sharing no variable
        // never deletes anything).
        GraphPattern::Minus { left, right } => {
            let l = eval_pattern(ctx, left)?;
            let r = eval_pattern(ctx, right)?;
            Ok(l.into_iter()
                .filter(|ls| !r.iter().any(|rs| minus_compatible(ls, rs)))
                .collect())
        }
        // ORDER BY (CONCEPT:EG-KG.ontology.order-by-values-exists): a CORRECTNESS fix — the evaluator previously hit the
        // catch-all and errored, so ordered queries never returned in order. Evaluate the
        // inner pattern, then STABLE-sort its solutions by the `OrderExpression` list.
        GraphPattern::OrderBy { inner, expression } => {
            let mut sols = eval_pattern(ctx, inner)?;
            sort_solutions(ctx, &mut sols, expression);
            Ok(sols)
        }
        // VALUES (CONCEPT:EG-KG.ontology.order-by-values-exists): inline a ground-term data table into solutions; the
        // enclosing operator (a JOIN, typically) merges them with the rest of the pattern.
        GraphPattern::Values {
            variables,
            bindings,
        } => Ok(values_solutions(variables, bindings)),
        // SERVICE <ep> { … } — federated query (CONCEPT:EG-KG.query.sparql-service-federation-client). Dispatch the inner pattern
        // to a remote endpoint via `ctx.service`; the returned solutions flow up so the
        // enclosing Join/LeftJoin combines them with the local BGP (via `hash_join`).
        GraphPattern::Service {
            name,
            inner,
            silent,
        } => eval_service(ctx, name, inner, *silent),
        // With `Service` handled the match is now exhaustive over every `GraphPattern`
        // variant present in this build (the `Lateral` node is gated behind spargebra's
        // `sep-0006` feature, which we do not enable) — no catch-all needed.
    }
}

/// Evaluate a `SERVICE <ep> { inner }` clause (CONCEPT:EG-KG.query.sparql-service-federation-client) by delegating `inner` to a
/// remote SPARQL endpoint through `ctx.service`.
///
/// SILENT semantics: on ANY failure — a variable endpoint, no bound client, or a remote
/// HTTP/parse error — `silent` returns ONE empty solution (the join identity, so the
/// enclosing join passes the local side through unchanged); otherwise the error propagates.
/// Only a CONSTANT-IRI endpoint is supported (a `?var` endpoint is a failure per the rule).
fn eval_service(
    ctx: &Ctx,
    name: &NamedNodePattern,
    inner: &GraphPattern,
    silent: bool,
) -> Result<Vec<Solution>, String> {
    // One empty solution = the neutral element for a join (pass-through under SILENT).
    let hushed = |e: String| -> Result<Vec<Solution>, String> {
        if silent {
            Ok(vec![Solution::new()])
        } else {
            Err(e)
        }
    };
    let endpoint = match name {
        NamedNodePattern::NamedNode(n) => n.as_str(),
        // A variable endpoint (`SERVICE ?ep { … }`) is unsupported: it requires binding the
        // endpoint from an earlier pattern, which this evaluator does not resolve.
        NamedNodePattern::Variable(_) => {
            return hushed("eg-rdf SPARQL: SERVICE with a variable endpoint is unsupported".into());
        }
    };
    let client = match ctx.service {
        Some(c) => c,
        // Fail-closed: no client bound (feature off / allowlist empty) ⇒ SERVICE is disabled.
        None => {
            return hushed(format!(
                "eg-rdf SPARQL: SERVICE <{endpoint}> requires a remote client (feature `sparql-service`); none bound"
            ));
        }
    };
    let remote_query = build_service_query(inner);
    match client.select(endpoint, &remote_query) {
        Ok(res) => Ok(res.solutions),
        Err(e) => hushed(format!("eg-rdf SPARQL: SERVICE <{endpoint}> failed: {e}")),
    }
}

/// Build the SPARQL SELECT text sent to a remote SERVICE endpoint (CONCEPT:EG-KG.query.sparql-service-federation-client): wrap
/// `inner` in a `SELECT` projecting its in-scope variables and render it with spargebra's
/// `Display` (which emits valid SPARQL 1.1). The projected vars are what the enclosing join
/// binds on, so the remote side returns exactly the columns the local pattern needs.
fn build_service_query(inner: &GraphPattern) -> String {
    let mut variables: Vec<Variable> = Vec::new();
    inner.on_in_scope_variable(|v| {
        if !variables.contains(v) {
            variables.push(v.clone());
        }
    });
    let pattern = GraphPattern::Project {
        inner: Box::new(inner.clone()),
        variables,
    };
    Query::Select {
        dataset: None,
        pattern,
        base_iri: None,
    }
    .to_string()
}

/// Stable-sort solutions by an `ORDER BY` comparator list (CONCEPT:EG-KG.ontology.order-by-values-exists). Each
/// `OrderExpression` is Asc/Desc over an expression; solutions compare on the first
/// expression that distinguishes them (numeric when both sides parse as numbers, else
/// lexical). An UNBOUND/error value sorts FIRST in ascending order (SPARQL orders the
/// unbound below every bound value), and Desc simply reverses that comparator.
fn sort_solutions(ctx: &Ctx, sols: &mut [Solution], order: &[OrderExpression]) {
    sols.sort_by(|a, b| {
        for oe in order {
            let (expr, desc) = match oe {
                OrderExpression::Asc(e) => (e, false),
                OrderExpression::Desc(e) => (e, true),
            };
            let ord = cmp_binding(&eval_term(ctx, expr, a), &eval_term(ctx, expr, b));
            let ord = if desc { ord.reverse() } else { ord };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// The SPARQL `ORDER BY` term-type precedence rank (CONCEPT:EG-KG.ontology.completing-eg-order-by). The spec fixes a
/// total order ACROSS term kinds — an unbound value sorts before any bound value, then
/// blank nodes, then IRIs, then literals — and only compares *values* within the same
/// kind. Prior to EG-135 the comparator ignored the kind and compared every bound value
/// by its lexical string, so a query ordering over MIXED IRI/literal (or blank/IRI)
/// columns came back in the wrong group order. Ranks: unbound(0) < blank(1) < IRI(2) <
/// literal(3).
fn order_rank(b: &Option<Binding>) -> u8 {
    match b {
        None => 0,
        Some(Binding::Node(s)) if s.starts_with("_:") => 1,
        Some(Binding::Node(_)) => 2, // an `<iri>` node
        Some(Binding::Literal(_)) => 3,
    }
}

/// Compare two (possibly unbound) `ORDER BY` values under the full SPARQL term ordering
/// (CONCEPT:EG-KG.ontology.completing-eg-order-by, completing the EG-125 ORDER BY arm). Terms first order by KIND
/// ([`order_rank`]: unbound < blank node < IRI < literal); only within the SAME kind do
/// values compare — blank/IRI lexically by term id, and literals by a typed comparison:
/// numerically when both lexical forms parse as numbers, else lexically (xsd:dateTime /
/// xsd:date ISO-8601 lexicals already sort chronologically under a lexical compare for a
/// shared timezone, and plain strings compare by code point).
fn cmp_binding(a: &Option<Binding>, b: &Option<Binding>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    // Cross-kind: the type precedence decides it outright.
    let (ra, rb) = (order_rank(a), order_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        // Same rank ⇒ both unbound, both nodes, or both literals.
        (None, None) => Ordering::Equal,
        (Some(x), Some(y)) => {
            let (xs, ys) = (x.as_str(), y.as_str());
            // Typed value compare only applies to literals; nodes (same kind) order by id.
            if matches!((x, y), (Binding::Literal(_), Binding::Literal(_))) {
                match (xs.parse::<f64>(), ys.parse::<f64>()) {
                    (Ok(nx), Ok(ny)) => nx.partial_cmp(&ny).unwrap_or(Ordering::Equal),
                    _ => xs.cmp(ys),
                }
            } else {
                xs.cmp(ys)
            }
        }
        // Unreachable: differing ranks were handled above.
        _ => Ordering::Equal,
    }
}

/// Turn an inline `VALUES` table into solutions (CONCEPT:EG-KG.ontology.order-by-values-exists): one solution per row,
/// binding each variable to its ground term; an `UNDEF` cell (`None`) leaves that
/// variable unbound in that row.
fn values_solutions(variables: &[Variable], bindings: &[Vec<Option<GroundTerm>>]) -> Vec<Solution> {
    bindings
        .iter()
        .map(|row| {
            let mut sol = Solution::new();
            for (var, cell) in variables.iter().zip(row) {
                if let Some(gt) = cell {
                    sol.insert(var.as_str().to_string(), ground_term_binding(gt));
                }
            }
            sol
        })
        .collect()
}

/// A `VALUES` ground term → a solution binding (CONCEPT:EG-KG.ontology.order-by-values-exists): an IRI becomes a `Node`
/// (`<iri>`), a literal its lexical `Literal` value (matching how the BGP matcher binds).
fn ground_term_binding(gt: &GroundTerm) -> Binding {
    match gt {
        GroundTerm::NamedNode(n) => Binding::Node(format!("<{}>", n.as_str())),
        GroundTerm::Literal(l) => Binding::Literal(l.value().to_string()),
        #[allow(unreachable_patterns)]
        _ => Binding::Literal(String::new()),
    }
}

/// SPARQL MINUS compatibility (CONCEPT:EG-KG.ontology.minus): `l` and `r` are compatible iff they
/// agree on every variable bound in BOTH and share at least one such variable. A right
/// solution with a disjoint domain returns `false`, so it never removes a left solution.
fn minus_compatible(l: &Solution, r: &Solution) -> bool {
    let mut shared = false;
    for (k, v) in l {
        if let Some(rv) = r.get(k) {
            shared = true;
            if rv != v {
                return false;
            }
        }
    }
    shared
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

// ── GROUP BY + aggregates (CONCEPT:EG-KG.query.sparql-completeness) ────────────────────────────────────

/// Evaluate `GROUP BY group_vars` + the `aggregates` over `rows`. Returns one
/// solution per distinct group-key, binding each group-by var to its value AND each
/// aggregate-result var (the internal name spargebra assigns) to the computed scalar.
/// The wrapping `Extend` re-binds those internal vars to the user's projected names.
fn eval_group(
    ctx: &Ctx,
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
            let value = compute_aggregate(ctx, agg, &members);
            sol.insert(out_var.as_str().to_string(), Binding::Literal(value));
        }
        out.push(sol);
    }
    out
}

/// Compute ONE aggregate over a group's member solutions, returning its lexical value.
fn compute_aggregate(ctx: &Ctx, agg: &AggregateExpression, members: &[Solution]) -> String {
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
            let mut vals: Vec<String> = members
                .iter()
                .filter_map(|s| expr_str(ctx, expr, s))
                .collect();
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

/// Property-path evaluation (CONCEPT:EG-KG.query.sparql-completeness). spargebra DESUGARS a sequence path
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
        // Negated property set `!(p1|…|pn)` (CONCEPT:EG-KG.ontology.negated-property-set): every resource edge whose
        // projected predicate IRI is NOT one of the negated predicates.
        PropertyPathExpression::NegatedPropertySet(preds) => {
            let negated: std::collections::HashSet<String> =
                preds.iter().map(|p| p.as_str().to_string()).collect();
            negated_edge_pairs(ctx, &negated)
        }
    })
}

/// Every `(subject, object)` resource pair carrying a typed edge whose projected
/// predicate IRI is NOT in `negated` — the negated property set `!p` (CONCEPT:EG-KG.ontology.negated-property-set).
fn negated_edge_pairs(
    ctx: &Ctx,
    negated: &std::collections::HashSet<String>,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for ((s, o), blobs) in &ctx.active.edge_properties {
        for blob in blobs {
            if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob.as_slice()) {
                if let Some(rel) = v.get("type").and_then(|x| x.as_str()) {
                    if !negated.contains(&ctx.proj.pred_iri(rel)) {
                        out.push((ctx.proj.node_iri(s), ctx.proj.node_iri(o)));
                        break;
                    }
                }
            }
        }
    }
    dedup_pairs(out)
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
/// projected triples match the caller's vocabulary (CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary).
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
        // RDF-star (CONCEPT:EG-KG.ontology.concept-5): a quoted-triple object pattern does not match a
        // plain resource node (LPG persistence of quoted triples is a documented
        // follow-up; quoted triples round-trip natively via parse/serialize).
        #[cfg(feature = "sparql-star")]
        TermPattern::Triple(_) => false,
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

fn left_join(
    ctx: &Ctx,
    l: &[Solution],
    r: &[Solution],
    filter: Option<&Expression>,
) -> Vec<Solution> {
    let mut out = Vec::new();
    for a in l {
        let mut matched = false;
        for b in r {
            if let Some(m) = merge(a, b) {
                if filter.map(|e| eval_filter(ctx, e, &m)).unwrap_or(true) {
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

fn eval_filter(ctx: &Ctx, expr: &Expression, sol: &Solution) -> bool {
    eval_expr_bool(ctx, expr, sol).unwrap_or(false)
}

// Rich FILTER expression evaluation (CONCEPT:EG-KG.ontology.rich-filter). The evaluator has three layers:
//   * `eval_term`     — evaluates ANY expression to a typed term `Binding` (Node vs
//                       Literal), preserving the type info `isIRI`/`STR`/`DATATYPE` need.
//   * `eval_expr_bool`— the boolean (FILTER) layer: logical ops, comparisons, `IN`,
//                       `IF`, `COALESCE`, and the boolean built-in `FunctionCall`s.
//   * `expr_str`/`num`— scalar projections over `eval_term`, used by BIND/aggregates.
// Datatype-aware where feasible (numeric comparison/equality); unsupported forms still
// fail SAFE (the FILTER yields `false` / the bind yields no value).

fn eval_expr_bool(ctx: &Ctx, expr: &Expression, sol: &Solution) -> Option<bool> {
    match expr {
        Expression::Bound(v) => Some(sol.contains_key(v.as_str())),
        Expression::Equal(a, b) => Some(terms_equal(ctx, a, b, sol)),
        Expression::SameTerm(a, b) => Some(expr_str(ctx, a, sol)? == expr_str(ctx, b, sol)?),
        Expression::Greater(a, b) => Some(num(ctx, a, sol)? > num(ctx, b, sol)?),
        Expression::GreaterOrEqual(a, b) => Some(num(ctx, a, sol)? >= num(ctx, b, sol)?),
        Expression::Less(a, b) => Some(num(ctx, a, sol)? < num(ctx, b, sol)?),
        Expression::LessOrEqual(a, b) => Some(num(ctx, a, sol)? <= num(ctx, b, sol)?),
        Expression::And(a, b) => Some(eval_expr_bool(ctx, a, sol)? && eval_expr_bool(ctx, b, sol)?),
        Expression::Or(a, b) => Some(eval_expr_bool(ctx, a, sol)? || eval_expr_bool(ctx, b, sol)?),
        Expression::Not(a) => Some(!eval_expr_bool(ctx, a, sol)?),
        // `IN` (and `NOT IN`, which spargebra parses to `Not(In(…))`) — numeric-aware
        // membership over the candidate list.
        Expression::In(a, list) => {
            let lhs = eval_term(ctx, a, sol)?;
            Some(list.iter().any(|e| {
                eval_term(ctx, e, sol)
                    .map(|rhs| binding_terms_equal(&lhs, &rhs))
                    .unwrap_or(false)
            }))
        }
        Expression::If(c, t, e) => {
            if eval_expr_bool(ctx, c, sol).unwrap_or(false) {
                eval_expr_bool(ctx, t, sol)
            } else {
                eval_expr_bool(ctx, e, sol)
            }
        }
        Expression::Coalesce(args) => args.iter().find_map(|e| eval_expr_bool(ctx, e, sol)),
        Expression::FunctionCall(f, args) => eval_bool_function(ctx, f, args, sol),
        // FILTER EXISTS / NOT EXISTS (CONCEPT:EG-KG.ontology.order-by-values-exists). `NOT EXISTS` parses to
        // `Not(Exists(…))`, so the negation is handled by the `Not` arm above. Evaluate
        // the sub-pattern under the active context and report whether ANY of its solutions
        // is COMPATIBLE with the current solution (agrees on the shared variables) — the
        // substitution-and-nonempty semantics of EXISTS.
        Expression::Exists(pattern) => {
            let sols = eval_pattern(ctx, pattern).ok()?;
            Some(sols.iter().any(|s| merge(sol, s).is_some()))
        }
        // Effective boolean value of any other value-producing expression.
        _ => eval_term(ctx, expr, sol).map(|b| ebv(&b)),
    }
}

/// Evaluate any expression to a typed term `Binding` (CONCEPT:EG-KG.ontology.rich-filter). Preserves the
/// Node/Literal distinction so `isIRI`/`isLiteral`/`STR`/`DATATYPE` resolve correctly.
fn eval_term(ctx: &Ctx, e: &Expression, sol: &Solution) -> Option<Binding> {
    match e {
        Expression::Variable(v) => sol.get(v.as_str()).cloned(),
        Expression::Literal(l) => Some(Binding::Literal(l.value().to_string())),
        Expression::NamedNode(n) => Some(Binding::Node(format!("<{}>", n.as_str()))),
        Expression::Add(a, b) => Some(Binding::Literal(fmt_num(
            num(ctx, a, sol)? + num(ctx, b, sol)?,
        ))),
        Expression::Subtract(a, b) => Some(Binding::Literal(fmt_num(
            num(ctx, a, sol)? - num(ctx, b, sol)?,
        ))),
        Expression::Multiply(a, b) => Some(Binding::Literal(fmt_num(
            num(ctx, a, sol)? * num(ctx, b, sol)?,
        ))),
        Expression::Divide(a, b) => {
            let d = num(ctx, b, sol)?;
            if d == 0.0 {
                return None;
            }
            Some(Binding::Literal(fmt_num(num(ctx, a, sol)? / d)))
        }
        Expression::UnaryPlus(a) => Some(Binding::Literal(fmt_num(num(ctx, a, sol)?))),
        Expression::UnaryMinus(a) => Some(Binding::Literal(fmt_num(-num(ctx, a, sol)?))),
        Expression::If(c, t, f) => {
            if eval_expr_bool(ctx, c, sol).unwrap_or(false) {
                eval_term(ctx, t, sol)
            } else {
                eval_term(ctx, f, sol)
            }
        }
        Expression::Coalesce(args) => args.iter().find_map(|a| eval_term(ctx, a, sol)),
        Expression::FunctionCall(f, args) => eval_str_function(ctx, f, args, sol),
        // Boolean-valued expressions render as an xsd:boolean lexical — including
        // `EXISTS` used in a value context, e.g. `BIND(EXISTS { … } AS ?x)` (CONCEPT:EG-KG.ontology.order-by-values-exists).
        Expression::Bound(_)
        | Expression::Equal(..)
        | Expression::SameTerm(..)
        | Expression::Greater(..)
        | Expression::GreaterOrEqual(..)
        | Expression::Less(..)
        | Expression::LessOrEqual(..)
        | Expression::And(..)
        | Expression::Or(..)
        | Expression::Not(..)
        | Expression::In(..)
        | Expression::Exists(_) => Some(Binding::Literal(
            if eval_expr_bool(ctx, e, sol)? {
                "true"
            } else {
                "false"
            }
            .to_string(),
        )),
    }
}

/// Boolean SPARQL built-ins (CONCEPT:EG-KG.ontology.rich-filter): `REGEX`, `CONTAINS`/`STRSTARTS`/`STRENDS`,
/// `LANGMATCHES`, and the `isIRI`/`isBlank`/`isLiteral`/`isNumeric` type tests.
fn eval_bool_function(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<bool> {
    use spargebra::algebra::Function as F;
    match f {
        F::Contains => {
            Some(expr_str(ctx, args.first()?, sol)?.contains(&expr_str(ctx, args.get(1)?, sol)?))
        }
        F::StrStarts => {
            Some(expr_str(ctx, args.first()?, sol)?.starts_with(&expr_str(ctx, args.get(1)?, sol)?))
        }
        F::StrEnds => {
            Some(expr_str(ctx, args.first()?, sol)?.ends_with(&expr_str(ctx, args.get(1)?, sol)?))
        }
        F::LangMatches => {
            let tag = expr_str(ctx, args.first()?, sol)?.to_lowercase();
            let range = expr_str(ctx, args.get(1)?, sol)?.to_lowercase();
            Some(
                (range == "*" && !tag.is_empty())
                    || tag == range
                    || tag.starts_with(&format!("{range}-")),
            )
        }
        F::Regex => {
            let text = expr_str(ctx, args.first()?, sol)?;
            let pat = expr_str(ctx, args.get(1)?, sol)?;
            let flags = args
                .get(2)
                .and_then(|f| expr_str(ctx, f, sol))
                .unwrap_or_default();
            let pattern = if flags.contains('i') {
                format!("(?i){pat}")
            } else {
                pat
            };
            regex::Regex::new(&pattern)
                .ok()
                .map(|re| re.is_match(&text))
        }
        F::IsNumeric => Some(
            eval_term(ctx, args.first()?, sol)
                .map(|b| term_lexical(&b).parse::<f64>().is_ok())
                .unwrap_or(false),
        ),
        F::IsIri => Some(
            eval_term(ctx, args.first()?, sol)
                .map(|b| matches!(&b, Binding::Node(s) if s.starts_with('<')))
                .unwrap_or(false),
        ),
        F::IsBlank => Some(
            eval_term(ctx, args.first()?, sol)
                .map(|b| matches!(&b, Binding::Node(s) if s.starts_with("_:")))
                .unwrap_or(false),
        ),
        F::IsLiteral => Some(
            eval_term(ctx, args.first()?, sol)
                .map(|b| matches!(b, Binding::Literal(_)))
                .unwrap_or(false),
        ),
        // RDF-star (CONCEPT:EG-KG.ontology.concept-5): isTRIPLE tests whether the term is a quoted triple.
        #[cfg(feature = "sparql-star")]
        F::IsTriple => Some(
            eval_term(ctx, args.first()?, sol)
                .map(|b| is_quoted(&b))
                .unwrap_or(false),
        ),
        // GeoSPARQL boolean spatial relations (CONCEPT:EG-KG.ontology.concept-10): a `geof:sf*` call parses
        // to `Function::Custom(<geof-ns>…)`; we evaluate the two operands to their WKT
        // lexical forms and lower the relation onto eg-geo's DE-9IM predicates.
        #[cfg(feature = "geosparql")]
        F::Custom(iri) if iri.as_str().starts_with(crate::geosparql::GEOF_NS) => {
            let local = &iri.as_str()[crate::geosparql::GEOF_NS.len()..];
            let a = expr_str(ctx, args.first()?, sol)?;
            let b = expr_str(ctx, args.get(1)?, sol)?;
            crate::geosparql::eval_relation(local, &a, &b)
        }
        _ => None,
    }
}

/// String/term-valued SPARQL built-ins (CONCEPT:EG-KG.ontology.rich-filter): `STR`/`IRI`/`LANG`/`DATATYPE`,
/// `UCASE`/`LCASE`/`STRLEN`/`CONCAT`/`SUBSTR`, plus the boolean built-ins rendered as an
/// xsd:boolean lexical so they compose inside other string expressions.
fn eval_str_function(
    ctx: &Ctx,
    f: &Function,
    args: &[Expression],
    sol: &Solution,
) -> Option<Binding> {
    use spargebra::algebra::Function as F;
    match f {
        F::Str => Some(Binding::Literal(term_lexical(&eval_term(
            ctx,
            args.first()?,
            sol,
        )?))),
        F::Iri => {
            let iri = expr_str(ctx, args.first()?, sol)?
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_string();
            Some(Binding::Node(format!("<{iri}>")))
        }
        // No language tag is retained in a `Binding`, so LANG is the empty string (the
        // correct value for a plain/typed literal).
        F::Lang => Some(Binding::Literal(String::new())),
        F::Datatype => Some(Binding::Node(format!(
            "<{}>",
            best_effort_datatype(&eval_term(ctx, args.first()?, sol)?)
        ))),
        F::UCase => Some(Binding::Literal(
            expr_str(ctx, args.first()?, sol)?.to_uppercase(),
        )),
        F::LCase => Some(Binding::Literal(
            expr_str(ctx, args.first()?, sol)?.to_lowercase(),
        )),
        F::StrLen => Some(Binding::Literal(fmt_num(
            expr_str(ctx, args.first()?, sol)?.chars().count() as f64,
        ))),
        F::Concat => {
            let mut s = String::new();
            for a in args {
                s.push_str(&expr_str(ctx, a, sol)?);
            }
            Some(Binding::Literal(s))
        }
        F::SubStr => {
            // SPARQL SUBSTR is 1-based; an optional length truncates.
            let chars: Vec<char> = expr_str(ctx, args.first()?, sol)?.chars().collect();
            let begin = (num(ctx, args.get(1)?, sol)?.max(1.0) as usize).saturating_sub(1);
            let slice: String = match args.get(2) {
                Some(lenexpr) => {
                    let len = num(ctx, lenexpr, sol)?.max(0.0) as usize;
                    chars.iter().skip(begin).take(len).collect()
                }
                None => chars.iter().skip(begin).collect(),
            };
            Some(Binding::Literal(slice))
        }
        // ── Term constructors (CONCEPT:EG-KG.ontology.concept-4) ────────────────────────────────
        // BNODE() → a fresh blank node; BNODE(str) → a blank node labelled from the
        // arg. The arg-less form is NON-DETERMINISTIC (see `next_rand_u64`) and must
        // stay out of any cached/deterministic evaluation path.
        F::BNode => {
            let label = match args.first() {
                Some(a) => sanitize_bnode_label(&expr_str(ctx, a, sol)?),
                None => format!("b{:x}", fresh_id()),
            };
            Some(Binding::Node(format!("_:{label}")))
        }
        // STRDT(lexical, datatype) / STRLANG(lexical, lang): a `Binding` carries only
        // the lexical form (as DATATYPE/LANG already infer best-effort), so the value
        // round-trips while the datatype/lang ride along implicitly.
        F::StrDt | F::StrLang => Some(Binding::Literal(expr_str(ctx, args.first()?, sol)?)),
        // UUID() → a fresh urn:uuid: IRI; STRUUID() → its lexical form. NON-DETERMINISTIC.
        F::Uuid => Some(Binding::Node(format!("<urn:uuid:{}>", fresh_uuid()))),
        F::StrUuid => Some(Binding::Literal(fresh_uuid())),

        // ── Hash built-ins (CONCEPT:EG-KG.ontology.concept-4) ──────────────────────────────────
        // Pure-Rust RustCrypto, gated behind `sparql-hash` (OUT of pi). When the
        // feature is off they fall through to `_ => None` (unsupported, fails SAFE).
        #[cfg(feature = "sparql-hash")]
        F::Md5 => Some(Binding::Literal(hash_hex::<md5::Md5>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        #[cfg(feature = "sparql-hash")]
        F::Sha1 => Some(Binding::Literal(hash_hex::<sha1::Sha1>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        #[cfg(feature = "sparql-hash")]
        F::Sha256 => Some(Binding::Literal(hash_hex::<sha2::Sha256>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        #[cfg(feature = "sparql-hash")]
        F::Sha384 => Some(Binding::Literal(hash_hex::<sha2::Sha384>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),
        #[cfg(feature = "sparql-hash")]
        F::Sha512 => Some(Binding::Literal(hash_hex::<sha2::Sha512>(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),

        // ── Numeric built-ins (CONCEPT:EG-KG.ontology.concept-4) ───────────────────────────────
        F::Abs => Some(Binding::Literal(fmt_num(
            num(ctx, args.first()?, sol)?.abs(),
        ))),
        F::Ceil => Some(Binding::Literal(fmt_num(
            num(ctx, args.first()?, sol)?.ceil(),
        ))),
        F::Floor => Some(Binding::Literal(fmt_num(
            num(ctx, args.first()?, sol)?.floor(),
        ))),
        // SPARQL ROUND is half-towards-positive-infinity (ROUND(-2.5)=-2, ROUND(2.5)=3).
        F::Round => Some(Binding::Literal(fmt_num(
            (num(ctx, args.first()?, sol)? + 0.5).floor(),
        ))),
        // RAND() → xsd:double in [0,1). NON-DETERMINISTIC.
        F::Rand => Some(Binding::Literal(fmt_num(rand_f64()))),

        // ── Date-time built-ins (CONCEPT:EG-KG.ontology.concept-4) ─────────────────────────────
        // NOW() → the current xsd:dateTime (UTC). NON-DETERMINISTIC.
        F::Now => Some(Binding::Literal(now_xsd_datetime())),
        F::Year => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.year as f64,
        ))),
        F::Month => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.month as f64,
        ))),
        F::Day => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.day as f64,
        ))),
        F::Hours => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.hour as f64,
        ))),
        F::Minutes => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.minute as f64,
        ))),
        F::Seconds => Some(Binding::Literal(fmt_num(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.second,
        ))),
        F::Tz => Some(Binding::Literal(
            parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.tz,
        )),
        F::Timezone => Some(Binding::Literal(tz_to_duration(
            &parse_datetime(&expr_str(ctx, args.first()?, sol)?)?.tz,
        ))),

        // ── String extras (CONCEPT:EG-KG.ontology.concept-4) ───────────────────────────────────
        F::StrBefore => {
            let s = expr_str(ctx, args.first()?, sol)?;
            let sep = expr_str(ctx, args.get(1)?, sol)?;
            Some(Binding::Literal(match s.find(&sep) {
                Some(i) => s[..i].to_string(),
                None => String::new(),
            }))
        }
        F::StrAfter => {
            let s = expr_str(ctx, args.first()?, sol)?;
            let sep = expr_str(ctx, args.get(1)?, sol)?;
            Some(Binding::Literal(match s.find(&sep) {
                Some(i) => s[i + sep.len()..].to_string(),
                None => String::new(),
            }))
        }
        // REPLACE(str, pattern, replacement [, flags]) — regex-backed (reuses the
        // `regex` dep already in `sparql`); `$1` back-references pass through.
        F::Replace => {
            let s = expr_str(ctx, args.first()?, sol)?;
            let pat = expr_str(ctx, args.get(1)?, sol)?;
            let rep = expr_str(ctx, args.get(2)?, sol)?;
            let flags = args
                .get(3)
                .and_then(|fl| expr_str(ctx, fl, sol))
                .unwrap_or_default();
            let pattern = if flags.contains('i') {
                format!("(?i){pat}")
            } else {
                pat
            };
            let re = regex::Regex::new(&pattern).ok()?;
            Some(Binding::Literal(
                re.replace_all(&s, rep.as_str()).into_owned(),
            ))
        }
        F::EncodeForUri => Some(Binding::Literal(encode_for_uri(&expr_str(
            ctx,
            args.first()?,
            sol,
        )?))),

        // ── RDF-star / SPARQL-star term accessors (CONCEPT:EG-KG.ontology.concept-5) ────────────
        // A quoted triple is a first-class term encoded as the canonical `<< s p o >>`
        // string in a `Binding::Node`; TRIPLE constructs it and SUBJECT/PREDICATE/OBJECT
        // project its components.
        #[cfg(feature = "sparql-star")]
        F::Triple => {
            let s = eval_term(ctx, args.first()?, sol)?;
            let p = eval_term(ctx, args.get(1)?, sol)?;
            let o = eval_term(ctx, args.get(2)?, sol)?;
            Some(Binding::Node(encode_quoted(&s, &p, &o)))
        }
        #[cfg(feature = "sparql-star")]
        F::Subject => quoted_component(&eval_term(ctx, args.first()?, sol)?, 0),
        #[cfg(feature = "sparql-star")]
        F::Predicate => quoted_component(&eval_term(ctx, args.first()?, sol)?, 1),
        #[cfg(feature = "sparql-star")]
        F::Object => quoted_component(&eval_term(ctx, args.first()?, sol)?, 2),
        #[cfg(feature = "sparql-star")]
        F::IsTriple => Some(Binding::Literal(
            if eval_bool_function(ctx, f, args, sol)? {
                "true"
            } else {
                "false"
            }
            .to_string(),
        )),

        // Boolean built-ins composed in a string context → "true"/"false".
        F::Contains
        | F::StrStarts
        | F::StrEnds
        | F::Regex
        | F::LangMatches
        | F::IsIri
        | F::IsBlank
        | F::IsLiteral
        | F::IsNumeric => Some(Binding::Literal(
            if eval_bool_function(ctx, f, args, sol)? {
                "true"
            } else {
                "false"
            }
            .to_string(),
        )),
        // GeoSPARQL value functions (CONCEPT:EG-KG.ontology.concept-10): `geof:distance(a,b,units)` → a
        // numeric literal; `geof:buffer(g,radius,units)` → a WKT lexical (a wktLiteral).
        // A boolean `geof:sf*` used in a value context renders as "true"/"false".
        #[cfg(feature = "geosparql")]
        F::Custom(iri) if iri.as_str().starts_with(crate::geosparql::GEOF_NS) => {
            let local = &iri.as_str()[crate::geosparql::GEOF_NS.len()..];
            match local {
                "distance" => {
                    let a = expr_str(ctx, args.first()?, sol)?;
                    let b = expr_str(ctx, args.get(1)?, sol)?;
                    let units = args
                        .get(2)
                        .and_then(|u| expr_str(ctx, u, sol))
                        .unwrap_or_default();
                    Some(Binding::Literal(fmt_num(crate::geosparql::eval_distance(
                        &a, &b, &units,
                    )?)))
                }
                "buffer" => {
                    let g = expr_str(ctx, args.first()?, sol)?;
                    let radius = num(ctx, args.get(1)?, sol)?;
                    let units = args
                        .get(2)
                        .and_then(|u| expr_str(ctx, u, sol))
                        .unwrap_or_default();
                    Some(Binding::Literal(crate::geosparql::eval_buffer(
                        &g, radius, &units,
                    )?))
                }
                _ => Some(Binding::Literal(
                    if eval_bool_function(ctx, f, args, sol)? {
                        "true"
                    } else {
                        "false"
                    }
                    .to_string(),
                )),
            }
        }
        _ => None,
    }
}

/// Lexical value of a term: an IRI loses its angle brackets (`STR()` / comparison).
fn term_lexical(b: &Binding) -> String {
    match b {
        Binding::Node(s) => s.trim_start_matches('<').trim_end_matches('>').to_string(),
        Binding::Literal(s) => s.clone(),
    }
}

/// Best-effort xsd datatype IRI for `DATATYPE()` — bindings drop the original datatype,
/// so we infer numeric vs string from the lexical form (datatype-aware where feasible).
fn best_effort_datatype(b: &Binding) -> &'static str {
    match b {
        Binding::Node(_) => "http://www.w3.org/2001/XMLSchema#anyURI",
        Binding::Literal(s) => {
            if s.parse::<i64>().is_ok() {
                "http://www.w3.org/2001/XMLSchema#integer"
            } else if s.parse::<f64>().is_ok() {
                "http://www.w3.org/2001/XMLSchema#decimal"
            } else {
                "http://www.w3.org/2001/XMLSchema#string"
            }
        }
    }
}

/// Effective boolean value (EBV) of a term for a bare expression in FILTER position.
fn ebv(b: &Binding) -> bool {
    match b {
        Binding::Literal(s) => match s.parse::<f64>() {
            Ok(n) => n != 0.0,
            Err(_) => !s.is_empty() && !s.eq_ignore_ascii_case("false"),
        },
        Binding::Node(s) => !s.is_empty(),
    }
}

/// Datatype-aware `=` (CONCEPT:EG-KG.ontology.rich-filter): numeric comparison when both sides parse as
/// numbers, else lexical-term equality.
fn terms_equal(ctx: &Ctx, a: &Expression, b: &Expression, sol: &Solution) -> bool {
    match (eval_term(ctx, a, sol), eval_term(ctx, b, sol)) {
        (Some(x), Some(y)) => binding_terms_equal(&x, &y),
        _ => false,
    }
}

fn binding_terms_equal(x: &Binding, y: &Binding) -> bool {
    let (xs, ys) = (term_lexical(x), term_lexical(y));
    match (xs.parse::<f64>(), ys.parse::<f64>()) {
        (Ok(nx), Ok(ny)) => nx == ny,
        _ => xs == ys,
    }
}

fn expr_str(ctx: &Ctx, e: &Expression, sol: &Solution) -> Option<String> {
    eval_term(ctx, e, sol).map(|b| b.as_str().to_string())
}

fn num(ctx: &Ctx, e: &Expression, sol: &Solution) -> Option<f64> {
    expr_str(ctx, e, sol)?.parse::<f64>().ok()
}

// ── EG-127 helpers: hashing, non-deterministic sources, date-time, URI encoding ──

/// Hex-encoded digest of `input` for the SPARQL hash built-ins (CONCEPT:EG-KG.ontology.concept-4). The
/// `sha2::Digest` bound is the shared RustCrypto `digest::Digest` trait (re-exported by
/// `md-5`/`sha1`/`sha2` at the same 0.10 line), so it accepts `Md5`/`Sha1`/`Sha2*`.
#[cfg(feature = "sparql-hash")]
fn hash_hex<D: sha2::Digest>(input: &str) -> String {
    use std::fmt::Write as _;
    let out = D::digest(input.as_bytes());
    let mut s = String::with_capacity(out.len() * 2);
    for b in out {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Global PRNG state for the non-deterministic built-ins (`RAND`/`UUID`/`STRUUID`/
/// arg-less `BNODE`). NOT cryptographic and deliberately kept off any cached path.
static RNG_STATE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// SplitMix64 mixed with the wall clock + a monotonic counter — a cheap, dependency-free
/// non-deterministic source. Sufficient for SPARQL RAND/UUID (which need no crypto grade).
fn next_rand_u64() -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x =
        RNG_STATE.fetch_add(0x9E37_79B9_7F4A_7C15, std::sync::atomic::Ordering::Relaxed) ^ t;
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// A 53-bit-mantissa double in `[0, 1)` for `RAND()`.
fn rand_f64() -> f64 {
    (next_rand_u64() >> 11) as f64 / ((1u64 << 53) as f64)
}

/// A fresh opaque id for arg-less `BNODE()`.
fn fresh_id() -> u64 {
    next_rand_u64()
}

/// A blank-node label reduced to `[A-Za-z0-9_]` so `BNODE(str)` yields a legal label.
fn sanitize_bnode_label(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        format!("b{:x}", fresh_id())
    } else {
        cleaned
    }
}

/// An RFC-4122 v4 UUID string (used by `UUID()`/`STRUUID()`).
fn fresh_uuid() -> String {
    let (a, b) = (next_rand_u64(), next_rand_u64());
    let bytes = [
        (a >> 56) as u8,
        (a >> 48) as u8,
        (a >> 40) as u8,
        (a >> 32) as u8,
        (a >> 24) as u8,
        (a >> 16) as u8,
        (((a >> 8) as u8) & 0x0f) | 0x40, // version 4
        a as u8,
        (((b >> 56) as u8) & 0x3f) | 0x80, // variant 10xx
        (b >> 48) as u8,
        (b >> 40) as u8,
        (b >> 32) as u8,
        (b >> 24) as u8,
        (b >> 16) as u8,
        (b >> 8) as u8,
        b as u8,
    ];
    let mut s = String::with_capacity(36);
    use std::fmt::Write as _;
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{byte:02x}");
    }
    s
}

/// Percent-encode per SPARQL `ENCODE_FOR_URI` (unreserved set `A-Za-z0-9-_.~` pass through).
fn encode_for_uri(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// Decomposed xsd:dateTime / xsd:date fields for the accessor built-ins.
struct DateTimeParts {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: f64,
    /// The timezone lexical exactly as written (`""`, `"Z"`, `"+01:00"`, `"-05:00"`).
    tz: String,
}

/// Parse an xsd:dateTime (or xsd:date) lexical enough to serve YEAR..SECONDS/TZ/TIMEZONE.
fn parse_datetime(s: &str) -> Option<DateTimeParts> {
    let s = s.trim();
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };
    let mut di = date_part.split('-');
    let year: i64 = di.next()?.parse().ok()?;
    let month: u32 = di.next()?.parse().ok()?;
    let day: u32 = di.next()?.parse().ok()?;
    let (mut hour, mut minute, mut second, mut tz) = (0u32, 0u32, 0.0f64, String::new());
    if let Some(tp) = time_part {
        // Split off the timezone: `Z`/`+..` is unambiguous; a trailing `-..` is the tz.
        let (hms, tzs) = if let Some(i) = tp.find(['Z', '+']) {
            (&tp[..i], tp[i..].to_string())
        } else if let Some(i) = tp.rfind('-') {
            (&tp[..i], tp[i..].to_string())
        } else {
            (tp, String::new())
        };
        tz = tzs;
        let mut ti = hms.split(':');
        hour = ti.next()?.parse().ok()?;
        minute = ti.next()?.parse().ok()?;
        second = ti.next().unwrap_or("0").parse().ok()?;
    }
    Some(DateTimeParts {
        year: if neg { -year } else { year },
        month,
        day,
        hour,
        minute,
        second,
        tz,
    })
}

/// The `xsd:dayTimeDuration` form of a timezone lexical, per SPARQL `TIMEZONE`
/// (`Z`/empty → `PT0S`, `+01:00` → `PT1H`, `-05:30` → `-PT5H30M`).
fn tz_to_duration(tz: &str) -> String {
    if tz.is_empty() || tz == "Z" {
        return "PT0S".to_string();
    }
    let (sign, rest) = match tz.strip_prefix('+') {
        Some(r) => ("", r),
        None => match tz.strip_prefix('-') {
            Some(r) => ("-", r),
            None => return "PT0S".to_string(),
        },
    };
    let mut it = rest.split(':');
    let h: i64 = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let m: i64 = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let mut out = format!("{sign}PT");
    if h != 0 {
        out.push_str(&format!("{h}H"));
    }
    if m != 0 {
        out.push_str(&format!("{m}M"));
    }
    if h == 0 && m == 0 {
        out.push_str("0S");
    }
    out
}

/// The current UTC instant as an `xsd:dateTime` lexical (`NOW()`), without a date crate:
/// seconds-since-epoch → civil date via Howard Hinnant's `days_from_civil` inverse.
fn now_xsd_datetime() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// Civil (proleptic Gregorian) `(year, month, day)` from a days-since-1970-01-01 count.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ── EG-130 helpers: RDF-star quoted-triple term (string-encoded first-class term) ──

/// Render a `Binding` as a term inside a quoted triple: nodes keep their `<iri>`/`_:b`
/// form; literals are quoted (`"lit"`).
#[cfg(feature = "sparql-star")]
fn render_star_term(b: &Binding) -> String {
    match b {
        Binding::Node(s) => s.clone(),
        Binding::Literal(s) => format!("{s:?}"),
    }
}

/// Encode a quoted triple from its three component bindings as `<< s p o >>`.
#[cfg(feature = "sparql-star")]
fn encode_quoted(s: &Binding, p: &Binding, o: &Binding) -> String {
    format!(
        "<< {} {} {} >>",
        render_star_term(s),
        render_star_term(p),
        render_star_term(o)
    )
}

/// Whether a binding is a quoted-triple term (`<< … >>`).
#[cfg(feature = "sparql-star")]
fn is_quoted(b: &Binding) -> bool {
    matches!(b, Binding::Node(s) if s.starts_with("<<") && s.ends_with(">>"))
}

/// Project component `idx` (0=subject, 1=predicate, 2=object) of a quoted-triple term.
/// Components are whitespace-delimited canonical terms (IRIs/bnodes/simple literals);
/// space-bearing or nested-quoted components are a documented follow-up.
#[cfg(feature = "sparql-star")]
fn quoted_component(b: &Binding, idx: usize) -> Option<Binding> {
    if !is_quoted(b) {
        return None;
    }
    let Binding::Node(s) = b else { return None };
    let inner = s.strip_prefix("<<")?.strip_suffix(">>")?.trim();
    let tok = inner.split_whitespace().nth(idx)?;
    Some(if tok.starts_with('<') || tok.starts_with("_:") {
        Binding::Node(tok.to_string())
    } else {
        Binding::Literal(tok.trim_matches('"').to_string())
    })
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

    // ── CONCEPT:EG-KG.query.sparql-completeness — SPARQL completeness ──────────────────────────────

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

    // ── CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary — LPG→RDF projection vocabulary ────────────────────────

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

    // ── CONCEPT:EG-KG.query.named-graph-support — ASK / CONSTRUCT / DESCRIBE / named graphs ──────────────

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
            triples.iter().any(|t| t.subject.to_string() == alice
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
            vec![
                ("http://g/a".to_string(), &va),
                ("http://g/b".to_string(), &vb),
            ],
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

    // ── CONCEPT:EG-KG.ontology.sub-select — sub-SELECT ─────────────────────────────────────────────

    /// EG-051: a sub-SELECT that BINDS more vars than it projects must restrict each
    /// inner solution to the projected set, so an inner-only var can't leak and corrupt
    /// the outer join. Here the inner binds `?friend`/`?name` (the FRIEND's name) but
    /// projects only `?p`; the outer re-binds `?name` to the PERSON's own name. If the
    /// inner `?name` leaked, every join would mismatch and the result would be EMPTY.
    #[test]
    fn sub_select_restricts_to_projected_vars() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              { SELECT ?p WHERE { ?p ex:knows ?friend . ?friend ex:name ?name } }
              ?p ex:name ?name .
            }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        // alice (knows bob) and carol (knows alice) have an ex:knows; the join keys on
        // the projected ?p only, so each binds its OWN name.
        assert_eq!(names, vec!["Alice", "Carol"], "got {res:?}");
    }

    /// EG-051: a sub-SELECT computing `COUNT(*) AS ?n` projects + surfaces the scalar.
    #[test]
    fn sub_select_count_star() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?n WHERE {
              { SELECT (COUNT(*) AS ?n) WHERE { ?p a ex:Person } }
            }"#,
        )
        .unwrap();
        assert_eq!(res.solutions.len(), 1, "one aggregate row");
        assert_eq!(
            res.solutions[0].get("n").unwrap().as_str(),
            "3",
            "got {res:?}"
        );
    }

    /// EG-051 regression: a plain top-level SELECT is byte-for-byte unchanged.
    #[test]
    fn sub_select_top_level_unchanged() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?p ex:name ?name }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice", "Bob", "Carol"], "got {res:?}");
    }

    // ── CONCEPT:EG-KG.ontology.rich-filter — rich FILTER ────────────────────────────────────────────

    fn filtered_names(view: &GraphView, filter: &str) -> Vec<String> {
        let q = format!(
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {{
              ?p a ex:Person . ?p ex:name ?name . ?p ex:age ?age .
              FILTER ({filter})
            }}"#
        );
        let res = run(view, &q).unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        names
    }

    /// EG-053: REGEX (with the case-insensitive `i` flag).
    #[test]
    fn filter_regex() {
        let view = loaded_view();
        assert_eq!(
            filtered_names(&view, r#"REGEX(?name, "^a", "i")"#),
            vec!["Alice"]
        );
    }

    /// EG-053: arithmetic inside a comparison (`?age + 5 > 40`).
    #[test]
    fn filter_arithmetic_comparison() {
        let view = loaded_view();
        // ages 30/25/40 → 35/30/45; only carol (45) clears 40.
        assert_eq!(filtered_names(&view, "?age + 5 > 40"), vec!["Carol"]);
    }

    /// EG-053: `IN` membership (numeric-aware via the term compare).
    #[test]
    fn filter_in() {
        let view = loaded_view();
        let mut got = filtered_names(&view, r#"?name IN ("Alice", "Bob")"#);
        got.sort();
        assert_eq!(got, vec!["Alice", "Bob"]);
    }

    /// EG-053: `NOT IN` parses to `Not(In(…))` and excludes the listed members.
    #[test]
    fn filter_not_in() {
        let view = loaded_view();
        assert_eq!(
            filtered_names(&view, r#"?name NOT IN ("Alice", "Bob")"#),
            vec!["Carol"]
        );
    }

    /// EG-053: string built-ins — CONTAINS and UCASE.
    #[test]
    fn filter_string_functions() {
        let view = loaded_view();
        assert_eq!(
            filtered_names(&view, r#"CONTAINS(?name, "li")"#),
            vec!["Alice"]
        );
        assert_eq!(
            filtered_names(&view, r#"UCASE(?name) = "BOB""#),
            vec!["Bob"]
        );
        assert_eq!(filtered_names(&view, "STRLEN(?name) = 3"), vec!["Bob"]);
    }

    // ── CONCEPT:EG-KG.ontology.minus — MINUS ──────────────────────────────────────────────────

    /// EG-055: MINUS removes every left solution compatible with a right solution.
    /// alice/carol HAVE an ex:knows ⇒ removed; bob does not ⇒ kept.
    #[test]
    fn minus_set_difference() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p a ex:Person . ?p ex:name ?name .
              MINUS { ?p ex:knows ?o }
            }"#,
        )
        .unwrap();
        let names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(names, vec!["Bob"], "got {res:?}");
    }

    // ── CONCEPT:EG-KG.ontology.negated-property-set — negated property set `!p` ──────────────────────────────

    /// EG-056: `!ex:knows` matches every resource edge whose predicate is NOT ex:knows.
    #[test]
    fn negated_property_set() {
        let ttl = r#"
@prefix ex: <http://example.org/> .
ex:alice ex:knows ex:bob ; ex:likes ex:carol .
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
        let view = core.analysis_snapshot();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?s ?o WHERE { ?s !ex:knows ?o }"#,
        )
        .unwrap();
        let pairs: Vec<(String, String)> = res
            .solutions
            .iter()
            .map(|s| {
                (
                    s.get("s").unwrap().as_str().to_string(),
                    s.get("o").unwrap().as_str().to_string(),
                )
            })
            .collect();
        // only the ex:likes edge (alice→carol) survives; ex:knows is excluded.
        assert_eq!(pairs.len(), 1, "got {pairs:?}");
        assert!(
            pairs[0].0.contains("alice") && pairs[0].1.contains("carol"),
            "got {pairs:?}"
        );
    }

    // ── CONCEPT:EG-KG.ontology.order-by-values-exists — ORDER BY / VALUES / EXISTS ─────────────────────────────

    fn ordered_names(view: &GraphView, clause: &str) -> Vec<String> {
        let q = format!(
            "PREFIX ex: <http://example.org/> \
             SELECT ?name WHERE {{ ?p ex:name ?name ; ex:age ?age }} {clause}"
        );
        run(view, &q)
            .unwrap()
            .solutions
            .iter()
            .map(|s| s.get("name").unwrap().as_str().to_string())
            .collect()
    }

    /// EG-125: ORDER BY on a STRING var, ascending and descending.
    #[test]
    fn order_by_string_asc_desc() {
        let view = loaded_view();
        assert_eq!(
            ordered_names(&view, "ORDER BY ?name"),
            vec!["Alice", "Bob", "Carol"]
        );
        assert_eq!(
            ordered_names(&view, "ORDER BY DESC(?name)"),
            vec!["Carol", "Bob", "Alice"]
        );
    }

    /// EG-125: ORDER BY on a NUMERIC var sorts numerically (not lexically), asc + desc.
    #[test]
    fn order_by_numeric_asc_desc() {
        let view = loaded_view();
        // ages: Alice 30, Bob 25, Carol 40.
        assert_eq!(
            ordered_names(&view, "ORDER BY ?age"),
            vec!["Bob", "Alice", "Carol"]
        );
        assert_eq!(
            ordered_names(&view, "ORDER BY DESC(?age)"),
            vec!["Carol", "Alice", "Bob"]
        );
    }

    /// EG-125: an inline VALUES table joins with the surrounding BGP, restricting the
    /// result to the enumerated resources.
    #[test]
    fn values_join_restricts() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              VALUES ?p { ex:alice ex:carol }
              ?p ex:name ?name .
            }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .map(|s| s.get("name").unwrap().as_str().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice", "Carol"], "VALUES restricts to ?p set");
    }

    /// EG-125: FILTER EXISTS keeps only solutions whose sub-pattern has a match; NOT
    /// EXISTS keeps only those WITHOUT. Alice/Carol have ex:knows; Bob does not.
    #[test]
    fn filter_exists_and_not_exists() {
        let view = loaded_view();
        let names = |clause: &str| -> Vec<String> {
            let q = format!(
                "PREFIX ex: <http://example.org/> \
                 SELECT ?name WHERE {{ ?p ex:name ?name . FILTER {clause} }}"
            );
            let mut v: Vec<String> = run(&view, &q)
                .unwrap()
                .solutions
                .iter()
                .map(|s| s.get("name").unwrap().as_str().to_string())
                .collect();
            v.sort();
            v
        };
        assert_eq!(
            names("EXISTS { ?p ex:knows ?o }"),
            vec!["Alice", "Carol"],
            "EXISTS keeps the knowers"
        );
        assert_eq!(
            names("NOT EXISTS { ?p ex:knows ?o }"),
            vec!["Bob"],
            "NOT EXISTS keeps the non-knowers"
        );
    }

    // ── CONCEPT:EG-KG.ontology.completing-eg-order-by — SPARQL algebra completeness (ORDER BY spec / VALUES / MINUS) ──

    /// A fixture with a repeated primary sort key (`ex:dept`) so multi-key ORDER BY and
    /// top-k tie-breaking are exercised.
    fn ranked_view() -> GraphView {
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:a ex:dept "Eng"   ; ex:name "Zoe" ; ex:rank "2"^^xsd:integer .
ex:b ex:dept "Eng"   ; ex:name "Amy" ; ex:rank "1"^^xsd:integer .
ex:c ex:dept "Sales" ; ex:name "Bob" ; ex:rank "1"^^xsd:integer .
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

    fn ordered_col(view: &GraphView, q: &str, col: &str) -> Vec<String> {
        run(view, q)
            .unwrap()
            .solutions
            .iter()
            .map(|s| {
                s.get(col)
                    .map(|b| b.as_str().to_string())
                    .unwrap_or_default()
            })
            .collect()
    }

    /// EG-135: the ORDER BY term-type total order — unbound < blank node < IRI < literal —
    /// with typed value comparison WITHIN a kind. This is the correctness gap the EG-125
    /// arm left: bound values used to compare by lexical string regardless of kind.
    #[test]
    fn order_by_term_type_precedence_eg135() {
        use std::cmp::Ordering;
        let unbound: Option<Binding> = None;
        let blank = Some(Binding::Node("_:b1".to_string()));
        let iri = Some(Binding::Node("<http://ex/x>".to_string()));
        let lit = Some(Binding::Literal("Alice".to_string()));
        // Cross-kind precedence (each strictly less than the next kind).
        assert_eq!(cmp_binding(&unbound, &blank), Ordering::Less);
        assert_eq!(cmp_binding(&blank, &iri), Ordering::Less);
        assert_eq!(cmp_binding(&iri, &lit), Ordering::Less);
        assert_eq!(cmp_binding(&unbound, &lit), Ordering::Less);
        assert_eq!(cmp_binding(&lit, &unbound), Ordering::Greater);
        // Within literals: NUMERIC compare (not lexical — "9" must sort before "10").
        let nine = Some(Binding::Literal("9".to_string()));
        let ten = Some(Binding::Literal("10".to_string()));
        assert_eq!(cmp_binding(&nine, &ten), Ordering::Less);
        // Within literals: xsd:dateTime ISO-8601 lexicals sort chronologically.
        let early = Some(Binding::Literal("2020-01-01T00:00:00".to_string()));
        let late = Some(Binding::Literal("2021-06-15T12:00:00".to_string()));
        assert_eq!(cmp_binding(&early, &late), Ordering::Less);
        // Same-kind nodes order by term id, IRIs among themselves.
        let iri_a = Some(Binding::Node("<http://ex/a>".to_string()));
        let iri_b = Some(Binding::Node("<http://ex/b>".to_string()));
        assert_eq!(cmp_binding(&iri_a, &iri_b), Ordering::Less);
    }

    /// EG-135: multi-key ORDER BY (`?dept ASC, DESC(?rank)`) yields the exact top-level
    /// ROW ORDER — Eng before Sales, and within Eng the higher rank first.
    #[test]
    fn order_by_multikey_asc_desc_eg135() {
        let view = ranked_view();
        let q = "PREFIX ex: <http://example.org/> \
                 SELECT ?name WHERE { ?p ex:dept ?dept ; ex:name ?name ; ex:rank ?rank } \
                 ORDER BY ?dept DESC(?rank)";
        // Eng{rank2=Zoe, rank1=Amy} then Sales{Bob}.
        assert_eq!(ordered_col(&view, q, "name"), vec!["Zoe", "Amy", "Bob"]);
    }

    /// EG-135: `ORDER BY … LIMIT k` returns the correct top-k in order (the sort must run
    /// BEFORE the slice). ORDER BY ?rank then ?name, LIMIT 2 ⇒ the two lowest ranks,
    /// name-tie-broken: Amy(1), Bob(1) — Zoe(2) is cut.
    #[test]
    fn order_by_limit_topk_eg135() {
        let view = ranked_view();
        let q = "PREFIX ex: <http://example.org/> \
                 SELECT ?name WHERE { ?p ex:name ?name ; ex:rank ?rank } \
                 ORDER BY ?rank ?name LIMIT 2";
        assert_eq!(ordered_col(&view, q, "name"), vec!["Amy", "Bob"]);
    }

    /// EG-135: a `VALUES (?name ?tier)` table JOINED with a BGP both RESTRICTS (Carol,
    /// absent from the table, is dropped) and EXTENDS (binds the new `?tier` column).
    #[test]
    fn values_join_extends_eg135() {
        let view = loaded_view();
        let res = run(
            &view,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?tier WHERE {
              ?p ex:name ?name .
              VALUES (?name ?tier) { ("Alice" "gold") ("Bob" "silver") }
            }"#,
        )
        .unwrap();
        let mut rows: Vec<(String, String)> = res
            .solutions
            .iter()
            .map(|s| {
                (
                    s.get("name").unwrap().as_str().to_string(),
                    s.get("tier").unwrap().as_str().to_string(),
                )
            })
            .collect();
        rows.sort();
        // Carol has no VALUES row ⇒ dropped; Alice/Bob gain their tier.
        assert_eq!(
            rows,
            vec![
                ("Alice".to_string(), "gold".to_string()),
                ("Bob".to_string(), "silver".to_string())
            ],
            "VALUES join restricts to the table AND binds ?tier"
        );
    }

    /// EG-135: the DEFINITIVE MINUS-vs-NOT-EXISTS distinction — a right pattern sharing NO
    /// variable with the left. TRUE MINUS removes nothing when the domains are disjoint
    /// (so all rows survive), whereas FILTER NOT EXISTS evaluates the pattern's mere
    /// existence and, since `?x ex:knows ?y` HAS matches, removes EVERY row. Proving they
    /// differ confirms MINUS is real set-difference, not a NOT-EXISTS rewrite.
    #[test]
    fn minus_vs_not_exists_distinction_eg135() {
        let view = loaded_view();
        let names = |where_clause: &str| -> Vec<String> {
            let q = format!(
                "PREFIX ex: <http://example.org/> \
                 SELECT ?name WHERE {{ ?p ex:name ?name . {where_clause} }}"
            );
            let mut v: Vec<String> = run(&view, &q)
                .unwrap()
                .solutions
                .iter()
                .map(|s| s.get("name").unwrap().as_str().to_string())
                .collect();
            v.sort();
            v
        };
        // Disjoint-domain MINUS deletes nothing → all three survive.
        assert_eq!(
            names("MINUS { ?x ex:knows ?y }"),
            vec!["Alice", "Bob", "Carol"],
            "disjoint-domain MINUS must remove nothing"
        );
        // NOT EXISTS on the same disjoint pattern → the pattern HAS matches, so it removes
        // everything. This is the behavior MINUS deliberately does NOT share.
        assert_eq!(
            names("FILTER NOT EXISTS { ?x ex:knows ?y }"),
            Vec::<String>::new(),
            "NOT EXISTS on a matching disjoint pattern must remove all rows"
        );
    }

    // ── CONCEPT:EG-KG.ontology.from-from-named — FROM / FROM NAMED ──────────────────────────────────────

    /// EG-054: a `FROM <g>` clause scopes the default graph to that graph, so a plain
    /// (non-GRAPH) BGP only sees `g`'s triples — not the whole registered dataset.
    #[test]
    fn from_scopes_default_graph() {
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
        // A default graph that contains BOTH edges — FROM must narrow away from it.
        let both = merge_views([&va, &vb].into_iter());
        let ds = Dataset::new(
            &both,
            vec![
                ("http://g/a".to_string(), &va),
                ("http://g/b".to_string(), &vb),
            ],
        );
        let subjects = |q: &str| -> Vec<String> {
            let QueryOutcome::Solutions(r) =
                run_outcome_dataset(&ds, q, &Projection::raw()).unwrap()
            else {
                panic!()
            };
            let mut v: Vec<String> = r
                .solutions
                .iter()
                .map(|s| s.get("s").unwrap().as_str().to_string())
                .collect();
            v.sort();
            v
        };
        // No FROM: both edges visible in the default graph.
        assert_eq!(subjects("SELECT ?s WHERE { ?s ?p ?o }").len(), 2);
        // FROM <g/a>: only graph A's subject (ex:a) is visible.
        let from_a = {
            let QueryOutcome::Solutions(r) = run_outcome_dataset(
                &ds,
                "SELECT ?s FROM <http://g/a> WHERE { ?s ?p ?o }",
                &Projection::raw(),
            )
            .unwrap() else {
                panic!()
            };
            r.solutions
                .iter()
                .map(|s| s.get("s").unwrap().as_str().to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            from_a.len(),
            1,
            "FROM <g/a> restricts to one edge: {from_a:?}"
        );
        assert!(from_a[0].contains("<http://ex/a>"), "got {from_a:?}");
    }

    // ── CONCEPT:EG-KG.query.sparql-service-federation-client — SPARQL SERVICE federation ──────────────────────────────

    /// A mock [`RemoteSparql`] returning a fixed canned outcome, standing in for the
    /// facade's `ureq` client so the SERVICE algebra + SILENT/join semantics test in
    /// pure `eg-rdf` (no HTTP).
    struct MockService(Result<SparqlResult, String>);
    impl RemoteSparql for MockService {
        fn select(&self, _endpoint: &str, _query: &str) -> Result<SparqlResult, String> {
            self.0.clone()
        }
    }

    /// (a) SERVICE solutions JOIN a local BGP on the shared variable `?name`.
    #[test]
    fn service_joins_local_bgp() {
        let view = loaded_view();
        let ds = Dataset::single(&view);
        let mut row = Solution::new();
        row.insert("name".to_string(), Binding::Literal("Alice".to_string()));
        row.insert("score".to_string(), Binding::Literal("100".to_string()));
        let svc = MockService(Ok(SparqlResult {
            vars: vec!["name".to_string(), "score".to_string()],
            solutions: vec![row],
        }));
        let q = r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name ?score WHERE {
              ?p ex:name ?name .
              SERVICE <http://remote/e> { ?name ex:score ?score }
            }"#;
        let QueryOutcome::Solutions(r) =
            run_outcome_dataset_service(&ds, q, &Projection::raw(), Some(&svc)).unwrap()
        else {
            panic!()
        };
        // Only Alice has a remote score → the join keeps exactly one solution.
        assert_eq!(r.solutions.len(), 1, "got {:?}", r.solutions);
        assert_eq!(r.solutions[0].get("name").unwrap().as_str(), "Alice");
        assert_eq!(r.solutions[0].get("score").unwrap().as_str(), "100");
    }

    /// (b) SILENT swallows a remote error to ONE empty solution → the local side passes
    /// through (the three people bound by the BGP survive the join unchanged).
    #[test]
    fn service_silent_swallows_error() {
        let view = loaded_view();
        let ds = Dataset::single(&view);
        let svc = MockService(Err("remote down".to_string()));
        let q = r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p ex:name ?name .
              SERVICE SILENT <http://remote/e> { ?name ex:score ?score }
            }"#;
        let QueryOutcome::Solutions(r) =
            run_outcome_dataset_service(&ds, q, &Projection::raw(), Some(&svc)).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            r.solutions.len(),
            3,
            "SILENT pass-through: {:?}",
            r.solutions
        );
    }

    /// (c) A non-SILENT remote error propagates; a `None` client (fail-closed) errors too.
    #[test]
    fn service_error_propagates_without_silent() {
        let view = loaded_view();
        let ds = Dataset::single(&view);
        let svc = MockService(Err("remote down".to_string()));
        let q = r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p ex:name ?name .
              SERVICE <http://remote/e> { ?name ex:score ?score }
            }"#;
        assert!(
            run_outcome_dataset_service(&ds, q, &Projection::raw(), Some(&svc)).is_err(),
            "non-SILENT SERVICE error must propagate"
        );
        assert!(
            run_outcome_dataset_service(&ds, q, &Projection::raw(), None).is_err(),
            "no client bound is fail-closed"
        );
    }

    /// (d) The generated remote query round-trips through the parser, projecting the
    /// inner pattern's in-scope variables.
    #[test]
    fn service_remote_query_round_trips() {
        fn find_service_inner(p: &GraphPattern) -> Option<GraphPattern> {
            match p {
                GraphPattern::Service { inner, .. } => Some((**inner).clone()),
                GraphPattern::Join { left, right } => {
                    find_service_inner(left).or_else(|| find_service_inner(right))
                }
                GraphPattern::Project { inner, .. }
                | GraphPattern::Filter { inner, .. }
                | GraphPattern::Distinct { inner }
                | GraphPattern::Slice { inner, .. } => find_service_inner(inner),
                _ => None,
            }
        }
        let Query::Select { pattern, .. } = parse_query(
            "PREFIX ex: <http://example.org/> SELECT * WHERE { SERVICE <http://r/e> { ?s ex:p ?o } }",
        )
        .unwrap() else {
            panic!()
        };
        let inner = find_service_inner(&pattern).expect("a SERVICE node");
        let remote = build_service_query(&inner);
        assert!(
            parse_query(&remote).is_ok(),
            "generated remote query must parse: {remote}"
        );
        assert!(
            remote.contains("?s") && remote.contains("?o"),
            "projects in-scope vars: {remote}"
        );
    }

    // ── EG-127: SPARQL 1.1 builtin-function library completion ──────────────

    /// Evaluate a scalar expression via `BIND(<expr> AS ?x)` over an empty BGP.
    fn scalar(view: &GraphView, expr: &str) -> String {
        let q = format!("SELECT ?x WHERE {{ BIND({expr} AS ?x) }}");
        let res = run(view, &q).unwrap_or_else(|e| panic!("run {expr}: {e}"));
        res.solutions
            .first()
            .and_then(|s| s.get("x"))
            .unwrap_or_else(|| panic!("no ?x for {expr}"))
            .as_str()
            .to_string()
    }

    /// EG-127: hash built-ins against the canonical `"abc"` test vectors.
    #[cfg(feature = "sparql-hash")]
    #[test]
    fn eg127_hash_builtins_known_vectors() {
        let v = loaded_view();
        assert_eq!(
            scalar(&v, r#"MD5("abc")"#),
            "900150983cd24fb0d6963f7d28e17f72"
        );
        assert_eq!(
            scalar(&v, r#"SHA1("abc")"#),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            scalar(&v, r#"SHA256("abc")"#),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            scalar(&v, r#"SHA384("abc")"#),
            "cb00753f45a35e8bb5a03d699ac65007272c32ab0eded1631a8b605a43ff5bed\
8086072ba1e7cc2358baeca134c825a7"
        );
        assert_eq!(
            scalar(&v, r#"SHA512("abc")"#),
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a\
2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        );
    }

    /// EG-127: term constructors — UUID()/STRUUID() and STRDT().
    #[test]
    fn eg127_term_constructors() {
        let v = loaded_view();
        assert_eq!(scalar(&v, "STRLEN(STRUUID())"), "36");
        assert!(
            scalar(&v, "STR(UUID())").starts_with("urn:uuid:"),
            "UUID() is a urn:uuid: IRI"
        );
        // STRDT keeps the lexical value; BNODE(str) yields a labelled blank node.
        assert_eq!(
            scalar(
                &v,
                r#"STRDT("42", <http://www.w3.org/2001/XMLSchema#integer>)"#
            ),
            "42"
        );
        assert_eq!(scalar(&v, r#"STR(BNODE("tag1"))"#), "_:tag1");
    }

    /// EG-127: date-time accessors over an xsd:dateTime lexical + NOW().
    #[test]
    fn eg127_datetime_accessors() {
        let v = loaded_view();
        let dt = r#""2024-03-15T10:30:45+01:00""#;
        assert_eq!(scalar(&v, &format!("YEAR({dt})")), "2024");
        assert_eq!(scalar(&v, &format!("MONTH({dt})")), "3");
        assert_eq!(scalar(&v, &format!("DAY({dt})")), "15");
        assert_eq!(scalar(&v, &format!("HOURS({dt})")), "10");
        assert_eq!(scalar(&v, &format!("MINUTES({dt})")), "30");
        assert_eq!(scalar(&v, &format!("SECONDS({dt})")), "45");
        assert_eq!(scalar(&v, &format!("TZ({dt})")), "+01:00");
        assert_eq!(scalar(&v, &format!("TIMEZONE({dt})")), "PT1H");
        // NOW() is a well-formed xsd:dateTime whose year is in the plausible range.
        let now_year: i64 = scalar(&v, "YEAR(NOW())").parse().unwrap();
        assert!(now_year >= 2024, "NOW() year = {now_year}");
    }

    /// EG-127: REPLACE (regex-backed) + STRBEFORE/STRAFTER.
    #[test]
    fn eg127_string_extras() {
        let v = loaded_view();
        assert_eq!(scalar(&v, r#"REPLACE("abcabc", "b", "X")"#), "aXcaXc");
        assert_eq!(scalar(&v, r#"REPLACE("Foo", "o", "0", "i")"#), "F00");
        assert_eq!(scalar(&v, r#"STRBEFORE("hello@world", "@")"#), "hello");
        assert_eq!(scalar(&v, r#"STRAFTER("hello@world", "@")"#), "world");
        // Separator not found ⇒ empty string.
        assert_eq!(scalar(&v, r#"STRBEFORE("abc", "z")"#), "");
        assert_eq!(scalar(&v, r#"ENCODE_FOR_URI("a b/c")"#), "a%20b%2Fc");
    }

    /// EG-127: numeric built-ins ABS/CEIL/FLOOR/ROUND (half-toward-+inf).
    #[test]
    fn eg127_numeric_builtins() {
        let v = loaded_view();
        assert_eq!(scalar(&v, "ABS(-5)"), "5");
        assert_eq!(scalar(&v, "CEIL(1.2)"), "2");
        assert_eq!(scalar(&v, "FLOOR(1.8)"), "1");
        assert_eq!(scalar(&v, "ROUND(2.5)"), "3");
        assert_eq!(scalar(&v, "ROUND(-2.5)"), "-2");
        // RAND() is a double in [0,1).
        let r: f64 = scalar(&v, "RAND()").parse().unwrap();
        assert!((0.0..1.0).contains(&r), "RAND() = {r}");
    }

    // ── EG-130: SPARQL-star (RDF 1.2) — construct + project a quoted triple ──

    /// EG-130: TRIPLE() constructs a first-class quoted-triple term that binds to `?t`;
    /// isTRIPLE tests it and SUBJECT/PREDICATE/OBJECT project its components.
    #[cfg(feature = "sparql-star")]
    #[test]
    fn eg130_sparql_star_accessors() {
        let v = loaded_view();
        let t = "TRIPLE(<http://example.org/a>, <http://example.org/b>, <http://example.org/c>)";
        // The raw binding is the canonical quoted-triple term (STR() would strip the
        // outer angle brackets, so we read the binding directly here).
        assert!(
            scalar(&v, t).starts_with("<<"),
            "TRIPLE() yields a quoted-triple term"
        );
        assert_eq!(scalar(&v, &format!("isTRIPLE({t})")), "true");
        assert_eq!(scalar(&v, "isTRIPLE(<http://example.org/a>)"), "false");
        assert_eq!(
            scalar(&v, &format!("STR(SUBJECT({t}))")),
            "http://example.org/a"
        );
        assert_eq!(
            scalar(&v, &format!("STR(PREDICATE({t}))")),
            "http://example.org/b"
        );
        assert_eq!(
            scalar(&v, &format!("STR(OBJECT({t}))")),
            "http://example.org/c"
        );
    }

    // ── EG-261: GeoSPARQL baseline over a full SPARQL query ─────────────────────

    /// Load two features, each with the canonical `geo:hasGeometry`/`geo:asWKT` shape.
    #[cfg(feature = "geosparql")]
    fn geo_view() -> GraphView {
        let ttl = r#"
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix ex:  <http://example.org/> .
ex:cityA geo:hasGeometry ex:gA .
ex:gA    geo:asWKT "POINT(1 1)"^^geo:wktLiteral .
ex:cityB geo:hasGeometry ex:gB .
ex:gB    geo:asWKT "POINT(5 5)"^^geo:wktLiteral .
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

    /// EG-261: the `?feature geo:hasGeometry ?g . ?g geo:asWKT ?wkt` resolution pattern
    /// composes with a `geof:sfWithin` FILTER over a `geo:wktLiteral` constant — only the
    /// feature whose point lies inside the polygon survives.
    #[cfg(feature = "geosparql")]
    #[test]
    fn eg261_hasgeometry_aswkt_sfwithin_full_query() {
        let view = geo_view();
        let res = run(
            &view,
            r#"
            PREFIX geo:  <http://www.opengis.net/ont/geosparql#>
            PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
            SELECT ?f WHERE {
              ?f geo:hasGeometry ?g .
              ?g geo:asWKT ?wkt .
              FILTER(geof:sfWithin(?wkt, "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))"^^geo:wktLiteral))
            }"#,
        )
        .unwrap();
        let feats: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("f").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            feats,
            vec!["<http://example.org/cityA>"],
            "only cityA's POINT(1 1) is within the polygon; got {feats:?}"
        );
    }

    /// Load three polygon features (container + strictly-interior + boundary-tangential),
    /// each with the canonical `geo:asWKT` shape, for the RCC8 query test (CONCEPT:EG-KG.ontology.concept-7).
    #[cfg(feature = "geosparql")]
    fn rcc8_view() -> GraphView {
        let ttl = r#"
@prefix geo: <http://www.opengis.net/ont/geosparql#> .
@prefix ex:  <http://example.org/> .
ex:big   geo:asWKT "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"^^geo:wktLiteral .
ex:inner geo:asWKT "POLYGON((2 2, 4 2, 4 4, 2 4, 2 2))"^^geo:wktLiteral .
ex:edge  geo:asWKT "POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))"^^geo:wktLiteral .
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

    /// EG-155: a full SPARQL `FILTER(geof:rcc8ntpp(?pw, ?bw))` query end-to-end — the new
    /// RCC8 relation dispatches through the shared `geof:` boolean-function hook, admitting
    /// only the strictly-interior region (NTPP) and rejecting both the boundary-tangential
    /// region (that is TPP) and the container itself.
    #[cfg(feature = "geosparql")]
    #[test]
    fn eg155_sparql_filter_rcc8ntpp_full_query() {
        let view = rcc8_view();
        let res = run(
            &view,
            r#"
            PREFIX geo:  <http://www.opengis.net/ont/geosparql#>
            PREFIX geof: <http://www.opengis.net/def/function/geosparql/>
            SELECT ?part WHERE {
              ?part geo:asWKT ?pw .
              <http://example.org/big> geo:asWKT ?bw .
              FILTER(geof:rcc8ntpp(?pw, ?bw))
            }"#,
        )
        .unwrap();
        let parts: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("part").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(
            parts,
            vec!["<http://example.org/inner>"],
            "only the strictly-interior region is a non-tangential proper part; got {parts:?}"
        );
    }

    /// EG-261: `geof:distance` is usable as a projected value expression in a full query
    /// (the 3-4-5 planar triangle → 5).
    #[cfg(feature = "geosparql")]
    #[test]
    fn eg261_distance_value_in_query() {
        let view = geo_view();
        let d = scalar(
            &view,
            r#"<http://www.opengis.net/def/function/geosparql/distance>("POINT(0 0)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, "POINT(3 4)"^^<http://www.opengis.net/ont/geosparql#wktLiteral>, "")"#,
        );
        assert_eq!(d, "5", "planar distance of a 3-4-5 triangle; got {d}");
    }
}
