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
//! graph). Sub-selects lower into the same algebra, while `SERVICE` resolves through
//! the injected, fail-closed remote SPARQL source.
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
use spargebra::{Query, SparqlParser};

use crate::mapping::{cell_lexical, RDF_MULTI_VALUE_KEY};

mod graph_result;
use self::graph_result::*;
mod algebra;
use self::algebra::*;
mod aggregate;
use self::aggregate::*;
mod graph_match;
use self::graph_match::*;
mod expression;
use self::expression::*;
mod scalar;
use self::scalar::*;

mod bool_builtins;
use bool_builtins::{eval_bool_str_relation, term_type_test};
// EH-197 — witness proofs for SELECT rows (child module: it reuses the private
// pattern matcher and join rather than re-implementing them).
mod proof;
// EH-583 — multi-valued literal properties bind every value.
#[cfg(test)]
mod multivalue_tests;
pub use proof::{execute_explained, MAX_WITNESS_STEPS};

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
    SparqlParser::new()
        .parse_query(q)
        .map_err(|e| format!("sparql parse: {e}"))
}

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
        // BUG A3 (2026-08-12): each source view's TBox membership (derived at
        // ITS OWN snapshot time, `GraphCore::schema_refs`) must survive the
        // merge too -- omitting this would silently drop the schema exemption
        // for a class/property node reached only through a multi-graph
        // `FROM`/`GRAPH` merge, since `filter_view` no longer has a `_schema`
        // property key on the (now schema-blind) merged blob to fall back to.
        out.schema_node_ids
            .extend(v.schema_node_ids.iter().cloned());
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

impl QueryOutcome {
    /// Convert the typed outcome to the flat row table used by tabular wire clients.
    pub fn into_table(self) -> SparqlResult {
        match self {
            QueryOutcome::Solutions(result) => result,
            QueryOutcome::Boolean(value) => {
                let mut solution = Solution::new();
                solution.insert("ask".to_string(), Binding::Literal(value.to_string()));
                SparqlResult {
                    vars: vec!["ask".to_string()],
                    solutions: vec![solution],
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
                    .map(|triple| {
                        let mut solution = Solution::new();
                        solution.insert(
                            "subject".to_string(),
                            Binding::Node(triple.subject.to_string()),
                        );
                        solution.insert(
                            "predicate".to_string(),
                            Binding::Node(triple.predicate.to_string()),
                        );
                        solution.insert(
                            "object".to_string(),
                            Binding::Literal(triple.object.to_string()),
                        );
                        solution
                    })
                    .collect();
                SparqlResult { vars, solutions }
            }
        }
    }
}

/// Parse and evaluate any SPARQL query form over a named-graph-aware [`Dataset`].
/// This is the sole execution API. A configured remote `SERVICE` client is explicit;
/// `None` rejects non-SILENT federation rather than selecting another execution path.
pub fn execute(
    ds: &Dataset,
    query_str: &str,
    proj: &Projection,
    service: Option<&dyn RemoteSparql>,
) -> Result<QueryOutcome, String> {
    let q = parse_query(query_str)?;
    evaluate_query(ds, &q, proj, service)
}

/// Evaluate a parsed query of ANY form over a [`Dataset`] under projection `proj`.
///
/// * `SELECT`    → the projected solution table.
/// * `ASK`       → `true` iff the WHERE pattern yields ≥1 solution.
/// * `CONSTRUCT` → the WHERE solutions instantiated against the template triples.
/// * `DESCRIBE`  → the triples describing each bound resource (subject- AND
///   object-position — a minimal concise bounded description over the active graph).
fn evaluate_query(
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

#[cfg(test)]
mod tests {
    include!("sparql/tests/query.rs");
    include!("sparql/tests/filters.rs");
    include!("sparql/tests/builtins.rs");
}
