//! CONCEPT:EG-101 — OBDA virtual graphs / R2RML (Phase Q).
//!
//! Ontology-Based Data Access: expose a FOREIGN tabular source (a SQL table, a CSV, a
//! remote row set…) as a *virtual* RDF graph that SPARQL queries as if it were native
//! triples — WITHOUT ever materializing the whole dataset into the engine. The mapping
//! that turns rows into triples is an R2RML-style [`TriplesMap`]: a subject IRI template
//! plus a list of predicate→object maps. A [`VirtualGraph`] is a set of `TriplesMap`s
//! over one-or-more named foreign sources.
//!
//! ## How a SPARQL query runs against a virtual graph (the OBDA rewrite)
//!
//! A query over a virtual graph is answered by [`run_virtual`] / [`run_outcome_virtual`]:
//!
//!   1. **(needed columns)** walk the query's BGPs/paths to learn which *predicates* it
//!      references. Predicates the query never mentions cannot contribute solutions, so
//!      their object columns are never pulled — [`VirtualGraph::materialize`] computes the
//!      minimal column set (subject-template columns ∪ the object columns of the *wanted*
//!      predicate-object maps) and passes it to the source as a projection pushdown.
//!   2. **(foreign scan)** each [`TriplesMap`]'s `logical_source` is resolved in the
//!      [`ObdaSourceRegistry`] and scanned ON DEMAND for exactly those columns — the
//!      analogue of eg-plan's `Op::ForeignScan` against a [`crate`]-external row set.
//!   3. **(materialize)** each returned row is turned into triples by applying the
//!      subject/predicate/object templates (`{col}` placeholders bound from the row).
//!   4. **(join/bind)** the materialized triples are loaded into a *transient* in-memory
//!      view and the EXISTING SPARQL evaluator ([`crate::sparql`]) runs the full query
//!      over it — so BGP joins, `FILTER`, `OPTIONAL`, aggregates and projection all work
//!      unchanged. The view is discarded when the query returns; nothing is persisted.
//!
//! Only the query-relevant predicates, over only the query-relevant columns, are pulled —
//! the "no materialization of the whole graph" contract. A `VirtualGraph` spanning many
//! sources still runs in one query (each `TriplesMap` names its own source).
//!
//! ## Relationship to eg-plan federation (CONCEPT:EG-073)
//!
//! eg-plan's [`ForeignSource`]/`ForeignSourceRegistry` speaks the id+optional-score
//! `RowSet` currency (deliberately column-less — a candidate-id set for the cross-modal
//! algebra). R2RML needs *column values* to fill subject/object templates, so OBDA
//! defines a sibling, column-carrying seam — [`ObdaSource`] returning [`ForeignRow`]s —
//! mirroring the same "one scan-shaped method + a name registry" design. eg-rdf sits
//! ABOVE eg-plan in the crate DAG (eg-plan depends on eg-rdf, not the reverse), so this
//! source lives here rather than reusing the eg-plan trait. Bridging an eg-plan
//! `ForeignSource` (id-only) into an [`ObdaSource`] (single `id` column) is a documented
//! follow-up.
//!
//! ## Full R2RML Turtle parsing (CONCEPT:EG-305)
//!
//! Beyond EG-101's programmatic builders + minimal textual form, [`parse_r2rml_turtle`]
//! reads a STANDARD R2RML mapping document written in Turtle (`rr:TriplesMap`,
//! `rr:logicalTable`/`rr:tableName`, `rr:subjectMap` with `rr:template`/`rr:column`/
//! `rr:class`, `rr:predicateObjectMap` with `rr:predicate`/`rr:predicateMap` and
//! `rr:objectMap` carrying `rr:column`/`rr:template`/`rr:constant`/`rr:datatype`/
//! `rr:language`/`rr:termType`) into the SAME [`TriplesMap`]/[`ObjectMap`]/[`VirtualGraph`]
//! model. It REUSES the existing Turtle parser ([`crate::mapping::parse_turtle`]) to read
//! the RDF, then interprets the `rr:` vocabulary over the parsed triples — so typed/lang
//! object literals now materialize as `xsd:`-typed / language-tagged terms.
//!
//! SPARQL→SQL pushdown (issuing a filtered SQL query to the source rather than scanning +
//! filtering locally), `rr:sqlQuery` logical views, `rr:joinCondition` referencing object
//! maps, and dynamic (`rr:template`) predicate maps remain documented follow-ups.
//!
//! [`ForeignSource`]: ../../eg_plan/federation/trait.ForeignSource.html

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use oxrdf::{Literal, NamedNode, Subject, Term, Triple};

use crate::sparql::{Projection, QueryOutcome, SparqlResult};

/// The `rdf:type` predicate IRI (bare — used to emit `subject rdf:type <class>`).
const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

// ── the column-carrying foreign-source seam (CONCEPT:EG-101) ─────────────────────

/// A foreign-source NAME (the registry key a [`TriplesMap::logical_source`] resolves).
pub type ForeignSourceName = String;

/// One tabular row from a foreign source: column name → scalar value (lexical form).
/// Kept as strings — the lexical value is what R2RML templates and literal object maps
/// consume; typed literals are a documented follow-up.
pub type ForeignRow = HashMap<String, String>;

/// CONCEPT:EG-101 — the OBDA foreign-source seam: pull tabular rows on demand. This is
/// the column-carrying analogue of eg-plan's `ForeignSource` (which yields the id+score
/// `RowSet`); R2RML needs full columns to fill templates, so an `ObdaSource` returns
/// [`ForeignRow`]s. `scan` takes the columns the query actually needs (projection
/// pushdown) — an empty `needed` means "every column".
pub trait ObdaSource: Send + Sync {
    /// The columns this source can provide (its schema), if known. Advisory — used for
    /// validation and error messages; an empty vec means "schema not declared".
    fn columns(&self) -> Vec<String> {
        Vec::new()
    }

    /// Pull rows, projecting to `needed` when it is non-empty (the pushdown). Rows are
    /// pulled at query time — a network/read failure is an `Err` (a virtual graph over an
    /// unreachable source is a real error, not an empty result).
    fn scan(&self, needed: &BTreeSet<String>) -> Result<Vec<ForeignRow>, String>;
}

/// A boxed, thread-safe [`ObdaSource`] stored by name in an [`ObdaSourceRegistry`].
pub type SharedObdaSource = Arc<dyn ObdaSource>;

/// CONCEPT:EG-101 — an in-memory tabular source: the zero-dependency [`ObdaSource`] for
/// tests and pre-materialized foreign datasets. Mirrors eg-plan's `TableSource`, but
/// carries full columns rather than id+score.
#[derive(Clone, Debug, Default)]
pub struct TableSource {
    columns: Vec<String>,
    rows: Vec<ForeignRow>,
}

impl TableSource {
    /// Build from column names + POSITIONAL records (each record aligned to `columns`).
    /// A record shorter than `columns` leaves the trailing columns unset for that row.
    pub fn from_records<C, R>(columns: C, records: R) -> Self
    where
        C: IntoIterator<Item = String>,
        R: IntoIterator<Item = Vec<String>>,
    {
        let columns: Vec<String> = columns.into_iter().collect();
        let rows = records
            .into_iter()
            .map(|rec| {
                columns
                    .iter()
                    .cloned()
                    .zip(rec)
                    .collect::<HashMap<String, String>>()
            })
            .collect();
        Self { columns, rows }
    }

    /// Build from column names + already-keyed rows.
    pub fn from_rows<C, R>(columns: C, rows: R) -> Self
    where
        C: IntoIterator<Item = String>,
        R: IntoIterator<Item = ForeignRow>,
    {
        Self {
            columns: columns.into_iter().collect(),
            rows: rows.into_iter().collect(),
        }
    }
}

impl ObdaSource for TableSource {
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn scan(&self, needed: &BTreeSet<String>) -> Result<Vec<ForeignRow>, String> {
        if needed.is_empty() {
            return Ok(self.rows.clone());
        }
        // Projection pushdown: keep only the needed columns from each row.
        Ok(self
            .rows
            .iter()
            .map(|row| {
                needed
                    .iter()
                    .filter_map(|c| row.get(c).map(|v| (c.clone(), v.clone())))
                    .collect()
            })
            .collect())
    }
}

/// CONCEPT:EG-101 — an [`ObdaSource`] backed by a CLOSURE producing rows on demand (an
/// in-engine adapter over a dataset the host already holds). The closure receives the
/// needed-column set so it can push the projection down itself.
pub struct ClosureSource<F> {
    columns: Vec<String>,
    f: F,
}

impl<F> ClosureSource<F>
where
    F: Fn(&BTreeSet<String>) -> Result<Vec<ForeignRow>, String> + Send + Sync,
{
    /// Build from a declared schema + a row-producing closure.
    pub fn new(columns: Vec<String>, f: F) -> Self {
        Self { columns, f }
    }
}

impl<F> ObdaSource for ClosureSource<F>
where
    F: Fn(&BTreeSet<String>) -> Result<Vec<ForeignRow>, String> + Send + Sync,
{
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn scan(&self, needed: &BTreeSet<String>) -> Result<Vec<ForeignRow>, String> {
        (self.f)(needed)
    }
}

/// CONCEPT:EG-101 — the OBDA source registry: maps a foreign-source NAME to a live
/// [`ObdaSource`]. A [`TriplesMap::logical_source`] resolves through it, exactly as an
/// eg-plan `Named` foreign scan resolves through the `ForeignSourceRegistry`.
#[derive(Clone, Default)]
pub struct ObdaSourceRegistry {
    sources: HashMap<String, SharedObdaSource>,
}

impl ObdaSourceRegistry {
    /// A new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) a source under `name`.
    pub fn register(&mut self, name: impl Into<String>, source: SharedObdaSource) -> &mut Self {
        self.sources.insert(name.into(), source);
        self
    }

    /// Register an in-memory [`TableSource`] under `name` (the common test / preload path).
    pub fn register_table(&mut self, name: impl Into<String>, table: TableSource) -> &mut Self {
        self.register(name, Arc::new(table))
    }

    /// Look a source up by name.
    pub fn get(&self, name: &str) -> Option<&SharedObdaSource> {
        self.sources.get(name)
    }

    /// How many sources are registered.
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Whether NO sources are registered.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Resolve `name` to its source, or a CLEAN error naming the unbound source (and
    /// listing what IS registered). CONCEPT:EG-101.
    fn resolve(&self, name: &str) -> Result<&SharedObdaSource, String> {
        self.sources.get(name).ok_or_else(|| {
            let mut known: Vec<&str> = self.sources.keys().map(String::as_str).collect();
            known.sort_unstable();
            format!(
                "obda: no foreign source registered under name '{name}' \
                 (registered: {known:?}) (CONCEPT:EG-101)"
            )
        })
    }
}

// ── the R2RML-style mapping model (CONCEPT:EG-101) ───────────────────────────────

/// CONCEPT:EG-101 — how the OBJECT term of a triple is produced from a foreign row (an
/// R2RML `rr:objectMap`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectMap {
    /// A LITERAL from a column value (`rr:column`). A missing/empty column ⇒ no triple.
    Column(String),
    /// A TYPED literal from a column value with an explicit datatype IRI (`rr:column` +
    /// `rr:datatype`, e.g. `xsd:integer`). A missing/empty column ⇒ no triple.
    /// CONCEPT:EG-305 — typed R2RML object maps.
    TypedColumn(String, String),
    /// A LANGUAGE-TAGGED literal from a column value (`rr:column` + `rr:language`, e.g.
    /// `en`). A missing/empty column ⇒ no triple. CONCEPT:EG-305.
    LangColumn(String, String),
    /// An IRI built from a `{col}` template (`rr:template` — a reference / join to
    /// another entity's subject IRI).
    Template(String),
    /// A constant IRI object (`rr:constant`, an IRI).
    ConstantIri(String),
    /// A constant literal object (`rr:constant`, a literal).
    ConstantLiteral(String),
    /// A constant TYPED literal object (`rr:constant` + `rr:datatype`).
    /// CONCEPT:EG-305.
    TypedConstantLiteral(String, String),
}

impl ObjectMap {
    /// The foreign columns this object map references (added to `out`).
    fn columns_into(&self, out: &mut BTreeSet<String>) {
        match self {
            ObjectMap::Column(c)
            | ObjectMap::TypedColumn(c, _)
            | ObjectMap::LangColumn(c, _) => {
                out.insert(c.clone());
            }
            ObjectMap::Template(t) => template_columns_into(t, out),
            ObjectMap::ConstantIri(_)
            | ObjectMap::ConstantLiteral(_)
            | ObjectMap::TypedConstantLiteral(_, _) => {}
        }
    }

    /// Build the object [`Term`] for one row (`None` ⇒ omit the triple for this row).
    fn term_for(&self, row: &ForeignRow) -> Option<Term> {
        match self {
            ObjectMap::Column(c) => {
                let v = row.get(c)?;
                if v.is_empty() {
                    return None;
                }
                Some(Term::Literal(Literal::new_simple_literal(v)))
            }
            ObjectMap::TypedColumn(c, dt) => {
                let v = row.get(c)?;
                if v.is_empty() {
                    return None;
                }
                let datatype = NamedNode::new(dt).ok()?;
                Some(Term::Literal(Literal::new_typed_literal(v, datatype)))
            }
            ObjectMap::LangColumn(c, lang) => {
                let v = row.get(c)?;
                if v.is_empty() {
                    return None;
                }
                Some(Term::Literal(
                    Literal::new_language_tagged_literal(v, lang).ok()?,
                ))
            }
            ObjectMap::Template(t) => {
                let iri = expand_template(t, row)?;
                Some(Term::NamedNode(NamedNode::new(iri).ok()?))
            }
            ObjectMap::ConstantIri(iri) => Some(Term::NamedNode(NamedNode::new(iri).ok()?)),
            ObjectMap::ConstantLiteral(v) => Some(Term::Literal(Literal::new_simple_literal(v))),
            ObjectMap::TypedConstantLiteral(v, dt) => {
                let datatype = NamedNode::new(dt).ok()?;
                Some(Term::Literal(Literal::new_typed_literal(v, datatype)))
            }
        }
    }
}

/// CONCEPT:EG-101 — an R2RML `rr:TriplesMap`: one foreign source (`logical_source`), a
/// subject-IRI template, an optional `rdf:type` class, and the predicate→object maps that
/// turn each row into triples.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TriplesMap {
    /// The foreign-source name this map reads (resolved in the [`ObdaSourceRegistry`]).
    pub logical_source: ForeignSourceName,
    /// The subject-IRI template, e.g. `http://example.org/person/{id}` (`rr:subjectMap`
    /// `rr:template`). `{col}` placeholders bind from the row.
    pub subject_template: String,
    /// Optional class IRI — emits `subject rdf:type <class>` per row (`rr:class`).
    pub subject_class: Option<String>,
    /// `(predicate IRI, object map)` pairs — the `rr:predicateObjectMap`s.
    pub predicate_object_maps: Vec<(String, ObjectMap)>,
}

impl TriplesMap {
    /// Builder: a triples map over `source` with subject template `subject_template`.
    pub fn new(source: impl Into<ForeignSourceName>, subject_template: impl Into<String>) -> Self {
        Self {
            logical_source: source.into(),
            subject_template: subject_template.into(),
            subject_class: None,
            predicate_object_maps: Vec::new(),
        }
    }

    /// Set the `rdf:type` class (builder).
    pub fn with_class(mut self, class_iri: impl Into<String>) -> Self {
        self.subject_class = Some(class_iri.into());
        self
    }

    /// Add a predicate→literal-column map (builder).
    pub fn add_column(mut self, predicate: impl Into<String>, column: impl Into<String>) -> Self {
        self.predicate_object_maps
            .push((predicate.into(), ObjectMap::Column(column.into())));
        self
    }

    /// Add a predicate→typed-literal-column map with an explicit datatype IRI (builder).
    /// CONCEPT:EG-305.
    pub fn add_typed_column(
        mut self,
        predicate: impl Into<String>,
        column: impl Into<String>,
        datatype: impl Into<String>,
    ) -> Self {
        self.predicate_object_maps.push((
            predicate.into(),
            ObjectMap::TypedColumn(column.into(), datatype.into()),
        ));
        self
    }

    /// Add a predicate→IRI-template object map (a reference to another entity) (builder).
    pub fn add_ref(
        mut self,
        predicate: impl Into<String>,
        object_template: impl Into<String>,
    ) -> Self {
        self.predicate_object_maps.push((
            predicate.into(),
            ObjectMap::Template(object_template.into()),
        ));
        self
    }

    /// Add a fully-specified predicate→object map (builder).
    pub fn add(mut self, predicate: impl Into<String>, object: ObjectMap) -> Self {
        self.predicate_object_maps.push((predicate.into(), object));
        self
    }

    /// Whether this map's `rdf:type` triple is wanted under `wanted` (`None` ⇒ all).
    fn class_wanted(&self, wanted: &Option<BTreeSet<String>>) -> bool {
        self.subject_class.is_some()
            && wanted
                .as_ref()
                .map(|w| w.contains(RDF_TYPE_IRI))
                .unwrap_or(true)
    }
}

// ── the virtual graph (CONCEPT:EG-101) ───────────────────────────────────────────

/// CONCEPT:EG-101 — a VIRTUAL RDF graph: a set of [`TriplesMap`]s over one-or-more named
/// foreign sources. A SPARQL query runs against it via [`run_virtual`] without any of the
/// backing rows being persisted.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VirtualGraph {
    /// The triples maps that define the virtual graph.
    pub triples_maps: Vec<TriplesMap>,
}

impl VirtualGraph {
    /// An empty virtual graph.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a triples map (builder).
    pub fn with_map(mut self, map: TriplesMap) -> Self {
        self.triples_maps.push(map);
        self
    }

    /// CONCEPT:EG-101 — materialize the virtual graph's triples by scanning each backing
    /// source ON DEMAND. `wanted_predicates` restricts materialization to the given
    /// predicate IRIs (`None` ⇒ every predicate) — the BGP-driven pushdown: only those
    /// predicate-object maps run, and only their columns (plus the subject-template
    /// columns) are pulled from the source. Returns the materialized RDF triples.
    pub fn materialize(
        &self,
        reg: &ObdaSourceRegistry,
        wanted_predicates: &Option<BTreeSet<String>>,
    ) -> Result<Vec<Triple>, String> {
        let mut out = Vec::new();
        for map in &self.triples_maps {
            // Which predicate-object maps are wanted for this query?
            let active_poms: Vec<&(String, ObjectMap)> = map
                .predicate_object_maps
                .iter()
                .filter(|(pred, _)| match wanted_predicates {
                    None => true,
                    Some(w) => w.contains(pred),
                })
                .collect();
            let class_wanted = map.class_wanted(wanted_predicates);

            // Nothing from this map contributes to the query — skip its source scan.
            if active_poms.is_empty() && !class_wanted {
                continue;
            }

            // Needed columns = subject-template columns ∪ the active object columns.
            let mut needed = BTreeSet::new();
            template_columns_into(&map.subject_template, &mut needed);
            for (_, obj) in &active_poms {
                obj.columns_into(&mut needed);
            }

            let source = reg.resolve(&map.logical_source)?;
            let rows = source.scan(&needed)?;

            for row in &rows {
                let Some(subject_iri) = expand_template(&map.subject_template, row) else {
                    continue; // a subject-template column is missing for this row
                };
                let Ok(subject) = NamedNode::new(&subject_iri) else {
                    continue; // a template that doesn't yield a valid IRI is skipped
                };

                if class_wanted {
                    if let Some(class) = &map.subject_class {
                        if let (Ok(pred), Ok(obj)) =
                            (NamedNode::new(RDF_TYPE_IRI), NamedNode::new(class))
                        {
                            out.push(Triple::new(subject.clone(), pred, obj));
                        }
                    }
                }

                for (pred, obj) in &active_poms {
                    let Ok(predicate) = NamedNode::new(pred) else {
                        continue;
                    };
                    if let Some(object) = obj.term_for(row) {
                        out.push(Triple::new(subject.clone(), predicate, object));
                    }
                }
            }
        }
        Ok(out)
    }

    /// CONCEPT:EG-101 — materialize the ENTIRE virtual graph (every predicate, every
    /// column). Convenience for tests / debugging; the query path uses the pushdown
    /// [`materialize`](Self::materialize) so it never pulls the whole dataset.
    pub fn materialize_all(&self, reg: &ObdaSourceRegistry) -> Result<Vec<Triple>, String> {
        self.materialize(reg, &None)
    }
}

// ── SPARQL over a virtual graph (CONCEPT:EG-101) ─────────────────────────────────

/// CONCEPT:EG-101 — parse + evaluate a SPARQL SELECT (or any form, coerced to rows)
/// against a VIRTUAL graph, using the IDENTITY projection. See
/// [`run_outcome_virtual`] for the typed outcome and a custom projection.
///
/// The virtual-graph rewrite pulls only the query-relevant predicates/columns from the
/// backing [`ObdaSource`]s, materializes triples via the [`TriplesMap`]s, and runs the
/// existing SPARQL evaluator over the transient result — so BGP joins, `FILTER`,
/// `OPTIONAL`, aggregates and projection all work unchanged. Existing non-virtual SPARQL
/// entry points are untouched.
pub fn run_virtual(
    vg: &VirtualGraph,
    reg: &ObdaSourceRegistry,
    query_str: &str,
) -> Result<SparqlResult, String> {
    match run_outcome_virtual(vg, reg, query_str, &Projection::raw())? {
        QueryOutcome::Solutions(res) => Ok(res),
        QueryOutcome::Boolean(b) => Ok(bool_result(b)),
        #[cfg(feature = "rdf")]
        QueryOutcome::Graph(_) => Ok(SparqlResult {
            vars: Vec::new(),
            solutions: Vec::new(),
        }),
    }
}

/// CONCEPT:EG-101 — parse + evaluate a SPARQL query of ANY form against a VIRTUAL graph,
/// returning the typed [`QueryOutcome`] and projecting materialized terms under `proj`.
pub fn run_outcome_virtual(
    vg: &VirtualGraph,
    reg: &ObdaSourceRegistry,
    query_str: &str,
    proj: &Projection,
) -> Result<QueryOutcome, String> {
    // (1) parse, then learn which predicates the query references (the pushdown key).
    let query = crate::sparql::parse_query(query_str)?;
    let wanted = wanted_predicates(&query);

    // (2)+(3) scan the backing source(s) on demand for only the needed columns and
    // materialize the query-relevant triples via the TriplesMaps.
    let triples = vg.materialize(reg, &wanted)?;

    // (4) load into a TRANSIENT view and run the existing evaluator (joins/filters/…).
    let view = build_view(triples)?;
    crate::sparql::run_outcome(&view, query_str, proj)
}

/// Build a transient [`GraphView`] from materialized triples (nothing persisted).
fn build_view(triples: Vec<Triple>) -> Result<eg_core::graph::GraphView, String> {
    let core = eg_core::graph::GraphCore::new();
    let mut iris = crate::mapping::IriStore::default();
    crate::mapping::load_triples(
        &core,
        &mut iris,
        "__obda_virtual__",
        triples,
        #[cfg(feature = "rdf-redb")]
        None,
    )?;
    Ok(core.analysis_snapshot())
}

/// A one-row boolean result table (`ASK` over a virtual graph).
fn bool_result(b: bool) -> SparqlResult {
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

/// CONCEPT:EG-101 — the set of predicate IRIs a query's triple patterns reference, or
/// `None` when the pushdown cannot be narrowed (a variable predicate, a property path, or
/// an unrecognized algebra node) — in which case EVERY predicate is materialized so the
/// answer stays complete. This is the projection/predicate pushdown key.
fn wanted_predicates(query: &spargebra::Query) -> Option<BTreeSet<String>> {
    use spargebra::Query;
    let pattern = match query {
        Query::Select { pattern, .. }
        | Query::Construct { pattern, .. }
        | Query::Describe { pattern, .. }
        | Query::Ask { pattern, .. } => pattern,
    };
    let mut preds = BTreeSet::new();
    let mut unrestricted = false;
    collect_predicates(pattern, &mut preds, &mut unrestricted);
    if unrestricted {
        None
    } else {
        Some(preds)
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

// ── template helpers (CONCEPT:EG-101) ────────────────────────────────────────────

/// Expand a `{col}` template against a row: every `{col}` is replaced by `row[col]`.
/// Returns `None` if any referenced column is missing OR empty (an incomplete key
/// yields no term rather than a malformed IRI). A `{{`/`}}` is a literal brace.
fn expand_template(tpl: &str, row: &ForeignRow) -> Option<String> {
    let mut out = String::with_capacity(tpl.len());
    let mut chars = tpl.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' => {
                if chars.peek() == Some(&'{') {
                    chars.next();
                    out.push('{');
                    continue;
                }
                let mut name = String::new();
                let mut closed = false;
                for nc in chars.by_ref() {
                    if nc == '}' {
                        closed = true;
                        break;
                    }
                    name.push(nc);
                }
                if !closed {
                    return None; // unbalanced `{`
                }
                let val = row.get(&name)?;
                if val.is_empty() {
                    return None;
                }
                out.push_str(val);
            }
            '}' => {
                if chars.peek() == Some(&'}') {
                    chars.next();
                    out.push('}');
                } else {
                    out.push('}');
                }
            }
            other => out.push(other),
        }
    }
    Some(out)
}

/// Collect the `{col}` placeholder names referenced by a template into `out`.
fn template_columns_into(tpl: &str, out: &mut BTreeSet<String>) {
    let mut chars = tpl.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            if chars.peek() == Some(&'{') {
                chars.next();
                continue;
            }
            let mut name = String::new();
            let mut closed = false;
            for nc in chars.by_ref() {
                if nc == '}' {
                    closed = true;
                    break;
                }
                name.push(nc);
            }
            if closed && !name.is_empty() {
                out.insert(name);
            }
        }
    }
}

// ── minimal textual mapping form (CONCEPT:EG-101) ────────────────────────────────

/// CONCEPT:EG-101 — parse a COMPACT textual mapping into a [`VirtualGraph`]. This is the
/// lightweight alternative to full R2RML Turtle (a documented follow-up). One
/// `TriplesMap` per block; directives are one-per-line:
///
/// ```text
/// SOURCE people                                  # the foreign-source name
/// SUBJECT http://example.org/person/{id}         # subject IRI template
/// CLASS   http://example.org/Person              # optional rdf:type class
/// COLUMN  http://example.org/name  name          # predicate  <- literal column
/// REF     http://example.org/knows http://example.org/person/{friend_id}  # predicate <- IRI template
/// CONST   http://example.org/kind  http://example.org/Human  # predicate <- constant IRI (has "://")
/// ---                                            # block separator → next TriplesMap
/// ```
///
/// Blank lines and `#` comments are ignored. `CONST` treats a value containing `://` as
/// an IRI, otherwise a literal.
pub fn parse_mapping(text: &str) -> Result<VirtualGraph, String> {
    let mut vg = VirtualGraph::new();
    let mut cur: Option<PartialMap> = None;

    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if line == "---" {
            if let Some(pm) = cur.take() {
                vg.triples_maps.push(pm.finish(lineno)?);
            }
            continue;
        }
        let mut parts = line.splitn(2, char::is_whitespace);
        let directive = parts.next().unwrap_or("").to_ascii_uppercase();
        let rest = parts.next().unwrap_or("").trim();
        let pm = cur.get_or_insert_with(PartialMap::default);
        match directive.as_str() {
            "SOURCE" => pm.source = Some(rest.to_string()),
            "SUBJECT" => pm.subject = Some(rest.to_string()),
            "CLASS" => pm.class = Some(rest.to_string()),
            "COLUMN" | "REF" | "CONST" => {
                let mut kv = rest.splitn(2, char::is_whitespace);
                let pred = kv.next().unwrap_or("").trim().to_string();
                let val = kv.next().unwrap_or("").trim().to_string();
                if pred.is_empty() || val.is_empty() {
                    return Err(format!(
                        "obda: line {}: `{directive}` needs `<predicate> <value>` (CONCEPT:EG-101)",
                        lineno + 1
                    ));
                }
                let obj = match directive.as_str() {
                    "COLUMN" => ObjectMap::Column(val),
                    "REF" => ObjectMap::Template(val),
                    _ if val.contains("://") => ObjectMap::ConstantIri(val),
                    _ => ObjectMap::ConstantLiteral(val),
                };
                pm.poms.push((pred, obj));
            }
            other => {
                return Err(format!(
                    "obda: line {}: unknown directive `{other}` (CONCEPT:EG-101)",
                    lineno + 1
                ));
            }
        }
    }
    if let Some(pm) = cur.take() {
        vg.triples_maps.push(pm.finish(text.lines().count())?);
    }
    if vg.triples_maps.is_empty() {
        return Err("obda: mapping defined no TriplesMap (CONCEPT:EG-101)".into());
    }
    Ok(vg)
}

#[derive(Default)]
struct PartialMap {
    source: Option<String>,
    subject: Option<String>,
    class: Option<String>,
    poms: Vec<(String, ObjectMap)>,
}

impl PartialMap {
    fn finish(self, lineno: usize) -> Result<TriplesMap, String> {
        let source = self
            .source
            .ok_or_else(|| format!("obda: TriplesMap ending line {lineno} has no SOURCE"))?;
        let subject = self
            .subject
            .ok_or_else(|| format!("obda: TriplesMap ending line {lineno} has no SUBJECT"))?;
        Ok(TriplesMap {
            logical_source: source,
            subject_template: subject,
            subject_class: self.class,
            predicate_object_maps: self.poms,
        })
    }
}

// ── full R2RML Turtle parsing (CONCEPT:EG-305) ───────────────────────────────────

// R2RML term IRIs, built from the `rr:` vocabulary namespace
// (`http://www.w3.org/ns/r2rml#`) at compile time.
macro_rules! rr {
    ($name:literal) => {
        concat!("http://www.w3.org/ns/r2rml#", $name)
    };
}

/// A flat index of the parsed R2RML Turtle: subject-node-key → `(predicate IRI, object)`.
/// The key is the canonical `<iri>` / `_:bnode` id ([`node_key`]) so a term map / logical
/// table referenced by another node can be followed. CONCEPT:EG-305.
struct R2rmlDoc {
    by_subject: HashMap<String, Vec<(String, Term)>>,
}

impl R2rmlDoc {
    /// Build the index from a parsed Turtle triple set.
    fn from_triples(triples: &[Triple]) -> Self {
        let mut by_subject: HashMap<String, Vec<(String, Term)>> = HashMap::new();
        for t in triples {
            by_subject
                .entry(subject_key(&t.subject))
                .or_default()
                .push((t.predicate.as_str().to_string(), t.object.clone()));
        }
        Self { by_subject }
    }

    /// Every object of `subject`'s `pred` edges (cloned — the R2RML docs are small).
    fn objects(&self, subject: &str, pred: &str) -> Vec<Term> {
        self.by_subject
            .get(subject)
            .map(|pairs| {
                pairs
                    .iter()
                    .filter(|(p, _)| p.as_str() == pred)
                    .map(|(_, o)| o.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The first object of `subject`'s `pred` edges.
    fn first_object(&self, subject: &str, pred: &str) -> Option<Term> {
        self.objects(subject, pred).into_iter().next()
    }

    /// Whether `subject` is `rdf:type ty`.
    fn has_type(&self, subject: &str, ty: &str) -> bool {
        self.objects(subject, RDF_TYPE_IRI)
            .iter()
            .any(|o| matches!(o, Term::NamedNode(n) if n.as_str() == ty))
    }
}

/// Canonical node key for an RDF subject (`<iri>` or `_:bnode`).
fn subject_key(s: &Subject) -> String {
    match s {
        Subject::NamedNode(n) => format!("<{}>", n.as_str()),
        Subject::BlankNode(b) => format!("_:{}", b.as_str()),
        #[allow(unreachable_patterns)]
        _ => String::new(), // RDF-star quoted-triple subject — not an R2RML node.
    }
}

/// Canonical node key for an IRI/bnode TERM (so it can be followed as a subject); `None`
/// for a literal (literals are leaves, never term-map anchors).
fn node_key(t: &Term) -> Option<String> {
    match t {
        Term::NamedNode(n) => Some(format!("<{}>", n.as_str())),
        Term::BlankNode(b) => Some(format!("_:{}", b.as_str())),
        _ => None,
    }
}

/// The IRI of a term, if it is a `NamedNode`.
fn iri_value(t: &Term) -> Option<String> {
    match t {
        Term::NamedNode(n) => Some(n.as_str().to_string()),
        _ => None,
    }
}

/// The lexical value of a term, if it is a `Literal`.
fn literal_value(t: &Term) -> Option<String> {
    match t {
        Term::Literal(l) => Some(l.value().to_string()),
        _ => None,
    }
}

/// Strip an R2RML SQL identifier's optional surrounding double-quotes (`"EMP"` → `EMP`).
fn unquote_identifier(s: &str) -> String {
    s.trim_matches('"').to_string()
}

/// CONCEPT:EG-305 — parse a STANDARD R2RML mapping document (in Turtle) into a
/// [`VirtualGraph`]. This reuses the existing Turtle parser ([`crate::mapping::parse_turtle`])
/// to read the RDF, then interprets the `rr:` vocabulary over the parsed triples, mapping
/// onto the EG-101 [`TriplesMap`]/[`ObjectMap`] model.
///
/// Supported R2RML constructs:
/// - `rr:TriplesMap` (recognized by `rr:logicalTable`/`rr:subjectMap`/`rr:subject`/type).
/// - `rr:logicalTable` → `rr:tableName` (or `rr:sqlQuery` best-effort) → `logical_source`.
/// - `rr:subjectMap` with `rr:template` / `rr:column` (→ `{col}`) / `rr:constant`, plus
///   one-or-more `rr:class` (first → `subject_class`, extras → `rdf:type` object maps);
///   `rr:subject` constant shortcut.
/// - `rr:predicateObjectMap` with `rr:predicate` / `rr:predicateMap`→`rr:constant`, and
///   `rr:object` (constant shortcut) / `rr:objectMap` carrying `rr:column` /
///   `rr:template` / `rr:constant`, refined by `rr:datatype` (typed literal),
///   `rr:language` (lang literal) and `rr:termType` (`rr:IRI` on a column ⇒ IRI object).
///   Multiple predicates × multiple object maps expand to the R2RML cross-product.
///
/// Returns a `Result` (Turtle can fail to parse, and a malformed map is a real error).
/// The EG-101 programmatic + [`parse_mapping`] builders are unaffected.
pub fn parse_r2rml_turtle(doc: &str) -> Result<VirtualGraph, String> {
    let triples = crate::mapping::parse_turtle(doc)?;
    let index = R2rmlDoc::from_triples(&triples);

    // Discover triples-map anchors: any node carrying the R2RML TriplesMap shape.
    let mut tm_keys: BTreeSet<String> = BTreeSet::new();
    for (key, pairs) in &index.by_subject {
        let is_tm = index.has_type(key, rr!("TriplesMap"))
            || pairs.iter().any(|(p, _)| {
                matches!(
                    p.as_str(),
                    rr!("logicalTable") | rr!("subjectMap") | rr!("subject")
                )
            });
        if is_tm {
            tm_keys.insert(key.clone());
        }
    }
    if tm_keys.is_empty() {
        return Err("obda: R2RML document defines no rr:TriplesMap (CONCEPT:EG-305)".into());
    }

    let mut vg = VirtualGraph::new();
    for tm_key in &tm_keys {
        vg.triples_maps
            .push(parse_triples_map(&index, tm_key)?);
    }
    Ok(vg)
}

/// Parse one R2RML triples map (anchored at `tm_key`) into a [`TriplesMap`]. CONCEPT:EG-305.
fn parse_triples_map(index: &R2rmlDoc, tm_key: &str) -> Result<TriplesMap, String> {
    // (1) logical source: rr:logicalTable → rr:tableName (or rr:sqlQuery best-effort).
    let logical_source = index
        .first_object(tm_key, rr!("logicalTable"))
        .and_then(|lt| node_key(&lt))
        .and_then(|lt_key| {
            index
                .first_object(&lt_key, rr!("tableName"))
                .and_then(|t| literal_value(&t))
                .map(|n| unquote_identifier(&n))
                .or_else(|| {
                    index
                        .first_object(&lt_key, rr!("sqlQuery"))
                        .and_then(|t| literal_value(&t))
                })
        })
        .ok_or_else(|| {
            format!(
                "obda: R2RML triples map {tm_key} has no rr:logicalTable with \
                 rr:tableName/rr:sqlQuery (CONCEPT:EG-305)"
            )
        })?;

    // (2) subject map: rr:subjectMap [ rr:template|rr:column|rr:constant ; rr:class* ] or
    //     the rr:subject constant shortcut.
    let mut subject_class: Option<String> = None;
    let mut extra_class_poms: Vec<(String, ObjectMap)> = Vec::new();
    let subject_template = if let Some(sm) = index
        .first_object(tm_key, rr!("subjectMap"))
        .and_then(|t| node_key(&t))
    {
        // Classes: first → subject_class; any extras → rdf:type constant object maps so
        // no class is dropped (the struct carries a single subject_class).
        for class in index.objects(&sm, rr!("class")) {
            if let Some(iri) = iri_value(&class) {
                if subject_class.is_none() {
                    subject_class = Some(iri);
                } else {
                    extra_class_poms
                        .push((RDF_TYPE_IRI.to_string(), ObjectMap::ConstantIri(iri)));
                }
            }
        }
        subject_term_string(index, &sm).ok_or_else(|| {
            format!(
                "obda: rr:subjectMap of {tm_key} has no rr:template/rr:column/rr:constant \
                 (CONCEPT:EG-305)"
            )
        })?
    } else if let Some(subj) = index
        .first_object(tm_key, rr!("subject"))
        .and_then(|t| iri_value(&t))
    {
        // rr:subject constant shortcut (a fixed subject IRI, no placeholders).
        subj
    } else {
        return Err(format!(
            "obda: R2RML triples map {tm_key} has no rr:subjectMap/rr:subject (CONCEPT:EG-305)"
        ));
    };

    let mut predicate_object_maps: Vec<(String, ObjectMap)> = Vec::new();

    // (3) predicate-object maps: rr:predicateObjectMap [ rr:predicate* ; rr:object(Map)* ].
    for pom in index.objects(tm_key, rr!("predicateObjectMap")) {
        let Some(pom_key) = node_key(&pom) else {
            continue;
        };

        // Predicates: rr:predicate <IRI> and rr:predicateMap [ rr:constant <IRI> ].
        let mut predicates: Vec<String> = Vec::new();
        for p in index.objects(&pom_key, rr!("predicate")) {
            if let Some(iri) = iri_value(&p) {
                predicates.push(iri);
            }
        }
        for pm in index.objects(&pom_key, rr!("predicateMap")) {
            if let Some(pm_key) = node_key(&pm) {
                if let Some(iri) = index
                    .first_object(&pm_key, rr!("constant"))
                    .and_then(|t| iri_value(&t))
                {
                    predicates.push(iri);
                }
            }
        }

        // Object maps: rr:object <term> (constant shortcut) and rr:objectMap [ … ].
        let mut object_maps: Vec<ObjectMap> = Vec::new();
        for o in index.objects(&pom_key, rr!("object")) {
            object_maps.push(constant_object_map(&o));
        }
        for om in index.objects(&pom_key, rr!("objectMap")) {
            if let Some(om_key) = node_key(&om) {
                if let Some(obj) = parse_object_map(index, &om_key) {
                    object_maps.push(obj);
                }
            }
        }

        // R2RML cross-product: each predicate × each object map.
        for pred in &predicates {
            for obj in &object_maps {
                predicate_object_maps.push((pred.clone(), obj.clone()));
            }
        }
    }

    predicate_object_maps.extend(extra_class_poms);

    Ok(TriplesMap {
        logical_source,
        subject_template,
        subject_class,
        predicate_object_maps,
    })
}

/// Resolve a subject term map's IRI-producing form to a subject template string:
/// `rr:template "…{col}…"`, `rr:column "c"` (→ `{c}`), or `rr:constant <IRI>`.
fn subject_term_string(index: &R2rmlDoc, sm_key: &str) -> Option<String> {
    if let Some(t) = index
        .first_object(sm_key, rr!("template"))
        .and_then(|t| literal_value(&t))
    {
        return Some(t);
    }
    if let Some(c) = index
        .first_object(sm_key, rr!("column"))
        .and_then(|t| literal_value(&t))
    {
        return Some(format!("{{{c}}}"));
    }
    index
        .first_object(sm_key, rr!("constant"))
        .and_then(|t| iri_value(&t))
}

/// Interpret a constant object (`rr:object`): an IRI ⇒ [`ObjectMap::ConstantIri`], a
/// datatyped literal ⇒ [`ObjectMap::TypedConstantLiteral`], else a plain constant literal.
fn constant_object_map(term: &Term) -> ObjectMap {
    match term {
        Term::NamedNode(n) => ObjectMap::ConstantIri(n.as_str().to_string()),
        Term::Literal(l) => {
            let dt = l.datatype();
            // A plain (xsd:string) literal → simple constant; anything else keeps its type.
            if dt.as_str() == "http://www.w3.org/2001/XMLSchema#string" {
                ObjectMap::ConstantLiteral(l.value().to_string())
            } else {
                ObjectMap::TypedConstantLiteral(l.value().to_string(), dt.as_str().to_string())
            }
        }
        _ => ObjectMap::ConstantLiteral(String::new()),
    }
}

/// Parse an `rr:objectMap` term map node into an [`ObjectMap`]. CONCEPT:EG-305:
/// `rr:column` (+ `rr:datatype`/`rr:language`/`rr:termType rr:IRI`), `rr:template`, or
/// `rr:constant`.
fn parse_object_map(index: &R2rmlDoc, om_key: &str) -> Option<ObjectMap> {
    let term_type = index
        .first_object(om_key, rr!("termType"))
        .and_then(|t| iri_value(&t));
    let datatype = index
        .first_object(om_key, rr!("datatype"))
        .and_then(|t| iri_value(&t));
    let language = index
        .first_object(om_key, rr!("language"))
        .and_then(|t| literal_value(&t));

    if let Some(col) = index
        .first_object(om_key, rr!("column"))
        .and_then(|t| literal_value(&t))
    {
        // rr:termType rr:IRI on a column ⇒ the column value IS an IRI object.
        if term_type.as_deref() == Some(rr!("IRI")) {
            return Some(ObjectMap::Template(format!("{{{col}}}")));
        }
        if let Some(dt) = datatype {
            return Some(ObjectMap::TypedColumn(col, dt));
        }
        if let Some(lang) = language {
            return Some(ObjectMap::LangColumn(col, lang));
        }
        return Some(ObjectMap::Column(col));
    }

    if let Some(tpl) = index
        .first_object(om_key, rr!("template"))
        .and_then(|t| literal_value(&t))
    {
        // A template defaults to an IRI term; rr:termType rr:Literal is a documented
        // follow-up (our Template variant always yields an IRI).
        return Some(ObjectMap::Template(tpl));
    }

    index
        .first_object(om_key, rr!("constant"))
        .map(|c| constant_object_map(&c))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `people` source: id, name, age, plus a `friend_id` reference column.
    fn people_registry() -> ObdaSourceRegistry {
        let table = TableSource::from_records(
            ["id", "name", "age", "friend_id"].map(String::from),
            [
                vec!["1".into(), "Alice".into(), "30".into(), "2".into()],
                vec!["2".into(), "Bob".into(), "25".into(), "".into()],
                vec!["3".into(), "Carol".into(), "40".into(), "1".into()],
            ],
        );
        let mut reg = ObdaSourceRegistry::new();
        reg.register_table("people", table);
        reg
    }

    /// The virtual graph over `people`: Person subjects with name/age + a `knows` ref.
    fn people_vgraph() -> VirtualGraph {
        VirtualGraph::new().with_map(
            TriplesMap::new("people", "http://example.org/person/{id}")
                .with_class("http://example.org/Person")
                .add_column("http://example.org/name", "name")
                .add_column("http://example.org/age", "age")
                .add_ref(
                    "http://example.org/knows",
                    "http://example.org/person/{friend_id}",
                ),
        )
    }

    /// CONCEPT:EG-101 — a SPARQL SELECT over a virtual graph returns rows materialized via
    /// the TriplesMap; subject/predicate/object templates apply.
    #[test]
    fn eg101_virtual_graph_select_materializes_via_triples_map() {
        let reg = people_registry();
        let vg = people_vgraph();
        let res = run_virtual(
            &vg,
            &reg,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE { ?p a ex:Person ; ex:name ?name . }"#,
        )
        .unwrap();
        assert!(res.vars.contains(&"name".to_string()));
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
    }

    /// CONCEPT:EG-101 — the SUBJECT template is applied: selecting `?p` yields the
    /// templated subject IRIs, not the raw column values.
    #[test]
    fn eg101_subject_template_applies() {
        let reg = people_registry();
        let vg = people_vgraph();
        let res = run_virtual(
            &vg,
            &reg,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?p WHERE { ?p ex:name "Alice" . }"#,
        )
        .unwrap();
        let subjects: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("p").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(subjects, vec!["<http://example.org/person/1>"]);
    }

    /// CONCEPT:EG-101 — a 2-pattern BGP joins across the virtual graph: `?a knows ?b`
    /// then `?b name ?bname` binds via the object IRI template ↔ subject IRI template.
    #[test]
    fn eg101_bgp_two_patterns_join_over_virtual_graph() {
        let reg = people_registry();
        let vg = people_vgraph();
        let res = run_virtual(
            &vg,
            &reg,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?aname ?bname WHERE {
              ?a ex:name ?aname .
              ?a ex:knows ?b .
              ?b ex:name ?bname .
            }"#,
        )
        .unwrap();
        let mut pairs: Vec<(String, String)> = res
            .solutions
            .iter()
            .filter_map(|s| {
                Some((
                    s.get("aname")?.as_str().to_string(),
                    s.get("bname")?.as_str().to_string(),
                ))
            })
            .collect();
        pairs.sort();
        // Alice(1)→knows→Bob(2); Carol(3)→knows→Alice(1). Bob has no friend_id.
        assert_eq!(
            pairs,
            vec![
                ("Alice".to_string(), "Bob".to_string()),
                ("Carol".to_string(), "Alice".to_string()),
            ]
        );
    }

    /// CONCEPT:EG-101 — a FILTER over materialized virtual triples works (the existing
    /// evaluator runs unchanged over the transient view).
    #[test]
    fn eg101_filter_over_virtual_graph() {
        let reg = people_registry();
        let vg = people_vgraph();
        let res = run_virtual(
            &vg,
            &reg,
            r#"
            PREFIX ex: <http://example.org/>
            SELECT ?name WHERE {
              ?p ex:name ?name ; ex:age ?age .
              FILTER (?age > 28)
            }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice", "Carol"]);
    }

    /// CONCEPT:EG-101 — predicate pushdown: a query mentioning only `ex:name` pulls only
    /// the `name` (and subject `id`) columns, and materializes only `name` triples.
    #[test]
    fn eg101_predicate_pushdown_narrows_columns_and_triples() {
        let query = crate::sparql::parse_query(
            "PREFIX ex: <http://example.org/> SELECT ?n WHERE { ?p ex:name ?n }",
        )
        .unwrap();
        let wanted = wanted_predicates(&query);
        assert_eq!(
            wanted,
            Some(
                ["http://example.org/name".to_string()]
                    .into_iter()
                    .collect()
            )
        );

        let reg = people_registry();
        let vg = people_vgraph();
        let triples = vg.materialize(&reg, &wanted).unwrap();
        // Only name triples materialized — no age, no knows, no rdf:type.
        assert_eq!(triples.len(), 3);
        assert!(triples
            .iter()
            .all(|t| t.predicate.as_str() == "http://example.org/name"));
    }

    /// CONCEPT:EG-101 — a variable predicate cannot be narrowed → full materialization
    /// (completeness preserved).
    #[test]
    fn eg101_variable_predicate_disables_pushdown() {
        let query = crate::sparql::parse_query("SELECT * WHERE { ?s ?p ?o }").unwrap();
        assert_eq!(wanted_predicates(&query), None);
    }

    /// CONCEPT:EG-101 — an unregistered source resolves to a clean, listing error.
    #[test]
    fn eg101_unregistered_source_errors_cleanly() {
        let reg = ObdaSourceRegistry::new();
        let vg = people_vgraph();
        let err = run_virtual(&vg, &reg, "SELECT * WHERE { ?s ?p ?o }").unwrap_err();
        assert!(err.contains("no foreign source registered under name 'people'"));
        assert!(err.contains("CONCEPT:EG-101"));
    }

    /// CONCEPT:EG-101 — the minimal textual mapping form parses to an equivalent graph.
    #[test]
    fn eg101_parse_textual_mapping_roundtrips_to_select() {
        let mapping = r#"
            # people → RDF
            SOURCE  people
            SUBJECT http://example.org/person/{id}
            CLASS   http://example.org/Person
            COLUMN  http://example.org/name  name
            COLUMN  http://example.org/age   age
            REF     http://example.org/knows http://example.org/person/{friend_id}
        "#;
        let vg = parse_mapping(mapping).unwrap();
        assert_eq!(vg, people_vgraph());

        let reg = people_registry();
        let res = run_virtual(
            &vg,
            &reg,
            "PREFIX ex: <http://example.org/> SELECT ?name WHERE { ?p a ex:Person ; ex:name ?name }",
        )
        .unwrap();
        assert_eq!(res.solutions.len(), 3);
    }

    /// CONCEPT:EG-101 — `materialize_all` over the textual form yields every triple
    /// (3 rdf:type + 3 name + 3 age + 2 knows = 11; Bob has no friend_id).
    #[test]
    fn eg101_materialize_all_counts() {
        let reg = people_registry();
        let vg = people_vgraph();
        let triples = vg.materialize_all(&reg).unwrap();
        assert_eq!(triples.len(), 11);
    }

    /// CONCEPT:EG-101 — the template expander handles `{col}`, empty-column skips, and
    /// escaped braces.
    #[test]
    fn eg101_template_expansion() {
        let mut row = ForeignRow::new();
        row.insert("id".into(), "7".into());
        assert_eq!(
            expand_template("http://ex/p/{id}", &row),
            Some("http://ex/p/7".to_string())
        );
        // Missing column → None.
        assert_eq!(expand_template("http://ex/p/{missing}", &row), None);
        // Escaped braces.
        assert_eq!(expand_template("a{{b}}c", &row), Some("a{b}c".to_string()));
    }

    // ── CONCEPT:EG-305 — full R2RML Turtle parsing ───────────────────────────────

    /// A standard R2RML mapping document (in Turtle) over the `people` table: a subject
    /// template + class, a plain-literal name, a typed (xsd:integer) age, and an IRI-
    /// template `knows` reference to another person's subject.
    const PEOPLE_R2RML: &str = r#"
        @prefix rr:  <http://www.w3.org/ns/r2rml#> .
        @prefix ex:  <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

        <http://example.org/map/People> a rr:TriplesMap ;
            rr:logicalTable [ rr:tableName "people" ] ;
            rr:subjectMap [
                rr:template "http://example.org/person/{id}" ;
                rr:class ex:Person
            ] ;
            rr:predicateObjectMap [
                rr:predicate ex:name ;
                rr:objectMap [ rr:column "name" ]
            ] ;
            rr:predicateObjectMap [
                rr:predicate ex:age ;
                rr:objectMap [ rr:column "age" ; rr:datatype xsd:integer ]
            ] ;
            rr:predicateObjectMap [
                rr:predicate ex:knows ;
                rr:objectMap [ rr:template "http://example.org/person/{friend_id}" ]
            ] .
    "#;

    /// CONCEPT:EG-305 — a real R2RML Turtle doc parses to a VirtualGraph whose SPARQL
    /// results match the EG-101 programmatic graph (subject template + class + name).
    #[test]
    fn eg305_r2rml_turtle_parses_and_selects() {
        let vg = parse_r2rml_turtle(PEOPLE_R2RML).unwrap();
        assert_eq!(vg.triples_maps.len(), 1);
        assert_eq!(vg.triples_maps[0].logical_source, "people");
        assert_eq!(
            vg.triples_maps[0].subject_class.as_deref(),
            Some("http://example.org/Person")
        );

        let reg = people_registry();
        let res = run_virtual(
            &vg,
            &reg,
            r#"PREFIX ex: <http://example.org/>
               SELECT ?name WHERE { ?p a ex:Person ; ex:name ?name . }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
    }

    /// CONCEPT:EG-305 — the subject IRI template from `rr:subjectMap`/`rr:template` is
    /// applied when selecting the subject variable.
    #[test]
    fn eg305_r2rml_subject_template_applies() {
        let vg = parse_r2rml_turtle(PEOPLE_R2RML).unwrap();
        let reg = people_registry();
        let res = run_virtual(
            &vg,
            &reg,
            r#"PREFIX ex: <http://example.org/>
               SELECT ?p WHERE { ?p ex:name "Alice" . }"#,
        )
        .unwrap();
        let subjects: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("p").map(|b| b.as_str().to_string()))
            .collect();
        assert_eq!(subjects, vec!["<http://example.org/person/1>"]);
    }

    /// CONCEPT:EG-305 — a typed object map (`rr:column` + `rr:datatype xsd:integer`)
    /// materializes a TYPED literal, so a numeric SPARQL FILTER compares as an integer.
    #[test]
    fn eg305_r2rml_typed_object_map_numeric_filter() {
        let vg = parse_r2rml_turtle(PEOPLE_R2RML).unwrap();

        // The parsed object map for ex:age carries the xsd:integer datatype.
        let age_pom = vg.triples_maps[0]
            .predicate_object_maps
            .iter()
            .find(|(p, _)| p == "http://example.org/age")
            .map(|(_, o)| o.clone());
        assert_eq!(
            age_pom,
            Some(ObjectMap::TypedColumn(
                "age".to_string(),
                "http://www.w3.org/2001/XMLSchema#integer".to_string()
            ))
        );

        let reg = people_registry();
        let res = run_virtual(
            &vg,
            &reg,
            r#"PREFIX ex: <http://example.org/>
               SELECT ?name WHERE {
                 ?p ex:name ?name ; ex:age ?age .
                 FILTER (?age > 28)
               }"#,
        )
        .unwrap();
        let mut names: Vec<String> = res
            .solutions
            .iter()
            .filter_map(|s| s.get("name").map(|b| b.as_str().to_string()))
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice", "Carol"]);
    }

    /// CONCEPT:EG-305 — an IRI-template object map (`rr:objectMap`/`rr:template`) joins
    /// across the virtual graph: `?a knows ?b`, `?b name ?bname`.
    #[test]
    fn eg305_r2rml_ref_template_joins() {
        let vg = parse_r2rml_turtle(PEOPLE_R2RML).unwrap();
        let reg = people_registry();
        let res = run_virtual(
            &vg,
            &reg,
            r#"PREFIX ex: <http://example.org/>
               SELECT ?aname ?bname WHERE {
                 ?a ex:name ?aname .
                 ?a ex:knows ?b .
                 ?b ex:name ?bname .
               }"#,
        )
        .unwrap();
        let mut pairs: Vec<(String, String)> = res
            .solutions
            .iter()
            .filter_map(|s| {
                Some((
                    s.get("aname")?.as_str().to_string(),
                    s.get("bname")?.as_str().to_string(),
                ))
            })
            .collect();
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("Alice".to_string(), "Bob".to_string()),
                ("Carol".to_string(), "Alice".to_string()),
            ]
        );
    }

    /// CONCEPT:EG-305 — the R2RML shortcuts `rr:predicate`/`rr:object` (constant IRI) and
    /// a `rr:predicateMap`/`rr:constant` all resolve; multiple `rr:class` are preserved
    /// (first → subject_class, extras → rdf:type object maps).
    #[test]
    fn eg305_r2rml_shortcuts_and_multiple_classes() {
        let doc = r#"
            @prefix rr:  <http://www.w3.org/ns/r2rml#> .
            @prefix ex:  <http://example.org/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

            <http://example.org/map/M> a rr:TriplesMap ;
                rr:logicalTable [ rr:tableName "people" ] ;
                rr:subjectMap [
                    rr:template "http://example.org/person/{id}" ;
                    rr:class ex:Person ;
                    rr:class ex:Agent
                ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:name ;
                    rr:objectMap [ rr:column "name" ]
                ] ;
                rr:predicateObjectMap [
                    rr:predicate ex:kind ;
                    rr:object ex:Human
                ] .
        "#;
        let vg = parse_r2rml_turtle(doc).unwrap();
        let tm = &vg.triples_maps[0];
        // One of the two classes lands in subject_class; the other as an rdf:type pom.
        assert!(tm.subject_class.is_some());
        assert!(tm
            .predicate_object_maps
            .iter()
            .any(|(p, o)| p == RDF_TYPE_IRI
                && matches!(o, ObjectMap::ConstantIri(i) if i == "http://example.org/Agent")));
        // The rr:object constant shortcut → ConstantIri.
        assert!(tm.predicate_object_maps.iter().any(|(p, o)| p
            == "http://example.org/kind"
            && matches!(o, ObjectMap::ConstantIri(i) if i == "http://example.org/Human")));

        // Every person is both ex:Person and ex:Agent.
        let reg = people_registry();
        let res = run_virtual(
            &vg,
            &reg,
            r#"PREFIX ex: <http://example.org/>
               SELECT ?p WHERE { ?p a ex:Agent . }"#,
        )
        .unwrap();
        assert_eq!(res.solutions.len(), 3);
    }

    /// CONCEPT:EG-305 — a document with no triples map is a clean, cited error.
    #[test]
    fn eg305_r2rml_empty_document_errors() {
        let err = parse_r2rml_turtle("@prefix ex: <http://example.org/> . ex:a ex:b ex:c .")
            .unwrap_err();
        assert!(err.contains("no rr:TriplesMap"));
        assert!(err.contains("CONCEPT:EG-305"));
    }

    /// CONCEPT:EG-305 — a triples map missing its logical table is a cited error.
    #[test]
    fn eg305_r2rml_missing_logical_table_errors() {
        let doc = r#"
            @prefix rr: <http://www.w3.org/ns/r2rml#> .
            @prefix ex: <http://example.org/> .
            <http://example.org/map/M> a rr:TriplesMap ;
                rr:subjectMap [ rr:template "http://example.org/person/{id}" ] .
        "#;
        let err = parse_r2rml_turtle(doc).unwrap_err();
        assert!(err.contains("rr:logicalTable"));
        assert!(err.contains("CONCEPT:EG-305"));
    }

    /// CONCEPT:EG-305 — parsing does not disturb the EG-101 builders: the minimal textual
    /// form still round-trips to the programmatic graph.
    #[test]
    fn eg305_eg101_builders_still_work() {
        let mapping = r#"
            SOURCE  people
            SUBJECT http://example.org/person/{id}
            CLASS   http://example.org/Person
            COLUMN  http://example.org/name  name
            COLUMN  http://example.org/age   age
            REF     http://example.org/knows http://example.org/person/{friend_id}
        "#;
        assert_eq!(parse_mapping(mapping).unwrap(), people_vgraph());
    }
}
