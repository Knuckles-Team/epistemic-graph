//! GraphQL **cross-modal transaction** seam (CONCEPT:EG-KG.query.eg-9/380/381) — the GraphQL
//! surface for the EG-359..363 in-txn cross-modal seam, so a GraphQL client reaches the
//! SAME *stage graph + vector + timeseries + OWL/CONSTRUCT → read-your-own-writes →
//! atomic commit* power the RPC (EG-359..363) and pgwire (EG-372) surfaces already
//! expose. North-star EG-373: every cross-modal seam is implemented at EVERY surface.
//!
//! ## Model — a multi-request `txnId` handle (CONCEPT:EG-KG.compute.eg-178)
//! GraphQL over HTTP is request/response with no built-in session, so a multi-request
//! transaction needs an explicit handle. `beginTransaction` mints a `txnId` and registers
//! a staged transaction in a [`CrossModalTxnRegistry`] — a `Mutex<HashMap>`, the SAME
//! stateful-registry idiom the crate's [`crate::hardening::ApqRegistry`] already uses.
//! Subsequent `stageEmbedding` / `addMeasurement` / `sparqlUpdate` / `sparqlConstruct`
//! mutations and the in-txn `unifiedQuery` read carry that `txnId`; `commitTransaction`
//! applies every staged modality atomically and drops the handle; `rollbackTransaction`
//! discards it. A server carrier holds ONE shared registry across a connection's requests
//! (the facade reconcile — see below). Because the crate is otherwise runtime-free, the
//! registry is the ONLY state, owned by the caller and passed in per call.
//!
//! ## Routing onto the shared machinery — NO logic duplicated
//! `eg-graphql` sits BELOW the facade in the crate DAG (parallel to `eg-rdf`/`eg-query`,
//! above `eg-core`), so it CANNOT call the facade's `GraphTxnState` /
//! `commit_cross_modal_txn` / `run_unified_overlaid` (those live in `src/server/*`, ABOVE
//! this crate; calling up would be a dependency cycle). Instead it routes onto the SAME
//! LOWER primitives those wrappers are themselves built on:
//!   * **in-txn read** (`unifiedQuery`) → `GraphView::overlay_*` (the exact overlay ops
//!     the facade's `overlay_write_set` uses) + `eg_core::compute::semantic::semantic_overlay`
//!     + `eg_plan::StagedSeries`, then `eg_plan::execute` over an `eg_plan::uql`-parsed
//!     plan — byte-for-byte the executor `run_unified` wraps. Read-your-own-writes off the
//!     overlay; off-txn / another request reads the committed store (empty overlay).
//!   * **CONSTRUCT / SPARQL-UPDATE lowering** → `eg_rdf::sparql::run_outcome` /
//!     `eg_rdf::update::insert_data_triples` + the canonical triples→property-graph
//!     projection ([`lower_triples`], mirroring the facade's `handlers::txn::triples_to_methods`).
//!   * **commit** → the staged graph + vector writes land in the live `GraphCore`
//!     in-memory (one `GraphTxn` for atomicity), matching the engine's
//!     "no persistence ⇒ in-memory only" durability tier.
//!
//! ## Reconcile hooks (owned by the facade at engine 2.10.0 — not editable from this crate)
//!   * **EG-383 — durable atomic commit.** The DURABLE all-modality commit (ONE redb
//!     `WriteTransaction`, including tsdb measurement SERIES) is the facade's
//!     `commit_cross_modal_txn`, which needs `ServerState`/persistence above this crate.
//!     The reconcile: the facade's GraphQL HTTP carrier, on `commitTransaction`, converts
//!     the staged [`CrossModalTxn`] into a facade `GraphTxnState` and calls
//!     `commit_cross_modal_txn` — EXACTLY as pgwire's `commit_txn_state` does. Until then
//!     the in-crate commit is the in-memory tier (graph + vector to `GraphCore`;
//!     measurements are durable-only and land at the reconcile).
//!   * **Shared triples lowering.** Hoist the facade's `pub(crate)`
//!     `handlers::txn::triples_to_methods` into `eg-rdf` as `pub` so this crate and the
//!     facade txn handler share ONE lowering instead of the mirror kept here.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use eg_core::graph::GraphCore;
use serde_json::{Map, Value};

use crate::parser::{parse_operation, Field, GqlValue, Operation};

/// A lowered graph write staged into a cross-modal txn — a node upsert or a typed edge.
/// The SAME shape the in-txn overlay ([`overlay_txn`]) and the commit ([`commit_txn`])
/// both apply, so read-your-own-writes and the committed result are identical by
/// construction.
pub enum GraphWrite {
    Node {
        id: String,
        blob: Vec<u8>,
    },
    Edge {
        from: String,
        to: String,
        blob: Vec<u8>,
    },
}

/// A staged time-series batch (CONCEPT:EG-KG.compute.series-name-its): a `series` name and its
/// `(ts_ns, field_values)` points — the SAME `Vec<(i64, Vec<f64>)>` shape
/// `eg_plan::StagedSeries::push_points` consumes. Always defined (a transparent alias, no
/// cost) so the facade-facing [`CrossModalTxn::measurements`] accessor names ONE type
/// regardless of the `crossmodal-tsdb` feature (CONCEPT:EG-KG.query.facade-reconcile-hook).
pub type StagedMeasurement = (String, Vec<(i64, Vec<f64>)>);

/// One staged cross-modal transaction — the GraphQL-surface analogue of the facade's
/// `GraphTxnState`, holding only the buffers this crate overlays + commits directly.
#[derive(Default)]
pub struct CrossModalTxn {
    /// Node/edge writes lowered from `sparqlUpdate` / `sparqlConstruct` (CONCEPT:EG-KG.query.eg-9).
    graph: Vec<GraphWrite>,
    /// Embeddings staged by `stageEmbedding`.
    vectors: Vec<(String, Vec<f32>)>,
    /// Time-series batches staged by `addMeasurement` (CONCEPT:EG-KG.compute.series-name-its). tsdb-gated (parity
    /// with the RPC/pgwire seam, whose measurement staging is behind `tsdb`).
    #[cfg(feature = "crossmodal-tsdb")]
    measurements: Vec<StagedMeasurement>,
}

impl CrossModalTxn {
    /// Staged node/edge writes (CONCEPT:EG-KG.query.facade-reconcile-hook) — the facade lowers each to
    /// `AddNode`/`AddEdge` for the durable `commit_cross_modal_txn`.
    pub fn graph_writes(&self) -> &[GraphWrite] {
        &self.graph
    }

    /// Staged embeddings (CONCEPT:EG-KG.query.facade-reconcile-hook).
    pub fn vectors(&self) -> &[(String, Vec<f32>)] {
        &self.vectors
    }

    /// Staged tsdb measurement batches (CONCEPT:EG-KG.query.facade-reconcile-hook) as `(series, [(ts_ns, values)])` —
    /// EMPTY when built without `crossmodal-tsdb`, so the facade can always call it.
    pub fn measurements(&self) -> Vec<StagedMeasurement> {
        #[cfg(feature = "crossmodal-tsdb")]
        {
            self.measurements.clone()
        }
        #[cfg(not(feature = "crossmodal-tsdb"))]
        {
            Vec::new()
        }
    }
}

/// Holds the open GraphQL cross-modal transactions across a connection's requests
/// (CONCEPT:EG-KG.compute.eg-178). A server carrier owns ONE instance and threads it through each
/// GraphQL request so a `txnId` minted by `beginTransaction` survives to the later
/// `stage*` / `unifiedQuery` / `commitTransaction` requests.
#[derive(Default)]
pub struct CrossModalTxnRegistry {
    txns: Mutex<HashMap<String, CrossModalTxn>>,
    seq: AtomicU64,
}

impl CrossModalTxnRegistry {
    /// A fresh, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint a process-unique txn id (a monotonic atomic — no `rand`/time dep, matching
    /// the engine's `TxnIdGen` idiom).
    fn next_id(&self) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        format!("gqltxn-{n:016x}")
    }

    /// Remove + return a staged txn so the facade can commit it DURABLY (CONCEPT:EG-KG.query.facade-reconcile-hook).
    /// The facade's GraphQL carrier calls this on `commitTransaction`, converts the returned
    /// [`CrossModalTxn`] into a facade `GraphTxnState`, and lands every modality in ONE redb
    /// `WriteTransaction` via `commit_cross_modal_txn` — instead of the in-crate in-memory
    /// [`commit_transaction`] tier. `None` for an unknown/already-consumed id.
    pub fn take(&self, txn_id: &str) -> Option<CrossModalTxn> {
        self.txns.lock().unwrap().remove(txn_id)
    }
}

/// How the facade should route a GraphQL cross-modal mutation (CONCEPT:EG-KG.query.facade-reconcile-hook).
pub enum CrossModalRoute {
    /// A lone `commitTransaction(txnId)` — the facade takes the staged txn and lands it
    /// DURABLY via `commit_cross_modal_txn` (one redb `WriteTransaction`), matching pgwire.
    Commit(String),
    /// A begin/stage/read/rollback verb (or a multi-verb doc) — the facade runs it in-memory
    /// via [`execute`] over the shared registry (staging + read-your-own-writes; no durable
    /// side effect until commit).
    Staging,
    /// Not a cross-modal mutation — the facade routes it to the ordinary GraphQL mutation
    /// path (`execute_mutation`).
    NotCrossModal,
}

/// Classify a GraphQL mutation `src` for facade routing (CONCEPT:EG-KG.query.facade-reconcile-hook) with ONE parse:
/// a lone `commitTransaction` → [`CrossModalRoute::Commit`]; a doc whose roots are all
/// cross-modal verbs → [`CrossModalRoute::Staging`]; anything else → `NotCrossModal`.
pub fn classify_crossmodal(src: &str) -> CrossModalRoute {
    const VERBS: [&str; 8] = [
        "beginTransaction",
        "stageEmbedding",
        "addMeasurement",
        "sparqlUpdate",
        "sparqlConstruct",
        "unifiedQuery",
        "commitTransaction",
        "rollbackTransaction",
    ];
    let m = match parse_operation(src) {
        Ok(Operation::Mutation(m)) => m,
        _ => return CrossModalRoute::NotCrossModal,
    };
    if m.roots.is_empty() || !m.roots.iter().all(|f| VERBS.contains(&f.name.as_str())) {
        return CrossModalRoute::NotCrossModal;
    }
    if m.roots.len() == 1 && m.roots[0].name == "commitTransaction" {
        if let Ok(id) = arg_str(&m.roots[0], "txnId") {
            return CrossModalRoute::Commit(id);
        }
    }
    CrossModalRoute::Staging
}

/// Parse + execute a GraphQL cross-modal MUTATION over `core`, threading the shared
/// `registry` so a `txnId` spans requests. Returns the GraphQL-shaped `{"data": …}` JSON.
///
/// Recognized root fields: `beginTransaction`, `stageEmbedding`, `addMeasurement`,
/// `sparqlUpdate`, `sparqlConstruct`, `unifiedQuery`, `commitTransaction`,
/// `rollbackTransaction`. A non-cross-modal field (e.g. `createNode`) is an error here —
/// route those to [`crate::execute_mutation`].
pub fn execute(
    core: &GraphCore,
    registry: &CrossModalTxnRegistry,
    src: &str,
) -> Result<Value, String> {
    let m = match parse_operation(src).map_err(|e| e.to_string())? {
        Operation::Mutation(m) => m,
        Operation::Query(_) => {
            return Err("GraphQL: expected a cross-modal mutation, got a query".into())
        }
        Operation::Subscription(_) => {
            return Err("GraphQL: expected a cross-modal mutation, got a subscription".into())
        }
    };

    let mut data = Map::new();
    for field in &m.roots {
        let result = execute_field(core, registry, field)?;
        data.insert(field.alias.clone(), result);
    }
    Ok(Value::Object(
        [("data".to_string(), Value::Object(data))]
            .into_iter()
            .collect(),
    ))
}

/// Dispatch one cross-modal root field to its verb.
fn execute_field(
    core: &GraphCore,
    registry: &CrossModalTxnRegistry,
    field: &Field,
) -> Result<Value, String> {
    match field.name.as_str() {
        "beginTransaction" => begin_transaction(registry),
        "stageEmbedding" => stage_embedding(registry, field),
        "addMeasurement" => add_measurement(registry, field),
        "sparqlUpdate" => sparql_update(registry, field),
        "sparqlConstruct" => sparql_construct(core, registry, field),
        "unifiedQuery" => unified_query(core, registry, field),
        "commitTransaction" => commit_transaction(core, registry, field),
        "rollbackTransaction" => rollback_transaction(registry, field),
        other => Err(format!(
            "GraphQL: unknown cross-modal field `{other}` (expected beginTransaction / \
             stageEmbedding / addMeasurement / sparqlUpdate / sparqlConstruct / \
             unifiedQuery / commitTransaction / rollbackTransaction)"
        )),
    }
}

/// `beginTransaction` → register a fresh staged txn, return `{ txnId }`.
fn begin_transaction(registry: &CrossModalTxnRegistry) -> Result<Value, String> {
    let txn_id = registry.next_id();
    registry
        .txns
        .lock()
        .unwrap()
        .insert(txn_id.clone(), CrossModalTxn::default());
    Ok(obj([("txnId", Value::String(txn_id))]))
}

/// `stageEmbedding(txnId, id, vector: [Float])` → stage a vector upsert. `{ staged: true }`.
fn stage_embedding(registry: &CrossModalTxnRegistry, field: &Field) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let id = arg_str(field, "id")?;
    let vector = arg_f32_vec(field, "vector")?;
    with_txn(registry, &txn_id, |txn| {
        txn.vectors.push((id, vector));
        Ok(())
    })?;
    Ok(obj([("staged", Value::Bool(true))]))
}

/// `addMeasurement(txnId, series, ts: Int, values: [Float])` → stage one time-series
/// point (CONCEPT:EG-KG.compute.series-name-its). tsdb-gated: without `crossmodal-tsdb` the verb returns a clear
/// "not built" error rather than silently dropping the modality.
#[cfg(feature = "crossmodal-tsdb")]
fn add_measurement(registry: &CrossModalTxnRegistry, field: &Field) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let series = arg_str(field, "series")?;
    let ts = arg_i64(field, "ts")?;
    let values = arg_f64_vec(field, "values")?;
    with_txn(registry, &txn_id, |txn| {
        txn.measurements.push((series, vec![(ts, values)]));
        Ok(())
    })?;
    Ok(obj([("staged", Value::Bool(true))]))
}

/// `addMeasurement` in a build WITHOUT `crossmodal-tsdb` — the modality is not compiled in
/// (parity with the facade's `tsdb`-gated `TxnAddMeasurement`). Explicit error, never a
/// silent no-op.
#[cfg(not(feature = "crossmodal-tsdb"))]
fn add_measurement(_registry: &CrossModalTxnRegistry, _field: &Field) -> Result<Value, String> {
    Err("GraphQL: addMeasurement is not available in this build (needs `crossmodal-tsdb`)".into())
}

/// `sparqlUpdate(txnId, update)` → lower an `INSERT DATA` update's triples to staged graph
/// writes (CONCEPT:EG-KG.query.eg-9), reusing `eg_rdf::update::insert_data_triples`.
fn sparql_update(registry: &CrossModalTxnRegistry, field: &Field) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let update = arg_str(field, "update")?;
    let triples = eg_rdf::update::insert_data_triples(&update)
        .map_err(|e| format!("GraphQL sparqlUpdate: {e}"))?;
    let writes = lower_triples(&triples);
    with_txn(registry, &txn_id, |txn| {
        txn.graph.extend(writes);
        Ok(())
    })?;
    Ok(obj([("staged", Value::Bool(true))]))
}

/// `sparqlConstruct(txnId, query)` → evaluate a CONSTRUCT/DESCRIBE against the committed
/// snapshot and lower its produced triples to staged graph writes (CONCEPT:EG-KG.query.eg-9),
/// reusing `eg_rdf::sparql::run_outcome`.
fn sparql_construct(
    core: &GraphCore,
    registry: &CrossModalTxnRegistry,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let query = arg_str(field, "query")?;
    let snap = core.analysis_snapshot();
    let proj = eg_rdf::sparql::Projection::from_wire("", "");
    let triples = match eg_rdf::sparql::run_outcome(&snap, &query, &proj) {
        Ok(eg_rdf::sparql::QueryOutcome::Graph(t)) => t,
        Ok(_) => return Err("GraphQL sparqlConstruct: CONSTRUCT/DESCRIBE query required".into()),
        Err(e) => return Err(format!("GraphQL sparqlConstruct: {e}")),
    };
    let writes = lower_triples(&triples);
    with_txn(registry, &txn_id, |txn| {
        txn.graph.extend(writes);
        Ok(())
    })?;
    Ok(obj([("staged", Value::Bool(true))]))
}

/// `unifiedQuery(txnId, uql)` → run a UQL cross-modal plan over the committed snapshot
/// OVERLAID with the txn's staged graph writes + embeddings (+ staged series when tsdb),
/// giving read-your-own-writes (CONCEPT:EG-KG.query.eg-9). Returns `{ rows: [{ id, score }] }`. The
/// overlay is empty for a fresh/other txn, so an off-txn read sees committed data only.
fn unified_query(
    core: &GraphCore,
    registry: &CrossModalTxnRegistry,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let uql = arg_str(field, "uql")?;
    let plan = eg_plan::uql::parse(&uql).map_err(|e| e.render(&uql))?;

    let guard = registry.txns.lock().unwrap();
    let txn = guard
        .get(&txn_id)
        .ok_or_else(|| format!("GraphQL: unknown transaction '{txn_id}'"))?;

    // Committed base snapshot overlaid with the txn's staged graph writes (the SAME
    // `GraphView::overlay_*` primitives the facade's `overlay_write_set` uses).
    let mut view = core.analysis_snapshot();
    overlay_txn(&mut view, txn);
    // Committed embeddings overlaid with the txn's staged vectors (RYOW for Rank/kNN).
    let semantic = eg_core::compute::semantic::semantic_overlay(
        core.semantic_store.read().clone(),
        &txn.vectors,
    );
    // The txn's staged, uncommitted series → an in-memory overlay `Op::TsScan` reads
    // before the committed store (CONCEPT:EG-KG.query.txn-tsdb-read-your parity). Absent without tsdb.
    #[cfg(feature = "crossmodal-tsdb")]
    let staged_series = {
        let mut s = eg_plan::StagedSeries::new();
        for (series, points) in &txn.measurements {
            s.push_points(series, points.iter().cloned());
        }
        s
    };

    let ctx = eg_plan::PlanCtx::new(&view, &semantic);
    #[cfg(feature = "crossmodal-tsdb")]
    let ctx = ctx.with_staged_series(&staged_series);
    let result = eg_plan::execute(&plan, &ctx).map_err(|e| format!("GraphQL unifiedQuery: {e}"))?;

    let rows: Vec<Value> = result
        .rows()
        .iter()
        .map(|r| {
            obj([
                ("id", Value::String(r.id.clone())),
                (
                    "score",
                    r.score
                        .and_then(|s| serde_json::Number::from_f64(s as f64).map(Value::Number))
                        .unwrap_or(Value::Null),
                ),
            ])
        })
        .collect();
    Ok(obj([("rows", Value::Array(rows))]))
}

/// `commitTransaction(txnId)` → apply the staged graph + vector writes to the live
/// `GraphCore` in-memory (CONCEPT:EG-KG.query.eg-9). Graph writes land through ONE `GraphTxn`
/// (in-memory atomic); vectors go to the semantic store; `mark_dirty` bumps the OCC
/// version once. Measurements are durable-only (facade reconcile — see module docs).
/// Returns `{ committed: true }`. Drops the handle.
fn commit_transaction(
    core: &GraphCore,
    registry: &CrossModalTxnRegistry,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let txn = registry
        .txns
        .lock()
        .unwrap()
        .remove(&txn_id)
        .ok_or_else(|| format!("GraphQL: unknown transaction '{txn_id}'"))?;
    commit_txn(core, txn);
    Ok(obj([("committed", Value::Bool(true))]))
}

/// `rollbackTransaction(txnId)` → discard the staged txn (nothing was applied). Returns
/// `{ rolledBack: <bool> }` — `false` for an already-committed/unknown id.
fn rollback_transaction(registry: &CrossModalTxnRegistry, field: &Field) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let removed = registry.txns.lock().unwrap().remove(&txn_id).is_some();
    Ok(obj([("rolledBack", Value::Bool(removed))]))
}

// ── overlay + commit ──────────────────────────────────────────────────────────────

/// Overlay a txn's staged graph writes onto a cloned `GraphView` (CONCEPT:EG-KG.query.eg-9), so an
/// in-txn `unifiedQuery`'s scan/traverse legs observe the txn's own uncommitted graph
/// writes. The SAME `GraphView::overlay_*` ops the facade's `overlay_write_set` uses.
fn overlay_txn(view: &mut eg_core::graph::GraphView, txn: &CrossModalTxn) {
    for w in &txn.graph {
        match w {
            GraphWrite::Node { id, blob } => view.overlay_add_node(id.clone(), blob.clone()),
            GraphWrite::Edge { from, to, blob } => {
                // `overlay_add_edge` returns whether both endpoints resolved; a staged edge
                // to a not-yet-present endpoint is a best-effort no-op (matching the RPC
                // `apply_staged`/`overlay_write_set` contract), so the result is ignored.
                let _ = view.overlay_add_edge(from.clone(), to.clone(), blob.clone());
            }
        }
    }
}

/// Apply a txn's staged graph + vector writes to the live `GraphCore` (CONCEPT:EG-KG.query.eg-9).
fn commit_txn(core: &GraphCore, txn: CrossModalTxn) {
    {
        let mut gtxn = core.txn();
        for w in &txn.graph {
            match w {
                GraphWrite::Node { id, blob } => gtxn.add_node(id.clone(), blob.clone()),
                GraphWrite::Edge { from, to, blob } => {
                    let _ = gtxn.add_edge(from.clone(), to.clone(), blob.clone());
                }
            }
        }
    }
    {
        let mut store = core.semantic_store.write();
        for (id, embedding) in &txn.vectors {
            store.add_embedding(id.clone(), embedding.clone());
        }
    }
    core.mark_dirty();
}

/// Lower a slice of RDF triples to staged graph writes (CONCEPT:EG-KG.query.eg-9), mirroring the
/// canonical `eg_rdf::mapping::load_triples` property-graph projection (and the facade's
/// `handlers::txn::triples_to_methods`) so the durable rows match the in-memory model:
///   * literal object  → a property `{predicate: literal-cell}` on the subject node;
///   * resource object → subject + object nodes + a typed edge `{"type": predicate}`,
///     and (for `rdf:type`) the subject's `type` label.
///
/// RECONCILE (EG-383): replace with a shared `pub` lowering hoisted into `eg-rdf`.
fn lower_triples(triples: &[eg_rdf::oxrdf::Triple]) -> Vec<GraphWrite> {
    use eg_rdf::oxrdf::{NamedOrBlankNode, Term};
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    type Props = Map<String, Value>;

    let subject_id = |s: &NamedOrBlankNode| -> String {
        match s {
            NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
            NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
            #[allow(unreachable_patterns)]
            other => format!("{other}"),
        }
    };

    let mut node_props: HashMap<String, Props> = HashMap::new();
    let mut edges: Vec<(String, String, String)> = Vec::new();
    for t in triples {
        let s_id = subject_id(&t.subject);
        let pred = t.predicate.as_str().to_string();
        node_props.entry(s_id.clone()).or_default();
        match &t.object {
            Term::Literal(lit) => {
                node_props
                    .entry(s_id.clone())
                    .or_default()
                    .entry(pred)
                    .or_insert_with(|| eg_rdf::mapping::literal_to_cell(lit));
            }
            Term::NamedNode(n) => {
                let o_id = format!("<{}>", n.as_str());
                node_props.entry(o_id.clone()).or_default();
                if pred == RDF_TYPE {
                    node_props
                        .entry(s_id.clone())
                        .or_default()
                        .entry("type".to_string())
                        .or_insert_with(|| Value::String(n.as_str().to_string()));
                }
                edges.push((s_id.clone(), pred, o_id));
            }
            Term::BlankNode(b) => {
                let o_id = format!("_:{}", b.as_str());
                node_props.entry(o_id.clone()).or_default();
                edges.push((s_id.clone(), pred, o_id));
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }

    let mut writes = Vec::with_capacity(node_props.len() + edges.len());
    for (id, props) in node_props {
        let blob = rmp_serde::to_vec_named(&Value::Object(props)).unwrap_or_default();
        writes.push(GraphWrite::Node { id, blob });
    }
    for (s, p, o) in edges {
        let blob = rmp_serde::to_vec_named(&serde_json::json!({ "type": p })).unwrap_or_default();
        writes.push(GraphWrite::Edge {
            from: s,
            to: o,
            blob,
        });
    }
    writes
}

// ── helpers ─────────────────────────────────────────────────────────────────────

/// Run `f` against the open txn named `txn_id`, erroring if it is unknown.
fn with_txn<F>(registry: &CrossModalTxnRegistry, txn_id: &str, f: F) -> Result<(), String>
where
    F: FnOnce(&mut CrossModalTxn) -> Result<(), String>,
{
    let mut guard = registry.txns.lock().unwrap();
    let txn = guard
        .get_mut(txn_id)
        .ok_or_else(|| format!("GraphQL: unknown transaction '{txn_id}'"))?;
    f(txn)
}

/// Build a small JSON object from `(key, value)` pairs.
fn obj<const N: usize>(pairs: [(&str, Value); N]) -> Value {
    Value::Object(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

/// Look up a named argument on a field.
fn arg<'a>(field: &'a Field, name: &str) -> Option<&'a GqlValue> {
    field.args.iter().find(|(k, _)| k == name).map(|(_, v)| v)
}

/// Require a string-valued argument.
fn arg_str(field: &Field, name: &str) -> Result<String, String> {
    match arg(field, name) {
        Some(GqlValue::Str(s)) => Ok(s.clone()),
        Some(_) => Err(format!("`{name}` must be a string")),
        None => Err(format!("`{}` requires a `{name}` argument", field.name)),
    }
}

/// Require an integer-valued argument.
#[cfg(feature = "crossmodal-tsdb")]
fn arg_i64(field: &Field, name: &str) -> Result<i64, String> {
    match arg(field, name) {
        Some(GqlValue::Int(n)) => Ok(*n),
        Some(_) => Err(format!("`{name}` must be an integer")),
        None => Err(format!("`{}` requires a `{name}` argument", field.name)),
    }
}

/// Require a `[Float]`/`[Int]` list argument as `Vec<f32>`.
fn arg_f32_vec(field: &Field, name: &str) -> Result<Vec<f32>, String> {
    num_list(field, name)?
        .iter()
        .map(|n| Ok(*n as f32))
        .collect()
}

/// Require a `[Float]`/`[Int]` list argument as `Vec<f64>`.
#[cfg(feature = "crossmodal-tsdb")]
fn arg_f64_vec(field: &Field, name: &str) -> Result<Vec<f64>, String> {
    num_list(field, name)
}

/// A `[Float]`/`[Int]` list argument as `Vec<f64>` (shared by the f32/f64 accessors).
fn num_list(field: &Field, name: &str) -> Result<Vec<f64>, String> {
    let items = match arg(field, name) {
        Some(GqlValue::List(items)) => items,
        Some(_) => return Err(format!("`{name}` must be a list of numbers")),
        None => return Err(format!("`{}` requires a `{name}` argument", field.name)),
    };
    items
        .iter()
        .map(|v| match v {
            GqlValue::Float(f) => Ok(*f),
            GqlValue::Int(n) => Ok(*n as f64),
            _ => Err(format!("`{name}` must be a list of numbers")),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pbytes(v: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// alice/bob People, NO embeddings — the committed base for the cross-modal proofs.
    fn core_fixture() -> GraphCore {
        let core = GraphCore::new();
        core.add_node(
            "alice".into(),
            pbytes(json!({"type":"Person","name":"Alice"})),
        );
        core.add_node("bob".into(), pbytes(json!({"type":"Person","name":"Bob"})));
        core
    }

    /// Convenience: the alice score a `MATCH (:Person) |> RANK BY ~[1,0]` returns (None if
    /// alice is absent — no embedding to rank).
    fn alice_rank_score(res: &Value) -> Option<f64> {
        res["data"]["unifiedQuery"]["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["id"] == json!("alice"))
            .and_then(|r| r["score"].as_f64())
    }

    /// THE cross-modal roundtrip proof (CONCEPT:EG-KG.query.eg-9/380/382): in ONE multi-request txn
    /// stage an embedding (vector) + a SPARQL-UPDATE graph write, read them back in-txn
    /// via `unifiedQuery` (read-your-own-writes), confirm an off-txn read is ISOLATED
    /// until commit, then confirm commit makes both modalities visible.
    #[test]
    fn cross_modal_txn_ryow_and_isolation() {
        let core = core_fixture();
        let reg = CrossModalTxnRegistry::new();

        // ── begin ──
        let begun = execute(&core, &reg, "mutation { beginTransaction { txnId } }").unwrap();
        let txn_id = begun["data"]["beginTransaction"]["txnId"]
            .as_str()
            .unwrap()
            .to_string();
        assert!(txn_id.starts_with("gqltxn-"));

        // ── stage a VECTOR (embedding for alice) + a GRAPH write (a new node via SPARQL) ──
        let se = format!(
            r#"mutation {{ stageEmbedding(txnId: "{txn_id}", id: "alice", vector: [1.0, 0.0]) {{ staged }} }}"#
        );
        assert_eq!(
            execute(&core, &reg, &se).unwrap()["data"]["stageEmbedding"]["staged"],
            json!(true)
        );
        let su = format!(
            r#"mutation {{ sparqlUpdate(txnId: "{txn_id}", update: "INSERT DATA {{ <http://ex/w1> <http://ex/name> \"W1\" }}") {{ staged }} }}"#
        );
        assert_eq!(
            execute(&core, &reg, &su).unwrap()["data"]["sparqlUpdate"]["staged"],
            json!(true)
        );

        // ── in-txn unifiedQuery: READ-YOUR-OWN-WRITES for the staged embedding ──
        // alice has no committed embedding; the txn staged [1,0], so an in-txn RANK by
        // ~[1,0] returns alice with cosine ≈ 1.0.
        let uq = format!(
            r#"mutation {{ unifiedQuery(txnId: "{txn_id}", uql: "MATCH (:Person) |> RANK BY ~[1.0, 0.0]") {{ rows }} }}"#
        );
        let in_txn = execute(&core, &reg, &uq).unwrap();
        let in_score =
            alice_rank_score(&in_txn).expect("in-txn RANK must see the staged embedding");
        assert!(
            (in_score - 1.0).abs() < 1e-3,
            "in-txn RYOW score ≈ 1.0, got {in_score}"
        );

        // ── ISOLATION: a SECOND (empty) txn's unifiedQuery sees NO staged embedding ──
        let other = execute(&core, &reg, "mutation { beginTransaction { txnId } }").unwrap();
        let other_id = other["data"]["beginTransaction"]["txnId"].as_str().unwrap();
        let uq_other = format!(
            r#"mutation {{ unifiedQuery(txnId: "{other_id}", uql: "MATCH (:Person) |> RANK BY ~[1.0, 0.0]") {{ rows }} }}"#
        );
        assert!(
            alice_rank_score(&execute(&core, &reg, &uq_other).unwrap()).is_none(),
            "off-txn read must be isolated (no committed embedding yet)"
        );
        // ── ISOLATION: the staged graph node is NOT in the committed store pre-commit ──
        assert!(
            core.get_node_properties("<http://ex/w1>").is_none(),
            "staged graph write must be isolated until commit"
        );

        // ── commit ──
        let ct = format!(r#"mutation {{ commitTransaction(txnId: "{txn_id}") {{ committed }} }}"#);
        assert_eq!(
            execute(&core, &reg, &ct).unwrap()["data"]["commitTransaction"]["committed"],
            json!(true)
        );

        // ── post-commit visibility: BOTH modalities landed ──
        // graph: the SPARQL-UPDATE node is now committed.
        assert!(
            core.get_node_properties("<http://ex/w1>").is_some(),
            "committed graph write must be visible"
        );
        // vector: an off-txn RANK now sees alice's committed embedding.
        let after = execute(&core, &reg, &uq_other).unwrap();
        let after_score = alice_rank_score(&after).expect("committed embedding must be visible");
        assert!((after_score - 1.0).abs() < 1e-3, "post-commit score ≈ 1.0");
    }

    /// `rollbackTransaction` discards staged writes: nothing commits, and the id is gone.
    #[test]
    fn cross_modal_txn_rollback_discards() {
        let core = core_fixture();
        let reg = CrossModalTxnRegistry::new();
        let begun = execute(&core, &reg, "mutation { beginTransaction { txnId } }").unwrap();
        let txn_id = begun["data"]["beginTransaction"]["txnId"]
            .as_str()
            .unwrap()
            .to_string();
        let su = format!(
            r#"mutation {{ sparqlUpdate(txnId: "{txn_id}", update: "INSERT DATA {{ <http://ex/z> <http://ex/name> \"Z\" }}") {{ staged }} }}"#
        );
        execute(&core, &reg, &su).unwrap();
        let rb =
            format!(r#"mutation {{ rollbackTransaction(txnId: "{txn_id}") {{ rolledBack }} }}"#);
        assert_eq!(
            execute(&core, &reg, &rb).unwrap()["data"]["rollbackTransaction"]["rolledBack"],
            json!(true)
        );
        assert!(
            core.get_node_properties("<http://ex/z>").is_none(),
            "a rolled-back write must never commit"
        );
        // committing the now-gone id is a clean error.
        let ct = format!(r#"mutation {{ commitTransaction(txnId: "{txn_id}") {{ committed }} }}"#);
        assert!(execute(&core, &reg, &ct).is_err());
    }

    /// tsdb measurement staging + the in-txn `StagedSeries` read-your-own-writes overlay
    /// (CONCEPT:EG-KG.compute.series-name-its). Gated by `crossmodal-tsdb`. Proves an `Op::TsScan` over the SAME
    /// overlay `unifiedQuery` threads reads the txn's own uncommitted points (UQL has no
    /// `TSSCAN` source, so the plan is built directly — the overlay path is identical).
    #[cfg(feature = "crossmodal-tsdb")]
    #[test]
    fn cross_modal_txn_measurement_staged_series_ryow() {
        use eg_plan::{Op, Plan};

        let core = core_fixture();
        let reg = CrossModalTxnRegistry::new();
        let begun = execute(&core, &reg, "mutation { beginTransaction { txnId } }").unwrap();
        let txn_id = begun["data"]["beginTransaction"]["txnId"]
            .as_str()
            .unwrap()
            .to_string();
        let am = format!(
            r#"mutation {{ addMeasurement(txnId: "{txn_id}", series: "temp", ts: 100, values: [21.5]) {{ staged }} }}"#
        );
        assert_eq!(
            execute(&core, &reg, &am).unwrap()["data"]["addMeasurement"]["staged"],
            json!(true)
        );

        // Build the StagedSeries overlay from the staged measurement exactly as
        // `unified_query` does, and confirm a TsScan reads the staged point (RYOW).
        let guard = reg.txns.lock().unwrap();
        let txn = guard.get(&txn_id).unwrap();
        let mut staged = eg_plan::StagedSeries::new();
        for (series, points) in &txn.measurements {
            staged.push_points(series, points.iter().cloned());
        }
        assert!(!staged.is_empty(), "the measurement must be staged");

        let view = core.analysis_snapshot();
        let semantic = core.semantic_store.read().clone();
        let ctx = eg_plan::PlanCtx::new(&view, &semantic).with_staged_series(&staged);
        let plan = Plan::new(vec![Op::TsScan {
            series: vec!["temp".to_string()],
            from: 0.0,
            to: 1000.0,
        }]);
        let rows = eg_plan::execute(&plan, &ctx).unwrap();
        assert!(
            rows.rows().iter().any(|r| r.id == "100"),
            "TsScan must read the txn's own staged measurement point (RYOW)"
        );
    }
}
