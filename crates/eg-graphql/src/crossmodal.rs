//! GraphQL **cross-modal transaction** seam (CONCEPT:EG-KG.query.eg-9/380/381) — the GraphQL
//! surface for the EG-359..363 in-txn cross-modal seam, so a GraphQL client reaches the
//! SAME *stage graph + vector + timeseries + OWL/CONSTRUCT → read-your-own-writes →
//! atomic commit* power the RPC (EG-359..363) and pgwire (EG-372) surfaces already
//! expose. North-star EG-373: every cross-modal seam is implemented at EVERY surface.
//!
//! ## Model — a multi-request `txnId` handle (CONCEPT:EG-KG.compute.eg-178)
//! GraphQL over HTTP is request/response with no built-in session, so a multi-request
//! transaction needs an explicit handle. `beginTransaction` mints a `txnId` and registers
//! a staged transaction under the caller's verified opaque owner scope in a
//! [`CrossModalTxnRegistry`] — a `Mutex<HashMap>`, the SAME
//! stateful-registry idiom the crate's [`crate::hardening::ApqRegistry`] already uses.
//! Subsequent `stageEmbedding` / `addMeasurement` / `sparqlUpdate` / `sparqlConstruct`
//! mutations and the in-txn `unifiedQuery` read carry that `txnId`; `commitTransaction`
//! applies every staged modality atomically and drops the handle; `rollbackTransaction`
//! discards it. A handle is never a bearer credential: every lookup also requires the
//! verified opaque owner scope supplied by the facade. A server carrier holds ONE shared
//! registry across requests. Because the crate is otherwise runtime-free, the registry is
//! the ONLY state, owned by the caller and passed in per call.
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
//!   * **CONSTRUCT / SPARQL-UPDATE lowering** → `eg_rdf::sparql::execute` /
//!     `eg_rdf::update::insert_data_triples` + the canonical triples→property-graph
//!     projection (`eg_rdf::mapping::lower_triples`), shared by every RDF mutation surface.
//!   * **commit** → the facade takes the owner-bound staged transaction and lands every
//!     modality through its authoritative `MutationBatch` commit. This lower crate has no
//!     direct commit function and can never publish uncommitted RAM state.

use std::collections::HashMap;
use std::sync::Mutex;

use eg_core::graph::GraphCore;
use serde_json::{Map, Value};

use crate::parser::{parse_operation, Field, GqlValue, Operation};

/// A lowered graph write staged into a cross-modal txn — a node upsert or a typed edge.
/// The SAME shape the in-txn overlay and authoritative facade commit both apply, so
/// read-your-own-writes and the committed result are identical by construction.
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
/// `GraphTxnState`, holding only the buffers this crate overlays and the facade commits.
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

/// Holds the open GraphQL cross-modal transactions across authenticated requests
/// (CONCEPT:EG-KG.compute.eg-178). A server carrier owns ONE instance and threads it through each
/// GraphQL request so a `txnId` minted by `beginTransaction` survives to the later
/// `stage*` / `unifiedQuery` / `commitTransaction` requests.
#[derive(Default)]
pub struct CrossModalTxnRegistry {
    txns: Mutex<HashMap<(String, String), CrossModalTxn>>,
}

impl CrossModalTxnRegistry {
    /// A fresh, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Mint a restart-safe, unguessable transaction id. Ownership checks remain
    /// mandatory; randomness prevents durable coordinator collisions across restarts.
    fn next_id(&self) -> String {
        format!("gqltxn-{}", uuid::Uuid::new_v4().simple())
    }

    /// Remove + return a staged txn so the facade can commit it DURABLY (CONCEPT:EG-KG.query.facade-reconcile-hook).
    /// The facade's GraphQL carrier calls this on `commitTransaction`, converts the returned
    /// [`CrossModalTxn`] into a facade `GraphTxnState`, and lands every modality in ONE redb
    /// `WriteTransaction` via `commit_cross_modal_txn`. `None` for an unknown,
    /// caller-mismatched, or already-consumed id.
    pub fn take(&self, owner_scope: &str, txn_id: &str) -> Option<CrossModalTxn> {
        self.txns
            .lock()
            .unwrap()
            .remove(&(owner_scope.to_string(), txn_id.to_string()))
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
    /// A cross-modal document whose commit shape is unsafe or ambiguous. A commit must
    /// be the document's sole root field so the facade owns the one durable commit point.
    Invalid(String),
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
    let commits = m
        .roots
        .iter()
        .filter(|field| field.name == "commitTransaction")
        .count();
    if commits > 0 && m.roots.len() != 1 {
        return CrossModalRoute::Invalid(
            "GraphQL: commitTransaction must be the mutation's sole root field".to_string(),
        );
    }
    if commits == 1 {
        if let Ok(id) = arg_str(&m.roots[0], "txnId") {
            return CrossModalRoute::Commit(id);
        }
        return CrossModalRoute::Invalid(
            "GraphQL: commitTransaction requires a string txnId".to_string(),
        );
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
    owner_scope: &str,
    src: &str,
) -> Result<Value, String> {
    if owner_scope.is_empty() {
        return Err("GraphQL: cross-modal transaction owner scope is required".to_string());
    }
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
        let result = execute_field(core, registry, owner_scope, field)?;
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
    owner_scope: &str,
    field: &Field,
) -> Result<Value, String> {
    match field.name.as_str() {
        "beginTransaction" => begin_transaction(registry, owner_scope),
        "stageEmbedding" => stage_embedding(registry, owner_scope, field),
        "addMeasurement" => add_measurement(registry, owner_scope, field),
        "sparqlUpdate" => sparql_update(registry, owner_scope, field),
        "sparqlConstruct" => sparql_construct(core, registry, owner_scope, field),
        "unifiedQuery" => unified_query(core, registry, owner_scope, field),
        "commitTransaction" => Err(
            "GraphQL: commitTransaction is available only through the authoritative facade"
                .to_string(),
        ),
        "rollbackTransaction" => rollback_transaction(registry, owner_scope, field),
        other => Err(format!(
            "GraphQL: unknown cross-modal field `{other}` (expected beginTransaction / \
             stageEmbedding / addMeasurement / sparqlUpdate / sparqlConstruct / \
             unifiedQuery / commitTransaction / rollbackTransaction)"
        )),
    }
}

/// `beginTransaction` → register a fresh staged txn, return `{ txnId }`.
fn begin_transaction(registry: &CrossModalTxnRegistry, owner_scope: &str) -> Result<Value, String> {
    let txn_id = registry.next_id();
    registry.txns.lock().unwrap().insert(
        (owner_scope.to_string(), txn_id.clone()),
        CrossModalTxn::default(),
    );
    Ok(obj([("txnId", Value::String(txn_id))]))
}

/// `stageEmbedding(txnId, id, vector: [Float])` → stage a vector upsert. `{ staged: true }`.
fn stage_embedding(
    registry: &CrossModalTxnRegistry,
    owner_scope: &str,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let id = arg_str(field, "id")?;
    let vector = arg_f32_vec(field, "vector")?;
    with_txn(registry, owner_scope, &txn_id, |txn| {
        txn.vectors.push((id, vector));
        Ok(())
    })?;
    Ok(obj([("staged", Value::Bool(true))]))
}

/// `addMeasurement(txnId, series, ts: Int, values: [Float])` → stage one time-series
/// point (CONCEPT:EG-KG.compute.series-name-its). tsdb-gated: without `crossmodal-tsdb` the verb returns a clear
/// "not built" error rather than silently dropping the modality.
#[cfg(feature = "crossmodal-tsdb")]
fn add_measurement(
    registry: &CrossModalTxnRegistry,
    owner_scope: &str,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let series = arg_str(field, "series")?;
    let ts = arg_i64(field, "ts")?;
    let values = arg_f64_vec(field, "values")?;
    with_txn(registry, owner_scope, &txn_id, |txn| {
        txn.measurements.push((series, vec![(ts, values)]));
        Ok(())
    })?;
    Ok(obj([("staged", Value::Bool(true))]))
}

/// `addMeasurement` in a build WITHOUT `crossmodal-tsdb` — the modality is not compiled in
/// (parity with the facade's `tsdb`-gated `TxnAddMeasurement`). Explicit error, never a
/// silent no-op.
#[cfg(not(feature = "crossmodal-tsdb"))]
fn add_measurement(
    _registry: &CrossModalTxnRegistry,
    _owner_scope: &str,
    _field: &Field,
) -> Result<Value, String> {
    Err("GraphQL: addMeasurement is not available in this build (needs `crossmodal-tsdb`)".into())
}

/// `sparqlUpdate(txnId, update)` → lower an `INSERT DATA` update's triples to staged graph
/// writes (CONCEPT:EG-KG.query.eg-9), reusing `eg_rdf::update::insert_data_triples`.
fn sparql_update(
    registry: &CrossModalTxnRegistry,
    owner_scope: &str,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let update = arg_str(field, "update")?;
    let triples = eg_rdf::update::insert_data_triples(&update)
        .map_err(|e| format!("GraphQL sparqlUpdate: {e}"))?;
    let writes = lower_triples(&triples)?;
    with_txn(registry, owner_scope, &txn_id, |txn| {
        txn.graph.extend(writes);
        Ok(())
    })?;
    Ok(obj([("staged", Value::Bool(true))]))
}

/// `sparqlConstruct(txnId, query)` → evaluate a CONSTRUCT/DESCRIBE against the committed
/// snapshot and lower its produced triples to staged graph writes (CONCEPT:EG-KG.query.eg-9),
/// reusing `eg_rdf::sparql::execute`.
fn sparql_construct(
    core: &GraphCore,
    registry: &CrossModalTxnRegistry,
    owner_scope: &str,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let query = arg_str(field, "query")?;
    let snap = core.analysis_snapshot();
    let proj = eg_rdf::sparql::Projection::from_wire("", "");
    let triples = match eg_rdf::sparql::execute(
        &eg_rdf::sparql::Dataset::new(&snap, Vec::new()),
        &query,
        &proj,
        None,
    ) {
        Ok(eg_rdf::sparql::QueryOutcome::Graph(t)) => t,
        Ok(_) => return Err("GraphQL sparqlConstruct: CONSTRUCT/DESCRIBE query required".into()),
        Err(e) => return Err(format!("GraphQL sparqlConstruct: {e}")),
    };
    let writes = lower_triples(&triples)?;
    with_txn(registry, owner_scope, &txn_id, |txn| {
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
    owner_scope: &str,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let uql = arg_str(field, "uql")?;
    let plan = eg_plan::uql::parse(&uql).map_err(|e| e.render(&uql))?;

    let guard = registry.txns.lock().unwrap();
    let txn = guard
        .get(&(owner_scope.to_string(), txn_id.clone()))
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

/// `rollbackTransaction(txnId)` → discard the staged txn (nothing was applied). Returns
/// `{ rolledBack: <bool> }` — `false` for an already-committed/unknown id.
fn rollback_transaction(
    registry: &CrossModalTxnRegistry,
    owner_scope: &str,
    field: &Field,
) -> Result<Value, String> {
    let txn_id = arg_str(field, "txnId")?;
    let removed = registry
        .txns
        .lock()
        .unwrap()
        .remove(&(owner_scope.to_string(), txn_id))
        .is_some();
    Ok(obj([("rolledBack", Value::Bool(removed))]))
}

// ── overlay ───────────────────────────────────────────────────────────────────────

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

/// Lower a slice of RDF triples to staged graph writes (CONCEPT:EG-KG.query.eg-9), mirroring the
/// canonical `eg_rdf::mapping::load_triples` property-graph projection (and the facade's
/// `handlers::txn::triples_to_methods`) so the durable rows match the in-memory model:
///   * literal object  → a property `{predicate: literal-cell}` on the subject node;
///   * resource object → subject + object nodes + a typed edge
///     `{"relationship": predicate}`,
///     and (for `rdf:type`) the subject's `type` label.
///
fn lower_triples(triples: &[eg_rdf::oxrdf::Triple]) -> Result<Vec<GraphWrite>, String> {
    let lowered = eg_rdf::mapping::lower_triples(triples.iter().cloned())?;
    let mut writes = Vec::with_capacity(lowered.nodes.len() + lowered.edges.len());
    writes.extend(
        lowered
            .nodes
            .into_iter()
            .map(|(id, blob)| GraphWrite::Node { id, blob }),
    );
    writes.extend(
        lowered
            .edges
            .into_iter()
            .map(|(from, to, blob)| GraphWrite::Edge { from, to, blob }),
    );
    Ok(writes)
}

// ── helpers ─────────────────────────────────────────────────────────────────────

/// Run `f` against the open txn named `txn_id`, erroring if it is unknown.
fn with_txn<F>(
    registry: &CrossModalTxnRegistry,
    owner_scope: &str,
    txn_id: &str,
    f: F,
) -> Result<(), String>
where
    F: FnOnce(&mut CrossModalTxn) -> Result<(), String>,
{
    let mut guard = registry.txns.lock().unwrap();
    let txn = guard
        .get_mut(&(owner_scope.to_string(), txn_id.to_string()))
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

    const OWNER: &str = "opaque-owner-a";
    const OTHER_OWNER: &str = "opaque-owner-b";

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
    /// until the authoritative facade takes the owner-bound transaction.
    #[test]
    fn cross_modal_txn_ryow_and_isolation() {
        let core = core_fixture();
        let reg = CrossModalTxnRegistry::new();

        // ── begin ──
        let begun = execute(
            &core,
            &reg,
            OWNER,
            "mutation { beginTransaction { txnId } }",
        )
        .unwrap();
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
            execute(&core, &reg, OWNER, &se).unwrap()["data"]["stageEmbedding"]["staged"],
            json!(true)
        );
        let su = format!(
            r#"mutation {{ sparqlUpdate(txnId: "{txn_id}", update: "INSERT DATA {{ <http://ex/w1> <http://ex/name> \"W1\" }}") {{ staged }} }}"#
        );
        assert_eq!(
            execute(&core, &reg, OWNER, &su).unwrap()["data"]["sparqlUpdate"]["staged"],
            json!(true)
        );

        // ── in-txn unifiedQuery: READ-YOUR-OWN-WRITES for the staged embedding ──
        // alice has no committed embedding; the txn staged [1,0], so an in-txn RANK by
        // ~[1,0] returns alice with cosine ≈ 1.0.
        let uq = format!(
            r#"mutation {{ unifiedQuery(txnId: "{txn_id}", uql: "MATCH (:Person) |> RANK BY ~[1.0, 0.0]") {{ rows }} }}"#
        );
        let in_txn = execute(&core, &reg, OWNER, &uq).unwrap();
        let in_score =
            alice_rank_score(&in_txn).expect("in-txn RANK must see the staged embedding");
        assert!(
            (in_score - 1.0).abs() < 1e-3,
            "in-txn RYOW score ≈ 1.0, got {in_score}"
        );

        // ── ISOLATION: a SECOND (empty) txn's unifiedQuery sees NO staged embedding ──
        let other = execute(
            &core,
            &reg,
            OWNER,
            "mutation { beginTransaction { txnId } }",
        )
        .unwrap();
        let other_id = other["data"]["beginTransaction"]["txnId"].as_str().unwrap();
        let uq_other = format!(
            r#"mutation {{ unifiedQuery(txnId: "{other_id}", uql: "MATCH (:Person) |> RANK BY ~[1.0, 0.0]") {{ rows }} }}"#
        );
        assert!(
            alice_rank_score(&execute(&core, &reg, OWNER, &uq_other).unwrap()).is_none(),
            "off-txn read must be isolated (no committed embedding yet)"
        );
        // ── ISOLATION: the staged graph node is NOT in the committed store pre-commit ──
        assert!(
            core.get_node_properties("<http://ex/w1>").is_none(),
            "staged graph write must be isolated until commit"
        );

        // The lower crate cannot commit to RAM. Only the authoritative facade may take
        // this owner-bound staged value and publish it after its durable commit.
        let staged = reg.take(OWNER, &txn_id).expect("owner may take its txn");
        assert!(!staged.graph_writes().is_empty());
        assert_eq!(staged.vectors().len(), 1);
        assert!(core.get_node_properties("<http://ex/w1>").is_none());
        assert!(alice_rank_score(&execute(&core, &reg, OWNER, &uq_other).unwrap()).is_none());
    }

    /// `rollbackTransaction` discards staged writes: nothing commits, and the id is gone.
    #[test]
    fn cross_modal_txn_rollback_discards() {
        let core = core_fixture();
        let reg = CrossModalTxnRegistry::new();
        let begun = execute(
            &core,
            &reg,
            OWNER,
            "mutation { beginTransaction { txnId } }",
        )
        .unwrap();
        let txn_id = begun["data"]["beginTransaction"]["txnId"]
            .as_str()
            .unwrap()
            .to_string();
        let su = format!(
            r#"mutation {{ sparqlUpdate(txnId: "{txn_id}", update: "INSERT DATA {{ <http://ex/z> <http://ex/name> \"Z\" }}") {{ staged }} }}"#
        );
        execute(&core, &reg, OWNER, &su).unwrap();
        let rb =
            format!(r#"mutation {{ rollbackTransaction(txnId: "{txn_id}") {{ rolledBack }} }}"#);
        assert_eq!(
            execute(&core, &reg, OWNER, &rb).unwrap()["data"]["rollbackTransaction"]["rolledBack"],
            json!(true)
        );
        assert!(
            core.get_node_properties("<http://ex/z>").is_none(),
            "a rolled-back write must never commit"
        );
        assert!(reg.take(OWNER, &txn_id).is_none());
    }

    #[test]
    fn transaction_handles_are_owner_bound_and_commit_is_facade_only() {
        let core = core_fixture();
        let reg = CrossModalTxnRegistry::new();
        let begun = execute(
            &core,
            &reg,
            OWNER,
            "mutation { beginTransaction { txnId } }",
        )
        .unwrap();
        let txn_id = begun["data"]["beginTransaction"]["txnId"].as_str().unwrap();
        let stage = format!(
            r#"mutation {{ stageEmbedding(txnId: "{txn_id}", id: "alice", vector: [1.0]) {{ staged }} }}"#
        );
        assert!(execute(&core, &reg, OTHER_OWNER, &stage).is_err());
        assert!(reg.take(OTHER_OWNER, txn_id).is_none());

        let commit =
            format!(r#"mutation {{ commitTransaction(txnId: "{txn_id}") {{ committed }} }}"#);
        assert!(execute(&core, &reg, OWNER, &commit).is_err());
        assert!(matches!(
            classify_crossmodal(&commit),
            CrossModalRoute::Commit(id) if id == txn_id
        ));
        let mixed = format!(
            r#"mutation {{ stageEmbedding(txnId: "{txn_id}", id: "alice", vector: [1.0]) {{ staged }} commitTransaction(txnId: "{txn_id}") {{ committed }} }}"#
        );
        assert!(matches!(
            classify_crossmodal(&mixed),
            CrossModalRoute::Invalid(_)
        ));
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
        let begun = execute(
            &core,
            &reg,
            OWNER,
            "mutation { beginTransaction { txnId } }",
        )
        .unwrap();
        let txn_id = begun["data"]["beginTransaction"]["txnId"]
            .as_str()
            .unwrap()
            .to_string();
        let am = format!(
            r#"mutation {{ addMeasurement(txnId: "{txn_id}", series: "temp", ts: 100, values: [21.5]) {{ staged }} }}"#
        );
        assert_eq!(
            execute(&core, &reg, OWNER, &am).unwrap()["data"]["addMeasurement"]["staged"],
            json!(true)
        );

        // Build the StagedSeries overlay from the staged measurement exactly as
        // `unified_query` does, and confirm a TsScan reads the staged point (RYOW).
        let guard = reg.txns.lock().unwrap();
        let txn = guard.get(&(OWNER.to_string(), txn_id.clone())).unwrap();
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
