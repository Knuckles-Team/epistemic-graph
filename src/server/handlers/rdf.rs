//! Native RDF/SPARQL handler (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / KG-2.218, features `rdf`/`sparql`).
//!
//! Owns the `// ── RDF/SPARQL ──` protocol section — `AddTriples` / `GetRdf`
//! (feature `rdf`) and `Sparql` (feature `sparql`). These are GRAPH-SCOPED ops: the
//! RDF dataset maps onto the SAME property-graph the rest of the engine uses (a
//! resource object ⇒ a typed edge, a literal object ⇒ a typed property cell,
//! `rdf:type` ⇒ the engine `type` label, a named graph ⇒ the target registry graph).
//! So they route through the normal `dispatch_graph_op` chain like Sql/Cypher.
//!
//! * `AddTriples` is a DURABLE MUTATION (it writes nodes + edges). The dispatch
//!   shell records the `AddTriples` Method into the WAL/Raft like any other durable
//!   write; replay re-parses the source text (deterministic) via `crate::wal::apply`.
//!   The in-RAM apply happens HERE.
//! * `GetRdf` serializes the graph back OUT to N-Triples (read-only).
//! * `Sparql` evaluates a SPARQL 1.1 SELECT over an off-lock GraphView snapshot
//!   (read-only), same idiom as the SQL/Cypher handlers.
//!
//! The optional lossless quad store (`rdf-redb`) lives on `ServerState`; the handler
//! reads it under a brief lock to preserve the one lossy edge (multi-valued literal
//! predicates) and to union those extras back on export.

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use tokio::sync::RwLock;

#[cfg(any(feature = "rdf", feature = "sparql", feature = "owl"))]
use super::super::compute::compute_off_lock;
use super::super::state::ServerState;
use crate::graph::GraphCore;
use crate::protocol::{Method, Response, ResultPayload};

/// Handle the RDF/SPARQL methods. `Err(method)` hands a non-RDF method back to the
/// dispatch chain (routing fall-through); dispatch only ever routes RDF methods here.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: Arc<GraphCore>,
    method: Method,
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    // `caller`/`rls` are consumed only by the `sparql`-gated read path below; in a
    // `security`-but-no-`sparql` build keep them referenced (no dead-param warning).
    #[cfg(all(feature = "security", not(feature = "sparql")))]
    let _ = (caller, rls);
    match method {
        #[cfg(feature = "rdf")]
        Method::AddTriples { turtle, ntriples } => {
            Ok(handle_add_triples(state, req_id, graph_name, &core, turtle, ntriples).await)
        }
        #[cfg(feature = "rdf")]
        Method::GetRdf => Ok(handle_get_rdf(state, req_id, graph_name, &core).await),
        #[cfg(feature = "rdf")]
        Method::RemoveTriples { turtle, ntriples } => {
            Ok(handle_remove_triples(req_id, &core, turtle, ntriples).await)
        }
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => {
            Ok(handle_drop_named_graph(state, req_id, graph_name, &core).await)
        }
        #[cfg(feature = "sparql")]
        Method::Sparql {
            query,
            base_iri,
            type_convention,
        } => {
            // LPG→RDF projection vocabulary (CONCEPT:EG-KG.ontology.lpg-rdf-projection-vocabulary). An empty `base_iri` ⇒
            // the identity projection (verbatim keys), so existing callers are
            // unchanged; a caller-supplied namespace + CamelCase convention projects
            // the live property graph into that vocabulary.
            let proj = eg_rdf::sparql::Projection::from_wire(&base_iri, &type_convention);
            // Off-lock snapshot + blocking-pool idiom, identical to SQL/Cypher.
            // Version-keyed, RLS-aware result cache (CONCEPT:EG-KG.coordination.distributed-cache-coherence × KG-2.231): a
            // repeated SPARQL on an unchanged graph serves cached bytes; any write
            // bumps `version()` → miss. The cache KEY folds in the caller's RLS context
            // (the agent_id when RLS is active) so agent A's filtered SELECT result is
            // never served to agent B; the snapshot is then RLS-FILTERED to the caller's
            // visible rows BEFORE SPARQL evaluation, so a SELECT can't exfiltrate a
            // forbidden row. The key also folds in the projection vocabulary so a raw
            // and a namespaced query of the SAME text never alias.
            #[cfg(feature = "result-cache")]
            let cache_key = format!("{query}\u{0}{base_iri}\u{0}{type_convention}");
            #[cfg(feature = "result-cache")]
            let (snap, version, hash) = {
                #[cfg(feature = "security")]
                let hash = {
                    let kind = if rls.has_rules() {
                        format!("rls:{}:sparql", caller.unwrap_or(""))
                    } else {
                        "sparql".to_string()
                    };
                    eg_core::result_cache::ResultCache::hash_query(&kind, cache_key.as_bytes())
                };
                #[cfg(not(feature = "security"))]
                let hash =
                    eg_core::result_cache::ResultCache::hash_query("sparql", cache_key.as_bytes());
                let (mut snap, version) = core.analysis_snapshot_versioned();
                if let Some(bytes) = core.result_cache().get(hash, version) {
                    return Ok(Response::ok(req_id, ResultPayload::Raw(bytes)));
                }
                #[cfg(feature = "security")]
                rls.filter_view(caller.unwrap_or(""), &mut snap);
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let snap = {
                let mut snap = core.analysis_snapshot();
                #[cfg(feature = "security")]
                rls.filter_view(caller.unwrap_or(""), &mut snap);
                snap
            };
            let resp = match compute_off_lock(req_id, move || {
                eg_rdf::sparql::run_projected(&snap, &query, &proj)
            })
            .await
            {
                Ok(Ok(result)) => {
                    let (vars, rows) = result.to_rows();
                    let wire = crate::protocol::SparqlResult { vars, rows };
                    let bytes = rmp_serde::to_vec_named(&wire).unwrap_or_default();
                    #[cfg(feature = "result-cache")]
                    core.result_cache().put(hash, version, bytes.clone());
                    Response::ok(req_id, ResultPayload::Raw(bytes))
                }
                Ok(Err(msg)) => Response::err(req_id, format!("SPARQL error: {msg}")),
                Err(resp) => resp,
            };
            Ok(resp)
        }
        #[cfg(feature = "owl")]
        Method::OwlReason {
            ontology,
            target_class,
            min_confidence,
        } => Ok(handle_owl_reason(req_id, &core, ontology, target_class, min_confidence).await),
        #[cfg(feature = "rdf")]
        Method::RunRules {
            ontology_ttl,
            rules,
            query_predicate,
            min_confidence,
            derived_only,
        } => Ok(handle_run_rules(
            req_id,
            &core,
            ontology_ttl,
            rules,
            query_predicate,
            min_confidence,
            derived_only,
            #[cfg(feature = "security")]
            caller,
            #[cfg(feature = "security")]
            rls,
        )
        .await),
        // SHACL Core validation (CONCEPT:EG-KG.ontology.concept-6). Read-only. Gated `shacl`; a build
        // without it drops this arm → `other => Err(other)` → the dispatch not-available
        // catch-all (the variant is unconditional in the enum, like Backup/EG-090).
        #[cfg(feature = "shacl")]
        Method::ShaclValidate { shapes, data_graph } => {
            Ok(handle_shacl_validate(state, req_id, graph_name, &core, shapes, data_graph).await)
        }
        // ShEx Core validation (CONCEPT:EG-KG.compute.concept-2). Read-only. Gated `shex`; a build without
        // it drops this arm → `other => Err(other)` → the dispatch not-available catch-all
        // (the variant is unconditional in the enum, like ShaclValidate/EG-132).
        #[cfg(feature = "shex")]
        Method::ShexValidate {
            schema,
            data_graph,
            shape_map,
        } => Ok(handle_shex_validate(
            state, req_id, graph_name, &core, schema, data_graph, shape_map,
        )
        .await),
        other => Err(other),
    }
}

/// Validate the request graph (or an inline `data_graph` Turtle document) against a
/// SHACL `shapes` Turtle document (CONCEPT:EG-KG.ontology.concept-6), returning a `Json`
/// `sh:ValidationReport`. Read-only: an empty `data_graph` exports the LIVE graph's RDF
/// (the same triples `GetRdf` serializes) and validates that.
#[cfg(feature = "shacl")]
async fn handle_shacl_validate(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    shapes: String,
    data_graph: String,
) -> Response {
    let shapes_graph = match eg_shacl::graph_from_turtle(&shapes) {
        Ok(g) => g,
        Err(e) => return Response::err(req_id, format!("ShaclValidate: bad shapes graph: {e}")),
    };
    // Data graph: an inline Turtle document, else the live graph's exported RDF.
    let data = if data_graph.trim().is_empty() {
        #[cfg(feature = "rdf-redb")]
        let quads = state.read().await.rdf_quads.clone();
        #[cfg(not(feature = "rdf-redb"))]
        let _ = state;
        let exported = eg_rdf::mapping::export_triples(
            core,
            graph_name,
            #[cfg(feature = "rdf-redb")]
            quads.as_deref(),
        );
        match exported {
            Ok(triples) => {
                let mut g = eg_shacl::Graph::new();
                for t in &triples {
                    g.insert(t);
                }
                g
            }
            Err(e) => {
                return Response::err(req_id, format!("ShaclValidate: export live graph: {e}"))
            }
        }
    } else {
        match eg_shacl::graph_from_turtle(&data_graph) {
            Ok(g) => g,
            Err(e) => return Response::err(req_id, format!("ShaclValidate: bad data graph: {e}")),
        }
    };
    let report = eg_shacl::validate(&shapes_graph, &data);
    match serde_json::to_value(&report) {
        Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
        Err(e) => Response::err(req_id, format!("ShaclValidate: serialize report: {e}")),
    }
}

/// Validate the request graph (or an inline `data_graph` Turtle document) against a
/// **ShExJ** `schema` for a `shape_map` (`[node_iri, shape_label]` pairs) (CONCEPT:EG-KG.compute.concept-2),
/// returning a `Json` `ShexReport`. Read-only: an empty `data_graph` exports the LIVE
/// graph's RDF (the same triples `GetRdf` serializes) and validates that.
#[cfg(feature = "shex")]
async fn handle_shex_validate(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    schema: String,
    data_graph: String,
    shape_map: Vec<[String; 2]>,
) -> Response {
    let schema = match eg_shex::Schema::from_shexj(&schema) {
        Ok(s) => s,
        Err(e) => return Response::err(req_id, format!("ShexValidate: bad schema: {e}")),
    };
    // Data graph: an inline Turtle document, else the live graph's exported RDF.
    let data = if data_graph.trim().is_empty() {
        #[cfg(feature = "rdf-redb")]
        let quads = state.read().await.rdf_quads.clone();
        #[cfg(not(feature = "rdf-redb"))]
        let _ = state;
        let exported = eg_rdf::mapping::export_triples(
            core,
            graph_name,
            #[cfg(feature = "rdf-redb")]
            quads.as_deref(),
        );
        match exported {
            Ok(triples) => {
                let mut g = eg_shex::Graph::new();
                for t in &triples {
                    g.insert(t);
                }
                g
            }
            Err(e) => {
                return Response::err(req_id, format!("ShexValidate: export live graph: {e}"))
            }
        }
    } else {
        match eg_shex::graph_from_turtle(&data_graph) {
            Ok(g) => g,
            Err(e) => return Response::err(req_id, format!("ShexValidate: bad data graph: {e}")),
        }
    };
    let pairs: Vec<(&str, &str)> = shape_map
        .iter()
        .map(|p| (p[0].as_str(), p[1].as_str()))
        .collect();
    let map = eg_shex::ShapeMap::from_iri_pairs(&pairs);
    let report = eg_shex::validate(&schema, &data, &map);
    match serde_json::to_value(&report) {
        Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
        Err(e) => Response::err(req_id, format!("ShexValidate: serialize report: {e}")),
    }
}

/// Run a parameterised custom-rule reasoning request over the request's graph view
/// (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog / EG-023). Read-only: it reasons over an off-lock snapshot (its
/// folded TBox axioms + asserted facts) plus any inline `ontology_ttl` and the user
/// `rules`, then returns the inferred facts as a `Raw` [`eg_rdf::rules::RuleReasonResponse`].
/// The snapshot is RLS-filtered to the caller's visible rows BEFORE reasoning, so the
/// inference cannot surface a forbidden fact.
#[cfg(feature = "rdf")]
#[allow(clippy::too_many_arguments)]
async fn handle_run_rules(
    req_id: u64,
    core: &Arc<GraphCore>,
    ontology_ttl: String,
    rules: Vec<String>,
    query_predicate: Option<String>,
    min_confidence: f64,
    derived_only: bool,
    #[cfg(feature = "security")] caller: Option<&str>,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Response {
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller.unwrap_or(""), &mut snap);
    let req = eg_rdf::rules::RuleReasonRequest {
        ontology_ttl,
        rules,
        query_predicate,
        min_confidence,
        derived_only,
    };
    match compute_off_lock(req_id, move || {
        eg_rdf::rules::run_rule_reasoning_on_view(&snap, &req)
    })
    .await
    {
        Ok(Ok(response)) => Response::ok(req_id, ResultPayload::raw(&response)),
        Ok(Err(msg)) => Response::err(req_id, format!("RunRules error: {msg}")),
        Err(resp) => resp,
    }
}

/// Top-level routing for the cross-shard `OwlReasonDistributed` method (CONCEPT:EG-KG.ontology.concept-13).
/// It is NOT graph-scoped (it unions several graphs), so dispatch routes it here directly
/// with `state` rather than through `dispatch_graph_op`. `Err(method)` ⇒ not mine.
#[cfg(feature = "owl")]
pub(crate) async fn try_handle_distributed(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::OwlReasonDistributed {
            graphs,
            ontology,
            target_class,
            min_confidence,
        } => Ok(handle_owl_reason_distributed(
            state,
            req_id,
            graphs,
            ontology,
            target_class,
            min_confidence,
        )
        .await),
        other => Err(other),
    }
}

/// The decay half-life (seconds) the reasoner uses to age type facts — the SAME source
/// of truth as the maintenance decay loop (`GRAPH_SERVICE_DECAY_HALF_LIFE`, default 7d).
#[cfg(feature = "owl")]
fn decay_half_life_secs() -> f64 {
    std::env::var("GRAPH_SERVICE_DECAY_HALF_LIFE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|h| *h > 0.0)
        .unwrap_or(604_800.0)
}

/// Wall-clock unix seconds — the `now` the Ebbinghaus fact-decay is measured against.
#[cfg(feature = "owl")]
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Run the native OWL 2 reasoner over an off-lock snapshot and materialize entailments
/// (CONCEPT:EG-KG.ontology.incremental-materialization / KG-2.236). Read-only — confidence-weighted classification /
/// consistency over the graph's axioms (+ any extra `ontology` Turtle); returns the
/// derived subsumptions + per-entailment confidence, the inferred instance memberships
/// (optionally restricted to `target_class`, thresholded by `min_confidence`), and
/// consistency.
#[cfg(feature = "owl")]
async fn handle_owl_reason(
    req_id: u64,
    core: &Arc<GraphCore>,
    ontology: String,
    target_class: String,
    min_confidence: f64,
) -> Response {
    let snap = core.analysis_snapshot();
    let now = now_secs();
    let hl = decay_half_life_secs();
    let resp = match compute_off_lock(req_id, move || {
        owl_reason(&[&snap], &ontology, &target_class, now, hl, min_confidence)
    })
    .await
    {
        Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
        Ok(Err(msg)) => Response::err(req_id, format!("OwlReason error: {msg}")),
        Err(resp) => resp,
    };
    resp
}

/// DISTRIBUTED reasoning over the UNION of `graphs` (CONCEPT:EG-KG.ontology.concept-13). Gathers each
/// graph's off-lock snapshot (the cross-shard union-read seam), then runs the SAME
/// weighted closure as the single-graph path over the unioned axioms + facts.
#[cfg(feature = "owl")]
async fn handle_owl_reason_distributed(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graphs: Vec<String>,
    ontology: String,
    target_class: String,
    min_confidence: f64,
) -> Response {
    // Gather each shard's snapshot under the registry lock, release before compute.
    let snaps: Vec<crate::graph::GraphView> = {
        let s = state.read().await;
        graphs
            .iter()
            .filter_map(|name| s.registry.get(name).map(|e| e.core.analysis_snapshot()))
            .collect()
    };
    let now = now_secs();
    let hl = decay_half_life_secs();
    let resp = match compute_off_lock(req_id, move || {
        let views: Vec<&crate::graph::GraphView> = snaps.iter().collect();
        owl_reason(&views, &ontology, &target_class, now, hl, min_confidence)
    })
    .await
    {
        Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
        Ok(Err(msg)) => Response::err(req_id, format!("OwlReasonDistributed error: {msg}")),
        Err(resp) => resp,
    };
    resp
}

/// Classify the UNION of `views` (+ optional extra axioms) with confidence propagation
/// and project the wire result. One view = the single-graph fast path; many views = the
/// distributed cross-shard path. Both go through `eg_rdf::owl::reason_distributed_weighted`.
#[cfg(feature = "owl")]
fn owl_reason(
    views: &[&crate::graph::GraphView],
    ontology: &str,
    target_class: &str,
    now: u64,
    half_life: f64,
    min_confidence: f64,
) -> Result<crate::protocol::OwlReasonResult, String> {
    let extra = if ontology.trim().is_empty() {
        Vec::new()
    } else {
        eg_rdf::mapping::parse_turtle(ontology)?
    };
    let res = eg_rdf::owl::reason_distributed_weighted(
        views,
        &extra,
        now,
        half_life,
        target_class,
        min_confidence,
    );

    let mut subclasses = Vec::with_capacity(res.subclasses.len());
    let mut subclass_conf = Vec::with_capacity(res.subclasses.len());
    for (sub, sup, c) in res.subclasses {
        subclasses.push((sub, sup));
        subclass_conf.push(c);
    }
    let mut instances = Vec::with_capacity(res.instances.len());
    let mut instance_conf = Vec::with_capacity(res.instances.len());
    for (inst, class, c) in res.instances {
        instances.push((inst, class));
        instance_conf.push(c);
    }

    Ok(crate::protocol::OwlReasonResult {
        subclasses,
        subclass_conf,
        instances,
        instance_conf,
        consistent: res.consistent,
        unsatisfiable: res.unsatisfiable,
    })
}

/// Parse Turtle/N-Triples and store into the target graph; route multi-valued
/// literal extras to the lossless quad store when configured.
#[cfg(feature = "rdf")]
async fn handle_add_triples(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    turtle: String,
    ntriples: String,
) -> Response {
    let triples = match parse_either(&turtle, &ntriples) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };

    #[cfg(feature = "rdf-redb")]
    let quads = state.read().await.rdf_quads.clone();
    #[cfg(not(feature = "rdf-redb"))]
    let _ = state;

    // Record the named-graph marker linking this RDF dataset to its registry graph.
    eg_rdf::mapping::register_named_graph(core, graph_name);

    let mut iris = eg_rdf::mapping::IriStore::default();
    let report = eg_rdf::mapping::load_triples(
        core,
        &mut iris,
        graph_name,
        triples,
        #[cfg(feature = "rdf-redb")]
        quads.as_deref(),
    );
    match report {
        Ok(r) => Response::ok(req_id, ResultPayload::raw(&r)),
        Err(e) => Response::err(req_id, format!("AddTriples error: {e}")),
    }
}

/// Serialize the target graph back OUT to N-Triples (unioning the lossless extras).
#[cfg(feature = "rdf")]
async fn handle_get_rdf(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
) -> Response {
    #[cfg(feature = "rdf-redb")]
    let quads = state.read().await.rdf_quads.clone();
    #[cfg(not(feature = "rdf-redb"))]
    let _ = state;

    let exported = eg_rdf::mapping::export_triples(
        core,
        graph_name,
        #[cfg(feature = "rdf-redb")]
        quads.as_deref(),
    );
    match exported.and_then(|t| eg_rdf::mapping::to_ntriples(&t)) {
        Ok(nt) => Response::ok(req_id, ResultPayload::raw(&nt)),
        Err(e) => Response::err(req_id, format!("GetRdf error: {e}")),
    }
}

/// Physically RETRACT triples from the target graph (CONCEPT:EG-KG.query.named-graph-support) — the durable
/// inverse of `AddTriples`. Routes through the reusable `eg_rdf::update::remove_triples`
/// engine op (surgical: literal cells + the one matching typed edge). Returns the count.
#[cfg(feature = "rdf")]
async fn handle_remove_triples(
    req_id: u64,
    core: &Arc<GraphCore>,
    turtle: String,
    ntriples: String,
) -> Response {
    let triples = match parse_either(&turtle, &ntriples) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };
    let removed = eg_rdf::update::remove_triples(core, &triples);
    Response::ok(req_id, ResultPayload::Count(removed as u64))
}

/// DROP the target named graph's RDF content (CONCEPT:EG-KG.query.named-graph-support): clear the property-graph
/// nodes/edges AND the lossless multi-valued-literal quad-store rows for this graph. The
/// graph stays addressable (distinct from `DeleteGraph` evicting the registry entry).
#[cfg(feature = "rdf")]
async fn handle_drop_named_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
) -> Response {
    core.clear();
    #[cfg(feature = "rdf-redb")]
    {
        let quads = state.read().await.rdf_quads.clone();
        if let Some(store) = quads {
            if let Err(e) = store.clear_graph(graph_name) {
                return Response::err(req_id, format!("DropNamedGraph quad-store clear: {e}"));
            }
        }
    }
    #[cfg(not(feature = "rdf-redb"))]
    let _ = (state, graph_name);
    Response::ok(req_id, ResultPayload::String("ok".to_string()))
}

/// Parse exactly one of Turtle / N-Triples (whichever is non-empty).
#[cfg(feature = "rdf")]
fn parse_either(turtle: &str, ntriples: &str) -> Result<Vec<eg_rdf::oxrdf::Triple>, String> {
    match (turtle.trim().is_empty(), ntriples.trim().is_empty()) {
        (false, true) => eg_rdf::mapping::parse_turtle(turtle),
        (true, false) => eg_rdf::mapping::parse_ntriples(ntriples),
        (true, true) => Err("AddTriples: both `turtle` and `ntriples` are empty".into()),
        (false, false) => Err("AddTriples: provide exactly one of `turtle` or `ntriples`".into()),
    }
}

// ── RunRules dispatch wiring (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog / EG-023) ────────────────────────────
#[cfg(all(test, feature = "rdf"))]
mod run_rules_dispatch_tests {
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::auth::compute_auth_token;
    use crate::server::dispatch;
    use crate::server::state::ServerState;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "run-rules-test-secret";

    fn state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: IsolationLayer::new(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            persistence: None,
            redb_authoritative: false,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::from_env()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen::default()),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "rdf-redb")]
            rdf_quads: None,
            #[cfg(feature = "streaming")]
            cdc: Some(Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: Arc::new(DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        Request {
            id,
            graph: "__commons__".into(),
            auth_token: compute_auth_token(SECRET, id),
            agent_id: None,
            method,
        }
    }

    /// A `RunRules` dispatched over the wire returns the DERIVED facts (CONCEPT:EG-KG.query.mirrors-pgwire):
    /// the `grandparent` entailment from the two `parent` ABox triples + the SWRL rule.
    #[tokio::test]
    async fn run_rules_returns_inferred_facts_via_dispatch() {
        let state = state();
        let resp: Response = dispatch(
            &state,
            req(
                1,
                Method::RunRules {
                    ontology_ttl: "@prefix ex: <http://ex/> .\nex:alice ex:parent ex:bob .\nex:bob ex:parent ex:carol .\n".into(),
                    rules: vec![
                        "parent(?x,?y) ^ parent(?y,?z) -> grandparent(?x,?z) @0.8".into(),
                    ],
                    query_predicate: Some("grandparent".into()),
                    min_confidence: 0.0,
                    derived_only: true,
                },
            ),
        )
        .await;
        assert!(resp.error.is_none(), "RunRules failed: {:?}", resp.error);
        let bytes = match &resp.result {
            Some(ResultPayload::Raw(b)) => b.clone(),
            other => panic!("expected Raw, got {other:?}"),
        };
        let out: eg_rdf::rules::RuleReasonResponse =
            rmp_serde::from_slice(&bytes).expect("RuleReasonResponse");
        assert!(out.consistent);
        assert_eq!(out.registered_rules.len(), 1);
        assert_eq!(
            out.facts.len(),
            1,
            "only the grandparent fact: {:?}",
            out.facts
        );
        assert_eq!(out.facts[0].predicate, "grandparent");
        assert!(out.facts[0].derived, "the returned fact is an inference");
    }
}
