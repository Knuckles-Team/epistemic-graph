//! Native RDF/SPARQL handler (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql / KG-2.218, features `rdf`/`sparql`).
//!
//! Owns the `// ── RDF/SPARQL ──` protocol section — `AddTriples` / `GetRdf`
//! (feature `rdf`) and `Sparql` (feature `sparql`). These are GRAPH-SCOPED ops: the
//! RDF dataset maps onto the SAME property-graph the rest of the engine uses (a
//! resource object ⇒ a typed edge, a literal object ⇒ a typed property cell,
//! `rdf:type` ⇒ the engine `type` label, a named graph ⇒ the target registry graph).
//! So they route through the normal `dispatch_graph_op` chain like Sql/Cypher.
//!
//! * `AddTriples` is a DURABLE MUTATION (it writes nodes + edges). The mutation
//!   gateway runs it against an isolated graph image, commits that complete image
//!   through `MutationBatch`, then publishes it to RAM.
//! * `GetRdf` serializes the graph back OUT to N-Triples (read-only).
//! * `Sparql` evaluates a SPARQL 1.1 SELECT over an off-lock GraphView snapshot
//!   (read-only), same idiom as the SQL/Cypher handlers.
//!
//! Multi-valued literals live inside the authoritative node blob, so RDF has no
//! secondary persistence or read authority.

#![allow(clippy::result_large_err)]

use std::sync::Arc;

use tokio::sync::RwLock;

#[cfg(feature = "owl")]
use super::super::access::check_graph_access;
use super::super::access::GraphReadAuthority;
#[cfg(any(feature = "rdf", feature = "sparql", feature = "owl"))]
use super::super::compute::compute_off_lock;
use super::super::state::ServerState;
use crate::graph::GraphCore;
#[cfg(feature = "owl")]
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};

/// Handle the RDF/SPARQL methods. `Err(method)` hands a non-RDF method back to the
/// dispatch chain (routing fall-through); dispatch only ever routes RDF methods here.
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    ctx: super::TryHandleContext<'_>,
    core: Arc<GraphCore>,
    method: Method,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Result<Response, Method> {
    let super::TryHandleContext {
        req_id,
        graph_name,
        read_authority,
        caller,
    } = ctx;
    #[cfg(not(feature = "security"))]
    let _ = caller;
    // `caller`/`rls` are consumed only by the `sparql`-gated read path below; in a
    // `security`-but-no-`sparql` build keep them referenced (no dead-param warning).
    #[cfg(all(feature = "security", not(feature = "sparql")))]
    let _ = (caller, rls);
    match method {
        #[cfg(feature = "rdf")]
        Method::AddTriples { turtle, ntriples } => {
            Ok(handle_add_triples(req_id, graph_name, &core, turtle, ntriples).await)
        }
        #[cfg(feature = "rdf")]
        Method::GetRdf => {
            let authority =
                read_authority.expect("GetRdf must carry the universal served-read authority");
            let core = authority.project_core(&core);
            Ok(handle_get_rdf(req_id, graph_name, &core).await)
        }
        #[cfg(feature = "rdf")]
        Method::RemoveTriples { turtle, ntriples } => Ok(handle_remove_triples(
            #[cfg(feature = "shacl")]
            state,
            req_id,
            #[cfg(feature = "shacl")]
            graph_name,
            &core,
            turtle,
            ntriples,
        )
        .await),
        #[cfg(feature = "rdf")]
        Method::DropNamedGraph => Ok(handle_drop_named_graph(req_id, graph_name, &core).await),
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
                    let kind = format!("rls:{caller}:sparql");
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
                rls.filter_view(caller, &mut snap);
                (snap, version, hash)
            };
            #[cfg(not(feature = "result-cache"))]
            #[cfg_attr(not(feature = "security"), allow(unused_mut))]
            let snap = {
                let mut snap = core.analysis_snapshot();
                #[cfg(feature = "security")]
                rls.filter_view(caller, &mut snap);
                snap
            };
            let resp = match compute_off_lock(req_id, move || {
                eg_rdf::sparql::execute(
                    &eg_rdf::sparql::Dataset::new(&snap, Vec::new()),
                    &query,
                    &proj,
                    None,
                )
                .map(eg_rdf::sparql::QueryOutcome::into_table)
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
        } => {
            let authority =
                read_authority.expect("OwlReason must carry the universal served-read authority");
            let core = authority.project_core(&core);
            Ok(handle_owl_reason(req_id, &core, ontology, target_class, min_confidence).await)
        }
        #[cfg(feature = "owl")]
        Method::OwlExplain { ontology, sub, sup } => {
            let authority =
                read_authority.expect("OwlExplain must carry the universal served-read authority");
            let core = authority.project_core(&core);
            Ok(handle_owl_explain(req_id, &core, ontology, sub, sup).await)
        }
        // OBDA / R2RML virtual graph query (CONCEPT:EG-KG.query.r2rml-virtual-graph). NOT
        // graph-scoped in any meaningful way (the virtual triples come from the OBDA
        // registry, not the request's graph core) — it matches here anyway (like
        // Sparql/OwlReason) so it rides the SAME dispatch chain / ACL / result-cache
        // plumbing with no new top-level routing. A build without `obda` drops the arm
        // and the variant isn't in the enum.
        #[cfg(feature = "obda")]
        Method::SparqlVirtual {
            query,
            mapping,
            tables,
        } => {
            let authority = read_authority
                .and_then(GraphReadAuthority::carrier)
                .expect("SparqlVirtual must carry verified tenant authority");
            let persist_dir = state.read().await.persist_dir.clone();
            let store = match crate::server::sql_tables::user_table_store(
                authority,
                persist_dir.as_deref().map(std::path::Path::new),
            ) {
                Ok(store) => store,
                Err(error) => return Ok(Response::err(req_id, error)),
            };
            Ok(handle_sparql_virtual(req_id, store, query, mapping, tables).await)
        }
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
            let authority = read_authority
                .expect("ShaclValidate must carry the universal served-read authority");
            let core = authority.project_core(&core);
            Ok(handle_shacl_validate(req_id, graph_name, &core, shapes, data_graph).await)
        }
        // IcvConfigure (CONCEPT:EG-P0-2 bypass guard, L11): GATEWAY_ROUTED —
        // `dispatch_graph_op` routes it through `graph_ops::try_handle_gateway`
        // BEFORE this handler is ever reached, so this arm is structurally
        // unreachable here now, not merely undocumented.
        #[cfg(feature = "shacl")]
        Method::IcvConfigure { .. } => unreachable!(
            "IcvConfigure is mutation::GATEWAY_ROUTED; dispatch_graph_op must route \
             it through try_handle_gateway before it ever reaches this fallback handler"
        ),
        // ShEx Core validation (CONCEPT:EG-KG.compute.concept-2). Read-only. Gated `shex`; a build without
        // it drops this arm → `other => Err(other)` → the dispatch not-available catch-all
        // (the variant is unconditional in the enum, like ShaclValidate/EG-132).
        #[cfg(feature = "shex")]
        Method::ShexValidate {
            schema,
            data_graph,
            shape_map,
        } => {
            let authority = read_authority
                .expect("ShexValidate must carry the universal served-read authority");
            let core = authority.project_core(&core);
            Ok(
                handle_shex_validate(req_id, graph_name, &core, schema, data_graph, shape_map)
                    .await,
            )
        }
        other => Err(other),
    }
}

/// Validate the request graph (or an inline `data_graph` Turtle document) against a
/// SHACL `shapes` Turtle document (CONCEPT:EG-KG.ontology.concept-6), returning a `Json`
/// `sh:ValidationReport`. Read-only: an empty `data_graph` exports the LIVE graph's RDF
/// (the same triples `GetRdf` serializes) and validates that.
#[cfg(feature = "shacl")]
async fn handle_shacl_validate(
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
        let exported = eg_rdf::mapping::export_triples(core, graph_name);
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
        let exported = eg_rdf::mapping::export_triples(core, graph_name);
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
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Response {
    #[cfg_attr(not(feature = "security"), allow(unused_mut))]
    let mut snap = core.analysis_snapshot();
    #[cfg(feature = "security")]
    rls.filter_view(caller, &mut snap);
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
    read_authority: &GraphReadAuthority,
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
            read_authority,
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
    read_authority: &GraphReadAuthority,
    graphs: Vec<String>,
    ontology: String,
    target_class: String,
    min_confidence: f64,
) -> Response {
    // Resolve and ACL-check every shard under the registry lock, then project each
    // core after releasing it. No graph in the union can become an RLS bypass.
    let cores = {
        let s = state.read().await;
        let mut cores = Vec::with_capacity(graphs.len());
        for name in &graphs {
            let Some(entry) = s.registry.get(name) else {
                continue;
            };
            if let Err(denied) = check_graph_access(
                &s.isolation,
                read_authority.actor(),
                name,
                entry.graph_type,
                entry.owner.as_deref(),
                AccessLevel::Read,
            ) {
                return Response::err(req_id, denied);
            }
            cores.push(entry.core.clone());
        }
        cores
    };
    let snaps: Vec<crate::graph::GraphView> = cores
        .iter()
        .map(|core| read_authority.project_core(core).analysis_snapshot())
        .collect();
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
    let class_base = eg_rdf::owl::class_namespace(target_class).ok_or_else(|| {
        "OwlReason requires an absolute target class with a current class namespace".to_string()
    })?;
    let res = eg_rdf::owl::reason_distributed_weighted(
        views,
        &extra,
        now,
        half_life,
        &class_base,
        target_class,
        min_confidence,
    )?;

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

/// Run the native OWL 2 reasoner over an off-lock snapshot and reconstruct the PROOF
/// TREE for one named-class subsumption `sub ⊑ sup` (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation —
/// Stardog's flagship "explanation" feature, native here). Classifies the graph's own
/// TBox axioms (+ any extra `ontology` Turtle) WITH confidence propagation (the same
/// classifier `OwlReason` uses), then reconstructs
/// [`eg_rdf::owl::Classification::explain`]'s recursive justification tree and projects
/// it to the wire [`crate::protocol::OwlExplainResult`]. Read-only. Gated `owl`.
#[cfg(feature = "owl")]
async fn handle_owl_explain(
    req_id: u64,
    core: &Arc<GraphCore>,
    ontology: String,
    sub: String,
    sup: String,
) -> Response {
    let snap = core.analysis_snapshot();
    let resp =
        match compute_off_lock(req_id, move || owl_explain(&snap, &ontology, &sub, &sup)).await {
            Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
            Ok(Err(msg)) => Response::err(req_id, format!("OwlExplain error: {msg}")),
            Err(resp) => resp,
        };
    resp
}

/// Classify `view` (+ optional extra `ontology` Turtle axioms) with confidence
/// propagation, canonicalize `sub`/`sup` into the reasoner's `<iri>` node-id form, and
/// project [`eg_rdf::owl::Classification::explain`]'s proof tree to the wire shape.
#[cfg(feature = "owl")]
fn owl_explain(
    view: &crate::graph::GraphView,
    ontology: &str,
    sub: &str,
    sup: &str,
) -> Result<crate::protocol::OwlExplainResult, String> {
    let extra = if ontology.trim().is_empty() {
        Vec::new()
    } else {
        eg_rdf::mapping::parse_turtle(ontology)?
    };
    let mut triples = eg_rdf::owl::tbox_triples_from_view(view);
    triples.extend(extra);

    let mut reasoner = eg_rdf::owl::Reasoner::from_triples(&triples);
    let cls = reasoner.classify_weighted();

    let canon = |s: &str| -> String {
        if s.starts_with('<') {
            s.to_string()
        } else {
            format!("<{}>", s.trim_start_matches('<').trim_end_matches('>'))
        }
    };
    let sub = canon(sub);
    let sup = canon(sup);

    let tree = cls.explain(&sub, &sup).map(proof_node_to_wire);
    Ok(crate::protocol::OwlExplainResult {
        found: tree.is_some(),
        tree,
        consistent: cls.consistent,
        unsatisfiable: cls.unsatisfiable.into_iter().collect(),
    })
}

/// Recursively project an `eg_rdf::owl::ProofNode` into its wire twin
/// (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation). A plain field-for-field walk — the tree shape is
/// identical on both sides, this only crosses the eg-rdf → eg-types boundary.
#[cfg(feature = "owl")]
fn proof_node_to_wire(node: eg_rdf::owl::ProofNode) -> crate::protocol::ProofNodeWire {
    crate::protocol::ProofNodeWire {
        sub: node.sub,
        sup: node.sup,
        rule: node.rule,
        axioms: node.axioms,
        confidence: node.confidence,
        premises: node.premises.into_iter().map(proof_node_to_wire).collect(),
    }
}

/// OBDA / R2RML virtual-graph query (CONCEPT:EG-KG.query.r2rml-virtual-graph /
/// CONCEPT:EG-KG.query.obda-query-rewrite). Registers each of `tables` as a foreign
/// [`eg_rdf::obda::ObdaSource`] backed by the engine's own SQL user-table store
/// ([`crate::server::sql_tables::user_table_store`]), parses `mapping` (auto-detecting
/// standard R2RML Turtle vs. the compact EG-101 textual form), and runs `query` through
/// the OBDA query-rewrite path: only the query-relevant columns are scanned from the
/// table(s), only the query-relevant triples are materialized into a TRANSIENT view —
/// the user table itself is never mutated, and nothing is persisted into any graph.
/// Read-only. Gated `obda` (implies `sparql` + `query`).
#[cfg(feature = "obda")]
async fn handle_sparql_virtual(
    req_id: u64,
    store: eg_query::TableStore,
    query: String,
    mapping: String,
    tables: Vec<String>,
) -> Response {
    let out =
        tokio::task::spawn_blocking(move || sparql_virtual(&store, &query, &mapping, &tables))
            .await;
    match out {
        Ok(Ok(result)) => Response::ok(req_id, ResultPayload::raw(&result)),
        Ok(Err(msg)) => Response::err(req_id, format!("SparqlVirtual error: {msg}")),
        Err(e) => Response::err(req_id, format!("SparqlVirtual task join error: {e}")),
    }
}

/// A [`eg_rdf::obda::ObdaSource`] backed by one table of the verified caller's owner-scoped
/// [`eg_query::TableStore`] (CONCEPT:EG-KG.query.r2rml-virtual-graph). Bridges the
/// SQL-side typed [`eg_query::Cell`] to the OBDA seam's lexical `String` columns (R2RML
/// templates/literal object maps consume the lexical form; typed SQL round-tripping
/// through a `rr:datatype` object map is a documented follow-up, mirrored from the
/// EG-101 `ObdaSource` contract). The full row is always scanned from the table (the
/// store's `scan` has no column-projection API); THIS layer still applies the
/// `needed`-column projection when handing rows back, so the OBDA-side pushdown
/// contract (only query-relevant columns end up in a materialized triple) still holds
/// even though the underlying store scan itself is not column-pushed.
#[cfg(feature = "obda")]
struct TableStoreSource {
    schema: eg_query::TableSchema,
    rows: Vec<Vec<eg_query::Cell>>,
}

#[cfg(feature = "obda")]
impl TableStoreSource {
    fn load(store: &eg_query::TableStore, table: &str) -> Result<Self, String> {
        let schema = store
            .get_schema(table)?
            .ok_or_else(|| format!("obda: unknown user table `{table}`"))?;
        let rows = store.scan(table)?;
        Ok(Self { schema, rows })
    }

    /// The lexical form of one cell (R2RML templates/literal object maps consume a
    /// string; `Null` yields no lexical value so a template/column referencing it
    /// correctly omits the triple, per the OBDA `expand_template`/`term_for` contract).
    fn lexical(cell: &eg_query::Cell) -> Option<String> {
        use eg_query::Cell;
        match cell {
            Cell::Null => None,
            Cell::Int(i) => Some(i.to_string()),
            Cell::Float(f) => Some(f.to_string()),
            Cell::Text(s) => Some(s.clone()),
            Cell::Bool(b) => Some(b.to_string()),
            Cell::Timestamp(t) => Some(t.to_string()),
            Cell::Bytes(b) => Some(hex_lexical(b)),
            Cell::Json(v) => Some(v.to_string()),
            Cell::Vector(v) => Some(format!("{v:?}")),
        }
    }
}

/// Hex-encode bytes for a lexical fallback (a `Bytes` cell has no natural R2RML lexical
/// form; hex is a stable, unambiguous rendering). No new dependency — a tiny hand-rolled
/// encoder, mirroring the idiom the rest of this crate uses to avoid pulling `hex` where
/// a two-line loop suffices.
#[cfg(feature = "obda")]
fn hex_lexical(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(feature = "obda")]
impl eg_rdf::obda::ObdaSource for TableStoreSource {
    fn columns(&self) -> Vec<String> {
        self.schema
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect()
    }

    fn scan(
        &self,
        needed: &std::collections::BTreeSet<String>,
    ) -> Result<Vec<eg_rdf::obda::ForeignRow>, String> {
        let mut out = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let mut fr = eg_rdf::obda::ForeignRow::new();
            for (col, cell) in self.schema.columns().iter().zip(row.iter()) {
                if !needed.is_empty() && !needed.contains(&col.name) {
                    continue;
                }
                if let Some(v) = Self::lexical(cell) {
                    fr.insert(col.name.clone(), v);
                }
            }
            out.push(fr);
        }
        Ok(out)
    }
}

/// The blocking half of [`handle_sparql_virtual`]: build the [`eg_rdf::obda::ObdaSourceRegistry`]
/// from `tables`, parse `mapping`, run the OBDA query-rewrite, and project the SPARQL
/// result to the wire shape. Split out (not `async`) so it runs on the blocking pool —
/// the table scan + SPARQL evaluation are both synchronous CPU/redb-read work.
#[cfg(feature = "obda")]
fn sparql_virtual(
    store: &eg_query::TableStore,
    query: &str,
    mapping: &str,
    tables: &[String],
) -> Result<crate::protocol::SparqlResult, String> {
    let mut reg = eg_rdf::obda::ObdaSourceRegistry::new();
    for table in tables {
        let src = TableStoreSource::load(store, table)?;
        reg.register(table.clone(), std::sync::Arc::new(src));
    }

    // Auto-detect standard R2RML Turtle (carries the `rr:` namespace) vs. the compact
    // EG-101 textual mapping form (`SOURCE`/`SUBJECT`/… directives).
    let vg = if mapping.contains("r2rml#") || mapping.contains("TriplesMap") {
        eg_rdf::obda::parse_r2rml_turtle(mapping)?
    } else {
        eg_rdf::obda::parse_mapping(mapping)?
    };

    let proj = eg_rdf::sparql::Projection::raw();
    let outcome = eg_rdf::obda::run_outcome_virtual(&vg, &reg, query, &proj)?;
    let (vars, rows) = match outcome {
        eg_rdf::sparql::QueryOutcome::Solutions(res) => res.to_rows(),
        eg_rdf::sparql::QueryOutcome::Boolean(b) => {
            (vec!["_ask".to_string()], vec![vec![Some(b.to_string())]])
        }
        #[cfg(feature = "rdf")]
        eg_rdf::sparql::QueryOutcome::Graph(_) => (Vec::new(), Vec::new()),
    };
    Ok(crate::protocol::SparqlResult { vars, rows })
}

/// Parse Turtle/N-Triples and store into the target graph; route multi-valued
/// literal extras to the lossless quad store when configured.
#[cfg(feature = "rdf")]
async fn handle_add_triples(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    turtle: String,
    ntriples: String,
) -> Response {
    #[cfg(not(feature = "shacl"))]
    return Response::err(
        req_id,
        "AddTriples requires the shacl integrity-guard feature",
    );

    let triples = match parse_either(&turtle, &ntriples) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };

    // X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard): reject BEFORE the write
    // lands. Missing feature, policy, or invalid policy all fail closed.
    #[cfg(feature = "shacl")]
    if let Err(rej) = crate::server::icv_guard::check_before_write(core, graph_name, &triples, &[])
    {
        return Response::err(req_id, format!("AddTriples rejected: {rej}"));
    }

    // Record the named-graph marker linking this RDF dataset to its registry graph.
    eg_rdf::mapping::register_named_graph(core, graph_name);

    let mut iris = eg_rdf::mapping::IriStore::default();
    let report = eg_rdf::mapping::load_triples(core, &mut iris, graph_name, triples);
    match report {
        Ok(r) => Response::ok(req_id, ResultPayload::raw(&r)),
        Err(e) => Response::err(req_id, format!("AddTriples error: {e}")),
    }
}

/// Serialize the target graph back OUT to N-Triples (unioning the lossless extras).
#[cfg(feature = "rdf")]
async fn handle_get_rdf(req_id: u64, graph_name: &str, core: &Arc<GraphCore>) -> Response {
    let exported = eg_rdf::mapping::export_triples(core, graph_name);
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
    #[cfg(feature = "shacl")] _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    turtle: String,
    ntriples: String,
) -> Response {
    #[cfg(not(feature = "shacl"))]
    return Response::err(
        req_id,
        "RemoveTriples requires the shacl integrity-guard feature",
    );

    let triples = match parse_either(&turtle, &ntriples) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, e),
    };

    // X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard): reject BEFORE the
    // removal lands — see `handle_add_triples`.
    #[cfg(feature = "shacl")]
    {
        if let Err(rej) =
            crate::server::icv_guard::check_before_write(core, graph_name, &[], &triples)
        {
            return Response::err(req_id, format!("RemoveTriples rejected: {rej}"));
        }
    }

    let removed = eg_rdf::update::remove_triples(core, &triples);
    Response::ok(req_id, ResultPayload::Count(removed as u64))
}

/// DROP the target named graph's RDF content (CONCEPT:EG-KG.query.named-graph-support).
/// The lossless dataset lives inside the authoritative graph snapshot, so this
/// one clear is staged and committed atomically.
#[cfg(feature = "rdf")]
async fn handle_drop_named_graph(req_id: u64, graph_name: &str, core: &Arc<GraphCore>) -> Response {
    #[cfg(not(feature = "shacl"))]
    return Response::err(
        req_id,
        "DropNamedGraph requires the shacl integrity-guard feature",
    );

    #[cfg(feature = "shacl")]
    {
        let removals = match eg_rdf::mapping::export_triples(core, graph_name) {
            Ok(removals) => removals,
            Err(error) => return Response::err(req_id, error),
        };
        if let Err(rejection) =
            crate::server::icv_guard::check_before_write(core, graph_name, &[], &removals)
        {
            return Response::err(req_id, format!("DropNamedGraph rejected: {rejection}"));
        }
    }
    core.clear();
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
    use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
    use crate::channels::ChannelManager;
    use crate::isolation::IsolationLayer;
    use crate::protocol::{Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use crate::server::dispatch;
    use crate::server::state::ServerState;
    use crate::server::{compute_verified_envelope_token, VerifiedEnvelopeParams};
    use dashmap::DashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "run-rules-test-secret";
    const TEST_AGENT: &str = "unit-test-agent";
    static NONCE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

    fn current_isolation() -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: TEST_AGENT.to_string(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation
    }

    fn state() -> Arc<RwLock<ServerState>> {
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation: current_isolation(),
            channels: ChannelManager::new(),
            auth_secret: SECRET.to_string(),
            persist_dir: Some(
                crate::server::sql_tables::test_persist_dir()
                    .to_string_lossy()
                    .into_owned(),
            ),
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
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
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    fn req(id: u64, method: Method) -> Request {
        std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
        std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
        std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
        std::env::set_var(
            "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
            std::env::temp_dir().join(format!("epistemic-graph-unit-auth-{}", std::process::id())),
        );
        let context = RequestContextClaims {
            principal: TEST_AGENT.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: TEST_AGENT.to_string(),
            roles: Vec::new(),
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
        };
        let mut request = Request {
            id,
            graph: "__commons__".into(),
            auth_token: String::new(),
            agent_id: Some(TEST_AGENT.to_string()),
            method,
        };
        let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let issued_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the system clock is after the Unix epoch");
        let nonce = format!(
            "rdf-rules-{}-{id}-{sequence}-{}",
            std::process::id(),
            issued_at.as_nanos()
        );
        let idempotency_key = format!("rdf-rules-request-{id}-{sequence}");
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &context,
                timestamp: issued_at.as_secs(),
                nonce: &nonce,
                idempotency_key: &idempotency_key,
            },
        );
        request
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
