use std::sync::Arc;

#[cfg(feature = "owl")]
use tokio::sync::RwLock;

use crate::graph::GraphCore;
#[cfg(feature = "owl")]
use crate::isolation::AccessLevel;
#[cfg(feature = "owl")]
use crate::protocol::Method;
use crate::protocol::{Response, ResultPayload};
#[cfg(feature = "owl")]
use crate::server::access::{check_graph_access, GraphReadAuthority};
use crate::server::compute::compute_off_lock;
#[cfg(feature = "owl")]
use crate::server::state::ServerState;

/// Run a parameterised custom-rule reasoning request over the request's graph view
/// (CONCEPT:EG-KG.ontology.eg-runtime-swrl-datalog / EG-023). Read-only: it reasons over an off-lock snapshot (its
/// folded TBox axioms + asserted facts) plus any inline `ontology_ttl` and the user
/// `rules`, then returns the inferred facts as a `Raw` [`eg_rdf::rules::RuleReasonResponse`].
/// The snapshot is RLS-filtered to the caller's visible rows BEFORE reasoning, so the
/// inference cannot surface a forbidden fact.
#[cfg(feature = "rdf")]
pub(super) struct RunRulesRequest<'a> {
    pub(super) req_id: u64,
    pub(super) core: &'a Arc<GraphCore>,
    pub(super) ontology_ttl: String,
    pub(super) rules: Vec<String>,
    pub(super) query_predicate: Option<String>,
    pub(super) min_confidence: f64,
    pub(super) derived_only: bool,
    #[cfg(feature = "security")]
    pub(super) caller: &'a str,
    #[cfg(feature = "security")]
    pub(super) rls: &'a Arc<crate::isolation::IsolationLayer>,
}

#[cfg(feature = "rdf")]
pub(super) async fn handle_run_rules(request: RunRulesRequest<'_>) -> Response {
    let RunRulesRequest {
        req_id,
        core,
        ontology_ttl,
        rules,
        query_predicate,
        min_confidence,
        derived_only,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    } = request;
    // perf/row-visibility-index (B-sweep): same per-(actor,version)
    // `FilteredViewCache` probe-then-build as the `Sparql` arm above — see that
    // site's comment for the full "version before the snapshot is a safe lower
    // bound" reasoning. Unlike `Sparql`, `RunRules` has no `result-cache`-gated
    // sibling: it never used `analysis_snapshot_versioned` even when
    // `result-cache` is on, so this wiring applies unconditionally here.
    #[cfg(feature = "security")]
    let snap: Arc<crate::graph::GraphView> = {
        let probe_version = core.version();
        match core.cached_filtered_view(caller, probe_version) {
            Some(cached) => cached,
            None => {
                let generation = core.filtered_view_cache_generation();
                let mut snap = core.analysis_snapshot();
                rls.filter_view(caller, &mut snap);
                let snap = Arc::new(snap);
                core.put_cached_filtered_view(
                    caller.to_string(),
                    probe_version,
                    generation,
                    snap.clone(),
                );
                snap
            }
        }
    };
    #[cfg(not(feature = "security"))]
    let snap = Arc::new(core.analysis_snapshot());
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
        Ok(Ok(response)) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::reasoning::RunRules>(&response),
        ),
        Ok(Err(msg)) => Response::err(req_id, format!("RunRules error: {msg}")),
        Err(resp) => resp,
    }
}

/// Top-level routing for the cross-shard `OwlReasonDistributed` method (CONCEPT:EG-KG.ontology.concept-13).
/// It is NOT graph-scoped (it unions several graphs), so dispatch routes it here directly
/// with `state` rather than through `dispatch_graph_op`. `Err(method)` ⇒ not mine.
#[cfg(feature = "owl")]
pub(in crate::server) async fn try_handle_distributed(
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
            class_base,
            min_confidence,
        } => Ok(handle_owl_reason_distributed(
            state,
            req_id,
            read_authority,
            OwlReasonDistributedRequest {
                graphs,
                ontology,
                target_class,
                class_base,
                min_confidence,
            },
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
pub(super) async fn handle_owl_reason(
    req_id: u64,
    core: &Arc<GraphCore>,
    ontology: String,
    target_class: String,
    class_base: String,
    min_confidence: f64,
) -> Response {
    let snap = core.analysis_snapshot();
    let now = now_secs();
    let hl = decay_half_life_secs();
    let resp = match compute_off_lock(req_id, move || {
        owl_reason(
            &[&snap],
            &ontology,
            &target_class,
            &class_base,
            now,
            hl,
            min_confidence,
        )
    })
    .await
    {
        Ok(Ok(result)) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::reasoning::OwlReason>(&result),
        ),
        Ok(Err(msg)) => Response::err(req_id, format!("OwlReason error: {msg}")),
        Err(resp) => resp,
    };
    resp
}

#[cfg(feature = "owl")]
pub(super) struct OwlReasonDistributedRequest {
    pub(super) graphs: Vec<String>,
    pub(super) ontology: String,
    pub(super) target_class: String,
    pub(super) class_base: String,
    pub(super) min_confidence: f64,
}

/// DISTRIBUTED reasoning over the UNION of `graphs` (CONCEPT:EG-KG.ontology.concept-13). Gathers each
/// graph's off-lock snapshot (the cross-shard union-read seam), then runs the SAME
/// weighted closure as the single-graph path over the unioned axioms + facts.
#[cfg(feature = "owl")]
pub(super) async fn handle_owl_reason_distributed(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    read_authority: &GraphReadAuthority,
    request: OwlReasonDistributedRequest,
) -> Response {
    let OwlReasonDistributedRequest {
        graphs,
        ontology,
        target_class,
        class_base,
        min_confidence,
    } = request;
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
        owl_reason(
            &views,
            &ontology,
            &target_class,
            &class_base,
            now,
            hl,
            min_confidence,
        )
    })
    .await
    {
        Ok(Ok(result)) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::reasoning::OwlReasonDistributed>(
                &result,
            ),
        ),
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
    class_base: &str,
    now: u64,
    half_life: f64,
    min_confidence: f64,
) -> Result<crate::protocol::OwlReasonResult, String> {
    let extra = if ontology.trim().is_empty() {
        Vec::new()
    } else {
        eg_rdf::mapping::parse_turtle(ontology)?
    };
    // BUG-281: `class_base` (the namespace a bare string node `type` bridges into) is
    // independent of `target_class` (which ONLY filters `instances`, and is legitimately
    // empty per its own documented "all classes" contract). Prefer an explicit
    // `class_base`; fall back to deriving one from an absolute `target_class` for a
    // caller that only ever set that field (the pre-existing convenience).
    let base_source = if !class_base.trim().is_empty() {
        class_base
    } else {
        target_class
    };
    // Both empty: no restriction AND no namespace requested at all -- a legitimate
    // "reason over everything, using my graph's own local vocabulary" call, not an
    // error. `bridge_type_identity_or_class` (eg-rdf) then keeps a bare `type` bare
    // instead of fabricating or rejecting a namespace for it. A NON-empty but
    // unparseable `base_source` (a caller who DID try to supply one) still errors --
    // that stays a caller mistake, not silently degraded to identity mode.
    let class_base = if base_source.trim().is_empty() {
        String::new()
    } else {
        eg_rdf::owl::class_namespace(base_source).ok_or_else(|| {
            "OwlReason requires an absolute class_base (or target_class) with a current class namespace".to_string()
        })?
    };
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
pub(super) async fn handle_owl_explain(
    req_id: u64,
    core: &Arc<GraphCore>,
    ontology: String,
    sub: String,
    sup: String,
) -> Response {
    let snap = core.analysis_snapshot();
    let resp =
        match compute_off_lock(req_id, move || owl_explain(&snap, &ontology, &sub, &sup)).await {
            Ok(Ok(result)) => Response::ok(
                req_id,
                ResultPayload::of_ref::<eg_types::result_contract::reasoning::OwlExplain>(&result),
            ),
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
