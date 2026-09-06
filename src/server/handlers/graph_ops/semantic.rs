use super::*;

use super::terminal::GraphOpsContext;

const DISCOVER_FANOUT: usize = 4;
const DISCOVER_LEX_SCAN_CAP: usize = 4096;
const DISCOVER_SEM_WEIGHT: f32 = 0.6;
const DISCOVER_KW_WEIGHT: f32 = 0.4;

/// Read a node's human-readable `(name, description, type)` triple from its
/// MessagePack property blob (CONCEPT:EG-KG.retrieval.one-round-trip-discovery), used to hydrate `Discover` hits.
/// `name` falls back to the node id, `type` falls back to a `node_type` field, and
/// a missing/undecodable blob yields `(id, "", "")` — so the op never fails on a
/// text-less node, it just returns what it can.
fn node_text(core: &GraphCore, node_id: &str) -> (String, String, String) {
    let Some(blob) = core.get_node_properties(node_id) else {
        return (node_id.to_string(), String::new(), String::new());
    };
    let obj = super::gateway_graph::decode_json_object(&blob);
    let get = |k: &str| {
        obj.get(k)
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .to_string()
    };
    let name = {
        let n = get("name");
        if n.is_empty() {
            node_id.to_string()
        } else {
            n
        }
    };
    let ntype = {
        let t = get("type");
        if t.is_empty() {
            get("node_type")
        } else {
            t
        }
    };
    (name, get("description"), ntype)
}

/// Fraction of the distinct query keywords that appear (as a substring, case-insensitively)
/// anywhere in a candidate's `name`/`description`/`type` — a `[0.0, 1.0]` lexical score.
fn keyword_overlap(kws: &[String], name: &str, description: &str, ntype: &str) -> f32 {
    if kws.is_empty() {
        return 0.0;
    }
    let haystack = format!(
        "{} {} {}",
        name.to_lowercase(),
        description.to_lowercase(),
        ntype.to_lowercase()
    );
    let matched = kws.iter().filter(|kw| haystack.contains(*kw)).count();
    matched as f32 / kws.len() as f32
}

/// One-round-trip hybrid discovery (CONCEPT:EG-KG.retrieval.one-round-trip-discovery).
///
/// Ranks nodes by BOTH lexical keyword overlap (over `name`/`description`/`type`)
/// AND semantic similarity to `query_embedding`, returning the top-`k` hydrated
/// with their human-readable text as `[{id,name,description,type,score}, …]`.
/// Complements [`Method::SemanticSearch`], which returns bare `(id, score)` and
/// leaves keyword matching + text hydration to the caller.
///
/// Candidate generation reuses the HNSW batch primitive
/// ([`SemanticStore::semantic_search`]) — over-fetched by [`DISCOVER_FANOUT`] so
/// the keyword re-rank has headroom — rather than an O(N) scan. Only when
/// `query_embedding` is empty (embedder/vLLM degraded) does it fall back to a
/// keyword-only scan, and even then bounded by [`DISCOVER_LEX_SCAN_CAP`].
///
/// Scoring (all reads are cheap in-memory, so this runs inline):
/// * both signals → `DISCOVER_SEM_WEIGHT · sim + DISCOVER_KW_WEIGHT · kw`;
/// * embedding only → `sim`;
/// * keywords only → `kw`.
fn discover(
    core: &GraphCore,
    keywords: &[String],
    query_embedding: &[f32],
    k: usize,
    req_id: u64,
) -> Response {
    // De-duplicate + lowercase the keyword set (order-independent).
    let kws: Vec<String> = {
        let mut seen = std::collections::BTreeSet::new();
        keywords
            .iter()
            .map(|w| w.to_lowercase())
            .filter(|w| !w.is_empty() && seen.insert(w.clone()))
            .collect()
    };
    let has_kw = !kws.is_empty();
    let has_emb = !query_embedding.is_empty();

    // Per-candidate score components, keyed by node id.
    struct Hit {
        sim: f32,
        kw: f32,
    }
    let mut hits: std::collections::HashMap<String, Hit> = std::collections::HashMap::new();

    // 1. Dense candidates via the HNSW batch primitive (over-fetched for re-rank).
    if has_emb {
        let fanout = k.max(1).saturating_mul(DISCOVER_FANOUT).max(k);
        let raw = core
            .semantic_store
            .read()
            .semantic_search(query_embedding, fanout);
        for (id, sim) in raw {
            let kw = if has_kw {
                let (name, desc, ntype) = node_text(core, &id);
                keyword_overlap(&kws, &name, &desc, &ntype)
            } else {
                0.0
            };
            hits.insert(id, Hit { sim, kw });
        }
    } else if has_kw {
        // 2. Embedding-absent fallback: bounded keyword-only scan (documented
        //    degraded path — no HNSW batch primitive is usable without a vector).
        for id in core.node_ids().into_iter().take(DISCOVER_LEX_SCAN_CAP) {
            let (name, desc, ntype) = node_text(core, &id);
            let kw = keyword_overlap(&kws, &name, &desc, &ntype);
            if kw > 0.0 {
                hits.insert(id, Hit { sim: 0.0, kw });
            }
        }
    }

    // Combine, sort, take top-k, hydrate text.
    let mut ranked: Vec<(String, f32)> = hits
        .into_iter()
        .map(|(id, h)| {
            let score = if has_emb && has_kw {
                DISCOVER_SEM_WEIGHT * h.sim.clamp(0.0, 1.0) + DISCOVER_KW_WEIGHT * h.kw
            } else if has_emb {
                h.sim.clamp(0.0, 1.0)
            } else {
                h.kw
            };
            (id, score)
        })
        .collect();
    ranked.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    ranked.truncate(k);

    let results: Vec<serde_json::Value> = ranked
        .into_iter()
        .map(|(id, score)| {
            let (name, description, ntype) = node_text(core, &id);
            serde_json::json!({
                "id": id,
                "name": name,
                "description": description,
                "type": ntype,
                "score": score,
            })
        })
        .collect();

    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::Value::Array(results)),
    )
}

/// `SemanticSearch`: pure extract-method from `try_handle`'s match arm,
/// byte-identical behaviour, no signature change.
async fn handle_semantic_search(
    req_id: u64,
    core: &Arc<GraphCore>,
    query_embedding: Vec<f32>,
    n_results: usize,
) -> Response {
    // CONCEPT:EG-KG.txn.per-graph-write-isolation / Phase C-D — the HNSW index is now maintained
    // incrementally, so the ANN query is O(log n): hold the embedding read
    // lock only for the query itself (no whole-store clone, no per-query
    // index rebuild — at most a one-time lazy rebuild after load). The
    // per-candidate Ebbinghaus decay scoring still runs off-lock below.
    // Fetch more results initially to account for filtered-out nodes.
    let raw_results = {
        core.semantic_store
            .read()
            .semantic_search(&query_embedding, n_results * 2)
    };

    // Bounded candidate-metadata fetch: only the hit ids, not the graph.
    let candidates: Vec<(String, f32, Option<Vec<u8>>)> = {
        let g = &**core;
        raw_results
            .into_iter()
            .map(|(id, sim)| {
                let props = g.get_node_properties(&id);
                (id, sim, props)
            })
            .collect()
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match compute_off_lock(req_id, move || {
        weight_semantic_results(candidates, now, n_results)
    })
    .await
    {
        Ok(weighted_results) => Response::ok(req_id, ResultPayload::raw(&weighted_results)),
        Err(resp) => resp,
    }
}

/// Handle semantic and embedding operations.
pub(super) async fn try_handle_semantic_compute(
    ctx: GraphOpsContext<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    let GraphOpsContext { req_id, core, .. } = ctx;
    ControlFlow::Break(match method {
        Method::AddEmbedding { .. } => unreachable!(
            "AddEmbedding is mutation::GATEWAY_ROUTED; dispatch_graph_op must route it \
                 through try_handle_gateway before it ever reaches this terminal handler"
        ),
        Method::SemanticSearch {
            query_embedding,
            n_results,
        } => handle_semantic_search(req_id, core, query_embedding, n_results).await,
        Method::Discover {
            keywords,
            query_embedding,
            k,
        } => discover(core, &keywords, &query_embedding, k, req_id),
        // CONCEPT:EG-KG.compute.l2-normalize-batch-vectors — kernel-backed in-engine batch L2-normalize (compute-near-data).
        // The `numeric` feature links the pure eg-numeric kernel (faer/ndarray, no Python-extension FFI);
        // a no-numeric build (e.g. `pi`) has no eg-numeric, so the op reports it's absent.
        Method::BatchL2Normalize { vectors } => {
            #[cfg(feature = "numeric")]
            {
                let out = eg_numeric::linalg::batch_l2_normalize(&vectors);
                Response::ok(req_id, ResultPayload::raw(&out))
            }
            #[cfg(not(feature = "numeric"))]
            {
                let _ = vectors;
                Response::err(
                    req_id,
                    "BatchL2Normalize requires the `numeric` feature (eg-numeric kernel)."
                        .to_string(),
                )
            }
        }
        // AddEdge/RemoveEdge (CONCEPT:EG-P0-2 bypass guard): GATEWAY_ROUTED — see
        // the AddNode/RemoveNode comment above for why these are structurally
        // unreachable here, not merely undocumented.
        other => return ControlFlow::Continue(other),
    })
}
