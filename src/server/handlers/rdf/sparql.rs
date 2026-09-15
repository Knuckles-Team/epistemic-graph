use std::sync::Arc;

use crate::graph::{GraphCore, GraphView};
use crate::protocol::{Response, ResultPayload};
use crate::server::compute::compute_off_lock;

/// Evaluate a SPARQL SELECT against the caller-visible graph snapshot.
#[cfg(feature = "sparql")]
pub(super) async fn handle_sparql(
    req_id: u64,
    core: Arc<GraphCore>,
    query: String,
    base_iri: String,
    type_convention: String,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Response {
    // An empty `base_iri` keeps the identity projection; a caller-supplied namespace
    // and convention project the live property graph into that vocabulary.
    let proj = eg_rdf::sparql::Projection::from_wire(&base_iri, &type_convention);
    #[cfg(feature = "result-cache")]
    let cache_key = format!("{query}\u{0}{base_iri}\u{0}{type_convention}");
    #[cfg(feature = "result-cache")]
    let hash = sparql_cache_hash(
        &cache_key,
        #[cfg(feature = "security")]
        caller,
    );
    #[cfg(feature = "result-cache")]
    if let Some(response) = sparql_cache_hit(&core, req_id, hash) {
        return response;
    }
    #[cfg(feature = "result-cache")]
    let (snap, version) = sparql_snapshot(
        &core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    #[cfg(not(feature = "result-cache"))]
    let snap = sparql_snapshot_uncached(
        &core,
        #[cfg(feature = "security")]
        caller,
        #[cfg(feature = "security")]
        rls,
    );
    evaluate_sparql(
        req_id,
        snap,
        query,
        proj,
        #[cfg(feature = "result-cache")]
        core,
        #[cfg(feature = "result-cache")]
        hash,
        #[cfg(feature = "result-cache")]
        version,
    )
    .await
}

#[cfg(feature = "result-cache")]
fn sparql_cache_hash(cache_key: &str, #[cfg(feature = "security")] caller: &str) -> u128 {
    #[cfg(feature = "security")]
    {
        let kind = format!("rls:{caller}:sparql");
        return eg_core::result_cache::ResultCache::hash_query(&kind, cache_key.as_bytes());
    }
    #[cfg(not(feature = "security"))]
    eg_core::result_cache::ResultCache::hash_query("sparql", cache_key.as_bytes())
}

#[cfg(feature = "result-cache")]
fn sparql_cache_hit(core: &GraphCore, req_id: u64, hash: u128) -> Option<Response> {
    core.result_cache().get(hash, core.version()).map(|bytes| {
        Response::ok(
            req_id,
            ResultPayload::of_cache_hit::<eg_types::result_contract::reasoning::Sparql>(bytes),
        )
    })
}

/// Acquire the snapshot used by the result-cache path. The whole-result probe is
/// performed by [`sparql_cache_hit`] before this per-actor filtered-view probe.
#[cfg(feature = "result-cache")]
fn sparql_snapshot(
    core: &Arc<GraphCore>,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> (Arc<GraphView>, u64) {
    #[cfg(feature = "security")]
    {
        let probe_version = core.version();
        return match core.cached_filtered_view(caller, probe_version) {
            Some(cached) => (cached, probe_version),
            None => {
                let generation = core.filtered_view_cache_generation();
                let (mut snap, built_version) = core.analysis_snapshot_versioned();
                rls.filter_view(caller, &mut snap);
                let snap = Arc::new(snap);
                core.put_cached_filtered_view(
                    caller.to_string(),
                    built_version,
                    generation,
                    snap.clone(),
                );
                (snap, built_version)
            }
        };
    }
    #[cfg(not(feature = "security"))]
    core.analysis_snapshot_versioned()
}

/// Acquire a filtered snapshot when the result cache is not compiled.
#[cfg(not(feature = "result-cache"))]
fn sparql_snapshot_uncached(
    core: &Arc<GraphCore>,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Arc<GraphView> {
    #[cfg(feature = "security")]
    {
        let probe_version = core.version();
        return match core.cached_filtered_view(caller, probe_version) {
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
        };
    }
    #[cfg(not(feature = "security"))]
    Arc::new(core.analysis_snapshot())
}

async fn evaluate_sparql(
    req_id: u64,
    snap: Arc<GraphView>,
    query: String,
    proj: eg_rdf::sparql::Projection,
    #[cfg(feature = "result-cache")] core: Arc<GraphCore>,
    #[cfg(feature = "result-cache")] hash: u128,
    #[cfg(feature = "result-cache")] version: u64,
) -> Response {
    match compute_off_lock(req_id, move || {
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
            match ResultPayload::of_ref::<eg_types::result_contract::reasoning::Sparql>(&wire) {
                Ok(payload) => {
                    #[cfg(feature = "result-cache")]
                    eg_core::result_cache::cache_result(
                        core.result_cache(),
                        hash,
                        version,
                        &payload,
                    );
                    Response::ok(req_id, payload)
                }
                Err(error) => Response::err(req_id, error),
            }
        }
        Ok(Err(msg)) => Response::err(req_id, format!("SPARQL error: {msg}")),
        Err(resp) => resp,
    }
}
