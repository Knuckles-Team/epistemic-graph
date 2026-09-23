use std::sync::Arc;

use crate::graph::{GraphCore, GraphView};
use crate::protocol::{Response, ResultPayload};
use crate::server::compute::compute_off_lock;

/// Whether a SPARQL response carries per-row witness proofs (EH-197).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SparqlProofMode {
    Rows,
    RowsWithProofs,
}

impl SparqlProofMode {
    /// The mode a request's `explain` flag asks for.
    pub(super) fn of(explain: bool) -> Self {
        if explain {
            Self::RowsWithProofs
        } else {
            Self::Rows
        }
    }
}

/// Evaluate a SPARQL SELECT against the caller-visible graph snapshot. `proj` is the
/// request's LPG→RDF projection: an empty `base_iri` keeps the identity projection; a
/// caller-supplied namespace and convention project the live property graph into
/// that vocabulary.
#[cfg(feature = "sparql")]
pub(super) async fn handle_sparql(
    req_id: u64,
    core: Arc<GraphCore>,
    query: String,
    proj: eg_rdf::sparql::Projection,
    mode: SparqlProofMode,
    #[cfg(feature = "security")] caller: &str,
    #[cfg(feature = "security")] rls: &Arc<crate::isolation::IsolationLayer>,
) -> Response {
    #[cfg(feature = "result-cache")]
    let cache_key = format!(
        "{query}\u{0}{:?}\u{0}{}\u{0}{mode:?}",
        proj.base_iri, proj.camel_type
    );
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
        (query, proj, mode),
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
        eg_core::result_cache::ResultCache::hash_query(&kind, cache_key.as_bytes())
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
        match core.cached_filtered_view(caller, probe_version) {
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
        }
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
    (query, proj, mode): (String, eg_rdf::sparql::Projection, SparqlProofMode),
    #[cfg(feature = "result-cache")] core: Arc<GraphCore>,
    #[cfg(feature = "result-cache")] hash: u128,
    #[cfg(feature = "result-cache")] version: u64,
) -> Response {
    match compute_off_lock(req_id, move || {
        run_sparql(
            &eg_rdf::sparql::Dataset::new(&snap, Vec::new()),
            &query,
            &proj,
            mode,
        )
    })
    .await
    {
        Ok(Ok(wire)) => {
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

/// Evaluate `query` and project it to the wire table, with a witness proof per row
/// when `mode` asks for one.
fn run_sparql(
    ds: &eg_rdf::sparql::Dataset,
    query: &str,
    proj: &eg_rdf::sparql::Projection,
    mode: SparqlProofMode,
) -> Result<crate::protocol::SparqlResult, String> {
    let (table, proofs) = match mode {
        SparqlProofMode::Rows => (
            eg_rdf::sparql::execute(ds, query, proj, None)?.into_table(),
            Vec::new(),
        ),
        SparqlProofMode::RowsWithProofs => eg_rdf::sparql::execute_explained(ds, query, proj)?,
    };
    let (vars, rows) = table.to_rows();
    Ok(crate::protocol::SparqlResult { vars, rows, proofs })
}
