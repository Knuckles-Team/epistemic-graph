//! The query-side adapter on the served read paths (EH-396).
//!
//! One owner answers "which adapter applies to this query": the tenant's
//! adapter state for the graph's embedding space, read from the Agent Library
//! control owner on every vector-ranked read (so an activation or rollback is
//! seen by the next query, on every replica that shares the owner), with the
//! immutable, content-addressed body cached by digest.
//!
//! The adapter only ever changes the QUERY vector, and only where the probe is
//! already confined to what the caller may see: `SemanticSearch`/`Discover`
//! run over the caller's row-projected core, and a unified plan's `Rank` ranks
//! inside its RLS-filtered candidate allowlist. It therefore re-orders visible
//! rows and can never admit one. A space with no active adapter -- or an
//! adapter state that cannot be read -- serves the base query unchanged.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use eg_core::graph::GraphCore;
use eg_numeric::decision::adapter::AdapterKernel;
use eg_types::decision::statistical::retrieval_adapter::{AdapterFitted, AdapterState};

use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::decode_artifact;
use crate::server::state::ServerState;

/// Bodies kept decoded; the cache is cleared when it would grow past this.
const MAX_CACHED_BODIES: usize = 32;

/// Key of a fitted adapter (body + receipt).
pub(crate) fn adapter_key(adapter_digest: &str) -> String {
    format!("adapter:{adapter_digest}")
}

/// Key of a space's adapter state.
pub(crate) fn state_key(space_digest: &str) -> String {
    format!("adapter-state:{space_digest}")
}

/// A space's stored state bytes (for a compare-and-set) and the decoded state.
pub(crate) fn read_state(
    store: &AgentLibraryStore,
    tenant: &str,
    space_digest: &str,
) -> Result<(Option<Vec<u8>>, AdapterState), String> {
    let Some(bytes) = store.decision_artifact(tenant, &state_key(space_digest))? else {
        let state = AdapterState {
            space_digest: space_digest.to_string(),
            ..AdapterState::default()
        };
        return Ok((None, state));
    };
    let state = decode_artifact(&bytes, "adapter state")?;
    Ok((Some(bytes), state))
}

/// A fitted adapter stored under `tenant`.
pub(crate) fn read_fitted(
    store: &AgentLibraryStore,
    tenant: &str,
    adapter_digest: &str,
) -> Result<Option<AdapterFitted>, String> {
    store
        .decision_artifact(tenant, &adapter_key(adapter_digest))?
        .map(|bytes| decode_artifact(&bytes, "query adapter"))
        .transpose()
}

/// The active adapter of one space, decoded for serving.
pub(crate) struct ServedAdapter {
    digest: String,
    kernel: AdapterKernel,
}

impl ServedAdapter {
    /// Decode a checked body for serving under its content `digest`.
    pub(crate) fn for_body(
        digest: &str,
        body: &eg_types::decision::statistical::retrieval_adapter::QueryAdapterBody,
    ) -> Result<Self, String> {
        Ok(Self {
            digest: digest.to_string(),
            kernel: AdapterKernel::of(body)?,
        })
    }

    /// The content digest the result cache is salted with.
    pub(crate) fn digest(&self) -> &str {
        &self.digest
    }

    /// The adapted query, or `None` for a query of another width.
    pub(crate) fn adapt(&self, query: &[f32]) -> Option<Vec<f32>> {
        self.kernel.apply(query)
    }
}

fn body_cache() -> &'static parking_lot::Mutex<BTreeMap<String, Arc<ServedAdapter>>> {
    static CACHE: OnceLock<parking_lot::Mutex<BTreeMap<String, Arc<ServedAdapter>>>> =
        OnceLock::new();
    CACHE.get_or_init(|| parking_lot::Mutex::new(BTreeMap::new()))
}

fn cached_or_decode(
    store: &AgentLibraryStore,
    tenant: &str,
    digest: &str,
) -> Result<Option<Arc<ServedAdapter>>, String> {
    if let Some(hit) = body_cache().lock().get(digest) {
        return Ok(Some(Arc::clone(hit)));
    }
    let Some(fitted) = read_fitted(store, tenant, digest)? else {
        return Ok(None);
    };
    let served = Arc::new(ServedAdapter::for_body(digest, &fitted.body)?);
    let mut cache = body_cache().lock();
    if cache.len() >= MAX_CACHED_BODIES {
        cache.clear();
    }
    cache.insert(digest.to_string(), Arc::clone(&served));
    Ok(Some(served))
}

fn load(
    store: &AgentLibraryStore,
    tenant: &str,
    space_digest: &str,
) -> Result<Option<Arc<ServedAdapter>>, String> {
    let (_, state) = read_state(store, tenant, space_digest)?;
    let Some(digest) = state.active.and_then(|event| event.adapter_digest) else {
        return Ok(None);
    };
    cached_or_decode(store, tenant, &digest)
}

/// The adapter serving `core`'s embedding space for the verified carrier
/// tenant `tenant`, if one is active. `None` when there is no carrier, the
/// store declares no space, the Agent Library was never opened, or the state
/// cannot be read (logged; the base query is served).
pub(crate) async fn served_for(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    tenant: Option<&str>,
    core: &GraphCore,
) -> Option<Arc<ServedAdapter>> {
    let tenant = tenant?.to_string();
    let space = core.semantic_store.read().space()?.digest.clone();
    let store = state.read().await.agent_library.clone()?;
    let loaded = tokio::task::spawn_blocking(move || load(&store, &tenant, &space)).await;
    match loaded {
        Ok(Ok(adapter)) => adapter,
        Ok(Err(error)) => {
            tracing::warn!(%error, "query adapter state unreadable; serving the base query");
            None
        }
        Err(error) => {
            tracing::warn!(%error, "query adapter load task failed; serving the base query");
            None
        }
    }
}

/// `query`, adapted when an adapter serves `core`'s space for `tenant`.
pub(crate) async fn adapted_query(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    tenant: Option<&str>,
    core: &GraphCore,
    query: Vec<f32>,
) -> Vec<f32> {
    match served_for(state, tenant, core).await {
        Some(adapter) => adapter.adapt(&query).unwrap_or(query),
        None => query,
    }
}

/// The verified carrier tenant of a read, the partition adapter state is
/// served from (the same scope `DecisionLog.retrieval` activates under).
pub(crate) fn carrier_tenant(
    authority: Option<&crate::server::access::GraphReadAuthority>,
) -> Option<&str> {
    authority
        .and_then(crate::server::access::GraphReadAuthority::carrier)
        .map(crate::server::access::CarrierAuthority::tenant_scope)
}

/// The adapter a unified plan over `graph` runs with: only a plan that ranks
/// by vector looks one up.
#[cfg(feature = "query")]
pub(crate) async fn served_plan_adapter(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    graph: &str,
    authority: Option<&crate::server::access::GraphReadAuthority>,
    plan: &eg_plan::Plan,
) -> Option<Arc<ServedAdapter>> {
    if !plan_ranks_by_vector(&plan.ops) {
        return None;
    }
    let core = Arc::clone(&state.read().await.registry.get(graph)?.core);
    served_for(state, carrier_tenant(authority), &core).await
}

/// `plan` with every vector rank re-aimed by `adapter` (unchanged without one).
#[cfg(feature = "query")]
pub(crate) fn adapt_plan(
    plan: eg_plan::Plan,
    adapter: Option<Arc<ServedAdapter>>,
) -> eg_plan::Plan {
    match adapter {
        Some(adapter) => {
            let embedder = crate::server::handlers::query::uql_text_embedder();
            eg_plan::Plan::new(rewrite_ops(plan.ops, &adapter, embedder))
        }
        None => plan,
    }
}

/// Rewrite every vector rank through `adapter`. A `RankEmbed` is resolved to
/// its vector with the server-side `embedder` first; with none bound (or an
/// embedding error) it is left as it was, so the executor reports exactly what
/// it reported before.
#[cfg(feature = "query")]
fn rewrite_ops(
    ops: Vec<eg_plan::Op>,
    adapter: &ServedAdapter,
    embedder: Option<&dyn eg_plan::TextEmbedder>,
) -> Vec<eg_plan::Op> {
    use eg_plan::Op;
    let adapt = |query: Vec<f32>| Op::Rank {
        query: adapter.adapt(&query).unwrap_or(query),
    };
    ops.into_iter()
        .map(|op| match op {
            Op::Rank { query } => adapt(query),
            Op::RankEmbed { text } => match embedder.map(|e| e.embed(&text)) {
                Some(Ok(query)) => adapt(query),
                Some(Err(_)) | None => Op::RankEmbed { text },
            },
            Op::FuseRrf { branches, k } => Op::FuseRrf {
                branches: branches
                    .into_iter()
                    .map(|branch| rewrite_ops(branch, adapter, embedder))
                    .collect(),
                k,
            },
            other => other,
        })
        .collect()
}

/// Whether a plan ranks by vector anywhere (only then is an adapter looked up).
#[cfg(feature = "query")]
pub(crate) fn plan_ranks_by_vector(ops: &[eg_plan::Op]) -> bool {
    use eg_plan::Op;
    ops.iter().any(|op| match op {
        Op::Rank { .. } | Op::RankEmbed { .. } => true,
        Op::FuseRrf { branches, .. } => branches.iter().any(|b| plan_ranks_by_vector(b)),
        _ => false,
    })
}
