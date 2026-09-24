//! Standing impact watches (EH-526): event-driven probabilistic impact propagation.
//!
//! **Opt-in, never default-on.** Only graphs named in
//! [`IMPACT_ON_WRITE_ENV`] are watched; with it unset nothing is installed on the
//! CDC hub and no task is spawned, so the write path is unchanged.
//!
//! 1. **Declare** — a watch is an `:ImpactWatch` node in the graph it watches
//!    ([`plan::ImpactWatchSpec`]): the seed labels (an incident, a conformance
//!    violation, a CVE finding, an outage …), the dependency relationship impact
//!    flows along, the model and the hop bound. No wire method registers it.
//! 2. **Notice** — [`crate::server::cdc::CdcHub::emit`] calls [`ImpactWatchHub::note`]
//!    for every committed change: one hashset lookup for an unwatched graph; for a
//!    watched one it records the changed node when its label is a seed label (or a
//!    watch declaration).
//! 3. **Recompute** — a debounced sweep plans each affected watch over the graph's
//!    current snapshot ([`plan::plan_watch`]: only the changed seeds' downstream
//!    cone) and submits the resulting `MineRiskPropagation` writeback through the
//!    ordinary dispatch path as the fixed service principal
//!    `service:impact-watch` — the same authorization, durability, WAL replay and
//!    audit as any caller's write, never a bypass. Its assessments are
//!    `:ImpactAssessment` nodes scoped by the watch id and stamped with the as-of
//!    time and the run's provenance digest.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

#[cfg(feature = "security")]
mod grant;
pub mod plan;
#[cfg(test)]
mod tests;

pub use plan::{load_watches, plan_watch, ImpactWatchSpec, WATCH_LABEL};

/// Opt-in graph allowlist (comma-separated exact names). Unset ⇒ disabled.
pub const IMPACT_ON_WRITE_ENV: &str = "EPISTEMIC_GRAPH_IMPACT_ON_WRITE";
/// Debounce override, milliseconds.
pub const IMPACT_DEBOUNCE_ENV: &str = "EPISTEMIC_GRAPH_IMPACT_ON_WRITE_DEBOUNCE_MS";
/// The service the watch writes as (principal `service:impact-watch`).
pub(crate) const WATCH_SERVICE: &str = "impact-watch";

const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(500);
const SWEEP_TICK: Duration = Duration::from_millis(50);

/// What changed on one watched graph since its last sweep.
#[derive(Default)]
struct Pending {
    changed: BTreeSet<String>,
    watches_changed: bool,
    last_note: Option<Instant>,
}

/// The installed watch hub: config plus per-graph pending changes.
pub struct ImpactWatchHub {
    watched: HashSet<String>,
    debounce: Duration,
    pending: DashMap<String, Pending>,
    /// Seed labels of each graph's watches, loaded at its first sweep; until
    /// then every labelled change is recorded.
    seed_labels: DashMap<String, BTreeSet<String>>,
}

impl ImpactWatchHub {
    /// A hub over `watched` graphs.
    pub fn new(watched: HashSet<String>, debounce: Duration) -> Self {
        Self {
            watched,
            debounce,
            pending: DashMap::new(),
            seed_labels: DashMap::new(),
        }
    }

    /// Read the allowlist and debounce once, at startup.
    pub fn from_env() -> Self {
        let raw = std::env::var(IMPACT_ON_WRITE_ENV).unwrap_or_default();
        let watched = raw
            .split(',')
            .map(str::trim)
            .filter(|g| !g.is_empty())
            .map(str::to_string)
            .collect();
        let debounce = std::env::var(IMPACT_DEBOUNCE_ENV)
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|ms| *ms > 0)
            .map_or(DEFAULT_DEBOUNCE, Duration::from_millis);
        Self::new(watched, debounce)
    }

    /// Whether any graph is watched.
    pub fn is_active(&self) -> bool {
        !self.watched.is_empty()
    }

    /// Whether `graph` is watched.
    pub fn watches(&self, graph: &str) -> bool {
        self.watched.contains(graph)
    }

    /// The CDC hook. THE COST GATE: one hashset lookup for an unwatched graph.
    pub fn note(&self, graph: &str, label: &str, node_id: &str) {
        if !self.watched.contains(graph) || label.is_empty() {
            return;
        }
        let is_watch = label == WATCH_LABEL;
        let relevant = is_watch
            || self
                .seed_labels
                .get(graph)
                .is_none_or(|labels| labels.contains(label));
        if !relevant {
            return;
        }
        let mut entry = self.pending.entry(graph.to_string()).or_default();
        entry.last_note = Some(Instant::now());
        if is_watch {
            entry.watches_changed = true;
        } else {
            entry.changed.insert(node_id.to_string());
        }
    }

    /// Whether `graph` has nothing pending (a structural cost-gate proof for tests).
    pub fn has_no_state(&self, graph: &str) -> bool {
        !self.pending.contains_key(graph)
    }

    /// Take every graph whose changes have been quiet for the debounce window.
    fn take_due(&self, now: Instant) -> Vec<(String, Pending)> {
        let due: Vec<String> = self
            .pending
            .iter()
            .filter(|kv| {
                kv.last_note
                    .is_some_and(|t| now.duration_since(t) >= self.debounce)
            })
            .map(|kv| kv.key().clone())
            .collect();
        due.into_iter()
            .filter_map(|graph| self.pending.remove(&graph))
            .collect()
    }

    /// Recompute every due graph's watches. Returns the watch runs submitted.
    pub async fn sweep_due(&self, state: &Arc<RwLock<ServerState>>) -> usize {
        let mut runs = 0;
        for (graph, pending) in self.take_due(Instant::now()) {
            runs += self.run_graph(state, &graph, &pending).await;
        }
        runs
    }

    /// Plan and submit each watch of `graph` that the pending changes touch.
    async fn run_graph(
        &self,
        state: &Arc<RwLock<ServerState>>,
        graph: &str,
        pending: &Pending,
    ) -> usize {
        let core = {
            let guard = state.read().await;
            match guard.registry.get(graph) {
                Some(entry) => entry.core.clone(),
                None => return 0,
            }
        };
        let view = core.analysis_snapshot();
        let watches = load_watches(&view);
        let labels: BTreeSet<String> = watches
            .iter()
            .flat_map(|w| w.seed_labels.iter().cloned())
            .collect();
        self.seed_labels.insert(graph.to_string(), labels);
        let as_of_ms = crate::server::dispatch::authoritative_now_ms();
        let mut runs = 0;
        for watch in &watches {
            let changed = watch_changes(&view, watch, pending);
            match plan_watch(&view, watch, &changed, as_of_ms) {
                Ok(Some(method)) => runs += submit(state, graph, watch, method, as_of_ms).await,
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(graph, watch = %watch.id, %error, "impact watch plan failed")
                }
            }
        }
        runs
    }
}

/// The changed seeds of `watch`: every seed candidate when a declaration changed
/// (a new or edited watch recomputes in full), else the changed seed-labelled nodes.
fn watch_changes(
    view: &eg_core::graph::GraphView,
    watch: &ImpactWatchSpec,
    pending: &Pending,
) -> Vec<String> {
    if pending.watches_changed {
        return plan::seed_candidates(view, watch);
    }
    pending
        .changed
        .iter()
        .filter(|id| watch.watches_label(plan::label_of(&plan::node_props(view, id))))
        .cloned()
        .collect()
}

/// Submit one planned writeback as the watch service principal. Returns 1 when it
/// committed, 0 (logged) when it was refused.
async fn submit(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    watch: &ImpactWatchSpec,
    method: crate::protocol::Method,
    as_of_ms: u64,
) -> usize {
    #[cfg(feature = "security")]
    if let Err(error) = grant::ensure(&mut state.write().await.isolation, graph) {
        tracing::warn!(graph, %error, "impact watch cannot be granted its graph");
        return 0;
    }
    let context = match service_context(&watch.id, as_of_ms) {
        Ok(context) => context,
        Err(error) => {
            tracing::warn!(graph, %error, "impact watch service identity unavailable");
            return 0;
        }
    };
    let request = crate::protocol::Request {
        id: 0,
        graph: graph.to_string(),
        auth_token: String::new(),
        agent_id: Some(context.agent_id().to_string()),
        method,
    };
    let response =
        crate::server::dispatch::dispatch_verified_request(state, request, context).await;
    match response.error {
        None => 1,
        Some(error) => {
            tracing::warn!(graph, watch = %watch.id, %error, "impact watch writeback refused");
            0
        }
    }
}

/// The service identity one watch run writes as; its operation key names the watch
/// and the as-of time, so a retried run replays instead of writing twice.
pub(crate) fn service_context(
    watch_id: &str,
    as_of_ms: u64,
) -> Result<VerifiedRequestContext, String> {
    let service = VerifiedRequestContext::authenticated_fixed_service_actor(
        WATCH_SERVICE,
        &["mining:write"],
    )?;
    Ok(VerifiedRequestContext::from_verified_claims_with_nonce(
        service.claims().clone(),
        format!("{WATCH_SERVICE}:{watch_id}:{as_of_ms}"),
        Some(eg_types::contract::Nonce::minted()),
    ))
}

/// Start the periodic sweep (only when the hub is active).
pub fn spawn(state: Arc<RwLock<ServerState>>, hub: Arc<ImpactWatchHub>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(SWEEP_TICK);
        loop {
            tick.tick().await;
            hub.sweep_due(&state).await;
        }
    });
}
