//! Per-tenant memory budget + autoscale signals (CONCEPT:EG-KG.compute.lane-v, Lane V —
//! cost/efficiency).
//!
//! The engine already bounds memory per *graph* (`EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH`
//! LRU eviction, KG-2.191) and can drop a cold *graph* entirely (hibernation,
//! KG-2.224). This module adds the *tenant* dimension and a memory-byte budget on top:
//!
//! * **Per-tenant memory budget.** A tenant owns one or more graphs (the tenant is
//!   derived from the graph name — the prefix before the first `:`, e.g. `agent` for
//!   `agent:planner`, or the whole name when there is no `:`). The enforcer tracks an
//!   approximate resident-RAM estimate per graph ([`GraphCore::memory_estimate`]) and
//!   sums it per tenant. When a tenant exceeds its byte budget, its COLDEST graphs
//!   (least node activity) are reclaimed — first by durability-gated LRU eviction
//!   (reusing [`crate::persist::evict_oversized_all`]'s per-graph path), then, if a
//!   graph is fully reducible, by hibernation ([`GraphCore::hibernate`]) — until the
//!   tenant is back under budget. Data is never lost: eviction is durability-gated and
//!   hibernation drops only RAM (the durable redb tier + the read-through seam stay
//!   intact, so an evicted/hibernated node rehydrates on access).
//!
//! * **Global ceiling + fair caps.** A process-wide memory ceiling caps total resident
//!   RAM; a fair per-tenant cap (`ceiling / max(active_tenants, 1)`) is the floor under
//!   each tenant's budget, so one hot tenant cannot consume the whole ceiling and
//!   starve others. A tenant's effective budget is `min(its configured budget, the
//!   fair share)` once the global ceiling is under pressure.
//!
//! * **Autoscale signals.** [`collect_resource_stats`] gathers a [`ResourceSnapshot`]
//!   (per-graph + per-tenant + process aggregate) for the `ResourceStats` Method and the
//!   Prometheus metrics endpoint — the signals an external autoscaler (agent-utilities
//!   OS-5.27) consumes.
//!
//! Pure-Rust: the enforcer is a periodic sweep over the registry reusing existing
//! evict/hibernate ops — no DataFusion / openraft / object_store. Pi-safe.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::graph::GraphCore;
use crate::server::ServerState;

/// Derive the tenant a graph belongs to (CONCEPT:EG-KG.compute.lane-v). The convention across the
/// ecosystem is `tenant:scope` graph names (`agent:planner`, `team:research`,
/// `enterprise-acme:billing`); the tenant is the prefix before the FIRST `:`. A name
/// with no `:` (e.g. `__commons__`) is its own tenant. This is the one place the
/// mapping lives, so a future explicit tenant catalog only changes here.
pub fn tenant_of(graph_name: &str) -> &str {
    match graph_name.split_once(':') {
        Some((tenant, _)) => tenant,
        None => graph_name,
    }
}

/// Process-global cost bookkeeping (CONCEPT:EG-KG.compute.lane-v): which graphs the enforcer has
/// hibernated (so the snapshot reports resident-vs-hibernated accurately even though a
/// hibernated graph just looks like an empty one), and a monotonic eviction counter for
/// the rate signal. A singleton (like `metrics::SEEN_GRAPHS`) so it needs no ServerState
/// field / constructor churn — hibernation status + eviction totals are process-global.
#[derive(Default)]
struct CostState {
    /// Graphs the budget enforcer has hibernated (drives the resident/hibernated split).
    hibernated: Mutex<std::collections::HashSet<String>>,
    /// Total LRU nodes evicted by the budget enforcer (the rate is its delta over time).
    evicted_total: std::sync::atomic::AtomicU64,
    /// Total graphs hibernated by the budget enforcer.
    hibernated_total: std::sync::atomic::AtomicU64,
}

fn cost_state() -> &'static CostState {
    use std::sync::OnceLock;
    static STATE: OnceLock<CostState> = OnceLock::new();
    STATE.get_or_init(CostState::default)
}

/// Mark a graph hibernated / resident in the process-global bookkeeping.
fn note_hibernated(graph: &str, hibernated: bool) {
    let mut set = cost_state().hibernated.lock();
    if hibernated {
        set.insert(graph.to_string());
        cost_state()
            .hibernated_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    } else {
        set.remove(graph);
    }
    crate::metrics::set_graph_hibernated(graph, hibernated);
}

/// Is this graph currently hibernated by the enforcer?
fn is_hibernated(graph: &str) -> bool {
    cost_state().hibernated.lock().contains(graph)
}

// ── Configuration ─────────────────────────────────────────────────────────

/// Per-tenant memory budget configuration (CONCEPT:EG-KG.compute.lane-v), read once at startup.
#[derive(Debug, Clone, Copy)]
pub struct CostConfig {
    /// Process-wide resident-memory ceiling, bytes.
    pub global_ceiling_bytes: u64,
    /// Default per-tenant budget, bytes. A tenant may never exceed this; under global
    /// pressure its effective budget is further capped to the fair share.
    pub per_tenant_budget_bytes: u64,
    /// Sweep interval, seconds.
    pub interval_secs: u64,
}

/// Effective memory available to this process. The shared `eg-resource` resolver
/// already intersects host RAM with cgroup v1/v2 and reserves RAM headroom, so
/// this module must not maintain a second parser.
fn system_memory_limit_bytes() -> Option<u64> {
    Some(crate::autosize::detect_capacity().reserved_ram_bytes())
}

impl CostConfig {
    /// Resolve from the environment (CONCEPT:EG-KG.compute.lane-v). ONE knob is enough to turn it on
    /// with a sensible auto-sized default:
    ///
    /// * `EPISTEMIC_GRAPH_MEMORY_BUDGET` — the global resident-memory ceiling. Accepts a
    ///   plain byte count OR a `k`/`m`/`g` suffix (`512m`, `2g`). When UNSET it AUTO-SIZES
    ///   to 40% of the cgroup-aware effective RAM. Explicit values may lower the
    ///   automatic ceiling but are clamped to it, so an override cannot widen a
    ///   constrained process beyond its measured budget.
    /// * `EPISTEMIC_GRAPH_TENANT_BUDGET` — optional per-tenant budget override. Defaults
    ///   to the global ceiling (so a single-tenant box budgets against the ceiling); the
    ///   fair-share cap still applies under multi-tenant pressure regardless.
    /// * `EPISTEMIC_GRAPH_BUDGET_INTERVAL` — sweep cadence, seconds (default 15).
    pub fn from_env() -> Result<Self, String> {
        let automatic_ceiling = system_memory_limit_bytes()
            .map(|total| (total / 10 * 4).max(1))
            .unwrap_or(512 * 1024 * 1024 / 10 * 4);
        let global_ceiling_bytes = match std::env::var("EPISTEMIC_GRAPH_MEMORY_BUDGET") {
            Ok(value) => {
                let requested = parse_bytes(&value)
                    .filter(|parsed| *parsed > 0)
                    .ok_or_else(|| "EPISTEMIC_GRAPH_MEMORY_BUDGET must be positive".to_string())?;
                let bounded = requested.min(automatic_ceiling);
                if bounded != requested {
                    tracing::warn!(
                        requested,
                        bounded,
                        automatic = automatic_ceiling,
                        "EPISTEMIC_GRAPH_MEMORY_BUDGET exceeds cgroup-aware automatic capacity; clamping"
                    );
                }
                bounded
            }
            // A shared engine leaves most memory available to its host process,
            // filesystem cache, and peer services. Operators of a dedicated node
            // can explicitly raise this value.
            Err(std::env::VarError::NotPresent) => automatic_ceiling,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err("EPISTEMIC_GRAPH_MEMORY_BUDGET is not valid Unicode".to_string())
            }
        };
        let per_tenant_budget_bytes = match std::env::var("EPISTEMIC_GRAPH_TENANT_BUDGET") {
            Ok(value) => {
                let requested = parse_bytes(&value)
                    .filter(|budget| *budget > 0)
                    .ok_or_else(|| "EPISTEMIC_GRAPH_TENANT_BUDGET must be positive".to_string())?;
                let bounded = requested.min(global_ceiling_bytes);
                if bounded != requested {
                    tracing::warn!(
                        requested,
                        bounded,
                        automatic = global_ceiling_bytes,
                        "EPISTEMIC_GRAPH_TENANT_BUDGET exceeds the global cgroup-aware capacity; clamping"
                    );
                }
                bounded
            }
            Err(std::env::VarError::NotPresent) => global_ceiling_bytes,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err("EPISTEMIC_GRAPH_TENANT_BUDGET is not valid Unicode".to_string())
            }
        };
        let interval_secs = match std::env::var("EPISTEMIC_GRAPH_BUDGET_INTERVAL") {
            Ok(value) => value
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|interval| (1..=3_600).contains(interval))
                .ok_or_else(|| {
                    "EPISTEMIC_GRAPH_BUDGET_INTERVAL must be between 1 and 3600".to_string()
                })?,
            Err(std::env::VarError::NotPresent) => 15,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err("EPISTEMIC_GRAPH_BUDGET_INTERVAL is not valid Unicode".to_string())
            }
        };
        Ok(CostConfig {
            global_ceiling_bytes,
            per_tenant_budget_bytes,
            interval_secs,
        })
    }
}

/// Parse a byte count with an optional `k`/`m`/`g` (1024-based) suffix.
fn parse_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, mult) = match s.chars().last().unwrap().to_ascii_lowercase() {
        'k' => (&s[..s.len() - 1], 1024),
        'm' => (&s[..s.len() - 1], 1024 * 1024),
        'g' => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    num.trim()
        .parse::<u64>()
        .ok()
        .and_then(|value| value.checked_mul(mult))
}

// ── Resource snapshot (autoscale signals) ───────────────────────────────────

/// Per-graph resource snapshot (CONCEPT:EG-KG.compute.lane-v).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GraphResourceStats {
    pub graph: String,
    pub tenant: String,
    pub nodes: u64,
    pub edges: u64,
    /// Approximate resident RAM, bytes ([`GraphCore::memory_estimate`]).
    pub memory_bytes: u64,
    /// `true` if hibernated (in-RAM state dropped, durable in redb).
    pub hibernated: bool,
}

/// Per-tenant rollup (CONCEPT:EG-KG.compute.lane-v).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TenantResourceStats {
    pub tenant: String,
    pub graphs: u64,
    pub resident_graphs: u64,
    pub hibernated_graphs: u64,
    pub nodes: u64,
    pub edges: u64,
    pub memory_bytes: u64,
    /// The tenant's configured byte budget (before the fair-share cap).
    pub budget_bytes: u64,
    /// `true` when `memory_bytes > budget_bytes` (the enforcer is reclaiming).
    pub over_budget: bool,
}

/// The full resource snapshot returned by `Method::ResourceStats` (CONCEPT:EG-KG.compute.lane-v) and
/// scraped into Prometheus. The signals an autoscaler (OS-5.27) needs in ONE round-trip.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourceSnapshot {
    /// Total resident RAM across all graphs (sum of `memory_bytes`).
    pub total_memory_bytes: u64,
    /// Current process RSS (falling back to peak RSS), bytes — the OS-observed
    /// footprint used for calibration and hard-ceiling pressure.
    pub process_rss_bytes: u64,
    /// Configured global ceiling. The cgroup-aware automatic policy is always
    /// positive; an explicit override can lower it but cannot disable it.
    pub global_ceiling_bytes: u64,
    pub total_nodes: u64,
    pub total_edges: u64,
    pub graph_count: u64,
    pub tenant_count: u64,
    pub resident_graphs: u64,
    pub hibernated_graphs: u64,
    /// Requests currently holding an admission permit (in-flight depth).
    pub in_flight: u64,
    /// Admission permits still available (`max_inflight - in_flight`); a small value
    /// means the queue is saturated (shedding BUSY).
    pub inflight_permits_available: u64,
    /// Cumulative LRU nodes evicted by the budget enforcer (rate = delta/interval).
    pub budget_evictions_total: u64,
    /// Cumulative graphs hibernated by the budget enforcer.
    pub budget_hibernations_total: u64,
    pub tenants: Vec<TenantResourceStats>,
    pub graphs: Vec<GraphResourceStats>,
}

/// Current process RSS in bytes, falling back to peak RSS when a platform omits
/// `VmRSS`. Returns `0` when `/proc` is unavailable.
fn process_rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for key in ["VmRSS:", "VmHWM:"] {
        for line in s.lines() {
            let Some(rest) = line.strip_prefix(key) else {
                continue;
            };
            if let Some(kb) = rest
                .split_whitespace()
                .next()
                .and_then(|n| n.parse::<u64>().ok())
            {
                return kb * 1024;
            }
        }
    }
    0
}

/// Gather a [`ResourceSnapshot`] across the registry (CONCEPT:EG-KG.compute.lane-v). Reads node/edge
/// counts + the memory estimate per graph, rolls them up per tenant, and pulls the live
/// in-flight admission state — the structured signal an autoscaler scales on. Off the hot
/// path (a `ResourceStats` request / the metrics scrape).
pub async fn collect_resource_stats(
    state: &Arc<RwLock<ServerState>>,
) -> Result<ResourceSnapshot, String> {
    let config = CostConfig::from_env()?;
    let (entries, configured_max_inflight, permits_available) = {
        let s = state.read().await;
        let entries: Vec<(String, Arc<GraphCore>)> = s
            .registry
            .all_entries()
            .iter()
            .map(|e| (e.name.clone(), e.core.clone()))
            .collect();
        let permits = s.max_in_flight.available_permits();
        // The global admission semaphore was constructed with the configured max; recover
        // it from the env so `in_flight = max - available` is exact. The UNSET default
        // now auto-sizes from effective cgroup-aware capacity
        // (CONCEPT:AU-KG.backend.b-auto-size) — the SAME derivation
        // `main.rs` used to build the semaphore, so the gauge stays exact on a Pi and a
        // big box alike (previously hard-coded 1024).
        let automatic_max = crate::autosize::detect_capacity().max_inflight();
        let max = std::env::var("EPISTEMIC_GRAPH_MAX_INFLIGHT")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| n > 0)
            .map(|value| crate::autosize::bound_explicit(value, automatic_max))
            .unwrap_or(automatic_max);
        (entries, max, permits)
    };
    let in_flight = configured_max_inflight.saturating_sub(permits_available) as u64;

    let mut graphs = Vec::with_capacity(entries.len());
    let mut tenant_rollup: HashMap<String, TenantResourceStats> = HashMap::new();
    let mut total_memory_bytes = 0u64;
    let mut total_nodes = 0u64;
    let mut total_edges = 0u64;
    let mut resident_graphs = 0u64;
    let mut hibernated_graphs = 0u64;

    for (name, core) in &entries {
        let tenant = tenant_of(name).to_string();
        let nodes = core.node_count() as u64;
        let edges = core.edge_count() as u64;
        let mem = core.memory_estimate();
        let hibernated = is_hibernated(name);

        total_memory_bytes += mem;
        total_nodes += nodes;
        total_edges += edges;
        if hibernated {
            hibernated_graphs += 1;
        } else {
            resident_graphs += 1;
        }

        // Keep the per-graph metric gauges fresh on every snapshot.
        crate::metrics::set_graph_memory(name, mem as i64);

        let t = tenant_rollup
            .entry(tenant.clone())
            .or_insert_with(|| TenantResourceStats {
                tenant: tenant.clone(),
                graphs: 0,
                resident_graphs: 0,
                hibernated_graphs: 0,
                nodes: 0,
                edges: 0,
                memory_bytes: 0,
                budget_bytes: config.per_tenant_budget_bytes,
                over_budget: false,
            });
        t.graphs += 1;
        if hibernated {
            t.hibernated_graphs += 1;
        } else {
            t.resident_graphs += 1;
        }
        t.nodes += nodes;
        t.edges += edges;
        t.memory_bytes += mem;

        graphs.push(GraphResourceStats {
            graph: name.clone(),
            tenant,
            nodes,
            edges,
            memory_bytes: mem,
            hibernated,
        });
    }

    for t in tenant_rollup.values_mut() {
        t.over_budget = t.memory_bytes > config.per_tenant_budget_bytes;
    }

    let mut tenants: Vec<TenantResourceStats> = tenant_rollup.into_values().collect();
    tenants.sort_by_key(|t| std::cmp::Reverse(t.memory_bytes));
    graphs.sort_by_key(|g| std::cmp::Reverse(g.memory_bytes));

    let st = cost_state();
    Ok(ResourceSnapshot {
        total_memory_bytes,
        process_rss_bytes: process_rss_bytes(),
        global_ceiling_bytes: config.global_ceiling_bytes,
        total_nodes,
        total_edges,
        graph_count: entries.len() as u64,
        tenant_count: tenants.len() as u64,
        resident_graphs,
        hibernated_graphs,
        in_flight,
        inflight_permits_available: permits_available as u64,
        budget_evictions_total: st.evicted_total.load(std::sync::atomic::Ordering::Relaxed),
        budget_hibernations_total: st
            .hibernated_total
            .load(std::sync::atomic::Ordering::Relaxed),
        tenants,
        graphs,
    })
}

// ── Budget enforcement ──────────────────────────────────────────────────────

/// Enforce the per-tenant memory budgets ONCE (CONCEPT:EG-KG.compute.lane-v). For every tenant over
/// its effective budget (the smaller of its configured budget and its fair share of the
/// global ceiling), reclaim memory from its COLDEST graphs until it is back under budget:
///
/// 1. **Evict** the graph's LRU nodes (durability-gated, reusing the per-graph eviction
///    path) down toward an empty resident set.
/// 2. If the graph is still resident and the tenant is still over budget, **hibernate**
///    the graph (drop all its in-RAM state; durable in redb, read-through serves reads).
///
/// "Coldest" = fewest nodes touched (the graph with the smallest node count is reclaimed
/// first, preserving the hot working set). Returns `(nodes_evicted, graphs_hibernated)`.
/// Pure-Rust; reuses the existing durability-gated evict + hibernate ops, so it never
/// loses data. No-op when budgeting is disabled.
pub async fn enforce_memory_budgets(
    state: &Arc<RwLock<ServerState>>,
    config: CostConfig,
) -> (u64, u64) {
    // Snapshot per-graph footprint + durable authority.
    let (entries, backend) = {
        let s = state.read().await;
        let entries: Vec<(String, Arc<GraphCore>)> = s
            .registry
            .all_entries()
            .iter()
            .map(|e| (e.name.clone(), e.core.clone()))
            .collect();
        (entries, s.persistence.clone())
    };

    // Roll up resident memory per tenant + count active tenants for the fair share.
    let mut per_tenant: HashMap<String, Vec<(String, Arc<GraphCore>, u64)>> = HashMap::new();
    let mut tenant_mem: HashMap<String, u64> = HashMap::new();
    for (name, core) in entries {
        let mem = core.memory_estimate();
        let tenant = tenant_of(&name).to_string();
        *tenant_mem.entry(tenant.clone()).or_default() += mem;
        per_tenant
            .entry(tenant)
            .or_default()
            .push((name, core, mem));
    }

    // Fair per-tenant cap: the global ceiling split evenly across active tenants. A
    // tenant's EFFECTIVE budget is the smaller of its configured budget and this share,
    // so one hot tenant cannot consume the whole ceiling and starve others.
    let active_tenants = per_tenant.len().max(1) as u64;
    // `process_rss_bytes()` (real OS-observed RSS) used to scale this target: it is
    // NOT commensurable with the tenants' modeled `memory_estimate()` sum. A
    // process's RSS also carries the binary image, allocator arenas, thread
    // stacks, and every other subsystem's heap — overhead entirely unrelated to
    // graph data, and in any modest process (very much including a `cargo test`
    // binary) far larger than a lightly-loaded tenant's modeled footprint.
    // Scaling the modeled total by the raw `global_ceiling / rss` ratio crushed
    // `pressure_target` toward zero whenever that fixed overhead dominated RSS —
    // reclaiming a tenant already comfortably under budget by any measure of its
    // OWN data (the `fair_cap_protects_small_tenant` regression this fixes: an
    // 8KiB ceiling against a multi-hundred-MB test-process RSS zeroed the fair
    // share and evicted a tenant holding a single tiny node). The fair share is
    // therefore the plain, documented `ceiling / active_tenants` split — sound
    // regardless of how much of real RSS the modeled graph data happens to be.
    let fair_share = config.global_ceiling_bytes / active_tenants;
    let effective_budget = config.per_tenant_budget_bytes.min(fair_share.max(1));

    let mut total_evicted = 0u64;
    let mut total_hibernated = 0u64;

    for (tenant, mut graphs) in per_tenant {
        let mut tenant_resident = *tenant_mem.get(&tenant).unwrap_or(&0);
        if tenant_resident <= effective_budget {
            continue;
        }
        // Reclaim coldest-first (fewest nodes ⇒ least active).
        graphs.sort_by_key(|(_, core, _)| core.node_count());

        for (name, core, mem) in graphs {
            if tenant_resident <= effective_budget {
                break;
            }
            // The `__commons__` shared graph is never reclaimed for a budget — it is not
            // a tenant's private working set and is needed by every agent.
            if name == "__commons__" {
                continue;
            }

            // Step 1: durability-gated LRU eviction down to empty (max_nodes = 0 evicts
            // every durable node; a node whose durability can't be confirmed stays).
            let evicted = evict_graph_to(&name, &core, 0, &backend).await;
            if evicted > 0 {
                total_evicted += evicted as u64;
                cost_state()
                    .evicted_total
                    .fetch_add(evicted as u64, std::sync::atomic::Ordering::Relaxed);
                crate::metrics::budget_evicted(evicted as u64);
            }

            // Step 2: if still resident and the tenant is still over budget, hibernate
            // only after confirming every remaining node in durable authority.
            let still_resident = core.node_count() > 0;
            if still_resident && tenant_resident.saturating_sub(mem) < effective_budget {
                let freed = hibernate_graph_if_durable(&name, &core, &backend);
                if freed > 0 {
                    total_hibernated += 1;
                    note_hibernated(&name, true);
                    crate::metrics::budget_hibernated();
                }
            }

            // U-148 / BUG-130: Steps 1-2 above only ever drop this graph's RAM
            // (`GraphCore::evict_resident_nodes`/`hibernate`) — they never touch the
            // REGISTRY's residency bookkeeping. When they leave the durability-confirmed
            // core fully empty, the registry still reports `is_resident(name) == true`
            // with zero topology: the dispatch lazy-open path (`cold_offload::lazy_open`)
            // short-circuits on that residency check and never rehydrates it, so every
            // whole-graph read (Cypher, counts, traversal) observes a permanently empty
            // snapshot while a point `get_node_properties` still finds data through the
            // separate read-through seam. Mirror `admit_capacity`'s already-correct
            // pattern: once fully durable-confirmed-empty, transition the registry entry
            // to catalog-only so the NEXT access takes the existing bounded durable
            // lazy-open instead of silently serving the stale empty resident image.
            if core.node_count() == 0 {
                let mut s = state.write().await;
                s.registry.evict_resident(&name);
            }

            // Recompute this graph's residual footprint and update the tenant total.
            let new_mem = core.memory_estimate();
            tenant_resident = tenant_resident.saturating_sub(mem).saturating_add(new_mem);

            // A graph that came back resident (rehydrated by access between sweeps) is no
            // longer hibernated.
            if core.node_count() > 0 && is_hibernated(&name) {
                note_hibernated(&name, false);
            }
        }
    }

    (total_evicted, total_hibernated)
}

/// Evict one graph's LRU nodes down to `max_nodes` only after durable presence is
/// confirmed (CONCEPT:EG-KG.storage.read-through-seam-exercised).
async fn evict_graph_to(
    fname_graph: &str,
    core: &Arc<GraphCore>,
    max_nodes: usize,
    backend: &Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
) -> usize {
    let backend = match backend {
        Some(b) => b,
        None => return 0, // nothing durable to confirm against ⇒ never drop
    };
    let candidates = core.lru_eviction_candidates(max_nodes);
    if candidates.is_empty() {
        return 0;
    }
    let fname = crate::persist::sanitize(fname_graph);
    let presence = match backend.durable_node_presence(&fname, &candidates) {
        Ok(value) if value.len() == candidates.len() => value,
        Ok(_) | Err(_) => return 0,
    };
    let durable: Vec<String> = candidates
        .into_iter()
        .zip(presence)
        .filter_map(|(node_id, present)| present.then_some(node_id))
        .collect();
    core.evict_resident_nodes(&durable)
}

fn hibernate_graph_if_durable(
    graph_name: &str,
    core: &GraphCore,
    backend: &Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
) -> usize {
    let Some(backend) = backend else {
        return 0;
    };
    let node_ids: Vec<String> = core
        .get_nodes()
        .into_iter()
        .map(|(node_id, _)| node_id)
        .collect();
    if node_ids.is_empty() {
        return 0;
    }
    let fname = crate::persist::sanitize(graph_name);
    match backend.durable_node_presence(&fname, &node_ids) {
        Ok(presence)
            if presence.len() == node_ids.len() && presence.iter().all(|present| *present) =>
        {
            core.hibernate()
        }
        Ok(_) | Err(_) => 0,
    }
}

// ── Cost model / capacity planning (CONCEPT:EG-KG.compute.lane-v) ───────────────────────

/// A capacity estimate for a target tenant count + average working set
/// (CONCEPT:EG-KG.compute.lane-v). Maps a workload onto a resource footprint so an operator can pick
/// a deployment tier (Pi → cluster) and size the shard count. See `docs/cost-model.md`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct CapacityEstimate {
    pub tenants: u64,
    /// Average resident working-set bytes per tenant the estimate assumed.
    pub avg_working_set_bytes: u64,
    /// Total resident RAM a single engine would need to hold ALL working sets hot.
    pub total_working_set_bytes: u64,
    /// Resident RAM the engine actually needs given the configured budget (the budget
    /// enforcer evicts/hibernates cold tenants past this — so a box only needs to hold
    /// the budgeted resident fraction hot, the rest serve read-through from the durable
    /// tier).
    pub budgeted_resident_bytes: u64,
    /// How many shards of `per_shard_budget_bytes` it takes to hold the budgeted resident
    /// set (the autoscale target).
    pub recommended_shards: u64,
}

/// Estimate the resource footprint for `tenants` tenants each with an `avg_working_set`
/// (CONCEPT:EG-KG.compute.lane-v). `global_budget` is the per-shard resident-memory ceiling, and
/// `resident_fraction` (0.0–1.0) is the share of tenants expected hot at once (the rest
/// hibernate / read-through). Pure arithmetic — no engine state — so it is a planning
/// helper callable offline (and the doc's worked examples derive from it).
pub fn capacity_estimate(
    tenants: u64,
    avg_working_set_bytes: u64,
    per_shard_budget_bytes: u64,
    resident_fraction: f64,
) -> CapacityEstimate {
    let resident_fraction = resident_fraction.clamp(0.0, 1.0);
    let total = tenants.saturating_mul(avg_working_set_bytes);
    let budgeted_resident = ((total as f64) * resident_fraction) as u64;
    let recommended_shards = if per_shard_budget_bytes == 0 {
        0
    } else {
        budgeted_resident.div_ceil(per_shard_budget_bytes).max(1)
    };
    CapacityEstimate {
        tenants,
        avg_working_set_bytes,
        total_working_set_bytes: total,
        budgeted_resident_bytes: budgeted_resident,
        recommended_shards,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_of_splits_on_first_colon() {
        assert_eq!(tenant_of("agent:planner"), "agent");
        assert_eq!(tenant_of("enterprise-acme:billing:q1"), "enterprise-acme");
        assert_eq!(tenant_of("__commons__"), "__commons__");
        assert_eq!(tenant_of("plain"), "plain");
    }

    #[test]
    fn parse_bytes_handles_suffixes() {
        assert_eq!(parse_bytes("1024"), Some(1024));
        assert_eq!(parse_bytes("512k"), Some(512 * 1024));
        assert_eq!(parse_bytes("2m"), Some(2 * 1024 * 1024));
        assert_eq!(parse_bytes("3g"), Some(3 * 1024 * 1024 * 1024));
        assert_eq!(parse_bytes("4G"), Some(4 * 1024 * 1024 * 1024));
        assert_eq!(parse_bytes(""), None);
        assert_eq!(parse_bytes("notanumber"), None);
    }

    // ── Budget-enforcement integration proofs (CONCEPT:EG-KG.compute.lane-v) ────────────
    // These need a real durable backend (redb) so eviction is durability-gated and a
    // rehydrate read serves the evicted node back. Gated on `redb` (present in
    // pi/node/cluster/full); the budget/snapshot logic itself is feature `cost`.
    #[cfg(feature = "redb")]
    mod integration {
        use super::super::*;
        use crate::acl::{AgentIdentity, AgentRole, RequestContextClaims};
        use crate::channels::ChannelManager;
        use crate::durability::DurabilityPolicy;
        use crate::isolation::IsolationLayer;
        use crate::protocol::{GraphType, Method, Request, Response, ResultPayload};
        use crate::registry::GraphRegistry;
        use crate::server::persistence::read_through::{
            BackendGraphMaterializer, BackendReadThroughFactory,
        };
        use crate::server::persistence::redb_backend::RedbBackend;
        use crate::server::persistence::PersistenceBackend;
        use crate::server::{
            compute_verified_envelope_token, dispatch, ServerState, VerifiedEnvelopeParams,
        };
        use std::future::Future;
        use std::pin::Pin;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;
        use std::time::{SystemTime, UNIX_EPOCH};
        use tokio::sync::{RwLock, Semaphore};

        const SECRET: &str = "cost-budget-test";
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

        fn props(v: serde_json::Value) -> Vec<u8> {
            rmp_serde::to_vec(&v).unwrap()
        }

        async fn redb_state(dir_s: &str) -> Arc<RwLock<ServerState>> {
            let backend: Arc<dyn PersistenceBackend> = Arc::new(
                RedbBackend::open(dir_s.to_string(), DurabilityPolicy::Each, 256).expect("open"),
            );
            let state = Arc::new(RwLock::new(ServerState {
                #[cfg(feature = "redb")]
                cold_tracker: std::sync::Arc::new(
                    crate::server::persistence::cold_offload::ColdTenantTracker::new(),
                ),
                registry: GraphRegistry::new(),
                isolation: current_isolation(),
                channels: ChannelManager::new(),
                #[cfg(feature = "viz-static-export")]
                viz_engine: None,
                auth_secret: SECRET.to_string(),
                persist_dir: Some(dir_s.to_string()),
                persistence: Some(backend.clone()),
                max_in_flight: Arc::new(Semaphore::new(64)),
                read_admission: Arc::new(Semaphore::new(64)),
                per_graph_inflight: Arc::new(dashmap::DashMap::new()),
                per_graph_inflight_limit: 32,
                write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
                routed_write_coalescer: Arc::new(
                    crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
                ),
                open_txns: Arc::new(dashmap::DashMap::new()),
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
                foreign_sources: Arc::new(dashmap::DashMap::new()),
                #[cfg(feature = "kv")]
                kv: None,
                #[cfg(feature = "lake")]
                lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
            }));
            // Wire the durable read-through exactly like main.rs under authoritative mode.
            {
                let mut s = state.write().await;
                let factory = Arc::new(BackendReadThroughFactory::new(backend.clone()));
                s.registry.set_read_through_factory(factory);
                let materializer = Arc::new(BackendGraphMaterializer::new(backend.clone()));
                s.registry.set_materializer(materializer);
            }
            state
        }

        fn req(id: u64, graph: &str, method: Method) -> Request {
            // `std::env::set_var` mutates the WHOLE process's environment, and this
            // helper is called on every request built by every test in this module —
            // concurrently, across many `cargo test` worker threads in ONE process.
            // Every call site across the crate sets the SAME fixed values, so a
            // one-time `Once`-guarded init removes the redundant concurrent
            // `set_var` calls (each individually a data race per `std::env::set_var`'s
            // own thread-safety caveat) without changing behavior.
            static TEST_AUTH_ENV: std::sync::Once = std::sync::Once::new();
            TEST_AUTH_ENV.call_once(|| {
                std::env::set_var("EPISTEMIC_GRAPH_AUDIENCE", "epistemic-graph-test");
                std::env::set_var("EPISTEMIC_GRAPH_TENANT", "tenant-shared");
                std::env::set_var("EPISTEMIC_GRAPH_POLICY_VERSION", "policy-test");
                std::env::set_var(
                    "EPISTEMIC_GRAPH_SECURITY_STATE_DIR",
                    std::env::temp_dir()
                        .join(format!("epistemic-graph-unit-auth-{}", std::process::id())),
                );
            });
            let context = RequestContextClaims {
                principal: TEST_AGENT.to_string(),
                tenant: "tenant-shared".to_string(),
                audience: "epistemic-graph-test".to_string(),
                agent_id: TEST_AGENT.to_string(),
                roles: Vec::new(),
                scopes: vec!["*".to_string()],
                policy_version: "policy-test".to_string(),
                delegation: Vec::new(),
                node: None,
                priority: None,
            };
            let mut request = Request {
                id,
                graph: graph.to_string(),
                auth_token: String::new(),
                agent_id: Some(TEST_AGENT.to_string()),
                method,
            };
            let sequence = NONCE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let issued_at = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the system clock is after the Unix epoch");
            let nonce = format!(
                "cost-{}-{id}-{sequence}-{}",
                std::process::id(),
                issued_at.as_nanos()
            );
            let idempotency_key = format!("cost-request-{id}-{sequence}");
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

        /// Keep the full dispatcher state machine behind one heap indirection. The
        /// all-feature dispatcher is intentionally broad, and embedding its future in
        /// this integration test's future can exhaust the test harness thread stack
        /// before the first request is polled. Production transport tasks are already
        /// heap-owned; this gives the direct library test the same structural boundary.
        fn dispatch_on_heap<'a>(
            state: &'a Arc<RwLock<ServerState>>,
            request: Request,
        ) -> Pin<Box<dyn Future<Output = Response> + Send + 'a>> {
            Box::pin(dispatch(state, request))
        }

        async fn assert_authoritative_projection(state: &Arc<RwLock<ServerState>>, graph: &str) {
            let (core, backend, manifest) = {
                let state = state.read().await;
                let core = state
                    .registry
                    .get(graph)
                    .unwrap_or_else(|| panic!("graph {graph} is not resident"))
                    .core
                    .clone();
                let backend = state
                    .persistence
                    .clone()
                    .expect("cost integration state has durable persistence");
                let manifest = state
                    .registry
                    .materialization_manifest(graph)
                    .expect("resident graph has a materialization manifest");
                (core, backend, manifest)
            };
            let fname = crate::persist::sanitize(graph);
            let durable_version = backend
                .read_mutation_graph_version(&fname)
                .await
                .expect("authoritative version read")
                .expect("created graph has an authoritative version");
            assert_eq!(
                core.version(),
                durable_version,
                "serving projection must publish the committed authoritative version"
            );
            assert!(manifest.valid, "resident projection must be complete");
            assert_eq!(
                manifest.source_snapshot_version,
                Some(durable_version),
                "materialization manifest must carry the authoritative watermark"
            );
        }

        async fn create(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str) {
            let r = dispatch_on_heap(
                state,
                req(
                    id,
                    graph,
                    Method::CreateGraph {
                        graph_name: graph.into(),
                        graph_type: GraphType::Agent,
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "create {graph}: {:?}", r.error);
            assert_authoritative_projection(state, graph).await;
        }

        async fn add(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str, node: &str) {
            assert_authoritative_projection(state, graph).await;
            let r = dispatch_on_heap(
                state,
                req(
                    id,
                    graph,
                    Method::AddNode {
                        node_id: node.into(),
                        properties_msgpack: props(serde_json::json!({"payload": node})),
                    },
                ),
            )
            .await;
            assert!(r.error.is_none(), "add {node}: {:?}", r.error);
        }

        /// PROOF 1: a tenant over its byte budget triggers eviction/hibernation of its
        /// cold graphs and stays under cap; the evicted data rehydrates correctly on a
        /// read (durability-gated, no loss).
        #[tokio::test(flavor = "multi_thread")]
        async fn over_budget_evicts_and_rehydrates() {
            let dir = std::env::temp_dir().join(format!("eg-cost-budget-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();
            let state = redb_state(&dir_s).await;

            // One tenant `acme` with two graphs; fill them with nodes so the tenant's
            // resident memory is well over a tiny budget.
            create(&state, 1, "acme:hot").await;
            create(&state, 2, "acme:cold").await;
            let hot_was_evicted = {
                let state = state.read().await;
                let resident = state.registry.get("acme:hot").is_some();
                let manifest = state
                    .registry
                    .materialization_manifest("acme:hot")
                    .expect("created graph remains cataloged");
                if !resident {
                    assert_eq!(
                        manifest.phase,
                        crate::registry::MaterializationPhase::CatalogOnly,
                        "capacity eviction must leave a catalog-only generation"
                    );
                }
                !resident
            };
            if hot_was_evicted {
                let reopened = dispatch_on_heap(
                    &state,
                    req(
                        50,
                        "acme:hot",
                        Method::GetNodeProperties {
                            node_id: "version-probe".into(),
                        },
                    ),
                )
                .await;
                assert!(
                    reopened.error.is_none(),
                    "catalog-only graph must reopen: {:?}",
                    reopened.error
                );
            }
            assert_authoritative_projection(&state, "acme:hot").await;
            let mut id = 100u64;
            for i in 0..40 {
                add(&state, id, "acme:hot", &format!("h{i}")).await;
                id += 1;
                add(&state, id, "acme:cold", &format!("c{i}")).await;
                id += 1;
            }

            let before = collect_resource_stats(&state).await.unwrap();
            let acme_before = before
                .tenants
                .iter()
                .find(|t| t.tenant == "acme")
                .expect("acme tenant present");
            assert!(acme_before.memory_bytes > 0);

            // A budget below the tenant's TOTAL footprint (both graphs, ~3.2KB each at
            // today's `NODE_OVERHEAD` calibration) forces reclamation, but above ONE
            // graph's individual footprint so coldest-first eviction stops after
            // reclaiming exactly one graph, leaving the other resident — matching the
            // "reclamation is coldest-graph-granular" comment below. A budget under one
            // graph's footprint (the previous 2048/4096 constants) evicts BOTH graphs to
            // catalog-only, emptying the tenant from `collect_resource_stats` entirely
            // and failing the `acme` lookup below — not a BUG-192/193 regression, a
            // pre-existing miscalibration against the real per-node accounting.
            let cfg = CostConfig {
                global_ceiling_bytes: 9000,
                per_tenant_budget_bytes: 4500,
                interval_secs: 1,
            };
            assert!(acme_before.memory_bytes > cfg.per_tenant_budget_bytes);

            let (evicted, hibernated) = enforce_memory_budgets(&state, cfg).await;
            assert!(
                evicted > 0 || hibernated > 0,
                "expected the over-budget tenant to be reclaimed"
            );

            // STAYS UNDER CAP: after enforcement the tenant's resident footprint is at
            // or under the budget (within one graph's residual, since reclamation is
            // coldest-graph-granular).
            let after = collect_resource_stats(&state).await.unwrap();
            let acme_after = after.tenants.iter().find(|t| t.tenant == "acme").unwrap();
            assert!(
                acme_after.memory_bytes < acme_before.memory_bytes,
                "tenant footprint should shrink: {} !< {}",
                acme_after.memory_bytes,
                acme_before.memory_bytes
            );

            // REHYDRATES CORRECTLY: a read of an evicted node serves its durable blob
            // back through the read-through seam (no data loss).
            let r = dispatch_on_heap(
                &state,
                req(
                    9000,
                    "acme:cold",
                    Method::GetNodeProperties {
                        node_id: "c0".into(),
                    },
                ),
            )
            .await;
            assert!(
                r.error.is_none(),
                "evicted node must still read: {:?}",
                r.error
            );
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// U-142/U-148 (BUG-130) — regression closing the exact coverage gap the bug
        /// report calls out: `over_budget_evicts_and_rehydrates` above only ever
        /// checks `GetNodeProperties`, whose redb point-read seam serves an
        /// evicted-but-still-"resident" node just fine and therefore MASKS this
        /// defect. Pre-fix, `enforce_memory_budgets` durability-confirmed every node
        /// empty via `GraphCore::evict_resident_nodes` but never told the REGISTRY —
        /// `GraphRegistry::get`/`is_resident` kept reporting the graph resident with
        /// zero topology, so `dispatch.rs`'s cold-path lazy-open (gated on a registry
        /// MISS) never re-triggered, and the graph stayed permanently, silently empty
        /// to any whole-graph operation for the rest of the process's life. A
        /// governed `GetNodeProperties` point read still worked (redb read-through has
        /// no registry-residency dependency); a native Cypher `MATCH` did not (it runs
        /// `analysis_snapshot()` over the live, now-permanently-empty resident
        /// `GraphCore`, with no read-through fallback). FAILS without the
        /// `registry.evict_resident` call added to `enforce_memory_budgets`; PASSES
        /// with it.
        #[tokio::test(flavor = "multi_thread")]
        async fn budget_eviction_leaves_graph_catalog_only_so_cypher_and_point_read_agree() {
            let dir =
                std::env::temp_dir().join(format!("eg-cost-cypher-evict-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();
            let state = redb_state(&dir_s).await;

            const NODE_COUNT: usize = 20;
            create(&state, 1, "acme:solo").await;
            for (id, i) in (100u64..).zip(0..NODE_COUNT) {
                add(&state, id, "acme:solo", &format!("n{i}")).await;
            }

            // `acme:solo` is the ONLY graph for tenant `acme`, so a budget far below
            // its footprint forces the enforcer to reclaim it down to zero (there is
            // no colder sibling graph to reclaim instead).
            let cfg = CostConfig {
                global_ceiling_bytes: 1,
                per_tenant_budget_bytes: 1,
                interval_secs: 1,
            };
            enforce_memory_budgets(&state, cfg).await;

            // The registry must have transitioned this graph OUT of the resident map
            // entirely (catalog-only) — not merely emptied its topology while still
            // reporting resident. This is the exact defect: pre-fix, `registry.get`
            // still returned `Some` here because only the `GraphCore`'s RAM was
            // cleared, never the registry entry.
            {
                let s = state.read().await;
                assert!(
                    s.registry.get("acme:solo").is_none(),
                    "a fully durability-reclaimed graph must leave the resident map \
                     (go catalog-only), or dispatch's cold-path lazy-open (gated on a \
                     registry MISS) never re-triggers on the next access"
                );
                let manifest = s
                    .registry
                    .materialization_manifest("acme:solo")
                    .expect("an evicted graph remains cataloged, just not resident");
                assert_eq!(
                    manifest.phase,
                    crate::registry::MaterializationPhase::CatalogOnly,
                    "an evicted graph must be catalog-only, ready for the next lazy-open"
                );
            }

            // GOVERNED POINT READ: rehydrates via lazy-open, sees the durable node.
            let point = dispatch_on_heap(
                &state,
                req(
                    9001,
                    "acme:solo",
                    Method::GetNodeProperties {
                        node_id: "n0".into(),
                    },
                ),
            )
            .await;
            assert!(point.error.is_none(), "point read: {:?}", point.error);

            // NATIVE CYPHER over the SAME graph, the SAME data, via the whole-graph
            // MATCH path (`analysis_snapshot()`, no read-through fallback) — must see
            // every durably-written node, not zero. This is the assertion prior
            // "only GetNodeProperties" coverage never made, and the one U-142/U-148
            // reproduced live: governed reads saw data while Cypher saw none.
            let cypher = dispatch_on_heap(
                &state,
                req(
                    9002,
                    "acme:solo",
                    Method::CypherQuery {
                        query: "MATCH (n) RETURN n".into(),
                        mode: crate::protocol::CypherMode::Read,
                    },
                ),
            )
            .await;
            assert!(cypher.error.is_none(), "cypher: {:?}", cypher.error);
            let bytes = match &cypher.result {
                Some(ResultPayload::Raw(b)) => b.clone(),
                other => panic!("expected Raw cypher result, got {other:?}"),
            };
            let result: crate::protocol::QueryResult = rmp_serde::from_slice(&bytes).unwrap();
            assert_eq!(
                result.rows.len(),
                NODE_COUNT,
                "Cypher must observe every durably-written node once the budget \
                 eviction's catalog-only transition lets dispatch lazily reopen the \
                 graph — a stale/empty-but-\"resident\" image would report 0 rows \
                 here even though the governed point read above already found data \
                 (through redb's separate read-through seam, which does not depend \
                 on registry residency the way a whole-graph scan does)"
            );

            let _ = std::fs::remove_dir_all(&dir);
        }

        /// PROOF 2: a fair per-tenant cap stops one hot tenant from starving others —
        /// the hot tenant is reclaimed while a small tenant under the fair share is left
        /// alone.
        #[tokio::test(flavor = "multi_thread")]
        async fn fair_cap_protects_small_tenant() {
            let dir = std::env::temp_dir().join(format!("eg-cost-fair-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();
            let state = redb_state(&dir_s).await;

            create(&state, 1, "whale:big").await;
            create(&state, 2, "minnow:small").await;
            let mut id = 100u64;
            for i in 0..60 {
                add(&state, id, "whale:big", &format!("b{i}")).await;
                id += 1;
            }
            add(&state, id, "minnow:small", "s0").await;

            let small_before = {
                let s = collect_resource_stats(&state).await.unwrap();
                s.tenants
                    .iter()
                    .find(|t| t.tenant == "minnow")
                    .unwrap()
                    .memory_bytes
            };

            // Global ceiling split across 2 tenants → fair share is generous enough for
            // the minnow but the whale blows past it.
            let cfg = CostConfig {
                global_ceiling_bytes: 8192,
                per_tenant_budget_bytes: 8192,
                interval_secs: 1,
            };
            enforce_memory_budgets(&state, cfg).await;

            let after = collect_resource_stats(&state).await.unwrap();
            let small_after = after
                .tenants
                .iter()
                .find(|t| t.tenant == "minnow")
                .unwrap()
                .memory_bytes;
            // The minnow (well under its fair share) is untouched.
            assert_eq!(
                small_after, small_before,
                "small tenant under the fair share must not be reclaimed"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }

        /// PROOF 3: ResourceStats returns accurate per-graph + aggregate counts under a
        /// realistic fill (the autoscale-signal accuracy gate).
        #[tokio::test(flavor = "multi_thread")]
        async fn resource_stats_counts_are_accurate() {
            let dir = std::env::temp_dir().join(format!("eg-cost-stats-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            let dir_s = dir.to_string_lossy().to_string();
            let state = redb_state(&dir_s).await;

            create(&state, 1, "acme:a").await;
            create(&state, 2, "beta:b").await;
            for i in 0..10 {
                add(&state, 100 + i, "acme:a", &format!("a{i}")).await;
            }
            for i in 0..5 {
                add(&state, 200 + i, "beta:b", &format!("b{i}")).await;
            }

            let snap = collect_resource_stats(&state).await.unwrap();

            // Per-graph node counts are exact.
            let ga = snap.graphs.iter().find(|g| g.graph == "acme:a").unwrap();
            let gb = snap.graphs.iter().find(|g| g.graph == "beta:b").unwrap();
            assert_eq!(ga.nodes, 10, "acme:a node count");
            assert_eq!(gb.nodes, 5, "beta:b node count");
            assert_eq!(ga.tenant, "acme");
            assert_eq!(gb.tenant, "beta");
            assert!(
                ga.memory_bytes > gb.memory_bytes,
                "more nodes ⇒ more memory"
            );

            // Aggregate node count = 15 (+ whatever __commons__ holds, which is 0 here).
            assert_eq!(snap.total_nodes, 15, "aggregate node count");
            // Three graphs: __commons__ + acme:a + beta:b.
            assert_eq!(snap.graph_count, 3);
            // Three tenants: __commons__, acme, beta.
            assert_eq!(snap.tenant_count, 3);
            // Per-tenant rollup is exact.
            let ta = snap.tenants.iter().find(|t| t.tenant == "acme").unwrap();
            assert_eq!(ta.nodes, 10);
            assert_eq!(ta.graphs, 1);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn capacity_estimate_scales_with_tenants_and_shards() {
        // 1000 tenants, 1 MiB working set each, 256 MiB/shard, 50% hot at once.
        let mib = 1024 * 1024;
        let est = capacity_estimate(1000, mib, 256 * mib, 0.5);
        assert_eq!(est.total_working_set_bytes, 1000 * mib);
        assert_eq!(est.budgeted_resident_bytes, 500 * mib);
        // 500 MiB / 256 MiB = 2 shards (ceil).
        assert_eq!(est.recommended_shards, 2);

        // Holding everything hot (fraction 1.0) needs more shards.
        let hot = capacity_estimate(1000, mib, 256 * mib, 1.0);
        assert_eq!(hot.budgeted_resident_bytes, 1000 * mib);
        assert_eq!(hot.recommended_shards, 4);

        // Zero shard budget ⇒ undefined shard count (0), never a divide-by-zero.
        assert_eq!(capacity_estimate(10, mib, 0, 1.0).recommended_shards, 0);
    }
}
