use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use eg_types::result_contract::coordination::{GraphResourceStats, TenantResourceStats};
use tokio::sync::RwLock;

use crate::server::ServerState;

use super::{
    is_hibernated, tenant_of, CostConfig, ResourceStatsCandidate, ResourceStatsRequest,
    TenantMatcher, MAX_RESOURCE_STATS_CURSOR_BYTES, MAX_RESOURCE_STATS_TENANTS,
};

pub(super) struct ScanSummary {
    pub(super) total_memory_bytes: u64,
    pub(super) total_nodes: u64,
    pub(super) total_edges: u64,
    pub(super) resident_graphs: u64,
    pub(super) hibernated_graphs: u64,
    pub(super) has_more: bool,
    pub(super) next_cursor: Option<String>,
    pub(super) tenant_count_truncated: bool,
    pub(super) tenants: Vec<TenantResourceStats>,
    pub(super) graphs: Vec<GraphResourceStats>,
}

#[derive(Clone, Copy)]
enum GraphResidence {
    Resident,
    Hibernated,
}

impl GraphResidence {
    fn is_hibernated(self) -> bool {
        matches!(self, Self::Hibernated)
    }
}

#[derive(Clone, Copy)]
enum CursorPosition {
    Before,
    After,
}

impl CursorPosition {
    fn is_after(self) -> bool {
        matches!(self, Self::After)
    }
}

/// One graph's measured size: the node, edge, and estimated-memory counts every
/// accumulator stage adds to its totals.
#[derive(Clone, Copy)]
struct GraphFootprint {
    nodes: u64,
    edges: u64,
    memory_bytes: u64,
}

#[derive(Clone, Copy)]
struct RecordMode {
    residence: GraphResidence,
    cursor: CursorPosition,
}

impl RecordMode {
    fn for_graph(graph: &str, incarnation_id: &str, cursor: CursorPosition) -> Self {
        let residence = if is_hibernated(graph, incarnation_id) {
            GraphResidence::Hibernated
        } else {
            GraphResidence::Resident
        };
        Self { residence, cursor }
    }
}

struct ScanAccumulator {
    page_candidates: BinaryHeap<ResourceStatsCandidate>,
    tenant_rollup: HashMap<String, TenantResourceStats>,
    total_memory_bytes: u64,
    total_nodes: u64,
    total_edges: u64,
    resident_graphs: u64,
    hibernated_graphs: u64,
    visible_after_cursor: u64,
    tenant_count_truncated: bool,
    oversized_graph_name: bool,
}

impl ScanAccumulator {
    fn new(limit: usize) -> Self {
        Self {
            page_candidates: BinaryHeap::with_capacity(limit),
            tenant_rollup: HashMap::new(),
            total_memory_bytes: 0,
            total_nodes: 0,
            total_edges: 0,
            resident_graphs: 0,
            hibernated_graphs: 0,
            visible_after_cursor: 0,
            tenant_count_truncated: false,
            oversized_graph_name: false,
        }
    }

    fn record(
        &mut self,
        graph: String,
        incarnation_id: &str,
        request: &ResourceStatsRequest,
        tenant_budget_bytes: u64,
        cursor: CursorPosition,
        footprint: GraphFootprint,
    ) {
        let tenant = tenant_of(&graph).to_string();
        let mode = RecordMode::for_graph(&graph, incarnation_id, cursor);
        self.record_totals(mode, footprint);
        self.record_tenant(&tenant, tenant_budget_bytes, mode, footprint);
        self.record_candidate(request, graph, tenant, mode, footprint);
    }

    fn record_totals(&mut self, mode: RecordMode, footprint: GraphFootprint) {
        let GraphFootprint {
            nodes,
            edges,
            memory_bytes,
        } = footprint;
        self.total_memory_bytes = self.total_memory_bytes.saturating_add(memory_bytes);
        self.total_nodes = self.total_nodes.saturating_add(nodes);
        self.total_edges = self.total_edges.saturating_add(edges);
        if mode.residence.is_hibernated() {
            self.hibernated_graphs = self.hibernated_graphs.saturating_add(1);
        } else {
            self.resident_graphs = self.resident_graphs.saturating_add(1);
        }
        if mode.cursor.is_after() {
            self.visible_after_cursor = self.visible_after_cursor.saturating_add(1);
        }
    }

    fn record_tenant(
        &mut self,
        tenant: &str,
        budget_bytes: u64,
        mode: RecordMode,
        footprint: GraphFootprint,
    ) {
        let GraphFootprint {
            nodes,
            edges,
            memory_bytes,
        } = footprint;
        if let Some(rollup) = self.tenant_rollup.get_mut(tenant) {
            rollup.graphs = rollup.graphs.saturating_add(1);
            rollup.resident_graphs = rollup
                .resident_graphs
                .saturating_add(u64::from(!mode.residence.is_hibernated()));
            rollup.hibernated_graphs = rollup
                .hibernated_graphs
                .saturating_add(u64::from(mode.residence.is_hibernated()));
            rollup.nodes = rollup.nodes.saturating_add(nodes);
            rollup.edges = rollup.edges.saturating_add(edges);
            rollup.memory_bytes = rollup.memory_bytes.saturating_add(memory_bytes);
        } else if self.tenant_rollup.len() < MAX_RESOURCE_STATS_TENANTS {
            self.tenant_rollup.insert(
                tenant.to_string(),
                TenantResourceStats {
                    tenant: tenant.to_string(),
                    graphs: 1,
                    resident_graphs: u64::from(!mode.residence.is_hibernated()),
                    hibernated_graphs: u64::from(mode.residence.is_hibernated()),
                    nodes,
                    edges,
                    memory_bytes,
                    budget_bytes,
                    over_budget: false,
                },
            );
        } else {
            self.tenant_count_truncated = true;
        }
    }

    fn record_candidate(
        &mut self,
        request: &ResourceStatsRequest,
        graph: String,
        tenant: String,
        mode: RecordMode,
        footprint: GraphFootprint,
    ) {
        if request.summary || !mode.cursor.is_after() {
            return;
        }
        let candidate = ResourceStatsCandidate {
            graph,
            tenant,
            nodes: footprint.nodes,
            edges: footprint.edges,
            memory_bytes: footprint.memory_bytes,
            hibernated: mode.residence.is_hibernated(),
        };
        if self.page_candidates.len() < request.limit {
            self.page_candidates.push(candidate);
        } else if self
            .page_candidates
            .peek()
            .is_some_and(|worst| candidate.graph < worst.graph)
        {
            let _ = self.page_candidates.pop();
            self.page_candidates.push(candidate);
        }
    }

    fn finish(mut self, request: &ResourceStatsRequest) -> ScanSummary {
        for rollup in self.tenant_rollup.values_mut() {
            rollup.over_budget = rollup.memory_bytes > rollup.budget_bytes;
        }
        let mut tenants: Vec<TenantResourceStats> = self.tenant_rollup.into_values().collect();
        tenants.sort_by(|left, right| {
            right
                .memory_bytes
                .cmp(&left.memory_bytes)
                .then_with(|| left.tenant.cmp(&right.tenant))
        });

        let mut graphs: Vec<GraphResourceStats> = self
            .page_candidates
            .into_vec()
            .into_iter()
            .map(ResourceStatsCandidate::into_stats)
            .collect();
        graphs.sort_by(|left, right| left.graph.cmp(&right.graph));
        let has_more = !request.summary && self.visible_after_cursor > graphs.len() as u64;
        let next_cursor = has_more
            .then(|| graphs.last().map(|graph| graph.graph.clone()))
            .flatten();
        ScanSummary {
            total_memory_bytes: self.total_memory_bytes,
            total_nodes: self.total_nodes,
            total_edges: self.total_edges,
            resident_graphs: self.resident_graphs,
            hibernated_graphs: self.hibernated_graphs,
            has_more,
            next_cursor,
            tenant_count_truncated: self.tenant_count_truncated,
            tenants,
            graphs,
        }
    }
}

pub(super) async fn scan_registry(
    state: &Arc<RwLock<ServerState>>,
    authorization: Option<(&str, &str, bool)>,
    request: &ResourceStatsRequest,
    config: &CostConfig,
) -> Result<ScanSummary, String> {
    let mut scan = ScanAccumulator::new(request.limit);
    {
        let current = state.read().await;
        let isolation = current.isolation.clone();
        let actor = authorization.map(|(actor, _, _)| actor);
        let tenant_matcher = authorization.map(|(_, tenant, _)| TenantMatcher::new(tenant));
        let admin = authorization.is_some_and(|(_, _, admin)| admin);
        current.registry.for_each_entry(|entry| {
            if !visible_to_authority(&isolation, actor, tenant_matcher.as_ref(), admin, entry) {
                return;
            }
            if entry.name.len() > MAX_RESOURCE_STATS_CURSOR_BYTES
                || entry.name.bytes().any(|byte| byte == 0)
            {
                scan.oversized_graph_name = true;
                return;
            }
            let cursor = match request.cursor.as_deref() {
                Some(cursor) if entry.name.as_str() <= cursor => CursorPosition::Before,
                _ => CursorPosition::After,
            };
            scan.record(
                entry.name.clone(),
                &entry.incarnation_id,
                request,
                config.per_tenant_budget_bytes,
                cursor,
                GraphFootprint {
                    nodes: entry.core.node_count() as u64,
                    edges: entry.core.edge_count() as u64,
                    memory_bytes: entry.core.memory_estimate(),
                },
            );
        });
    }
    if scan.oversized_graph_name {
        return Err(format!(
            "ResourceStats contains a graph name that is not a valid bounded cursor key (maximum {MAX_RESOURCE_STATS_CURSOR_BYTES} bytes, NUL-free)"
        ));
    }
    Ok(scan.finish(request))
}

fn visible_to_authority(
    isolation: &crate::isolation::IsolationLayer,
    actor: Option<&str>,
    tenant_matcher: Option<&TenantMatcher>,
    admin: bool,
    entry: &crate::registry::GraphEntry,
) -> bool {
    let Some(tenant_matcher) = tenant_matcher else {
        return true;
    };
    if !admin && !tenant_matcher.matches(&entry.name) {
        return false;
    }
    admin
        || crate::server::access::check_graph_access(
            isolation,
            actor,
            &entry.name,
            entry.graph_type,
            entry.owner.as_deref(),
            crate::isolation::AccessLevel::Read,
        )
        .is_ok()
}
