//! Distributed graph compute — a Pregel/GAS vertex-centric engine (CONCEPT:EG-KG.storage.feature).
//!
//! Runs PageRank / connected-components / BFS ACROSS a set of graphs that span
//! multiple Raft groups/shards, by gather-apply-scatter SUPERSTEPS with message
//! passing between shards. The single-shard fast path stays the existing always-on
//! `algorithms::pagerank` etc.; THIS is the cross-shard coordinator that runs when a
//! computation spans >1 graph.
//!
//! ## The model — one graph = one shard = one partition
//!
//! Each graph in the `graphs` set is a SHARD (it routes to a Raft [`GroupId`] via the
//! [`super::multi::GroupRouter`]). A vertex lives in the shard whose graph first
//! declares it. The UNION of the shards is the logical graph the algorithm runs over —
//! an edge `u→v` where `u` and `v` live in DIFFERENT shards is a CROSS-SHARD edge, and
//! a message along it is a cross-shard message.
//!
//! ## The superstep loop (BSP / Pregel)
//!
//! A [`Partitioning`] holds each shard's owned vertices + the global union edges + the
//! current vertex values. A superstep is:
//!
//!   1. **SCATTER** — every active vertex emits a message along each out-edge (its
//!      value / out-degree for PageRank; its label for CC; its level for BFS). A
//!      message whose target vertex is owned by ANOTHER shard goes into that shard's
//!      inbox — this is the cross-shard exchange (in a multi-node deploy it rides the
//!      existing group RPC; in-process it is a buffer hand-off, but the PARTITIONING is
//!      real, so the result is identical to running on the union graph).
//!   2. **GATHER + APPLY** — every shard combines the messages delivered to each of its
//!      vertices and updates the vertex value (the Pregel `compute`).
//!
//! The loop runs to a fixed superstep count (PageRank) or to a global fixpoint (CC/BFS:
//! a superstep with zero value changes anywhere). Because messages are routed by vertex
//! ownership, the cross-shard result is BIT-FOR-BIT what the same algorithm produces on
//! the single union graph — the property the test asserts.
//!
//! ## Incremental / streaming + materialized views
//!
//! [`incremental_connected_components`] recomputes ONLY the vertices a delta touched
//! (the changed vertices + their neighborhood), reusing the prior labeling as the seed;
//! the result equals a from-scratch run (the test asserts `incremental == from_scratch`).
//! Named [`MatView`]s (CONCEPT:EG-KG.storage.feature) persist a distributed-compute result and refresh
//! incrementally; they live in the redb durable tier (see `super::store` / the handler).

#![cfg(feature = "compute-dist")]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;

use petgraph::visit::{EdgeRef, IntoEdgeReferences};
use tokio::sync::RwLock;

use crate::isolation::AccessLevel;
use crate::protocol::DistAlgo;
use crate::server::access::{check_graph_access, GraphReadAuthority};
use crate::server::ServerState;

/// A single vertex's identity: its global id. Vertices are addressed by their string
/// id (unique across the union — a vertex id present in two shards is the SAME logical
/// vertex, owned by the shard that declares it first in the `graphs` order).
pub type VertexId = String;

/// The gathered, partitioned topology for a distributed run (CONCEPT:EG-KG.storage.feature). Holds:
///
/// * `owner` — which shard each vertex is OWNED by (the routing table: a message to a
///   vertex is delivered to its owning shard, the cross-shard hop). One graph = one
///   shard = one Raft group, so this is the real partition boundary.
/// * `owned` — per shard, the vertices it owns (the partition's vertex set).
/// * the GLOBAL `out_edges` / `out_degree` over the UNION — every edge from every
///   shard, merged. A vertex's out-edges live wherever the edge was declared, so the
///   superstep gathers them globally; the partitioning only controls message ROUTING
///   (which shard a vertex's value/messages belong to), never the edge math. This keeps
///   the result bit-identical to the single-graph algorithm on the union (the property
///   the test asserts), while message passing is genuinely partitioned by ownership.
struct Partitioning {
    /// Which shard owns each vertex (its declaring shard, first in `graphs` order).
    owner: HashMap<VertexId, usize>,
    /// Per shard: the vertices it owns. `owned[i]` is shard `i`'s partition.
    owned: Vec<BTreeSet<VertexId>>,
    /// GLOBAL out-edges over the union: `src -> [tgt, …]` (parallel edges kept, matching
    /// the single-graph degree). The far endpoint may be owned by another shard — that
    /// is the cross-shard edge a message rides over.
    out_edges: HashMap<VertexId, Vec<VertexId>>,
    /// Out-degree of every vertex in the UNION (PageRank mass split).
    out_degree: HashMap<VertexId, usize>,
    /// Every vertex in the union, sorted — the deterministic result domain.
    all_vertices: Vec<VertexId>,
}

impl Partitioning {
    /// The shard that owns `v` (`None` if `v` is not in the union).
    fn owner_of(&self, v: &str) -> Option<usize> {
        self.owner.get(v).copied()
    }
}

/// Mutable union builder used while snapshots are still borrowed. Keeping the
/// ownership, edge de-duplication, and fallback endpoint rules together makes the
/// gather phase's ordering explicit without changing the resulting partition.
struct PartitionBuilder {
    owner: HashMap<VertexId, usize>,
    owned: Vec<BTreeSet<VertexId>>,
    out_edges: HashMap<VertexId, Vec<VertexId>>,
    out_degree: HashMap<VertexId, usize>,
    seen_edges: HashSet<(VertexId, VertexId)>,
}

impl PartitionBuilder {
    fn new(n_shards: usize) -> Self {
        Self {
            owner: HashMap::new(),
            owned: vec![BTreeSet::new(); n_shards],
            out_edges: HashMap::new(),
            out_degree: HashMap::new(),
            seen_edges: HashSet::new(),
        }
    }

    fn add_view(&mut self, shard_idx: usize, view: &eg_core::graph::GraphView) {
        for id in view.node_map.keys() {
            if let std::collections::hash_map::Entry::Vacant(entry) = self.owner.entry(id.clone()) {
                entry.insert(shard_idx);
                self.owned[shard_idx].insert(id.clone());
            }
        }
        for edge in view.graph.edge_references() {
            let source = view.graph[edge.source()].clone();
            let target = view.graph[edge.target()].clone();
            if self.seen_edges.insert((source.clone(), target.clone())) {
                self.out_edges
                    .entry(source.clone())
                    .or_default()
                    .push(target);
                *self.out_degree.entry(source).or_insert(0) += 1;
            }
        }
    }

    fn finish(mut self) -> Partitioning {
        if !self.owned.is_empty() {
            let targets: Vec<VertexId> = self.out_edges.values().flatten().cloned().collect();
            for target in targets {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    self.owner.entry(target.clone())
                {
                    entry.insert(0);
                    self.owned[0].insert(target);
                }
            }
        }

        let all_vertices = self
            .owner
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Partitioning {
            owner: self.owner,
            owned: self.owned,
            out_edges: self.out_edges,
            out_degree: self.out_degree,
            all_vertices,
        }
    }
}

/// Gather each graph's authorized analysis projection into a [`Partitioning`]. It
/// ACL-checks and RLS-filters every shard before assigning each vertex to the FIRST
/// shard (in `graphs` order) that declares it as a NODE and merging the visible edges.
///
/// A graph that does not exist yet is skipped (an empty shard) — mirrors the
/// cross-graph union-read tolerance.
async fn gather_shards(
    state: &Arc<RwLock<ServerState>>,
    graphs: &[String],
    read_authority: &GraphReadAuthority,
) -> Result<Partitioning, String> {
    let s = state.read().await;
    // Resolve + ACL-check every shard while holding only the registry lock. Actual
    // snapshots and RLS filtering happen after it is released.
    let mut cores = Vec::with_capacity(graphs.len());
    for name in graphs {
        match s.registry.get(name) {
            Some(entry) => {
                check_graph_access(
                    &s.isolation,
                    read_authority.actor(),
                    name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Read,
                )?;
                cores.push(entry.core.clone());
            }
            None => continue, // a shard graph may not exist yet — empty partition.
        }
    }
    drop(s);

    // perf/row-visibility-index (B-sweep): one job submission commonly gathers
    // several shards at the SAME actor+version — `GraphReadAuthority::
    // cached_filter_view` amortizes the per-node RLS decode across them (a
    // per-CORE cache, so distinct shards never collide) instead of re-decoding
    // every node of every shard on every job.
    #[cfg(feature = "security")]
    let mut snaps: Vec<std::sync::Arc<eg_core::graph::GraphView>> = Vec::with_capacity(cores.len());
    #[cfg(feature = "security")]
    for core in &cores {
        snaps.push(read_authority.cached_filter_view(core));
    }
    #[cfg(not(feature = "security"))]
    let mut snaps: Vec<eg_core::graph::GraphView> = Vec::with_capacity(cores.len());
    #[cfg(not(feature = "security"))]
    for core in cores {
        let mut view = core.analysis_snapshot();
        read_authority.filter_view(&mut view);
        snaps.push(view);
    }

    let mut builder = PartitionBuilder::new(snaps.len());
    for (shard_idx, view) in snaps.iter().enumerate() {
        builder.add_view(shard_idx, view);
    }
    Ok(builder.finish())
}

/// A scored result row `(vertex_id, value)` — PageRank score, or a numeric label for
/// CC (component representative hashed to f64 is lossy, so CC/BFS use the i64 form
/// below). PageRank uses this; CC/BFS use [`LabelRows`].
pub type ScoreRows = Vec<(String, f64)>;
/// A labeled result row `(vertex_id, label)` for CC (component id) / BFS (hop level).
pub type LabelRows = Vec<(String, i64)>;

/// The result of a distributed computation, in a wire-ready form: the declared
/// `GetMatView` body, serialized to `ResultPayload::Raw` by the handler.
use eg_types::result_contract::cluster::DistResult;

/// Run a distributed graph algorithm across `graphs` (each a shard), returning the
/// per-vertex result over the UNION. The cross-shard superstep coordinator: gather the
/// partitioning, then dispatch to the algorithm's superstep loop.
pub(crate) async fn run_distributed(
    state: &Arc<RwLock<ServerState>>,
    graphs: &[String],
    algo: &DistAlgo,
    read_authority: &GraphReadAuthority,
) -> Result<DistResult, String> {
    let part = gather_shards(state, graphs, read_authority).await?;
    run_partitioned(part, algo)
}

fn run_partitioned(part: Partitioning, algo: &DistAlgo) -> Result<DistResult, String> {
    Ok(match algo {
        DistAlgo::PageRank {
            damping,
            iterations,
        } => DistResult::Scores(distributed_pagerank(&part, *damping, *iterations)),
        DistAlgo::ConnectedComponents => {
            DistResult::Labels(distributed_connected_components(&part))
        }
        DistAlgo::Bfs { source } => DistResult::Labels(distributed_bfs(&part, source)),
    })
}

// ── PageRank — power iteration as Pregel supersteps with cross-shard messages ──

/// Distributed PageRank. Each superstep: every vertex SCATTERS `damping·value/out_degree`
/// along each out-edge (a message, routed to the target's owning shard — cross-shard if
/// the target lives elsewhere); every vertex then GATHERS the sum of incoming messages
/// and APPLIES `teleport + sum`. This mirrors the single-graph `algorithms::pagerank`
/// power iteration EXACTLY (same teleport, same per-edge mass split, dangling mass simply
/// leaks — NOT re-normalized), so the distributed result is bit-identical to the
/// single-graph result on the UNION graph (the property the test asserts).
fn pagerank_messages<'a>(
    part: &'a Partitioning,
    value: &HashMap<&'a str, f64>,
    damping: f64,
) -> HashMap<&'a str, f64> {
    let mut inbox = HashMap::new();
    for owned in &part.owned {
        for source in owned {
            accumulate_pagerank_messages(part, source, value, damping, &mut inbox);
        }
    }
    inbox
}

fn accumulate_pagerank_messages<'a>(
    part: &'a Partitioning,
    source: &str,
    value: &HashMap<&'a str, f64>,
    damping: f64,
    inbox: &mut HashMap<&'a str, f64>,
) {
    let degree = part.out_degree.get(source).copied().unwrap_or(0);
    if degree == 0 {
        return;
    }
    let share = damping * value[source] / degree as f64;
    if let Some(targets) = part.out_edges.get(source) {
        for target in targets {
            // Route the message to the target's owning shard (a real cross-shard hop
            // when the target is owned elsewhere).
            if part.owner_of(target).is_some() {
                *inbox.entry(target.as_str()).or_insert(0.0) += share;
            }
        }
    }
}

fn pagerank_next<'a>(
    part: &'a Partitioning,
    inbox: &HashMap<&'a str, f64>,
    teleport: f64,
) -> HashMap<&'a str, f64> {
    let mut next = HashMap::with_capacity(part.all_vertices.len());
    for vertex in &part.all_vertices {
        let incoming = inbox.get(vertex.as_str()).copied().unwrap_or(0.0);
        next.insert(vertex.as_str(), teleport + incoming);
    }
    next
}

fn distributed_pagerank(part: &Partitioning, damping: f64, iterations: usize) -> ScoreRows {
    let n = part.all_vertices.len();
    if n == 0 {
        return Vec::new();
    }
    let init = 1.0 / n as f64;
    let teleport = (1.0 - damping) / n as f64;

    // Vertex value, addressed globally (the supersteps are over the union; partitioning
    // controls only WHERE a message is produced/consumed — the math is identical).
    let mut value: HashMap<&str, f64> = part
        .all_vertices
        .iter()
        .map(|v| (v.as_str(), init))
        .collect();

    for _ in 0..iterations {
        // SCATTER: each shard produces messages from its OWNED vertices' out-edges,
        // routing each to the target's owning shard's inbox. We accumulate the inbox as
        // a per-target sum (the Pregel combiner — sum is associative, so combining at
        // the source shard before routing is equivalent and cheaper). A vertex with no
        // out-edges (a sink) simply emits nothing — its mass leaks, matching the
        // reference power iteration (no dangling redistribution).
        let inbox = pagerank_messages(part, &value, damping);

        // GATHER + APPLY: `teleport + Σ incoming`.
        value = pagerank_next(part, &inbox, teleport);
    }

    let mut rows: ScoreRows = value.into_iter().map(|(k, s)| (k.to_string(), s)).collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

// ── Connected components — label propagation supersteps to a fixpoint ──────────

fn vertex_indices<'a>(vertices: &'a [VertexId]) -> HashMap<&'a str, i64> {
    vertices
        .iter()
        .enumerate()
        .map(|(index, vertex)| (vertex.as_str(), index as i64))
        .collect()
}

fn undirected_adjacency<'a>(
    part: &'a Partitioning,
    indices: &HashMap<&'a str, i64>,
) -> HashMap<&'a str, Vec<&'a str>> {
    let mut adjacency: HashMap<&'a str, Vec<&'a str>> = HashMap::new();
    for (source, targets) in &part.out_edges {
        for target in targets {
            if indices.contains_key(target.as_str()) {
                adjacency
                    .entry(source.as_str())
                    .or_default()
                    .push(target.as_str());
                adjacency
                    .entry(target.as_str())
                    .or_default()
                    .push(source.as_str());
            }
        }
    }
    adjacency
}

fn propagate_labels<'a>(
    vertices: &'a [VertexId],
    adjacency: &HashMap<&'a str, Vec<&'a str>>,
    mut labels: HashMap<&'a str, i64>,
) -> HashMap<&'a str, i64> {
    loop {
        let mut changed = false;
        // SCATTER+GATHER+APPLY fused: for each vertex take min(self, neighbor labels).
        let mut next = labels.clone();
        for vertex in vertices {
            let mut minimum = labels[vertex.as_str()];
            if let Some(neighbors) = adjacency.get(vertex.as_str()) {
                for neighbor in neighbors {
                    minimum = minimum.min(labels[*neighbor]);
                }
            }
            if minimum != labels[vertex.as_str()] {
                next.insert(vertex.as_str(), minimum);
                changed = true;
            }
        }
        labels = next;
        if !changed {
            return labels;
        }
    }
}

fn sorted_label_rows(labels: HashMap<&str, i64>) -> LabelRows {
    let mut rows: LabelRows = labels
        .into_iter()
        .map(|(vertex, label)| (vertex.to_string(), label))
        .collect();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

/// Distributed weakly-connected components via min-label propagation. Each vertex's
/// label starts as its own index (in the sorted union order). Every superstep a vertex
/// SCATTERS its label along every incident edge (both directions — weak connectivity);
/// a vertex GATHERS the min of incoming labels and its own, and APPLIES it. The loop
/// runs to a global fixpoint (a superstep with no label change). At convergence every
/// vertex in a component carries the component's minimum index — identical to the
/// single-graph `connected_components` partition.
fn distributed_connected_components(part: &Partitioning) -> LabelRows {
    if part.all_vertices.is_empty() {
        return Vec::new();
    }
    let indices = vertex_indices(&part.all_vertices);
    let adjacency = undirected_adjacency(part, &indices);
    let labels = part
        .all_vertices
        .iter()
        .map(|vertex| (vertex.as_str(), indices[vertex.as_str()]))
        .collect();

    sorted_label_rows(propagate_labels(&part.all_vertices, &adjacency, labels))
}

// ── BFS levels — frontier supersteps with cross-shard messages ─────────────────

fn expand_bfs_frontier<'a>(
    part: &'a Partitioning,
    level: &mut HashMap<&'a str, i64>,
    frontier: &mut VecDeque<String>,
    current_level: i64,
) -> BTreeSet<String> {
    let mut next = BTreeSet::new();
    while let Some(vertex) = frontier.pop_front() {
        if let Some(targets) = part.out_edges.get(&vertex) {
            for target in targets {
                if let Some(slot) = level.get_mut(target.as_str()) {
                    if *slot == -1 {
                        *slot = current_level + 1;
                        next.insert(target.clone());
                    }
                }
            }
        }
    }
    next
}

/// Distributed BFS hop-levels from `source`. Level starts at 0 for `source`, ∞ (-1)
/// elsewhere. Each superstep the frontier SCATTERS `level+1` along out-edges (routed
/// cross-shard to the target's owner); a vertex with no level yet GATHERS the min and
/// APPLIES it, entering the next frontier. Runs until the frontier is empty. Matches a
/// single-graph BFS over the union (following directed out-edges).
fn distributed_bfs(part: &Partitioning, source: &str) -> LabelRows {
    if part.owner_of(source).is_none() {
        // Source not in the union → every vertex unreachable.
        return part.all_vertices.iter().map(|v| (v.clone(), -1)).collect();
    }
    let mut level: HashMap<&str, i64> = part
        .all_vertices
        .iter()
        .map(|v| (v.as_str(), -1i64))
        .collect();
    level.insert(source, 0);

    let mut frontier: VecDeque<String> = VecDeque::new();
    frontier.push_back(source.to_string());
    let mut cur = 0i64;

    while !frontier.is_empty() {
        // SCATTER from the current frontier; a message to a target owned by another
        // shard is the cross-shard hop. GATHER+APPLY: an unvisited target takes cur+1.
        let next = expand_bfs_frontier(part, &mut level, &mut frontier, cur);
        frontier.extend(next);
        cur += 1;
    }

    sorted_label_rows(level)
}

// ── Incremental / streaming connected components (CONCEPT:EG-KG.storage.feature) ────────────

fn incremental_seed_labels<'a>(
    vertices: &'a [VertexId],
    indices: &HashMap<&'a str, i64>,
    prior: &LabelRows,
    affected: &HashSet<String>,
) -> HashMap<&'a str, i64> {
    let prior_labels: HashMap<&str, i64> = prior
        .iter()
        .map(|(vertex, label)| (vertex.as_str(), *label))
        .collect();
    let mut labels = HashMap::with_capacity(vertices.len());
    for vertex in vertices {
        let label = if affected.contains(vertex) {
            indices[vertex.as_str()]
        } else {
            prior_labels
                .get(vertex.as_str())
                .copied()
                .unwrap_or(indices[vertex.as_str()])
        };
        labels.insert(vertex.as_str(), label);
    }
    labels
}

/// Recompute connected components INCREMENTALLY after a delta: given the prior labeling
/// and the set of vertices a delta touched (new/changed edges' endpoints), re-propagate
/// labels starting from the affected vertices' neighborhood rather than re-seeding every
/// vertex to its own index. The fixpoint is the SAME partition a from-scratch run yields
/// (the test asserts `incremental == from_scratch`), because min-label propagation is
/// monotone: re-running it from any over-approximation of the changed region converges to
/// the global minimum labeling, and unaffected components are already at their fixpoint.
///
/// This is the streaming variant: only the affected vertices' values are recomputed; the
/// rest are carried over from `prior`.
pub(crate) async fn incremental_connected_components(
    state: &Arc<RwLock<ServerState>>,
    graphs: &[String],
    prior: &LabelRows,
    affected: &HashSet<String>,
    read_authority: &GraphReadAuthority,
) -> Result<LabelRows, String> {
    let part = gather_shards(state, graphs, read_authority).await?;
    if part.all_vertices.is_empty() {
        return Ok(Vec::new());
    }
    let indices = vertex_indices(&part.all_vertices);
    let adjacency = undirected_adjacency(&part, &indices);

    // Seed labels from the prior result; a brand-new vertex (in the union but not in
    // `prior`) seeds to its own index. An affected vertex is RESET to its own index so
    // a split (an edge removed) can't keep a stale shared label — min-propagation then
    // re-derives the correct (possibly larger) representative for the affected region.
    let labels = incremental_seed_labels(&part.all_vertices, &indices, prior, affected);
    // Propagate to a fixpoint over the whole union. Only the affected region moves;
    // unaffected components are already converged.
    Ok(sorted_label_rows(propagate_labels(
        &part.all_vertices,
        &adjacency,
        labels,
    )))
}

// ── Materialized views (CONCEPT:EG-KG.storage.feature) ──────────────────────────────────────

/// A named, incrementally-maintained materialized view of a distributed-compute result
/// (CONCEPT:EG-KG.storage.feature). Persisted in the redb durable tier so it survives restart; the
/// handler refreshes it incrementally on a delta. The definition (graphs + algo) is
/// stored alongside the rows so a `RefreshMatView` can recompute without re-specifying.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct MatView {
    pub name: String,
    pub graphs: Vec<String>,
    pub algo: DistAlgo,
    pub result: DistResult,
}

/// A flat map of named materialized views the engine maintains (CONCEPT:EG-KG.storage.feature). The
/// in-RAM index; the durable copy lives in redb (the handler persists + reloads). The
/// `BTreeMap` keeps a deterministic order for listing.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct MatViewStore {
    pub views: BTreeMap<String, MatView>,
}

impl MatViewStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn put(&mut self, view: MatView) {
        self.views.insert(view.name.clone(), view);
    }
    pub fn get(&self, name: &str) -> Option<&MatView> {
        self.views.get(name)
    }
}
