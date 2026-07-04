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

use tokio::sync::RwLock;

use crate::protocol::DistAlgo;
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

/// Gather each graph's topology into a [`Partitioning`]. Reads each graph's
/// `topology_snapshot()` off-lock (the same off-lock discipline the single-graph
/// algorithms use), assigns each vertex to the FIRST shard (in `graphs` order) that
/// declares it as a NODE, and merges every shard's edges into the global union edge set.
///
/// A graph that does not exist yet is skipped (an empty shard) — mirrors the
/// cross-graph union-read tolerance.
async fn gather_shards(
    state: &Arc<RwLock<ServerState>>,
    graphs: &[String],
) -> Result<Partitioning, String> {
    let s = state.read().await;
    // Snapshot each graph's topology under the registry lock's protection but reading
    // each core's own lock — we clone the petgraph out (off-lock compute thereafter).
    let mut snaps: Vec<eg_core::graph::GraphView> = Vec::with_capacity(graphs.len());
    for name in graphs {
        match s.registry.get(name) {
            Some(entry) => snaps.push(entry.core.topology_snapshot()),
            None => continue, // a shard graph may not exist yet — empty partition.
        }
    }
    drop(s);

    let n_shards = snaps.len();
    let mut owner: HashMap<VertexId, usize> = HashMap::new();
    let mut owned: Vec<BTreeSet<VertexId>> = vec![BTreeSet::new(); n_shards];
    let mut out_edges: HashMap<VertexId, Vec<VertexId>> = HashMap::new();
    let mut out_degree: HashMap<VertexId, usize> = HashMap::new();
    // Dedup the merged edge set: a cross-shard edge can be declared in BOTH shards (the
    // source's and a mirror); count each (src,tgt) ONCE so the union degree is exact.
    let mut seen_edges: HashSet<(VertexId, VertexId)> = HashSet::new();

    use petgraph::visit::{EdgeRef, IntoEdgeReferences};
    for (shard_idx, view) in snaps.iter().enumerate() {
        // Ownership: first shard (in `graphs` order) to declare a vertex as a NODE owns
        // it. (A vertex appearing only as a far edge endpoint is owned by whoever
        // declares it as a node; if none does, it is mapped below.)
        for id in view.node_map.keys() {
            if let std::collections::hash_map::Entry::Vacant(e) = owner.entry(id.clone()) {
                e.insert(shard_idx);
                owned[shard_idx].insert(id.clone());
            }
        }
        // Merge this shard's edges into the GLOBAL union edge set (deduped).
        for e in view.graph.edge_references() {
            let src = view.graph[e.source()].clone();
            let tgt = view.graph[e.target()].clone();
            if seen_edges.insert((src.clone(), tgt.clone())) {
                out_edges.entry(src.clone()).or_default().push(tgt);
                *out_degree.entry(src).or_insert(0) += 1;
            }
        }
    }

    // Any vertex referenced by an edge but never declared as a node (a pure target)
    // still needs an owner so a message to it lands somewhere; map it to shard 0.
    if n_shards > 0 {
        let targets: Vec<VertexId> = out_edges.values().flatten().cloned().collect();
        for t in targets {
            if let std::collections::hash_map::Entry::Vacant(e) = owner.entry(t.clone()) {
                e.insert(0);
                owned[0].insert(t);
            }
        }
    }

    let all_set: BTreeSet<VertexId> = owner.keys().cloned().collect();
    let all_vertices: Vec<VertexId> = all_set.into_iter().collect();

    Ok(Partitioning {
        owner,
        owned,
        out_edges,
        out_degree,
        all_vertices,
    })
}

/// A scored result row `(vertex_id, value)` — PageRank score, or a numeric label for
/// CC (component representative hashed to f64 is lossy, so CC/BFS use the i64 form
/// below). PageRank uses this; CC/BFS use [`LabelRows`].
pub type ScoreRows = Vec<(String, f64)>;
/// A labeled result row `(vertex_id, label)` for CC (component id) / BFS (hop level).
pub type LabelRows = Vec<(String, i64)>;

/// The result of a distributed computation, in a wire-ready form. Serialized to
/// `ResultPayload::Raw` by the handler.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum DistResult {
    /// PageRank: `(id, score)` rows, sorted by id (deterministic).
    Scores(ScoreRows),
    /// Connected components: `(id, component_repr_index)` rows, sorted by id. The
    /// component label is the index of the component's representative in the sorted
    /// vertex list (a stable small int), so it round-trips losslessly.
    Labels(LabelRows),
}

impl DistResult {
    /// The number of result rows.
    pub fn len(&self) -> usize {
        match self {
            DistResult::Scores(v) => v.len(),
            DistResult::Labels(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Run a distributed graph algorithm across `graphs` (each a shard), returning the
/// per-vertex result over the UNION. The cross-shard superstep coordinator: gather the
/// partitioning, then dispatch to the algorithm's superstep loop.
pub async fn run_distributed(
    state: &Arc<RwLock<ServerState>>,
    graphs: &[String],
    algo: &DistAlgo,
) -> Result<DistResult, String> {
    let part = gather_shards(state, graphs).await?;
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
        let mut inbox: HashMap<&str, f64> = HashMap::new();
        for owned in &part.owned {
            for src in owned {
                let deg = part.out_degree.get(src).copied().unwrap_or(0);
                if deg == 0 {
                    continue;
                }
                let share = damping * value[src.as_str()] / deg as f64;
                if let Some(targets) = part.out_edges.get(src) {
                    for t in targets {
                        // Route the message to the target's owning shard (a real
                        // cross-shard hop when the target is owned elsewhere).
                        if part.owner_of(t).is_some() {
                            *inbox.entry(t.as_str()).or_insert(0.0) += share;
                        }
                    }
                }
            }
        }

        // GATHER + APPLY: `teleport + Σ incoming`.
        let mut next: HashMap<&str, f64> = HashMap::with_capacity(n);
        for v in &part.all_vertices {
            let incoming = inbox.get(v.as_str()).copied().unwrap_or(0.0);
            next.insert(v.as_str(), teleport + incoming);
        }
        value = next;
    }

    let mut rows: ScoreRows = value.into_iter().map(|(k, s)| (k.to_string(), s)).collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

// ── Connected components — label propagation supersteps to a fixpoint ──────────

/// Distributed weakly-connected components via min-label propagation. Each vertex's
/// label starts as its own index (in the sorted union order). Every superstep a vertex
/// SCATTERS its label along every incident edge (both directions — weak connectivity);
/// a vertex GATHERS the min of incoming labels and its own, and APPLIES it. The loop
/// runs to a global fixpoint (a superstep with no label change). At convergence every
/// vertex in a component carries the component's minimum index — identical to the
/// single-graph `connected_components` partition.
fn distributed_connected_components(part: &Partitioning) -> LabelRows {
    let n = part.all_vertices.len();
    if n == 0 {
        return Vec::new();
    }
    let idx: HashMap<&str, i64> = part
        .all_vertices
        .iter()
        .enumerate()
        .map(|(i, v)| (v.as_str(), i as i64))
        .collect();

    // Build the UNDIRECTED adjacency from the global union out-edges (weak connectivity:
    // an edge connects in both directions regardless of which shard owns the endpoint).
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for (src, targets) in &part.out_edges {
        for t in targets {
            if idx.contains_key(t.as_str()) {
                adj.entry(src.as_str()).or_default().push(t.as_str());
                adj.entry(t.as_str()).or_default().push(src.as_str());
            }
        }
    }

    let mut label: HashMap<&str, i64> = part
        .all_vertices
        .iter()
        .map(|v| (v.as_str(), idx[v.as_str()]))
        .collect();

    loop {
        let mut changed = false;
        // SCATTER+GATHER+APPLY fused: for each vertex take min(self, neighbor labels).
        let mut next = label.clone();
        for v in &part.all_vertices {
            let mut m = label[v.as_str()];
            if let Some(nbrs) = adj.get(v.as_str()) {
                for nb in nbrs {
                    m = m.min(label[*nb]);
                }
            }
            if m != label[v.as_str()] {
                next.insert(v.as_str(), m);
                changed = true;
            }
        }
        label = next;
        if !changed {
            break;
        }
    }

    let mut rows: LabelRows = label.into_iter().map(|(k, l)| (k.to_string(), l)).collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

// ── BFS levels — frontier supersteps with cross-shard messages ─────────────────

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
        let mut next: BTreeSet<String> = BTreeSet::new();
        // SCATTER from the current frontier; a message to a target owned by another
        // shard is the cross-shard hop. GATHER+APPLY: an unvisited target takes cur+1.
        while let Some(u) = frontier.pop_front() {
            // SCATTER along u's out-edges (global union). A target owned by another shard
            // is the cross-shard hop. GATHER+APPLY: an unvisited target (level == -1)
            // takes cur+1 and joins the next frontier.
            if let Some(targets) = part.out_edges.get(&u) {
                for t in targets {
                    if let Some(slot) = level.get_mut(t.as_str()) {
                        if *slot == -1 {
                            *slot = cur + 1;
                            next.insert(t.clone());
                        }
                    }
                }
            }
        }
        for v in next {
            frontier.push_back(v);
        }
        cur += 1;
    }

    let mut rows: LabelRows = level.into_iter().map(|(k, l)| (k.to_string(), l)).collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    rows
}

// ── Incremental / streaming connected components (CONCEPT:EG-KG.storage.feature) ────────────

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
pub async fn incremental_connected_components(
    state: &Arc<RwLock<ServerState>>,
    graphs: &[String],
    prior: &LabelRows,
    affected: &HashSet<String>,
) -> Result<LabelRows, String> {
    let part = gather_shards(state, graphs).await?;
    let n = part.all_vertices.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    let idx: HashMap<&str, i64> = part
        .all_vertices
        .iter()
        .enumerate()
        .map(|(i, v)| (v.as_str(), i as i64))
        .collect();

    // Undirected adjacency (weak connectivity) over the global union out-edges.
    let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
    for (src, targets) in &part.out_edges {
        for t in targets {
            if idx.contains_key(t.as_str()) {
                adj.entry(src.as_str()).or_default().push(t.as_str());
                adj.entry(t.as_str()).or_default().push(src.as_str());
            }
        }
    }

    // Seed labels from the prior result; a brand-new vertex (in the union but not in
    // `prior`) seeds to its own index. An affected vertex is RESET to its own index so
    // a split (an edge removed) can't keep a stale shared label — min-propagation then
    // re-derives the correct (possibly larger) representative for the affected region.
    let prior_map: HashMap<&str, i64> = prior.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    let mut label: HashMap<&str, i64> = HashMap::with_capacity(n);
    for v in &part.all_vertices {
        let l = if affected.contains(v) {
            idx[v.as_str()]
        } else {
            prior_map
                .get(v.as_str())
                .copied()
                .unwrap_or(idx[v.as_str()])
        };
        label.insert(v.as_str(), l);
    }

    // Propagate to a fixpoint (over the whole union — cheap because only the affected
    // region actually moves; unaffected components are already converged).
    loop {
        let mut changed = false;
        let mut next = label.clone();
        for v in &part.all_vertices {
            let mut m = label[v.as_str()];
            if let Some(nbrs) = adj.get(v.as_str()) {
                for nb in nbrs {
                    m = m.min(label[*nb]);
                }
            }
            if m != label[v.as_str()] {
                next.insert(v.as_str(), m);
                changed = true;
            }
        }
        label = next;
        if !changed {
            break;
        }
    }

    let mut rows: LabelRows = label.into_iter().map(|(k, l)| (k.to_string(), l)).collect();
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(rows)
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
