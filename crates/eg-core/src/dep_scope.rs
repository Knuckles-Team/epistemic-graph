// CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation (W1.6 / P7) — the per-graph
// dependency clock that replaces "bump one version ⇒ invalidate EVERY cached result" with
// "a write invalidates only the cached entries whose dependency set OVERLAPS what the write
// changed".
//
// ## The problem it solves
//
// The version-keyed result cache (`crate::result_cache`) keys every entry on the graph's
// monotonic `version()`, which bumps on EVERY committed write. Under a mixed read/write
// workload the hit-rate therefore collapses toward the WRITE rate: a single unrelated write
// (an insert into a different label, an edge add) retires every cached read result for the
// whole graph, even results that could not possibly have changed. A repeated `MATCH (:A)`
// query on a graph being continuously ingested with `:B` nodes never hits.
//
// ## The mechanism
//
// Each committed write reports a [`WriteFootprint`] — the labels, property keys, and
// node/edge coarse dimensions it actually touched. `note_footprint` records, per dimension,
// the graph version at which that dimension was last written. A cached entry is tagged with
// the [`DepSet`] it READ (the labels it scanned, whether it read all nodes / edges) and the
// version it was computed at. It stays valid until one of ITS dimensions is written:
// [`DepClock::is_valid`] is `true` iff no dimension in the entry's dep-set has a
// last-write version newer than the entry's compute version.
//
// ## Soundness — the coarse floor and the covered-through watermark
//
// A stale hit is a correctness bug, so the clock is conservative by construction:
//
//   * `floor` — the highest version of a write that was NOT attributed to specific
//     dimensions (a bypass write that bumped `version()` without a footprint, or a footprint
//     that could not be computed soundly). Every dep-set is implicitly floored by it: an
//     entry computed at `V` is invalid the moment `floor > V`, whatever its dims say. This is
//     what keeps a follower's replicated writes (applied without an index-maintenance
//     footprint) or an un-attributable change from ever serving a stale result.
//   * `covered_through` — the highest version a footprint has accounted for. `note_version_bump`
//     (called from every `mark_dirty`) advances `floor` to the new version ONLY when that
//     version is beyond `covered_through`, i.e. only for a write no footprint covered. A
//     footprinted write advances `covered_through` first, so its own `mark_dirty` does NOT
//     floor — that is the whole win. See the module test `footprinted_write_does_not_floor`.
//
// Writes to one graph are serialized through a single applier at a time (a leader's gateway/
// coalescer OR a follower's replication, never both concurrently on the same graph — the raft
// leader/follower split), so the `covered_through`→`floor` reconciliation has no cross-path
// same-graph race. The footprint's version is an under-estimate-or-exact of the write's true
// version (it counts only recorded changes, and the per-op `mark_dirty` count can only be
// larger), so `floor`/`covered_through` may over-invalidate (an extra recompute — harmless)
// but never under-invalidate (a stale hit — unsound). That asymmetry is the safety margin.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

/// Cap on the number of distinct per-label / per-key dimensions the clock tracks
/// individually. Beyond it a write falls back to the coarse `all_nodes` dimension instead of
/// growing the map unbounded, so a pathological graph with millions of distinct labels cannot
/// blow up the clock's memory. Well above any realistic label/indexed-key cardinality.
const MAX_TRACKED_DIMS: usize = 8192;

/// One dimension a query can DEPEND on / a write can TOUCH. The label / property-key variants
/// are fine-grained (per value); the `AllNodes` / `AllEdges` variants are the coarse catch-alls
/// a query that reads the whole node/edge set (an unlabeled scan, a traversal) depends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dim {
    /// A specific node label's membership + the properties of nodes carrying it. Bumped by any
    /// add / remove / property-update of a node with this label. A `Scan { label }` query
    /// depends on exactly this.
    Label(String),
    /// A specific property key. Bumped by any write that adds / removes / changes that key on
    /// any node. A property-equality query (`WHERE k = v`) depends on this.
    Key(String),
    /// The whole node set (existence + every property). Bumped by any node write. An unlabeled
    /// scan depends on it.
    AllNodes,
    /// The whole edge set. Bumped by any edge write. A traversal depends on it.
    AllEdges,
}

/// The set of dimensions a cached query result actually READ — its dependency set. A write
/// invalidates the entry iff its change-set overlaps this. Built by the query handler from the
/// query plan; when the plan shape cannot be soundly reduced to a dependency set the handler
/// does NOT construct a `DepSet` at all and falls back to the version-keyed cache path
/// (coarse, unchanged) — this type only ever represents a PROVEN-complete dependency set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DepSet {
    dims: Vec<Dim>,
}

impl DepSet {
    /// A dependency set from an explicit dimension list. Empty is legal (a query that reads no
    /// graph state — e.g. a pure constant — which then survives every write).
    pub fn new(dims: Vec<Dim>) -> Self {
        Self { dims }
    }

    /// The dimensions this entry depends on.
    pub fn dims(&self) -> &[Dim] {
        &self.dims
    }
}

/// The change-set a committed write reports to the clock: the specific labels / property keys it
/// touched plus the coarse node/edge flags. Computed once per committed batch from its
/// `ChangeSet` (+ the live node properties for adds/updates, and captured blobs for removes).
#[derive(Debug, Clone, Default)]
pub struct WriteFootprint {
    /// Labels of every node this write added, removed, or updated (an updated node's CURRENT
    /// labels; a removed node's captured labels). A query scoped to any of these is invalidated.
    pub labels: Vec<String>,
    /// Property keys this write added, removed, or changed on any node.
    pub keys: Vec<String>,
    /// Any node was added / removed / updated — bumps the coarse `AllNodes` dimension.
    pub node_changed: bool,
    /// Any edge was added / removed — bumps the coarse `AllEdges` dimension.
    pub edge_changed: bool,
    /// A node change could NOT be attributed to specific labels/keys (a remove whose properties
    /// were not captured, or an unknown-scope update). Forces the coarse `floor` up so no
    /// dependency-scoped entry can survive it — the sound fallback for an un-attributable write.
    pub coarse_node: bool,
}

impl WriteFootprint {
    /// Whether this footprint carries any signal at all (an empty footprint — e.g. a
    /// vector-only `AddEmbedding` — touches no queryable node/edge dimension, so it advances
    /// `covered_through` without invalidating anything).
    pub fn is_touching(&self) -> bool {
        self.node_changed || self.edge_changed || self.coarse_node
    }
}

/// Per-graph dependency clock (CONCEPT:EG-KG.coordination.dependency-scoped-cache-invalidation).
/// Lives on `GraphCore` behind the `result-cache` feature. All fields are monotonic.
#[derive(Debug, Default)]
pub struct DepClock {
    /// `Label(l)` / `Key(k)` → the version at which that dimension was last written. A missing
    /// key means "never written since construction" (effective version 0).
    fine: Mutex<HashMap<Dim, u64>>,
    /// Last version any node was written (the coarse `AllNodes` dimension).
    all_nodes: AtomicU64,
    /// Last version any edge was written (the coarse `AllEdges` dimension).
    all_edges: AtomicU64,
    /// Highest version of a write that could NOT be attributed to specific dimensions. Every
    /// dep-set is implicitly floored by this; an entry computed at `V < floor` is always stale.
    floor: AtomicU64,
    /// Highest version a footprint has accounted for. `note_version_bump` floors only versions
    /// beyond this — so a footprinted write does not floor itself.
    covered_through: AtomicU64,
    /// Whether the per-label/per-key map hit `MAX_TRACKED_DIMS` and a write fell back to the
    /// coarse `AllNodes` dimension. Sticky (once saturated, fine label/key deps can no longer be
    /// proven fresh, so they always consult `all_nodes`). Observability + the read path.
    saturated: std::sync::atomic::AtomicBool,
}

impl DepClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a committed write's [`WriteFootprint`] at `version` (the batch's committed graph
    /// version). Bumps only the dimensions the write touched, then advances `covered_through`
    /// so the write's subsequent `mark_dirty` does not floor. Returns the number of fine
    /// dimensions recorded (observability).
    pub fn note_footprint(&self, fp: &WriteFootprint, version: u64) {
        if fp.node_changed || fp.coarse_node {
            self.all_nodes.fetch_max(version, Ordering::AcqRel);
        }
        if fp.edge_changed {
            self.all_edges.fetch_max(version, Ordering::AcqRel);
        }
        // An un-attributable node change floors everything at this version: no
        // dependency-scoped entry computed at or before it may survive.
        if fp.coarse_node {
            self.floor.fetch_max(version, Ordering::AcqRel);
        }
        if !fp.labels.is_empty() || !fp.keys.is_empty() {
            let mut fine = self.fine.lock();
            for label in &fp.labels {
                Self::bump_fine(
                    &mut fine,
                    &self.saturated,
                    Dim::Label(label.clone()),
                    version,
                );
            }
            for key in &fp.keys {
                Self::bump_fine(&mut fine, &self.saturated, Dim::Key(key.clone()), version);
            }
        }
        // ORDER: advance covered_through LAST, after every dimension is recorded, so a
        // concurrent reader that observes covered_through >= V is guaranteed to also observe
        // the dimension writes for V (AcqRel fences the fine-map mutex release).
        self.covered_through.fetch_max(version, Ordering::AcqRel);
        // GOOD LOGS: the invalidation DECISION — which dependency dimensions this write touched
        // (so a dependency-scoped result-cache entry overlapping them is now invalid) and whether
        // it floored (an un-attributable change invalidating everything). Off the hot path (once
        // per committed batch, field formatting lazy under `tracing`).
        if fp.is_touching() || !fp.labels.is_empty() || !fp.keys.is_empty() {
            tracing::debug!(
                target: "epistemic_graph::dep_cache",
                version,
                labels = ?fp.labels,
                keys = ?fp.keys,
                node = fp.node_changed,
                edge = fp.edge_changed,
                coarse_floor = fp.coarse_node,
                "dependency-scoped invalidation: entries depending on these dimensions are retired"
            );
        }
    }

    /// Record that `version` is now the committed graph version (called from every `mark_dirty`).
    /// Floors everything at `version` ONLY when no footprint has covered it — i.e. only for a
    /// write that bypassed the footprint path (a follower's replicated apply, or any mutation
    /// that bumped `version()` without reporting a change-set). A footprinted write, which has
    /// already advanced `covered_through` to at least `version`, does NOT floor.
    pub fn note_version_bump(&self, version: u64) {
        if version > self.covered_through.load(Ordering::Acquire) {
            self.floor.fetch_max(version, Ordering::AcqRel);
        }
    }

    /// The effective last-write version of one dimension: the max of its fine entry (if tracked)
    /// and the matching coarse dimension. When the fine map has saturated, a label/key dim can
    /// no longer be proven independent of an untracked label, so it conservatively reads
    /// `all_nodes`.
    fn dim_version(&self, dim: &Dim, fine: &HashMap<Dim, u64>) -> u64 {
        match dim {
            Dim::AllNodes => self.all_nodes.load(Ordering::Acquire),
            Dim::AllEdges => self.all_edges.load(Ordering::Acquire),
            Dim::Label(_) | Dim::Key(_) => {
                let tracked = fine.get(dim).copied().unwrap_or(0);
                if self.saturated.load(Ordering::Acquire) {
                    tracked.max(self.all_nodes.load(Ordering::Acquire))
                } else {
                    tracked
                }
            }
        }
    }

    /// Is a cached entry with dependency set `deps`, computed at graph version `computed_at`,
    /// still valid? True iff the coarse floor has not passed it AND no dimension it depends on
    /// has been written since. A `false` here means the query must be recomputed.
    pub fn is_valid(&self, deps: &DepSet, computed_at: u64) -> bool {
        if self.floor.load(Ordering::Acquire) > computed_at {
            return false;
        }
        let fine = self.fine.lock();
        for dim in deps.dims() {
            if self.dim_version(dim, &fine) > computed_at {
                return false;
            }
        }
        true
    }

    /// The effective NODE epoch: the version of the most recent write that could have changed any
    /// node (an add / remove / property update), OR any un-attributable bypass write (the floor).
    /// Folding in the floor keeps it SOUND for the SQL-context node-table sub-cache (W1.6/P7 site
    /// 3): a follower's replicated node write bumps the floor even though it recorded no footprint,
    /// so a cached node table can never be reused across it. Two calls observing the SAME
    /// `node_epoch` are guaranteed to see the SAME node set + node properties (hence the same
    /// RLS-filtered node projection), so the O(V) `nodes` Arrow batch may be reused across a
    /// pure-edge or catalog-only write instead of rebuilt.
    pub fn node_epoch(&self) -> u64 {
        self.all_nodes
            .load(Ordering::Acquire)
            .max(self.floor.load(Ordering::Acquire))
    }

    /// The effective EDGE epoch: the version of the most recent edge write or bypass write. The
    /// sibling of [`node_epoch`](Self::node_epoch) for the edge table.
    pub fn edge_epoch(&self) -> u64 {
        self.all_edges
            .load(Ordering::Acquire)
            .max(self.floor.load(Ordering::Acquire))
    }

    /// The current coarse floor (observability / tests).
    pub fn floor(&self) -> u64 {
        self.floor.load(Ordering::Acquire)
    }

    /// The current covered-through watermark (observability / tests).
    pub fn covered_through(&self) -> u64 {
        self.covered_through.load(Ordering::Acquire)
    }

    /// Reset every dimension to `version` — the belt-and-braces coarse invalidation the
    /// remote-change path uses, mirroring `ResultCache::invalidate_all`: after it, no
    /// dependency-scoped entry computed at or before `version` can be revalidated.
    pub fn invalidate_all(&self, version: u64) {
        self.floor.fetch_max(version, Ordering::AcqRel);
        self.all_nodes.fetch_max(version, Ordering::AcqRel);
        self.all_edges.fetch_max(version, Ordering::AcqRel);
        self.covered_through.fetch_max(version, Ordering::AcqRel);
    }

    /// Reset the clock to its brand-new state (every dimension 0, fine map empty). Used when the
    /// graph's WHOLE image is replaced (`replace_snapshot`): the old graph's per-label/per-key
    /// versions are meaningless against the new image and, left in place, would spuriously
    /// invalidate future entries for a long time (an old high `Label(A)` version outranking new
    /// entries until the version counter climbed past it). The result cache's own entries are
    /// cleared alongside, so resetting the clock DOWN is safe — there is nothing to revalidate.
    pub fn reset(&self) {
        self.floor.store(0, Ordering::Release);
        self.all_nodes.store(0, Ordering::Release);
        self.all_edges.store(0, Ordering::Release);
        self.covered_through.store(0, Ordering::Release);
        self.saturated.store(false, Ordering::Release);
        self.fine.lock().clear();
    }

    fn bump_fine(
        fine: &mut HashMap<Dim, u64>,
        saturated: &std::sync::atomic::AtomicBool,
        dim: Dim,
        version: u64,
    ) {
        if !fine.contains_key(&dim) && fine.len() >= MAX_TRACKED_DIMS {
            // Do not grow the fine map past the cap: mark saturated so every subsequent
            // fine-dim read folds in `all_nodes` (which we also bump here), keeping validity
            // sound without unbounded memory.
            saturated.store(true, Ordering::Release);
            return;
        }
        fine.entry(dim)
            .and_modify(|v| *v = (*v).max(version))
            .or_insert(version);
    }
}

// Hash for Dim so it can key the fine map.
impl std::hash::Hash for Dim {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Dim::Label(l) => {
                0u8.hash(state);
                l.hash(state);
            }
            Dim::Key(k) => {
                1u8.hash(state);
                k.hash(state);
            }
            Dim::AllNodes => 2u8.hash(state),
            Dim::AllEdges => 3u8.hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(l: &str) -> Dim {
        Dim::Label(l.to_string())
    }

    #[test]
    fn disjoint_write_leaves_entry_valid() {
        let clock = DepClock::new();
        // A query on label A, computed at version 5.
        let deps = DepSet::new(vec![label("A")]);
        assert!(clock.is_valid(&deps, 5));

        // A footprinted write to label B at version 6 (covers itself, does not floor).
        clock.note_footprint(
            &WriteFootprint {
                labels: vec!["B".into()],
                node_changed: true,
                ..Default::default()
            },
            6,
        );
        clock.note_version_bump(6);

        // The A-query survives the B-write: its dependency set is disjoint.
        assert!(
            clock.is_valid(&deps, 5),
            "disjoint write must not invalidate"
        );
    }

    #[test]
    fn overlapping_write_invalidates_entry() {
        let clock = DepClock::new();
        let deps = DepSet::new(vec![label("A")]);
        clock.note_footprint(
            &WriteFootprint {
                labels: vec!["A".into()],
                node_changed: true,
                ..Default::default()
            },
            6,
        );
        clock.note_version_bump(6);
        assert!(
            !clock.is_valid(&deps, 5),
            "a write to label A must invalidate an A-query"
        );
    }

    #[test]
    fn footprinted_write_does_not_floor() {
        // The core win: a footprinted write advances covered_through so its mark_dirty does not
        // raise the floor, leaving disjoint entries alive.
        let clock = DepClock::new();
        clock.note_footprint(
            &WriteFootprint {
                labels: vec!["A".into()],
                node_changed: true,
                ..Default::default()
            },
            6,
        );
        clock.note_version_bump(6);
        assert_eq!(clock.floor(), 0, "a footprinted write must not floor");
        assert_eq!(clock.covered_through(), 6);
    }

    #[test]
    fn bypass_write_floors_everything() {
        // A write that bumps the version WITHOUT a footprint (a follower's replicated apply)
        // floors the clock, so no dependency-scoped entry can survive it.
        let clock = DepClock::new();
        clock.note_version_bump(7); // no note_footprint(7) preceded it
        assert_eq!(clock.floor(), 7);
        let deps = DepSet::new(vec![label("A")]);
        assert!(
            !clock.is_valid(&deps, 5),
            "a bypass write must invalidate every entry"
        );
    }

    #[test]
    fn all_nodes_scan_sees_every_node_write() {
        let clock = DepClock::new();
        let scan = DepSet::new(vec![Dim::AllNodes]);
        clock.note_footprint(
            &WriteFootprint {
                labels: vec!["B".into()],
                node_changed: true,
                ..Default::default()
            },
            6,
        );
        clock.note_version_bump(6);
        assert!(
            !clock.is_valid(&scan, 5),
            "an unlabeled scan depends on every node write"
        );
    }

    #[test]
    fn edge_write_leaves_node_query_valid() {
        let clock = DepClock::new();
        let node_q = DepSet::new(vec![label("A")]);
        clock.note_footprint(
            &WriteFootprint {
                edge_changed: true,
                ..Default::default()
            },
            6,
        );
        clock.note_version_bump(6);
        assert!(
            clock.is_valid(&node_q, 5),
            "an edge-only write must not invalidate a node query"
        );
    }

    #[test]
    fn uncaptured_remove_floors() {
        // A remove whose properties were not captured cannot be attributed to a label, so it
        // floors — the sound fallback.
        let clock = DepClock::new();
        let deps = DepSet::new(vec![label("A")]);
        clock.note_footprint(
            &WriteFootprint {
                node_changed: true,
                coarse_node: true,
                ..Default::default()
            },
            6,
        );
        clock.note_version_bump(6);
        assert!(
            !clock.is_valid(&deps, 5),
            "an un-attributable remove must invalidate every entry"
        );
    }
}
