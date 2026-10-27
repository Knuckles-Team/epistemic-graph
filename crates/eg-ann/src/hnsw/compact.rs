//! Compact immutable HNSW storage for opt-in no-rebuild serving.
//!
//! [`HnswIndex`](super::HnswIndex) is the mutable build representation.  Each
//! node owns a `Vec` for its vector and one `Vec` for every graph layer, which is
//! convenient while wiring the graph but expensive to keep resident after a
//! generation is activated.  This module keeps the same node order, graph
//! metadata, metric, traversal semantics, and optionally vectors while replacing
//! the per-node allocations
//! with contiguous buffers:
//!
//! * stored vectors, when selected, are one row-major `f32` buffer;
//! * `node_layer_offsets` maps a node to its variable number of layers;
//! * `adjacency_offsets` maps each node/layer pair into one `u32` neighbour
//!   buffer.
//!
//! The representation is immutable.  Building and activation remain owned by
//! the caller of the artifact codec; the default mutable HNSW path is unchanged.
//! The primary saving is resident heap overhead from per-node/per-layer
//! allocations.  The versioned encoded length is measured explicitly and is not
//! presented as a universal disk-compression claim.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};
use std::fmt;
use std::io::{Error, ErrorKind, Result};

use crate::flat::Metric;
use crate::ivfpq::SearchResult;

use super::{HnswIndex, MAX_LEVEL_CAP};

/// The byte width of every compact adjacency entry and offset.
pub(crate) const COMPACT_INDEX_WIDTH: usize = std::mem::size_of::<u32>();

/// The caller's explicit upper bound on provider fetches for one query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchBudget {
    pub max_vector_fetches: usize,
}

/// Accounting returned after a provider-backed query succeeds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SearchStats {
    /// Number of provider calls made while scoring this query.  The budget is
    /// checked before each call, so this never exceeds the requested limit.
    pub fetched_vectors: usize,
}

/// A bounded source of vectors for a vectorless compact graph.
///
/// The provider owns the returned vector.  The ANN engine validates its shape
/// and every coordinate before using it, so a provider cannot bypass the
/// dimension or finite-value checks by claiming an artifact is trusted.
/// This seam supplies embedding vectors only; source-text fetch and exact
/// rescore authorization remain a separate caller-owned operation.
pub trait VectorProvider {
    fn fetch(&mut self, node: usize, external_id: u64) -> std::io::Result<Vec<f32>>;
}

/// Errors from provider-backed compact search.
#[derive(Debug)]
pub enum CompactSearchError {
    QueryDimension {
        expected: usize,
        actual: usize,
    },
    QueryNonFinite {
        coordinate: usize,
    },
    VectorFetchBudgetExceeded {
        max_vector_fetches: usize,
    },
    MissingVector {
        node: usize,
        external_id: u64,
    },
    Provider {
        node: usize,
        external_id: u64,
        source: Error,
    },
    VectorDimension {
        node: usize,
        external_id: u64,
        expected: usize,
        actual: usize,
    },
    VectorNonFinite {
        node: usize,
        external_id: u64,
        coordinate: usize,
    },
}

impl fmt::Display for CompactSearchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryDimension { expected, actual } => {
                write!(formatter, "query dimension {actual} does not equal {expected}")
            }
            Self::QueryNonFinite { coordinate } => {
                write!(formatter, "query coordinate {coordinate} is not finite")
            }
            Self::VectorFetchBudgetExceeded { max_vector_fetches } => {
                write!(formatter, "vector fetch budget {max_vector_fetches} exhausted")
            }
            Self::MissingVector { node, external_id } => {
                write!(formatter, "vector provider has no vector for node {node} (id {external_id})")
            }
            Self::Provider {
                node,
                external_id,
                source,
            } => write!(
                formatter,
                "vector provider failed for node {node} (id {external_id}): {source}"
            ),
            Self::VectorDimension {
                node,
                external_id,
                expected,
                actual,
            } => write!(
                formatter,
                "vector for node {node} (id {external_id}) has dimension {actual}, expected {expected}"
            ),
            Self::VectorNonFinite {
                node,
                external_id,
                coordinate,
            } => write!(
                formatter,
                "vector for node {node} (id {external_id}) has a non-finite coordinate at {coordinate}"
            ),
        }
    }
}

impl std::error::Error for CompactSearchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Provider { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// A read-only HNSW image with contiguous vectors and CSR-style adjacency.
///
/// This is an opt-in serving representation.  It is deliberately separate from
/// [`HnswIndex`](super::HnswIndex), so existing serde and v1 artifact readers
/// keep their wire and search behavior unchanged.
#[derive(Clone, Debug)]
pub struct CompactHnswIndex {
    /// Embedding dimension.
    pub(crate) dim: usize,
    /// Distance metric used by graph traversal and returned results.
    pub(crate) metric: Metric,
    /// HNSW upper-layer degree target.
    pub(crate) m: usize,
    /// HNSW layer-0 degree target.
    pub(crate) m0: usize,
    /// Build-time beam width retained for artifact identity.
    pub(crate) ef_construction: usize,
    /// Deterministic level-assignment seed retained for artifact identity.
    pub(crate) seed: u64,
    /// Highest layer in the graph.
    pub(crate) max_level: usize,
    /// Entry point in the compact node order.
    pub(crate) entry_point: Option<usize>,
    pub(crate) ids: Vec<u64>,
    /// Optional row-major vectors.  `None` is the explicit vectorless format;
    /// callers must use [`Self::search_with`] with a provider for that image.
    pub(crate) vectors: Option<Vec<f32>>,
    /// For node `i`, its layer adjacency-offset range is
    /// `node_layer_offsets[i]..node_layer_offsets[i + 1]`.
    pub(crate) node_layer_offsets: Vec<u32>,
    /// Each adjacent pair at `base + layer` bounds a slice in `neighbors`.
    pub(crate) adjacency_offsets: Vec<u32>,
    /// Flattened neighbour IDs in node/layer order.
    pub(crate) neighbors: Vec<u32>,
}

fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::InvalidData, message)
}

struct ProviderSearchContext<'a, P: VectorProvider> {
    budget: SearchBudget,
    stats: &'a mut SearchStats,
    provider: &'a mut P,
}

impl CompactHnswIndex {
    /// Build the immutable representation from a validated mutable HNSW graph.
    pub fn from_hnsw(index: &HnswIndex) -> Result<Self> {
        Self::from_hnsw_with_vectors(index, true)
    }

    /// Build an immutable graph without embedding storage.  The graph can only
    /// be searched through [`Self::search_with`] and an explicit provider.
    pub fn from_hnsw_without_vectors(index: &HnswIndex) -> Result<Self> {
        Self::from_hnsw_with_vectors(index, false)
    }

    fn from_hnsw_with_vectors(index: &HnswIndex, store_vectors: bool) -> Result<Self> {
        super::artifact::validate(index)?;
        let node_count = index.nodes.len();
        let node_count_u32 = u32::try_from(node_count)
            .map_err(|_| invalid("compact HNSW node count exceeds u32"))?;

        let total_layers: usize = index.nodes.iter().map(|node| node.neighbors.len()).sum();
        let total_edges: usize = index
            .nodes
            .iter()
            .flat_map(|node| node.neighbors.iter())
            .map(Vec::len)
            .sum();

        let vector_len = node_count
            .checked_mul(index.dim)
            .ok_or_else(|| invalid("compact HNSW vector length overflows"))?;
        let stored_vector_len = if store_vectors { vector_len } else { 0 };
        let body_len = node_count
            .checked_mul(std::mem::size_of::<u64>())
            .and_then(|size| {
                size.checked_add(stored_vector_len.checked_mul(std::mem::size_of::<f32>())?)
            })
            .and_then(|size| size.checked_add((node_count + 1).checked_mul(COMPACT_INDEX_WIDTH)?))
            .and_then(|size| size.checked_add((total_layers + 1).checked_mul(COMPACT_INDEX_WIDTH)?))
            .and_then(|size| size.checked_add(total_edges.checked_mul(COMPACT_INDEX_WIDTH)?))
            .ok_or_else(|| invalid("compact HNSW artifact length overflows"))?;
        if body_len > super::artifact::MAX_ARTIFACT_BYTES - super::artifact::COMPACT_HEADER_BYTES {
            return Err(invalid("compact HNSW artifact exceeds byte bound"));
        }

        let mut ids = Vec::new();
        ids.try_reserve_exact(node_count)
            .map_err(|_| invalid("compact HNSW id allocation failed"))?;
        let mut vectors = store_vectors.then(Vec::new);
        if let Some(vectors) = vectors.as_mut() {
            vectors
                .try_reserve_exact(vector_len)
                .map_err(|_| invalid("compact HNSW vector allocation failed"))?;
        }

        let mut node_layer_offsets = Vec::with_capacity(node_count + 1);
        let mut adjacency_offsets = Vec::new();
        adjacency_offsets
            .try_reserve_exact(total_layers + 1)
            .map_err(|_| invalid("compact HNSW offset allocation failed"))?;
        let mut neighbors = Vec::new();
        neighbors
            .try_reserve_exact(total_edges)
            .map_err(|_| invalid("compact HNSW adjacency allocation failed"))?;

        node_layer_offsets.push(0);
        adjacency_offsets.push(0);
        for node in &index.nodes {
            ids.push(node.id);
            if let Some(vectors) = vectors.as_mut() {
                vectors.extend_from_slice(&node.vector);
            }
            for layer in &node.neighbors {
                for &neighbor in layer {
                    neighbors.push(
                        u32::try_from(neighbor)
                            .map_err(|_| invalid("compact HNSW edge exceeds u32"))?,
                    );
                }
                let offset = u32::try_from(neighbors.len())
                    .map_err(|_| invalid("compact HNSW adjacency exceeds u32"))?;
                adjacency_offsets.push(offset);
            }
            let layer_offset = u32::try_from(adjacency_offsets.len() - 1)
                .map_err(|_| invalid("compact HNSW layer offsets exceed u32"))?;
            node_layer_offsets.push(layer_offset);
        }

        let entry_point = index.entry_point;
        let compact = Self {
            dim: index.dim,
            metric: index.metric,
            m: index.m,
            m0: index.m0,
            ef_construction: index.ef_construction,
            seed: index.seed,
            max_level: index.max_level,
            entry_point,
            ids,
            vectors,
            node_layer_offsets,
            adjacency_offsets,
            neighbors,
        };
        compact.validate()?;
        // Keep the conversion's bound explicit even though `validate` checks it;
        // this guards this constructor if the limits change independently.
        if node_count_u32 as usize != compact.ids.len() {
            return Err(invalid("compact HNSW node count changed during conversion"));
        }
        Ok(compact)
    }

    /// Whether this image carries its own vectors.
    pub fn has_vectors(&self) -> bool {
        self.vectors.is_some()
    }

    /// Number of nodes represented by this image.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// Embedding dimension represented by this image.
    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Metric used by compact graph traversal.
    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// Whether this image has no nodes.
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// Number of flattened adjacency entries.
    pub fn edge_count(&self) -> usize {
        self.neighbors.len()
    }

    /// External ID at compact node position `node`.
    pub fn id(&self, node: usize) -> Option<u64> {
        self.ids.get(node).copied()
    }

    /// Heap-byte footprint of the compact image, including buffer capacity.
    ///
    /// This is an accounting value for the in-memory image.  It is intentionally
    /// separate from the encoded artifact length, which is reported by
    /// [`Self::encoded_size`].
    pub fn byte_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.ids.capacity() * std::mem::size_of::<u64>()
            + self
                .vectors
                .as_ref()
                .map_or(0, |vectors| vectors.capacity() * std::mem::size_of::<f32>())
            + self.node_layer_offsets.capacity() * COMPACT_INDEX_WIDTH
            + self.adjacency_offsets.capacity() * COMPACT_INDEX_WIDTH
            + self.neighbors.capacity() * COMPACT_INDEX_WIDTH
    }

    /// Encoded compact-artifact length, when the size arithmetic is representable.
    pub fn encoded_size(&self) -> Option<usize> {
        let body = self
            .ids
            .len()
            .checked_mul(std::mem::size_of::<u64>())?
            .checked_add(self.vectors.as_ref().map_or(Some(0), |vectors| {
                vectors.len().checked_mul(std::mem::size_of::<f32>())
            })?)?
            .checked_add(
                self.node_layer_offsets
                    .len()
                    .checked_mul(COMPACT_INDEX_WIDTH)?,
            )?
            .checked_add(
                self.adjacency_offsets
                    .len()
                    .checked_mul(COMPACT_INDEX_WIDTH)?,
            )?
            .checked_add(self.neighbors.len().checked_mul(COMPACT_INDEX_WIDTH)?)?;
        super::artifact::COMPACT_HEADER_BYTES.checked_add(body)
    }

    /// Return a contiguous layer adjacency slice for one compact node.
    #[inline]
    pub(crate) fn layer_neighbors(&self, node: usize, layer: usize) -> &[u32] {
        let layer_base = self.node_layer_offsets[node] as usize;
        let start = self.adjacency_offsets[layer_base + layer] as usize;
        let end = self.adjacency_offsets[layer_base + layer + 1] as usize;
        &self.neighbors[start..end]
    }

    #[inline]
    fn layer_count(&self, node: usize) -> usize {
        (self.node_layer_offsets[node + 1] - self.node_layer_offsets[node]) as usize
    }

    #[inline]
    fn vector(&self, node: usize) -> &[f32] {
        let vectors = self
            .vectors
            .as_ref()
            .expect("vectorless compact HNSW requires search_with");
        let start = node * self.dim;
        &vectors[start..start + self.dim]
    }

    #[inline]
    fn distance(&self, query: &[f32], node: usize) -> f32 {
        self.metric.distance(query, self.vector(node))
    }

    /// Search the compact graph with the same metric, beam, and deterministic
    /// ordering as [`HnswIndex::search`](super::HnswIndex::search).
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<SearchResult> {
        assert!(
            self.vectors.is_some(),
            "vectorless compact HNSW requires search_with"
        );
        assert_eq!(query.len(), self.dim, "query length must equal dim");
        if k == 0 || self.ids.is_empty() {
            return Vec::new();
        }
        let Some(mut entry) = self.entry_point else {
            return Vec::new();
        };
        for layer in (1..=self.max_level).rev() {
            entry = self.greedy_closest(query, entry, layer);
        }
        // The graph is the only source of candidates, so cap caller-provided
        // beam and result sizes before any search-side allocation. This also
        // makes usize::MAX inputs harmless for the stored-vector path.
        let ef = ef.max(k).min(self.ids.len());
        let mut found = self.search_layer(query, entry, ef, 0);
        found.sort_unstable();
        found.truncate(k);
        found
            .into_iter()
            .map(|candidate| SearchResult {
                id: candidate.id,
                distance: candidate.dist,
            })
            .collect()
    }

    fn greedy_closest(&self, query: &[f32], entry: usize, layer: usize) -> usize {
        let mut best = entry;
        let mut best_distance = self.distance(query, best);
        let mut best_id = self.ids[best];
        loop {
            let mut improved = false;
            for &neighbor in self.layer_neighbors(best, layer) {
                let neighbor = neighbor as usize;
                let distance = self.distance(query, neighbor);
                let neighbor_id = self.ids[neighbor];
                if distance < best_distance || (distance == best_distance && neighbor_id < best_id)
                {
                    best = neighbor;
                    best_distance = distance;
                    best_id = neighbor_id;
                    improved = true;
                }
            }
            if !improved {
                return best;
            }
        }
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry: usize,
        ef: usize,
        layer: usize,
    ) -> Vec<CompactCandidate> {
        let capacity = ef.saturating_mul(4).min(self.ids.len());
        let mut visited = HashSet::with_capacity(capacity);
        let mut candidates = BinaryHeap::<Reverse<CompactCandidate>>::new();
        let mut kept = BinaryHeap::<CompactCandidate>::new();
        let first = CompactCandidate {
            dist: self.distance(query, entry),
            node: entry,
            id: self.ids[entry],
        };
        visited.insert(entry);
        candidates.push(Reverse(first));
        kept.push(first);

        while let Some(Reverse(candidate)) = candidates.pop() {
            if kept.len() >= ef && kept.peek().is_some_and(|worst| candidate.dist > worst.dist) {
                break;
            }
            for &neighbor in self.layer_neighbors(candidate.node, layer) {
                let neighbor = neighbor as usize;
                if !visited.insert(neighbor) {
                    continue;
                }
                let next = CompactCandidate {
                    dist: self.distance(query, neighbor),
                    node: neighbor,
                    id: self.ids[neighbor],
                };
                if kept.len() < ef || kept.peek().is_none_or(|worst| next < *worst) {
                    candidates.push(Reverse(next));
                    kept.push(next);
                    if kept.len() > ef {
                        kept.pop();
                    }
                }
            }
        }
        kept.into_vec()
    }

    /// Search an image using vectors fetched on demand from an external
    /// provider.  This is the required query path for vectorless artifacts;
    /// every candidate distance consumes one explicitly budgeted fetch. HNSW
    /// can score the same node once per traversed layer, so `fetched_vectors`
    /// reports provider calls rather than distinct external IDs.
    pub fn search_with<P: VectorProvider>(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        budget: SearchBudget,
        provider: &mut P,
    ) -> std::result::Result<(Vec<SearchResult>, SearchStats), CompactSearchError> {
        if query.len() != self.dim {
            return Err(CompactSearchError::QueryDimension {
                expected: self.dim,
                actual: query.len(),
            });
        }
        if let Some(coordinate) = query.iter().position(|value| !value.is_finite()) {
            return Err(CompactSearchError::QueryNonFinite { coordinate });
        }
        let mut stats = SearchStats::default();
        if k == 0 || self.ids.is_empty() {
            return Ok((Vec::new(), stats));
        }
        let Some(mut entry) = self.entry_point else {
            return Ok((Vec::new(), stats));
        };
        for layer in (1..=self.max_level).rev() {
            entry = self.greedy_closest_with(query, entry, layer, budget, &mut stats, provider)?;
        }
        // Bound both traversal work and the initial visited allocation by the
        // immutable graph. Provider fetches are bounded separately below.
        let ef = ef.max(k).min(self.ids.len());
        let mut context = ProviderSearchContext {
            budget,
            stats: &mut stats,
            provider,
        };
        let mut found = self.search_layer_with(query, entry, ef, 0, &mut context)?;
        found.sort_unstable();
        found.truncate(k);
        let results = found
            .into_iter()
            .map(|candidate| SearchResult {
                id: candidate.id,
                distance: candidate.dist,
            })
            .collect();
        Ok((results, stats))
    }

    fn fetch_distance<P: VectorProvider>(
        &self,
        query: &[f32],
        node: usize,
        budget: SearchBudget,
        stats: &mut SearchStats,
        provider: &mut P,
    ) -> std::result::Result<f32, CompactSearchError> {
        if stats.fetched_vectors >= budget.max_vector_fetches {
            return Err(CompactSearchError::VectorFetchBudgetExceeded {
                max_vector_fetches: budget.max_vector_fetches,
            });
        }
        // Count before calling the provider so a failing provider cannot be
        // retried to exceed the explicit query budget.
        stats.fetched_vectors += 1;
        let external_id = self.ids[node];
        let vector = provider
            .fetch(node, external_id)
            .map_err(|source| match source.kind() {
                ErrorKind::NotFound => CompactSearchError::MissingVector { node, external_id },
                _ => CompactSearchError::Provider {
                    node,
                    external_id,
                    source,
                },
            })?;
        if vector.len() != self.dim {
            return Err(CompactSearchError::VectorDimension {
                node,
                external_id,
                expected: self.dim,
                actual: vector.len(),
            });
        }
        if let Some(coordinate) = vector.iter().position(|value| !value.is_finite()) {
            return Err(CompactSearchError::VectorNonFinite {
                node,
                external_id,
                coordinate,
            });
        }
        Ok(self.metric.distance(query, &vector))
    }

    fn greedy_closest_with<P: VectorProvider>(
        &self,
        query: &[f32],
        entry: usize,
        layer: usize,
        budget: SearchBudget,
        stats: &mut SearchStats,
        provider: &mut P,
    ) -> std::result::Result<usize, CompactSearchError> {
        let mut best = entry;
        let mut best_distance = self.fetch_distance(query, best, budget, stats, provider)?;
        let mut best_id = self.ids[best];
        loop {
            let mut improved = false;
            for &neighbor in self.layer_neighbors(best, layer) {
                let neighbor = neighbor as usize;
                let distance = self.fetch_distance(query, neighbor, budget, stats, provider)?;
                let neighbor_id = self.ids[neighbor];
                if distance < best_distance || (distance == best_distance && neighbor_id < best_id)
                {
                    best = neighbor;
                    best_distance = distance;
                    best_id = neighbor_id;
                    improved = true;
                }
            }
            if !improved {
                return Ok(best);
            }
        }
    }

    fn search_layer_with<P: VectorProvider>(
        &self,
        query: &[f32],
        entry: usize,
        ef: usize,
        layer: usize,
        context: &mut ProviderSearchContext<'_, P>,
    ) -> std::result::Result<Vec<CompactCandidate>, CompactSearchError> {
        let capacity = ef
            .saturating_mul(4)
            .min(self.ids.len())
            .min(context.budget.max_vector_fetches);
        let mut visited = HashSet::with_capacity(capacity);
        let mut candidates = BinaryHeap::<Reverse<CompactCandidate>>::new();
        let mut kept = BinaryHeap::<CompactCandidate>::new();
        let first = CompactCandidate {
            dist: self.fetch_distance(
                query,
                entry,
                context.budget,
                &mut *context.stats,
                &mut *context.provider,
            )?,
            node: entry,
            id: self.ids[entry],
        };
        visited.insert(entry);
        candidates.push(Reverse(first));
        kept.push(first);

        while let Some(Reverse(candidate)) = candidates.pop() {
            if kept.len() >= ef && kept.peek().is_some_and(|worst| candidate.dist > worst.dist) {
                break;
            }
            for &neighbor in self.layer_neighbors(candidate.node, layer) {
                let neighbor = neighbor as usize;
                if !visited.insert(neighbor) {
                    continue;
                }
                let next = CompactCandidate {
                    dist: self.fetch_distance(
                        query,
                        neighbor,
                        context.budget,
                        &mut *context.stats,
                        &mut *context.provider,
                    )?,
                    node: neighbor,
                    id: self.ids[neighbor],
                };
                if kept.len() < ef || kept.peek().is_none_or(|worst| next < *worst) {
                    candidates.push(Reverse(next));
                    kept.push(next);
                    if kept.len() > ef {
                        kept.pop();
                    }
                }
            }
        }
        Ok(kept.into_vec())
    }

    /// Validate all compact shape, edge, and numeric invariants before serving.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.dim == 0
            || self.dim > super::artifact::MAX_DIM
            || !(2..=super::artifact::MAX_M).contains(&self.m)
            || self.m0 != self.m * 2
            || !(self.m..=super::artifact::MAX_EF_CONSTRUCTION).contains(&self.ef_construction)
            || self.max_level > MAX_LEVEL_CAP
            || self.ids.len() > super::artifact::MAX_NODES
        {
            return Err(invalid("compact HNSW parameters exceed bounds"));
        }

        let node_count = self.ids.len();
        let expected_vectors = node_count
            .checked_mul(self.dim)
            .ok_or_else(|| invalid("compact HNSW vector length overflows"))?;
        if self
            .vectors
            .as_ref()
            .is_some_and(|vectors| vectors.len() != expected_vectors)
            || self.node_layer_offsets.len() != node_count + 1
            || self.node_layer_offsets.first().copied() != Some(0)
            || self.adjacency_offsets.is_empty()
            || self.adjacency_offsets.first().copied() != Some(0)
            || self.adjacency_offsets.last().copied().map(|v| v as usize)
                != Some(self.neighbors.len())
        {
            return Err(invalid("compact HNSW buffers have inconsistent lengths"));
        }
        if node_count == 0 {
            if self.entry_point.is_some()
                || self.max_level != 0
                || self.node_layer_offsets != [0]
                || self.adjacency_offsets != [0]
            {
                return Err(invalid("empty compact HNSW image has graph state"));
            }
            return Ok(());
        }

        if self
            .node_layer_offsets
            .windows(2)
            .any(|window| window[1] < window[0])
            || self
                .adjacency_offsets
                .windows(2)
                .any(|window| window[1] < window[0])
            || self
                .adjacency_offsets
                .iter()
                .any(|&offset| offset as usize > self.neighbors.len())
        {
            return Err(invalid("compact HNSW offsets are not monotonic"));
        }

        let mut ids = HashSet::with_capacity(node_count);
        if self.ids.iter().any(|id| !ids.insert(*id)) {
            return Err(invalid("compact HNSW graph has duplicate external ids"));
        }

        let Some(entry) = self.entry_point else {
            return Err(invalid("nonempty compact HNSW image has no entry point"));
        };
        if entry >= node_count {
            return Err(invalid("compact HNSW entry point is out of range"));
        }

        let mut highest = 0usize;
        for node in 0..node_count {
            let node_start = self.node_layer_offsets[node] as usize;
            let node_end = self.node_layer_offsets[node + 1] as usize;
            if node_end < node_start
                || node_end > self.adjacency_offsets.len() - 1
                || node_end - node_start == 0
                || node_end - node_start > MAX_LEVEL_CAP + 1
            {
                return Err(invalid("compact HNSW node layer offsets are invalid"));
            }
            highest = highest.max(node_end - node_start - 1);
            if let Some(vectors) = self.vectors.as_ref() {
                let start = node * self.dim;
                let vector = &vectors[start..start + self.dim];
                if vector.iter().any(|value| {
                    !value.is_finite() || value.abs() > super::artifact::MAX_COORDINATE
                }) {
                    return Err(invalid("compact HNSW vector has invalid coordinates"));
                }
            }
            for layer in 0..(node_end - node_start) {
                let start = self.adjacency_offsets[node_start + layer] as usize;
                let end = self.adjacency_offsets[node_start + layer + 1] as usize;
                let cap = if layer == 0 { self.m0 } else { self.m };
                if start > end || end > self.neighbors.len() || end - start > cap {
                    return Err(invalid("compact HNSW adjacency offsets are invalid"));
                }
                let mut seen = HashSet::with_capacity(end - start);
                for &neighbor in &self.neighbors[start..end] {
                    let neighbor = neighbor as usize;
                    if neighbor >= node_count
                        || neighbor == node
                        || self.layer_count(neighbor) <= layer
                        || !seen.insert(neighbor)
                    {
                        return Err(invalid("compact HNSW adjacency has an invalid edge"));
                    }
                }
            }
        }
        if highest != self.max_level || self.layer_count(entry) - 1 != highest {
            return Err(invalid("compact HNSW entry point does not own top layer"));
        }
        Ok(())
    }
}

/// A scored compact node.  Ordering matches the mutable HNSW candidate order:
/// nearest distance first after heap reversal, then ascending external ID and
/// internal position for deterministic ties.
#[derive(Clone, Copy, Debug)]
struct CompactCandidate {
    dist: f32,
    node: usize,
    id: u64,
}

impl PartialEq for CompactCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.dist.to_bits() == other.dist.to_bits()
            && self.id == other.id
            && self.node == other.node
    }
}

impl Eq for CompactCandidate {}

impl Ord for CompactCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.dist.is_nan(), other.dist.is_nan()) {
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (true, true) => self.dist.to_bits().cmp(&other.dist.to_bits()),
            (false, false) => self.dist.total_cmp(&other.dist),
        }
        .then_with(|| self.id.cmp(&other.id))
        .then_with(|| self.node.cmp(&other.node))
    }
}

impl PartialOrd for CompactCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
