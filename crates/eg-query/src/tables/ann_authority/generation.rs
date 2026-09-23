//! One immutable ANN generation: an eg-ann graph over one source snapshot.
//!
//! `hnsw` builds an [`HnswIndex`] in the registration's own metric. `ivfflat`
//! builds an [`IvfPq`], whose coarse cells and PQ codes are Euclidean; cosine
//! and inner product are therefore mapped into a space where Euclidean order IS
//! the requested order (unit-normalised vectors for cosine, the standard
//! max-norm augmentation for inner product), so the IVF candidate order is
//! faithful to the metric the query asked for. Every candidate is re-scored
//! exactly on its current vector by the probe either way.
//!
//! A generation is either FULL (built from one snapshot of every row) or an
//! EXTENSION of a full base: the base graph plus the current vectors of every
//! row changed since, folded in by the maintenance worker instead of a rebuild.
//! An extension indexes a changed row under its new vector and keeps the stale
//! entry, which is harmless: every candidate is re-read and re-scored.

use std::collections::BTreeSet;

use eg_ann::{HnswIndex, IvfPq, IvfPqParams, Metric, SearchParams};
use serde::{Deserialize, Serialize};

use crate::sql::{isqrt, metric_to_ann, AnnMethod, VectorMetric};
use crate::tables::store::{AnnChangedRows, AnnSourceRows};

/// Deterministic seed for every maintained build, so two builds over the same
/// snapshot are identical.
const BUILD_SEED: u64 = 0x00E6_0019;
/// HNSW fan-out (`M`) and construction beam.
const HNSW_M: usize = 16;
const HNSW_EF_CONSTRUCTION: usize = 200;
/// Lloyd iterations for the IVF coarse + PQ k-means.
const IVF_KMEANS_ITERS: usize = 15;
/// k-means trains on at most this many vectors (an even stride of the rows).
const IVF_TRAINING_ROWS: usize = 65_536;
/// Cells probed exhaustively while the index has at most this many.
const IVF_EXHAUSTIVE_CELLS: usize = 64;
/// SQ8 refine over-fetch inside one IVF probe.
const IVF_REFINE_FACTOR: usize = 4;

/// One built generation. Immutable once activated.
pub(crate) struct AnnGeneration {
    pub(super) generation: u64,
    pub(super) method: AnnMethod,
    /// The vector width, when the snapshot held any vector.
    pub(super) dim: Option<usize>,
    /// The tenant SQL source epoch the snapshot observed.
    pub(super) built_epoch: u64,
    /// Highest row id in the snapshot; rows above it are scored exactly.
    pub(super) max_rowid: Option<u64>,
    /// Rows this generation indexes.
    pub(super) rows: usize,
    /// The full generation this one extends; `None` for a full generation.
    pub(super) base: Option<u64>,
    /// Row ids folded in on top of `base`, ascending.
    pub(super) delta: Vec<u64>,
    pub(super) graph: AnnGraph,
}

pub(super) enum AnnGraph {
    Empty,
    Hnsw(HnswIndex),
    Ivf {
        index: Box<IvfPq>,
        space: IvfSpace,
        nprobe: usize,
    },
}

/// The Euclidean space an IVF generation is trained in.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub(super) enum IvfSpace {
    /// L2 as-is.
    Euclidean,
    /// Cosine: unit-normalised rows and queries.
    UnitSphere,
    /// Inner product: rows gain `sqrt(max_norm² − |x|²)`, queries gain `0`, so
    /// `|q − x|² = |q|² + max_norm² − 2·q·x` ranks exactly by `−q·x`.
    MaxNormAugmented { max_norm_sq: f32 },
}

impl AnnGeneration {
    /// Build generation number `generation` from one source snapshot.
    pub(super) fn build(
        generation: u64,
        method: AnnMethod,
        metric: VectorMetric,
        source: AnnSourceRows,
    ) -> Self {
        let graph = match source.dim {
            Some(dim) if !source.rows.is_empty() => {
                build_graph(method, metric_to_ann(metric), dim, &source.rows)
            }
            _ => AnnGraph::Empty,
        };
        Self {
            generation,
            method,
            dim: source.dim,
            built_epoch: source.epoch,
            max_rowid: source.max_rowid,
            rows: source.rows.len(),
            base: None,
            delta: Vec::new(),
            graph,
        }
    }

    /// Generation `generation`: this one with `changed` folded in, as of the
    /// changed rows' snapshot. `None` when this generation cannot be extended
    /// (it indexes nothing yet, so it has no width or trained space).
    pub(super) fn extend(&self, generation: u64, changed: &AnnChangedRows) -> Option<Self> {
        let dim = self.dim?;
        let admitted: Vec<(u64, Vec<f32>)> = changed
            .rows
            .iter()
            .filter_map(|(rowid, vector)| {
                vector
                    .as_ref()
                    .filter(|vector| vector.len() == dim)
                    .map(|vector| (*rowid, vector.clone()))
            })
            .collect();
        let changed_ids: BTreeSet<u64> = changed.rows.iter().map(|(rowid, _)| *rowid).collect();
        let graph = self.graph.extended(&changed_ids, &admitted)?;
        let mut delta: BTreeSet<u64> = self.delta.iter().copied().collect();
        delta.extend(changed_ids.iter().copied());
        Some(Self {
            generation,
            method: self.method,
            dim: self.dim,
            built_epoch: changed.epoch,
            max_rowid: self.max_rowid.max(changed_ids.last().copied()),
            rows: self.rows + admitted.len(),
            base: Some(self.base.unwrap_or(self.generation)),
            delta: delta.into_iter().collect(),
            graph,
        })
    }

    /// Row ids folded in since the base, counting repeats as one.
    pub(super) fn delta_len(&self) -> usize {
        self.delta.len()
    }

    /// Up to `pool` candidate row ids nearest `query` whose rows `allow` admits.
    /// `allow` runs INSIDE the walk, so a hidden row never occupies the pool.
    pub(super) fn candidates(
        &self,
        query: &[f32],
        pool: usize,
        allow: &dyn Fn(u64) -> bool,
    ) -> Vec<u64> {
        let found = match &self.graph {
            AnnGraph::Empty => Vec::new(),
            AnnGraph::Hnsw(index) => index.search_filtered(query, pool, pool, Some(allow)),
            AnnGraph::Ivf {
                index,
                space,
                nprobe,
            } => index.search_filtered(
                &space.query(query),
                pool,
                SearchParams {
                    nprobe: *nprobe,
                    refine: true,
                    refine_factor: IVF_REFINE_FACTOR,
                },
                Some(allow),
            ),
        };
        found.into_iter().map(|hit| hit.id).collect()
    }
}

impl AnnGraph {
    /// A copy of this graph with every row of `changed` re-indexed: an IVF row
    /// is tombstoned and re-added, an HNSW row gains a node for its new vector.
    fn extended(&self, changed: &BTreeSet<u64>, rows: &[(u64, Vec<f32>)]) -> Option<Self> {
        match self {
            Self::Empty => None,
            Self::Hnsw(index) => {
                let mut index = index.clone();
                index.insert_batch(rows);
                Some(Self::Hnsw(index))
            }
            Self::Ivf {
                index,
                space,
                nprobe,
            } => {
                let mut copy = copy_ivf(index)?;
                tombstone(&mut copy, changed);
                let mapped: Vec<(u64, Vec<f32>)> = rows
                    .iter()
                    .map(|(rowid, vector)| (*rowid, space.row(vector)))
                    .collect();
                copy.add(&mapped);
                Some(Self::Ivf {
                    index: Box::new(copy),
                    space: *space,
                    nprobe: *nprobe,
                })
            }
        }
    }
}

/// An independent copy of a trained IVF index, through its validated durable
/// codes (no retraining).
pub(super) fn copy_ivf(index: &IvfPq) -> Option<IvfPq> {
    let artifact = eg_ann::durable_codes::encode(index).ok()?;
    eg_ann::durable_codes::decode(&artifact).ok()
}

/// Tombstone every live row of `index` whose id is in `ids`, in one pass.
fn tombstone(index: &mut IvfPq, ids: &BTreeSet<u64>) {
    for (row, id) in index.ids.iter().enumerate() {
        if ids.contains(id) {
            index.deleted[row] = 1;
        }
    }
}

fn build_graph(
    method: AnnMethod,
    metric: Metric,
    dim: usize,
    rows: &[(u64, Vec<f32>)],
) -> AnnGraph {
    match method {
        AnnMethod::Hnsw => {
            let mut index = HnswIndex::new(dim, metric, HNSW_M, HNSW_EF_CONSTRUCTION, BUILD_SEED);
            index.insert_batch(rows);
            AnnGraph::Hnsw(index)
        }
        AnnMethod::IvfFlat => build_ivf(IvfSpace::for_metric(metric, rows), dim, rows),
    }
}

fn build_ivf(space: IvfSpace, dim: usize, rows: &[(u64, Vec<f32>)]) -> AnnGraph {
    let items: Vec<(u64, Vec<f32>)> = rows
        .iter()
        .map(|(rowid, vector)| (*rowid, space.row(vector)))
        .collect();
    let stride = items.len().div_ceil(IVF_TRAINING_ROWS).max(1);
    let training: Vec<Vec<f32>> = items
        .iter()
        .step_by(stride)
        .map(|(_, vector)| vector.clone())
        .collect();
    let nlist = isqrt(items.len()).clamp(1, items.len());
    let params = IvfPqParams {
        dim: dim + space.extra_dims(),
        nlist,
        // One subquantizer divides every width; the probe re-scores exactly.
        m: 1,
        kmeans_iters: IVF_KMEANS_ITERS,
        opq_iters: 0,
        seed: BUILD_SEED,
    };
    let mut index = IvfPq::train(&params, &training);
    index.add(&items);
    AnnGraph::Ivf {
        index: Box::new(index),
        space,
        nprobe: probe_cells(nlist),
    }
}

/// Every cell of a small index; a quarter of a large one (never fewer than
/// the small-index ceiling).
fn probe_cells(nlist: usize) -> usize {
    if nlist <= IVF_EXHAUSTIVE_CELLS {
        return nlist;
    }
    (nlist / 4).max(IVF_EXHAUSTIVE_CELLS)
}

impl IvfSpace {
    fn for_metric(metric: Metric, rows: &[(u64, Vec<f32>)]) -> Self {
        match metric {
            Metric::L2 => Self::Euclidean,
            Metric::Cosine => Self::UnitSphere,
            Metric::InnerProduct => Self::MaxNormAugmented {
                max_norm_sq: rows
                    .iter()
                    .map(|(_, vector)| norm_sq(vector))
                    .fold(0.0, f32::max),
            },
        }
    }

    fn extra_dims(self) -> usize {
        match self {
            Self::Euclidean | Self::UnitSphere => 0,
            Self::MaxNormAugmented { .. } => 1,
        }
    }

    fn row(self, vector: &[f32]) -> Vec<f32> {
        match self {
            Self::Euclidean => vector.to_vec(),
            Self::UnitSphere => unit(vector),
            Self::MaxNormAugmented { max_norm_sq } => {
                let mut out = vector.to_vec();
                out.push((max_norm_sq - norm_sq(vector)).max(0.0).sqrt());
                out
            }
        }
    }

    fn query(self, query: &[f32]) -> Vec<f32> {
        match self {
            Self::Euclidean => query.to_vec(),
            Self::UnitSphere => unit(query),
            Self::MaxNormAugmented { .. } => {
                let mut out = query.to_vec();
                out.push(0.0);
                out
            }
        }
    }
}

fn norm_sq(vector: &[f32]) -> f32 {
    vector.iter().map(|x| x * x).sum()
}

/// `vector / |vector|`; the zero vector stays zero.
fn unit(vector: &[f32]) -> Vec<f32> {
    let norm = norm_sq(vector).sqrt();
    if norm == 0.0 {
        return vector.to_vec();
    }
    vector.iter().map(|x| x / norm).collect()
}
