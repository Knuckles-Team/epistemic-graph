//! One immutable ANN generation: an eg-ann graph over one source snapshot.
//!
//! `hnsw` builds an [`HnswIndex`] in the registration's own metric. `ivfflat`
//! builds an [`IvfPq`], whose coarse cells and PQ codes are Euclidean; cosine
//! and inner product are therefore mapped into a space where Euclidean order IS
//! the requested order (unit-normalised vectors for cosine, the standard
//! max-norm augmentation for inner product), so the IVF candidate order is
//! faithful to the metric the query asked for. Every candidate is re-scored
//! exactly on its current vector by the probe either way.

use eg_ann::{HnswIndex, IvfPq, IvfPqParams, Metric, SearchParams};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::sql::{isqrt, metric_to_ann, AnnMethod, VectorMetric};
use crate::tables::store::AnnSourceRows;

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
    pub(super) metric: VectorMetric,
    /// The vector width, when the snapshot held any vector.
    pub(super) dim: Option<usize>,
    /// The tenant SQL source epoch the snapshot observed.
    pub(super) built_epoch: u64,
    /// Highest row id in the snapshot; rows above it are scored exactly.
    pub(super) max_rowid: Option<u64>,
    /// Rows this generation indexes.
    pub(super) rows: usize,
    graph: AnnGraph,
}

#[derive(Clone, Copy)]
pub(crate) struct GenerationMetadata {
    pub(crate) generation: u64,
    pub(crate) method: AnnMethod,
    pub(crate) metric: VectorMetric,
    pub(crate) dim: Option<usize>,
    pub(crate) built_epoch: u64,
    pub(crate) max_rowid: Option<u64>,
    pub(crate) rows: usize,
}

enum AnnGraph {
    Empty,
    Hnsw(HnswIndex),
    Ivf {
        index: Box<IvfPq>,
        space: IvfSpace,
        nprobe: usize,
    },
}

/// The Euclidean space an IVF generation is trained in.
#[derive(Clone, Copy, Serialize, Deserialize)]
enum IvfSpace {
    /// L2 as-is.
    Euclidean,
    /// Cosine: unit-normalised rows and queries.
    UnitSphere,
    /// Inner product: rows gain `sqrt(max_norm² − |x|²)`, queries gain `0`, so
    /// `|q − x|² = |q|² + max_norm² − 2·q·x` ranks exactly by `−q·x`.
    MaxNormAugmented { max_norm_sq: f32 },
}

impl AnnGeneration {
    pub(crate) fn metadata(&self) -> GenerationMetadata {
        GenerationMetadata {
            generation: self.generation,
            method: self.method,
            metric: self.metric,
            dim: self.dim,
            built_epoch: self.built_epoch,
            max_rowid: self.max_rowid,
            rows: self.rows,
        }
    }
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
            metric,
            dim: source.dim,
            built_epoch: source.epoch,
            max_rowid: source.max_rowid,
            rows: source.rows.len(),
            graph,
        }
    }

    /// Exact graph bytes for one immutable generation. The SQL owner stores
    /// these under part keys and publishes its live pointer in the same write.
    pub(crate) fn artifact_parts(&self) -> Result<Vec<(&'static str, Vec<u8>)>, String> {
        match &self.graph {
            AnnGraph::Empty => Ok(vec![("empty", Vec::new())]),
            AnnGraph::Hnsw(index) => Ok(vec![(
                "hnsw",
                eg_ann::hnsw_artifact::encode(index).map_err(|error| error.to_string())?,
            )]),
            AnnGraph::Ivf { index, space, .. } => {
                let artifact =
                    eg_ann::durable_codes::encode(index).map_err(|error| error.to_string())?;
                Ok(vec![
                    ("ivf_meta", artifact.meta),
                    ("ivf_codes", artifact.codes),
                    ("ivf_refine", artifact.refine),
                    (
                        "ivf_space",
                        rmp_serde::to_vec_named(space).map_err(|error| error.to_string())?,
                    ),
                ])
            }
        }
    }

    /// Reopen only a fully verified artifact for the current registration.
    /// The SQL owner verifies identity, schema, epochs, part hashes and limits
    /// before calling this graph-level decoder.
    pub(crate) fn from_artifact_parts(
        generation: u64,
        method: AnnMethod,
        metric: VectorMetric,
        dim: Option<usize>,
        built_epoch: u64,
        max_rowid: Option<u64>,
        rows: usize,
        parts: &BTreeMap<String, Vec<u8>>,
    ) -> Result<Self, String> {
        if generation == 0
            || rows > 500_000
            || dim.is_some_and(|width| width == 0 || width > 8_192)
            || (rows > 0 && max_rowid.is_none())
        {
            return Err("ANN generation metadata is outside its bounds".to_string());
        }
        let graph = if rows == 0 {
            if parts.len() != 1 || parts.get("empty").is_none_or(|bytes| !bytes.is_empty()) {
                return Err("empty ANN generation has unexpected parts".to_string());
            }
            AnnGraph::Empty
        } else {
            let width = dim.ok_or_else(|| "ANN generation is missing a dimension".to_string())?;
            match method {
                AnnMethod::Hnsw => {
                    if parts.len() != 1 {
                        return Err("HNSW generation has unexpected parts".to_string());
                    }
                    let bytes = parts
                        .get("hnsw")
                        .ok_or_else(|| "HNSW generation is missing graph bytes".to_string())?;
                    let index =
                        eg_ann::hnsw_artifact::decode(bytes).map_err(|error| error.to_string())?;
                    if index.len() != rows
                        || index.dim != width
                        || index.metric != metric_to_ann(metric)
                    {
                        return Err("HNSW generation does not match its manifest".to_string());
                    }
                    AnnGraph::Hnsw(index)
                }
                AnnMethod::IvfFlat => {
                    if parts.len() != 4 {
                        return Err("IVF generation has unexpected parts".to_string());
                    }
                    let part = |name: &str| {
                        parts
                            .get(name)
                            .cloned()
                            .ok_or_else(|| format!("IVF generation is missing {name}"))
                    };
                    let artifact = eg_ann::durable_codes::AnnCodeArtifact {
                        meta: part("ivf_meta")?,
                        codes: part("ivf_codes")?,
                        refine: part("ivf_refine")?,
                    };
                    let space_bytes = parts
                        .get("ivf_space")
                        .ok_or_else(|| "IVF generation is missing its metric space".to_string())?;
                    if space_bytes.len() > 128 {
                        return Err("IVF metric-space metadata exceeds its bound".to_string());
                    }
                    let space: IvfSpace =
                        rmp_serde::from_slice(space_bytes).map_err(|error| error.to_string())?;
                    if !space.matches_metric(metric) {
                        return Err(
                            "IVF generation metric space does not match registration".to_string()
                        );
                    }
                    let index = eg_ann::durable_codes::decode(&artifact)
                        .map_err(|error| error.to_string())?;
                    if index.dim != width + space.extra_dims() || index.ids.len() != rows {
                        return Err("IVF generation does not match its manifest".to_string());
                    }
                    if index.ids.iter().any(|id| Some(*id) > max_rowid) {
                        return Err("IVF generation row id exceeds high water".to_string());
                    }
                    let nprobe = probe_cells(index.nlist);
                    AnnGraph::Ivf {
                        index: Box::new(index),
                        space,
                        nprobe,
                    }
                }
            }
        };
        Ok(Self {
            generation,
            method,
            metric,
            dim,
            built_epoch,
            max_rowid,
            rows,
            graph,
        })
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
    fn matches_metric(self, metric: VectorMetric) -> bool {
        match (self, metric) {
            (Self::Euclidean, VectorMetric::L2) | (Self::UnitSphere, VectorMetric::Cosine) => true,
            (Self::MaxNormAugmented { max_norm_sq }, VectorMetric::InnerProduct) => {
                max_norm_sq.is_finite() && max_norm_sq >= 0.0
            }
            _ => false,
        }
    }
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
