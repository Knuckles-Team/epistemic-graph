//! One immutable edge-index generation over one graph snapshot (EH-351).
//!
//! The generation owns a key table — position `i` is the [`EdgeKey`] behind
//! candidate id `i` — and either an HNSW graph over the indexed vector property
//! or BM25 postings over the indexed text property. Keys are enumerated in
//! `(source, target, ordinal)` order, so two builds over one snapshot are
//! identical.

use std::collections::BTreeMap;

use eg_ann::HnswIndex;
use eg_core::graph::GraphView;
use eg_core::index::{IndexBlock, IndexBlockReason, IndexManifest};
use serde_json::{Map, Value};

use super::{EdgeIndexKind, EdgeIndexLimits, EdgeIndexSpec, EdgeKey};
use crate::sql::metric_to_ann;

/// Deterministic seed for every edge build.
const BUILD_SEED: u64 = 0x00E6_0351;
/// HNSW fan-out (`M`) and construction beam.
const HNSW_M: usize = 16;
const HNSW_EF_CONSTRUCTION: usize = 200;

/// One indexed property value of one edge.
pub(super) enum EdgeValue {
    Vector(Vec<f32>),
    Text(String),
}

/// What a generation searches.
pub(super) enum EdgeGraph {
    /// No edge carried a vector, so the generation has no width.
    Empty,
    Vector {
        index: Box<HnswIndex>,
        dim: usize,
    },
    Text(TextPostings),
}

/// BM25 postings: every token's candidate ids, and the corpus statistics a
/// search scores current texts against.
pub(super) struct TextPostings {
    pub(super) postings: BTreeMap<String, Vec<u64>>,
    pub(super) corpus: eg_text::Corpus,
}

pub(super) struct EdgeGeneration {
    pub(super) generation: u64,
    /// The graph version of the snapshot the generation was built from.
    pub(super) built_version: u64,
    pub(super) keys: Vec<EdgeKey>,
    pub(super) graph: EdgeGraph,
    /// The snapshot's exact source coverage.
    pub(super) manifest: IndexManifest,
}

impl EdgeGeneration {
    /// Build generation `generation` from `view`, taken at graph `version`.
    pub(super) fn build(
        generation: u64,
        spec: &EdgeIndexSpec,
        view: &GraphView,
        version: u64,
        limits: EdgeIndexLimits,
    ) -> Result<Self, IndexBlock> {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut edge_count = 0u64;
        for (key, value) in view_edges(view, &spec.property, spec.kind) {
            edge_count += 1;
            let Some(value) = value else {
                continue;
            };
            if keys.len() == limits.build_edges {
                return Err(IndexBlock::new(
                    IndexBlockReason::BuildBound,
                    &format!(
                        "edge index build exceeds its bound of {} edges",
                        limits.build_edges
                    ),
                ));
            }
            keys.push(key);
            values.push(value);
        }
        let graph = match spec.kind {
            EdgeIndexKind::Vector { metric } => vector_graph(metric_to_ann(metric), values),
            EdgeIndexKind::Text => text_graph(values),
        };
        Ok(Self {
            generation,
            built_version: version,
            keys,
            graph,
            manifest: IndexManifest::valid(version, view.node_map.len() as u64, edge_count),
        })
    }
}

/// Every edge of `view` in key order, with its indexable value of `property`.
pub(super) fn view_edges<'v>(
    view: &'v GraphView,
    property: &'v str,
    kind: EdgeIndexKind,
) -> impl Iterator<Item = (EdgeKey, Option<EdgeValue>)> + 'v {
    let mut pairs: Vec<&(String, String)> = view.edge_properties.keys().collect();
    pairs.sort();
    pairs.into_iter().flat_map(move |pair| {
        view.edge_properties[pair]
            .iter()
            .enumerate()
            .map(move |(ordinal, blob)| {
                let key = EdgeKey {
                    source: pair.0.clone(),
                    target: pair.1.clone(),
                    ordinal: ordinal as u32,
                };
                let value = decode_properties(blob).and_then(|map| value_of(&map, property, kind));
                (key, value)
            })
    })
}

/// An edge's property map; `None` for a blob that does not decode to one.
pub(super) fn decode_properties(blob: &[u8]) -> Option<Map<String, Value>> {
    eg_types::msgpack::decode_bounded(
        blob,
        eg_types::msgpack::MsgpackLimits::new(
            eg_types::msgpack::MAX_PROPERTY_BYTES,
            eg_types::msgpack::MAX_PROPERTY_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .ok()
}

/// The value of `property` an index of `kind` indexes: a non-empty numeric
/// array, or a string.
pub(super) fn value_of(
    properties: &Map<String, Value>,
    property: &str,
    kind: EdgeIndexKind,
) -> Option<EdgeValue> {
    let value = properties.get(property)?;
    match kind {
        EdgeIndexKind::Vector { .. } => {
            let vector: Option<Vec<f32>> = value
                .as_array()?
                .iter()
                .map(|item| item.as_f64().map(|x| x as f32))
                .collect();
            vector
                .filter(|vector| !vector.is_empty())
                .map(EdgeValue::Vector)
        }
        EdgeIndexKind::Text => value.as_str().map(|text| EdgeValue::Text(text.to_string())),
    }
}

fn vector_graph(metric: eg_ann::Metric, values: Vec<EdgeValue>) -> EdgeGraph {
    let rows: Vec<(u64, Vec<f32>)> = values
        .into_iter()
        .enumerate()
        .filter_map(|(id, value)| match value {
            EdgeValue::Vector(vector) => Some((id as u64, vector)),
            EdgeValue::Text(_) => None,
        })
        .collect();
    let Some(dim) = rows.first().map(|(_, vector)| vector.len()) else {
        return EdgeGraph::Empty;
    };
    let rows: Vec<(u64, Vec<f32>)> = rows
        .into_iter()
        .filter(|(_, vector)| vector.len() == dim)
        .collect();
    let mut index = HnswIndex::new(dim, metric, HNSW_M, HNSW_EF_CONSTRUCTION, BUILD_SEED);
    index.insert_batch(&rows);
    EdgeGraph::Vector {
        index: Box::new(index),
        dim,
    }
}

fn text_graph(values: Vec<EdgeValue>) -> EdgeGraph {
    let texts: Vec<String> = values
        .into_iter()
        .map(|value| match value {
            EdgeValue::Text(text) => text,
            EdgeValue::Vector(_) => String::new(),
        })
        .collect();
    let mut postings: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (id, text) in texts.iter().enumerate() {
        let mut tokens = eg_text::tokenize(text);
        tokens.sort();
        tokens.dedup();
        for token in tokens {
            postings.entry(token).or_default().push(id as u64);
        }
    }
    // Built even over no text at all: an edge added later is scored exactly
    // against these (empty) corpus statistics.
    EdgeGraph::Text(TextPostings {
        postings,
        corpus: eg_text::Corpus::from_docs(texts.iter().map(String::as_str)),
    })
}
