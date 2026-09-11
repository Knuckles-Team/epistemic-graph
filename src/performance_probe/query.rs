//! Query, vector, time-series, ANN, ranking, and SQL probes.

use std::collections::{BTreeSet, HashMap, HashSet};

use super::{allocation_bytes, timed, Observation, ProbeError};
use eg_ann::{FlatIndex, HnswIndex, IvfPq, IvfPqParams, Metric, SearchParams};
use eg_plan::leanrag::{AnnIndex, GraphTopology, HierarchicalRetriever, RetrievalParams, Scored};
use eg_tsdb::point::Point;
use eg_tsdb::promql::{query_instant, MemSeriesSource, Value as PromValue};
use eg_tsdb::query::{sensor_fuse, time_bucket, Agg, Cell, Sample, SeriesRef};

pub(super) fn probe_promql(scale: usize) -> Result<Observation, ProbeError> {
    let mut source = MemSeriesSource::new();
    let points: Vec<_> = (0..scale)
        .map(|index| (index as i64, index as f64))
        .collect();
    source.push(MemSeriesSource::labels("g37_metric", &[]), points.clone());
    let t = scale.saturating_sub(1) as i64;
    let (value, latency) = timed(|| query_instant(&source, "g37_metric", t));
    let equivalent = matches!(
        value?,
        PromValue::Instant(ref samples)
            if samples.len() == 1 && samples[0].value == scale.saturating_sub(1) as f64
    );
    Ok(Observation {
        work_units: scale.ilog2() as u64 + 1,
        memory_bytes: std::mem::size_of::<eg_tsdb::promql::InstantSample>() as u64,
        latency_ns: latency,
        equivalent,
    })
}

fn vector(index: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|column| (((index + 1) * (column + 3)) % 97) as f32 / 97.0)
        .collect()
}

pub(super) fn probe_flat_vector(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let dim = 8;
    let items: Vec<_> = (0..scale)
        .map(|index| (index as u64, vector(index, dim)))
        .collect();
    let query = vector(scale / 3, dim);
    let mut index = FlatIndex::new(dim);
    index.add(&items);
    match row_id {
        "G37-HP-023" => probe_flat_vector_lookup(&index, &items, &query, scale, dim),
        "G37-HP-028" => probe_flat_vector_search(&index, &items, &query, scale, dim),
        _ => Err("invalid flat-vector probe row".into()),
    }
}

fn probe_flat_vector_lookup(
    index: &FlatIndex,
    items: &[(u64, Vec<f32>)],
    query: &[f32],
    scale: usize,
    dim: usize,
) -> Result<Observation, ProbeError> {
    let target = (scale / 2) as u64;
    let _ = index.vector_of(target);
    let candidates: Vec<_> = (0..scale.min(64) as u64).rev().collect();
    let ((looked_up, reranked), latency) = timed(|| {
        (
            index.vector_of(target).map(Vec::from),
            index.rerank(&query, &candidates, 8),
        )
    });
    let reference = items
        .iter()
        .find(|(id, _)| *id == target)
        .map(|(_, value)| value.clone());
    Ok(Observation {
        work_units: scale.ilog2() as u64 + candidates.len() as u64 * dim as u64 + 1,
        memory_bytes: index.byte_size().max(1) as u64,
        latency_ns: latency,
        equivalent: looked_up == reference
            && reranked.windows(2).all(|pair| {
                pair[0].distance < pair[1].distance
                    || (pair[0].distance == pair[1].distance && pair[0].id <= pair[1].id)
            }),
    })
}

fn probe_flat_vector_search(
    index: &FlatIndex,
    items: &[(u64, Vec<f32>)],
    query: &[f32],
    scale: usize,
    dim: usize,
) -> Result<Observation, ProbeError> {
    let (selected, latency) = timed(|| index.search(&query, 8, Metric::L2));
    let mut reference: Vec<_> = items
        .iter()
        .map(|(id, value)| (*id, Metric::L2.distance(&query, value)))
        .collect();
    reference.sort_by(|left, right| left.1.total_cmp(&right.1).then(left.0.cmp(&right.0)));
    reference.truncate(8.min(reference.len()));
    Ok(Observation {
        work_units: scale.saturating_mul(dim).max(1) as u64,
        memory_bytes: index.byte_size().max(1) as u64,
        latency_ns: latency,
        equivalent: selected
            .iter()
            .map(|hit| (hit.id, hit.distance))
            .eq(reference),
    })
}

pub(super) fn probe_time(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-024" => probe_time_append(scale),
        "G37-HP-025" => probe_time_range(scale),
        "G37-HP-026" => probe_time_bucket(scale),
        "G37-HP-027" => probe_time_fusion(scale),
        _ => Err("invalid time probe row".into()),
    }
}

fn probe_time_append(scale: usize) -> Result<Observation, ProbeError> {
    let existing: Vec<_> = (0..scale)
        .map(|index| Point::single(index as i64 * 2, index as f64))
        .collect();
    let mut late: Vec<_> = (0..scale)
        .rev()
        .map(|index| Point::single(index as i64 * 2 + 1, -(index as f64)))
        .collect();
    let (_, latency) = timed(|| late.sort_by_key(|point| point.ts));
    let mut merged = existing.clone();
    merged.extend(late);
    merged.sort_by_key(|point| point.ts);
    Ok(Observation {
        work_units: scale.saturating_mul(scale.ilog2() as usize + 2).max(1) as u64,
        memory_bytes: allocation_bytes::<Point>(merged.capacity()),
        latency_ns: latency,
        equivalent: merged.windows(2).all(|pair| pair[0].ts <= pair[1].ts)
            && merged.len() == scale.saturating_mul(2),
    })
}

fn probe_time_range(scale: usize) -> Result<Observation, ProbeError> {
    let points: Vec<_> = (0..scale)
        .map(|index| Point::single(index as i64, index as f64))
        .collect();
    let from = (scale / 3) as i64;
    let to = (scale * 2 / 3) as i64;
    let (slice, latency) = timed(|| {
        let start = points.partition_point(|point| point.ts < from);
        let end = points.partition_point(|point| point.ts < to);
        points[start..end].to_vec()
    });
    let reference: Vec<_> = points
        .iter()
        .filter(|point| point.ts >= from && point.ts < to)
        .cloned()
        .collect();
    Ok(Observation {
        work_units: scale.ilog2() as u64 * 2 + slice.len() as u64 + 1,
        memory_bytes: allocation_bytes::<Point>(points.capacity() + slice.capacity()),
        latency_ns: latency,
        equivalent: slice == reference,
    })
}

fn probe_time_bucket(scale: usize) -> Result<Observation, ProbeError> {
    let points: Vec<_> = (0..scale)
        .map(|index| Point::single(index as i64, index as f64))
        .collect();
    let width = (scale / 16).max(1) as i64;
    let (buckets, latency) = timed(|| time_bucket(&points, width, Agg::Mean));
    let total: usize = buckets.iter().map(|bucket| bucket.count).sum();
    let reference_first = points
        .iter()
        .take(width as usize)
        .map(|point| point.values[0])
        .sum::<f64>()
        / width.min(scale as i64) as f64;
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: allocation_bytes::<eg_tsdb::query::Bucket>(buckets.capacity()),
        latency_ns: latency,
        equivalent: total == scale
            && buckets
                .first()
                .is_some_and(|bucket| (bucket.value - reference_first).abs() < 1e-9),
    })
}

fn probe_time_fusion(scale: usize) -> Result<Observation, ProbeError> {
    let streams: Vec<_> = (0..scale)
        .map(|stream| SeriesRef {
            name: format!("s-{stream}"),
            samples: (0..16)
                .map(|sample| {
                    Sample::scalar((sample * scale + stream) as i64, (stream + sample) as f64)
                })
                .collect(),
        })
        .collect();
    let total_samples: usize = streams.iter().map(|stream| stream.samples.len()).sum();
    let (fused, latency) = timed(|| sensor_fuse(&streams, None));
    let expected_clock: BTreeSet<_> = streams
        .iter()
        .flat_map(|stream| stream.samples.iter().map(|sample| sample.ts))
        .collect();
    Ok(Observation {
        work_units: total_samples
            .saturating_mul(scale.ilog2() as usize + 1)
            .max(1) as u64,
        memory_bytes: allocation_bytes::<Sample>(total_samples).saturating_add(allocation_bytes::<
            Option<Cell>,
        >(
            fused.len().saturating_mul(scale),
        )),
        latency_ns: latency,
        equivalent: fused.iter().map(|row| row.ts).eq(expected_clock)
            && fused.iter().all(|row| row.channels.len() == scale),
    })
}

pub(super) fn probe_ivfpq(scale: usize, seed: u64) -> Result<Observation, ProbeError> {
    let dim = 8;
    let training_len = scale.clamp(256, 512);
    let training: Vec<_> = (0..training_len).map(|index| vector(index, dim)).collect();
    let params = IvfPqParams {
        dim,
        nlist: 16.min(training_len),
        m: 2,
        kmeans_iters: 2,
        opq_iters: 0,
        seed,
    };
    let mut index = IvfPq::train(&params, &training);
    let items: Vec<_> = (0..scale)
        .map(|item| (item as u64, vector(item, dim)))
        .collect();
    index.add(&items);
    let query = vector(scale / 3, dim);
    let search = SearchParams {
        nprobe: 4,
        refine: true,
        refine_factor: 4,
    };
    let (selected, latency) = timed(|| index.search(&query, 8, search));
    let repeated = index.search(&query, 8, search);
    Ok(Observation {
        work_units: scale.saturating_mul(dim).max(1) as u64,
        memory_bytes: index.codes.capacity() as u64
            + index.sq_codes.capacity() as u64
            + allocation_bytes::<u64>(index.ids.capacity()),
        latency_ns: latency,
        equivalent: selected == repeated
            && selected.windows(2).all(|pair| {
                pair[0].distance < pair[1].distance
                    || (pair[0].distance == pair[1].distance && pair[0].id <= pair[1].id)
            }),
    })
}

pub(super) fn probe_hnsw(row_id: &str, scale: usize, seed: u64) -> Result<Observation, ProbeError> {
    let dim = 8;
    let items: Vec<_> = (0..scale)
        .map(|item| (item as u64, vector(item, dim)))
        .collect();
    let query = vector(scale / 3, dim);
    if row_id == "G37-HP-031" {
        let mut flat = FlatIndex::new(dim);
        flat.add(&items);
        let (selected, latency) = timed(|| flat.search(&query, 8, Metric::Cosine));
        let repeated = flat.search(&query, 8, Metric::Cosine);
        return Ok(Observation {
            work_units: scale.saturating_mul(dim).max(1) as u64,
            memory_bytes: flat.byte_size().max(1) as u64,
            latency_ns: latency,
            equivalent: selected == repeated,
        });
    }
    let mut index = HnswIndex::new(dim, Metric::L2, 8, 32, seed);
    index.insert_batch(&items);
    let (selected, latency) = timed(|| index.search(&query, 8, 32));
    let repeated = index.search(&query, 8, 32);
    Ok(Observation {
        work_units: scale.saturating_mul(dim).max(1) as u64,
        memory_bytes: index.byte_size().max(1) as u64,
        latency_ns: latency,
        equivalent: selected == repeated
            && selected.windows(2).all(|pair| {
                pair[0].distance < pair[1].distance
                    || (pair[0].distance == pair[1].distance && pair[0].id <= pair[1].id)
            }),
    })
}

struct RankFixture {
    embeddings: HashMap<String, Vec<f32>>,
    children: HashMap<String, Vec<String>>,
}

impl GraphTopology for RankFixture {
    fn label(&self, id: &str) -> Option<String> {
        (id == "summary").then(|| "SummaryNode".to_string())
    }

    fn children(&self, id: &str) -> Vec<String> {
        self.children.get(id).cloned().unwrap_or_default()
    }

    fn embedding(&self, id: &str) -> Option<Vec<f32>> {
        self.embeddings.get(id).cloned()
    }
}

impl AnnIndex for RankFixture {
    fn search(
        &self,
        _query: &[f32],
        k: usize,
        allow: Option<&dyn Fn(&str) -> bool>,
    ) -> Vec<Scored> {
        let mut scored: Vec<_> = self
            .embeddings
            .keys()
            .filter(|id| allow.is_none_or(|predicate| predicate(id)))
            .map(|id| Scored {
                id: id.clone(),
                score: if id == "summary" { 1.0 } else { 0.5 },
            })
            .collect();
        scored.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then(left.id.cmp(&right.id))
        });
        scored.truncate(k);
        scored
    }
}

pub(super) fn probe_rank_selection(scale: usize) -> Result<Observation, ProbeError> {
    let mut fixture = RankFixture {
        embeddings: HashMap::new(),
        children: HashMap::new(),
    };
    fixture
        .embeddings
        .insert("summary".to_string(), vec![1.0, 0.0]);
    let mut children = Vec::with_capacity(scale);
    for index in 0..scale {
        let id = format!("leaf-{index:08}");
        fixture
            .embeddings
            .insert(id.clone(), vec![1.0, index as f32 / scale.max(1) as f32]);
        children.push(id);
    }
    fixture.children.insert("summary".to_string(), children);
    let retriever = HierarchicalRetriever::new(&fixture, &fixture);
    let params = RetrievalParams {
        k: 1,
        drill_depth: 1,
        drill_breadth: 16,
        leaf_budget: 8,
    };
    let (result, latency) = timed(|| retriever.retrieve(&[1.0, 0.0], params));
    let unique = result.context_ids().into_iter().collect::<HashSet<_>>();
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: allocation_bytes::<(String, Vec<f32>)>(fixture.embeddings.capacity())
            .saturating_add(allocation_bytes::<String>(scale)),
        latency_ns: latency,
        equivalent: result
            .summaries
            .first()
            .is_some_and(|item| item.id == "summary")
            && result.leaves.len() == scale.min(8)
            && unique.len() == result.context.len(),
    })
}

pub(super) fn probe_sql_kernel(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let width = 16usize;
    let columns: Vec<_> = (0..width).map(|column| format!("c-{column:02}")).collect();
    match row_id {
        "G37-HP-039" => probe_sql_conflicts(scale),
        "G37-HP-040" => probe_sql_schema(&columns),
        _ => Err("invalid SQL probe row".into()),
    }
}

fn probe_sql_conflicts(scale: usize) -> Result<Observation, ProbeError> {
    let mut unique: Vec<HashMap<u64, usize>> =
        (0..4).map(|_| HashMap::with_capacity(scale)).collect();
    for row in 0..scale {
        for (column, index) in unique.iter_mut().enumerate() {
            index.insert((row * 4 + column) as u64, row);
        }
    }
    let batch: Vec<_> = (0..scale)
        .map(|row| {
            [
                (row * 4) as u64,
                (row * 4 + 1) as u64,
                (row * 4 + 2) as u64,
                (row * 4 + 3) as u64,
            ]
        })
        .collect();
    let (conflicts, latency) = timed(|| count_sql_conflicts(&unique, &batch));
    Ok(Observation {
        work_units: scale.saturating_mul(unique.len()).max(1) as u64,
        memory_bytes: unique
            .iter()
            .map(|index| allocation_bytes::<(u64, usize)>(index.capacity()))
            .sum(),
        latency_ns: latency,
        equivalent: conflicts == scale,
    })
}

fn count_sql_conflicts(unique: &[HashMap<u64, usize>], batch: &[[u64; 4]]) -> usize {
    batch
        .iter()
        .filter(|values| {
            unique
                .iter()
                .zip(values.iter())
                .any(|(index, value)| index.contains_key(value))
        })
        .count()
}

fn probe_sql_schema(columns: &[String]) -> Result<Observation, ProbeError> {
    let directory: HashMap<_, _> = columns
        .iter()
        .enumerate()
        .map(|(index, name)| (name.as_str(), index))
        .collect();
    let target = columns.last().expect("fixed schema");
    let (position, latency) = timed(|| directory.get(target.as_str()).copied());
    Ok(Observation {
        work_units: 1,
        memory_bytes: allocation_bytes::<(&str, usize)>(directory.capacity()),
        latency_ns: latency,
        equivalent: position == columns.iter().position(|name| name == target),
    })
}
