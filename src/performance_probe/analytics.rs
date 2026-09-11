//! Trace, knowledge-batch, mining, and observability probes.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use super::{allocation_bytes, timed, Observation, ProbeError};
use eg_plan::knowledge_batch::{KnowledgeBatch, KnowledgeBatchRow};
use eg_tsdb::traces::{Span, SpanStore, TraceQuery};

pub(super) fn probe_traces(scale: usize) -> Result<Observation, ProbeError> {
    let store = SpanStore::new();
    let spans: Vec<_> = (0..scale)
        .map(|index| Span {
            trace_id: format!("trace-{:08}", index / 2),
            span_id: format!("span-{index:08}"),
            parent_span_id: String::new(),
            service: if index % 2 == 0 { "target" } else { "other" }.to_string(),
            operation: "probe".to_string(),
            start_time: index as i64,
            duration: 1,
            status: "OK".to_string(),
            attributes: BTreeMap::new(),
            events: Vec::new(),
        })
        .collect();
    let accepted = store.add_spans(spans);
    let mut query = TraceQuery::new(16);
    query.service = Some("target".to_string());
    let (result, latency) = timed(|| store.search(&query));
    let starts: Vec<_> = result.iter().map(|trace| trace.start_time).collect();
    Ok(Observation {
        work_units: scale.saturating_mul(2).max(1) as u64,
        memory_bytes: allocation_bytes::<Span>(scale),
        latency_ns: latency,
        equivalent: accepted == scale
            && result.len() == (scale.saturating_add(1) / 2).min(16)
            && starts.windows(2).all(|pair| pair[0] >= pair[1]),
    })
}

pub(super) fn probe_knowledge_projection(scale: usize) -> Result<Observation, ProbeError> {
    let rows: Vec<_> = (0..scale)
        .map(|index| KnowledgeBatchRow {
            id: format!("row-{index:08}"),
            kind: "g37".to_string(),
            scores: vec![
                ("score".to_string(), Some(index as f32)),
                ("aux".to_string(), Some((scale - index) as f32)),
            ],
            confidence: 1.0,
            ..KnowledgeBatchRow::default()
        })
        .collect();
    let batch = KnowledgeBatch {
        rows,
        score_names: vec!["score".to_string(), "aux".to_string()],
    };
    let (record_batch, latency) = timed(|| batch.to_record_batch());
    let record_batch = record_batch?;
    let names: Vec<_> = record_batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let unique = names.iter().collect::<HashSet<_>>();
    Ok(Observation {
        work_units: scale.saturating_mul(4).max(1) as u64,
        memory_bytes: allocation_bytes::<KnowledgeBatchRow>(batch.rows.capacity())
            .saturating_add(record_batch.get_array_memory_size() as u64),
        latency_ns: latency,
        equivalent: record_batch.num_rows() == scale && unique.len() == names.len(),
    })
}

pub(super) fn probe_mining_similarity(
    row_id: &str,
    scale: usize,
) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-049" => probe_mining_extensions(scale),
        "G37-HP-050" => probe_mining_neighbors(scale),
        "G37-HP-051" => probe_mining_similarity_topk(scale),
        "G37-HP-052" => probe_mining_similarity_semantic(scale),
        _ => Err("invalid mining/similarity probe row".into()),
    }
}

fn probe_mining_extensions(scale: usize) -> Result<Observation, ProbeError> {
    let embedding: HashMap<usize, usize> = (0..scale).map(|index| (index, index)).collect();
    let pattern_edges: HashSet<(usize, usize)> =
        (1..scale).map(|index| (index - 1, index)).collect();
    let incident: Vec<_> = (0..scale)
        .map(|index| (index, (index + 1) % scale.max(1)))
        .collect();
    let (extensions, latency) = timed(|| {
        incident
            .iter()
            .filter(|(source, target)| {
                embedding.contains_key(source) && !pattern_edges.contains(&(*source, *target))
            })
            .count()
    });
    let reference = incident
        .iter()
        .filter(|(source, target)| {
            embedding.keys().any(|node| node == source)
                && !pattern_edges.iter().any(|edge| edge == &(*source, *target))
        })
        .count();
    Ok(Observation {
        work_units: scale.saturating_mul(3).max(1) as u64,
        memory_bytes: allocation_bytes::<(usize, usize)>(
            embedding.capacity() + pattern_edges.capacity() + incident.capacity(),
        ),
        latency_ns: latency,
        equivalent: extensions == reference,
    })
}

fn probe_mining_neighbors(scale: usize) -> Result<Observation, ProbeError> {
    let adjacency: Vec<Vec<usize>> = (0..scale)
        .map(|node| {
            vec![
                (node + 1) % scale.max(1),
                (node + scale.saturating_sub(1)) % scale.max(1),
            ]
        })
        .collect();
    let (prepared, latency) = timed(|| {
        adjacency
            .iter()
            .map(|neighbors| neighbors.as_slice())
            .collect::<Vec<_>>()
    });
    Ok(Observation {
        work_units: scale.saturating_mul(2).max(1) as u64,
        memory_bytes: allocation_bytes::<Vec<usize>>(adjacency.capacity())
            .saturating_add(allocation_bytes::<usize>(scale.saturating_mul(2))),
        latency_ns: latency,
        equivalent: prepared.len() == scale
            && prepared.iter().all(|neighbors| neighbors.len() == 2),
    })
}

fn probe_mining_similarity_topk(scale: usize) -> Result<Observation, ProbeError> {
    let mut scored: Vec<_> = (0..scale)
        .map(|node| (node, ((node * 37) % 101) as f64 / 101.0))
        .collect();
    let full = {
        let mut value = scored.clone();
        value.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
        value.truncate(16.min(value.len()));
        value
    };
    let ((), latency) = timed(|| {
        let keep = 16.min(scored.len());
        if keep < scored.len() {
            scored.select_nth_unstable_by(keep, |left, right| {
                right.1.total_cmp(&left.1).then(left.0.cmp(&right.0))
            });
            scored.truncate(keep);
        }
        scored.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    });
    Ok(Observation {
        work_units: scale.saturating_mul(scale).max(1) as u64,
        memory_bytes: allocation_bytes::<(usize, f64)>(scale),
        latency_ns: latency,
        equivalent: scored == full,
    })
}

fn probe_mining_similarity_semantic(scale: usize) -> Result<Observation, ProbeError> {
    let mut weighted: Vec<_> = (0..scale)
        .map(|index| (index, ((index * 19) % 97) as f64 / 97.0, index))
        .collect();
    let mut full = weighted.clone();
    full.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.2.cmp(&right.2)));
    full.truncate(16.min(full.len()));
    let ((), latency) = timed(|| {
        let keep = 16.min(weighted.len());
        if keep < weighted.len() {
            weighted.select_nth_unstable_by(keep, |left, right| {
                right.1.total_cmp(&left.1).then(left.2.cmp(&right.2))
            });
            weighted.truncate(keep);
        }
        weighted.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.2.cmp(&right.2)));
    });
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: allocation_bytes::<(usize, f64, usize)>(scale),
        latency_ns: latency,
        equivalent: weighted == full,
    })
}

pub(super) fn probe_observability_symbol(
    row_id: &str,
    scale: usize,
) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-053" => probe_observability_prefix(scale),
        "G37-HP-054" => probe_observability_callsites(scale),
        _ => Err("invalid observability/symbol probe row".into()),
    }
}

fn probe_observability_prefix(scale: usize) -> Result<Observation, ProbeError> {
    let mut records: Vec<_> = (0..scale)
        .map(|ordinal| (((ordinal * 29) % scale.max(1)) as i64, ordinal))
        .collect();
    let mut reference = records.clone();
    reference.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    reference.truncate(16.min(reference.len()));
    let ((), latency) = timed(|| {
        let keep = 16.min(records.len());
        if keep < records.len() {
            records.select_nth_unstable_by(keep, |left, right| {
                left.0.cmp(&right.0).then(left.1.cmp(&right.1))
            });
            records.truncate(keep);
        }
        records.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    });
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: allocation_bytes::<(i64, usize)>(scale),
        latency_ns: latency,
        equivalent: records == reference,
    })
}

fn probe_observability_callsites(scale: usize) -> Result<Observation, ProbeError> {
    let calls: Vec<_> = (0..scale)
        .rev()
        .map(|index| format!("symbol::{:08}", index % 97))
        .collect();
    let (retained, latency) = timed(|| {
        let mut bounded = BTreeSet::new();
        for call in &calls {
            bounded.insert(call.clone());
            if bounded.len() > 64 {
                let largest = bounded.last().cloned().expect("non-empty bounded set");
                bounded.remove(&largest);
            }
        }
        bounded.into_iter().collect::<Vec<_>>()
    });
    let mut reference = calls.clone();
    reference.sort();
    reference.dedup();
    reference.truncate(64);
    Ok(Observation {
        work_units: scale.saturating_mul(7).max(1) as u64,
        memory_bytes: allocation_bytes::<String>(retained.capacity()),
        latency_ns: latency,
        equivalent: retained == reference,
    })
}
