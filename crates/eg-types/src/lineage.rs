//! Dataset and column lineage records.
//!
//! * MEDULLA-UNIFIED-DATA-PLANE-R702: ETL runs, content-addressed dataset lineage
//!   edges and the idempotent drain of lineage events. A malformed dataset
//!   reference quarantines with a typed reason and its payload digest; it is
//!   never dropped. A replayed event adds no edge.
//! * MEDULLA-UNIFIED-DATA-PLANE-R763.1: column-level lineage edges.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A content-addressed dataset id: `ds:` plus the SHA-256 of the canonical
/// `namespace/name` reference.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DatasetId(String);

impl DatasetId {
    /// Address a dataset by its `namespace` and `name`. Both must be
    /// non-blank and free of whitespace.
    pub fn address(namespace: &str, name: &str) -> Result<Self, QuarantineReason> {
        let valid = |part: &str| !part.is_empty() && !part.chars().any(char::is_whitespace);
        if !valid(namespace) || !valid(name) {
            return Err(QuarantineReason::MalformedDatasetRef(format!(
                "{namespace:?}/{name:?}"
            )));
        }
        let digest = Sha256::digest(format!("{namespace}/{name}").as_bytes());
        Ok(Self(format!("ds:{}", hex::encode(digest))))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One ETL run that read and wrote datasets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtlRun {
    pub run_id: String,
    pub job: String,
    pub started_at_ms: i64,
    pub ended_at_ms: Option<i64>,
}

/// One dataset lineage edge produced by a run.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LineageEdge {
    pub dataset_in: DatasetId,
    pub dataset_out: DatasetId,
    pub run: String,
}

/// A dataset reference as it arrives on the lineage topic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetRef {
    pub namespace: String,
    pub name: String,
}

/// One lineage event as received.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LineageEvent {
    pub event_id: String,
    pub run: EtlRun,
    pub inputs: Vec<DatasetRef>,
    pub outputs: Vec<DatasetRef>,
}

impl LineageEvent {
    /// SHA-256 of the canonical JSON payload, kept on quarantine.
    pub fn payload_digest(&self) -> String {
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        hex::encode(Sha256::digest(&bytes))
    }
}

/// Why an event was quarantined.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "detail")]
pub enum QuarantineReason {
    MalformedDatasetRef(String),
    NoInputsOrOutputs,
}

impl QuarantineReason {
    pub fn code(&self) -> &'static str {
        match self {
            Self::MalformedDatasetRef(_) => "LINEAGE_DATASET_REF_MALFORMED",
            Self::NoInputsOrOutputs => "LINEAGE_EVENT_EMPTY",
        }
    }
}

/// An event that could not map, kept with its original payload digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantinedLineageEvent {
    pub event_id: String,
    pub payload_digest: String,
    pub reason: QuarantineReason,
}

fn address_all(refs: &[DatasetRef]) -> Result<Vec<DatasetId>, QuarantineReason> {
    refs.iter()
        .map(|r| DatasetId::address(&r.namespace, &r.name))
        .collect()
}

/// Map one event to its lineage edges. Pure: the same event always maps to
/// the same edges or the same quarantine reason.
pub fn map_lineage_event(event: &LineageEvent) -> Result<Vec<LineageEdge>, QuarantineReason> {
    if event.inputs.is_empty() || event.outputs.is_empty() {
        return Err(QuarantineReason::NoInputsOrOutputs);
    }
    let inputs = address_all(&event.inputs)?;
    let outputs = address_all(&event.outputs)?;
    let mut edges = Vec::with_capacity(inputs.len() * outputs.len());
    for dataset_in in &inputs {
        for dataset_out in &outputs {
            edges.push(LineageEdge {
                dataset_in: dataset_in.clone(),
                dataset_out: dataset_out.clone(),
                run: event.run.run_id.clone(),
            });
        }
    }
    Ok(edges)
}

/// The result of draining one batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainBatch {
    pub new_edges: Vec<LineageEdge>,
    pub quarantined: Vec<QuarantinedLineageEvent>,
    pub replayed: usize,
}

/// Idempotent lineage drain. It remembers applied event ids and processes
/// events in batches of `batch_size`, so memory per call is bounded by the
/// batch.
#[derive(Clone, Debug)]
pub struct LineageDrain {
    batch_size: usize,
    seen_events: BTreeSet<String>,
    edges: BTreeSet<LineageEdge>,
}

impl LineageDrain {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
            seen_events: BTreeSet::new(),
            edges: BTreeSet::new(),
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn edges(&self) -> &BTreeSet<LineageEdge> {
        &self.edges
    }

    /// Drain up to `batch_size` events from the front of `pending`. A
    /// replayed event id adds no edge; an unmappable event quarantines.
    pub fn drain(&mut self, pending: &mut Vec<LineageEvent>) -> DrainBatch {
        let take = pending.len().min(self.batch_size);
        let mut out = DrainBatch::default();
        for event in pending.drain(..take) {
            if self.seen_events.contains(&event.event_id) {
                out.replayed += 1;
                continue;
            }
            match map_lineage_event(&event) {
                Ok(edges) => {
                    for edge in edges {
                        if self.edges.insert(edge.clone()) {
                            out.new_edges.push(edge);
                        }
                    }
                }
                Err(reason) => out.quarantined.push(QuarantinedLineageEvent {
                    event_id: event.event_id.clone(),
                    payload_digest: event.payload_digest(),
                    reason,
                }),
            }
            self.seen_events.insert(event.event_id);
        }
        out
    }
}

/// How an output column derives from its input column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnTransform {
    Identity,
    Expression,
    Aggregate,
    FilterOnly,
}

/// One column lineage edge, bound to the query that produced it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnLineageEdge {
    pub source_column: String,
    pub target_column: String,
    pub transform: ColumnTransform,
    pub query_digest: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset(namespace: &str, name: &str) -> DatasetRef {
        DatasetRef {
            namespace: namespace.to_string(),
            name: name.to_string(),
        }
    }

    fn event(id: &str, inputs: Vec<DatasetRef>, outputs: Vec<DatasetRef>) -> LineageEvent {
        LineageEvent {
            event_id: id.to_string(),
            run: EtlRun {
                run_id: format!("run-{id}"),
                job: "nightly".to_string(),
                started_at_ms: 1,
                ended_at_ms: Some(2),
            },
            inputs,
            outputs,
        }
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R702.1
    #[test]
    fn lineage_model_round_trips_and_addresses_by_content() {
        let a = DatasetId::address("pg://app", "public.orders").unwrap();
        assert_eq!(a, DatasetId::address("pg://app", "public.orders").unwrap());
        assert_ne!(a, DatasetId::address("pg://app", "public.items").unwrap());
        assert!(a.as_str().starts_with("ds:") && a.as_str().len() == 67);
        assert!(DatasetId::address("pg://app", "bad name").is_err());
        assert!(DatasetId::address("", "orders").is_err());

        let original = event("e1", vec![dataset("pg", "a")], vec![dataset("lake", "b")]);
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(
            serde_json::from_str::<LineageEvent>(&json).unwrap(),
            original
        );
        let edges = map_lineage_event(&original).unwrap();
        let json = serde_json::to_string(&edges).unwrap();
        assert_eq!(
            serde_json::from_str::<Vec<LineageEdge>>(&json).unwrap(),
            edges
        );
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R702.2
    #[test]
    fn mapping_is_pure_and_drain_is_batched() {
        for size in [10usize, 1_000, 100_000] {
            let events: Vec<LineageEvent> = (0..size)
                .map(|i| {
                    event(
                        &i.to_string(),
                        vec![dataset("pg", &format!("t{i}"))],
                        vec![dataset("lake", &format!("t{i}"))],
                    )
                })
                .collect();
            assert_eq!(
                map_lineage_event(&events[0]).unwrap(),
                map_lineage_event(&events[0]).unwrap()
            );
            let mut drain = LineageDrain::new(256);
            let mut pending = events;
            let mut batches = 0;
            while !pending.is_empty() {
                let batch = drain.drain(&mut pending);
                assert!(batch.new_edges.len() <= drain.batch_size());
                batches += 1;
            }
            assert_eq!(drain.edges().len(), size);
            assert_eq!(batches, size.div_ceil(256));
        }
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R702.4
    #[test]
    fn replay_adds_no_edge_and_malformed_ref_quarantines() {
        let mut drain = LineageDrain::new(16);
        let good = event("e1", vec![dataset("pg", "a")], vec![dataset("lake", "b")]);
        let bad = event("e2", vec![dataset("pg", "a b")], vec![dataset("lake", "b")]);
        let mut pending = vec![good.clone(), bad.clone(), good.clone()];
        let batch = drain.drain(&mut pending);
        assert_eq!(batch.new_edges.len(), 1);
        assert_eq!(batch.replayed, 1);
        assert_eq!(batch.quarantined.len(), 1);
        assert_eq!(
            batch.quarantined[0].reason.code(),
            "LINEAGE_DATASET_REF_MALFORMED"
        );
        let before = drain.edges().clone();
        let again = drain.drain(&mut vec![good]);
        assert!(again.new_edges.is_empty());
        assert_eq!(drain.edges(), &before);
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R702.5
    #[test]
    fn quarantined_event_keeps_its_payload_digest() {
        let bad = event("e9", vec![], vec![dataset("lake", "b")]);
        let expected = bad.payload_digest();
        let batch = LineageDrain::new(4).drain(&mut vec![bad.clone()]);
        let record = &batch.quarantined[0];
        assert_eq!(record.payload_digest, expected);
        assert_eq!(record.reason, QuarantineReason::NoInputsOrOutputs);
        let json = serde_json::to_string(record).unwrap();
        assert_eq!(
            serde_json::from_str::<QuarantinedLineageEvent>(&json).unwrap(),
            *record
        );
        assert_eq!(expected, bad.payload_digest(), "digest is reproducible");
    }
}
