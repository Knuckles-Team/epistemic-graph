//! Native governed `KnowledgeBatch` streaming currency.
//!
//! Graph, SQL, RDF, vector, time-series, analytics-job, and cross-modal producers
//! all enter through this module. Producers yield one row at a time; the adapter
//! emits bounded `KnowledgeBatch` chunks with a stable Arrow schema and injects the
//! verified tenant/policy/placement/snapshot/query/derivation/evidence context into every row.
//! No family-specific wire result is considered native once this feature is enabled.

use std::collections::HashSet;
use std::fmt;
use std::io::Write;
use std::iter::Peekable;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use eg_modality::{EvidenceLocus, OpaqueRef};
use serde::{Deserialize, Serialize};

use crate::knowledge_batch::{KnowledgeBatch, KnowledgeBatchRow};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServedResultFamily {
    Graph,
    Sql,
    Rdf,
    Vector,
    TimeSeries,
    Job,
    CrossModal,
}

impl ServedResultFamily {
    pub const ALL: [Self; 7] = [
        Self::Graph,
        Self::Sql,
        Self::Rdf,
        Self::Vector,
        Self::TimeSeries,
        Self::Job,
        Self::CrossModal,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Graph => "graph",
            Self::Sql => "sql",
            Self::Rdf => "rdf",
            Self::Vector => "vector",
            Self::TimeSeries => "time_series",
            Self::Job => "job",
            Self::CrossModal => "cross_modal",
        }
    }
}

/// Reproducibility, authority, and placement context attached to every streamed batch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeStreamContext {
    pub tenant_ref: OpaqueRef,
    pub access_policy_ref: OpaqueRef,
    pub placement_ref: OpaqueRef,
    pub snapshot_ref: OpaqueRef,
    pub query_ref: OpaqueRef,
    pub derivation_ref: OpaqueRef,
    pub evidence_set_ref: OpaqueRef,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeStreamCursor {
    pub family: ServedResultFamily,
    pub tenant_ref: OpaqueRef,
    pub access_policy_ref: OpaqueRef,
    pub placement_ref: OpaqueRef,
    pub snapshot_ref: OpaqueRef,
    pub query_ref: OpaqueRef,
    pub derivation_ref: OpaqueRef,
    pub evidence_set_ref: OpaqueRef,
    pub batch_size: u32,
    pub row_offset: u64,
    pub batch_index: u64,
    pub exhausted: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct KnowledgeBatchEnvelope {
    pub family: ServedResultFamily,
    pub context: KnowledgeStreamContext,
    pub cursor: KnowledgeStreamCursor,
    pub batch: KnowledgeBatch,
}

pub type KnowledgeRowResult = Result<KnowledgeBatchRow, KnowledgeStreamError>;

/// Pull-based bounded producer. It never materializes the complete input row set.
pub struct KnowledgeBatchStream<I>
where
    I: Iterator<Item = KnowledgeRowResult>,
{
    family: ServedResultFamily,
    context: KnowledgeStreamContext,
    score_names: Vec<String>,
    rows: Peekable<I>,
    batch_size: usize,
    row_offset: u64,
    batch_index: u64,
    exhausted: bool,
    failed: bool,
}

impl<I> KnowledgeBatchStream<I>
where
    I: Iterator<Item = KnowledgeRowResult>,
{
    pub fn try_new(
        family: ServedResultFamily,
        context: KnowledgeStreamContext,
        score_names: Vec<String>,
        rows: I,
        batch_size: usize,
    ) -> Result<Self, KnowledgeStreamError> {
        validate_score_names(&score_names)?;
        Ok(Self {
            family,
            context,
            score_names,
            rows: rows.peekable(),
            batch_size: batch_size.clamp(1, 65_536),
            row_offset: 0,
            batch_index: 0,
            exhausted: false,
            failed: false,
        })
    }

    pub fn cursor(&self) -> KnowledgeStreamCursor {
        KnowledgeStreamCursor {
            family: self.family,
            tenant_ref: self.context.tenant_ref.clone(),
            access_policy_ref: self.context.access_policy_ref.clone(),
            placement_ref: self.context.placement_ref.clone(),
            snapshot_ref: self.context.snapshot_ref.clone(),
            query_ref: self.context.query_ref.clone(),
            derivation_ref: self.context.derivation_ref.clone(),
            evidence_set_ref: self.context.evidence_set_ref.clone(),
            batch_size: self.batch_size as u32,
            row_offset: self.row_offset,
            batch_index: self.batch_index,
            exhausted: self.exhausted,
        }
    }

    /// Resume a deterministic source by discarding the already acknowledged prefix.
    /// Snapshot and result family must match exactly.
    pub fn resume_from(
        mut self,
        cursor: &KnowledgeStreamCursor,
    ) -> Result<Self, KnowledgeStreamError> {
        if cursor.family != self.family
            || cursor.tenant_ref != self.context.tenant_ref
            || cursor.access_policy_ref != self.context.access_policy_ref
            || cursor.placement_ref != self.context.placement_ref
            || cursor.snapshot_ref != self.context.snapshot_ref
            || cursor.query_ref != self.context.query_ref
            || cursor.derivation_ref != self.context.derivation_ref
            || cursor.evidence_set_ref != self.context.evidence_set_ref
            || cursor.batch_size as usize != self.batch_size
            || !valid_cursor_position(cursor)
        {
            return Err(KnowledgeStreamError::CursorMismatch);
        }
        for _ in 0..cursor.row_offset {
            match self.rows.next() {
                Some(Ok(_)) => {}
                Some(Err(_)) => return Err(KnowledgeStreamError::SourceFailure),
                None => return Err(KnowledgeStreamError::CursorMismatch),
            }
        }
        self.row_offset = cursor.row_offset;
        self.batch_index = cursor.batch_index;
        self.exhausted = cursor.exhausted;
        if cursor.exhausted && self.rows.peek().is_some() {
            return Err(KnowledgeStreamError::CursorMismatch);
        }
        Ok(self)
    }

    pub fn next_batch(&mut self) -> Result<Option<KnowledgeBatchEnvelope>, KnowledgeStreamError> {
        if self.failed {
            return Err(KnowledgeStreamError::StreamFailed);
        }
        if self.exhausted {
            return Ok(None);
        }

        let mut rows = Vec::with_capacity(self.batch_size);
        while rows.len() < self.batch_size {
            match self.rows.next() {
                Some(Ok(mut row)) => {
                    enrich_row(&mut row, &self.context);
                    if let Err(error) = validate_native_row(&row, &self.score_names) {
                        self.failed = true;
                        return Err(error);
                    }
                    rows.push(row);
                }
                Some(Err(_)) => {
                    self.failed = true;
                    return Err(KnowledgeStreamError::SourceFailure);
                }
                None => break,
            }
        }

        self.exhausted = self.rows.peek().is_none();
        if rows.is_empty() {
            self.exhausted = true;
            return Ok(None);
        }
        let batch = KnowledgeBatch {
            rows,
            score_names: self.score_names.clone(),
        };
        validate_native_batch(&batch)?;
        self.row_offset = self.row_offset.saturating_add(batch.len() as u64);
        self.batch_index = self.batch_index.saturating_add(1);
        Ok(Some(KnowledgeBatchEnvelope {
            family: self.family,
            context: self.context.clone(),
            cursor: self.cursor(),
            batch,
        }))
    }

    /// Write a genuine multi-batch Arrow IPC stream directly to a sink. Only one
    /// bounded `KnowledgeBatch` and one Arrow `RecordBatch` are resident at a time.
    pub fn write_arrow_ipc<W: Write>(&mut self, sink: W) -> Result<(), KnowledgeStreamError> {
        let schema = Arc::new(
            KnowledgeBatch {
                rows: Vec::new(),
                score_names: self.score_names.clone(),
            }
            .arrow_schema(),
        );
        let mut writer =
            StreamWriter::try_new(sink, &schema).map_err(|_| KnowledgeStreamError::ArrowFailure)?;
        while let Some(envelope) = self.next_batch()? {
            let record_batch = envelope
                .batch
                .to_record_batch()
                .map_err(|_| KnowledgeStreamError::ArrowFailure)?;
            writer
                .write(&record_batch)
                .map_err(|_| KnowledgeStreamError::ArrowFailure)?;
        }
        writer
            .finish()
            .map_err(|_| KnowledgeStreamError::ArrowFailure)
    }
}

fn enrich_row(row: &mut KnowledgeBatchRow, context: &KnowledgeStreamContext) {
    push_unique(&mut row.source_refs, context.snapshot_ref.as_str());
    push_unique(&mut row.source_refs, context.query_ref.as_str());
    push_unique(&mut row.source_refs, context.evidence_set_ref.as_str());
    push_unique(&mut row.policy_labels, context.tenant_ref.as_str());
    push_unique(&mut row.policy_labels, context.access_policy_ref.as_str());
    push_unique(&mut row.policy_labels, context.placement_ref.as_str());
    push_unique(&mut row.transformation_ids, context.derivation_ref.as_str());
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values.iter().any(|existing| existing == value) {
        values.push(value.to_string());
    }
}

pub fn validate_native_batch(batch: &KnowledgeBatch) -> Result<(), KnowledgeStreamError> {
    validate_score_names(&batch.score_names)?;
    for row in &batch.rows {
        validate_native_row(row, &batch.score_names)?;
    }
    Ok(())
}

fn validate_score_names(score_names: &[String]) -> Result<(), KnowledgeStreamError> {
    let mut seen = HashSet::with_capacity(score_names.len());
    if score_names.is_empty()
        || score_names
            .iter()
            .any(|name| !safe_token(name) || !seen.insert(name.as_str()))
    {
        return Err(KnowledgeStreamError::SchemaInvariant);
    }
    Ok(())
}

fn validate_native_row(
    row: &KnowledgeBatchRow,
    score_names: &[String],
) -> Result<(), KnowledgeStreamError> {
    if !safe_reference(&row.id)
        || !safe_token(&row.kind)
        || !row.confidence.is_finite()
        || !(0.0..=1.0).contains(&row.confidence)
        || row.scores.len() != score_names.len()
        || row
            .scores
            .iter()
            .zip(score_names)
            .any(|((name, score), expected)| {
                name != expected || score.is_some_and(|value| !value.is_finite())
            })
        || !valid_window(row.valid_time)
        || !valid_window(row.tx_time)
        || row
            .source_refs
            .iter()
            .chain(&row.policy_labels)
            .chain(&row.transformation_ids)
            .chain(&row.proof_ids)
            .chain(&row.alternative_ids)
            .chain(&row.contradiction_ids)
            .any(|value| !safe_reference(value))
        || row
            .blob_handle
            .as_ref()
            .is_some_and(|value| !safe_reference(value))
        || row.has_payload != row.blob_handle.is_some()
        || row
            .evidence_refs
            .iter()
            .any(|evidence| !safe_evidence(evidence))
    {
        return Err(KnowledgeStreamError::RowInvariant);
    }
    // The adapter injected all of these. Their absence means a producer bypassed
    // the native governed stream path.
    if row.source_refs.len() < 3 || row.policy_labels.len() < 2 || row.transformation_ids.is_empty()
    {
        return Err(KnowledgeStreamError::GovernanceInvariant);
    }
    Ok(())
}

fn valid_window(window: (Option<u64>, Option<u64>)) -> bool {
    !matches!(window, (Some(start), Some(end)) if end < start)
}

fn valid_cursor_position(cursor: &KnowledgeStreamCursor) -> bool {
    if cursor.batch_index == 0 {
        return cursor.row_offset == 0;
    }
    let batch_size = cursor.batch_size as u64;
    if batch_size == 0 {
        return false;
    }
    let maximum = cursor.batch_index.saturating_mul(batch_size);
    if cursor.exhausted {
        let minimum = cursor
            .batch_index
            .saturating_sub(1)
            .saturating_mul(batch_size)
            .saturating_add(1);
        (minimum..=maximum).contains(&cursor.row_offset)
    } else {
        cursor.row_offset == maximum
    }
}

fn safe_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn safe_reference(value: &str) -> bool {
    OpaqueRef::new(value.to_string()).is_ok()
}

fn safe_evidence(evidence: &EvidenceLocus) -> bool {
    evidence.validate().is_ok()
}

fn ok_row(row: KnowledgeBatchRow) -> KnowledgeRowResult {
    Ok(row)
}

#[doc(hidden)]
pub type OkRows<I> = std::iter::Map<I, fn(KnowledgeBatchRow) -> KnowledgeRowResult>;

fn stream_from_rows<I>(
    family: ServedResultFamily,
    context: KnowledgeStreamContext,
    score_names: Vec<String>,
    rows: I,
    batch_size: usize,
) -> Result<KnowledgeBatchStream<OkRows<I::IntoIter>>, KnowledgeStreamError>
where
    I: IntoIterator<Item = KnowledgeBatchRow>,
{
    KnowledgeBatchStream::try_new(
        family,
        context,
        score_names,
        rows.into_iter().map(ok_row as fn(_) -> _),
        batch_size,
    )
}

macro_rules! family_adapter {
    ($name:ident, $family:expr) => {
        pub fn $name<I>(
            context: KnowledgeStreamContext,
            score_names: Vec<String>,
            rows: I,
            batch_size: usize,
        ) -> Result<KnowledgeBatchStream<OkRows<I::IntoIter>>, KnowledgeStreamError>
        where
            I: IntoIterator<Item = KnowledgeBatchRow>,
        {
            stream_from_rows($family, context, score_names, rows, batch_size)
        }
    };
}

family_adapter!(graph_result_stream, ServedResultFamily::Graph);
family_adapter!(sql_result_stream, ServedResultFamily::Sql);
family_adapter!(rdf_result_stream, ServedResultFamily::Rdf);
family_adapter!(vector_result_stream, ServedResultFamily::Vector);
family_adapter!(time_series_result_stream, ServedResultFamily::TimeSeries);
family_adapter!(job_result_stream, ServedResultFamily::Job);
family_adapter!(cross_modal_result_stream, ServedResultFamily::CrossModal);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KnowledgeStreamError {
    SchemaInvariant,
    RowInvariant,
    GovernanceInvariant,
    CursorMismatch,
    SourceFailure,
    ArrowFailure,
    StreamFailed,
}

impl fmt::Display for KnowledgeStreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::SchemaInvariant => "invalid knowledge batch schema",
            Self::RowInvariant => "invalid knowledge batch row",
            Self::GovernanceInvariant => "missing knowledge batch governance context",
            Self::CursorMismatch => "knowledge stream cursor mismatch",
            Self::SourceFailure => "knowledge stream source failed",
            Self::ArrowFailure => "knowledge stream Arrow encoding failed",
            Self::StreamFailed => "knowledge stream is failed",
        })
    }
}

impl std::error::Error for KnowledgeStreamError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(namespace: &str, suffix: u8) -> OpaqueRef {
        OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
    }

    fn context() -> KnowledgeStreamContext {
        KnowledgeStreamContext {
            tenant_ref: r("tenant", 1),
            access_policy_ref: r("policy", 2),
            placement_ref: r("placement", 7),
            snapshot_ref: r("snapshot", 3),
            query_ref: r("query", 4),
            derivation_ref: r("derivation", 5),
            evidence_set_ref: r("evidenceset", 6),
        }
    }

    fn row(index: usize) -> KnowledgeBatchRow {
        KnowledgeBatchRow {
            id: format!("eg:node:{index:016x}"),
            kind: "result".to_string(),
            scores: vec![("score".to_string(), Some(index as f32 / 10.0))],
            confidence: 0.9,
            ..KnowledgeBatchRow::default()
        }
    }

    #[test]
    fn every_served_family_streams_the_same_governed_batch_currency() {
        for family in ServedResultFamily::ALL {
            let mut stream = KnowledgeBatchStream::try_new(
                family,
                context(),
                vec!["score".to_string()],
                (0..5).map(|index| Ok(row(index))),
                2,
            )
            .unwrap();
            let mut sizes = Vec::new();
            while let Some(envelope) = stream.next_batch().unwrap() {
                assert_eq!(envelope.family, family);
                assert!(envelope.batch.rows.iter().all(|row| {
                    row.source_refs.len() >= 3
                        && row.policy_labels.len() >= 2
                        && !row.transformation_ids.is_empty()
                }));
                sizes.push(envelope.batch.len());
            }
            assert_eq!(sizes, vec![2, 2, 1]);
            assert!(stream.cursor().exhausted);
        }
    }

    #[test]
    fn duplicate_score_names_are_rejected_for_large_schemas() {
        let mut names: Vec<String> = (0..1_024).map(|index| format!("score_{index}")).collect();
        names.push("score_0".to_string());
        let result = KnowledgeBatchStream::try_new(
            ServedResultFamily::Graph,
            context(),
            names,
            std::iter::empty::<KnowledgeRowResult>(),
            1,
        );
        assert!(matches!(result, Err(KnowledgeStreamError::SchemaInvariant)));
    }

    #[test]
    fn arrow_writer_consumes_bounded_batches() {
        let mut stream =
            graph_result_stream(context(), vec!["score".to_string()], (0..3).map(row), 1).unwrap();
        let mut output = Vec::new();
        stream.write_arrow_ipc(&mut output).unwrap();
        assert!(!output.is_empty());
        assert!(stream.cursor().exhausted);
    }

    /// Resident-set size in bytes on Linux (`/proc/self/statm` field 2 × page size),
    /// `None` elsewhere — the flat-RSS probe below is Linux-gated.
    #[cfg(target_os = "linux")]
    fn resident_bytes() -> Option<u64> {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        Some(resident_pages * 4096)
    }

    /// A lazy source that COUNTS how many rows it has yielded, so a test can prove the
    /// producer pulls incrementally (one batch ahead at most) rather than draining the
    /// whole source up front — the structural half of the bounded-memory guarantee.
    struct CountingRows {
        next: usize,
        total: usize,
        pulled: std::rc::Rc<std::cell::Cell<usize>>,
    }

    impl Iterator for CountingRows {
        type Item = KnowledgeBatchRow;
        fn next(&mut self) -> Option<Self::Item> {
            if self.next >= self.total {
                return None;
            }
            let index = self.next;
            self.next += 1;
            self.pulled.set(self.pulled.get() + 1);
            Some(row(index))
        }
    }

    /// A 1M-row scan streams to completion holding only ONE bounded batch resident —
    /// the whole point of the columnar streaming currency (flat RSS regardless of scan
    /// size). Proven three ways: (1) the lazy source is a range never collected into a
    /// `Vec`; (2) the producer is verified to pull at most one `batch_size` page ahead
    /// of what it has emitted (never draining all 1M up front); (3) on Linux, resident
    /// memory grows by a small bound across the whole scan, not proportionally to 1M
    /// rows (which materialized would cost hundreds of MB).
    #[test]
    fn million_row_scan_streams_with_flat_memory() {
        const ROWS: usize = 1_000_000;
        const BATCH: usize = 1_024;

        let pulled = std::rc::Rc::new(std::cell::Cell::new(0usize));
        let source = CountingRows {
            next: 0,
            total: ROWS,
            pulled: pulled.clone(),
        };
        let mut stream =
            graph_result_stream(context(), vec!["score".to_string()], source, BATCH).unwrap();

        #[cfg(target_os = "linux")]
        let baseline = resident_bytes();

        // Drive the stream batch-by-batch to a discarding sink, asserting the bound
        // holds at every step: each resident batch is ≤ BATCH rows, and the source has
        // never been pulled more than one page beyond what has already been emitted.
        let mut emitted: usize = 0;
        let mut batches: u64 = 0;
        while let Some(envelope) = stream.next_batch().unwrap() {
            let rows = envelope.batch.len();
            assert!(
                rows <= BATCH,
                "resident batch {rows} exceeded bound {BATCH}"
            );
            emitted += rows;
            batches += 1;
            assert!(
                pulled.get() <= emitted + BATCH,
                "producer read {} rows but only {emitted} emitted (+{BATCH} lookahead) — \
                 not streaming incrementally",
                pulled.get(),
            );
        }

        assert_eq!(emitted, ROWS);
        assert_eq!(batches, (ROWS as u64).div_ceil(BATCH as u64));
        assert!(stream.cursor().exhausted);

        // Flat RSS: streaming 1M rows must not grow resident memory proportionally to
        // the scan (materialized, 1M `KnowledgeBatchRow`s would cost hundreds of MB).
        #[cfg(target_os = "linux")]
        if let (Some(before), Some(after)) = (baseline, resident_bytes()) {
            let growth = after.saturating_sub(before);
            assert!(
                growth < 64 * 1024 * 1024,
                "RSS grew {growth} bytes over a 1M-row stream — not flat (bounded batch expected)",
            );
        }
    }

    #[test]
    fn local_paths_and_unversioned_context_are_rejected() {
        let mut unsafe_row = row(0);
        unsafe_row.evidence_refs.push(EvidenceLocus {
            id: eg_modality::EvidenceLocusId::from_token("0000000000000001").unwrap(),
            subject: eg_modality::ResourceId::Artifact(
                eg_modality::ArtifactId::from_token("0000000000000002").unwrap(),
            ),
            address: eg_modality::EvidenceAddress::CodeSymbol {
                revision_ref: r("revision", 3),
                symbol_ref: r("symbol", 4),
                start_line: 2,
                end_line: 1,
            },
            policy_ref: r("policy", 5),
            derivation_ref: eg_modality::DerivationId::from_token("0000000000000006").unwrap(),
        });
        let mut stream =
            graph_result_stream(context(), vec!["score".to_string()], [unsafe_row], 1).unwrap();
        assert_eq!(stream.next_batch(), Err(KnowledgeStreamError::RowInvariant));

        let mut named_row = row(1);
        named_row.id = "display-name".to_string();
        let mut named_stream =
            graph_result_stream(context(), vec!["score".to_string()], [named_row], 1).unwrap();
        assert_eq!(
            named_stream.next_batch(),
            Err(KnowledgeStreamError::RowInvariant)
        );
    }

    #[test]
    fn resume_requires_same_family_and_snapshot() {
        let mut stream =
            graph_result_stream(context(), vec!["score".to_string()], (0..4).map(row), 2).unwrap();
        let cursor = stream.next_batch().unwrap().unwrap().cursor;
        let resumed = graph_result_stream(context(), vec!["score".to_string()], (0..4).map(row), 2)
            .unwrap()
            .resume_from(&cursor)
            .unwrap();
        assert_eq!(resumed.cursor().row_offset, 2);

        let mut different_query = context();
        different_query.query_ref = r("query", 99);
        let mismatch = graph_result_stream(
            different_query,
            vec!["score".to_string()],
            (0..4).map(row),
            2,
        )
        .unwrap()
        .resume_from(&cursor);
        assert!(matches!(
            mismatch,
            Err(KnowledgeStreamError::CursorMismatch)
        ));

        let mut different_evidence = context();
        different_evidence.evidence_set_ref = r("evidenceset", 98);
        let mismatch = graph_result_stream(
            different_evidence,
            vec!["score".to_string()],
            (0..4).map(row),
            2,
        )
        .unwrap()
        .resume_from(&cursor);
        assert!(matches!(
            mismatch,
            Err(KnowledgeStreamError::CursorMismatch)
        ));
    }
}
