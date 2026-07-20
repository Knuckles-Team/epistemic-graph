//! `ModalityContract` retrofit for [`SeriesMeta`] and [`Span`] (CONCEPT:E4/X1).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF, and implying both
//! `redb-store` since [`SeriesMeta`] lives in the `store` module gated on it, AND
//! `traces` since [`Span`] lives in the `traces` module gated on IT) — see
//! `eg-modality`'s crate docs / README for the retrofit-order rationale (`eg-tsdb`/
//! `eg-stream` are next after the `eg-tensor`/`eg-geo` pilots).
//!
//! [`SeriesMeta`] — not a single point — is the crate's primary STORED value: it is
//! the one thing keyed 1:1 by `series_id` in `SERIES_META` (a bare [`crate::point::Point`]
//! has no id of its own; a series' points are appended UNDER a `SeriesMeta`'s id). It
//! already derives `Clone + Debug + PartialEq + Serialize + Deserialize`, so it needs
//! no new bounds to satisfy `ConformanceTestable`.
//!
//! [`Span`] contributes a trace address only when both distributed-tracing ids are
//! valid opaque tokens. It already derives `PartialEq`, so it needs no new bounds
//! for `ConformanceTestable`.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceAddress, IngestReport,
    ModalityContract, ModalitySelfTest, OpaqueRef, Provenance, RowSetShape, StagedWrite,
    StorageStats, TckPoint,
};

use crate::store::SeriesMeta;
use crate::traces::Span;

impl ModalityContract for SeriesMeta {
    fn storage_kind(&self) -> &'static str {
        "tsdb"
    }

    /// A series' metadata is a SOURCE-shaped candidate (`SERIES_META` keyed by
    /// `series_id`), not intrinsically ranked — a rank only emerges from a query
    /// primitive over the series' actual points (ASOF/OHLC/downsample), which this
    /// value doesn't carry. Unranked, exactly like `eg-geo::Geometry`.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    /// Staging a `SeriesMeta` write under `id` is the same shape as the store's own
    /// `SERIES_META` upsert (a msgpack-encoded value keyed by `series_id`) — here
    /// encoded via the shared `encode_staged` (serde_json) since `StagedWrite`'s
    /// payload is a transport-agnostic description, not the store's own on-disk
    /// codec.
    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// The most natural non-`None` `cdc_topic` in the retrofit set (per the crate
    /// README): a series append is a legitimate CDC-observable event even though
    /// nothing wires it today (mirrors `eg-stream`'s `stream.event.append`).
    fn cdc_topic(&self) -> Option<&'static str> {
        Some("tsdb.series.append")
    }

    /// Series metadata has no derivation history of its own (it is either asserted
    /// by an ingest pipeline or accumulated by appends) — default `None` is correct
    /// as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// No located-evidence concept applies to bare series metadata — default `None`.
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        None
    }

    /// The crate's real native TS query primitives (`crate::query`), listed here
    /// exactly like `eg-tensor`/`eg-geo` list their real ops — not a placeholder.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "time_bucket",
            "asof_join_backward",
            "gap_fill_locf",
            "ohlc_bars",
            "downsample",
            "decay_weighted_mean",
        ]
    }

    // ── EG-P1-1 hooks — real, minimal implementations over SeriesMeta's
    // serialization path and txn staging. ──

    /// Batch ingest = parse a `SeriesMeta` back from its serialized form. Streaming
    /// is N/A: series metadata is a scalar, not an append stream (the series POINTS
    /// are appended; the metadata describes them, not the stream itself).
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<SeriesMeta>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "series metadata is a scalar container descriptor, not an append stream (points are appended under it)",
            ),
        }
    }

    /// Real storage stats from the serialized metadata: logical size from encoded
    /// length; element count is the number of fields in the series. Series metadata
    /// is not secondary-indexed (the points are, by time), so `has_secondary_index`
    /// is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: self.count,
            has_secondary_index: false,
        })
    }

    /// N/A: SeriesMeta is metadata, not a durable codec. Durability is maintained
    /// by the underlying store layer (SERIES_META table in redb or equivalent).
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "series metadata is a schema descriptor, not a durable value itself — durability is maintained by the SERIES_META store layer",
        )
    }

    /// Simulated single-node crash-and-recover through the txn staging path.
    /// Stage the metadata as an in-txn write; the staged payload IS the WAL record;
    /// on "restart" replay-decode it and confirm the recovered metadata is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<SeriesMeta>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Series metadata has no CDC, policy, or provenance of its own: CDC is for the
    /// actual points appended under this metadata; policy is at the graph-node layer;
    /// provenance is either asserted or implicit in the ingest pipeline, not in the schema.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "CDC applies to series points, not metadata — the metadata container is immutable once created; point append/delete is CDC-observable separately",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the series",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "series metadata has no derivation history — it is either asserted at series creation or implicit in the ingest pipeline",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for SeriesMeta {
    fn conformance_sample() -> Self {
        SeriesMeta {
            n_fields: 2,
            bucket_ns: 3_600_000_000_000, // 1h buckets
            field_names: vec!["price".to_string(), "volume".to_string()],
            count: 42,
            min_ts: 1_700_000_000_000_000_000,
            max_ts: 1_700_000_600_000_000_000,
        }
    }
}

eg_modality::modality_conformance_tests!(SeriesMeta);

impl ModalityContract for Span {
    fn storage_kind(&self) -> &'static str {
        "trace"
    }

    /// A span is a SOURCE-shaped candidate (keyed by `span_id` within a trace), not
    /// intrinsically ranked — unranked, exactly like `SeriesMeta`.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A span arrives via OTLP-JSON ingest into the in-memory `SpanStore` (see
    /// `crate::traces` module docs), not the engine's own txn/write path — not (yet)
    /// on the CDC/streaming surface, mirroring `SeriesMeta`'s real `Some` being the
    /// exception rather than the rule for this crate's non-store values.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// Series metadata (above) has no derivation history; a span doesn't either — an
    /// observed span is asserted-by-observation, not derived. Default `None` is
    /// correct as-is; no override needed.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    fn evidence_address(&self) -> Option<EvidenceAddress> {
        Some(EvidenceAddress::TraceSpan {
            trace_ref: OpaqueRef::scoped("trace", &self.trace_id).ok()?,
            span_ref: OpaqueRef::scoped("span", &self.span_id).ok()?,
        })
    }

    // ── EG-P1-1 hooks — real, minimal implementations over Span's serialization
    // and txn staging. Span is an observed distributed-trace record. ──

    /// Batch ingest = parse a `Span` back from its serialized form. Streaming is N/A:
    /// a single span is a whole unit, not an append stream (the trace as a whole may be
    /// streamed in, but individual spans are atomic).
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<Span>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a span is a whole observed unit; tracing streams are reconstructed from individual spans",
            ),
        }
    }

    /// Real storage stats from the serialized Span: logical size from encoded length;
    /// element count is 1 (a single span is one unit). Spans are not secondary-indexed
    /// (query is by trace_id/span_id), so `has_secondary_index` is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: 1,
            has_secondary_index: false,
        })
    }

    /// N/A: Span is an ingest-time observation, not a backed-up or migrated value.
    /// Once written to the trace store, durability is maintained by that store layer.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "a span is an observed distributed-trace record; backup/restore/migrate is a trace-store-layer concern, not a modality-value capability",
        )
    }

    /// Simulated single-node crash-and-recover through the txn staging path.
    /// Stage the span as an in-txn write; the staged payload IS the WAL record;
    /// on "restart" replay-decode it and confirm the recovered span is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<Span>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Spans have no CDC, policy, or provenance of their own: CDC is implicit in
    /// OTLP ingest; policy is at the graph-node layer; provenance is the trace
    /// itself (already captured in trace_id/span_id/parent_span_id).
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "CDC is implicit in OTLP ingest; span observation is append-only and immutable at the trace-store level",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the span",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "a span's own trace_id/span_id/parent_span_id IS the lineage (already captured in the span itself)",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for Span {
    fn conformance_sample() -> Self {
        Span {
            trace_id: "0123456789abcdef0123456789abcdef".to_string(),
            span_id: "0123456789abcdef".to_string(),
            parent_span_id: String::new(),
            service: "gateway".to_string(),
            operation: "GET /".to_string(),
            start_time: 1_700_000_000_000_000_000,
            duration: 500_000,
            status: "OK".to_string(),
            attributes: Default::default(),
            events: Vec::new(),
        }
    }
}

// `modality_conformance_tests!` expands to a FIXED `mod eg_modality_conformance`
// name — nested here (rather than invoked a second time at this file's top level,
// where `SeriesMeta`'s invocation above already claims that name) so the two
// batteries don't collide (CONCEPT:E4/X1).
mod span_conformance {
    // The macro's generated conformance battery is itself `#[cfg(test)]`-gated (it
    // expands to nothing in a non-test build), so `Span` has no real consumer
    // outside `cfg(test)` here — gate the import the same way to avoid an
    // "unused import" warning under a plain `cargo build`/`clippy` (no `--tests`).
    #[cfg(test)]
    use super::Span;

    eg_modality::modality_conformance_tests!(Span);
}

// A direct test of the governed evidence-address mapping itself.
#[cfg(test)]
mod span_evidence {
    use super::*;

    #[test]
    fn maps_trace_and_span_id_losslessly() {
        let span = Span::conformance_sample();
        let ev = span
            .evidence_address()
            .expect("an observed Span always has evidence");
        assert_eq!(
            ev,
            EvidenceAddress::TraceSpan {
                trace_ref: OpaqueRef::scoped("trace", "0123456789abcdef0123456789abcdef").unwrap(),
                span_ref: OpaqueRef::scoped("span", "0123456789abcdef").unwrap(),
            }
        );
    }
}
