//! `ModalityContract` retrofit for [`LakeBatch`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s README for the retrofit-order rationale: "`eg-lake` (Parquet/Delta —
//! `txn_stage` maps onto its async materialization tier)".
//!
//! `LakeBatch` lives in `schema.rs`, which is pure serde and NOT gated behind the
//! `lake` feature (only the real Parquet/Avro transcode in `parquet_io`/`iceberg_avro`
//! is), so this retrofit needs no extra dependency beyond the optional `eg-modality`
//! path-dep. `provenance`/`evidence`/`policy_labels` are left at their trait defaults —
//! a raw table batch has no derivation history, no located-evidence concept and no
//! embedded policy labels of its own; those are exactly the "nothing to report" cases
//! the trait's module docs describe.

use eg_modality::{
    encode_staged, ConformanceTestable, ModalityContract, RowSetShape, StagedWrite,
};

use crate::schema::{CellValue, LakeBatch, LakeField, LakeSchema, LakeType};

impl ModalityContract for LakeBatch {
    fn storage_kind(&self) -> &'static str {
        "lake"
    }

    /// A table/columnar batch has no single query-relative score of its own (that is
    /// a downstream analytical query's business, e.g. a Spark/Trino aggregate over the
    /// materialized Parquet) — unranked, exactly like an unranked FILTER/TRAVERSE
    /// `RowSet` awaiting a RANK op.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    /// Maps onto the crate's async materialization tier (per `eg-modality`'s README
    /// retrofit-order note: "`eg-lake` (Parquet/Delta — `txn_stage` maps onto its async
    /// materialization tier)") — the engine-side seam documented in `lib.rs` that drains
    /// the WAL up to an LSN and transcodes a batch into Parquet + Delta/Iceberg logs.
    /// This method only DESCRIBES the write (a `Put` of the batch's own serialized
    /// payload); actually running that async tier is `LakeTable::materialize`'s job.
    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// Materialization is a batch/async tier, not itself a live CDC-observable write
    /// path today — a future Delta/Iceberg commit-event connector could change that,
    /// but nothing wires it yet.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// The crate's real top-level entry points across the Parquet/Delta/Iceberg
    /// interop seams (see `lib.rs` module docs): the Parquet write
    /// (`materialize_with_column_stats`, the function `LakeTable::materialize` calls),
    /// the Delta log append (`build_delta_log`) and the Iceberg metadata build
    /// (`build_iceberg`).
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec![
            "materialize_with_column_stats",
            "build_delta_log",
            "build_iceberg",
        ]
    }
}

impl ConformanceTestable for LakeBatch {
    fn conformance_sample() -> Self {
        let schema = LakeSchema::new(vec![
            LakeField::new("id", LakeType::Long),
            LakeField::new("label", LakeType::String),
        ]);
        let rows = vec![
            vec![CellValue::Long(1), CellValue::String("alpha".to_string())],
            vec![CellValue::Long(2), CellValue::String("beta".to_string())],
        ];
        LakeBatch::new(schema, rows).expect("2 columns x 2 rows matches the schema arity")
    }
}

eg_modality::modality_conformance_tests!(LakeBatch);
