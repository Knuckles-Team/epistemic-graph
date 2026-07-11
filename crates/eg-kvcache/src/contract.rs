//! `ModalityContract` retrofit for [`StoredBlock`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s README for the retrofit-order rationale: "`eg-kvcache` (a cache
//! leaf — `cdc_topic` is almost certainly `None`, `analytics_ops` empty)". This is the
//! one pilot in the retrofit set expected to need NO overrides beyond the 4 core
//! methods — `provenance`/`evidence`/`policy_labels`/`analytics_ops` all stay at their
//! trait defaults, exactly as the README anticipates: a WARM-tier compressed KV block
//! has no derivation history, no located-evidence concept, no embedded policy labels
//! and no analytics ops of its own beyond store/retrieve.
//!
//! `StoredBlock`/`Codec` only derive `serde::Serialize`/`Deserialize` under this same
//! `contract` feature (see `src/compress.rs`), so the base Pi-lean build (no
//! dependencies at all) is completely unaffected.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, IngestReport, ModalityContract,
    ModalitySelfTest, RowSetShape, StagedWrite, StorageStats, TckPoint,
};

use crate::compress::StoredBlock;

impl ModalityContract for StoredBlock {
    fn storage_kind(&self) -> &'static str {
        "kvcache"
    }

    /// A cached KV block has no query-relative rank of its own — unranked, exactly
    /// like an unranked FILTER/TRAVERSE `RowSet` awaiting a RANK op.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A cache leaf — not (yet) on the CDC/streaming surface, and unlikely ever to be
    /// (a demote/promote is an internal tiering move, not a durable write a downstream
    /// consumer would subscribe to).
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    // ── EG-P1-1 hooks — minimal TCK implementations. ──

    /// Batch ingest = round-trip through serialization.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<StoredBlock>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable("a cached block is not a stream"),
        }
    }

    /// Real storage stats: serialized size; element count is 1 (a single block).
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        Some(StorageStats {
            logical_bytes: encode_staged(self).len() as u64,
            element_count: 1,
            has_secondary_index: false,
        })
    }

    /// Simulated crash-and-recover.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<StoredBlock>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// N/A: cache block durability is tiering-layer concern.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "a cached block is ephemeral; durability is maintained by the tiering layer",
        )
    }

    /// No CDC or policy.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some("cache blocks are ephemeral tiers"),
            TckPoint::TenantRowRegionPolicy => Some("policy at table layer"),
            _ => None,
        }
    }
}

impl ConformanceTestable for StoredBlock {
    fn conformance_sample() -> Self {
        StoredBlock::encode(b"hello world, this is a test block of bytes")
    }
}

eg_modality::modality_conformance_tests!(StoredBlock);
