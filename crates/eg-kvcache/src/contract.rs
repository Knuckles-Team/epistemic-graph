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
    encode_staged, ConformanceTestable, ModalityContract, RowSetShape, StagedWrite,
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
}

impl ConformanceTestable for StoredBlock {
    fn conformance_sample() -> Self {
        StoredBlock::encode(b"hello world, this is a test block of bytes")
    }
}

eg_modality::modality_conformance_tests!(StoredBlock);
