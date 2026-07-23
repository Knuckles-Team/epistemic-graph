//! Pure wire types for governed document/image/audio/video serving.
//!
//! Concrete artifact, runtime, and policy types deliberately live above `eg-types`.
//! The wire carries opaque identifiers, a certified bundle encoded as MessagePack,
//! and ephemeral source bytes. Source bytes are authenticated as part of the request
//! body but are never copied into a durable mutation receipt.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServedModalityKind {
    Document,
    Image,
    Audio,
    Video,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServedSegmentKind {
    Page,
    Paragraph,
    Table,
    Row,
    Region,
    AudioRange,
    VideoShot,
    FrameRange,
    TimeWindow,
    CodeSymbol,
    TraceSpan,
}

/// Native, index-backed query predicates. Document text is ephemeral request data;
/// the server converts it to an authority-keyed lexeme before index access and never
/// includes it in cursors, logs, receipts, or persisted state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "predicate", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServedNativePredicate {
    DocumentLexical {
        term: String,
        #[serde(default)]
        page: Option<u32>,
    },
    ImageRegion {
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    ImagePerceptualHash {
        hash: u64,
        maximum_distance: u8,
    },
    AudioWindow {
        start_ms: u64,
        end_ms: u64,
        minimum_rms: f32,
    },
    VideoWindow {
        start_ms: u64,
        end_ms: u64,
        keyframes_only: bool,
    },
}

/// One element of an atomic, bounded served-modality ingest stream. Every item uses
/// the enclosing operation's modality and is decoded before any durable state changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServedModalityIngestItem {
    pub idempotency_ref: String,
    pub target_occurrence_id: String,
    #[serde(default)]
    pub expected_version: Option<u64>,
    #[serde(with = "serde_bytes")]
    pub bundle_msgpack: Vec<u8>,
    /// Ephemeral decoder input; excluded from receipts, snapshots, events, and indexes.
    #[serde(with = "serde_bytes")]
    pub source_bytes: Vec<u8>,
}

/// One operation on the graph-scoped governed modality runtime.
///
/// Query authority is intentionally absent from this DTO. The server derives tenant,
/// access-policy, purpose, and classification scope from the cryptographically
/// verified request context, so a caller cannot widen its own RLS boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ServedModalityOp {
    /// Return the opaque policy references derived for this verified request. This
    /// lets a producer construct a matching certified `ArtifactBundle` without any
    /// deployment-specific identifiers or paths in configuration.
    Authority,
    /// Decode the concrete native format, validate that it matches the certified
    /// bundle, and atomically create or update one occurrence.
    Ingest {
        modality: ServedModalityKind,
        idempotency_ref: String,
        target_occurrence_id: String,
        #[serde(default)]
        expected_version: Option<u64>,
        #[serde(with = "serde_bytes")]
        bundle_msgpack: Vec<u8>,
        /// Ephemeral source bytes. The runtime derives only normalized metadata and a
        /// content address; these bytes are omitted from snapshots, audit, CDC, and
        /// MutationBatch status/outbox records.
        #[serde(with = "serde_bytes")]
        source_bytes: Vec<u8>,
    },
    /// Decode and apply two or more records as one atomic bounded stream. A decode,
    /// authority, OCC, or resource failure rolls back every item and emits no event.
    IngestStream {
        modality: ServedModalityKind,
        items: Vec<ServedModalityIngestItem>,
    },
    Query {
        modality: ServedModalityKind,
        #[serde(default)]
        segment_kind: Option<ServedSegmentKind>,
        #[serde(default)]
        after_occurrence_id: Option<String>,
        #[serde(default)]
        limit: usize,
        #[serde(default)]
        include_cold: bool,
    },
    NativeQuery {
        predicate: ServedNativePredicate,
        #[serde(default)]
        after_occurrence_id: Option<String>,
        #[serde(default)]
        limit: usize,
        #[serde(default)]
        include_cold: bool,
    },
    Delete {
        modality: ServedModalityKind,
        idempotency_ref: String,
        occurrence_id: String,
        expected_version: u64,
    },
    MoveToCold {
        modality: ServedModalityKind,
        occurrence_id: String,
    },
    Restore {
        modality: ServedModalityKind,
        occurrence_id: String,
    },
    Events {
        modality: ServedModalityKind,
        #[serde(default)]
        after_sequence: u64,
        #[serde(default)]
        limit: usize,
    },
    /// Return live, authority-partitioned storage/index/event counts. No identifier,
    /// source byte, query text, path, or deployment field is included.
    Stats { modality: ServedModalityKind },
    /// Physically collect authorized tombstoned records after retention has been
    /// demonstrated. The durable idempotency/event ledger remains append-only.
    CollectTombstones {
        modality: ServedModalityKind,
        /// Inclusive durable event fence. Tombstones newer than this checkpoint are
        /// retained, so collection cannot race an unobserved delete.
        through_event_sequence: u64,
    },
    /// Return the concrete leaf component's TCK summary. This is deliberately not a
    /// release-readiness claim; G-14 derives that only from executable behavior plus
    /// same-artifact performance evidence.
    Capabilities { modality: ServedModalityKind },
}

impl ServedModalityOp {
    pub fn mutates(&self) -> bool {
        matches!(
            self,
            Self::Ingest { .. }
                | Self::IngestStream { .. }
                | Self::Delete { .. }
                | Self::MoveToCold { .. }
                | Self::Restore { .. }
                | Self::CollectTombstones { .. }
        )
    }

    pub fn is_idempotent_mutation(&self) -> bool {
        matches!(
            self,
            Self::Ingest { .. } | Self::IngestStream { .. } | Self::Delete { .. }
        )
    }
}
