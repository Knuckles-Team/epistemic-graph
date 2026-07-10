//! `EvidenceSpan` — the X1 seam.
//!
//! **This type defines a shape; it does NOT implement anything.** X1 (the
//! multimodal-evidence exceed item) needs a way for E3's `KnowledgeSet`
//! `evidence_refs` to point at a LOCATED span inside a source artifact — not just
//! "this claim came from document 7", but "this claim came from characters 120..340
//! of document 7", or "row 4, columns B..D of table 2", or "the region
//! (x=10,y=20,w=100,h=80) of image 3", etc. `ModalityContract::evidence` is the
//! default-unused hook where a modality that DOES track located evidence (a future
//! document/table/image/audio/video/code/trace modality) would return one of these.
//!
//! Resolving an `EvidenceSpan` back into rendered content (e.g. re-extracting the
//! text at a `DocumentSpan`, or cropping the pixels at an `ImageRegion`) is
//! explicitly OUT of scope here — that is X1's resolver work. This is only the wire
//! shape the resolvers will eventually consume, defined now so E3's `KnowledgeSet`
//! can start carrying `evidence_refs: Vec<EvidenceSpan>` (or similar) without a
//! breaking change later.
//!
//! Pure serde, no behavior, no new dependency — a leaf enum like every other
//! `eg-modality` DTO.

use serde::{Deserialize, Serialize};

/// A located reference into a source artifact, precise enough for a future resolver
/// to re-extract exactly the evidence a claim was derived from. One variant per
/// evidence-bearing modality X1 is expected to cover.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EvidenceSpan {
    /// A character range `[start, end)` inside a text document.
    DocumentSpan {
        document_id: String,
        start: usize,
        end: usize,
    },
    /// A rectangular cell range inside a tabular source (inclusive bounds).
    TableCellRange {
        table_id: String,
        row_start: usize,
        row_end: usize,
        col_start: usize,
        col_end: usize,
    },
    /// A pixel-space rectangular region of an image.
    ImageRegion {
        image_id: String,
        x: f64,
        y: f64,
        width: f64,
        height: f64,
    },
    /// A time range (milliseconds) inside an audio recording.
    AudioSegment {
        audio_id: String,
        start_ms: u64,
        end_ms: u64,
    },
    /// A shot boundary time range (milliseconds) inside a video.
    VideoShot {
        video_id: String,
        start_ms: u64,
        end_ms: u64,
    },
    /// A named symbol (function/class/etc.) inside a source file, by line range.
    CodeSymbol {
        file_path: String,
        symbol: String,
        start_line: u32,
        end_line: u32,
    },
    /// A distributed-tracing span (mirrors `eg-tsdb`'s `traces` module's span model),
    /// for evidence whose provenance is "observed during this trace span" rather
    /// than a static artifact.
    TraceSpan { trace_id: String, span_id: String },
}
