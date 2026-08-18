//! Native realtime audio-ingress contract (GOC-32, `OWNER-VOICE-INGRESS`).
//!
//! This module is the **versioned wire contract plus the pure in-process ingress
//! runtime** that a first-party voice worker (or, for now, any embedding caller)
//! uses to accept browser/native-device audio frames, apply monotonic
//! sequence/watermark bookkeeping, and bound memory under a slow or absent
//! consumer. It intentionally contains **no** device capture, codec decode,
//! network transport, CAS I/O, or graph mutation — those are GOC-32-W03..W06
//! concerns layered on top of this contract by the (not-yet-built)
//! `epistemic-graph-voice-worker` process. See the crate-level and module docs
//! below for exactly what is and is not proven here.
//!
//! ## Authority boundary
//!
//! Per `plans/graph-os-completion-program/lanes/GOC-32-native-realtime-audio-
//! ingress.md`: EG is authoritative for stream identity, sequence/watermark
//! state, and checkpoint manifests; the bounded media plane is authoritative for
//! bytes *between* checkpoints only. Nothing in this module acknowledges a
//! frame as a durable fact — [`IngressSession::accept`] only ever admits a frame
//! into a bounded in-memory queue or rejects it with a typed [`IngressError`].
//! Durable commit (GOC-03 fence, GOC-04 rows, CAS chunk refs) is out of scope
//! for this module and is not claimed anywhere here.
//!
//! ## What is proven here (with tests in this file)
//!
//! - A malformed or truncated frame (oversized payload, payload/sample-count
//!   mismatch, tampered bytes whose digest does not match, a frame from a
//!   stale/fenced generation) is rejected with a bounded, typed
//!   [`IngressError`] and leaves the session's sequence tracker and queue
//!   **byte-for-byte unchanged** — see `accepting_a_frame_never_mutates_state_
//!   on_rejection`.
//! - A slow/absent consumer cannot grow ingress memory without bound: the
//!   admission queue enforces an independent frame-count cap **and** a
//!   cumulative-byte cap; once either is reached, every further push fails with
//!   `IngressError::Backpressure` and the queue's own accounting never exceeds
//!   the configured limits, no matter how many further pushes are attempted —
//!   see `slow_consumer_cannot_grow_queue_past_its_bound`.
//! - Reorder within a bounded jitter window is tolerated; a duplicate
//!   `(sequence, digest)` is idempotently ignored; the same sequence with a
//!   different digest is a typed conflict; a sequence beyond the reorder
//!   window is reported as an explicit gap — never silently renumbered or
//!   hidden. See the `sequence_tracker` test module.
//! - The sequence watermark only ever advances for a frame that was *also*
//!   successfully queued — a backpressure rejection can never advance the
//!   watermark past bytes that were not actually admitted, so a degraded
//!   stream can never be reported complete by construction (see
//!   `backpressure_rejection_does_not_advance_watermark`).
//!
//! ## What is explicitly NOT proven or claimed here
//!
//! No browser, WebRTC, cpal/native-device, worker-process, CAS, or GOC-03/04
//! commit path exists yet. No SLO, resource, or hardware evidence is claimed.
//! Downstream lanes (GOC-33 ASR, GOC-34 TTS, GOC-35 full-duplex) should treat
//! this module as the frozen **type-level** ingress contract only.

use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Current wire/behavioral version of the `audio.ingress.*` contract family.
/// A caller that does not understand this major version must refuse before
/// capture, per the lane contract's schema-compatibility rule.
pub const INGRESS_PROTOCOL_VERSION: u16 = 1;

const MIN_TOKEN_LEN: usize = 1;
const MAX_TOKEN_LEN: usize = 128;

/// A bounded, validated wire token (stream/idempotency/chunk identity, carrier
/// fields). This is a **transport-layer** identity, deliberately smaller and
/// dependency-free compared to `eg_modality::OpaqueRef` (this crate's `ingress`
/// feature adds no dependency on `eg-modality`). A durable committer (GOC-03/
/// GOC-04/GOC-05) is expected to mint a real governed `OpaqueRef` from a
/// `BoundedId` when it persists a checkpoint or manifest; this type only
/// guarantees the token is non-empty, bounded, and free of raw separators.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BoundedId(String);

impl BoundedId {
    pub fn new(value: impl Into<String>) -> Result<Self, IngressError> {
        let value = value.into();
        let valid = (MIN_TOKEN_LEN..=MAX_TOKEN_LEN).contains(&value.len())
            && value
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-' || b == b':');
        if !valid {
            return Err(IngressError::MalformedFrame {
                reason: "invalid bounded identity token",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub type StreamId = BoundedId;
pub type IdempotencyKey = BoundedId;
pub type ChunkRef = BoundedId;

/// The verified-identity/policy reference every ingress request must carry
/// (GOC-15/GOC-16). This module never authenticates or authorizes a carrier —
/// it only records the shape a verified carrier is expected to fill in. A
/// caller wiring this to a real deployment MUST verify these fields through
/// the GOC-15 identity boundary before constructing one; nothing here trusts
/// the fields at face value.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CarrierRef {
    pub tenant: BoundedId,
    pub actor: BoundedId,
    pub consent_ref: BoundedId,
    pub purpose: BoundedId,
    pub trace_id: BoundedId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceCapability {
    Microphone,
    System,
    Mixed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransportKind {
    WebRtcOpus,
    WebSocketPcmFallback,
    NativeUds,
}

/// Ingress accepts exactly these codecs. Anything else is
/// `IngressError::UnsupportedCodec` before capture, never a best-effort decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Codec {
    Pcm16Le,
    Opus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamGeneration(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Sequence(pub u64);

/// Bounds a stream must declare before capture (`audio.ingress.start.v1`).
/// These are policy inputs, not claims — see the lane doc's "Algorithmic and
/// resource budget" section for suggested defaults; nothing here hardcodes a
/// default because the qualified value is a deployment/W07 concern.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameLimits {
    pub max_payload_bytes: u32,
    pub max_inflight_frames: u32,
    pub max_uncommitted_bytes: u64,
    /// Frames tolerated ahead of the current watermark before a gap is
    /// reported (the bounded reorder/jitter window).
    pub reorder_window: u32,
}

/// `audio.ingress.start.v1`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressStartRequest {
    pub stream_id: StreamId,
    pub carrier: CarrierRef,
    pub source_capability: SourceCapability,
    pub transport: TransportKind,
    pub codec: Codec,
    pub sample_rate: u32,
    pub channels: u16,
    pub generation: StreamGeneration,
    pub limits: FrameLimits,
    pub idempotency_key: IdempotencyKey,
}

/// Frame payload is either bounded inline bytes or a reference to bytes
/// already committed to CAS by an earlier stage. Ingress never buffers a
/// `ChunkRef` frame's bytes a second time.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FramePayload {
    Inline(Vec<u8>),
    ChunkRef(ChunkRef),
}

/// `audio.frame.v1` — a transport receipt, never an EG mutation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AudioFrame {
    pub stream_id: StreamId,
    pub generation: StreamGeneration,
    pub sequence: Sequence,
    /// Client-reported capture time. Informational only — **never** treated
    /// as authoritative ordering or durability evidence by this module.
    pub capture_time_ms: u64,
    pub codec: Codec,
    pub payload: FramePayload,
    pub sample_count: u32,
    pub channel_layout: u16,
    pub discontinuity: bool,
    /// Lowercase hex SHA-256 over the inline payload bytes (64 chars). Must
    /// match the actual bytes for an `Inline` payload; ignored for
    /// `ChunkRef` (already content-addressed at commit time).
    pub payload_digest: String,
}

/// `audio.control.v1`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlFrame {
    Pause {
        generation: StreamGeneration,
        cursor: Sequence,
    },
    Resume {
        generation: StreamGeneration,
        cursor: Sequence,
    },
    Flush {
        generation: StreamGeneration,
    },
    Cancel {
        generation: StreamGeneration,
        reason: CancelReason,
    },
    Reconnect {
        generation: StreamGeneration,
        resume_from: Sequence,
    },
    Finalize {
        generation: StreamGeneration,
    },
}

/// A closed set of cancel reasons — never a freeform string, so a cancel
/// control frame cannot smuggle arbitrary/raw content through logs or
/// telemetry (see the lane doc's "opaque reason codes" requirement).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CancelReason {
    ClientRequested,
    ConsentRevoked,
    PolicyDenied,
    Expired,
    TransportLost,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualitySummary {
    pub gap_count: u32,
    pub dropped_frame_count: u32,
    pub degraded: bool,
}

/// `audio.checkpoint.v1`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub stream_id: StreamId,
    pub generation: StreamGeneration,
    pub watermark: Sequence,
    pub chunk_refs: Vec<ChunkRef>,
    pub byte_total: u64,
    pub duration_ms_total: u64,
    pub quality: QualitySummary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestStatus {
    Complete,
    Degraded,
    Cancelled,
}

/// `audio.manifest.v1`
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub stream_id: StreamId,
    pub generation: StreamGeneration,
    pub codec: Codec,
    pub sample_rate: u32,
    pub channels: u16,
    pub checkpoints: Vec<Checkpoint>,
    pub status: ManifestStatus,
    /// Lowercase hex SHA-256 over the checkpoint list, giving a stable
    /// content identity for the manifest as a whole.
    pub content_digest: String,
}

/// `audio.error.v1` — a typed, bounded diagnostic. No variant carries raw
/// audio, a path, a device name, transcript text, or a credential.
// NOTE: `Serialize` only (not `Deserialize`) — the `reason`/`limit` fields are
// `&'static str` so this crate can only ever construct one from its own closed
// set of literals (no arbitrary/raw content can enter a diagnostic); a
// `&'static str` field cannot soundly implement `Deserialize<'de>` for an
// arbitrary borrowed input lifetime, and an error type has no reason to be
// deserialized as an inbound request in the first place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum IngressError {
    PermissionDenied,
    UnsupportedCodec,
    Backpressure,
    Gap {
        expected: Sequence,
        got: Sequence,
    },
    Conflict {
        sequence: Sequence,
    },
    PolicyDenied,
    /// The frame's generation does not match the stream's currently fenced
    /// generation (a stale/pre-reconnect frame).
    Expired,
    ResourceExhausted {
        limit: &'static str,
    },
    MalformedFrame {
        reason: &'static str,
    },
    Unavailable,
}

/// Result of admitting a sequence into the tracker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Admission {
    New,
    DuplicateIgnored,
}

fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn is_valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Bounded shape/integrity validation for one frame. Pure — never mutates
/// tracker or queue state, so a caller can validate before deciding whether
/// to admit. Returns the first bounded, typed violation found.
pub fn validate_frame(
    frame: &AudioFrame,
    limits: &FrameLimits,
    active_generation: StreamGeneration,
) -> Result<(), IngressError> {
    if frame.generation != active_generation {
        return Err(IngressError::Expired);
    }
    match &frame.payload {
        FramePayload::Inline(bytes) => {
            if bytes.is_empty() && !frame.discontinuity {
                return Err(IngressError::MalformedFrame {
                    reason: "empty payload on a non-discontinuity frame",
                });
            }
            if bytes.len() as u64 > u64::from(limits.max_payload_bytes) {
                return Err(IngressError::ResourceExhausted {
                    limit: "max_payload_bytes",
                });
            }
            if !is_valid_digest(&frame.payload_digest) {
                return Err(IngressError::MalformedFrame {
                    reason: "payload_digest is not a 64-char lowercase hex sha256",
                });
            }
            if hex_digest(bytes) != frame.payload_digest {
                return Err(IngressError::MalformedFrame {
                    reason: "payload_digest does not match payload bytes",
                });
            }
            if frame.codec == Codec::Pcm16Le && !bytes.is_empty() {
                let channels = u64::from(frame.channel_layout.max(1));
                let expected = u64::from(frame.sample_count)
                    .checked_mul(channels)
                    .and_then(|v| v.checked_mul(2))
                    .ok_or(IngressError::MalformedFrame {
                        reason: "sample_count/channel_layout overflow",
                    })?;
                if expected != bytes.len() as u64 {
                    return Err(IngressError::MalformedFrame {
                        reason: "sample_count does not match pcm16 payload length",
                    });
                }
            }
        }
        FramePayload::ChunkRef(_) => {
            // Bytes were already validated at CAS-commit time; nothing to
            // re-check here beyond generation fencing above.
        }
    }
    Ok(())
}

/// Monotonic sequence/watermark tracker with a bounded reorder window.
///
/// Split into a pure `classify` (read-only) and a `commit` (mutating) step
/// deliberately: [`IngressSession::accept`] only calls `commit` once the frame
/// has *also* been durably admitted into the bounded queue, so a backpressure
/// rejection can never advance the watermark for bytes that were not actually
/// queued (see the crate-level "digested" claim above and the
/// `backpressure_rejection_does_not_advance_watermark` test).
#[derive(Clone, Debug)]
pub struct SequenceTracker {
    next_expected: u64,
    reorder_window: u32,
    // Digests for sequences >= next_expected already accepted but not yet
    // contiguous; bounded to at most `reorder_window` entries by construction
    // (classify rejects anything at/after next_expected + reorder_window).
    pending: BTreeMap<u64, String>,
}

impl SequenceTracker {
    pub fn new(start: Sequence, reorder_window: u32) -> Self {
        Self {
            next_expected: start.0,
            reorder_window,
            pending: BTreeMap::new(),
        }
    }

    /// The next sequence not yet contiguously admitted (i.e. one past the
    /// last committed watermark).
    pub fn next_expected(&self) -> u64 {
        self.next_expected
    }

    /// Read-only: classifies `sequence` without mutating tracker state.
    pub fn classify(&self, sequence: Sequence, digest: &str) -> Result<Admission, IngressError> {
        let seq = sequence.0;
        if seq < self.next_expected {
            // Already advanced past this sequence. Digests for committed
            // sequences are not retained (bounded memory) so a replay of
            // already-durable bytes is treated as an idempotent duplicate
            // rather than re-verified here.
            return Ok(Admission::DuplicateIgnored);
        }
        if seq
            >= self
                .next_expected
                .saturating_add(u64::from(self.reorder_window))
        {
            return Err(IngressError::Gap {
                expected: Sequence(self.next_expected),
                got: sequence,
            });
        }
        match self.pending.get(&seq) {
            Some(existing) if existing == digest => Ok(Admission::DuplicateIgnored),
            Some(_) => Err(IngressError::Conflict { sequence }),
            None => Ok(Admission::New),
        }
    }

    /// Mutating: records `sequence` as admitted and advances the watermark
    /// over any now-contiguous run. Caller must only call this for a frame
    /// that has already been durably admitted into the bounded queue (or an
    /// equivalent durable sink) — never speculatively.
    pub fn commit(&mut self, sequence: Sequence, digest: String) {
        let seq = sequence.0;
        if seq < self.next_expected {
            return;
        }
        self.pending.insert(seq, digest);
        while self.pending.contains_key(&self.next_expected) {
            self.pending.remove(&self.next_expected);
            self.next_expected += 1;
        }
    }
}

/// A fixed-capacity admission queue: an independent frame-count cap **and** a
/// cumulative-byte cap. Both are enforced on every push, so a slow or absent
/// consumer can never grow this queue's memory past
/// `max_inflight_frames * max_frame_size` and `max_uncommitted_bytes` —
/// pushes beyond either bound fail closed with `IngressError::Backpressure`
/// rather than growing the buffer or silently dropping the new frame as if it
/// had been accepted.
#[derive(Debug)]
pub struct BoundedIngressQueue {
    max_frames: u32,
    max_bytes: u64,
    frames: VecDeque<AudioFrame>,
    bytes: u64,
}

impl BoundedIngressQueue {
    pub fn new(limits: &FrameLimits) -> Self {
        Self {
            max_frames: limits.max_inflight_frames,
            max_bytes: limits.max_uncommitted_bytes,
            frames: VecDeque::new(),
            bytes: 0,
        }
    }

    fn frame_bytes(frame: &AudioFrame) -> u64 {
        match &frame.payload {
            FramePayload::Inline(bytes) => bytes.len() as u64,
            // Already durable in CAS; this bounded ingress buffer does not
            // hold a second copy.
            FramePayload::ChunkRef(_) => 0,
        }
    }

    pub fn try_push(&mut self, frame: AudioFrame) -> Result<(), IngressError> {
        if self.frames.len() as u32 >= self.max_frames {
            return Err(IngressError::Backpressure);
        }
        let size = Self::frame_bytes(&frame);
        if self.bytes.saturating_add(size) > self.max_bytes {
            return Err(IngressError::Backpressure);
        }
        self.bytes += size;
        self.frames.push_back(frame);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<AudioFrame> {
        let frame = self.frames.pop_front()?;
        self.bytes = self.bytes.saturating_sub(Self::frame_bytes(&frame));
        Some(frame)
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

/// The single ingress entry point: validates one inbound frame, applies
/// sequence/watermark policy, and admits it into the bounded queue. This is
/// the type a first-party voice worker embeds per active stream generation;
/// it performs no I/O of its own.
#[derive(Debug)]
pub struct IngressSession {
    active_generation: StreamGeneration,
    limits: FrameLimits,
    tracker: SequenceTracker,
    queue: BoundedIngressQueue,
}

impl IngressSession {
    pub fn new(generation: StreamGeneration, start: Sequence, limits: FrameLimits) -> Self {
        Self {
            active_generation: generation,
            tracker: SequenceTracker::new(start, limits.reorder_window),
            queue: BoundedIngressQueue::new(&limits),
            limits,
        }
    }

    /// Accept exactly one inbound frame. On any rejection the session's
    /// sequence tracker and queue are left completely unchanged — a hostile
    /// or malformed frame can only ever return a typed [`IngressError`],
    /// never panic or corrupt in-flight state. The watermark advances only
    /// for a frame that was also successfully queued.
    pub fn accept(&mut self, frame: AudioFrame) -> Result<Admission, IngressError> {
        validate_frame(&frame, &self.limits, self.active_generation)?;
        let sequence = frame.sequence;
        let digest = frame.payload_digest.clone();
        let admission = self.tracker.classify(sequence, &digest)?;
        if matches!(admission, Admission::New) {
            self.queue.try_push(frame)?;
            self.tracker.commit(sequence, digest);
        }
        Ok(admission)
    }

    /// Fence to a new generation (reconnect/cancel). The tracker resets to
    /// `resume_from`; any frames still queued from the old generation must be
    /// drained or discarded by the caller before this returns control to a
    /// new capture source — this method does not implicitly drop them.
    pub fn fence_generation(
        &mut self,
        new_generation: StreamGeneration,
        resume_from: Sequence,
    ) -> BoundedIngressQueue {
        self.active_generation = new_generation;
        self.tracker = SequenceTracker::new(resume_from, self.limits.reorder_window);
        std::mem::replace(&mut self.queue, BoundedIngressQueue::new(&self.limits))
    }

    pub fn drain_one(&mut self) -> Option<AudioFrame> {
        self.queue.pop()
    }

    pub fn queued_frames(&self) -> usize {
        self.queue.len()
    }

    pub fn queued_bytes(&self) -> u64 {
        self.queue.bytes()
    }

    pub fn watermark(&self) -> u64 {
        self.tracker.next_expected()
    }

    pub fn active_generation(&self) -> StreamGeneration {
        self.active_generation
    }
}

#[cfg(test)]
mod fixtures {
    use super::*;

    pub fn id(value: &str) -> BoundedId {
        BoundedId::new(value).expect("fixture id is valid")
    }

    pub fn limits() -> FrameLimits {
        FrameLimits {
            max_payload_bytes: 64 * 1024,
            max_inflight_frames: 4,
            max_uncommitted_bytes: 4 * 1024,
            reorder_window: 4,
        }
    }

    pub fn pcm_frame(generation: u64, sequence: u64, samples: &[i16]) -> AudioFrame {
        let mut bytes = Vec::with_capacity(samples.len() * 2);
        for sample in samples {
            bytes.extend_from_slice(&sample.to_le_bytes());
        }
        let digest = hex_digest(&bytes);
        AudioFrame {
            stream_id: id("stream-1"),
            generation: StreamGeneration(generation),
            sequence: Sequence(sequence),
            capture_time_ms: 0,
            codec: Codec::Pcm16Le,
            payload: FramePayload::Inline(bytes),
            sample_count: samples.len() as u32,
            channel_layout: 1,
            discontinuity: false,
            payload_digest: digest,
        }
    }
}

#[cfg(test)]
mod validate_frame_tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn accepts_a_well_formed_pcm_frame() {
        let limits = limits();
        let frame = pcm_frame(1, 0, &[1, 2, 3, 4]);
        assert_eq!(validate_frame(&frame, &limits, StreamGeneration(1)), Ok(()));
    }

    #[test]
    fn rejects_a_frame_from_a_stale_generation() {
        let limits = limits();
        let frame = pcm_frame(1, 0, &[1, 2]);
        assert_eq!(
            validate_frame(&frame, &limits, StreamGeneration(2)),
            Err(IngressError::Expired)
        );
    }

    #[test]
    fn rejects_oversized_payload() {
        let mut limits = limits();
        limits.max_payload_bytes = 2; // smaller than the fixture's 8-byte payload
        let frame = pcm_frame(1, 0, &[1, 2, 3, 4]);
        assert_eq!(
            validate_frame(&frame, &limits, StreamGeneration(1)),
            Err(IngressError::ResourceExhausted {
                limit: "max_payload_bytes"
            })
        );
    }

    /// Known-bad input: the frame claims a `sample_count` that does not match
    /// the actual PCM payload length (a truncated/corrupted frame).
    #[test]
    fn rejects_truncated_pcm_frame() {
        let limits = limits();
        let mut frame = pcm_frame(1, 0, &[1, 2, 3, 4]);
        frame.sample_count = 100; // claims far more samples than the 4 present
        assert_eq!(
            validate_frame(&frame, &limits, StreamGeneration(1)),
            Err(IngressError::MalformedFrame {
                reason: "sample_count does not match pcm16 payload length"
            })
        );
    }

    /// Known-bad input: payload bytes were tampered with after the digest
    /// was computed (bit flip / corruption in transit).
    #[test]
    fn rejects_payload_that_does_not_match_its_digest() {
        let limits = limits();
        let mut frame = pcm_frame(1, 0, &[1, 2, 3, 4]);
        if let FramePayload::Inline(bytes) = &mut frame.payload {
            bytes[0] ^= 0xFF;
        }
        assert_eq!(
            validate_frame(&frame, &limits, StreamGeneration(1)),
            Err(IngressError::MalformedFrame {
                reason: "payload_digest does not match payload bytes"
            })
        );
    }

    #[test]
    fn rejects_malformed_digest_field() {
        let limits = limits();
        let mut frame = pcm_frame(1, 0, &[1, 2]);
        frame.payload_digest = "not-a-hex-digest".to_string();
        assert_eq!(
            validate_frame(&frame, &limits, StreamGeneration(1)),
            Err(IngressError::MalformedFrame {
                reason: "payload_digest is not a 64-char lowercase hex sha256"
            })
        );
    }

    #[test]
    fn rejects_empty_payload_on_a_non_discontinuity_frame() {
        let limits = limits();
        let mut frame = pcm_frame(1, 0, &[]);
        frame.discontinuity = false;
        assert_eq!(
            validate_frame(&frame, &limits, StreamGeneration(1)),
            Err(IngressError::MalformedFrame {
                reason: "empty payload on a non-discontinuity frame"
            })
        );
    }
}

#[cfg(test)]
mod sequence_tracker_tests {
    use super::*;

    #[test]
    fn admits_in_order_frames_and_advances_watermark() {
        let mut tracker = SequenceTracker::new(Sequence(0), 4);
        assert_eq!(tracker.classify(Sequence(0), "d0"), Ok(Admission::New));
        tracker.commit(Sequence(0), "d0".to_string());
        assert_eq!(tracker.next_expected(), 1);
    }

    #[test]
    fn tolerates_reorder_within_the_window() {
        let mut tracker = SequenceTracker::new(Sequence(0), 4);
        // sequence 2 arrives before sequence 0/1.
        assert_eq!(tracker.classify(Sequence(2), "d2"), Ok(Admission::New));
        tracker.commit(Sequence(2), "d2".to_string());
        assert_eq!(tracker.next_expected(), 0, "gap not yet filled");

        assert_eq!(tracker.classify(Sequence(0), "d0"), Ok(Admission::New));
        tracker.commit(Sequence(0), "d0".to_string());
        assert_eq!(tracker.next_expected(), 1);

        assert_eq!(tracker.classify(Sequence(1), "d1"), Ok(Admission::New));
        tracker.commit(Sequence(1), "d1".to_string());
        // 0,1,2 are now contiguous.
        assert_eq!(tracker.next_expected(), 3);
    }

    #[test]
    fn duplicate_same_digest_is_idempotently_ignored() {
        let mut tracker = SequenceTracker::new(Sequence(0), 4);
        tracker.commit(Sequence(0), "d0".to_string());
        assert_eq!(
            tracker.classify(Sequence(0), "d0"),
            Ok(Admission::DuplicateIgnored)
        );

        // Also a duplicate while still pending (ahead of the watermark).
        assert_eq!(tracker.classify(Sequence(3), "d3"), Ok(Admission::New));
        tracker.commit(Sequence(3), "d3".to_string());
        assert_eq!(
            tracker.classify(Sequence(3), "d3"),
            Ok(Admission::DuplicateIgnored)
        );
    }

    #[test]
    fn same_sequence_different_digest_is_a_typed_conflict() {
        let mut tracker = SequenceTracker::new(Sequence(0), 4);
        assert_eq!(tracker.classify(Sequence(2), "d2"), Ok(Admission::New));
        tracker.commit(Sequence(2), "d2".to_string());
        assert_eq!(
            tracker.classify(Sequence(2), "different-digest"),
            Err(IngressError::Conflict {
                sequence: Sequence(2)
            })
        );
    }

    #[test]
    fn sequence_beyond_the_reorder_window_is_a_visible_gap() {
        let tracker = SequenceTracker::new(Sequence(0), 4);
        assert_eq!(
            tracker.classify(Sequence(10), "d10"),
            Err(IngressError::Gap {
                expected: Sequence(0),
                got: Sequence(10),
            })
        );
    }
}

#[cfg(test)]
mod backpressure_tests {
    use super::fixtures::*;
    use super::*;

    /// Known-bad scenario: a consumer that never drains. Every push beyond
    /// the configured caps must fail, and the queue's own accounting must
    /// never exceed those caps no matter how many further pushes are
    /// attempted — the structural proof that a slow consumer cannot grow
    /// ingress memory without bound.
    #[test]
    fn slow_consumer_cannot_grow_queue_past_its_bound() {
        let limits = limits(); // max_inflight_frames = 4
        let mut queue = BoundedIngressQueue::new(&limits);

        for seq in 0..limits.max_inflight_frames {
            assert!(queue
                .try_push(pcm_frame(1, u64::from(seq), &[1, 2]))
                .is_ok());
        }
        assert_eq!(queue.len(), limits.max_inflight_frames as usize);

        // Nobody ever calls pop(). Hammer it with far more pushes than
        // capacity and assert the bound never moves.
        for seq in limits.max_inflight_frames..(limits.max_inflight_frames * 50) {
            let result = queue.try_push(pcm_frame(1, u64::from(seq), &[1, 2]));
            assert_eq!(result, Err(IngressError::Backpressure));
            assert!(queue.len() as u32 <= limits.max_inflight_frames);
            assert!(queue.bytes() <= limits.max_uncommitted_bytes);
        }
    }

    #[test]
    fn byte_budget_is_enforced_independently_of_frame_count() {
        let mut limits = limits();
        limits.max_inflight_frames = 1000; // effectively unbounded by count
        limits.max_uncommitted_bytes = 4; // 4 bytes total
        let mut queue = BoundedIngressQueue::new(&limits);

        // First 4-byte frame (2 i16 samples) exactly fills the byte budget.
        assert!(queue.try_push(pcm_frame(1, 0, &[1, 2])).is_ok());
        assert_eq!(queue.bytes(), 4);

        // A second frame of any size must be rejected on the byte budget.
        let result = queue.try_push(pcm_frame(1, 1, &[1, 2]));
        assert_eq!(result, Err(IngressError::Backpressure));
        assert_eq!(queue.bytes(), 4);
    }

    #[test]
    fn pop_frees_capacity_for_a_later_push() {
        let limits = limits();
        let mut queue = BoundedIngressQueue::new(&limits);
        for seq in 0..limits.max_inflight_frames {
            queue
                .try_push(pcm_frame(1, u64::from(seq), &[1, 2]))
                .expect("push under capacity succeeds");
        }
        assert_eq!(
            queue.try_push(pcm_frame(1, 99, &[1, 2])),
            Err(IngressError::Backpressure)
        );

        assert!(queue.pop().is_some());
        assert!(queue.try_push(pcm_frame(1, 99, &[1, 2])).is_ok());
    }
}

#[cfg(test)]
mod ingress_session_tests {
    use super::fixtures::*;
    use super::*;

    #[test]
    fn accepting_a_frame_never_mutates_state_on_rejection() {
        let mut session = IngressSession::new(StreamGeneration(1), Sequence(0), limits());
        let before_watermark = session.watermark();
        let before_frames = session.queued_frames();
        let before_bytes = session.queued_bytes();

        let mut bad = pcm_frame(1, 0, &[1, 2, 3, 4]);
        bad.sample_count = 999; // truncated/corrupted claim
        assert_eq!(
            session.accept(bad),
            Err(IngressError::MalformedFrame {
                reason: "sample_count does not match pcm16 payload length"
            })
        );

        assert_eq!(session.watermark(), before_watermark);
        assert_eq!(session.queued_frames(), before_frames);
        assert_eq!(session.queued_bytes(), before_bytes);

        let stale = pcm_frame(2, 0, &[1, 2]); // wrong generation
        assert_eq!(session.accept(stale), Err(IngressError::Expired));
        assert_eq!(session.watermark(), before_watermark);
        assert_eq!(session.queued_frames(), before_frames);
    }

    /// The watermark must only advance for frames that were also actually
    /// queued — a rejected/backpressured frame can never be reported as
    /// contiguous, bounded audio.
    #[test]
    fn backpressure_rejection_does_not_advance_watermark() {
        let mut limits = limits();
        limits.max_inflight_frames = 1;
        let mut session = IngressSession::new(StreamGeneration(1), Sequence(0), limits);

        assert_eq!(session.accept(pcm_frame(1, 0, &[1, 2])), Ok(Admission::New));
        assert_eq!(session.watermark(), 1);

        // Queue is now full (capacity 1) and nothing drains it.
        let result = session.accept(pcm_frame(1, 1, &[1, 2]));
        assert_eq!(result, Err(IngressError::Backpressure));
        // Watermark must NOT have advanced to 2 — sequence 1's bytes were
        // never actually admitted.
        assert_eq!(session.watermark(), 1);
        assert_eq!(session.queued_frames(), 1);
    }

    #[test]
    fn duplicate_frame_is_ignored_without_double_counting_bytes() {
        let mut session = IngressSession::new(StreamGeneration(1), Sequence(0), limits());
        assert_eq!(session.accept(pcm_frame(1, 0, &[1, 2])), Ok(Admission::New));
        let bytes_after_first = session.queued_bytes();
        assert_eq!(
            session.accept(pcm_frame(1, 0, &[1, 2])),
            Ok(Admission::DuplicateIgnored)
        );
        assert_eq!(session.queued_bytes(), bytes_after_first);
        assert_eq!(session.queued_frames(), 1);
    }

    #[test]
    fn fence_generation_rejects_frames_from_the_old_generation() {
        let mut session = IngressSession::new(StreamGeneration(1), Sequence(0), limits());
        assert_eq!(session.accept(pcm_frame(1, 0, &[1, 2])), Ok(Admission::New));
        let _drained = session.fence_generation(StreamGeneration(2), Sequence(0));

        assert_eq!(
            session.accept(pcm_frame(1, 1, &[1, 2])),
            Err(IngressError::Expired)
        );
        assert_eq!(session.accept(pcm_frame(2, 0, &[1, 2])), Ok(Admission::New));
    }
}
