//! Bounded phrase segmentation and cancellable, chunk-streaming synthesis.
//!
//! `piper-rs::Piper::create` returns one whole-phrase `Vec<f32>` buffer per call — it
//! has no stream or cancellation contract of its own (GOC-34 lane audit,
//! `src/lib.rs:71-103`). This module is what makes synthesis actually streaming and
//! cancellable at the ENGINE level: a phrase is the smallest unit of ONNX work (an
//! in-flight forward pass cannot itself be interrupted — there is no async yield point
//! inside `ort::Session::run`), so cancellation is checked at every phrase boundary AND
//! at every audio-chunk boundary within an already-synthesized phrase, and chunks are
//! delivered to the caller one at a time over a bounded channel as they are produced —
//! never after buffering the whole response.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use sha2::{Digest, Sha256};

use eg_audio::tts::{
    ChunkQuality, ChunkSequence, InputMode, JobId, OutputEncoding, OutputFormat, RequestId,
    SpeakerSelection, SynthesisControls, TtsBoundedId, TtsChunk, TtsError, TtsLimits,
};

use crate::voice::LoadedVoice;

/// A shared, cloneable cancellation flag. Cloning shares the SAME underlying flag —
/// the producer thread and any number of caller-held handles observe one truth.
#[derive(Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// One synthesized, chunk-bounded slice of audio: the frozen [`TtsChunk`] wire record
/// plus the actual PCM16LE bytes it describes. `eg-audio`'s `tts.rs` deliberately keeps
/// raw audio bytes OUT of `TtsChunk` (only a `rendition_ref`/digest cross that
/// contract — durable CAS publication is GOC-05/eg-jobs territory, out of this lane's
/// scope). Until that durable publication path exists, `pcm` is this crate's own
/// direct, in-process carrier of the bytes `rendition_digest` describes.
#[derive(Clone, Debug)]
pub struct SynthesizedChunk {
    pub chunk: TtsChunk,
    pub pcm: Vec<u8>,
}

/// Deterministic phrase segmentation over ORIGINAL text: split on sentence-ending
/// punctuation (`.`/`!`/`?`) and line breaks, trimming whitespace, dropping empty
/// segments. A text with no such boundary is one phrase. Pure and total — never
/// panics, never returns an empty phrase for non-empty input.
pub fn segment_phrases(text: &str) -> Vec<String> {
    let mut phrases = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        current.push(ch);
        if matches!(ch, '.' | '!' | '?' | '\n') {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                phrases.push(trimmed.to_string());
            }
            current.clear();
        }
    }
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        phrases.push(trimmed.to_string());
    }
    phrases
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Convert f32 PCM (as piper-rs emits it, nominal range `[-1.0, 1.0]`) to little-endian
/// 16-bit PCM, honestly measuring quality as it goes: a non-finite (NaN/inf) sample is
/// encoded as digital silence (i16 cannot represent it) but COUNTED — `eg_audio::tts`'s
/// `finalize_result` refuses a `Succeeded` status whenever `non_finite_samples > 0`, so
/// the corruption can never be silently promoted to success, only honestly reported. A
/// finite sample outside `[-1.0, 1.0]` is clamped before conversion but also counted as
/// clipped — never silently accepted as clean.
fn pcm16le_with_quality(samples: &[f32]) -> (Vec<u8>, u32, u32, f32) {
    let mut bytes = Vec::with_capacity(samples.len() * 2);
    let mut clipped = 0u32;
    let mut non_finite = 0u32;
    let mut peak_abs = 0.0f32;
    for &s in samples {
        if !s.is_finite() {
            non_finite += 1;
            bytes.extend_from_slice(&0i16.to_le_bytes());
            continue;
        }
        if s.abs() > 1.0 {
            clipped += 1;
        }
        peak_abs = peak_abs.max(s.abs());
        let clamped = s.clamp(-1.0, 1.0);
        let value = (clamped * f32::from(i16::MAX)).round() as i16;
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    (bytes, clipped, non_finite, peak_abs)
}

/// A pull-based iterator over synthesized chunks, backed by a producer thread and a
/// bounded channel (capacity 4 — bounds resident memory to a handful of in-flight
/// chunks, never the whole request's audio). Dropping the stream before it is
/// exhausted disconnects the channel; the producer's next blocked `send` then returns
/// an error and the thread exits promptly — no orphaned thread, no deadlock.
pub struct ChunkStream {
    rx: mpsc::Receiver<Result<SynthesizedChunk, TtsError>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for ChunkStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkStream").finish_non_exhaustive()
    }
}

impl Iterator for ChunkStream {
    type Item = Result<SynthesizedChunk, TtsError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

impl Drop for ChunkStream {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Everything [`synthesize_streaming`] needs beyond the loaded voice and cancellation
/// handle, grouped into one struct so the function signature stays small regardless of
/// how many `tts.rs` request fields a synthesis call ultimately depends on.
pub struct SynthesizeRequest {
    pub request_id: RequestId,
    pub job_id: JobId,
    pub input_mode: InputMode,
    pub input_text: String,
    pub espeak_voice: String,
    pub speaker: SpeakerSelection,
    pub controls: SynthesisControls,
    pub output_format: OutputFormat,
    pub limits: TtsLimits,
}

/// Synthesize `request.input_text` against `voice`, streaming bounded, ordered
/// [`TtsChunk`]s as they are produced. Fails closed SYNCHRONOUSLY (before spawning any
/// work) on an output-format mismatch or an out-of-range speaker; failures discovered
/// DURING synthesis (phonemization, inference, or a resource cap) are delivered as the
/// next item of the returned stream rather than panicking or being lost.
pub fn synthesize_streaming(
    mut voice: LoadedVoice,
    request: SynthesizeRequest,
    cancel: CancellationToken,
) -> Result<ChunkStream, TtsError> {
    let SynthesizeRequest {
        request_id,
        job_id,
        input_mode,
        input_text,
        espeak_voice,
        speaker,
        controls,
        output_format,
        limits,
    } = request;

    if output_format.encoding != OutputEncoding::Pcm16Le {
        return Err(TtsError::MalformedRequest {
            reason: "only Pcm16Le output encoding is supported",
        });
    }
    if output_format.sample_rate != voice.sample_rate() || output_format.channels != 1 {
        return Err(TtsError::MalformedRequest {
            reason: "output_format does not match the loaded voice's native mono sample_rate; \
                no resample/upmix is performed",
        });
    }
    let speaker_id = voice.validate_speaker(speaker)?;

    // Phrase-level segmentation is only meaningful over ORIGINAL text — arbitrary
    // caller-supplied phonemes (InputMode::Phonemes) have no reliably-detectable
    // sentence boundary, so that mode streams as one phrase (still chunked at the
    // audio-byte level below).
    let phrases: Vec<String> = match input_mode {
        InputMode::Text => segment_phrases(&input_text),
        InputMode::Phonemes => {
            let trimmed = input_text.trim();
            if trimmed.is_empty() {
                Vec::new()
            } else {
                vec![trimmed.to_string()]
            }
        }
    };
    if phrases.is_empty() {
        return Err(TtsError::MalformedRequest {
            reason: "no synthesizable phrase found in input",
        });
    }

    let (tx, rx) = mpsc::sync_channel(4);
    let max_samples_per_chunk = (limits.max_chunk_decoded_bytes / 2).max(1) as usize;
    let max_chunks = u64::from(limits.max_chunks);

    let handle = thread::spawn(move || {
        let phrase_count = phrases.len();
        let mut sample_offset: u64 = 0;
        let mut sequence: u64 = 0;

        for (phrase_index, phrase) in phrases.into_iter().enumerate() {
            if cancel.is_cancelled() {
                let _ = tx.send(Err(TtsError::Cancelled));
                return;
            }

            let phonemes = match voice.resolve_phonemes(input_mode, &phrase, &espeak_voice) {
                Ok(p) => p,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };
            if phonemes.trim().is_empty() {
                // A phrase that resolved to no phonemes at all (e.g. pure
                // punctuation) contributes no audio — not an error.
                continue;
            }

            let (samples, sample_rate) = match voice.create_raw(&phonemes, speaker_id, controls) {
                Ok(v) => v,
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return;
                }
            };
            if samples.is_empty() {
                continue;
            }

            let is_last_phrase = phrase_index + 1 == phrase_count;
            let mut start = 0usize;
            while start < samples.len() {
                if cancel.is_cancelled() {
                    let _ = tx.send(Err(TtsError::Cancelled));
                    return;
                }
                let end = (start + max_samples_per_chunk).min(samples.len());
                let slice = &samples[start..end];
                let (pcm, clipped_samples, non_finite_samples, peak_abs) =
                    pcm16le_with_quality(slice);
                let digest = sha256_hex(&pcm);
                let rendition_ref = TtsBoundedId::new(format!("chunk-{sequence}")).expect(
                    "`chunk-<u64>` always satisfies BoundedId's 1..=128 alnum/dash/colon shape",
                );
                let is_final = is_last_phrase && end == samples.len();

                let chunk = TtsChunk {
                    request_id: request_id.clone(),
                    job_id: job_id.clone(),
                    sequence: ChunkSequence(sequence),
                    phrase_index: phrase_index as u32,
                    sample_offset,
                    sample_count: slice.len() as u32,
                    sample_rate,
                    channels: 1,
                    encoding: OutputEncoding::Pcm16Le,
                    rendition_ref,
                    rendition_digest: digest,
                    is_final,
                    quality: ChunkQuality::Measured {
                        clipped_samples,
                        non_finite_samples,
                        peak_abs,
                    },
                };

                sample_offset += slice.len() as u64;
                sequence += 1;
                if sequence > max_chunks {
                    let _ = tx.send(Err(TtsError::ResourceExhausted {
                        limit: "max_chunks",
                    }));
                    return;
                }
                if tx.send(Ok(SynthesizedChunk { chunk, pcm })).is_err() {
                    // Receiver dropped (caller stopped consuming) — stop producing.
                    return;
                }
                start = end;
            }
        }
    });

    Ok(ChunkStream {
        rx,
        handle: Some(handle),
    })
}
