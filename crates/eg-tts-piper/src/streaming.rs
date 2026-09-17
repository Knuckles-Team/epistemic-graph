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

use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

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
/// chunks, never the whole request's audio).
///
/// Every wait on the producer is bounded by the request's own `request_deadline_ms`:
/// * `next` waits at most until the deadline. A producer that is alive but wedged
///   (an ONNX forward pass cannot be interrupted) yields [`TtsError::Timeout`] once,
///   cancels the producer, and ends the stream. A producer that died or finished
///   disconnects its sender, which ends the stream promptly.
/// * Dropping the stream cancels the producer and disconnects the channel FIRST, so a
///   producer blocked on a full channel sees its `send` fail instead of waiting on a
///   receiver that is still alive, then waits for the producer's exit signal no longer
///   than the remaining deadline.
pub struct ChunkStream {
    rx: Option<mpsc::Receiver<Result<SynthesizedChunk, TtsError>>>,
    producer_exited: mpsc::Receiver<()>,
    cancel: CancellationToken,
    deadline: Instant,
}

impl std::fmt::Debug for ChunkStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkStream").finish_non_exhaustive()
    }
}

impl ChunkStream {
    fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
}

impl Iterator for ChunkStream {
    type Item = Result<SynthesizedChunk, TtsError>;

    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.remaining();
        let rx = self.rx.as_ref()?;
        match rx.recv_timeout(remaining) {
            Ok(item) => Some(item),
            Err(mpsc::RecvTimeoutError::Disconnected) => None,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                self.cancel.cancel();
                self.rx = None;
                Some(Err(TtsError::Timeout))
            }
        }
    }
}

impl Drop for ChunkStream {
    fn drop(&mut self) {
        self.cancel.cancel();
        // Disconnect before waiting: the producer's blocked `send` must fail now.
        self.rx = None;
        // Either signal (sent, or the producer's sender dropped) means it is gone. A
        // timeout means it is inside one uninterruptible forward pass; it observes the
        // cancellation at the next phrase or chunk boundary and exits on its own.
        let _ = self.producer_exited.recv_timeout(self.remaining());
    }
}

/// Signals the stream when the producer thread leaves its closure by any path —
/// return or panic — so `ChunkStream::drop` can wait for it without a join.
struct ProducerExitSignal(mpsc::SyncSender<()>);

impl Drop for ProducerExitSignal {
    fn drop(&mut self) {
        let _ = self.0.try_send(());
    }
}

/// The phrases to synthesize, in order. Phrase-level segmentation is only meaningful
/// over ORIGINAL text — arbitrary caller-supplied phonemes (InputMode::Phonemes) have
/// no reliably-detectable sentence boundary, so that mode streams as one phrase (still
/// chunked at the audio-byte level). Fails closed when nothing is synthesizable.
fn request_phrases(input_mode: InputMode, input_text: &str) -> Result<Vec<String>, TtsError> {
    let phrases: Vec<String> = match input_mode {
        InputMode::Text => segment_phrases(input_text),
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
    Ok(phrases)
}

/// The producer thread of [`synthesize_streaming`]: the request state the synthesis
/// loop reads, plus the running chunk position (`sample_offset`, `sequence`).
struct Producer {
    voice: LoadedVoice,
    tx: mpsc::SyncSender<Result<SynthesizedChunk, TtsError>>,
    cancel: CancellationToken,
    request_id: RequestId,
    job_id: JobId,
    input_mode: InputMode,
    espeak_voice: String,
    speaker_id: Option<i64>,
    controls: SynthesisControls,
    max_samples_per_chunk: usize,
    max_chunks: u64,
    sample_offset: u64,
    sequence: u64,
}

impl Producer {
    /// Synthesize and stream every phrase in order, stopping at the first delivered
    /// error, cancellation, resource cap, or dropped receiver.
    fn run(mut self, phrases: Vec<String>) {
        let phrase_count = phrases.len();
        for (phrase_index, phrase) in phrases.into_iter().enumerate() {
            let is_last_phrase = phrase_index + 1 == phrase_count;
            if self
                .stream_phrase(phrase_index, &phrase, is_last_phrase)
                .is_break()
            {
                return;
            }
        }
    }

    /// Synthesize one phrase and stream its audio in chunks of at most
    /// `max_samples_per_chunk` samples, checking cancellation before the phrase and
    /// before every chunk. `Break` ends production.
    fn stream_phrase(
        &mut self,
        phrase_index: usize,
        phrase: &str,
        is_last_phrase: bool,
    ) -> ControlFlow<()> {
        if self.cancel.is_cancelled() {
            return self.fail(TtsError::Cancelled);
        }
        let (samples, sample_rate) = match self.phrase_samples(phrase) {
            Ok(Some(audio)) => audio,
            Ok(None) => return ControlFlow::Continue(()),
            Err(e) => return self.fail(e),
        };
        let mut start = 0usize;
        while start < samples.len() {
            if self.cancel.is_cancelled() {
                return self.fail(TtsError::Cancelled);
            }
            let end = (start + self.max_samples_per_chunk).min(samples.len());
            let is_final = is_last_phrase && end == samples.len();
            self.send_chunk(&samples[start..end], phrase_index, sample_rate, is_final)?;
            start = end;
        }
        ControlFlow::Continue(())
    }

    /// The phrase's raw samples and sample rate, or `None` when it contributes no
    /// audio: a phrase that resolved to no phonemes at all (e.g. pure punctuation) or
    /// synthesized to no samples — neither is an error.
    fn phrase_samples(&mut self, phrase: &str) -> Result<Option<(Vec<f32>, u32)>, TtsError> {
        let phonemes = self
            .voice
            .resolve_phonemes(self.input_mode, phrase, &self.espeak_voice)?;
        if phonemes.trim().is_empty() {
            return Ok(None);
        }
        let (samples, sample_rate) =
            self.voice
                .create_raw(&phonemes, self.speaker_id, self.controls)?;
        Ok((!samples.is_empty()).then_some((samples, sample_rate)))
    }

    /// Encode one chunk of samples, advance the chunk position, and send it — unless
    /// that exceeds `max_chunks` (delivered as `ResourceExhausted`) or the receiver
    /// is gone (the caller stopped consuming). Either ends production.
    fn send_chunk(
        &mut self,
        slice: &[f32],
        phrase_index: usize,
        sample_rate: u32,
        is_final: bool,
    ) -> ControlFlow<()> {
        let (pcm, clipped_samples, non_finite_samples, peak_abs) = pcm16le_with_quality(slice);
        let digest = sha256_hex(&pcm);
        let rendition_ref = TtsBoundedId::new(format!("chunk-{}", self.sequence))
            .expect("`chunk-<u64>` always satisfies BoundedId's 1..=128 alnum/dash/colon shape");
        let chunk = TtsChunk {
            request_id: self.request_id.clone(),
            job_id: self.job_id.clone(),
            sequence: ChunkSequence(self.sequence),
            phrase_index: phrase_index as u32,
            sample_offset: self.sample_offset,
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

        self.sample_offset += slice.len() as u64;
        self.sequence += 1;
        if self.sequence > self.max_chunks {
            return self.fail(TtsError::ResourceExhausted {
                limit: "max_chunks",
            });
        }
        if self.tx.send(Ok(SynthesizedChunk { chunk, pcm })).is_err() {
            // Receiver dropped (caller stopped consuming) — stop producing.
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }

    /// Deliver `error` as the stream's next item and stop producing.
    fn fail(&self, error: TtsError) -> ControlFlow<()> {
        let _ = self.tx.send(Err(error));
        ControlFlow::Break(())
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
    voice: LoadedVoice,
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

    let phrases = request_phrases(input_mode, &input_text)?;

    let (tx, rx) = mpsc::sync_channel(4);
    let (exited, producer_exited) = mpsc::sync_channel(1);
    let deadline = Instant::now() + Duration::from_millis(u64::from(limits.request_deadline_ms));
    let stream_cancel = cancel.clone();

    let producer = Producer {
        voice,
        tx,
        cancel,
        request_id,
        job_id,
        input_mode,
        espeak_voice,
        speaker_id,
        controls,
        max_samples_per_chunk: (limits.max_chunk_decoded_bytes / 2).max(1) as usize,
        max_chunks: u64::from(limits.max_chunks),
        sample_offset: 0,
        sequence: 0,
    };

    // Detached on purpose: the stream waits on `ProducerExitSignal`, never on a join.
    thread::spawn(move || {
        let _exit_signal = ProducerExitSignal(exited);
        producer.run(phrases);
    });

    Ok(ChunkStream {
        rx: Some(rx),
        producer_exited,
        cancel: stream_cancel,
        deadline,
    })
}

/// `ChunkStream`'s bounded paths without ONNX: the test stands in for the producer
/// by holding the far ends of both channels, so each outcome is forced, not raced.
#[cfg(test)]
mod chunk_stream_tests {
    use super::*;

    /// Bound for waits that must NOT expire in a passing run.
    const GENEROUS: Duration = Duration::from_secs(30);

    type ChunkSender = mpsc::SyncSender<Result<SynthesizedChunk, TtsError>>;

    fn stream_without_producer(
        deadline_in: Duration,
    ) -> (ChunkStream, ChunkSender, mpsc::SyncSender<()>) {
        let (tx, rx) = mpsc::sync_channel(4);
        let (exited, producer_exited) = mpsc::sync_channel(1);
        let stream = ChunkStream {
            rx: Some(rx),
            producer_exited,
            cancel: CancellationToken::new(),
            deadline: Instant::now() + deadline_in,
        };
        (stream, tx, exited)
    }

    #[test]
    fn next_passes_a_produced_item_through() {
        let (mut stream, tx, _exited) = stream_without_producer(GENEROUS);
        tx.send(Err(TtsError::Cancelled))
            .expect("stream is receiving");
        assert!(matches!(stream.next(), Some(Err(TtsError::Cancelled))));
    }

    #[test]
    fn next_ends_promptly_once_the_producer_is_gone() {
        let (mut stream, tx, _exited) = stream_without_producer(GENEROUS);
        drop(tx);
        let started = Instant::now();
        assert!(stream.next().is_none());
        assert!(
            started.elapsed() < GENEROUS,
            "a disconnect must not wait out the deadline"
        );
    }

    #[test]
    fn next_on_a_silent_live_producer_times_out_once_cancels_and_ends() {
        // `_tx` stays alive and never sends: a wedged producer.
        let (mut stream, _tx, _exited) = stream_without_producer(Duration::from_millis(50));
        let cancel = stream.cancel.clone();
        assert!(matches!(stream.next(), Some(Err(TtsError::Timeout))));
        assert!(cancel.is_cancelled(), "a timeout must cancel the producer");
        assert!(stream.next().is_none(), "a timed-out stream ends");
    }

    #[test]
    fn drop_cancels_and_disconnects_before_waiting_for_the_producer_to_exit() {
        // The deadline sits far past GENEROUS, so a drop that disconnects only
        // after its wait fails the loop below instead of racing it.
        let (stream, tx, exited) = stream_without_producer(Duration::from_secs(300));
        let cancel = stream.cancel.clone();
        let (dropped, drop_done) = mpsc::sync_channel(1);
        thread::spawn(move || {
            drop(stream);
            let _ = dropped.send(());
        });

        // A producer blocked on a full channel is released only if the receiver
        // is gone while drop waits. Keep sending until the channel disconnects.
        let limit = Instant::now() + GENEROUS;
        loop {
            match tx.try_send(Err(TtsError::Cancelled)) {
                Err(mpsc::TrySendError::Disconnected(_)) => break,
                _ if Instant::now() > limit => panic!("drop never disconnected the chunk channel"),
                _ => thread::sleep(Duration::from_millis(1)),
            }
        }
        assert!(cancel.is_cancelled(), "drop must cancel the producer");
        assert!(
            drop_done.try_recv().is_err(),
            "drop waits for the producer's exit signal while one is still possible"
        );

        exited
            .send(())
            .expect("drop is waiting for the exit signal");
        assert_eq!(drop_done.recv_timeout(GENEROUS), Ok(()));
    }

    #[test]
    fn drop_returns_at_the_deadline_when_the_producer_never_exits() {
        // `_exited` stays alive and never signals: a producer inside one
        // uninterruptible forward pass.
        let (stream, _tx, _exited) = stream_without_producer(Duration::from_millis(100));
        let (dropped, drop_done) = mpsc::sync_channel(1);
        thread::spawn(move || {
            drop(stream);
            let _ = dropped.send(());
        });
        assert_eq!(drop_done.recv_timeout(GENEROUS), Ok(()));
    }

    #[test]
    fn the_exit_signal_fires_when_the_producer_panics() {
        let (exited, exit_seen) = mpsc::sync_channel(1);
        thread::spawn(move || {
            let _signal = ProducerExitSignal(exited);
            panic!("producer fixture panics");
        });
        assert_eq!(exit_seen.recv_timeout(GENEROUS), Ok(()));
    }
}
