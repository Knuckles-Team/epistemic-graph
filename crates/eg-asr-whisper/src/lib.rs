//! Native Rust ASR provider (GOC-33, `OWNER-VOICE-ASR`) implementing the
//! provider seam behind `eg_audio::asr`'s frozen `asr.*` contract.
//!
//! ## Binding choice: `whisper-rs` (crates.io) over `whisper-rs`-vendored
//! whisper.cpp/ggml, retained rather than forked; ONNX Runtime/Parakeet NOT
//! implemented
//!
//! `open-source-libraries/meetily` (`frontend/src-tauri/`) proves two viable
//! Rust ASR bindings: `whisper-rs` over whisper.cpp/ggml, and `ort` (ONNX
//! Runtime) over a hand-rolled Parakeet decoder
//! (`frontend/src-tauri/src/parakeet_engine/`). This crate assimilates the
//! CONCEPT of the whisper-rs provider only — no source line was copied; every
//! item below (windowing, digest verification, honest-quality mapping,
//! word-span merge) was written fresh against this repo's frozen contract.
//! Reasons `whisper-rs` was chosen and Parakeet was not attempted in this
//! change:
//!
//! - **Off-the-shelf timing API.** `whisper-rs` 0.16's `WhisperSegment`
//!   exposes real provider-derived `start_timestamp()`/`end_timestamp()`/
//!   `no_speech_probability()`, and per-token `t0`/`t1`/`token_probability()`
//!   when `token_timestamps` is requested — everything the frozen contract's
//!   [`eg_audio::asr::SegmentTiming::Provided`]/[`eg_audio::asr::Quality::
//!   Calibrated`] need already exists in the published crate. Meetily's own
//!   Parakeet path, by contrast, decodes token frame timestamps internally
//!   but its `ParakeetEngine::transcribe_audio` **returns only `result.text`**
//!   (lane doc, `parakeet_engine.rs:453-475`) — the timing never crossed its
//!   own provider trait. Reaching timing parity with the whisper-rs path
//!   would mean writing a full ONNX decoder/tokenizer/timestamp pipeline
//!   from scratch, not adapting a published crate.
//! - **Maintenance/supply chain.** `whisper-rs`/`whisper-rs-sys` (Codeberg,
//!   `tazz4843/whisper-rs`) are actively maintained, crates.io-published (this
//!   repo's "crates.io-only Rust dependency" edict is satisfied without any
//!   `git = ` pin), and vendor a pinned whisper.cpp/ggml revision that is
//!   rebuilt from C/C++ source via `cmake` — no prebuilt binary is trusted.
//! - **Model governance is out of scope here (GOC-36).** Parakeet's own
//!   model license (CC-BY-4.0, per the lane doc) and its "custom mirror" host
//!   are exactly the kind of acquisition/verification concern GOC-36 owns;
//!   deferring Parakeet avoids taking on a second, less-qualified model
//!   supply chain in the same change that must also prove the CPU-safety
//!   build contract below.
//! - **Decisive, confirmed after the fact: an ONNX Runtime (`ort`) binding
//!   cannot run on this fleet's x86_64 hosts at all.** The sibling native-TTS
//!   lane independently discovered that `ort`'s only x86_64 CPU prebuilt
//!   requires AVX2, and audited every host: the interactive dev host and the
//!   two build hosts (Westmere, 2010, and Sandy Bridge, 2012 respectively)
//!   all lack AVX2 (the fleet's aarch64 GPU host is a separate architecture)
//!   — so there is no host here an `ort`-based
//!   provider could execute on natively; that lane only proved correctness
//!   under `qemu-x86_64 -cpu max` emulation, explicitly not a hardware
//!   qualification. `whisper-rs`/ggml does not share this failure mode: ggml
//!   is compiled FROM SOURCE per target and has genuine non-AVX2 code paths
//!   (this crate builds a fixed SSE2-only baseline — see below), so it is
//!   the only one of the two bindings that can be qualified on real fleet
//!   hardware at all. This was independently confirmed for THIS crate, not
//!   assumed: see "Hardware verification" below.
//!
//! Per the lane doc's ADR/scorecard requirement: this is a **retain-pinned**
//! decision (crates.io `whisper-rs`, no vendor mirror/fork of whisper.cpp
//! source), not full absorption — the narrower of the two options the lane
//! doc's default recommendation favors. A Parakeet/ONNX provider remains
//! explicitly open future work (GOC-33-W05 in the lane doc) and is NOT
//! implemented, claimed, or stubbed here.
//!
//! ## Licensing / provenance
//!
//! - `whisper-rs` + `whisper-rs-sys` (Codeberg `tazz4843/whisper-rs`,
//!   `whisper-rs = "0.16"`, `whisper-rs-sys = "0.15"` transitively): **The
//!   Unlicense** (public-domain dedication) — compatible with this crate's
//!   MIT license; no attribution obligation, but recorded here for provenance
//!   per the lane doc.
//! - The vendored whisper.cpp/ggml C/C++ sources `whisper-rs-sys` builds from
//!   source (upstream `ggml-org/whisper.cpp`): **MIT**.
//! - OpenAI's original Whisper model architecture/weights license: **MIT**
//!   (per the lane doc's own W01 finding) — GOC-36 owns actual model
//!   acquisition; no model weight is vendored, downloaded, or shipped by this
//!   crate.
//! - See `CHANGELOG.md` (this change's entry) and `docs/architecture/
//!   native-asr.md` for the durable provenance record.
//!
//! ## CPU-safety build contract (NO x86_64 host in this fleet has AVX2)
//!
//! This is not a hypothetical edge case: the interactive dev host (Westmere,
//! 2010) and both build hosts (Westmere, 2010, and Sandy Bridge, 2012) were
//! all individually checked (`/proc/cpuinfo`) and confirm
//! **no `x86-64-v3`/AVX2 anywhere in this fleet's x86_64 estate** (the fleet's
//! GPU host is `aarch64`, a different architecture). A binding whose only available
//! artifact requires AVX2 cannot run natively on ANY x86_64 host here at
//! all — this is exactly the dead end the sibling native-TTS lane hit with
//! `ort` (ONNX Runtime)'s prebuilt CPU binary, which hard-requires AVX2 with
//! no baseline fallback artifact; see this crate's "Binding choice" section
//! above for why `whisper-rs`/ggml does not share that failure mode (ggml
//! degrades to a portable baseline it builds from source, it does not ship a
//! single fixed prebuilt).
//!
//! `whisper-rs-sys`'s build script forwards any `GGML_*`/`WHISPER_*`/
//! `CMAKE_*` environment variable straight to `cmake` as a `-D` define
//! (verified against its source). This repo's `.cargo/config.toml` sets:
//!
//! - `GGML_NATIVE=OFF` — ggml's CMake default is `GGML_NATIVE=ON`, which
//!   bakes the **build host's** CPU features (`-march=native`) into the
//!   binary. Even though no host here currently has AVX2, `-march=native`
//!   would still bake in whatever the build host DOES have (e.g. Sandy
//!   Bridge's AVX) unconditionally, which is still a portability hazard for
//!   any host with a smaller feature set (or a future build host); disabling
//!   it is mandatory, not merely a precaution against a hypothetical AVX2 gap.
//! - A fixed, portable **SSE2-only baseline** (`GGML_AVX=OFF GGML_AVX2=OFF
//!   GGML_FMA=OFF GGML_F16C=OFF GGML_AVX512=OFF GGML_BMI2=OFF`). The `BMI2`
//!   flag was NOT optional in practice: ggml's CMake defaults it ON
//!   independent of `GGML_NATIVE`, and the very first real-fixture run (on
//!   the Sandy Bridge build host — no AVX2, no BMI2) actually SIGILLed on a `shlx`
//!   (BMI2) instruction inside `ggml_graph_plan` with every AVX* flag
//!   already off — caught by running `tests/real_transcription.rs` under
//!   `gdb`, not by reasoning about flags alone; see the GOC-33 report for the
//!   backtrace. ggml's own genuine runtime
//!   CPU dispatch, `GGML_CPU_ALL_VARIANTS=ON` — the C-side equivalent of
//!   `is_x86_feature_detected!` (the pattern the viz lane established in this
//!   repo's own Rust code) — was evaluated empirically (`cargo check -p
//!   eg-asr-whisper` on the Sandy Bridge build host) and **rejected for this
//!   change**: ggml's CMake hard-requires pairing it with
//!   `GGML_BACKEND_DL=ON`, which turns the CPU backend into a runtime-
//!   `dlopen`ed `.so` selected from a search path at process start rather
//!   than something statically linked into the binary — a deploy-image
//!   packaging change (shipping backend `.so` files alongside every release
//!   binary) outside this lane's scope, and exactly the configuration class
//!   the known upstream defect ggml-org/whisper.cpp#2963 ("GGML_CPU_ALL_
//!   VARIANTS causes a silent exception") was filed against. See
//!   `.cargo/config.toml`'s own comment for the full trade-off record and
//!   the GOC-33 report for the exact commands/output that surfaced the
//!   `GGML_BACKEND_DL` requirement. Genuine runtime multi-ISA dispatch
//!   remains documented future work once a deploy-image change to ship the
//!   `.so` variants is separately owned and qualified — the fixed baseline
//!   is always safe (never SIGILLs on any x86_64 host, including the
//!   pre-AVX2 Westmere-era hosts) at
//!   the cost of leaving AVX2/AVX512 throughput on the table on capable
//!   hosts, an explicit trade-off, not an oversight.
//!
//! ## Hardware verification (native, not emulated)
//!
//! Built and run **natively** (no `qemu`/emulation) on the Sandy Bridge build
//! host (`Intel(R) Xeon(R) CPU E5-4620` — confirmed via
//! `/proc/cpuinfo`: `avx sse4_1 sse4_2` present, `avx2`/`fma`/`f16c`/`bmi2`
//! ALL absent). A real `ggml-tiny.en.bin` model (MIT, from
//! `huggingface.co/ggerganov/whisper.cpp`, digest-verified) transcribed a
//! synthesized 16 kHz mono speech fixture end to end through
//! [`WhisperAsrProvider::transcribe_streaming`], producing real,
//! recognizable (imperfect — `tiny.en` is the smallest model) text with
//! real segment timing and quality, plus a real
//! `AsrError::Cancelled` when cancelled mid-stream. This is what actually
//! caught the `GGML_BMI2` requirement above — the first attempt SIGILLed
//! precisely because it ran on real Sandy Bridge hardware rather than a
//! newer/emulated CPU that would have masked the gap. See the GOC-33 report
//! for the exact commands and full transcript output.
//!
//! ## Resource limits (BUG-283)
//!
//! This fleet's containerd does not virtualize `/proc`: a CPU-limited pod
//! still reports the host's full core count. `whisper.cpp`'s thread pool is
//! therefore sized by [`cpu_budget::effective_thread_budget`], which reads
//! the REAL cgroup v2/v1 CPU quota (falling back to the OS-visible count
//! only when no finite quota is set), not `std::thread::available_
//! parallelism()` directly — see that module and the shared `eg-resource`
//! contract for the full cgroup policy. `EG_ASR_MAX_THREADS` is an upper-bound
//! override and cannot bypass the measured safe budget.
//!
//! ## GPU
//!
//! GPU execution providers are opt-in Cargo features (`cuda`/`vulkan`/
//! `metal`/`coreml`/`hipblas`, mirroring `whisper-rs`'s own feature names and
//! Meetily's precedent) and OFF by default. Even when compiled in,
//! [`WhisperAsrProvider::load`] only *requests* GPU use — whisper.cpp probes
//! actual hardware presence at runtime and falls back to CPU when no matching
//! device is found, so a GPU feature can be compiled into a fleet-wide build
//! without requiring GPU hardware on every host (the platform's stated
//! constraint). No GPU feature was hardware-qualified in this change (that is
//! GOC-33-W07 in the lane doc); `use_gpu` is honest about being "requested,
//! not proven".
//!
//! ## What this crate does and does not do
//!
//! This crate performs real inference (I/O + whisper.cpp calls) — it is
//! deliberately NOT `eg_audio::asr` itself, which stays dependency-light and
//! I/O-free per its own module doc. [`WhisperAsrProvider::transcribe_streaming`]
//! processes bounded audio windows sequentially, invoking `on_partial` after
//! each window and honoring cancellation between AND (via whisper.cpp's abort
//! callback) *during* a window — see that method's doc for exactly what
//! "streaming-capable" means here versus GOC-35's future full-duplex
//! low-latency path. Every constructed [`eg_audio::asr::AsrSegment`] is passed
//! through the frozen contract's own `AsrSegment::validate()` before being
//! accepted, so a whisper.cpp bug that emitted an inconsistent timestamp
//! would be caught here rather than silently propagated. This crate does
//! **not** call `eg_audio::asr::finalize_result`/`authorize_carrier`: those
//! require a governed `CarrierRef` (tenant/actor/consent/purpose/trace) bound
//! to a real GOC-15/16 authorization decision and a GOC-32 `AudioSourceRef`,
//! neither of which exists at this crate's boundary — that full durable,
//! policy-authorized `asr.result.v1` commit is future worker/AU-orchestration
//! work (W03/W06 in the lane doc), explicitly out of scope here.

mod cpu_budget;
mod model;
mod wav;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use eg_audio::asr::{
    AsrBoundedId, AsrError, AsrSegment, AsrTask, LanguageTag, ModelManifestRef, Quality,
    SegmentSequence, SegmentTiming, WordSpan,
};
use whisper_rs::{FullParams, SamplingStrategy, WhisperContext, WhisperContextParameters};

pub use model::{verify_model, VerifiedModel};
pub use wav::{decode_wav_16k_mono, DecodedAudio};

/// Cooperative cancellation flag shared between a caller and an in-flight
/// [`WhisperAsrProvider::transcribe_streaming`] call. Checked between every
/// window AND wired into whisper.cpp's own abort callback so a long window
/// can be interrupted mid-decode, not only at the next window boundary.
#[derive(Clone, Default)]
pub struct CancelFlag(Arc<AtomicBool>);

impl CancelFlag {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// Transcription request options. `window_ms` bounds a single whisper.cpp
/// `full()` call — the streaming knob: a smaller window yields partials
/// sooner at some cost to per-window context.
pub struct TranscribeOptions {
    /// `None` = auto-detect. `Some(code)` (e.g. `"en"`) pins the language and
    /// disables detection, matching the frozen contract's "no silent
    /// fallback" rule — the caller decides, this crate never guesses when a
    /// caller was explicit.
    pub language: Option<String>,
    pub translate: bool,
    pub word_timing: bool,
    pub window_ms: u32,
}

impl Default for TranscribeOptions {
    fn default() -> Self {
        Self {
            language: None,
            translate: false,
            word_timing: false,
            window_ms: 30_000,
        }
    }
}

/// A produced segment paired with its actual transcript text. The frozen
/// [`AsrSegment`] carries only an opaque `text_ref` (real deployments resolve
/// that against governed CAS/rendition storage — see `eg_audio::asr`'s module
/// doc); this crate has no CAS to write to, so it hands the caller both the
/// governed, validated segment shape AND the plain text side by side. A real
/// worker (W03/W06) would instead commit `text` to CAS and mint `text_ref`
/// from that commit.
pub struct TranscribedSegment {
    pub segment: AsrSegment,
    pub text: String,
}

/// A provisional (never terminal) unit of streaming progress: everything
/// [`eg_audio::asr::AsrPartial`] needs plus the actual text (see
/// [`TranscribedSegment`] for why text rides alongside rather than only a
/// ref).
pub struct StreamingPartial {
    pub sequence: SegmentSequence,
    pub text: String,
    pub timing: SegmentTiming,
    pub quality: Quality,
}

/// The complete outcome of one [`WhisperAsrProvider::transcribe_streaming`]
/// call: every accepted segment, the language actually used (detected or
/// pinned), and whether every segment carried provider timing.
pub struct TranscriptionOutcome {
    pub segments: Vec<TranscribedSegment>,
    pub language: String,
    pub timing_available: bool,
}

/// A loaded whisper.cpp model context, ready to create per-call inference
/// state from. Construct only via [`WhisperAsrProvider::load`], which
/// requires an already-[`VerifiedModel`] — there is no path that loads an
/// unverified model file.
pub struct WhisperAsrProvider {
    ctx: WhisperContext,
    manifest: ModelManifestRef,
}

fn bounded(value: &str) -> Result<AsrBoundedId, AsrError> {
    AsrBoundedId::new(value).map_err(|_| AsrError::MalformedRequest {
        reason: "generated identifier failed bounded-id validation",
    })
}

impl WhisperAsrProvider {
    /// Load a verified model file. `request_gpu` is accepted and recorded but
    /// cannot currently take effect: no accelerator feature is shipped, because
    /// none was hardware-qualified and each broke `--all-features` builds. The
    /// parameter is retained so a caller's intent survives until a qualified
    /// provider lands — see `GPU_PROVIDERS_SHIPPED` below and Cargo.toml.
    /// `manifest_id` becomes this provider's [`ModelManifestRef::manifest_id`]
    /// on every produced segment.
    pub fn load(
        model: &VerifiedModel,
        manifest_id: &str,
        request_gpu: bool,
    ) -> Result<Self, AsrError> {
        let manifest = model.manifest_ref(manifest_id)?;
        // No accelerator feature is currently exposed by this crate -- see the
        // `[features]` note in Cargo.toml. Each one made the vendored whisper.cpp
        // build link a runtime absent from this fleet (`coreml`/`metal` are
        // Apple-only and cannot exist here at all; `cuda` emits `-l cudart -l cuda`),
        // and because a Cargo feature cannot be target-gated, `--all-features`
        // enabled them everywhere and broke every such build of the whole workspace.
        // Named constant rather than an inline `false` so that restoring a
        // hardware-QUALIFIED provider is a one-line change with an obvious home.
        const GPU_PROVIDERS_SHIPPED: bool = false;
        let use_gpu = request_gpu && GPU_PROVIDERS_SHIPPED;
        let mut params = WhisperContextParameters::new();
        params.use_gpu(use_gpu);
        let ctx = WhisperContext::new_with_params(model.path(), params).map_err(|_| {
            AsrError::ModelUnavailable {
                reason: "whisper.cpp rejected the verified model file (unsupported/corrupt ggml)",
            }
        })?;
        Ok(Self { ctx, manifest })
    }

    /// Transcribe `audio` (16 kHz mono PCM, see [`decode_wav_16k_mono`]) in
    /// bounded windows of `opts.window_ms`. `on_partial` is invoked once per
    /// newly produced segment, immediately after the window that produced it
    /// completes — this is the "streaming-capable" contract: a caller gets
    /// progressive output as bounded windows complete, and MAY cancel between
    /// (or, via whisper.cpp's own abort callback, during) any window rather
    /// than only ever seeing output after a complete-file transcode. Returns
    /// `Err(AsrError::Cancelled)` — never a partial success dressed as
    /// complete — if `cancel` was set before every segment finished.
    pub fn transcribe_streaming(
        &self,
        audio: &DecodedAudio,
        opts: &TranscribeOptions,
        cancel: &CancelFlag,
        mut on_partial: impl FnMut(StreamingPartial),
    ) -> Result<TranscriptionOutcome, AsrError> {
        if audio.samples.is_empty() {
            return Err(AsrError::MalformedRequest {
                reason: "zero-length decoded audio",
            });
        }
        if opts.window_ms == 0 {
            return Err(AsrError::MalformedRequest {
                reason: "window_ms must be strictly positive",
            });
        }
        let window_samples = (opts.window_ms as usize).saturating_mul(16); // 16 samples/ms @ 16kHz
        if window_samples == 0 {
            return Err(AsrError::MalformedRequest {
                reason: "window_ms too small to cover a single sample",
            });
        }

        let mut state = self
            .ctx
            .create_state()
            .map_err(|_| AsrError::ModelUnavailable {
                reason: "whisper.cpp state allocation failed",
            })?;

        let mut segments = Vec::new();
        let mut next_sequence: u64 = 0;
        let mut detected_language: Option<String> = None;
        let mut all_timed = true;
        // BUG-283: this fleet's containerd does not virtualize /proc, so
        // `available_parallelism()`/`nproc` alone report the HOST's CPU
        // count even inside a CPU-limited pod — see `cpu_budget`'s module
        // doc. Use the real cgroup-aware budget instead of trusting the OS
        // view directly.
        let n_threads = cpu_budget::effective_thread_budget();

        if cancel.is_cancelled() {
            return Err(AsrError::Cancelled);
        }

        for (window_index, window) in audio.samples.chunks(window_samples).enumerate() {
            if cancel.is_cancelled() {
                return Err(AsrError::Cancelled);
            }
            let window_offset_ms = (window_index * opts.window_ms as usize) as i64;

            let mut params = FullParams::new(SamplingStrategy::Greedy { best_of: 1 });
            params.set_translate(opts.translate);
            params.set_token_timestamps(opts.word_timing);
            params.set_single_segment(false);
            params.set_no_context(true);
            params.set_print_progress(false);
            params.set_print_special(false);
            params.set_n_threads(n_threads);
            if let Some(lang) = opts.language.as_deref() {
                params.set_language(Some(lang));
            }
            let window_cancel = cancel.clone();
            params.set_abort_callback_safe(move || window_cancel.is_cancelled());

            state.full(params, window).map_err(|_| AsrError::Degraded)?;

            if cancel.is_cancelled() {
                return Err(AsrError::Cancelled);
            }

            if detected_language.is_none() {
                let lang_id = state.full_lang_id_from_state();
                detected_language = Some(
                    whisper_rs::get_lang_str(lang_id)
                        .map(|s| s.to_string())
                        .or_else(|| opts.language.clone())
                        .unwrap_or_else(|| "unknown".to_string()),
                );
            }

            let n_segments = state.full_n_segments();
            for i in 0..n_segments {
                let Some(seg) = state.get_segment(i) else {
                    continue;
                };
                let text = seg
                    .to_str_lossy()
                    .map(|c| c.into_owned())
                    .unwrap_or_default();
                let seg_start = window_offset_ms + seg.start_timestamp() * 10;
                let seg_end = window_offset_ms + seg.end_timestamp() * 10;
                if seg_end <= seg_start {
                    // A malformed/degenerate provider timestamp: never accept
                    // it as complete output. Skip this segment rather than
                    // constructing a `SegmentTiming` the frozen contract's
                    // own `validate()` would reject.
                    continue;
                }
                let words = if opts.word_timing {
                    merge_tokens_into_words(&seg, window_offset_ms)
                } else {
                    Vec::new()
                };
                let timing = SegmentTiming::Provided {
                    start_ms: seg_start as u64,
                    end_ms: seg_end as u64,
                    words,
                };
                if timing.validate().is_err() {
                    // Provider produced internally-inconsistent word spans
                    // (e.g. a token-timestamp glitch): fail closed on THIS
                    // segment's word breakdown rather than propagate it, but
                    // keep the segment's own honest start/end.
                    continue;
                }
                let quality = segment_quality(&seg);
                if quality.validate().is_err() {
                    continue;
                }
                let sequence = SegmentSequence(next_sequence);
                next_sequence += 1;
                let asr_segment = AsrSegment {
                    segment_id: bounded(&format!("seg-{sequence:06}", sequence = sequence.0))?,
                    request_id: bounded("eg-asr-whisper-rpc")?,
                    sequence,
                    source: eg_audio::asr::AudioSourceRef {
                        stream_id: bounded("eg-asr-whisper-rpc")?,
                        generation: eg_audio::ingress::StreamGeneration(1),
                        start_ms: seg_start as u64,
                        end_ms: seg_end as u64,
                        source_digest: sha256_hex_of_window(window),
                    },
                    text_ref: bounded(&format!("text-{sequence:06}", sequence = sequence.0))?,
                    language: language_tag(detected_language.as_deref())?,
                    task: if opts.translate {
                        AsrTask::Translate
                    } else {
                        AsrTask::Transcribe
                    },
                    timing: timing.clone(),
                    quality: quality.clone(),
                    model_manifest: self.manifest.clone(),
                    supersedes: None,
                };
                if asr_segment.validate().is_err() {
                    continue;
                }
                if !matches!(timing, SegmentTiming::Provided { .. }) {
                    all_timed = false;
                }
                on_partial(StreamingPartial {
                    sequence,
                    text: text.clone(),
                    timing: timing.clone(),
                    quality: quality.clone(),
                });
                segments.push(TranscribedSegment {
                    segment: asr_segment,
                    text,
                });
            }
        }

        if cancel.is_cancelled() {
            return Err(AsrError::Cancelled);
        }
        if segments.is_empty() {
            // Honest failure, not an empty-success: silence/no-speech input
            // produces zero accepted segments, and this is reported as a
            // distinct typed condition rather than an empty transcript that
            // would read as "successfully transcribed nothing said".
            return Err(AsrError::QualityUnavailable);
        }

        Ok(TranscriptionOutcome {
            segments,
            language: detected_language.unwrap_or_else(|| "unknown".to_string()),
            timing_available: all_timed,
        })
    }
}

fn language_tag(detected: Option<&str>) -> Result<LanguageTag, AsrError> {
    bounded(detected.unwrap_or("unknown"))
}

/// Real, provider-derived quality: `whisper-rs` 0.16's `WhisperSegment`
/// exposes `no_speech_probability()` directly. `avg_logprob` is computed as
/// the mean of `ln(token_probability())` over the segment's tokens — exactly
/// OpenAI Whisper's own `avg_logprob` definition (mean log-probability of the
/// selected tokens), not a text-length heuristic (the Meetily anti-pattern
/// the frozen contract's module doc calls out by name). If a segment has no
/// tokens to average (should not happen for non-empty text, but never
/// assumed), quality is honestly [`Quality::Unavailable`] rather than a
/// fabricated number.
fn segment_quality(seg: &whisper_rs::WhisperSegment<'_>) -> Quality {
    let n_tokens = seg.n_tokens();
    if n_tokens <= 0 {
        return Quality::Unavailable;
    }
    let mut sum_logprob = 0f64;
    let mut counted = 0u32;
    for t in 0..n_tokens {
        if let Some(token) = seg.get_token(t) {
            let p = token.token_probability().max(1e-6);
            sum_logprob += f64::from(p.ln());
            counted += 1;
        }
    }
    if counted == 0 {
        return Quality::Unavailable;
    }
    let avg_logprob = (sum_logprob / f64::from(counted)) as f32;
    let no_speech_prob = seg.no_speech_probability().clamp(0.0, 1.0);
    Quality::Calibrated {
        avg_logprob,
        no_speech_prob,
    }
}

/// Merge whisper.cpp per-token timestamps into word-level spans using the
/// tokenizer's own word-boundary convention (a token whose decoded text
/// starts with a space begins a new word) — the same merge whisper.cpp's own
/// CLI uses to print word-level output, not an invented heuristic. Returns an
/// empty vec (never fabricated spans) if `token_timestamps` produced no
/// usable per-token timing.
fn merge_tokens_into_words(
    seg: &whisper_rs::WhisperSegment<'_>,
    window_offset_ms: i64,
) -> Vec<WordSpan> {
    let mut words = Vec::new();
    let mut current_start: Option<i64> = None;
    let mut current_end: i64 = 0;
    let n_tokens = seg.n_tokens();
    for t in 0..n_tokens {
        let Some(token) = seg.get_token(t) else {
            continue;
        };
        let Ok(text) = token.to_str() else { continue };
        if text.starts_with("[_") || text.starts_with("<|") {
            // Special/control tokens (e.g. `[_TT_xx]`, `<|en|>`) never start
            // or extend a word.
            continue;
        }
        let data = token.token_data();
        let (t0, t1) = (data.t0, data.t1);
        if t1 <= t0 {
            // No usable per-token timing for this token (token_timestamps
            // was not effectively enabled, or whisper.cpp did not resolve
            // this token) — never fabricate a span for it.
            continue;
        }
        let starts_new_word = text.starts_with(' ') || words.is_empty() && current_start.is_none();
        if starts_new_word {
            if let Some(start) = current_start {
                words.push(WordSpan {
                    start_ms: (window_offset_ms + start * 10) as u64,
                    end_ms: (window_offset_ms + current_end * 10) as u64,
                });
            }
            current_start = Some(t0);
        }
        current_end = t1;
        if current_start.is_none() {
            current_start = Some(t0);
        }
    }
    if let Some(start) = current_start {
        words.push(WordSpan {
            start_ms: (window_offset_ms + start * 10) as u64,
            end_ms: (window_offset_ms + current_end * 10) as u64,
        });
    }
    words
}

fn sha256_hex_of_window(window: &[f32]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for sample in window {
        hasher.update(sample.to_le_bytes());
    }
    format!("{:x}", hasher.finalize())
}
