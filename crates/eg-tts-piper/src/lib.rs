//! # eg-tts-piper — native Piper-ONNX text-to-speech provider (GOC-34, `OWNER-VOICE-TTS`)
//!
//! Implements a real, streaming, cancellable synthesis provider behind the frozen
//! `tts.*` wire contract in `eg-audio`'s `tts` feature (`crates/eg-audio/src/tts.rs`).
//! That module is validation-only ("no I/O, no inference"); this crate is the first
//! concrete engine wired behind it.
//!
//! ## Integration choice: depend on `piper-rs`, do not vendor it
//!
//! [`piper-rs`](https://github.com/thewh1teagle/piper-rs) `0.2.0` and its `espeak-rs`
//! `0.2.0` dependency are published to **crates.io** (`piper-rs` publish timestamp
//! 2026-05-21, matching the `Cargo.toml` version audited at this workspace's
//! `open-source-libraries/piper-rs` checkout). This crate depends on both as ordinary,
//! exact-pinned (`=0.2.0`) registry dependencies — **not** a `git =` source and **not**
//! a vendored/forked copy of piper-rs's source tree.
//!
//! This is a deliberate choice, not a default:
//!
//! - **The workspace's crates.io-only Rust dependency edict** (`agent-packages/
//!   epistemic-graph/AGENTS.md`) forbids a `git =` source for a published crate. Since
//!   piper-rs 0.2.0 is genuinely on the registry, a fork/vendor mirror would be *adding*
//!   a maintenance burden (tracking upstream fixes by hand) to work around a constraint
//!   that no longer applies, not honoring it.
//! - The GOC-34 lane's own audit of `open-source-libraries/piper-rs` (see the lane
//!   doc's "Evidence and current behavior") found piper-rs's *inference core* — ONNX
//!   session load, phoneme-id tensor construction, eSpeak phonemization — sound enough
//!   to reuse; what it lacks is everything a governed engine must add on TOP: bounded
//!   phrase segmentation, cancellable streaming, digest-verified model loading, and
//!   pre-flight validation of phoneme coverage and speaker-id range against the ACTUAL
//!   loaded voice (not just a declared limit). None of that requires touching piper-rs's
//!   internals — it is a wrapping/orchestration problem, which is exactly what this
//!   crate is. Forking would duplicate ~450 lines of working ONNX/tensor code for no
//!   behavioral gain.
//! - `espeak-rs-sys` (transitive) vendors the espeak-ng C sources and CMake-builds them
//!   **inside its own published crate tarball** — the identical "C sources ship in the
//!   crate, built by `cc`/`cmake`, no network fetch at build time" shape the workspace's
//!   root `Cargo.toml` already accepts for the `ros2-rmw` cyclonedds leg. It needs a C
//!   toolchain (`cc`, `cmake`) and `libclang` (for `bindgen`) at build time but nothing
//!   else — no submodule, no runtime download.
//!
//! ## What this crate does NOT do (DEF-017 / DEF-018 scope guards)
//!
//! - **DEF-017**: this is a Piper-specific ONNX/JSON runtime. It loads exactly the
//!   fixed-shape VITS-family graph (`input`/`input_lengths`/`scales`[/`sid`] tensors,
//!   a `phoneme_id_map`-keyed vocabulary) piper-rs's `model.rs` targets. It never claims,
//!   and has no code path implying, generic HuggingFace/Transformers/safetensors support.
//!   An incompatible model/config is a typed [`eg_audio::tts::TtsError::ModelUnavailable`]
//!   before any inference is attempted — never a best-effort load.
//! - **DEF-018**: no speaker diarization, voice-biometric identity extraction, or speaker
//!   verification exists here. [`eg_audio::tts::SpeakerSelection`] only selects which
//!   trained embedding ROW a multi-speaker voice's decoder conditions on for synthesis
//!   OUTPUT — it never identifies, verifies, or fingerprints a human speaker.
//! - **No model acquisition.** GOC-36 owns governed model acquisition, digest
//!   verification, licensing, and manifests. This crate takes a resolved, on-disk
//!   `(model_path, config_path)` pair as input (see [`voice::resolve_voice_paths`]) and
//!   independently re-verifies both files against the request's declared SHA-256
//!   digests before ever touching them — it never downloads, and an absent or
//!   digest-mismatched pair is a typed [`eg_audio::tts::TtsError::ModelUnavailable`]
//!   failure, never a silent fetch or a best-effort load. Until GOC-36's real artifact
//!   resolver lands, path resolution reads a single operator-configured directory (see
//!   `voice::VOICE_MODEL_DIR_ENV`) — an explicit, documented interim seam, not a
//!   permanent design.
//!
//! ## Licensing note
//!
//! Piper voices ship as an `.onnx` weight file plus an `.onnx.json` config. Trained
//! voice weights carry their OWN license (commonly but not universally CC-BY/CC0 per
//! voice — some Piper voices are released under more restrictive terms). This crate
//! ships **no voice weights** and makes no claim about any specific voice's license;
//! GOC-36's manifest is the place that record is meant to live, per the lane contract's
//! explicit call-out that "some [voices] carry restrictive terms."

pub mod streaming;
pub mod voice;

pub use streaming::{
    segment_phrases, synthesize_streaming, CancellationToken, ChunkStream, SynthesizeRequest,
    SynthesizedChunk,
};
pub use voice::{resolve_voice_paths, verify_voice_digests, LoadedVoice, VoicePaths};

// A default/`full` engine build links none of this crate — see root Cargo.toml's
// `tts-piper` feature, which is the ONLY facade feature that pulls `dep:eg-tts-piper`.
// This guardrail is a compile-time fact (no `#[cfg]` here would even make sense to
// assert it from inside this crate), so it is asserted from the FACADE side instead —
// see `epistemic-graph/src/lib.rs`'s equivalent guardrail test for `tts`/`asr`/`ingress`.
