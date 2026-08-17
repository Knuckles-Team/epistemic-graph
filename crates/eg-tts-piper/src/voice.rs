//! Digest-verified Piper voice loading and pre-flight validation.
//!
//! Closes two gaps the GOC-34 lane audit found in `piper-rs` itself: an unknown
//! phoneme character is silently skipped (`model.rs`'s `phonemes_to_ids`), and a
//! caller-supplied speaker id is never checked against the model's actual speaker
//! count. Both checks require the ACTUAL loaded voice config, which `eg-audio`'s pure
//! `tts.rs` module explicitly defers to "worker time" (see that module's doc on
//! `SpeakerSelection`) — this is that worker-time code.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use eg_audio::tts::{SpeakerSelection, TtsError, VoiceModelRef};

/// Operator-configured directory containing `<model_artifact_id>.onnx` and
/// `<config_artifact_id>.onnx.json` files. An explicit, documented interim seam
/// (see the crate doc's "No model acquisition" section) until GOC-36 supplies a
/// real governed artifact resolver — this crate never falls back to a default
/// path, a bundled model, or a network fetch when this is unset.
pub const VOICE_MODEL_DIR_ENV: &str = "EPISTEMIC_GRAPH_VOICE_MODEL_DIR";

/// Resolved, not-yet-verified filesystem locations for one voice's model + config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoicePaths {
    pub model_path: PathBuf,
    pub config_path: PathBuf,
}

/// Resolve `voice`'s `model_artifact_id`/`config_artifact_id` against `dir`.
///
/// `TtsBoundedId`'s own charset (`[A-Za-z0-9_:-]`, 1..=128 bytes — see
/// `eg-audio/src/ingress.rs`'s `BoundedId::new`) contains neither `/` nor `.`, so a
/// path built by joining `dir` with an artifact id cannot escape `dir` — no separate
/// path-traversal check is needed here, the id's own type guarantees it.
///
/// Fails closed with [`TtsError::ModelUnavailable`] when the directory is
/// unconfigured or either file does not exist — this function never downloads or
/// synthesizes a fallback path.
pub fn resolve_voice_paths(dir: &Path, voice: &VoiceModelRef) -> Result<VoicePaths, TtsError> {
    let model_path = dir.join(format!("{}.onnx", voice.model_artifact_id.as_str()));
    let config_path = dir.join(format!("{}.onnx.json", voice.config_artifact_id.as_str()));
    if !model_path.is_file() {
        return Err(TtsError::ModelUnavailable {
            reason: "voice model .onnx file is absent at the configured voice model directory",
        });
    }
    if !config_path.is_file() {
        return Err(TtsError::ModelUnavailable {
            reason:
                "voice config .onnx.json file is absent at the configured voice model directory",
        });
    }
    Ok(VoicePaths {
        model_path,
        config_path,
    })
}

fn sha256_hex_file(path: &Path) -> Result<String, TtsError> {
    let file = fs::File::open(path).map_err(|_| TtsError::ModelUnavailable {
        reason: "voice artifact file could not be opened for digest verification",
    })?;
    let mut reader = std::io::BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader
            .read(&mut buf)
            .map_err(|_| TtsError::ModelUnavailable {
                reason: "voice artifact file could not be read for digest verification",
            })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex_lower(&hasher.finalize()))
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Independently re-verify both artifact files' SHA-256 against `voice`'s DECLARED
/// digests — a real worker's own defense-in-depth on top of `VoiceModelRef::validate`
/// (which only checks the digest STRING is 64-hex-shaped, not that it matches real
/// bytes). Fails closed with [`TtsError::ModelUnavailable`] on any I/O error or
/// mismatch; never proceeds on an unverified pair.
pub fn verify_voice_digests(paths: &VoicePaths, voice: &VoiceModelRef) -> Result<(), TtsError> {
    let model_digest = sha256_hex_file(&paths.model_path)?;
    if model_digest != voice.model_digest {
        return Err(TtsError::ModelUnavailable {
            reason: "voice model file digest does not match the request's declared model_digest",
        });
    }
    let config_digest = sha256_hex_file(&paths.config_path)?;
    if config_digest != voice.config_digest {
        return Err(TtsError::ModelUnavailable {
            reason: "voice config file digest does not match the request's declared config_digest",
        });
    }
    Ok(())
}

/// Minimal, independently-parsed mirror of piper-rs's private `model::ModelConfig` —
/// held ONLY for pre-flight validation (phoneme coverage, speaker bound, native
/// sample rate) this crate performs BEFORE calling into piper-rs, since piper-rs
/// does not expose its own parsed config back to the caller.
#[derive(Deserialize)]
struct PreflightConfig {
    audio: PreflightAudio,
    #[serde(default)]
    num_speakers: u32,
    #[serde(default)]
    speaker_id_map: HashMap<String, i64>,
    phoneme_id_map: HashMap<char, Vec<i64>>,
}

#[derive(Deserialize)]
struct PreflightAudio {
    sample_rate: u32,
}

const BOS: char = '^';
const EOS: char = '$';
const PAD: char = '_';

/// A loaded, digest-verified Piper voice, ready to synthesize.
pub struct LoadedVoice {
    piper: piper_rs::Piper,
    sample_rate: u32,
    num_speakers: u32,
    max_speaker_id: i64,
    phoneme_chars: HashSet<char>,
}

impl std::fmt::Debug for LoadedVoice {
    /// `piper_rs::Piper` itself has no `Debug` impl (it holds a live ONNX
    /// `Session`) — this reports only the metadata this crate independently
    /// parsed, never the session/model internals.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedVoice")
            .field("sample_rate", &self.sample_rate)
            .field("num_speakers", &self.num_speakers)
            .field("max_speaker_id", &self.max_speaker_id)
            .field("known_phoneme_count", &self.phoneme_chars.len())
            .finish()
    }
}

/// Build an ONNX `Session` over `model_path` with graph optimization DISABLED and a
/// single intra-op thread — the most PORTABLE CPU execution-provider path. `ort`'s
/// only x86_64 prebuilt CPU binary (`ort-sys`'s `download-binaries`) assumes a modern
/// AVX2 baseline in its optimized/fused kernels; part of this workspace's fleet
/// predates AVX2 (documented at `src/lib.rs`'s crate doc). Disabling graph fusion asks
/// the runtime for its most conservative kernel path rather than an optimized one —
/// "CPU is the required baseline... acceleration is opt-in and auto-detected, never
/// required" applies here too: this is the SAFE default, never a claim of the fastest
/// path. `piper_rs::Piper::new` builds an unconfigured `Session::builder()`
/// internally (`src/lib.rs:52-63`), so this crate builds the session itself and hands
/// it to `Piper::from_session` instead.
fn build_portable_onnx_session(model_path: &Path) -> ort::Result<ort::session::Session> {
    let mut builder = ort::session::Session::builder()?
        .with_optimization_level(ort::session::builder::GraphOptimizationLevel::Disable)?
        .with_intra_threads(1)?;
    builder.commit_from_file(model_path)
}

impl LoadedVoice {
    /// Load and pre-flight-validate a digest-verified voice. Callers MUST call
    /// [`verify_voice_digests`] first — this function does not re-check digests, only
    /// shape/compatibility.
    pub fn load(paths: &VoicePaths) -> Result<Self, TtsError> {
        let config_bytes =
            fs::read(&paths.config_path).map_err(|_| TtsError::ModelUnavailable {
                reason: "voice config file could not be read",
            })?;
        let preflight: PreflightConfig =
            serde_json::from_slice(&config_bytes).map_err(|_| TtsError::ModelUnavailable {
                reason: "voice config is not a valid Piper JSON config (DEF-017: this is a \
                    Piper-specific ONNX/JSON loader, not a generic HuggingFace config reader)",
            })?;
        if preflight.phoneme_id_map.is_empty() {
            return Err(TtsError::ModelUnavailable {
                reason: "voice config phoneme_id_map is empty — not a usable Piper voice",
            });
        }
        // Build the ONNX session explicitly (rather than `piper_rs::Piper::new`, which
        // builds an unconfigured `Session::builder()` internally — `src/lib.rs:52-63`)
        // so the CPU execution provider path is forced to the most PORTABLE kernel
        // selection: graph optimization disabled and single-threaded intra-op. This
        // is the honest "CPU is the required baseline, acceleration is opt-in and
        // auto-detected, never required" posture for a fleet with pre-AVX2 hosts (see
        // this crate's own doc) — never a claim of a faster/optimized path.
        let piper_config: piper_rs::ModelConfig =
            serde_json::from_slice(&config_bytes).map_err(|_| TtsError::ModelUnavailable {
                reason: "voice config is not a valid Piper JSON config",
            })?;
        let session = build_portable_onnx_session(&paths.model_path).map_err(|_| {
            TtsError::ModelUnavailable {
                reason: "ONNX session failed to build (malformed graph or unsupported ONNX \
                    shape — DEF-017: this is a Piper-specific loader, an incompatible graph \
                    is a typed rejection, never a best-effort load)",
            }
        })?;
        let piper = piper_rs::Piper::from_session(session, piper_config);
        let max_speaker_id = if !preflight.speaker_id_map.is_empty() {
            preflight
                .speaker_id_map
                .values()
                .copied()
                .max()
                .unwrap_or(0)
        } else {
            i64::from(preflight.num_speakers).saturating_sub(1).max(0)
        };
        Ok(Self {
            piper,
            sample_rate: preflight.audio.sample_rate,
            num_speakers: preflight.num_speakers,
            max_speaker_id,
            phoneme_chars: preflight.phoneme_id_map.into_keys().collect(),
        })
    }

    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }

    /// Validate a request's [`SpeakerSelection`] against the voice's ACTUAL speaker
    /// count/map (not just the request's own declared `max_speaker_id` bound, which
    /// `eg_audio::tts::validate_request` already checked). Returns the `speaker_id`
    /// argument to pass to piper-rs's `create` (`None` for a single-speaker voice,
    /// where piper-rs ignores the argument entirely — see `model.rs`'s
    /// `config.num_speakers > 1` gate).
    pub fn validate_speaker(&self, selection: SpeakerSelection) -> Result<Option<i64>, TtsError> {
        match selection {
            SpeakerSelection::Default => Ok(None),
            SpeakerSelection::Id(id) => {
                if self.num_speakers <= 1 {
                    if id == 0 {
                        Ok(None)
                    } else {
                        Err(TtsError::UnsupportedSpeaker)
                    }
                } else if i64::from(id) > self.max_speaker_id {
                    Err(TtsError::UnsupportedSpeaker)
                } else {
                    Ok(Some(i64::from(id)))
                }
            }
        }
    }

    /// Resolve `input_mode`'s text/phonemes to a final phoneme string, phonemizing
    /// through `espeak-rs` for [`eg_audio::tts::InputMode::Text`] exactly as
    /// piper-rs's own `Piper::create` does internally (`src/lib.rs:83-89`) — done
    /// here instead so the result can be pre-flight-validated against the ACTUAL
    /// loaded phoneme vocabulary before inference ever runs.
    pub fn resolve_phonemes(
        &self,
        input_mode: eg_audio::tts::InputMode,
        text_or_phonemes: &str,
        espeak_voice: &str,
    ) -> Result<String, TtsError> {
        let phonemes = match input_mode {
            eg_audio::tts::InputMode::Phonemes => text_or_phonemes.to_string(),
            eg_audio::tts::InputMode::Text => {
                let phrases = espeak_rs::text_to_phonemes(text_or_phonemes, espeak_voice, None)
                    .map_err(|_| TtsError::UnsupportedLanguage)?;
                phrases.join(" ")
            }
        };
        self.validate_phonemes(&phonemes)?;
        Ok(phonemes)
    }

    /// Reject a phoneme string containing any character absent from the voice's
    /// `phoneme_id_map` — the "silent phoneme drop" gap the lane audit found in
    /// piper-rs's `phonemes_to_ids` (`model.rs:42-58`), closed here instead of inside
    /// piper-rs (that function is private to the piper-rs crate).
    fn validate_phonemes(&self, phonemes: &str) -> Result<(), TtsError> {
        if phonemes.trim().is_empty() {
            return Ok(()); // an empty phrase (e.g. pure punctuation) is skipped by the
                           // caller, not an error at this layer.
        }
        for ch in phonemes.chars() {
            if matches!(ch, BOS | EOS | PAD) {
                continue;
            }
            if ch.is_whitespace() {
                continue;
            }
            if !self.phoneme_chars.contains(&ch) {
                return Err(TtsError::MalformedRequest {
                    reason: "phoneme character has no entry in the voice's phoneme_id_map \
                        (piper-rs would silently drop it; this provider fails closed instead)",
                });
            }
        }
        Ok(())
    }

    /// Run one bounded phrase-level inference. Returns raw f32 PCM samples at the
    /// voice's native sample rate. This is ONE synchronous ONNX forward pass — it
    /// cannot be interrupted mid-computation (no async yield point exists inside
    /// `ort`'s `Session::run`), matching the lane design's own acceptance that
    /// cancellation granularity is per-phrase, not mid-inference.
    pub fn create_raw(
        &mut self,
        phonemes: &str,
        speaker_id: Option<i64>,
        controls: eg_audio::tts::SynthesisControls,
    ) -> Result<(Vec<f32>, u32), TtsError> {
        self.piper
            .create(
                phonemes,
                true, // is_phonemes: this crate always resolves to phonemes first (see resolve_phonemes)
                speaker_id,
                Some(controls.length_scale),
                Some(controls.noise_scale),
                Some(controls.noise_w),
            )
            .map_err(|e| TtsError::ModelUnavailable {
                reason: piper_infer_error_reason(&e),
            })
    }
}

fn piper_infer_error_reason(err: &piper_rs::PiperError) -> &'static str {
    match err {
        piper_rs::PiperError::InferenceError(_) => {
            "ONNX inference failed for this phoneme sequence (unsupported graph I/O or \
                non-finite intermediate state)"
        }
        piper_rs::PiperError::PhonemizationError(_) => {
            "eSpeak-ng phonemization failed for this input"
        }
        piper_rs::PiperError::FailedToLoadResource(_) => {
            "voice reported a resource-load error during synthesis"
        }
    }
}
