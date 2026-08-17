//! Integration tests: real ONNX inference through `ort`/`piper-rs`, real streaming,
//! real cancellation, and honest fail-closed behavior for every required negative
//! case. The fixture voice (see `support::onnx_fixture`) contains no trained weights —
//! it is a tiny hand-built ONNX graph that tiles a caller-chosen template by phoneme
//! count, letting these tests engineer exact audio properties (clean / clipping /
//! non-finite) while still exercising this crate's REAL code paths end to end.

mod support;

use std::collections::HashMap;

use eg_audio::ingress::CarrierRef;
use eg_audio::tts::{
    self, InputMode, JobStatus, OutputEncoding, OutputFormat, PolicyDecision, SpeakerSelection,
    SynthesisControls, TtsBoundedId, TtsError, TtsLimits,
};
use eg_tts_piper::{
    resolve_voice_paths, segment_phrases, synthesize_streaming, verify_voice_digests,
    CancellationToken, LoadedVoice, SynthesizeRequest,
};
use support::{sha256_hex_bytes, FixtureConfig};

fn id(s: &str) -> TtsBoundedId {
    TtsBoundedId::new(s).expect("fixture id is a valid bounded token")
}

fn carrier() -> CarrierRef {
    CarrierRef {
        tenant: id("tenant-1"),
        actor: id("actor-1"),
        consent_ref: id("consent-1"),
        purpose: id("purpose-1"),
        trace_id: id("trace-1"),
    }
}

fn limits() -> TtsLimits {
    TtsLimits {
        max_text_bytes: 64 * 1024,
        max_phoneme_bytes: 64 * 1024,
        max_chunk_decoded_bytes: 2 * 1024 * 1024,
        max_chunks: 256,
        max_output_ms: 30_000,
        max_speaker_id: 8,
        request_deadline_ms: 60_000,
    }
}

fn clean_sine_template(len: usize, amplitude: f32) -> Vec<f32> {
    (0..len)
        .map(|i| amplitude * (2.0 * std::f32::consts::PI * 3.0 * i as f32 / len as f32).sin())
        .collect()
}

/// Full contract-to-audio round trip: `validate_request` -> digest-verified voice
/// load -> real streaming ONNX synthesis -> `finalize_result`. Proves real audio
/// properties, not just "bytes returned".
#[test]
fn full_contract_round_trip_produces_real_audio() {
    let template = clean_sine_template(256, 0.4);
    let config = FixtureConfig::single_speaker(&['a', 'b']);
    let fixture = support::write_fixture("round-trip", &template, &config);

    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: fixture.model_digest(),
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };

    let phonemes = "a b a";
    let input_digest = sha256_hex_bytes(phonemes.as_bytes());

    let request = tts::TtsRequest {
        protocol_version: tts::TTS_PROTOCOL_VERSION,
        request_id: id("request-1"),
        job_id: id("job-1"),
        carrier: carrier(),
        voice: voice_ref.clone(),
        language: id("en"),
        espeak_voice: id("en-us"),
        speaker: SpeakerSelection::Default,
        input: tts::SensitiveInput {
            mode: InputMode::Phonemes,
            input_ref: id("input-1"),
            input_digest: input_digest.clone(),
            byte_len: phonemes.len() as u32,
        },
        controls: SynthesisControls {
            length_scale: 1.0,
            noise_scale: 0.667,
            noise_w: 0.8,
        },
        output_format: OutputFormat {
            encoding: OutputEncoding::Pcm16Le,
            sample_rate: config.sample_rate,
            channels: 1,
        },
        limits: limits(),
        deadline_ms: 60_000,
        idempotency_key: id("idem-1"),
    };

    tts::validate_request(&request).expect("well-formed request passes the frozen contract");
    assert_eq!(
        sha256_hex_bytes(phonemes.as_bytes()),
        request.input.input_digest,
        "sensitive input bytes must hash to the request's declared digest before use"
    );

    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).expect("fixture files resolve");
    verify_voice_digests(&paths, &voice_ref).expect("fixture digests verify");
    let voice = LoadedVoice::load(&paths).expect("fixture voice loads");
    assert_eq!(voice.sample_rate(), config.sample_rate);

    let stream = synthesize_streaming(
        voice,
        SynthesizeRequest {
            request_id: request.request_id.clone(),
            job_id: request.job_id.clone(),
            input_mode: request.input.mode,
            input_text: phonemes.to_string(),
            espeak_voice: request.espeak_voice.as_str().to_string(),
            speaker: request.speaker,
            controls: request.controls,
            output_format: request.output_format,
            limits: request.limits,
        },
        CancellationToken::new(),
    )
    .expect("synthesis starts");

    let mut chunks = Vec::new();
    let mut pcm_total = Vec::new();
    for item in stream {
        let synthesized = item.expect("no synthesis error for a clean fixture");
        pcm_total.extend_from_slice(&synthesized.pcm);
        chunks.push(synthesized.chunk);
    }
    assert!(!chunks.is_empty(), "at least one chunk must be produced");
    assert!(chunks.last().unwrap().is_final);

    // Real audio-property assertions, not just "bytes returned":
    assert_eq!(pcm_total.len() % 2, 0, "PCM16LE is 2 bytes per sample");
    let sample_count = pcm_total.len() / 2;
    // piper-rs's `phonemes_to_ids` (model.rs) wraps the sequence as
    // BOS, (char_id, pad_id) per recognized character, EOS — for "a b a" (spaces
    // skipped, not in the phoneme map) that's 1 + 3*2 + 1 = 8 ids, and this
    // fixture's `Tile` graph repeats its 256-sample template once per id (its
    // `input_lengths` value), so 8 * 256 = 2048 samples.
    assert_eq!(sample_count, 8 * 256);
    let expected_ms = (sample_count as u64 * 1000) / u64::from(config.sample_rate);
    assert!(
        (1..=1000).contains(&expected_ms),
        "plausible non-zero duration for 3 phonemes (8 ids incl. BOS/EOS/PAD), got {expected_ms}ms"
    );

    let samples: Vec<i16> = pcm_total
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect();
    assert!(
        samples.iter().any(|&s| s != 0),
        "synthesized audio must not be silent"
    );
    let peak = samples.iter().map(|&s| s.unsigned_abs()).max().unwrap();
    // amplitude 0.4 -> peak ~= 0.4 * 32767 ~= 13106; allow generous tolerance.
    assert!(
        (5_000..=20_000).contains(&(peak as i32)),
        "peak amplitude {peak} should reflect the fixture's 0.4 template amplitude"
    );

    let authorized = tts::authorize_carrier(&request.carrier, PolicyDecision::Authorized)
        .expect("policy authorizes this carrier");
    let result = tts::finalize_result(&authorized, &request, chunks, JobStatus::Succeeded)
        .expect("well-formed, clean chunks finalize as Succeeded");
    assert_eq!(result.total_sample_count, sample_count as u64);
    assert_eq!(result.quality.non_finite_samples, 0);
    assert_eq!(result.quality.clipped_samples, 0);
    assert_eq!(result.deterministic, tts::DeterminismClaim::Unverified);
}

#[test]
fn cancellation_stops_streaming_before_all_chunks_are_produced() {
    // A long single phrase, with a tiny max_chunk_decoded_bytes, so it splits into
    // many audio-byte chunks within ONE phrase — cancellation is checked between
    // every sub-chunk, not just between phrases.
    let template = clean_sine_template(2000, 0.2);
    let config = FixtureConfig::single_speaker(&['a']);
    let fixture = support::write_fixture("cancel", &template, &config);
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: fixture.model_digest(),
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let voice = LoadedVoice::load(&paths).unwrap();

    let mut tight_limits = limits();
    // Single phoneme 'a' -> piper's BOS/(char,pad)/EOS wrapping gives input_len = 4,
    // so this fixture's Tile graph produces 4 * 2000 = 8000 samples; 32 samples per
    // chunk (max_chunk_decoded_bytes=64, 2 bytes/sample) -> 250 chunks total.
    tight_limits.max_chunk_decoded_bytes = 64;

    let cancel = CancellationToken::new();
    let stream = synthesize_streaming(
        voice,
        SynthesizeRequest {
            request_id: id("request-1"),
            job_id: id("job-1"),
            input_mode: InputMode::Phonemes,
            input_text: "a".to_string(),
            espeak_voice: "en-us".to_string(),
            speaker: SpeakerSelection::Default,
            controls: SynthesisControls {
                length_scale: 1.0,
                noise_scale: 0.667,
                noise_w: 0.8,
            },
            output_format: OutputFormat {
                encoding: OutputEncoding::Pcm16Le,
                sample_rate: config.sample_rate,
                channels: 1,
            },
            limits: tight_limits,
        },
        cancel.clone(),
    )
    .expect("synthesis starts");

    let mut received = 0u32;
    let mut saw_cancelled_error = false;
    for item in stream {
        match item {
            Ok(_) => {
                received += 1;
                if received == 1 {
                    cancel.cancel();
                }
            }
            Err(TtsError::Cancelled) => {
                saw_cancelled_error = true;
                break;
            }
            Err(other) => panic!("unexpected error: {other:?}"),
        }
    }
    assert!(
        saw_cancelled_error,
        "a cancelled stream must report Cancelled"
    );
    assert!(
        received < 250,
        "cancellation must stop synthesis before all 250 chunks are produced, got {received}"
    );
}

#[test]
fn missing_model_file_fails_closed() {
    let dir = std::env::temp_dir().join(format!("eg-tts-piper-empty-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id("absent-model"),
        config_artifact_id: id("absent-config"),
        model_digest: "a".repeat(64),
        config_digest: "b".repeat(64),
        signature_ref: id("signature-1"),
    };
    let err = resolve_voice_paths(&dir, &voice_ref).unwrap_err();
    assert!(matches!(err, TtsError::ModelUnavailable { .. }));
}

#[test]
fn digest_mismatch_fails_closed() {
    let template = clean_sine_template(64, 0.2);
    let config = FixtureConfig::single_speaker(&['a']);
    let fixture = support::write_fixture("digest-mismatch", &template, &config);
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: "0".repeat(64), // wrong on purpose
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).unwrap();
    let err = verify_voice_digests(&paths, &voice_ref).unwrap_err();
    assert!(matches!(err, TtsError::ModelUnavailable { .. }));
}

#[test]
fn malformed_config_json_fails_closed() {
    let dir = std::env::temp_dir().join(format!("eg-tts-piper-badcfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let template = clean_sine_template(64, 0.2);
    std::fs::write(
        dir.join("model-badcfg.onnx"),
        support::build_fixture_onnx(&template),
    )
    .unwrap();
    std::fs::write(dir.join("config-badcfg.onnx.json"), b"{ not json").unwrap();
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id("model-badcfg"),
        config_artifact_id: id("config-badcfg"),
        model_digest: support::sha256_hex_file(&dir.join("model-badcfg.onnx")),
        config_digest: support::sha256_hex_file(&dir.join("config-badcfg.onnx.json")),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let err = LoadedVoice::load(&paths).unwrap_err();
    assert!(matches!(err, TtsError::ModelUnavailable { .. }));
}

#[test]
fn malformed_onnx_fails_closed() {
    let dir = std::env::temp_dir().join(format!("eg-tts-piper-badonnx-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let config = FixtureConfig::single_speaker(&['a']);
    std::fs::write(dir.join("model-badonnx.onnx"), b"not an onnx file at all").unwrap();
    std::fs::write(dir.join("config-badonnx.onnx.json"), config.to_json()).unwrap();
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id("model-badonnx"),
        config_artifact_id: id("config-badonnx"),
        model_digest: support::sha256_hex_file(&dir.join("model-badonnx.onnx")),
        config_digest: support::sha256_hex_file(&dir.join("config-badonnx.onnx.json")),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let err = LoadedVoice::load(&paths).unwrap_err();
    assert!(matches!(err, TtsError::ModelUnavailable { .. }));
}

#[test]
fn unsupported_speaker_id_fails_closed_before_inference() {
    let template = clean_sine_template(64, 0.2);
    let mut speaker_id_map = HashMap::new();
    speaker_id_map.insert("narrator".to_string(), 0i64);
    speaker_id_map.insert("narrator2".to_string(), 1i64);
    let config = FixtureConfig {
        sample_rate: 22_050,
        num_speakers: 2,
        speaker_id_map,
        phoneme_chars: vec!['a'],
    };
    let fixture = support::write_fixture("speaker", &template, &config);
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: fixture.model_digest(),
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let voice = LoadedVoice::load(&paths).unwrap();
    let err = voice
        .validate_speaker(SpeakerSelection::Id(99))
        .unwrap_err();
    assert!(matches!(err, TtsError::UnsupportedSpeaker));
    // In-range ids are accepted.
    assert_eq!(
        voice.validate_speaker(SpeakerSelection::Id(1)).unwrap(),
        Some(1)
    );
}

#[test]
fn empty_text_input_fails_closed_synchronously() {
    let template = clean_sine_template(64, 0.2);
    let config = FixtureConfig::single_speaker(&['a']);
    let fixture = support::write_fixture("empty", &template, &config);
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: fixture.model_digest(),
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let voice = LoadedVoice::load(&paths).unwrap();

    let err = synthesize_streaming(
        voice,
        SynthesizeRequest {
            request_id: id("request-1"),
            job_id: id("job-1"),
            input_mode: InputMode::Text,
            input_text: "   ".to_string(),
            espeak_voice: "en-us".to_string(),
            speaker: SpeakerSelection::Default,
            controls: SynthesisControls {
                length_scale: 1.0,
                noise_scale: 0.667,
                noise_w: 0.8,
            },
            output_format: OutputFormat {
                encoding: OutputEncoding::Pcm16Le,
                sample_rate: config.sample_rate,
                channels: 1,
            },
            limits: limits(),
        },
        CancellationToken::new(),
    )
    .unwrap_err();
    assert!(matches!(err, TtsError::MalformedRequest { .. }));
}

#[test]
fn unknown_phoneme_character_fails_closed() {
    let template = clean_sine_template(64, 0.2);
    // Config only knows 'a' — 'z' has no entry.
    let config = FixtureConfig::single_speaker(&['a']);
    let fixture = support::write_fixture("unknown-phoneme", &template, &config);
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: fixture.model_digest(),
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let voice = LoadedVoice::load(&paths).unwrap();

    let stream = synthesize_streaming(
        voice,
        SynthesizeRequest {
            request_id: id("request-1"),
            job_id: id("job-1"),
            input_mode: InputMode::Phonemes,
            input_text: "z".to_string(),
            espeak_voice: "en-us".to_string(),
            speaker: SpeakerSelection::Default,
            controls: SynthesisControls {
                length_scale: 1.0,
                noise_scale: 0.667,
                noise_w: 0.8,
            },
            output_format: OutputFormat {
                encoding: OutputEncoding::Pcm16Le,
                sample_rate: config.sample_rate,
                channels: 1,
            },
            limits: limits(),
        },
        CancellationToken::new(),
    )
    .expect("stream starts (per-phrase phonemization failures surface as stream items)");

    let first = stream.into_iter().next().expect("one item").unwrap_err();
    assert!(matches!(first, TtsError::MalformedRequest { .. }));
}

#[test]
fn output_format_mismatch_fails_closed_synchronously() {
    let template = clean_sine_template(64, 0.2);
    let config = FixtureConfig::single_speaker(&['a']);
    let fixture = support::write_fixture("format-mismatch", &template, &config);
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: fixture.model_digest(),
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let voice = LoadedVoice::load(&paths).unwrap();

    let err = synthesize_streaming(
        voice,
        SynthesizeRequest {
            request_id: id("request-1"),
            job_id: id("job-1"),
            input_mode: InputMode::Phonemes,
            input_text: "a".to_string(),
            espeak_voice: "en-us".to_string(),
            speaker: SpeakerSelection::Default,
            controls: SynthesisControls {
                length_scale: 1.0,
                noise_scale: 0.667,
                noise_w: 0.8,
            },
            output_format: OutputFormat {
                encoding: OutputEncoding::Pcm16Le,
                sample_rate: config.sample_rate + 1, // deliberately wrong
                channels: 1,
            },
            limits: limits(),
        },
        CancellationToken::new(),
    )
    .unwrap_err();
    assert!(matches!(err, TtsError::MalformedRequest { .. }));
}

#[test]
fn non_finite_template_is_measured_and_blocks_succeeded_but_not_degraded() {
    let mut template = clean_sine_template(64, 0.2);
    template[10] = f32::NAN;
    let config = FixtureConfig::single_speaker(&['a']);
    let fixture = support::write_fixture("nonfinite", &template, &config);
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: fixture.model_digest(),
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let voice = LoadedVoice::load(&paths).unwrap();

    let request = tts::TtsRequest {
        protocol_version: tts::TTS_PROTOCOL_VERSION,
        request_id: id("request-1"),
        job_id: id("job-1"),
        carrier: carrier(),
        voice: voice_ref,
        language: id("en"),
        espeak_voice: id("en-us"),
        speaker: SpeakerSelection::Default,
        input: tts::SensitiveInput {
            mode: InputMode::Phonemes,
            input_ref: id("input-1"),
            input_digest: sha256_hex_bytes(b"a"),
            byte_len: 1,
        },
        controls: SynthesisControls {
            length_scale: 1.0,
            noise_scale: 0.667,
            noise_w: 0.8,
        },
        output_format: OutputFormat {
            encoding: OutputEncoding::Pcm16Le,
            sample_rate: config.sample_rate,
            channels: 1,
        },
        limits: limits(),
        deadline_ms: 60_000,
        idempotency_key: id("idem-1"),
    };

    let stream = synthesize_streaming(
        voice,
        SynthesizeRequest {
            request_id: request.request_id.clone(),
            job_id: request.job_id.clone(),
            input_mode: InputMode::Phonemes,
            input_text: "a".to_string(),
            espeak_voice: "en-us".to_string(),
            speaker: SpeakerSelection::Default,
            controls: request.controls,
            output_format: request.output_format,
            limits: request.limits,
        },
        CancellationToken::new(),
    )
    .unwrap();
    let chunks: Vec<_> = stream.map(|c| c.unwrap().chunk).collect();
    assert!(chunks
        .iter()
        .any(|c| matches!(c.quality, tts::ChunkQuality::Measured { non_finite_samples, .. } if non_finite_samples > 0)));

    let authorized = tts::authorize_carrier(&request.carrier, PolicyDecision::Authorized).unwrap();
    let succeeded_err =
        tts::finalize_result(&authorized, &request, chunks.clone(), JobStatus::Succeeded)
            .unwrap_err();
    assert_eq!(succeeded_err, TtsError::NonFiniteAudio);

    let degraded = tts::finalize_result(&authorized, &request, chunks, JobStatus::Degraded)
        .expect("a degraded result may honestly carry the non-finite evidence");
    assert!(degraded.quality.non_finite_samples > 0);
}

#[test]
fn clipping_template_is_measured() {
    let mut template = clean_sine_template(64, 0.2);
    template[5] = 1.8; // deliberately above the [-1.0, 1.0] nominal range
    template[6] = -1.5;
    let config = FixtureConfig::single_speaker(&['a']);
    let fixture = support::write_fixture("clipping", &template, &config);
    let voice_ref = tts::VoiceModelRef {
        model_artifact_id: id(&fixture.model_id),
        config_artifact_id: id(&fixture.config_id),
        model_digest: fixture.model_digest(),
        config_digest: fixture.config_digest(),
        signature_ref: id("signature-1"),
    };
    let paths = resolve_voice_paths(&fixture.dir, &voice_ref).unwrap();
    verify_voice_digests(&paths, &voice_ref).unwrap();
    let voice = LoadedVoice::load(&paths).unwrap();

    let stream = synthesize_streaming(
        voice,
        SynthesizeRequest {
            request_id: id("request-1"),
            job_id: id("job-1"),
            input_mode: InputMode::Phonemes,
            input_text: "a".to_string(),
            espeak_voice: "en-us".to_string(),
            speaker: SpeakerSelection::Default,
            controls: SynthesisControls {
                length_scale: 1.0,
                noise_scale: 0.667,
                noise_w: 0.8,
            },
            output_format: OutputFormat {
                encoding: OutputEncoding::Pcm16Le,
                sample_rate: config.sample_rate,
                channels: 1,
            },
            limits: limits(),
        },
        CancellationToken::new(),
    )
    .unwrap();
    let chunks: Vec<_> = stream.map(|c| c.unwrap().chunk).collect();
    let total_clipped: u32 = chunks
        .iter()
        .map(|c| match c.quality {
            tts::ChunkQuality::Measured {
                clipped_samples, ..
            } => clipped_samples,
            tts::ChunkQuality::Unavailable => 0,
        })
        .sum();
    // Phoneme 'a' -> piper's BOS/(char,pad)/EOS wrapping gives input_len = 4, so
    // this fixture's Tile graph repeats the 64-sample template (with its 2
    // deliberately out-of-range samples) 4 times: 2 * 4 = 8 clipped samples.
    assert_eq!(
        total_clipped, 8,
        "the 2 deliberately out-of-range template samples, repeated 4x"
    );
}

#[test]
fn segment_phrases_splits_on_sentence_boundaries_and_is_total() {
    assert_eq!(
        segment_phrases("Hello there. How are you? Fine!"),
        vec!["Hello there.", "How are you?", "Fine!"]
    );
    assert_eq!(
        segment_phrases("no terminal punctuation"),
        vec!["no terminal punctuation"]
    );
    assert!(segment_phrases("").is_empty());
    assert!(segment_phrases("   ").is_empty());
}
