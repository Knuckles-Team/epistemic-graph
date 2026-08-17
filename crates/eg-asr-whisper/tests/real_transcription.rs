//! Real-model proofs. GOC-33 explicitly does NOT own model acquisition
//! (GOC-36) and this crate never downloads a model — so these tests are
//! gated on an operator-supplied fixture and SKIP (loudly, not silently)
//! when it is absent, rather than being disabled or deleted. To run:
//!
//! ```text
//! EG_ASR_TEST_MODEL_PATH=/var/tmp/l9/ggml-tiny.en.bin \
//! EG_ASR_TEST_WAV_PATH=/var/tmp/l9/jfk-16k-mono.wav \
//! cargo test -p eg-asr-whisper --test real_transcription --target-dir ./target-isolated -- --nocapture
//! ```

use eg_asr_whisper::{
    decode_wav_16k_mono, verify_model, CancelFlag, TranscribeOptions, WhisperAsrProvider,
};
use eg_audio::asr::AsrError;
use sha2::{Digest, Sha256};

struct Fixture {
    model_path: String,
    wav_path: String,
}

fn fixture() -> Option<Fixture> {
    let model_path = std::env::var("EG_ASR_TEST_MODEL_PATH").ok()?;
    let wav_path = std::env::var("EG_ASR_TEST_WAV_PATH").ok()?;
    Some(Fixture {
        model_path,
        wav_path,
    })
}

fn load_provider(fixture: &Fixture) -> WhisperAsrProvider {
    let model_bytes = std::fs::read(&fixture.model_path).expect("read test model");
    let digest = format!("{:x}", Sha256::digest(&model_bytes));
    let verified =
        verify_model(&fixture.model_path, &digest).expect("model verifies against its own digest");
    WhisperAsrProvider::load(&verified, "eg-asr-whisper-test-model", false)
        .expect("model loads into whisper.cpp")
}

#[test]
fn transcribes_real_audio_and_produces_provider_derived_timing_and_quality() {
    let Some(fixture) = fixture() else {
        eprintln!("SKIPPED: EG_ASR_TEST_MODEL_PATH/EG_ASR_TEST_WAV_PATH not set — no model fixture configured (GOC-36 owns acquisition, this crate never downloads one).");
        return;
    };
    let provider = load_provider(&fixture);
    let wav_bytes = std::fs::read(&fixture.wav_path).expect("read test wav");
    let audio = decode_wav_16k_mono(&wav_bytes).expect("fixture wav decodes as 16k mono pcm16");

    let opts = TranscribeOptions {
        language: Some("en".to_string()),
        translate: false,
        word_timing: true,
        window_ms: 30_000,
    };
    let cancel = CancelFlag::new();
    let mut partial_count = 0usize;
    let outcome = provider
        .transcribe_streaming(&audio, &opts, &cancel, |_partial| {
            partial_count += 1;
        })
        .expect("real audio with real speech transcribes successfully");

    assert!(
        !outcome.segments.is_empty(),
        "a real speech fixture must produce at least one segment"
    );
    assert!(partial_count > 0, "on_partial must have been called");
    let joined: String = outcome
        .segments
        .iter()
        .map(|s| s.text.as_str())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        !joined.trim().is_empty(),
        "transcript text must be non-empty for real speech, never an empty-success"
    );
    for s in &outcome.segments {
        assert!(
            s.segment.validate().is_ok(),
            "every emitted segment must pass the frozen contract's own validator"
        );
    }
    eprintln!("transcript: {joined:?}");
    eprintln!("language: {}", outcome.language);
    eprintln!("timing_available: {}", outcome.timing_available);
}

#[test]
fn cancellation_mid_stream_yields_a_typed_cancelled_error_never_a_truncated_success() {
    let Some(fixture) = fixture() else {
        eprintln!("SKIPPED: EG_ASR_TEST_MODEL_PATH/EG_ASR_TEST_WAV_PATH not set.");
        return;
    };
    let provider = load_provider(&fixture);
    let wav_bytes = std::fs::read(&fixture.wav_path).expect("read test wav");
    let audio = decode_wav_16k_mono(&wav_bytes).expect("fixture wav decodes");

    // A short window forces multiple windows over any several-second fixture,
    // so cancelling from inside the FIRST window's partial callback
    // deterministically leaves later windows unprocessed — no sleep/race.
    let opts = TranscribeOptions {
        language: Some("en".to_string()),
        translate: false,
        word_timing: false,
        window_ms: 2_000,
    };
    let cancel = CancelFlag::new();
    let cancel_inner = cancel.clone();
    let result = provider.transcribe_streaming(&audio, &opts, &cancel, |_partial| {
        cancel_inner.cancel();
    });

    assert_eq!(
        result.err(),
        Some(AsrError::Cancelled),
        "a request cancelled mid-stream must surface as a distinct typed Cancelled error, \
         never as a silently truncated 'success'"
    );
}

#[test]
fn unverified_model_is_never_loaded() {
    let Some(fixture) = fixture() else {
        eprintln!("SKIPPED: no model fixture configured.");
        return;
    };
    let wrong_digest = "0".repeat(64);
    let err = verify_model(&fixture.model_path, &wrong_digest)
        .expect_err("a real model file with the WRONG declared digest must still fail closed");
    assert_eq!(
        err,
        AsrError::ModelUnavailable {
            reason: "model file content digest does not match the declared digest"
        }
    );
}
