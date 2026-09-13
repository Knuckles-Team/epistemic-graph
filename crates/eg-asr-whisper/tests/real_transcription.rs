//! Real-model proofs. GOC-33 explicitly does NOT own model acquisition
//! (GOC-36) and this crate never downloads a model, so these tests read an
//! operator-supplied fixture from the environment. They never skip: when the
//! fixture is not configured they FAIL with instructions. To run:
//!
//! ```text
//! export $(scripts/fetch_whisper_test_fixture.sh)   # pinned, sha256-verified
//! cargo test -p eg-asr-whisper --test real_transcription -- --nocapture
//! ```

use eg_asr_whisper::{
    decode_wav_16k_mono, verify_model, CancelFlag, TranscribeOptions, WhisperAsrProvider,
};
use eg_audio::asr::AsrError;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

struct Fixture {
    model_path: String,
    wav_path: String,
}

/// The operator-supplied model + speech fixture. Panics with how to provide it when
/// either variable is unset.
fn fixture() -> Fixture {
    let var = |name: &str| {
        std::env::var(name).unwrap_or_else(|_| {
            panic!(
                "{name} is not set: the real-model tests need a whisper model and a 16 kHz \
                 mono WAV. Run `export $(scripts/fetch_whisper_test_fixture.sh)` from the \
                 repository root (pinned, sha256-verified download)."
            )
        })
    };
    Fixture {
        model_path: var("EG_ASR_TEST_MODEL_PATH"),
        wav_path: var("EG_ASR_TEST_WAV_PATH"),
    }
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
    let fixture = fixture();
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
fn native_poll_cancellation_yields_a_typed_error_never_a_truncated_success() {
    let fixture = fixture();
    let provider = load_provider(&fixture);
    let wav_bytes = std::fs::read(&fixture.wav_path).expect("read test wav");
    let audio = decode_wav_16k_mono(&wav_bytes).expect("fixture wav decodes");

    // Keep the fixture in one window. The test hook runs from whisper.cpp's
    // real abort callback, so cancellation occurs INSIDE `full()`, without a
    // sleeping helper thread or a scheduler/load-dependent race. If the
    // `abort_bridge::install` call is removed, the hook is never polled and
    // both the typed-result and poll-count assertions below fail: that is the
    // perturbation proof that this test covers the native bridge rather than
    // only the between-window check.
    let opts = TranscribeOptions {
        language: Some("en".to_string()),
        translate: false,
        word_timing: false,
        window_ms: 30_000,
    };
    let cancel = CancelFlag::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let polled = Arc::clone(&polls);
    let cancel_inner = cancel.clone();
    cancel.set_poll_hook(move || {
        polled.fetch_add(1, Ordering::Relaxed);
        cancel_inner.cancel();
    });
    let result = provider.transcribe_streaming(&audio, &opts, &cancel, |_partial| {});

    assert_eq!(
        result.err(),
        Some(AsrError::Cancelled),
        "a request cancelled mid-stream must surface as a distinct typed Cancelled error, \
         never as a silently truncated 'success'"
    );
    assert!(
        polls.load(Ordering::Relaxed) > 0,
        "a real native abort poll must trigger the cancellation bound"
    );
}

#[test]
fn unverified_model_is_never_loaded() {
    let fixture = fixture();
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
