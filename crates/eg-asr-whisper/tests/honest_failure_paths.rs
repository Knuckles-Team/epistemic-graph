//! Honest-failure-path proofs that need no whisper.cpp model at all: a
//! provider that cannot transcribe must return a distinct typed failure, not
//! an empty transcript that reads as success. Model-dependent proofs (real
//! transcription, cancellation mid-stream) live in
//! `tests/real_transcription.rs`, gated on an operator-supplied model per the
//! GOC-33 lane doc ("model acquisition is not this crate's job").

use eg_asr_whisper::{decode_wav_16k_mono, verify_model};
use eg_audio::asr::AsrError;

#[test]
fn model_absent_fails_closed() {
    let err = verify_model("/definitely/not/a/real/path.bin", &"a".repeat(64))
        .expect_err("absent model must fail closed, never silently proceed");
    assert_eq!(
        err,
        AsrError::ModelUnavailable {
            reason: "model file is absent or unreadable"
        }
    );
}

#[test]
fn unverified_model_digest_mismatch_fails_closed() {
    let dir = std::env::temp_dir();
    let path = dir.join(format!(
        "eg-asr-whisper-honest-test-{}.bin",
        std::process::id()
    ));
    std::fs::write(&path, b"not actually a ggml model").expect("write scratch file");
    let wrong_digest = "1".repeat(64);
    let err = verify_model(&path, &wrong_digest).expect_err("digest mismatch must fail closed");
    assert_eq!(
        err,
        AsrError::ModelUnavailable {
            reason: "model file content digest does not match the declared digest"
        }
    );
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unsupported_sample_rate_is_a_distinct_typed_failure() {
    // 8kHz mono 16-bit — a real, well-formed WAV, just the wrong rate. Must be
    // rejected before any provider/model code ever sees it — never resampled
    // silently.
    let bytes = wav_fixture(8_000, 1, 16, &[0, 1, -1, 100]);
    let err = decode_wav_16k_mono(&bytes).expect_err("wrong sample rate must be rejected");
    assert_eq!(
        err,
        AsrError::MalformedRequest {
            reason: "unsupported sample rate: only 16000 Hz is accepted"
        }
    );
}

#[test]
fn unsupported_format_stereo_is_a_distinct_typed_failure() {
    let bytes = wav_fixture(16_000, 2, 16, &[0, 0, 1, 1]);
    let err = decode_wav_16k_mono(&bytes).expect_err("stereo must be rejected, never mixed down");
    assert_eq!(
        err,
        AsrError::MalformedRequest {
            reason: "unsupported channel layout: only mono is accepted"
        }
    );
}

#[test]
fn truncated_audio_is_a_distinct_typed_failure() {
    // A header declaring more PCM bytes than are actually present.
    let mut bytes = wav_fixture(16_000, 1, 16, &[0, 0, 0, 0]);
    let riff_size_pos = 4;
    let claimed_extra = 8u32;
    let current = u32::from_le_bytes(bytes[riff_size_pos..riff_size_pos + 4].try_into().unwrap());
    bytes[riff_size_pos..riff_size_pos + 4]
        .copy_from_slice(&(current + claimed_extra).to_le_bytes());
    let data_len_pos = bytes.len() - 8;
    let old_len = u32::from_le_bytes(bytes[data_len_pos..data_len_pos + 4].try_into().unwrap());
    bytes[data_len_pos..data_len_pos + 4].copy_from_slice(&(old_len + claimed_extra).to_le_bytes());
    let err = decode_wav_16k_mono(&bytes).expect_err("truncated audio must be rejected");
    assert_eq!(
        err,
        AsrError::MalformedRequest {
            reason: "not a well-formed RIFF/WAVE PCM header"
        }
    );
}

#[test]
fn empty_audio_is_a_distinct_typed_failure() {
    let bytes = wav_fixture(16_000, 1, 16, &[]);
    let err = decode_wav_16k_mono(&bytes).expect_err("zero-length audio must be rejected");
    assert_eq!(
        err,
        AsrError::MalformedRequest {
            reason: "zero-length audio data"
        }
    );
}

fn wav_fixture(sample_rate: u32, channels: u16, bits: u16, samples: &[i16]) -> Vec<u8> {
    let bytes_per_sample = (bits / 8) as u32;
    let block_align = bytes_per_sample * channels as u32;
    let data_size = samples.len() as u32 * bytes_per_sample;
    let byte_rate = sample_rate * block_align;
    let mut b = Vec::new();
    b.extend_from_slice(b"RIFF");
    b.extend_from_slice(&(36u32 + data_size).to_le_bytes());
    b.extend_from_slice(b"WAVE");
    b.extend_from_slice(b"fmt ");
    b.extend_from_slice(&16u32.to_le_bytes());
    b.extend_from_slice(&1u16.to_le_bytes());
    b.extend_from_slice(&channels.to_le_bytes());
    b.extend_from_slice(&sample_rate.to_le_bytes());
    b.extend_from_slice(&byte_rate.to_le_bytes());
    b.extend_from_slice(&(block_align as u16).to_le_bytes());
    b.extend_from_slice(&bits.to_le_bytes());
    b.extend_from_slice(b"data");
    b.extend_from_slice(&data_size.to_le_bytes());
    for s in samples {
        b.extend_from_slice(&s.to_le_bytes());
    }
    b
}
