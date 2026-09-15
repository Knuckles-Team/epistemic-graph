# Native Piper provider status

`crates/eg-tts-piper` remains an internal, opt-in Piper/ONNX provider library.
The root `tts-piper` feature links that provider and `eg-audio/tts`, but it does
not add a public wire method, capability-policy row, dispatch arm, or server
handler.

The former `TtsSynthesize { request_msgpack, input_bytes }` surface was an
interim carrier that accepted sensitive input bytes and returned inline audio.
It is deleted rather than retained as a compatibility surface: this repository
has no released legacy contract to preserve, and the interim shape did not meet
the current content-addressed serving contract.

## Acceptance boundary

A public synthesis surface may be introduced only with an accepted current
design that:

- resolves sensitive input through an authorized content-addressed reference;
- publishes output through the canonical CAS/rendition and analytics-job path;
- binds tenant, principal, consent, model artifact, and idempotency authority;
- returns a typed receipt/reference rather than inline opaque output bytes;
- provides bounded cancellation, replay, audit, and resource-accounting proofs.

Until that contract exists, provider APIs are available only to internal code
that explicitly links the feature; the served protocol fails closed because no
TTS method exists.

## Provider scope

The leaf provider remains intentionally narrow: Piper-specific ONNX/JSON model
execution, no model acquisition, no generic Transformers/safetensors claim, and
no speaker diarization, biometric identity, or verification. Callers must supply
already-resolved model/config artifacts, and the provider independently verifies
their declared SHA-256 digests before loading them.

## Running the synthesis tests

`crates/eg-tts-piper/tests/piper_synthesis.rs` drives real ONNX inference. Its
fixture voice is a tiny ONNX graph generated in Rust at test time (no committed
binary, no trained weights), so the only external prerequisite is the runtime.

Under `--all-features` the crate is built with `ort-load-dynamic`, and `ort`
loads the runtime from `ORT_DYLIB_PATH`:

```bash
export ORT_DYLIB_PATH="$(scripts/fetch_onnxruntime.sh)"
cargo test -p eg-tts-piper --all-features
```

A missing or unreadable runtime FAILS every inference test. It never skips them.
The script downloads the official ONNX Runtime 1.24.2 release, the version
`ort-sys` 2.0.0-rc.12 pins (api-24), and checks its sha256.

Measured on 2026-09-13 on a build host without AVX2: all synthesis tests pass
with this library. Debian's `libonnxruntime1.23` package does not work, because
its API version is older than the pin.
