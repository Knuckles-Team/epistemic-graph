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
