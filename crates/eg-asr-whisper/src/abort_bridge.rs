//! Correctly-typed native abort-callback wiring for whisper.cpp's `full()`,
//! replacing a type-confusion bug in `whisper-rs` 0.16.0's safe wrapper.
//!
//! ## Root cause (confirmed against `whisper-rs` 0.16.0's own source —
//! `~/.cargo/registry/src/*/whisper-rs-0.16.0/src/whisper_params.rs`, and
//! still present verbatim on the `codeberg.org/tazz4843/whisper-rs` `master`
//! branch as of 2026-09-13, so there is no newer release to upgrade to —
//! 0.16.0 is both the latest crates.io release and current upstream HEAD):
//!
//! `FullParams::set_abort_callback_safe` boxes the caller's closure TWICE —
//! `Box::new(closure) as Box<dyn FnMut() -> bool>` (a *fat* pointer: 16
//! bytes, data pointer + vtable pointer), then `Box::new(...)` again (a
//! *thin* pointer to that fat pointer) — and stores `Box::into_raw` of the
//! OUTER box as `abort_callback_user_data`. Its `trampoline<F>` is generic
//! over `F`, the caller's ORIGINAL (un-boxed, un-erased) closure type, and
//! reads the data back as:
//! ```text
//! let user_data = &mut *(user_data as *mut F);
//! user_data()
//! ```
//! This reinterprets the memory at `user_data` — which actually holds a
//! `Box<dyn FnMut() -> bool>` (two machine words: data ptr + vtable ptr) —
//! as if it directly held a value of the caller's concrete closure type `F`.
//! Calling through that reinterpreted reference is undefined behavior. The
//! observed effect in this crate: the abort callback reads garbage and
//! whisper.cpp aborts every encode with `WhisperError::FailedToEncode`,
//! surfaced here as `AsrError::Degraded` for every single call — real
//! cancellation or not.
//!
//! ## Fix: this crate's own correctly-typed trampoline over the raw API
//!
//! `FullParams::set_abort_callback` / `set_abort_callback_user_data` (both
//! `unsafe`, both sound when used correctly) accept a plain
//! `unsafe extern "C" fn(*mut c_void) -> bool` and an opaque `*mut c_void`
//! that whisper.cpp promises to pass back unchanged, with no generic
//! reinterpretation on either side. [`install`] wires a `trampoline` whose
//! signature exactly matches `whisper_rs::WhisperAbortCallback`'s C ABI, and
//! casts `user_data` back to EXACTLY the pointer type it was derived from
//! (`*const CancelState`, via `Arc::as_ptr`) — never to the wrong type, and
//! never through a double-indirection the trampoline doesn't know about.
//! See [`install`]'s own `// SAFETY:` note for the full lifetime/aliasing
//! argument this depends on.
//!
//! Per this repo's crates.io-only Rust dependency edict (see this crate's
//! `lib.rs` module doc, "Binding choice"), this works around the bug IN this
//! crate rather than vendor-patching `whisper-rs` itself via `[patch]`: no
//! `[patch]` entry, no git dependency, no fork.

use std::ffi::c_void;
use std::sync::Arc;

use crate::CancelState;

/// The `extern "C"` callback whisper.cpp polls during `WhisperState::full`
/// between encoder graph executions and at checkpoints in the per-token
/// decode loop. It cannot preempt a graph step already executing. Returning
/// `true` aborts; whisper.cpp then returns `WhisperError::FailedToEncode` /
/// `FailedToDecode` from `full()`, indistinguishable at that point from a
/// genuine provider failure, so `transcribe_streaming` must check its own
/// cancellation flag itself to tell the two apart (see its handling of
/// `state.full(...)`'s `Err` case).
///
/// # Safety
/// This function is called by whisper.cpp (native C code), not by any Rust
/// caller directly, so its safety depends on the CONTRACT [`install`]
/// establishes rather than on anything a Rust call site can enforce:
/// `user_data` must be exactly the `*const CancelState` pointer [`install`]
/// derived via `Arc::as_ptr`, and the `Arc<CancelState>` it was derived from
/// must still be alive (an un-dropped clone held by `install`'s caller) for
/// as long as whisper.cpp might still invoke this trampoline — i.e. for the
/// entire duration of the one `full()` call `install` wired it into.
/// `transcribe_streaming` upholds this by keeping `cancel` (the
/// `&CancelFlag` parameter, itself never dropped mid-call) alive across the
/// `state.full(params, window)` call it installs into, and by never reusing
/// a `FullParams` — hence never this callback registration — across more
/// than one `full()` call.
unsafe extern "C" fn trampoline(user_data: *mut c_void) -> bool {
    // SAFETY: per the function-level contract above, `user_data` is exactly
    // the `*const CancelState` produced by `Arc::as_ptr` in `install`, cast
    // to `*mut c_void` and back to that SAME concrete type with no
    // reinterpretation to any other type or layout (the bug this module
    // exists to avoid — see the module doc), and the `Arc` it points into is
    // kept alive by `install`'s caller for the full duration of the one
    // `full()` call whisper.cpp invokes this trampoline during. The
    // resulting `&CancelState` is a shared borrow that only performs atomic
    // operations in `CancelState::poll`. It invokes no caller-controlled
    // code and never mutates through this raw pointer directly, so it does
    // not alias against the live `Arc`'s shared access. The unwind barrier is
    // defensive: even if `poll` changes later, a Rust panic cannot cross the
    // native callback boundary and abort the process. Its fail-closed value
    // asks whisper.cpp to stop.
    let state = unsafe { &*(user_data.cast::<CancelState>()) };
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| state.poll())).unwrap_or(true)
}

/// Wires `flag`'s shared state into `params` as whisper.cpp's abort callback
/// for exactly the ONE `full()` call `params` is about to be consumed by.
///
/// # Safety
/// The caller (`transcribe_streaming`) must keep `flag` (or a clone sharing
/// the same `Arc<CancelState>`) alive on the stack until AFTER that `full()`
/// call returns. This holds today because `transcribe_streaming` builds a
/// fresh `FullParams` for every window from a `cancel: &CancelFlag`
/// parameter that outlives the whole function, and never persists
/// `abort_callback`/`abort_callback_user_data` beyond the single `full()`
/// call each `FullParams` is consumed by (whisper.cpp itself does not
/// retain them past that one call either).
pub(crate) fn install(params: &mut whisper_rs::FullParams, flag: &crate::CancelFlag) {
    let ptr = Arc::as_ptr(&flag.0) as *mut c_void;
    // SAFETY: `trampoline` has the exact signature
    // `unsafe extern "C" fn(*mut c_void) -> bool`, matching
    // `whisper_rs::WhisperAbortCallback`'s C ABI precisely, so
    // `set_abort_callback` receives a correctly-typed function pointer —
    // never a Rust closure smuggled through the wrong type, which is the
    // whisper-rs 0.16.0 bug this module works around (see the module doc).
    // `ptr` is `Arc::as_ptr(&flag.0)`: a valid, non-dangling
    // `*const CancelState` for as long as any clone of that `Arc` is alive,
    // which this function's own safety contract requires the caller to
    // uphold across the `full()` call this callback is wired for.
    unsafe {
        params.set_abort_callback(Some(trampoline));
        params.set_abort_callback_user_data(ptr);
    }
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use crate::{
        decode_wav_16k_mono, verify_model, CancelFlag, TranscribeOptions, WhisperAsrProvider,
    };
    use eg_audio::asr::AsrError;

    fn required_fixture(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| {
            panic!("{name} is not set: run `export $(scripts/fetch_whisper_test_fixture.sh)`")
        })
    }

    /// A real native checkpoint drives this cancellation. No thread scheduling,
    /// sleep, public hook, mutex, or caller-controlled FFI callback is involved.
    /// Removing `abort_bridge::install` makes transcription complete and this
    /// assertion fail, which is the bridge's perturbation proof.
    #[test]
    fn native_callback_checkpoint_cancels_a_running_window() {
        let model_path = required_fixture("EG_ASR_TEST_MODEL_PATH");
        let model_bytes = std::fs::read(&model_path).expect("read test model");
        let digest = format!("{:x}", Sha256::digest(model_bytes));
        let model = verify_model(&model_path, &digest).expect("verify test model");
        let provider = WhisperAsrProvider::load(&model, "eg-asr-whisper-test-model", false)
            .expect("load test model");
        let wav_path = required_fixture("EG_ASR_TEST_WAV_PATH");
        let wav = std::fs::read(wav_path).expect("read test wav");
        let audio = decode_wav_16k_mono(&wav).expect("decode 16 kHz mono fixture");
        let options = TranscribeOptions {
            language: Some("en".to_string()),
            translate: false,
            word_timing: false,
            window_ms: 30_000,
        };
        let cancel = CancelFlag::new();
        cancel.cancel_on_next_native_poll();

        let result = provider.transcribe_streaming(&audio, &options, &cancel, |_| {});

        assert_eq!(result.err(), Some(AsrError::Cancelled));
        assert!(
            cancel.is_cancelled(),
            "the native checkpoint must trip the flag"
        );
        assert!(
            !cancel.native_poll_is_armed(),
            "the one-window test arm must be cleared"
        );
    }
}
