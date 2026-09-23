//! The `eg2.` canonical method body, for clients that are not Python.
//!
//! The engine MACs `Method::canonical_body_bytes()` of the request it DECODED,
//! never the bytes a client sent. A client therefore signs correctly only if it
//! reproduces that re-serialization exactly: Rust declaration order, serde
//! defaults materialized, `skip_serializing_if` honoured, maps sorted, and
//! float and byte widths per field. The Python wheel gets those bytes from
//! eg-types through a native module (`eg-numeric`, codec `eg/method-body/v1`);
//! this crate exports the same call as a WebAssembly module so the Go and
//! JavaScript clients embed the engine's own codec instead of restating it.
//!
//! The module has no imports and a four-call ABI over its exported `memory`:
//!
//! 1. `eg_input_reserve(len) -> ptr`: size the input buffer; the host writes
//!    one MessagePack request frame (exactly as the transport would carry it)
//!    at `ptr`.
//! 2. `eg_canonical_body() -> status`: decode the frame as the transport does
//!    and re-encode its `Method`. Status [`BODY`] leaves the canonical body in
//!    the output buffer; [`REFUSED`] leaves a UTF-8 reason there instead.
//! 3. `eg_output_ptr()` / `eg_output_len()`: locate the output buffer.
//!
//! [`CODEC`] names the body format; `eg_codec_ptr`/`eg_codec_len` expose it so
//! a host can refuse a module that speaks a different one.

use std::cell::RefCell;

use eg_types::protocol::Method;

/// The canonical-body format this module produces; the Python wheel's native
/// codec reports the same identity as `__method_body_codec__`.
pub const CODEC: &str = "eg/method-body/v1";

/// [`eg_canonical_body`] status: the output buffer holds the canonical body.
pub const BODY: u32 = 0;

/// [`eg_canonical_body`] status: the frame does not decode as an engine
/// request; the output buffer holds the reason.
pub const REFUSED: u32 = 1;

// A wasm32-unknown-unknown module runs on one thread, so the two buffers are
// thread-local cells rather than locks: nothing can contend for them. A call
// that traps aborts without unwinding and may leave a cell borrowed, which is
// why the hosts discard a trapped instance instead of reusing it.
thread_local! {
    static INPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    static OUTPUT: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// The canonical body the engine derives from one request frame.
pub fn canonical_body(frame: &[u8]) -> Result<Vec<u8>, String> {
    Method::canonical_body_of_request_frame(frame)
}

/// Resize the input buffer to `len` zero bytes and return where the host
/// writes the request frame.
#[no_mangle]
pub extern "C" fn eg_input_reserve(len: u32) -> *mut u8 {
    INPUT.with_borrow_mut(|input| {
        input.clear();
        input.resize(len as usize, 0);
        input.as_mut_ptr()
    })
}

/// Derive the canonical body of the frame in the input buffer.
#[no_mangle]
pub extern "C" fn eg_canonical_body() -> u32 {
    let (status, bytes) = match INPUT.with_borrow(|frame| canonical_body(frame)) {
        Ok(body) => (BODY, body),
        Err(reason) => (REFUSED, reason.into_bytes()),
    };
    OUTPUT.set(bytes);
    status
}

/// Start of the output buffer.
#[no_mangle]
pub extern "C" fn eg_output_ptr() -> *const u8 {
    OUTPUT.with_borrow(|output| output.as_ptr())
}

/// Length of the output buffer.
#[no_mangle]
pub extern "C" fn eg_output_len() -> u32 {
    OUTPUT.with_borrow(|output| output.len() as u32)
}

/// Start of the [`CODEC`] identity string.
#[no_mangle]
pub extern "C" fn eg_codec_ptr() -> *const u8 {
    CODEC.as_ptr()
}

/// Length of the [`CODEC`] identity string.
#[no_mangle]
pub extern "C" fn eg_codec_len() -> u32 {
    CODEC.len() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive the exports as a host does. The buffers are thread-local, so each
    /// test thread has its own and the tests need no serialization.
    fn run(frame: &[u8]) -> (u32, Vec<u8>) {
        eg_input_reserve(frame.len() as u32);
        INPUT.with_borrow_mut(|input| input.copy_from_slice(frame));
        let status = eg_canonical_body();
        let output = OUTPUT.with_borrow(Vec::clone);
        assert_eq!(output.len(), eg_output_len() as usize);
        (status, output)
    }

    fn frame(params: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({
            "params": params,
            "method": "CancelRequest",
            "agent_id": null,
            "auth_token": "",
            "graph": "",
            "id": 0,
        }))
        .expect("a JSON frame encodes")
    }

    #[test]
    fn the_export_returns_the_engine_canonical_body() {
        let expected = Method::CancelRequest { target_req_id: 7 }.canonical_body_bytes();
        assert_eq!(
            run(&frame(serde_json::json!({"target_req_id": 7}))),
            (BODY, expected)
        );
    }

    #[test]
    fn an_undecodable_frame_is_refused_with_its_reason() {
        let (status, reason) = run(&frame(serde_json::json!({"target_req_id": "seven"})));
        assert_eq!(status, REFUSED);
        let reason = String::from_utf8(reason).expect("the reason is UTF-8");
        assert!(
            reason.starts_with("request frame does not decode"),
            "{reason}"
        );
    }

    #[test]
    fn the_codec_identity_is_the_python_wheel_identity() {
        assert_eq!(CODEC, "eg/method-body/v1");
        assert_eq!(eg_codec_len() as usize, CODEC.len());
    }
}
