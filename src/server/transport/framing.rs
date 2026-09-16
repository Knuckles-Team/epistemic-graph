//! Framed ingress and the single response-writer loop for one native connection.

use crate::protocol::{Request, Response};

const DEFAULT_MAX_REQUEST_FRAME_BYTES: usize = 64 * 1024 * 1024;
pub(super) const HARD_MAX_REQUEST_FRAME_BYTES: usize = 384 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_FRAME_BYTES: usize = 64 * 1024 * 1024;
const HARD_MAX_RESPONSE_FRAME_BYTES: usize = 384 * 1024 * 1024;
const DEFAULT_MAX_MSGPACK_ITEMS: usize = 1_000_000;
const HARD_MAX_MSGPACK_ITEMS: usize = 4_000_000;
pub(super) const MAX_MSGPACK_NESTING_DEPTH: usize = 64;
const DEFAULT_CONNECTION_IO_TIMEOUT_SECS: u64 = 120;
/// Hard ceiling on ONE dispatch, after which its admission permits are released and
/// the client is answered with an error (see the parent dispatch deadline). Sized ~20x
/// the widest dispatch-latency bucket the server records (30 s), so it can only ever
/// fire on work that is genuinely stuck, never on a slow-but-live request.
const DEFAULT_DISPATCH_DEADLINE_SECS: u64 = 600;

/// Serialize a response to a length-prefixable frame. On the (essentially
/// impossible) event that encoding fails, emit a VALID error frame rather than an
/// empty one — a 0-length frame would be read by the client as a zero-byte
/// response and desync the stream. Replaces a previous `unwrap_or_default()` that
/// silently produced exactly that empty frame.
fn encode_response(resp: &Response) -> Vec<u8> {
    match rmp_serde::to_vec_named(resp) {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!("response encode failed (id={}): {}", resp.id, e);
            rmp_serde::to_vec_named(&Response::err(
                resp.id,
                "internal: response serialization failed",
            ))
            .unwrap_or_default()
        }
    }
}

/// Serialize a [`Response`] to a complete, length-prefixed wire frame
/// (`4-byte big-endian len ++ MessagePack body`). The id-tagged response is what
/// the client demuxes by, so a frame can be written in ANY order relative to the
/// requests that produced it (CONCEPT:EG-KG.backend.framed-response).
pub(super) fn encode_frame(resp: &Response) -> Vec<u8> {
    let body = encode_response(resp);
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

pub(super) fn encode_bounded_frame(resp: &Response, max_frame_bytes: usize) -> Vec<u8> {
    let frame = encode_frame(resp);
    if frame.len().saturating_sub(4) <= max_frame_bytes {
        return frame;
    }
    encode_frame(&Response::err(
        resp.id,
        "response frame exceeds the configured resource limit",
    ))
}

/// Bound the allocation driven by an untrusted frame prefix. The hard ceiling is
/// large enough for the modality service's separately capped source + bundle
/// maximum, while the lower default protects ordinary deployments. Operators that
/// raise a modality limit must explicitly raise this transport limit too.
pub(super) fn max_request_frame_bytes() -> usize {
    std::env::var("EPISTEMIC_GRAPH_MAX_REQUEST_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_REQUEST_FRAME_BYTES)
        .min(HARD_MAX_REQUEST_FRAME_BYTES)
}

fn max_response_frame_bytes() -> usize {
    std::env::var("EPISTEMIC_GRAPH_MAX_RESPONSE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_RESPONSE_FRAME_BYTES)
        .min(HARD_MAX_RESPONSE_FRAME_BYTES)
}

/// Bound the number of values/collection slots a MessagePack request may ask the
/// decoder to allocate. A frame-length cap alone is insufficient: a five-byte
/// `array32` header can declare billions of entries and some serde visitors use
/// that untrusted size hint for preallocation before noticing the body is absent.
fn max_msgpack_items() -> usize {
    std::env::var("EPISTEMIC_GRAPH_MAX_MSGPACK_ITEMS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_MSGPACK_ITEMS)
        .min(HARD_MAX_MSGPACK_ITEMS)
}

pub(super) fn validate_msgpack_frame(input: &[u8], max_items: usize) -> Result<(), ()> {
    eg_types::msgpack::validate_single_value(
        input,
        eg_types::msgpack::MsgpackLimits::new(input.len(), max_items, MAX_MSGPACK_NESTING_DEPTH),
    )
    .map_err(|_| ())
}

/// Minimal correlation-id-only view of a request frame (U-98). A frame that
/// fails to decode as a full [`Request`] — e.g. a closed wire enum like
/// `GraphType` rejecting an unsupported string — still carries a well-formed
/// `id` field in the vast majority of cases (only the field the decoder
/// choked on is malformed). Recovering just that field lets the error
/// response route back to the ACTUAL caller waiting on it instead of being
/// silently dropped under a synthetic id `0`, which otherwise starves the
/// caller for its full timeout/retry budget (see `_pending`/`_read_loop` in
/// `epistemic_graph/client.py`, which drops any response whose id has no
/// matching in-flight future).
#[derive(Debug, serde::Deserialize)]
struct MinimalRequestId {
    id: u64,
}

/// Best-effort recovery of a malformed request's correlation id. Falls back to
/// `0` only when even this minimal envelope cannot be parsed — never dispatches
/// the invalid request, only borrows its `id` for the error reply.
pub(super) fn recover_request_id(payload: &[u8]) -> u64 {
    rmp_serde::from_slice::<MinimalRequestId>(payload)
        .map(|m| m.id)
        .unwrap_or(0)
}

/// Run the same allocation-free structural preflight over MessagePack embedded
/// inside a request's binary field. The outer frame scanner deliberately treats
/// `bin` as opaque bytes, so handlers must call this before nested deserialization.
pub(crate) fn validate_nested_msgpack(
    input: &[u8],
    max_bytes: usize,
    max_items: usize,
) -> Result<(), &'static str> {
    eg_types::msgpack::validate_single_value(
        input,
        eg_types::msgpack::MsgpackLimits::new(
            max_bytes,
            max_items.min(HARD_MAX_MSGPACK_ITEMS),
            MAX_MSGPACK_NESTING_DEPTH,
        ),
    )
    .map_err(|_| "invalid or over-complex nested MessagePack payload")
}

/// One environment-configured whole-second timeout: a positive `u64`, clamped into the
/// range that timeout accepts, else the compiled-in default.
///
/// The transport's three timeouts differ only in variable, default and accepted range,
/// so none of them can drift into accepting a zero, a non-numeric value, or an
/// unbounded one the others reject.
pub(super) fn seconds_from_env(
    variable: &str,
    default_seconds: u64,
    accepted: std::ops::RangeInclusive<u64>,
) -> std::time::Duration {
    let seconds = std::env::var(variable)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default_seconds)
        .clamp(*accepted.start(), *accepted.end());
    std::time::Duration::from_secs(seconds)
}

fn connection_io_timeout() -> std::time::Duration {
    seconds_from_env(
        "EPISTEMIC_GRAPH_CONNECTION_IO_TIMEOUT_SECS",
        DEFAULT_CONNECTION_IO_TIMEOUT_SECS,
        1..=3_600,
    )
}

/// CONCEPT:EG-KG.coordination.backpressure-busy-signal — the hard per-dispatch deadline.
///
/// Every admission permit the server issues (the QoS permit, the global pool permit, the
/// per-graph permit, the reserved-read permit, and the per-connection permit) is held by
/// the dispatch task and released only when that task returns. That makes an unbounded
/// dispatch an unbounded RESERVATION: a dispatch that never completes retires none of
/// them, ever. Bounding the dispatch is therefore what makes "a permanently-held
/// admission slot" unrepresentable, at the one place every served request passes through.
///
/// Default 600 s; override with `EPISTEMIC_GRAPH_DISPATCH_DEADLINE_SECS` (clamped to
/// `[1, 86_400]`), following the same idiom as the two timeouts above. It is a ceiling,
/// not a target — the cooperative SQL deadline
/// (`EPISTEMIC_GRAPH_SQL_REQUEST_TIMEOUT_MS`, `server::request_cancel`) is the tunable
/// per-query bound and remains opt-in; this one exists so a NON-cooperative stall (a
/// wedged durable-writer thread, a lost oneshot, a dropped completion) can still not
/// strand the reservation.
pub(super) fn dispatch_deadline() -> std::time::Duration {
    seconds_from_env(
        "EPISTEMIC_GRAPH_DISPATCH_DEADLINE_SECS",
        DEFAULT_DISPATCH_DEADLINE_SECS,
        1..=86_400,
    )
}

#[derive(Clone, Copy)]
pub(super) struct ConnectionLimits {
    pub(super) request_bytes: usize,
    pub(super) response_bytes: usize,
    pub(super) msgpack_items: usize,
    pub(super) io_timeout: std::time::Duration,
    pub(super) dispatch_deadline: std::time::Duration,
}

impl ConnectionLimits {
    pub(super) fn configured() -> Self {
        Self {
            request_bytes: max_request_frame_bytes(),
            response_bytes: max_response_frame_bytes(),
            msgpack_items: max_msgpack_items(),
            io_timeout: connection_io_timeout(),
            dispatch_deadline: dispatch_deadline(),
        }
    }
}

pub(super) enum FrameReadError {
    Rejected(Response),
    Closed(Option<Response>),
}

/// Prefix and body reads each retain their own I/O deadline. A bad length closes
/// the connection without allocating or draining its untrusted body; a bounded
/// malformed body is recoverable and preserves the caller's correlation id.
pub(super) async fn read_request_frame<R>(
    reader: &mut R,
    limits: ConnectionLimits,
) -> Result<Request, FrameReadError>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;

    let mut prefix = [0u8; 4];
    if !matches!(
        tokio::time::timeout(limits.io_timeout, reader.read_exact(&mut prefix)).await,
        Ok(Ok(_))
    ) {
        return Err(FrameReadError::Closed(None));
    }
    let len = u32::from_be_bytes(prefix) as usize;
    if len == 0 || len > limits.request_bytes {
        return Err(FrameReadError::Closed(Some(Response::err(
            0,
            "request frame exceeds the configured resource limit",
        ))));
    }
    let mut payload = vec![0u8; len];
    if !matches!(
        tokio::time::timeout(limits.io_timeout, reader.read_exact(&mut payload)).await,
        Ok(Ok(_))
    ) {
        return Err(FrameReadError::Closed(None));
    }
    if validate_msgpack_frame(&payload, limits.msgpack_items).is_err() {
        return Err(FrameReadError::Rejected(Response::err(
            recover_request_id(&payload),
            "INVALID_ARGUMENT: invalid or over-complex request encoding",
        )));
    }
    match rmp_serde::from_slice(&payload) {
        Ok(request) => Ok(request),
        Err(_) => Err(FrameReadError::Rejected(Response::err(
            recover_request_id(&payload),
            "INVALID_ARGUMENT: invalid request encoding",
        ))),
    }
}

/// Recoverable frame errors are answered before the next frame is read. A closed
/// writer stops ingress; an invalid prefix gets its final error before closing.
pub(super) async fn next_request<R>(
    reader: &mut R,
    tx: &tokio::sync::mpsc::Sender<Vec<u8>>,
    limits: ConnectionLimits,
) -> Option<Request>
where
    R: tokio::io::AsyncRead + Unpin,
{
    loop {
        match read_request_frame(reader, limits).await {
            Ok(request) => return Some(request),
            Err(FrameReadError::Rejected(response)) => {
                if tx.send(encode_frame(&response)).await.is_err() {
                    return None;
                }
            }
            Err(FrameReadError::Closed(response)) => {
                if let Some(response) = response {
                    let _ = tx.send(encode_frame(&response)).await;
                }
                return None;
            }
        }
    }
}

pub(super) async fn write_responses<W>(
    mut writer: W,
    mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    io_timeout: std::time::Duration,
) where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    while let Some(frame) = rx.recv().await {
        if !matches!(
            tokio::time::timeout(io_timeout, writer.write_all(&frame)).await,
            Ok(Ok(()))
        ) {
            break;
        }
    }
    let _ = tokio::time::timeout(io_timeout, writer.flush()).await;
}
