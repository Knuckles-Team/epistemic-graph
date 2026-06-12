//! Wire transport: response framing, the per-connection loop (with backpressure
//! admission), and the UDS/TCP listeners. Routing/auth live in `dispatch`.

use std::sync::Arc;
use tokio::net::TcpListener;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info};

use super::{dispatch, ServerState};
use crate::protocol::{Method, Request, Response};

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

/// Handle one client connection: length-prefixed MessagePack frames, per-request
/// backpressure admission (global + per-graph), dispatch, and response framing.
pub async fn handle_connection<S>(mut stream: S, state: Arc<RwLock<ServerState>>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Snapshot the shared backpressure handles once per connection.
    let (sem, pg_map, pg_limit) = {
        let s = state.read().await;
        (
            s.max_in_flight.clone(),
            s.per_graph_inflight.clone(),
            s.per_graph_inflight_limit,
        )
    };

    loop {
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            break;
        }
        let len = u32::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        if stream.read_exact(&mut payload).await.is_err() {
            break;
        }

        let req: Request = match rmp_serde::from_slice(&payload) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::err(0, format!("Invalid request MsgPack: {}", e));
                let out = encode_response(&resp);
                let out_len = out.len() as u32;
                let _ = stream.write_all(&out_len.to_be_bytes()).await;
                let _ = stream.write_all(&out).await;
                continue;
            }
        };

        let is_shutdown = matches!(req.method, Method::Shutdown);

        // Global backpressure: acquire an in-flight permit, or shed load with BUSY.
        let _permit = match sem.clone().try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                crate::metrics::busy_rejected();
                let resp = Response::err(req.id, "BUSY: server at capacity, retry with backoff");
                let out = encode_response(&resp);
                let out_len = out.len() as u32;
                if stream.write_all(&out_len.to_be_bytes()).await.is_err() {
                    break;
                }
                if stream.write_all(&out).await.is_err() {
                    break;
                }
                continue;
            }
        };

        // Per-graph fairness (Phase C-D): cap how many of the global slots this one
        // graph may hold. On exhaustion, shed THIS graph's load with BUSY and
        // release the global permit so other tenants keep flowing.
        let pg_sem = pg_map
            .entry(req.graph.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(pg_limit)))
            .clone();
        let _pg_permit = match pg_sem.try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                drop(_permit);
                crate::metrics::busy_rejected();
                let resp = Response::err(req.id, "BUSY: graph at capacity, retry with backoff");
                let out = encode_response(&resp);
                let out_len = out.len() as u32;
                if stream.write_all(&out_len.to_be_bytes()).await.is_err() {
                    break;
                }
                if stream.write_all(&out).await.is_err() {
                    break;
                }
                continue;
            }
        };
        crate::metrics::connection_request_started(sem.available_permits());
        let resp = dispatch(&state, req).await;
        drop(_pg_permit);
        drop(_permit);
        crate::metrics::connection_request_finished(sem.available_permits());

        let out = encode_response(&resp);
        let out_len = out.len() as u32;
        if stream.write_all(&out_len.to_be_bytes()).await.is_err() {
            break;
        }
        if stream.write_all(&out).await.is_err() {
            break;
        }

        if is_shutdown {
            break;
        }
    }
}

/// Start the server on a Unix Domain Socket (unix only; Windows uses TCP).
#[cfg(unix)]
pub async fn serve_uds(socket_path: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    // Remove stale socket file.
    let _ = std::fs::remove_file(socket_path);
    let listener = UnixListener::bind(socket_path)?;
    info!("Listening on UDS: {}", socket_path);

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    handle_connection(stream, state).await;
                });
            }
            Err(e) => {
                error!("Accept error: {}", e);
            }
        }
    }
}

/// Start the server on a TCP address.
pub async fn serve_tcp(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on TCP: {}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                info!("TCP connection from {}", addr);
                let state = state.clone();
                tokio::spawn(async move {
                    handle_connection(stream, state).await;
                });
            }
            Err(e) => {
                error!("Accept error: {}", e);
            }
        }
    }
}
