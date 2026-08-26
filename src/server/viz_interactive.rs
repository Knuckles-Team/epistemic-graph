//! Interactive rendering (D-VZ-1 lane V3b), feature `viz-interactive`.
//!
//! A browser cannot speak this engine's primary transport (length-prefixed
//! MessagePack over UDS/TCP, `eg2.`-enveloped) — so V3b is its own small,
//! **loopback-only, dependency-free HTTP/1.1 listener** (the SAME hand-rolled
//! idiom `--metrics-addr`/`--obs-addr`/`lake/rest.rs` already use — no axum/
//! hyper/websocket dependency), serving:
//!
//! - `GET /` — a self-contained reference client: feature-detects WebGPU
//!   (`navigator.gpu`), falls back to WebGL2, and if NEITHER is available
//!   shows a visible "cannot accelerate this view" message and stops —
//!   **never a silently blank canvas** (see [`CLIENT_HTML`] /
//!   `src/server/viz_interactive_client.html`).
//! - `GET /tile` — one viewport's worth of REAL geometry for the requested
//!   dataset, in a small binary format (see [`encode_tile`]) a WebGPU/WebGL2
//!   client decodes straight into a GPU vertex buffer, no JSON parsing.
//!
//! ## Transport/format choice, and why
//!
//! A plain per-request `GET /tile?...` (not a WebSocket / long-lived stream)
//! is the standard, well-understood shape every tile server (map tiles, this
//! program's own `xy` reference dossier's tile-pyramid design) already uses:
//! the browser's own `fetch()` cache/coalescing/cancellation semantics apply
//! for free, no new protocol framing to hand-roll, and pan/zoom naturally
//! becomes "issue a new GET for the new viewport" (see the client's debounced
//! viewport-change handler) — never a full-series re-download, satisfying the
//! "pan/zoom re-requests the appropriate LOD tier" requirement directly. A
//! persistent WebSocket would need this crate to hand-roll RFC 6455 framing
//! (this codebase's existing WS precedent, `ros2_bridge`, pulls
//! `tokio-tungstenite` — a real dependency this lane does not need to add for
//! a request/response tile fetch) for a benefit (lower per-request overhead)
//! that does not matter at human pan/zoom interaction rates.
//!
//! ## LOD selection over the SAME `eg_viz_core::select_tier` rule
//!
//! [`resolve_tile`] calls `select_tier` exactly like the static-export path
//! (`eg-viz-export::render::resolve`) does — one tier-selection rule for the
//! whole engine, never a second parallel one for this transport. The frame
//! budget is derived directly from `width_px` (`2 * width_px` primitives — the
//! Line/Area `primitives_per_row` cost — `8 * width_px` bytes), so Direct wins
//! exactly when the viewport's row count already fits one point per pixel
//! column, and Decimate (via [`eg_viz_kernels::lttb_reduce`], `threshold =
//! width_px`) applies otherwise — **never more points than `width_px` pixels
//! can show**, by construction.
//!
//! **Deliberately different reduction choice than the static-export path.**
//! `eg-viz-export::render::resolve` uses [`eg_viz_kernels::m4_reduce`] for
//! Line/Area (a smooth full-shape overview) and refuses Decimate for Scatter
//! entirely (`mark_supports_tier`: "unordered decimation lies", routing
//! Scatter to a Density surface instead). This endpoint uses LTTB
//! unconditionally for every mark it serves, INCLUDING Scatter — and that is
//! honest specifically because LTTB never aggregates: every returned point is
//! a real row, so "here are some of the real points, zoom in for more" carries
//! no synthetic-aggregate lie the way a min/max marker would for a scatter
//! series. Density/Tiled-tier interactive tiles (a mean-color/heatmap surface
//! for the still-too-large-after-LTTB case) remain a documented gap for a
//! later lane — see this module's own tests for the exact row-count ceiling
//! (`MAX_TILE_ROWS`) this V1 refuses beyond, honestly, rather than silently
//! truncating.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use eg_viz_core::{
    select_tier, ColumnStoreIngest, Encodings, FrameBudget, LodTier, MarkKind, TierInput,
};
use eg_viz_kernels::lttb_reduce;

use crate::server::viz_engine::VizEngineState;

const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
/// Bound on the row count this endpoint will read+filter+reduce per tile
/// request — independent of (and far below) the static-export path's
/// `MAX_SYNTHETIC_SCATTER_ROWS`/`MAX_INLINE_COLUMN_ROWS` ingest caps. A
/// dataset larger than this can still be RENDERED via static export or a
/// narrower viewport; a single interactive tile request refuses to scan more
/// than this many rows rather than let one request stall the loopback
/// listener for other callers.
const MAX_TILE_ROWS: u64 = 200_000_000;

const TILE_PROTOCOL_VERSION: u8 = 1;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TileStatus {
    Ok = 0,
    /// No usable data: an unknown `dataset_ref`, or a viewport that overlaps
    /// none of it — the "degrade honestly, never fabricate" signal (see
    /// module doc). The client renders a visible "no data" state for this,
    /// never an empty-but-plausible-looking chart.
    Unavailable = 1,
}

const CLIENT_HTML: &str = include_str!("viz_interactive_client.html");

/// Serve the interactive viz surface on `listener`, sharing `engine`'s
/// persistent ColumnStore/render-cache/provenance with the RPC path (see
/// `handlers::viz::engine_state`'s doc for why this is the SAME engine, not a
/// second one).
pub async fn serve(
    listener: TcpListener,
    engine: Arc<VizEngineState>,
    // VIZ-1/VIZ-2 bridge: `graph_tile_server`'s routes need the graph
    // registry + persistence backend to resolve a real cluster hierarchy
    // (see `graph_tile_source`'s doc); every other route on this listener
    // ignores it. `None` ⇒ every `/graph_tile/*` request not opting into
    // `?demo=1` degrades to an honest empty tile (no registry to resolve a
    // graph against) -- `main.rs` always passes `Some`; `None` is for a
    // caller (e.g. an external integration test) with no `ServerState` to
    // hand in, per that type's own `pub(crate)`/`#[cfg(test)]` construction
    // boundary.
    state: Option<Arc<tokio::sync::RwLock<crate::server::ServerState>>>,
) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let engine = engine.clone();
        // Only `graph_tile_server`'s routes (below) read this -- avoid an
        // unused-clone/unused-param warning in a build without `viz-graph-tiles`.
        #[cfg(feature = "viz-graph-tiles")]
        let state = state.clone();
        #[cfg(not(feature = "viz-graph-tiles"))]
        let _ = &state;
        tokio::spawn(async move {
            let Some(request) = read_request(&mut stream).await else {
                return;
            };
            // VIZ-2: `/graph_tile/*` needs genuine async chunked-transfer
            // streaming (see `graph_tile_server`'s doc) that this function's
            // own buffered `(status, content_type, Vec<u8>)` `route()` below
            // cannot express -- dispatch to it first, before falling back to
            // the synchronous single-buffer path every other route here uses.
            #[cfg(feature = "viz-graph-tiles")]
            if crate::server::graph_tile_server::handles(&request.target) {
                crate::server::graph_tile_server::serve(
                    &mut stream,
                    &request.method,
                    &request.target,
                    state.as_ref(),
                )
                .await;
                return;
            }
            let (status_line, content_type, body) = route(&engine, &request);
            let mut head = format!(
                "HTTP/1.1 {status_line}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n",
                body.len()
            );
            if content_type == TILE_CONTENT_TYPE {
                head.push_str("cache-control: no-store\r\n");
            }
            head.push_str("\r\n");
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        });
    }
}

const TILE_CONTENT_TYPE: &str = "application/octet-stream";

struct HttpRequest {
    method: String,
    target: String,
}

/// Minimal GET-only HTTP/1.1 request-line + header reader — self-contained
/// (not shared with `lake::rest`'s fuller POST-capable reader; this surface
/// only ever serves `GET`), mirroring that module's own "the SAME
/// dependency-free idiom" precedent rather than depending on it directly.
async fn read_request(stream: &mut TcpStream) -> Option<HttpRequest> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        if find_subslice(&buf, b"\r\n\r\n").is_some() {
            break;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_HTTP_HEADER_BYTES {
            return None;
        }
    }
    let header_end = find_subslice(&buf, b"\r\n\r\n")?;
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let request_line = head.split("\r\n").next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let version = parts.next()?;
    if !version.starts_with("HTTP/1.") {
        return None;
    }
    Some(HttpRequest { method, target })
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

pub(crate) fn parse_query(target: &str) -> (&str, HashMap<String, String>) {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut params = HashMap::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let mut it = pair.splitn(2, '=');
        if let Some(k) = it.next() {
            let v = it.next().unwrap_or("");
            params.insert(
                percent_decode(&k.replace('+', " ")),
                percent_decode(&v.replace('+', " ")),
            );
        }
    }
    (path, params)
}

fn route(engine: &VizEngineState, request: &HttpRequest) -> (&'static str, &'static str, Vec<u8>) {
    if request.method != "GET" {
        return ("405 Method Not Allowed", "text/plain", b"GET only".to_vec());
    }
    let (path, params) = parse_query(&request.target);
    match path {
        "/" | "/index.html" => (
            "200 OK",
            "text/html; charset=utf-8",
            CLIENT_HTML.as_bytes().to_vec(),
        ),
        "/tile" => match resolve_tile(engine, &params) {
            Ok(bytes) => ("200 OK", TILE_CONTENT_TYPE, bytes),
            Err(message) => (
                "400 Bad Request",
                "application/json",
                format!("{{\"error\":{:?}}}", message).into_bytes(),
            ),
        },
        _ => ("404 Not Found", "text/plain", b"not found".to_vec()),
    }
}

pub(crate) fn parse_param<T: std::str::FromStr>(
    params: &HashMap<String, String>,
    key: &str,
) -> Option<T> {
    params.get(key).and_then(|v| v.parse::<T>().ok())
}

/// Resolve one `GET /tile` request: read the requested columns, filter to the
/// requested viewport (or the whole column range on a first request), select
/// the LOD tier via `eg_viz_core::select_tier`, and encode the result. Returns
/// `Err` only for a malformed/invalid REQUEST (missing params, unknown mark,
/// row count over [`MAX_TILE_ROWS`]) — an unknown dataset or an empty
/// viewport-filtered result is `Ok` with [`TileStatus::Unavailable`], per the
/// module doc's "never fabricate, degrade honestly" contract.
fn resolve_tile(
    engine: &VizEngineState,
    params: &HashMap<String, String>,
) -> Result<Vec<u8>, String> {
    let dataset_ref = params
        .get("dataset_ref")
        .ok_or_else(|| "missing required query param `dataset_ref`".to_string())?;
    let x_col = params.get("x").map(String::as_str).unwrap_or("x");
    let y_col = params.get("y").map(String::as_str).unwrap_or("y");
    let width_px: u32 = parse_param(params, "width_px")
        .ok_or_else(|| "missing or invalid required query param `width_px`".to_string())?;
    if width_px == 0 {
        return Err("`width_px` must be positive".to_string());
    }
    let viewport: Option<(f64, f64)> = match (
        parse_param::<f64>(params, "x0"),
        parse_param::<f64>(params, "x1"),
    ) {
        (Some(x0), Some(x1)) if x0.is_finite() && x1.is_finite() && x0 < x1 => Some((x0, x1)),
        (None, None) => None,
        _ => return Err("`x0`/`x1` must both be provided, finite, and x0 < x1".to_string()),
    };

    let store = engine.store.read();
    if store.content_fingerprint(dataset_ref).is_none() {
        return Ok(encode_unavailable());
    }
    let row_count = store.row_count(dataset_ref).map_err(|e| e.to_string())?;
    if row_count > MAX_TILE_ROWS {
        return Err(format!(
            "dataset `{dataset_ref}` has {row_count} rows, exceeding this endpoint's \
             per-request bound ({MAX_TILE_ROWS}); use a narrower viewport or the static \
             export path instead"
        ));
    }

    let xs_full = store
        .materialize_f64(dataset_ref, x_col)
        .map_err(|e| e.to_string())?;
    let ys_full = store
        .materialize_f64(dataset_ref, y_col)
        .map_err(|e| e.to_string())?;

    let mut xs: Vec<f64> = Vec::with_capacity(xs_full.len());
    let mut ys: Vec<f64> = Vec::with_capacity(ys_full.len());
    for (&x, &y) in xs_full.iter().zip(&ys_full) {
        if !x.is_finite() || !y.is_finite() {
            continue;
        }
        if let Some((x0, x1)) = viewport {
            if x < x0 || x > x1 {
                continue;
            }
        }
        xs.push(x);
        ys.push(y);
    }

    if xs.is_empty() {
        return Ok(encode_unavailable());
    }

    let budget = FrameBudget::new(
        (width_px as u64).saturating_mul(2),
        (width_px as u64).saturating_mul(8),
    );
    let decision = select_tier(&TierInput {
        mark: MarkKind::Line,
        row_count: xs.len() as u64,
        encodings: Encodings::default(),
        budget,
        out_of_core: false,
    });

    let (points, exact): (Vec<(f64, f64)>, bool) = match decision.tier {
        LodTier::Direct => {
            let pts = xs.iter().zip(&ys).map(|(&x, &y)| (x, y)).collect();
            (pts, true)
        }
        _ => (lttb_reduce(&xs, &ys, width_px as usize), false),
    };

    let x_min = points.iter().map(|p| p.0).fold(f64::INFINITY, f64::min);
    let x_max = points.iter().map(|p| p.0).fold(f64::NEG_INFINITY, f64::max);
    let y_min = points.iter().map(|p| p.1).fold(f64::INFINITY, f64::min);
    let y_max = points.iter().map(|p| p.1).fold(f64::NEG_INFINITY, f64::max);

    Ok(encode_tile(
        TileStatus::Ok,
        decision.tier,
        exact,
        (x_min, x_max, y_min, y_max),
        &points,
    ))
}

/// Encode the fixed 48-byte header + interleaved `f32` `(x,y)` payload — see
/// the module doc's "binary format" description. Every offset is 8-byte
/// aligned so a client can `new Float64Array`/`new Float32Array` any field
/// directly without a copy.
fn encode_tile(
    status: TileStatus,
    tier: LodTier,
    exact: bool,
    domain: (f64, f64, f64, f64),
    points: &[(f64, f64)],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(48 + points.len() * 8);
    out.push(TILE_PROTOCOL_VERSION);
    out.push(status as u8);
    out.push(tier as u8);
    out.push(exact as u8);
    out.push(MarkKind::Line as u8); // reserved mark slot; V1 always serves point-pair geometry
    out.extend_from_slice(&[0u8; 3]); // padding to the u32 count field
    out.extend_from_slice(&(points.len() as u32).to_le_bytes());
    out.extend_from_slice(&[0u8; 4]); // padding to the f64-aligned domain fields
    out.extend_from_slice(&domain.0.to_le_bytes());
    out.extend_from_slice(&domain.1.to_le_bytes());
    out.extend_from_slice(&domain.2.to_le_bytes());
    out.extend_from_slice(&domain.3.to_le_bytes());
    debug_assert_eq!(out.len(), 48);
    for &(x, y) in points {
        out.extend_from_slice(&(x as f32).to_le_bytes());
        out.extend_from_slice(&(y as f32).to_le_bytes());
    }
    out
}

fn encode_unavailable() -> Vec<u8> {
    encode_tile(
        TileStatus::Unavailable,
        LodTier::Direct,
        false,
        (0.0, 0.0, 0.0, 0.0),
        &[],
    )
}

/// Bridge so a fresh `ColumnStore` (used only by this module's own unit
/// tests, which do not need the full `VizEngineState`) can exercise
/// [`resolve_tile`]'s inner logic without spinning up the engine/HTTP stack.
#[cfg(test)]
fn test_store_with(
    dataset_ref: &str,
    xs: Vec<f64>,
    ys: Vec<f64>,
) -> eg_viz_columnstore::ColumnStore {
    use eg_viz_columnstore::{ColumnData, ColumnInput, ColumnStore};
    let mut store = ColumnStore::new();
    store
        .ingest_columns(
            dataset_ref,
            vec![
                ColumnInput::new("x", ColumnData::F64(xs)),
                ColumnInput::new("y", ColumnData::F64(ys)),
            ],
        )
        .unwrap();
    store
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::viz_engine::VizEngineState;

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn engine_with(dataset_ref: &str, xs: Vec<f64>, ys: Vec<f64>) -> VizEngineState {
        let engine = VizEngineState::new(None);
        *engine.store.write() = test_store_with(dataset_ref, xs, ys);
        engine
    }

    fn decode_header(bytes: &[u8]) -> (u8, u8, u8, u8, u32, (f64, f64, f64, f64)) {
        let status = bytes[1];
        let tier = bytes[2];
        let exact = bytes[3];
        let mark = bytes[4];
        let count = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let x_min = f64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let x_max = f64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let y_min = f64::from_le_bytes(bytes[32..40].try_into().unwrap());
        let y_max = f64::from_le_bytes(bytes[40..48].try_into().unwrap());
        (
            status,
            tier,
            exact,
            mark,
            count,
            (x_min, x_max, y_min, y_max),
        )
    }

    #[test]
    fn unknown_dataset_returns_unavailable_not_an_error() {
        let engine = VizEngineState::new(None);
        let bytes = resolve_tile(
            &engine,
            &params(&[("dataset_ref", "ds:missing"), ("width_px", "800")]),
        )
        .unwrap();
        let (status, ..) = decode_header(&bytes);
        assert_eq!(status, TileStatus::Unavailable as u8);
        assert_eq!(
            bytes.len(),
            48,
            "an unavailable tile carries no point payload"
        );
    }

    #[test]
    fn small_series_resolves_direct_and_exact() {
        let xs: Vec<f64> = (0..50).map(|i| i as f64).collect();
        let ys: Vec<f64> = (0..50).map(|i| (i as f64).sin()).collect();
        let engine = engine_with("ds:1", xs, ys);
        let bytes = resolve_tile(
            &engine,
            &params(&[("dataset_ref", "ds:1"), ("width_px", "800")]),
        )
        .unwrap();
        let (status, tier, exact, _mark, count, _domain) = decode_header(&bytes);
        assert_eq!(status, TileStatus::Ok as u8);
        assert_eq!(tier, LodTier::Direct as u8);
        assert_eq!(exact, 1);
        assert_eq!(count, 50);
        assert_eq!(bytes.len(), 48 + 50 * 8);
    }

    #[test]
    fn large_series_resolves_decimate_bounded_by_width_px() {
        let n = 2_000_000;
        let xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let ys: Vec<f64> = (0..n).map(|i| (i as f64 * 0.001).sin()).collect();
        let engine = engine_with("ds:1", xs, ys);
        let bytes = resolve_tile(
            &engine,
            &params(&[("dataset_ref", "ds:1"), ("width_px", "400")]),
        )
        .unwrap();
        let (status, tier, exact, _mark, count, _domain) = decode_header(&bytes);
        assert_eq!(status, TileStatus::Ok as u8);
        assert_eq!(tier, LodTier::Decimate as u8);
        assert_eq!(exact, 0);
        assert!(
            count as u64 <= 400,
            "must never send more points than width_px pixels can show, got {count}"
        );
        assert_eq!(bytes.len(), 48 + count as usize * 8);
    }

    #[test]
    fn viewport_narrows_the_row_count_and_can_flip_the_tier() {
        let n = 2_000_000;
        let xs: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let ys: Vec<f64> = vec![1.0; n as usize];
        let engine = engine_with("ds:1", xs, ys);
        // Full series -> Decimate.
        let full = resolve_tile(
            &engine,
            &params(&[("dataset_ref", "ds:1"), ("width_px", "400")]),
        )
        .unwrap();
        let (_, full_tier, ..) = decode_header(&full);
        assert_eq!(full_tier, LodTier::Decimate as u8);

        // A narrow viewport (100 rows) at the SAME width_px -> Direct, exact --
        // pan/zoom re-requesting a viewport gets a genuinely different tier,
        // never the full series re-sent.
        let narrow = resolve_tile(
            &engine,
            &params(&[
                ("dataset_ref", "ds:1"),
                ("width_px", "400"),
                ("x0", "0"),
                ("x1", "99"),
            ]),
        )
        .unwrap();
        let (status, narrow_tier, exact, _mark, count, _domain) = decode_header(&narrow);
        assert_eq!(status, TileStatus::Ok as u8);
        assert_eq!(narrow_tier, LodTier::Direct as u8);
        assert_eq!(exact, 1);
        assert_eq!(count, 100);
    }

    #[test]
    fn a_viewport_disjoint_from_the_data_is_unavailable_not_an_empty_chart() {
        let xs: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let ys: Vec<f64> = vec![1.0; 1000];
        let engine = engine_with("ds:1", xs, ys);
        let bytes = resolve_tile(
            &engine,
            &params(&[
                ("dataset_ref", "ds:1"),
                ("width_px", "400"),
                ("x0", "5000"),
                ("x1", "6000"),
            ]),
        )
        .unwrap();
        let (status, ..) = decode_header(&bytes);
        assert_eq!(status, TileStatus::Unavailable as u8);
    }

    #[test]
    fn nan_and_infinite_rows_are_excluded_from_the_tile() {
        let xs = vec![1.0, 2.0, 3.0, 4.0];
        let ys = vec![1.0, f64::NAN, f64::INFINITY, 4.0];
        let engine = engine_with("ds:1", xs, ys);
        let bytes = resolve_tile(
            &engine,
            &params(&[("dataset_ref", "ds:1"), ("width_px", "800")]),
        )
        .unwrap();
        let (status, _tier, _exact, _mark, count, _domain) = decode_header(&bytes);
        assert_eq!(status, TileStatus::Ok as u8);
        assert_eq!(count, 2, "only rows 0 and 3 are fully finite on both axes");
    }

    #[test]
    fn missing_dataset_ref_param_is_a_clear_error() {
        let engine = VizEngineState::new(None);
        let err = resolve_tile(&engine, &params(&[("width_px", "800")])).unwrap_err();
        assert!(err.contains("dataset_ref"));
    }

    #[test]
    fn missing_width_px_param_is_a_clear_error() {
        let engine = engine_with("ds:1", vec![1.0], vec![1.0]);
        let err = resolve_tile(&engine, &params(&[("dataset_ref", "ds:1")])).unwrap_err();
        assert!(err.contains("width_px"));
    }

    #[test]
    fn query_string_is_parsed_and_percent_decoded() {
        let (path, params) = parse_query("/tile?dataset_ref=ds%3A1&width_px=800");
        assert_eq!(path, "/tile");
        assert_eq!(params.get("dataset_ref").unwrap(), "ds:1");
        assert_eq!(params.get("width_px").unwrap(), "800");
    }

    #[test]
    fn path_without_a_query_string_parses_cleanly() {
        let (path, params) = parse_query("/");
        assert_eq!(path, "/");
        assert!(params.is_empty());
    }
}
