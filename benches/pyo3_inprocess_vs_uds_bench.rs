//! In-process (PyO3-shaped) vs out-of-process (UDS wire) latency for the SAME
//! `AddNode` + `GetNodeProperties` op (unified-binary program, workstream W-A;
//! see `docs/architecture/unified-inprocess-engine.md`).
//!
//! Both arms call the IDENTICAL `epistemic_graph::graph::GraphCore` mutation —
//! the only variable is the transport in between:
//!
//!   * `inprocess`  — calls `GraphCore::add_node` / `get_node_properties`
//!     directly. This is exactly what `crates/eg-pyengine`'s pyo3 boundary does
//!     inside `Python::detach` (this bench does not spin up a Python
//!     interpreter; it isolates the transport delta the embedding removes, not
//!     the pyo3 arg-marshaling cost itself — see the design doc's "what this
//!     does not measure").
//!   * `uds_socket` — wraps the SAME call behind a real 4-byte big-endian
//!     length-prefixed MessagePack frame (the framing `AGENTS.md` documents),
//!     sent over a real Unix domain socket to a Tokio task in the SAME OS
//!     process, decoded back into a `Method`, applied to the SAME kind of
//!     `GraphCore`, and framed back. ONE persistent connection is reused
//!     across every sample (never reconnecting mid-benchmark), matching how
//!     the real client/server keep one long-lived connection
//!     (`epistemic_graph.client`'s `_ensure_connection`) — so this isolates
//!     per-request framing/socket cost, not connection setup.
//!
//! Deliberately NOT measured here: the `eg2.` authenticated envelope (HMAC
//! verify, replay ledger, RBAC) the served transport always requires — that
//! cost is orthogonal to this workstream (identity is unchanged by *where* the
//! engine runs) and is already captured end-to-end in `docs/benchmarks.md`
//! (`AddNode` p50 ~= 0.187 ms, p99 ~= 0.223 ms over UDS, single connection,
//! via `scripts/bench_transport.py`). This bench isolates the
//! serialize-then-socket-then-deserialize tax specifically, which is the part
//! the in-process shape removes.
//!
//! Run: cargo bench --profile release --features server --bench
//! pyo3_inprocess_vs_uds_bench --target-dir ./target-isolated
//!
//! `--profile release` is worth passing explicitly: in this workspace a bare
//! `cargo bench` resolves a different build fingerprint than
//! `cargo build --release` even for byte-identical optimization settings, and
//! forces a second full dependency recompilation (DataFusion et al.) instead
//! of reusing an already-built release tree.
//!
//! Gated to `--features server` (needs the tokio runtime + a real UDS); a
//! default `cargo check --benches` skips it. DEV-only; never linked into the
//! server binary.

use std::cell::Cell;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use epistemic_graph::graph::GraphCore;
use epistemic_graph::protocol::Method;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Builder;

fn node_props(k: usize) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({ "k": k, "label": "bench" })).unwrap()
}

/// One AddNode + one GetNodeProperties directly against `core` — the unit of
/// work both arms perform identically (only how the call REACHES `core` differs).
fn add_then_get_inprocess(core: &GraphCore, id: usize) {
    let node_id = format!("n{id}");
    core.add_node(node_id.clone(), node_props(id));
    let props = core.get_node_properties(&node_id);
    std::hint::black_box(props);
}

// ── Framing helpers — the SAME 4-byte big-endian length prefix + MessagePack
//    body `AGENTS.md` documents for the served transport. ─────────────────
async fn write_frame(stream: &mut UnixStream, bytes: &[u8]) {
    stream
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .expect("write length prefix");
    stream.write_all(bytes).await.expect("write frame body");
}

/// Client-side frame read: panics on any I/O error — the client always
/// expects a reply from the still-running persistent server, so any error
/// here is a genuine bug, not an expected condition.
async fn read_frame(stream: &mut UnixStream) -> Vec<u8> {
    try_read_frame(stream)
        .await
        .expect("read length prefix: connection closed unexpectedly")
}

/// Server-side frame read: `None` on a clean EOF at the frame boundary (the
/// client disconnected), `Some` on a full frame, panics on any OTHER I/O
/// error (a genuinely malformed frame). The persistent server loop uses this
/// to exit quietly when the client disconnects — a clean EOF is EXPECTED
/// exactly once, when the whole benchmark process tears down and drops its
/// client connection, and treating that as a panic (as an unconditional
/// `read_exact(..).expect(..)` would) is a false failure signal on a
/// perfectly successful run, not a real bug.
async fn try_read_frame(stream: &mut UnixStream) -> Option<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
        Err(e) => panic!("read length prefix: {e}"),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await.expect("read frame body");
    Some(buf)
}

/// Decode one `Method`, apply it to `core`, and return the framed reply body.
/// Only the two ops this bench drives are handled — anything else is a bug in
/// the bench, not a production dispatch path (the real router is
/// `src/server/dispatch.rs`; this is a minimal stand-in isolating transport
/// cost, not a reimplementation of the router).
fn apply_and_reply(core: &GraphCore, method: Method) -> Vec<u8> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            core.add_node(node_id, properties_msgpack);
            rmp_serde::to_vec_named(&()).expect("encode ack")
        }
        Method::GetNodeProperties { node_id } => {
            let props = core.get_node_properties(&node_id);
            rmp_serde::to_vec_named(&props).expect("encode properties reply")
        }
        other => panic!("bench only drives AddNode/GetNodeProperties, got {other:?}"),
    }
}

async fn call_over_uds(stream: &mut UnixStream, method: &Method) -> Vec<u8> {
    let payload = rmp_serde::to_vec_named(method).expect("encode request");
    write_frame(stream, &payload).await;
    read_frame(stream).await
}

async fn add_then_get_over_uds(stream: &mut UnixStream, id: usize) {
    let node_id = format!("n{id}");
    let add = Method::AddNode {
        node_id: node_id.clone(),
        properties_msgpack: node_props(id),
    };
    std::hint::black_box(call_over_uds(stream, &add).await);
    let get = Method::GetNodeProperties { node_id };
    std::hint::black_box(call_over_uds(stream, &get).await);
}

/// The persistent server loop: accept ONE connection on `listener` and then
/// serially frame-in / apply / frame-out until the client disconnects (which
/// happens exactly once, when the whole benchmark process tears down and
/// drops its client connection) — one connection is reused for the whole
/// benchmark, matching the client side.
async fn persistent_server_loop(listener: UnixListener, core: Arc<GraphCore>) {
    let (mut stream, _) = listener.accept().await.expect("accept bench connection");
    loop {
        let Some(req_bytes) = try_read_frame(&mut stream).await else {
            return; // client disconnected — benchmark process is exiting
        };
        let method: Method = rmp_serde::from_slice(&req_bytes).expect("decode Method");
        let reply = apply_and_reply(&core, method);
        write_frame(&mut stream, &reply).await;
    }
}

fn bench_inprocess(c: &mut Criterion) {
    let core = GraphCore::new();
    let offset = Cell::new(0usize);

    let mut group = c.benchmark_group("pyo3_inprocess_vs_uds_add_then_get");
    for &n in &[100usize, 1_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("inprocess", n), &n, |b, &n| {
            b.iter(|| {
                let base = offset.get();
                offset.set(base + n);
                for i in base..base + n {
                    add_then_get_inprocess(&core, i);
                }
            });
        });
    }
    group.finish();
}

fn bench_uds(c: &mut Criterion) {
    let rt = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let dir = tempfile::tempdir().expect("bench temp dir");
    let sock_path = dir.path().join("pyo3-vs-uds-bench.sock");
    let core = Arc::new(GraphCore::new());

    // Bind the listener, spawn the persistent server loop, and connect the
    // persistent client — all inside ONE `block_on`, since `UnixListener::bind`
    // and `tokio::spawn` both need an active Tokio runtime context even though
    // neither is itself awaited here.
    let mut client_stream = rt.block_on(async {
        let listener = UnixListener::bind(&sock_path).expect("bind bench UDS listener");
        tokio::spawn(persistent_server_loop(listener, core));
        UnixStream::connect(&sock_path)
            .await
            .expect("connect bench UDS client")
    });
    let offset = Cell::new(0usize);

    let mut group = c.benchmark_group("pyo3_inprocess_vs_uds_add_then_get");
    for &n in &[100usize, 1_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("uds_socket", n), &n, |b, &n| {
            b.iter(|| {
                let base = offset.get();
                offset.set(base + n);
                rt.block_on(async {
                    for i in base..base + n {
                        add_then_get_over_uds(&mut client_stream, i).await;
                    }
                });
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench_inprocess, bench_uds
}
criterion_main!(benches);
