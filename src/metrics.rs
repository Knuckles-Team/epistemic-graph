// CONCEPT:KG-2.51 — Engine Observability (Prometheus metrics)
//
// First-class metrics for the Rust tier: request rate/latency per operation,
// in-flight admission state, per-graph activity and size, checkpoint health,
// and security counters (auth failures / ACL denials). Exposed in Prometheus
// text format by a tiny HTTP listener (`serve`) bound via `--metrics-addr` /
// `GRAPH_SERVICE_METRICS_ADDR` — fully separate from the MessagePack RPC
// transport, disabled unless an address is configured.
//
// The module compiles to no-op shims when the `metrics` cargo feature (on by
// default) is disabled, so call sites never need `#[cfg]` guards.
//
// Cardinality: `op` is bounded by the protocol's `Method` variants. The
// `graph` label is bounded by `MAX_GRAPH_LABELS` distinct names — graphs past
// the cap aggregate under `__overflow__` so an unbounded tenant namespace can
// never blow up the time-series store.

#[cfg(feature = "metrics")]
mod imp {
    use lazy_static::lazy_static;
    use parking_lot::Mutex;
    use prometheus::{
        Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
        IntGaugeVec, Opts, Registry, TextEncoder,
    };
    use std::collections::HashSet;

    /// UDS round-trips are sub-millisecond; analytics can run for seconds —
    /// span both regimes.
    const DURATION_BUCKETS: &[f64] = &[
        0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
        5.0, 10.0, 30.0,
    ];

    /// Distinct `graph` label values tracked before aggregating into
    /// `__overflow__` (deleting a graph frees its slot).
    const MAX_GRAPH_LABELS: usize = 128;
    const OVERFLOW_LABEL: &str = "__overflow__";

    fn counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
        let m = IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    fn gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
        let m = IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    fn gauge(name: &str, help: &str) -> IntGauge {
        let m = IntGauge::new(name, help).expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    fn counter(name: &str, help: &str) -> IntCounter {
        let m = IntCounter::new(name, help).expect("valid metric");
        REGISTRY
            .register(Box::new(m.clone()))
            .expect("unique metric");
        m
    }

    lazy_static! {
        static ref REGISTRY: Registry = Registry::new();
        static ref SEEN_GRAPHS: Mutex<HashSet<String>> = Mutex::new(HashSet::new());
        static ref REQUESTS_TOTAL: IntCounterVec = counter_vec(
            "epistemic_graph_requests_total",
            "Requests dispatched, by protocol operation",
            &["op"],
        );
        static ref REQUEST_DURATION: HistogramVec = {
            let m = HistogramVec::new(
                HistogramOpts::new(
                    "epistemic_graph_request_duration_seconds",
                    "Dispatch latency, by protocol operation",
                )
                .buckets(DURATION_BUCKETS.to_vec()),
                &["op"],
            )
            .expect("valid metric");
            REGISTRY
                .register(Box::new(m.clone()))
                .expect("unique metric");
            m
        };
        static ref IN_FLIGHT: IntGauge = gauge(
            "epistemic_graph_in_flight_requests",
            "Connection requests currently holding an admission permit",
        );
        static ref PERMITS_AVAILABLE: IntGauge = gauge(
            "epistemic_graph_inflight_permits_available",
            "Admission-semaphore permits still available \
             (EPISTEMIC_GRAPH_MAX_INFLIGHT minus in-flight; admission is \
             try-acquire, so nothing ever queues — excess load is shed as BUSY)",
        );
        static ref BUSY_REJECTIONS: IntCounter = counter(
            "epistemic_graph_busy_rejections_total",
            "Requests shed with BUSY because the admission semaphore was exhausted",
        );
        static ref WAL_APPEND_DROPPED: IntCounter = counter(
            "epistemic_graph_wal_append_dropped_total",
            "Durable mutations whose WAL append was dropped because the off-reactor \
             WAL writer channel was saturated (data is still in memory + checkpointed; \
             the crash-recovery window widened — investigate disk/throughput)",
        );
        static ref GRAPH_OPS: IntCounterVec = counter_vec(
            "epistemic_graph_graph_ops_total",
            "Graph-targeted operations admitted past the ACL, by graph (bounded cardinality)",
            &["graph"],
        );
        static ref GRAPH_NODES: IntGaugeVec = gauge_vec(
            "epistemic_graph_graph_nodes",
            "Node count per graph, updated on mutation",
            &["graph"],
        );
        static ref GRAPH_EDGES: IntGaugeVec = gauge_vec(
            "epistemic_graph_graph_edges",
            "Edge count per graph, updated on mutation",
            &["graph"],
        );
        static ref CHECKPOINT_DURATION: Histogram = {
            let m = Histogram::with_opts(
                HistogramOpts::new(
                    "epistemic_graph_checkpoint_duration_seconds",
                    "Wall-clock duration of a full registry checkpoint",
                )
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0]),
            )
            .expect("valid metric");
            REGISTRY
                .register(Box::new(m.clone()))
                .expect("unique metric");
            m
        };
        static ref CHECKPOINT_LAST_SUCCESS: IntGauge = gauge(
            "epistemic_graph_checkpoint_last_success_timestamp_seconds",
            "Unix timestamp of the last successful checkpoint",
        );
        static ref AUTH_FAILURES: IntCounter = counter(
            "epistemic_graph_auth_failures_total",
            "Requests rejected by HMAC authentication",
        );
        static ref ACCESS_DENIED: IntCounter = counter(
            "epistemic_graph_access_denied_total",
            "Graph operations denied by the isolation ACL",
        );
        // Per-graph write-coalescer observability (CONCEPT:KG-2.182). The win is
        // visible as ops/batch > 1: BATCHES counts topology-lock acquisitions on the
        // coalesced path, OPS counts the writes those acquisitions applied, so
        // (OPS / BATCHES) is the lock-acquisitions-saved ratio per graph.
        static ref WRITE_BATCHES: IntCounterVec = counter_vec(
            "epistemic_graph_write_batches_total",
            "Coalesced write batches committed (= topology-lock acquisitions on the \
             coalesced path), by graph",
            &["graph"],
        );
        static ref WRITE_BATCHED_OPS: IntCounterVec = counter_vec(
            "epistemic_graph_write_batched_ops_total",
            "Single-op writes applied via the coalescer, by graph. Divide by \
             write_batches_total for the average batch size (ops per lock acquisition)",
            &["graph"],
        );
        // ── Cost / efficiency autoscale signals (CONCEPT:KG-2.234, Lane V) ──
        static ref GRAPH_MEMORY_BYTES: IntGaugeVec = gauge_vec(
            "epistemic_graph_graph_memory_bytes",
            "Approximate resident RAM per graph (node/edge property blobs + topology \
             overhead + embedding vectors), updated by the budget sweep",
            &["graph"],
        );
        static ref GRAPH_HIBERNATED: IntGaugeVec = gauge_vec(
            "epistemic_graph_graph_hibernated",
            "1 if the graph is hibernated (in-RAM state dropped, durable in redb), 0 if \
             resident — by graph",
            &["graph"],
        );
        static ref BUDGET_EVICTIONS: IntCounter = counter(
            "epistemic_graph_budget_evictions_total",
            "LRU nodes evicted by the per-tenant memory-budget enforcer (the eviction \
             rate an autoscaler watches: sustained eviction = memory pressure)",
        );
        static ref BUDGET_HIBERNATIONS: IntCounter = counter(
            "epistemic_graph_budget_hibernations_total",
            "Cold graphs hibernated (in-RAM state dropped) by the budget enforcer to keep \
             a tenant under its memory budget",
        );
    }

    /// Map a graph name onto the bounded label space.
    fn graph_label(graph: &str) -> String {
        let mut seen = SEEN_GRAPHS.lock();
        if seen.contains(graph) {
            graph.to_string()
        } else if seen.len() < MAX_GRAPH_LABELS {
            seen.insert(graph.to_string());
            graph.to_string()
        } else {
            OVERFLOW_LABEL.to_string()
        }
    }

    pub fn record_request(op: &str, seconds: f64) {
        REQUESTS_TOTAL.with_label_values(&[op]).inc();
        REQUEST_DURATION.with_label_values(&[op]).observe(seconds);
    }

    pub fn connection_request_started(permits_available: usize) {
        IN_FLIGHT.inc();
        PERMITS_AVAILABLE.set(permits_available as i64);
    }

    pub fn connection_request_finished(permits_available: usize) {
        IN_FLIGHT.dec();
        PERMITS_AVAILABLE.set(permits_available as i64);
    }

    pub fn busy_rejected() {
        BUSY_REJECTIONS.inc();
    }

    pub fn graph_op(graph: &str) {
        GRAPH_OPS.with_label_values(&[&graph_label(graph)]).inc();
    }

    pub fn set_graph_size(graph: &str, nodes: i64, edges: i64) {
        let label = graph_label(graph);
        GRAPH_NODES.with_label_values(&[&label]).set(nodes);
        GRAPH_EDGES.with_label_values(&[&label]).set(edges);
    }

    /// Set the per-graph resident-memory estimate gauge (CONCEPT:KG-2.234), in bytes.
    pub fn set_graph_memory(graph: &str, bytes: i64) {
        GRAPH_MEMORY_BYTES
            .with_label_values(&[&graph_label(graph)])
            .set(bytes);
    }

    /// Mark a graph hibernated (1) or resident (0) (CONCEPT:KG-2.234).
    pub fn set_graph_hibernated(graph: &str, hibernated: bool) {
        GRAPH_HIBERNATED
            .with_label_values(&[&graph_label(graph)])
            .set(hibernated as i64);
    }

    /// Record `n` LRU nodes evicted by the per-tenant budget enforcer (CONCEPT:KG-2.234).
    pub fn budget_evicted(n: u64) {
        BUDGET_EVICTIONS.inc_by(n);
    }

    /// Record one graph hibernated by the budget enforcer (CONCEPT:KG-2.234).
    pub fn budget_hibernated() {
        BUDGET_HIBERNATIONS.inc();
    }

    /// Forget a deleted graph: drop its series and free its label slot.
    pub fn drop_graph(graph: &str) {
        let _ = GRAPH_NODES.remove_label_values(&[graph]);
        let _ = GRAPH_EDGES.remove_label_values(&[graph]);
        let _ = GRAPH_OPS.remove_label_values(&[graph]);
        let _ = GRAPH_MEMORY_BYTES.remove_label_values(&[graph]);
        let _ = GRAPH_HIBERNATED.remove_label_values(&[graph]);
        SEEN_GRAPHS.lock().remove(graph);
    }

    pub fn checkpoint_completed(seconds: f64) {
        CHECKPOINT_DURATION.observe(seconds);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        CHECKPOINT_LAST_SUCCESS.set(now as i64);
    }

    pub fn auth_failure() {
        AUTH_FAILURES.inc();
    }

    pub fn access_denied() {
        ACCESS_DENIED.inc();
    }

    pub fn wal_append_dropped() {
        WAL_APPEND_DROPPED.inc();
    }

    /// Record one committed coalesced write batch of `ops` single-op writes against
    /// `graph` (CONCEPT:KG-2.182): +1 lock acquisition, +`ops` applied writes.
    pub fn write_batch_committed(graph: &str, ops: usize) {
        let label = graph_label(graph);
        WRITE_BATCHES.with_label_values(&[&label]).inc();
        WRITE_BATCHED_OPS
            .with_label_values(&[&label])
            .inc_by(ops as u64);
    }

    /// Render the full registry in Prometheus text exposition format.
    pub fn render() -> String {
        let encoder = TextEncoder::new();
        let mut buf = Vec::new();
        if encoder.encode(&REGISTRY.gather(), &mut buf).is_err() {
            return String::new();
        }
        String::from_utf8(buf).unwrap_or_default()
    }
}

#[cfg(feature = "metrics")]
pub use imp::*;

// ── No-op shims (feature `metrics` disabled) ─────────────────────────────
// Call sites stay free of #[cfg] guards; the optimizer erases these.
#[cfg(not(feature = "metrics"))]
mod imp {
    pub fn record_request(_op: &str, _seconds: f64) {}
    pub fn connection_request_started(_permits_available: usize) {}
    pub fn connection_request_finished(_permits_available: usize) {}
    pub fn busy_rejected() {}
    pub fn graph_op(_graph: &str) {}
    pub fn set_graph_size(_graph: &str, _nodes: i64, _edges: i64) {}
    pub fn set_graph_memory(_graph: &str, _bytes: i64) {}
    pub fn set_graph_hibernated(_graph: &str, _hibernated: bool) {}
    pub fn budget_evicted(_n: u64) {}
    pub fn budget_hibernated() {}
    pub fn drop_graph(_graph: &str) {}
    pub fn checkpoint_completed(_seconds: f64) {}
    pub fn auth_failure() {}
    pub fn access_denied() {}
    pub fn wal_append_dropped() {}
    pub fn write_batch_committed(_graph: &str, _ops: usize) {}
    pub fn render() -> String {
        String::new()
    }
}

#[cfg(not(feature = "metrics"))]
pub use imp::*;

/// Minimal HTTP/1.1 exposition endpoint. Every request to the bound address
/// (whatever the path) receives the full registry — keeps the listener free
/// of an HTTP-framework dependency and completely separate from the
/// length-prefixed MessagePack RPC ports.
#[cfg(all(feature = "metrics", feature = "server"))]
pub async fn serve(listener: tokio::net::TcpListener) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        tokio::spawn(async move {
            // Drain (up to one read of) the request head; the path is
            // irrelevant for a single-purpose exposition endpoint.
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let body = render();
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes()).await;
            let _ = stream.shutdown().await;
        });
    }
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::*;

    // The Prometheus REGISTRY and the bounded graph-label cap (SEEN_GRAPHS) are
    // process-global singletons. Tests that depend on the labelled-series set must
    // not run concurrently with the cardinality test that saturates the 128-slot
    // cap (which would aggregate their graph into `__overflow__` and drop the
    // labelled needle). Serialize those tests on this lock.
    static GLOBAL_LABEL_STATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_render_contains_recorded_series() {
        let _guard = GLOBAL_LABEL_STATE.lock().unwrap_or_else(|e| e.into_inner());
        record_request("Ping", 0.0007);
        graph_op("agent:metrics-test");
        set_graph_size("agent:metrics-test", 3, 2);
        auth_failure();
        access_denied();
        busy_rejected();
        checkpoint_completed(0.02);

        let out = render();
        // Note: the per-graph label series (graph_ops_total{graph=...}, graph_nodes,
        // graph_edges) are NOT asserted by exact label here — the graph-label space is
        // a process-global bounded cap (SEEN_GRAPHS, 128) that concurrent server
        // dispatch tests also populate via graph_op(), so a specific label can spill to
        // `__overflow__` under parallel load. The cap/overflow behavior is covered by
        // test_graph_label_cardinality_is_bounded; here we only assert the metric
        // FAMILIES are registered + rendered (robust to the shared global registry).
        for needle in [
            "epistemic_graph_requests_total{op=\"Ping\"}",
            "epistemic_graph_request_duration_seconds_bucket{op=\"Ping\"",
            "epistemic_graph_graph_ops_total",
            "epistemic_graph_graph_nodes",
            "epistemic_graph_graph_edges",
            "epistemic_graph_auth_failures_total",
            "epistemic_graph_access_denied_total",
            "epistemic_graph_busy_rejections_total",
            "epistemic_graph_checkpoint_duration_seconds_count",
            "epistemic_graph_checkpoint_last_success_timestamp_seconds",
        ] {
            assert!(out.contains(needle), "missing {needle} in:\n{out}");
        }
    }

    #[test]
    fn test_graph_label_cardinality_is_bounded() {
        let _guard = GLOBAL_LABEL_STATE.lock().unwrap_or_else(|e| e.into_inner());
        // Saturate the label space, then confirm new names aggregate.
        for i in 0..200 {
            graph_op(&format!("agent:cardinality-{i}"));
        }
        let out = render();
        assert!(
            out.contains("epistemic_graph_graph_ops_total{graph=\"__overflow__\"}"),
            "expected overflow aggregation past the label cap"
        );
        // Deleting frees the slot and removes the series.
        drop_graph("agent:cardinality-0");
        assert!(!render().contains("graph=\"agent:cardinality-0\""));
    }

    #[cfg(feature = "server")]
    #[tokio::test]
    async fn test_http_exposition_endpoint() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        record_request("Health", 0.001);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { serve(listener).await });

        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        let text = String::from_utf8_lossy(&out);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "got: {text}");
        assert!(text.contains("epistemic_graph_requests_total"));
    }
}
