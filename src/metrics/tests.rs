//! Tests of the engine metrics registry (moved out of `metrics.rs` to keep
//! the module under the per-file line cap).
use super::*;
use crate::lock_recovery::LockRecovery;

// The Prometheus REGISTRY and the bounded graph-label cap (SEEN_GRAPHS) are
// process-global singletons. Tests that depend on the labelled-series set must
// not run concurrently with the cardinality test that saturates the 128-slot
// cap (which would aggregate their graph into `__overflow__` and drop the
// labelled needle). Serialize those tests on this lock.
static GLOBAL_LABEL_STATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn test_render_contains_recorded_series() {
    let _guard = GLOBAL_LABEL_STATE.lock_recovering("global metric label state");
    record_request("Ping", 0.0007);
    graph_op("agent:metrics-test");
    set_graph_size("agent:metrics-test", 3, 2);
    auth_failure();
    access_denied();
    busy_rejected();
    checkpoint_completed(0.02);
    // CONCEPT:EG-OS.observability.write-lock-gap-histogram — write-lock-gap histograms register + render.
    observe_write_lock_wait("agent:metrics-test", 0.003);
    observe_write_lock_hold("agent:metrics-test", 0.0005);
    set_resource_stats(2, 1024, 2048, 3, 256, 10);

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
        "epistemic_graph_write_lock_wait_seconds_bucket",
        "epistemic_graph_write_lock_hold_seconds_bucket",
        "epistemic_graph_effective_cpu_cores",
        "epistemic_graph_effective_memory_limit_bytes",
        "epistemic_graph_process_rss_bytes",
        "epistemic_graph_write_coalescer_queue_depth",
        "epistemic_graph_write_coalescer_queue_bytes",
        "epistemic_graph_write_coalescer_operations_total",
    ] {
        assert!(out.contains(needle), "missing {needle} in:\n{out}");
    }
}

#[test]
fn test_graph_label_cardinality_is_bounded() {
    let _guard = GLOBAL_LABEL_STATE.lock_recovering("global metric label state");
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

    // Deleting any graph clears the shared overflow series. Recreating a
    // name after all tracked slots are freed must acquire a fresh bounded
    // raw slot rather than inheriting stale `__overflow__` state.
    for i in 1..200 {
        drop_graph(&format!("agent:cardinality-{i}"));
    }
    assert!(
        !render().contains("graph=\"__overflow__\""),
        "last deleted overflow owner must clear every overflow series"
    );
    graph_op("agent:cardinality-recreated");
    let recreated = render();
    assert!(
        recreated.contains("graph=\"agent:cardinality-recreated\""),
        "a recreated graph should receive a fresh bounded label slot"
    );
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
