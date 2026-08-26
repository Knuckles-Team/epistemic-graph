//! CA-11 P3 — live end-to-end proof that a `CdcHub::emit` on the REAL
//! production hook (the same one `mutation.rs::commit_finalize` calls)
//! reaches Kafka in order. Feature `cdc-kafka` only; compiles to nothing
//! otherwise.
//!
//! `#[ignore]`d because it needs a live broker — this is infra-integration,
//! not a unit test. Run explicitly:
//!
//! ```text
//! EPISTEMIC_GRAPH_CDC_KAFKA_BROKERS=<host:port> \
//!   cargo test --features cdc-kafka --target-dir ./target-isolated -j 12 \
//!   --test e2e_ca_cdc_kafka -- --ignored --nocapture
//! ```
//!
//! Independent read-back (NOT this test — an external tool, matching the
//! sibling OpenLineage lane's `kafka-console-consumer` proof) is documented
//! in the CA-11 lane report, not embedded here: this crate takes no
//! Kafka-CONSUMER dependency (`eg_stream::sink`'s `rdkafka` feature set is
//! producer-only: `cmake-build,ssl,sasl`, no default consumer group
//! machinery pulled in), so asserting delivery from inside this binary is
//! limited to "the producer accepted every send and CdcHub's own seq
//! ordering held" — real, but not a substitute for the external read-back.
#![cfg(feature = "cdc-kafka")]

use std::sync::Arc;

use epistemic_graph::server::cdc::CdcHub;
use epistemic_graph::server::cdc_sink;
use epistemic_graph::wire::CdcKind;

#[test]
#[ignore = "needs a live Kafka broker; see module doc for the exact invocation"]
fn cdc_hub_emit_reaches_the_installed_kafka_sink_in_order() {
    let graph = format!(
        "ca11_e2e_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let hub = Arc::new(CdcHub::new());
    cdc_sink::install_from_env(&hub);

    // Ten ordered node writes on a graph name unique to this run (see the
    // module doc for why: this proves reachability, not idempotent replay).
    let mut seqs = Vec::new();
    for i in 0..10u32 {
        let seq = hub.emit(
            &graph,
            CdcKind::AddNode,
            format!("n{i}"),
            String::new(),
            None,
            // Real MessagePack (not just JSON-shaped bytes) so the read-back
            // proves the actual `decode_property_value` -> envelope `after`
            // path, not merely that a byte blob rode the wire unmodified.
            Some(rmp_serde::to_vec(&serde_json::json!({"i": i})).unwrap()),
        );
        seqs.push(seq);
    }

    // Block for real delivery before asserting/exiting -- `KafkaCdcSink::emit`
    // is non-blocking by design (never stalls a commit), so without this the
    // test process can exit before librdkafka's background I/O thread has
    // actually sent anything, proving nothing about delivery. `flush_sink`
    // is NOT on `CdcHub::emit`'s path; this is the one place it belongs.
    hub.flush_sink(10_000);

    // `CdcHub::emit`'s per-graph seq is strictly monotonic by construction
    // (cdc.rs: `feed.next_seq += 1` under the feeds lock) — this is the
    // in-process half of P3's ordering claim; the broker-side half (that
    // `KafkaCdcSink` forwarded every one of these, in this order, to
    // partition 0) is the external `kafka-console-consumer` read-back in
    // the lane report.
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "CdcHub seq is not monotonic: {seqs:?}");
    assert_eq!(seqs.len(), 10);

    eprintln!(
        "cdc-kafka e2e: emitted graph={graph} seqs={seqs:?} -- read back with:\n\
         kubectl exec -n apps deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
         --bootstrap-server localhost:9092 --topic eg.cdc.{graph} --from-beginning \
         --timeout-ms 5000"
    );
}

/// P3's "ordering under concurrent multi-graph writes" (test matrix row) --
/// N threads hammer `CdcHub::emit` on the SAME graph concurrently. `emit`
/// assigns `seq` and forwards to the sink inside the SAME `feeds` mutex
/// (`cdc.rs`), so contention serializes both the seq assignment AND the
/// Kafka send in lockstep -- this is what makes the single-partition
/// ordering guarantee hold under concurrency, not just in the single-thread
/// case above. Assert in-process (seq assignment has no gaps/dupes); the
/// external read-back (lane report) confirms partition 0 preserves it.
#[test]
#[ignore = "needs a live Kafka broker; see module doc for the exact invocation"]
fn cdc_hub_emit_stays_ordered_under_concurrent_writers() {
    let graph = Arc::new(format!(
        "ca11_e2e_concurrent_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let hub = Arc::new(CdcHub::new());
    cdc_sink::install_from_env(&hub);

    const WRITERS: usize = 8;
    const PER_WRITER: usize = 25;
    let handles: Vec<_> = (0..WRITERS)
        .map(|w| {
            let hub = Arc::clone(&hub);
            let graph = Arc::clone(&graph);
            std::thread::spawn(move || {
                let mut seqs = Vec::with_capacity(PER_WRITER);
                for i in 0..PER_WRITER {
                    let seq = hub.emit(
                        &graph,
                        CdcKind::AddNode,
                        format!("w{w}n{i}"),
                        String::new(),
                        None,
                        Some(rmp_serde::to_vec(&serde_json::json!({"w": w, "i": i})).unwrap()),
                    );
                    seqs.push(seq);
                }
                seqs
            })
        })
        .collect();

    let mut all_seqs: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().expect("writer thread panicked"))
        .collect();

    hub.flush_sink(15_000);

    all_seqs.sort_unstable();
    let expected: Vec<u64> = (0..(WRITERS * PER_WRITER) as u64).collect();
    assert_eq!(
        all_seqs, expected,
        "concurrent emit produced a gap or duplicate seq -- the ring's per-graph \
         monotonic counter (cdc.rs) was supposed to make this impossible"
    );

    eprintln!(
        "cdc-kafka e2e (concurrent): emitted graph={graph} n={} seqs 0..{} -- read back with:\n\
         kubectl exec -n apps deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
         --bootstrap-server localhost:9092 --topic eg.cdc.{graph} --from-beginning \
         --timeout-ms 5000 | jq -s 'map(.seq) == (map(.seq) | sort)'",
        WRITERS * PER_WRITER,
        WRITERS * PER_WRITER
    );
}
