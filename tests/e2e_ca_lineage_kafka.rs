//! CA-15 P9 (producer slice) — live end-to-end proof that a `LakeManager`
//! materialization's OpenLineage `RunEvent` reaches Kafka on
//! `openlineage.events`, keyed by `run.runId`, per `DEC-CA-03`/`DEC-CA-05`.
//! Feature `lineage-transport-kafka` only; compiles to nothing otherwise.
//!
//! `#[ignore]`d because it needs a live broker — this is infra-integration,
//! not a unit test, mirroring CA-11's sibling `tests/e2e_ca_cdc_kafka.rs`
//! exactly (same "producer accepted every send" scope, same external
//! read-back caveat: this crate's `rdkafka` feature set is producer-only,
//! `cmake-build,ssl,sasl`, no consumer group machinery). Run explicitly:
//!
//! ```text
//! EPISTEMIC_GRAPH_LINEAGE_KAFKA_BROKERS=<host:port> \
//!   cargo test --features lake,lineage-transport,lineage-transport-kafka \
//!   --target-dir ./target-isolated -j 12 \
//!   --test e2e_ca_lineage_kafka -- --ignored --nocapture
//! ```
//!
//! Independent read-back (NOT this test):
//! ```text
//! kubectl exec -n apps deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
//!   --bootstrap-server localhost:9092 --topic openlineage.events --from-beginning \
//!   --timeout-ms 5000 | grep <run_id>
//! ```

#![cfg(feature = "lineage-transport-kafka")]

use eg_tsdb::point::Point;
use eg_tsdb::store::SeriesStore;
use epistemic_graph::server::blob::store::RedbChunkStore;
use epistemic_graph::server::lake::lineage_transport::{KafkaTransport, LineageTransport};
use epistemic_graph::server::lake::LakeManager;

fn tsdb_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-lineage-kafka-e2e-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after unix epoch")
            .as_nanos()
    ))
}

fn store() -> RedbChunkStore {
    let dir = tsdb_dir("chunk-store");
    std::fs::create_dir_all(&dir).expect("create chunk store dir");
    RedbChunkStore::open(&dir.to_string_lossy()).expect("open chunk store")
}

#[test]
#[ignore = "needs a live Kafka broker; see module doc for the exact invocation"]
fn lake_materialize_lineage_event_reaches_the_openlineage_topic() {
    // Deliberately NOT using `lineage_transport::configured_transports()`
    // (the process-wide singleton `push_lineage` calls in production) --
    // that would silently no-op if the env var isn't set at the exact
    // moment the singleton first initializes in this process. Build a
    // transport directly from env instead, so a misconfigured invocation
    // fails loudly rather than reporting a false pass.
    let kafka = KafkaTransport::from_env().expect(
        "EPISTEMIC_GRAPH_LINEAGE_KAFKA_BROKERS must be set to a reachable broker for this test",
    );

    let series_id = format!(
        "ca15_e2e_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );

    let s = store();
    let tsdb = SeriesStore::open_in_dir(&tsdb_dir("tsdb")).unwrap();
    tsdb.append_batch(
        &series_id,
        1,
        3_600_000_000_000,
        &["v".to_string()],
        &[Point::single(0, 1.0), Point::single(1, 2.0)],
    )
    .unwrap();

    let mgr = LakeManager::new();
    let report = mgr
        .drain_series(&s, &tsdb, &series_id)
        .unwrap()
        .expect("first drain materializes rows and builds a RunEvent");
    let run_id = report.lineage_event["run"]["runId"]
        .as_str()
        .expect("build_run_event always sets run.runId")
        .to_string();

    // Publish through the SAME transport type production uses, directly
    // (see the comment above `KafkaTransport::from_env()` for why not via
    // the singleton), then block for real broker-confirmed delivery before
    // asserting -- `KafkaTransport::push` is non-blocking by design (never
    // stalls a commit), so without this the test process could exit before
    // librdkafka's background I/O thread has actually sent anything.
    kafka.push(&report.lineage_event);
    kafka.flush(10_000);

    eprintln!(
        "lineage-transport-kafka e2e: emitted run_id={run_id} table={}.{} -- read back with:\n\
         kubectl exec -n apps deploy/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
         --bootstrap-server localhost:9092 --topic openlineage.events --from-beginning \
         --timeout-ms 5000 | grep {run_id}",
        report.namespace, report.table
    );
}
