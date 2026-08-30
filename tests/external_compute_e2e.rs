//! Current external-compute contract: signed `KnowledgeStream` pulls plus the
//! durable native `AnalyticsJob` state machine. There is one result protocol and
//! no auxiliary HTTP listener, registry, or result-publication path.

#![cfg(all(
    feature = "knowledge-batch",
    feature = "jobs",
    feature = "redb",
    feature = "security"
))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use arrow::ipc::reader::StreamReader;

use eg_types::jobs::{JobKind, JobOp, SubmitJobSpec};
use epistemic_graph::durability::DurabilityPolicy;
use epistemic_graph::knowledge_stream::{
    KnowledgeResultFamily, KnowledgeStreamBatchV1, KnowledgeStreamProjection, KnowledgeStreamQuery,
    KnowledgeStreamRequestV1, KNOWLEDGE_STREAM_SCHEMA_VERSION,
};
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;

const SECRET: &str = "external-compute-contract-secret";
const GRAPH: &str = "__commons__";

fn state(persist_dir: String) -> test_support::SharedState {
    let persistence: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(persist_dir.clone(), DurabilityPolicy::Each, 64)
            .expect("open authoritative persistence"),
    );
    test_support::state_with(
        SECRET,
        common::current_isolation(),
        Some(persist_dir),
        Some(persistence),
    )
}

fn request(id: u64, method: Method) -> epistemic_graph::protocol::Request {
    test_support::request(SECRET, id, GRAPH, method)
}

async fn dispatch(state: &test_support::SharedState, request: Request) -> Response {
    test_support::dispatch(state, request).await
}

fn success_json(response: Response) -> serde_json::Value {
    assert!(
        response.error.is_none(),
        "dispatch failed: {:?}",
        response.error
    );
    match response.result {
        Some(ResultPayload::Json(value)) => value,
        other => panic!("expected JSON response, received {other:?}"),
    }
}

fn success_batch(response: Response) -> KnowledgeStreamBatchV1 {
    assert!(
        response.error.is_none(),
        "KnowledgeStream failed: {:?}",
        response.error
    );
    match response.result {
        Some(ResultPayload::Raw(bytes)) => {
            rmp_serde::from_slice(&bytes).expect("decode KnowledgeStream batch")
        }
        other => panic!("expected raw KnowledgeStream batch, received {other:?}"),
    }
}

fn arrow_rows(payload: &[u8]) -> usize {
    StreamReader::try_new(Cursor::new(payload), None)
        .expect("open Arrow IPC stream")
        .map(|batch| batch.expect("decode Arrow record batch").num_rows())
        .sum()
}

fn stream(query: KnowledgeStreamQuery) -> Method {
    Method::KnowledgeStream {
        request: KnowledgeStreamRequestV1 {
            schema_version: KNOWLEDGE_STREAM_SCHEMA_VERSION,
            query,
            batch_size: 32,
            cursor: None,
            projection: KnowledgeStreamProjection::ArrowIpcV1,
        },
    }
}

#[test]
fn signed_knowledge_stream_and_native_analytics_publication_round_trip() {
    let driver = epistemic_graph::server::spawn_engine_driver(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(epistemic_graph::server::ENGINE_WORKER_STACK_BYTES)
            .enable_all()
            .build()
            .expect("build engine-contract runtime");
        runtime.block_on(async {
            let directory = tempfile::tempdir().expect("create private test persistence");
            let state = state(directory.path().to_string_lossy().into_owned());

            for (request_id, node_id) in [(1, "source-a"), (2, "source-b"), (3, "source-c")] {
                let response = Box::pin(dispatch(
                    &state,
                    request(
                        request_id,
                        Method::AddNode {
                            node_id: node_id.to_string(),
                            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                                "kind": "Source",
                                "confidence": 1.0
                            }))
                            .expect("encode node properties"),
                        },
                    ),
                ))
                .await;
                assert!(
                    response.error.is_none(),
                    "seed write failed: {:?}",
                    response.error
                );
            }

            let graph_batch = success_batch(
                Box::pin(dispatch(
                    &state,
                    request(
                        10,
                        stream(KnowledgeStreamQuery::Graph {
                            label: String::new(),
                            limit: 0,
                        }),
                    ),
                ))
                .await,
            );
            assert_eq!(graph_batch.family, KnowledgeResultFamily::Graph);
            assert_eq!(
                graph_batch.projection,
                KnowledgeStreamProjection::ArrowIpcV1
            );
            assert_eq!(arrow_rows(&graph_batch.payload), 3);
            assert!(graph_batch.cursor.exhausted);
            for reference in [
                &graph_batch.cursor.snapshot_ref,
                &graph_batch.cursor.query_ref,
                &graph_batch.cursor.integrity_ref,
            ] {
                assert!(reference.starts_with("eg:"));
            }

            let submitted = success_json(
                Box::pin(dispatch(
                    &state,
                    request(
                        20,
                        Method::AnalyticsJob {
                            op: JobOp::Submit(SubmitJobSpec {
                                graph: GRAPH.to_string(),
                                tenant: String::new(),
                                actor: String::new(),
                                purpose: "external-compute-contract".to_string(),
                                priority: 0,
                                deadline_unix_ms: None,
                                quota_cpu_ms: None,
                                memory_bytes: None,
                                io_bytes: None,
                                output_bytes: None,
                                worker_pool: String::new(),
                                worker_region: String::new(),
                                required_capabilities: Vec::new(),
                                max_attempts: 1,
                                backoff_ms: 0,
                                kind: JobKind::MineAssociate {
                                    transactions: vec![
                                        vec!["item-a".to_string(), "item-b".to_string()],
                                        vec!["item-a".to_string(), "item-b".to_string()],
                                        vec!["item-a".to_string(), "item-c".to_string()],
                                    ],
                                    min_support: 0.3,
                                    min_confidence: 0.5,
                                    algorithm: "fpgrowth".to_string(),
                                },
                            }),
                        },
                    ),
                ))
                .await,
            );
            let job_id = submitted
                .get("job_id")
                .and_then(serde_json::Value::as_str)
                .expect("submitted job id")
                .to_string();

            let mut succeeded = false;
            for request_id in 30..130 {
                let status = success_json(
                    Box::pin(dispatch(
                        &state,
                        request(
                            request_id,
                            Method::AnalyticsJob {
                                op: JobOp::Status {
                                    job_id: job_id.clone(),
                                },
                            },
                        ),
                    ))
                    .await,
                );
                let state_value = status.get("state").expect("job state");
                if state_value.get("Succeeded").is_some() {
                    assert!(status.get("output").is_some_and(|value| !value.is_null()));
                    succeeded = true;
                    break;
                }
                assert!(
                    state_value.get("Failed").is_none() && state_value.get("Cancelled").is_none(),
                    "native analytics job terminated unsuccessfully: {status}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            assert!(
                succeeded,
                "native analytics job did not publish before timeout"
            );

            let result_batch = success_batch(
                Box::pin(dispatch(
                    &state,
                    request(
                        140,
                        stream(KnowledgeStreamQuery::Job {
                            job_id: job_id.clone(),
                        }),
                    ),
                ))
                .await,
            );
            assert_eq!(result_batch.family, KnowledgeResultFamily::Job);
            assert!(result_batch.cursor.exhausted);
            assert!(arrow_rows(&result_batch.payload) > 0);
        });
    })
    .expect("start engine-contract driver");
    epistemic_graph::server::join_engine_driver(driver).expect("engine-contract driver failed");
}
