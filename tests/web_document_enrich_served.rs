//! EG-UNIFIED-DATA-PLANE-R651.7 and EG-DECISION-ENGINE-R802 through the real
//! `dispatch` shell over a redb-backed `ServerState`.
//!
//! A held web Document is enriched: its chunks, `chunkOf` edges and vectors
//! commit through the ChangeEnvelope authority, `SemanticSearch` finds a
//! chunk by its own vector, a re-run replays without advancing the graph, a
//! new text supersedes the earlier chunks, and both fetch backends store the
//! same chunk set. `SelectWebPages` then dedupes against the held pages.

#![cfg(all(
    feature = "redb",
    feature = "query",
    feature = "modality-serving",
    feature = "decide"
))]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use eg_plan::TextEmbedder;
use eg_types::decision::web_fetch_selection::{
    SelectWebPagesAnswer, SelectWebPagesRequest, SkipReason, WebCandidate,
};
use eg_types::source_ingestion::web_document::{
    EnrichDocumentReceipt, EnrichDocumentRequest, FetchBackend,
};
use epistemic_graph::protocol::{GraphType, Method, Response, ResultPayload};

const SECRET: &str = "web-document-enrich-secret";
const GRAPH: &str = "webenrich";
const NOW: u64 = 1_800_000_000_000;
const HOUR: u64 = 3_600_000;

async fn call(state: &test_support::SharedState, id: u64, method: Method) -> Response {
    let response =
        test_support::dispatch(state, test_support::request(SECRET, id, GRAPH, method)).await;
    assert!(
        response.error.is_none(),
        "request {id} failed: {:?}",
        response.error
    );
    response
}

fn raw<T: serde::de::DeserializeOwned>(response: Response) -> T {
    match response.result {
        Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).expect("typed result"),
        other => panic!("unexpected payload {other:?}"),
    }
}

fn page_text(extra: &str) -> String {
    let mut text = String::from("# Web ingest\n\nPages enter as Documents.\n\n");
    for section in 0..6 {
        text.push_str(&format!("## Section {section}\n\n"));
        text.push_str(&"Chunks carry offsets so a citation can quote the source. ".repeat(12));
        text.push_str("\n\n");
    }
    text.push_str(extra);
    text
}

async fn add_document(
    state: &test_support::SharedState,
    id: u64,
    node_id: &str,
    text: &str,
    url: &str,
    retrieved_at_ms: u64,
    digest: &str,
) {
    let properties = serde_json::json!({
        "type": "Document",
        "title": "Web ingest",
        "text": text,
        "url": url,
        "retrieved_at": retrieved_at_ms,
        "content_digest": digest,
    });
    call(
        state,
        id,
        Method::AddNode {
            node_id: node_id.into(),
            properties_msgpack: rmp_serde::to_vec_named(&properties).unwrap(),
        },
    )
    .await;
}

async fn enrich(
    state: &test_support::SharedState,
    id: u64,
    node_id: &str,
    backend: FetchBackend,
) -> EnrichDocumentReceipt {
    let request = EnrichDocumentRequest {
        document_id: node_id.into(),
        text_property: None,
        fetch_backend: Some(backend),
        max_chunk_bytes: Some(600),
        overlap_bytes: Some(80),
    };
    raw(call(
        state,
        id,
        Method::EnrichDocument {
            request: Box::new(request),
        },
    )
    .await)
}

async fn bool_call(state: &test_support::SharedState, id: u64, method: Method) -> bool {
    match call(state, id, method).await.result {
        Some(ResultPayload::Bool(value)) => value,
        other => panic!("unexpected payload {other:?}"),
    }
}

async fn chunk_props(state: &test_support::SharedState, id: u64, node: &str) -> serde_json::Value {
    let response = call(
        state,
        id,
        Method::GetNodeProperties {
            node_id: node.into(),
        },
    )
    .await;
    match response.result {
        Some(ResultPayload::Raw(bytes)) => {
            eg_types::msgpack::decode_property_value(&bytes).unwrap()
        }
        Some(ResultPayload::Json(value)) => value,
        other => panic!("unexpected payload {other:?}"),
    }
}

fn graph_version(state: &test_support::SharedState) -> u64 {
    state
        .try_read()
        .unwrap()
        .registry
        .get(GRAPH)
        .unwrap()
        .core
        .version()
}

fn on_engine<F: std::future::Future<Output = ()> + 'static>(test: fn() -> F) {
    let driver = epistemic_graph::server::spawn_engine_driver(move || {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_stack_size(epistemic_graph::server::ENGINE_WORKER_STACK_BYTES)
            .enable_all()
            .build()
            .expect("build engine runtime")
            .block_on(test());
    })
    .expect("spawn engine driver");
    epistemic_graph::server::join_engine_driver(driver).expect("engine test body failed");
}

async fn durable_graph() -> (
    std::path::PathBuf,
    test_support::SharedPersistence,
    test_support::SharedState,
) {
    test_support::provision_encryption_key_once("web-document-enrich-encryption-key");
    let dir = test_support::fresh_dir("eg-webenrich");
    let dir_s = dir.to_string_lossy().to_string();
    let (backend, state) = test_support::redb_state_at(SECRET, common::current_isolation(), &dir_s);
    call(
        &state,
        1,
        Method::CreateGraph {
            graph_name: GRAPH.into(),
            graph_type: GraphType::Global,
        },
    )
    .await;
    (dir, backend, state)
}

// spec: EG-UNIFIED-DATA-PLANE-R651.7
// spec: EG-UNIFIED-DATA-PLANE-R651.7.3
// spec: EG-UNIFIED-DATA-PLANE-R651.7.5
// spec: EG-UNIFIED-DATA-PLANE-R651.7.6
// spec: EG-DECISION-ENGINE-R802
// spec: EG-DECISION-ENGINE-R802.3
// spec: EG-DECISION-ENGINE-R802.6
#[test]
fn test_web_page_enriches_once_for_both_backends() {
    on_engine(enrich_replay_supersede_and_search);
}

async fn enrich_replay_supersede_and_search() {
    let (_dir, _backend, state) = durable_graph().await;
    let text = page_text("");
    add_document(
        &state,
        2,
        "doc:a",
        &text,
        "https://example.org/a",
        NOW - 48 * HOUR,
        "sha256:aa",
    )
    .await;
    add_document(
        &state,
        3,
        "doc:b",
        &text,
        "https://example.org/b",
        NOW - HOUR,
        "sha256:bb",
    )
    .await;

    let a = enrich(&state, 4, "doc:a", FetchBackend::Webarchiver).await;
    assert!(!a.replayed);
    assert!(
        a.chunk_count > 3,
        "a multi-section page chunks into several windows"
    );
    assert_eq!(a.vector_count, a.chunk_count);
    assert_eq!(
        a.embedder, "hash",
        "no endpoint configured: the hash fallback is recorded"
    );
    assert_eq!(a.fetch_backend, Some(FetchBackend::Webarchiver));
    for (id, chunk) in a.chunk_ids.iter().enumerate() {
        assert!(
            bool_call(
                &state,
                10 + id as u64,
                Method::HasEdge {
                    source_id: chunk.clone(),
                    target_id: "doc:a".into()
                }
            )
            .await
        );
    }
    let first = chunk_props(&state, 40, &a.chunk_ids[1]).await;
    assert_eq!(first["fetch_backend"], "webarchiver");
    assert_eq!(first["embedder"], "hash");
    assert_eq!(first["url"], "https://example.org/a");
    let (start, end) = (
        first["start_char"].as_u64().unwrap() as usize,
        first["end_char"].as_u64().unwrap() as usize,
    );
    let cited: String = text.chars().skip(start).take(end - start).collect();
    assert_eq!(first["text"], cited, "offsets address the exact passage");

    // The vector path: SemanticSearch finds the chunk by its own vector.
    let query = eg_plan::HashEmbedder::new(a.dimensions as usize)
        .embed(&cited)
        .unwrap();
    let hits: Vec<(String, f32)> = raw(call(
        &state,
        41,
        Method::SemanticSearch {
            query_embedding: query,
            n_results: 3,
        },
    )
    .await);
    assert_eq!(
        hits.first().map(|h| h.0.as_str()),
        Some(a.chunk_ids[1].as_str())
    );

    // Idempotent by content digest: nothing is written again.
    let version = graph_version(&state);
    let again = enrich(&state, 42, "doc:a", FetchBackend::Webarchiver).await;
    assert!(again.replayed);
    assert_eq!(again.chunk_ids, a.chunk_ids);
    assert_eq!(
        graph_version(&state),
        version,
        "an enriched digest must not commit twice"
    );

    // Both backends store the same chunk set; only the recorded backend differs.
    let b = enrich(&state, 43, "doc:b", FetchBackend::Archivebox).await;
    assert_eq!(b.chunk_count, a.chunk_count);
    assert_eq!(b.content_digest, a.content_digest);
    let b_first = chunk_props(&state, 44, &b.chunk_ids[1]).await;
    assert_eq!(b_first["fetch_backend"], "archivebox");
    for key in ["text", "start_char", "end_char", "heading_path"] {
        assert_eq!(
            b_first[key], first[key],
            "{key} must not depend on the backend"
        );
    }

    // A new text supersedes: the earlier digest's chunks are removed.
    add_document(
        &state,
        45,
        "doc:a",
        &page_text("A new closing paragraph."),
        "https://example.org/a",
        NOW,
        "sha256:cc",
    )
    .await;
    let changed = enrich(&state, 46, "doc:a", FetchBackend::Webarchiver).await;
    assert!(!changed.replayed);
    assert_eq!(changed.superseded_chunks, a.chunk_count);
    assert!(
        !bool_call(
            &state,
            47,
            Method::HasNode {
                node_id: a.chunk_ids[0].clone()
            }
        )
        .await
    );
    assert!(
        bool_call(
            &state,
            48,
            Method::HasNode {
                node_id: changed.chunk_ids[0].clone()
            }
        )
        .await
    );

    // Selection over the held pages.
    let row = |url: &str, rank: u32, digest: Option<&str>| WebCandidate {
        url: url.into(),
        title: "Web ingest offsets".into(),
        snippet: "citation offsets".into(),
        engine: "searxng".into(),
        rank,
        content_digest: digest.map(str::to_string),
        published_at_ms: None,
    };
    let request = SelectWebPagesRequest {
        query: "web ingest citation offsets".into(),
        candidates: vec![
            row("https://example.org/b", 1, None),
            row("https://example.org/new", 2, None),
            row("https://example.org/copy", 3, Some("sha256:cc")),
            row("https://example.org/new?utm_source=x", 4, None),
        ],
        max_fetches: 4,
        freshness_window_s: Some(6 * 3_600),
        now_ms: Some(NOW + HOUR),
    };
    let answer: SelectWebPagesAnswer = raw(call(
        &state,
        49,
        Method::SelectWebPages {
            request: Box::new(request),
        },
    )
    .await);
    let reason = |url: &str| {
        answer
            .skipped
            .iter()
            .find(|s| s.url == url)
            .map(|s| s.reason)
    };
    assert_eq!(reason("https://example.org/b"), Some(SkipReason::HeldFresh));
    assert_eq!(
        reason("https://example.org/copy"),
        Some(SkipReason::DuplicateDigest)
    );
    assert_eq!(
        reason("https://example.org/new?utm_source=x"),
        Some(SkipReason::DuplicateUrl)
    );
    assert_eq!(answer.selected.len(), 1);
    assert_eq!(answer.selected[0].canonical_url, "https://example.org/new");
}

// spec: EG-UNIFIED-DATA-PLANE-R651.7.4
#[test]
fn test_web_document_refusals() {
    on_engine(refusals);
}

async fn refusals() {
    let (_dir, _backend, state) = durable_graph().await;
    let request = |id: &str| EnrichDocumentRequest {
        document_id: id.into(),
        text_property: None,
        fetch_backend: None,
        max_chunk_bytes: None,
        overlap_bytes: None,
    };
    let missing = test_support::dispatch(
        &state,
        test_support::request(
            SECRET,
            2,
            GRAPH,
            Method::EnrichDocument {
                request: Box::new(request("doc:none")),
            },
        ),
    )
    .await;
    assert!(format!("{:?}", missing.error).contains("DOCUMENT_NOT_FOUND"));
    let properties = serde_json::json!({"type": "Document", "url": "https://example.org/x"});
    call(
        &state,
        3,
        Method::AddNode {
            node_id: "doc:empty".into(),
            properties_msgpack: rmp_serde::to_vec_named(&properties).unwrap(),
        },
    )
    .await;
    let version = graph_version(&state);
    let empty = test_support::dispatch(
        &state,
        test_support::request(
            SECRET,
            4,
            GRAPH,
            Method::EnrichDocument {
                request: Box::new(request("doc:empty")),
            },
        ),
    )
    .await;
    assert!(format!("{:?}", empty.error).contains("DOCUMENT_TEXT_MISSING"));
    assert_eq!(graph_version(&state), version, "a refusal writes nothing");
}
