// EG-UNIFIED-DATA-PLANE-R651.7: the enrichment write-set links every chunk and
// vector to its Document, replays an enriched digest and supersedes an old one.

use super::*;

fn request() -> EnrichDocumentRequest {
    EnrichDocumentRequest {
        document_id: "doc:1".into(),
        text_property: None,
        fetch_backend: None,
        max_chunk_bytes: Some(200),
        overlap_bytes: Some(40),
    }
}

fn document(text: &str, backend: &str) -> Vec<u8> {
    rmp_serde::to_vec_named(&json!({
        "type": "Document",
        "text": text,
        "url": "https://example.org/a",
        "fetch_backend": backend,
    }))
    .unwrap()
}

fn plan_for<'a>(
    held: &'a HeldDocument,
    spans: &'a [ChunkSpan],
    prior: Vec<PriorEnrichment>,
) -> EnrichmentPlan<'a> {
    EnrichmentPlan {
        document_id: "doc:1",
        document: held,
        config: chunking(&request()).unwrap(),
        spans,
        vectors: spans.iter().map(|_| vec![0.5, 0.5]).collect(),
        embedder: embedder::HASH,
        embedding_model: None,
        dimensions: 2,
        prior,
    }
}

// spec: EG-UNIFIED-DATA-PLANE-R651.7.2
#[test]
fn links_chunks_vectors_and_facts() {
    let text = "# Page\n\n".to_string() + &"A sentence about chunking. ".repeat(30);
    let held = held_document(&request(), &document(&text, "archivebox")).unwrap();
    assert_eq!(held.backend, Some(FetchBackend::Archivebox));
    let spans = chunks(&held, &chunking(&request()).unwrap()).unwrap();
    assert!(spans.len() > 2);
    let (write_set, receipt) = lower(plan_for(&held, &spans, Vec::new())).unwrap();
    assert_eq!(receipt.chunk_count as usize, spans.len());
    assert_eq!(receipt.vector_count, receipt.chunk_count);
    assert_eq!(receipt.embedder, "hash");
    let methods = write_set.methods();
    for id in &receipt.chunk_ids {
        assert!(methods.iter().any(|m| matches!(m,
            Method::AddEdge { source_id, target_id, .. } if source_id == id && target_id == "doc:1")));
        assert!(methods.iter().any(|m| matches!(m,
            Method::AddEmbedding { node_id, .. } if node_id == id)));
    }
    let Some(Method::AddNode {
        properties_msgpack, ..
    }) = methods
        .iter()
        .find(|m| matches!(m, Method::AddNode { node_id, .. } if node_id == &receipt.chunk_ids[0]))
    else {
        panic!("first chunk node");
    };
    let chunk = eg_types::msgpack::decode_property_value(properties_msgpack).unwrap();
    assert_eq!(chunk["fetch_backend"], "archivebox");
    assert_eq!(chunk["url"], "https://example.org/a");
    assert_eq!(chunk["heading_path"], "Page");
    let (start, end) = (
        chunk["start_char"].as_u64().unwrap() as usize,
        chunk["end_char"].as_u64().unwrap() as usize,
    );
    let cited: String = text.chars().skip(start).take(end - start).collect();
    assert_eq!(chunk["text"], cited);

    // The same digest replays: the record is found and nothing is rewritten.
    let record = rmp_serde::to_vec_named(&json!({
        "type": ENRICHMENT_TYPE, "document_id": "doc:1",
        "content_digest": held.content_digest(), "chunk_count": spans.len(),
        "vector_count": spans.len(), "embedder": "hash", "fetch_backend": "archivebox",
    }))
    .unwrap();
    let records = vec![(receipt.enrichment_id.clone(), record.clone())];
    let (current, prior) = prior_enrichments("doc:1", &held.digest_hex, records);
    let replay = replay_receipt("doc:1", &held.digest_hex, &current.expect("enriched"));
    assert!(prior.is_empty());
    assert!(replay.replayed);
    assert_eq!(replay.chunk_ids, receipt.chunk_ids);

    // A new digest supersedes: the old chunks and record are removed first.
    let changed = held_document(&request(), &document(&(text + " New."), "webarchiver")).unwrap();
    let records = vec![(receipt.enrichment_id.clone(), record)];
    let (current, prior) = prior_enrichments("doc:1", &changed.digest_hex, records);
    assert!(current.is_none());
    let new_spans = chunks(&changed, &chunking(&request()).unwrap()).unwrap();
    let (write_set, receipt2) = lower(plan_for(&changed, &new_spans, prior)).unwrap();
    assert_eq!(receipt2.superseded_chunks as usize, spans.len());
    for id in &receipt.chunk_ids {
        assert!(write_set
            .methods()
            .iter()
            .any(|m| matches!(m, Method::RemoveNode { node_id } if node_id == id)));
    }
    assert_ne!(receipt2.enrichment_id, receipt.enrichment_id);
}

// spec: EG-UNIFIED-DATA-PLANE-R651.7.4
#[test]
fn refuses_missing_text_and_unknown_backend() {
    let empty = rmp_serde::to_vec_named(&json!({"type": "Document", "text": "  "})).unwrap();
    assert!(held_document(&request(), &empty)
        .err()
        .unwrap()
        .contains("DOCUMENT_TEXT_MISSING"));
    let bad = document("text", "curl");
    assert!(held_document(&request(), &bad)
        .err()
        .unwrap()
        .contains("WEB_BACKEND_INVALID"));
    let mut tiny = request();
    tiny.max_chunk_bytes = Some(10);
    assert!(chunking(&tiny)
        .unwrap_err()
        .contains("CHUNK_CONFIG_INVALID"));
}

#[test]
fn retrieved_at_parses_numbers_and_rfc3339() {
    assert_eq!(
        retrieved_at_ms(&json!(1_700_000_000)),
        Some(1_700_000_000_000)
    );
    assert_eq!(
        retrieved_at_ms(&json!(1_700_000_000_123u64)),
        Some(1_700_000_000_123)
    );
    assert_eq!(
        retrieved_at_ms(&json!("1970-01-02T00:00:01Z")),
        Some(86_401_000)
    );
    assert_eq!(
        retrieved_at_ms(&json!("2023-11-14T22:13:20Z")),
        Some(1_700_000_000_000)
    );
    assert_eq!(retrieved_at_ms(&json!("yesterday")), None);
}
