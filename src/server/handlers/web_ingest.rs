//! Web page ingest: Document enrichment (EG-UNIFIED-DATA-PLANE-R651.7) and
//! page selection (EG-DECISION-ENGINE-R802).
//!
//! Enrichment chunks one held Document with `eg_document::processing`,
//! embeds the chunks, and lowers chunk nodes, `chunkOf` edges, vectors and one
//! `DocumentEnrichment` record into ONE write-set that commits through the
//! same ChangeEnvelope authority as `IndexRepository` -- no second graph
//! writer. The record id is keyed by the Document and its content digest, so
//! an enriched digest replays without writing, and a new digest removes the
//! earlier digest's chunks in the same commit.

use std::collections::{BTreeMap, BTreeSet};

use eg_document::processing::{chunk_text, ChunkSpan, ChunkingConfig, CHUNKER_VERSION};
use eg_types::source_ingestion::web_document::{
    EnrichDocumentReceipt, EnrichDocumentRequest, FetchBackend,
};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use super::repository_index::{stored_value, EnvelopeLabels, IndexWriteSet};
use crate::protocol::Method;

pub(crate) mod embedder;

pub(crate) const ENRICHMENT_TYPE: &str = "DocumentEnrichment";
pub(crate) const CHUNK_TYPE: &str = "DocumentChunk";
pub(crate) const CHUNK_OF: &str = "chunkOf";
pub(crate) const HAS_ENRICHMENT: &str = "hasEnrichment";
const ENVELOPE_PREFIX: &str = "document-enrichment:";
pub(crate) const LABELS: EnvelopeLabels = EnvelopeLabels {
    version_type: "document-enrichment-sha256",
    policy_version: "document-enrichment",
};
/// Document properties copied onto every chunk so a citation needs no join.
const CITATION_FIELDS: &[&str] = &["url", "canonical_url", "retrieved_at", "title"];

/// What enrichment read from the held Document.
pub(crate) struct HeldDocument {
    pub text: String,
    pub digest_hex: String,
    pub backend: Option<FetchBackend>,
    pub citation: Map<String, Value>,
}

impl HeldDocument {
    pub(crate) fn content_digest(&self) -> String {
        format!("sha256:{}", self.digest_hex)
    }
}

/// Read the text, digest, backend and citation fields of a Document's stored
/// properties. Refuses a Document with no text or an unknown backend.
pub(crate) fn held_document(
    request: &EnrichDocumentRequest,
    properties: &[u8],
) -> Result<HeldDocument, String> {
    let value = eg_types::msgpack::decode_property_value(properties)
        .map_err(|_| "DOCUMENT_PROPERTIES_INVALID: the Document properties do not decode")?;
    let candidates: Vec<&str> = match request.text_property.as_deref() {
        Some(name) => vec![name],
        None => vec!["text", "content", "body"],
    };
    let text = candidates
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
        .filter(|text| !text.trim().is_empty())
        .ok_or("DOCUMENT_TEXT_MISSING: the Document holds no text to enrich")?
        .to_string();
    let stored = ["fetch_backend", "backend"]
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str));
    let backend = match (request.fetch_backend, stored) {
        (Some(backend), _) => Some(backend),
        (None, Some(name)) => Some(FetchBackend::parse(name).ok_or_else(|| {
            format!("WEB_BACKEND_INVALID: '{name}' is not webarchiver or archivebox")
        })?),
        (None, None) => None,
    };
    let citation = CITATION_FIELDS
        .iter()
        .filter_map(|name| Some((name.to_string(), value.get(*name)?.clone())))
        .collect();
    Ok(HeldDocument {
        digest_hex: hex::encode(Sha256::digest(text.as_bytes())),
        text,
        backend,
        citation,
    })
}

/// The chunking config a request asks for.
pub(crate) fn chunking(request: &EnrichDocumentRequest) -> Result<ChunkingConfig, String> {
    let default = ChunkingConfig::default();
    let config = ChunkingConfig {
        max_bytes: request
            .max_chunk_bytes
            .map_or(default.max_bytes, |b| b as usize),
        overlap_bytes: request
            .overlap_bytes
            .map_or(default.overlap_bytes, |b| b as usize),
    };
    config.validate()?;
    Ok(config)
}

pub(crate) fn enrichment_id(document_id: &str, digest_hex: &str) -> String {
    format!("enrichment:{document_id}:{}", &digest_hex[..32])
}

fn chunk_id(document_id: &str, digest_hex: &str, ordinal: u32) -> String {
    format!("chunk:{document_id}:{}:{ordinal}", &digest_hex[..16])
}

/// An earlier enrichment of the same Document under another digest.
pub(crate) struct PriorEnrichment {
    pub enrichment_id: String,
    pub chunk_ids: Vec<String>,
}

/// Find the Document's enrichment records among `records` (id, properties).
/// Returns the one for `digest_hex`, if any, and every other one.
pub(crate) fn prior_enrichments(
    document_id: &str,
    digest_hex: &str,
    records: Vec<(String, Vec<u8>)>,
) -> (Option<Value>, Vec<PriorEnrichment>) {
    let mut current = None;
    let mut prior = Vec::new();
    for (id, bytes) in records {
        let Ok(value) = eg_types::msgpack::decode_property_value(&bytes) else {
            continue;
        };
        if value["document_id"].as_str() != Some(document_id) {
            continue;
        }
        let Some(old_hex) = value["content_digest"]
            .as_str()
            .and_then(|d| d.strip_prefix("sha256:"))
        else {
            continue;
        };
        if old_hex == digest_hex {
            current = Some(value);
            continue;
        }
        let count = value["chunk_count"].as_u64().unwrap_or(0) as u32;
        prior.push(PriorEnrichment {
            enrichment_id: id,
            chunk_ids: (0..count)
                .map(|i| chunk_id(document_id, old_hex, i))
                .collect(),
        });
    }
    (current, prior)
}

/// The receipt of an already-enriched digest, read back from its record.
pub(crate) fn replay_receipt(
    document_id: &str,
    digest_hex: &str,
    record: &Value,
) -> EnrichDocumentReceipt {
    let count = record["chunk_count"].as_u64().unwrap_or(0) as u32;
    EnrichDocumentReceipt {
        document_id: document_id.to_string(),
        enrichment_id: enrichment_id(document_id, digest_hex),
        content_digest: format!("sha256:{digest_hex}"),
        fetch_backend: record["fetch_backend"]
            .as_str()
            .and_then(FetchBackend::parse),
        embedder: record["embedder"].as_str().unwrap_or_default().to_string(),
        embedding_model: record["embedding_model"].as_str().map(str::to_string),
        dimensions: record["dimensions"].as_u64().unwrap_or(0) as u32,
        chunker: record["chunker"].as_str().unwrap_or_default().to_string(),
        chunk_count: count,
        vector_count: record["vector_count"].as_u64().unwrap_or(0) as u32,
        superseded_chunks: 0,
        replayed: true,
        chunk_ids: (0..count)
            .map(|i| chunk_id(document_id, digest_hex, i))
            .collect(),
    }
}

/// Everything one enrichment writes.
pub(crate) struct EnrichmentPlan<'a> {
    pub document_id: &'a str,
    pub document: &'a HeldDocument,
    pub config: ChunkingConfig,
    pub spans: &'a [ChunkSpan],
    pub vectors: Vec<Vec<f32>>,
    pub embedder: &'static str,
    pub embedding_model: Option<String>,
    pub dimensions: usize,
    pub prior: Vec<PriorEnrichment>,
}

fn encode(value: &Value) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value).map_err(|error| format!("enrichment encoding failed: {error}"))
}

fn node(id: String, properties: Value) -> Result<Method, String> {
    Ok(Method::AddNode {
        node_id: id,
        properties_msgpack: encode(&properties)?,
    })
}

fn edge(source: &str, target: &str, relationship: &str) -> Result<Method, String> {
    Ok(Method::AddEdge {
        source_id: source.to_string(),
        target_id: target.to_string(),
        properties_msgpack: encode(&json!({ "relationship": relationship }))?,
    })
}

/// The chunk node's properties: offsets, section, text and provenance.
fn chunk_properties(plan: &EnrichmentPlan<'_>, span: &ChunkSpan) -> Value {
    let heading = span.heading_path.join(" > ");
    let name = match (heading.is_empty(), plan.document.citation.get("title")) {
        (false, _) => format!("{heading} #{}", span.ordinal),
        (true, Some(Value::String(title))) => format!("{title} #{}", span.ordinal),
        _ => format!("{} #{}", plan.document_id, span.ordinal),
    };
    let mut properties = json!({
        "type": CHUNK_TYPE,
        "name": stored_value(&name),
        "text": stored_value(span.text(&plan.document.text)),
        "document_id": plan.document_id,
        "ordinal": span.ordinal,
        "heading_path": stored_value(&heading),
        "start_char": span.start_char,
        "end_char": span.end_char,
        "start_byte": span.start_byte,
        "end_byte": span.end_byte,
        "content_digest": plan.document.content_digest(),
        "embedder": plan.embedder,
        "fetch_backend": plan.document.backend.map(FetchBackend::as_str),
    });
    for (key, value) in &plan.document.citation {
        let value = match value {
            Value::String(text) => Value::String(stored_value(text).to_string()),
            other => other.clone(),
        };
        properties[key] = value;
    }
    properties
}

/// Lower a plan to its digest-keyed write-set and its receipt.
pub(crate) fn lower(
    plan: EnrichmentPlan<'_>,
) -> Result<(IndexWriteSet, EnrichDocumentReceipt), String> {
    let digest_hex = &plan.document.digest_hex;
    let record_id = enrichment_id(plan.document_id, digest_hex);
    let mut methods = Vec::new();
    let mut superseded = 0;
    for prior in &plan.prior {
        for id in &prior.chunk_ids {
            methods.push(Method::RemoveNode {
                node_id: id.clone(),
            });
            superseded += 1;
        }
        methods.push(Method::RemoveNode {
            node_id: prior.enrichment_id.clone(),
        });
    }
    methods.push(node(
        record_id.clone(),
        json!({
            "type": ENRICHMENT_TYPE,
            "document_id": plan.document_id,
            "content_digest": plan.document.content_digest(),
            "fetch_backend": plan.document.backend.map(FetchBackend::as_str),
            "embedder": plan.embedder,
            "embedding_model": plan.embedding_model,
            "dimensions": plan.dimensions,
            "chunker": CHUNKER_VERSION,
            "max_chunk_bytes": plan.config.max_bytes,
            "overlap_bytes": plan.config.overlap_bytes,
            "chunk_count": plan.spans.len(),
            "vector_count": plan.vectors.len(),
        }),
    )?);
    methods.push(edge(plan.document_id, &record_id, HAS_ENRICHMENT)?);
    let mut chunk_ids = Vec::with_capacity(plan.spans.len());
    for (span, vector) in plan.spans.iter().zip(&plan.vectors) {
        let id = chunk_id(plan.document_id, digest_hex, span.ordinal);
        methods.push(node(id.clone(), chunk_properties(&plan, span))?);
        methods.push(edge(&id, plan.document_id, CHUNK_OF)?);
        methods.push(Method::AddEmbedding {
            node_id: id.clone(),
            embedding: vector.clone(),
        });
        chunk_ids.push(id);
    }
    let write_set =
        IndexWriteSet::from_methods(ENVELOPE_PREFIX, b"eg/document-enrichment", methods)?;
    let receipt = EnrichDocumentReceipt {
        document_id: plan.document_id.to_string(),
        enrichment_id: record_id,
        content_digest: plan.document.content_digest(),
        fetch_backend: plan.document.backend,
        embedder: plan.embedder.to_string(),
        embedding_model: plan.embedding_model,
        dimensions: plan.dimensions as u32,
        chunker: CHUNKER_VERSION.to_string(),
        chunk_count: plan.spans.len() as u32,
        vector_count: plan.vectors.len() as u32,
        superseded_chunks: superseded,
        replayed: false,
        chunk_ids,
    };
    Ok((write_set, receipt))
}

/// Chunk a held Document's text under `config`.
pub(crate) fn chunks(
    document: &HeldDocument,
    config: &ChunkingConfig,
) -> Result<Vec<ChunkSpan>, String> {
    chunk_text(&document.text, config)
}

// ── Page selection (EG-DECISION-ENGINE-R802) ────────────────────────────────

/// Unix milliseconds of a stored `retrieved_at`: a number (seconds below
/// 10^11, else milliseconds) or an RFC 3339 UTC timestamp.
pub(crate) fn retrieved_at_ms(value: &Value) -> Option<u64> {
    if let Some(number) = value.as_u64() {
        return Some(if number < 100_000_000_000 {
            number * 1_000
        } else {
            number
        });
    }
    let text = value.as_str()?;
    let date: Vec<i64> = text
        .get(..10)?
        .split('-')
        .map(|p| p.parse().ok())
        .collect::<Option<_>>()?;
    let time: Vec<i64> = text
        .get(11..19)?
        .split(':')
        .map(|p| p.parse().ok())
        .collect::<Option<_>>()?;
    if date.len() != 3 || time.len() != 3 {
        return None;
    }
    let (y, m, d) = (date[0] - i64::from(date[1] <= 2), date[1], date[2]);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    let days = era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468;
    let seconds = days * 86_400 + time[0] * 3_600 + time[1] * 60 + time[2];
    u64::try_from(seconds).ok().map(|s| s * 1_000)
}

/// The request graph's held web pages, keyed by canonical URL, and every
/// content digest it holds. `documents` are RLS-visible `Document` rows.
#[cfg(feature = "decide")]
pub(crate) fn held_pages(
    documents: Vec<(String, Vec<u8>)>,
) -> (
    BTreeMap<String, eg_numeric::decision::web_fetch_selection::HeldPage>,
    BTreeSet<String>,
) {
    use eg_numeric::decision::web_fetch_selection::{canonical_url, HeldPage};
    let mut held = BTreeMap::new();
    let mut digests = BTreeSet::new();
    for (_, bytes) in documents {
        let Ok(value) = eg_types::msgpack::decode_property_value(&bytes) else {
            continue;
        };
        let digest = value["content_digest"].as_str().map(str::to_string);
        if let Some(digest) = &digest {
            digests.insert(digest.clone());
        }
        let url = value["canonical_url"]
            .as_str()
            .or_else(|| value["url"].as_str());
        let Some(canonical) = url.and_then(canonical_url) else {
            continue;
        };
        let page = HeldPage {
            retrieved_at_ms: retrieved_at_ms(&value["retrieved_at"])
                .or_else(|| value["retrieved_at_ms"].as_u64()),
            content_digest: digest,
        };
        // Keep the newest copy of a URL.
        let newer = held
            .get(&canonical)
            .is_none_or(|old: &HeldPage| old.retrieved_at_ms < page.retrieved_at_ms);
        if newer {
            held.insert(canonical, page);
        }
    }
    (held, digests)
}

pub(crate) fn query_digest(query: &str) -> String {
    format!(
        "sha256:{}",
        hex::encode(Sha256::digest(query.trim().as_bytes()))
    )
}

#[cfg(test)]
#[path = "web_ingest_tests.rs"]
mod tests;
