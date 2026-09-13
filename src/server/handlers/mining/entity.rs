use super::*;
use super::{input::*, writeback::*};
use eg_compute::mining::entity_resolution;
use eg_types::compute_result::mining::{EntityMatchRow, EntityResolutionMiningResult};
use eg_types::result_contract::compute as results;

// Entity resolution + record linkage follows the shared explicit-or-derived
// input → compute → optional typed-node/claim write-back contract.
pub(in crate::server::handlers) struct EntityResolutionRequest {
    pub(in crate::server::handlers) records: Vec<Vec<String>>,
    pub(in crate::server::handlers) block_keys: Vec<String>,
    pub(in crate::server::handlers) vectors: Vec<Vec<f64>>,
    pub(in crate::server::handlers) source: Option<VectorSource>,
    pub(in crate::server::handlers) ids: Vec<String>,
    pub(in crate::server::handlers) bucket_precision: i32,
    pub(in crate::server::handlers) threshold: f64,
    pub(in crate::server::handlers) writeback: WritebackOptions,
}

pub(in crate::server::handlers) fn handle_entity_resolve(
    req_id: u64,
    core: &GraphCore,
    request: EntityResolutionRequest,
) -> Response {
    let EntityResolutionRequest {
        records,
        block_keys,
        vectors,
        source,
        ids,
        bucket_precision,
        threshold,
        writeback,
    } = request;
    let batch = if !records.is_empty() {
        let keys = resolve_block_keys(&block_keys, records.len());
        let matches = entity_resolution::link_records(&records, &keys, threshold);
        EntityResolutionBatch {
            matches,
            ids: resolve_ids(&ids, records.len()),
            n_records: records.len(),
            method: "jaccard",
            #[cfg(feature = "epistemic")]
            provenance: "records:explicit".to_string(),
        }
    } else {
        let (rows, resolved_ids) = if !vectors.is_empty() {
            let n = vectors.len();
            (vectors, resolve_ids(&ids, n))
        } else {
            match &source {
                Some(spec) => gather_embeddings(core, spec),
                None => (Vec::new(), Vec::new()),
            }
        };
        if let Err(e) = validate_matrix(&rows) {
            return Response::err(req_id, e);
        }
        EntityResolutionBatch {
            matches: entity_resolution::resolve_entities(&rows, bucket_precision, threshold),
            ids: resolved_ids,
            n_records: rows.len(),
            method: "cosine",
            #[cfg(feature = "epistemic")]
            provenance: entity_provenance(&source),
        }
    };
    finish_entity_resolution(req_id, core, batch, writeback)
}

/// The common result of explicit record linkage and vector-based entity
/// resolution. Both input modes produce the same response and optional
/// node/claim writeback contract after their family-specific compute step.
pub(super) struct EntityResolutionBatch {
    matches: Vec<entity_resolution::EntityMatch>,
    ids: Vec<String>,
    n_records: usize,
    method: &'static str,
    #[cfg(feature = "epistemic")]
    provenance: String,
}

pub(super) fn finish_entity_resolution(
    req_id: u64,
    core: &GraphCore,
    batch: EntityResolutionBatch,
    writeback: WritebackOptions,
) -> Response {
    let written = if writeback.enabled {
        materialize_entity_matches(core, &batch.matches, &batch.ids, batch.method)
    } else {
        0
    };
    #[cfg(feature = "epistemic")]
    if writeback.enabled && writeback.as_claim {
        materialize_entity_match_claims(core, &batch.matches, &batch.ids, &batch.provenance);
    }
    entity_resolve_response(req_id, &batch.matches, &batch.ids, batch.n_records, written)
}

/// Resolve an id vector to exactly `n` entries: explicit ids win positionally;
/// a missing/short entry falls back to its index (stringified).
pub(super) fn resolve_ids(ids: &[String], n: usize) -> Vec<String> {
    (0..n)
        .map(|i| ids.get(i).cloned().unwrap_or_else(|| i.to_string()))
        .collect()
}

/// Resolve block keys to exactly `n` entries: a length mismatch (the caller
/// omitted them) degrades to ONE global block (empty key for every record).
pub(super) fn resolve_block_keys(block_keys: &[String], n: usize) -> Vec<String> {
    if block_keys.len() == n {
        block_keys.to_vec()
    } else {
        vec![String::new(); n]
    }
}

pub(super) fn entity_resolve_response(
    req_id: u64,
    matches: &[entity_resolution::EntityMatch],
    ids: &[String],
    n_records: usize,
    written: usize,
) -> Response {
    let rows: Vec<EntityMatchRow> = matches
        .iter()
        .map(|m| EntityMatchRow {
            left: ids
                .get(m.left)
                .cloned()
                .unwrap_or_else(|| m.left.to_string()),
            right: ids
                .get(m.right)
                .cloned()
                .unwrap_or_else(|| m.right.to_string()),
            similarity: m.similarity,
            block_key: m.block_key.clone(),
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::of::<results::MineEntityResolve>(EntityResolutionMiningResult {
            matches: rows,
            n_records,
            n_matches: matches.len(),
            written_back: written,
        }),
    )
}

/// Materialize each match as a typed `:EntityMatch` node (CONCEPT:EG-KG.mining.entity-resolution),
/// id = a deterministic digest of the CANONICALIZED (order-independent) member
/// pair. Linked to both members via `ENTITY_MATCH_MEMBER` edges when resident.
pub(super) fn materialize_entity_matches(
    core: &GraphCore,
    matches: &[entity_resolution::EntityMatch],
    ids: &[String],
    method: &str,
) -> usize {
    let mut written = 0usize;
    for m in matches {
        let left = ids
            .get(m.left)
            .cloned()
            .unwrap_or_else(|| m.left.to_string());
        let right = ids
            .get(m.right)
            .cloned()
            .unwrap_or_else(|| m.right.to_string());
        let node_id = entity_match_node_id(&left, &right);
        let props = serde_json::json!({
            "type": "EntityMatch",
            "left": left,
            "right": right,
            "similarity": m.similarity,
            "block_key": m.block_key,
            "method": method,
        });
        if !writeback_node(core, &node_id, &props) {
            continue;
        }
        for member in [&left, &right] {
            if core.has_node(member) {
                writeback_relationship(core, &node_id, member, "ENTITY_MATCH_MEMBER");
            }
        }
        written += 1;
    }
    written
}

pub(super) fn entity_match_node_id(left: &str, right: &str) -> String {
    use sha2::{Digest, Sha256};
    let (a, b) = if left <= right {
        (left, right)
    } else {
        (right, left)
    };
    let mut hasher = Sha256::new();
    hasher.update(a.as_bytes());
    hasher.update([0u8]);
    hasher.update(b.as_bytes());
    format!("entity_match:{}", hex::encode(&hasher.finalize()[..12]))
}

#[cfg(feature = "epistemic")]
pub(super) fn materialize_entity_match_claims(
    core: &GraphCore,
    matches: &[entity_resolution::EntityMatch],
    ids: &[String],
    provenance: &str,
) {
    for m in matches {
        let left = ids
            .get(m.left)
            .cloned()
            .unwrap_or_else(|| m.left.to_string());
        let right = ids
            .get(m.right)
            .cloned()
            .unwrap_or_else(|| m.right.to_string());
        let node_id = entity_match_node_id(&left, &right);
        materialize_claim(
            core,
            &node_id,
            "entity_resolution",
            m.similarity.clamp(0.0, 1.0),
            provenance,
        );
    }
}

#[cfg(feature = "epistemic")]
pub(super) fn entity_provenance(source: &Option<VectorSource>) -> String {
    match source {
        Some(s) => format!("vectors:{}", s.node_label),
        None => "vectors:explicit".to_string(),
    }
}
