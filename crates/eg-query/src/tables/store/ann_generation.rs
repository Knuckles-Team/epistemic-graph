//! Durable, derived ANN generations in the SQL owner's closed table registry.
//!
//! Generation zero is the live pointer. Positive generations contain immutable,
//! bounded artifact chunks. All rows are written through the SQL mutation
//! authority in one commit; the source epoch is never advanced by this cache.

use std::collections::{BTreeMap, BTreeSet};

use eg_storage::SQL_ANN_GENERATIONS;
use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{
    authority::next_source_epoch_in, decode_stored, get_schema_in, get_schema_read, map_err,
    SqlWrite, TableStore, ANN_INDEXES,
};
use crate::sql::{AnnIndexPlan, AnnMethod, VectorMetric};
use crate::tables::ann_authority::{AnnGeneration, GenerationMetadata};
use crate::tables::schema::ColumnType;

const FORMAT_VERSION: u32 = 1;
const PART_CHUNK_BYTES: usize = 1024 * 1024;
const MAX_GENERATION_BYTES: usize = 512 * 1024 * 1024;
const MAX_CHUNKS: usize = MAX_GENERATION_BYTES / PART_CHUNK_BYTES + 4;
const MAX_STORED_ROWS_PER_INDEX: usize = 8 * MAX_CHUNKS;
const LIVE: &str = "live";

#[derive(Clone, Serialize, Deserialize)]
struct PartManifest {
    name: String,
    bytes: usize,
    chunks: usize,
    sha256: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
struct GenerationManifest {
    version: u32,
    index_key: String,
    table: String,
    column: String,
    method: AnnMethod,
    metric: VectorMetric,
    schema_digest: String,
    source_authority_digest: [u8; 32],
    generation: u64,
    dim: Option<usize>,
    built_epoch: u64,
    max_rowid: Option<u64>,
    rows: usize,
    parts: Vec<PartManifest>,
}

#[derive(Serialize, Deserialize)]
struct LivePointer {
    manifest: GenerationManifest,
    manifest_sha256: [u8; 32],
}

fn hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn pointer(manifest: GenerationManifest) -> Result<LivePointer, String> {
    let encoded = rmp_serde::to_vec_named(&manifest)
        .map_err(|error| format!("encode ANN generation manifest: {error}"))?;
    Ok(LivePointer {
        manifest,
        manifest_sha256: hash(&encoded),
    })
}

fn decode_pointer(bytes: &[u8]) -> Result<GenerationManifest, String> {
    let pointer: LivePointer = decode_stored(bytes, "ANN generation pointer")?;
    let expected = self::pointer(pointer.manifest.clone())?;
    if pointer.manifest_sha256 != expected.manifest_sha256 {
        return Err("ANN generation manifest digest mismatch".to_string());
    }
    validate_manifest_shape(&pointer.manifest)?;
    Ok(pointer.manifest)
}

fn expected_parts(method: AnnMethod, rows: usize) -> &'static [&'static str] {
    if rows == 0 {
        &["empty"]
    } else {
        match method {
            AnnMethod::Hnsw => &["hnsw"],
            AnnMethod::IvfFlat => &["ivf_codes", "ivf_meta", "ivf_refine", "ivf_space"],
        }
    }
}

fn validate_manifest_shape(manifest: &GenerationManifest) -> Result<(), String> {
    if manifest.version != FORMAT_VERSION || manifest.generation == 0 {
        return Err("ANN generation manifest version or number is invalid".to_string());
    }
    if manifest.rows > 1_000_000 || manifest.rows > 0 && manifest.max_rowid.is_none() {
        return Err("ANN generation row metadata is invalid".to_string());
    }
    if manifest.rows > 0 && manifest.dim.is_none_or(|dim| dim == 0) {
        return Err("ANN generation dimension is invalid".to_string());
    }
    let expected: BTreeSet<&str> = expected_parts(manifest.method, manifest.rows)
        .iter()
        .copied()
        .collect();
    let mut actual = BTreeSet::new();
    let mut total_bytes = 0usize;
    let mut total_chunks = 0usize;
    for part in &manifest.parts {
        if !actual.insert(part.name.as_str()) {
            return Err("ANN generation manifest has duplicate parts".to_string());
        }
        let required_chunks = part.bytes.div_ceil(PART_CHUNK_BYTES).max(1);
        if part.chunks != required_chunks {
            return Err("ANN generation part chunk count is invalid".to_string());
        }
        total_bytes = total_bytes
            .checked_add(part.bytes)
            .ok_or("ANN generation byte bound overflow")?;
        total_chunks = total_chunks
            .checked_add(part.chunks)
            .ok_or("ANN generation chunk bound overflow")?;
    }
    if actual != expected || total_bytes > MAX_GENERATION_BYTES || total_chunks > MAX_CHUNKS {
        return Err("ANN generation part set or size is invalid".to_string());
    }
    Ok(())
}

fn part_key(name: &str, chunk: usize) -> String {
    format!("{name}:{chunk:08}")
}

/// Remove one registration's pointer and all generations inside its admitted
/// owner mutation. The bounded census makes a surprising accumulation fail
/// closed instead of allowing an unbounded drop or replacement.
pub(super) fn clear_ann_generation_rows_in(
    write: &SqlWrite<'_>,
    index_key: &str,
) -> Result<(), String> {
    let mut table = write.open_table(SQL_ANN_GENERATIONS)?;
    let mut keys = Vec::new();
    for row in table.range((index_key, 0, "")..).map_err(map_err)? {
        let (key, _) = row.map_err(map_err)?;
        let (found_index, generation, part) = key.value();
        if found_index != index_key {
            break;
        }
        if keys.len() == MAX_STORED_ROWS_PER_INDEX {
            return Err("ANN generation cleanup exceeds row bound".to_string());
        }
        keys.push((generation, part.to_string()));
    }
    for (generation, part) in keys {
        table
            .remove((index_key, generation, part.as_str()))
            .map_err(map_err)?;
    }
    Ok(())
}

fn registration_matches(stored: &AnnIndexPlan, requested: &AnnIndexPlan) -> bool {
    stored == requested
}

fn manifest_matches(
    manifest: &GenerationManifest,
    plan: &AnnIndexPlan,
    schema_digest: &str,
    authority_digest: [u8; 32],
    source_epoch: u64,
) -> Result<(), String> {
    if manifest.index_key != TableStore::ann_index_key(plan)
        || manifest.table != plan.table
        || manifest.column != plan.column
        || manifest.method != plan.method
        || manifest.metric != plan.metric
        || manifest.schema_digest != schema_digest
        || manifest.source_authority_digest != authority_digest
        || manifest.built_epoch > source_epoch
    {
        return Err(
            "ANN generation does not match its live registration, schema, or source authority"
                .to_string(),
        );
    }
    Ok(())
}

fn check_column(
    plan: &AnnIndexPlan,
    schema: &crate::tables::schema::TableSchema,
) -> Result<(String, Option<usize>), String> {
    let column = schema
        .columns()
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(&plan.column))
        .ok_or_else(|| "ANN generation column is missing".to_string())?;
    let ColumnType::Vector(declared_dim) = column.ty else {
        return Err("ANN generation column is no longer a vector".to_string());
    };
    Ok((schema.schema_digest()?, declared_dim))
}

impl TableStore {
    /// Commit one complete artifact and its live pointer atomically as derived
    /// owner rows. An older build cannot replace a newer live generation.
    pub(crate) fn persist_ann_generation(
        &self,
        plan: &AnnIndexPlan,
        generation: &AnnGeneration,
    ) -> Result<(), String> {
        let metadata: GenerationMetadata = generation.metadata();
        if metadata.generation == 0
            || metadata.method != plan.method
            || metadata.metric != plan.metric
        {
            return Err("ANN generation metadata disagrees with registration".to_string());
        }
        let parts = generation.artifact_parts()?;
        let artifact_bytes = parts.iter().try_fold(0usize, |total, (_, bytes)| {
            total
                .checked_add(bytes.len())
                .ok_or("ANN generation byte bound overflow")
        })?;
        if artifact_bytes > MAX_GENERATION_BYTES {
            return Err("ANN generation exceeds durable artifact byte bound".to_string());
        }
        let mut descriptors = Vec::new();
        for (name, bytes) in &parts {
            let chunks = bytes.len().div_ceil(PART_CHUNK_BYTES).max(1);
            descriptors.push(PartManifest {
                name: (*name).to_string(),
                bytes: bytes.len(),
                chunks,
                sha256: hash(bytes),
            });
        }
        descriptors.sort_by(|left, right| left.name.cmp(&right.name));
        let key = Self::ann_index_key(plan);
        let authority_digest = self.authority.source_authority_digest();
        self.authority
            .maintain_derived("persist-ann-generation", &key, |write| {
                let schema = get_schema_in(write, &plan.table)?
                    .ok_or_else(|| "ANN generation table is missing".to_string())?;
                let (schema_digest, declared_dim) = check_column(plan, &schema)?;
                if declared_dim.is_some_and(|dim| metadata.dim != Some(dim)) {
                    return Err("ANN generation dimension disagrees with table schema".to_string());
                }
                let indexes = write.open_table(ANN_INDEXES)?;
                let registration = indexes
                    .get(key.as_str())
                    .map_err(map_err)?
                    .ok_or_else(|| "ANN generation registration is missing".to_string())?;
                let stored: AnnIndexPlan = decode_stored(registration.value(), "ANN index")?;
                if !registration_matches(&stored, plan) {
                    return Err("ANN generation registration changed".to_string());
                }
                drop(registration);
                drop(indexes);
                let source_epoch = next_source_epoch_in(write)? - 1;
                let manifest = GenerationManifest {
                    version: FORMAT_VERSION,
                    index_key: key.clone(),
                    table: plan.table.clone(),
                    column: plan.column.clone(),
                    method: metadata.method,
                    metric: metadata.metric,
                    schema_digest,
                    source_authority_digest: authority_digest,
                    generation: metadata.generation,
                    dim: metadata.dim,
                    built_epoch: metadata.built_epoch,
                    max_rowid: metadata.max_rowid,
                    rows: metadata.rows,
                    parts: descriptors,
                };
                validate_manifest_shape(&manifest)?;
                manifest_matches(
                    &manifest,
                    plan,
                    &manifest.schema_digest,
                    authority_digest,
                    source_epoch,
                )?;
                let pointer = pointer(manifest)?;
                let pointer_bytes = rmp_serde::to_vec_named(&pointer)
                    .map_err(|error| format!("encode ANN generation pointer: {error}"))?;
                let table = write.open_table(SQL_ANN_GENERATIONS)?;
                if let Some(existing) = table.get((key.as_str(), 0, LIVE)).map_err(map_err)? {
                    let old = decode_pointer(existing.value())?;
                    if old.generation >= metadata.generation {
                        return Err(
                            "ANN generation was superseded by an existing live pointer".to_string()
                        );
                    }
                }
                drop(table);
                clear_ann_generation_rows_in(write, &key)?;
                let mut table = write.open_table(SQL_ANN_GENERATIONS)?;
                for (name, bytes) in parts {
                    if bytes.is_empty() {
                        let part = part_key(name, 0);
                        table
                            .insert((key.as_str(), metadata.generation, part.as_str()), &[][..])
                            .map_err(map_err)?;
                    } else {
                        for (number, chunk) in bytes.chunks(PART_CHUNK_BYTES).enumerate() {
                            let part = part_key(name, number);
                            table
                                .insert((key.as_str(), metadata.generation, part.as_str()), chunk)
                                .map_err(map_err)?;
                        }
                    }
                }
                table
                    .insert((key.as_str(), 0, LIVE), pointer_bytes.as_slice())
                    .map_err(map_err)?;
                Ok(())
            })
    }

    /// Restore the live graph without rebuilding. Every declared chunk and the
    /// exact positive-generation part set must verify before decoding.
    pub(crate) fn restore_ann_generation(
        &self,
        plan: &AnnIndexPlan,
    ) -> Result<Option<AnnGeneration>, String> {
        let read = self.authority.read()?;
        let key = Self::ann_index_key(plan);
        let schema = get_schema_read(&read, &plan.table)?
            .ok_or_else(|| "ANN generation table is missing".to_string())?;
        let (schema_digest, declared_dim) = check_column(plan, &schema)?;
        let indexes = read.open_owner_table(ANN_INDEXES)?;
        let registration = indexes
            .get(key.as_str())
            .map_err(map_err)?
            .ok_or_else(|| "ANN generation registration is missing".to_string())?;
        let stored: AnnIndexPlan = decode_stored(registration.value(), "ANN index")?;
        if !registration_matches(&stored, plan) {
            return Err("ANN generation registration changed".to_string());
        }
        let source = self.authority.source_snapshot(&read)?;
        let table = read.open_owner_table(SQL_ANN_GENERATIONS)?;
        let Some(live) = table.get((key.as_str(), 0, LIVE)).map_err(map_err)? else {
            return Ok(None);
        };
        let manifest = decode_pointer(live.value())?;
        if declared_dim.is_some_and(|dim| manifest.dim != Some(dim)) {
            return Err("ANN generation dimension disagrees with table schema".to_string());
        }
        manifest_matches(
            &manifest,
            plan,
            &schema_digest,
            source.authority_digest,
            source.epoch,
        )?;
        drop(live);
        let expected_keys: BTreeSet<String> = manifest
            .parts
            .iter()
            .flat_map(|part| (0..part.chunks).map(move |chunk| part_key(&part.name, chunk)))
            .collect();
        let mut actual_keys = BTreeSet::new();
        let mut chunks = BTreeMap::new();
        for row in table
            .range((key.as_str(), manifest.generation, "")..)
            .map_err(map_err)?
        {
            let (row_key, value) = row.map_err(map_err)?;
            if row_key.value().0 != key || row_key.value().1 != manifest.generation {
                break;
            }
            let part_key = row_key.value().2.to_string();
            if value.value().len() > PART_CHUNK_BYTES || !actual_keys.insert(part_key.clone()) {
                return Err("ANN generation chunk is invalid".to_string());
            }
            chunks.insert(part_key, value.value().to_vec());
            if actual_keys.len() > MAX_CHUNKS {
                return Err("ANN generation exceeds chunk bound".to_string());
            }
        }
        if actual_keys != expected_keys {
            return Err("ANN generation has missing or undeclared chunks".to_string());
        }
        let mut parts = BTreeMap::new();
        for part in &manifest.parts {
            let mut bytes = Vec::with_capacity(part.bytes);
            for chunk in 0..part.chunks {
                let piece = chunks
                    .remove(&part_key(&part.name, chunk))
                    .ok_or_else(|| "ANN generation chunk is missing".to_string())?;
                bytes.extend_from_slice(&piece);
            }
            if bytes.len() != part.bytes || hash(&bytes) != part.sha256 {
                return Err("ANN generation part length or digest mismatch".to_string());
            }
            parts.insert(part.name.clone(), bytes);
        }
        AnnGeneration::from_artifact_parts(
            manifest.generation,
            manifest.method,
            manifest.metric,
            manifest.dim,
            manifest.built_epoch,
            manifest.max_rowid,
            manifest.rows,
            &parts,
        )
        .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> GenerationManifest {
        GenerationManifest {
            version: FORMAT_VERSION,
            index_key: "docs.emb.L2".to_string(),
            table: "docs".to_string(),
            column: "emb".to_string(),
            method: AnnMethod::Hnsw,
            metric: VectorMetric::L2,
            schema_digest: "a".repeat(64),
            source_authority_digest: [1; 32],
            generation: 1,
            dim: Some(8),
            built_epoch: 3,
            max_rowid: Some(4),
            rows: 5,
            parts: vec![PartManifest {
                name: "hnsw".to_string(),
                bytes: 17,
                chunks: 1,
                sha256: [2; 32],
            }],
        }
    }

    #[test]
    fn pointer_digest_and_part_shape_fail_closed() {
        let valid = pointer(manifest()).unwrap();
        let encoded = rmp_serde::to_vec_named(&valid).unwrap();
        assert!(decode_pointer(&encoded).is_ok());

        let mut altered = valid;
        altered.manifest.parts[0].bytes = PART_CHUNK_BYTES + 1;
        let encoded = rmp_serde::to_vec_named(&altered).unwrap();
        assert!(decode_pointer(&encoded).is_err(), "stale manifest digest");

        altered = pointer(manifest()).unwrap();
        altered.manifest.parts[0].name = "unexpected".to_string();
        altered = pointer(altered.manifest).unwrap();
        let encoded = rmp_serde::to_vec_named(&altered).unwrap();
        assert!(decode_pointer(&encoded).is_err(), "undeclared part");
    }
}
