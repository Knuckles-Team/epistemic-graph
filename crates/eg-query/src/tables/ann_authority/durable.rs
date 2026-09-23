//! Durable generations (RF-019 contract request, EH-352): the encoding of one
//! activated generation for `__sql_ann_generations__`, and its restore when a
//! store is reopened.
//!
//! A FULL generation stores its graph: an HNSW graph as-is, an IVF index as its
//! validated `eg_ann::durable_codes` buffers. An EXTENSION stores only the row
//! ids folded in on top of its full base; restoring it re-applies those rows'
//! current vectors to the restored base. Rows read at restore time may be newer
//! than the extension's build epoch, which is harmless: a generation proposes
//! candidates, and every row changed after its build epoch is scored exactly.
//!
//! The manifest pins the payload's SHA-256, checked before a byte of the
//! payload is decoded; a mismatch refuses the restore and the worker rebuilds.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::generation::{AnnGeneration, AnnGraph, IvfSpace};
use crate::sql::{AnnIndexPlan, AnnMethod};
use crate::tables::store::{AnnChangedRows, StoredGeneration};
use crate::tables::TableStore;

/// Largest payload one generation may restore from.
const MAX_PAYLOAD_BYTES: u64 = 16 * 1024 * 1024 * 1024;

/// How one generation is stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Encoding {
    /// The whole graph.
    Full,
    /// Only the rows folded in on top of the full base, which is still stored.
    Extension,
}

#[derive(Debug, Serialize, Deserialize)]
enum Body {
    Empty,
    Hnsw,
    Ivf {
        space: IvfSpace,
        nprobe: usize,
        meta_bytes: usize,
        code_bytes: usize,
    },
    Extension {
        base: u64,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Manifest {
    generation: u64,
    method: AnnMethod,
    dim: Option<usize>,
    built_epoch: u64,
    max_rowid: Option<u64>,
    rows: usize,
    body: Body,
    digest: [u8; 32],
}

/// Encode `generation` for the durable generation table.
pub(super) fn encode(
    generation: &AnnGeneration,
    encoding: Encoding,
) -> Result<StoredGeneration, String> {
    let (body, payload) = match (encoding, generation.base) {
        (Encoding::Extension, Some(base)) => {
            (Body::Extension { base }, encode_rmp(&generation.delta)?)
        }
        _ => encode_graph(&generation.graph)?,
    };
    let manifest = Manifest {
        generation: generation.generation,
        method: generation.method,
        dim: generation.dim,
        built_epoch: generation.built_epoch,
        max_rowid: generation.max_rowid,
        rows: generation.rows,
        body,
        digest: Sha256::digest(&payload).into(),
    };
    Ok(StoredGeneration {
        generation: generation.generation,
        manifest: encode_rmp(&manifest)?,
        payload,
    })
}

fn encode_graph(graph: &AnnGraph) -> Result<(Body, Vec<u8>), String> {
    match graph {
        AnnGraph::Empty => Ok((Body::Empty, Vec::new())),
        AnnGraph::Hnsw(index) => Ok((Body::Hnsw, encode_rmp(index)?)),
        AnnGraph::Ivf {
            index,
            space,
            nprobe,
        } => {
            let artifact = eg_ann::durable_codes::encode(index)
                .map_err(|error| format!("encode IVF generation: {error}"))?;
            let body = Body::Ivf {
                space: *space,
                nprobe: *nprobe,
                meta_bytes: artifact.meta.len(),
                code_bytes: artifact.codes.len(),
            };
            Ok((
                body,
                [artifact.meta, artifact.codes, artifact.refine].concat(),
            ))
        }
    }
}

fn encode_rmp<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(value).map_err(|error| format!("encode ANN generation: {error}"))
}

/// The verified manifest of `stored`: decodable, of `plan`'s method, and
/// pinning exactly `stored`'s payload.
pub(super) fn verified_manifest(
    stored: &StoredGeneration,
    plan: &AnnIndexPlan,
) -> Result<Manifest, String> {
    let manifest: Manifest = rmp_serde::from_slice(&stored.manifest)
        .map_err(|error| format!("ANN generation manifest is unreadable: {error}"))?;
    if manifest.generation != stored.generation || manifest.method != plan.method {
        return Err("ANN generation manifest does not describe this registration".to_string());
    }
    if stored.payload.len() as u64 > MAX_PAYLOAD_BYTES {
        return Err("ANN generation payload exceeds its restore bound".to_string());
    }
    let digest: [u8; 32] = Sha256::digest(&stored.payload).into();
    if digest != manifest.digest {
        return Err("ANN generation payload does not match its manifest digest".to_string());
    }
    Ok(manifest)
}

/// Rebuild the full generation `manifest` describes from its verified payload.
fn full_generation(manifest: &Manifest, payload: &[u8]) -> Result<AnnGeneration, String> {
    let graph = match &manifest.body {
        Body::Empty => AnnGraph::Empty,
        Body::Hnsw => AnnGraph::Hnsw(
            rmp_serde::from_slice(payload)
                .map_err(|error| format!("HNSW generation is unreadable: {error}"))?,
        ),
        Body::Ivf {
            space,
            nprobe,
            meta_bytes,
            code_bytes,
        } => ivf_graph(payload, *space, *nprobe, (*meta_bytes, *code_bytes))?,
        Body::Extension { .. } => {
            return Err("ANN generation extends another extension".to_string());
        }
    };
    Ok(AnnGeneration {
        generation: manifest.generation,
        method: manifest.method,
        dim: manifest.dim,
        built_epoch: manifest.built_epoch,
        max_rowid: manifest.max_rowid,
        rows: manifest.rows,
        base: None,
        delta: Vec::new(),
        graph,
    })
}

fn ivf_graph(
    payload: &[u8],
    space: IvfSpace,
    nprobe: usize,
    (meta_bytes, code_bytes): (usize, usize),
) -> Result<AnnGraph, String> {
    let codes_end = meta_bytes
        .checked_add(code_bytes)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| "IVF generation payload is truncated".to_string())?;
    let artifact = eg_ann::durable_codes::AnnCodeArtifact {
        meta: payload[..meta_bytes].to_vec(),
        codes: payload[meta_bytes..codes_end].to_vec(),
        refine: payload[codes_end..].to_vec(),
    };
    let index = eg_ann::durable_codes::decode(&artifact)
        .map_err(|error| format!("IVF generation is unreadable: {error}"))?;
    Ok(AnnGraph::Ivf {
        index: Box::new(index),
        space,
        nprobe,
    })
}

impl TableStore {
    /// The persisted live generation of `plan`'s registration `index`, restored
    /// to memory, or `None` when none was persisted.
    pub(super) fn restore_ann_generation(
        &self,
        plan: &AnnIndexPlan,
        index: &str,
    ) -> Result<Option<AnnGeneration>, String> {
        let Some(stored) = self.live_ann_generation(index)? else {
            return Ok(None);
        };
        let manifest = verified_manifest(&stored, plan)?;
        let Body::Extension { base } = &manifest.body else {
            return full_generation(&manifest, &stored.payload).map(Some);
        };
        let base = *base;
        let base_stored = self
            .stored_ann_generation(index, base)?
            .ok_or_else(|| format!("ANN generation {base} (an extension's base) is missing"))?;
        let base_generation = full_generation(
            &verified_manifest(&base_stored, plan)?,
            &base_stored.payload,
        )?;
        let rowids: Vec<u64> = rmp_serde::from_slice(&stored.payload)
            .map_err(|error| format!("ANN extension rows are unreadable: {error}"))?;
        let changed = AnnChangedRows {
            epoch: manifest.built_epoch,
            rows: self.ann_row_vectors(&plan.table, &plan.column, &rowids)?,
            complete: true,
        };
        base_generation
            .extend(manifest.generation, &changed)
            .map(Some)
            .ok_or_else(|| "ANN extension's base indexes no vector width".to_string())
    }
}
