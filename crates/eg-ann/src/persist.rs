//! Persistent on-disk format — the no-rebuild-on-load win (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed).
//!
//! An index directory holds three files:
//!   * `meta.bin` — versioned postcard of everything EXCEPT the two bulk code buffers: the
//!     OPQ rotation, coarse + PQ centroids, ids, `list_of`, tombstones, per-row
//!     SQ8 min/scale, and params.
//!   * `codes.bin` — `N*m` u8 PQ codes (the IVF-PQ bulk), mmapped on open.
//!   * `refine.bin` — `N*dim` u8 SQ8 refine codes, mmapped on open.
//!
//! `open()` validates and decodes `meta.bin`, mmaps the two code files (copying the bytes
//! into the `IvfPq` so it stays one owned struct — still NO f32 reconstruction, NO
//! k-means, NO graph build), and rebuilds posting lists with one O(N) integer
//! pass. Reopen does no vector arithmetic — the expensive structure (rotation +
//! codebooks + cell assignment) is read back, not recomputed. This is the precise
//! contrast with `SemanticStore`'s HNSW, which is `#[serde(skip)]` and rebuilds
//! insert-by-insert on first search after load.

use crate::ivfpq::{IvfPq, PQ_KSUB};
use memmap2::Mmap;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::fs::{self, File};
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;

const META_FILE: &str = "meta.bin";
const CODES_FILE: &str = "codes.bin";
const REFINE_FILE: &str = "refine.bin";
pub(crate) const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;
const MAX_DIMENSION: usize = 8_192;
const MAX_LISTS: usize = 1_000_000;
const MAX_SUBQUANTIZERS: usize = 8_192;
const MAX_ROWS: usize = 5_000_000;
const MAX_MODEL_VALUES: usize = (MAX_METADATA_BYTES as usize) / std::mem::size_of::<f32>();

#[derive(Serialize)]
pub(crate) struct Meta {
    dim: usize,
    nlist: usize,
    m: usize,
    dsub: usize,
    rotation: Vec<f32>,
    coarse_centroids: Vec<f32>,
    pq_centroids: Vec<f32>,
    sq_min: Vec<f32>,
    sq_scale: Vec<f32>,
    ids: Vec<u64>,
    list_of: Vec<u32>,
    deleted: Vec<u8>,
}

struct MetadataView<'a> {
    dim: usize,
    nlist: usize,
    m: usize,
    dsub: usize,
    rotation: &'a [f32],
    coarse_centroids: &'a [f32],
    pq_centroids: &'a [f32],
    sq_min: &'a [f32],
    sq_scale: &'a [f32],
    ids: &'a [u64],
    list_of: &'a [u32],
    deleted: &'a [u8],
}

struct BoundedVec<T, const LIMIT: usize>(Vec<T>);

impl<'de, T, const LIMIT: usize> Deserialize<'de> for BoundedVec<T, LIMIT>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BoundedVisitor<T, const LIMIT: usize>(PhantomData<T>);

        impl<'de, T, const LIMIT: usize> Visitor<'de> for BoundedVisitor<T, LIMIT>
        where
            T: Deserialize<'de>,
        {
            type Value = BoundedVec<T, LIMIT>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a sequence with at most {LIMIT} elements")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    if values.len() >= LIMIT {
                        return Err(de::Error::custom("ANN metadata sequence limit exceeded"));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }

        deserializer.deserialize_seq(BoundedVisitor::<T, LIMIT>(PhantomData))
    }
}

impl<'de> Deserialize<'de> for Meta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MetaVisitor;

        impl<'de> Visitor<'de> for MetaVisitor {
            type Value = Meta;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("strict ANN metadata")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                macro_rules! field {
                    ($index:expr, $type:ty) => {
                        sequence
                            .next_element::<$type>()?
                            .ok_or_else(|| de::Error::invalid_length($index, &self))?
                    };
                }

                Ok(Meta {
                    dim: field!(0, usize),
                    nlist: field!(1, usize),
                    m: field!(2, usize),
                    dsub: field!(3, usize),
                    rotation: field!(4, BoundedVec<f32, MAX_MODEL_VALUES>).0,
                    coarse_centroids: field!(5, BoundedVec<f32, MAX_MODEL_VALUES>).0,
                    pq_centroids: field!(6, BoundedVec<f32, MAX_MODEL_VALUES>).0,
                    sq_min: field!(7, BoundedVec<f32, MAX_ROWS>).0,
                    sq_scale: field!(8, BoundedVec<f32, MAX_ROWS>).0,
                    ids: field!(9, BoundedVec<u64, MAX_ROWS>).0,
                    list_of: field!(10, BoundedVec<u32, MAX_ROWS>).0,
                    deleted: field!(11, BoundedVec<u8, MAX_ROWS>).0,
                })
            }
        }

        deserializer.deserialize_tuple(12, MetaVisitor)
    }
}

impl Meta {
    pub(crate) fn from_index(idx: &IvfPq) -> Self {
        Self {
            dim: idx.dim,
            nlist: idx.nlist,
            m: idx.m,
            dsub: idx.dsub,
            rotation: idx.rotation.clone(),
            coarse_centroids: idx.coarse_centroids.clone(),
            pq_centroids: idx.pq_centroids.clone(),
            sq_min: idx.sq_min.clone(),
            sq_scale: idx.sq_scale.clone(),
            ids: idx.ids.clone(),
            list_of: idx.list_of.clone(),
            deleted: idx.deleted.clone(),
        }
    }

    pub(crate) fn expected_code_lengths(&self) -> std::io::Result<(usize, usize)> {
        validate_components(MetadataView {
            dim: self.dim,
            nlist: self.nlist,
            m: self.m,
            dsub: self.dsub,
            rotation: &self.rotation,
            coarse_centroids: &self.coarse_centroids,
            pq_centroids: &self.pq_centroids,
            sq_min: &self.sq_min,
            sq_scale: &self.sq_scale,
            ids: &self.ids,
            list_of: &self.list_of,
            deleted: &self.deleted,
        })
    }

    pub(crate) fn into_index(self, codes: Vec<u8>, sq_codes: Vec<u8>) -> std::io::Result<IvfPq> {
        let idx = IvfPq {
            dim: self.dim,
            nlist: self.nlist,
            m: self.m,
            dsub: self.dsub,
            rotation: self.rotation,
            coarse_centroids: self.coarse_centroids,
            pq_centroids: self.pq_centroids,
            codes,
            sq_codes,
            sq_min: self.sq_min,
            sq_scale: self.sq_scale,
            ids: self.ids,
            list_of: self.list_of,
            deleted: self.deleted,
            postings: Vec::new(),
        };
        validate_index(&idx)?;
        Ok(idx)
    }
}

/// Atomically write the index to `dir` (write-to-temp + rename per file).
pub fn save(idx: &IvfPq, dir: &Path) -> std::io::Result<()> {
    validate_index(idx)?;
    fs::create_dir_all(dir)?;
    let meta = Meta::from_index(idx);
    let encoded = crate::codec::serialize(&meta).map_err(invalid_data)?;
    if encoded.len() as u64 > MAX_METADATA_BYTES {
        return Err(invalid_data("ANN metadata exceeds its safety bound"));
    }
    write_atomic(&dir.join(META_FILE), &encoded)?;
    write_atomic(&dir.join(CODES_FILE), &idx.codes)?;
    write_atomic(&dir.join(REFINE_FILE), &idx.sq_codes)?;
    Ok(())
}

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}

/// Open WITHOUT rebuilding from raw f32. mmaps the two code files, loads meta,
/// rebuilds posting lists (integer pass only). Ready to query.
pub fn open(dir: &Path) -> std::io::Result<IvfPq> {
    let mbytes = read_bounded(&dir.join(META_FILE), MAX_METADATA_BYTES)?;
    let meta: Meta = crate::codec::deserialize(&mbytes).map_err(invalid_data)?;
    let (codes_len, refine_len) = meta.expected_code_lengths()?;

    let codes = mmap_to_vec(&dir.join(CODES_FILE), codes_len)?;
    let sq_codes = mmap_to_vec(&dir.join(REFINE_FILE), refine_len)?;

    let mut idx = meta.into_index(codes, sq_codes)?;
    idx.rebuild_postings(); // O(N) integer pass — no vector math
    Ok(idx)
}

fn read_bounded(path: &Path, maximum: u64) -> std::io::Result<Vec<u8>> {
    let length = fs::metadata(path)?.len();
    if length > maximum {
        return Err(invalid_data("ANN metadata exceeds its safety bound"));
    }
    fs::read(path)
}

fn mmap_to_vec(path: &Path, expected: usize) -> std::io::Result<Vec<u8>> {
    let f = File::open(path)?;
    let len = f.metadata()?.len();
    if len != u64::try_from(expected).map_err(invalid_data)? {
        return Err(invalid_data(
            "ANN code-buffer length does not match metadata",
        ));
    }
    if len == 0 {
        return Ok(Vec::new());
    }
    let mmap = unsafe { Mmap::map(&f)? };
    Ok(mmap.to_vec())
}

pub(crate) fn validate_index(idx: &IvfPq) -> std::io::Result<()> {
    let (codes_len, refine_len) = validate_components(MetadataView {
        dim: idx.dim,
        nlist: idx.nlist,
        m: idx.m,
        dsub: idx.dsub,
        rotation: &idx.rotation,
        coarse_centroids: &idx.coarse_centroids,
        pq_centroids: &idx.pq_centroids,
        sq_min: &idx.sq_min,
        sq_scale: &idx.sq_scale,
        ids: &idx.ids,
        list_of: &idx.list_of,
        deleted: &idx.deleted,
    })?;
    if idx.codes.len() != codes_len || idx.sq_codes.len() != refine_len {
        return Err(invalid_data(
            "ANN code-buffer shape does not match metadata",
        ));
    }
    Ok(())
}

fn validate_components(metadata: MetadataView<'_>) -> std::io::Result<(usize, usize)> {
    let MetadataView {
        dim,
        nlist,
        m,
        dsub,
        rotation,
        coarse_centroids,
        pq_centroids,
        sq_min,
        sq_scale,
        ids,
        list_of,
        deleted,
    } = metadata;
    if dim == 0
        || dim > MAX_DIMENSION
        || nlist == 0
        || nlist > MAX_LISTS
        || m == 0
        || m > MAX_SUBQUANTIZERS
        || !dim.is_multiple_of(m)
        || dsub != dim / m
    {
        return Err(invalid_data("ANN dimensions are inconsistent"));
    }
    if ids.len() > MAX_ROWS {
        return Err(invalid_data(
            "ANN index exceeds its addressable row or list range",
        ));
    }

    let rotation_len = dim
        .checked_mul(dim)
        .ok_or_else(|| invalid_data("ANN rotation shape overflow"))?;
    let coarse_len = nlist
        .checked_mul(dim)
        .ok_or_else(|| invalid_data("ANN coarse-centroid shape overflow"))?;
    let pq_len = m
        .checked_mul(PQ_KSUB)
        .and_then(|value| value.checked_mul(dsub))
        .ok_or_else(|| invalid_data("ANN PQ-centroid shape overflow"))?;
    if rotation.len() != rotation_len
        || coarse_centroids.len() != coarse_len
        || pq_centroids.len() != pq_len
    {
        return Err(invalid_data(
            "ANN model-buffer shape does not match dimensions",
        ));
    }

    let rows = ids.len();
    if sq_min.len() != rows
        || sq_scale.len() != rows
        || list_of.len() != rows
        || deleted.len() != rows
    {
        return Err(invalid_data("ANN row metadata is not parallel"));
    }
    if list_of.iter().any(|&cell| cell as usize >= nlist) {
        return Err(invalid_data("ANN row references an unknown posting list"));
    }
    if deleted.iter().any(|&value| value > 1) {
        return Err(invalid_data("ANN tombstone contains an invalid value"));
    }
    if rotation
        .iter()
        .chain(coarse_centroids)
        .chain(pq_centroids)
        .chain(sq_min)
        .chain(sq_scale)
        .any(|value| !value.is_finite())
    {
        return Err(invalid_data("ANN metadata contains a non-finite value"));
    }

    let codes_len = rows
        .checked_mul(m)
        .ok_or_else(|| invalid_data("ANN PQ-code shape overflow"))?;
    let refine_len = rows
        .checked_mul(dim)
        .ok_or_else(|| invalid_data("ANN refine-code shape overflow"))?;
    Ok((codes_len, refine_len))
}

pub(crate) fn invalid_data(error: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error.to_string())
}

/// Compaction / VACUUM: rewrite the index dropping all tombstoned rows, WITHOUT
/// retraining (rotation + codebooks are kept; only the row buffers are rebuilt).
/// This reclaims posting-list bloat from accumulated deletes — the per-list
/// compaction the spike flagged as a hard problem. Returns a fresh compacted
/// `IvfPq`; the codebooks/rotation are shared by clone (cheap relative to the data).
pub fn compact(idx: &IvfPq) -> IvfPq {
    let dim = idx.dim;
    let m = idx.m;
    let live: Vec<usize> = (0..idx.ids.len())
        .filter(|&r| idx.deleted[r] == 0)
        .collect();
    let n = live.len();

    let mut codes = Vec::with_capacity(n * m);
    let mut sq_codes = Vec::with_capacity(n * dim);
    let mut sq_min = Vec::with_capacity(n);
    let mut sq_scale = Vec::with_capacity(n);
    let mut ids = Vec::with_capacity(n);
    let mut list_of = Vec::with_capacity(n);
    for &row in &live {
        codes.extend_from_slice(&idx.codes[row * m..(row + 1) * m]);
        sq_codes.extend_from_slice(&idx.sq_codes[row * dim..(row + 1) * dim]);
        sq_min.push(idx.sq_min[row]);
        sq_scale.push(idx.sq_scale[row]);
        ids.push(idx.ids[row]);
        list_of.push(idx.list_of[row]);
    }
    let deleted = vec![0u8; n];

    let mut out = IvfPq {
        dim,
        nlist: idx.nlist,
        m,
        dsub: idx.dsub,
        rotation: idx.rotation.clone(),
        coarse_centroids: idx.coarse_centroids.clone(),
        pq_centroids: idx.pq_centroids.clone(),
        codes,
        sq_codes,
        sq_min,
        sq_scale,
        ids,
        list_of,
        deleted,
        postings: Vec::new(),
    };
    out.rebuild_postings();
    out
}
