//! Chunk encoding, zone maps, and content addressing (D-VZ-1 lane V1).
//!
//! A [`Column`](crate::store::Column) is split into fixed-size row ranges
//! ([`CHUNK_ROWS`]). Each range is encoded to a flat byte buffer by
//! [`encode_chunk`], hashed (`sha256`, matching `eg-viz-core::job`'s digest
//! convention) into a [`Chunk::content_id`], and stored ONCE in
//! [`crate::store::ColumnStore`]'s content-addressed chunk table — two chunks
//! with byte-identical encoded content (e.g. a run of a constant value, or the
//! same data ingested twice) collapse to the same `content_id` and are stored
//! only once. [`ZoneMap`] carries that chunk's `(row_count, null_count, min,
//! max)` so a future query-planner lane can skip a whole chunk without decoding
//! it — the "prune before scan" discipline the rest of this engine already uses
//! for Parquet segments (`EPISTEMIC_GRAPH_OBS_ADDR`'s per-segment manifest).

use bitvec::order::Lsb0;
use bitvec::vec::BitVec;
use sha2::{Digest, Sha256};

use crate::dictionary::NULL_CODE;
use crate::types::ColumnData;

/// Rows per chunk. A fixed, engine-tunable constant (not a caller knob in V1) —
/// small enough that a zone map's pruning grain is meaningful, large enough that
/// per-chunk overhead (one `content_id` + one hash) stays negligible relative to
/// the encoded payload.
pub const CHUNK_ROWS: usize = 8192;

/// Per-chunk summary statistics, computed at encode time. `min`/`max` are
/// populated only for numeric logical types (`F64`/`F32`/`I64`/`U64`) — a
/// string/boolean/categorical chunk carries `null_count` only.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ZoneMap {
    pub row_count: u32,
    pub null_count: u32,
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// One encoded, content-addressed row range of a column.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Chunk {
    /// `hex(sha256(encoded_bytes))[..32]` — see [`content_id`].
    pub content_id: String,
    pub zone: ZoneMap,
}

/// The content address for a chunk's encoded bytes. Matches `eg-viz-core::job`'s
/// digest convention (a domain-separated sha256 prefix, hex-encoded) so a
/// consumer inspecting both a `query_hash` and a `content_id` recognizes the same
/// hashing discipline.
pub fn content_id(encoded: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg-viz-columnstore.chunk.v1\0");
    hasher.update(encoded);
    hex::encode(&hasher.finalize()[..16])
}

fn pack_validity(mask: &[bool]) -> Vec<u8> {
    let mut bits: BitVec<u8, Lsb0> = BitVec::with_capacity(mask.len());
    for &valid in mask {
        bits.push(valid);
    }
    bits.into_vec()
}

fn unpack_validity(bytes: &[u8], row_count: usize) -> Vec<bool> {
    let bits: &bitvec::slice::BitSlice<u8, Lsb0> = bitvec::slice::BitSlice::from_slice(bytes);
    (0..row_count).map(|i| bits[i]).collect()
}

fn validity_byte_len(row_count: usize) -> usize {
    row_count.div_ceil(8)
}

/// Encode one chunk's worth of a column (`data[start..end]`, `validity` sliced
/// the same way if the column is nullable) into its flat byte payload plus the
/// [`ZoneMap`] describing it. `dictionary_codes`, when the column is
/// `Categorical`, must already be the interned `u32` codes for `data[start..end]`
/// (interning — which mutates the column's shared dictionary — happens in
/// [`crate::store`], not here; this module only encodes already-resolved values).
pub fn encode_range(
    data: &ColumnData,
    start: usize,
    end: usize,
    validity: Option<&[bool]>,
    dictionary_codes: Option<&[u32]>,
) -> (Vec<u8>, ZoneMap) {
    let row_count = end - start;
    let null_count = validity
        .map(|v| v.iter().filter(|b| !**b).count())
        .unwrap_or(0) as u32;
    let mut bytes = Vec::new();
    if let Some(mask) = validity {
        bytes.extend(pack_validity(mask));
    }

    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    let mut fold_numeric = |value: f64, valid: bool| {
        if !valid {
            return;
        }
        min = Some(min.map_or(value, |m| m.min(value)));
        max = Some(max.map_or(value, |m| m.max(value)));
    };
    let is_valid = |i: usize| validity.map(|v| v[i]).unwrap_or(true);

    match data {
        ColumnData::F64(values) => {
            for (i, &v) in values[start..end].iter().enumerate() {
                fold_numeric(v, is_valid(i));
                bytes.extend(v.to_le_bytes());
            }
        }
        ColumnData::F32(values) => {
            for (i, &v) in values[start..end].iter().enumerate() {
                fold_numeric(v as f64, is_valid(i));
                bytes.extend(v.to_le_bytes());
            }
        }
        ColumnData::I64(values) => {
            for (i, &v) in values[start..end].iter().enumerate() {
                fold_numeric(v as f64, is_valid(i));
                bytes.extend(v.to_le_bytes());
            }
        }
        ColumnData::U64(values) => {
            for (i, &v) in values[start..end].iter().enumerate() {
                fold_numeric(v as f64, is_valid(i));
                bytes.extend(v.to_le_bytes());
            }
        }
        ColumnData::Bool(values) => {
            let bits = pack_validity(&values[start..end]);
            bytes.extend(bits);
        }
        ColumnData::Utf8(values) => {
            for s in &values[start..end] {
                let len = s.len() as u32;
                bytes.extend(len.to_le_bytes());
                bytes.extend(s.as_bytes());
            }
        }
        ColumnData::Categorical(_) => {
            let codes = dictionary_codes.expect("categorical chunk requires interned codes");
            for &code in codes {
                bytes.extend(code.to_le_bytes());
            }
        }
    }

    (
        bytes,
        ZoneMap {
            row_count: row_count as u32,
            null_count,
            min,
            max,
        },
    )
}

/// Decode a chunk's numeric column back to `f64`, one entry per row. A null slot
/// decodes to `f64::NAN` (the same "skip on render, never a fabricated zero"
/// convention [`crate::store::ColumnStore::materialize_f64`] documents).
pub fn decode_numeric(
    bytes: &[u8],
    row_count: usize,
    nullable: bool,
    width: usize,
    mut read_at: impl FnMut(&[u8]) -> f64,
) -> Vec<f64> {
    let mut offset = 0;
    let validity = if nullable {
        let len = validity_byte_len(row_count);
        let mask = unpack_validity(&bytes[offset..offset + len], row_count);
        offset += len;
        Some(mask)
    } else {
        None
    };
    let mut out = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let value = read_at(&bytes[offset..offset + width]);
        offset += width;
        let valid = validity.as_ref().map(|v| v[i]).unwrap_or(true);
        out.push(if valid { value } else { f64::NAN });
    }
    out
}

pub fn decode_utf8(bytes: &[u8], row_count: usize, nullable: bool) -> Vec<String> {
    let mut offset = 0;
    let validity = if nullable {
        let len = validity_byte_len(row_count);
        let mask = unpack_validity(&bytes[offset..offset + len], row_count);
        offset += len;
        Some(mask)
    } else {
        None
    };
    let mut out = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let valid = validity.as_ref().map(|v| v[i]).unwrap_or(true);
        let s = if valid {
            String::from_utf8_lossy(&bytes[offset..offset + len]).into_owned()
        } else {
            String::new()
        };
        offset += len;
        out.push(s);
    }
    out
}

pub fn decode_bool(bytes: &[u8], row_count: usize, nullable: bool) -> Vec<bool> {
    let mut offset = 0;
    let validity = if nullable {
        let len = validity_byte_len(row_count);
        offset += len;
        Some(unpack_validity(&bytes[..len], row_count))
    } else {
        None
    };
    let value_bytes = &bytes[offset..];
    let values = unpack_validity(value_bytes, row_count);
    (0..row_count)
        .map(|i| {
            let valid = validity.as_ref().map(|v| v[i]).unwrap_or(true);
            valid && values[i]
        })
        .collect()
}

pub fn decode_categorical_codes(bytes: &[u8], row_count: usize, nullable: bool) -> Vec<u32> {
    let mut offset = 0;
    let validity = if nullable {
        let len = validity_byte_len(row_count);
        offset += len;
        Some(unpack_validity(&bytes[..len], row_count))
    } else {
        None
    };
    let mut out = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let code = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let valid = validity.as_ref().map(|v| v[i]).unwrap_or(true);
        out.push(if valid { code } else { NULL_CODE });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f64_chunk_round_trips_without_nulls() {
        let data = ColumnData::F64(vec![1.0, 2.5, -3.0, 4.25]);
        let (bytes, zone) = encode_range(&data, 0, 4, None, None);
        assert_eq!(zone.row_count, 4);
        assert_eq!(zone.null_count, 0);
        assert_eq!(zone.min, Some(-3.0));
        assert_eq!(zone.max, Some(4.25));
        let decoded = decode_numeric(&bytes, 4, false, 8, |b| {
            f64::from_le_bytes(b.try_into().unwrap())
        });
        assert_eq!(decoded, vec![1.0, 2.5, -3.0, 4.25]);
    }

    #[test]
    fn nullable_f64_chunk_round_trips_with_nan_for_null() {
        let data = ColumnData::F64(vec![10.0, 0.0, 30.0]);
        let validity = [true, false, true];
        let (bytes, zone) = encode_range(&data, 0, 3, Some(&validity), None);
        assert_eq!(zone.null_count, 1);
        assert_eq!(zone.min, Some(10.0));
        assert_eq!(zone.max, Some(30.0));
        let decoded = decode_numeric(&bytes, 3, true, 8, |b| {
            f64::from_le_bytes(b.try_into().unwrap())
        });
        assert_eq!(decoded[0], 10.0);
        assert!(decoded[1].is_nan());
        assert_eq!(decoded[2], 30.0);
    }

    #[test]
    fn utf8_chunk_round_trips() {
        let data = ColumnData::Utf8(vec!["a".into(), "bb".into(), "ccc".into()]);
        let (bytes, zone) = encode_range(&data, 0, 3, None, None);
        assert_eq!(zone.row_count, 3);
        let decoded = decode_utf8(&bytes, 3, false);
        assert_eq!(decoded, vec!["a", "bb", "ccc"]);
    }

    #[test]
    fn bool_chunk_round_trips() {
        let data = ColumnData::Bool(vec![true, false, true, true, false]);
        let (bytes, _zone) = encode_range(&data, 0, 5, None, None);
        let decoded = decode_bool(&bytes, 5, false);
        assert_eq!(decoded, vec![true, false, true, true, false]);
    }

    #[test]
    fn categorical_codes_round_trip() {
        let data = ColumnData::Categorical(vec!["x".into(), "y".into(), "x".into()]);
        let codes = [0u32, 1, 0];
        let (bytes, zone) = encode_range(&data, 0, 3, None, Some(&codes));
        assert_eq!(zone.row_count, 3);
        let decoded = decode_categorical_codes(&bytes, 3, false);
        assert_eq!(decoded, vec![0, 1, 0]);
    }

    #[test]
    fn identical_ranges_hash_to_the_same_content_id() {
        let data = ColumnData::F64(vec![1.0; CHUNK_ROWS]);
        let (bytes_a, _) = encode_range(&data, 0, CHUNK_ROWS, None, None);
        let data_b = ColumnData::F64(vec![1.0; CHUNK_ROWS]);
        let (bytes_b, _) = encode_range(&data_b, 0, CHUNK_ROWS, None, None);
        assert_eq!(content_id(&bytes_a), content_id(&bytes_b));
    }

    #[test]
    fn different_ranges_hash_to_different_content_ids() {
        let data = ColumnData::F64((0..CHUNK_ROWS).map(|i| i as f64).collect());
        let (a, _) = encode_range(&data, 0, CHUNK_ROWS, None, None);
        let data2 = ColumnData::F64((0..CHUNK_ROWS).map(|i| i as f64 + 1.0).collect());
        let (b, _) = encode_range(&data2, 0, CHUNK_ROWS, None, None);
        assert_ne!(content_id(&a), content_id(&b));
    }

    #[test]
    fn zone_map_reflects_only_its_own_chunk_range_not_the_whole_column() {
        // ★ proof that zone maps are a per-chunk pruning grain, not a
        // whole-column summary: chunk 0 covers [0, CHUNK_ROWS) and chunk 1 covers
        // [CHUNK_ROWS, 2*CHUNK_ROWS) of a strictly increasing column, so their
        // min/max ranges must be disjoint.
        let values: Vec<f64> = (0..2 * CHUNK_ROWS).map(|i| i as f64).collect();
        let data = ColumnData::F64(values);
        let (_bytes0, zone0) = encode_range(&data, 0, CHUNK_ROWS, None, None);
        let (_bytes1, zone1) = encode_range(&data, CHUNK_ROWS, 2 * CHUNK_ROWS, None, None);
        assert_eq!(zone0.min, Some(0.0));
        assert_eq!(zone0.max, Some((CHUNK_ROWS - 1) as f64));
        assert_eq!(zone1.min, Some(CHUNK_ROWS as f64));
        assert_eq!(zone1.max, Some((2 * CHUNK_ROWS - 1) as f64));
        assert!(zone0.max.unwrap() < zone1.min.unwrap());
    }
}
