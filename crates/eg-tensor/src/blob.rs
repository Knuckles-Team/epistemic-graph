//! The compact byte-blob codec (CONCEPT:EG-085) — a tensor ⇄ `Vec<u8>` serialization
//! whose bytes are content-addressable in the engine's blob CAS (`ChunkStore` +
//! EG-071 CDC). The layout is a small self-describing header + little-endian data:
//!
//! ```text
//! [0]        dtype tag        (u8; see DType::tag)
//! [1..5]     ndim             (u32 LE)
//! [5..5+8n]  shape[0..ndim]   (u64 LE each)
//! [..]       data             (numel × sizeof(dtype), little-endian)
//! ```
//!
//! Deterministic (no map/hash ordering), so the SAME tensor always yields the SAME
//! bytes → the SAME content hash. This is the persisted form; the serde derive on
//! [`Tensor`] is the in-memory/typed-property form.

use crate::tensor::{Buffer, DType, Tensor};

impl Tensor {
    /// Serialize to the compact little-endian byte blob (header + data). The returned
    /// bytes are what the CAS content-addresses.
    pub fn to_blob(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(self.dtype.tag());
        out.extend_from_slice(&(self.shape.len() as u32).to_le_bytes());
        for &d in &self.shape {
            out.extend_from_slice(&(d as u64).to_le_bytes());
        }
        match &self.data {
            Buffer::F32(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Buffer::F64(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Buffer::I32(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Buffer::I64(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Buffer::U8(v) => out.extend_from_slice(v),
        }
        out
    }

    /// Parse a tensor back from the compact byte blob produced by [`Tensor::to_blob`].
    /// Validates the header, dtype tag, and that the data length matches the shape.
    pub fn from_blob(bytes: &[u8]) -> Result<Tensor, String> {
        if bytes.len() < 5 {
            return Err("tensor blob too short for header".into());
        }
        let dtype = DType::from_tag(bytes[0]).ok_or_else(|| format!("bad dtype tag {}", bytes[0]))?;
        let ndim = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        let mut off = 5;
        let need_shape = ndim.checked_mul(8).ok_or("ndim overflow")?;
        if bytes.len() < off + need_shape {
            return Err("tensor blob too short for shape header".into());
        }
        let mut shape = Vec::with_capacity(ndim);
        for _ in 0..ndim {
            let mut b = [0u8; 8];
            b.copy_from_slice(&bytes[off..off + 8]);
            shape.push(u64::from_le_bytes(b) as usize);
            off += 8;
        }
        let numel: usize = shape.iter().product();
        let payload = &bytes[off..];
        let data = decode_data(dtype, numel, payload)?;
        Tensor::new(shape, data)
    }
}

/// Decode `numel` little-endian elements of `dtype` from `payload`.
fn decode_data(dtype: DType, numel: usize, payload: &[u8]) -> Result<Buffer, String> {
    macro_rules! decode {
        ($ty:ty, $sz:expr, $variant:ident) => {{
            let need = numel
                .checked_mul($sz)
                .ok_or("tensor data size overflow")?;
            if payload.len() != need {
                return Err(format!(
                    "tensor data len {} != expected {} ({} elems)",
                    payload.len(),
                    need,
                    numel
                ));
            }
            let mut v = Vec::with_capacity(numel);
            for chunk in payload.chunks_exact($sz) {
                let mut b = [0u8; $sz];
                b.copy_from_slice(chunk);
                v.push(<$ty>::from_le_bytes(b));
            }
            Buffer::$variant(v)
        }};
    }
    Ok(match dtype {
        DType::F32 => decode!(f32, 4, F32),
        DType::F64 => decode!(f64, 8, F64),
        DType::I32 => decode!(i32, 4, I32),
        DType::I64 => decode!(i64, 8, I64),
        DType::U8 => {
            if payload.len() != numel {
                return Err(format!(
                    "tensor u8 data len {} != {} elems",
                    payload.len(),
                    numel
                ));
            }
            Buffer::U8(payload.to_vec())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_round_trip_byte_identical() {
        let cases = vec![
            Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.5, 3.0, 4.0, 5.0, 6.0])).unwrap(),
            Tensor::new(vec![2, 2], Buffer::F64(vec![-1.0, 0.0, 3.5, 1e10])).unwrap(),
            Tensor::new(vec![4], Buffer::I32(vec![-1, 0, 7, i32::MAX])).unwrap(),
            Tensor::new(vec![3], Buffer::I64(vec![i64::MIN, 0, i64::MAX])).unwrap(),
            Tensor::new(vec![2, 2, 2], Buffer::U8((0..8).collect())).unwrap(),
        ];
        for t in cases {
            let blob = t.to_blob();
            let back = Tensor::from_blob(&blob).unwrap();
            // value-identical round-trip
            assert_eq!(t, back, "tensor mismatch after round-trip");
            // BYTE-identical: re-serializing the decoded tensor yields the same bytes
            assert_eq!(blob, back.to_blob(), "blob bytes not byte-identical");
        }
    }

    #[test]
    fn from_blob_rejects_garbage() {
        assert!(Tensor::from_blob(&[]).is_err());
        assert!(Tensor::from_blob(&[9, 0, 0, 0, 0]).is_err()); // bad dtype tag
        // truncated data
        let mut b = Tensor::new(vec![4], Buffer::I32(vec![1, 2, 3, 4]))
            .unwrap()
            .to_blob();
        b.truncate(b.len() - 2);
        assert!(Tensor::from_blob(&b).is_err());
    }
}
