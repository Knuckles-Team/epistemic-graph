//! The IVF-PQ index's durable image and its bounded identifier sidecar.
//!
//! RF-RULING-007: an index generation becomes durable as an **admitted
//! mutation** on `Native(SemanticIndex)`, not as a directory of files this
//! crate writes for itself. So this module produces and consumes an owned
//! in-memory image and opens nothing: no `File`, no directory, no store. The
//! durable authority is the storage kernel, reached by the holder of a
//! mutation capability (`compute::semantic_ann_codes`).

use super::*;

/// The complete durable image of one `AnnIndex`: eg-ann's three code buffers
/// plus this crate's bounded row-id sidecar (eg-ann is integer-keyed, the
/// sidecar carries the node ids). Owned bytes and nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnIndexImage {
    /// eg-ann's `meta`/`codes`/`refine` buffers.
    pub codes: eg_ann::durable_codes::AnnCodeArtifact,
    /// The versioned row-id sidecar (`encode_ids`).
    pub ids: Vec<u8>,
}

impl AnnIndex {
    /// This index's durable image. Restoring it is `from_image` — NO rebuild.
    pub fn to_image(&self) -> std::io::Result<AnnIndexImage> {
        if self.dim == 0 || self.dim > MAX_MAINTAINED_DIMENSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ANN dimension exceeds the maintained artifact ceiling",
            ));
        }
        Ok(AnnIndexImage {
            codes: eg_ann::durable_codes::encode(&self.index)?,
            ids: encode_ids(&self.row_to_id)?,
        })
    }

    /// Rebuild an index from its durable image WITHOUT retraining from raw
    /// vectors. Every bound the file reader checked is checked here.
    pub fn from_image(image: &AnnIndexImage) -> std::io::Result<Self> {
        let index = eg_ann::durable_codes::decode(&image.codes)?;
        let dim = index.dim;
        validate_loaded_dimension(dim)?;
        let row_to_id = decode_identifier_map(&image.ids)?;
        validate_row_count(&row_to_id, index.len())?;
        let id_to_row = map_live_identifiers(&row_to_id)?;
        Ok(Self {
            index,
            row_to_id,
            id_to_row,
            dim,
        })
    }
}

fn validate_loaded_dimension(dim: usize) -> std::io::Result<()> {
    if dim == 0 || dim > MAX_MAINTAINED_DIMENSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "persisted ANN dimension exceeds the maintained artifact ceiling",
        ));
    }
    Ok(())
}

fn decode_identifier_map(bytes: &[u8]) -> std::io::Result<Vec<String>> {
    if bytes.len() as u64 > MAX_ID_MAP_BYTES {
        return Err(invalid_id_map());
    }
    decode_ids(bytes)
}

fn validate_row_count(ids: &[String], expected: usize) -> std::io::Result<()> {
    if ids.len() != expected {
        return Err(invalid_id_map());
    }
    Ok(())
}

fn map_live_identifiers(row_to_id: &[String]) -> std::io::Result<HashMap<String, u64>> {
    let mut id_to_row = HashMap::with_capacity(row_to_id.len());
    for (row, id) in row_to_id.iter().enumerate() {
        if id.len() > MAX_NODE_ID_BYTES {
            return Err(invalid_id_map());
        }
        if id.is_empty() {
            continue;
        }
        if id_to_row.contains_key(id) {
            return Err(invalid_id_map());
        }
        id_to_row.insert(id.clone(), row as u64);
    }
    Ok(id_to_row)
}

fn encode_ids(ids: &[String]) -> std::io::Result<Vec<u8>> {
    validate_identifier_count(ids)?;
    let encoded_len = identifier_wire_size(ids)?;
    let mut out = Vec::with_capacity(encoded_len);
    out.extend_from_slice(ID_MAP_MAGIC);
    out.extend_from_slice(&(ids.len() as u64).to_le_bytes());
    for id in ids {
        out.extend_from_slice(&(id.len() as u32).to_le_bytes());
        out.extend_from_slice(id.as_bytes());
    }
    Ok(out)
}

fn validate_identifier_count(ids: &[String]) -> std::io::Result<()> {
    if ids.len() > MAX_IDS {
        return Err(invalid_id_map());
    }
    if ids.iter().any(|id| id.len() > MAX_NODE_ID_BYTES) {
        return Err(invalid_id_map());
    }
    Ok(())
}

fn identifier_wire_size(ids: &[String]) -> std::io::Result<usize> {
    let mut size = ID_MAP_MAGIC.len() + std::mem::size_of::<u64>();
    for id in ids {
        size = size
            .checked_add(std::mem::size_of::<u32>())
            .and_then(|length| length.checked_add(id.len()))
            .ok_or_else(invalid_id_map)?;
    }
    if size as u64 > MAX_ID_MAP_BYTES {
        return Err(invalid_id_map());
    }
    Ok(size)
}

fn decode_ids(bytes: &[u8]) -> std::io::Result<Vec<String>> {
    if bytes.len() as u64 > MAX_ID_MAP_BYTES || !bytes.starts_with(ID_MAP_MAGIC) {
        return Err(invalid_id_map());
    }
    let mut offset = ID_MAP_MAGIC.len();
    let count = usize::try_from(read_u64(bytes, &mut offset)?).map_err(|_| invalid_id_map())?;
    if count > MAX_IDS || count > bytes.len().saturating_sub(offset) / 4 {
        return Err(invalid_id_map());
    }
    let mut out = Vec::new();
    out.try_reserve_exact(count).map_err(|_| invalid_id_map())?;
    for _ in 0..count {
        let length = read_u32(bytes, &mut offset)? as usize;
        if length > MAX_NODE_ID_BYTES {
            return Err(invalid_id_map());
        }
        let end = offset.checked_add(length).ok_or_else(invalid_id_map)?;
        let value = bytes.get(offset..end).ok_or_else(invalid_id_map)?;
        out.push(
            std::str::from_utf8(value)
                .map_err(|_| invalid_id_map())?
                .to_owned(),
        );
        offset = end;
    }
    if offset != bytes.len() {
        return Err(invalid_id_map());
    }
    Ok(out)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> std::io::Result<u32> {
    Ok(u32::from_le_bytes(read_fixed(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> std::io::Result<u64> {
    Ok(u64::from_le_bytes(read_fixed(bytes, offset)?))
}

fn read_fixed<const SIZE: usize>(bytes: &[u8], offset: &mut usize) -> std::io::Result<[u8; SIZE]> {
    let end = offset.checked_add(SIZE).ok_or_else(invalid_id_map)?;
    let value = bytes.get(*offset..end).ok_or_else(invalid_id_map)?;
    *offset = end;
    value.try_into().map_err(|_| invalid_id_map())
}

fn invalid_id_map() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "ANN identifier map is invalid or unsupported; rebuild the index",
    )
}

#[cfg(test)]
mod persistence_tests {
    use super::*;

    #[test]
    fn identifier_map_is_versioned_and_round_trips() {
        let ids = vec!["node-a".to_string(), String::new(), "node-c".to_string()];
        let encoded = encode_ids(&ids).unwrap();
        assert!(encoded.starts_with(ID_MAP_MAGIC));
        assert_eq!(decode_ids(&encoded).unwrap(), ids);
    }

    #[test]
    fn identifier_map_rejects_unknown_format_and_trailing_bytes() {
        assert!(decode_ids(&[0; 16]).is_err());

        let mut encoded = encode_ids(&["node-a".to_string()]).unwrap();
        encoded.push(0);
        assert!(decode_ids(&encoded).is_err());
    }

    #[test]
    fn identifier_map_rejects_unbounded_count_before_allocation() {
        let mut encoded = ID_MAP_MAGIC.to_vec();
        encoded.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(decode_ids(&encoded).is_err());
    }
}
