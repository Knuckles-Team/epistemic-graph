//! Persisted IVF-PQ artifacts and their bounded identifier sidecar.

use super::*;
use std::fs::File;
use std::io::Write;
use std::path::Path;

impl AnnIndex {
    /// Persist the index (codes + meta) to `dir`. Reopen is `load` — NO rebuild.
    pub fn save(&self, dir: &Path) -> std::io::Result<()> {
        if self.dim == 0 || self.dim > MAX_MAINTAINED_DIMENSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "ANN dimension exceeds the maintained artifact ceiling",
            ));
        }
        eg_ann::save(&self.index, dir)?;
        let map_bytes = encode_ids(&self.row_to_id)?;
        write_atomic(&dir.join("ids.bin"), &map_bytes)
    }

    /// Width of the maintained ANN artifact, used by the store reopen checks.
    pub(crate) fn dim(&self) -> usize {
        self.dim
    }

    /// Integer row ids in persisted order, used for exact member-set checks.
    pub(crate) fn row_ids(&self) -> &[String] {
        &self.row_to_id
    }

    /// Reopen a persisted index WITHOUT rebuilding from raw vectors.
    pub fn load(dir: &Path) -> std::io::Result<Self> {
        let index = eg_ann::open(dir)?;
        let dim = index.dim;
        validate_loaded_dimension(dim)?;
        let id_path = dir.join("ids.bin");
        let row_to_id = read_identifier_map(&id_path)?;
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

fn read_identifier_map(path: &Path) -> std::io::Result<Vec<String>> {
    if std::fs::metadata(path)?.len() > MAX_ID_MAP_BYTES {
        return Err(invalid_id_map());
    }
    decode_ids(&std::fs::read(path)?)
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

fn write_atomic(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let temporary = path.with_extension("tmp");
    {
        let mut file = File::create(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(temporary, path)
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
