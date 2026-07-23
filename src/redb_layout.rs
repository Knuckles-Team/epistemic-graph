//! Canonical authoritative redb shard layout shared by server and embedded modes.

use std::path::{Path, PathBuf};

/// Unindexed filename accepted only by the explicit offline migrator.
pub(crate) const RETIRED_SINGLE_SHARD_FILENAME: &str = "graph.redb";
/// Hard resource ceiling shared by startup, restore, and migration.
pub(crate) const MAX_SHARD_COUNT: usize = 64;

/// Validate the process-wide shard-count ceiling before allocating files or writers.
pub(crate) fn validate_shard_count(count: usize) -> Result<usize, String> {
    if !(1..=MAX_SHARD_COUNT).contains(&count) {
        return Err(format!(
            "redb shard count must be in 1..={MAX_SHARD_COUNT}, got {count}"
        ));
    }
    Ok(count)
}

/// The sole current filename for an authoritative shard.
pub(crate) fn shard_filename(index: usize) -> String {
    format!("graph-{index}.redb")
}

/// Discover and validate current indexed shard files without considering the retired
/// unindexed filename. An empty result means a fresh store.
pub(crate) fn discover_indexed_shards(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|error| format!("read redb persist directory failed: {error}"))?;
    let mut indexed = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| format!("read redb directory entry failed: {error}"))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(rest) = name.strip_prefix("graph-") else {
            continue;
        };
        let Some(number) = rest.strip_suffix(".redb") else {
            continue;
        };
        let file_type = entry
            .file_type()
            .map_err(|error| format!("inspect redb shard failed: {error}"))?;
        if !file_type.is_file() {
            return Err(format!("redb shard {name:?} must be a regular file"));
        }
        let index = number.parse::<usize>().map_err(|_| {
            format!("invalid redb shard filename {name:?}; expected graph-<index>.redb")
        })?;
        let canonical = shard_filename(index);
        if name.as_ref() != canonical.as_str() {
            return Err(format!(
                "non-canonical redb shard filename {name:?}; expected {:?}",
                canonical
            ));
        }
        indexed.push((index, entry.path()));
    }

    indexed.sort_by_key(|(index, _)| *index);
    for (expected, (actual, _)) in indexed.iter().enumerate() {
        if *actual != expected {
            return Err(format!(
                "non-contiguous redb shard layout: expected graph-{expected}.redb, found graph-{actual}.redb"
            ));
        }
    }
    Ok(indexed.into_iter().map(|(_, path)| path).collect())
}

/// Inspect the retired K=1 filename. The offline migrator may consume a regular file;
/// every runtime path rejects the returned value.
pub(crate) fn retired_single_shard(dir: &Path) -> Result<Option<PathBuf>, String> {
    let path = dir.join(RETIRED_SINGLE_SHARD_FILENAME);
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) => {
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err("retired redb shard must be a regular file".to_string());
            }
            Ok(Some(path))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("inspect retired redb shard failed: {error}")),
    }
}

/// Discover the current layout and reject the retired filename even when current files
/// also exist. This is the only discovery function runtime-adjacent code may call.
pub(crate) fn discover_current_shards(dir: &Path) -> Result<Vec<PathBuf>, String> {
    if retired_single_shard(dir)?.is_some() {
        return Err(
            "retired redb layout detected (graph.redb); run the offline migrate-shards command before startup"
                .to_string(),
        );
    }
    discover_indexed_shards(dir)
}

/// Honor an existing current K, or use `requested_k` for a fresh store.
pub(crate) fn reconcile_shard_layout(dir: &Path, requested_k: usize) -> Result<usize, String> {
    let requested_k = validate_shard_count(requested_k)?;
    let shards = discover_current_shards(dir)?;
    if shards.is_empty() {
        Ok(requested_k)
    } else {
        validate_shard_count(shards.len())?;
        Ok(shards.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_count_is_bounded_and_filename_is_always_indexed() {
        assert!(validate_shard_count(0).is_err());
        assert_eq!(validate_shard_count(1), Ok(1));
        assert_eq!(validate_shard_count(MAX_SHARD_COUNT), Ok(MAX_SHARD_COUNT));
        assert!(validate_shard_count(MAX_SHARD_COUNT + 1).is_err());
        assert_eq!(shard_filename(0), "graph-0.redb");
    }
}
