//! Digest-bound readiness manifests for immutable migration snapshots.

use crate::redb_layout::shard_filename;
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};

pub(super) const IN_PLACE_READY_MARKER: &str = ".migration-ready";
pub(super) const READY_MARKER_VERSION: &str = "eg-shard-migration-v2";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadyTarget {
    pub(super) name: String,
    pub(super) sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadyMigration {
    pub(super) backup: PathBuf,
    pub(super) source_names: Vec<String>,
    pub(super) new_k: usize,
    pub(super) targets: Vec<ReadyTarget>,
}

pub(super) fn file_sha256(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path)
        .map_err(|error| format!("open migration artifact {} failed: {error}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(|error| {
            format!("read migration artifact {} failed: {error}", path.display())
        })?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

pub(super) fn expected_target_name(index: usize) -> String {
    shard_filename(index)
}

pub(super) fn parse_ready_marker(
    path: &Path,
    requested_k: usize,
) -> Result<ReadyMigration, String> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        format!(
            "read shard migration readiness marker {} failed: {error}",
            path.display()
        )
    })?;
    let mut lines = text.lines();
    let (backup, new_k) = parse_ready_marker_header(path, &mut lines, requested_k)?;
    let (source_names, targets) = parse_ready_file_manifest(path, lines, new_k)?;
    Ok(ReadyMigration {
        backup,
        source_names,
        new_k,
        targets,
    })
}

/// Bind readiness to the marker format, immutable backup and requested topology.
fn parse_ready_marker_header(
    path: &Path,
    lines: &mut std::str::Lines<'_>,
    requested_k: usize,
) -> Result<(PathBuf, usize), String> {
    if lines.next() != Some(READY_MARKER_VERSION) {
        return Err(format!(
            "shard migration readiness marker {} has an unsupported version",
            path.display()
        ));
    }
    let backup = lines
        .next()
        .and_then(|line| line.strip_prefix("backup="))
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            format!(
                "shard migration readiness marker {} has no backup",
                path.display()
            )
        })?;
    let new_k = lines
        .next()
        .and_then(|line| line.strip_prefix("new_k="))
        .ok_or_else(|| {
            format!(
                "shard migration readiness marker {} has no target K",
                path.display()
            )
        })?
        .parse::<usize>()
        .map_err(|_| {
            format!(
                "shard migration readiness marker {} has an invalid target K",
                path.display()
            )
        })?;
    if new_k != requested_k {
        return Err(format!(
            "shard migration readiness target K {new_k} does not match requested K {requested_k}; preserving live source and temp"
        ));
    }
    Ok((backup, new_k))
}

/// Parse the distinct source/target identities, then require the complete ordered target set.
fn parse_ready_file_manifest(
    path: &Path,
    lines: std::str::Lines<'_>,
    new_k: usize,
) -> Result<(Vec<String>, Vec<ReadyTarget>), String> {
    let mut source_names = Vec::new();
    let mut targets = Vec::new();
    for line in lines {
        if let Some(name) = line.strip_prefix("source=") {
            append_ready_source(path, name, &mut source_names)?;
        } else if let Some(target) = line.strip_prefix("target=") {
            append_ready_target(path, target, &mut targets)?;
        } else {
            return Err(format!(
                "shard migration readiness marker {} has an unknown entry",
                path.display()
            ));
        }
    }
    if source_names.is_empty() || targets.len() != new_k {
        return Err(format!(
            "shard migration readiness marker {} has an incomplete file manifest",
            path.display()
        ));
    }
    for (index, target) in targets.iter().enumerate() {
        if target.name != expected_target_name(index) {
            return Err(format!(
                "shard migration readiness marker {} has a non-contiguous target manifest",
                path.display()
            ));
        }
    }
    Ok((source_names, targets))
}

/// Admit one basename-only source identity; duplicates cannot name a second authority.
fn append_ready_source(
    path: &Path,
    name: &str,
    source_names: &mut Vec<String>,
) -> Result<(), String> {
    if name.is_empty()
        || Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name)
        || source_names.iter().any(|existing| existing == name)
    {
        return Err(format!(
            "shard migration readiness marker {} has an invalid source manifest",
            path.display()
        ));
    }
    source_names.push(name.to_string());
    Ok(())
}

/// Admit one unique target basename and normalize its exact SHA-256 binding.
fn append_ready_target(
    path: &Path,
    target: &str,
    targets: &mut Vec<ReadyTarget>,
) -> Result<(), String> {
    let (name, sha256) = target.split_once('\t').ok_or_else(|| {
        format!(
            "shard migration readiness marker {} has an invalid target manifest",
            path.display()
        )
    })?;
    if name.is_empty()
        || Path::new(name).file_name().and_then(|value| value.to_str()) != Some(name)
        || targets.iter().any(|existing| existing.name == name)
        || sha256.len() != 64
        || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(format!(
            "shard migration readiness marker {} has an invalid target manifest",
            path.display()
        ));
    }
    targets.push(ReadyTarget {
        name: name.to_string(),
        sha256: sha256.to_ascii_lowercase(),
    });
    Ok(())
}

pub(super) fn write_ready_marker(
    tmp: &Path,
    backup: &Path,
    src_paths: &[PathBuf],
    new_k: usize,
) -> Result<ReadyMigration, String> {
    let mut marker = format!(
        "{READY_MARKER_VERSION}\nbackup={}\nnew_k={new_k}\n",
        backup.display()
    );
    for source in src_paths {
        let name = source
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| format!("source shard has no filename: {}", source.display()))?;
        marker.push_str("source=");
        marker.push_str(name);
        marker.push('\n');
    }
    for index in 0..new_k {
        let name = expected_target_name(index);
        let staged = tmp.join(&name);
        let sha256 = file_sha256(&staged)?;
        marker.push_str("target=");
        marker.push_str(&name);
        marker.push('\t');
        marker.push_str(&sha256);
        marker.push('\n');
    }
    let path = tmp.join(IN_PLACE_READY_MARKER);
    std::fs::write(&path, marker.as_bytes()).map_err(|error| {
        format!(
            "write shard migration readiness marker {} failed: {error}",
            path.display()
        )
    })?;
    parse_ready_marker(&path, new_k)
}
