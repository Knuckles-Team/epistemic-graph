//! Backup/restore path validation and staging helpers for the admin handler.

use sha2::{Digest, Sha256};

const BACKUP_ROOT_ENV: &str = "EPISTEMIC_GRAPH_BACKUP_ROOT";

#[cfg(feature = "redb")]
fn backup_root() -> Result<std::path::PathBuf, String> {
    let configured = std::env::var_os(BACKUP_ROOT_ENV)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("backup RPC is disabled; configure {BACKUP_ROOT_ENV}"))?;
    let configured = std::path::PathBuf::from(configured);
    let metadata = std::fs::symlink_metadata(&configured)
        .map_err(|_| "configured backup root is unavailable".to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("configured backup root must be a real directory".to_string());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("configured backup root must have private permissions".to_string());
        }
    }
    configured
        .canonicalize()
        .map_err(|_| "configured backup root is unavailable".to_string())
}

#[cfg(feature = "redb")]
pub(crate) fn backup_bundle_name(value: &str) -> Result<&str, String> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return Err("backup bundle name is required".to_string());
    };
    if value.len() > 128
        || !first.is_ascii_alphanumeric()
        || !chars.all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
        })
    {
        return Err("backup bundle name must be a bounded logical name".to_string());
    }
    Ok(value)
}

#[cfg(feature = "redb")]
pub(crate) fn resolve_backup_destination(
    value: &str,
    request_id: u64,
) -> Result<(std::path::PathBuf, std::path::PathBuf), String> {
    let root = backup_root()?;
    let name = backup_bundle_name(value)?;
    let destination = root.join(name);
    if destination.exists() || std::fs::symlink_metadata(&destination).is_ok() {
        return Err("backup destination already exists".to_string());
    }
    let token = opaque_ref(&format!("backup-stage:{request_id}:{name}"));
    let stage = root.join(format!(
        ".backup-stage-{}",
        token.trim_start_matches("sha256:")
    ));
    if let Ok(metadata) = std::fs::symlink_metadata(&stage) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err("backup staging target is unsafe".to_string());
        }
        std::fs::remove_dir_all(&stage).map_err(|_| "backup staging cleanup failed".to_string())?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder
            .create(&stage)
            .map_err(|_| "create private backup stage failed".to_string())?;
    }
    #[cfg(not(unix))]
    std::fs::create_dir(&stage).map_err(|_| "create private backup stage failed".to_string())?;
    Ok((destination, stage))
}

#[cfg(feature = "redb")]
pub(crate) fn resolve_backup_source(value: &str) -> Result<std::path::PathBuf, String> {
    let root = backup_root()?;
    let name = backup_bundle_name(value)?;
    let candidate = root.join(name);
    let metadata = std::fs::symlink_metadata(&candidate)
        .map_err(|_| "backup source does not exist".to_string())?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err("backup source must be a real directory".to_string());
    }
    let source = candidate
        .canonicalize()
        .map_err(|_| "backup source is unavailable".to_string())?;
    if source.parent() != Some(root.as_path()) {
        return Err("backup source escaped the configured root".to_string());
    }
    Ok(source)
}

#[cfg(feature = "redb")]
pub(crate) fn cleanup_backup_stage(stage: &std::path::Path) {
    if matches!(
        std::fs::symlink_metadata(stage),
        Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink()
    ) {
        let _ = std::fs::remove_dir_all(stage);
    }
}

#[cfg(feature = "redb")]
pub(crate) fn cleanup_restore_retry_stage(stage: &std::path::Path) -> Result<(), String> {
    match std::fs::symlink_metadata(stage) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            Err("staged restore target is not an engine-owned directory".to_string())
        }
        Ok(metadata) if metadata.is_dir() => std::fs::remove_dir_all(stage).map_err(|error| {
            format!(
                "Restore retry cleanup failed; error_ref={}",
                opaque_ref(&error.to_string())
            )
        }),
        Ok(_) => Err("staged restore target is not an engine-owned directory".to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "Restore retry inspection failed; error_ref={}",
            opaque_ref(&error.to_string())
        )),
    }
}

#[cfg(feature = "redb")]
pub(crate) fn create_private_directory(path: &std::path::Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.mode(0o700);
        builder
            .create(path)
            .map_err(|_| "create private engine directory failed".to_string())
    }
    #[cfg(not(unix))]
    {
        std::fs::create_dir(path).map_err(|_| "create private engine directory failed".to_string())
    }
}

/// Wall-clock Unix seconds for the backup/restore RPC. Lives in the handler
/// (application code), never in the library backup/restore_bundle functions.
pub(crate) fn now_secs() -> u64 {
    crate::server::dispatch::authoritative_now_secs()
}

pub(crate) fn opaque_ref(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    format!("sha256:{encoded}")
}
