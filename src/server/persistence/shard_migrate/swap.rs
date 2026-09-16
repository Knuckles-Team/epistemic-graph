//! Preflight and resumable installation of a complete ready shard tree.

use super::readiness::{file_sha256, ReadyMigration, ReadyTarget};
use super::MigrationFault;
use std::path::Path;

pub(super) fn swap_ready_shards(
    base: &Path,
    tmp: &Path,
    ready: &ReadyMigration,
    fault: Option<MigrationFault>,
) -> Result<(), String> {
    if ready.new_k != ready.targets.len() {
        return Err("ready migration target manifest does not match its target K".to_string());
    }
    let old = ready.backup.join("old");
    std::fs::create_dir_all(&old).map_err(|error| error.to_string())?;
    let source_backup = ready.backup.join("source");

    // Finish the entire preflight before any live-source rename. A foreign target
    // leaves the old layout and the ready temp tree untouched.
    for target in &ready.targets {
        validate_ready_swap_target(base, tmp, &source_backup, ready, target)?;
    }
    move_old_source_shards(base, &old, &source_backup, ready, fault)?;
    install_ready_target_shards(base, tmp, ready, fault)?;
    std::fs::remove_dir_all(tmp).map_err(|error| {
        format!(
            "remove completed shard migration temp tree {} failed: {error}",
            tmp.display()
        )
    })?;
    Ok(())
}

/// Before moving sources, authenticate each target or its still-live source predecessor.
fn validate_ready_swap_target(
    base: &Path,
    tmp: &Path,
    source_backup: &Path,
    ready: &ReadyMigration,
    target: &ReadyTarget,
) -> Result<(), String> {
    let staged = tmp.join(&target.name);
    let installed = base.join(&target.name);
    if staged.exists() && file_sha256(&staged)? != target.sha256 {
        return Err(format!(
            "staged shard {} does not match its ready digest",
            staged.display()
        ));
    }
    if installed.exists() {
        let installed_digest = file_sha256(&installed)?;
        if installed_digest != target.sha256 {
            let source_digest = if ready.source_names.iter().any(|name| name == &target.name) {
                file_sha256(&source_backup.join(&target.name))?
            } else {
                String::new()
            };
            if installed_digest != source_digest {
                return Err(format!(
                    "installed shard {} is neither its ready target nor the original source",
                    installed.display()
                ));
            }
        }
    } else if !staged.exists() && !ready.source_names.iter().any(|name| name == &target.name) {
        return Err(format!(
            "ready migration is missing staged and installed target {}",
            target.name
        ));
    }
    Ok(())
}

/// Retain authentic already-moved sources, recognizing installed targets on retry.
fn validate_backed_up_source(
    old_path: &Path,
    source: &Path,
    source_digest: &str,
    ready: &ReadyMigration,
    name: &str,
) -> Result<(), String> {
    if file_sha256(old_path)? != source_digest {
        return Err(format!(
            "migration recovery backup {} does not match immutable source",
            old_path.display()
        ));
    }
    if source.exists() {
        let installed_target = ready
            .targets
            .iter()
            .find(|target| target.name == name)
            .is_some_and(|target| {
                file_sha256(source).ok().as_deref() == Some(target.sha256.as_str())
            });
        if !installed_target {
            return Err(format!(
                "migration recovery has both old and live source file {}",
                source.display()
            ));
        }
    }
    Ok(())
}

/// Move one original source aside, returning true only for an actual rename.
fn move_one_old_source_shard(
    base: &Path,
    old: &Path,
    source_backup: &Path,
    ready: &ReadyMigration,
    name: &str,
) -> Result<bool, String> {
    let source = base.join(name);
    let old_path = old.join(name);
    let source_digest = file_sha256(&source_backup.join(name))?;
    if old_path.exists() {
        validate_backed_up_source(&old_path, &source, &source_digest, ready, name)?;
        return Ok(false);
    }
    if !source.exists() {
        return Ok(false);
    }
    let source_digest_now = file_sha256(&source)?;
    let already_installed = ready
        .targets
        .iter()
        .find(|target| target.name == name)
        .is_some_and(|target| source_digest_now == target.sha256);
    if already_installed {
        return Ok(false);
    }
    if source_digest_now != source_digest {
        return Err(format!(
            "live shard {} is neither the immutable source nor its ready target",
            source.display()
        ));
    }
    std::fs::rename(&source, &old_path).map_err(|error| {
        format!(
            "move old shard {} into recovery backup failed: {error}",
            source.display()
        )
    })?;
    Ok(true)
}

/// Move the whole old layout before installation, counting only completed source renames.
fn move_old_source_shards(
    base: &Path,
    old: &Path,
    source_backup: &Path,
    ready: &ReadyMigration,
    fault: Option<MigrationFault>,
) -> Result<(), String> {
    let mut moved_old = 0usize;
    for name in &ready.source_names {
        if !move_one_old_source_shard(base, old, source_backup, ready, name)? {
            continue;
        }
        moved_old = moved_old.saturating_add(1);
        if fault.is_some_and(
            |fault| matches!(fault, MigrationFault::AfterOldMove(limit) if moved_old >= limit),
        ) {
            return Err(format!(
                "injected migration fault after {moved_old} old shard move(s)"
            ));
        }
    }
    Ok(())
}

/// Install one authenticated target, or verify both copies when it is already installed.
fn install_one_ready_target_shard(
    base: &Path,
    tmp: &Path,
    target: &ReadyTarget,
) -> Result<bool, String> {
    let staged = tmp.join(&target.name);
    let installed = base.join(&target.name);
    if installed.exists() {
        if file_sha256(&installed)? != target.sha256 {
            return Err(format!(
                "installed shard {} does not match its ready digest",
                installed.display()
            ));
        }
        if staged.exists() && file_sha256(&staged)? != target.sha256 {
            return Err(format!(
                "staged shard {} does not match its ready digest",
                staged.display()
            ));
        }
        return Ok(false);
    }
    if !staged.exists() {
        return Err(format!(
            "ready migration is missing both staged and installed shard {}",
            target.name
        ));
    }
    if file_sha256(&staged)? != target.sha256 {
        return Err(format!(
            "staged shard {} does not match its ready digest",
            staged.display()
        ));
    }
    std::fs::rename(&staged, &installed).map_err(|error| {
        format!(
            "install migrated shard {} failed: {error}",
            installed.display()
        )
    })?;
    Ok(true)
}

/// Install the complete new layout before deleting readiness/recovery state.
fn install_ready_target_shards(
    base: &Path,
    tmp: &Path,
    ready: &ReadyMigration,
    fault: Option<MigrationFault>,
) -> Result<(), String> {
    let mut installed_targets = 0usize;
    for target in &ready.targets {
        if !install_one_ready_target_shard(base, tmp, target)? {
            continue;
        }
        installed_targets = installed_targets.saturating_add(1);
        if fault.is_some_and(|fault| {
            matches!(fault, MigrationFault::AfterInstall(limit) if installed_targets >= limit)
        }) {
            return Err(format!(
                "injected migration fault after {installed_targets} target install(s)"
            ));
        }
    }
    Ok(())
}
