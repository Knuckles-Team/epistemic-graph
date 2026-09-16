//! Immutable source snapshots and fresh-or-ready in-place migration.

use super::readiness::{
    parse_ready_marker, write_ready_marker, ReadyMigration, IN_PLACE_READY_MARKER,
};
use super::swap::swap_ready_shards;
use super::{discover_source_shards, migrate_shards_inner, MigrationFault, MigrationReport};
use crate::redb_layout::validate_shard_count;
use std::path::{Path, PathBuf};

fn unique_backup_dir(base: &Path) -> Result<PathBuf, String> {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    for attempt in 0..100u32 {
        let candidate = base.join(format!(".shard-migrate-backup-{}-{attempt}", stamp));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }
    Err("could not allocate a unique shard migration backup directory".to_string())
}

fn copy_source_snapshot(src_paths: &[PathBuf], snapshot: &Path) -> Result<(), String> {
    std::fs::create_dir_all(snapshot).map_err(|error| {
        format!(
            "create immutable shard migration source snapshot {} failed: {error}",
            snapshot.display()
        )
    })?;
    for source in src_paths {
        let name = source
            .file_name()
            .ok_or_else(|| format!("source shard has no filename: {}", source.display()))?;
        std::fs::copy(source, snapshot.join(name)).map_err(|error| {
            format!(
                "copy source shard {} into immutable snapshot failed: {error}",
                source.display()
            )
        })?;
    }
    Ok(())
}

/// Migrate the store under `persist_dir` to `new_k` in place
/// (CONCEPT:EG-KG.sharding.atomic-shard-swap). Preserve an immutable source
/// snapshot, build a ready destination from its working copy, then install it.
pub fn migrate_in_place(persist_dir: &str, new_k: usize) -> Result<MigrationReport, String> {
    migrate_in_place_inner(persist_dir, new_k, None)
}

#[cfg(test)]
pub(super) fn migrate_in_place_after_graph_for_test(
    persist_dir: &str,
    new_k: usize,
    after_graphs: usize,
) -> Result<MigrationReport, String> {
    migrate_in_place_inner(
        persist_dir,
        new_k,
        Some(MigrationFault::AfterGraph(after_graphs)),
    )
}

#[cfg(test)]
pub(super) fn migrate_in_place_before_swap_for_test(
    persist_dir: &str,
    new_k: usize,
) -> Result<MigrationReport, String> {
    migrate_in_place_inner(persist_dir, new_k, Some(MigrationFault::BeforeSwap))
}

#[cfg(test)]
pub(super) fn migrate_in_place_after_old_move_for_test(
    persist_dir: &str,
    new_k: usize,
    after_files: usize,
) -> Result<MigrationReport, String> {
    migrate_in_place_inner(
        persist_dir,
        new_k,
        Some(MigrationFault::AfterOldMove(after_files)),
    )
}

#[cfg(test)]
pub(super) fn migrate_in_place_after_install_for_test(
    persist_dir: &str,
    new_k: usize,
    after_files: usize,
) -> Result<MigrationReport, String> {
    migrate_in_place_inner(
        persist_dir,
        new_k,
        Some(MigrationFault::AfterInstall(after_files)),
    )
}

fn migrate_in_place_inner(
    persist_dir: &str,
    new_k: usize,
    fault: Option<MigrationFault>,
) -> Result<MigrationReport, String> {
    let new_k = validate_shard_count(new_k)?;
    let base = Path::new(persist_dir);
    let tmp = base.join(".shard-migrate-tmp");
    let (ready, report) = if tmp.exists() {
        resume_ready_migration(base, &tmp, new_k)?
    } else {
        build_ready_migration(base, &tmp, new_k, fault)?
    };
    swap_ready_shards(base, &tmp, &ready, fault)?;
    tracing::info!(
        "shard migration complete: {} -> {} shards, {} graphs; immutable source backup at {}",
        report.source_shards,
        report.dest_shards,
        report.graphs,
        ready.backup.display()
    );
    Ok(report)
}

/// Resume only a complete ready tree bound to an immutable backup in this live store.
fn resume_ready_migration(
    base: &Path,
    tmp: &Path,
    new_k: usize,
) -> Result<(ReadyMigration, MigrationReport), String> {
    let marker = tmp.join(IN_PLACE_READY_MARKER);
    if !marker.exists() {
        return Err(format!(
            "shard migration temp tree {} is incomplete; preserving it and the live source for recovery",
            tmp.display()
        ));
    }
    let ready = parse_ready_marker(&marker, new_k)?;
    if ready.backup.parent() != Some(base) {
        return Err(format!(
            "ready shard migration backup {} is outside the live store; preserving source and temp",
            ready.backup.display()
        ));
    }
    if !ready.backup.join("source").is_dir() {
        return Err(format!(
            "ready shard migration references missing source backup {}",
            ready.backup.display()
        ));
    }
    // The report is rebuilt from the ready destination's durable files only
    // after the swap.  A resume therefore returns a conservative report with
    // source/destination topology; row counters are not used for recovery.
    let report = MigrationReport {
        source_shards: ready.source_names.len(),
        dest_shards: new_k,
        dest_raft_groups: new_k,
        ..MigrationReport::default()
    };
    Ok((ready, report))
}

/// Build from a disposable working copy while retaining the pristine source evidence.
fn build_ready_migration(
    base: &Path,
    tmp: &Path,
    new_k: usize,
    fault: Option<MigrationFault>,
) -> Result<(ReadyMigration, MigrationReport), String> {
    let src_paths = discover_source_shards(base)?;
    let backup = unique_backup_dir(base)?;
    let snapshot = backup.join("source");
    copy_source_snapshot(&src_paths, &snapshot)?;
    let build_source = backup.join("build-source");
    let snapshot_paths: Vec<PathBuf> = src_paths
        .iter()
        .map(|source| {
            let name = source
                .file_name()
                .ok_or_else(|| format!("source shard has no filename: {}", source.display()))?;
            Ok(snapshot.join(name))
        })
        .collect::<Result<_, String>>()?;
    // The working copy must be rebound to its new physical root before grafting.
    // The immutable snapshot stays byte-identical evidence and is never opened or
    // rebound here. Both copies still carry the ORIGINAL source root, so each
    // working copy is rebound against the live source, not the intermediate snapshot.
    copy_source_snapshot(&snapshot_paths, &build_source)?;
    for source in &src_paths {
        let name = source
            .file_name()
            .ok_or_else(|| format!("source shard has no filename: {}", source.display()))?;
        let copy = build_source.join(name);
        eg_storage::rebind_copied_store(&copy, source).map_err(|error| {
            format!(
                "rebind migration build source {} failed: {error}",
                copy.display()
            )
        })?;
    }
    std::fs::create_dir_all(tmp).map_err(|error| error.to_string())?;
    let report = match migrate_shards_inner(&build_source, tmp, new_k, fault) {
        Ok(report) => report,
        Err(error) => {
            // Preserve both the immutable source snapshot and any partial
            // destination rows.  A later operator can inspect or remove
            // the named tree explicitly; this function never destroys it.
            return Err(format!(
                "in-place shard migration build failed; source snapshot {}, build source {} and temp {} preserved: {error}",
                snapshot.display(),
                build_source.display(),
                tmp.display()
            ));
        }
    };
    if let Err(error) = std::fs::remove_dir_all(&build_source) {
        tracing::warn!(
            "leaving consumed shard migration build snapshot {} after cleanup failure: {error}",
            build_source.display()
        );
    }
    let ready = write_ready_marker(tmp, &backup, &src_paths, new_k)?;
    if matches!(fault, Some(MigrationFault::BeforeSwap)) {
        return Err(format!(
            "injected migration fault before swap; readiness marker {} preserved",
            tmp.join(IN_PLACE_READY_MARKER).display()
        ));
    }
    Ok((ready, report))
}
