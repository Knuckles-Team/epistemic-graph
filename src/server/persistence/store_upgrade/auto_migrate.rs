//! Automatic migration of an older but supported store when the engine opens
//! its data directory (`MEDULLA-DURABLE-KERNEL-R070.1`).
//!
//! The engine runs the same registered upgrades as `store-upgrade apply`,
//! through the same per-store inspection and single-commit transition. It
//! adds four things around them:
//!
//! * a [`PersistDirLock`] parameter, so only the holder of the engine's
//!   exclusive lock can call it;
//! * a crash-safe journal, written before the first upgrade and updated after
//!   each one, so an interrupted run is detected and resumed at the next open;
//! * one log line and one metric sample per step;
//! * typed refusals for a store that is too old or too new to migrate.
//!
//! Every upgrade is one atomic commit, and an interrupted upgrade leaves the
//! file in its previous layout. A resumed run therefore classifies every store
//! again and continues; a store that an earlier run upgraded is current and is
//! not written again.

use std::ops::ControlFlow;
use std::path::{Path, PathBuf};

use medulla_kernel::persist_lock::PersistDirLock;
use medulla_storage::{classify_owner_store_format, OwnerLayout, OwnerStoreFormat};
use serde::{Deserialize, Serialize};

use super::discover::{discover, StoreFile};
use super::{apply_store, existing_directory, ApplyOptions, Staging, StoreReport, StoreStatus};
use super::{DEFAULT_STAGING_DIRECTORY, STAGING_SCRATCH_DIRECTORY};

/// The switch. Unset or `true` turns automatic migration on.
pub const AUTO_MIGRATE_ENV: &str = "MEDULLA_STORE_AUTO_MIGRATE";
/// Private local staging directory of an automatic migration. It has the same
/// rules as `store-upgrade apply --staging-dir`.
pub const AUTO_MIGRATE_STAGING_ENV: &str = "MEDULLA_STORE_UPGRADE_STAGING_DIR";
/// The journal file, inside the private staging directory.
const JOURNAL_FILE: &str = "auto-migrate.journal.json";
const JOURNAL_VERSION: u32 = 1;

/// Why the engine did not migrate, or could not finish migrating, a store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoMigrationError {
    /// A declared predecessor that no registered upgrade starts from.
    TooOld { file: String, detail: String },
    /// A layout outside this build's lineage: a newer build wrote it, or it is
    /// corrupt. The engine never guesses which.
    TooNew { file: String, layout: &'static str },
    /// An admitted upgrade was refused, or the journal could not be written.
    /// The store keeps its previous layout and the journal stays in place.
    Failed { file: String, detail: String },
    /// The switch has a value that is neither on nor off.
    InvalidSwitch(String),
}

impl AutoMigrationError {
    /// The stable error code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::TooOld { .. } => "STORE_FORMAT_TOO_OLD",
            Self::TooNew { .. } => "STORE_FORMAT_TOO_NEW",
            Self::Failed { .. } => "STORE_AUTO_MIGRATE_FAILED",
            Self::InvalidSwitch(_) => "STORE_AUTO_MIGRATE_INVALID_SWITCH",
        }
    }
}

impl std::fmt::Display for AutoMigrationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = self.code();
        match self {
            Self::TooOld { file, detail } => write!(
                formatter,
                "{code}: {file} is in a format this build cannot migrate: {detail}"
            ),
            Self::TooNew { file, layout } => write!(
                formatter,
                "{code}: the {layout} store {file} has a layout that is not in this build's \
                 lineage; a newer build wrote it or it is corrupt, and it is left untouched"
            ),
            Self::Failed { file, detail } => write!(
                formatter,
                "{code}: {file}: {detail}. The store keeps its previous layout; the next start \
                 resumes the migration, or set {AUTO_MIGRATE_ENV}=false and run the \
                 store-upgrade command"
            ),
            Self::InvalidSwitch(value) => write!(
                formatter,
                "{code}: {AUTO_MIGRATE_ENV}={value:?} is not one of true, false, 1, 0, on, off"
            ),
        }
    }
}

/// What one automatic migration did.
#[derive(Debug, Default)]
pub struct AutoMigration {
    /// The journal of an interrupted earlier run was found and resumed.
    pub resumed: bool,
    /// One line per store the run upgraded, or found current after a resume.
    pub stores: Vec<StoreReport>,
    /// Stores the run refused to migrate. The engine's ordinary open of each
    /// refuses it again with its named format error.
    pub refused: Vec<Refused>,
}

/// One store the run refused to migrate.
#[derive(Debug)]
pub struct Refused {
    pub error: AutoMigrationError,
    /// A graph shard, which the engine opens eagerly: its refusal stops
    /// startup. Any other store keeps the local refusal of its lazy open.
    pub blocks_startup: bool,
}

impl AutoMigration {
    /// The refusal that stops startup: a graph shard, which the engine opens
    /// eagerly, in a format it cannot migrate. A refused lazily opened store
    /// keeps the local refusal of its own open.
    pub fn startup_refusal(&self) -> Option<&AutoMigrationError> {
        self.refused
            .iter()
            .find(|refused| refused.blocks_startup)
            .map(|refused| &refused.error)
    }
}

/// Whether the switch turns automatic migration on. Unset means on.
pub fn auto_migrate_enabled() -> Result<bool, AutoMigrationError> {
    parse_switch(std::env::var(AUTO_MIGRATE_ENV).ok().as_deref())
}

pub(super) fn parse_switch(value: Option<&str>) -> Result<bool, AutoMigrationError> {
    let Some(value) = value else {
        return Ok(true);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "1" | "true" | "on" | "yes" => Ok(true),
        "0" | "false" | "off" | "no" => Ok(false),
        _ => Err(AutoMigrationError::InvalidSwitch(value.to_string())),
    }
}

/// Migrate every older but supported store under `data_dir` before the engine
/// opens it. The caller proves it holds the engine's exclusive lock.
pub fn migrate_on_open(
    data_dir: &Path,
    lock: &PersistDirLock,
) -> Result<AutoMigration, AutoMigrationError> {
    migrate_with(data_dir, lock, staging_options(), |_| {
        ControlFlow::Continue(())
    })
}

fn staging_options() -> ApplyOptions {
    ApplyOptions {
        confirm: true,
        staging_dir: std::env::var_os(AUTO_MIGRATE_STAGING_ENV)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from),
        max_store_bytes: None,
    }
}

/// The run, with a hook after each upgraded store. A test stops the run there
/// to leave exactly the state a crash between two upgrades leaves.
pub(super) fn migrate_with(
    data_dir: &Path,
    _lock: &PersistDirLock,
    options: ApplyOptions,
    mut after_store: impl FnMut(&StoreReport) -> ControlFlow<()>,
) -> Result<AutoMigration, AutoMigrationError> {
    let failed = |detail: String| AutoMigrationError::Failed {
        file: data_dir.display().to_string(),
        detail,
    };
    let data_dir = existing_directory(data_dir).map_err(failed)?;
    let journal_path = journal_path(&data_dir).map_err(failed)?;
    let mut migration = AutoMigration {
        resumed: journal_path.exists(),
        ..AutoMigration::default()
    };
    if migration.resumed {
        tracing::warn!(
            journal = %journal_path.display(),
            "store migration: resuming an interrupted migration"
        );
        medulla_kernel::metrics::store_migration_step("resume", "found");
    }
    let stores = discover(&data_dir).map_err(failed)?;
    let pending = plan(&stores, &mut migration);
    if pending.is_empty() {
        clear_journal(&journal_path).map_err(failed)?;
        finish(&migration);
        return Ok(migration);
    }
    create_journal_directory(&data_dir).map_err(failed)?;
    let mut journal = Journal::planned(&pending);
    journal.write(&journal_path).map_err(failed)?;
    medulla_kernel::metrics::store_migration_step("journal", "written");
    tracing::info!(
        stores = pending.len(),
        journal = %journal_path.display(),
        "store migration: journal written; migrating stores"
    );
    let staging = Staging::new(&data_dir, &options);
    for (index, store) in pending.iter().enumerate() {
        let report = migrate_store(store, &staging)?;
        journal.stores[index].state = JournalState::Upgraded;
        journal.write(&journal_path).map_err(failed)?;
        let flow = after_store(&report);
        migration.stores.push(report);
        if flow.is_break() {
            return Ok(migration);
        }
    }
    clear_journal(&journal_path).map_err(failed)?;
    medulla_kernel::metrics::store_migration_step("journal", "cleared");
    finish(&migration);
    Ok(migration)
}

/// Classify every store once. Stores a registered upgrade applies to are
/// returned in report order; refusals are recorded and nothing is written.
fn plan<'a>(stores: &'a [StoreFile], migration: &mut AutoMigration) -> Vec<&'a StoreFile> {
    let mut pending = Vec::new();
    for store in stores {
        match classify_owner_store_format(&store.path) {
            Ok(OwnerStoreFormat::UpgradeAvailable(_)) => pending.push(store),
            Ok(OwnerStoreFormat::NoUpgrade(predecessor)) => refuse(
                migration,
                store,
                AutoMigrationError::TooOld {
                    file: store.name.clone(),
                    detail: format!("{}. {}", predecessor.label, predecessor.operator_step()),
                },
            ),
            Ok(OwnerStoreFormat::Unknown(layout)) => refuse(
                migration,
                store,
                AutoMigrationError::TooNew {
                    file: store.name.clone(),
                    layout: OwnerLayout::canonical_name(layout),
                },
            ),
            // A current store needs nothing. A store whose manifest cannot be
            // read read-only (an unclean shutdown) is recovered by its ordinary
            // open, which refuses it by name if it is still a predecessor.
            Ok(OwnerStoreFormat::Current(_)) | Err(_) => {}
        }
    }
    pending
}

fn refuse(migration: &mut AutoMigration, store: &StoreFile, error: AutoMigrationError) {
    tracing::error!(code = error.code(), "store migration: {error}");
    medulla_kernel::metrics::store_migration_step("store", error.code());
    migration.refused.push(Refused {
        error,
        blocks_startup: store.served_layout == Some(OwnerLayout::GraphShard),
    });
}

fn migrate_store(store: &StoreFile, staging: &Staging) -> Result<StoreReport, AutoMigrationError> {
    tracing::info!(store = %store.name, "store migration: upgrading store");
    let report = apply_store(store, staging);
    match report.status {
        StoreStatus::Upgraded | StoreStatus::Current => {
            tracing::info!(store = %store.name, "store migration: {}", report.detail);
            medulla_kernel::metrics::store_migration_step("store", "upgraded");
            Ok(report)
        }
        _ => {
            medulla_kernel::metrics::store_migration_step("store", "failed");
            let refusal = AutoMigrationError::Failed {
                file: store.name.clone(),
                detail: report.detail,
            };
            tracing::error!(code = refusal.code(), "store migration: {refusal}");
            Err(refusal)
        }
    }
}

fn finish(migration: &AutoMigration) {
    let outcome = match (migration.stores.is_empty(), migration.refused.is_empty()) {
        (_, false) => "refused",
        (true, true) => "nothing_to_do",
        (false, true) => "migrated",
    };
    tracing::info!(
        upgraded = migration.stores.len(),
        refused = migration.refused.len(),
        resumed = migration.resumed,
        outcome,
        "store migration: finished"
    );
    medulla_kernel::metrics::store_migration_step("run", outcome);
}

/// The journal lives in the same private directory as the default staging
/// root, which this module creates with mode 0700.
///
/// Naming the path creates nothing: a fresh or current data directory must
/// stay exactly as it was, because an empty directory is how a new store is
/// recognised. The private directory is created only before a journal write.
fn journal_path(data_dir: &Path) -> Result<PathBuf, String> {
    Ok(data_dir.join(DEFAULT_STAGING_DIRECTORY).join(JOURNAL_FILE))
}

/// Create the private directory the journal and the default staging root
/// live in, with mode 0700.
fn create_journal_directory(data_dir: &Path) -> Result<(), String> {
    let scratch = super::default_staging_root(data_dir)?;
    debug_assert!(scratch.ends_with(STAGING_SCRATCH_DIRECTORY));
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum JournalState {
    Pending,
    Upgraded,
}

#[derive(Debug, Serialize, Deserialize)]
struct JournalEntry {
    file: String,
    state: JournalState,
}

/// What a run planned and how far it got. It is evidence for the operator and
/// the resume signal for the next open; the stores' own manifests stay the
/// authority on their format.
#[derive(Debug, Serialize, Deserialize)]
struct Journal {
    version: u32,
    engine_version: String,
    stores: Vec<JournalEntry>,
}

impl Journal {
    fn planned(stores: &[&StoreFile]) -> Self {
        Self {
            version: JOURNAL_VERSION,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            stores: stores
                .iter()
                .map(|store| JournalEntry {
                    file: store.name.clone(),
                    state: JournalState::Pending,
                })
                .collect(),
        }
    }

    /// Replace the journal atomically: write a sibling, flush it, rename it
    /// over the journal and flush the directory.
    fn write(&self, path: &Path) -> Result<(), String> {
        use std::io::Write;

        let bytes = serde_json::to_vec_pretty(self).map_err(|error| error.to_string())?;
        let partial = path.with_extension("json.partial");
        let mut file = std::fs::File::create(&partial)
            .map_err(|error| format!("write migration journal: {error}"))?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_all())
            .map_err(|error| format!("write migration journal: {error}"))?;
        std::fs::rename(&partial, path)
            .map_err(|error| format!("publish migration journal: {error}"))?;
        sync_parent(path)
    }
}

fn clear_journal(path: &Path) -> Result<(), String> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("remove migration journal: {error}")),
    }
}

fn sync_parent(path: &Path) -> Result<(), String> {
    let parent = path.parent().unwrap_or(Path::new("."));
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("flush migration journal directory: {error}"))
}

#[cfg(test)]
pub(super) fn read_journal_states(data_dir: &Path) -> Option<Vec<(String, &'static str)>> {
    let path = journal_path(&existing_directory(data_dir).ok()?).ok()?;
    let journal: Journal = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    Some(
        journal
            .stores
            .into_iter()
            .map(|entry| {
                let state = match entry.state {
                    JournalState::Pending => "pending",
                    JournalState::Upgraded => "upgraded",
                };
                (entry.file, state)
            })
            .collect(),
    )
}
