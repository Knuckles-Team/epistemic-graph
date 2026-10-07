//! Operator command over the storage kernel's registered offline store
//! upgrades, for one stopped engine's data directory.
//!
//! * [`inspect`] is read-only. It reads each store's owner manifest and says
//!   whether the store is current, has a registered upgrade waiting, is in a
//!   format this build neither opens nor upgrades, or could not be read.
//! * [`apply`] holds the engine's own single-writer lock for the whole run, so
//!   it refuses while an engine is running, and runs each applicable upgrade
//!   through the storage kernel's inspected, single-commit transition. It stops
//!   at the first failure; a failed upgrade leaves that file in its previous
//!   layout. A second run finds nothing to do.
//! * [`refuse_upgradable_stores`] is the server's startup check: the engine
//!   never upgrades a store itself, it refuses to start and names the command.
//!
//! Which upgrades exist is decided by the storage kernel's registry
//! (`eg_storage::OFFLINE_STORE_UPGRADES`); nothing here names a specific one.

use std::path::{Path, PathBuf};

use eg_storage::{
    classify_owner_store_format, offline_upgrades_for, InspectedStoreUpgrade, OfflineStoreUpgrade,
    OfflineUpgradeInspectionOptions, OwnerLayoutUpgradeReport, OwnerStoreFormat,
    PhysicalStoreIdentity, OFFLINE_UPGRADE_APPLY_COMMAND,
};

mod discover;
mod report;

use discover::{discover, served_identity, StoreFile};
pub use report::{
    Report, StoreReport, StoreStatus, EXIT_BLOCKED, EXIT_FAILED, EXIT_NOTHING_TO_DO,
    EXIT_UNDETERMINED, EXIT_UPGRADE_AVAILABLE,
};

/// Directory, under the data directory, that holds the private scratch copy an
/// inspection recovers and reads. It is empty between runs.
const DEFAULT_STAGING_DIRECTORY: &str = ".store-upgrade-staging";
/// The staging root itself, inside the private default directory.
const STAGING_SCRATCH_DIRECTORY: &str = "scratch";
/// Room an inspection's scratch copy may use beyond the store's own size.
const STAGING_HEADROOM_BYTES: u64 = 64 * 1024 * 1024;

/// What `apply` was asked to do.
#[derive(Debug, Clone, Default)]
pub struct ApplyOptions {
    /// The explicit confirmation. Without it nothing is opened.
    pub confirm: bool,
    /// Private local staging directory (mode 0700, inside a mode 0700
    /// directory); defaults to one this command creates inside the data
    /// directory.
    pub staging_dir: Option<PathBuf>,
    /// Disk budget of one store's scratch copy; defaults to the store's size
    /// plus a quarter and a fixed headroom.
    pub max_store_bytes: Option<u64>,
}

/// List every store under `data_dir` with its format. Nothing is written.
pub fn inspect(data_dir: &Path) -> Report {
    match inspect_stores(data_dir) {
        Ok(stores) => Report::finished("inspect", data_dir, stores, 0),
        Err(error) => Report::refused("inspect", data_dir, error),
    }
}

/// Run every applicable registered upgrade under `data_dir`, in place.
pub fn apply(data_dir: &Path, options: &ApplyOptions) -> Report {
    match apply_stores(data_dir, options) {
        Ok((stores, not_processed)) => Report::finished("apply", data_dir, stores, not_processed),
        Err(error) => Report::refused("apply", data_dir, error),
    }
}

/// Startup check. A store that a registered offline upgrade applies to is
/// refused by its named error together with the exact command to run. A store
/// whose manifest cannot be read this way is left to its ordinary open.
pub fn refuse_upgradable_stores(data_dir: &Path) -> Result<(), String> {
    let refusals: Vec<String> = discover(data_dir)?
        .iter()
        .filter_map(upgrade_refusal)
        .collect();
    if refusals.is_empty() {
        return Ok(());
    }
    let command =
        OFFLINE_UPGRADE_APPLY_COMMAND.replace("<data-dir>", &data_dir.display().to_string());
    Err(format!("{}\nRun: {command}", refusals.join("\n")))
}

fn upgrade_refusal(store: &StoreFile) -> Option<String> {
    match classify_owner_store_format(&store.path) {
        Ok(OwnerStoreFormat::UpgradeAvailable(upgrade)) => Some(upgrade.refusal(&store.path)),
        _ => None,
    }
}

fn inspect_stores(data_dir: &Path) -> Result<Vec<StoreReport>, String> {
    let data_dir = existing_directory(data_dir)?;
    if crate::persist_lock::is_held(&data_dir)? {
        return Err(
            "an engine is running on this data directory and holds its stores; stop it first"
                .to_string(),
        );
    }
    Ok(discover(&data_dir)?
        .iter()
        .map(|store| {
            StoreReport::classified(&store.name, &classify_owner_store_format(&store.path))
        })
        .collect())
}

fn apply_stores(
    data_dir: &Path,
    options: &ApplyOptions,
) -> Result<(Vec<StoreReport>, usize), String> {
    if !options.confirm {
        return Err(
            "apply upgrades store files in place; pass --confirm to run it (inspect first)"
                .to_string(),
        );
    }
    let data_dir = existing_directory(data_dir)?;
    // The same single-writer lock the engine holds for its whole lifetime: a
    // running engine makes this fail, and no engine can start during the run.
    let _lease = crate::persist_lock::acquire(&data_dir.to_string_lossy())?;
    let staging = Staging::new(&data_dir, options);
    let stores = discover(&data_dir)?;
    let mut reports = Vec::with_capacity(stores.len());
    for store in &stores {
        let report = apply_store(store, &staging);
        let failed = report.status == StoreStatus::Failed;
        reports.push(report);
        if failed {
            break;
        }
    }
    let not_processed = stores.len() - reports.len();
    Ok((reports, not_processed))
}

/// The canonical data directory. Canonical because the staging root below it
/// is opened component by component without following links.
fn existing_directory(data_dir: &Path) -> Result<PathBuf, String> {
    let canonical = std::fs::canonicalize(data_dir).map_err(|error| {
        format!(
            "data directory {} is unavailable: {error}",
            data_dir.display()
        )
    })?;
    if !canonical.is_dir() {
        return Err(format!("{} is not a directory", data_dir.display()));
    }
    Ok(canonical)
}

/// Where and how large one inspection's private scratch copy may be.
struct Staging {
    data_dir: PathBuf,
    explicit_root: Option<PathBuf>,
    max_store_bytes: Option<u64>,
}

impl Staging {
    fn new(data_dir: &Path, options: &ApplyOptions) -> Self {
        Self {
            data_dir: data_dir.to_path_buf(),
            explicit_root: options.staging_dir.clone(),
            max_store_bytes: options.max_store_bytes,
        }
    }

    fn options(&self, store: &Path) -> Result<OfflineUpgradeInspectionOptions, String> {
        let root = match &self.explicit_root {
            Some(root) => root.clone(),
            None => default_staging_root(&self.data_dir)?,
        };
        let budget = match self.max_store_bytes {
            Some(budget) => budget,
            None => default_budget(store)?,
        };
        OfflineUpgradeInspectionOptions::new(root, budget)
    }
}

/// The storage kernel stages only in a private directory whose parent is
/// private too, and a data directory usually is not. The default root is
/// therefore one level inside a directory this command creates with mode
/// 0700, the first time an inspection needs it.
#[cfg(unix)]
fn default_staging_root(data_dir: &Path) -> Result<PathBuf, String> {
    use std::os::unix::fs::DirBuilderExt;

    let private = data_dir.join(DEFAULT_STAGING_DIRECTORY);
    match std::fs::DirBuilder::new().mode(0o700).create(&private) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(format!("create the private staging directory: {error}")),
    }
    Ok(private.join(STAGING_SCRATCH_DIRECTORY))
}

#[cfg(not(unix))]
fn default_staging_root(_data_dir: &Path) -> Result<PathBuf, String> {
    Err("offline store inspection is unsupported on this platform".to_string())
}

fn default_budget(store: &Path) -> Result<u64, String> {
    let size = std::fs::metadata(store)
        .map_err(|error| format!("stat store for its staging budget: {error}"))?
        .len();
    Ok(size
        .saturating_add(size / 4)
        .saturating_add(STAGING_HEADROOM_BYTES)
        .min(eg_storage::direct_state::HARD_MAX_DIRECT_STATE_BYTES))
}

/// What running the candidate upgrades of one store came to.
enum Attempt {
    Upgraded(&'static OfflineStoreUpgrade, OwnerLayoutUpgradeReport),
    /// No candidate's inspection admitted the file; nothing was written.
    NotAdmitted(String),
    /// An admitted upgrade's commit was refused; the file keeps its layout.
    Failed(String),
}

fn apply_store(store: &StoreFile, staging: &Staging) -> StoreReport {
    let format = classify_owner_store_format(&store.path);
    let candidates = candidates(store, &format);
    if candidates.is_empty() {
        return StoreReport::classified(&store.name, &format);
    }
    match attempt(store, &candidates, staging) {
        Attempt::Upgraded(upgrade, report) => StoreReport::upgraded(
            &store.name,
            upgrade.predecessor.layout,
            upgrade.predecessor.label,
            &report,
        ),
        Attempt::Failed(error) => failed(store, &candidates, error),
        Attempt::NotAdmitted(error) => not_admitted(store, &format, &candidates, error),
    }
}

/// The upgrades worth inspecting for one store: the one its manifest matches,
/// or, when the manifest cannot be read read-only (an unclean shutdown leaves
/// such a file), every registered upgrade of the kind the server opens it as.
/// The inspection itself decides; it recovers a private copy, never the file.
fn candidates(
    store: &StoreFile,
    format: &Result<OwnerStoreFormat, String>,
) -> Vec<&'static OfflineStoreUpgrade> {
    match (format, store.served_layout) {
        (Ok(OwnerStoreFormat::UpgradeAvailable(upgrade)), _) => vec![*upgrade],
        (Err(_), Some(layout)) => offline_upgrades_for(layout).collect(),
        _ => Vec::new(),
    }
}

fn attempt(
    store: &StoreFile,
    candidates: &[&'static OfflineStoreUpgrade],
    staging: &Staging,
) -> Attempt {
    let mut refusal = String::new();
    for upgrade in candidates {
        match admit(store, upgrade, staging) {
            Ok(inspected) => {
                return match inspected.apply() {
                    Ok(report) => Attempt::Upgraded(upgrade, report),
                    Err(error) => Attempt::Failed(error),
                }
            }
            Err(error) => refusal = error,
        }
    }
    Attempt::NotAdmitted(refusal)
}

/// Inspect `store` as a file of `upgrade`'s predecessor generation, expecting
/// the physical identity the server opens that store kind under.
fn admit(
    store: &StoreFile,
    upgrade: &OfflineStoreUpgrade,
    staging: &Staging,
) -> Result<InspectedStoreUpgrade, String> {
    let layout = upgrade.predecessor.layout;
    if store.served_layout.is_some_and(|served| served != layout) {
        return Err(format!(
            "the file holds a {} store, which is not what the server opens it as",
            layout.canonical_name()
        ));
    }
    let identity = served_identity(layout)
        .ok_or_else(|| format!("this build serves no {} store", layout.canonical_name()))?;
    upgrade.inspect(
        &store.path,
        PhysicalStoreIdentity::new(identity)?,
        None,
        staging.options(&store.path)?,
    )
}

fn failed(
    store: &StoreFile,
    candidates: &[&'static OfflineStoreUpgrade],
    error: String,
) -> StoreReport {
    StoreReport::new(
        &store.name,
        candidates.first().map(|upgrade| upgrade.predecessor.layout),
        StoreStatus::Failed,
        format!("the upgrade was refused and the file keeps its previous layout: {error}"),
    )
}

/// A store whose manifest named a registered predecessor but whose inspection
/// refused it is a failure. One that could not even be read stays what it
/// was: not inspectable, untouched, and left to the engine's ordinary open.
fn not_admitted(
    store: &StoreFile,
    format: &Result<OwnerStoreFormat, String>,
    candidates: &[&'static OfflineStoreUpgrade],
    error: String,
) -> StoreReport {
    match format {
        Err(unreadable) => StoreReport::new(
            &store.name,
            None,
            StoreStatus::NotInspectable,
            format!("{unreadable}; no registered upgrade admitted it ({error})"),
        ),
        Ok(_) => failed(store, candidates, error),
    }
}

#[cfg(test)]
mod tests;
