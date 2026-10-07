//! The one report both verbs produce: a line per store for the operator and a
//! final single-line JSON summary for the job that ran the command.

use std::path::Path;

use eg_storage::{
    layout_digest_hex, OwnerLayout, OwnerLayoutUpgradeReport, OwnerStoreFormat,
    OFFLINE_UPGRADE_APPLY_COMMAND,
};
use serde::Serialize;

/// Every store is current, or was upgraded by this run.
pub const EXIT_NOTHING_TO_DO: i32 = 0;
/// The command was refused, or an upgrade failed and the run stopped.
pub const EXIT_FAILED: i32 = 1;
/// At least one store has a registered upgrade waiting.
pub const EXIT_UPGRADE_AVAILABLE: i32 = 10;
/// At least one store is in a format this build neither opens nor upgrades.
/// Such a store is never modified.
pub const EXIT_BLOCKED: i32 = 20;
/// At least one store could not be read, so whether it needs an upgrade is
/// not known. An unclean shutdown leaves stores like this; the engine's own
/// start recovers such a store and then opens it or refuses it by name.
pub const EXIT_UNDETERMINED: i32 = 30;

/// What the command found, or did, for one store file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreStatus {
    Current,
    UpgradeAvailable,
    Upgraded,
    NoUpgrade,
    UnknownFormat,
    NotInspectable,
    Failed,
}

/// Displays as the name the JSON summary spells the status with.
impl std::fmt::Display for StoreStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.serialize(formatter)
    }
}

/// One store file's line in the report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoreReport {
    /// Path relative to the data directory.
    pub file: String,
    /// The store kind its manifest declares, when the manifest was readable.
    pub layout: Option<&'static str>,
    pub status: StoreStatus,
    pub detail: String,
}

impl StoreReport {
    pub(super) fn new(
        file: &str,
        layout: Option<OwnerLayout>,
        status: StoreStatus,
        detail: String,
    ) -> Self {
        Self {
            file: file.to_string(),
            layout: layout.map(OwnerLayout::canonical_name),
            status,
            detail,
        }
    }

    /// The line for a store that was only classified, not changed.
    pub(super) fn classified(file: &str, format: &Result<OwnerStoreFormat, String>) -> Self {
        match format {
            Ok(OwnerStoreFormat::Current(layout)) => Self::new(
                file,
                Some(*layout),
                StoreStatus::Current,
                format!("current layout {}", layout_digest_hex(*layout)),
            ),
            Ok(OwnerStoreFormat::UpgradeAvailable(upgrade)) => Self::new(
                file,
                Some(upgrade.predecessor.layout),
                StoreStatus::UpgradeAvailable,
                format!(
                    "{}; apply with `{OFFLINE_UPGRADE_APPLY_COMMAND}`",
                    upgrade.predecessor.label
                ),
            ),
            Ok(OwnerStoreFormat::NoUpgrade(predecessor)) => Self::new(
                file,
                Some(predecessor.layout),
                StoreStatus::NoUpgrade,
                format!(
                    "{}; no offline upgrade is registered. {}",
                    predecessor.label,
                    predecessor.operator_step()
                ),
            ),
            Ok(OwnerStoreFormat::Unknown(layout)) => Self::new(
                file,
                Some(*layout),
                StoreStatus::UnknownFormat,
                "layout digest is not in this build's lineage; the file may be corrupt or \
                 written by another build, and it is left untouched"
                    .to_string(),
            ),
            Err(reason) => Self::new(file, None, StoreStatus::NotInspectable, reason.clone()),
        }
    }

    /// The line for a store one registered upgrade just brought current.
    pub(super) fn upgraded(
        file: &str,
        layout: OwnerLayout,
        label: &str,
        report: &OwnerLayoutUpgradeReport,
    ) -> Self {
        Self::new(
            file,
            Some(layout),
            StoreStatus::Upgraded,
            format!(
                "upgraded from {label}: layout {} -> {}, authority epoch {} -> {}",
                hex::encode(report.previous_layout_digest),
                hex::encode(report.current_layout_digest),
                report.previous_authority_epoch,
                report.current_authority_epoch,
            ),
        )
    }
}

/// The whole result of one `inspect` or `apply` run.
#[derive(Debug, Serialize)]
pub struct Report {
    command: &'static str,
    pub verb: &'static str,
    pub data_dir: String,
    pub outcome: &'static str,
    pub exit_code: i32,
    /// No verb copies a store before upgrading it: an upgrade is one atomic
    /// commit and an interrupted one leaves the previous layout in place.
    pub pre_upgrade_copy: &'static str,
    /// Stores an `apply` run did not reach because it stopped at a failure.
    pub not_processed: usize,
    pub error: Option<String>,
    pub stores: Vec<StoreReport>,
}

impl Report {
    /// A run that reached the stores.
    pub(super) fn finished(
        verb: &'static str,
        data_dir: &Path,
        stores: Vec<StoreReport>,
        not_processed: usize,
    ) -> Self {
        let exit_code = exit_code(&stores);
        Self {
            command: "store-upgrade",
            verb,
            data_dir: data_dir.display().to_string(),
            outcome: outcome(exit_code, &stores),
            exit_code,
            pre_upgrade_copy: "none",
            not_processed,
            error: None,
            stores,
        }
    }

    /// A run that was refused before it looked at any store.
    pub(super) fn refused(verb: &'static str, data_dir: &Path, error: String) -> Self {
        let mut report = Self::finished(verb, data_dir, Vec::new(), 0);
        report.outcome = "failed";
        report.exit_code = EXIT_FAILED;
        report.error = Some(error);
        report
    }

    /// One line per store, then the single-line JSON summary as the last line.
    pub fn render(&self) -> String {
        let mut out = format!("store-upgrade {} {}\n", self.verb, self.data_dir);
        for store in &self.stores {
            out.push_str(&format!(
                "  {} [{}] {}: {}\n",
                store.file,
                store.layout.unwrap_or("unidentified"),
                store.status,
                store.detail
            ));
        }
        if let Some(error) = &self.error {
            out.push_str(&format!("  refused: {error}\n"));
        }
        out.push_str(&serde_json::to_string(self).expect("a store-upgrade report is plain JSON"));
        out.push('\n');
        out
    }
}

fn exit_code(stores: &[StoreReport]) -> i32 {
    let any = |status: StoreStatus| stores.iter().any(|store| store.status == status);
    if any(StoreStatus::Failed) {
        EXIT_FAILED
    } else if any(StoreStatus::NoUpgrade) || any(StoreStatus::UnknownFormat) {
        EXIT_BLOCKED
    } else if any(StoreStatus::NotInspectable) {
        EXIT_UNDETERMINED
    } else if any(StoreStatus::UpgradeAvailable) {
        EXIT_UPGRADE_AVAILABLE
    } else {
        EXIT_NOTHING_TO_DO
    }
}

fn outcome(exit_code: i32, stores: &[StoreReport]) -> &'static str {
    let upgraded = stores
        .iter()
        .any(|store| store.status == StoreStatus::Upgraded);
    match exit_code {
        EXIT_FAILED => "failed",
        EXIT_BLOCKED => "blocked",
        EXIT_UNDETERMINED => "undetermined",
        EXIT_UPGRADE_AVAILABLE => "upgrade_available",
        _ if upgraded => "upgraded",
        _ => "nothing_to_do",
    }
}
