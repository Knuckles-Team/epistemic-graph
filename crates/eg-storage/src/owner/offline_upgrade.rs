//! The one registry of offline owner-store upgrades, and the read-only format
//! classification an operator command and a server startup check share.
//!
//! Each row names one declared predecessor generation and the inspected,
//! data-preserving transition that brings a file of that generation to the
//! current layout. Nothing here runs at an ordinary open: a caller asks for the
//! inspection explicitly and then consumes it. Registering an upgrade is one
//! row in [`OFFLINE_STORE_UPGRADES`]; the named refusal of that predecessor,
//! the classification and the operator document all follow from the row.

use super::agent_library_upgrade::{
    inspect_agent_library_mcp_catalog_upgrade, inspect_graph_shard_upgrade,
    upgrade_agent_library_mcp_catalog, upgrade_graph_shard, GraphShardSource,
    OwnerLayoutUpgradeReport,
};
use super::identity::PhysicalStoreIdentity;
use super::layout::OwnerLayout;
use super::lineage::{
    layout_predecessors, AGENT_LIBRARY_BEFORE_MCP_CATALOG, GRAPH_SHARD_BEFORE_AUDIT_REQUESTS,
    GRAPH_SHARD_BEFORE_ENRICHMENT, SQL_BEFORE_DURABLE_ANN, SQL_BEFORE_SOURCE_CHECKPOINTS,
};
use super::persisted_layout::{read_persisted_manifest, LayoutPredecessor};
use super::sql_checkpoint_upgrade::{
    inspect_sql_ann_generation_upgrade, inspect_sql_source_checkpoint_upgrade,
    upgrade_sql_ann_generations, upgrade_sql_source_checkpoints,
    SqlSourceCheckpointInspectionOptions, SqlSourceCheckpointUpgradeReport,
};
use crate::physical::integrity::PrivatePayloadIntegrity;
use crate::physical::manifest::OwnerManifest;
use crate::StorageKernel;
use std::path::Path;
use std::sync::Arc;

/// The operator command that applies every registered upgrade found under one
/// data directory. Named by the refusal of an upgradable predecessor.
pub const OFFLINE_UPGRADE_APPLY_COMMAND: &str =
    "epistemic-graph-server store-upgrade apply <data-dir> --confirm";

/// The read-only companion of [`OFFLINE_UPGRADE_APPLY_COMMAND`].
pub const OFFLINE_UPGRADE_INSPECT_COMMAND: &str =
    "epistemic-graph-server store-upgrade inspect <data-dir>";

/// Private local staging root and disk budget of one offline inspection.
pub type OfflineUpgradeInspectionOptions = SqlSourceCheckpointInspectionOptions;

type InspectOfflineUpgrade = fn(
    &Path,
    PhysicalStoreIdentity,
    Option<Arc<dyn PrivatePayloadIntegrity>>,
    OfflineUpgradeInspectionOptions,
) -> Result<InspectedStoreUpgrade, String>;

/// One registered offline upgrade: the declared predecessor generation it
/// starts from (which names the store kind, the generation and the file) and
/// the inspection that admits a file of exactly that generation.
pub struct OfflineStoreUpgrade {
    pub predecessor: &'static LayoutPredecessor,
    inspect: InspectOfflineUpgrade,
}

impl OfflineStoreUpgrade {
    /// Inspect `path` without write authority. The source bytes are not
    /// changed; success is the only way to obtain an [`InspectedStoreUpgrade`].
    pub fn inspect(
        &self,
        path: &Path,
        expected_physical_identity: PhysicalStoreIdentity,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
        options: OfflineUpgradeInspectionOptions,
    ) -> Result<InspectedStoreUpgrade, String> {
        (self.inspect)(path, expected_physical_identity, private_integrity, options)
    }

    /// The named refusal an ordinary open gives a file of this generation.
    pub fn refusal(&self, path: &Path) -> String {
        self.predecessor.refusal(Some(path))
    }
}

/// An admitted upgrade of one exact physical file. It is bound to the bytes
/// the inspection read and is consumed by the single durable commit.
pub struct InspectedStoreUpgrade {
    apply: Box<dyn FnOnce() -> Result<OwnerLayoutUpgradeReport, String>>,
}

impl InspectedStoreUpgrade {
    fn bind<T, R>(token: T, apply: fn(T) -> Result<(StorageKernel, R), String>) -> Self
    where
        T: 'static,
        R: Into<OwnerLayoutUpgradeReport> + 'static,
    {
        Self {
            apply: Box::new(move || {
                let (kernel, report) = apply(token)?;
                // The upgraded file is closed again: the caller starts the
                // engine, which is the only process that serves it.
                drop(kernel);
                Ok(report.into())
            }),
        }
    }

    /// Perform the one durable commit. A refusal leaves the file in its
    /// predecessor layout.
    pub fn apply(self) -> Result<OwnerLayoutUpgradeReport, String> {
        (self.apply)()
    }
}

impl From<SqlSourceCheckpointUpgradeReport> for OwnerLayoutUpgradeReport {
    fn from(report: SqlSourceCheckpointUpgradeReport) -> Self {
        Self {
            previous_layout_digest: report.previous_layout_digest,
            current_layout_digest: report.current_layout_digest,
            previous_authority_epoch: report.previous_authority_epoch,
            current_authority_epoch: report.current_authority_epoch,
        }
    }
}

/// One registry row: the declared predecessor, its inspect function and its
/// apply function. The inspect function takes the path, the expected physical
/// identity, the private-payload authenticator and the inspection options; a
/// closure adapts an inspection that needs more than those.
macro_rules! offline_upgrade {
    ($predecessor:expr, $inspect:expr, $apply:expr) => {
        OfflineStoreUpgrade {
            predecessor: &$predecessor,
            inspect: |path, identity, integrity, options| {
                $inspect(path, identity, integrity, options)
                    .map(|token| InspectedStoreUpgrade::bind(token, $apply))
            },
        }
    };
}

/// Every offline upgrade this build can run, in the order an operator command
/// reports them.
pub static OFFLINE_STORE_UPGRADES: &[OfflineStoreUpgrade] = &[
    offline_upgrade!(
        SQL_BEFORE_SOURCE_CHECKPOINTS,
        inspect_sql_source_checkpoint_upgrade,
        upgrade_sql_source_checkpoints
    ),
    offline_upgrade!(
        SQL_BEFORE_DURABLE_ANN,
        inspect_sql_ann_generation_upgrade,
        upgrade_sql_ann_generations
    ),
    offline_upgrade!(
        AGENT_LIBRARY_BEFORE_MCP_CATALOG,
        inspect_agent_library_mcp_catalog_upgrade,
        upgrade_agent_library_mcp_catalog
    ),
    offline_upgrade!(
        GRAPH_SHARD_BEFORE_ENRICHMENT,
        |path, identity, integrity, options| inspect_graph_shard_upgrade(
            path,
            identity,
            integrity,
            options,
            GraphShardSource::BeforeEnrichment
        ),
        upgrade_graph_shard
    ),
    offline_upgrade!(
        GRAPH_SHARD_BEFORE_AUDIT_REQUESTS,
        |path, identity, integrity, options| inspect_graph_shard_upgrade(
            path,
            identity,
            integrity,
            options,
            GraphShardSource::BeforeAuditRequests
        ),
        upgrade_graph_shard
    ),
];

/// The registered upgrade that starts from `predecessor`, when there is one.
pub fn registered_offline_upgrade(
    predecessor: &LayoutPredecessor,
) -> Option<&'static OfflineStoreUpgrade> {
    OFFLINE_STORE_UPGRADES
        .iter()
        .find(|upgrade| upgrade.predecessor == predecessor)
}

/// Every registered upgrade of one store kind.
pub fn offline_upgrades_for(
    layout: OwnerLayout,
) -> impl Iterator<Item = &'static OfflineStoreUpgrade> {
    OFFLINE_STORE_UPGRADES
        .iter()
        .filter(move |upgrade| upgrade.predecessor.layout == layout)
}

/// What a read-only look at one owner file's manifest says about its format.
#[derive(Clone, Copy)]
pub enum OwnerStoreFormat {
    /// Exactly the layout this build opens.
    Current(OwnerLayout),
    /// A declared predecessor that a registered offline upgrade starts from.
    UpgradeAvailable(&'static OfflineStoreUpgrade),
    /// A declared predecessor with no upgrade: the file is moved aside.
    NoUpgrade(&'static LayoutPredecessor),
    /// A layout digest outside this build's lineage: possibly corrupt, or
    /// written by another build.
    Unknown(OwnerLayout),
}

/// Classify the owner file at `path` from its manifest alone. The file is
/// opened read-only and never written. An error means the manifest could not
/// be read this way (not an owner file, held open by an engine, or left by an
/// unclean shutdown), which says nothing about the file's format.
pub fn classify_owner_store_format(path: &Path) -> Result<OwnerStoreFormat, String> {
    read_persisted_manifest(path).map(|manifest| classify_manifest(&manifest))
}

fn classify_manifest(manifest: &OwnerManifest) -> OwnerStoreFormat {
    if manifest.validate_current().is_ok() {
        return OwnerStoreFormat::Current(manifest.layout);
    }
    let declared = layout_predecessors(manifest.layout)
        .iter()
        .find(|predecessor| predecessor.matches(manifest));
    match declared {
        Some(predecessor) => registered_offline_upgrade(predecessor).map_or(
            OwnerStoreFormat::NoUpgrade(predecessor),
            OwnerStoreFormat::UpgradeAvailable,
        ),
        None => OwnerStoreFormat::Unknown(manifest.layout),
    }
}

/// The removal step of one predecessor in the generated owner-store format
/// document. A registered predecessor is upgraded in place instead.
pub(crate) fn render_removal_step(predecessor: &LayoutPredecessor) -> String {
    match registered_offline_upgrade(predecessor) {
        Some(_) => format!(
            "none. This generation is upgraded in place with `{OFFLINE_UPGRADE_APPLY_COMMAND}` \
             while the engine is stopped; see the procedure below. Moving the file aside \
             instead loses its rows."
        ),
        None => format!(
            "stop the engine, move `{}` aside (keep it until the restarted engine is confirmed \
             healthy), and restart; a fresh store is created. Backup bundles taken before this \
             release cannot restore this file.",
            predecessor.file_name
        ),
    }
}

/// The operator procedure section of the generated owner-store format
/// document, with one row per registered upgrade.
pub(crate) fn render_offline_upgrade_procedure() -> String {
    let mut out = format!(
        "\n## Offline upgrade procedure\n\n\
         A predecessor listed in the table below is upgraded in place, with its rows \
         preserved, by an explicit operator command. The engine never runs an upgrade \
         itself: it refuses to start on such a store and names the command.\n\n\
         1. Stop the engine. Both verbs refuse while an engine holds the data directory.\n\
         2. Inspect: `{inspect}`. This reads every store's manifest and changes nothing. \
         Exit status `0` means nothing to do, `10` an upgrade is available, `20` a store is \
         in a format this build neither opens nor upgrades, `30` a store could not be read, \
         and `1` the command was refused.\n\
         3. Apply: `{apply}`. Each upgrade is one atomic commit that creates the missing \
         empty tables and replaces the owner manifest; existing rows are not rewritten, and \
         a failed or interrupted upgrade leaves the file in its previous layout. No separate \
         copy of the store is kept, so take a filesystem snapshot first if one is wanted. \
         The run stops at the first failure with exit status `1`; a store it does not \
         upgrade is never written; a second run reports nothing to do. Before an upgrade is \
         admitted the store is checked on a private scratch copy, which needs free space of \
         about the store's size under `<data-dir>/.store-upgrade-staging` (`--staging-dir` \
         names another local directory, which must have mode 0700 and sit inside a mode \
         0700 directory; `--max-store-bytes` sets the per-store budget).\n\
         4. Start the engine.\n\n\
         A store that was not closed cleanly, as after a killed engine, cannot be read \
         without recovering it, which `inspect` never does: it reports the store as not \
         inspectable (`30`). `apply` recovers a private copy of such a store, upgrades the \
         store if a registered upgrade admits it, and otherwise leaves it untouched and \
         reports `30`; the engine recovers that store at its next start and then opens it \
         or refuses it by name.\n\n\
         The last output line of both verbs is one JSON object with the outcome, the exit \
         status and one entry per store.\n\n\
         | Store | Predecessor generation | Store file |\n|---|---|---|\n",
        inspect = OFFLINE_UPGRADE_INSPECT_COMMAND,
        apply = OFFLINE_UPGRADE_APPLY_COMMAND,
    );
    for upgrade in OFFLINE_STORE_UPGRADES {
        out.push_str(&format!(
            "| `{}` | {} | `{}` |\n",
            upgrade.predecessor.layout.canonical_name(),
            upgrade.predecessor.label,
            upgrade.predecessor.file_name,
        ));
    }
    out
}

#[cfg(test)]
mod tests;
