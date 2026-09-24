//! Owner-store layout lineage (X11): the one registry of every earlier table
//! set each layout has had, and the one chokepoint that refuses them.
//!
//! An owner file's manifest pins the exact digest of its table contracts, so a
//! file written under an earlier layout fails the normal open. Without a
//! record of earlier layouts that failure is a generic digest mismatch,
//! indistinguishable from corruption or a foreign file. This module keeps that
//! record:
//!
//! * [`layout_predecessors`] names every refused predecessor of a layout. A
//!   predecessor file is refused with `{LAYOUT}_FORMAT_UPGRADE_REQUIRED` and its
//!   removal step (EG 2.27.x ships `Refuse` only: old data is disposable, there
//!   is no migration code).
//! * [`validate_against_lineage`] is the check every manifest read runs --
//!   open, recovery adoption, classification and backup restore all read the
//!   manifest through `manifest_io::read_manifest` -- so none of them can miss
//!   it (EH-150: the named refusal sits at the manifest read, not the census).
//! * [`refuse_lineage_file`] runs the same check on a read-only open before any
//!   writable open, so a refused file's bytes are never touched.
//! * [`pinned_layout_digest`] pins each layout's current digest. Changing a
//!   layout without declaring its predecessor fails the lineage test, naming
//!   the layout; the only way through is to declare the format change.

use std::path::Path;

use crate::owner::layout::OwnerLayout;
use crate::owner::persisted_layout::LayoutPredecessor;
use crate::physical::incarnation::STORAGE_KERNEL_SCHEMA_VERSION;
use crate::physical::manifest::OwnerManifest;

/// `agent_library.redb` before connector packs and governed write-back.
pub const AGENT_LIBRARY_BEFORE_CONNECTOR_PACKS: LayoutPredecessor = LayoutPredecessor {
    layout: OwnerLayout::AgentLibrary,
    label: "Agent Library before connector packs and governed write-back",
    owner_tables: &[
        "agent_library",
        "agent_library_heads",
        "agent_graph",
        "agent_graph_heads",
        "agent_component",
        "agent_component_heads",
        "agent_template",
        "agent_template_heads",
    ],
    data_lost: "its pre-ConnectorPack and governed-write-back Agent Library rows are \
                intentionally not upgraded",
    file_name: "agent_library.redb",
};

/// `blob.redb` before holder rows, retention and the cursor high-water mark.
pub const BLOB_BEFORE_HOLDERS: LayoutPredecessor = LayoutPredecessor {
    layout: OwnerLayout::Blob,
    label: "blob store from before holder-scoped references",
    owner_tables: &["cas_chunks", "cas_blobs", "cas_refcount", "cas_uploads"],
    data_lost: "its chunks, manifests, reference counts and uploads are not migrated, \
                so media blobs referenced by graphs must be uploaded again",
    file_name: "blob.redb",
};

/// `sql.redb` before durable SQL source checkpoints (ruling D2: refused, the
/// bespoke offline upgrader deleted).
pub const SQL_BEFORE_SOURCE_CHECKPOINTS: LayoutPredecessor = LayoutPredecessor {
    layout: OwnerLayout::Sql,
    label: "SQL catalog store before durable source checkpoints",
    owner_tables: &[
        "__sql_catalog__",
        "__sql_functions__",
        "__sql_ann_indexes__",
        "__sql_secondary_indexes__",
        "__sql_secondary_index_entries__",
        "__sql_hypertables__",
        "__sql_source_authority__",
        "__sql_views__",
        "__sql_extensions__",
        "__sql_rows__",
        "__sql_seq__",
        "__sql_schema_catalog_versions__",
        "__sql_schema_versions__",
        "__sql_schema_migrations__",
        "__sql_schema_migration_order__",
        "__sql_schema_catalog_order__",
        "__sql_property_graphs__",
        "__sql_property_graph_seq__",
    ],
    data_lost: "its SQL catalog and rows are not migrated; re-ingest the sources",
    file_name: "sql.redb",
};

/// Every predecessor of `layout` this build refuses by name, oldest first.
///
/// Exhaustive on purpose: a new layout must state that it has none.
pub fn layout_predecessors(layout: OwnerLayout) -> &'static [LayoutPredecessor] {
    match layout {
        OwnerLayout::AgentLibrary => &[AGENT_LIBRARY_BEFORE_CONNECTOR_PACKS],
        OwnerLayout::Blob => &[BLOB_BEFORE_HOLDERS],
        OwnerLayout::Sql => &[SQL_BEFORE_SOURCE_CHECKPOINTS],
        OwnerLayout::LedgerOnly
        | OwnerLayout::Rbac
        | OwnerLayout::Jobs
        | OwnerLayout::Statechart
        | OwnerLayout::TimeSeries
        | OwnerLayout::Kv
        | OwnerLayout::SemanticIndex
        | OwnerLayout::PathIndex
        | OwnerLayout::RequestReplay
        | OwnerLayout::VizProvenance
        | OwnerLayout::ColdTier
        | OwnerLayout::TenantCatalog
        | OwnerLayout::NodeInfo
        | OwnerLayout::ClusterHierarchy
        | OwnerLayout::GraphShard => &[],
    }
}

/// Every layout, in registry order.
pub const ALL_LAYOUTS: [OwnerLayout; 18] = [
    OwnerLayout::LedgerOnly,
    OwnerLayout::Rbac,
    OwnerLayout::Jobs,
    OwnerLayout::Statechart,
    OwnerLayout::TimeSeries,
    OwnerLayout::Kv,
    OwnerLayout::Blob,
    OwnerLayout::SemanticIndex,
    OwnerLayout::Sql,
    OwnerLayout::PathIndex,
    OwnerLayout::RequestReplay,
    OwnerLayout::VizProvenance,
    OwnerLayout::ColdTier,
    OwnerLayout::TenantCatalog,
    OwnerLayout::NodeInfo,
    OwnerLayout::ClusterHierarchy,
    OwnerLayout::GraphShard,
    OwnerLayout::AgentLibrary,
];

/// The digest each layout had when its lineage was last declared, hex.
///
/// A closed contract pin, not a baseline: when a layout changes, this pin and
/// that layout's [`layout_predecessors`] change together, in the commit that
/// changes the layout.
pub fn pinned_layout_digest(layout: OwnerLayout) -> &'static str {
    match layout {
        OwnerLayout::LedgerOnly => {
            "9faad03bf787a2a893618b7a8b386de30bdd5b4e09f3930448040c3e47c59e3e"
        }
        OwnerLayout::Rbac => "3c89b3207faa962a94eb1ce724f944dde5a4acf94841a23d9c416855866d4c8e",
        OwnerLayout::Jobs => "64eb7ffd327980a8269bc8cf61147bc269b4ee8eab1ae0f5de3a244bbec9dad4",
        OwnerLayout::Statechart => {
            "d8ab661d50af9249a342a101537f0ef92c13df6808b5fd149c037d8587cff8a7"
        }
        OwnerLayout::TimeSeries => {
            "5f2b379c6ea02f36f38bcfd2fd8f6277e5174467f3c81b309fffdf87ec1a1581"
        }
        OwnerLayout::Kv => "48b6464ad5960bd14fc0976119b689c0016be63e8cc8be72df4e7030ed593f9e",
        OwnerLayout::Blob => "982e463fd13db3ec48fbdda77249ecaefed3b9174276c61ac7cd2eb206c47d6b",
        OwnerLayout::SemanticIndex => {
            "3008d8726d15a864daf6c723004152fc61bb40bc8de0fea486dfdaace0c985d8"
        }
        OwnerLayout::Sql => "2d562fabc75b19e9c36309b1bda7fcc9a050c68acbec876fc069d3b506d93e1e",
        OwnerLayout::PathIndex => {
            "c79d89a41ad97d55cf35238ca8706db8c958923624de068a430c3bca72656df9"
        }
        OwnerLayout::RequestReplay => {
            "ccb7051fef1a624274939944f7af9ee0f3ee03eee062794eeaf105d1c6117809"
        }
        OwnerLayout::VizProvenance => {
            "57e566cd7fae6e67c83778cb50423130b8c250c2ce58dd3e93f5b203151661d3"
        }
        OwnerLayout::ColdTier => "9170bbb3960d0eddb71c4b080878abcb4440698472124911e78125aa6f3931ad",
        OwnerLayout::TenantCatalog => {
            "01b945d16fa187e69ca3dd5777a27066310b64a8b351380af1ba810f5dcd57aa"
        }
        OwnerLayout::NodeInfo => "02e5ca262822f4de0a1507780b2e908e605655d3a758e78c2a75f205ffa80347",
        OwnerLayout::ClusterHierarchy => {
            "0561a2a2ae13f067bf01a4c94d6cbaeb280aaefa56c68036a1f01da92cba8431"
        }
        OwnerLayout::GraphShard => {
            "25d465035cd1c285493e057e754e4a97f8757ffe19fb910392217fa7386de48c"
        }
        OwnerLayout::AgentLibrary => {
            "7ea9bcde54e8961cedaa586b3520f890100c5f53f9f5f9574a30dea02c882445"
        }
    }
}

/// `layout`'s compiled digest, lowercase hex, as [`pinned_layout_digest`]
/// spells it.
pub fn layout_digest_hex(layout: OwnerLayout) -> String {
    layout
        .digest()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The lineage chokepoint: accept a current manifest, refuse a declared
/// predecessor by name, and report any other digest as unknown -- possibly
/// corrupt or written by another build -- rather than as a predecessor.
pub(crate) fn validate_against_lineage(manifest: &OwnerManifest) -> Result<(), String> {
    let Err(error) = manifest.validate_current() else {
        return Ok(());
    };
    if let Some(predecessor) = layout_predecessors(manifest.layout)
        .iter()
        .find(|predecessor| predecessor.matches(manifest))
    {
        return Err(predecessor.refusal(None));
    }
    if manifest.schema_version == STORAGE_KERNEL_SCHEMA_VERSION
        && manifest.layout_digest != manifest.layout.digest()
    {
        return Err(format!(
            "OWNER_STORE_FORMAT_UNKNOWN: the {} store's layout digest is not in this build's \
             lineage; the file may be corrupt or written by another build ({error})",
            manifest.layout.canonical_name()
        ));
    }
    Err(error)
}

/// Run the lineage check on a read-only open of `path` before anything opens
/// it for write. A file that cannot be inspected read-only is left to the
/// normal open, which is the authority on what is wrong with it.
pub(crate) fn refuse_lineage_file(path: &Path, layout: OwnerLayout) -> Result<(), String> {
    for predecessor in layout_predecessors(layout) {
        crate::owner::persisted_layout::refuse_known_predecessor(path, predecessor)?;
    }
    Ok(())
}

/// The operator documentation of every layout's format history, generated
/// from this registry (`docs/operations/owner-store-formats.md`).
pub fn render_owner_store_formats() -> String {
    let mut out = String::from(
        "# Owner-store formats\n\n\
         Generated from `crates/eg-storage/src/owner/lineage.rs`; do not edit by hand.\n\
         Regenerate with `cargo run -p eg-storage --example gen_owner_store_formats`.\n\n\
         Every durable owner file records the digest of its exact table layout. A file whose \
         layout is a declared predecessor below is refused at open with the named error and \
         must be moved aside; its data is not migrated (EG 2.27.x ships refusal only). Any \
         other digest is refused as `OWNER_STORE_FORMAT_UNKNOWN`.\n\n\
         | Store | Current layout digest | Refused predecessors |\n|---|---|---|\n",
    );
    for layout in ALL_LAYOUTS {
        let predecessors = layout_predecessors(layout);
        let refused = if predecessors.is_empty() {
            "none".to_string()
        } else {
            predecessors
                .iter()
                .map(|predecessor| format!("`{}`", predecessor.error_code()))
                .collect::<Vec<_>>()
                .join(", ")
        };
        out.push_str(&format!(
            "| `{}` | `{}` | {} |\n",
            layout.canonical_name(),
            layout_digest_hex(layout),
            refused
        ));
    }
    for layout in ALL_LAYOUTS {
        for predecessor in layout_predecessors(layout) {
            out.push_str(&format!(
                "\n## `{code}`\n\n* Store file: `{file}`\n* Refused generation: {label}\n\
                 * Data lost: {lost}\n* Owner tables of the refused generation: {tables}\n\
                 * Removal step: stop the engine, move `{file}` aside (keep it until the \
                 restarted engine is confirmed healthy), and restart; a fresh store is \
                 created. Backup bundles taken before this release cannot restore this file.\n",
                code = predecessor.error_code(),
                file = predecessor.file_name,
                label = predecessor.label,
                lost = predecessor.data_lost,
                tables = predecessor
                    .owner_tables
                    .iter()
                    .map(|table| format!("`{table}`"))
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
    }
    out
}

#[cfg(test)]
#[path = "lineage_tests.rs"]
mod tests;
