//! Test support: the graph-shard manifests recorded under
//! `fixtures/lineage/`.
//!
//! Each fixture is the manifest row and table census an EARLIER build of this
//! crate wrote for a fresh graph shard, captured by running that build (see the
//! fixture directory's `README.md` for the commit and command of each). They
//! are the evidence that a layout predecessor declared in
//! [`crate::owner::lineage`] is the layout existing files really have, rather
//! than whatever the current table list happens to reduce to.

use crate::codec::decode_ledger_record;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::physical::manifest::OwnerManifest;
use crate::tables::OWNER_MANIFEST;
use redb::{ReadableDatabase, TableHandle};
use std::path::Path;

/// A fresh graph shard as written before the repository-enrichment tables.
pub(crate) const BEFORE_ENRICHMENT: &str =
    include_str!("../../fixtures/lineage/graph-shard-before-enrichment.txt");

/// A fresh graph shard as written immediately before the operation
/// audit-append idempotency index.
pub(crate) const BEFORE_AUDIT_REQUESTS: &str =
    include_str!("../../fixtures/lineage/graph-shard-before-audit-requests.txt");

/// What one build persisted for a fresh graph shard.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RecordedLayout {
    /// The exact bytes of the single owner manifest row.
    pub(crate) manifest_bytes: Vec<u8>,
    /// Every table the file holds, sorted by name.
    pub(crate) tables: Vec<String>,
}

fn field<'a>(recorded: &'a str, name: &str) -> &'a str {
    recorded
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("the lineage fixture has no `{name}` line"))
}

fn decode_hex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd-length hex in fixture");
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex digit in fixture"))
        .collect()
}

/// The identity a fixture's file was created under.
pub(crate) fn recorded_identity(recorded: &str) -> PhysicalStoreIdentity {
    PhysicalStoreIdentity::new(field(recorded, "identity")).unwrap()
}

impl RecordedLayout {
    pub(crate) fn parse(recorded: &str) -> Self {
        assert_eq!(field(recorded, "multimap_tables"), "");
        Self {
            manifest_bytes: decode_hex(field(recorded, "manifest_hex")),
            tables: field(recorded, "tables")
                .split(',')
                .map(str::to_string)
                .collect(),
        }
    }

    /// What the file at `path` persists right now, read through plain redb so
    /// no layout validation of this build stands between the test and the
    /// bytes.
    pub(crate) fn read(path: &Path) -> Self {
        let database = redb::Database::open(path).unwrap();
        let read = database.begin_read().unwrap();
        let manifest_bytes = read
            .open_table(OWNER_MANIFEST)
            .unwrap()
            .get("manifest")
            .unwrap()
            .expect("an owner file has a manifest row")
            .value()
            .to_vec();
        let mut tables: Vec<String> = read
            .list_tables()
            .unwrap()
            .map(|table| table.name().to_string())
            .collect();
        tables.sort();
        Self {
            manifest_bytes,
            tables,
        }
    }

    pub(crate) fn manifest(&self) -> OwnerManifest {
        decode_ledger_record(&self.manifest_bytes).unwrap()
    }
}
