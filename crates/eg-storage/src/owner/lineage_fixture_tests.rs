//! The lineage registry against manifests that earlier builds really wrote.
//!
//! The other lineage tests build a predecessor by reducing the CURRENT layout,
//! so they stay green if a historical table set or digest is rewritten to match
//! the current tables. These do not: each compares the registry with bytes
//! recorded from the build that had that layout.

use super::*;
use crate::owner::layout::layout_digest_over;
use crate::owner::lineage_fixtures::{
    recorded_identity, RecordedLayout, BEFORE_AUDIT_REQUESTS, BEFORE_ENRICHMENT,
};
use crate::owner::persisted_layout::create_predecessor_owner_file;

/// Each recorded generation: its fixture, the predecessor the registry must
/// recognize it as, and the digest that generation pinned as current.
const RECORDED: [(&str, &LayoutPredecessor, [u8; 32]); 2] = [
    (
        BEFORE_ENRICHMENT,
        &GRAPH_SHARD_BEFORE_ENRICHMENT,
        GRAPH_SHARD_PRE_ENRICHMENT_DIGEST,
    ),
    (
        BEFORE_AUDIT_REQUESTS,
        &GRAPH_SHARD_BEFORE_AUDIT_REQUESTS,
        GRAPH_SHARD_PRE_AUDIT_REQUESTS_DIGEST,
    ),
];

#[test]
fn recorded_manifests_carry_the_frozen_historical_digests() {
    for (recorded, predecessor, digest) in RECORDED {
        let manifest = RecordedLayout::parse(recorded).manifest();
        assert_eq!(manifest.layout_digest, digest, "{}", predecessor.label);
        assert_eq!(
            layout_digest_over(manifest.layout, &manifest.tables),
            digest,
            "{}",
            predecessor.label
        );
        assert_ne!(digest, OwnerLayout::GraphShard.digest());
        assert!(predecessor.matches(&manifest), "{}", predecessor.label);
    }
}

// spec: EG-DURABLE-KERNEL-R003
#[test]
fn recorded_manifests_are_refused_by_name_and_never_as_unknown() {
    for (recorded, predecessor, _) in RECORDED {
        let manifest = RecordedLayout::parse(recorded).manifest();
        let error = validate_against_lineage(&manifest).unwrap_err();
        assert!(
            error.starts_with("GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED: "),
            "{error}"
        );
        assert!(error.contains(predecessor.label), "{error}");
    }
}

/// The predecessor file builder every refusal and upgrade test relies on
/// writes the same manifest row and the same tables as the build that had the
/// layout, so those tests run against a faithful file.
#[test]
fn the_predecessor_file_builder_reproduces_the_recorded_files() {
    let directory = tempfile::tempdir().unwrap();
    for (generation, (recorded, predecessor, _)) in RECORDED.into_iter().enumerate() {
        let path = directory.path().join(format!("graph-{generation}.redb"));
        create_predecessor_owner_file(&path, recorded_identity(recorded), predecessor).unwrap();
        assert_eq!(
            RecordedLayout::read(&path),
            RecordedLayout::parse(recorded),
            "{}",
            predecessor.label
        );
    }
}

/// The table sets of earlier generations are spelled out, not derived from the
/// current census: a table added later must appear in none of them.
#[test]
fn no_historical_graph_shard_table_set_contains_the_audit_request_index() {
    for predecessor in layout_predecessors(OwnerLayout::GraphShard) {
        assert!(
            !predecessor.owner_tables.contains(&"audit_requests"),
            "{}",
            predecessor.label
        );
    }
    assert!(
        crate::owner::registry::owner_table_names(OwnerLayout::GraphShard)
            .contains(&"audit_requests")
    );
}
