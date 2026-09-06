use crate::kernel::{create_physical, open_physical};
use crate::owner::contract::{CAP_CAS, CAP_DELETE, CAP_INSERT, CAP_READ, CAP_UPDATE};
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::{OwnerLayout, OWNER_LAYOUT_DOMAINS};
use crate::tables::ledger_table_names;
use crate::owner::registry::{
    declared_table_names, is_known_mutation_table, owner_layouts, owner_table_names, KV,
};
use crate::owner::table_api::{owner_table_access, OwnerTableAccess};
use crate::physical::manifest::{OwnerManifest, TableContract, TableOwnership, TableScope};
use crate::physical::root::{is_retired_prototype_table, retired_prototype_table_names};
use crate::recovery::adopt::{classify_recovery_store, RecoveryExpectation};
use redb::{MultimapTableDefinition, ReadableDatabase, TableDefinition};

#[test]
fn owner_layout_registry_has_frozen_cardinality() {
    // The physical ledger has exactly 18 tables total. OWNER_MANIFEST is one
    // of those 18; it is not a nineteenth table. Ledger format v2 added the two
    // replay tables (`mutation_replay_nonces_v1`,
    // `mutation_replay_operations_v1`) and the mutation-class label
    // (`mutation_classes_v1`, which makes a maintenance write explicit) to the
    // closed census.
    assert_eq!(ledger_table_names().len(), 18);
    assert_eq!(owner_table_names(OwnerLayout::LedgerOnly).len(), 0);
    assert_eq!(owner_table_names(OwnerLayout::Rbac).len(), 1);
    assert_eq!(owner_table_names(OwnerLayout::Jobs).len(), 13);
    assert_eq!(owner_table_names(OwnerLayout::Statechart).len(), 2);
    assert_eq!(owner_table_names(OwnerLayout::TimeSeries).len(), 3);
    assert_eq!(owner_table_names(OwnerLayout::Kv).len(), 2);
    assert_eq!(owner_table_names(OwnerLayout::Blob).len(), 4);
    assert_eq!(owner_table_names(OwnerLayout::SemanticIndex).len(), 16);
    assert_eq!(owner_table_names(OwnerLayout::Sql).len(), 20);
    assert_eq!(owner_table_names(OwnerLayout::PathIndex).len(), 1);
    // Six root-binary sidecar layouts. Each is one physical file with one
    // fixed native ControlPlane scope, so each declares only its own table(s):
    // `request-replay.redb`, `viz_provenance.redb`, `cold.redb`,
    // `catalog.redb`, `node_info.redb`, `cluster_hierarchy.redb`.
    assert_eq!(owner_table_names(OwnerLayout::RequestReplay).len(), 1);
    assert_eq!(owner_table_names(OwnerLayout::VizProvenance).len(), 1);
    assert_eq!(owner_table_names(OwnerLayout::ColdTier).len(), 1);
    assert_eq!(owner_table_names(OwnerLayout::TenantCatalog).len(), 1);
    assert_eq!(owner_table_names(OwnerLayout::NodeInfo).len(), 2);
    assert_eq!(owner_table_names(OwnerLayout::ClusterHierarchy).len(), 1);
    assert_eq!(owner_layouts().len(), 16);
}

#[test]
fn public_declared_table_projection_is_exact_and_sorted() {
    for layout in owner_layouts() {
        let names = declared_table_names(layout);
        assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            names.len(),
            ledger_table_names().len() + owner_table_names(layout).len()
        );
        assert!(ledger_table_names().iter().all(|name| names.contains(name)));
        assert!(owner_table_names(layout)
            .iter()
            .all(|name| names.contains(name)));
    }
}

#[test]
fn every_owner_surface_has_one_closed_cutover_disposition() {
    let mut names: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for layout in owner_layouts() {
        names.extend(owner_table_names(layout));
    }
    let service = names
        .iter()
        .filter(|name| owner_table_access(name) == OwnerTableAccess::DomainService)
        .count();
    let shared = names
        .iter()
        .filter(|name| owner_table_access(name) == OwnerTableAccess::SharedService)
        .count();
    // 69 owner tables across the sixteen layouts: RF-RULING-004 puts the
    // complete physical registry in the storage kernel, so the consumer-owned
    // tables (`path_index_v1`, `eg_ann`, `eg_kvcache_cold`, and the 20
    // `__sql_*`) are declared here rather than by the crates that read them.
    // +7 over the previous 62: the seven tables of the six root-binary
    // sidecar owner files, which stopped being raw `Database::create` sites.
    assert_eq!((names.len(), service, shared), (69, 67, 2));
}

#[test]
fn manifest_has_no_compatibility_or_inference_entrypoint() {
    let source = concat!(
        include_str!("contract.rs"),
        include_str!("layout.rs"),
        include_str!("manifest_io.rs"),
        include_str!("../physical/manifest.rs"),
    );
    for forbidden in [
        concat!("migrate_", "owner_manifest"),
        concat!("load_or_", "derive_manifest"),
        concat!("infer_", "owner_manifest"),
        concat!("fallback_", "owner_manifest"),
    ] {
        assert!(!source.contains(forbidden), "forbidden symbol: {forbidden}");
    }
}

#[test]
fn manifest_registry_rejects_missing_extra_and_codec_mismatch() {
    let physical = PhysicalStoreIdentity::new("physical:test:manifest").unwrap();
    let manifest = OwnerManifest::new(physical, OwnerLayout::Kv).unwrap();

    let mut missing = manifest.clone();
    missing.tables.pop();
    assert!(missing.validate().is_err());

    let mut extra = manifest.clone();
    extra.tables.push(extra.tables[0].clone());
    assert!(extra.validate().is_err());

    let mut mismatch = manifest;
    mismatch.tables[0].key_type_id = "wrong-key-type".to_string();
    assert!(mismatch.validate().is_err());
}

#[test]
fn manifest_registry_records_exact_unit_and_tuple_value_types() {
    let manifest = OwnerManifest::new(
        PhysicalStoreIdentity::new("physical:test:codecs").unwrap(),
        OwnerLayout::Jobs,
    )
    .unwrap();
    let type_id = |name: &str| {
        manifest
            .tables
            .iter()
            .find(|table| table.table_id == name)
            .unwrap()
            .value_type_id
            .as_str()
    };
    assert_eq!(type_id("mutation_outbox_topic_index_v1"), "()");
    assert_eq!(
        type_id("analytics_job_active_totals_by_tenant"),
        "(u64,u64)"
    );
}

#[test]
fn manifest_registry_is_closed_for_every_layout() {
    for layout in owner_layouts() {
        let manifest = OwnerManifest::new(
            PhysicalStoreIdentity::new(format!("physical:test:{}", layout.canonical_name()))
                .unwrap(),
            layout,
        )
        .unwrap();
        assert_eq!(manifest.tables.len(), 18 + owner_table_names(layout).len());
        let names = manifest
            .tables
            .iter()
            .map(|table| table.table_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), manifest.tables.len());
        assert!(manifest.tables.iter().all(|table| {
            !table.schema_id.is_empty()
                && !table.key_type_id.is_empty()
                && !table.value_type_id.is_empty()
                && table.key_codec == "redb-native-key-v1"
                && table.value_codec == "redb-native-value-v1"
                && !table.logical_schema_id.is_empty()
                && !table.logical_codec_id.is_empty()
        }));
    }
}

#[test]
fn every_owner_table_declares_its_partition_boundary() {
    for layout in owner_layouts()
        .into_iter()
        .filter(|layout| !owner_table_names(*layout).is_empty())
    {
        let manifest = OwnerManifest::new(
            PhysicalStoreIdentity::new(format!(
                "physical:test:scope-key:{}",
                layout.canonical_name()
            ))
            .unwrap(),
            layout,
        )
        .unwrap();
        for table in manifest
            .tables
            .iter()
            .filter(|table| table.ownership == TableOwnership::Owner)
        {
            assert_eq!(table.domain, Some(OWNER_LAYOUT_DOMAINS[layout as usize]));
            match table.scope {
                TableScope::Serving => assert!(matches!(
                    layout,
                    OwnerLayout::TimeSeries
                        | OwnerLayout::Blob
                        | OwnerLayout::SemanticIndex
                        | OwnerLayout::Sql
                )),
                TableScope::StorePrivate => {
                    assert!(matches!(
                        layout,
                        OwnerLayout::Rbac
                            | OwnerLayout::Jobs
                            | OwnerLayout::Statechart
                            | OwnerLayout::Kv
                            | OwnerLayout::PathIndex
                            | OwnerLayout::RequestReplay
                            | OwnerLayout::VizProvenance
                            | OwnerLayout::ColdTier
                            | OwnerLayout::TenantCatalog
                            | OwnerLayout::NodeInfo
                            | OwnerLayout::ClusterHierarchy
                    ));
                }
                TableScope::SharedService => {
                    assert!(matches!(
                        table.table_id.as_str(),
                        "cas_chunks" | "cas_refcount"
                    ));
                }
                TableScope::Physical => panic!("owner table cannot have physical-only scope"),
            }
        }
    }
}

#[test]
fn jobs_manifest_matches_the_current_global_store_schema() {
    let manifest = OwnerManifest::new(
        PhysicalStoreIdentity::new("physical:test:jobs-schema").unwrap(),
        OwnerLayout::Jobs,
    )
    .unwrap();
    let expected = [
        ("analytics_jobs", "&str", "&[u8]"),
        ("analytics_job_committed_results", "&str", "&str"),
        ("job_intents", "&str", "&[u8]"),
        ("job_idempotency_ledger", "&str", "&str"),
        ("analytics_job_knowledge_batches", "&str", "&[u8]"),
        ("analytics_job_scheduler_meta", "&str", "u64"),
        ("analytics_job_ready_by_priority", "(u32,i64,&str)", "()"),
        (
            "analytics_job_ready_by_capability",
            "(&str,u32,i64,&str)",
            "()",
        ),
        ("analytics_job_lease_by_worker", "&str", "&str"),
        ("analytics_job_lease_by_expiry", "(i64,&str)", "()"),
        ("analytics_job_active_totals_by_tenant", "&str", "(u64,u64)"),
        ("analytics_job_by_deadline", "(i64,&str)", "()"),
        ("analytics_job_cancellation_reconcile", "&str", "()"),
    ];
    for (name, key, value) in expected {
        let table = manifest
            .tables
            .iter()
            .find(|table| table.table_id == name)
            .unwrap();
        assert_eq!(
            (table.key_type_id.as_str(), table.value_type_id.as_str()),
            (key, value)
        );
        assert_eq!(table.scope, TableScope::StorePrivate);
        assert_eq!(owner_table_access(name), OwnerTableAccess::DomainService);
    }

    for (name, capabilities, projection) in [
        ("analytics_jobs", CAP_READ | CAP_INSERT | CAP_UPDATE, false),
        (
            "analytics_job_committed_results",
            CAP_READ | CAP_INSERT,
            false,
        ),
        ("job_intents", CAP_READ | CAP_INSERT | CAP_UPDATE, false),
        ("job_idempotency_ledger", CAP_READ | CAP_INSERT, false),
        (
            "analytics_job_knowledge_batches",
            CAP_READ | CAP_INSERT,
            false,
        ),
        (
            "analytics_job_scheduler_meta",
            CAP_READ | CAP_INSERT | CAP_UPDATE,
            true,
        ),
        (
            "analytics_job_ready_by_priority",
            CAP_READ | CAP_INSERT | CAP_DELETE,
            true,
        ),
        (
            "analytics_job_ready_by_capability",
            CAP_READ | CAP_INSERT | CAP_DELETE,
            true,
        ),
        (
            "analytics_job_lease_by_worker",
            CAP_READ | CAP_INSERT | CAP_DELETE,
            true,
        ),
        (
            "analytics_job_lease_by_expiry",
            CAP_READ | CAP_INSERT | CAP_DELETE,
            true,
        ),
        (
            "analytics_job_active_totals_by_tenant",
            CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
            true,
        ),
        (
            "analytics_job_by_deadline",
            CAP_READ | CAP_INSERT | CAP_DELETE,
            true,
        ),
        (
            "analytics_job_cancellation_reconcile",
            CAP_READ | CAP_INSERT | CAP_DELETE,
            true,
        ),
    ] {
        let table = manifest
            .tables
            .iter()
            .find(|table| table.table_id == name)
            .unwrap();
        assert_eq!(table.capabilities, capabilities);
        assert_eq!((table.index, table.derived), (projection, projection));
    }
}

fn owner_contract(layout: OwnerLayout, name: &str) -> TableContract {
    OwnerManifest::new(
        PhysicalStoreIdentity::new(format!("physical:test:signed:{}", layout.canonical_name()))
            .unwrap(),
        layout,
    )
    .unwrap()
    .tables
    .into_iter()
    .find(|table| table.table_id == name)
    .unwrap()
}

fn assert_signed_owner_contract(
    layout: OwnerLayout,
    name: &str,
    key: &str,
    value: &str,
    scope: TableScope,
    codec: &str,
    capabilities: u16,
    index: bool,
) {
    let table = owner_contract(layout, name);
    assert_eq!(table.key_type_id, key);
    assert_eq!(table.value_type_id, value);
    assert_eq!(table.scope, scope);
    assert_eq!(table.logical_codec_id, codec);
    assert_eq!(table.capabilities, capabilities);
    assert_eq!((table.index, table.derived), (index, index));
    assert_eq!(owner_table_access(name), OwnerTableAccess::DomainService);
}

#[test]
fn signed_store_private_layouts_match_current_provider_schemas() {
    let rw = CAP_READ | CAP_INSERT | CAP_UPDATE;
    assert_signed_owner_contract(
        OwnerLayout::Rbac,
        "rbac_v1",
        "&str",
        "&[u8]",
        TableScope::StorePrivate,
        "json-utf8-v1",
        rw,
        false,
    );
    for (name, caps) in [
        ("statechart_defs", CAP_READ | CAP_INSERT),
        ("statechart_instances", rw),
    ] {
        assert_signed_owner_contract(
            OwnerLayout::Statechart,
            name,
            "&str",
            "&[u8]",
            TableScope::StorePrivate,
            "msgpack-v1",
            caps,
            false,
        );
    }
    assert_signed_owner_contract(
        OwnerLayout::Kv,
        "kv",
        "(&str,&str)",
        "&[u8]",
        TableScope::StorePrivate,
        "raw-bytes-v1",
        CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE | CAP_CAS,
        false,
    );
}

#[test]
fn signed_timeseries_layout_matches_encoded_scope_provider_schema() {
    let caps = CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE;
    for (name, key, codec) in [
        ("series_chunks", "(&str,u64)", "packed-timeseries-chunk-v1"),
        ("series_meta", "&str", "msgpack-v1"),
        ("series_projection_state", "&str", "msgpack-v1"),
    ] {
        assert_signed_owner_contract(
            OwnerLayout::TimeSeries,
            name,
            key,
            "&[u8]",
            TableScope::Serving,
            codec,
            caps,
            false,
        );
    }
}

#[test]
fn signed_blob_layout_uses_domain_services_and_shared_cas_authority() {
    for (name, key, value, scope, codec, caps, access) in [
        (
            "cas_blobs",
            "(&str,&str)",
            "&[u8]",
            TableScope::Serving,
            "msgpack-v1",
            CAP_READ | CAP_INSERT | CAP_DELETE,
            OwnerTableAccess::DomainService,
        ),
        (
            "cas_chunks",
            "&str",
            "&[u8]",
            TableScope::SharedService,
            "raw-bytes-v1",
            CAP_READ | CAP_INSERT | CAP_DELETE,
            OwnerTableAccess::SharedService,
        ),
        (
            "cas_refcount",
            "&str",
            "u64",
            TableScope::SharedService,
            "redb-scalar-v1",
            CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE | CAP_CAS,
            OwnerTableAccess::SharedService,
        ),
        (
            "cas_uploads",
            "(&str,u64)",
            "&[u8]",
            TableScope::Serving,
            "msgpack-v1",
            CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE,
            OwnerTableAccess::DomainService,
        ),
    ] {
        let table = owner_contract(OwnerLayout::Blob, name);
        assert_eq!(
            (
                table.key_type_id.as_str(),
                table.value_type_id.as_str(),
                table.scope,
                table.logical_codec_id.as_str(),
                table.capabilities,
                owner_table_access(name),
            ),
            (key, value, scope, codec, caps, access)
        );
        assert_eq!((table.index, table.derived), (false, false));
    }
}

#[test]
fn signed_semantic_layout_is_closed_domain_service_authority() {
    for name in owner_table_names(OwnerLayout::SemanticIndex) {
        let table = owner_contract(OwnerLayout::SemanticIndex, name);
        let projection = matches!(
            *name,
            "semantic_binding_heads_v1"
                | "semantic_lexical_manifests_v1"
                | "semantic_ann_manifests_v1"
                | "semantic_vectors_v1"
        );
        assert_eq!(table.scope, TableScope::Serving);
        assert_eq!(owner_table_access(name), OwnerTableAccess::DomainService);
        assert_eq!((table.index, table.derived), (projection, projection));
        assert_eq!(
            table.capabilities,
            if projection {
                CAP_READ | CAP_INSERT | CAP_DELETE
            } else {
                CAP_READ | CAP_INSERT | CAP_UPDATE | CAP_DELETE
            }
        );
        assert_eq!(
            table.logical_codec_id,
            match *name {
                "semantic_binding_heads_v1" => "redb-scalar-v1",
                // `eg_ann` stores the ANN index's three opaque buffers
                // (meta/codes/refine), not a semantic-index row, so its codec
                // states what it actually is.
                "eg_ann" => "raw-bytes-v1",
                _ => "semantic-index-bytes-v1",
            }
        );
    }
}

#[test]
fn open_rejects_unknown_normal_and_multimap_tables() {
    for multimap in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("closed-world.redb");
        let physical = PhysicalStoreIdentity::new("physical:test:closed-world").unwrap();
        let store = create_physical(&path, physical.clone(), None, OwnerLayout::LedgerOnly).unwrap();
        let wtx = store.database().begin_write().unwrap();
        if multimap {
            let definition: MultimapTableDefinition<&str, &str> =
                MultimapTableDefinition::new("undeclared_multimap");
            wtx.open_multimap_table(definition).unwrap();
        } else {
            let definition: TableDefinition<&str, &str> = TableDefinition::new("undeclared_normal");
            wtx.open_table(definition).unwrap();
        }
        wtx.commit().unwrap();
        drop(store);
        assert!(open_physical(&path, physical, None, OwnerLayout::LedgerOnly).is_err());
    }
}

#[test]
fn write_rejects_missing_or_wrong_type_without_recreating_table() {
    for wrong_type in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wrong-table.redb");
        let physical = PhysicalStoreIdentity::new("physical:test:wrong-table").unwrap();
        let store = create_physical(&path, physical, None, OwnerLayout::Kv).unwrap();
        let wtx = store.database().begin_write().unwrap();
        wtx.delete_table(KV).unwrap();
        if wrong_type {
            let wrong: TableDefinition<&str, &str> = TableDefinition::new("kv");
            wtx.open_table(wrong).unwrap();
        }
        wtx.commit().unwrap();
        assert!(store.begin_write().is_err());
        let rtx = store.database().begin_read().unwrap();
        let names = rtx
            .list_tables()
            .unwrap()
            .map(|table| redb::TableHandle::name(&table).to_string())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.contains("kv"), wrong_type);
    }
}

#[test]
fn plain_recovery_rejects_every_known_mutation_table_marker() {
    let mut names = ledger_table_names()
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    for layout in owner_layouts() {
        names.extend(owner_table_names(layout));
    }
    // 18 ledger + 69 owner tables across the sixteen layouts. The
    // consumer-owned tables `path_index_v1`, `eg_ann`, `eg_kvcache_cold` and
    // the 20 `__sql_*` tables joined the registry because RF-RULING-004 puts
    // the complete physical table registry in the storage kernel; the seven
    // root-binary sidecar tables joined it when their raw opens were cut.
    assert_eq!(names.len(), 87);
    for (ordinal, name) in names.into_iter().enumerate() {
        assert!(is_known_mutation_table(name));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("known-marker-{ordinal}.redb"));
        let database = redb::Database::create(&path).unwrap();
        let write = database.begin_write().unwrap();
        let marker: TableDefinition<&str, &str> = TableDefinition::new(name);
        write.open_table(marker).unwrap();
        write.commit().unwrap();
        drop(database);
        assert!(classify_recovery_store(&path, RecoveryExpectation::Plain, None).is_err());
    }
}

#[test]
fn plain_recovery_rejects_every_retired_mutation_table_marker() {
    let names = retired_prototype_table_names();
    assert_eq!(names.len(), 11);
    assert!(names.contains(&"mutation_versions"));
    assert!(names.contains(&"mutation_store_root_v3"));
    for (ordinal, name) in names.iter().copied().enumerate() {
        assert!(is_retired_prototype_table(name));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(format!("retired-marker-{ordinal}.redb"));
        let database = redb::Database::create(&path).unwrap();
        let write = database.begin_write().unwrap();
        let marker: TableDefinition<&str, &str> = TableDefinition::new(name);
        write.open_table(marker).unwrap();
        write.commit().unwrap();
        drop(database);
        assert!(classify_recovery_store(&path, RecoveryExpectation::Plain, None).is_err());
    }
}

/// Every ledger table's declared contract types must equal its real redb K/V,
/// the same guarantee `validate_owner_registry_equality` gives owner tables.
#[test]
fn ledger_contract_types_match_their_real_redb_types() {
    crate::owner::registry::validate_ledger_registry_types().unwrap();
}
