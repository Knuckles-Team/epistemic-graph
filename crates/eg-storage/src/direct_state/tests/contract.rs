use super::{
    super::{authority::*, contract::*, *},
    support::*,
};

#[test]
fn registry_domain_order_is_canonical_and_duplicates_fail_closed() {
    let authority = StateImageAuthority::new();
    let registry = make_registry(&authority, providers()).unwrap();
    assert_eq!(
        registry.domains().collect::<Vec<_>>(),
        DirectStateDomain::ALL
    );
    let mut duplicate = providers();
    duplicate.push(
        providers()
            .into_iter()
            .find(|provider| provider.domain() == DirectStateDomain::Blob)
            .unwrap(),
    );
    assert!(make_registry(&authority, duplicate).is_err());
    assert!(make_registry(
        &authority,
        providers()
            .into_iter()
            .filter(|provider| provider.domain() != DirectStateDomain::Blob)
            .collect(),
    )
    .is_err());
}

#[test]
fn manifests_reject_wrong_owner_and_unbounded_or_incoherent_sections() {
    let mut manifest = DirectStateSectionManifest {
        schema_version: DIRECT_STATE_SCHEMA_VERSION,
        domain: DirectStateDomain::Blob,
        scope: DirectStateScope::DefaultGlobal {
            control_group: DEFAULT_GROUP,
            control_applied_index: 1,
            authority_epoch: 2,
        },
        capture_set_sha256: "c".repeat(64),
        source_generation_sha256: "d".repeat(64),
        owner_manifest_sha256: "a".repeat(64),
        logical_bytes: 1,
        chunk_count: 1,
        content_sha256: "b".repeat(64),
    };
    manifest.validate(1).unwrap();
    manifest.owner_manifest_sha256 = "A".repeat(64);
    assert!(manifest.validate(1).is_err());
    manifest.owner_manifest_sha256 = "a".repeat(64);
    manifest.logical_bytes = 2;
    assert!(manifest.validate(1).is_err());
    manifest.logical_bytes = 0;
    assert!(manifest.validate(1).is_err());
    manifest.logical_bytes = 1;
    manifest.scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 1,
        authority_epoch: 0,
    };
    assert!(manifest.validate(1).is_err());
}

#[test]
fn owner_manifest_is_static_canonical_layout_not_runtime_epoch() {
    let owner = DirectStateOwnerManifest {
        schema_version: DIRECT_STATE_SCHEMA_VERSION,
        domain: DirectStateDomain::Blob,
        authority_kind: DirectStateAuthorityKind::MutationStore,
        store_schema_version: 1,
        tables: vec!["blob_chunks".to_string(), "scope_bindings".to_string()],
    };
    let first = owner.sha256().unwrap();
    assert_eq!(first, owner.sha256().unwrap());
    let mut unordered = owner;
    unordered.tables.reverse();
    assert!(unordered.sha256().is_err());
}
