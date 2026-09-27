use super::{
    existing_semantic_service, legacy_semantic_owner_dir, open_semantic_service,
    refuse_split_semantic_owner, sanitize_owner_segment, tenant_semantic_migration_fence_file,
    tenant_semantic_owner_file,
};

#[test]
fn v2_owner_path_remains_readable_and_v3_marker_fences_split_authority() {
    let persist_dir = std::env::temp_dir().join(format!(
        "eg-semantic-owner-upgrade-{}",
        uuid::Uuid::new_v4()
    ));
    let tenant = format!("tenant:organic:19:{}", uuid::Uuid::new_v4());
    let binding = "binding-a";
    let legacy = legacy_semantic_owner_dir(&persist_dir, &tenant, binding);
    assert_eq!(
        legacy,
        persist_dir
            .join("semantic-index")
            .join(sanitize_owner_segment(&tenant))
            .join(sanitize_owner_segment(binding))
    );
    let owner = open_semantic_service(&persist_dir, &tenant, binding)
        .expect("the existing per-binding format must still open");
    assert!(legacy.is_dir());
    assert!(existing_semantic_service(&persist_dir, &tenant, binding).is_ok());

    let v3 = tenant_semantic_owner_file(&persist_dir, &tenant);
    assert_ne!(v3, legacy);
    std::fs::write(&v3, b"reserved migration marker").unwrap();
    assert!(refuse_split_semantic_owner(&persist_dir, &tenant).is_err());
    assert!(open_semantic_service(&persist_dir, &tenant, binding).is_err());
    assert!(existing_semantic_service(&persist_dir, &tenant, binding).is_err());
    drop(owner);
    let _ = std::fs::remove_dir_all(persist_dir);
}

#[test]
fn durable_fence_blocks_v2_after_v3_name_disappears() {
    let persist_dir = std::env::temp_dir().join(format!(
        "eg-semantic-migration-fence-{}",
        uuid::Uuid::new_v4()
    ));
    let tenant = format!("tenant:{}", uuid::Uuid::new_v4());
    let binding = "binding-a";
    let prior = open_semantic_service(&persist_dir, &tenant, binding).unwrap();
    let v3 = tenant_semantic_owner_file(&persist_dir, &tenant);
    std::fs::write(&v3, b"v3 marker").unwrap();
    std::fs::write(
        tenant_semantic_migration_fence_file(&persist_dir, &tenant),
        b"durable fence marker",
    )
    .unwrap();
    std::fs::remove_file(&v3).unwrap();
    assert!(refuse_split_semantic_owner(&persist_dir, &tenant).is_err());
    assert!(open_semantic_service(&persist_dir, &tenant, binding).is_err());
    assert!(existing_semantic_service(&persist_dir, &tenant, binding).is_err());
    drop(prior);
    let _ = std::fs::remove_dir_all(persist_dir);
}

#[test]
fn tenant_owner_path_discriminates_delimiter_collisions() {
    let persist_dir = std::path::Path::new("/tmp/semantic-owner-layout-test");
    let first = tenant_semantic_owner_file(persist_dir, "tenant:a");
    let second = tenant_semantic_owner_file(persist_dir, "tenant_a");
    assert_ne!(first, second);
    assert!(first
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("semantic_tenant-"));
    assert!(second
        .file_name()
        .unwrap()
        .to_string_lossy()
        .starts_with("semantic_tenant-"));
}
