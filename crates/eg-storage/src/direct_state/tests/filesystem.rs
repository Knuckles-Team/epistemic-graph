use super::{
    super::{
        authority::*, capture::*, contract::*, filesystem::*, generation::*, journal::*,
        registry::*, *,
    },
    support::*,
};

#[test]
fn durable_records_require_bounded_exact_canonical_bytes() {
    let journal = placeholder_journal(DirectStateInstallPhase::Prepared);
    let canonical = rmp_serde::to_vec_named(&journal).unwrap();
    let decoded: DirectStateInstallJournal =
        journal::decode_canonical_durable_record(&canonical, "test journal").unwrap();
    assert_eq!(decoded, journal);

    let mut trailing = canonical;
    trailing.push(0xc0);
    assert!(
        journal::decode_canonical_durable_record::<DirectStateInstallJournal>(
            &trailing,
            "test journal"
        )
        .is_err()
    );

    let allocation_claim = [0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(
        journal::decode_canonical_durable_record::<DirectStateInstallJournal>(
            &allocation_claim,
            "test journal"
        )
        .is_err()
    );
}

#[tokio::test]
async fn durable_current_token_rejects_same_inode_rewrite() {
    let directory = private_tempdir();
    let current_path = directory.path().join("Current");
    let image = journal::current_image_from_journal(&placeholder_journal(
        DirectStateInstallPhase::Published,
    ))
    .unwrap();
    std::fs::write(&current_path, rmp_serde::to_vec_named(&image).unwrap()).unwrap();
    let authority = StateImageAuthority::new();
    let token = DurableCurrentImage {
        authority_identity: authority.identity.clone(),
        registry_identity: Arc::new(DirectStateRegistryIdentity {
            authority_identity: authority.identity.clone(),
            contract_sha256: "a".repeat(64),
        }),
        image,
        path: current_path.clone(),
        authority_file: File::open(&current_path).unwrap(),
        root: PinnedPrivateDirectory::open(directory.path()).unwrap(),
    };
    token.validate_live().unwrap();
    std::fs::write(&current_path, b"same inode, different bytes").unwrap();
    assert!(token.validate_live().is_err());
}

#[test]
fn durable_temporary_authority_is_reopened_read_only() {
    let directory = private_tempdir();
    let root = PinnedPrivateDirectory::open(directory.path()).unwrap();
    let (authority, _) = root
        .mutations()
        .write_temporary("Current", "test", b"canonical")
        .unwrap();
    let mut authority_ref = &authority;
    assert!(authority_ref.write_all(b"mutation").is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn replaced_provider_root_rejects_before_capture_callback() {
    use std::os::unix::fs::PermissionsExt;

    PROVIDER_CAPTURE_CALLS.with(|counter| counter.store(0, Ordering::Relaxed));
    let root = private_tempdir();
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let registry = make_registry(&authority, flow_providers(root.path())).unwrap();
    let staging = root.path().join("staging");
    let original = root.path().join("staging-original");
    std::fs::rename(&staging, &original).unwrap();
    std::fs::create_dir(&staging).unwrap();
    std::fs::set_permissions(&staging, std::fs::Permissions::from_mode(0o700)).unwrap();

    let capture = authority.write().await.unwrap();
    let result = registry.capture_all(
        &capture,
        DirectStateScope::DefaultGlobal {
            control_group: DEFAULT_GROUP,
            control_applied_index: 1,
            authority_epoch: 1,
        },
        1024,
    );
    assert!(result.is_err());
    assert_eq!(
        PROVIDER_CAPTURE_CALLS.with(|counter| counter.load(Ordering::Relaxed)),
        0
    );
    assert_eq!(std::fs::read_dir(&staging).unwrap().count(), 0);
}

#[cfg(unix)]
#[test]
fn ancestor_symlink_and_invalid_contract_have_no_filesystem_effect() {
    use std::os::unix::fs::symlink;

    let root = private_tempdir();
    let real = root.path().join("real");
    let leaf = real.join("leaf");
    ensure_private_directory(&real).unwrap();
    ensure_private_directory(&leaf).unwrap();
    symlink(&real, root.path().join("alias")).unwrap();
    assert!(PinnedPrivateDirectory::open(&root.path().join("alias/leaf")).is_err());

    let authority = StateImageAuthority::new();
    let providers = flow_providers(root.path());
    let mut owners = providers
        .iter()
        .map(|provider| provider.owner_manifest().unwrap())
        .collect::<Vec<_>>();
    owners[0].tables = vec!["wrong_rows".into()];
    let journal = root.path().join("durable-journal/direct-state.pending");
    let current = root.path().join("durable-current/direct-state.current");
    assert!(DirectStateRegistry::new(
        &authority,
        &journal,
        &current,
        DirectStateRegistryContract::new(owners).unwrap(),
        providers,
    )
    .is_err());
    assert!(!journal.parent().unwrap().exists());
    assert!(!current.parent().unwrap().exists());
}

#[tokio::test]
async fn repeated_failed_capture_attempts_leave_no_managed_files() {
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let permit = authority.write().await.unwrap();
    let root = private_tempdir();
    for _ in 0..2 {
        let result = capture_physical_image_source(
            &permit,
            root.path(),
            DirectStateDomain::Blob,
            &capture_binding(
                &permit,
                DirectStateScope::DefaultGlobal {
                    control_group: DEFAULT_GROUP,
                    control_applied_index: 1,
                    authority_epoch: 1,
                },
                root.path(),
            ),
            "a".repeat(64),
            1024,
            |path| {
                std::fs::write(path, b"partial").unwrap();
                Err("injected capture failure".into())
            },
        );
        assert!(result.is_err());
    }
    let managed = std::fs::read_dir(root.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| is_managed_capture_name(name, DirectStateDomain::Blob))
        .count();
    assert_eq!(managed, 0);
}

#[tokio::test]
async fn rejected_oversized_capture_is_removed_by_pinned_owner() {
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let permit = authority.write().await.unwrap();
    let root = private_tempdir();
    let result = capture_physical_image_source(
        &permit,
        root.path(),
        DirectStateDomain::Blob,
        &capture_binding(
            &permit,
            DirectStateScope::DefaultGlobal {
                control_group: DEFAULT_GROUP,
                control_applied_index: 1,
                authority_epoch: 1,
            },
            root.path(),
        ),
        "a".repeat(64),
        1,
        |path| std::fs::write(path, b"too large").map_err(|error| error.to_string()),
    );
    assert!(result.is_err());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[cfg(unix)]
#[test]
fn precreated_insecure_staging_root_is_rejected() {
    use std::os::unix::fs::PermissionsExt;

    let root = private_tempdir();
    let insecure = root.path().join("insecure");
    std::fs::create_dir(&insecure).unwrap();
    std::fs::set_permissions(&insecure, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(ensure_private_directory(&insecure).is_err());
}

#[test]
fn pinned_read_write_descriptor_copies_exact_bytes() {
    let directory = private_tempdir();
    let source_path = directory.path().join("source");
    let target_path = directory.path().join("target");
    std::fs::write(&source_path, b"original").unwrap();
    let source = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&source_path)
        .unwrap();
    let root = PinnedPrivateDirectory::open(directory.path()).unwrap();
    copy_regular_file_exact(&source, &root, "target", 8, &hex_sha256(b"original")).unwrap();
    assert_eq!(std::fs::read(target_path).unwrap(), b"original");
}

#[test]
fn capture_name_parser_rejects_reserved_prefix_lookalikes() {
    let canonical = capture_file_name(DirectStateDomain::Blob, 12, 34).unwrap();
    assert!(is_managed_capture_name(&canonical, DirectStateDomain::Blob));
    for lookalike in [
        ".direct-state-blob.capture.operator-backup",
        ".direct-state-blob.capture.12.34.extra",
        ".direct-state-blob.capture.012.34",
        ".direct-state-blob.capture.12.0",
    ] {
        assert!(!is_managed_capture_name(lookalike, DirectStateDomain::Blob));
    }
}
