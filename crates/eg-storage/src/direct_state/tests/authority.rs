use super::{
    super::{authority::*, contract::*, generation::*, journal::*, *},
    support::*,
};

#[test]
fn current_fence_accepts_exact_replay_and_rejects_equal_epoch_or_index_rollback() {
    let previous = OpenedCurrentFence {
        sha256: "a".repeat(64),
        scope: DirectStateScope::DefaultGlobal {
            control_group: DEFAULT_GROUP,
            control_applied_index: 20,
            authority_epoch: 4,
        },
    };
    assert_eq!(previous, previous.clone());
    let equal_epoch_different = OpenedCurrentFence {
        sha256: "b".repeat(64),
        scope: previous.scope.clone(),
    };
    assert!(equal_epoch_different
        .validate_successor_of(&previous)
        .is_err());
    let later_same_epoch = OpenedCurrentFence {
        sha256: "c".repeat(64),
        scope: DirectStateScope::DefaultGlobal {
            control_group: DEFAULT_GROUP,
            control_applied_index: 21,
            authority_epoch: 4,
        },
    };
    later_same_epoch.validate_successor_of(&previous).unwrap();
    let lower_index = OpenedCurrentFence {
        sha256: "d".repeat(64),
        scope: DirectStateScope::DefaultGlobal {
            control_group: DEFAULT_GROUP,
            control_applied_index: 19,
            authority_epoch: 5,
        },
    };
    assert!(lower_index.validate_successor_of(&previous).is_err());
}

#[tokio::test]
async fn read_session_pins_whole_generation_until_drop() {
    use std::time::Duration;

    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let session = authority.read_session().await.unwrap();
    assert!(session.get::<BlobValue>().is_ok());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), authority.begin_install())
            .await
            .is_err()
    );
    drop(session);
    let permit = tokio::time::timeout(Duration::from_secs(1), authority.begin_install())
        .await
        .unwrap();
    assert!(!authority.is_ready());
    drop(permit);
}

#[tokio::test]
async fn permits_cannot_cross_authorities() {
    let first = StateImageAuthority::new();
    let second = StateImageAuthority::new();
    *first.current.write().unwrap() = Some(generation(&first));
    *second.current.write().unwrap() = Some(generation(&second));
    first.ready.store(true, Ordering::Release);
    second.ready.store(true, Ordering::Release);
    let wrong = second.write().await.unwrap();
    let registry = make_registry(&first, providers()).unwrap();
    assert!(registry
        .capture_all(
            &wrong,
            DirectStateScope::DefaultGlobal {
                control_group: DEFAULT_GROUP,
                control_applied_index: 1,
                authority_epoch: 1,
            },
            DEFAULT_MAX_DIRECT_STATE_BYTES,
        )
        .is_err());
}

#[tokio::test]
async fn durable_tokens_cannot_cross_registries() {
    let first = StateImageAuthority::new();
    let second = StateImageAuthority::new();
    let registry = make_registry(&second, providers()).unwrap();
    let permit = second.begin_install().await;
    let file = tempfile::NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();
    let prepared = DurablePreparedJournal {
        authority_identity: first.identity.clone(),
        registry_identity: Arc::new(DirectStateRegistryIdentity {
            authority_identity: first.identity.clone(),
            contract_sha256: "f".repeat(64),
        }),
        journal: placeholder_journal(DirectStateInstallPhase::Prepared),
        path: path.clone(),
        authority_file: file.reopen().unwrap(),
        root: registry.journal_root.try_clone_token().unwrap(),
    };
    let assembled = AssembledDirectStateGeneration {
        authority_identity: first.identity.clone(),
        registry_identity: Arc::new(DirectStateRegistryIdentity {
            authority_identity: first.identity.clone(),
            contract_sha256: "f".repeat(64),
        }),
        source_journal_sha256: None,
        current_image_sha256: "b".repeat(64),
        generation: generation(&first),
    };
    assert!(registry
        .bind_recovered_prepared(&permit, prepared, assembled)
        .is_err());

    let current = DurableCurrentImage {
        authority_identity: first.identity.clone(),
        registry_identity: Arc::new(DirectStateRegistryIdentity {
            authority_identity: first.identity.clone(),
            contract_sha256: "f".repeat(64),
        }),
        image: DirectStateCurrentImage {
            schema_version: DIRECT_STATE_SCHEMA_VERSION,
            snapshot_sha256: "a".repeat(64),
            scope: DirectStateScope::DefaultGlobal {
                control_group: DEFAULT_GROUP,
                control_applied_index: 1,
                authority_epoch: 1,
            },
            sections: Vec::new(),
        },
        path,
        authority_file: file.reopen().unwrap(),
        root: registry.current_root.try_clone_token().unwrap(),
    };
    assert!(registry.recover_all(&permit, Some(&current), None).is_err());
}

#[tokio::test]
async fn one_authority_cannot_cross_compose_different_registry_roots() {
    let authority = StateImageAuthority::new();
    let first_root = private_tempdir();
    let second_root = private_tempdir();
    let first = make_registry(&authority, flow_providers(first_root.path())).unwrap();
    let second = make_registry(&authority, flow_providers(second_root.path())).unwrap();
    let current_path = second_root.path().join("staging/direct-state.current");
    let file = std::fs::File::create(&current_path).unwrap();
    let current = DurableCurrentImage {
        authority_identity: authority.identity.clone(),
        registry_identity: second.registry_identity.clone(),
        image: DirectStateCurrentImage {
            schema_version: DIRECT_STATE_SCHEMA_VERSION,
            snapshot_sha256: "a".repeat(64),
            scope: DirectStateScope::DefaultGlobal {
                control_group: DEFAULT_GROUP,
                control_applied_index: 1,
                authority_epoch: 1,
            },
            sections: Vec::new(),
        },
        path: current_path,
        authority_file: file,
        root: second.current_root.try_clone_token().unwrap(),
    };
    let assembled = AssembledDirectStateGeneration {
        authority_identity: authority.identity.clone(),
        registry_identity: first.registry_identity.clone(),
        source_journal_sha256: None,
        current_image_sha256: "b".repeat(64),
        generation: generation(&authority),
    };
    let permit = authority.begin_install().await;
    assert_eq!(
        second
            .publish_current(&authority, &permit, &current, assembled)
            .unwrap_err(),
        "direct-state Current publication belongs to another registry"
    );
}
