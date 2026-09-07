use super::{
    super::{authority::*, capture::*, contract::*, filesystem::*, generation::*, journal::*, *},
    support::*,
};

#[tokio::test]
async fn pending_abandonment_rejects_canonical_name_resurrection() {
    let root = private_tempdir();
    let authority = StateImageAuthority::new();
    let registry = make_registry(&authority, flow_providers(root.path())).unwrap();
    let install = authority.begin_install().await;
    let current_image = journal::current_image_from_journal(&placeholder_journal(
        DirectStateInstallPhase::Published,
    ))
    .unwrap();
    let current_bytes = rmp_serde::to_vec_named(&current_image).unwrap();
    std::fs::write(&registry.current_path, current_bytes).unwrap();
    let current = DurableCurrentImage {
        authority_identity: authority.identity.clone(),
        registry_identity: registry.registry_identity.clone(),
        image: current_image,
        path: registry.current_path.clone(),
        authority_file: File::open(&registry.current_path).unwrap(),
        root: registry.current_root.try_clone_token().unwrap(),
    };
    let live_name = registry
        .journal_root
        .validate_path(&registry.journal_path, "test Pending journal")
        .unwrap()
        .to_string();
    let quarantine_name = format!(".{live_name}.abandoned.test");
    let quarantine_path = registry.journal_root.path.join(&quarantine_name);
    std::fs::write(&quarantine_path, b"retired Pending").unwrap();
    let pending_authority = File::open(&quarantine_path).unwrap();
    std::fs::write(&registry.journal_path, b"replacement Pending").unwrap();
    let recovery = PendingAbandonRecovery {
        current,
        assembled: AssembledDirectStateGeneration {
            authority_identity: authority.identity.clone(),
            registry_identity: registry.registry_identity.clone(),
            source_journal_sha256: None,
            current_image_sha256: "c".repeat(64),
            generation: generation(&authority),
        },
        journal_root: registry.journal_root.try_clone_token().unwrap(),
        pending_authority,
        live_name,
        quarantine_name,
        retirement_complete: false,
        first_error: "injected".into(),
    };

    let recovery = match registry.retry_pending_abandonment(&install, recovery) {
        Err(recovery) => recovery,
        Ok(_) => panic!("a resurrected canonical Pending name must fail closed"),
    };
    assert!(recovery.retirement_complete);
    assert_eq!(
        std::fs::read(&registry.journal_path).unwrap(),
        b"replacement Pending"
    );
    assert!(!quarantine_path.exists());
}

#[tokio::test]
async fn prepared_provider_view_rejects_path_replacement() {
    let root = private_tempdir();
    let generations = root.path().join("generations");
    ensure_private_directory(&generations).unwrap();
    let authority = StateImageAuthority::new();
    let permit = authority.begin_install().await;
    let bytes = b"pinned-prepared";
    let manifest = DirectStateSectionManifest {
        schema_version: DIRECT_STATE_SCHEMA_VERSION,
        domain: DirectStateDomain::Blob,
        scope: DirectStateScope::DefaultGlobal {
            control_group: DEFAULT_GROUP,
            control_applied_index: 1,
            authority_epoch: 1,
        },
        capture_set_sha256: "a".repeat(64),
        source_generation_sha256: "b".repeat(64),
        owner_manifest_sha256: "c".repeat(64),
        logical_bytes: bytes.len() as u64,
        chunk_count: 1,
        content_sha256: hex_sha256(bytes),
    };
    let incoming_path = root.path().join("incoming");
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut incoming_file = options.open(&incoming_path).unwrap();
    incoming_file.write_all(bytes).unwrap();
    incoming_file.sync_all().unwrap();
    incoming_file.seek(SeekFrom::Start(0)).unwrap();
    let registry_identity = Arc::new(DirectStateRegistryIdentity {
        authority_identity: permit.authority_identity.clone(),
        contract_sha256: "e".repeat(64),
    });
    let incoming = DirectStatePhysicalImage {
        authority_identity: permit.authority_identity.clone(),
        registry_identity: registry_identity.clone(),
        roots: RegisteredDirectStateRoots {
            registry_identity,
            domain: DirectStateDomain::Blob,
            staging: PinnedPrivateDirectory::open(root.path()).unwrap(),
            generations: PinnedPrivateDirectory::open(&generations).unwrap(),
        },
        path: incoming_path,
        domain: DirectStateDomain::Blob,
        manifest_sha256: manifest.sha256().unwrap(),
        authority_file: incoming_file,
        cleanup_on_drop: true,
    };
    let prepared = prepare_mutable_generation(&permit, incoming, &manifest, &generations).unwrap();
    let prepared_path = generations.join(format!(
        ".direct-state-blob-{}.prepared",
        manifest.sha256().unwrap()
    ));
    let replacement = generations.join("replacement");
    let result = prepared.with_pinned_path(|descriptor_path| {
        assert_eq!(std::fs::read(descriptor_path).unwrap(), bytes);
        std::fs::write(&replacement, b"substitution").unwrap();
        std::fs::rename(&replacement, &prepared_path).unwrap();
        Ok(())
    });
    assert!(result.is_err());
    assert_eq!(std::fs::read(prepared_path).unwrap(), b"substitution");
}

#[tokio::test]
async fn one_authority_cannot_cross_registry_staging_or_recovery_tokens() {
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let first_root = private_tempdir();
    let second_root = private_tempdir();
    let first = make_registry(&authority, flow_providers(first_root.path())).unwrap();
    let second = make_registry(&authority, flow_providers(second_root.path())).unwrap();
    let scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 2,
        authority_epoch: 1,
    };
    let captured = {
        let capture = authority.write().await.unwrap();
        first.capture_all(&capture, scope, 1024).unwrap()
    };
    let install = authority.begin_install().await;
    let validated = first.validate_all(&install, captured, 1024, 2, 1).unwrap();
    let staged = first.stage_all(&install, validated).unwrap();
    let journal = staged.prepared_journal("a".repeat(64)).unwrap();
    let staged_recovery = PreparedGenerationRecovery {
        staged,
        journal_path: first.journal_path.clone(),
        journal: journal.clone(),
        first_error: "injected".into(),
    };
    assert!(staged_recovery.retry(&second, &install).is_err());
    assert!(!second.journal_path.exists());

    let first_current_path = first.current_path.clone();
    let current_file = File::create(&first_current_path).unwrap();
    let current = DurableCurrentImage {
        authority_identity: authority.identity.clone(),
        registry_identity: first.registry_identity.clone(),
        image: DirectStateCurrentImage {
            schema_version: DIRECT_STATE_SCHEMA_VERSION,
            snapshot_sha256: "b".repeat(64),
            scope: DirectStateScope::DefaultGlobal {
                control_group: DEFAULT_GROUP,
                control_applied_index: 2,
                authority_epoch: 1,
            },
            sections: Vec::new(),
        },
        path: first_current_path,
        authority_file: current_file,
        root: first.current_root.try_clone_token().unwrap(),
    };
    let cleanup = CurrentCleanupRecovery {
        current: current.try_clone_token().unwrap(),
        first_error: "injected".into(),
    };
    assert!(second.retry_current_cleanup(&install, cleanup).is_err());
    let journal_file = File::create(&first.journal_path).unwrap();
    let durability = CurrentDurabilityRecovery {
        current: current.try_clone_token().unwrap(),
        published: DurablePublishedJournal {
            authority_identity: authority.identity.clone(),
            registry_identity: first.registry_identity.clone(),
            journal: placeholder_journal(DirectStateInstallPhase::Published),
            path: first.journal_path.clone(),
            authority_file: journal_file,
            root: first.journal_root.try_clone_token().unwrap(),
            retirement_quarantine: None,
            retirement_complete: false,
        },
        displaced_retirement: None,
        first_error: "injected".into(),
    };
    assert!(second
        .retry_current_durability(&install, durability)
        .is_err());
    let abandon = PendingAbandonRecovery {
        current,
        assembled: AssembledDirectStateGeneration {
            authority_identity: authority.identity.clone(),
            registry_identity: first.registry_identity.clone(),
            source_journal_sha256: None,
            current_image_sha256: "c".repeat(64),
            generation: generation(&authority),
        },
        journal_root: first.journal_root.try_clone_token().unwrap(),
        pending_authority: File::open(&first.journal_path).unwrap(),
        live_name: "direct-state.pending".into(),
        quarantine_name: ".direct-state.pending.abandoned.test".into(),
        retirement_complete: false,
        first_error: "injected".into(),
    };
    assert!(second.retry_pending_abandonment(&install, abandon).is_err());

    let journal_file = File::create(&first.journal_path).unwrap();
    let prepared_recovery = PreparedJournalRecovery {
        authority_identity: authority.identity.clone(),
        registry_identity: first.registry_identity.clone(),
        journal: placeholder_journal(DirectStateInstallPhase::Prepared),
        path: first.journal_path.clone(),
        authority_file: journal_file,
        root: first.journal_root.try_clone_token().unwrap(),
        temporary_retirement: None,
        prepared: VisiblePreparedState {
            assembled: AssembledDirectStateGeneration {
                authority_identity: authority.identity.clone(),
                registry_identity: first.registry_identity.clone(),
                source_journal_sha256: None,
                current_image_sha256: "d".repeat(64),
                generation: generation(&authority),
            },
            retained_images: Vec::new(),
        },
        first_error: "injected".into(),
    };
    assert!(prepared_recovery
        .retry_durability(&second, &install)
        .is_err());
    let published_recovery = PublishedJournalRecovery {
        durable: DurablePublishedJournal {
            authority_identity: authority.identity.clone(),
            registry_identity: first.registry_identity.clone(),
            journal: placeholder_journal(DirectStateInstallPhase::Published),
            path: first.journal_path.clone(),
            authority_file: OpenOptions::new()
                .read(true)
                .open(&first.journal_path)
                .unwrap(),
            root: first.journal_root.try_clone_token().unwrap(),
            retirement_quarantine: None,
            retirement_complete: false,
        },
        displaced_retirement: None,
        first_error: "injected".into(),
    };
    assert!(published_recovery
        .retry_durability(&second, &install)
        .is_err());
}

#[tokio::test]
async fn current_durability_retry_rejects_published_journal_path_replacement() {
    let root = private_tempdir();
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    let registry = make_registry(&authority, flow_providers(root.path())).unwrap();
    let scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 2,
        authority_epoch: 1,
    };
    let captured = {
        let capture = authority.write().await.unwrap();
        registry.capture_all(&capture, scope, 1024).unwrap()
    };
    let install = authority.begin_install().await;
    let received = registry
        .receive_all(&install, wire_roundtrip(&registry, &install, captured))
        .unwrap();
    let validated = registry
        .validate_all(&install, received, 1024, 2, 1)
        .unwrap();
    let staged = registry.stage_all(&install, validated).unwrap();
    let mut journal = staged.prepared_journal("a".repeat(64)).unwrap();
    journal.phase = DirectStateInstallPhase::Published;
    let current_image = journal::current_image_from_journal(&journal).unwrap();
    let current_bytes = rmp_serde::to_vec_named(&current_image).unwrap();
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut current_file = options.open(&registry.current_path).unwrap();
    current_file.write_all(&current_bytes).unwrap();
    current_file.sync_all().unwrap();
    sync_directory(registry.current_path.parent().unwrap()).unwrap();
    let current = DurableCurrentImage {
        authority_identity: authority.identity.clone(),
        registry_identity: registry.registry_identity.clone(),
        image: current_image,
        path: registry.current_path.clone(),
        authority_file: current_file,
        root: registry.current_root.try_clone_token().unwrap(),
    };
    let published = write_test_published(&authority, &registry, &registry.journal_path, journal);
    let recovery = CurrentDurabilityRecovery {
        current,
        published,
        displaced_retirement: None,
        first_error: "injected Current parent fsync failure".into(),
    };

    std::fs::remove_file(&registry.journal_path).unwrap();
    let replacement = b"unrelated replacement journal";
    std::fs::write(&registry.journal_path, replacement).unwrap();
    let recovery = match registry.retry_current_durability(&install, recovery) {
        Err(recovery) => recovery,
        Ok(_) => panic!("replacement must keep Current recovery required"),
    };
    assert!(recovery.first_error().contains("path identity changed"));
    assert_eq!(std::fs::read(&registry.journal_path).unwrap(), replacement);
}

#[tokio::test]
async fn failed_install_leaves_reads_and_capture_closed() {
    let authority = StateImageAuthority::new();
    *authority.current.write().unwrap() = Some(generation(&authority));
    authority.ready.store(true, Ordering::Release);
    drop(authority.begin_install().await);
    assert!(authority.read_session().await.is_err());
    assert!(authority.write().await.is_err());
}
