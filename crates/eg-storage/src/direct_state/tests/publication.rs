use super::{
    super::{authority::*, contract::*, filesystem::*, journal::*, registry::*, *},
    support::*,
};

#[cfg(unix)]
#[test]
fn no_replace_publication_preserves_existing_inode_and_bytes() {
    let root = private_tempdir();
    let pinned = PinnedPrivateDirectory::open(root.path()).unwrap();
    let mut source = pinned
        .mutations()
        .create_new("source", "create source")
        .unwrap();
    source.write_all(b"source").unwrap();
    source.sync_all().unwrap();
    let mut target = pinned
        .mutations()
        .create_new("target", "create target")
        .unwrap();
    target.write_all(b"target").unwrap();
    target.sync_all().unwrap();
    let expected_identity = FlowProvider::physical_authority(&root.path().join("target")).unwrap();

    assert!(pinned
        .mutations()
        .link_no_replace("source", &pinned, "target", "test no-replace")
        .is_err());
    assert_eq!(
        std::fs::read(root.path().join("target")).unwrap(),
        b"target"
    );
    assert_eq!(
        FlowProvider::physical_authority(&root.path().join("target")).unwrap(),
        expected_identity
    );
}

#[cfg(unix)]
#[test]
fn exact_retirement_preserves_substitutes_and_never_retargets_reused_name() {
    let root = private_tempdir();
    let pinned = PinnedPrivateDirectory::open(root.path()).unwrap();
    std::fs::write(root.path().join("victim"), b"original").unwrap();
    let expected = File::open(root.path().join("victim")).unwrap();
    let mut substituted =
        ExactRetirement::prepare(&pinned, &expected, "victim", "test exact retirement").unwrap();
    std::fs::rename(root.path().join("victim"), root.path().join("original")).unwrap();
    std::fs::write(root.path().join("victim"), b"substitute").unwrap();
    assert!(substituted.retry().is_err());
    assert_eq!(
        std::fs::read(root.path().join("original")).unwrap(),
        b"original"
    );
    assert_eq!(
        std::fs::read(root.path().join("victim")).unwrap(),
        b"substitute"
    );
    assert!(std::fs::read_dir(root.path())
        .unwrap()
        .filter_map(Result::ok)
        .all(|entry| {
            !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".victim.retired.")
        }));

    std::fs::write(root.path().join("complete"), b"retire me").unwrap();
    let complete_file = File::open(root.path().join("complete")).unwrap();
    let mut complete =
        ExactRetirement::prepare(&pinned, &complete_file, "complete", "completed retirement")
            .unwrap();
    complete.retry().unwrap();
    std::fs::write(root.path().join("complete"), b"new authority").unwrap();
    complete.retry().unwrap();
    assert_eq!(
        std::fs::read(root.path().join("complete")).unwrap(),
        b"new authority"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn post_sync_retirement_failure_cannot_publish_and_retry_retires_exact_link() {
    struct RetirementFaultGuard;

    impl RetirementFaultGuard {
        fn arm() -> Self {
            FAIL_PREPARED_RETIREMENT_AFTER_SYNC.with(|fault| fault.set(true));
            Self
        }

        fn disarm(&self) {
            FAIL_PREPARED_RETIREMENT_AFTER_SYNC.with(|fault| fault.set(false));
        }
    }

    impl Drop for RetirementFaultGuard {
        fn drop(&mut self) {
            self.disarm();
        }
    }

    let root = private_tempdir();
    let journal_path = root.path().join("direct-state.pending");
    let current_path = root.path().join("direct-state.current");
    let authority = StateImageAuthority::new();
    let registry = registry_at(
        &authority,
        &journal_path,
        &current_path,
        flow_providers(root.path()),
    )
    .unwrap();
    let install = authority.begin_install().await;
    let scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 1,
        authority_epoch: 1,
    };
    let captured = registry
        .capture_initial_generation(&install, &journal_path, &current_path, scope, 1024)
        .unwrap();
    let received = registry
        .receive_all(&install, wire_roundtrip(&registry, &install, captured))
        .unwrap();
    let validated = registry
        .validate_all(&install, received, 1024, 1, 1)
        .unwrap();
    let fault = RetirementFaultGuard::arm();
    let staged = registry.stage_all(&install, validated).unwrap();
    let journal = staged.prepared_journal("a".repeat(64)).unwrap();
    let recovery = match registry
        .write_prepared_journal_no_replace(&install, &journal_path, staged, &journal)
        .unwrap()
    {
        PreparedJournalPublication::GenerationRecoveryRequired(recovery) => recovery,
        _ => panic!("post-sync retirement debt must block Prepared publication"),
    };
    assert!(!journal_path.exists());

    fault.disarm();
    let prepared = match recovery.retry(&registry, &install).unwrap() {
        PreparedJournalPublication::Durable(prepared) => prepared,
        _ => panic!("retirement retry must complete Prepared publication"),
    };
    assert!(journal_path.exists());
    assert!(prepared.journal().validate_live().is_ok());
    assert!(std::fs::read_dir(root.path().join("generations"))
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .all(|name| !name.ends_with(".prepared")));
}

fn require_durable_current(
    registry: &DirectStateRegistry,
    install: &StateImageInstallPermit,
    promotion: CurrentPromotion,
) -> DurableCurrentImage {
    match promotion {
        CurrentPromotion::Durable(current) => current,
        CurrentPromotion::DurabilityRecoveryRequired(recovery) => {
            match registry.retry_current_durability(install, recovery) {
                Ok(CurrentPromotion::Durable(current)) => current,
                Ok(CurrentPromotion::CleanupRecoveryRequired(recovery)) => registry
                    .retry_current_cleanup(install, recovery)
                    .unwrap_or_else(|_| panic!("test filesystem must finish Current cleanup")),
                Ok(CurrentPromotion::DurabilityRecoveryRequired(_)) | Err(_) => {
                    panic!("test filesystem must make Current durable")
                }
            }
        }
        CurrentPromotion::CleanupRecoveryRequired(recovery) => registry
            .retry_current_cleanup(install, recovery)
            .unwrap_or_else(|_| panic!("test filesystem must complete Current cleanup")),
    }
}

#[tokio::test]
async fn seven_domain_prepared_crash_recovers_publishes_and_restarts_current() {
    let root = private_tempdir();
    let journal_path = root.path().join("direct-state.pending");
    let current_path = root.path().join("direct-state.current");
    let scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 9,
        authority_epoch: 1,
    };

    let first = StateImageAuthority::new();
    let registry = registry_at(
        &first,
        &journal_path,
        &current_path,
        flow_providers(root.path()),
    )
    .unwrap();
    let install = first.begin_install().await;
    let captured = registry
        .capture_initial_generation(&install, &journal_path, &current_path, scope.clone(), 1024)
        .unwrap();
    let received = registry
        .receive_all(&install, wire_roundtrip(&registry, &install, captured))
        .unwrap();
    let validated = registry
        .validate_all(&install, received, 1024, 9, 1)
        .unwrap();
    let staged = registry.stage_all(&install, validated).unwrap();
    let prepared_journal = staged.prepared_journal("a".repeat(64)).unwrap();
    let prepared = match registry
        .write_prepared_journal_no_replace(&install, &journal_path, staged, &prepared_journal)
        .unwrap()
    {
        PreparedJournalPublication::Durable(prepared) => prepared,
        _ => panic!("test filesystem must make Prepared durable"),
    };
    drop(prepared);
    drop(install);
    drop(registry);
    drop(first);

    // Crash/restart after Prepared: the new process authenticates all seven
    // retained Incoming files before any provider may reopen a generation.
    let second = StateImageAuthority::new();
    let registry = registry_at(
        &second,
        &journal_path,
        &current_path,
        flow_providers(root.path()),
    )
    .unwrap();
    let install = second.begin_install().await;
    let durable_prepared = registry
        .load_prepared_journal(&install, &journal_path)
        .unwrap()
        .unwrap();
    let assembled = registry
        .recover_all(
            &install,
            None,
            Some(DurablePendingJournal::Prepared(&durable_prepared)),
        )
        .unwrap()
        .into_ready()
        .unwrap();
    let prepared = registry
        .bind_recovered_prepared(&install, durable_prepared, assembled)
        .unwrap();
    let mut published_journal = prepared.journal().journal().clone();
    published_journal.phase = DirectStateInstallPhase::Published;
    let published = match registry
        .transition_prepared_to_published(&install, &journal_path, &prepared, &published_journal)
        .unwrap()
    {
        PublishedJournalPublication::Durable(published) => published,
        PublishedJournalPublication::RecoveryRequired(recovery) => {
            recovery.retry_durability(&registry, &install).unwrap()
        }
    };
    let promotion = registry
        .promote_published_to_current(&install, &journal_path, &current_path, &published)
        .unwrap();
    let current = require_durable_current(&registry, &install, promotion);
    let completion = registry
        .publish_current(&second, &install, &current, prepared.into_assembled())
        .unwrap();
    second.open(install, &current, completion).unwrap();
    assert_eq!(
        second
            .read_session()
            .await
            .unwrap()
            .get::<BlobValue>()
            .unwrap()
            .0,
        7
    );

    // A later snapshot in the same authority epoch advances by its strictly
    // newer applied index, and successful cleanup retains exactly one physical
    // generation per closed domain without requiring a process restart.
    let next_scope = DirectStateScope::DefaultGlobal {
        control_group: DEFAULT_GROUP,
        control_applied_index: 10,
        authority_epoch: 1,
    };
    let captured = {
        let capture = second.write().await.unwrap();
        registry.capture_all(&capture, next_scope, 1024).unwrap()
    };
    let install = second.begin_install().await;
    let received = registry
        .receive_all(&install, wire_roundtrip(&registry, &install, captured))
        .unwrap();
    let validated = registry
        .validate_all(&install, received, 1024, 10, 1)
        .unwrap();
    let staged = registry.stage_all(&install, validated).unwrap();
    let prepared_journal = staged.prepared_journal("c".repeat(64)).unwrap();
    let prepared = match registry
        .write_prepared_journal_no_replace(&install, &journal_path, staged, &prepared_journal)
        .unwrap()
    {
        PreparedJournalPublication::Durable(prepared) => prepared,
        _ => panic!("test filesystem must make second Prepared durable"),
    };
    let mut published_journal = prepared.journal().journal().clone();
    published_journal.phase = DirectStateInstallPhase::Published;
    let published = match registry
        .transition_prepared_to_published(&install, &journal_path, &prepared, &published_journal)
        .unwrap()
    {
        PublishedJournalPublication::Durable(published) => published,
        PublishedJournalPublication::RecoveryRequired(recovery) => {
            recovery.retry_durability(&registry, &install).unwrap()
        }
    };
    let promotion = registry
        .promote_published_to_current(&install, &journal_path, &current_path, &published)
        .unwrap();
    let current = require_durable_current(&registry, &install, promotion);
    let completion = registry
        .publish_current(&second, &install, &current, prepared.into_assembled())
        .unwrap();
    second.open(install, &current, completion).unwrap();
    let generation_count = std::fs::read_dir(root.path().join("generations"))
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| name.ends_with(".image"))
        .count();
    assert_eq!(generation_count, DirectStateDomain::ALL.len());

    // Exact Published replay keeps the existing Current inode. A different
    // image at the same epoch is rejected before the durable pointer changes.
    let install = second.begin_install().await;
    let original_bytes = std::fs::read(&current_path).unwrap();
    let original_identity = FlowProvider::physical_authority(&current_path).unwrap();
    let exact_journal = DirectStateInstallJournalV1 {
        schema_version: current.image().schema_version,
        snapshot_sha256: current.image().snapshot_sha256.clone(),
        scope: current.image().scope.clone(),
        phase: DirectStateInstallPhase::Published,
        sections: current.image().sections.clone(),
    };
    let exact = write_test_published(&second, &registry, &journal_path, exact_journal);
    assert!(matches!(
        registry
            .promote_published_to_current(&install, &journal_path, &current_path, &exact)
            .unwrap(),
        CurrentPromotion::Durable(_)
    ));
    assert_eq!(
        FlowProvider::physical_authority(&current_path).unwrap(),
        original_identity
    );
    assert!(!journal_path.exists());

    let stale_journal = DirectStateInstallJournalV1 {
        schema_version: current.image().schema_version,
        snapshot_sha256: "b".repeat(64),
        scope: current.image().scope.clone(),
        phase: DirectStateInstallPhase::Published,
        sections: current.image().sections.clone(),
    };
    let stale = write_test_published(&second, &registry, &journal_path, stale_journal);
    assert!(registry
        .promote_published_to_current(&install, &journal_path, &current_path, &stale)
        .is_err());
    assert_eq!(std::fs::read(&current_path).unwrap(), original_bytes);
    assert_eq!(
        FlowProvider::physical_authority(&current_path).unwrap(),
        original_identity
    );
    std::fs::remove_file(&journal_path).unwrap();
    sync_directory(root.path()).unwrap();
    drop(install);
    drop(registry);
    drop(second);

    // Ordinary restart trusts the exact mutable Current physical identities,
    // not stale snapshot-time byte hashes.
    let third = StateImageAuthority::new();
    let registry = registry_at(
        &third,
        &journal_path,
        &current_path,
        flow_providers(root.path()),
    )
    .unwrap();
    let install = third.begin_install().await;
    let current = registry
        .read_current_image(&install, &current_path)
        .unwrap()
        .unwrap();
    let assembled = registry
        .recover_all(&install, Some(&current), None)
        .unwrap()
        .into_ready()
        .unwrap();
    let completion = registry
        .publish_current(&third, &install, &current, assembled)
        .unwrap();
    third.open(install, &current, completion).unwrap();
    assert_eq!(
        third
            .read_session()
            .await
            .unwrap()
            .get::<BlobValue>()
            .unwrap()
            .0,
        7
    );
}
