//! Whole-image optimistic concurrency under the existing RBAC owner.
use super::*;
use crate::acl::{AgentRole, Role};
use crate::isolation::IsolationLayer;
use crate::test_threads::{BoundedThreads, StartGate};
use std::sync::{mpsc, Arc};
use std::time::Duration;

fn directory(label: &str) -> std::path::PathBuf {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "eg-rbac-cas-{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

fn assert_stale(result: Result<u64, RbacPersistError>, expected: u64, actual: u64) {
    assert!(matches!(result, Err(RbacPersistError::StaleRevision {
        expected: e, actual: a
    }) if e == expected && a == actual));
}

fn exercise_stale_image(store: &dyn RbacPolicyStore) {
    let before = store.authority_snapshot().unwrap();
    let mut policy = before.policy().clone();
    policy.add_role(Role::new("current-role"));
    let identities = BTreeMap::from([(
        "current-agent".into(),
        AgentIdentity {
            agent_id: "current-agent".into(),
            role: AgentRole::Agent,
            teams: vec![],
            roles: vec!["current-role".into()],
        },
    )]);
    let committed = store
        .compare_and_save(
            before.revision(),
            &policy,
            &identities,
            IdentityBootstrapState::Consumed,
        )
        .unwrap();
    assert_eq!(committed, before.revision() + 1);
    assert_stale(
        store.compare_and_save(
            before.revision(),
            before.policy(),
            before.identities(),
            before.bootstrap(),
        ),
        before.revision(),
        committed,
    );
    // Equal target bytes do not turn a stale preimage into a valid write.
    assert_stale(
        store.compare_and_save(
            before.revision(),
            &policy,
            &identities,
            IdentityBootstrapState::Consumed,
        ),
        before.revision(),
        committed,
    );
    assert!(store
        .save(before.policy(), before.identities(), before.bootstrap())
        .is_err());
    let after = store.authority_snapshot().unwrap();
    assert_eq!(after.revision(), committed);
    assert_eq!(after.bootstrap(), IdentityBootstrapState::Consumed);
    assert_eq!(after.identities()["current-agent"].roles, ["current-role"]);
    assert!(after.policy().roles().any(|r| r.name == "current-role"));
    // A fresh equal-image save is a real revision, not a stale-success shortcut.
    assert_eq!(
        store
            .compare_and_save(
                after.revision(),
                after.policy(),
                after.identities(),
                after.bootstrap()
            )
            .unwrap(),
        committed + 1
    );
}

#[test]
fn stale_and_unversioned_images_are_refused_in_memory_and_redb() {
    exercise_stale_image(&MemoryRbacStore::new());
    let dir = directory("stale");
    {
        let store = test_support::open_test_store(&dir).unwrap();
        exercise_stale_image(&store);
    }
    std::fs::remove_dir_all(dir).unwrap();
}

fn exercise_race(store: Arc<dyn RbacPolicyStore>) {
    let initial = store.authority_snapshot().unwrap().revision();
    let (ready_tx, ready_rx) = mpsc::sync_channel(2);
    let start = Arc::new(StartGate::default());
    let mut workers = BoundedThreads::new("RBAC image contenders", 2);
    for name in ["first", "second"] {
        let store = Arc::clone(&store);
        let ready = ready_tx.clone();
        let start = Arc::clone(&start);
        workers.spawn(move || {
            let snapshot = store.authority_snapshot().unwrap();
            let mut policy = snapshot.policy().clone();
            policy.add_role(Role::new(name));
            ready.send(()).unwrap();
            start.wait("RBAC image contenders");
            let result = store.compare_and_save(
                snapshot.revision(),
                &policy,
                snapshot.identities(),
                IdentityBootstrapState::Consumed,
            );
            (name, result)
        });
    }
    for _ in 0..2 {
        ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    }
    start.open();
    let results = workers.finish();
    assert_eq!(
        results.iter().filter(|(_, result)| result.is_ok()).count(),
        1
    );
    let winner = results.iter().find(|(_, result)| result.is_ok()).unwrap().0;
    for (_, result) in results {
        match result {
            Ok(revision) => assert_eq!(revision, initial + 1),
            error => assert_stale(error, initial, initial + 1),
        }
    }
    let final_image = store.authority_snapshot().unwrap();
    assert_eq!(final_image.revision(), initial + 1);
    assert_eq!(
        final_image
            .policy()
            .roles()
            .map(|role| role.name.as_str())
            .collect::<Vec<_>>(),
        [winner]
    );
}

#[test]
fn concurrent_images_have_exactly_one_winner_in_memory_and_redb() {
    exercise_race(Arc::new(MemoryRbacStore::new()));
    let dir = directory("race");
    exercise_race(Arc::new(test_support::open_test_store(&dir).unwrap()));
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn stale_revision_remains_refused_after_reopen_and_fresh_reload_can_commit() {
    let dir = directory("reopen");
    let old;
    let committed;
    {
        let store = test_support::open_test_store(&dir).unwrap();
        old = store.authority_snapshot().unwrap();
        let mut policy = old.policy().clone();
        policy.add_role(Role::new("durable-role"));
        committed = store
            .compare_and_save(
                old.revision(),
                &policy,
                old.identities(),
                IdentityBootstrapState::Consumed,
            )
            .unwrap();
        // Even the private initializer must not overwrite an existing image.
        assert!(durable_write::initialize_authority_state(&store).is_err());
    }
    {
        let store = test_support::open_test_store(&dir).unwrap();
        assert_stale(
            store.compare_and_save(
                old.revision(),
                old.policy(),
                old.identities(),
                old.bootstrap(),
            ),
            old.revision(),
            committed,
        );
        let fresh = store.authority_snapshot().unwrap();
        assert_eq!(fresh.revision(), committed);
        assert_eq!(fresh.bootstrap(), IdentityBootstrapState::Consumed);
        assert!(fresh
            .policy()
            .roles()
            .any(|role| role.name == "durable-role"));
        assert_eq!(
            store
                .compare_and_save(
                    fresh.revision(),
                    fresh.policy(),
                    fresh.identities(),
                    fresh.bootstrap()
                )
                .unwrap(),
            committed + 1
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn cloned_layers_keep_independent_revisions_and_roll_back_stale_edits() {
    let mut current = IsolationLayer::new();
    let mut stale = current.clone();
    current.try_add_role(Role::new("accepted")).unwrap();
    let before = serde_json::to_value(stale.rbac()).unwrap();
    assert!(stale
        .try_add_role(Role::new("rejected"))
        .unwrap_err()
        .contains("RBAC_STALE_REVISION"));
    assert_eq!(serde_json::to_value(stale.rbac()).unwrap(), before);
    // Failure did not adopt the other clone's newer revision.
    assert!(stale.try_add_role(Role::new("still-rejected")).is_err());
    current.try_add_role(Role::new("next")).unwrap();
    let mut reloaded = IsolationLayer::with_policy_store(current.policy_store().unwrap()).unwrap();
    reloaded.try_add_role(Role::new("fresh")).unwrap();
    assert_eq!(reloaded.rbac().roles().count(), 3);
}

#[test]
fn checked_save_does_not_repair_a_partial_image() {
    let dir = directory("partial");
    {
        let store = test_support::open_test_store(&dir).unwrap();
        let snapshot = store.authority_snapshot().unwrap();
        store
            .remove_mandatory_record_for_test(IDENTITIES_KEY)
            .unwrap();
        let revision = store.current_version().unwrap();
        // Deliberately submit the latest counter with the old data: the missing
        // mandatory row still refuses, and no receipt/revision is committed.
        assert!(store
            .compare_and_save(
                revision,
                snapshot.policy(),
                snapshot.identities(),
                snapshot.bootstrap()
            )
            .is_err());
        assert_eq!(store.current_version().unwrap(), revision);
        assert!(store.load().is_err());
    }
    assert!(test_support::open_test_store(&dir).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn legacy_adapter_cannot_fall_back_to_unchecked_save() {
    struct LegacyStore {
        writes: std::sync::atomic::AtomicUsize,
    }
    impl RbacPolicyStore for LegacyStore {
        fn load(
            &self,
        ) -> Result<
            (
                RbacPolicy,
                BTreeMap<String, AgentIdentity>,
                IdentityBootstrapState,
            ),
            RbacPersistError,
        > {
            MemoryRbacStore::new().load()
        }
        fn save(
            &self,
            _policy: &RbacPolicy,
            _identities: &BTreeMap<String, AgentIdentity>,
            _bootstrap: IdentityBootstrapState,
        ) -> Result<(), RbacPersistError> {
            self.writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn authority_snapshot(&self) -> Result<RbacAuthoritySnapshot, RbacPersistError> {
            MemoryRbacStore::new().authority_snapshot()
        }
    }
    let store = Arc::new(LegacyStore {
        writes: std::sync::atomic::AtomicUsize::new(0),
    });
    let mut layer = IsolationLayer::with_policy_store(store.clone()).unwrap();
    assert!(layer
        .try_add_role(Role::new("refused"))
        .unwrap_err()
        .contains("RBAC_CHECKED_WRITE_UNSUPPORTED"));
    assert_eq!(store.writes.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(layer.rbac().roles().count(), 0);
}

fn exercise_access_refresh(store: Arc<dyn RbacPolicyStore>, admin: bool) {
    use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector};
    use crate::isolation::AccessLevel;
    use crate::protocol::GraphType;

    let initial = store.authority_snapshot().unwrap();
    let mut policy = initial.policy().clone();
    policy.add_role(Role::new("reader"));
    policy.add_grant(Grant {
        role: "reader".into(),
        resource: ResourceSelector::Graph(if admin { "__admin__" } else { "graph" }.into()),
        action: if admin {
            RbacAction::Admin
        } else {
            RbacAction::Read
        },
        effect: GrantEffect::Allow,
    });
    let mut identities = BTreeMap::from([(
        "reader".into(),
        AgentIdentity {
            agent_id: "reader".into(),
            role: AgentRole::Agent,
            teams: vec![],
            roles: vec!["reader".into()],
        },
    )]);
    store
        .compare_and_save(
            initial.revision(),
            &policy,
            &identities,
            IdentityBootstrapState::Consumed,
        )
        .unwrap();
    let stale = IsolationLayer::with_policy_store(Arc::clone(&store)).unwrap();
    let check = |layer: &IsolationLayer| {
        if admin {
            layer.has_admin_capability("reader")
        } else {
            layer.check_access("reader", "graph", GraphType::Agent, None, AccessLevel::Read)
        }
    };
    let allowed = || check(&stale);
    assert!(allowed());

    if admin {
        let mut explicit_deny = policy.clone();
        explicit_deny.add_grant(Grant {
            role: "reader".into(),
            resource: ResourceSelector::Graph("__admin__".into()),
            action: RbacAction::Admin,
            effect: GrantEffect::Deny,
        });
        let current = store.authority_snapshot().unwrap();
        store
            .compare_and_save(
                current.revision(),
                &explicit_deny,
                &identities,
                IdentityBootstrapState::Consumed,
            )
            .unwrap();
        assert!(
            !allowed(),
            "a fresh explicit deny overrides the cached allow"
        );
    }

    let current = store.authority_snapshot().unwrap();
    let denied = RbacPolicy::new();
    store
        .compare_and_save(
            current.revision(),
            &denied,
            &identities,
            IdentityBootstrapState::Consumed,
        )
        .unwrap();
    assert!(!allowed(), "a stale clone must observe removed grants");

    identities.get_mut("reader").unwrap().role = AgentRole::System;
    let current = store.authority_snapshot().unwrap();
    store
        .compare_and_save(
            current.revision(),
            &denied,
            &identities,
            IdentityBootstrapState::Consumed,
        )
        .unwrap();
    let system_clone = IsolationLayer::with_policy_store(Arc::clone(&store)).unwrap();
    assert!(check(&system_clone));
    identities.get_mut("reader").unwrap().role = AgentRole::Agent;
    let current = store.authority_snapshot().unwrap();
    store
        .compare_and_save(
            current.revision(),
            &denied,
            &identities,
            IdentityBootstrapState::Consumed,
        )
        .unwrap();
    assert!(
        !check(&system_clone),
        "cached System status must not survive demotion"
    );

    let current = store.authority_snapshot().unwrap();
    store
        .compare_and_save(
            current.revision(),
            &policy,
            &BTreeMap::new(),
            IdentityBootstrapState::Consumed,
        )
        .unwrap();
    assert!(
        !allowed(),
        "a removed identity must not regain its old grant"
    );
}

#[test]
fn graph_access_refreshes_revoked_grants_and_identities_in_memory_and_redb() {
    exercise_access_refresh(Arc::new(MemoryRbacStore::new()), false);
    let dir = directory("access-refresh");
    exercise_access_refresh(
        Arc::new(test_support::open_test_store(&dir).unwrap()),
        false,
    );
    let reopened =
        IsolationLayer::with_policy_store(Arc::new(test_support::open_test_store(&dir).unwrap()))
            .unwrap();
    assert!(!reopened.check_access(
        "reader",
        "graph",
        crate::protocol::GraphType::Agent,
        None,
        crate::isolation::AccessLevel::Read
    ));
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn admin_access_refreshes_revoked_grants_and_identities_in_memory_and_redb() {
    exercise_access_refresh(Arc::new(MemoryRbacStore::new()), true);
    let dir = directory("admin-refresh");
    exercise_access_refresh(Arc::new(test_support::open_test_store(&dir).unwrap()), true);
    let reopened =
        IsolationLayer::with_policy_store(Arc::new(test_support::open_test_store(&dir).unwrap()))
            .unwrap();
    assert!(!reopened.has_admin_capability("reader"));
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}
