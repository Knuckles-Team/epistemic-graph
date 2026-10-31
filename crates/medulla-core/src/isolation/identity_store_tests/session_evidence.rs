//! Native persistence fixtures; these do not qualify served admission.
use super::*;
use crate::rbac_persist::test_support::open_test_store;

// spec: MEDULLA-TYPED-PACKS-R085.1
// spec: MEDULLA-TYPED-PACKS-R085.7.2
// spec: MEDULLA-TYPED-PACKS-R085.6.2
#[test]
fn exact_session_facts_and_revocation_survive_durable_reopen() {
    let dir = durable_fixture_dir("medulla-session-record");
    let request = SessionTouch {
        refresh: None,
        session_token: Secret::default(),
        code: Secret::default(),
    };
    let mut owner = stamp(IDENTITY_AUTHENTICATE_SCOPE);
    owner.token_hashes = vec!["durable-session-hash".into()];
    owner.minted_session_ref = Some("ses:durable-one".into());
    owner.session_tenant = Some("tenant-original".into());
    {
        let mut fixture = seeded();
        apply(
            &mut fixture,
            IdentityOp::Credential(CredentialOp::BootstrapSession {
                request: request.clone(),
            }),
            &owner,
        )
        .unwrap();
        // Seed qualified storage facts only; the test does not emulate MFA admission.
        let source = fixture
            .policy_store()
            .unwrap()
            .authority_snapshot()
            .unwrap();
        let mut image = serde_json::to_value(source.policy()).unwrap();
        image["identity"]["sessions"]["durable-session-hash"]["mfa_at_ms"] = (NOW - 17).into();
        let policy = serde_json::from_value(image).unwrap();
        let disk = open_test_store(&dir).unwrap();
        let empty = disk.authority_snapshot().unwrap();
        disk.compare_and_save(
            empty.revision(),
            &policy,
            source.identities(),
            source.bootstrap(),
        )
        .unwrap();
    }
    let op = IdentityOp::Session(SessionOp::Resolve { request });
    let expected = {
        let mut reopened =
            IsolationLayer::with_policy_store(Arc::new(open_test_store(&dir).unwrap())).unwrap();
        let before_observation = reopened
            .policy_store()
            .unwrap()
            .authority_snapshot()
            .unwrap();
        let observe = IdentityOp::Session(SessionOp::Resolve {
            request: SessionTouch {
                refresh: Some(false),
                session_token: Secret::default(),
                code: Secret::default(),
            },
        });
        let IdentityReply::Resolution(observed) = reopened
            .try_apply_identity(&observe, &owner, NOW + 61_000, &Registry)
            .unwrap()
        else {
            panic!("expected observed credential")
        };
        assert_eq!(observed.principal_id, BOOTSTRAP_PRINCIPAL);
        let after_observation = reopened
            .policy_store()
            .unwrap()
            .authority_snapshot()
            .unwrap();
        assert_eq!(before_observation.revision(), after_observation.revision());
        assert_eq!(
            serde_json::to_value(before_observation.policy()).unwrap(),
            serde_json::to_value(after_observation.policy()).unwrap()
        );
        let IdentityReply::Resolution(value) = reopened
            .try_apply_identity(&op, &owner, NOW + 61_000, &Registry)
            .unwrap()
        else {
            panic!("expected resolution")
        };
        let record = value.session_record.unwrap();
        let inspect = IdentityOp::Session(SessionOp::InspectReference {
            session_ref: record.session_ref.clone(),
            tenant: record.tenant.clone(),
        });
        let mut subject = owner.clone();
        subject.actor.principal_id = record.principal_id.clone();
        subject.actor.scopes = BTreeSet::from([IDENTITY_SELF_SCOPE.into()]);
        let inspected = reopened
            .try_apply_identity(&inspect, &subject, NOW + 61_001, &Registry)
            .unwrap();
        let IdentityReply::SessionAuthority {
            record: inspected_record,
            ..
        } = inspected
        else {
            panic!("expected authority snapshot")
        };
        assert_eq!(inspected_record, record);
        assert_eq!(record.session_ref, "ses:durable-one");
        assert_eq!(record.tenant, "tenant-original");
        assert_eq!(record.mfa_at_ms, Some(NOW - 17));
        assert!(record.idle_expires_at_ms > NOW + 61_000);
        record
    };
    {
        let mut reopened =
            IsolationLayer::with_policy_store(Arc::new(open_test_store(&dir).unwrap())).unwrap();
        let IdentityReply::Resolution(value) = reopened
            .try_apply_identity(&op, &owner, NOW + 61_001, &Registry)
            .unwrap()
        else {
            panic!("expected resolution")
        };
        assert_eq!(value.session_record.unwrap(), expected);
        let mut wrong = owner.clone();
        wrong.token_hashes = vec!["unrelated-session-hash".into()];
        assert!(reopened
            .try_apply_identity(&op, &wrong, NOW + 61_001, &Registry)
            .is_err());
        reopened
            .try_apply_identity(
                &IdentityOp::Session(SessionOp::RevokeAll {
                    request: ObjectRef {
                        id: expected.principal_id.clone(),
                    },
                }),
                &stamp(IDENTITY_ADMIN_SCOPE),
                NOW + 61_001,
                &Registry,
            )
            .unwrap();
    }
    {
        let mut reopened =
            IsolationLayer::with_policy_store(Arc::new(open_test_store(&dir).unwrap())).unwrap();
        assert!(reopened
            .try_apply_identity(&op, &owner, NOW + 61_002, &Registry)
            .is_err());
        let inspect = IdentityOp::Session(SessionOp::InspectReference {
            session_ref: expected.session_ref.clone(),
            tenant: expected.tenant.clone(),
        });
        let mut subject = owner.clone();
        subject.actor.principal_id = expected.principal_id.clone();
        subject.actor.scopes = BTreeSet::from([IDENTITY_SELF_SCOPE.into()]);
        assert!(reopened
            .try_apply_identity(&inspect, &subject, NOW + 61_002, &Registry)
            .is_err());
    }
    std::fs::remove_dir_all(dir).unwrap();
}

// spec: MEDULLA-TYPED-PACKS-R085.4.2
#[test]
fn renewal_uses_global_revision_and_survives_reopen_without_resurrection() {
    let dir = durable_fixture_dir("medulla-session-renew");
    let mut fixture = seeded();
    let mut issuer = stamp(IDENTITY_AUTHENTICATE_SCOPE);
    issuer.token_hashes = vec!["renew-session-hash".into()];
    issuer.minted_session_ref = Some("ses:renew-original".into());
    issuer.session_tenant = Some("tenant-original".into());
    apply(
        &mut fixture,
        IdentityOp::Credential(CredentialOp::BootstrapSession {
            request: SessionTouch {
                refresh: None,
                session_token: Secret::default(),
                code: Secret::default(),
            },
        }),
        &issuer,
    )
    .unwrap();
    let source = fixture
        .policy_store()
        .unwrap()
        .authority_snapshot()
        .unwrap();
    let store = Arc::new(open_test_store(&dir).unwrap());
    let empty = store.authority_snapshot().unwrap();
    store
        .compare_and_save(
            empty.revision(),
            source.policy(),
            source.identities(),
            source.bootstrap(),
        )
        .unwrap();
    let mut layer = IsolationLayer::with_policy_store(store.clone()).unwrap();
    let mut subject = stamp(IDENTITY_SELF_SCOPE);
    subject.actor.principal_id = BOOTSTRAP_PRINCIPAL.into();
    subject.session_tenant = Some("tenant-original".into());
    let inspect = IdentityOp::Session(SessionOp::InspectReference {
        session_ref: "ses:renew-original".into(),
        tenant: "tenant-original".into(),
    });
    let IdentityReply::SessionAuthority {
        record,
        scopes,
        authority_revision,
    } = layer
        .try_apply_identity(&inspect, &subject, NOW, &Registry)
        .unwrap()
    else {
        panic!("expected authority")
    };
    assert_eq!(
        store.authority_snapshot().unwrap().revision(),
        authority_revision
    );
    let renew = IdentityOp::Session(SessionOp::RenewReference {
        session_ref: record.session_ref.clone(),
        tenant: record.tenant.clone(),
        expected_authority_revision: authority_revision,
        expected_scopes: scopes.clone(),
    });
    let mut expanded = scopes.clone();
    expanded.insert("invented:scope".into());
    let wrong = IdentityOp::Session(SessionOp::RenewReference {
        session_ref: record.session_ref.clone(),
        tenant: record.tenant.clone(),
        expected_authority_revision: authority_revision,
        expected_scopes: expanded,
    });
    assert!(layer
        .try_apply_identity(&wrong, &subject, NOW + 1, &Registry)
        .is_err());
    assert_eq!(
        store.authority_snapshot().unwrap().revision(),
        authority_revision
    );
    let renewed = layer
        .try_apply_identity(&renew, &subject, NOW + 1, &Registry)
        .unwrap();
    let IdentityReply::SessionAuthority {
        record: after,
        authority_revision: next,
        ..
    } = &renewed
    else {
        panic!("expected authority")
    };
    assert!(*next > authority_revision);
    assert!(after.idle_expires_at_ms > record.idle_expires_at_ms);
    assert_eq!(after.absolute_expires_at_ms, record.absolute_expires_at_ms);
    assert!(after.idle_expires_at_ms <= after.absolute_expires_at_ms);
    assert!(layer
        .try_apply_identity(&renew, &subject, NOW + 2, &Registry)
        .is_err());
    let mut reopened = IsolationLayer::with_policy_store(store.clone()).unwrap();
    assert_eq!(
        reopened
            .try_apply_identity(&inspect, &subject, NOW + 2, &Registry)
            .unwrap(),
        renewed
    );
    let fresh = IdentityOp::Session(SessionOp::RenewReference {
        session_ref: after.session_ref.clone(),
        tenant: after.tenant.clone(),
        expected_authority_revision: *next,
        expected_scopes: scopes,
    });
    assert!(reopened
        .try_apply_identity(&fresh, &subject, after.idle_expires_at_ms, &Registry)
        .is_err());
    let racing = Arc::new(RevokeBeforeSave {
        inner: store.clone(),
    });
    let mut contested = IsolationLayer::with_policy_store(racing).unwrap();
    let conflict = contested
        .try_apply_identity(&fresh, &subject, NOW + 2, &Registry)
        .unwrap_err();
    assert!(conflict.to_string().contains("RBAC_STALE_REVISION"));
    let before_refusal = store.authority_snapshot().unwrap();
    assert!(layer
        .try_apply_identity(&fresh, &subject, NOW + 3, &Registry)
        .is_err());
    assert_eq!(
        store.authority_snapshot().unwrap().revision(),
        before_refusal.revision()
    );
    assert!(layer
        .try_apply_identity(&inspect, &subject, NOW + 3, &Registry)
        .is_err());
    drop(contested);
    drop(reopened);
    drop(layer);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

/// Inject a real competing durable revocation between snapshot and renewal commit.
struct RevokeBeforeSave {
    inner: Arc<dyn RbacPolicyStore>,
}

impl RbacPolicyStore for RevokeBeforeSave {
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
        self.inner.load()
    }
    fn authority_snapshot(&self) -> Result<RbacAuthoritySnapshot, RbacPersistError> {
        self.inner.authority_snapshot()
    }
    fn compare_and_save(
        &self,
        expected: u64,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<u64, RbacPersistError> {
        let mut competing = IsolationLayer::with_policy_store(self.inner.clone()).unwrap();
        competing
            .try_apply_identity(
                &IdentityOp::Session(SessionOp::RevokeAll {
                    request: ObjectRef {
                        id: BOOTSTRAP_PRINCIPAL.into(),
                    },
                }),
                &stamp(IDENTITY_ADMIN_SCOPE),
                NOW + 2,
                &Registry,
            )
            .unwrap();
        self.inner
            .compare_and_save(expected, policy, identities, bootstrap)
    }
}
