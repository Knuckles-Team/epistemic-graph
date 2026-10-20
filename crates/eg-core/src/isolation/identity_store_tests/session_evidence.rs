//! Native persistence fixtures; these do not qualify served admission.
use super::*;
use crate::rbac_persist::test_support::open_test_store;

// spec: EG-TYPED-PACKS-R085.1
#[test]
fn exact_session_facts_and_revocation_survive_durable_reopen() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("eg-session-record-{}-{nonce}", std::process::id()));
    let request = SessionTouch {
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
        let IdentityReply::Resolution(value) = reopened
            .try_apply_identity(&op, &owner, NOW + 61_000, &Registry)
            .unwrap()
        else {
            panic!("expected resolution")
        };
        let record = value.session_record.unwrap();
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
    }
    std::fs::remove_dir_all(dir).unwrap();
}
