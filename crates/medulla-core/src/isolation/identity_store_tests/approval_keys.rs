//! Native durable storage qualification, not broker proof or served enrollment.
use super::*;
use crate::rbac_persist::test_support::open_test_store;

fn key() -> ApprovalKeyLookup {
    ApprovalKeyLookup {
        key_id: "approval:durable".into(),
        principal_id: BOOTSTRAP_PRINCIPAL.into(),
        purpose: ApprovalKeyPurpose::SignedApproval,
    }
}

fn register(revision: u64) -> IdentityOp {
    IdentityOp::Credential(CredentialOp::RegisterApprovalKey {
        request: ApprovalKeyRegistration {
            session_token: Secret::default(),
            binding: ApprovalKeyBinding {
                key_id: key().key_id,
                principal_id: BOOTSTRAP_PRINCIPAL.into(),
                tenant: "tenant-original".into(),
                purpose: ApprovalKeyPurpose::SignedApproval,
                public_key_hex: if revision == 0 { "ab" } else { "cd" }.repeat(32),
                expected_revision: revision,
                expires_at_ms: NOW + 60_000,
            },
            attestation: ApprovalKeyAttestation::BrokerVerifiedEd25519,
        },
    })
}

fn revocation(revision: u64) -> ApprovalKeyRevocation {
    ApprovalKeyRevocation {
        session_token: Secret::default(),
        key: key(),
        expected_revision: revision,
    }
}

fn inspect(layer: &mut IsolationLayer, owner: &IdentityStamp) -> ApprovalKeyRecord {
    let IdentityReply::ApprovalKey(record) = apply(
        layer,
        IdentityOp::Credential(CredentialOp::InspectApprovalKey { request: key() }),
        owner,
    )
    .unwrap() else {
        panic!("expected public key record")
    };
    record
}

fn refuse_unchanged(layer: &mut IsolationLayer, operation: IdentityOp, owner: &IdentityStamp) {
    let store = layer.policy_store().unwrap();
    let before = store.authority_snapshot().unwrap();
    assert!(apply(layer, operation, owner).is_err());
    let after = store.authority_snapshot().unwrap();
    assert_eq!(after.revision(), before.revision());
    assert_eq!(
        serde_json::to_value(after.policy()).unwrap(),
        serde_json::to_value(before.policy()).unwrap()
    );
}

// spec: MEDULLA-TYPED-PACKS-R085.5.2
#[test]
fn approval_key_reopen_preserves_rotation_revocation_and_refuses_stale_cas() {
    let dir = durable_fixture_dir("medulla-approval-key");
    let mut owner = stamp(IDENTITY_AUTHENTICATE_SCOPE);
    owner.actor.principal_id = "svc:graph-os".into();
    owner.token_hashes = vec!["synthetic-approval-session-hash".into()];
    owner.session_tenant = Some("tenant-original".into());
    {
        let fixture = session_fixture(&owner);
        let source = fixture
            .policy_store()
            .unwrap()
            .authority_snapshot()
            .unwrap();
        let disk = open_test_store(&dir).unwrap();
        let empty = disk.authority_snapshot().unwrap();
        disk.compare_and_save(
            empty.revision(),
            source.policy(),
            source.identities(),
            source.bootstrap(),
        )
        .unwrap();
        let mut durable = IsolationLayer::with_policy_store(Arc::new(disk)).unwrap();
        apply(&mut durable, register(0), &owner).unwrap();
        assert_eq!(inspect(&mut durable, &owner).revision, 1);
    }
    {
        let mut reopened =
            IsolationLayer::with_policy_store(Arc::new(open_test_store(&dir).unwrap())).unwrap();
        let first = inspect(&mut reopened, &owner);
        assert_eq!(first.revision, 1);
        assert_eq!(first.public_key_hex, "ab".repeat(32));
        assert_eq!(first.tenant, "tenant-original");
        let mut stale_writer = reopened.clone();
        apply(&mut reopened, register(1), &owner).unwrap();
        refuse_unchanged(&mut stale_writer, register(1), &owner);
        refuse_unchanged(
            &mut reopened,
            IdentityOp::Credential(CredentialOp::RevokeApprovalKey {
                request: revocation(1),
            }),
            &owner,
        );
        assert_eq!(inspect(&mut reopened, &owner).revision, 2);
    }
    {
        let mut reopened =
            IsolationLayer::with_policy_store(Arc::new(open_test_store(&dir).unwrap())).unwrap();
        let rotated = inspect(&mut reopened, &owner);
        assert_eq!(rotated.public_key_hex, "cd".repeat(32));
        assert_eq!(rotated.revision, 2);
        apply(
            &mut reopened,
            IdentityOp::Credential(CredentialOp::RevokeApprovalKey {
                request: revocation(2),
            }),
            &owner,
        )
        .unwrap();
        refuse_unchanged(&mut reopened, register(2), &owner);
        refuse_unchanged(
            &mut reopened,
            IdentityOp::Credential(CredentialOp::RevokeApprovalKey {
                request: revocation(2),
            }),
            &owner,
        );
    }
    {
        let mut reopened =
            IsolationLayer::with_policy_store(Arc::new(open_test_store(&dir).unwrap())).unwrap();
        refuse_unchanged(
            &mut reopened,
            IdentityOp::Credential(CredentialOp::InspectApprovalKey { request: key() }),
            &owner,
        );
        let snapshot = reopened
            .policy_store()
            .unwrap()
            .authority_snapshot()
            .unwrap();
        let persisted = serde_json::to_value(snapshot.policy()).unwrap();
        let record = &persisted["identity"]["approval_keys"]["approval:durable"];
        assert_eq!(record["revision"], 3);
        assert_eq!(record["revoked_at_ms"], NOW);
    }
    std::fs::remove_dir_all(dir).unwrap();
}
