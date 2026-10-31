//! Request-boundary stamping only, not served human enrollment qualification.
use super::*;

// spec: MEDULLA-TYPED-PACKS-R085.5.3
#[tokio::test]
async fn approval_key_stamping_erases_session_secret_and_replaces_untrusted_evidence() {
    let state = state();
    let binding = ApprovalKeyBinding {
        key_id: "approval:one".into(),
        principal_id: "usr:alice".into(),
        tenant: "tenant-one".into(),
        purpose: ApprovalKeyPurpose::SignedApproval,
        public_key_hex: "ab".repeat(32),
        expected_revision: 0,
        expires_at_ms: u64::MAX,
    };
    let key = ApprovalKeyLookup {
        key_id: binding.key_id.clone(),
        principal_id: binding.principal_id.clone(),
        purpose: binding.purpose,
    };
    let operations = [
        CredentialOp::RegisterApprovalKey {
            request: ApprovalKeyRegistration {
                session_token: Secret::new(SESSION),
                binding,
                attestation: ApprovalKeyAttestation::BrokerVerifiedEd25519,
            },
        },
        CredentialOp::RevokeApprovalKey {
            request: ApprovalKeyRevocation {
                session_token: Secret::new(SESSION),
                key,
                expected_revision: 1,
            },
        },
    ];
    for op in operations {
        let context = broker();
        let mut forged = IdentityStamp::for_actor(identity_actor(&context));
        forged.token_hashes.push("caller-supplied-hash".into());
        forged.session_tenant = Some("caller-tenant".into());
        let mut method = Method::Identity {
            op: IdentityOp::Credential(op),
            stamp: Some(forged),
        };
        stamp_identity(
            &state,
            &mut method,
            &context,
            ElevationStampAuthority::External,
        )
        .await
        .unwrap();
        assert!(!serde_json::to_string(&method).unwrap().contains(SESSION));
        let Method::Identity {
            stamp: Some(stamp), ..
        } = method
        else {
            panic!("missing stamp")
        };
        assert_eq!(stamp.token_hashes, vec![secrets::token_hash(SESSION)]);
        assert_eq!(
            stamp.session_tenant,
            resolution_context::record_tenant(&context)
        );
        assert_eq!(stamp.actor.principal_id, "svc:graph-os");
    }
}
