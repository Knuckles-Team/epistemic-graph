//! Source store fixtures; these do not qualify broker proof or served enrollment.
use super::auth::{sign_in, verdict, with_password};
use super::*;

fn opened() -> (IdentityStore, IdentityStamp) {
    let mut store = store_in(AuthMode::Local);
    let principal = with_password(&mut store, "alice");
    let mut stamp = verdict(&store, Some(&principal), true, "approval-session");
    stamp.session_tenant = Some("tenant-one".into());
    apply_kept(&mut store, &sign_in("alice"), &stamp, NOW).unwrap();
    let mut stamp = broker();
    stamp.token_hashes.push("approval-session".into());
    stamp.session_tenant = Some("tenant-one".into());
    (store, stamp)
}

fn binding(revision: u64) -> ApprovalKeyBinding {
    ApprovalKeyBinding {
        key_id: "approval:one".into(),
        principal_id: "usr:alice".into(),
        tenant: "tenant-one".into(),
        purpose: ApprovalKeyPurpose::SignedApproval,
        public_key_hex: "ab".repeat(32),
        expected_revision: revision,
        expires_at_ms: NOW + 60_000,
    }
}

fn register(binding: ApprovalKeyBinding) -> IdentityOp {
    IdentityOp::Credential(CredentialOp::RegisterApprovalKey {
        request: ApprovalKeyRegistration {
            session_token: Secret::default(),
            binding,
            attestation: ApprovalKeyAttestation::BrokerVerifiedEd25519,
        },
    })
}

fn key() -> ApprovalKeyLookup {
    ApprovalKeyLookup {
        key_id: "approval:one".into(),
        principal_id: "usr:alice".into(),
        purpose: ApprovalKeyPurpose::SignedApproval,
    }
}

fn read() -> IdentityOp {
    IdentityOp::Credential(CredentialOp::InspectApprovalKey { request: key() })
}

fn revoke(revision: u64) -> IdentityOp {
    IdentityOp::Credential(CredentialOp::RevokeApprovalKey {
        request: ApprovalKeyRevocation {
            session_token: Secret::default(),
            key: key(),
            expected_revision: revision,
        },
    })
}

fn record(reply: IdentityReply) -> ApprovalKeyRecord {
    let IdentityReply::ApprovalKey(record) = reply else {
        panic!("wrong reply")
    };
    record
}

fn unchanged_refusal(store: &mut IdentityStore, op: &IdentityOp, stamp: &IdentityStamp, now: u64) {
    let before = store.clone();
    assert!(apply_kept(store, op, stamp, now).is_err());
    assert_eq!(*store, before);
}

// spec: MEDULLA-TYPED-PACKS-R085.5.1
#[test]
fn lifecycle_persists_public_facts_and_enforces_rotation_and_revocation_cas() {
    let (mut store, stamp) = opened();
    assert_ne!(
        stamp.actor.principal_id, "usr:alice",
        "the broker is not the human"
    );
    assert_eq!(
        record(apply_kept(&mut store, &register(binding(0)), &stamp, NOW).unwrap()).revision,
        1
    );
    store = serde_json::from_slice(&serde_json::to_vec(&store).unwrap()).unwrap();
    let public = record(apply_kept(&mut store, &read(), &stamp, NOW).unwrap());
    assert_eq!(public.principal_id, "usr:alice");
    assert_eq!(
        serde_json::to_value(public)
            .unwrap()
            .as_object()
            .unwrap()
            .len(),
        8
    );
    unchanged_refusal(&mut store, &register(binding(0)), &stamp, NOW);
    let mut replacement = binding(1);
    replacement.public_key_hex = "cd".repeat(32);
    assert_eq!(
        record(apply_kept(&mut store, &register(replacement), &stamp, NOW).unwrap()).revision,
        2
    );
    unchanged_refusal(&mut store, &revoke(1), &stamp, NOW);
    let revoked = record(apply_kept(&mut store, &revoke(2), &stamp, NOW).unwrap());
    assert_eq!((revoked.revision, revoked.revoked_at_ms), (3, Some(NOW)));
    unchanged_refusal(&mut store, &read(), &stamp, NOW);
    unchanged_refusal(&mut store, &register(binding(3)), &stamp, NOW);
    unchanged_refusal(&mut store, &revoke(3), &stamp, NOW);
}

// spec: MEDULLA-TYPED-PACKS-R085.5.1
#[test]
fn registration_requires_broker_live_human_session_and_exact_tenant() {
    for case in 0..8 {
        let (mut store, mut stamp) = opened();
        match case {
            0 => stamp = admin(),
            1 => stamp.session_tenant = None,
            2 => stamp.session_tenant = Some("tenant-two".into()),
            3 => {
                store
                    .sessions
                    .get_mut("approval-session")
                    .unwrap()
                    .mfa_pending = true
            }
            4 => {
                store
                    .sessions
                    .get_mut("approval-session")
                    .unwrap()
                    .idle_expires_at_ms = NOW
            }
            5 => {
                store
                    .sessions
                    .get_mut("approval-session")
                    .unwrap()
                    .revoked_at_ms = Some(NOW)
            }
            6 => store.users.get_mut("usr:alice").unwrap().status = UserStatus::Disabled,
            7 => store.users.get_mut("usr:alice").unwrap().kind = UserKind::Service,
            _ => unreachable!(),
        }
        unchanged_refusal(&mut store, &register(binding(0)), &stamp, NOW);
    }
}

// spec: MEDULLA-TYPED-PACKS-R085.5.1
#[test]
fn bindings_expiry_and_public_lookup_remain_closed() {
    let (mut store, stamp) = opened();
    for case in 0..6 {
        let mut request = binding(0);
        match case {
            0 => request.principal_id = "usr:bob".into(),
            1 => request.tenant = "tenant-two".into(),
            2 => request.public_key_hex = "AB".repeat(32),
            3 => request.expires_at_ms = NOW,
            4 => request.expected_revision = 9,
            5 => request.key_id.clear(),
            _ => unreachable!(),
        }
        unchanged_refusal(&mut store, &register(request), &stamp, NOW);
    }
    apply_kept(&mut store, &register(binding(0)), &stamp, NOW).unwrap();
    let mut foreign = stamp.clone();
    foreign.session_tenant = Some("tenant-two".into());
    unchanged_refusal(&mut store, &read(), &foreign, NOW);
    unchanged_refusal(&mut store, &read(), &admin(), NOW);
    let mut lookup = key();
    lookup.principal_id = "usr:bob".into();
    unchanged_refusal(
        &mut store,
        &IdentityOp::Credential(CredentialOp::InspectApprovalKey { request: lookup }),
        &stamp,
        NOW,
    );
    unchanged_refusal(&mut store, &read(), &stamp, NOW + 60_000);
    // Reading public facts does not require or return the human's credential.
    let mut public_reader = stamp.clone();
    public_reader.token_hashes.clear();
    assert!(apply_kept(&mut store, &read(), &public_reader, NOW).is_ok());
    unchanged_refusal(&mut store, &revoke(1), &public_reader, NOW);
}

// spec: MEDULLA-TYPED-PACKS-R085.5.1
#[test]
fn registration_message_has_exact_domain_lengths_and_revision_binding() {
    let request = binding(0);
    let message = request.registration_message().unwrap();
    let mut expected = b"eg/identity/approval-key-registration/v1\0".to_vec();
    for value in [
        "usr:alice",
        "tenant-one",
        "approval:one",
        "signed_approval",
        &"ab".repeat(32),
    ] {
        expected.extend_from_slice(&(value.len() as u32).to_be_bytes());
        expected.extend_from_slice(value.as_bytes());
    }
    expected.extend_from_slice(&0_u64.to_be_bytes());
    expected.extend_from_slice(&(NOW + 60_000).to_be_bytes());
    assert_eq!(message, expected);
    assert_ne!(message, binding(1).registration_message().unwrap());
    let mut wire = serde_json::to_value(request).unwrap();
    wire["purpose"] = "mfa".into();
    assert!(serde_json::from_value::<ApprovalKeyBinding>(wire).is_err());
    let mut registration = serde_json::to_value(register(binding(0))).unwrap();
    registration["request"]
        .as_object_mut()
        .unwrap()
        .remove("attestation");
    assert!(serde_json::from_value::<IdentityOp>(registration).is_err());
}

// spec: MEDULLA-TYPED-PACKS-R085.5.1
#[test]
fn delegated_broker_cannot_attest_or_revoke_but_may_read_public_facts() {
    let (mut store, stamp) = opened();
    let mut delegated = stamp.clone();
    delegated.actor.delegated = true;
    unchanged_refusal(&mut store, &register(binding(0)), &delegated, NOW);
    apply_kept(&mut store, &register(binding(0)), &stamp, NOW).unwrap();
    unchanged_refusal(&mut store, &revoke(1), &delegated, NOW);
    assert!(apply_kept(&mut store, &read(), &delegated, NOW).is_ok());
}
