//! Durable facts only: these store fixtures do not establish served admission.
use super::auth::{sign_in, verdict, with_password};
use super::*;

fn opened() -> IdentityStore {
    let mut store = store_in(AuthMode::Local);
    let alice = with_password(&mut store, "alice");
    for (hash, reference) in [("hash-one", "ses:one"), ("hash-two", "ses:two")] {
        let mut stamp = verdict(&store, Some(&alice), true, hash);
        stamp.minted_session_ref = Some(reference.into());
        stamp.session_tenant = Some("tenant-one".into());
        apply_kept(&mut store, &sign_in("alice"), &stamp, NOW).unwrap();
    }
    store
}

fn resolve(
    store: &mut IdentityStore,
    hash: &str,
    now: u64,
) -> Result<IdentityReply, IdentityRefusal> {
    let op = IdentityOp::Session(SessionOp::Resolve {
        request: SessionTouch {
            refresh: None,
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    let mut stamp = broker();
    stamp.token_hashes.push(hash.into());
    apply_kept(store, &op, &stamp, now)
}

// spec: MEDULLA-TYPED-PACKS-R085.1
#[test]
fn projection_survives_persistence_and_reports_post_touch_facts() {
    let mut store: IdentityStore =
        serde_json::from_slice(&serde_json::to_vec(&opened()).unwrap()).unwrap();
    for (hash, reference) in [("hash-one", "ses:one"), ("hash-two", "ses:two")] {
        let IdentityReply::Resolution(resolution) =
            resolve(&mut store, hash, NOW + 61_000).unwrap()
        else {
            panic!("wrong reply")
        };
        let projection = resolution.session_record.unwrap();
        assert_eq!(projection.session_ref, reference);
        assert_eq!(projection.tenant, "tenant-one");
        assert_eq!(projection.principal_id, "usr:alice");
        assert_eq!(
            projection.idle_expires_at_ms,
            store.sessions[hash].idle_expires_at_ms
        );
        assert_eq!(
            projection.absolute_expires_at_ms,
            store.sessions[hash].absolute_expires_at_ms
        );
        let wire = serde_json::to_value(projection).unwrap();
        assert_eq!(wire.as_object().unwrap().len(), 6);
        assert!(wire["mfa_at_ms"].is_null());
        let mut missing = wire;
        missing.as_object_mut().unwrap().remove("mfa_at_ms");
        assert!(serde_json::from_value::<SessionRecordProjection>(missing).is_err());
    }
}

// spec: MEDULLA-TYPED-PACKS-R085.1
#[test]
fn legacy_unbound_and_non_session_resolutions_omit_projection() {
    let mut store = opened();
    let mut image = serde_json::to_value(&store).unwrap();
    image["sessions"]["hash-one"]
        .as_object_mut()
        .unwrap()
        .remove("session_ref");
    store = serde_json::from_value(image).unwrap();
    let reply = resolve(&mut store, "hash-one", NOW).unwrap();
    assert!(serde_json::to_value(reply).unwrap()["value"]
        .get("session_record")
        .is_none());
    store.sessions.get_mut("hash-two").unwrap().session_tenant = None;
    let reply = resolve(&mut store, "hash-two", NOW).unwrap();
    assert!(serde_json::to_value(reply).unwrap()["value"]
        .get("session_record")
        .is_none());
    // The common principal projection used by API-key resolution has no session facts.
    assert!(store
        .resolve("usr:alice", &TestRegistry)
        .unwrap()
        .session_record
        .is_none());
}

// spec: MEDULLA-TYPED-PACKS-R085.1
#[test]
fn projection_preserves_existing_pending_and_nonlive_behavior() {
    let mut store = opened();
    store.sessions.get_mut("hash-one").unwrap().mfa_pending = true;
    let IdentityReply::Resolution(value) = resolve(&mut store, "hash-one", NOW).unwrap() else {
        panic!("wrong reply")
    };
    assert!(value.session_mfa_pending);
    assert!(value.session_record.is_some());
    for state in ["revoked", "idle", "absolute", "disabled", "wrong"] {
        let mut store = opened();
        let record = store.sessions.get_mut("hash-one").unwrap();
        match state {
            "revoked" => record.revoked_at_ms = Some(NOW),
            "idle" => record.idle_expires_at_ms = NOW,
            "absolute" => record.absolute_expires_at_ms = NOW,
            "disabled" => store.users.get_mut("usr:alice").unwrap().status = UserStatus::Disabled,
            _ => {}
        }
        let before = store.clone();
        assert!(resolve(
            &mut store,
            if state == "wrong" {
                "missing"
            } else {
                "hash-one"
            },
            NOW
        )
        .is_err());
        assert_eq!(store, before);
    }
}

// spec: MEDULLA-TYPED-PACKS-R085.1
#[cfg(feature = "contract-schema")]
#[test]
fn projection_schema_requires_explicit_nullable_mfa() {
    let schema = serde_json::to_value(schemars::schema_for!(SessionRecordProjection)).unwrap();
    let required = schema["required"].as_array().unwrap();
    assert_eq!(required.len(), 6);
    assert!(required.contains(&serde_json::json!("mfa_at_ms")));
    assert_eq!(
        schema["properties"]["mfa_at_ms"]["type"],
        serde_json::json!(["integer", "null"])
    );
}

// spec: MEDULLA-TYPED-PACKS-R085.1
#[test]
fn resolve_preserves_creation_tenant_and_exact_mfa_timestamp() {
    let mut store = opened();
    store.sessions.get_mut("hash-one").unwrap().mfa_at_ms = Some(NOW - 17);
    let op = IdentityOp::Session(SessionOp::Resolve {
        request: SessionTouch {
            refresh: None,
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    let mut changed = broker();
    changed.token_hashes.push("hash-one".into());
    changed.session_tenant = Some("other-current-tenant".into());
    changed.minted_session_ref = Some("replacement-reference".into());
    let IdentityReply::Resolution(value) = apply_kept(&mut store, &op, &changed, NOW).unwrap()
    else {
        panic!("expected resolution")
    };
    let record = value.session_record.unwrap();
    assert_eq!(record.session_ref, "ses:one");
    assert_eq!(record.tenant, "tenant-one");
    assert_eq!(record.mfa_at_ms, Some(NOW - 17));
}

// spec: MEDULLA-TYPED-PACKS-R085.7.1
#[test]
fn original_reference_inspection_is_owner_bound_and_does_not_touch() {
    let mut store = opened();
    let before = store.clone();
    let (op, owner) = reference_inspection();
    let IdentityReply::SessionRecord(record) =
        apply_kept(&mut store, &op, &owner, NOW + 61_000).unwrap()
    else {
        panic!("expected original record")
    };
    assert_eq!(record.session_ref, "ses:one");
    assert_eq!(record.principal_id, "usr:alice");
    assert_eq!(store, before);
    for (principal, delegated) in [("usr:other", false), ("usr:alice", true)] {
        let mut wrong = owner.clone();
        wrong.actor.principal_id = principal.into();
        wrong.actor.delegated = delegated;
        assert!(apply_kept(&mut store, &op, &wrong, NOW).is_err());
        assert_eq!(store, before);
    }
    for (reference, tenant) in [("hash-one", "tenant-one"), ("ses:one", "tenant-other")] {
        let wrong = IdentityOp::Session(SessionOp::InspectReference {
            session_ref: reference.into(),
            tenant: tenant.into(),
        });
        assert!(apply_kept(&mut store, &wrong, &owner, NOW).is_err());
        assert_eq!(store, before);
    }
    let expiry = store.sessions["hash-one"].idle_expires_at_ms;
    assert!(apply_kept(&mut store, &op, &owner, expiry).is_err());
    assert_eq!(store, before);
    store.sessions.get_mut("hash-one").unwrap().revoked_at_ms = Some(NOW);
    assert!(apply_kept(&mut store, &op, &owner, NOW).is_err());
}

// spec: MEDULLA-TYPED-PACKS-R085.7.1
#[test]
fn reference_inspection_refuses_pending_disabled_and_ambiguous_records() {
    let original = opened();
    let (op, owner) = reference_inspection();
    let mut pending = original.clone();
    pending.sessions.get_mut("hash-one").unwrap().mfa_pending = true;
    assert!(apply_kept(&mut pending, &op, &owner, NOW).is_err());
    let mut ambiguous = original.clone();
    ambiguous.sessions.get_mut("hash-two").unwrap().session_ref = Some("ses:one".into());
    assert!(apply_kept(&mut ambiguous, &op, &owner, NOW).is_err());
    let mut disabled = original;
    disabled.users.get_mut("usr:alice").unwrap().status = UserStatus::Disabled;
    assert!(apply_kept(&mut disabled, &op, &owner, NOW).is_err());
}

// spec: MEDULLA-TYPED-PACKS-R085.4.1
#[test]
fn renewal_preserves_original_scope_ceiling_and_refuses_legacy_evidence() {
    let original = opened();
    let captured = original.resolve("usr:alice", &TestRegistry).unwrap().scopes;
    assert_eq!(
        original.sessions["hash-one"].original_scopes.as_ref(),
        Some(&captured)
    );
    let mut subject = IdentityStamp::for_actor(actor("usr:alice", &[IDENTITY_SELF_SCOPE]));
    subject.session_tenant = Some("tenant-one".into());
    let renew = |scopes| {
        IdentityOp::Session(SessionOp::RenewReference {
            session_ref: "ses:one".into(),
            tenant: "tenant-one".into(),
            // Pure transitions do not own the persistence revision; the core CAS fixture does.
            expected_authority_revision: 0,
            expected_scopes: scopes,
        })
    };
    let mut legacy_json = serde_json::to_value(&original).unwrap();
    legacy_json["sessions"]["hash-one"]
        .as_object_mut()
        .unwrap()
        .remove("original_scopes");
    let mut legacy: IdentityStore = serde_json::from_value(legacy_json).unwrap();
    let before = legacy.clone();
    assert!(apply_kept(&mut legacy, &renew(captured.clone()), &subject, NOW + 1).is_err());
    assert_eq!(legacy, before);
    let mut expanded = original.clone();
    expanded
        .roles
        .get_mut(USER_ROLE)
        .unwrap()
        .scopes
        .insert("kg:write".into());
    // Even a fresh read carrying all current scopes cannot increase the original ceiling.
    let current = expanded.resolve("usr:alice", &TestRegistry).unwrap().scopes;
    assert!(!current.is_subset(&captured));
    let before = expanded.clone();
    assert!(apply_kept(&mut expanded, &renew(current), &subject, NOW + 1).is_err());
    assert_eq!(expanded, before);
    let mut reduced: IdentityStore =
        serde_json::from_slice(&serde_json::to_vec(&original).unwrap()).unwrap();
    reduced
        .roles
        .get_mut(USER_ROLE)
        .unwrap()
        .scopes
        .remove("kg:read");
    let narrower = reduced.resolve("usr:alice", &TestRegistry).unwrap().scopes;
    assert!(narrower.is_subset(&captured));
    assert!(apply_kept(&mut reduced, &renew(narrower), &subject, NOW + 1).is_ok());
    assert_eq!(
        reduced.sessions["hash-one"].original_scopes.as_ref(),
        Some(&captured)
    );
}

// spec: MEDULLA-TYPED-PACKS-R085.6.1
#[test]
fn credential_observation_preserves_state_and_existing_refusals() {
    let mut store = opened();
    let before = store.clone();
    let request = SessionTouch {
        refresh: Some(false),
        session_token: Secret::default(),
        code: Secret::default(),
    };
    let op = IdentityOp::Session(SessionOp::Resolve {
        request: request.clone(),
    });
    assert!(!op.is_mutation());
    let mut caller = broker();
    caller.token_hashes = vec!["hash-one".into()];
    let reply = apply_kept(&mut store, &op, &caller, NOW + 61_000).unwrap();
    let IdentityReply::Resolution(resolved) = reply else {
        panic!("expected resolution")
    };
    assert_eq!(resolved.principal_id, "usr:alice");
    assert_eq!(store, before);
    let expiry = before.sessions["hash-one"].idle_expires_at_ms;
    assert!(apply_kept(&mut store, &op, &caller, expiry).is_err());
    assert_eq!(store, before);
    let revoke = IdentityOp::Session(SessionOp::Revoke { request });
    assert!(revoke.is_mutation());
    apply_kept(&mut store, &revoke, &caller, NOW + 61_001).unwrap();
    assert!(apply_kept(&mut store, &op, &caller, NOW + 61_002).is_err());
}

// spec: MEDULLA-TYPED-PACKS-R085.6.1
#[test]
fn omitted_refresh_preserves_legacy_touch_and_authority() {
    let request: SessionTouch =
        serde_json::from_value(serde_json::json!({"session_token":"", "code":""})).unwrap();
    assert_eq!(request.refresh, None);
    let op = IdentityOp::Session(SessionOp::Resolve { request });
    assert!(op.is_mutation());
    let mut store = opened();
    let mut caller = broker();
    caller.token_hashes = vec!["hash-one".into()];
    apply_kept(&mut store, &op, &caller, NOW + 61_000).unwrap();
    assert_eq!(store.sessions["hash-one"].last_seen_at_ms, NOW + 61_000);
    let observe = IdentityOp::Session(SessionOp::Resolve {
        request: SessionTouch {
            refresh: Some(false),
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    caller.actor.scopes.clear();
    assert!(apply_kept(&mut store, &observe, &caller, NOW + 61_001).is_err());
}

// spec: MEDULLA-TYPED-PACKS-R085.7.1
// spec: MEDULLA-TYPED-PACKS-R085.4.1
#[test]
fn reference_operations_refuse_service_principal_substitution_without_mutation() {
    let mut store = opened();
    store.users.get_mut("usr:alice").unwrap().kind = UserKind::Service;
    let mut owner = IdentityStamp::for_actor(actor("usr:alice", &[IDENTITY_SELF_SCOPE]));
    owner.session_tenant = Some("tenant-one".into());
    let scopes = store.resolve("usr:alice", &TestRegistry).unwrap().scopes;
    let before = store.clone();
    for op in [
        IdentityOp::Session(SessionOp::InspectReference {
            session_ref: "ses:one".into(),
            tenant: "tenant-one".into(),
        }),
        IdentityOp::Session(SessionOp::RenewReference {
            session_ref: "ses:one".into(),
            tenant: "tenant-one".into(),
            expected_authority_revision: 0,
            expected_scopes: scopes,
        }),
    ] {
        assert_eq!(
            apply_kept(&mut store, &op, &owner, NOW + 1),
            Err(IdentityRefusal::NotAuthorized)
        );
        assert_eq!(store, before);
    }
}

// spec: MEDULLA-TYPED-PACKS-R085.4.1
#[test]
fn renewal_clamps_idle_to_original_absolute_expiry_and_preserves_subject_mfa() {
    let mut store = opened();
    let session = store.sessions.get_mut("hash-one").unwrap();
    session.absolute_expires_at_ms = NOW + 120_000;
    session.idle_expires_at_ms = NOW + 90_000;
    session.mfa_at_ms = Some(NOW - 17);
    let before = session.clone();
    let (op, owner) = renewal_for(&store);
    apply_kept(&mut store, &op, &owner, NOW + 1).unwrap();
    let after = &store.sessions["hash-one"];
    let mut expected = before;
    expected.last_seen_at_ms = NOW + 1;
    expected.idle_expires_at_ms = expected.absolute_expires_at_ms;
    assert_eq!(after, &expected);
    let renewed = store.clone();
    assert!(apply_kept(&mut store, &op, &owner, NOW + 120_000).is_err());
    assert_eq!(store, renewed);
}

// spec: MEDULLA-TYPED-PACKS-R085.4.1
#[test]
fn renewal_refuses_invalid_authority_without_mutation() {
    for reason in [
        InvalidRenewal::Revoked,
        InvalidRenewal::Idle,
        InvalidRenewal::Absolute,
        InvalidRenewal::Pending,
        InvalidRenewal::Disabled,
        InvalidRenewal::Ambiguous,
        InvalidRenewal::Owner,
        InvalidRenewal::Tenant,
        InvalidRenewal::Delegated,
        InvalidRenewal::Scopes,
    ] {
        let mut store = opened();
        let mut owner = IdentityStamp::for_actor(actor("usr:alice", &[IDENTITY_SELF_SCOPE]));
        owner.session_tenant = Some("tenant-one".into());
        let mut scopes = store.resolve("usr:alice", &TestRegistry).unwrap().scopes;
        invalidate_renewal(reason, &mut store, &mut owner, &mut scopes);
        let op = IdentityOp::Session(SessionOp::RenewReference {
            session_ref: "ses:one".into(),
            tenant: "tenant-one".into(),
            expected_authority_revision: 0,
            expected_scopes: scopes,
        });
        let before = store.clone();
        assert!(
            apply_kept(&mut store, &op, &owner, NOW + 1).is_err(),
            "{reason:?}"
        );
        assert_eq!(store, before, "{reason:?}");
    }
}

// spec: MEDULLA-TYPED-PACKS-R085.4.1
#[test]
fn repeated_renewal_never_becomes_issuer_reauthentication() {
    let mut store = opened();
    store.sessions.get_mut("hash-one").unwrap().mfa_at_ms = Some(NOW - 17);
    let original = store.sessions["hash-one"].clone();
    let idle = original.idle_expires_at_ms - NOW;
    assert!(idle > 1);
    let (op, owner) = renewal_for(&store);
    let mut now = NOW;
    let mut renewals = 0;
    while store.sessions["hash-one"].idle_expires_at_ms < original.absolute_expires_at_ms {
        now += idle / 2;
        let mut expected = store.sessions["hash-one"].clone();
        expected.last_seen_at_ms = now;
        expected.idle_expires_at_ms = (now + idle).min(original.absolute_expires_at_ms);
        apply_kept(&mut store, &op, &owner, now).unwrap();
        assert_eq!(store.sessions["hash-one"], expected);
        renewals += 1;
    }
    assert!(renewals > 1);
    let before = store.clone();
    for time in [
        now + 1,
        original.absolute_expires_at_ms,
        original.absolute_expires_at_ms + 1,
    ] {
        assert!(apply_kept(&mut store, &op, &owner, time).is_err());
        assert_eq!(store, before);
    }
}

fn reference_inspection() -> (IdentityOp, IdentityStamp) {
    let op = IdentityOp::Session(SessionOp::InspectReference {
        session_ref: "ses:one".into(),
        tenant: "tenant-one".into(),
    });
    let mut owner = broker();
    owner.actor.principal_id = "usr:alice".into();
    owner.actor.scopes = std::collections::BTreeSet::from([IDENTITY_SELF_SCOPE.into()]);
    owner.session_tenant = Some("tenant-one".into());
    (op, owner)
}

fn renewal_for(store: &IdentityStore) -> (IdentityOp, IdentityStamp) {
    let mut owner = IdentityStamp::for_actor(actor("usr:alice", &[IDENTITY_SELF_SCOPE]));
    owner.session_tenant = Some("tenant-one".into());
    let op = IdentityOp::Session(SessionOp::RenewReference {
        session_ref: "ses:one".into(),
        tenant: "tenant-one".into(),
        expected_authority_revision: 0,
        expected_scopes: store.resolve("usr:alice", &TestRegistry).unwrap().scopes,
    });
    (op, owner)
}

#[derive(Debug, Clone, Copy)]
enum InvalidRenewal {
    Revoked,
    Idle,
    Absolute,
    Pending,
    Disabled,
    Ambiguous,
    Owner,
    Tenant,
    Delegated,
    Scopes,
}

fn invalidate_renewal(
    reason: InvalidRenewal,
    store: &mut IdentityStore,
    owner: &mut IdentityStamp,
    scopes: &mut std::collections::BTreeSet<String>,
) {
    match reason {
        InvalidRenewal::Revoked => {
            store.sessions.get_mut("hash-one").unwrap().revoked_at_ms = Some(NOW)
        }
        InvalidRenewal::Idle => {
            store
                .sessions
                .get_mut("hash-one")
                .unwrap()
                .idle_expires_at_ms = NOW + 1
        }
        InvalidRenewal::Absolute => {
            store
                .sessions
                .get_mut("hash-one")
                .unwrap()
                .absolute_expires_at_ms = NOW + 1
        }
        InvalidRenewal::Pending => store.sessions.get_mut("hash-one").unwrap().mfa_pending = true,
        InvalidRenewal::Disabled => {
            store.users.get_mut("usr:alice").unwrap().status = UserStatus::Disabled
        }
        InvalidRenewal::Ambiguous => {
            store.sessions.get_mut("hash-two").unwrap().session_ref = Some("ses:one".into())
        }
        InvalidRenewal::Owner => owner.actor.principal_id = "usr:other".into(),
        InvalidRenewal::Tenant => owner.session_tenant = Some("tenant-other".into()),
        InvalidRenewal::Delegated => owner.actor.delegated = true,
        InvalidRenewal::Scopes => {
            scopes.remove("kg:read");
        }
    }
}
