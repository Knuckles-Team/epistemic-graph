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
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    let mut stamp = broker();
    stamp.token_hashes.push(hash.into());
    apply_kept(store, &op, &stamp, now)
}

// spec: EG-TYPED-PACKS-R085.1
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

// spec: EG-TYPED-PACKS-R085.1
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

// spec: EG-TYPED-PACKS-R085.1
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

// spec: EG-TYPED-PACKS-R085.1
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

// spec: EG-TYPED-PACKS-R085.1
#[test]
fn resolve_preserves_creation_tenant_and_exact_mfa_timestamp() {
    let mut store = opened();
    store.sessions.get_mut("hash-one").unwrap().mfa_at_ms = Some(NOW - 17);
    let op = IdentityOp::Session(SessionOp::Resolve {
        request: SessionTouch {
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
