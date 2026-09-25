//! One-time tokens, API keys and external sign-in.

use super::*;

fn issue(
    purpose: TokenPurpose,
    principal: Option<&str>,
    hash: &str,
) -> (IdentityOp, IdentityStamp) {
    let mut stamp = broker();
    stamp.token_hashes = vec![ADMIN_SESSION.to_string(), hash.to_string()];
    let op = IdentityOp::Token(TokenOp::IssueOneTime {
        request: OneTimeTokenIssue {
            session_token: Secret::default(),
            purpose,
            principal_id: principal.map(str::to_string),
            token: Secret::default(),
            ttl_ms: 30 * 60 * 1000,
        },
    });
    (op, stamp)
}

fn redeem(purpose: TokenPurpose, hash: &str) -> (IdentityOp, IdentityStamp) {
    let mut stamp = broker();
    stamp.token_hashes = vec![hash.to_string()];
    stamp.password_hash = Some("$argon2id$reset".to_string());
    let op = IdentityOp::Token(TokenOp::RedeemOneTime {
        request: TokenRedeem {
            purpose,
            token: Secret::default(),
            new_password: Secret::default(),
            link: None,
        },
    });
    (op, stamp)
}

#[test]
fn a_one_time_token_is_spent_once_for_its_own_purpose_only() {
    let mut store = store_in(AuthMode::Local);
    with_admin_session(&mut store);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    let (op, stamp) = issue(TokenPurpose::AdminReset, Some(&alice), "t1");
    apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    let (wrong, stamp_wrong) = redeem(TokenPurpose::PasswordReset, "t1");
    assert_eq!(
        apply_kept(&mut store, &wrong, &stamp_wrong, NOW),
        Err(IdentityRefusal::TokenSpent)
    );
    let (op, stamp) = redeem(TokenPurpose::AdminReset, "t1");
    assert!(apply_kept(&mut store, &op, &stamp, NOW).is_ok());
    assert!(store.credential_of(&alice).is_some());
    assert_eq!(
        apply_kept(&mut store, &op, &stamp, NOW),
        Err(IdentityRefusal::TokenSpent),
        "a replay is refused"
    );
}

#[test]
fn issuing_a_token_needs_a_live_administrator_session() {
    let mut store = store_in(AuthMode::Local);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    let (op, stamp) = issue(TokenPurpose::AdminReset, Some(&alice), "t0");
    assert_eq!(
        apply_kept(&mut store, &op, &stamp, NOW),
        Err(IdentityRefusal::NotAuthorized),
        "no administrator session exists yet"
    );
    let mut direct = admin();
    direct.token_hashes = stamp.token_hashes.clone();
    with_admin_session(&mut store);
    assert_eq!(
        apply_kept(&mut store, &op, &direct, NOW),
        Err(IdentityRefusal::NotAuthorized),
        "only the broker submits a caller-generated token"
    );
    assert!(apply_kept(&mut store, &op, &stamp, NOW).is_ok());
    open_session(&mut store, "alice", &alice, "alice-session");
    let mut as_alice = stamp.clone();
    as_alice.token_hashes = vec!["alice-session".to_string(), "t9".to_string()];
    assert_eq!(
        apply_kept(&mut store, &op, &as_alice, NOW),
        Err(IdentityRefusal::NotAuthorized),
        "a non-administrator's session cannot issue one"
    );
}

#[test]
fn an_expired_token_is_spent() {
    let mut store = store_in(AuthMode::Local);
    with_admin_session(&mut store);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    let (op, stamp) = issue(TokenPurpose::PasswordReset, Some(&alice), "t2");
    apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    let (op, stamp) = redeem(TokenPurpose::PasswordReset, "t2");
    assert_eq!(
        apply_kept(&mut store, &op, &stamp, NOW + 31 * 60 * 1000),
        Err(IdentityRefusal::TokenSpent)
    );
    assert!(apply_kept(&mut store, &op, &stamp, NOW + 60_000).is_ok());
}

fn api_key(principal: &str, key_id: &str, scopes: &[&str]) -> (IdentityOp, IdentityStamp) {
    let mut stamp = broker();
    stamp.token_hashes = vec![ADMIN_SESSION.to_string(), format!("{key_id}-secret")];
    let op = IdentityOp::Token(TokenOp::IssueApiKey {
        request: ApiKeyIssue {
            session_token: Secret::default(),
            principal_id: principal.to_string(),
            key_id: key_id.to_string(),
            secret: Secret::default(),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            ttl_ms: 60 * 60 * 1000,
        },
    });
    (op, stamp)
}

fn verify_key(key_id: &str, secret_hash: &str) -> (IdentityOp, IdentityStamp) {
    let mut stamp = broker();
    stamp.token_hashes = vec![secret_hash.to_string()];
    let op = IdentityOp::Token(TokenOp::VerifyApiKey {
        request: ApiKeyUse {
            key_id: key_id.to_string(),
            secret: Secret::default(),
        },
    });
    (op, stamp)
}

#[test]
fn api_key_listing_is_paged_redacted_and_read_scoped() {
    let mut store = store_in(AuthMode::Local);
    with_admin_session(&mut store);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    for id in ["k1", "k2"] {
        let (op, stamp) = api_key(&alice, id, &["kg:read"]);
        apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    }
    let reader = IdentityStamp::for_actor(actor("usr:reader", &[IDENTITY_READ_SCOPE]));
    let page = |after| {
        IdentityOp::Token(TokenOp::ListApiKeys {
            request: PrincipalListQuery {
                principal_id: alice.clone(),
                after,
                limit: 1,
            },
        })
    };
    let IdentityReply::ApiKeys(first) = apply_kept(&mut store, &page(None), &reader, NOW).unwrap()
    else {
        panic!("expected API keys")
    };
    assert_eq!(first[0].key_id, "k1");
    let serialized = serde_json::to_string(&first).unwrap();
    assert!(!serialized.contains("secret"));
    let IdentityReply::ApiKeys(second) =
        apply_kept(&mut store, &page(Some("k1".to_string())), &reader, NOW).unwrap()
    else {
        panic!("expected API keys")
    };
    assert_eq!(second[0].key_id, "k2");
    assert_eq!(
        apply_kept(&mut store, &page(None), &broker(), NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
}

#[test]
fn an_api_key_carries_only_scopes_its_owner_holds_and_narrows_with_the_owner() {
    let mut store = store_in(AuthMode::Local);
    with_admin_session(&mut store);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    let (op, stamp) = api_key(&alice, "k1", &["kg:write"]);
    assert_eq!(
        apply_kept(&mut store, &op, &stamp, NOW),
        Err(IdentityRefusal::ClassViolation),
        "alice does not hold kg:write"
    );
    let (op, stamp) = api_key(BOOTSTRAP_PRINCIPAL, "k2", &["kg:admin"]);
    assert_eq!(
        apply_kept(&mut store, &op, &stamp, NOW),
        Err(IdentityRefusal::ClassViolation),
        "no administrator scope rides a bearer key"
    );
    let (op, stamp) = api_key(&alice, "k3", &["kg:read"]);
    apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    let (verify, good) = verify_key("k3", "k3-secret");
    let IdentityReply::Resolution(resolved) = apply_kept(&mut store, &verify, &good, NOW).unwrap()
    else {
        panic!("a key answers a resolution");
    };
    assert_eq!(resolved.scopes, ["kg:read".to_string()].into());
    let (_, wrong) = verify_key("k3", "not-the-secret");
    assert_eq!(
        apply_kept(&mut store, &verify, &wrong, NOW),
        Err(IdentityRefusal::NotFound)
    );
    let unbind = IdentityOp::Access(AccessOp::ChangeUserRole {
        request: UserRoleChange {
            principal_id: alice.clone(),
            role_id: USER_ROLE.to_string(),
            change: BindingChange::Remove,
        },
    });
    apply_kept(&mut store, &unbind, &admin(), NOW).unwrap();
    let IdentityReply::Resolution(narrowed) = apply_kept(&mut store, &verify, &good, NOW).unwrap()
    else {
        panic!("a key answers a resolution");
    };
    assert!(
        narrowed.scopes.is_empty(),
        "the owner lost kg:read, so the key did"
    );
}

#[test]
fn a_revoked_key_is_refused() {
    let mut store = store_in(AuthMode::Local);
    with_admin_session(&mut store);
    let alice = create(&mut store, "alice", UserKind::Human).unwrap();
    let (op, stamp) = api_key(&alice, "k4", &["kg:read"]);
    apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    let revoke = IdentityOp::Token(TokenOp::RevokeApiKey {
        request: ObjectRef {
            id: "k4".to_string(),
        },
    });
    apply_kept(&mut store, &revoke, &admin(), NOW).unwrap();
    let (verify, good) = verify_key("k4", "k4-secret");
    assert_eq!(
        apply_kept(&mut store, &verify, &good, NOW),
        Err(IdentityRefusal::NotFound)
    );
}

fn keycloak_store(jit: JitPolicy) -> IdentityStore {
    let mut store = store_in(AuthMode::Local);
    let idp = IdentityOp::Idp(IdpOp::Upsert {
        request: IdpConfig {
            idp_id: "keycloak".to_string(),
            kind: IdpKind::Oidc,
            display_name: "Keycloak".to_string(),
            enabled: true,
            config_json: "{}".to_string(),
            secret_ref: None,
            jit_policy: jit,
            email_domains: Vec::new(),
            order: 0,
            rules: vec![MappingRule {
                rule_id: "approvers".to_string(),
                claim_path: "groups".to_string(),
                match_kind: "equals".to_string(),
                value: "elevation-approvers".to_string(),
                target: format!("group:{ELEVATION_APPROVERS_GROUP}"),
                privileged: true,
            }],
        },
    });
    apply_kept(&mut store, &idp, &admin(), NOW).unwrap();
    let mut json = serde_json::to_value(&store).unwrap();
    json["config"]["mode"] = serde_json::json!("external");
    serde_json::from_value(json).unwrap()
}

fn external(subject: &str, groups: &[&str], session: &str) -> (IdentityOp, IdentityStamp) {
    let mut stamp = broker();
    stamp.token_hashes = vec![session.to_string()];
    stamp.minted_principal_id = Some(format!("usr:jit-{subject}"));
    let op = IdentityOp::Credential(CredentialOp::ExternalLogin {
        request: ExternalLogin {
            idp_id: "keycloak".to_string(),
            subject: subject.to_string(),
            claims: [(
                "groups".to_string(),
                groups.iter().map(|group| group.to_string()).collect(),
            )]
            .into(),
            username_hint: Some(subject.to_string()),
            session_token: Secret::default(),
            ip_prefix: None,
        },
    });
    (op, stamp)
}

#[test]
fn an_idp_group_removal_removes_the_membership_at_the_next_sign_in() {
    let mut store = keycloak_store(JitPolicy::Create);
    let (op, stamp) = external("carol", &["elevation-approvers"], "c1");
    apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    let carol = "usr:jit-carol";
    assert!(store
        .resolve(carol, &TestRegistry)
        .unwrap()
        .scopes
        .contains("rbac:approve-elevation"));
    let (op, stamp) = external("carol", &[], "c2");
    apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    assert!(!store
        .resolve(carol, &TestRegistry)
        .unwrap()
        .scopes
        .contains("rbac:approve-elevation"));
}

#[test]
fn a_deny_jit_idp_refuses_an_unlinked_subject_and_admits_a_linked_one() {
    let mut store = keycloak_store(JitPolicy::Deny);
    let (op, stamp) = external("dave", &[], "d1");
    let IdentityReply::Authenticate(refused) = apply_kept(&mut store, &op, &stamp, NOW).unwrap()
    else {
        panic!("external sign-in answers an authenticate result");
    };
    assert_eq!(refused.outcome, AuthenticateOutcome::Bad);
    let dave = create(&mut store, "dave", UserKind::Human).unwrap();
    let link = IdentityOp::Idp(IdpOp::Link {
        request: LinkRequest {
            idp_id: "keycloak".to_string(),
            subject: "dave".to_string(),
            principal_id: dave.clone(),
        },
    });
    apply_kept(&mut store, &link, &admin(), NOW).unwrap();
    let IdentityReply::Authenticate(ok) = apply_kept(&mut store, &op, &stamp, NOW).unwrap() else {
        panic!("external sign-in answers an authenticate result");
    };
    assert_eq!(ok.principal_id.as_deref(), Some(dave.as_str()));
}

#[test]
fn leaving_external_puts_credentialless_humans_into_pending_reset() {
    let mut store = keycloak_store(JitPolicy::Create);
    let (op, stamp) = external("erin", &[], "e1");
    apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    let epoch = store.config().unwrap().epoch;
    let back = IdentityOp::Config(ConfigOp::Transition {
        request: ModeTransition {
            expected_epoch: epoch,
            to: AuthMode::Local,
            local_fallback: None,
            ack: None,
            issuer_kid: "kid-back".to_string(),
        },
    });
    apply_kept(&mut store, &back, &admin(), NOW).unwrap();
    let erin = store.resolve("usr:jit-erin", &TestRegistry).unwrap();
    assert_eq!(erin.status, UserStatus::PendingReset);
    assert!(
        store.manages("usr:jit-erin"),
        "the account and its link are kept"
    );
}
