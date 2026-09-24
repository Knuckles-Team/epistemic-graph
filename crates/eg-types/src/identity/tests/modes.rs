//! First run and the mode state machine (IDM-04).

use super::*;

fn transition(to: AuthMode, epoch: u64, ack: Option<&str>) -> IdentityOp {
    IdentityOp::Config(ConfigOp::Transition {
        request: ModeTransition {
            expected_epoch: epoch,
            to,
            local_fallback: None,
            ack: ack.map(str::to_string),
            issuer_kid: format!("kid-{epoch}"),
        },
    })
}

fn set_password(principal: &str) -> (IdentityOp, IdentityStamp) {
    let mut stamp = admin();
    stamp.password_hash = Some("$argon2id$set".to_string());
    let op = IdentityOp::Credential(CredentialOp::SetPassword {
        request: PasswordSet {
            principal_id: principal.to_string(),
            password: Secret::default(),
            must_change: false,
        },
    });
    (op, stamp)
}

#[test]
fn initialize_runs_once_and_only_for_the_first_run_authority() {
    let mut store = IdentityStore::default();
    let op = IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode: AuthMode::None,
            admin_username: None,
            admin_password: Secret::default(),
        },
    });
    let reader = IdentityStamp::for_actor(actor("usr:x", &[IDENTITY_READ_SCOPE]));
    assert_eq!(
        store.apply(&op, &reader, &ctx_at(NOW)),
        Err(IdentityRefusal::NotAuthorized)
    );
    let reply = store.apply(&op, &broker(), &ctx_at(NOW)).unwrap();
    assert_eq!(
        reply,
        IdentityReply::Principal {
            principal_id: BOOTSTRAP_PRINCIPAL.to_string()
        }
    );
    assert_eq!(store.config().unwrap().mode, AuthMode::None);
    assert_eq!(
        store.config().unwrap().registration_policy,
        RegistrationPolicy::Disabled
    );
    assert_eq!(
        store.apply(&op, &broker(), &ctx_at(NOW)),
        Err(IdentityRefusal::AlreadyInitialized)
    );
}

#[test]
fn local_initialize_requires_the_first_admin_credential() {
    let mut store = IdentityStore::default();
    let op = IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode: AuthMode::Local,
            admin_username: Some("root".to_string()),
            admin_password: Secret::default(),
        },
    });
    assert_eq!(
        store.clone().apply(&op, &broker(), &ctx_at(NOW)),
        Err(IdentityRefusal::Unstamped)
    );
    let mut stamp = broker();
    stamp.password_hash = Some("$argon2id$first".to_string());
    store.apply(&op, &stamp, &ctx_at(NOW)).unwrap();
    assert_eq!(
        store.config().unwrap().registration_policy,
        RegistrationPolicy::AdminOnly
    );
    assert!(store.credential_of(BOOTSTRAP_PRINCIPAL).is_some());
}

#[test]
fn every_op_but_initialize_needs_an_initialized_store() {
    let mut store = IdentityStore::default();
    let op = IdentityOp::Config(ConfigOp::Get);
    assert_eq!(
        store.apply(&op, &broker(), &ctx_at(NOW)),
        Err(IdentityRefusal::NotInitialized)
    );
}

#[test]
fn none_to_local_needs_an_administrator_with_a_credential() {
    let mut store = store_in(AuthMode::None);
    assert_eq!(
        apply_kept(
            &mut store,
            &transition(AuthMode::Local, 1, None),
            &admin(),
            NOW
        ),
        Err(IdentityRefusal::PreconditionFailed)
    );
    let (op, stamp) = set_password(BOOTSTRAP_PRINCIPAL);
    apply_kept(&mut store, &op, &stamp, NOW).unwrap();
    let reply = apply_kept(
        &mut store,
        &transition(AuthMode::Local, 1, None),
        &admin(),
        NOW,
    )
    .unwrap();
    let IdentityReply::Config(config) = reply else {
        panic!("transition answers the config");
    };
    assert_eq!(config.mode, AuthMode::Local);
    assert_eq!(config.epoch, 2);
    assert_eq!(config.issuer_kid_current.as_deref(), Some("kid-1"));
}

#[test]
fn a_stale_epoch_and_an_illegal_edge_are_refused() {
    let mut store = store_in(AuthMode::Local);
    assert_eq!(
        apply_kept(
            &mut store,
            &transition(AuthMode::External, 7, None),
            &admin(),
            NOW
        ),
        Err(IdentityRefusal::EpochConflict)
    );
    assert_eq!(
        apply_kept(
            &mut store,
            &transition(AuthMode::Local, 1, None),
            &admin(),
            NOW
        ),
        Err(IdentityRefusal::IllegalTransition)
    );
    assert_eq!(
        apply_kept(
            &mut store,
            &transition(AuthMode::External, 1, None),
            &admin(),
            NOW
        ),
        Err(IdentityRefusal::PreconditionFailed),
        "no enabled IdP links an administrator yet"
    );
}

#[test]
fn entering_none_needs_loopback_and_the_exact_ack() {
    let mut store = store_in(AuthMode::Local);
    let mut loopback = admin();
    loopback.engine_loopback = true;
    let refused = [
        (transition(AuthMode::None, 1, Some(NONE_MODE_ACK)), admin()),
        (transition(AuthMode::None, 1, None), loopback.clone()),
        (
            transition(AuthMode::None, 1, Some("I-UNDERSTAND")),
            loopback.clone(),
        ),
    ];
    for (op, stamp) in refused {
        assert_eq!(
            apply_kept(&mut store, &op, &stamp, NOW),
            Err(IdentityRefusal::PreconditionFailed)
        );
    }
    let accepted = transition(AuthMode::None, 1, Some(NONE_MODE_ACK));
    apply_kept(&mut store, &accepted, &loopback, NOW).unwrap();
    assert_eq!(store.config().unwrap().mode, AuthMode::None);
}

#[test]
fn a_delegated_administrator_cannot_transition() {
    let mut store = store_in(AuthMode::Local);
    let mut stamp = admin();
    stamp.actor.delegated = true;
    stamp.engine_loopback = true;
    let op = transition(AuthMode::None, 1, Some(NONE_MODE_ACK));
    assert_eq!(
        apply_kept(&mut store, &op, &stamp, NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
}

#[test]
fn every_transition_revokes_every_session_and_keeps_every_account() {
    let mut store = store_in(AuthMode::Local);
    let mut sign_in = broker();
    sign_in.password_check = Some(PasswordCheck {
        principal_id: Some(BOOTSTRAP_PRINCIPAL.to_string()),
        matched: true,
        rehash: None,
    });
    sign_in.token_hashes = vec!["session-hash".to_string()];
    let authenticate = IdentityOp::Credential(CredentialOp::Authenticate {
        request: AuthenticateRequest {
            username: "root-admin".to_string(),
            password: Secret::default(),
            session_token: Secret::default(),
            ip_prefix: None,
            new_password: Secret::default(),
        },
    });
    apply_kept(&mut store, &authenticate, &sign_in, NOW).unwrap();
    assert!(store.session_principal("session-hash", NOW).is_some());
    let mut loopback = admin();
    loopback.engine_loopback = true;
    let op = transition(AuthMode::None, 1, Some(NONE_MODE_ACK));
    apply_kept(&mut store, &op, &loopback, NOW).unwrap();
    assert!(store.session_principal("session-hash", NOW).is_none());
    assert!(store.manages(BOOTSTRAP_PRINCIPAL));
    assert!(store.credential_of(BOOTSTRAP_PRINCIPAL).is_some());
}

#[test]
fn bootstrap_sessions_exist_only_in_none_mode() {
    let mut none = store_in(AuthMode::None);
    let mut stamp = broker();
    stamp.token_hashes = vec!["demo-session".to_string()];
    let op = IdentityOp::Credential(CredentialOp::BootstrapSession {
        request: SessionTouch {
            session_token: Secret::default(),
            code: Secret::default(),
        },
    });
    apply_kept(&mut none, &op, &stamp, NOW).unwrap();
    assert_eq!(
        none.session_principal("demo-session", NOW),
        Some(BOOTSTRAP_PRINCIPAL)
    );
    let mut local = store_in(AuthMode::Local);
    assert_eq!(
        apply_kept(&mut local, &op, &stamp, NOW),
        Err(IdentityRefusal::PreconditionFailed)
    );
}

#[test]
fn update_policy_is_epoch_guarded_and_bounded() {
    let mut store = store_in(AuthMode::Local);
    let update = |epoch, min| {
        IdentityOp::Config(ConfigOp::UpdatePolicy {
            request: PolicyUpdate {
                expected_epoch: epoch,
                registration_policy: Some(RegistrationPolicy::Invite),
                local_fallback: None,
                password_min_chars: Some(min),
            },
        })
    };
    assert_eq!(
        apply_kept(&mut store, &update(1, 4), &admin(), NOW),
        Err(IdentityRefusal::InvalidRequest)
    );
    assert_eq!(
        apply_kept(&mut store, &update(9, 14), &admin(), NOW),
        Err(IdentityRefusal::EpochConflict)
    );
    apply_kept(&mut store, &update(1, 14), &admin(), NOW).unwrap();
    let config = store.config().unwrap();
    assert_eq!(config.password_min_chars, 14);
    assert_eq!(config.registration_policy, RegistrationPolicy::Invite);
}
