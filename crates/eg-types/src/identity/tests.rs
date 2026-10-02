//! Identity store tests. Every refusal is tested both ways: the refused
//! input, and the nearest input that is accepted.

use std::collections::BTreeSet;

use super::*;

mod access;
mod auth;
mod factors;
mod modes;
mod provision;
mod recovery;
mod tokens;
mod verdicts;

/// A registry fixture with one scope of every class.
pub(super) struct TestRegistry;

const REGISTRY: [(&str, ScopeClass, Option<&str>); 15] = [
    ("governance:read", ScopeClass::User, None),
    ("identity:provision", ScopeClass::ServiceOnly, None),
    ("kg:read", ScopeClass::User, None),
    ("identity:self", ScopeClass::User, None),
    ("finance:alerts", ScopeClass::Domain, None),
    ("capacity:throttle", ScopeClass::ServiceOnly, None),
    ("identity:authenticate", ScopeClass::ServiceOnly, None),
    (
        "rbac:approve-elevation",
        ScopeClass::Approver,
        Some(ELEVATION_APPROVERS_GROUP),
    ),
    (
        "finance:approve-live-order",
        ScopeClass::Approver,
        Some(LIVE_ORDER_APPROVERS_GROUP),
    ),
    ("kg:admin", ScopeClass::Admin, None),
    ("webui:admin", ScopeClass::Admin, None),
    ("identity:admin", ScopeClass::Admin, None),
    ("identity:read", ScopeClass::Admin, None),
    ("kg:write", ScopeClass::User, None),
    (
        "governance:approve-schema-repair",
        ScopeClass::Approver,
        Some(SCHEMA_APPROVERS_GROUP),
    ),
];

impl ScopeClassifier for TestRegistry {
    fn class_of(&self, scope: &str) -> Option<ScopeClass> {
        REGISTRY.iter().find(|row| row.0 == scope).map(|row| row.1)
    }

    fn approver_group_of(&self, scope: &str) -> Option<&'static str> {
        REGISTRY
            .iter()
            .find(|row| row.0 == scope)
            .and_then(|row| row.2)
    }
}

pub(super) const NOW: u64 = 1_800_000_000_000;

pub(super) fn ctx_at(now_ms: u64) -> ApplyContext<'static> {
    ApplyContext {
        now_ms,
        classifier: &TestRegistry,
    }
}

pub(super) fn actor(principal: &str, scopes: &[&str]) -> IdentityActor {
    IdentityActor {
        principal_id: principal.to_string(),
        delegated: false,
        scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
    }
}

pub(super) fn admin() -> IdentityStamp {
    IdentityStamp::for_actor(actor("usr:bootstrap", &[IDENTITY_ADMIN_SCOPE]))
}

pub(super) fn broker() -> IdentityStamp {
    IdentityStamp::for_actor(actor("svc:graph-os", &[IDENTITY_AUTHENTICATE_SCOPE]))
}

/// A store initialized in `mode` (local sets a bootstrap password hash).
pub(super) fn store_in(mode: AuthMode) -> IdentityStore {
    let mut store = IdentityStore::default();
    let mut stamp = broker();
    stamp.password_hash = Some("$argon2id$bootstrap".to_string());
    let op = IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode,
            admin_username: Some("root-admin".to_string()),
            admin_password: Secret::default(),
        },
    });
    store.apply(&op, &stamp, &ctx_at(NOW)).expect("initialize");
    store
}

pub(super) fn create(
    store: &mut IdentityStore,
    username: &str,
    kind: UserKind,
) -> Result<String, IdentityRefusal> {
    let mut stamp = admin();
    stamp.minted_principal_id = Some(format!("usr:{username}"));
    let op = IdentityOp::User(UserOp::Create {
        request: CreateUserRequest {
            username: username.to_string(),
            kind,
            principal_id: None,
            display_name: None,
            email: None,
            roles: BTreeSet::new(),
            groups: BTreeSet::new(),
            password: Secret::default(),
            must_change: false,
        },
    });
    match store.apply(&op, &stamp, &ctx_at(NOW))? {
        IdentityReply::Principal { principal_id } => Ok(principal_id),
        other => panic!("unexpected reply {other:?}"),
    }
}

/// The verdict the boundary would stamp for `principal`: bound to the
/// generation of the credential the store holds for it right now.
pub(super) fn check_for(
    store: &IdentityStore,
    principal: Option<&str>,
    matched: bool,
) -> PasswordCheck {
    PasswordCheck {
        principal_id: principal.map(str::to_string),
        generation: principal
            .and_then(|principal| store.credential_of(principal))
            .map(|credential| credential.generation),
        matched,
        rehash: None,
    }
}

/// Open a live session `session` for the principal of `username` (its
/// password verdict is stamped as matched).
pub(super) fn open_session(
    store: &mut IdentityStore,
    username: &str,
    principal: &str,
    session: &str,
) {
    let mut stamp = broker();
    stamp.password_check = Some(check_for(store, Some(principal), true));
    stamp.token_hashes = vec![session.to_string()];
    let op = IdentityOp::Credential(CredentialOp::Authenticate {
        request: AuthenticateRequest {
            username: username.to_string(),
            password: Secret::default(),
            session_token: Secret::default(),
            ip_prefix: None,
            new_password: Secret::default(),
        },
    });
    let reply = apply_kept(store, &op, &stamp, NOW).expect("sign-in");
    assert!(
        matches!(reply, IdentityReply::Authenticate(ref result) if result.outcome == AuthenticateOutcome::Ok),
        "{reply:?}"
    );
}

/// The bootstrap administrator's live session in a local-mode store.
pub(super) const ADMIN_SESSION: &str = "admin-session";

pub(super) fn with_admin_session(store: &mut IdentityStore) {
    open_session(store, "root-admin", BOOTSTRAP_PRINCIPAL, ADMIN_SESSION);
}

/// Apply on a clone and keep it only on success -- the engine's discipline.
pub(super) fn apply_kept(
    store: &mut IdentityStore,
    op: &IdentityOp,
    stamp: &IdentityStamp,
    now_ms: u64,
) -> Result<IdentityReply, IdentityRefusal> {
    let mut next = store.clone();
    let reply = next.apply(op, stamp, &ctx_at(now_ms))?;
    *store = next;
    Ok(reply)
}

#[test]
fn an_empty_store_serializes_to_nothing_and_round_trips() {
    let empty = IdentityStore::default();
    assert!(empty.is_empty());
    assert_eq!(serde_json::to_string(&empty).unwrap(), "{}");
    let store = store_in(AuthMode::Local);
    let bytes = serde_json::to_vec(&store).unwrap();
    let back: IdentityStore = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(back, store);
}

#[test]
fn the_wire_op_carries_both_tags_and_refuses_unknown_fields() {
    let op = IdentityOp::User(UserOp::Get {
        request: ObjectRef {
            id: "usr:a".to_string(),
        },
    });
    let json = serde_json::to_value(&op).unwrap();
    assert_eq!(json["family"], "user");
    assert_eq!(json["op"], "get");
    let back: IdentityOp = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(back, op);
    let mut extra = json;
    extra["request"]["smuggled"] = serde_json::json!(true);
    assert!(serde_json::from_value::<IdentityOp>(extra).is_err());
}

/// A refusal is served as its wire token, and the served boundary keeps only
/// tokens the engine declares: an undeclared one would reach the caller as an
/// unclassified internal error.
#[test]
fn every_refusal_is_reported_as_a_declared_wire_code() {
    for refusal in IdentityRefusal::ALL {
        let code = refusal.to_string();
        assert!(code.starts_with("IDENTITY_"), "{code}");
        assert!(crate::contract::declared_error_code(&code), "{code}");
    }
}

#[test]
fn a_secret_never_prints() {
    let secret = Secret::new("hunter2hunter2");
    assert!(!format!("{secret:?}").contains("hunter2"));
    let mut cleared = secret.clone();
    assert_eq!(cleared.take(), "hunter2hunter2");
    assert!(cleared.is_empty());
}

#[test]
fn views_never_carry_hashes_or_sealed_secrets() {
    let mut store = store_in(AuthMode::Local);
    let op = IdentityOp::User(UserOp::List {
        request: ListQuery {
            after: None,
            limit: 10,
        },
    });
    let reply = store.apply(&op, &admin(), &ctx_at(NOW)).unwrap();
    let text = serde_json::to_string(&reply).unwrap();
    assert!(text.contains("usr:bootstrap"));
    assert!(
        !text.contains("argon2"),
        "a view leaked a password hash: {text}"
    );
    assert!(text.contains("\"has_password\":true"));
}

#[test]
fn the_audit_trail_chains_and_detects_tampering() {
    let store = store_in(AuthMode::Local);
    assert!(store.audit_trail().verify().is_ok());
    let mut json = serde_json::to_value(&store).unwrap();
    json["audit"]["entries"][0]["actor"] = serde_json::json!("someone-else");
    let tampered: IdentityStore = serde_json::from_value(json).unwrap();
    assert_eq!(tampered.audit_trail().verify(), Err(0));
}

#[test]
fn the_denial_sampler_keeps_one_per_principal_and_action_per_window() {
    let mut sampler = DenialSampler::default();
    let sample = |at_ms, principal: &str| DenialSample {
        at_ms,
        principal: principal.to_string(),
        action: "kg:write".to_string(),
        reason: "ACCESS_DENIED".to_string(),
    };
    assert!(sampler.offer(sample(NOW, "a")));
    assert!(
        !sampler.offer(sample(NOW + 1, "a")),
        "same key inside the window"
    );
    assert!(
        sampler.offer(sample(NOW + 1, "b")),
        "another principal is kept"
    );
    assert!(
        sampler.offer(sample(NOW + 61_000, "a")),
        "a new window keeps it again"
    );
    let (drained, dropped) = sampler.drain();
    assert_eq!(drained.len(), 3);
    assert_eq!(dropped, 0);
    assert!(sampler.is_empty());
}

#[test]
fn the_password_policy_refuses_short_self_naming_and_common_passwords() {
    assert!(check_password("correct horse battery", "alice", None, 12).is_ok());
    assert_eq!(
        check_password("short", "alice", None, 12),
        Err(IdentityRefusal::WeakPassword)
    );
    assert_eq!(
        check_password("alice-is-my-password", "alice", None, 12),
        Err(IdentityRefusal::WeakPassword)
    );
    assert_eq!(
        check_password("password1234", "bob", None, 12),
        Err(IdentityRefusal::WeakPassword)
    );
    assert_eq!(
        check_password("mail.box.rules!", "zed", Some("mail.box@example.org"), 12),
        Err(IdentityRefusal::WeakPassword)
    );
}

#[test]
fn usernames_are_ascii_normalized_and_bounded() {
    assert_eq!(normalize_username("  Alice.Smith ").unwrap(), "alice.smith");
    assert!(normalize_username("ålice").is_err(), "non-ASCII is refused");
    assert!(normalize_username("a b").is_err());
    assert!(normalize_username("").is_err());
}
