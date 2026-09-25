//! The engine's System bootstrap stays open over the identity store's SEED
//! only: initializing the store keeps it open; any real principal made
//! through the store (first-admin setup, SCIM or LDAP provisioning, an
//! administrator's role) closes it, so nobody can claim an instance someone
//! already set up.

use std::collections::{BTreeMap, BTreeSet};

use super::identity_store_tests::{agent, apply, seeded, stamp};
use super::IsolationLayer;
use eg_types::identity::*;

fn initialized(mode: AuthMode) -> IsolationLayer {
    let mut layer = IsolationLayer::new();
    let mut init = stamp(IDENTITY_AUTHENTICATE_SCOPE);
    let admin_username = (mode == AuthMode::Local).then(|| "root".to_string());
    if admin_username.is_some() {
        init.password_hash = Some("$argon2id$first-admin".to_string());
    }
    let op = IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode,
            admin_username,
            admin_password: Secret::default(),
        },
    });
    apply(&mut layer, op, &init).unwrap();
    layer
}

fn upsert_idp(layer: &mut IsolationLayer, idp_id: &str, kind: IdpKind, config_json: &str) {
    let op = IdentityOp::Idp(IdpOp::Upsert {
        request: IdpConfig {
            idp_id: idp_id.to_string(),
            kind,
            display_name: idp_id.to_string(),
            enabled: true,
            config_json: config_json.to_string(),
            secret_ref: None,
            jit_policy: JitPolicy::Deny,
            email_domains: Vec::new(),
            order: 0,
            rules: Vec::new(),
        },
    });
    apply(layer, op, &stamp(IDENTITY_ADMIN_SCOPE)).unwrap();
}

fn provision(layer: &mut IsolationLayer, idp_id: &str, mut actor: IdentityStamp) {
    actor.minted_principal_id = Some("usr:provisioned".to_string());
    let op = IdentityOp::Idp(IdpOp::Provision {
        request: ProvisionSubject {
            idp_id: idp_id.to_string(),
            subject: "ext-ann".to_string(),
            username: "ann".to_string(),
            display_name: None,
            email: None,
            active: true,
            claims: BTreeMap::new(),
        },
    });
    apply(layer, op, &actor).unwrap();
}

#[test]
fn a_seed_only_store_keeps_the_bootstrap_open_and_first_admin_setup_closes_it() {
    assert!(IsolationLayer::new().identity_bootstrap_pending());
    let seed = initialized(AuthMode::None);
    assert!(
        seed.identity_bootstrap_pending(),
        "initializing the store (built-in roles, credential-less bootstrap principal) keeps it open"
    );
    assert!(
        !initialized(AuthMode::Local).identity_bootstrap_pending(),
        "the first administrator's credential is a real principal"
    );
}

#[test]
fn a_scim_provisioned_user_closes_the_bootstrap() {
    let mut layer = initialized(AuthMode::None);
    upsert_idp(
        &mut layer,
        "scim",
        IdpKind::Scim,
        "{\"provisioner\":\"usr:admin\"}",
    );
    assert!(
        !layer.identity_bootstrap_pending(),
        "a configured IdP is already not the seed"
    );
    provision(&mut layer, "scim", stamp(IDENTITY_PROVISION_SCOPE));
    assert!(!layer.identity_bootstrap_pending());
}

#[test]
fn an_ldap_synced_user_closes_the_bootstrap() {
    let mut layer = initialized(AuthMode::None);
    upsert_idp(&mut layer, "ldap", IdpKind::Ldap, "{}");
    provision(&mut layer, "ldap", stamp(IDENTITY_AUTHENTICATE_SCOPE));
    assert!(!layer.identity_bootstrap_pending());
}

#[test]
fn a_real_user_or_an_administrator_made_role_closes_the_bootstrap() {
    assert!(
        !seeded().identity_bootstrap_pending(),
        "a created user with an administrator-made role"
    );
    let mut layer = initialized(AuthMode::None);
    let role = IdentityOp::Access(AccessOp::UpsertRole {
        request: RoleUpsert {
            role_id: "reports".to_string(),
            name: "reports".to_string(),
            description: None,
            scopes: BTreeSet::from(["kg:read".to_string()]),
            graph_grants: Vec::new(),
        },
    });
    apply(&mut layer, role, &stamp(IDENTITY_ADMIN_SCOPE)).unwrap();
    assert!(!layer.identity_bootstrap_pending());
}

#[test]
fn the_durable_corruption_check_admits_store_state_but_never_a_foreign_identity() {
    let layer = seeded();
    let store_owned: BTreeMap<_, _> = layer
        .agents
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert!(
        super::layer_store::holds_only_identity_store_state(layer.rbac(), &store_owned),
        "a store used before the System bootstrap is not corruption"
    );
    assert!(
        !layer.identity_bootstrap_pending(),
        "...but its real principal keeps the bootstrap closed"
    );
    let foreign = BTreeMap::from([("agent:x".to_string(), agent("agent:x"))]);
    assert!(
        !super::layer_store::holds_only_identity_store_state(layer.rbac(), &foreign),
        "a non-store identity before bootstrap is still corruption"
    );
}
