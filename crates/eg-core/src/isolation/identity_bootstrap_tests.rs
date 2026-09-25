//! The engine's System bootstrap and the identity store, in order:
//!
//! * the bootstrap stays open over the store's SEED only;
//! * until it has run, the store refuses every real principal, credential or
//!   grant (first-admin setup, SCIM/LDAP provisioning) with
//!   `SYSTEM_BOOTSTRAP_PENDING`, so the bootstrap can always still run;
//! * after it, a direct `identity:admin` may register or repair the System
//!   identity without replacing the agent's other roles.

use std::collections::{BTreeMap, BTreeSet};

use super::identity_store_tests::{agent, apply, bootstrapped, seeded, stamp, SYSTEM_AGENT};
use super::{AgentIdentity, AgentRole, IdentityStoreError, IsolationLayer};
use eg_types::identity::*;

fn initialize(layer: &mut IsolationLayer, mode: AuthMode) -> Result<(), IdentityStoreError> {
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
    apply(layer, op, &init).map(|_| ())
}

fn upsert_idp(kind: IdpKind, config_json: &str) -> IdentityOp {
    IdentityOp::Idp(IdpOp::Upsert {
        request: IdpConfig {
            idp_id: "directory".to_string(),
            kind,
            display_name: "directory".to_string(),
            enabled: true,
            config_json: config_json.to_string(),
            secret_ref: None,
            jit_policy: JitPolicy::Deny,
            email_domains: Vec::new(),
            order: 0,
            rules: Vec::new(),
        },
    })
}

fn provision(mut actor: IdentityStamp) -> (IdentityOp, IdentityStamp) {
    actor.minted_principal_id = Some("usr:provisioned".to_string());
    let op = IdentityOp::Idp(IdpOp::Provision {
        request: ProvisionSubject {
            idp_id: "directory".to_string(),
            subject: "ext-ann".to_string(),
            username: "ann".to_string(),
            display_name: None,
            email: None,
            active: true,
            claims: BTreeMap::new(),
        },
    });
    (op, actor)
}

fn repair(agent_id: &str) -> IdentityOp {
    IdentityOp::Config(ConfigOp::RepairSystemIdentity {
        request: ObjectRef {
            id: agent_id.to_string(),
        },
    })
}

fn pending_refusal(outcome: Result<IdentityReply, IdentityStoreError>) -> bool {
    matches!(outcome, Err(IdentityStoreError::SystemBootstrapPending))
        && IdentityStoreError::SystemBootstrapPending
            .to_string()
            .contains("SYSTEM_BOOTSTRAP_PENDING")
}

#[test]
fn first_admin_setup_waits_for_the_system_bootstrap() {
    let mut layer = IsolationLayer::new();
    assert!(
        matches!(
            initialize(&mut layer, AuthMode::Local),
            Err(IdentityStoreError::SystemBootstrapPending)
        ),
        "a first administrator before the System bootstrap is refused"
    );
    assert!(layer.identity_bootstrap_pending(), "and nothing was kept");
    initialize(&mut layer, AuthMode::None).unwrap();
    assert!(
        layer.identity_bootstrap_pending(),
        "the credential-less seed keeps the bootstrap open"
    );
    let mut ready = bootstrapped();
    initialize(&mut ready, AuthMode::Local).unwrap();
    assert!(ready
        .rbac()
        .identity_store()
        .credential_of(BOOTSTRAP_PRINCIPAL)
        .is_some());
}

#[test]
fn directory_provisioning_waits_for_the_system_bootstrap() {
    for (kind, config, actor) in [
        (
            IdpKind::Scim,
            "{\"provisioner\":\"usr:admin\"}",
            stamp(IDENTITY_PROVISION_SCOPE),
        ),
        (IdpKind::Ldap, "{}", stamp(IDENTITY_AUTHENTICATE_SCOPE)),
    ] {
        let mut early = IsolationLayer::new();
        initialize(&mut early, AuthMode::None).unwrap();
        let admin = stamp(IDENTITY_ADMIN_SCOPE);
        assert!(pending_refusal(apply(
            &mut early,
            upsert_idp(kind, config),
            &admin
        )));
        let mut ready = bootstrapped();
        initialize(&mut ready, AuthMode::None).unwrap();
        apply(&mut ready, upsert_idp(kind, config), &admin).unwrap();
        let (op, actor) = provision(actor);
        apply(&mut ready, op, &actor).unwrap();
        assert!(ready.rbac().identity_store().manages("usr:provisioned"));
        assert!(!ready.identity_bootstrap_pending());
    }
}

#[test]
fn a_real_principal_is_never_the_seed() {
    assert!(IsolationLayer::new().identity_bootstrap_pending());
    let layer = seeded();
    assert!(!layer.rbac().identity_store().holds_only_seed());
    let mut role_only = bootstrapped();
    initialize(&mut role_only, AuthMode::None).unwrap();
    assert!(role_only.rbac().identity_store().holds_only_seed());
    let role = IdentityOp::Access(AccessOp::UpsertRole {
        request: RoleUpsert {
            role_id: "reports".to_string(),
            name: "reports".to_string(),
            description: None,
            scopes: BTreeSet::from(["kg:read".to_string()]),
            graph_grants: Vec::new(),
        },
    });
    apply(&mut role_only, role, &stamp(IDENTITY_ADMIN_SCOPE)).unwrap();
    assert!(!role_only.rbac().identity_store().holds_only_seed());
}

#[test]
fn an_administrator_repairs_the_system_identity_without_replacing_its_roles() {
    let mut layer = seeded();
    let mut graph_os = agent("svc:graph-os");
    graph_os.roles = vec!["ops".to_string()];
    layer.agents.insert("svc:graph-os".to_string(), graph_os);
    for refused in [IDENTITY_READ_SCOPE, IDENTITY_AUTHENTICATE_SCOPE, "kg:admin"] {
        assert_eq!(
            apply(&mut layer, repair("svc:graph-os"), &stamp(refused)),
            Err(IdentityStoreError::Refused(IdentityRefusal::NotAuthorized)),
            "{refused}"
        );
    }
    let mut delegated = stamp(IDENTITY_ADMIN_SCOPE);
    delegated.actor.delegated = true;
    assert_eq!(
        apply(&mut layer, repair("svc:graph-os"), &delegated),
        Err(IdentityStoreError::Refused(IdentityRefusal::NotAuthorized))
    );
    assert_eq!(
        apply(
            &mut layer,
            repair("usr:alice"),
            &stamp(IDENTITY_ADMIN_SCOPE)
        ),
        Err(IdentityStoreError::Refused(IdentityRefusal::KindMismatch)),
        "a store principal is never the System identity"
    );
    assert_eq!(layer.agents["svc:graph-os"].role, AgentRole::Agent);
    apply(
        &mut layer,
        repair("svc:graph-os"),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    let repaired: &AgentIdentity = &layer.agents["svc:graph-os"];
    assert_eq!(repaired.role, AgentRole::System);
    assert_eq!(repaired.roles, ["ops"], "unrelated roles are kept");
    apply(
        &mut layer,
        repair("svc:new-root"),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    assert_eq!(layer.agents["svc:new-root"].role, AgentRole::System);
    assert_eq!(layer.agents[SYSTEM_AGENT].role, AgentRole::System);
    let last = layer
        .rbac()
        .identity_store()
        .audit_trail()
        .entries()
        .last()
        .cloned();
    assert_eq!(
        last.map(|entry| entry.event),
        Some(IdentityEvent::SystemIdentityRepaired)
    );
}

#[test]
fn a_repair_before_the_bootstrap_is_refused() {
    let mut layer = IsolationLayer::new();
    initialize(&mut layer, AuthMode::None).unwrap();
    assert!(pending_refusal(apply(
        &mut layer,
        repair("svc:graph-os"),
        &stamp(IDENTITY_ADMIN_SCOPE)
    )));
    assert!(layer.identity_bootstrap_pending());
}

#[test]
fn the_durable_corruption_check_admits_store_state_but_never_a_foreign_identity() {
    let layer = seeded();
    let store = layer.rbac().identity_store();
    let store_owned: BTreeMap<_, _> = layer
        .agents
        .iter()
        .filter(|(principal, _)| store.manages(principal))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    assert!(
        super::layer_store::holds_only_identity_store_state(layer.rbac(), &store_owned),
        "store state before the System bootstrap is not corruption"
    );
    let foreign = BTreeMap::from([("agent:x".to_string(), agent("agent:x"))]);
    assert!(
        !super::layer_store::holds_only_identity_store_state(layer.rbac(), &foreign),
        "a non-store identity before bootstrap is still corruption"
    );
}
