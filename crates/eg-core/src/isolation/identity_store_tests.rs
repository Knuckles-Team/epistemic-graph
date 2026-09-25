//! IDM-01/IDM-03: the identity store over the RBAC image.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::{
    AccessLevel, AgentIdentity, AgentRole, AuditActor, IdentityStoreError, IsolationLayer,
};
use crate::acl::{GrantEffect, RbacAction, RbacAdminOp, ResourceSelector, Role};
use crate::protocol::GraphType;
use crate::rbac::RbacPolicy;
use crate::rbac_persist::{
    IdentityBootstrapState, RbacAuthoritySnapshot, RbacPersistError, RbacPolicyStore,
};
use eg_types::identity::*;

const NOW: u64 = 1_800_000_000_000;
const GRAPH: &str = "g-reports";

struct Registry;

impl ScopeClassifier for Registry {
    fn class_of(&self, scope: &str) -> Option<ScopeClass> {
        match scope {
            "kg:read" | "identity:self" | "governance:read" => Some(ScopeClass::User),
            "kg:admin" | "webui:admin" | "identity:admin" | "identity:read" => {
                Some(ScopeClass::Admin)
            }
            "rbac:approve-elevation"
            | "finance:approve-live-order"
            | "governance:approve-schema-repair" => Some(ScopeClass::Approver),
            _ => None,
        }
    }

    fn approver_group_of(&self, scope: &str) -> Option<&'static str> {
        match scope {
            "rbac:approve-elevation" => Some(ELEVATION_APPROVERS_GROUP),
            "finance:approve-live-order" => Some(LIVE_ORDER_APPROVERS_GROUP),
            "governance:approve-schema-repair" => Some(SCHEMA_APPROVERS_GROUP),
            _ => None,
        }
    }
}

fn stamp(scope: &str) -> IdentityStamp {
    IdentityStamp::for_actor(IdentityActor {
        principal_id: "usr:admin".to_string(),
        delegated: false,
        scopes: BTreeSet::from([scope.to_string()]),
    })
}

fn apply(
    layer: &mut IsolationLayer,
    op: IdentityOp,
    stamp: &IdentityStamp,
) -> Result<IdentityReply, IdentityStoreError> {
    layer.try_apply_identity(&op, stamp, NOW, &Registry)
}

/// A layer whose store is initialized (`none` mode) with `alice` holding a
/// role that may read [`GRAPH`].
fn seeded() -> IsolationLayer {
    let mut layer = IsolationLayer::new();
    let init = IdentityOp::Config(ConfigOp::Initialize {
        request: InitializeRequest {
            mode: AuthMode::None,
            admin_username: None,
            admin_password: Secret::default(),
        },
    });
    apply(&mut layer, init, &stamp(IDENTITY_AUTHENTICATE_SCOPE)).unwrap();
    let role = IdentityOp::Access(AccessOp::UpsertRole {
        request: RoleUpsert {
            role_id: "reports".to_string(),
            name: "reports".to_string(),
            description: None,
            scopes: BTreeSet::from(["kg:read".to_string()]),
            graph_grants: vec![RoleGraphGrant {
                resource: ResourceSelector::Graph(GRAPH.to_string()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            }],
        },
    });
    apply(&mut layer, role, &stamp(IDENTITY_ADMIN_SCOPE)).unwrap();
    let mut create = stamp(IDENTITY_ADMIN_SCOPE);
    create.minted_principal_id = Some("usr:alice".to_string());
    let user = IdentityOp::User(UserOp::Create {
        request: CreateUserRequest {
            username: "alice".to_string(),
            kind: UserKind::Human,
            principal_id: None,
            display_name: None,
            email: None,
            roles: BTreeSet::from(["reports".to_string()]),
            groups: BTreeSet::new(),
            password: Secret::default(),
            must_change: false,
        },
    });
    apply(&mut layer, user, &create).unwrap();
    layer
}

fn can_read(layer: &IsolationLayer) -> bool {
    layer.check_access(
        "usr:alice",
        GRAPH,
        GraphType::Agent,
        None,
        AccessLevel::Read,
    )
}

fn unbind(change: BindingChange) -> IdentityOp {
    IdentityOp::Access(AccessOp::ChangeUserRole {
        request: UserRoleChange {
            principal_id: "usr:alice".to_string(),
            role_id: "reports".to_string(),
            change,
        },
    })
}

#[test]
fn a_store_role_reaches_the_access_chokepoint_and_its_removal_is_atomic() {
    let mut layer = seeded();
    assert!(
        can_read(&layer),
        "the projected idm:reports grant allows the read"
    );
    let identity = layer.get_identity("usr:alice").unwrap();
    assert!(identity.roles.contains(&"idm:reports".to_string()));
    apply(
        &mut layer,
        unbind(BindingChange::Remove),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    assert!(
        !can_read(&layer),
        "the RBAC role went with the store binding"
    );
    apply(
        &mut layer,
        unbind(BindingChange::Add),
        &stamp(IDENTITY_ADMIN_SCOPE),
    )
    .unwrap();
    assert!(can_read(&layer));
}

#[test]
fn a_refused_identity_op_changes_neither_the_store_nor_rbac() {
    let mut layer = seeded();
    let before = serde_json::to_string(layer.rbac()).unwrap();
    let refused = apply(
        &mut layer,
        unbind(BindingChange::Remove),
        &stamp(IDENTITY_READ_SCOPE),
    );
    assert_eq!(
        refused,
        Err(IdentityStoreError::Refused(IdentityRefusal::NotAuthorized))
    );
    assert_eq!(serde_json::to_string(layer.rbac()).unwrap(), before);
    assert!(can_read(&layer));
}

#[test]
fn disabling_a_principal_removes_its_rbac_identity() {
    let mut layer = seeded();
    let disable = IdentityOp::User(UserOp::SetStatus {
        request: UserStatusChange {
            principal_id: "usr:alice".to_string(),
            status: UserStatus::Disabled,
        },
    });
    apply(&mut layer, disable, &stamp(IDENTITY_ADMIN_SCOPE)).unwrap();
    assert!(layer.get_identity("usr:alice").is_none());
    assert!(!can_read(&layer));
}

fn actor() -> AuditActor<'static> {
    AuditActor {
        principal: "svc:au",
        now_ms: NOW,
    }
}

fn agent(id: &str) -> AgentIdentity {
    AgentIdentity {
        agent_id: id.to_string(),
        role: AgentRole::Agent,
        teams: Vec::new(),
        roles: Vec::new(),
    }
}

#[test]
fn register_identity_cannot_overwrite_a_store_principal_and_is_audited_otherwise() {
    let mut layer = seeded();
    assert_eq!(
        layer.try_register_agent_audited(agent("usr:alice"), actor()),
        Err(super::STORE_MANAGED.to_string())
    );
    assert!(can_read(&layer), "the store's identity is untouched");
    let mut forged = agent("agent:x");
    forged.roles = vec!["idm:reports".to_string()];
    assert_eq!(
        layer.try_register_agent_audited(forged, actor()),
        Err(super::STORE_NAMESPACE.to_string())
    );
    let audited_before = layer
        .rbac()
        .identity_store()
        .audit_trail()
        .entries()
        .count();
    layer
        .try_register_agent_audited(agent("agent:planner"), actor())
        .unwrap();
    let trail = layer.rbac().identity_store().audit_trail();
    assert_eq!(trail.entries().count(), audited_before + 1);
    let last = trail.entries().last().unwrap();
    assert_eq!(last.event, IdentityEvent::RbacIdentityRegistered);
    assert_eq!(last.target.as_deref(), Some("agent:planner"));
    assert!(trail.verify().is_ok());
}

#[test]
fn rbac_admin_cannot_touch_the_store_namespace_and_is_audited_otherwise() {
    let mut layer = seeded();
    let refused =
        layer.try_rbac_admin_audited(RbacAdminOp::RemoveRole("idm:reports".to_string()), actor());
    assert_eq!(refused, Err(super::STORE_NAMESPACE.to_string()));
    assert!(can_read(&layer));
    layer
        .try_rbac_admin_audited(RbacAdminOp::AddRole(Role::new("ops")), actor())
        .unwrap();
    let last = layer
        .rbac()
        .identity_store()
        .audit_trail()
        .entries()
        .last()
        .cloned()
        .unwrap();
    assert_eq!(last.event, IdentityEvent::RbacPolicyChanged);
    assert_eq!(last.target.as_deref(), Some("ops"));
}

struct FailingStore;

impl RbacPolicyStore for FailingStore {
    fn load(
        &self,
    ) -> Result<
        (
            RbacPolicy,
            BTreeMap<String, AgentIdentity>,
            IdentityBootstrapState,
        ),
        RbacPersistError,
    > {
        Err(RbacPersistError::IncompleteState("test store"))
    }

    fn save(
        &self,
        _policy: &RbacPolicy,
        _identities: &BTreeMap<String, AgentIdentity>,
        _bootstrap: IdentityBootstrapState,
    ) -> Result<(), RbacPersistError> {
        Err(RbacPersistError::IncompleteState("disk full"))
    }

    fn authority_snapshot(&self) -> Result<RbacAuthoritySnapshot, RbacPersistError> {
        Err(RbacPersistError::IncompleteState("test store"))
    }
}

#[test]
fn a_failed_write_rolls_back_the_store_and_the_projection_together() {
    let mut layer = seeded();
    let before = serde_json::to_string(layer.rbac()).unwrap();
    layer.persist = Some(Arc::new(FailingStore));
    let outcome = apply(
        &mut layer,
        unbind(BindingChange::Remove),
        &stamp(IDENTITY_ADMIN_SCOPE),
    );
    assert!(matches!(outcome, Err(IdentityStoreError::Persist(_))));
    assert_eq!(serde_json::to_string(layer.rbac()).unwrap(), before);
    assert!(can_read(&layer), "neither half of the change was kept");
    let audited = layer.try_rbac_admin_audited(RbacAdminOp::AddRole(Role::new("ops")), actor());
    assert!(audited.is_err());
    assert_eq!(
        serde_json::to_string(layer.rbac()).unwrap(),
        before,
        "nor its audit entry"
    );
}

#[test]
fn a_denial_sample_lands_in_the_trail() {
    let mut layer = seeded();
    let sample = DenialSample {
        at_ms: NOW,
        principal: "usr:mallory".to_string(),
        action: "kg:write".to_string(),
        reason: "ACCESS_DENIED".to_string(),
    };
    layer.try_record_denials(vec![sample], 2).unwrap();
    let entries: Vec<_> = layer
        .rbac()
        .identity_store()
        .audit_trail()
        .entries()
        .cloned()
        .collect();
    let denied: Vec<_> = entries
        .iter()
        .filter(|entry| entry.event == IdentityEvent::AccessDenied)
        .collect();
    assert_eq!(denied.len(), 2);
    assert_eq!(denied[1].detail, "dropped=2");
}

#[test]
fn initializing_the_store_leaves_the_system_bootstrap_open() {
    let layer = seeded();
    assert!(
        layer.identity_bootstrap_pending(),
        "the engine's System identity still registers through its own bootstrap"
    );
    let mut foreign = BTreeMap::new();
    foreign.insert("agent:x".to_string(), agent("agent:x"));
    assert!(super::layer_store::holds_only_identity_store_state(
        layer.rbac(),
        &layer
            .agents
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    ));
    assert!(
        !super::layer_store::holds_only_identity_store_state(layer.rbac(), &foreign),
        "a non-store identity before bootstrap is still corruption"
    );
}
