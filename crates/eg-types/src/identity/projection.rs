//! Effective authority of a principal, and its projection into the RBAC
//! image.
//!
//! effective roles  = direct roles ∪ group roles ∪ IdP-mapped roles
//! effective scopes = ⋃ effective roles → role scopes, filtered by the registry
//!
//! The projection is the FULL RBAC identity of every principal the store owns
//! (never a partial `RegisterIdentity`), written in the same durable write as
//! the store change that produced it.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::model::{UserKind, UserStatus};
use super::scope::ScopeClassifier;
use super::store::IdentityStore;
use super::{IdentityRefusal, RBAC_ROLE_PREFIX};
use crate::acl::{AgentIdentity, AgentRole, Grant, Role};

/// What the local issuer puts in a principal's token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PrincipalResolution {
    pub principal_id: String,
    pub username: String,
    pub kind: UserKind,
    pub status: UserStatus,
    pub is_bootstrap: bool,
    pub roles: BTreeSet<String>,
    pub groups: BTreeSet<String>,
    /// Registered scopes only; an unregistered scope on a role is dropped.
    pub scopes: BTreeSet<String>,
    /// A group this principal belongs to requires a second factor.
    pub mfa_required: bool,
    pub mfa_enrolled: bool,
    /// Set by `resolve_session`: the session still owes its second factor.
    #[serde(default)]
    pub session_mfa_pending: bool,
}

/// The RBAC state the store owns.
#[derive(Debug, Clone, Default)]
pub struct RbacProjection {
    /// Every `idm:<role_id>` role.
    pub roles: Vec<Role>,
    /// Every grant of those roles.
    pub grants: Vec<Grant>,
    /// The identity of every ACTIVE principal (inactive ones hold none).
    pub identities: BTreeMap<String, AgentIdentity>,
    /// Every principal the store owns, active or not: their RBAC identities
    /// are replaced (or removed) wholesale by the projection.
    pub managed: BTreeSet<String>,
}

/// The RBAC role name of store role `role_id`.
pub fn rbac_role_name(role_id: &str) -> String {
    format!("{RBAC_ROLE_PREFIX}{role_id}")
}

impl IdentityStore {
    /// Roles and groups reaching `principal_id`, without scopes.
    pub(crate) fn effective_roles(
        &self,
        principal_id: &str,
    ) -> (BTreeSet<String>, BTreeSet<String>) {
        let mut roles: BTreeSet<String> = self
            .users
            .get(principal_id)
            .map(|user| user.roles.clone())
            .unwrap_or_default();
        let mut groups = BTreeSet::new();
        for group in self.groups.values() {
            if group.members.contains_key(principal_id) {
                groups.insert(group.group_id.clone());
                roles.extend(group.roles.iter().cloned());
            }
        }
        for (key, mapped) in &self.link_roles {
            let owned = self
                .links
                .get(key)
                .is_some_and(|link| link.principal_id == principal_id);
            if owned {
                roles.extend(mapped.iter().cloned());
            }
        }
        (roles, groups)
    }

    /// The registered scopes `roles` carry.
    pub(crate) fn scopes_of(
        &self,
        roles: &BTreeSet<String>,
        classifier: &dyn ScopeClassifier,
    ) -> BTreeSet<String> {
        roles
            .iter()
            .filter_map(|role| self.roles.get(role))
            .flat_map(|role| role.scopes.iter())
            .filter(|scope| classifier.class_of(scope).is_some())
            .cloned()
            .collect()
    }

    /// A principal's full resolution.
    pub fn resolve(
        &self,
        principal_id: &str,
        classifier: &dyn ScopeClassifier,
    ) -> Result<PrincipalResolution, IdentityRefusal> {
        let user = self
            .users
            .get(principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        let (roles, groups) = self.effective_roles(principal_id);
        let scopes = self.scopes_of(&roles, classifier);
        let mfa_required = groups
            .iter()
            .filter_map(|group| self.groups.get(group))
            .any(|group| group.mfa_required);
        Ok(PrincipalResolution {
            principal_id: principal_id.to_string(),
            username: user.username.clone(),
            kind: user.kind,
            status: user.status,
            is_bootstrap: user.is_bootstrap,
            roles,
            groups,
            scopes,
            mfa_required,
            mfa_enrolled: self.mfa_enrolled(principal_id),
            session_mfa_pending: false,
        })
    }

    /// Whether `principal_id` has a confirmed second factor (a confirmed
    /// TOTP factor or any WebAuthn credential).
    pub(crate) fn mfa_enrolled(&self, principal_id: &str) -> bool {
        self.totp_confirmed(principal_id) || self.has_webauthn(principal_id)
    }

    /// Whether `principal_id` has a confirmed TOTP factor.
    pub(crate) fn totp_confirmed(&self, principal_id: &str) -> bool {
        self.totp
            .get(principal_id)
            .is_some_and(|record| record.confirmed_at_ms.is_some())
    }

    /// The RBAC state this store owns, computed from scratch.
    pub fn rbac_projection(&self) -> RbacProjection {
        let mut projection = RbacProjection::default();
        for role in self.roles.values() {
            let name = rbac_role_name(&role.role_id);
            projection.roles.push(Role::new(name.clone()));
            projection
                .grants
                .extend(role.graph_grants.iter().map(|grant| Grant {
                    role: name.clone(),
                    resource: grant.resource.clone(),
                    action: grant.action,
                    effect: grant.effect,
                }));
        }
        for user in self.users.values() {
            projection.managed.insert(user.principal_id.clone());
            if !user.status.is_active() {
                continue;
            }
            let (roles, _) = self.effective_roles(&user.principal_id);
            projection.identities.insert(
                user.principal_id.clone(),
                AgentIdentity {
                    agent_id: user.principal_id.clone(),
                    role: AgentRole::Agent,
                    teams: Vec::new(),
                    roles: roles.iter().map(|role| rbac_role_name(role)).collect(),
                },
            );
        }
        projection
    }
}
