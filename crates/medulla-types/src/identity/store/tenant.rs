//! Engine-created tenant grants for principals owned by the identity store.
//! These use ordinary store roles, so identity role/binding revocation remains
//! authoritative. No roles are imported from the projected RBAC identity.

use sha2::{Digest, Sha256};

use super::super::access::RoleRecord;
use super::super::requests_admin::RoleGraphGrant;
use super::super::{IdentityRefusal, MAX_ROLES};
use super::IdentityStore;
use crate::acl::{GrantEffect, RbacAction, ResourceSelector};

impl IdentityStore {
    /// Bind an active managed creator to the tenant role after graph creation.
    /// The caller must establish the tenant from the engine's graph naming
    /// contract. Existing roles must have exactly the engine-issued authority;
    /// a name collision never imports additional scopes or graph permissions.
    ///
    /// Revoke through identity role/grant or user-binding operations. A later
    /// identity update never recreates a removed grant or binding.
    pub fn provision_tenant_graph_access(
        &mut self,
        principal: &str,
        tenant: &str,
    ) -> Result<(), IdentityRefusal> {
        if !self.is_active(principal) {
            return Err(IdentityRefusal::NotAuthorized);
        }
        let role_id = tenant_role_id(tenant);
        let grants: Vec<_> = [RbacAction::Read, RbacAction::Write]
            .into_iter()
            .map(|action| RoleGraphGrant {
                resource: ResourceSelector::Pattern(format!("tenant__{tenant}__*")),
                action,
                effect: GrantEffect::Allow,
            })
            .collect();
        if let Some(role) = self.roles.get(&role_id) {
            if role.builtin || !role.scopes.is_empty() || role.graph_grants != grants {
                return Err(IdentityRefusal::Collision);
            }
        } else {
            if self.roles.len() >= MAX_ROLES {
                return Err(IdentityRefusal::Full);
            }
            self.roles.insert(
                role_id.clone(),
                RoleRecord {
                    role_id: role_id.clone(),
                    name: role_id.clone(),
                    description: None,
                    builtin: false,
                    scopes: Default::default(),
                    graph_grants: grants,
                },
            );
        }
        self.users
            .get_mut(principal)
            .ok_or(IdentityRefusal::NotFound)?
            .roles
            .insert(role_id);
        Ok(())
    }
}

// Keep ordinary role IDs stable. The disjoint fallback namespace preserves exact
// tenant bytes without imposing identity identifier limits on graph names.
fn tenant_role_id(tenant: &str) -> String {
    let role_id = format!("tenant:{tenant}");
    if super::super::text::identifier(&role_id).is_ok() {
        role_id
    } else {
        format!("tenant-sha256:{:x}", Sha256::digest(tenant.as_bytes()))
    }
}
