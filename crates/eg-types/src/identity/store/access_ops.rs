//! Roles, groups, bindings, identity providers and subject links.

use super::super::access::{GroupRecord, IdpConfig, IdpKind, MappingRule, RoleRecord};
use super::super::audit::IdentityEvent;
use super::super::model::ExternalIdentity;
use super::super::ops::{AccessOp, IdpOp};
use super::super::requests::LinkRequest;
use super::super::requests_admin::{
    BindingChange, GroupMembershipChange, GroupUpsert, ObjectRef, RoleUpsert, UserRoleChange,
};
use super::super::scope::ScopeClass;
use super::super::stamp::IdentityStamp;
use super::super::text::{bounded, bounded_opt, identifier, MAX_NAME_BYTES, MAX_TEXT_BYTES};
use super::super::views::IdentityReply;
use super::super::{
    IdentityRefusal, ADMINISTRATORS_GROUP, MAX_GROUPS, MAX_IDPS, MAX_MAPPING_RULES, MAX_ROLES,
};
use super::{link_key, ApplyContext, IdentityStore};

/// Rule match kinds the store evaluates. `regex` is refused: the engine
/// evaluates mapping rules deterministically with no pattern engine.
const MATCH_KINDS: [&str; 2] = ["equals", "prefix"];

impl IdentityStore {
    pub(super) fn apply_access(
        &mut self,
        op: &AccessOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let now_ms = ctx.now_ms;
        let reply = match op {
            AccessOp::UpsertRole { request } => self.upsert_role(request)?,
            AccessOp::RemoveRole { request } => self.remove_role(request)?,
            AccessOp::UpsertGroup { request } => self.upsert_group(request)?,
            AccessOp::RemoveGroup { request } => self.remove_group(request)?,
            AccessOp::ChangeMembership { request } => self.change_membership(request)?,
            AccessOp::ChangeUserRole { request } => self.change_user_role(request)?,
            AccessOp::ListRoles => {
                return Ok(IdentityReply::Roles(self.roles.values().cloned().collect()))
            }
            AccessOp::ListGroups => {
                return Ok(IdentityReply::Groups(
                    self.groups.values().cloned().collect(),
                ))
            }
        };
        self.audit_event(stamp, now_ms, access_event(op), None);
        Ok(reply)
    }

    fn upsert_role(&mut self, request: &RoleUpsert) -> Result<IdentityReply, IdentityRefusal> {
        identifier(&request.role_id)?;
        bounded(&request.name, MAX_NAME_BYTES)?;
        bounded_opt(request.description.as_deref(), MAX_TEXT_BYTES)?;
        let builtin = self
            .roles
            .get(&request.role_id)
            .is_some_and(|role| role.builtin);
        if !self.roles.contains_key(&request.role_id) && self.roles.len() >= MAX_ROLES {
            return Err(IdentityRefusal::Full);
        }
        self.roles.insert(
            request.role_id.clone(),
            RoleRecord {
                role_id: request.role_id.clone(),
                name: request.name.clone(),
                description: request.description.clone(),
                builtin,
                scopes: request.scopes.clone(),
                graph_grants: request.graph_grants.clone(),
            },
        );
        Ok(IdentityReply::Done { changed: true })
    }

    /// Remove a role and every binding of it. Built-ins stay.
    fn remove_role(&mut self, request: &ObjectRef) -> Result<IdentityReply, IdentityRefusal> {
        let role = self
            .roles
            .get(&request.id)
            .ok_or(IdentityRefusal::NotFound)?;
        if role.builtin {
            return Err(IdentityRefusal::BuiltIn);
        }
        self.roles.remove(&request.id);
        for user in self.users.values_mut() {
            user.roles.remove(&request.id);
        }
        for group in self.groups.values_mut() {
            group.roles.remove(&request.id);
        }
        for roles in self.link_roles.values_mut() {
            roles.remove(&request.id);
        }
        Ok(IdentityReply::Done { changed: true })
    }

    fn upsert_group(&mut self, request: &GroupUpsert) -> Result<IdentityReply, IdentityRefusal> {
        identifier(&request.group_id)?;
        bounded(&request.name, MAX_NAME_BYTES)?;
        if request
            .roles
            .iter()
            .any(|role| !self.roles.contains_key(role))
        {
            return Err(IdentityRefusal::NotFound);
        }
        let existing = self.groups.get(&request.group_id);
        if existing.is_none() && self.groups.len() >= MAX_GROUPS {
            return Err(IdentityRefusal::Full);
        }
        let builtin = existing.is_some_and(|group| group.builtin);
        let changes_builtin_roles =
            builtin && existing.is_some_and(|group| group.roles != request.roles);
        if changes_builtin_roles {
            return Err(IdentityRefusal::BuiltIn);
        }
        let group = GroupRecord {
            group_id: request.group_id.clone(),
            name: request.name.clone(),
            source: existing.map_or_else(|| "local".to_string(), |group| group.source.clone()),
            builtin,
            members: existing
                .map(|group| group.members.clone())
                .unwrap_or_default(),
            roles: request.roles.clone(),
            mfa_required: request.mfa_required,
        };
        self.groups.insert(request.group_id.clone(), group);
        Ok(IdentityReply::Done { changed: true })
    }

    fn remove_group(&mut self, request: &ObjectRef) -> Result<IdentityReply, IdentityRefusal> {
        let group = self
            .groups
            .get(&request.id)
            .ok_or(IdentityRefusal::NotFound)?;
        if group.builtin {
            return Err(IdentityRefusal::BuiltIn);
        }
        self.groups.remove(&request.id);
        Ok(IdentityReply::Done { changed: true })
    }

    fn change_membership(
        &mut self,
        request: &GroupMembershipChange,
    ) -> Result<IdentityReply, IdentityRefusal> {
        if !self.users.contains_key(&request.principal_id) {
            return Err(IdentityRefusal::NotFound);
        }
        let removing_last_admin = request.group_id == ADMINISTRATORS_GROUP
            && request.change == BindingChange::Remove
            && self.is_last_active_admin(&request.principal_id);
        if removing_last_admin {
            return Err(IdentityRefusal::PreconditionFailed);
        }
        let group = self
            .groups
            .get_mut(&request.group_id)
            .ok_or(IdentityRefusal::NotFound)?;
        let changed = match request.change {
            BindingChange::Add => group
                .members
                .insert(request.principal_id.clone(), "local".to_string())
                .is_none(),
            BindingChange::Remove => group.members.remove(&request.principal_id).is_some(),
        };
        Ok(IdentityReply::Done { changed })
    }

    fn change_user_role(
        &mut self,
        request: &UserRoleChange,
    ) -> Result<IdentityReply, IdentityRefusal> {
        if !self.roles.contains_key(&request.role_id) {
            return Err(IdentityRefusal::NotFound);
        }
        let user = self
            .users
            .get_mut(&request.principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        let changed = match request.change {
            BindingChange::Add => user.roles.insert(request.role_id.clone()),
            BindingChange::Remove => user.roles.remove(&request.role_id),
        };
        Ok(IdentityReply::Done { changed })
    }

    pub(super) fn apply_idp(
        &mut self,
        op: &IdpOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        match op {
            IdpOp::Upsert { .. }
            | IdpOp::Remove { .. }
            | IdpOp::Link { .. }
            | IdpOp::Unlink { .. }
            | IdpOp::List
            | IdpOp::UpsertScimClient { .. }
            | IdpOp::GetScimClient { .. }
            | IdpOp::ListScimClients
            | IdpOp::RemoveScimClient { .. } => self.apply_idp_admin(op, stamp, ctx),
            IdpOp::Provision { .. }
            | IdpOp::ListProvisioned { .. }
            | IdpOp::ProvisionGroup { .. }
            | IdpOp::RemoveDirectoryGroup { .. }
            | IdpOp::ListDirectoryGroups { .. } => self.apply_provisioning(op, stamp, ctx),
        }
    }

    /// IdP configuration and administrator-made links.
    fn apply_idp_admin(
        &mut self,
        op: &IdpOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let now_ms = ctx.now_ms;
        match op {
            IdpOp::Upsert { request } => {
                self.upsert_idp(request, ctx)?;
                self.audit_event(
                    stamp,
                    now_ms,
                    IdentityEvent::IdpChanged,
                    Some(&request.idp_id),
                );
                Ok(IdentityReply::Done { changed: true })
            }
            IdpOp::Remove { request } => {
                self.idps
                    .remove(&request.id)
                    .ok_or(IdentityRefusal::NotFound)?;
                self.forget_idp_directory(&request.id);
                self.audit_event(stamp, now_ms, IdentityEvent::IdpChanged, Some(&request.id));
                Ok(IdentityReply::Done { changed: true })
            }
            IdpOp::Link { request } => {
                self.link(
                    &request.idp_id,
                    &request.subject,
                    &request.principal_id,
                    stamp,
                    now_ms,
                )?;
                Ok(IdentityReply::Done { changed: true })
            }
            IdpOp::Unlink { request } => self.unlink(request, stamp, now_ms),
            IdpOp::List => Ok(IdentityReply::Idps(self.idps.values().cloned().collect())),
            IdpOp::UpsertScimClient { request } => self.upsert_scim_client(request, stamp, now_ms),
            IdpOp::GetScimClient { request } => {
                Ok(IdentityReply::ScimClient(self.scim_client(&request.id)?))
            }
            IdpOp::ListScimClients => Ok(IdentityReply::ScimClients(self.scim_clients())),
            IdpOp::RemoveScimClient { request } => {
                self.remove_scim_client(&request.id, stamp, now_ms)
            }
            IdpOp::Provision { .. }
            | IdpOp::ListProvisioned { .. }
            | IdpOp::ProvisionGroup { .. }
            | IdpOp::RemoveDirectoryGroup { .. }
            | IdpOp::ListDirectoryGroups { .. } => Err(IdentityRefusal::InvalidRequest),
        }
    }

    fn upsert_idp(
        &mut self,
        idp: &IdpConfig,
        ctx: &ApplyContext<'_>,
    ) -> Result<(), IdentityRefusal> {
        identifier(&idp.idp_id)?;
        bounded(&idp.display_name, MAX_NAME_BYTES)?;
        bounded(&idp.config_json, MAX_TEXT_BYTES)?;
        let object = serde_json::from_str::<serde_json::Value>(&idp.config_json)
            .map_err(|_| IdentityRefusal::InvalidRequest)?;
        if !object.is_object() || idp.rules.len() > MAX_MAPPING_RULES {
            return Err(IdentityRefusal::InvalidRequest);
        }
        if !self.idps.contains_key(&idp.idp_id) && self.idps.len() >= MAX_IDPS {
            return Err(IdentityRefusal::Full);
        }
        for rule in &idp.rules {
            self.validate_rule(rule, ctx)?;
        }
        self.idps.insert(idp.idp_id.clone(), idp.clone());
        Ok(())
    }

    /// A rule names an existing role or group, uses a supported match, and
    /// is marked `privileged` whenever its target reaches an approver- or
    /// administrator-class scope.
    fn validate_rule(
        &self,
        rule: &MappingRule,
        ctx: &ApplyContext<'_>,
    ) -> Result<(), IdentityRefusal> {
        identifier(&rule.rule_id)?;
        bounded(&rule.claim_path, MAX_NAME_BYTES)?;
        bounded(&rule.value, MAX_NAME_BYTES)?;
        if !MATCH_KINDS.contains(&rule.match_kind.as_str()) {
            return Err(IdentityRefusal::InvalidRequest);
        }
        let roles = self.target_roles(&rule.target)?;
        let privileged_target = self
            .scopes_of(&roles, ctx.classifier)
            .iter()
            .filter_map(|scope| ctx.classifier.class_of(scope))
            .any(|class| matches!(class, ScopeClass::Approver | ScopeClass::Admin));
        if privileged_target && !rule.privileged {
            return Err(IdentityRefusal::ClassViolation);
        }
        Ok(())
    }

    /// The roles a rule target reaches: `role:<id>` or `group:<id>`.
    pub(crate) fn target_roles(
        &self,
        target: &str,
    ) -> Result<std::collections::BTreeSet<String>, IdentityRefusal> {
        match target.split_once(':') {
            Some(("role", id)) if self.roles.contains_key(id) => Ok([id.to_string()].into()),
            Some(("group", id)) => self
                .groups
                .get(id)
                .map(|group| group.roles.clone())
                .ok_or(IdentityRefusal::NotFound),
            Some(_) | None => Err(IdentityRefusal::NotFound),
        }
    }

    /// Link `(idp_id, subject)` to an existing principal.
    pub(crate) fn link(
        &mut self,
        idp_id: &str,
        subject: &str,
        principal_id: &str,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<(), IdentityRefusal> {
        bounded(subject, MAX_NAME_BYTES)?;
        let idp = self.idps.get(idp_id).ok_or(IdentityRefusal::NotFound)?;
        if idp.kind == IdpKind::Scim || !self.users.contains_key(principal_id) {
            return Err(IdentityRefusal::InvalidRequest);
        }
        let key = link_key(idp_id, subject);
        if self.links.contains_key(&key) {
            return Err(IdentityRefusal::Collision);
        }
        self.links.insert(
            key,
            ExternalIdentity {
                idp_id: idp_id.to_string(),
                subject: subject.to_string(),
                principal_id: principal_id.to_string(),
                linked_at_ms: now_ms,
                linked_by: stamp.actor.principal_id.clone(),
            },
        );
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::IdentityLinked,
            Some(principal_id),
        );
        Ok(())
    }

    fn unlink(
        &mut self,
        request: &LinkRequest,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let key = link_key(&request.idp_id, &request.subject);
        let link = self.links.remove(&key).ok_or(IdentityRefusal::NotFound)?;
        self.link_roles.remove(&key);
        self.forget_link(&link);
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::IdentityUnlinked,
            Some(&link.principal_id),
        );
        Ok(IdentityReply::Done { changed: true })
    }
}

fn access_event(op: &AccessOp) -> IdentityEvent {
    match op {
        AccessOp::UpsertRole { .. } | AccessOp::RemoveRole { .. } => IdentityEvent::RoleChanged,
        AccessOp::ChangeUserRole { .. } => IdentityEvent::RoleChanged,
        AccessOp::UpsertGroup { .. }
        | AccessOp::RemoveGroup { .. }
        | AccessOp::ChangeMembership { .. } => IdentityEvent::GroupChanged,
        AccessOp::ListRoles | AccessOp::ListGroups => IdentityEvent::RoleChanged,
    }
}
