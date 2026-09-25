//! Directory provisioning (IDM-14 store side): SCIM `Users` / `Groups` and
//! the LDAP group sync.
//!
//! Every op is bound to ONE identity provider: a principal holding
//! `identity:provision` may provision only the `kind=scim` IdP whose
//! `config_json.provisioner` names it; the broker (`identity:authenticate`,
//! the LDAP client) only `kind=ldap` IdPs. Anything else, including a
//! missing or disabled IdP, is `not_authorized`.
//!
//! A provisioned subject's IdP bindings are recomputed exactly as an external
//! sign-in recomputes them, over the provisioned claims plus a `groups` claim
//! naming the IdP's directory groups that contain the principal. A directory
//! group grants nothing by itself.

use std::collections::{BTreeMap, BTreeSet};

use super::super::access::{IdpConfig, IdpKind};
use super::super::audit::IdentityEvent;
use super::super::model::{ExternalIdentity, UserKind, UserRecord, UserStatus};
use super::super::ops::{IdpOp, IDENTITY_AUTHENTICATE_SCOPE, IDENTITY_PROVISION_SCOPE};
use super::super::requests_admin::MAX_PAGE;
use super::super::requests_provision::{
    DirectoryGroup, DirectoryGroupQuery, DirectoryGroupRef, ProvisionSubject, ProvisionedQuery,
    ProvisionedUser,
};
use super::super::stamp::{IdentityActor, IdentityStamp};
use super::super::text::{bounded, bounded_opt, email, MAX_NAME_BYTES};
use super::super::views::IdentityReply;
use super::super::{
    normalize_username, IdentityRefusal, MAX_DIRECTORY_GROUPS, MAX_PROVISIONED_CLAIM_BYTES,
    MAX_USERS, USER_ROLE,
};
use super::{link_key, ApplyContext, IdentityStore};

type Claims = BTreeMap<String, Vec<String>>;

/// The claim path directory-group display names are offered under.
const GROUPS_CLAIM: &str = "groups";

/// The principal a `kind=scim` IdP's configuration names as its provisioner.
fn provisioner_of(idp: &IdpConfig) -> Option<String> {
    let config: serde_json::Value = serde_json::from_str(&idp.config_json).ok()?;
    config.get("provisioner")?.as_str().map(str::to_string)
}

/// Whether `actor` may provision `idp` (see the module docs).
fn may_provision(idp: &IdpConfig, actor: &IdentityActor) -> bool {
    let bound = match idp.kind {
        IdpKind::Scim => {
            actor.holds(IDENTITY_PROVISION_SCOPE)
                && provisioner_of(idp).as_deref() == Some(actor.principal_id.as_str())
        }
        IdpKind::Ldap => actor.holds(IDENTITY_AUTHENTICATE_SCOPE),
        IdpKind::Oidc | IdpKind::Saml => false,
    };
    idp.enabled && bound
}

/// `<kind>:<idp_id>`: the source of a directory-managed principal.
fn directory_source(idp: &IdpConfig) -> String {
    let kind = match idp.kind {
        IdpKind::Scim => "scim",
        IdpKind::Ldap => "ldap",
        IdpKind::Oidc => "oidc",
        IdpKind::Saml => "saml",
    };
    format!("{kind}:{}", idp.idp_id)
}

fn claims_bytes(claims: &Claims) -> usize {
    claims
        .iter()
        .map(|(path, values)| path.len() + values.iter().map(String::len).sum::<usize>())
        .sum()
}

/// Validate a subject's profile fields; answers the normalized username.
fn checked_profile(request: &ProvisionSubject) -> Result<String, IdentityRefusal> {
    bounded(&request.subject, MAX_NAME_BYTES)?;
    bounded_opt(request.display_name.as_deref(), MAX_NAME_BYTES)?;
    request.email.as_deref().map(email).transpose()?;
    if claims_bytes(&request.claims) > MAX_PROVISIONED_CLAIM_BYTES {
        return Err(IdentityRefusal::InvalidRequest);
    }
    normalize_username(&request.username)
}

fn checked_group(request: &DirectoryGroup) -> Result<(), IdentityRefusal> {
    bounded(&request.group_id, MAX_NAME_BYTES)?;
    bounded(&request.display_name, MAX_NAME_BYTES)?;
    bounded_opt(request.external_id.as_deref(), MAX_NAME_BYTES)?;
    if request.members.len() > MAX_USERS {
        return Err(IdentityRefusal::Full);
    }
    Ok(())
}

fn group_matches(group: &DirectoryGroup, query: &DirectoryGroupQuery) -> bool {
    let eq = |filter: &Option<String>, value: Option<&str>| {
        filter.as_deref().is_none_or(|wanted| Some(wanted) == value)
    };
    group.idp_id == query.idp_id
        && eq(&query.group_id, Some(group.group_id.as_str()))
        && eq(&query.display_name, Some(group.display_name.as_str()))
        && eq(&query.external_id, group.external_id.as_deref())
        && query
            .after
            .as_deref()
            .is_none_or(|after| group.group_id.as_str() > after)
}

impl IdentityStore {
    /// The provisioning ops of the IdP family (routed here by `apply_idp`).
    pub(super) fn apply_provisioning(
        &mut self,
        op: &IdpOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let now_ms = ctx.now_ms;
        match op {
            IdpOp::Provision { request } => self.provision(request, stamp, now_ms),
            IdpOp::ListProvisioned { request } => self.list_provisioned(request, stamp),
            IdpOp::ProvisionGroup { request } => self.provision_group(request, stamp, now_ms),
            IdpOp::RemoveDirectoryGroup { request } => {
                self.remove_directory_group(request, stamp, now_ms)
            }
            IdpOp::ListDirectoryGroups { request } => self.list_directory_groups(request, stamp),
            IdpOp::Upsert { .. }
            | IdpOp::Remove { .. }
            | IdpOp::Link { .. }
            | IdpOp::Unlink { .. }
            | IdpOp::List => Err(IdentityRefusal::InvalidRequest),
        }
    }

    /// The IdP `stamp`'s actor may provision, or `NotAuthorized`.
    fn provisioning_idp(
        &self,
        idp_id: &str,
        stamp: &IdentityStamp,
    ) -> Result<IdpConfig, IdentityRefusal> {
        self.idps
            .get(idp_id)
            .filter(|idp| may_provision(idp, &stamp.actor))
            .cloned()
            .ok_or(IdentityRefusal::NotAuthorized)
    }

    fn provision(
        &mut self,
        request: &ProvisionSubject,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let idp = self.provisioning_idp(&request.idp_id, stamp)?;
        let username = checked_profile(request)?;
        let key = link_key(&idp.idp_id, &request.subject);
        let linked = self.links.get(&key).map(|link| link.principal_id.clone());
        let principal = match linked {
            Some(principal) => {
                self.update_directory_profile(&idp, &principal, request, username)?;
                principal
            }
            None if request.active => {
                self.create_provisioned(&idp, request, username, stamp, now_ms)?
            }
            None => return Ok(IdentityReply::Done { changed: false }),
        };
        self.apply_directory_status(&idp, &principal, request.active, now_ms)?;
        self.link_claims.insert(key, request.claims.clone());
        self.resync_member(&idp, &request.subject, &principal);
        let event = if request.active {
            IdentityEvent::UserProvisioned
        } else {
            IdentityEvent::UserDeprovisioned
        };
        self.audit_event(stamp, now_ms, event, Some(&principal));
        Ok(IdentityReply::User(self.view_of(&principal)?))
    }

    /// A new active human for a first-seen subject, linked to it.
    fn create_provisioned(
        &mut self,
        idp: &IdpConfig,
        request: &ProvisionSubject,
        username: String,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<String, IdentityRefusal> {
        let principal_id = stamp
            .minted_principal_id
            .clone()
            .ok_or(IdentityRefusal::Unstamped)?;
        if self.usernames.contains_key(&username) || self.users.contains_key(&principal_id) {
            return Err(IdentityRefusal::Collision);
        }
        if self.users.len() >= MAX_USERS {
            return Err(IdentityRefusal::Full);
        }
        self.insert_user(
            UserRecord {
                principal_id: principal_id.clone(),
                username,
                display_name: request.display_name.clone(),
                email: request.email.clone(),
                kind: UserKind::Human,
                status: UserStatus::Active,
                is_bootstrap: false,
                source: directory_source(idp),
                roles: BTreeSet::from([USER_ROLE.to_string()]),
                created_at_ms: now_ms,
                disabled_at_ms: None,
                last_login_at_ms: None,
            },
            &BTreeSet::new(),
            "local",
        );
        self.links.insert(
            link_key(&idp.idp_id, &request.subject),
            ExternalIdentity {
                idp_id: idp.idp_id.clone(),
                subject: request.subject.clone(),
                principal_id: principal_id.clone(),
                linked_at_ms: now_ms,
                linked_by: stamp.actor.principal_id.clone(),
            },
        );
        Ok(principal_id)
    }

    /// Replace the profile of a principal this IdP manages; a principal
    /// from anywhere else keeps its profile.
    fn update_directory_profile(
        &mut self,
        idp: &IdpConfig,
        principal: &str,
        request: &ProvisionSubject,
        username: String,
    ) -> Result<(), IdentityRefusal> {
        let user = self.users.get(principal).ok_or(IdentityRefusal::NotFound)?;
        if user.source != directory_source(idp) {
            return Ok(());
        }
        let old_username = user.username.clone();
        let taken = self
            .usernames
            .get(&username)
            .is_some_and(|owner| owner != principal);
        if taken {
            return Err(IdentityRefusal::Collision);
        }
        self.usernames.remove(&old_username);
        self.usernames
            .insert(username.clone(), principal.to_string());
        if let Some(user) = self.users.get_mut(principal) {
            user.username = username;
            user.display_name = request.display_name.clone();
            user.email = request.email.clone();
        }
        Ok(())
    }

    /// `active=false` deprovisions (sessions and API keys revoked, data,
    /// links and ownership kept); `active=true` restores a principal this
    /// IdP deprovisioned. Any other status is left alone.
    fn apply_directory_status(
        &mut self,
        idp: &IdpConfig,
        principal: &str,
        active: bool,
        now_ms: u64,
    ) -> Result<(), IdentityRefusal> {
        let user = self.users.get(principal).ok_or(IdentityRefusal::NotFound)?;
        let restorable =
            user.status == UserStatus::Deprovisioned && user.source == directory_source(idp);
        let target = match (active, restorable) {
            (false, _) => UserStatus::Deprovisioned,
            (true, true) => UserStatus::Active,
            (true, false) => return Ok(()),
        };
        if user.status == target {
            return Ok(());
        }
        self.transition_status(principal, target, now_ms)
    }

    /// Recompute one link's IdP bindings over its provisioned claims plus
    /// the directory groups that contain the principal.
    fn resync_member(&mut self, idp: &IdpConfig, subject: &str, principal: &str) {
        let mut claims = self
            .link_claims
            .get(&link_key(&idp.idp_id, subject))
            .cloned()
            .unwrap_or_default();
        let names = self
            .directory_groups
            .values()
            .filter(|group| group.idp_id == idp.idp_id && group.members.contains(principal))
            .map(|group| group.display_name.clone());
        let groups = claims.entry(GROUPS_CLAIM.to_string()).or_default();
        groups.extend(names);
        groups.sort();
        groups.dedup();
        self.sync_idp_bindings(idp, subject, principal, &claims);
    }

    /// principal → its subjects, for every link of `idp_id`.
    fn linked_principals(&self, idp_id: &str) -> BTreeMap<String, Vec<String>> {
        let mut linked: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for link in self.links.values().filter(|link| link.idp_id == idp_id) {
            linked
                .entry(link.principal_id.clone())
                .or_default()
                .push(link.subject.clone());
        }
        linked
    }

    fn resync_members(&mut self, idp: &IdpConfig, touched: &BTreeSet<String>) {
        let linked = self.linked_principals(&idp.idp_id);
        for principal in touched {
            for subject in linked.get(principal).into_iter().flatten() {
                self.resync_member(idp, subject, principal);
            }
        }
    }

    fn list_provisioned(
        &self,
        request: &ProvisionedQuery,
        stamp: &IdentityStamp,
    ) -> Result<IdentityReply, IdentityRefusal> {
        self.provisioning_idp(&request.idp_id, stamp)?;
        let username = request
            .username
            .as_deref()
            .map(|raw| normalize_username(raw).unwrap_or_else(|_| raw.to_string()));
        let mut rows: Vec<(&str, &str)> = self
            .links
            .values()
            .filter(|link| self.link_matches(link, request, username.as_deref()))
            .map(|link| (link.principal_id.as_str(), link.subject.as_str()))
            .collect();
        rows.sort_unstable();
        let rows = rows
            .into_iter()
            .take(request.limit.min(MAX_PAGE) as usize)
            .map(|(principal, subject)| {
                Ok(ProvisionedUser {
                    subject: subject.to_string(),
                    user: self.view_of(principal)?,
                })
            })
            .collect::<Result<Vec<_>, IdentityRefusal>>()?;
        Ok(IdentityReply::Provisioned(rows))
    }

    fn link_matches(
        &self,
        link: &ExternalIdentity,
        request: &ProvisionedQuery,
        username: Option<&str>,
    ) -> bool {
        let principal = link.principal_id.as_str();
        link.idp_id == request.idp_id
            && request.subject.as_deref().is_none_or(|s| s == link.subject)
            && request
                .principal_id
                .as_deref()
                .is_none_or(|p| p == principal)
            && request
                .after
                .as_deref()
                .is_none_or(|after| principal > after)
            && username.is_none_or(|name| {
                self.users
                    .get(principal)
                    .is_some_and(|user| user.username == name)
            })
    }

    fn provision_group(
        &mut self,
        request: &DirectoryGroup,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let idp = self.provisioning_idp(&request.idp_id, stamp)?;
        checked_group(request)?;
        let linked = self.linked_principals(&idp.idp_id);
        if request
            .members
            .iter()
            .any(|member| !linked.contains_key(member))
        {
            return Err(IdentityRefusal::NotFound);
        }
        let key = link_key(&idp.idp_id, &request.group_id);
        let new = !self.directory_groups.contains_key(&key);
        if new && self.directory_groups.len() >= MAX_DIRECTORY_GROUPS {
            return Err(IdentityRefusal::Full);
        }
        let previous = self.directory_groups.insert(key, request.clone());
        let mut touched = previous.map(|group| group.members).unwrap_or_default();
        touched.extend(request.members.iter().cloned());
        self.resync_members(&idp, &touched);
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::DirectoryGroupChanged,
            Some(&request.group_id),
        );
        Ok(IdentityReply::DirectoryGroup(request.clone()))
    }

    fn remove_directory_group(
        &mut self,
        request: &DirectoryGroupRef,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let idp = self.provisioning_idp(&request.idp_id, stamp)?;
        let removed = self
            .directory_groups
            .remove(&link_key(&idp.idp_id, &request.group_id))
            .ok_or(IdentityRefusal::NotFound)?;
        self.resync_members(&idp, &removed.members);
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::DirectoryGroupChanged,
            Some(&request.group_id),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    fn list_directory_groups(
        &self,
        request: &DirectoryGroupQuery,
        stamp: &IdentityStamp,
    ) -> Result<IdentityReply, IdentityRefusal> {
        self.provisioning_idp(&request.idp_id, stamp)?;
        let groups = self
            .directory_groups
            .values()
            .filter(|group| group_matches(group, request))
            .take(request.limit.min(MAX_PAGE) as usize)
            .cloned()
            .collect();
        Ok(IdentityReply::DirectoryGroups(groups))
    }

    /// An unlinked subject leaves no provisioned claims behind, and a
    /// principal no longer linked to the IdP leaves its directory groups.
    pub(super) fn forget_link(&mut self, link: &ExternalIdentity) {
        self.link_claims
            .remove(&link_key(&link.idp_id, &link.subject));
        if self
            .linked_principals(&link.idp_id)
            .contains_key(&link.principal_id)
        {
            return;
        }
        for group in self
            .directory_groups
            .values_mut()
            .filter(|group| group.idp_id == link.idp_id)
        {
            group.members.remove(&link.principal_id);
        }
    }

    /// A removed IdP takes its directory groups with it.
    pub(super) fn forget_idp_directory(&mut self, idp_id: &str) {
        self.directory_groups
            .retain(|_, group| group.idp_id != idp_id);
    }
}
