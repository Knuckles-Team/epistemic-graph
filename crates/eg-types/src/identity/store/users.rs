//! Principal lifecycle: create, update, status, unlock and the reads.

use std::collections::BTreeSet;

use super::super::audit::IdentityEvent;
use super::super::model::{UserKind, UserRecord, UserStatus};
use super::super::ops::UserOp;
use super::super::requests::{CreateUserRequest, UserStatusChange, UserUpdate};
use super::super::requests_admin::{ListQuery, MAX_PAGE};
use super::super::stamp::IdentityStamp;
use super::super::text::{bounded_opt, email, MAX_NAME_BYTES};
use super::super::views::{CredentialFacts, IdentityReply, UserView};
use super::super::{
    normalize_username, validate_principal_id, IdentityRefusal, ADMINISTRATORS_GROUP, MAX_USERS,
    USER_ROLE,
};
use super::throttle::account_key;
use super::{ApplyContext, IdentityStore};

impl IdentityStore {
    pub(super) fn apply_user(
        &mut self,
        op: &UserOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        match op {
            UserOp::Create { request } => self.create_user(request, stamp, ctx.now_ms),
            UserOp::Update { request } => self.update_user(request, stamp, ctx.now_ms),
            UserOp::SetStatus { request } => self.set_status(request, stamp, ctx.now_ms),
            UserOp::Unlock { request } => {
                self.users
                    .get(&request.id)
                    .ok_or(IdentityRefusal::NotFound)?;
                self.clear_throttle(&account_key(&request.id));
                self.audit_event(
                    stamp,
                    ctx.now_ms,
                    IdentityEvent::UserUnlocked,
                    Some(&request.id),
                );
                Ok(IdentityReply::Done { changed: true })
            }
            UserOp::Get { request } => Ok(IdentityReply::User(self.view_of(&request.id)?)),
            UserOp::List { request } => Ok(IdentityReply::Users(self.list_users(request))),
            UserOp::Resolve { request } => Ok(IdentityReply::Resolution(
                self.resolve(&request.id, ctx.classifier)?,
            )),
        }
    }

    pub(crate) fn view_of(&self, principal_id: &str) -> Result<UserView, IdentityRefusal> {
        let user = self
            .users
            .get(principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        Ok(UserView::of(user, self.facts_of(principal_id)))
    }

    fn facts_of(&self, principal_id: &str) -> CredentialFacts {
        CredentialFacts {
            has_password: self.passwords.contains_key(principal_id),
            totp_enrolled: self.totp_confirmed(principal_id),
            recovery_codes_left: self.recovery.get(principal_id).map_or(0, |codes| {
                codes
                    .iter()
                    .filter(|code| code.used_at_ms.is_none())
                    .count()
            }),
        }
    }

    fn list_users(&self, request: &ListQuery) -> Vec<UserView> {
        let limit = request.limit.min(MAX_PAGE) as usize;
        self.users
            .values()
            .filter(|user| {
                request
                    .after
                    .as_deref()
                    .is_none_or(|after| user.principal_id.as_str() > after)
            })
            .take(limit)
            .map(|user| UserView::of(user, self.facts_of(&user.principal_id)))
            .collect()
    }

    /// Validate a new principal's id and username against the store.
    fn new_principal(
        &self,
        request: &CreateUserRequest,
        stamp: &IdentityStamp,
    ) -> Result<(String, String), IdentityRefusal> {
        let principal_id = match &request.principal_id {
            Some(explicit) => explicit.clone(),
            None => stamp
                .minted_principal_id
                .clone()
                .ok_or(IdentityRefusal::Unstamped)?,
        };
        validate_principal_id(&principal_id)?;
        let username = normalize_username(&request.username)?;
        if self.users.contains_key(&principal_id) || self.usernames.contains_key(&username) {
            return Err(IdentityRefusal::Collision);
        }
        if self.users.len() >= MAX_USERS {
            return Err(IdentityRefusal::Full);
        }
        Ok((principal_id, username))
    }

    fn create_user(
        &mut self,
        request: &CreateUserRequest,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let (principal_id, username) = self.new_principal(request, stamp)?;
        bounded_opt(request.display_name.as_deref(), MAX_NAME_BYTES)?;
        request.email.as_deref().map(email).transpose()?;
        let unknown_binding = request
            .roles
            .iter()
            .any(|role| !self.roles.contains_key(role))
            || request
                .groups
                .iter()
                .any(|group| !self.groups.contains_key(group));
        if unknown_binding {
            return Err(IdentityRefusal::NotFound);
        }
        if stamp.password_hash.is_some() && request.kind != UserKind::Human {
            return Err(IdentityRefusal::KindMismatch);
        }
        let mut roles: BTreeSet<String> = request.roles.clone();
        if request.kind == UserKind::Human {
            roles.insert(USER_ROLE.to_string());
        }
        self.insert_user(
            UserRecord {
                principal_id: principal_id.clone(),
                username,
                display_name: request.display_name.clone(),
                email: request.email.clone(),
                kind: request.kind,
                status: UserStatus::Active,
                is_bootstrap: false,
                source: "local".to_string(),
                roles,
                created_at_ms: now_ms,
                disabled_at_ms: None,
                last_login_at_ms: None,
            },
            &request.groups,
            "local",
        );
        if let Some(hash) = &stamp.password_hash {
            self.store_password(&principal_id, hash, request.must_change, now_ms);
        }
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::UserCreated,
            Some(&principal_id),
        );
        Ok(IdentityReply::Principal { principal_id })
    }

    /// Insert a user, its username index entry and its group memberships.
    pub(crate) fn insert_user(
        &mut self,
        user: UserRecord,
        groups: &BTreeSet<String>,
        source: &str,
    ) {
        let principal_id = user.principal_id.clone();
        self.usernames
            .insert(user.username.clone(), principal_id.clone());
        self.users.insert(principal_id.clone(), user);
        for group in groups {
            if let Some(group) = self.groups.get_mut(group) {
                group
                    .members
                    .insert(principal_id.clone(), source.to_string());
            }
        }
    }

    fn update_user(
        &mut self,
        request: &UserUpdate,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        bounded_opt(request.display_name.as_deref(), MAX_NAME_BYTES)?;
        request.email.as_deref().map(email).transpose()?;
        let username = request
            .username
            .as_deref()
            .map(normalize_username)
            .transpose()?;
        let current = self
            .users
            .get(&request.principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        let old_username = current.username.clone();
        if let Some(new) = &username {
            let taken = self
                .usernames
                .get(new)
                .is_some_and(|owner| owner != &request.principal_id);
            if taken {
                return Err(IdentityRefusal::Collision);
            }
            self.usernames.remove(&old_username);
            self.usernames
                .insert(new.clone(), request.principal_id.clone());
        }
        let user = self
            .users
            .get_mut(&request.principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        user.username = username.unwrap_or(old_username);
        if request.display_name.is_some() {
            user.display_name = request.display_name.clone();
        }
        if request.email.is_some() {
            user.email = request.email.clone();
        }
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::UserUpdated,
            Some(&request.principal_id),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    /// Change a status. Leaving `active` revokes every session and API key,
    /// and the last active administrator can never be taken out of service.
    fn set_status(
        &mut self,
        request: &UserStatusChange,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        self.transition_status(&request.principal_id, request.status, now_ms)?;
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::UserStatusChanged,
            Some(&request.principal_id),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    /// Move a principal to `status`. Leaving `active` revokes every session
    /// and API key; the last active administrator is never taken out.
    pub(crate) fn transition_status(
        &mut self,
        principal_id: &str,
        status: UserStatus,
        now_ms: u64,
    ) -> Result<(), IdentityRefusal> {
        let user = self
            .users
            .get(principal_id)
            .ok_or(IdentityRefusal::NotFound)?;
        let leaving = user.status.is_active() && !status.is_active();
        if leaving && self.is_last_active_admin(principal_id) {
            return Err(IdentityRefusal::PreconditionFailed);
        }
        if let Some(user) = self.users.get_mut(principal_id) {
            user.status = status;
            user.disabled_at_ms = (!status.is_active()).then_some(now_ms);
        }
        if leaving {
            self.revoke_principal_sessions(principal_id, now_ms, "status_change");
            self.revoke_principal_api_keys(principal_id, now_ms);
        }
        Ok(())
    }

    /// Whether `principal_id` is the only active human administrator.
    pub(crate) fn is_last_active_admin(&self, principal_id: &str) -> bool {
        let Some(group) = self.groups.get(ADMINISTRATORS_GROUP) else {
            return false;
        };
        if !group.members.contains_key(principal_id) {
            return false;
        }
        !group.members.keys().any(|member| {
            member != principal_id
                && self
                    .users
                    .get(member)
                    .is_some_and(|user| user.kind == UserKind::Human && user.status.is_active())
        })
    }
}
