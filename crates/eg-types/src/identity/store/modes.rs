//! First-run initialization, the mode state machine and policy (IDM-04).

use std::collections::{BTreeMap, BTreeSet};

use super::super::access::{GroupRecord, RoleRecord};
use super::super::audit::IdentityEvent;
use super::super::config::{
    AuthMode, IdentityConfig, ModeTransition, HARD_PASSWORD_MIN_CHARS, NONE_MODE_ACK,
};
use super::super::model::{PasswordCredential, UserKind, UserRecord, UserStatus};
use super::super::ops::ConfigOp;
use super::super::requests::InitializeRequest;
use super::super::requests_admin::{ListQuery, PolicyUpdate, RoleGraphGrant, MAX_PAGE};
use super::super::stamp::IdentityStamp;
use super::super::text::{bounded, MAX_ID_BYTES};
use super::super::views::IdentityReply;
use super::super::{
    normalize_username, IdentityRefusal, ACTION_APPROVERS_GROUP, ACTION_APPROVER_ROLE,
    ADMINISTRATORS_GROUP, ADMIN_ROLE, BOOTSTRAP_PRINCIPAL, BUILTIN_APPROVER_GROUPS,
    ELEVATION_APPROVERS_GROUP, ELEVATION_APPROVER_ROLE, LIVE_ORDER_APPROVERS_GROUP,
    LIVE_ORDER_APPROVER_ROLE, SCHEMA_APPROVERS_GROUP, SCHEMA_APPROVER_ROLE, USER_ROLE,
};
use super::{ApplyContext, IdentityStore};
use crate::acl::{GrantEffect, RbacAction, ResourceSelector};

/// Built-in roles: `(role_id, scopes)`. Administrators additionally get an
/// allow-all graph grant for every action.
const BUILTIN_ROLES: [(&str, &[&str]); 6] = [
    (
        ADMIN_ROLE,
        &[
            "kg:admin",
            "webui:admin",
            "identity:admin",
            "identity:read",
            "identity:self",
        ],
    ),
    (USER_ROLE, &["kg:read", "identity:self"]),
    (ELEVATION_APPROVER_ROLE, &["rbac:approve-elevation"]),
    (LIVE_ORDER_APPROVER_ROLE, &["finance:approve-live-order"]),
    (
        ACTION_APPROVER_ROLE,
        &["approvals:read", "approvals:decide"],
    ),
    (
        SCHEMA_APPROVER_ROLE,
        &["governance:approve-schema-repair", "governance:read"],
    ),
];

/// Built-in groups: `(group_id, role_id)`.
const BUILTIN_GROUPS: [(&str, &str); 5] = [
    (ADMINISTRATORS_GROUP, ADMIN_ROLE),
    (ELEVATION_APPROVERS_GROUP, ELEVATION_APPROVER_ROLE),
    (LIVE_ORDER_APPROVERS_GROUP, LIVE_ORDER_APPROVER_ROLE),
    (ACTION_APPROVERS_GROUP, ACTION_APPROVER_ROLE),
    (SCHEMA_APPROVERS_GROUP, SCHEMA_APPROVER_ROLE),
];

/// The direct roles `initialize` gives the bootstrap principal.
fn seed_user_roles() -> BTreeSet<String> {
    BTreeSet::from([USER_ROLE.to_string()])
}

fn admin_grants() -> Vec<RoleGraphGrant> {
    [RbacAction::Read, RbacAction::Write, RbacAction::Admin]
        .into_iter()
        .map(|action| RoleGraphGrant {
            resource: ResourceSelector::All,
            action,
            effect: GrantEffect::Allow,
        })
        .collect()
}

impl IdentityStore {
    pub(super) fn apply_config(
        &mut self,
        op: &ConfigOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        match op {
            ConfigOp::Initialize { request } => self.initialize(request, stamp, ctx.now_ms),
            ConfigOp::Transition { request } => self.transition(request, stamp, ctx.now_ms),
            ConfigOp::UpdatePolicy { request } => self.update_policy(request, stamp, ctx.now_ms),
            ConfigOp::Get => Ok(IdentityReply::Config(self.require_initialized()?.clone())),
            ConfigOp::Audit { request } => self.audit_page(request),
            ConfigOp::ExportSql => Ok(IdentityReply::Sql(super::super::sql_dump::render_dump(
                &self.sql_relations(),
            ))),
            ConfigOp::ImportSql { request } => self.import_sql(&request.sql, stamp, ctx.now_ms),
            ConfigOp::RepairSystemIdentity { request } => {
                self.repair_system_identity(&request.id, stamp, ctx.now_ms)
            }
        }
    }

    fn audit_page(&self, request: &ListQuery) -> Result<IdentityReply, IdentityRefusal> {
        let after = request.after.as_deref().map(str::parse::<u64>);
        let after = after
            .transpose()
            .map_err(|_| IdentityRefusal::InvalidRequest)?;
        let limit = request.limit.min(MAX_PAGE) as usize;
        Ok(IdentityReply::Audit(self.audit.page(after, limit)))
    }

    fn import_sql(
        &mut self,
        sql: &str,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let imported = self.import_dump(sql, now_ms)?;
        self.audit_event(stamp, now_ms, IdentityEvent::Imported, None);
        Ok(IdentityReply::Done {
            changed: imported > 0,
        })
    }

    /// The store's half of a System-identity repair: validate the agent id
    /// and audit the repair. A principal the store manages (a person, a
    /// provisioned service) is never the engine's System identity. The engine
    /// applies the identity itself in the same durable write.
    fn repair_system_identity(
        &mut self,
        agent_id: &str,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        bounded(agent_id, MAX_ID_BYTES)?;
        if agent_id.chars().any(char::is_whitespace) {
            return Err(IdentityRefusal::InvalidRequest);
        }
        if self.manages(agent_id) {
            return Err(IdentityRefusal::KindMismatch);
        }
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::SystemIdentityRepaired,
            Some(agent_id),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    /// Seed the singleton, the built-in roles and groups, and the bootstrap
    /// principal. `none` seeds it without a credential; `local` requires the
    /// first administrator's credential on it.
    fn initialize(
        &mut self,
        request: &InitializeRequest,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        if self.config.is_some() {
            return Err(IdentityRefusal::AlreadyInitialized);
        }
        let username = match request.mode {
            AuthMode::None => "bootstrap".to_string(),
            AuthMode::Local => normalize_username(
                request
                    .admin_username
                    .as_deref()
                    .ok_or(IdentityRefusal::InvalidRequest)?,
            )?,
            AuthMode::External => return Err(IdentityRefusal::IllegalTransition),
        };
        self.seed_builtins();
        self.seed_bootstrap(username, now_ms);
        if request.mode == AuthMode::Local {
            self.passwords.insert(
                BOOTSTRAP_PRINCIPAL.to_string(),
                PasswordCredential {
                    hash: stamp.new_password_hash()?.to_string(),
                    changed_at_ms: now_ms,
                    must_change: false,
                    history: Vec::new(),
                },
            );
        }
        self.config = Some(IdentityConfig::seeded(request.mode, now_ms));
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::Initialized,
            Some(BOOTSTRAP_PRINCIPAL),
        );
        Ok(IdentityReply::Principal {
            principal_id: BOOTSTRAP_PRINCIPAL.to_string(),
        })
    }

    /// Whether the store holds NOTHING beyond what a credential-less
    /// `initialize` seeds: exactly the built-in roles (with their seeded
    /// scopes and grants) and built-in groups, and at most the bootstrap
    /// principal with no credential of any kind. Any real principal, any
    /// password, API key, second factor, IdP, link or directory state, and any
    /// administrator-made role or group means a real identity exists -- the
    /// engine's System bootstrap must then stay closed (an attacker must not
    /// be able to claim an instance someone already set up). An uninitialized
    /// store is trivially seed-only.
    pub fn holds_only_seed(&self) -> bool {
        if self.config.is_none() {
            return true;
        }
        let mut seed = Self::default();
        seed.seed_builtins();
        if let Some(user) = self.users.get(BOOTSTRAP_PRINCIPAL) {
            seed.seed_bootstrap(user.username.clone(), user.created_at_ms);
        }
        let bootstrap_only = self.users.keys().all(|p| p == BOOTSTRAP_PRINCIPAL)
            && self
                .users
                .values()
                .all(|user| user.roles == seed_user_roles());
        bootstrap_only
            && self.roles == seed.roles
            && self.groups == seed.groups
            && self.holds_no_credential_or_directory_state()
    }

    fn holds_no_credential_or_directory_state(&self) -> bool {
        self.passwords.is_empty()
            && self.api_keys.is_empty()
            && self.one_time.is_empty()
            && self.totp.is_empty()
            && self.recovery.is_empty()
            && self.webauthn.is_empty()
            && self.idps.is_empty()
            && self.links.is_empty()
            && self.link_roles.is_empty()
            && self.link_claims.is_empty()
            && self.directory_groups.is_empty()
    }

    fn seed_builtins(&mut self) {
        for (role_id, scopes) in BUILTIN_ROLES {
            let graph_grants = if role_id == ADMIN_ROLE {
                admin_grants()
            } else {
                Vec::new()
            };
            self.roles.insert(
                role_id.to_string(),
                RoleRecord {
                    role_id: role_id.to_string(),
                    name: role_id.to_string(),
                    description: None,
                    builtin: true,
                    scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
                    graph_grants,
                },
            );
        }
        for (group_id, role_id) in BUILTIN_GROUPS {
            self.groups.insert(
                group_id.to_string(),
                GroupRecord {
                    group_id: group_id.to_string(),
                    name: group_id.to_string(),
                    source: "local".to_string(),
                    builtin: true,
                    members: BTreeMap::new(),
                    roles: BTreeSet::from([role_id.to_string()]),
                    mfa_required: BUILTIN_APPROVER_GROUPS.contains(&group_id),
                },
            );
        }
    }

    fn seed_bootstrap(&mut self, username: String, now_ms: u64) {
        let principal = BOOTSTRAP_PRINCIPAL.to_string();
        self.usernames.insert(username.clone(), principal.clone());
        self.users.insert(
            principal.clone(),
            UserRecord {
                principal_id: principal.clone(),
                username,
                display_name: None,
                email: None,
                kind: UserKind::Human,
                status: UserStatus::Active,
                is_bootstrap: true,
                source: "local".to_string(),
                roles: seed_user_roles(),
                created_at_ms: now_ms,
                disabled_at_ms: None,
                last_login_at_ms: None,
            },
        );
        if let Some(group) = self.groups.get_mut(ADMINISTRATORS_GROUP) {
            group.members.insert(principal, "local".to_string());
        }
    }

    /// Move the mode along one edge of the state machine. Every accepted
    /// transition bumps the epoch, records the new issuer key id and revokes
    /// every session; none deletes an account, credential, link or grant.
    fn transition(
        &mut self,
        request: &ModeTransition,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let config = self.require_initialized()?.clone();
        if request.expected_epoch != config.epoch {
            return Err(IdentityRefusal::EpochConflict);
        }
        if !config.mode.may_move_to(request.to) {
            return Err(IdentityRefusal::IllegalTransition);
        }
        super::super::text::identifier(&request.issuer_kid)?;
        self.check_transition(config.mode, request, stamp)?;
        if (config.mode, request.to) == (AuthMode::External, AuthMode::Local) {
            self.mark_credentialless_pending_reset();
        }
        self.revoke_all_sessions();
        let next = IdentityConfig {
            mode: request.to,
            local_fallback: request.local_fallback.unwrap_or(config.local_fallback),
            epoch: config.epoch + 1,
            issuer_kid_current: Some(request.issuer_kid.clone()),
            ..config
        };
        self.config = Some(next.clone());
        self.audit_event(stamp, now_ms, IdentityEvent::ModeTransition, None);
        Ok(IdentityReply::Config(next))
    }

    /// The per-edge preconditions of §2.3.
    fn check_transition(
        &self,
        from: AuthMode,
        request: &ModeTransition,
        stamp: &IdentityStamp,
    ) -> Result<(), IdentityRefusal> {
        let holds = match (from, request.to) {
            (AuthMode::None, AuthMode::Local) => self.some_admin_has_password(),
            (AuthMode::None | AuthMode::Local, AuthMode::External) => self.some_admin_is_linked(),
            (AuthMode::External, AuthMode::Local) => self.every_admin_has_password(),
            (_, AuthMode::None) => {
                stamp.engine_loopback && request.ack.as_deref() == Some(NONE_MODE_ACK)
            }
            (AuthMode::Local, AuthMode::Local) | (AuthMode::External, AuthMode::External) => false,
        };
        if holds {
            Ok(())
        } else {
            Err(IdentityRefusal::PreconditionFailed)
        }
    }

    /// Active human administrators.
    fn administrators(&self) -> impl Iterator<Item = &UserRecord> {
        self.groups
            .get(ADMINISTRATORS_GROUP)
            .into_iter()
            .flat_map(|group| group.members.keys())
            .filter_map(|principal| self.users.get(principal))
            .filter(|user| user.kind == UserKind::Human && user.status.is_active())
    }

    fn some_admin_has_password(&self) -> bool {
        self.administrators()
            .any(|user| self.passwords.contains_key(&user.principal_id))
    }

    fn every_admin_has_password(&self) -> bool {
        let mut admins = self.administrators().peekable();
        admins.peek().is_some()
            && self
                .administrators()
                .all(|user| self.passwords.contains_key(&user.principal_id))
    }

    fn some_admin_is_linked(&self) -> bool {
        let admins: BTreeSet<&str> = self
            .administrators()
            .map(|user| user.principal_id.as_str())
            .collect();
        self.links.values().any(|link| {
            admins.contains(link.principal_id.as_str())
                && self.idps.get(&link.idp_id).is_some_and(|idp| idp.enabled)
        })
    }

    /// Leaving `external`: an active human with no local credential must set
    /// one before signing in again. The account and its links are kept.
    fn mark_credentialless_pending_reset(&mut self) {
        let passwords = &self.passwords;
        for user in self.users.values_mut() {
            let needs_reset = user.kind == UserKind::Human
                && user.status.is_active()
                && !passwords.contains_key(&user.principal_id);
            if needs_reset {
                user.status = UserStatus::PendingReset;
            }
        }
    }

    fn update_policy(
        &mut self,
        request: &PolicyUpdate,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let mut config = self.require_initialized()?.clone();
        if request.expected_epoch != config.epoch {
            return Err(IdentityRefusal::EpochConflict);
        }
        if let Some(min) = request.password_min_chars {
            let bounded = (HARD_PASSWORD_MIN_CHARS..=64).contains(&min);
            if !bounded {
                return Err(IdentityRefusal::InvalidRequest);
            }
            config.password_min_chars = min;
        }
        config.registration_policy = request
            .registration_policy
            .unwrap_or(config.registration_policy);
        config.local_fallback = request.local_fallback.unwrap_or(config.local_fallback);
        config.epoch += 1;
        self.config = Some(config.clone());
        self.audit_event(stamp, now_ms, IdentityEvent::PolicyUpdated, None);
        Ok(IdentityReply::Config(config))
    }
}
