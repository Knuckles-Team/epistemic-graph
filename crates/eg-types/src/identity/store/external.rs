//! External sign-in (§4): graph-os verified an identity-provider assertion;
//! the store resolves the subject to a principal (link, or the IdP's JIT
//! policy), recomputes the IdP-sourced roles and group memberships from the
//! mapping rules, and opens a session.
//!
//! Mapping is a deterministic UNION of every matching rule; removal in the
//! IdP removes the role at the next sign-in because IdP-sourced bindings are
//! recomputed, while locally granted roles are never touched.

use std::collections::{BTreeMap, BTreeSet};

use super::super::access::{IdpConfig, JitPolicy, MappingRule};
use super::super::audit::IdentityEvent;
use super::super::config::AuthMode;
use super::super::model::{UserKind, UserRecord, UserStatus};
use super::super::requests::ExternalLogin;
use super::super::stamp::IdentityStamp;
use super::super::views::AuthenticateResult;
use super::super::{normalize_username, IdentityRefusal, USER_ROLE};
use super::{digest_hex, link_key, ApplyContext, IdentityStore};

/// Whether one rule matches the forwarded claims.
fn rule_matches(rule: &MappingRule, claims: &BTreeMap<String, Vec<String>>) -> bool {
    let values = claims.get(&rule.claim_path).map(Vec::as_slice).unwrap_or(&[]);
    values.iter().any(|value| match rule.match_kind.as_str() {
        "equals" => value == &rule.value,
        "prefix" => value.starts_with(&rule.value),
        _ => false,
    })
}

/// The `(roles, groups)` the matching rules of `idp` target.
fn mapped_targets(
    idp: &IdpConfig,
    claims: &BTreeMap<String, Vec<String>>,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut roles = BTreeSet::new();
    let mut groups = BTreeSet::new();
    for rule in idp.rules.iter().filter(|rule| rule_matches(rule, claims)) {
        match rule.target.split_once(':') {
            Some(("role", id)) => {
                roles.insert(id.to_string());
            }
            Some(("group", id)) => {
                groups.insert(id.to_string());
            }
            Some(_) | None => {}
        }
    }
    (roles, groups)
}

impl IdentityStore {
    pub(super) fn external_login(
        &mut self,
        request: &ExternalLogin,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<AuthenticateResult, IdentityRefusal> {
        let mode = self.require_initialized()?.mode;
        let idp = self
            .idps
            .get(&request.idp_id)
            .filter(|idp| idp.enabled)
            .cloned();
        let Some(idp) = idp.filter(|_| mode == AuthMode::External) else {
            return Ok(AuthenticateResult::bad());
        };
        let Some(principal) = self.principal_for_subject(&idp, request, stamp, ctx.now_ms)? else {
            self.audit_event(stamp, ctx.now_ms, IdentityEvent::LoginFailed, None);
            return Ok(AuthenticateResult::bad());
        };
        if !self.is_active(&principal) {
            return Ok(AuthenticateResult::bad());
        }
        self.sync_idp_bindings(&idp, &request.subject, &principal, &request.claims);
        if let Some(user) = self.users.get_mut(&principal) {
            user.last_login_at_ms = Some(ctx.now_ms);
        }
        self.audit_event(stamp, ctx.now_ms, IdentityEvent::LoginSucceeded, Some(&principal));
        let method = format!("idp:{}", idp.idp_id);
        self.open_with_mfa_policy(&principal, &method, request.ip_prefix.clone(), stamp, ctx)
    }

    /// The linked principal, or one the IdP's JIT policy provides.
    fn principal_for_subject(
        &mut self,
        idp: &IdpConfig,
        request: &ExternalLogin,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<Option<String>, IdentityRefusal> {
        let key = link_key(&idp.idp_id, &request.subject);
        if let Some(link) = self.links.get(&key) {
            return Ok(Some(link.principal_id.clone()));
        }
        let principal = match idp.jit_policy {
            JitPolicy::Deny => None,
            JitPolicy::Create => Some(self.jit_create(idp, request, stamp, now_ms)?),
            JitPolicy::LinkByVerifiedEmail => self.principal_by_verified_email(&request.claims),
        };
        if let Some(principal) = &principal {
            self.link(&idp.idp_id, &request.subject, principal, stamp, now_ms)?;
        }
        Ok(principal)
    }

    /// Link by e-mail only when the IdP asserts it verified that e-mail and
    /// exactly one principal holds it.
    fn principal_by_verified_email(&self, claims: &BTreeMap<String, Vec<String>>) -> Option<String> {
        let verified = claims.get("email_verified").is_some_and(|v| v == &["true"]);
        let email = claims.get("email").filter(|v| v.len() == 1)?.first()?;
        let mut owners = self
            .users
            .values()
            .filter(|user| user.email.as_deref() == Some(email.as_str()));
        let owner = owners.next()?;
        (verified && owners.next().is_none()).then(|| owner.principal_id.clone())
    }

    /// Create a principal for a first sign-in. The username is the hint when
    /// free, else a stable derivation of the IdP and subject.
    fn jit_create(
        &mut self,
        idp: &IdpConfig,
        request: &ExternalLogin,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<String, IdentityRefusal> {
        let principal_id = stamp
            .minted_principal_id
            .clone()
            .ok_or(IdentityRefusal::Unstamped)?;
        let hinted = request
            .username_hint
            .as_deref()
            .and_then(|hint| normalize_username(hint).ok())
            .filter(|name| !self.usernames.contains_key(name));
        let derived = || {
            let digest = digest_hex(b"eg/identity-jit-username/v1\0", &link_key(&idp.idp_id, &request.subject));
            format!("{}-{}", idp.idp_id, &digest[..12])
        };
        let username = hinted.unwrap_or_else(derived);
        if self.usernames.contains_key(&username) || self.users.contains_key(&principal_id) {
            return Err(IdentityRefusal::Collision);
        }
        self.insert_user(
            UserRecord {
                principal_id: principal_id.clone(),
                username,
                display_name: None,
                email: None,
                kind: UserKind::Human,
                status: UserStatus::Active,
                is_bootstrap: false,
                source: format!("jit:{}", idp.idp_id),
                roles: BTreeSet::from([USER_ROLE.to_string()]),
                created_at_ms: now_ms,
                disabled_at_ms: None,
                last_login_at_ms: None,
            },
            &BTreeSet::new(),
            "local",
        );
        self.audit_event(stamp, now_ms, IdentityEvent::UserCreated, Some(&principal_id));
        Ok(principal_id)
    }

    /// Recompute this IdP's roles and group memberships for `principal`.
    fn sync_idp_bindings(
        &mut self,
        idp: &IdpConfig,
        subject: &str,
        principal: &str,
        claims: &BTreeMap<String, Vec<String>>,
    ) {
        let (roles, groups) = mapped_targets(idp, claims);
        self.link_roles.insert(link_key(&idp.idp_id, subject), roles);
        for group in self.groups.values_mut() {
            let sourced_here = group.members.get(principal) == Some(&idp.idp_id);
            if sourced_here && !groups.contains(&group.group_id) {
                group.members.remove(principal);
            }
            let absent = !group.members.contains_key(principal);
            if absent && groups.contains(&group.group_id) {
                group.members.insert(principal.to_string(), idp.idp_id.clone());
            }
        }
    }
}
