//! Server-side sessions: opened by a sign-in, touched by every use, revoked
//! on demand, on a status change and on every mode transition.

use super::super::audit::IdentityEvent;
use super::super::model::SessionRecord;
use super::super::ops::SessionOp;
use super::super::requests_admin::ObjectRef;
use super::super::scope::ScopeClass;
use super::super::stamp::IdentityStamp;
use super::super::views::{IdentityReply, SessionView};
use super::super::{IdentityRefusal, MAX_SESSIONS_PER_USER};
use super::{ApplyContext, IdentityStore};

/// Whether a session still owing its second factor may act.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingSession {
    Refuse,
    /// Only to enrol a first factor (the principal has none confirmed yet).
    AllowEnrollment,
}

/// How a new session starts.
pub(crate) struct SessionOpen<'a> {
    pub(crate) session_hash: &'a str,
    pub(crate) principal_id: &'a str,
    pub(crate) method: String,
    pub(crate) mfa_pending: bool,
    pub(crate) ip_prefix: Option<String>,
}

impl IdentityStore {
    pub(super) fn apply_session(
        &mut self,
        op: &SessionOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        match op {
            SessionOp::Resolve { .. } => self.resolve_session(stamp, ctx),
            SessionOp::Revoke { .. } => {
                let hash = stamp.token_hash(0)?.to_string();
                let changed = self.revoke_session(&hash, ctx.now_ms, "logout");
                if changed {
                    self.audit_event(stamp, ctx.now_ms, IdentityEvent::SessionRevoked, None);
                }
                Ok(IdentityReply::Done { changed })
            }
            SessionOp::RevokeAll { request } => self.revoke_user(request, stamp, ctx.now_ms),
            SessionOp::List { request } => Ok(IdentityReply::Sessions(
                self.sessions
                    .values()
                    .filter(|session| session.principal_id == request.id)
                    .map(SessionView::of)
                    .collect(),
            )),
        }
    }

    /// The principal a broker op acts for: the owner of the live session
    /// whose hash is the op's FIRST token hash. A person is proven by a
    /// session in this store, never by a claim the broker forwards.
    pub(crate) fn session_subject(
        &self,
        stamp: &IdentityStamp,
        now_ms: u64,
        pending: PendingSession,
    ) -> Result<String, IdentityRefusal> {
        let session = self
            .sessions
            .get(stamp.token_hash(0)?)
            .filter(|session| session.is_live(now_ms))
            .ok_or(IdentityRefusal::NotAuthorized)?;
        let principal = session.principal_id.clone();
        let pending_ok = match pending {
            PendingSession::Refuse => false,
            PendingSession::AllowEnrollment => !self.mfa_enrolled(&principal),
        };
        if (session.mfa_pending && !pending_ok) || !self.is_active(&principal) {
            return Err(IdentityRefusal::NotAuthorized);
        }
        Ok(principal)
    }

    /// The session subject, required to be an administrator.
    pub(crate) fn admin_subject(
        &self,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<String, IdentityRefusal> {
        let principal = self.session_subject(stamp, ctx.now_ms, PendingSession::Refuse)?;
        let scopes = self.resolve(&principal, ctx.classifier)?.scopes;
        if scopes.contains(super::super::ops::IDENTITY_ADMIN_SCOPE) {
            Ok(principal)
        } else {
            Err(IdentityRefusal::NotAuthorized)
        }
    }

    /// Touch a live session and answer its principal's resolution.
    fn resolve_session(
        &mut self,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let hash = stamp.token_hash(0)?;
        let now_ms = ctx.now_ms;
        let session = self
            .sessions
            .get(hash)
            .filter(|session| session.is_live(now_ms))
            .ok_or(IdentityRefusal::NotFound)?;
        let principal = session.principal_id.clone();
        let pending = session.mfa_pending;
        if !self.is_active(&principal) {
            return Err(IdentityRefusal::NotFound);
        }
        let mut resolution = self.resolve(&principal, ctx.classifier)?;
        resolution.session_mfa_pending = pending;
        let idle = self.idle_ms_for(&resolution.scopes, ctx);
        if let Some(session) = self.sessions.get_mut(hash) {
            // Touch at a coarse granularity: a resolve inside the same minute
            // changes nothing, so the engine persists nothing for it.
            if now_ms
                >= session
                    .last_seen_at_ms
                    .saturating_add(super::TOUCH_GRANULARITY_MS)
            {
                session.last_seen_at_ms = now_ms;
                session.idle_expires_at_ms = now_ms.saturating_add(idle);
            }
        }
        Ok(IdentityReply::Resolution(resolution))
    }

    fn revoke_user(
        &mut self,
        request: &ObjectRef,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        if !self.users.contains_key(&request.id) {
            return Err(IdentityRefusal::NotFound);
        }
        let changed = self.revoke_principal_sessions(&request.id, now_ms, "admin_revoke");
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::SessionRevoked,
            Some(&request.id),
        );
        Ok(IdentityReply::Done { changed })
    }

    /// Whether any of `scopes` is administrator- or approver-class: such a
    /// session gets the shorter privileged bounds.
    fn is_privileged(scopes: &std::collections::BTreeSet<String>, ctx: &ApplyContext<'_>) -> bool {
        scopes.iter().any(|scope| {
            matches!(
                ctx.classifier.class_of(scope),
                Some(ScopeClass::Admin | ScopeClass::Approver)
            )
        })
    }

    fn idle_ms_for(
        &self,
        scopes: &std::collections::BTreeSet<String>,
        ctx: &ApplyContext<'_>,
    ) -> u64 {
        let config = self.config.as_ref();
        match (config, Self::is_privileged(scopes, ctx)) {
            (Some(config), true) => config.privileged_idle_ms,
            (Some(config), false) => config.idle_ms,
            (None, _) => 0,
        }
    }

    /// Open a session, evicting the principal's oldest beyond the bound.
    pub(crate) fn open_session(
        &mut self,
        open: SessionOpen<'_>,
        ctx: &ApplyContext<'_>,
    ) -> Result<(), IdentityRefusal> {
        if self.sessions.contains_key(open.session_hash) {
            return Err(IdentityRefusal::Collision);
        }
        let config = self.require_initialized()?.clone();
        let scopes = self.resolve(open.principal_id, ctx.classifier)?.scopes;
        let privileged = Self::is_privileged(&scopes, ctx);
        let (idle, absolute) = if privileged {
            (config.privileged_idle_ms, config.privileged_absolute_ms)
        } else {
            (config.idle_ms, config.absolute_ms)
        };
        let now_ms = ctx.now_ms;
        self.sessions.insert(
            open.session_hash.to_string(),
            SessionRecord {
                session_hash: open.session_hash.to_string(),
                principal_id: open.principal_id.to_string(),
                created_at_ms: now_ms,
                last_seen_at_ms: now_ms,
                idle_expires_at_ms: now_ms.saturating_add(idle),
                absolute_expires_at_ms: now_ms.saturating_add(absolute),
                auth_methods: vec![open.method],
                mfa_pending: open.mfa_pending,
                mfa_at_ms: None,
                ip_prefix: open.ip_prefix,
                revoked_at_ms: None,
                revoke_reason: None,
            },
        );
        self.prune_sessions(open.principal_id, now_ms);
        Ok(())
    }

    /// Drop ended sessions of `principal_id` and evict its oldest live ones
    /// past the per-principal bound.
    fn prune_sessions(&mut self, principal_id: &str, now_ms: u64) {
        self.sessions
            .retain(|_, session| session.principal_id != principal_id || session.is_live(now_ms));
        let mut live: Vec<(u64, String)> = self
            .sessions
            .values()
            .filter(|session| session.principal_id == principal_id)
            .map(|session| (session.created_at_ms, session.session_hash.clone()))
            .collect();
        live.sort();
        let excess = live.len().saturating_sub(MAX_SESSIONS_PER_USER);
        for (_, hash) in live.into_iter().take(excess) {
            self.sessions.remove(&hash);
        }
    }

    pub(crate) fn revoke_session(&mut self, hash: &str, now_ms: u64, reason: &str) -> bool {
        match self.sessions.get_mut(hash) {
            Some(session) if session.revoked_at_ms.is_none() => {
                session.revoked_at_ms = Some(now_ms);
                session.revoke_reason = Some(reason.to_string());
                true
            }
            Some(_) | None => false,
        }
    }

    pub(crate) fn revoke_principal_sessions(
        &mut self,
        principal_id: &str,
        now_ms: u64,
        reason: &str,
    ) -> bool {
        let hashes: Vec<String> = self
            .sessions
            .values()
            .filter(|session| session.principal_id == principal_id)
            .map(|session| session.session_hash.clone())
            .collect();
        let mut changed = false;
        for hash in hashes {
            changed |= self.revoke_session(&hash, now_ms, reason);
        }
        self.void_verdicts(principal_id);
        changed
    }

    /// Every session ends (mode transitions): all are dropped, so no token
    /// minted under the previous mode can be refreshed from one, and every
    /// password verdict computed before the transition is void.
    pub(crate) fn revoke_all_sessions(&mut self) {
        self.sessions.clear();
        self.void_all_verdicts();
    }
}
