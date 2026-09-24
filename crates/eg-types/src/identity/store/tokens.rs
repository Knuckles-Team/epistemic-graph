//! One-time tokens and API keys. Both are caller-generated high-entropy
//! secrets; only their SHA-256 hashes (stamped at the boundary) are stored.

use std::collections::BTreeSet;

use super::super::audit::IdentityEvent;
use super::super::model::{ApiKeyRecord, OneTimeToken, TokenPurpose};
use super::super::ops::TokenOp;
use super::super::requests::{ApiKeyIssue, OneTimeTokenIssue, TokenRedeem};
use super::super::scope::ScopeClassifier;
use super::super::stamp::IdentityStamp;
use super::super::text::identifier;
use super::super::views::IdentityReply;
use super::super::{
    IdentityRefusal, MAX_API_KEYS_PER_USER, MAX_API_KEY_LIFETIME_MS, MAX_ONE_TIME_TOKENS,
    MAX_ONE_TIME_TOKEN_LIFETIME_MS,
};
use super::{ApplyContext, IdentityStore};

/// Which purposes must name a principal.
fn purpose_names_principal(purpose: TokenPurpose) -> bool {
    match purpose {
        TokenPurpose::PasswordReset
        | TokenPurpose::EmailVerify
        | TokenPurpose::LinkClaim
        | TokenPurpose::AdminReset => true,
        TokenPurpose::Invite => false,
    }
}

impl IdentityStore {
    pub(super) fn apply_token(
        &mut self,
        op: &TokenOp,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        match op {
            TokenOp::IssueOneTime { request } => self.issue_one_time(request, stamp, ctx),
            TokenOp::RedeemOneTime { request } => self.redeem_one_time(request, stamp, ctx.now_ms),
            TokenOp::IssueApiKey { request } => self.issue_api_key(request, stamp, ctx),
            TokenOp::VerifyApiKey { request } => {
                let resolution = self.use_api_key(&request.key_id, stamp, ctx)?;
                Ok(IdentityReply::Resolution(resolution))
            }
            TokenOp::RevokeApiKey { request } => {
                let key = self
                    .api_keys
                    .get_mut(&request.id)
                    .ok_or(IdentityRefusal::NotFound)?;
                let changed = key.revoked_at_ms.is_none();
                key.revoked_at_ms.get_or_insert(ctx.now_ms);
                self.audit_event(
                    stamp,
                    ctx.now_ms,
                    IdentityEvent::ApiKeyRevoked,
                    Some(&request.id),
                );
                Ok(IdentityReply::Done { changed })
            }
        }
    }

    /// Issue a token for an administrator's live session. Token hash 0 is
    /// the session; hash 1 is the new token.
    fn issue_one_time(
        &mut self,
        request: &OneTimeTokenIssue,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let issuer = self.admin_subject(stamp, ctx)?;
        let now_ms = ctx.now_ms;
        let lifetime_ok = (1..=MAX_ONE_TIME_TOKEN_LIFETIME_MS).contains(&request.ttl_ms);
        if !lifetime_ok
            || purpose_names_principal(request.purpose) != request.principal_id.is_some()
        {
            return Err(IdentityRefusal::InvalidRequest);
        }
        if let Some(principal) = &request.principal_id {
            self.users.get(principal).ok_or(IdentityRefusal::NotFound)?;
        }
        let hash = stamp.token_hash(1)?.to_string();
        self.one_time
            .retain(|_, token| token.used_at_ms.is_none() && token.expires_at_ms > now_ms);
        if self.one_time.len() >= MAX_ONE_TIME_TOKENS {
            return Err(IdentityRefusal::Full);
        }
        if self.one_time.contains_key(&hash) {
            return Err(IdentityRefusal::Collision);
        }
        self.one_time.insert(
            hash.clone(),
            OneTimeToken {
                token_hash: hash,
                purpose: request.purpose,
                principal_id: request.principal_id.clone(),
                expires_at_ms: now_ms.saturating_add(request.ttl_ms),
                used_at_ms: None,
                created_by: issuer,
            },
        );
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::TokenIssued,
            request.principal_id.as_deref(),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    /// Spend a token exactly once, for exactly its purpose.
    fn spend_token(
        &mut self,
        hash: &str,
        purpose: TokenPurpose,
        now_ms: u64,
    ) -> Result<Option<String>, IdentityRefusal> {
        let token = self
            .one_time
            .get_mut(hash)
            .ok_or(IdentityRefusal::TokenSpent)?;
        let usable =
            token.purpose == purpose && token.used_at_ms.is_none() && now_ms < token.expires_at_ms;
        if !usable {
            return Err(IdentityRefusal::TokenSpent);
        }
        token.used_at_ms = Some(now_ms);
        Ok(token.principal_id.clone())
    }

    fn redeem_one_time(
        &mut self,
        request: &TokenRedeem,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let hash = stamp.token_hash(0)?.to_string();
        let principal = self.spend_token(&hash, request.purpose, now_ms)?;
        match (request.purpose, principal) {
            (TokenPurpose::PasswordReset | TokenPurpose::AdminReset, Some(principal)) => {
                let new_hash = stamp.new_password_hash()?.to_string();
                self.store_password(&principal, &new_hash, false, now_ms);
                self.revoke_principal_sessions(&principal, now_ms, "password_reset");
                self.audit_event(
                    stamp,
                    now_ms,
                    IdentityEvent::TokenRedeemed,
                    Some(&principal),
                );
                Ok(IdentityReply::Principal {
                    principal_id: principal,
                })
            }
            (TokenPurpose::LinkClaim, Some(principal)) => {
                let link = request
                    .link
                    .as_ref()
                    .ok_or(IdentityRefusal::InvalidRequest)?;
                self.link(&link.idp_id, &link.subject, &principal, stamp, now_ms)?;
                Ok(IdentityReply::Principal {
                    principal_id: principal,
                })
            }
            (TokenPurpose::EmailVerify | TokenPurpose::Invite, principal) => {
                self.audit_event(
                    stamp,
                    now_ms,
                    IdentityEvent::TokenRedeemed,
                    principal.as_deref(),
                );
                Ok(IdentityReply::Done { changed: true })
            }
            (
                TokenPurpose::PasswordReset | TokenPurpose::AdminReset | TokenPurpose::LinkClaim,
                None,
            ) => Err(IdentityRefusal::InvalidRequest),
        }
    }

    fn issue_api_key(
        &mut self,
        request: &ApiKeyIssue,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<IdentityReply, IdentityRefusal> {
        self.admin_subject(stamp, ctx)?;
        identifier(&request.key_id)?;
        let lifetime_ok = (1..=MAX_API_KEY_LIFETIME_MS).contains(&request.ttl_ms);
        if !lifetime_ok || request.scopes.is_empty() {
            return Err(IdentityRefusal::InvalidRequest);
        }
        if self.api_keys.contains_key(&request.key_id) {
            return Err(IdentityRefusal::Collision);
        }
        let owned =
            self.api_key_scopes_allowed(&request.principal_id, &request.scopes, ctx.classifier)?;
        if !owned {
            return Err(IdentityRefusal::ClassViolation);
        }
        let live = self
            .api_keys
            .values()
            .filter(|key| key.principal_id == request.principal_id && key.revoked_at_ms.is_none())
            .count();
        if live >= MAX_API_KEYS_PER_USER {
            return Err(IdentityRefusal::Full);
        }
        self.api_keys.insert(
            request.key_id.clone(),
            ApiKeyRecord {
                key_id: request.key_id.clone(),
                principal_id: request.principal_id.clone(),
                secret_hash: stamp.token_hash(1)?.to_string(),
                scopes: request.scopes.clone(),
                created_at_ms: ctx.now_ms,
                expires_at_ms: ctx.now_ms.saturating_add(request.ttl_ms),
                last_used_at_ms: None,
                revoked_at_ms: None,
            },
        );
        self.audit_event(
            stamp,
            ctx.now_ms,
            IdentityEvent::ApiKeyIssued,
            Some(&request.key_id),
        );
        Ok(IdentityReply::Done { changed: true })
    }

    /// Whether every scope is one an API key may carry AND the owner holds.
    fn api_key_scopes_allowed(
        &self,
        principal_id: &str,
        scopes: &BTreeSet<String>,
        classifier: &dyn ScopeClassifier,
    ) -> Result<bool, IdentityRefusal> {
        let owner = self.resolve(principal_id, classifier)?;
        Ok(scopes.iter().all(|scope| {
            owner.scopes.contains(scope)
                && classifier
                    .class_of(scope)
                    .is_some_and(|class| class.allows_api_key())
        }))
    }

    /// Verify a key and answer its owner's resolution NARROWED to the key's
    /// scopes as they stand now: an owner who lost a scope narrows the key.
    fn use_api_key(
        &mut self,
        key_id: &str,
        stamp: &IdentityStamp,
        ctx: &ApplyContext<'_>,
    ) -> Result<super::super::PrincipalResolution, IdentityRefusal> {
        let hash = stamp.token_hash(0)?;
        let key = self.api_keys.get(key_id).ok_or(IdentityRefusal::NotFound)?;
        let usable = key.secret_hash == hash
            && key.revoked_at_ms.is_none()
            && ctx.now_ms < key.expires_at_ms
            && self.is_active(&key.principal_id);
        if !usable {
            return Err(IdentityRefusal::NotFound);
        }
        let key_scopes = key.scopes.clone();
        let key = key.clone();
        let mut resolution = self.resolve(&key.principal_id, ctx.classifier)?;
        resolution.scopes = resolution
            .scopes
            .intersection(&key_scopes)
            .cloned()
            .collect();
        resolution.scopes.retain(|scope| {
            ctx.classifier
                .class_of(scope)
                .is_some_and(|class| class.allows_api_key())
        });
        let stale = key
            .last_used_at_ms
            .is_none_or(|used| ctx.now_ms >= used.saturating_add(super::TOUCH_GRANULARITY_MS));
        if stale {
            if let Some(key) = self.api_keys.get_mut(key_id) {
                key.last_used_at_ms = Some(ctx.now_ms);
            }
            self.audit_event(stamp, ctx.now_ms, IdentityEvent::ApiKeyUsed, Some(key_id));
        }
        Ok(resolution)
    }

    pub(crate) fn revoke_principal_api_keys(&mut self, principal_id: &str, now_ms: u64) {
        for key in self.api_keys.values_mut() {
            if key.principal_id == principal_id {
                key.revoked_at_ms.get_or_insert(now_ms);
            }
        }
    }
}
