//! Broker-attested approval keys. The broker verifies possession; the native
//! store owns subject, tenant, current session, revision and lifecycle checks.

use super::super::approval_key::{
    ApprovalKeyBinding, ApprovalKeyLookup, ApprovalKeyRecord, ApprovalKeyRegistration,
    ApprovalKeyRevocation,
};
use super::super::{IdentityEvent, IdentityRefusal, IdentityReply, IdentityStamp, UserKind};
use super::sessions::PendingSession;
use super::IdentityStore;

const MAX_APPROVAL_KEYS_PER_PRINCIPAL: usize = 32;

impl IdentityStore {
    fn approval_key_subject(
        &self,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<(String, String), IdentityRefusal> {
        if stamp.actor.delegated {
            return Err(IdentityRefusal::NotAuthorized);
        }
        let principal = self.session_subject(stamp, now_ms, PendingSession::Refuse)?;
        let session = self
            .sessions
            .get(stamp.token_hash(0)?)
            .ok_or(IdentityRefusal::NotFound)?;
        let tenant = stamp
            .session_tenant
            .as_ref()
            .ok_or(IdentityRefusal::NotAuthorized)?;
        if session.session_tenant.as_ref() != Some(tenant)
            || self
                .users
                .get(&principal)
                .is_none_or(|user| user.kind != UserKind::Human)
        {
            return Err(IdentityRefusal::NotAuthorized);
        }
        Ok((principal, tenant.clone()))
    }

    fn validate_approval_key_revision(
        &self,
        binding: &ApprovalKeyBinding,
        principal: &str,
        tenant: &str,
        now_ms: u64,
    ) -> Result<(), IdentityRefusal> {
        match self.approval_keys.get(&binding.key_id) {
            Some(record) => {
                validate_replacement(record, binding, principal, tenant, now_ms)?;
            }
            None => {
                if binding.expected_revision != 0 {
                    return Err(IdentityRefusal::PreconditionFailed);
                }
                if self
                    .approval_keys
                    .values()
                    .filter(|key| key.principal_id == principal)
                    .count()
                    >= MAX_APPROVAL_KEYS_PER_PRINCIPAL
                {
                    return Err(IdentityRefusal::Full);
                }
            }
        }
        Ok(())
    }

    pub(super) fn register_approval_key(
        &mut self,
        request: &ApprovalKeyRegistration,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let binding = &request.binding;
        binding.validate()?;
        let (principal, tenant) = self.approval_key_subject(stamp, now_ms)?;
        if binding.principal_id != principal || binding.tenant != tenant {
            return Err(IdentityRefusal::NotAuthorized);
        }
        if binding.expires_at_ms <= now_ms {
            return Err(IdentityRefusal::InvalidRequest);
        }
        self.validate_approval_key_revision(binding, &principal, &tenant, now_ms)?;
        let revision = binding
            .expected_revision
            .checked_add(1)
            .ok_or(IdentityRefusal::PreconditionFailed)?;
        let record = ApprovalKeyRecord {
            key_id: binding.key_id.clone(),
            principal_id: principal.clone(),
            tenant,
            purpose: binding.purpose,
            public_key_hex: binding.public_key_hex.clone(),
            revision,
            expires_at_ms: binding.expires_at_ms,
            revoked_at_ms: None,
        };
        self.approval_keys
            .insert(record.key_id.clone(), record.clone());
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::ApprovalKeyRegistered,
            Some(&principal),
        );
        Ok(IdentityReply::ApprovalKey(record))
    }

    /// Public facts only. Lookup is not evidence of a live human credential.
    pub(super) fn inspect_approval_key(
        &self,
        request: &ApprovalKeyLookup,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let record = self.bound_approval_key(request, stamp)?;
        if record.revoked_at_ms.is_some()
            || record.expires_at_ms <= now_ms
            || !self.is_active(&record.principal_id)
        {
            return Err(IdentityRefusal::StaleCredential);
        }
        Ok(IdentityReply::ApprovalKey(record.clone()))
    }

    fn bound_approval_key(
        &self,
        request: &ApprovalKeyLookup,
        stamp: &IdentityStamp,
    ) -> Result<&ApprovalKeyRecord, IdentityRefusal> {
        let tenant = stamp
            .session_tenant
            .as_ref()
            .ok_or(IdentityRefusal::NotAuthorized)?;
        self.approval_keys
            .get(&request.key_id)
            .filter(|record| {
                record.principal_id == request.principal_id
                    && record.tenant == *tenant
                    && record.purpose == request.purpose
            })
            .ok_or(IdentityRefusal::NotFound)
    }

    pub(super) fn revoke_approval_key(
        &mut self,
        request: &ApprovalKeyRevocation,
        stamp: &IdentityStamp,
        now_ms: u64,
    ) -> Result<IdentityReply, IdentityRefusal> {
        let (principal, _) = self.approval_key_subject(stamp, now_ms)?;
        if request.key.principal_id != principal {
            return Err(IdentityRefusal::NotAuthorized);
        }
        let mut record = self.bound_approval_key(&request.key, stamp)?.clone();
        if record.revision != request.expected_revision {
            return Err(IdentityRefusal::PreconditionFailed);
        }
        if record.revoked_at_ms.is_some() {
            return Err(IdentityRefusal::StaleCredential);
        }
        record.revision = record
            .revision
            .checked_add(1)
            .ok_or(IdentityRefusal::PreconditionFailed)?;
        record.revoked_at_ms = Some(now_ms);
        self.approval_keys
            .insert(record.key_id.clone(), record.clone());
        self.audit_event(
            stamp,
            now_ms,
            IdentityEvent::ApprovalKeyRevoked,
            Some(&principal),
        );
        Ok(IdentityReply::ApprovalKey(record))
    }
}

fn validate_replacement(
    record: &ApprovalKeyRecord,
    binding: &ApprovalKeyBinding,
    principal: &str,
    tenant: &str,
    now_ms: u64,
) -> Result<(), IdentityRefusal> {
    if record.principal_id != principal
        || record.tenant != tenant
        || record.purpose != binding.purpose
    {
        return Err(IdentityRefusal::NotFound);
    }
    if record.revoked_at_ms.is_some() || record.expires_at_ms <= now_ms {
        return Err(IdentityRefusal::StaleCredential);
    }
    if record.revision != binding.expected_revision {
        return Err(IdentityRefusal::PreconditionFailed);
    }
    Ok(())
}
