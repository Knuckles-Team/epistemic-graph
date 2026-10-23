//! Session facts and renewal share the existing global authority revision.
use super::*;
use eg_types::identity::SessionOp;

impl IsolationLayer {
    pub(super) fn apply_session_authority(
        &mut self,
        op: &IdentityOp,
        stamp: &IdentityStamp,
        now_ms: u64,
        classifier: &dyn ScopeClassifier,
    ) -> Result<IdentityReply, IdentityStoreError> {
        let store = self.policy_store().ok_or_else(|| {
            IdentityStoreError::Persist("identity/RBAC policy store is not bound".into())
        })?;
        // with_policy_store reads image and revision in ONE authority snapshot.
        let mut current = Self::with_policy_store(store)
            .map_err(|error| IdentityStoreError::Persist(error.to_string()))?;
        if let IdentityOp::Session(SessionOp::RenewReference {
            expected_authority_revision,
            ..
        }) = op
        {
            if current.persist_revision != *expected_authority_revision {
                return Err(IdentityStoreError::Refused(
                    IdentityRefusal::PreconditionFailed,
                ));
            }
        }
        // A changed image persists with compare_and_save(current.persist_revision).
        // That comparison and the write share the owner's mutation lock, so a
        // concurrent revoke or unrelated authority write cannot be overwritten.
        let reply = current.apply_identity_image(op, stamp, now_ms, classifier)?;
        let IdentityReply::SessionRecord(record) = reply else {
            return Err(IdentityStoreError::Refused(
                IdentityRefusal::PreconditionFailed,
            ));
        };
        let scopes = current
            .rbac
            .identity_store()
            .resolve(&record.principal_id, classifier)
            .map_err(IdentityStoreError::Refused)?
            .scopes;
        let authority_revision = current.persist_revision;
        *self = current;
        Ok(IdentityReply::SessionAuthority {
            record,
            scopes,
            authority_revision,
        })
    }
}
