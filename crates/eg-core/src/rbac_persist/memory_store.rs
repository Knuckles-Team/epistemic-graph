use super::{
    AgentIdentity, BTreeMap, IdentityBootstrapState, RbacAuthoritySnapshot, RbacPersistError,
    RbacPolicy, RbacPolicyStore,
};

/// Current in-process policy store used by embedded/test isolation layers that do not
/// have a filesystem persistence directory. It preserves the same atomic full-image
/// contract as redb; there is no absent or no-op persistence state.
#[derive(Debug, Default)]
pub struct MemoryRbacStore {
    /// Policy, identities, bootstrap state, and the store revision under ONE
    /// lock so `authority_snapshot` cannot compose a revision with a policy
    /// image from a different checked write.
    ///
    /// GRAPH-POLICY-LEASE-CONTRACT.md §2.4 (R5): this store has no
    /// mutation ledger to reuse an existing durable counter from
    /// (unlike `RbacStore`), so the fourth slot is a genuinely new counter,
    /// incremented once per successful checked write -- the in-memory analogue of the
    /// same "monotonic, bumped once per successful save" property.
    state: parking_lot::RwLock<(
        RbacPolicy,
        BTreeMap<String, AgentIdentity>,
        IdentityBootstrapState,
        u64,
    )>,
}

impl MemoryRbacStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl RbacPolicyStore for MemoryRbacStore {
    fn load(
        &self,
    ) -> Result<
        (
            RbacPolicy,
            BTreeMap<String, AgentIdentity>,
            IdentityBootstrapState,
        ),
        RbacPersistError,
    > {
        let state = self.state.read();
        Ok((state.0.clone(), state.1.clone(), state.2))
    }

    fn compare_and_save(
        &self,
        expected_revision: u64,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<u64, RbacPersistError> {
        let mut state = self.state.write();
        if state.3 != expected_revision {
            return Err(RbacPersistError::StaleRevision {
                expected: expected_revision,
                actual: state.3,
            });
        }
        let revision = state.3.checked_add(1).ok_or_else(|| {
            RbacPersistError::Redb("identity/RBAC state version overflow".to_string())
        })?;
        *state = (policy.clone(), identities.clone(), bootstrap, revision);
        Ok(revision)
    }

    fn authority_snapshot(&self) -> Result<RbacAuthoritySnapshot, RbacPersistError> {
        let state = self.state.read();
        RbacAuthoritySnapshot::from_parts(state.3, state.0.clone(), state.1.clone(), state.2)
    }

    fn current_version(&self) -> Result<u64, RbacPersistError> {
        Ok(self.state.read().3)
    }
}
