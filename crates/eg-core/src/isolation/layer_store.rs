use super::*;

impl Default for IsolationLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl IsolationLayer {
    pub fn new() -> Self {
        IsolationLayer {
            agents: HashMap::new(),
            #[cfg(feature = "security")]
            rbac: crate::rbac::RbacPolicy::new(),
            #[cfg(feature = "security")]
            identity_bootstrap: crate::rbac_persist::IdentityBootstrapState::Pending,
            #[cfg(feature = "security")]
            persist: Some(std::sync::Arc::new(
                crate::rbac_persist::MemoryRbacStore::new(),
            )),
        }
    }

    /// Open an [`IsolationLayer`] backed by the durable, kernel-owned RBAC
    /// store at `dir`.
    ///
    /// `verifier` is the composition root's scope-grant proof authority
    /// (RF-RULING-004): the storage kernel owns the physical file, and only the
    /// root may decide that `principal` is entitled to serve its
    /// `security-control` scope. This layer never interprets the proof bytes.
    #[cfg(feature = "security")]
    pub fn with_persist_dir<P: AsRef<std::path::Path>>(
        dir: P,
        verifier: &dyn eg_storage::ScopeGrantVerifier,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self, crate::rbac_persist::RbacPersistError> {
        let store = crate::rbac_persist::RbacStore::open(dir, verifier, principal, proof)?;
        let (rbac, identities, identity_bootstrap) = store.load()?;
        if identity_bootstrap == crate::rbac_persist::IdentityBootstrapState::Pending
            && !holds_only_identity_store_state(&rbac, &identities)
        {
            return Err(crate::rbac_persist::RbacPersistError::IncompleteState(
                "pending identity bootstrap requires an empty policy and identity map",
            ));
        }
        let agents: HashMap<String, AgentIdentity> = identities.into_iter().collect();
        Ok(IsolationLayer {
            agents,
            rbac,
            identity_bootstrap,
            persist: Some(std::sync::Arc::new(store)),
        })
    }

    /// Return the durable identity/RBAC policy store bound to this layer.
    #[cfg(feature = "security")]
    pub fn policy_store(&self) -> Option<std::sync::Arc<dyn crate::rbac_persist::RbacPolicyStore>> {
        self.persist.clone()
    }

    /// Write through the complete RBAC and identity state.
    #[cfg(feature = "security")]
    pub(super) fn persist_state(&self) -> Result<(), String> {
        let store = self
            .persist
            .as_ref()
            .ok_or_else(|| "identity/RBAC policy store is not bound".to_string())?;
        let identities: std::collections::BTreeMap<String, AgentIdentity> = self
            .agents
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        store
            .save(&self.rbac, &identities, self.identity_bootstrap)
            .map_err(|error| format!("identity/RBAC policy save failed: {error}"))
    }
}

/// Whether a policy image still pending its System bootstrap holds nothing but
/// what the identity store projects (its `idm:` roles and grants and the
/// principals it manages). Anything else before bootstrap is corruption.
#[cfg(feature = "security")]
pub(super) fn holds_only_identity_store_state(
    rbac: &crate::rbac::RbacPolicy,
    identities: &std::collections::BTreeMap<String, AgentIdentity>,
) -> bool {
    store_owns_everything(rbac, identities.keys())
}

/// Whether every principal in `principals`, every role and every grant is
/// the identity store's own projection (vacuously true when all are empty).
#[cfg(feature = "security")]
pub(super) fn store_owns_everything<'a>(
    rbac: &crate::rbac::RbacPolicy,
    mut principals: impl Iterator<Item = &'a String>,
) -> bool {
    let owned = |name: &str| name.starts_with(eg_types::identity::RBAC_ROLE_PREFIX);
    let store = rbac.identity_store();
    principals.all(|principal| store.manages(principal))
        && rbac.roles().all(|role| owned(&role.name))
        && rbac.grants().iter().all(|grant| owned(&grant.role))
}

/// A policy-image transition's failure: its own refusal, or the write.
#[cfg(feature = "security")]
pub(super) enum PolicyWriteError<E> {
    Refused(E),
    Persist(String),
}

#[cfg(feature = "security")]
impl IsolationLayer {
    /// Apply `transition` to the RBAC policy image and write it through; a
    /// refusal or a failed write leaves the image exactly as it was. The one
    /// discipline behind elevations and governed changes.
    pub(super) fn transact_policy<R, E>(
        &mut self,
        transition: impl FnOnce(&mut crate::rbac::RbacPolicy) -> Result<R, E>,
    ) -> Result<R, PolicyWriteError<E>> {
        let previous = self.rbac.clone();
        let outcome = match transition(&mut self.rbac) {
            Ok(outcome) => outcome,
            Err(refusal) => {
                self.rbac = previous;
                return Err(PolicyWriteError::Refused(refusal));
            }
        };
        if let Err(error) = self.persist_state() {
            self.rbac = previous;
            return Err(PolicyWriteError::Persist(error));
        }
        Ok(outcome)
    }
}
