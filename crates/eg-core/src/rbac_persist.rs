//! Durable RBAC / identity persistence (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence, feature `security`).
//!
//! The RBAC evaluator (CONCEPT:EG-KG.compute.feature, [`crate::rbac::RbacPolicy`]) and the
//! registered [`AgentIdentity`] set both live in-memory in the
//! [`IsolationLayer`](crate::isolation::IsolationLayer). CONCEPT:EG-KG.compute.durable-rbac-identity-persistence makes that
//! state **durable**: it is written through to a redb table on every
//! `RbacAdmin`/`register_identity` mutation and reloaded at boot, so roles, grants
//! and identities survive a process restart.
//!
//! Design (mirrors the redb-backed cold tier, CONCEPT:EG-KG.coordination.distributed-cache-coherence):
//!   * ONE redb table `rbac_v1` in `{persist_dir}/rbac.redb` (a separate file, like
//!     the blob CAS / cold tier), three well-known keys:
//!       - `policy`     → serde_json bytes of the whole [`RbacPolicy`];
//!       - `identities` → serde_json bytes of a `BTreeMap<agent_id, AgentIdentity>`;
//!       - `bootstrap`  → the one-time identity-bootstrap state.
//!   * The identity map is a `BTreeMap` so the persisted bytes are **deterministic**
//!     (stable key order); a save→reopen always restores the identical logical state.
//!   * A brand-new store is atomically bootstrapped with the current empty,
//!     default-deny policy image plus an explicit empty identity map. A partial or
//!     absent image after bootstrap is corruption and fails closed at boot.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use eg_storage::{
    backup_strict_recovery_store, OwnedStoreHandle, PhysicalStoreIdentity, RbacOwner, ScopedRead,
    ScopeGrantVerifier, StorageKernelV1,
};
use eg_transaction::MutationKernelV1;
use redb::TableDefinition;
use sha2::{Digest, Sha256};

use crate::acl::AgentIdentity;
use crate::rbac::RbacPolicy;

/// `key → serde_json bytes`. One table, three well-known keys (`policy`,
/// `identities`, `bootstrap`) written in a single durable transaction
/// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
const RBAC_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("rbac_v1");
const POLICY_KEY: &str = "policy";
const IDENTITIES_KEY: &str = "identities";
const BOOTSTRAP_KEY: &str = "bootstrap";

/// Native mutation-scope identity for this store's RBAC/security-control
/// bookkeeping: tenant `"native"`, domain `ControlPlane` (mirrored below onto
/// every `MutationOperation::domain` this store writes — the native-scope
/// validator in `eg_types::mutation_batch::validation` requires the two to be
/// exactly equal), resource `"security-control"`.
const RBAC_SCOPE_TENANT: &str = "native";
const RBAC_SCOPE_RESOURCE: &str = "security-control";
/// Fixed logical-generation id for the RBAC/security-control scope. NOT
/// derived from the resource name above: unlike a graph, this scope is never
/// deleted and recreated with a new generation for the life of one
/// `rbac.redb` file, so every `RbacStore::open` of the same physical file
/// must bind (and re-validate against) the exact same identity or fail
/// closed (the storage kernel's idempotent scope-rebinding check).
const RBAC_SCOPE_INCARNATION: &str = "rbac-security-control:v1";

/// Operator-facing identity of the ONE physical `rbac.redb` owner file
/// (`eg_storage::PhysicalStoreIdentity`). Deliberately independent of the
/// logical serving scope above: it names the physical authority boundary the
/// storage kernel stamps into the owner manifest, so a file created for some
/// other owner can never be opened as this one.
const RBAC_PHYSICAL_STORE: &str = "eg-core:rbac-security-control";

/// Build the fixed [`eg_types::MutationScopeIdentity`] for this store's
/// native RBAC/security-control mutation scope (see the `RBAC_SCOPE_*`
/// constants above). A plain function rather than a `once_cell`/`const`:
/// `MutationScopeIdentity::native` computes a SHA-256 identity digest, which
/// is not `const`-evaluable, and every constructor here is fallible by
/// construction (`TenantId`/`LogicalName`/`IncarnationId` validate their
/// input), so failures are propagated rather than `.unwrap()`/`.expect()`'d
/// away even though the fixed literals above are known-valid by inspection.
fn native_security_control_identity() -> Result<eg_types::MutationScopeIdentity, RbacPersistError> {
    eg_types::MutationScopeIdentity::fixed_native(
        RBAC_SCOPE_TENANT,
        eg_types::mutation_batch::MutationDomain::ControlPlane,
        RBAC_SCOPE_RESOURCE,
        RBAC_SCOPE_INCARNATION,
    )
    .map_err(RbacPersistError::Redb)
}

/// Durable lifecycle for the only request admitted before an identity exists.
/// `Consumed` is never inferred from an empty identity map: removing every
/// identity therefore fails closed instead of silently reopening bootstrap.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityBootstrapState {
    #[default]
    Pending,
    Consumed,
}

/// Errors from the durable RBAC store (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). redb's several fallible
/// surfaces are flattened to a message string (matching the cold-tier convention);
/// io + serde carry their native errors so callers can inspect them.
#[derive(Debug)]
pub enum RbacPersistError {
    /// Creating the persist dir / opening the redb file failed.
    Io(std::io::Error),
    /// (De)serializing the policy or the identity map failed.
    Serde(serde_json::Error),
    /// A redb transaction/table/storage/commit operation failed.
    Redb(String),
    /// One or both mandatory current state records are absent.
    IncompleteState(&'static str),
}

impl fmt::Display for RbacPersistError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RbacPersistError::Io(e) => write!(f, "rbac persist io error: {e}"),
            RbacPersistError::Serde(e) => write!(f, "rbac persist serde error: {e}"),
            RbacPersistError::Redb(e) => write!(f, "rbac persist redb error: {e}"),
            RbacPersistError::IncompleteState(message) => {
                write!(f, "rbac persist current-state error: {message}")
            }
        }
    }
}

impl std::error::Error for RbacPersistError {}

impl From<std::io::Error> for RbacPersistError {
    fn from(e: std::io::Error) -> Self {
        RbacPersistError::Io(e)
    }
}

impl From<serde_json::Error> for RbacPersistError {
    fn from(e: serde_json::Error) -> Self {
        RbacPersistError::Serde(e)
    }
}

/// One atomic authorization image used by every policy decision lease.
///
/// The revision and both canonical digests are captured from the same store
/// snapshot as the policy and identity map. Consumers must never reconstruct
/// this object from separate `load` and `current_version` calls.
pub struct RbacAuthoritySnapshot {
    revision: u64,
    policy_digest: String,
    identity_digest: String,
    policy: RbacPolicy,
    identities: BTreeMap<String, AgentIdentity>,
}

impl RbacAuthoritySnapshot {
    fn from_parts(
        revision: u64,
        policy: RbacPolicy,
        identities: BTreeMap<String, AgentIdentity>,
    ) -> Result<Self, RbacPersistError> {
        Ok(Self {
            revision,
            policy_digest: authority_policy_digest(&policy)?,
            identity_digest: authority_identity_digest(&identities)?,
            policy,
            identities,
        })
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn policy_digest(&self) -> &str {
        &self.policy_digest
    }

    pub fn identity_digest(&self) -> &str {
        &self.identity_digest
    }

    pub fn policy(&self) -> &RbacPolicy {
        &self.policy
    }

    pub fn identities(&self) -> &BTreeMap<String, AgentIdentity> {
        &self.identities
    }
}

fn authority_policy_digest(policy: &RbacPolicy) -> Result<String, RbacPersistError> {
    #[derive(serde::Serialize)]
    struct CanonicalPolicy<'a> {
        roles: BTreeMap<&'a str, &'a crate::acl::Role>,
        grants: &'a [crate::acl::Grant],
    }
    let canonical = CanonicalPolicy {
        roles: policy
            .roles()
            .map(|role| (role.name.as_str(), role))
            .collect(),
        grants: policy.grants(),
    };
    let mut hasher = Sha256::new();
    hasher.update(b"eg/rbac-policy-snapshot/v1\0");
    hasher.update(serde_json::to_vec(&canonical)?);
    Ok(hex::encode(hasher.finalize()))
}

fn authority_identity_digest(
    identities: &BTreeMap<String, AgentIdentity>,
) -> Result<String, RbacPersistError> {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/rbac-identity-snapshot/v1\0");
    hasher.update(serde_json::to_vec(identities)?);
    Ok(hex::encode(hasher.finalize()))
}

fn read_authority_image(
    read: &ScopedRead<'_, RbacOwner>,
) -> Result<(RbacPolicy, BTreeMap<String, AgentIdentity>), RbacPersistError> {
    let state = read.open_table(RBAC_TABLE).map_err(RbacPersistError::Redb)?;
    let policy = state
        .get(POLICY_KEY)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?
        .ok_or(RbacPersistError::IncompleteState(
            "mandatory policy record is absent",
        ))
        .and_then(|value| serde_json::from_slice(value.value()).map_err(RbacPersistError::Serde))?;
    let identities = state
        .get(IDENTITIES_KEY)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?
        .ok_or(RbacPersistError::IncompleteState(
            "mandatory identities record is absent",
        ))
        .and_then(|value| serde_json::from_slice(value.value()).map_err(RbacPersistError::Serde))?;
    let _: IdentityBootstrapState = state
        .get(BOOTSTRAP_KEY)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?
        .ok_or(RbacPersistError::IncompleteState(
            "mandatory identity bootstrap record is absent",
        ))
        .and_then(|value| serde_json::from_slice(value.value()).map_err(RbacPersistError::Serde))?;
    Ok((policy, identities))
}

/// A durable, kernel-backed image of the RBAC policy + registered identities
/// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
///
/// RF-RULING-004: this domain crate owns no physical authority. The
/// [`StorageKernelV1`] it holds is the sole owner of the physical `rbac.redb`
/// file, opened under the declared [`eg_storage::OwnerLayout::Rbac`] whose only
/// owner table is `rbac_v1`; every read is a kernel-issued
/// [`ScopedRead`] and every write-through is admitted, ordered and committed by
/// the [`MutationKernelV1`] that holds the file's single move-once mutation
/// authority. `owner` is the one authenticated, bound serving scope (see
/// `native_security_control_identity`), validated once at open.
pub struct RbacStore {
    kernel: StorageKernelV1,
    mutations: MutationKernelV1,
    owner: OwnedStoreHandle<RbacOwner>,
}

/// Adapter seam for durable identity/RBAC policy state.  Production uses
/// [`RbacStore`]; tests or alternate storage backends can implement the same
/// atomic snapshot contract without changing [`IsolationLayer`](crate::isolation::IsolationLayer).
pub trait RbacPolicyStore: Send + Sync {
    fn load(
        &self,
    ) -> Result<
        (
            RbacPolicy,
            BTreeMap<String, AgentIdentity>,
            IdentityBootstrapState,
        ),
        RbacPersistError,
    >;
    fn save(
        &self,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<(), RbacPersistError>;

    /// Capture revision, policy, identities, and their canonical digests from
    /// one store snapshot. This is the only valid input to a policy decision
    /// lease; separate reads are intentionally insufficient.
    fn authority_snapshot(&self) -> Result<RbacAuthoritySnapshot, RbacPersistError>;

    /// Copy this store's durable image verbatim into a FRESH bundle file at
    /// `destination`, returning the number of rows copied.
    ///
    /// `None` ⇒ this adapter has no on-disk file to bundle (the in-memory store).
    /// An online backup that omits this file restores an engine with NO RBAC or
    /// identity state at all — every sign-in fails closed — so the durable
    /// implementation MUST override this.
    fn backup_into(&self, _destination: &Path) -> Option<Result<u64, String>> {
        None
    }

    /// `true` when this adapter owns an on-disk file that a backup must capture.
    fn has_durable_file(&self) -> bool {
        false
    }

    /// Audit/display-facing durable-policy epoch (GRAPH-POLICY-LEASE-CONTRACT.md
    /// §2.4, R5) — reuses the SAME monotonic counter the mutation kernel
    /// already bumps atomically with every [`RbacStore::save`] write, rather
    /// than adding a second, independently-maintained persisted counter.
    /// **Never the staleness ground truth**: `PolicyDecisionLease`'s fail-closed
    /// comparison is digest-only, always — this is populated onto
    /// `PolicySnapshot.version` purely for audit/display (e.g. "policy epoch
    /// 4,812"), and is a spuriously-bumpable approximation, not a strict
    /// count of logical policy changes (`RbacStore::save`'s `already_current`
    /// gate compares raw, non-canonical bytes — see that method's doc).
    ///
    /// Default body **fails closed** rather than returning a plausible-
    /// looking `0` — a store that cannot report its own epoch must not be
    /// usable to mint or revalidate a lease (contract §8 item 13). This
    /// This default remains for non-lease consumers; every store used to mint
    /// a lease must implement [`Self::authority_snapshot`] and returns that
    /// snapshot's revision instead of composing separate reads.
    fn current_version(&self) -> Result<u64, RbacPersistError> {
        Err(RbacPersistError::IncompleteState(
            "this policy store does not implement a durable policy version counter",
        ))
    }
}

mod durable_write;
mod memory_store;

pub use memory_store::MemoryRbacStore;

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use eg_storage::OwnerLayout;

    /// The one principal eg-core's own tests serve the fixed
    /// `security-control` scope as.
    pub(crate) const TEST_PRINCIPAL: &str =
        "principal:sha256:d70d97fc35a6e2dfbef26a2bca76a96c6dd2c4142ae2a14850deaf61b478bba0";
    pub(crate) const TEST_PROOF: &[u8] = b"eg-core-test-scope-grant";

    /// Stand-in composition root. Production supplies the real proof authority;
    /// this one still checks every field the kernel hands it, so a store opened
    /// with the wrong layout, scope or principal fails closed in tests too.
    pub(crate) struct TestScopeVerifier;

    impl ScopeGrantVerifier for TestScopeVerifier {
        fn verify(
            &self,
            _physical: &PhysicalStoreIdentity,
            layout: OwnerLayout,
            identity: &eg_types::MutationScopeIdentity,
            principal: &str,
            proof: &[u8],
        ) -> Result<(), String> {
            if layout != OwnerLayout::Rbac
                || identity.tenant().as_str() != RBAC_SCOPE_TENANT
                || principal != TEST_PRINCIPAL
                || proof != TEST_PROOF
            {
                return Err("test scope authority rejected".to_string());
            }
            Ok(())
        }
    }

    /// Open the durable RBAC store the way the composition root would.
    pub(crate) fn open_test_store(
        dir: impl AsRef<Path>,
    ) -> Result<RbacStore, RbacPersistError> {
        RbacStore::open(dir, &TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF)
    }
}

impl fmt::Debug for RbacStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RbacStore").finish_non_exhaustive()
    }
}

impl RbacStore {
    /// Open (or create) `{dir}/rbac.redb` through the storage kernel
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). The dir is
    /// created if absent.
    ///
    /// The kernel creates the file under [`eg_storage::OwnerLayout::Rbac`],
    /// which declares `rbac_v1` as the layout's one owner table, so the
    /// hand-written bootstrap closure the retired raw constructor needed is
    /// gone: the declared census is opened atomically at create time and
    /// re-validated on every later open.
    ///
    /// `verifier` is the composition root's proof authority: only it may decide
    /// that `principal` is entitled to serve this store's fixed
    /// `security-control` scope. The domain crate supplies the scope identity
    /// and the layout; it never interprets the proof bytes.
    pub fn open<P: AsRef<Path>>(
        dir: P,
        verifier: &dyn ScopeGrantVerifier,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self, RbacPersistError> {
        std::fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("rbac.redb");
        let identity = native_security_control_identity()?;
        let physical =
            PhysicalStoreIdentity::new(RBAC_PHYSICAL_STORE).map_err(RbacPersistError::Redb)?;
        let kernel = if path.exists() {
            StorageKernelV1::open_owner::<RbacOwner>(&path, physical, None)
        } else {
            StorageKernelV1::create_owner::<RbacOwner>(&path, physical, None)
        }
        .map_err(RbacPersistError::Redb)?;
        let (kernel, authority) = kernel
            .into_read_and_mutation_authority()
            .map_err(RbacPersistError::Redb)?;
        let mutations = MutationKernelV1::new(authority);
        let grant = kernel
            .authenticate_scope::<RbacOwner>(verifier, identity, principal.to_string(), proof)
            .map_err(RbacPersistError::Redb)?;
        // `initial_version: 0` -- the SAME "a brand new store starts at version
        // 0" semantics `save`/`current_version` rely on. Binding the identical
        // grant again on a later open is idempotent; any generation, tenant,
        // domain, resource or initial-version mismatch fails closed.
        let owner = kernel
            .bind_serving_scope(grant, 0)
            .map_err(RbacPersistError::Redb)?;
        mutations
            .bootstrap_ledger(&owner)
            .map_err(RbacPersistError::Redb)?;
        let store = Self {
            kernel,
            mutations,
            owner,
        };
        store.bootstrap_current_state()?;
        Ok(store)
    }

    /// One kernel-issued scoped read over this store's bound serving scope.
    pub(super) fn scoped_read(&self) -> Result<ScopedRead<'_, RbacOwner>, RbacPersistError> {
        self.kernel
            .read_scope(&self.owner)
            .map_err(RbacPersistError::Redb)
    }

    /// Copy the durable RBAC/identity image into a FRESH file at `destination`
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence), returning the
    /// number of rows copied. Refuses to overwrite an existing file.
    ///
    /// Delegated whole to [`backup_strict_recovery_store`], which owns the
    /// complete table census: it copies the ledger rows, re-stamps
    /// `SCOPE_BINDINGS` for the destination's own incarnation, copies every
    /// declared owner table of the layout (here `rbac_v1`), and reopens the
    /// destination to prove the per-table fingerprints match the source.
    /// Hand-listing tables here is what produced BUG-PE-054, and appending
    /// `rbac_v1` afterwards through a private `Database::create` made this
    /// crate a second physical authority -- both are deleted.
    pub fn backup_into(&self, destination: &Path) -> Result<u64, String> {
        if destination.exists() {
            return Err("bundled store file already exists (refusing to overwrite)".to_string());
        }
        let identity = PhysicalStoreIdentity::new(RBAC_PHYSICAL_STORE)?;
        let evidence = backup_strict_recovery_store(&self.kernel, destination, identity)?;
        Ok(evidence.ledger_rows.saturating_add(evidence.owner_rows))
    }

    /// Atomically create the explicit current bootstrap image for a brand-new store.
    /// A partial image is never repaired because that could silently discard part
    /// of an authorization state transition.
    fn bootstrap_current_state(&self) -> Result<(), RbacPersistError> {
        let read = self.scoped_read()?;
        let table = read.open_table(RBAC_TABLE).map_err(RbacPersistError::Redb)?;
        let policy_present = table
            .get(POLICY_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
            .is_some();
        let identities_present = table
            .get(IDENTITIES_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
            .is_some();
        let bootstrap_present = table
            .get(BOOTSTRAP_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
            .is_some();
        drop(table);
        drop(read);
        match (policy_present, identities_present, bootstrap_present) {
            (true, true, true) => Ok(()),
            (false, false, false) => self.save(
                &RbacPolicy::new(),
                &BTreeMap::new(),
                IdentityBootstrapState::Pending,
            ),
            _ => Err(RbacPersistError::IncompleteState(
                "policy, identities, and bootstrap state must all be present",
            )),
        }
    }

    /// Load the mandatory persisted policy, identities, and bootstrap lifecycle
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Missing records are
    /// rejected; only [`RbacStore::open`] may create the explicit bootstrap image.
    pub fn load(
        &self,
    ) -> Result<
        (
            RbacPolicy,
            BTreeMap<String, AgentIdentity>,
            IdentityBootstrapState,
        ),
        RbacPersistError,
    > {
        let read = self.scoped_read()?;
        let t = read.open_table(RBAC_TABLE).map_err(RbacPersistError::Redb)?;
        let policy = match t
            .get(POLICY_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
        {
            Some(v) => serde_json::from_slice(v.value())?,
            None => {
                return Err(RbacPersistError::IncompleteState(
                    "mandatory policy record is absent",
                ))
            }
        };
        let identities = match t
            .get(IDENTITIES_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
        {
            Some(v) => serde_json::from_slice(v.value())?,
            None => {
                return Err(RbacPersistError::IncompleteState(
                    "mandatory identities record is absent",
                ))
            }
        };
        let bootstrap = match t
            .get(BOOTSTRAP_KEY)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?
        {
            Some(v) => serde_json::from_slice(v.value())?,
            None => {
                return Err(RbacPersistError::IncompleteState(
                    "mandatory identity bootstrap record is absent",
                ))
            }
        };
        Ok((policy, identities, bootstrap))
    }

    /// Read the complete authorization image and its mutation revision from
    /// one redb MVCC transaction. This prevents a lease from combining policy
    /// bytes from one commit with the revision or identities from another.
    pub fn authority_snapshot(&self) -> Result<RbacAuthoritySnapshot, RbacPersistError> {
        let read = self.scoped_read()?;
        let (policy, identities) = read_authority_image(&read)?;
        let revision = eg_transaction::version(&read).map_err(RbacPersistError::Redb)?;
        RbacAuthoritySnapshot::from_parts(revision, policy, identities)
    }

    /// Write-through the FULL RBAC state in ONE durable
    /// (immediate-fsync) transaction (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Re-serializing the whole
    /// (small, admin-scale) state on each mutation keeps the three keys mutually
    /// consistent and the write path trivially correct.
    pub fn save(
        &self,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<(), RbacPersistError> {
        durable_write::save_authority_state(self, policy, identities, bootstrap)
    }

    /// Remove one mandatory durable record through the mutation kernel, so a
    /// test can produce a known-bad partial image without opening a store.
    #[cfg(test)]
    pub(super) fn remove_mandatory_record_for_test(
        &self,
        key: &'static str,
    ) -> Result<(), RbacPersistError> {
        durable_write::remove_authority_record(self, key)
    }

    /// GRAPH-POLICY-LEASE-CONTRACT.md §2.4 (R5): the SAME mutation-ledger
    /// counter [`RbacStore::save`] already bumps atomically (inside the one
    /// admitted write transaction, above) with the policy/identity/bootstrap
    /// writes -- reused here rather than adding a second, independently-
    /// maintained persisted counter.
    pub fn current_version(&self) -> Result<u64, RbacPersistError> {
        let read = self.scoped_read()?;
        eg_transaction::version(&read).map_err(RbacPersistError::Redb)
    }
}

impl RbacPolicyStore for RbacStore {
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
        RbacStore::load(self)
    }

    fn save(
        &self,
        policy: &RbacPolicy,
        identities: &BTreeMap<String, AgentIdentity>,
        bootstrap: IdentityBootstrapState,
    ) -> Result<(), RbacPersistError> {
        RbacStore::save(self, policy, identities, bootstrap)
    }

    fn authority_snapshot(&self) -> Result<RbacAuthoritySnapshot, RbacPersistError> {
        RbacStore::authority_snapshot(self)
    }

    fn backup_into(&self, destination: &Path) -> Option<Result<u64, String>> {
        Some(RbacStore::backup_into(self, destination))
    }

    fn has_durable_file(&self) -> bool {
        true
    }

    fn current_version(&self) -> Result<u64, RbacPersistError> {
        RbacStore::current_version(self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::test_support::open_test_store;
    use super::{
        IdentityBootstrapState, MemoryRbacStore, RbacPersistError, RbacPolicyStore,
        IDENTITIES_KEY,
    };
    use crate::acl::{
        AgentIdentity, AgentRole, Grant, GrantEffect, RbacAction, ResourceContext,
        ResourceSelector, Role,
    };
    use crate::rbac::RbacPolicy;

    /// A unique temp dir per test invocation (no external dev-dep needed).
    fn tmp_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "eg303-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ))
    }

    fn identity(id: &str, roles: Vec<String>) -> AgentIdentity {
        AgentIdentity {
            agent_id: id.to_string(),
            role: AgentRole::Agent,
            teams: vec![],
            roles,
        }
    }

    #[test]
    fn eg303_store_saves_and_reloads_policy_and_identities() {
        let dir = tmp_dir("store-rt");
        let mut policy = RbacPolicy::new();
        policy.add_role(Role::with_parents("editor", vec!["reader".into()]));
        policy.add_grant(Grant {
            role: "editor".into(),
            resource: ResourceSelector::Label("Doc".into()),
            action: RbacAction::Write,
            effect: GrantEffect::Allow,
        });
        let mut identities = BTreeMap::new();
        identities.insert("sam".to_string(), identity("sam", vec!["editor".into()]));

        {
            let store = open_test_store(&dir).unwrap();
            store
                .save(&policy, &identities, IdentityBootstrapState::Consumed)
                .unwrap();
        }
        // Reopen the SAME dir — the state is durable across "process" lifetimes.
        let store = open_test_store(&dir).unwrap();
        let (loaded_policy, loaded_ids, bootstrap) = store.load().unwrap();
        assert_eq!(loaded_policy.grants().len(), 1);
        assert!(loaded_policy.is_allowed(
            &["editor"],
            &ResourceContext {
                graph: "g".into(),
                label: Some("Doc".into())
            },
            RbacAction::Write
        ));
        assert_eq!(loaded_ids.len(), 1);
        assert_eq!(loaded_ids["sam"].roles, vec!["editor".to_string()]);
        assert_eq!(bootstrap, IdentityBootstrapState::Consumed);
        let authority = store.authority_snapshot().unwrap();
        assert_eq!(authority.policy().grants().len(), 1);
        assert_eq!(
            authority.identities()["sam"].roles,
            vec!["editor".to_string()]
        );
        assert!(authority.revision() > 0);
        assert_eq!(authority.policy_digest().len(), 64);
        assert_eq!(authority.identity_digest().len(), 64);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn memory_authority_snapshot_captures_revision_and_state_under_one_lock() {
        let store = MemoryRbacStore::new();
        let mut identities = BTreeMap::new();
        identities.insert("sam".to_string(), identity("sam", vec!["reader".into()]));
        store
            .save(
                &RbacPolicy::new(),
                &identities,
                IdentityBootstrapState::Consumed,
            )
            .unwrap();

        let authority = store.authority_snapshot().unwrap();
        assert_eq!(authority.revision(), 1);
        assert_eq!(
            authority.identities()["sam"].roles,
            vec!["reader".to_string()]
        );
        assert_eq!(authority.policy_digest().len(), 64);
        assert_eq!(authority.identity_digest().len(), 64);
    }

    #[test]
    fn eg303_new_store_bootstraps_explicit_default_deny_state() {
        let dir = tmp_dir("absent");
        let store = open_test_store(&dir).unwrap();
        let (policy, ids, bootstrap) = store.load().unwrap();
        assert!(policy.grants().is_empty());
        assert!(ids.is_empty());
        assert_eq!(bootstrap, IdentityBootstrapState::Pending);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn consumed_bootstrap_never_reopens_when_identities_are_empty() {
        let dir = tmp_dir("bootstrap-consumed");
        {
            let store = open_test_store(&dir).unwrap();
            store
                .save(
                    &RbacPolicy::new(),
                    &BTreeMap::new(),
                    IdentityBootstrapState::Consumed,
                )
                .unwrap();
        }
        let store = open_test_store(&dir).unwrap();
        let (_, identities, bootstrap) = store.load().unwrap();
        assert!(identities.is_empty());
        assert_eq!(bootstrap, IdentityBootstrapState::Consumed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eg303_partial_current_state_fails_closed() {
        let dir = tmp_dir("partial");
        let store = open_test_store(&dir).unwrap();
        // A known-bad durable input, produced the ONLY way this crate can now
        // write: an admitted mutation through the mutation kernel. The store
        // must still refuse to serve, and refuse to reopen, a partial image.
        store.remove_mandatory_record_for_test(IDENTITIES_KEY).unwrap();
        assert!(matches!(
            store.load(),
            Err(RbacPersistError::IncompleteState(_))
        ));
        drop(store);
        assert!(matches!(
            open_test_store(&dir),
            Err(RbacPersistError::IncompleteState(_))
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eg303_identity_bytes_are_deterministic() {
        // The BTreeMap identity container gives a stable byte serialization: two
        // saves of the same logical state produce identical persisted bytes.
        let mut a = BTreeMap::new();
        a.insert("b".to_string(), identity("b", vec!["r".into()]));
        a.insert("a".to_string(), identity("a", vec![]));
        // Insert in the OTHER order — BTreeMap normalizes ordering.
        let mut b = BTreeMap::new();
        b.insert("a".to_string(), identity("a", vec![]));
        b.insert("b".to_string(), identity("b", vec!["r".into()]));
        assert_eq!(
            serde_json::to_vec(&a).unwrap(),
            serde_json::to_vec(&b).unwrap()
        );
    }
}
