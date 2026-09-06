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

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

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
/// closed (`eg_mutation_store::bind_scope_in`'s idempotent-rebind check).
const RBAC_SCOPE_INCARNATION: &str = "rbac-security-control:v1";

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
    transaction: &redb::ReadTransaction,
) -> Result<(RbacPolicy, BTreeMap<String, AgentIdentity>), RbacPersistError> {
    let state = transaction
        .open_table(RBAC_TABLE)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
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

fn read_authority_revision(
    transaction: &redb::ReadTransaction,
    identity: &eg_types::MutationScopeIdentity,
) -> Result<u64, RbacPersistError> {
    let binding = identity.binding_digest().to_hex();
    let table = transaction
        .open_table(eg_mutation_store::VERSIONS)
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
    let revision = table
        .get(binding.as_str())
        .map_err(|error| RbacPersistError::Redb(error.to_string()))?
        .map(|value| value.value())
        .ok_or(RbacPersistError::IncompleteState(
            "mutation scope binding is missing its version row",
        ))?;
    Ok(revision)
}

/// A durable, redb-backed snapshot of the RBAC policy + registered identities
/// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Holds the
/// [`eg_mutation_store::MutationStore`] that owns the underlying physical
/// `rbac.redb` file (`RbacStore::db` before the MutationBatch v1 migration);
/// the RBAC_TABLE itself is opened directly off `mutation_store.database()`,
/// while every write-through also goes through the SAME store's mutation
/// ledger for `security-control`'s version bookkeeping. `identity` is the
/// fixed native scope identity (see `native_security_control_identity`)
/// reused on every load/save so it is validated exactly once per open.
pub struct RbacStore {
    mutation_store: eg_mutation_store::MutationStore,
    identity: eg_types::MutationScopeIdentity,
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
    /// §2.4, R5) — reuses the SAME monotonic counter `eg_mutation_store`
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

impl fmt::Debug for RbacStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RbacStore").finish_non_exhaustive()
    }
}

impl RbacStore {
    /// Open (or create) `{dir}/rbac.redb` and ensure the table exists
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). The dir is created if absent. Opening validates that the
    /// store is writable up front; subsequent fallible write-throughs still
    /// propagate commit failures so callers can roll state back and fail closed.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self, RbacPersistError> {
        std::fs::create_dir_all(dir.as_ref())?;
        let path = dir.as_ref().join("rbac.redb");
        let identity = native_security_control_identity()?;
        // `initialize` opens (or creates) the physical `rbac.redb`, establishes/
        // validates its `StoreIncarnation` root, and binds the fixed
        // `security-control` scope at `initial_version: 0` -- the SAME "brand
        // new store starts at version 0" semantics `save`/`current_version`
        // relied on via the old `eg_mutation_store::version(&db, "native",
        // "security-control")` call. The bootstrap closure runs only on the
        // FIRST-ever bind (a fresh file), and its only job is to make sure
        // RBAC_TABLE exists so a later `begin_read()` + `open_table` in
        // `load`/`bootstrap_current_state` never sees a "no such table" error;
        // opening a redb table for write auto-creates it, and once created it
        // persists across every later re-open of the same file.
        let mutation_store = eg_mutation_store::initialize(&path, &identity, 0, None, |wtx| {
            wtx.open_table(RBAC_TABLE).map_err(|e| e.to_string())?;
            Ok(())
        })
        .map_err(RbacPersistError::Redb)?;
        let store = Self {
            mutation_store,
            identity,
        };
        store.bootstrap_current_state()?;
        Ok(store)
    }

    /// Copy the durable RBAC/identity image verbatim into a FRESH file at
    /// `destination` (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
    ///
    /// Taken off a `begin_read()` MVCC snapshot of the LIVE handle and streamed
    /// table-by-table, exactly like the graph-shard bundle copy: value blobs move
    /// byte-for-byte, so nothing is decoded and no key is needed. Refuses to
    /// overwrite an existing file.
    ///
    /// The RBAC table AND the mutation-store bookkeeping that shares this file are
    /// both copied — `RegisterIdentity`/`RbacAdmin` commit their MutationBatch
    /// metadata in the same write txn as the identity snapshot, so restoring one
    /// without the other would reopen an already-acknowledged admission.
    /// Bundle this store into `destination`.
    ///
    /// The mutation-store half is delegated to
    /// `eg_mutation_store::backup_recovery_store`, which owns the complete table
    /// set and, crucially, re-stamps `SCOPE_BINDINGS` for the destination's own
    /// incarnation via `copy_bindings`. Hand-copying the table list here is what
    /// produced BUG-PE-054: `VERSIONS` was copied and `STORE_ROOT`/`SCOPE_BINDINGS`
    /// were not, so a restored bundle reopened with "mutation version row exists
    /// without a scope binding" the moment any version above zero existed. That
    /// list could not be kept correct from outside the crate that defines it --
    /// every table added to the mutation store would have had to be mirrored
    /// here, and silently was not.
    ///
    /// Only `RBAC_TABLE`, which this store genuinely owns, is copied locally.
    pub fn backup_into(&self, destination: &Path) -> Result<u64, String> {
        if destination.exists() {
            return Err("bundled store file already exists (refusing to overwrite)".to_string());
        }
        let counts = eg_mutation_store::backup_recovery_store(&self.mutation_store, destination)
            .map_err(|error| error.to_string())?;

        // `RBAC_TABLE` is this store's own table, not part of the mutation
        // store's contract, so it is appended with plain redb rather than
        // requiring a second `MutationStore` handle. Adding a table does not
        // disturb the incarnation, which binds (dev, ino).
        let target = Database::create(destination).map_err(|e| e.to_string())?;
        let rtx = self
            .mutation_store
            .database()
            .begin_read()
            .map_err(|e| e.to_string())?;
        let mut wtx = target.begin_write().map_err(|e| e.to_string())?;
        wtx.set_durability(redb::Durability::Immediate)
            .map_err(|e| e.to_string())?;
        let mut rows = 0u64;
        {
            let mut destination_table = wtx.open_table(RBAC_TABLE).map_err(|e| e.to_string())?;
            if let Ok(source_table) = rtx.open_table(RBAC_TABLE) {
                for row in source_table.iter().map_err(|e| e.to_string())? {
                    let (key, value) = row.map_err(|e| e.to_string())?;
                    destination_table
                        .insert(key.value(), value.value())
                        .map_err(|e| e.to_string())?;
                    rows += 1;
                }
            }
        }
        wtx.commit().map_err(|e| e.to_string())?;
        Ok(rows
            .saturating_add(counts.batches)
            .saturating_add(counts.idempotency)
            .saturating_add(counts.versions)
            .saturating_add(counts.fences)
            .saturating_add(counts.outbox)
            .saturating_add(counts.encrypted_private_payloads))
    }

    /// Atomically create the explicit current bootstrap image for a brand-new store.
    /// A partial image is never repaired because that could silently discard part
    /// of an authorization state transition.
    fn bootstrap_current_state(&self) -> Result<(), RbacPersistError> {
        let rtx = self
            .mutation_store
            .database()
            .begin_read()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        let table = rtx
            .open_table(RBAC_TABLE)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
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
        drop(rtx);
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
        let rtx = self
            .mutation_store
            .database()
            .begin_read()
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
        let t = rtx
            .open_table(RBAC_TABLE)
            .map_err(|e| RbacPersistError::Redb(e.to_string()))?;
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
        let transaction = self
            .mutation_store
            .database()
            .begin_read()
            .map_err(|error| RbacPersistError::Redb(error.to_string()))?;
        let (policy, identities) = read_authority_image(&transaction)?;
        let revision = read_authority_revision(&transaction, &self.identity)?;
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

    /// GRAPH-POLICY-LEASE-CONTRACT.md §2.4 (R5): the SAME `eg_mutation_store`
    /// counter [`RbacStore::save`] already bumps atomically (same `redb`
    /// write transaction, above) with the policy/identity/bootstrap writes --
    /// reused here rather than adding a second, independently-maintained
    /// persisted counter.
    pub fn current_version(&self) -> Result<u64, RbacPersistError> {
        eg_mutation_store::version(&self.mutation_store, &self.identity)
            .map_err(RbacPersistError::Redb)
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

    use super::{IdentityBootstrapState, RbacPersistError, RbacStore, IDENTITIES_KEY, RBAC_TABLE};
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
            let store = RbacStore::open(&dir).unwrap();
            store
                .save(&policy, &identities, IdentityBootstrapState::Consumed)
                .unwrap();
        }
        // Reopen the SAME dir — the state is durable across "process" lifetimes.
        let store = RbacStore::open(&dir).unwrap();
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
        let store = RbacStore::open(&dir).unwrap();
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
            let store = RbacStore::open(&dir).unwrap();
            store
                .save(
                    &RbacPolicy::new(),
                    &BTreeMap::new(),
                    IdentityBootstrapState::Consumed,
                )
                .unwrap();
        }
        let store = RbacStore::open(&dir).unwrap();
        let (_, identities, bootstrap) = store.load().unwrap();
        assert!(identities.is_empty());
        assert_eq!(bootstrap, IdentityBootstrapState::Consumed);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn eg303_partial_current_state_fails_closed() {
        let dir = tmp_dir("partial");
        let store = RbacStore::open(&dir).unwrap();
        let wtx = store.mutation_store.database().begin_write().unwrap();
        {
            let mut table = wtx.open_table(RBAC_TABLE).unwrap();
            table.remove(IDENTITIES_KEY).unwrap();
        }
        wtx.commit().unwrap();
        assert!(matches!(
            store.load(),
            Err(RbacPersistError::IncompleteState(_))
        ));
        drop(store);
        assert!(matches!(
            RbacStore::open(&dir),
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
