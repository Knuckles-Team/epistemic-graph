//! The SQL store's storage/mutation authority (RF-RULING-004, RF-RULING-006).
//!
//! `eg-storage` is the sole physical owner of the SQL owner file (declared
//! `OwnerLayout::Sql`) and `eg-transaction` its sole writer. Everything this
//! module exposes is a capability those two kernels issued: a scoped read, or an
//! owner-row write that exists only inside an admitted mutation. `TableStore`
//! holds one [`SqlAuthority`] and has no other way to reach the file.
//!
//! One physical SQL file serves MANY logical scopes -- one per `(tenant, graph)`
//! any served write path has used, plus the fixed cross-graph scopes such as the
//! sqlite importer's. Each is authenticated by the composition root's verifier
//! and bound on first use, exactly as `eg-tsdb`'s per-series scopes are; the
//! store's own `open_scoped` tenant scope is the bootstrap scope, which serves
//! every catalog read and every one-shot DDL/DML maintenance write.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};

use eg_storage::{
    ledger_scope_key, OwnedStoreHandle, PhysicalStoreIdentity, ScopeGrantVerifier, ScopedRead,
    SqlOwner, StorageKernelV1,
};
use eg_transaction::{AdmittedMutation, AdmittedOwnerWrite, Begin, MutationKernelV1};
use eg_types::mutation_batch::{
    MutationBatch, MutationBatchRecord, MutationDomain, MutationOperation, MutationRequestContext,
    MutationScopeIdentity, MutationSurface, VersionExpectation, COMPILED_BATCH_INCARNATION,
    MUTATION_BATCH_VERSION,
};
use eg_types::protocol::Method;

/// Operator-facing identity of the ONE physical SQL owner file. It names the
/// physical authority boundary the storage kernel stamps into the owner
/// manifest, so a file created for another owner can never be opened as this
/// one.
pub(crate) const SQL_PHYSICAL_STORE: &str = "eg-query:sql-user-tables";

/// Logical resource name of the bootstrap scope every catalog read and every
/// one-shot DDL/DML maintenance write of one store runs under. The tenant half
/// is the store's own `open_scoped` owner scope, so two tenants sharing one
/// physical file still have distinct bootstrap identities.
pub(crate) const SQL_BOOTSTRAP_RESOURCE: &str = "sql-user-tables";

/// The only owner-row write handle for the SQL layout. Reachable solely between
/// `AdmittedMutation::owner_rows` and `finish_owner`, and bounded to the tables
/// `owner_table_names(OwnerLayout::Sql)` declares.
pub(crate) type SqlWrite<'a> = AdmittedOwnerWrite<'a, SqlOwner>;

/// One kernel-issued scoped read over the SQL owner file.
pub(crate) type SqlRead<'a> = ScopedRead<'a, SqlOwner>;

/// The typed identity of one SQL mutation scope.
///
/// `COMPILED_BATCH_INCARNATION` is the incarnation the server's batch compiler
/// stamps on every `MutationDomain::SqlCatalog` batch
/// (`src/server/mutation_batch/compile.rs`), so a scope resolved here by
/// `(tenant, resource)` is byte-identical to the one a served batch carries.
pub(crate) fn sql_scope_identity(
    tenant: &str,
    resource: &str,
) -> Result<MutationScopeIdentity, String> {
    MutationScopeIdentity::fixed_native(
        tenant,
        MutationDomain::SqlCatalog,
        resource,
        COMPILED_BATCH_INCARNATION,
    )
}

/// Authenticate and bind ONE logical serving scope on this owner file.
///
/// The proof bytes are interpreted only by the composition root's verifier; this
/// crate supplies the identity and the layout and never inspects them. Binding
/// is idempotent for an exact re-entry, so reopening a store re-binds the
/// identical scope rather than failing.
fn bind_serving_scope(
    kernel: &StorageKernelV1,
    verifier: &dyn ScopeGrantVerifier,
    principal: &str,
    proof: &[u8],
    identity: &MutationScopeIdentity,
) -> Result<OwnedStoreHandle<SqlOwner>, String> {
    let grant = kernel.authenticate_scope::<SqlOwner>(
        verifier,
        identity.clone(),
        principal.to_string(),
        proof,
    )?;
    kernel.bind_serving_scope(grant, 0)
}

/// The batch for one SQL owner-maintenance mutation (RF-RULING-005).
///
/// A `CREATE TABLE`, an `INSERT`, a secondary-index build or a schema migration
/// issued through `TableStore`'s direct API carries no caller idempotency
/// identity -- there is no request nonce, no policy epoch and no canonical
/// payload digest to derive one from -- so it is admitted as maintenance:
/// ledgered, fenced and version-bumping like any other mutation, but outside
/// operation-replay conflict semantics.
///
/// `batch_id` is `(kind, scope version)`. Exactly one batch commits per version,
/// so it is unique per attempt and stable across a crash-retry of the same
/// attempt -- which is what makes a retry a replay rather than an
/// `IDEMPOTENCY_CONFLICT`. The subject (a table or catalog object name, up to
/// the SQL identifier limit) travels in the operation, not in the identity.
fn maintenance_batch(
    kind: &str,
    subject: &str,
    identity: &MutationScopeIdentity,
    principal: &str,
    expected_version: u64,
    created_at_ms: u64,
) -> Result<MutationBatch, String> {
    let batch_id = format!("sql-{kind}:v{expected_version}");
    let operation = MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Query,
        domain: MutationDomain::SqlCatalog,
        method: Method::ApplyMutation {
            event_type: format!("sql_{kind}"),
            query: subject.to_string(),
        },
    };
    let batch = MutationBatch {
        schema_version: MUTATION_BATCH_VERSION,
        batch_id: batch_id.clone(),
        context: MutationRequestContext {
            request_id: 0,
            principal: principal.to_string(),
            purpose: None,
            policy_fingerprint: None,
            trace_id: None,
            // A maintenance mutation claims no capability: it is a plain
            // `Native`-versioned write, not the reserved-system `Unversioned`
            // path. Empty is the true fact here, not a placeholder.
            verified_capabilities: std::collections::BTreeSet::new(),
        },
        identity: identity.clone(),
        placement_epoch: 0,
        idempotency_key: batch_id,
        version_expectation: VersionExpectation::Native(expected_version),
        fencing_token: None,
        authoritative_state: None,
        operations: vec![operation],
        outbox: Vec::new(),
        created_at_ms,
    };
    batch.validate()?;
    Ok(batch)
}

fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// One bound serving scope: the ledger scope key plus the principal it was
/// bound for. A SQL owner file binds a scope once per principal that writes it.
type BoundScopeKey = (String, String);

/// One bound handle, shared by `Arc` because `OwnedStoreHandle` is a capability
/// and deliberately not `Clone`.
type BoundScope = Arc<OwnedStoreHandle<SqlOwner>>;

/// The SQL store's one physical authority plus its bound serving scopes.
pub(crate) struct SqlAuthority {
    kernel: StorageKernelV1,
    mutations: MutationKernelV1,
    /// The composition root's proof authority. Held (not borrowed) because a
    /// scope reached for the first time by a served batch must be authenticated
    /// long after `open` returned.
    grants: Arc<dyn ScopeGrantVerifier>,
    principal: String,
    proof: Vec<u8>,
    bootstrap: Arc<OwnedStoreHandle<SqlOwner>>,
    /// Bound serving scopes, keyed by `(ledger scope key, principal)`.
    ///
    /// A SQL owner file is a tenant-shared catalog: one file legitimately serves
    /// MANY principals, unlike a statechart or jobs file. The durable
    /// `ScopeBinding` carries no principal -- it is an in-memory property of the
    /// capability, decided by the composition root's verifier -- so a scope is
    /// bound once per principal that writes it, and `AdmittedMutation::
    /// owner_rows` then matches the batch's own actor instead of forcing every
    /// caller's batch to be rewritten to one serving identity.
    ///
    /// `OwnedStoreHandle` is deliberately not `Clone` -- a handle IS a
    /// capability -- so the cache hands out `Arc` clones of the one bound handle
    /// rather than copies.
    scopes: RwLock<BTreeMap<BoundScopeKey, BoundScope>>,
}

impl std::fmt::Debug for SqlAuthority {
    /// `datafusion::catalog::TableProvider` requires `Debug` on everything a
    /// provider holds. A capability must not print its proof or its bound
    /// scopes, so this states the physical identity and nothing else.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlAuthority")
            .field("physical_store", &SQL_PHYSICAL_STORE)
            .finish_non_exhaustive()
    }
}

impl SqlAuthority {
    /// Open (or create) the SQL owner file at `path` through the storage kernel
    /// and bind `tenant_scope`'s bootstrap serving scope.
    ///
    /// The kernel materializes the whole declared `OwnerLayout::Sql` census when
    /// the file is created and re-validates it on every open, so every `__sql_*`
    /// table exists from the first read onward and no caller has to tolerate a
    /// missing one.
    pub(crate) fn open(
        path: &Path,
        tenant_scope: &str,
        verifier: Arc<dyn ScopeGrantVerifier>,
        principal: &str,
        proof: &[u8],
    ) -> Result<Self, String> {
        let identity = sql_scope_identity(tenant_scope, SQL_BOOTSTRAP_RESOURCE)?;
        let physical = PhysicalStoreIdentity::new(SQL_PHYSICAL_STORE)?;
        let kernel = if path.exists() {
            StorageKernelV1::open_owner::<SqlOwner>(path, physical, None)
        } else {
            StorageKernelV1::create_owner::<SqlOwner>(path, physical, None)
        }?;
        let (kernel, authority) = kernel.into_read_and_mutation_authority()?;
        let mutations = MutationKernelV1::new(authority);
        let bootstrap = Arc::new(bind_serving_scope(
            &kernel,
            verifier.as_ref(),
            principal,
            proof,
            &identity,
        )?);
        mutations.bootstrap_ledger(bootstrap.as_ref())?;
        Ok(Self {
            kernel,
            mutations,
            grants: verifier,
            principal: principal.to_string(),
            proof: proof.to_vec(),
            bootstrap,
            scopes: RwLock::new(BTreeMap::new()),
        })
    }

    /// The bound handle for one served scope and principal, binding it on first
    /// use.
    ///
    /// The ledger scope key is the scope's BINDING digest, which covers tenant
    /// and scope but not the lifecycle generation, so two incarnations of one
    /// logical name cannot coexist on a file. `bind_scope_in` says so with
    /// "mutation scope rebinding mismatch"; a cache hit would bypass that check,
    /// so the generation is compared here too.
    pub(crate) fn scope_handle(
        &self,
        identity: &MutationScopeIdentity,
        principal: &str,
    ) -> Result<BoundScope, String> {
        let key = (ledger_scope_key(identity), principal.to_string());
        let cached = self
            .scopes
            .read()
            .map_err(|_| "SQL scope cache is poisoned".to_string())?
            .get(&key)
            .map(Arc::clone);
        if let Some(handle) = cached {
            if handle.identity() != identity {
                return Err("mutation scope rebinding mismatch".to_string());
            }
            return Ok(handle);
        }
        let handle = Arc::new(bind_serving_scope(
            &self.kernel,
            self.grants.as_ref(),
            principal,
            &self.proof,
            identity,
        )?);
        self.scopes
            .write()
            .map_err(|_| "SQL scope cache is poisoned".to_string())?
            .insert(key, Arc::clone(&handle));
        Ok(handle)
    }

    /// The store's own serving principal -- the actor of every maintenance
    /// mutation and the reader of every scoped read it takes on its own behalf.
    pub(crate) fn principal(&self) -> &str {
        &self.principal
    }

    /// One kernel-issued scoped read over an exact bound scope -- the only way
    /// to reach that scope's ledger rows.
    pub(crate) fn read_scope(
        &self,
        owner: &OwnedStoreHandle<SqlOwner>,
    ) -> Result<SqlRead<'_>, String> {
        self.kernel.read_scope(owner)
    }

    /// One kernel-issued scoped read over the bootstrap scope -- the catalog
    /// view every read path uses. Owner rows are layout-bounded, not
    /// scope-bounded, because `__sql_catalog__` and its siblings carry no scope
    /// component in their keys: the physical file IS the catalog boundary.
    pub(crate) fn read(&self) -> Result<SqlRead<'_>, String> {
        self.kernel.read_scope(self.bootstrap.as_ref())
    }

    /// The authoritative mutation version of one bound scope.
    pub(crate) fn scope_version(&self, owner: &OwnedStoreHandle<SqlOwner>) -> Result<u64, String> {
        let read = self.kernel.read_scope(owner)?;
        eg_transaction::version(&read)
    }

    /// Every bound scope's authoritative version, in ledger-key order.
    ///
    /// The ledger version table is scope-bounded on read, so no reader can
    /// enumerate another scope's rows; this walks the scopes this store itself
    /// has bound, which is exactly the set any write through it can have
    /// touched.
    pub(crate) fn bound_scope_versions(&self) -> Result<Vec<(String, u64)>, String> {
        let handles: Vec<(String, BoundScope)> = {
            let scopes = self
                .scopes
                .read()
                .map_err(|_| "SQL scope cache is poisoned".to_string())?;
            std::iter::once((
                ledger_scope_key(self.bootstrap.identity()),
                Arc::clone(&self.bootstrap),
            ))
            .chain(
                scopes
                    .iter()
                    .map(|((key, _principal), handle)| (key.clone(), Arc::clone(handle))),
            )
            .collect()
        };
        let mut out = Vec::with_capacity(handles.len());
        for (key, handle) in handles {
            out.push((key, self.scope_version(handle.as_ref())?));
        }
        out.sort_unstable();
        out.dedup();
        Ok(out)
    }

    /// Admit one owner-maintenance mutation on the bootstrap scope: ledgered,
    /// fenced and version-bumping like any other write, but carrying no caller
    /// identity, which is exactly what a direct-API DDL/DML call is. There is no
    /// un-ledgered owner-write path any more, so this is how they land.
    ///
    /// The returned [`SqlMutation`] is the open write; the caller changes owner
    /// rows through it and then commits or aborts it. [`Self::maintain`] is the
    /// one-shot form for the common case where neither arm is conditional.
    pub(crate) fn begin_maintenance(
        &self,
        kind: &str,
        subject: &str,
    ) -> Result<SqlMutation<'_>, String> {
        self.begin_maintenance_on(Arc::clone(&self.bootstrap), kind, subject)
    }

    fn begin_maintenance_on(
        &self,
        owner: BoundScope,
        kind: &str,
        subject: &str,
    ) -> Result<SqlMutation<'_>, String> {
        let expected_version = self.scope_version(owner.as_ref())?;
        let batch = maintenance_batch(
            kind,
            subject,
            owner.identity(),
            &self.principal,
            expected_version,
            unix_ms(),
        )?;
        let (write, begun) = self.mutations.admit_maintenance(owner.as_ref(), &batch)?;
        let source_version = match begun {
            Begin::Replay(_) => {
                write.abort()?;
                return Err(format!(
                    "SQL maintenance batch '{}' was already committed",
                    batch.batch_id
                ));
            }
            Begin::Apply { source_version } => source_version,
        };
        Ok(SqlMutation {
            authority: self,
            owner,
            write,
            batch,
            source_version,
        })
    }

    /// Admit one caller-originated SQL statement batch on the scope its own
    /// identity names.
    ///
    /// This is `MutationClass::Operation`: the kernel's ledger decides
    /// idempotency, OCC and route fencing for it, and writes its receipt, class
    /// row, version bump, fence and outbox rows on `finish`. A returned
    /// [`Begin::Replay`] means the idempotency key already names a terminally
    /// committed receipt and the caller must abort rather than reapply.
    pub(crate) fn begin_operation(
        &self,
        batch: &MutationBatch,
    ) -> Result<(SqlMutation<'_>, Begin), String> {
        let owner = self.scope_handle(&batch.identity, &batch.context.principal)?;
        let (write, begun) = self.mutations.admit(owner.as_ref(), batch)?;
        let mutation = SqlMutation {
            authority: self,
            owner,
            write,
            batch: batch.clone(),
            source_version: None,
        };
        Ok((mutation, begun))
    }

    /// One owner-maintenance mutation whose owner rows are the whole write.
    pub(crate) fn maintain<T, F>(&self, kind: &str, subject: &str, apply: F) -> Result<T, String>
    where
        F: FnOnce(&SqlWrite<'_>) -> Result<T, String>,
    {
        let mutation = self.begin_maintenance(kind, subject)?;
        let outcome = match mutation.owner_rows(apply) {
            Ok(value) => value,
            Err(error) => {
                mutation.abort()?;
                return Err(error);
            }
        };
        mutation.commit()?;
        Ok(outcome)
    }
}

/// One admitted SQL mutation, open on the bootstrap scope.
///
/// It exists between admission and commit. Owner rows are reachable only
/// through [`Self::owner_rows`], which opens and closes the kernel's owner-write
/// gate around one closure -- dropping that gate unfinished poisons the whole
/// write, so a failing closure still has to close it or the poison, not the real
/// error, is what the caller sees.
pub(crate) struct SqlMutation<'a> {
    authority: &'a SqlAuthority,
    owner: BoundScope,
    write: AdmittedMutation<'a, SqlOwner>,
    batch: MutationBatch,
    source_version: Option<u64>,
}

impl SqlMutation<'_> {
    /// Record the authoritative version this write was admitted against.
    ///
    /// A maintenance mutation reads it at admission; a caller-originated one
    /// learns it from [`Begin::Apply`], which the caller must inspect anyway to
    /// tell an apply from a replay.
    pub(crate) fn set_source_version(&mut self, source_version: Option<u64>) {
        self.source_version = source_version;
    }

    pub(crate) fn owner_rows<T, F>(&self, apply: F) -> Result<T, String>
    where
        F: FnOnce(&SqlWrite<'_>) -> Result<T, String>,
    {
        let owner_write = self.write.owner_rows(self.owner.as_ref(), &self.batch)?;
        let outcome = apply(&owner_write);
        owner_write.finish_owner()?;
        outcome
    }

    /// Persist the batch's terminal metadata -- receipt, idempotency row, class
    /// row, version bump, fence and outbox rows -- without committing.
    pub(crate) fn finish(
        &self,
        result_msgpack: Option<Vec<u8>>,
        committed_at_ms: u64,
    ) -> Result<MutationBatchRecord, String> {
        self.authority.mutations.finish(
            &self.write,
            &self.batch,
            result_msgpack,
            committed_at_ms,
            self.source_version,
        )
    }

    /// Commit an already-finished write.
    pub(crate) fn commit_finished(self) -> Result<(), String> {
        self.authority.mutations.commit(self.write, &self.batch)
    }

    /// Persist the batch's terminal metadata and commit the write.
    pub(crate) fn commit(self) -> Result<(), String> {
        self.finish(None, unix_ms())?;
        self.commit_finished()
    }

    /// Discard every row written under this mutation.
    pub(crate) fn abort(self) -> Result<(), String> {
        self.write.abort()
    }
}
