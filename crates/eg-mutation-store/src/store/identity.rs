use super::*;
use eg_types::IncarnationId;
use redb::{ReadOnlyDatabase, ReadTransaction, TableHandle};
use serde::{Deserialize, Deserializer, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub const MUTATION_STORE_SCHEMA_VERSION: u16 = 1;
const STORE_ROOT_KEY: &str = "root";
const STORE_DIGEST_DOMAIN: &[u8] = b"eg/mutation-store-root/v1\0";
const PHYSICAL_ROOT_DOMAIN: &[u8] = b"eg/mutation-store-physical-root/v1\0";
// NOTE: bare `mutation_batches`/`mutation_idempotency`/`mutation_outbox` are
// DELIBERATELY NOT listed here. Those exact names are still the live,
// currently-written table names of `epistemic-graph`'s own pre-`eg-mutation-store`
// per-graph-shard mutation bookkeeping (`src/redb_store.rs`'s `MUTATION_BATCHES` /
// `MUTATION_IDEMPOTENCY` / `MUTATION_OUTBOX` consts) -- an unrelated, currently
// in-service subsystem, not a retired prototype of this crate. A graph shard file
// is intentionally multi-owner (e.g. `eg_tsdb::store::SeriesStore::open` binds an
// `eg-mutation-store` scope directly onto a shard path so a measurement lands in
// the SAME `WriteTransaction` as the graph rows -- see
// `epistemic-graph::server::persistence::redb_backend::tests::five_modality_atomic_commit`),
// so any table `reject_prototype_names` treats as disqualifying must be a name
// ONLY a genuinely-retired `eg-mutation-store` schema would ever have produced.
// Listing a name some OTHER live subsystem's table can also legitimately hold
// turns this guard into a false positive on every physical file they share.
const RETIRED_PROTOTYPE_TABLES: &[&str] = &[
    "mutation_versions",
    "mutation_fences",
    "mutation_private_payloads",
    "mutation_store_root_v3",
    "mutation_scope_bindings_v3",
    "mutation_batches_v3",
    "mutation_idempotency_v3",
    "mutation_versions_v3",
    "mutation_fences_v3",
    "mutation_outbox_v3",
    "mutation_private_payloads_v3",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreIdentityDigest([u8; 32]);

/// Immutable physical identity of one common mutation-store database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StoreIncarnation {
    schema_version: u16,
    root_id: IncarnationId,
    identity_digest: StoreIdentityDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreIncarnationWire {
    schema_version: u16,
    root_id: IncarnationId,
    identity_digest: StoreIdentityDigest,
}

impl StoreIncarnation {
    pub(crate) fn derive(path: &Path) -> Result<(Self, PathBuf), String> {
        let canonical = std::fs::canonicalize(path).map_err(|error| error.to_string())?;
        let root_id = physical_root_id(&canonical)?;
        let identity_digest = store_digest(MUTATION_STORE_SCHEMA_VERSION, &root_id);
        Ok((
            Self {
                schema_version: MUTATION_STORE_SCHEMA_VERSION,
                root_id,
                identity_digest,
            },
            canonical,
        ))
    }

    pub fn identity_digest(&self) -> StoreIdentityDigest {
        self.identity_digest
    }

    pub fn validate_digest(&self) -> Result<(), String> {
        if self.schema_version != MUTATION_STORE_SCHEMA_VERSION {
            return Err(format!(
                "unsupported mutation store schema {} (expected {})",
                self.schema_version, MUTATION_STORE_SCHEMA_VERSION
            ));
        }
        if self.identity_digest != store_digest(self.schema_version, &self.root_id) {
            return Err("mutation store root digest mismatch".to_string());
        }
        Ok(())
    }

    fn from_wire(wire: StoreIncarnationWire) -> Result<Self, String> {
        let incarnation = Self {
            schema_version: wire.schema_version,
            root_id: wire.root_id,
            identity_digest: wire.identity_digest,
        };
        incarnation.validate_digest()?;
        Ok(incarnation)
    }
}

impl<'de> Deserialize<'de> for StoreIncarnation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = StoreIncarnationWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(serde::de::Error::custom)
    }
}

fn decode_incarnation(bytes: &[u8]) -> Result<StoreIncarnation, String> {
    let wire: StoreIncarnationWire = decode_record(bytes)?;
    StoreIncarnation::from_wire(wire)
}

fn persisted_root<T>(table: &T) -> Result<Option<StoreIncarnation>, String>
where
    T: redb::ReadableTable<&'static str, &'static [u8]>,
{
    let mut rows = table.iter().map_err(|error| error.to_string())?;
    let Some(first) = rows.next() else {
        return Ok(None);
    };
    let (key, value) = first.map_err(|error| error.to_string())?;
    let has_extra_row = rows
        .next()
        .transpose()
        .map_err(|error| error.to_string())?
        .is_some();
    if key.value() != STORE_ROOT_KEY || has_extra_row {
        return Err("mutation store must contain exactly one canonical root".to_string());
    }
    decode_incarnation(value.value()).map(Some)
}

pub(super) fn require_persisted_root<T>(table: &T) -> Result<StoreIncarnation, String>
where
    T: redb::ReadableTable<&'static str, &'static [u8]>,
{
    persisted_root(table)?.ok_or_else(|| "mutation store root is missing".to_string())
}

/// Existing canonical AEAD/integrity authority supplied by the composition
/// root. The mutation store never implements or derives a second crypto key.
pub trait PrivatePayloadIntegrity: Send + Sync {
    fn authenticate(&self, sealed: &[u8], expected_plaintext_digest: &str) -> Result<(), String>;
}

/// Non-serializable proof that a store root was derived from and matched the
/// exact physical database.
#[derive(Debug, Clone)]
struct StoreHandle {
    incarnation: StoreIncarnation,
}

/// Open physical database plus its non-forgeable root and integrity authority.
pub struct MutationStore {
    database: Database,
    handle: StoreHandle,
    physical_path: PathBuf,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
}

/// Write transaction minted only by its owning [`MutationStore`].
pub struct MutationWrite {
    transaction: WriteTransaction,
    handle: StoreHandle,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
}

impl MutationStore {
    pub fn database(&self) -> &Database {
        &self.database
    }

    pub fn incarnation(&self) -> &StoreIncarnation {
        &self.handle.incarnation
    }

    pub fn write(&self) -> Result<MutationWrite, String> {
        self.validate_physical_root()?;
        let mut transaction = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        transaction
            .set_durability(redb::Durability::Immediate)
            .map_err(|error| error.to_string())?;
        validate_handle_write(&self.handle, &transaction)?;
        Ok(MutationWrite {
            transaction,
            handle: self.handle.clone(),
            private_integrity: self.private_integrity.clone(),
        })
    }

    pub(crate) fn validate_physical_root(&self) -> Result<(), String> {
        let (derived, canonical) = StoreIncarnation::derive(&self.physical_path)?;
        if derived != self.handle.incarnation || canonical != self.physical_path {
            return Err("mutation store physical root identity changed".to_string());
        }
        Ok(())
    }

    pub(crate) fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        authenticate_private(self.private_integrity.as_deref(), sealed, digest)
    }

    pub(crate) fn private_integrity(&self) -> Option<Arc<dyn PrivatePayloadIntegrity>> {
        self.private_integrity.clone()
    }

    pub(crate) fn empty_physical(
        path: &Path,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    ) -> Result<Self, String> {
        let database = Database::create(path).map_err(|error| error.to_string())?;
        let (expected, physical_path) = StoreIncarnation::derive(path)?;
        let mut transaction = database.begin_write().map_err(|error| error.to_string())?;
        transaction
            .set_durability(redb::Durability::Immediate)
            .map_err(|error| error.to_string())?;
        let handle = initialize_in(&transaction, &expected)?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(Self {
            database,
            handle,
            physical_path,
            private_integrity,
        })
    }
}

/// Read-only physical database plus its non-forgeable root and integrity
/// authority. Unlike [`MutationStore`] this never opens a write transaction:
/// `redb::Database`'s `Drop` unconditionally commits its own allocator-state
/// quick-repair transaction, which advances the file's transaction id and
/// changes its on-disk bytes on every open -- fine for a live store, fatal
/// for validating a backup bundle byte-for-byte against a manifest digest
/// computed when it was written. `ReadOnlyDatabase` never opens for write, so
/// opening and reading one never mutates the file.
pub struct ReadOnlyMutationStore {
    database: ReadOnlyDatabase,
    incarnation: StoreIncarnation,
    physical_path: PathBuf,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
}

impl ReadOnlyMutationStore {
    pub(crate) fn database(&self) -> &ReadOnlyDatabase {
        &self.database
    }

    pub fn incarnation(&self) -> &StoreIncarnation {
        &self.incarnation
    }

    pub(crate) fn validate_physical_root(&self) -> Result<(), String> {
        let (derived, canonical) = StoreIncarnation::derive(&self.physical_path)?;
        if derived != self.incarnation || canonical != self.physical_path {
            return Err("mutation store physical root identity changed".to_string());
        }
        Ok(())
    }

    pub(crate) fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        authenticate_private(self.private_integrity.as_deref(), sealed, digest)
    }
}

/// Open the exact physical database READ-ONLY, deriving its expected
/// incarnation from the file's current physical identity but never writing
/// to it. Use this to validate a backup bundle (via
/// [`crate::validate_recovery_store_read_only`]) without perturbing the
/// bytes a manifest's digests were computed over. This does NOT bootstrap or
/// bind anything -- the caller must already know the file holds an
/// initialized store, or every read against it will fail closed with a
/// missing-table error.
pub fn open_read_only(
    path: &Path,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
) -> Result<ReadOnlyMutationStore, String> {
    let (incarnation, physical_path) = StoreIncarnation::derive(path)?;
    let database = ReadOnlyDatabase::open(path).map_err(|error| error.to_string())?;
    Ok(ReadOnlyMutationStore {
        database,
        incarnation,
        physical_path,
        private_integrity,
    })
}

/// Adopt a store file that was copied verbatim from a validated backup
/// bundle to a NEW physical location -- e.g. `restore_bundle`'s
/// `std::fs::copy` destination. A copy always allocates a fresh inode, so
/// its freshly-derived physical incarnation can never equal the one the
/// bundle bytes were stamped with; normal [`initialize`] and
/// [`open_read_only`] both correctly fail closed on that mismatch, since
/// ordinarily it means the live file was substituted underneath an open
/// handle.
///
/// A restore is not that: it is an INTENDED, authenticated substitution.
/// This is the one named, explicit operation that is allowed to resolve it.
/// It:
///
/// 1. opens the file, reads the incarnation the bytes were stamped with, and
///    proves every recovery invariant holds under THAT incarnation --
///    exactly [`crate::validate_recovery_content`]'s checks (bindings match
///    their root, every version/batch/idempotency/fence/outbox/private row
///    is exactly and only where it should be) -- but deliberately does
///    *not* require that recorded incarnation to match this file's current
///    physical identity, since by construction (a fresh copy) it never can;
/// 2. only once every other invariant holds, re-derives this file's actual
///    physical incarnation and rewrites `STORE_ROOT` plus every
///    `SCOPE_BINDINGS` row's `store_identity_digest` to it, in one commit;
/// 3. returns a normally-opened [`MutationStore`] bound to the new
///    incarnation -- from this point on it is an ordinary live store, and
///    any FUTURE physical substitution is caught the normal, fail-closed
///    way again.
///
/// Any content inconsistency in step 1 fails closed before anything is
/// rewritten -- adoption never repairs a corrupt or tampered bundle, it only
/// re-anchors a proven-consistent one to its new location.
/// Adopt a restored store only if the file actually is one.
///
/// A restore bundle carries a mix of mutation-store-backed files (`rbac.redb`,
/// `kv.redb`) and plain redb stores that were never stamped with a
/// `StoreIncarnation` (`node_info.redb`). The restore path cannot know which is
/// which from the filename, and guessing from an error string would be brittle,
/// so absence of the canonical root is reported as `Ok(None)` -- "not a mutation
/// store" -- while any INCONSISTENCY in a file that *is* one still fails closed.
///
/// This is the distinction that matters: "there is nothing here to adopt" and
/// "what is here does not add up" must not collapse into one error.
pub fn adopt_restored_store_if_mutation_store(
    path: &Path,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
) -> Result<Option<MutationStore>, String> {
    let has_root = {
        let database = Database::open(path).map_err(|error| error.to_string())?;
        let rtx = database.begin_read().map_err(|error| error.to_string())?;
        // Bind before yielding: `list_tables` borrows `rtx`, and a block's tail
        // temporaries outlive the local, so returning this directly fails
        // borrowck (E0597) -- the same shape as the other reads in this module.
        let found = rtx
            .list_tables()
            .map_err(|error| error.to_string())?
            .any(|table| table.name() == STORE_ROOT.name());
        found
    };
    if !has_root {
        return Ok(None);
    }
    adopt_restored_store(path, private_integrity).map(Some)
}

pub fn adopt_restored_store(
    path: &Path,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
) -> Result<MutationStore, String> {
    let database = Database::open(path).map_err(|error| error.to_string())?;
    let recorded = {
        let rtx = database.begin_read().map_err(|error| error.to_string())?;
        reject_prototype_names(
            rtx.list_tables()
                .map_err(|error| error.to_string())?
                .map(|table| table.name().to_string()),
        )?;
        let recorded = {
            let table = rtx
                .open_table(STORE_ROOT)
                .map_err(|error| error.to_string())?;
            require_persisted_root(&table)?
        };
        let authenticate = |sealed: &[u8], digest: &str| {
            authenticate_private(private_integrity.as_deref(), sealed, digest)
        };
        crate::validate_recovery_content(&recorded, &rtx, &authenticate)?;
        recorded
    };

    let (adopted, physical_path) = StoreIncarnation::derive(path)?;
    let mut wtx = database.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    {
        let mut root_table = wtx
            .open_table(STORE_ROOT)
            .map_err(|error| error.to_string())?;
        let bytes = encode_bounded(&adopted, "mutation store root")?;
        root_table
            .insert(STORE_ROOT_KEY, bytes.as_slice())
            .map_err(|error| error.to_string())?;
    }
    {
        let mut bindings_table = wtx
            .open_table(SCOPE_BINDINGS)
            .map_err(|error| error.to_string())?;
        let keys: Vec<String> = bindings_table
            .iter()
            .map_err(|error| error.to_string())?
            .map(|row| {
                row.map(|(key, _)| key.value().to_string())
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        for key in keys {
            let bytes = bindings_table
                .get(key.as_str())
                .map_err(|error| error.to_string())?
                .map(|value| value.value().to_vec())
                .ok_or_else(|| "mutation scope binding vanished during adoption".to_string())?;
            let mut binding = decode_binding(&bytes)?;
            binding.store_identity_digest = adopted.identity_digest();
            let rebound = encode_bounded(&binding, "mutation scope binding")?;
            bindings_table
                .insert(key.as_str(), rebound.as_slice())
                .map_err(|error| error.to_string())?;
        }
    }
    wtx.commit().map_err(|error| error.to_string())?;
    debug_assert_eq!(recorded.schema_version, adopted.schema_version);

    Ok(MutationStore {
        database,
        handle: StoreHandle {
            incarnation: adopted,
        },
        physical_path,
        private_integrity,
    })
}

impl MutationWrite {
    pub fn owner_rows(&self) -> &WriteTransaction {
        &self.transaction
    }

    pub(crate) fn transaction(&self) -> &WriteTransaction {
        &self.transaction
    }

    pub(crate) fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        authenticate_private(self.private_integrity.as_deref(), sealed, digest)
    }

    pub(crate) fn commit(self) -> Result<(), String> {
        self.transaction.commit().map_err(|error| error.to_string())
    }

    pub(crate) fn abort(self) -> Result<(), String> {
        self.transaction.abort().map_err(|error| error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopeBinding {
    pub schema_version: u16,
    pub store_identity_digest: StoreIdentityDigest,
    pub identity: MutationScopeIdentity,
    pub initial_version: u64,
}

/// Open the exact physical database, initialize its derived root, bind one
/// logical scope, and execute owner bootstrap rows in one transaction.
pub fn initialize<F>(
    path: &Path,
    identity: &MutationScopeIdentity,
    initial_version: u64,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    bootstrap: F,
) -> Result<MutationStore, String>
where
    F: FnOnce(&WriteTransaction) -> Result<(), String>,
{
    let database = Database::create(path).map_err(|error| error.to_string())?;
    let (expected, physical_path) = StoreIncarnation::derive(path)?;
    let mut wtx = database.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    let handle = initialize_in(&wtx, &expected)?;
    if bind_scope_in(&handle, &wtx, identity, initial_version)? {
        bootstrap(&wtx)?;
    }
    wtx.commit().map_err(|error| error.to_string())?;
    Ok(MutationStore {
        database,
        handle,
        physical_path,
        private_integrity,
    })
}

/// Bind another logical scope and its owner bootstrap rows atomically.
pub fn bind_scope<F>(
    store: &MutationStore,
    identity: &MutationScopeIdentity,
    initial_version: u64,
    bootstrap: F,
) -> Result<(), String>
where
    F: FnOnce(&WriteTransaction) -> Result<(), String>,
{
    let write = store.write()?;
    let inserted = bind_scope_in(
        &write.handle,
        write.transaction(),
        identity,
        initial_version,
    )?;
    if inserted {
        bootstrap(write.owner_rows())?;
    }
    write.commit()
}

/// Transactional initialization seam for owner bootstrap rows.
fn initialize_in(
    wtx: &WriteTransaction,
    expected: &StoreIncarnation,
) -> Result<StoreHandle, String> {
    expected.validate_digest()?;
    reject_prototype_names(
        wtx.list_tables()
            .map_err(|error| error.to_string())?
            .map(|table| table.name().to_string()),
    )?;
    open_product_tables(wtx)?;
    let mut table = wtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    match persisted_root(&table)? {
        Some(stored) => {
            if stored != *expected {
                return Err("mutation store root incarnation mismatch".to_string());
            }
        }
        None => {
            let bytes = encode_bounded(expected, "mutation store root")?;
            table
                .insert(STORE_ROOT_KEY, bytes.as_slice())
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(StoreHandle {
        incarnation: expected.clone(),
    })
}

/// Bind a logical scope once. Exact re-entry is idempotent; any generation,
/// tenant, domain, resource, store-root, or initial-version mismatch fails closed.
fn bind_scope_in(
    handle: &StoreHandle,
    wtx: &WriteTransaction,
    identity: &MutationScopeIdentity,
    initial_version: u64,
) -> Result<bool, String> {
    validate_handle_write(handle, wtx)?;
    identity.validate_digest()?;
    let key = identity.binding_digest().to_hex();
    let proposed = ScopeBinding {
        schema_version: MUTATION_STORE_SCHEMA_VERSION,
        store_identity_digest: handle.incarnation.identity_digest(),
        identity: identity.clone(),
        initial_version,
    };
    let existing = read_scope_binding(wtx, &key)?;
    match existing {
        Some(stored) if stored == proposed => {
            require_existing_version(wtx, &key)?;
            Ok(false)
        }
        Some(_) => Err("mutation scope rebinding mismatch".to_string()),
        None => {
            persist_new_binding(wtx, &key, &proposed)?;
            Ok(true)
        }
    }
}

fn read_scope_binding(wtx: &WriteTransaction, key: &str) -> Result<Option<ScopeBinding>, String> {
    let table = wtx
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    // Bind before returning: the `AccessGuard` returned by `get` borrows
    // `table`, and a tail expression's temporaries outlive the local, so
    // returning this directly fails borrowck (E0597).
    let binding = table
        .get(key)
        .map_err(|error| error.to_string())?
        .map(|bytes| decode_binding(bytes.value()))
        .transpose();
    binding
}

fn require_existing_version(wtx: &WriteTransaction, key: &str) -> Result<(), String> {
    let table = wtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    if table.get(key).map_err(|error| error.to_string())?.is_none() {
        return Err("mutation scope binding is missing its version row".to_string());
    }
    Ok(())
}

fn persist_new_binding(
    wtx: &WriteTransaction,
    key: &str,
    binding: &ScopeBinding,
) -> Result<(), String> {
    let mut versions = wtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    if versions
        .get(key)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Err("mutation version row exists without a scope binding".to_string());
    }
    let bytes = encode_bounded(binding, "mutation scope binding")?;
    wtx.open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?
        .insert(key, bytes.as_slice())
        .map_err(|error| error.to_string())?;
    versions
        .insert(key, binding.initial_version)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn validate_handle_write(handle: &StoreHandle, wtx: &WriteTransaction) -> Result<(), String> {
    reject_prototype_names(
        wtx.list_tables()
            .map_err(|error| error.to_string())?
            .map(|table| table.name().to_string()),
    )?;
    let table = wtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    let stored = require_persisted_root(&table)?;
    if stored != handle.incarnation {
        return Err("mutation store handle does not match persisted root".to_string());
    }
    Ok(())
}

pub(crate) fn binding_for_write(
    write: &MutationWrite,
    identity: &MutationScopeIdentity,
) -> Result<ScopeBinding, String> {
    let handle = &write.handle;
    let wtx = write.transaction();
    validate_handle_write(handle, wtx)?;
    identity.validate_digest()?;
    let key = identity.binding_digest().to_hex();
    let table = wtx
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let bytes = table
        .get(key.as_str())
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "mutation scope is not bound to this store".to_string())?;
    validate_binding(decode_binding(bytes.value())?, handle, identity)
}

pub(crate) fn binding_for_read(
    store: &MutationStore,
    rtx: &ReadTransaction,
    identity: &MutationScopeIdentity,
) -> Result<ScopeBinding, String> {
    store.validate_physical_root()?;
    let handle = &store.handle;
    reject_prototype_names(
        rtx.list_tables()
            .map_err(|error| error.to_string())?
            .map(|table| table.name().to_string()),
    )?;
    let table = rtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    let stored = require_persisted_root(&table)?;
    if stored != handle.incarnation {
        return Err("mutation store handle does not match persisted root".to_string());
    }
    identity.validate_digest()?;
    let key = identity.binding_digest().to_hex();
    let table = rtx
        .open_table(SCOPE_BINDINGS)
        .map_err(|error| error.to_string())?;
    let bytes = table
        .get(key.as_str())
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "mutation scope is not bound to this store".to_string())?;
    validate_binding(decode_binding(bytes.value())?, handle, identity)
}

fn validate_binding(
    binding: ScopeBinding,
    handle: &StoreHandle,
    identity: &MutationScopeIdentity,
) -> Result<ScopeBinding, String> {
    if binding.identity != *identity
        || binding.store_identity_digest != handle.incarnation.identity_digest()
    {
        return Err("mutation scope binding identity mismatch".to_string());
    }
    Ok(binding)
}

pub(crate) fn scope_identity_key(identity: &MutationScopeIdentity) -> String {
    identity.identity_digest().to_hex()
}

fn decode_binding(bytes: &[u8]) -> Result<ScopeBinding, String> {
    let binding: ScopeBinding = decode_record(bytes)?;
    if binding.schema_version != MUTATION_STORE_SCHEMA_VERSION {
        return Err("unsupported mutation scope-binding schema".to_string());
    }
    binding.identity.validate_digest()?;
    Ok(binding)
}

pub(crate) fn reject_prototype_names(
    mut names: impl Iterator<Item = String>,
) -> Result<(), String> {
    if names.any(|name| RETIRED_PROTOTYPE_TABLES.contains(&name.as_str())) {
        return Err(
            "incompatible prototype mutation tables require quarantine before serving".to_string(),
        );
    }
    Ok(())
}

fn store_digest(schema_version: u16, root_id: &IncarnationId) -> StoreIdentityDigest {
    let mut hasher = Sha256::new();
    hasher.update(STORE_DIGEST_DOMAIN);
    hasher.update(schema_version.to_be_bytes());
    let bytes = root_id.as_str().as_bytes();
    let length = u32::try_from(bytes.len()).expect("validated store root identity fits LP32");
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    StoreIdentityDigest(hasher.finalize().into())
}

fn authenticate_private(
    integrity: Option<&dyn PrivatePayloadIntegrity>,
    sealed: &[u8],
    digest: &str,
) -> Result<(), String> {
    integrity
        .ok_or_else(|| "private recovery integrity authority is unavailable".to_string())?
        .authenticate(sealed, digest)
        .map_err(|_| "private recovery payload failed canonical authentication".to_string())
}

// NOTE: deliberately does NOT hash `path`. A store's physical identity is the
// device+inode it lives on, not the path string used to reach it: a store
// legitimately moves between a private staging path and its published
// location (a backup bundle) or gets copied to a fresh path (a restore),
// without becoming a different store. Binding identity to a location made
// "the same bytes at a different path" indistinguishable from "a substituted
// file" -- and only the second is the attack this control exists to stop
// (SEC-FINDING-V1-INCARNATION-BREAKS-RESTORE-20260903). `(dev, ino)` alone
// still detects a live store file being swapped out from under an open
// handle at its OWN unchanged path, which is the control's actual job.
#[cfg(unix)]
fn physical_root_id(path: &Path) -> Result<IncarnationId, String> {
    let metadata = std::fs::metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("mutation store physical root is not a regular file".to_string());
    }
    let mut hasher = Sha256::new();
    hasher.update(PHYSICAL_ROOT_DOMAIN);
    hasher.update(metadata.dev().to_be_bytes());
    hasher.update(metadata.ino().to_be_bytes());
    IncarnationId::new(format!("physical:sha256:{}", lower_hex(hasher.finalize())))
}

#[cfg(not(unix))]
fn physical_root_id(_path: &Path) -> Result<IncarnationId, String> {
    Err("physical mutation-store identity is unsupported on this platform".to_string())
}

fn lower_hex(bytes: impl AsRef<[u8]>) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}
