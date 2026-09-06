use crate::codec::encode_bounded;
use crate::owner::{validate_declared_tables_write, validate_manifest_write};
use crate::physical::incarnation::{
    persisted_root, require_persisted_root, StoreIncarnation, STORE_ROOT_KEY,
};
use crate::physical::integrity::{authenticate_with, PrivatePayloadIntegrity};
use crate::physical::manifest::OwnerManifest;
use crate::tables::{open_declared_ledger_tables, STORE_ROOT};
use redb::{Database, ReadTransaction, ReadableDatabase, TableHandle, WriteTransaction};
use std::path::PathBuf;
use std::sync::Arc;

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
    // The SQL `TableStore`'s private mutation ledger, retired onto
    // `MutationKernelV1`'s by RF-RULING-006. Unlike the bare
    // `mutation_batches`/`mutation_idempotency`/`mutation_outbox` names above,
    // these five are unambiguous: no live subsystem writes a `__sql_mutation_*__`
    // table, and only `eg-query`'s retired second ledger ever did. A file still
    // carrying one is quarantined rather than served -- greenfield, so there is
    // no reader for those rows anywhere.
    "__sql_mutation_batches__",
    "__sql_mutation_idempotency__",
    "__sql_mutation_version__",
    "__sql_mutation_fence__",
    "__sql_mutation_outbox__",
];

/// Non-serializable proof that a store root was derived from and matched the
/// exact physical database.
#[derive(Debug, Clone)]
pub(crate) struct StoreHandle {
    pub(crate) incarnation: StoreIncarnation,
}

/// One open physical owner file: its database, its non-forgeable root, its
/// declared owner manifest, and the integrity authority for sealed payloads.
///
/// This is the whole physical authority. It is never handed to a consumer; the
/// kernel issues scoped read, snapshot and write capabilities over it instead.
pub(crate) struct PhysicalStore {
    pub(crate) database: Database,
    pub(crate) handle: StoreHandle,
    physical_path: PathBuf,
    private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
    owner_manifest: OwnerManifest,
}

impl PhysicalStore {
    pub(crate) fn database(&self) -> &Database {
        &self.database
    }

    pub(crate) fn incarnation(&self) -> &StoreIncarnation {
        &self.handle.incarnation
    }

    pub(crate) fn manifest(&self) -> &OwnerManifest {
        &self.owner_manifest
    }

    /// Begin the one physical write transaction, revalidating physical root,
    /// persisted root, manifest authority and the declared table census first.
    pub(crate) fn begin_write(&self) -> Result<WriteTransaction, String> {
        self.validate_physical_root()?;
        let mut transaction = self
            .database
            .begin_write()
            .map_err(|error| error.to_string())?;
        transaction
            .set_durability(redb::Durability::Immediate)
            .map_err(|error| error.to_string())?;
        validate_handle_write(&self.handle, &transaction)?;
        let manifest = &self.owner_manifest;
        let persisted =
            validate_manifest_write(&transaction, &manifest.physical_identity, manifest.layout)?;
        if persisted != *manifest {
            return Err("cached owner manifest differs from persisted write authority".to_string());
        }
        validate_declared_tables_write(&transaction, manifest.layout)?;
        Ok(transaction)
    }

    pub(crate) fn begin_read(&self) -> Result<ReadTransaction, String> {
        self.database
            .begin_read()
            .map_err(|error| error.to_string())
    }

    pub(crate) fn validate_physical_root(&self) -> Result<(), String> {
        let (derived, canonical) = StoreIncarnation::derive(&self.physical_path)?;
        if derived != self.handle.incarnation || canonical != self.physical_path {
            return Err("mutation store physical root identity changed".to_string());
        }
        Ok(())
    }

    pub(crate) fn authenticate_private(&self, sealed: &[u8], digest: &str) -> Result<(), String> {
        authenticate_with(self.private_integrity.as_deref(), sealed, digest)
    }

    pub(crate) fn private_integrity(&self) -> Option<Arc<dyn PrivatePayloadIntegrity>> {
        self.private_integrity.clone()
    }

    pub(crate) fn from_parts(
        database: Database,
        handle: StoreHandle,
        physical_path: PathBuf,
        private_integrity: Option<Arc<dyn PrivatePayloadIntegrity>>,
        owner_manifest: OwnerManifest,
    ) -> Self {
        Self {
            database,
            handle,
            physical_path,
            private_integrity,
            owner_manifest,
        }
    }
}

/// Transactional initialization seam for owner bootstrap rows.
pub(crate) fn initialize_in(
    wtx: &WriteTransaction,
    expected: &StoreIncarnation,
) -> Result<StoreHandle, String> {
    expected.validate_digest()?;
    reject_prototype_names(
        wtx.list_tables()
            .map_err(|error| error.to_string())?
            .map(|table| table.name().to_string()),
    )?;
    open_declared_ledger_tables(wtx)?;
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

pub(crate) fn initialize_strict_in(
    wtx: &WriteTransaction,
    expected: &StoreIncarnation,
    manifest: &OwnerManifest,
) -> Result<StoreHandle, String> {
    manifest.validate()?;
    initialize_in(wtx, expected)
}

pub(crate) fn store_handle(incarnation: StoreIncarnation) -> StoreHandle {
    StoreHandle { incarnation }
}

pub(crate) fn validate_incarnation_read(
    rtx: &ReadTransaction,
    expected: &StoreIncarnation,
) -> Result<(), String> {
    reject_prototype_names(
        rtx.list_tables()
            .map_err(|error| error.to_string())?
            .map(|table| table.name().to_string()),
    )?;
    let root = rtx
        .open_table(STORE_ROOT)
        .map_err(|error| error.to_string())?;
    if require_persisted_root(&root)? != *expected {
        return Err("mutation store root incarnation mismatch".to_string());
    }
    Ok(())
}

pub(crate) fn validate_handle_write(
    handle: &StoreHandle,
    wtx: &WriteTransaction,
) -> Result<(), String> {
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

pub(crate) fn reject_prototype_names(
    mut names: impl Iterator<Item = String>,
) -> Result<(), String> {
    if names.any(|name| is_retired_prototype_table(&name)) {
        return Err(
            "incompatible prototype mutation tables require quarantine before serving".to_string(),
        );
    }
    Ok(())
}

pub(crate) fn is_retired_prototype_table(name: &str) -> bool {
    RETIRED_PROTOTYPE_TABLES.contains(&name)
}

#[cfg(test)]
pub(crate) fn retired_prototype_table_names() -> &'static [&'static str] {
    RETIRED_PROTOTYPE_TABLES
}
