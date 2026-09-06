use crate::codec::{decode_ledger_record, encode_bounded};
use crate::owner::{
    validate_declared_owner_tables, validate_declared_tables_write, validate_manifest_read,
    validate_manifest_write,
};
use crate::physical::incarnation::{
    require_persisted_root, StoreIdentityDigest, STORAGE_KERNEL_SCHEMA_VERSION,
};
use crate::physical::root::{
    reject_prototype_names, validate_handle_write, PhysicalStore, StoreHandle,
};
use crate::tables::{SCOPE_BINDINGS, STORE_ROOT, VERSIONS};
use eg_types::MutationScopeIdentity;
use redb::{ReadTransaction, ReadableTable, TableHandle, WriteTransaction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopeBinding {
    pub schema_version: u16,
    pub store_identity_digest: StoreIdentityDigest,
    pub identity: MutationScopeIdentity,
    pub initial_version: u64,
}

/// Bind a logical scope once. Exact re-entry is idempotent; any generation,
/// tenant, domain, resource, store-root, or initial-version mismatch fails closed.
pub(crate) fn bind_scope_in(
    handle: &StoreHandle,
    wtx: &WriteTransaction,
    identity: &MutationScopeIdentity,
    initial_version: u64,
) -> Result<bool, String> {
    validate_handle_write(handle, wtx)?;
    identity.validate_digest()?;
    let key = identity.binding_digest().to_hex();
    let proposed = ScopeBinding {
        schema_version: STORAGE_KERNEL_SCHEMA_VERSION,
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

pub(crate) fn binding_for_write(
    store: &PhysicalStore,
    wtx: &WriteTransaction,
    identity: &MutationScopeIdentity,
) -> Result<ScopeBinding, String> {
    let handle = &store.handle;
    validate_handle_write(handle, wtx)?;
    let cached = store.manifest();
    let persisted = validate_manifest_write(wtx, &cached.physical_identity, cached.layout)?;
    if persisted != *cached {
        return Err("mutation write manifest authority changed".to_string());
    }
    validate_declared_tables_write(wtx, persisted.layout)?;
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
    store: &PhysicalStore,
    rtx: &ReadTransaction,
    identity: &MutationScopeIdentity,
) -> Result<ScopeBinding, String> {
    store.validate_physical_root()?;
    let handle = &store.handle;
    let cached = store.manifest();
    let persisted = validate_manifest_read(rtx, &cached.physical_identity, cached.layout)?;
    if persisted != *cached {
        return Err("mutation read manifest authority changed".to_string());
    }
    validate_declared_owner_tables(rtx, persisted.layout)?;
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

pub(crate) fn decode_binding(bytes: &[u8]) -> Result<ScopeBinding, String> {
    let binding: ScopeBinding = decode_ledger_record(bytes)?;
    if binding.schema_version != STORAGE_KERNEL_SCHEMA_VERSION {
        return Err("unsupported mutation scope-binding schema".to_string());
    }
    binding.identity.validate_digest()?;
    Ok(binding)
}
