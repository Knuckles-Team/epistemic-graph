//! One admitted, encrypted owner store for foreign-source definitions.
//!
//! The registered spec and its mutation receipt commit in the same
//! ForeignCatalogOwner transaction. A restarted server loads only committed,
//! authenticated rows; there is no second sidecar mirror of an admin saga.

use std::path::Path;

use eg_storage::{OwnedStoreHandle, PhysicalStoreIdentity, StorageKernel};
use eg_transaction::{Begin, MutationKernel};
use eg_types::mutation_batch::{
    BatchContent, CompiledEnvelope, CompiledOperation, CompiledScope, DurabilityDomain,
    MutationBatch, MutationEnvelope, MutationOperation, MutationSurface,
};
use eg_types::protocol::Method;
use eg_types::wire::ForeignSourceSpec;
use redb::{ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::access::CarrierAuthority;

const FILE: &str = "foreign_catalog.redb";
const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("foreign_source_specs");
const MAX_ROWS: usize = 16_384;
const MAX_SEALED_BYTES: usize = 256 * 1024;

#[derive(Serialize, Deserialize)]
pub(super) struct StoredSource {
    pub(super) owner_scope: String,
    pub(super) name: String,
    pub(super) owner_agent: String,
    pub(super) spec: ForeignSourceSpec,
}

pub(super) struct ForeignCatalogStore {
    kernel: StorageKernel,
    mutations: MutationKernel,
    owner: OwnedStoreHandle<eg_storage::ForeignCatalogOwner>,
    cipher: Option<crate::crypto::ValueCipher>,
}

impl ForeignCatalogStore {
    pub(super) fn open(persist_dir: &str, tenant: &str) -> Result<Self, String> {
        let cipher = crate::crypto::ValueCipher::from_env_checked()?;
        Self::open_with_cipher(persist_dir, tenant, cipher)
    }

    fn open_with_cipher(
        persist_dir: &str,
        tenant: &str,
        cipher: Option<crate::crypto::ValueCipher>,
    ) -> Result<Self, String> {
        let path = Path::new(persist_dir).join(FILE);
        std::fs::create_dir_all(persist_dir).map_err(|error| error.to_string())?;
        let identity = eg_types::MutationScopeIdentity::fixed_native(
            tenant,
            DurabilityDomain::ControlPlane,
            "foreign-catalog",
            "foreign-catalog:v1",
        )?;
        let physical_name = format!(
            "epistemic-graph:foreign-catalog:{}",
            digest(tenant.as_bytes())
        );
        let physical = PhysicalStoreIdentity::new(physical_name.as_str())?;
        let kernel = if path.exists() {
            StorageKernel::open_owner::<eg_storage::ForeignCatalogOwner>(&path, physical, None)
        } else {
            StorageKernel::create_owner::<eg_storage::ForeignCatalogOwner>(&path, physical, None)
        }?;
        let (kernel, write_authority) = kernel.into_read_and_mutation_authority()?;
        let mutations = MutationKernel::new(write_authority);
        let authority = crate::store_authority::process_authority();
        let grant = kernel.authenticate_scope::<eg_storage::ForeignCatalogOwner>(
            authority,
            identity,
            authority.principal().to_string(),
            &authority.proof(),
        )?;
        let owner = kernel.bind_serving_scope(grant, 0)?;
        mutations.bootstrap_ledger(&owner)?;
        Ok(Self {
            kernel,
            mutations,
            owner,
            cipher,
        })
    }

    pub(super) fn load(&self) -> Result<Vec<StoredSource>, String> {
        let read = self.kernel.read_scope(&self.owner)?;
        let table = read.open_owner_table(TABLE)?;
        let mut rows = Vec::new();
        for entry in table.iter().map_err(|error| error.to_string())? {
            if rows.len() >= MAX_ROWS {
                return Err("foreign source catalog exceeds row limit".to_string());
            }
            let (key, value) = entry.map_err(|error| error.to_string())?;
            let sealed = value.value();
            if sealed.len() > MAX_SEALED_BYTES {
                return Err("foreign source catalog row exceeds size limit".to_string());
            }
            let cipher = self.cipher.as_ref().ok_or_else(|| {
                "FOREIGN_SOURCE_ENCRYPTION_KEY_REQUIRED: catalog has encrypted sources".to_string()
            })?;
            let bytes = cipher.unseal(sealed)?;
            let row: StoredSource = rmp_serde::from_slice(&bytes)
                .map_err(|_| "invalid encrypted foreign source row".to_string())?;
            if key.value() != source_key(&row.owner_scope, &row.name) {
                return Err("foreign source catalog key mismatch".to_string());
            }
            rows.push(row);
        }
        Ok(rows)
    }

    pub(super) fn register(
        &self,
        authority: &CarrierAuthority,
        request_id: u64,
        name: &str,
        spec: &ForeignSourceSpec,
    ) -> Result<(), String> {
        let cipher = self.cipher.as_ref().ok_or_else(|| {
            "FOREIGN_SOURCE_ENCRYPTION_KEY_REQUIRED: registration requires an at-rest key"
                .to_string()
        })?;
        let row = StoredSource {
            owner_scope: authority.owner_scope().to_string(),
            name: name.to_string(),
            owner_agent: authority.agent_id().to_string(),
            spec: spec.clone(),
        };
        let bytes =
            rmp_serde::to_vec_named(&row).map_err(|_| "invalid foreign source".to_string())?;
        if bytes.len() > MAX_SEALED_BYTES / 2 {
            return Err("INVALID_ARGUMENT: foreign source definition is too large".to_string());
        }
        let sealed = cipher.seal(&bytes);
        let key = source_key(&row.owner_scope, name);
        let digest = digest(&bytes);
        let batch_id = authority.namespace("foreign-source-register", authority.idempotency_key());
        let (txn, batch, begun) = self.mutations.admit_current(&self.owner, |version| {
            register_batch(
                &self.owner,
                authority,
                request_id,
                &batch_id,
                &digest,
                version,
            )
        })?;
        let source_version = match begun {
            Begin::Replay(_) => {
                txn.abort()?;
                return Err("FOREIGN_SOURCE_REPLAY: registration was already committed".to_string());
            }
            Begin::Apply { source_version } => source_version,
        };
        let write = txn.owner_rows(&self.owner, &batch)?;
        let insert = (|| {
            let mut table = write.open_table(TABLE)?;
            let exists = table
                .get(key.as_str())
                .map_err(|error| error.to_string())?
                .is_some();
            if !exists && table.len().map_err(|error| error.to_string())? >= MAX_ROWS as u64 {
                return Err("foreign source catalog exceeds row limit".to_string());
            }
            table
                .insert(key.as_str(), sealed.as_slice())
                .map(|_| ())
                .map_err(|error| error.to_string())
        })();
        if let Err(error) = insert {
            write.finish_owner()?;
            txn.abort()?;
            return Err(error);
        }
        write.finish_owner()?;
        self.mutations
            .finish(&txn, &batch, None, 0, source_version)?;
        self.mutations.commit(txn, &batch)
    }
}

fn source_key(owner_scope: &str, name: &str) -> String {
    let mut hash = Sha256::new();
    for value in [owner_scope, name] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hex::encode(hash.finalize())
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn register_batch(
    owner: &OwnedStoreHandle<eg_storage::ForeignCatalogOwner>,
    authority: &CarrierAuthority,
    request_id: u64,
    batch_id: &str,
    input_digest: &str,
    version: u64,
) -> Result<MutationBatch, String> {
    let operations = vec![MutationOperation {
        ordinal: 0,
        surface: MutationSurface::Other,
        domain: DurabilityDomain::ControlPlane,
        method: Method::ApplyMutation {
            event_type: "foreign_source_register".to_string(),
            query: format!("sha256:{input_digest}"),
        },
    }];
    let outbox = Vec::new();
    let content = BatchContent {
        operations: &operations,
        outbox: &outbox,
        authoritative_state: None,
    };
    let (_, schema_digest) = eg_capabilities::method_schema("ApplyMutation")
        .ok_or_else(|| "ApplyMutation is missing from contract".to_string())?;
    let compiled = CompiledOperation::for_content(
        owner.identity(),
        content,
        eg_types::contract::Digest256::from_bytes(schema_digest),
    )?;
    let now = crate::server::dispatch::authoritative_now_ms();
    let mut envelope = CompiledEnvelope::new(
        CompiledScope {
            identity: owner.identity(),
            actor: authority.actor_scope(),
            serving_principal: owner.principal(),
            request_id,
            idempotency_key: batch_id,
            nonce: authority
                .attempt_nonce()
                .unwrap_or_else(eg_types::contract::Nonce::minted),
            now_ms: now,
        },
        compiled,
    )?;
    envelope.catalog_digest =
        eg_types::contract::Digest256::parse(eg_capabilities::CONTRACT_CATALOG_DIGEST)?;
    let batch = MutationBatch::native(
        batch_id,
        MutationEnvelope::for_compiled_batch(envelope)?,
        owner.identity().clone(),
        version,
        (operations, outbox),
        now,
    );
    batch.validate_write_budget()?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sealed_owner_store_reopens_without_exposing_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let key = crate::crypto::ValueCipher::from_key_material(b"foreign-catalog-test-key");
        let owner = CarrierAuthority::verified_for_test("alice");
        let spec = ForeignSourceSpec::HttpJson {
            url: "https://example.org/secret-credential-sentinel".into(),
            json_path: "items".into(),
            field_map: eg_types::wire::HttpFieldMap {
                id: "id".into(),
                score: None,
            },
        };
        {
            let store =
                ForeignCatalogStore::open_with_cipher(path, "test-tenant", Some(key.clone()))
                    .unwrap();
            store.register(&owner, 42, "source-a", &spec).unwrap();
            assert_eq!(store.load().unwrap().len(), 1);
        }
        let raw = std::fs::read(dir.path().join(FILE)).unwrap();
        assert!(!raw
            .windows(b"secret-credential-sentinel".len())
            .any(|window| window == b"secret-credential-sentinel"));
        let reopened =
            ForeignCatalogStore::open_with_cipher(path, "test-tenant", Some(key)).unwrap();
        let rows = reopened.load().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].owner_scope, owner.owner_scope());
        assert_eq!(rows[0].name, "source-a");
        assert_eq!(rows[0].spec, spec);
    }

    #[test]
    fn encrypted_catalog_refuses_restart_without_its_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let key = crate::crypto::ValueCipher::from_key_material(b"foreign-catalog-test-key");
        let owner = CarrierAuthority::verified_for_test("alice");
        {
            let store =
                ForeignCatalogStore::open_with_cipher(path, "test-tenant", Some(key)).unwrap();
            store
                .register(
                    &owner,
                    43,
                    "source-a",
                    &ForeignSourceSpec::Named { name: "x".into() },
                )
                .unwrap();
        }
        let reopened = ForeignCatalogStore::open_with_cipher(path, "test-tenant", None).unwrap();
        assert!(reopened
            .load()
            .unwrap_err()
            .contains("FOREIGN_SOURCE_ENCRYPTION_KEY_REQUIRED"));
    }
}
