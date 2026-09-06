use crate::owner::{OwnerLayout, PhysicalStoreIdentity};
use crate::MutationStore;
use eg_types::{IncarnationId, MutationDomain};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PHYSICAL_IDENTITY_DOMAIN: &[u8] = b"eg/mutation-physical-identity/v1\0";

/// Opaque digest of one exact current owner manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OwnerManifestDigest([u8; 32]);

impl OwnerManifestDigest {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl MutationStore {
    pub fn owner_manifest_digest(&self) -> Result<OwnerManifestDigest, String> {
        self.current_owner_manifest()?.digest()
    }

    /// Digest of the current persisted owner manifest anchored to this exact
    /// store incarnation. Unlike the manifest digest, this changes when an
    /// otherwise identical store image is adopted at a new inode.
    pub fn owner_authority_digest(&self) -> Result<[u8; 32], String> {
        Ok(self
            .current_owner_manifest()?
            .authority_digest(self.incarnation()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnerManifest {
    pub(crate) schema_version: u16,
    pub(crate) physical_identity: PhysicalStoreIdentity,
    pub(crate) layout: OwnerLayout,
    pub(crate) layout_digest: [u8; 32],
    pub(crate) authority_epoch: u64,
    pub(crate) tables: Vec<TableContract>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TableOwnership {
    Ledger,
    Owner,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TableScope {
    Physical,
    Serving,
    StorePrivate,
    SharedService,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableContract {
    pub(crate) table_id: String,
    pub(crate) schema_id: String,
    pub(crate) key_type_id: String,
    pub(crate) value_type_id: String,
    pub(crate) key_codec: String,
    pub(crate) value_codec: String,
    pub(crate) logical_schema_id: String,
    pub(crate) logical_codec_id: String,
    pub(crate) ownership: TableOwnership,
    pub(crate) domain: Option<MutationDomain>,
    pub(crate) scope: TableScope,
    pub(crate) capabilities: u16,
    pub(crate) index: bool,
    pub(crate) derived: bool,
}

pub(crate) fn physical_identity_digest(name: &IncarnationId) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(PHYSICAL_IDENTITY_DOMAIN);
    hasher.update((name.as_str().len() as u64).to_be_bytes());
    hasher.update(name.as_str().as_bytes());
    hasher.finalize().into()
}

pub(crate) fn hash_table_contract(hasher: &mut Sha256, contract: &TableContract) {
    for field in [
        contract.table_id.as_str(),
        contract.schema_id.as_str(),
        contract.key_type_id.as_str(),
        contract.value_type_id.as_str(),
        contract.key_codec.as_str(),
        contract.value_codec.as_str(),
        contract.logical_schema_id.as_str(),
        contract.logical_codec_id.as_str(),
    ] {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field.as_bytes());
    }
    hasher.update([contract.ownership as u8]);
    hasher.update([contract.scope as u8]);
    hasher.update(contract.capabilities.to_be_bytes());
    hasher.update([contract.index as u8, contract.derived as u8]);
    hasher.update([contract.domain.map_or(u8::MAX, |domain| domain as u8)]);
}
