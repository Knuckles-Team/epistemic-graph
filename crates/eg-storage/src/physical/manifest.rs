use crate::codec::encode_bounded;
use crate::owner::contract::expected_table_contracts;
use crate::owner::identity::PhysicalStoreIdentity;
use crate::owner::layout::{layout_domain_tag, OwnerLayout};
use crate::physical::incarnation::{StoreIncarnation, STORAGE_KERNEL_SCHEMA_VERSION};
use eg_types::mutation_batch::DurabilityDomain;
use eg_types::IncarnationId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const PHYSICAL_IDENTITY_DOMAIN: &[u8] = b"eg/mutation-physical-identity/v1\0";
const OWNER_MANIFEST_DIGEST_DOMAIN: &[u8] = b"eg/mutation-owner-manifest/v1\0";

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
    pub(crate) domain: Option<DurabilityDomain>,
    pub(crate) scope: TableScope,
    pub(crate) capabilities: u16,
    pub(crate) index: bool,
    pub(crate) derived: bool,
}

impl OwnerManifest {
    pub(crate) fn new(
        physical_identity: PhysicalStoreIdentity,
        layout: OwnerLayout,
    ) -> Result<Self, String> {
        physical_identity.validate()?;
        Ok(Self {
            schema_version: STORAGE_KERNEL_SCHEMA_VERSION,
            physical_identity,
            layout,
            layout_digest: layout.digest(),
            authority_epoch: 0,
            tables: expected_table_contracts(layout),
        })
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.schema_version != STORAGE_KERNEL_SCHEMA_VERSION {
            return Err("unsupported mutation owner-manifest schema".to_string());
        }
        self.physical_identity.validate()?;
        if self.layout_digest != self.layout.digest() {
            return Err("mutation owner-layout digest mismatch".to_string());
        }
        if self.tables != expected_table_contracts(self.layout) {
            return Err("mutation owner table registry mismatch".to_string());
        }
        Ok(())
    }

    pub(crate) fn authority_digest(&self, incarnation: &StoreIncarnation) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(layout_domain_tag());
        hasher.update(incarnation.identity_digest().as_bytes());
        hasher.update(self.physical_identity.digest());
        hasher.update(self.layout_digest);
        hasher.update(self.authority_epoch.to_be_bytes());
        hasher.finalize().into()
    }

    pub(crate) fn digest(&self) -> Result<OwnerManifestDigest, String> {
        self.validate()?;
        let encoded = encode_bounded(self, "mutation owner manifest digest")?;
        let mut hasher = Sha256::new();
        hasher.update(OWNER_MANIFEST_DIGEST_DOMAIN);
        hasher.update(
            u64::try_from(encoded.len())
                .map_err(|_| "mutation owner manifest length overflow".to_string())?
                .to_be_bytes(),
        );
        hasher.update(encoded);
        Ok(OwnerManifestDigest::new(hasher.finalize().into()))
    }
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
