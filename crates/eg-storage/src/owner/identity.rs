use crate::physical::manifest::physical_identity_digest;
use eg_types::IncarnationId;
use serde::{Deserialize, Deserializer, Serialize};

/// Operator-supplied identity for a physical authority boundary.
///
/// This is deliberately independent of every logical serving scope. Creating a
/// store therefore cannot accidentally install a synthetic tenant or resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalStoreIdentity {
    name: IncarnationId,
    digest: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalStoreIdentityWire {
    name: IncarnationId,
    digest: [u8; 32],
}

impl PhysicalStoreIdentity {
    pub fn new(name: impl Into<String>) -> Result<Self, String> {
        let name = IncarnationId::new(name)?;
        Ok(Self {
            digest: physical_identity_digest(&name),
            name,
        })
    }

    pub fn name(&self) -> &IncarnationId {
        &self.name
    }

    pub fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.digest != physical_identity_digest(&self.name) {
            return Err("physical store identity digest mismatch".to_string());
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for PhysicalStoreIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = PhysicalStoreIdentityWire::deserialize(deserializer)?;
        let identity = Self {
            name: wire.name,
            digest: wire.digest,
        };
        identity.validate().map_err(serde::de::Error::custom)?;
        Ok(identity)
    }
}
