//! The connect-time seed graph carried beside a connector pack.
//!
//! A seed graph is a small, typed set of instance, catalog and reference
//! triples.  The import handler owns parsing and validation; this module keeps
//! the wire shape closed so a caller cannot silently add a fourth part kind.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::contract::{Digest256, ResourceId};

/// The three seed parts a connector may publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SeedPartKey {
    Identity,
    Catalog,
    References,
}

/// One seed part, addressed by the map key in [`SeedGraphImport::parts`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SeedPart {
    pub key: SeedPartKey,
    pub turtle: String,
    pub digest: Digest256,
}

/// The typed body supplied beside a connector pack at connect time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SeedGraphImport {
    pub connector: ResourceId,
    pub instance_iri: String,
    pub pack_digest: Digest256,
    pub parts: BTreeMap<SeedPartKey, SeedPart>,
    pub content_digest: Digest256,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(byte: u8) -> Digest256 {
        Digest256::from_bytes([byte; 32])
    }

    fn sample() -> SeedGraphImport {
        let mut parts = BTreeMap::new();
        parts.insert(
            SeedPartKey::Identity,
            SeedPart {
                key: SeedPartKey::Identity,
                turtle: "<urn:seed:instance> a <urn:seed:Identity> .".to_string(),
                digest: digest(2),
            },
        );
        parts.insert(
            SeedPartKey::Catalog,
            SeedPart {
                key: SeedPartKey::Catalog,
                turtle: "<urn:seed:item> a <urn:seed:CatalogEntry> .".to_string(),
                digest: digest(3),
            },
        );
        parts.insert(
            SeedPartKey::References,
            SeedPart {
                key: SeedPartKey::References,
                turtle: "<urn:seed:item> <urn:seed:references> <urn:source:item> .".to_string(),
                digest: digest(4),
            },
        );
        SeedGraphImport {
            connector: ResourceId::new("example").unwrap(),
            instance_iri: "urn:connector:example:instance".to_string(),
            pack_digest: digest(1),
            parts,
            content_digest: digest(5),
        }
    }

    /// R104.1: every model field round-trips, and the closed part key rejects
    /// an unknown map member before any seed import handler can run.
    #[test]
    fn seed_import_round_trips() {
        let import = sample();
        let wire = serde_json::to_value(&import).unwrap();
        let decoded: SeedGraphImport = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(decoded, import);
        assert_eq!(wire["parts"]["identity"]["key"], "identity");
        assert_eq!(wire["parts"]["catalog"]["key"], "catalog");
        assert_eq!(wire["parts"]["references"]["key"], "references");

        let mut unknown = wire;
        let parts = unknown["parts"].as_object_mut().unwrap();
        let identity = parts.remove("identity").unwrap();
        parts.insert("unknown".to_string(), identity);
        assert!(serde_json::from_value::<SeedGraphImport>(unknown).is_err());
    }
}
