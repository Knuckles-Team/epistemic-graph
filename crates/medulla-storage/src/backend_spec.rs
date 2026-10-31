//! Typed durable-store backend specification (MEDULLA-OBJECT-STORAGE-BACKEND-R001).
//!
//! A [`StorageBackendSpec`] declares the durable-store backend for a graph.
//! `local` (redb) is the default; `object_store` is S3-compatible. Endpoint,
//! bucket and root are named by secret reference only, never by value.
//! [`StorageBackendSpec::validate`] is the single admission step: it refuses
//! atomically with a typed [`BackendSpecRefusal`] before any effect.
//! Only the `local` variant is opened by this build; an `object_store` spec
//! that validates is reported as [`BackendSpecRefusal::Unsupported`] by
//! [`StorageBackendSpec::admit`] until the object-store open path lands
//! (R002-R004), so nothing silently falls back to redb.

use crate::owner::lineage::{pinned_layout_digest, ALL_LAYOUTS};
use serde::{Deserialize, Serialize};

/// How durable a committed manifest publish is before the client is acked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityClass {
    /// Acked after the local fsync.
    LocalFsync,
    /// Acked after the object store confirms the conditional manifest swap.
    ObjectStoreConditionalPut,
}

/// A reference to a secret held outside the spec; never the secret itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef(pub String);

/// The declared durable-store backend for a graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "variant", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageBackendSpec {
    /// redb on local disk: the default.
    Local,
    /// S3-compatible object storage.
    ObjectStore {
        endpoint: Option<SecretRef>,
        bucket: Option<SecretRef>,
        root: Option<SecretRef>,
        durability: Option<DurabilityClass>,
        /// Hex layout digest naming an entry in the owner-store lineage registry.
        layout_digest: Option<String>,
    },
}

impl Default for StorageBackendSpec {
    fn default() -> Self {
        Self::Local
    }
}

/// Typed, atomic refusal; none of these leaves any state changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "code", rename_all = "snake_case")]
pub enum BackendSpecRefusal {
    /// A required secret reference is absent or empty.
    MissingSecretRef { field: String },
    /// The object-store spec declares no durability class.
    MissingDurabilityClass,
    /// The variant tag is not one this engine knows.
    UnknownVariant { variant: String },
    /// The layout digest is absent from the lineage registry.
    LayoutNotInLineage { layout_digest: String },
    /// The spec is valid but this build cannot open it.
    Unsupported { variant: String },
}

impl StorageBackendSpec {
    /// Decode a wire spec (named-field msgpack); an unknown variant tag or
    /// malformed body is a typed refusal.
    pub fn from_wire(bytes: &[u8]) -> Result<Self, BackendSpecRefusal> {
        rmp_serde::from_slice(bytes).map_err(|e| BackendSpecRefusal::UnknownVariant {
            variant: e.to_string(),
        })
    }

    /// Encode to the wire form [`Self::from_wire`] reads.
    pub fn to_wire(&self) -> Vec<u8> {
        rmp_serde::to_vec_named(self).expect("backend spec encodes")
    }

    /// Check the spec is bound and names a registered layout. Pure.
    pub fn validate(&self) -> Result<(), BackendSpecRefusal> {
        let Self::ObjectStore {
            endpoint,
            bucket,
            root,
            durability,
            layout_digest,
        } = self
        else {
            return Ok(());
        };
        for (field, secret) in [("endpoint", endpoint), ("bucket", bucket), ("root", root)] {
            match secret {
                Some(SecretRef(name)) if !name.trim().is_empty() => {}
                _ => {
                    return Err(BackendSpecRefusal::MissingSecretRef {
                        field: field.to_string(),
                    })
                }
            }
        }
        if durability.is_none() {
            return Err(BackendSpecRefusal::MissingDurabilityClass);
        }
        let digest = layout_digest.clone().unwrap_or_default();
        if !ALL_LAYOUTS
            .iter()
            .any(|l| pinned_layout_digest(*l) == digest)
        {
            return Err(BackendSpecRefusal::LayoutNotInLineage {
                layout_digest: digest,
            });
        }
        Ok(())
    }

    /// Admission for opening: validate, then refuse any variant this build
    /// cannot open. `Local` is the only openable variant today.
    pub fn admit(&self) -> Result<(), BackendSpecRefusal> {
        self.validate()?;
        match self {
            Self::Local => Ok(()),
            Self::ObjectStore { .. } => Err(BackendSpecRefusal::Unsupported {
                variant: "object_store".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn good() -> StorageBackendSpec {
        StorageBackendSpec::ObjectStore {
            endpoint: Some(SecretRef("bao:eg/endpoint".into())),
            bucket: Some(SecretRef("bao:eg/bucket".into())),
            root: Some(SecretRef("bao:eg/root".into())),
            durability: Some(DurabilityClass::ObjectStoreConditionalPut),
            layout_digest: Some(pinned_layout_digest(ALL_LAYOUTS[0]).to_string()),
        }
    }

    #[test]
    fn default_is_local_and_admits() {
        assert_eq!(StorageBackendSpec::default(), StorageBackendSpec::Local);
        assert_eq!(StorageBackendSpec::Local.admit(), Ok(()));
    }

    // spec: MEDULLA-OBJECT-STORAGE-BACKEND-R001
    #[test]
    fn both_variants_round_trip_over_the_wire() {
        for spec in [StorageBackendSpec::Local, good()] {
            assert_eq!(StorageBackendSpec::from_wire(&spec.to_wire()).unwrap(), spec);
        }
    }

    // spec: MEDULLA-OBJECT-STORAGE-BACKEND-R001
    #[test]
    fn refusals_are_typed_and_pure() {
        assert_eq!(good().validate(), Ok(()));
        let mut spec = good();
        let before = spec.clone();
        if let StorageBackendSpec::ObjectStore { bucket, .. } = &mut spec {
            *bucket = None;
        }
        assert_eq!(
            spec.validate(),
            Err(BackendSpecRefusal::MissingSecretRef {
                field: "bucket".into()
            })
        );
        assert_ne!(spec, before);
        let mut spec = good();
        if let StorageBackendSpec::ObjectStore { durability, .. } = &mut spec {
            *durability = None;
        }
        assert_eq!(spec.validate(), Err(BackendSpecRefusal::MissingDurabilityClass));
        let mut spec = good();
        if let StorageBackendSpec::ObjectStore { layout_digest, .. } = &mut spec {
            *layout_digest = Some("00".into());
        }
        assert!(matches!(
            spec.validate(),
            Err(BackendSpecRefusal::LayoutNotInLineage { .. })
        ));
        #[derive(serde::Serialize)]
        struct Tape {
            variant: &'static str,
        }
        let tape = rmp_serde::to_vec_named(&Tape { variant: "tape" }).unwrap();
        assert!(matches!(
            StorageBackendSpec::from_wire(&tape),
            Err(BackendSpecRefusal::UnknownVariant { .. })
        ));
        assert!(matches!(
            good().admit(),
            Err(BackendSpecRefusal::Unsupported { .. })
        ));
    }
}
