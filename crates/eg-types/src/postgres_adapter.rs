//! EG-UNIFIED-DATA-PLANE-R013 — typed model for the Postgres attached-source
//! adapter. This slice is the config/capability types and their validation
//! only: connection shape, the pgoutput logical-replication capture
//! parameters, and the lossless type-mapping declarations for jsonb, array,
//! enum, and pgvector columns. The sqlx/tokio-postgres wiring, pg_catalog
//! read, and pgoutput decode loop are later slices of this requirement.

use serde::{Deserialize, Serialize};

/// TLS posture for the Postgres connection. Mirrors libpq's `sslmode` values
/// that this adapter actually supports; unlisted modes are rejected by the
/// caller before this type is ever constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PostgresSslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

/// A lossless column type mapping. Each variant captures exactly the extra
/// shape Postgres carries beyond a plain scalar, so the adapter can round-trip
/// the value without widening or truncating it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PostgresTypeMapping {
    /// A plain scalar column (int4, text, timestamptz, ...), named by its
    /// Postgres type name.
    Scalar { pg_type: String },
    /// `jsonb` (or `json`), preserved as opaque structured text — never
    /// flattened into the graph's own JSON representation.
    Jsonb,
    /// A one-dimensional array of some element mapping, e.g. `int4[]`.
    Array { element: Box<PostgresTypeMapping> },
    /// A Postgres enum type, carrying its ordered variant labels so the
    /// adapter can validate and round-trip values without a catalog round
    /// trip per row.
    Enum {
        type_name: String,
        variants: Vec<String>,
    },
    /// A `pgvector` `vector(n)` column.
    Vector { dimensions: u32 },
}

/// pgoutput logical-replication capture parameters for one attached
/// Postgres source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PostgresReplicationConfig {
    /// Replication slot name. Must already exist or be created with the
    /// `pgoutput` plugin before capture starts; this type does not create it.
    pub slot_name: String,
    /// `PUBLICATION` name whose tables this slot streams.
    pub publication_name: String,
}

/// Connection + capture + type-mapping configuration for one Postgres
/// attached source. This is the typed model the later connect/catalog/capture
/// slices of R013 consume; it does nothing on its own beyond validating its
/// own shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PostgresAdapterConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub ssl_mode: PostgresSslMode,
    /// Present only when this source is also a change-capture source. A
    /// federation-only (query-only) attachment leaves this `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<PostgresReplicationConfig>,
    /// Declared mappings for the columns this adapter is responsible for,
    /// keyed by `"schema.table.column"`.
    pub type_mappings: Vec<(String, PostgresTypeMapping)>,
}

/// Why a [`PostgresAdapterConfig`] (or a [`PostgresTypeMapping`] within it)
/// was refused. Every variant names the offending value so the caller can
/// report it without re-deriving the reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PostgresAdapterConfigError {
    EmptyHost,
    EmptyDatabase,
    /// Replication was requested with an empty slot or publication name.
    EmptyReplicationIdentifier,
    /// A `vector(n)` mapping declared zero dimensions.
    ZeroVectorDimensions {
        column: String,
    },
    /// An `enum` mapping declared no variants.
    EmptyEnumVariants {
        type_name: String,
    },
    /// An `enum` mapping repeated the same variant label.
    DuplicateEnumVariant {
        type_name: String,
        variant: String,
    },
    /// The same `"schema.table.column"` key appeared more than once.
    DuplicateColumnMapping {
        column: String,
    },
}

impl PostgresAdapterConfig {
    /// Validates this config's own shape. Does not contact Postgres: a
    /// non-existent slot/publication/host is a connect-time failure, not a
    /// refusal of this typed value.
    pub fn validate(&self) -> Result<(), PostgresAdapterConfigError> {
        if self.host.trim().is_empty() {
            return Err(PostgresAdapterConfigError::EmptyHost);
        }
        if self.database.trim().is_empty() {
            return Err(PostgresAdapterConfigError::EmptyDatabase);
        }
        if let Some(replication) = &self.replication {
            if replication.slot_name.trim().is_empty()
                || replication.publication_name.trim().is_empty()
            {
                return Err(PostgresAdapterConfigError::EmptyReplicationIdentifier);
            }
        }

        let mut seen_columns = std::collections::BTreeSet::new();
        for (column, mapping) in &self.type_mappings {
            if !seen_columns.insert(column.clone()) {
                return Err(PostgresAdapterConfigError::DuplicateColumnMapping {
                    column: column.clone(),
                });
            }
            validate_mapping(column, mapping)?;
        }
        Ok(())
    }
}

fn validate_mapping(
    column: &str,
    mapping: &PostgresTypeMapping,
) -> Result<(), PostgresAdapterConfigError> {
    match mapping {
        PostgresTypeMapping::Vector { dimensions } if *dimensions == 0 => {
            Err(PostgresAdapterConfigError::ZeroVectorDimensions {
                column: column.to_string(),
            })
        }
        PostgresTypeMapping::Enum {
            type_name,
            variants,
        } => {
            if variants.is_empty() {
                return Err(PostgresAdapterConfigError::EmptyEnumVariants {
                    type_name: type_name.clone(),
                });
            }
            let mut seen = std::collections::BTreeSet::new();
            for variant in variants {
                if !seen.insert(variant) {
                    return Err(PostgresAdapterConfigError::DuplicateEnumVariant {
                        type_name: type_name.clone(),
                        variant: variant.clone(),
                    });
                }
            }
            Ok(())
        }
        PostgresTypeMapping::Array { element } => validate_mapping(column, element),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> PostgresAdapterConfig {
        PostgresAdapterConfig {
            host: "postgres.example.test".to_string(),
            port: 5432,
            database: "gramps".to_string(),
            ssl_mode: PostgresSslMode::VerifyFull,
            replication: Some(PostgresReplicationConfig {
                slot_name: "eg_gramps_slot".to_string(),
                publication_name: "eg_gramps_pub".to_string(),
            }),
            type_mappings: vec![
                (
                    "public.people.attributes".to_string(),
                    PostgresTypeMapping::Jsonb,
                ),
                (
                    "public.people.embedding".to_string(),
                    PostgresTypeMapping::Vector { dimensions: 1536 },
                ),
            ],
        }
    }

    #[test]
    fn accepts_a_well_formed_config() {
        assert_eq!(valid_config().validate(), Ok(()));
    }

    #[test]
    fn refuses_empty_host() {
        let mut config = valid_config();
        config.host = "   ".to_string();
        assert_eq!(
            config.validate(),
            Err(PostgresAdapterConfigError::EmptyHost)
        );
    }

    #[test]
    fn refuses_empty_database() {
        let mut config = valid_config();
        config.database = String::new();
        assert_eq!(
            config.validate(),
            Err(PostgresAdapterConfigError::EmptyDatabase)
        );
    }

    #[test]
    fn refuses_replication_with_blank_slot_name() {
        let mut config = valid_config();
        config.replication = Some(PostgresReplicationConfig {
            slot_name: String::new(),
            publication_name: "eg_gramps_pub".to_string(),
        });
        assert_eq!(
            config.validate(),
            Err(PostgresAdapterConfigError::EmptyReplicationIdentifier)
        );
    }

    #[test]
    fn refuses_zero_dimension_vector_mapping() {
        let mut config = valid_config();
        config.type_mappings.push((
            "public.people.bad_vec".to_string(),
            PostgresTypeMapping::Vector { dimensions: 0 },
        ));
        assert_eq!(
            config.validate(),
            Err(PostgresAdapterConfigError::ZeroVectorDimensions {
                column: "public.people.bad_vec".to_string()
            })
        );
    }

    #[test]
    fn refuses_enum_mapping_with_no_variants() {
        let mut config = valid_config();
        config.type_mappings.push((
            "public.people.status".to_string(),
            PostgresTypeMapping::Enum {
                type_name: "person_status".to_string(),
                variants: vec![],
            },
        ));
        assert_eq!(
            config.validate(),
            Err(PostgresAdapterConfigError::EmptyEnumVariants {
                type_name: "person_status".to_string()
            })
        );
    }

    #[test]
    fn refuses_enum_mapping_with_duplicate_variant() {
        let mut config = valid_config();
        config.type_mappings.push((
            "public.people.status".to_string(),
            PostgresTypeMapping::Enum {
                type_name: "person_status".to_string(),
                variants: vec!["living".to_string(), "living".to_string()],
            },
        ));
        assert_eq!(
            config.validate(),
            Err(PostgresAdapterConfigError::DuplicateEnumVariant {
                type_name: "person_status".to_string(),
                variant: "living".to_string(),
            })
        );
    }

    #[test]
    fn refuses_duplicate_column_mapping() {
        let mut config = valid_config();
        let (column, _) = config.type_mappings[0].clone();
        config
            .type_mappings
            .push((column.clone(), PostgresTypeMapping::Jsonb));
        assert_eq!(
            config.validate(),
            Err(PostgresAdapterConfigError::DuplicateColumnMapping { column })
        );
    }

    #[test]
    fn refuses_zero_dimension_vector_nested_in_array() {
        let mut config = valid_config();
        config.type_mappings.push((
            "public.people.embeddings".to_string(),
            PostgresTypeMapping::Array {
                element: Box::new(PostgresTypeMapping::Vector { dimensions: 0 }),
            },
        ));
        assert_eq!(
            config.validate(),
            Err(PostgresAdapterConfigError::ZeroVectorDimensions {
                column: "public.people.embeddings".to_string()
            })
        );
    }
}
