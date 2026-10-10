//! Typed source-kind model for federating read-only warehouse/lake sources
//! (EG-UNIFIED-DATA-PLANE-R020): Snowflake, BigQuery, DuckDB, and Iceberg.
//! This is the typed-model slice (`.1`): the closed source-kind vocabulary,
//! its per-kind required-field contract, and the refusal for a config
//! missing a field its declared kind requires. The query pushdown (Arrow
//! Flight SQL where the source offers it, extending
//! `eg-query::sql::iceberg_federation` for Iceberg) and the conformance
//! entry against each native engine are later children.

use serde::{Deserialize, Serialize};

/// A read-only federated warehouse/lake source kind. Closed — an unlisted
/// kind is refused at parse time rather than admitted as a catch-all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarehouseSourceKind {
    Snowflake,
    BigQuery,
    DuckDb,
    Iceberg,
}

impl WarehouseSourceKind {
    /// Whether this kind offers Arrow Flight SQL, in which case the query
    /// path prefers it over the kind's native wire protocol.
    pub const fn offers_flight_sql(self) -> bool {
        matches!(self, Self::Snowflake | Self::BigQuery)
    }

    /// The connection-config field names this kind requires. Used to refuse
    /// an incomplete config before any connection attempt.
    pub const fn required_fields(self) -> &'static [&'static str] {
        match self {
            Self::Snowflake => &["account", "database", "warehouse"],
            Self::BigQuery => &["project", "dataset"],
            Self::DuckDb => &["database_path"],
            Self::Iceberg => &["catalog_uri"],
        }
    }
}

/// A federated warehouse source's connection configuration: its kind plus
/// the opaque field map a connector reads by name. Pure data — no I/O.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WarehouseSourceConfig {
    pub kind: Option<WarehouseSourceKind>,
    pub fields: std::collections::BTreeMap<String, String>,
}

/// A config was missing one or more fields its declared kind requires.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MissingWarehouseFields {
    pub kind: WarehouseSourceKind,
    pub missing: Vec<String>,
}

impl std::fmt::Display for MissingWarehouseFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?} config missing required field(s): {}",
            self.kind,
            self.missing.join(", ")
        )
    }
}

impl std::error::Error for MissingWarehouseFields {}

impl WarehouseSourceConfig {
    /// Confirm every field this config's declared kind requires is present
    /// and non-empty. Refuses rather than deferring the gap to connect time.
    /// A config with no declared kind is refused outright: a kind can never
    /// be inferred from its fields.
    pub fn validate(&self) -> Result<WarehouseSourceKind, MissingWarehouseFields> {
        let kind = match self.kind {
            Some(kind) => kind,
            None => {
                return Err(MissingWarehouseFields {
                    kind: WarehouseSourceKind::Iceberg,
                    missing: vec!["kind".to_string()],
                });
            }
        };
        let missing: Vec<String> = kind
            .required_fields()
            .iter()
            .filter(|name| {
                self.fields
                    .get(**name)
                    .map(|value| value.is_empty())
                    .unwrap_or(true)
            })
            .map(|name| name.to_string())
            .collect();
        if missing.is_empty() {
            Ok(kind)
        } else {
            Err(MissingWarehouseFields { kind, missing })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(kind: WarehouseSourceKind, pairs: &[(&str, &str)]) -> WarehouseSourceConfig {
        WarehouseSourceConfig {
            kind: Some(kind),
            fields: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.1
    #[test]
    fn complete_snowflake_config_validates() {
        let cfg = config(
            WarehouseSourceKind::Snowflake,
            &[("account", "acme"), ("database", "db"), ("warehouse", "wh")],
        );
        assert_eq!(cfg.validate(), Ok(WarehouseSourceKind::Snowflake));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.1
    #[test]
    fn incomplete_config_is_refused_with_named_fields() {
        let cfg = config(WarehouseSourceKind::BigQuery, &[("project", "p")]);
        let err = cfg.validate().unwrap_err();
        assert_eq!(err.missing, vec!["dataset".to_string()]);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.1
    #[test]
    fn empty_field_value_counts_as_missing() {
        let cfg = config(WarehouseSourceKind::DuckDb, &[("database_path", "")]);
        assert!(cfg.validate().is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.1
    #[test]
    fn absent_kind_is_refused_not_guessed() {
        let cfg = WarehouseSourceConfig::default();
        assert!(cfg.validate().is_err());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.1
    #[test]
    fn flight_sql_is_offered_only_by_the_declared_kinds() {
        assert!(WarehouseSourceKind::Snowflake.offers_flight_sql());
        assert!(WarehouseSourceKind::BigQuery.offers_flight_sql());
        assert!(!WarehouseSourceKind::DuckDb.offers_flight_sql());
        assert!(!WarehouseSourceKind::Iceberg.offers_flight_sql());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.1
    #[test]
    fn config_serializes_round_trip() {
        let cfg = config(WarehouseSourceKind::Iceberg, &[("catalog_uri", "http://x")]);
        let encoded = serde_json::to_string(&cfg).unwrap();
        let decoded: WarehouseSourceConfig = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, cfg);
    }
}
