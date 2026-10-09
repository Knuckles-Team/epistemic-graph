//! Typed model for the MySQL and MariaDB attached-source dialect entries
//! (EG-UNIFIED-DATA-PLANE-R014). MySQL and MariaDB are forked engines with
//! diverging binlog GTID formats, `information_schema` extensions and type
//! behavior past their common ancestor, so this keeps them as two separate,
//! explicitly declared dialect entries rather than one collapsed "mysql"
//! dialect — per spec.md: "No capability is inferred solely from a dialect
//! name."
//!
//! This is the R014.1 slice: the typed model plus its validation and
//! refusal tests only. The driver, SQL rendering, catalog reader and binlog
//! capture implementations are later slices, landed once the shared
//! attached-source registry (EG-UNIFIED-DATA-PLANE-R002) exists to host
//! them.

use serde::{Deserialize, Serialize};

/// The two forked MySQL-family engines this adapter distinguishes. Kept as
/// separate variants (never a shared "MySQL-family" catch-all) so each gets
/// its own conformance entry, per EG-UNIFIED-DATA-PLANE-R014.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SqlEngineKind {
    MySql,
    MariaDb,
}

/// Change-capture support declared explicitly for one engine kind. Row-based
/// binlog and GTID mode availability differ between MySQL and MariaDB, so
/// this is never derived from [`SqlEngineKind`] by pattern match alone —
/// each entry is constructed with its own explicit, reviewed declaration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlEngineCaptureSupport {
    pub row_based_binlog: bool,
    pub gtid_mode: bool,
}

/// One dialect-adapter entry for a MySQL-family engine: the engine kind, an
/// engine-version floor the entry was validated against, declared capture
/// support, and the type-map revision in effect. This is the typed-model
/// half of the five-part adapter contract (driver, rendering, catalog,
/// capture, type map) from spec.md DP-09; the other four parts land in
/// later R014.n slices.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlEngineDialectEntry {
    pub kind: SqlEngineKind,
    pub min_version: String,
    pub capture: SqlEngineCaptureSupport,
    pub type_map_version: u32,
}

/// Returned when a dialect entry is constructed without an explicit,
/// non-empty version floor or a reviewed type-map revision — refusing to
/// synthesize a default from the engine kind alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DialectDeclarationRefused {
    pub reason: &'static str,
}

impl SqlEngineDialectEntry {
    /// Construct a dialect entry from an EXPLICIT declaration. Fails closed
    /// rather than defaulting when the caller has not stated a version
    /// floor or a type-map revision: EG never infers an attached-source
    /// capability solely from the dialect name (spec.md, `unified-data-plane`).
    pub fn declare(
        kind: SqlEngineKind,
        min_version: impl Into<String>,
        capture: SqlEngineCaptureSupport,
        type_map_version: u32,
    ) -> Result<Self, DialectDeclarationRefused> {
        let min_version = min_version.into();
        if min_version.trim().is_empty() {
            return Err(DialectDeclarationRefused {
                reason: "min_version must be an explicit, non-empty version floor",
            });
        }
        if type_map_version == 0 {
            return Err(DialectDeclarationRefused {
                reason: "type_map_version must be an explicit, reviewed revision (nonzero)",
            });
        }
        Ok(Self {
            kind,
            min_version,
            capture,
            type_map_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-UNIFIED-DATA-PLANE-R014.1
    #[test]
    fn mysql_and_mariadb_are_distinct_entries() {
        let mysql = SqlEngineDialectEntry::declare(
            SqlEngineKind::MySql,
            "8.0",
            SqlEngineCaptureSupport {
                row_based_binlog: true,
                gtid_mode: true,
            },
            1,
        )
        .expect("mysql declaration with explicit version and type map must succeed");
        let mariadb = SqlEngineDialectEntry::declare(
            SqlEngineKind::MariaDb,
            "10.6",
            SqlEngineCaptureSupport {
                row_based_binlog: true,
                gtid_mode: true,
            },
            1,
        )
        .expect("mariadb declaration with explicit version and type map must succeed");

        assert_ne!(mysql.kind, mariadb.kind);
        assert_ne!(
            mysql, mariadb,
            "MySQL and MariaDB must stay separate conformance entries, never merged"
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R014.1
    #[test]
    fn refuses_missing_version_floor() {
        let err = SqlEngineDialectEntry::declare(
            SqlEngineKind::MariaDb,
            "",
            SqlEngineCaptureSupport {
                row_based_binlog: true,
                gtid_mode: true,
            },
            1,
        )
        .expect_err("an empty version floor must be refused, not defaulted");
        assert_eq!(
            err.reason,
            "min_version must be an explicit, non-empty version floor"
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R014.1
    #[test]
    fn refuses_unreviewed_type_map_version() {
        let err = SqlEngineDialectEntry::declare(
            SqlEngineKind::MySql,
            "8.0",
            SqlEngineCaptureSupport {
                row_based_binlog: true,
                gtid_mode: false,
            },
            0,
        )
        .expect_err("a zero type_map_version must be refused as unreviewed");
        assert_eq!(
            err.reason,
            "type_map_version must be an explicit, reviewed revision (nonzero)"
        );
    }
}
