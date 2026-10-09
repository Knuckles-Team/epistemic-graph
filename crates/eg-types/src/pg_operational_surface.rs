//! PostgreSQL wire protocol operational parity (EG-DURABLE-KERNEL-R035): the
//! closed set of required operational surfaces (pg_dump/pg_restore,
//! replication, pg_stat views, pg_locks, EXPLAIN, query cancellation, pooler
//! transaction mode) and a fail-closed compatibility matrix. This is the
//! typed model slice (`.1`): no surface is ever assumed supported. The real
//! pg_dump/pg_restore/replication/pg_stat/EXPLAIN/pooler implementations are
//! later children.

use std::collections::BTreeSet;
use std::fmt;

use serde::{Deserialize, Serialize};

/// A required PostgreSQL wire-protocol operational surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PgOperationalSurface {
    PgDump,
    PgRestore,
    Replication,
    PgStatViews,
    PgLocks,
    Explain,
    QueryCancellation,
    PoolerTransactionMode,
}

impl PgOperationalSurface {
    pub const ALL: [PgOperationalSurface; 8] = [
        PgOperationalSurface::PgDump,
        PgOperationalSurface::PgRestore,
        PgOperationalSurface::Replication,
        PgOperationalSurface::PgStatViews,
        PgOperationalSurface::PgLocks,
        PgOperationalSurface::Explain,
        PgOperationalSurface::QueryCancellation,
        PgOperationalSurface::PoolerTransactionMode,
    ];
}

/// A parity check named one or more surfaces that are not declared
/// supported. Carries every missing surface, not just the first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsupportedSurfaces(pub Vec<PgOperationalSurface>);

impl fmt::Display for UnsupportedSurfaces {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unsupported PostgreSQL operational surfaces: {:?}", self.0)
    }
}

impl std::error::Error for UnsupportedSurfaces {}

/// Tracks which operational surfaces this engine declares supported. Starts
/// fail-closed: [`PgCompatibilityMatrix::fail_closed`] is the only
/// constructor, and there is no bulk "declare all supported" method, so
/// claiming parity requires naming each surface explicitly.
#[derive(Clone, Debug, Default)]
pub struct PgCompatibilityMatrix {
    supported: BTreeSet<PgOperationalSurface>,
}

impl PgCompatibilityMatrix {
    pub fn fail_closed() -> Self {
        Self {
            supported: BTreeSet::new(),
        }
    }

    pub fn declare_supported(&mut self, surface: PgOperationalSurface) {
        self.supported.insert(surface);
    }

    pub fn is_supported(&self, surface: PgOperationalSurface) -> bool {
        self.supported.contains(&surface)
    }

    /// Refuses (naming every missing surface) unless every surface in
    /// `required` is declared supported.
    pub fn require_all(
        &self,
        required: &[PgOperationalSurface],
    ) -> Result<(), UnsupportedSurfaces> {
        let missing: Vec<PgOperationalSurface> = required
            .iter()
            .copied()
            .filter(|s| !self.is_supported(*s))
            .collect();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(UnsupportedSurfaces(missing))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surfaces_round_trip_through_their_wire_name() {
        for (surface, name) in [
            (PgOperationalSurface::PgDump, "pg_dump"),
            (PgOperationalSurface::PgRestore, "pg_restore"),
            (PgOperationalSurface::Replication, "replication"),
            (PgOperationalSurface::PgStatViews, "pg_stat_views"),
            (PgOperationalSurface::PgLocks, "pg_locks"),
            (PgOperationalSurface::Explain, "explain"),
            (PgOperationalSurface::QueryCancellation, "query_cancellation"),
            (
                PgOperationalSurface::PoolerTransactionMode,
                "pooler_transaction_mode",
            ),
        ] {
            let wire = serde_json::to_string(&surface).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            let back: PgOperationalSurface = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, surface);
        }
    }

    #[test]
    fn fresh_matrix_reports_every_surface_unsupported() {
        let matrix = PgCompatibilityMatrix::fail_closed();
        for surface in PgOperationalSurface::ALL {
            assert!(!matrix.is_supported(surface));
        }
    }

    #[test]
    fn parity_check_is_refused_and_names_exactly_the_missing_surfaces() {
        let mut matrix = PgCompatibilityMatrix::fail_closed();
        matrix.declare_supported(PgOperationalSurface::PgDump);
        matrix.declare_supported(PgOperationalSurface::Explain);
        let err = matrix.require_all(&PgOperationalSurface::ALL).unwrap_err();
        assert_eq!(err.0.len(), PgOperationalSurface::ALL.len() - 2);
        assert!(!err.0.contains(&PgOperationalSurface::PgDump));
        assert!(!err.0.contains(&PgOperationalSurface::Explain));
        assert!(err.0.contains(&PgOperationalSurface::PgRestore));
    }

    #[test]
    fn parity_check_succeeds_once_every_surface_is_declared() {
        let mut matrix = PgCompatibilityMatrix::fail_closed();
        for surface in PgOperationalSurface::ALL {
            matrix.declare_supported(surface);
        }
        assert!(matrix.require_all(&PgOperationalSurface::ALL).is_ok());
    }
}
