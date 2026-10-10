//! Typed model for the shared CloudNativePG/MariaDB platform's point-in-time
//! recovery schedule to object storage (EG-UNIFIED-DATA-PLANE-R023.2): the
//! schedule contract and its refusal path. This is the first slice (`.1`):
//! `PitrSchedule`/`validate` refuse a schedule whose continuous-archiving
//! mode does not match its cluster's engine (see `SharedPlatformEngine`,
//! EG-UNIFIED-DATA-PLANE-R023.1), a zero retention window, or a blank cron
//! or destination. No CloudNativePG/MariaDB operator is wired yet, and no
//! schedule is actually installed against object storage; wiring the real
//! operator CRDs to this schedule is a later child.

use serde::{Deserialize, Serialize};

use crate::shared_db_platform::SharedPlatformEngine;

/// How continuously changes are archived to object storage between full
/// base backups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuousArchivingMode {
    /// Postgres logical WAL shipping (requires `wal_level = logical`, see
    /// `SharedPlatformClusterGroup::validate`).
    LogicalWal,
    /// MariaDB binlog shipping.
    Binlog,
}

/// A cluster group's PITR schedule: how often a full base backup runs, how
/// changes are continuously archived between backups, how long backups are
/// retained, and the object-storage destination they land on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PitrSchedule {
    pub engine: SharedPlatformEngine,
    pub archiving_mode: ContinuousArchivingMode,
    /// A cron expression for the full base backup cadence.
    pub base_backup_cron: String,
    pub retention_days: u32,
    /// Object-storage URI (e.g. `s3://bucket/prefix`) backups land on.
    pub destination_uri: String,
}

/// A PITR schedule failed a platform invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidPitrSchedule {
    /// The archiving mode does not match the cluster group's engine.
    WrongArchivingModeForEngine,
    /// `retention_days` was zero -- a schedule that retains nothing
    /// recovers nothing.
    ZeroRetention,
    /// `base_backup_cron` was blank.
    BlankCron,
    /// `destination_uri` was blank.
    BlankDestination,
}

impl std::fmt::Display for InvalidPitrSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::WrongArchivingModeForEngine => {
                "archiving mode does not match the cluster group's engine"
            }
            Self::ZeroRetention => "retention_days must be greater than zero",
            Self::BlankCron => "base_backup_cron must not be blank",
            Self::BlankDestination => "destination_uri must not be blank",
        };
        write!(f, "{message}")
    }
}

impl std::error::Error for InvalidPitrSchedule {}

impl PitrSchedule {
    /// Refuse a schedule whose archiving mode does not match its engine, a
    /// zero retention window, or a blank cron/destination -- rather than
    /// installing an unrecoverable schedule.
    pub fn validate(&self) -> Result<(), InvalidPitrSchedule> {
        match (self.engine, self.archiving_mode) {
            (SharedPlatformEngine::CloudNativePg, ContinuousArchivingMode::Binlog)
            | (SharedPlatformEngine::MariaDb, ContinuousArchivingMode::LogicalWal) => {
                return Err(InvalidPitrSchedule::WrongArchivingModeForEngine);
            }
            _ => {}
        }
        if self.retention_days == 0 {
            return Err(InvalidPitrSchedule::ZeroRetention);
        }
        if self.base_backup_cron.trim().is_empty() {
            return Err(InvalidPitrSchedule::BlankCron);
        }
        if self.destination_uri.trim().is_empty() {
            return Err(InvalidPitrSchedule::BlankDestination);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg_schedule() -> PitrSchedule {
        PitrSchedule {
            engine: SharedPlatformEngine::CloudNativePg,
            archiving_mode: ContinuousArchivingMode::LogicalWal,
            base_backup_cron: "0 2 * * *".to_string(),
            retention_days: 30,
            destination_uri: "s3://eg-backups/gramps".to_string(),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.2.1
    #[test]
    fn well_formed_postgres_schedule_validates() {
        assert_eq!(pg_schedule().validate(), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.2.1
    #[test]
    fn postgres_schedule_with_binlog_archiving_is_refused() {
        let mut schedule = pg_schedule();
        schedule.archiving_mode = ContinuousArchivingMode::Binlog;
        assert_eq!(
            schedule.validate(),
            Err(InvalidPitrSchedule::WrongArchivingModeForEngine)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.2.1
    #[test]
    fn mariadb_schedule_with_logical_wal_is_refused() {
        let mut schedule = pg_schedule();
        schedule.engine = SharedPlatformEngine::MariaDb;
        schedule.archiving_mode = ContinuousArchivingMode::LogicalWal;
        assert_eq!(
            schedule.validate(),
            Err(InvalidPitrSchedule::WrongArchivingModeForEngine)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.2.1
    #[test]
    fn mariadb_schedule_with_binlog_validates() {
        let mut schedule = pg_schedule();
        schedule.engine = SharedPlatformEngine::MariaDb;
        schedule.archiving_mode = ContinuousArchivingMode::Binlog;
        assert_eq!(schedule.validate(), Ok(()));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.2.1
    #[test]
    fn zero_retention_is_refused() {
        let mut schedule = pg_schedule();
        schedule.retention_days = 0;
        assert_eq!(schedule.validate(), Err(InvalidPitrSchedule::ZeroRetention));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.2.1
    #[test]
    fn blank_cron_is_refused() {
        let mut schedule = pg_schedule();
        schedule.base_backup_cron = "  ".to_string();
        assert_eq!(schedule.validate(), Err(InvalidPitrSchedule::BlankCron));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R023.2.1
    #[test]
    fn blank_destination_is_refused() {
        let mut schedule = pg_schedule();
        schedule.destination_uri = "  ".to_string();
        assert_eq!(
            schedule.validate(),
            Err(InvalidPitrSchedule::BlankDestination)
        );
    }
}
