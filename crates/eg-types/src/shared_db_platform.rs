//! Typed model for the shared CloudNativePG/MariaDB attached-application
//! platform (EG-UNIFIED-DATA-PLANE-R023): how clusters group by engine,
//! major version and required extensions, and each application's dedicated
//! role set. This is the typed-model slice (`.1`): the cluster-group and
//! role-set shape, and the refusal for a Postgres group that does not
//! declare logical replication or a role set missing a required role. The
//! CloudNativePG/MariaDB operator wiring, the PITR schedule, and the
//! restore-drill automation are later children.

use serde::{Deserialize, Serialize};

/// The engine a shared cluster group runs. Closed — EG groups applications
/// onto exactly these two engines, never a third inferred from config.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SharedPlatformEngine {
    CloudNativePg,
    MariaDb,
}

/// One application's dedicated role set on a shared cluster group: its own
/// database/role, plus EG's separate read and replication roles. EG never
/// reuses the application's own role for its own access.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationRoleSet {
    pub application: String,
    pub database: String,
    pub application_role: String,
    pub eg_read_role: String,
    pub eg_replication_role: String,
}

/// A shared cluster group: applications grouped by engine, major version,
/// and required extensions, each with its own role set. `wal_level` applies
/// only to a `CloudNativePg` group (MariaDB has no such setting).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedPlatformClusterGroup {
    pub engine: SharedPlatformEngine,
    pub major_version: u16,
    #[serde(default)]
    pub required_extensions: Vec<String>,
    /// Postgres `wal_level`. Required present and `"logical"` for a
    /// `CloudNativePg` group (point-in-time recovery plus EG's replication
    /// role both depend on it); absent for `MariaDb`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal_level: Option<String>,
    pub applications: Vec<ApplicationRoleSet>,
}

/// A cluster group failed a platform invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidClusterGroup {
    /// A `CloudNativePg` group did not declare `wal_level = "logical"`.
    WalLevelNotLogical,
    /// A `MariaDb` group declared a `wal_level` (a Postgres-only setting).
    WalLevelOnMariaDb,
    /// An application's role set reused the same role name for two of its
    /// three distinct roles (application/read/replication must never alias).
    AliasedRole(String),
    /// No applications are grouped onto this cluster (an empty group serves
    /// nothing and should not exist).
    NoApplications,
}

impl std::fmt::Display for InvalidClusterGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::WalLevelNotLogical => {
                write!(f, "CloudNativePG group must declare wal_level=logical")
            }
            Self::WalLevelOnMariaDb => write!(f, "MariaDB group must not declare wal_level"),
            Self::AliasedRole(app) => {
                write!(f, "application {app:?} aliases two of its three roles")
            }
            Self::NoApplications => write!(f, "cluster group has no applications"),
        }
    }
}

impl std::error::Error for InvalidClusterGroup {}

impl SharedPlatformClusterGroup {
    /// Confirm the engine-specific `wal_level` rule, that no application
    /// aliases two of its three roles, and that the group is non-empty.
    /// Refuses rather than silently defaulting a missing `wal_level`.
    pub fn validate(&self) -> Result<(), InvalidClusterGroup> {
        match self.engine {
            SharedPlatformEngine::CloudNativePg => {
                if self.wal_level.as_deref() != Some("logical") {
                    return Err(InvalidClusterGroup::WalLevelNotLogical);
                }
            }
            SharedPlatformEngine::MariaDb => {
                if self.wal_level.is_some() {
                    return Err(InvalidClusterGroup::WalLevelOnMariaDb);
                }
            }
        }
        if self.applications.is_empty() {
            return Err(InvalidClusterGroup::NoApplications);
        }
        for app in &self.applications {
            if app.application_role == app.eg_read_role
                || app.application_role == app.eg_replication_role
                || app.eg_read_role == app.eg_replication_role
            {
                return Err(InvalidClusterGroup::AliasedRole(app.application.clone()));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roles(app: &str) -> ApplicationRoleSet {
        ApplicationRoleSet {
            application: app.to_string(),
            database: format!("{app}_db"),
            application_role: format!("{app}_app"),
            eg_read_role: format!("{app}_eg_read"),
            eg_replication_role: format!("{app}_eg_repl"),
        }
    }

    fn pg_group() -> SharedPlatformClusterGroup {
        SharedPlatformClusterGroup {
            engine: SharedPlatformEngine::CloudNativePg,
            major_version: 16,
            required_extensions: vec!["pg_trgm".to_string()],
            wal_level: Some("logical".to_string()),
            applications: vec![roles("gramps")],
        }
    }

    #[test]
    fn well_formed_postgres_group_validates() {
        assert_eq!(pg_group().validate(), Ok(()));
    }

    #[test]
    fn postgres_group_without_logical_wal_level_is_refused() {
        let mut group = pg_group();
        group.wal_level = Some("replica".to_string());
        assert_eq!(
            group.validate(),
            Err(InvalidClusterGroup::WalLevelNotLogical)
        );
    }

    #[test]
    fn postgres_group_with_absent_wal_level_is_refused_not_defaulted() {
        let mut group = pg_group();
        group.wal_level = None;
        assert_eq!(
            group.validate(),
            Err(InvalidClusterGroup::WalLevelNotLogical)
        );
    }

    #[test]
    fn mariadb_group_with_wal_level_is_refused() {
        let group = SharedPlatformClusterGroup {
            engine: SharedPlatformEngine::MariaDb,
            major_version: 11,
            required_extensions: vec![],
            wal_level: Some("logical".to_string()),
            applications: vec![roles("twenty")],
        };
        assert_eq!(
            group.validate(),
            Err(InvalidClusterGroup::WalLevelOnMariaDb)
        );
    }

    #[test]
    fn mariadb_group_without_wal_level_validates() {
        let group = SharedPlatformClusterGroup {
            engine: SharedPlatformEngine::MariaDb,
            major_version: 11,
            required_extensions: vec![],
            wal_level: None,
            applications: vec![roles("twenty")],
        };
        assert_eq!(group.validate(), Ok(()));
    }

    #[test]
    fn aliased_role_is_refused() {
        let mut group = pg_group();
        group.applications[0].eg_read_role = group.applications[0].application_role.clone();
        assert!(matches!(
            group.validate(),
            Err(InvalidClusterGroup::AliasedRole(_))
        ));
    }

    #[test]
    fn empty_group_is_refused() {
        let mut group = pg_group();
        group.applications.clear();
        assert_eq!(group.validate(), Err(InvalidClusterGroup::NoApplications));
    }
}
