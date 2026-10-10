//! EG-UNIFIED-DATA-PLANE-R014.2.1 — adapter skeleton for the MySQL/MariaDB
//! attached-source adapter: a typed connection config, its mapping to the
//! sqlx `MySqlConnectOptions`-shaped driver config, and the refusal path
//! before any network attempt. Real binlog/GTID change capture and the
//! per-engine conformance entries are EG-UNIFIED-DATA-PLANE-R014.2.2+.

use crate::attached_source_dialect::SqlEngineKind;
use serde::{Deserialize, Serialize};

/// TLS posture for the MySQL/MariaDB connection, mirroring the modes this
/// adapter actually supports; unlisted modes are rejected by the caller
/// before this type is ever constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MysqlMariadbSslMode {
    Disabled,
    Preferred,
    Required,
    VerifyCa,
    VerifyIdentity,
}

/// Connection-shape configuration for one MySQL or MariaDB attached source.
/// This is the typed model the connect step maps into a driver config; it
/// validates its own shape only and never contacts the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlMariadbConnectionConfig {
    pub engine: SqlEngineKind,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub ssl_mode: MysqlMariadbSslMode,
}

/// The sqlx `MySqlConnectOptions`-shaped driver config this adapter builds
/// from a validated [`MysqlMariadbConnectionConfig`]. Kept as our own type
/// (not `sqlx::mysql::MySqlConnectOptions` itself) so this crate — the
/// bottom of the crate DAG — stays dependency-free; the crate that actually
/// links `sqlx` (already present, gated behind the `federation-sql`
/// feature) converts this into the real driver options at connect time
/// (EG-UNIFIED-DATA-PLANE-R014.2.2+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MysqlMariadbDriverConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub ssl_mode: MysqlMariadbSslMode,
}

/// Why a [`MysqlMariadbConnectionConfig`] was refused before any connect
/// attempt was made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MysqlMariadbConnectRefusal {
    EmptyHost,
    EmptyDatabase,
    EmptyUsername,
    ZeroPort,
}

impl MysqlMariadbConnectionConfig {
    /// Validates this config and, if well-formed, maps it to the driver
    /// config the later connect slice hands to sqlx. Refuses rather than
    /// defaulting a missing field. Performs NO I/O: a refusal here is a
    /// config-shape error, never a network failure — this is the
    /// connect/refusal path for a connection that needs no live database to
    /// exercise.
    pub fn to_driver_config(&self) -> Result<MysqlMariadbDriverConfig, MysqlMariadbConnectRefusal> {
        if self.host.trim().is_empty() {
            return Err(MysqlMariadbConnectRefusal::EmptyHost);
        }
        if self.database.trim().is_empty() {
            return Err(MysqlMariadbConnectRefusal::EmptyDatabase);
        }
        if self.username.trim().is_empty() {
            return Err(MysqlMariadbConnectRefusal::EmptyUsername);
        }
        if self.port == 0 {
            return Err(MysqlMariadbConnectRefusal::ZeroPort);
        }
        Ok(MysqlMariadbDriverConfig {
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            username: self.username.clone(),
            ssl_mode: self.ssl_mode,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid(engine: SqlEngineKind) -> MysqlMariadbConnectionConfig {
        MysqlMariadbConnectionConfig {
            engine,
            host: "db.example.test".to_string(),
            port: 3306,
            database: "eg_attached".to_string(),
            username: "eg_reader".to_string(),
            ssl_mode: MysqlMariadbSslMode::VerifyIdentity,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R014.2.1
    #[test]
    fn mysql_and_mariadb_both_map_to_a_driver_config() {
        for engine in [SqlEngineKind::MySql, SqlEngineKind::MariaDb] {
            let config = valid(engine);
            let driver = config.to_driver_config().expect("well-formed config maps");
            assert_eq!(driver.host, "db.example.test");
            assert_eq!(driver.port, 3306);
            assert_eq!(driver.database, "eg_attached");
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R014.2.1
    #[test]
    fn refuses_empty_host_before_any_connect_attempt() {
        let mut config = valid(SqlEngineKind::MySql);
        config.host = "   ".to_string();
        assert_eq!(
            config.to_driver_config(),
            Err(MysqlMariadbConnectRefusal::EmptyHost)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R014.2.1
    #[test]
    fn refuses_empty_database() {
        let mut config = valid(SqlEngineKind::MariaDb);
        config.database = String::new();
        assert_eq!(
            config.to_driver_config(),
            Err(MysqlMariadbConnectRefusal::EmptyDatabase)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R014.2.1
    #[test]
    fn refuses_empty_username() {
        let mut config = valid(SqlEngineKind::MySql);
        config.username = String::new();
        assert_eq!(
            config.to_driver_config(),
            Err(MysqlMariadbConnectRefusal::EmptyUsername)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R014.2.1
    #[test]
    fn refuses_zero_port() {
        let mut config = valid(SqlEngineKind::MariaDb);
        config.port = 0;
        assert_eq!(
            config.to_driver_config(),
            Err(MysqlMariadbConnectRefusal::ZeroPort)
        );
    }
}
