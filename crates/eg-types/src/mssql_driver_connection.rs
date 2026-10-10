//! EG-UNIFIED-DATA-PLANE-R016.2.1 — adapter skeleton for the Microsoft SQL
//! Server attached-source adapter: a typed connection config, its mapping to
//! the tiberius `Config`-shaped driver config, and the refusal path before
//! any network attempt. T-SQL TOP/OFFSET-FETCH rendering and the
//! LSN-polled CDC/Change Tracking capture loop are
//! EG-UNIFIED-DATA-PLANE-R016.2.2+.

use crate::mssql_attached_catalog::MssqlCaptureMode;
use serde::{Deserialize, Serialize};

/// Transport encryption posture for the tiberius connection, mirroring the
/// modes this adapter actually supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MssqlEncryptionMode {
    Off,
    Required,
    /// Full certificate validation — never combined with trusting an
    /// unverified server certificate.
    Strict,
}

/// Connection-shape configuration for one attached SQL Server source. This
/// is the typed model the connect step maps into a driver config; it
/// validates its own shape only and never contacts the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MssqlConnectionConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub encryption: MssqlEncryptionMode,
    pub trust_server_certificate: bool,
    pub capture_mode: MssqlCaptureMode,
}

/// The tiberius `Config`-shaped driver config this adapter builds from a
/// validated [`MssqlConnectionConfig`]. Kept as our own type (not
/// `tiberius::Config` itself) so this crate — the bottom of the crate DAG —
/// stays dependency-free; the crate that links `tiberius` behind its own
/// feature gate converts this into the real driver config at connect time
/// (EG-UNIFIED-DATA-PLANE-R016.2.2+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MssqlDriverConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub encryption: MssqlEncryptionMode,
    pub trust_server_certificate: bool,
}

/// Why a [`MssqlConnectionConfig`] was refused before any connect attempt
/// was made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MssqlConnectRefusal {
    EmptyHost,
    EmptyDatabase,
    EmptyUsername,
    ZeroPort,
    /// `Strict` encryption asserts full certificate validation, which
    /// trusting an unverified server certificate directly contradicts.
    TrustServerCertificateWithStrictEncryption,
}

impl MssqlConnectionConfig {
    /// Validates this config and, if well-formed, maps it to the driver
    /// config the later connect slice hands to tiberius. Performs NO I/O: a
    /// refusal here is a config-shape error, never a network failure.
    pub fn to_driver_config(&self) -> Result<MssqlDriverConfig, MssqlConnectRefusal> {
        if self.host.trim().is_empty() {
            return Err(MssqlConnectRefusal::EmptyHost);
        }
        if self.database.trim().is_empty() {
            return Err(MssqlConnectRefusal::EmptyDatabase);
        }
        if self.username.trim().is_empty() {
            return Err(MssqlConnectRefusal::EmptyUsername);
        }
        if self.port == 0 {
            return Err(MssqlConnectRefusal::ZeroPort);
        }
        if self.trust_server_certificate && self.encryption == MssqlEncryptionMode::Strict {
            return Err(MssqlConnectRefusal::TrustServerCertificateWithStrictEncryption);
        }
        Ok(MssqlDriverConfig {
            host: self.host.clone(),
            port: self.port,
            database: self.database.clone(),
            username: self.username.clone(),
            encryption: self.encryption,
            trust_server_certificate: self.trust_server_certificate,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid(capture_mode: MssqlCaptureMode) -> MssqlConnectionConfig {
        MssqlConnectionConfig {
            host: "mssql.example.test".to_string(),
            port: 1433,
            database: "eg_attached".to_string(),
            username: "eg_reader".to_string(),
            encryption: MssqlEncryptionMode::Required,
            trust_server_certificate: false,
            capture_mode,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.2.1
    #[test]
    fn cdc_and_change_tracking_both_map_to_a_driver_config() {
        for capture_mode in [MssqlCaptureMode::Cdc, MssqlCaptureMode::ChangeTracking] {
            let config = valid(capture_mode);
            let driver = config.to_driver_config().expect("well-formed config maps");
            assert_eq!(driver.host, "mssql.example.test");
            assert_eq!(driver.port, 1433);
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.2.1
    #[test]
    fn refuses_empty_host_before_any_connect_attempt() {
        let mut config = valid(MssqlCaptureMode::Unsupported);
        config.host = "   ".to_string();
        assert_eq!(
            config.to_driver_config(),
            Err(MssqlConnectRefusal::EmptyHost)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.2.1
    #[test]
    fn refuses_empty_database() {
        let mut config = valid(MssqlCaptureMode::Cdc);
        config.database = String::new();
        assert_eq!(
            config.to_driver_config(),
            Err(MssqlConnectRefusal::EmptyDatabase)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.2.1
    #[test]
    fn refuses_zero_port() {
        let mut config = valid(MssqlCaptureMode::ChangeTracking);
        config.port = 0;
        assert_eq!(
            config.to_driver_config(),
            Err(MssqlConnectRefusal::ZeroPort)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R016.2.1
    #[test]
    fn refuses_trusting_server_certificate_under_strict_encryption() {
        let mut config = valid(MssqlCaptureMode::Unsupported);
        config.encryption = MssqlEncryptionMode::Strict;
        config.trust_server_certificate = true;
        assert_eq!(
            config.to_driver_config(),
            Err(MssqlConnectRefusal::TrustServerCertificateWithStrictEncryption)
        );
    }
}
