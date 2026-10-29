//! Oracle/Db2 attached-source driver config + connect/refusal path
//! (CONCEPT:MEDULLA-UNIFIED-DATA-PLANE-R018.2.1).
//!
//! This slice covers ONLY the typed driver config and the refusal gate: it proves
//! a [`RelationalDriverConfig`] can be built from a validated
//! [`RelationalEngine`] plus host/port/service name, and that [`connect`] refuses
//! an unsafe or invalid destination before issuing any driver/ODBC request. It
//! requires NO live Oracle/Db2 server. Reading the live catalog through the
//! driver/ODBC and wiring change capture through the Debezium bridge is the live
//! slice `MEDULLA-UNIFIED-DATA-PLANE-R018.2.2`, mirroring the loopback-only refusal
//! gate already used by `connect_remote_engine` in `crate::federation`.

use std::net::ToSocketAddrs;

/// The relational engine a driver config targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationalEngine {
    Oracle,
    Db2,
}

/// A validated Oracle/Db2 connection target. Construct only through
/// [`RelationalDriverConfig::build`], which refuses an empty host, a zero port,
/// or an empty service name before a config can exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalDriverConfig {
    pub engine: RelationalEngine,
    pub host: String,
    pub port: u16,
    pub service_name: String,
}

/// A connected (resolved, refusal-gate-passed) Oracle/Db2 target handle. The
/// live slice (`R018.2.2`) is responsible for actually reading the catalog over
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalConnection {
    pub engine: RelationalEngine,
    pub host: String,
    pub port: u16,
}

/// Why a [`RelationalDriverConfig`] could not be built, or [`connect`] refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationalConnectError {
    EmptyHost,
    InvalidPort,
    EmptyServiceName,
    UnresolvableDestination,
    DisallowedDestination,
}

impl std::fmt::Display for RelationalConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyHost => "relational: host must not be empty",
            Self::InvalidPort => "relational: port must not be zero",
            Self::EmptyServiceName => "relational: service name must not be empty",
            Self::UnresolvableDestination => "relational: unable to resolve destination",
            Self::DisallowedDestination => {
                "relational: destination is not allowed (not a local verified endpoint)"
            }
        };
        f.write_str(message)
    }
}

impl std::error::Error for RelationalConnectError {}

impl RelationalDriverConfig {
    /// Build a validated driver config for the engine, refusing an empty host,
    /// a zero port, or an empty service name.
    pub fn build(
        engine: RelationalEngine,
        host: &str,
        port: u16,
        service_name: &str,
    ) -> Result<Self, RelationalConnectError> {
        if host.trim().is_empty() {
            return Err(RelationalConnectError::EmptyHost);
        }
        if port == 0 {
            return Err(RelationalConnectError::InvalidPort);
        }
        if service_name.trim().is_empty() {
            return Err(RelationalConnectError::EmptyServiceName);
        }
        Ok(Self {
            engine,
            host: host.to_string(),
            port,
            service_name: service_name.to_string(),
        })
    }
}

/// Resolve the configured destination and refuse (never dial) anything that is
/// not a local verified endpoint. Mirrors `connect_remote_engine`'s loopback-only
/// gate in `crate::federation`; actually opening the driver/ODBC session and
/// reading the catalog is the live slice `MEDULLA-UNIFIED-DATA-PLANE-R018.2.2`.
pub fn connect(
    config: &RelationalDriverConfig,
) -> Result<RelationalConnection, RelationalConnectError> {
    let endpoint = format!("{}:{}", config.host, config.port);
    let addresses: Vec<_> = endpoint
        .to_socket_addrs()
        .map_err(|_| RelationalConnectError::UnresolvableDestination)?
        .take(8)
        .collect();
    if addresses.is_empty() || addresses.iter().any(|address| !address.ip().is_loopback()) {
        return Err(RelationalConnectError::DisallowedDestination);
    }
    Ok(RelationalConnection {
        engine: config.engine,
        host: config.host.clone(),
        port: config.port,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R018.2.1
    #[test]
    fn valid_config_round_trips_per_engine() {
        let oracle =
            RelationalDriverConfig::build(RelationalEngine::Oracle, "127.0.0.1", 1521, "ORCL")
                .unwrap();
        assert_eq!(oracle.engine, RelationalEngine::Oracle);
        assert_eq!(oracle.host, "127.0.0.1");
        assert_eq!(oracle.port, 1521);
        assert_eq!(oracle.service_name, "ORCL");

        let db2 = RelationalDriverConfig::build(RelationalEngine::Db2, "127.0.0.1", 50000, "BLUDB")
            .unwrap();
        assert_eq!(db2.engine, RelationalEngine::Db2);
        assert_eq!(db2.host, "127.0.0.1");
        assert_eq!(db2.port, 50000);
        assert_eq!(db2.service_name, "BLUDB");
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R018.2.1
    #[test]
    fn empty_host_is_refused() {
        assert_eq!(
            RelationalDriverConfig::build(RelationalEngine::Oracle, "", 1521, "ORCL"),
            Err(RelationalConnectError::EmptyHost)
        );
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R018.2.1
    #[test]
    fn zero_port_is_refused() {
        assert_eq!(
            RelationalDriverConfig::build(RelationalEngine::Db2, "127.0.0.1", 0, "BLUDB"),
            Err(RelationalConnectError::InvalidPort)
        );
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R018.2.1
    #[test]
    fn empty_service_name_is_refused() {
        assert_eq!(
            RelationalDriverConfig::build(RelationalEngine::Oracle, "127.0.0.1", 1521, ""),
            Err(RelationalConnectError::EmptyServiceName)
        );
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R018.2.1
    #[test]
    fn unresolvable_destination_is_refused_by_connect() {
        // RFC 2606 reserved TLD: resolution can never succeed, so this stays
        // deterministic with no live database or network access.
        let config = RelationalDriverConfig::build(
            RelationalEngine::Oracle,
            "no-such-host.invalid",
            1521,
            "ORCL",
        )
        .expect("valid config");
        assert_eq!(
            connect(&config),
            Err(RelationalConnectError::UnresolvableDestination)
        );
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R018.2.1
    #[test]
    fn non_loopback_destination_is_refused_by_connect() {
        // A TEST-NET-3 (RFC 5737) literal IP: resolves with no DNS/network
        // lookup, so this stays deterministic with no live database.
        let config =
            RelationalDriverConfig::build(RelationalEngine::Db2, "203.0.113.5", 50000, "BLUDB")
                .expect("valid config");
        assert_eq!(
            connect(&config),
            Err(RelationalConnectError::DisallowedDestination)
        );
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R018.2.1
    #[test]
    fn loopback_destination_passes_the_refusal_gate() {
        let config =
            RelationalDriverConfig::build(RelationalEngine::Oracle, "127.0.0.1", 1521, "ORCL")
                .unwrap();
        let connection = connect(&config).expect("loopback destination must pass the gate");
        assert_eq!(connection.engine, RelationalEngine::Oracle);
        assert_eq!(connection.host, "127.0.0.1");
        assert_eq!(connection.port, 1521);
    }
}
