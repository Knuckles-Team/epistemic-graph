//! ClickHouse attached-source driver config + connect/refusal path
//! (CONCEPT:EG-UNIFIED-DATA-PLANE-R017.2.1).
//!
//! This slice covers ONLY the typed driver config and the refusal gate: it proves a
//! [`ClickHouseDriverConfig`] can be built from validated host/port/database inputs,
//! and that [`connect`] refuses an unsafe or invalid destination before issuing any
//! HTTP/native-client request. It requires NO live ClickHouse server. Issuing a real
//! request and pushing federated query fragments down to ClickHouse is the live slice
//! `EG-UNIFIED-DATA-PLANE-R017.2.2`, mirroring the loopback-only refusal gate already
//! used by `connect_remote_engine` in `crate::federation`.

use std::net::ToSocketAddrs;

/// A validated ClickHouse connection target. Construct only through
/// [`ClickHouseDriverConfig::build`], which refuses an empty host, a zero port, or an
/// empty database before a config can exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseDriverConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub tls: bool,
}

/// A connected (resolved, refusal-gate-passed) ClickHouse target handle. The live
/// slice (`R017.2.2`) is responsible for actually issuing HTTP/native-client requests
/// over it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseConnection {
    pub host: String,
    pub port: u16,
}

/// Why a [`ClickHouseDriverConfig`] could not be built, or [`connect`] refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseConnectError {
    EmptyHost,
    InvalidPort,
    EmptyDatabase,
    UnresolvableDestination,
    DisallowedDestination,
}

impl std::fmt::Display for ClickHouseConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyHost => "clickhouse: host must not be empty",
            Self::InvalidPort => "clickhouse: port must not be zero",
            Self::EmptyDatabase => "clickhouse: database must not be empty",
            Self::UnresolvableDestination => "clickhouse: unable to resolve destination",
            Self::DisallowedDestination => {
                "clickhouse: destination is not allowed (not a local verified endpoint)"
            }
        };
        f.write_str(message)
    }
}

impl std::error::Error for ClickHouseConnectError {}

impl ClickHouseDriverConfig {
    /// Build a validated driver config, refusing an empty host, a zero port, or an
    /// empty database.
    pub fn build(
        host: &str,
        port: u16,
        database: &str,
        tls: bool,
    ) -> Result<Self, ClickHouseConnectError> {
        if host.trim().is_empty() {
            return Err(ClickHouseConnectError::EmptyHost);
        }
        if port == 0 {
            return Err(ClickHouseConnectError::InvalidPort);
        }
        if database.trim().is_empty() {
            return Err(ClickHouseConnectError::EmptyDatabase);
        }
        Ok(Self {
            host: host.to_string(),
            port,
            database: database.to_string(),
            tls,
        })
    }
}

/// Resolve the configured destination and refuse (never dial) anything that is not a
/// local verified endpoint. Mirrors `connect_remote_engine`'s loopback-only gate in
/// `crate::federation`; actually opening the HTTP/native-client session is the live
/// slice `EG-UNIFIED-DATA-PLANE-R017.2.2`.
pub fn connect(
    config: &ClickHouseDriverConfig,
) -> Result<ClickHouseConnection, ClickHouseConnectError> {
    let endpoint = format!("{}:{}", config.host, config.port);
    let addresses: Vec<_> = endpoint
        .to_socket_addrs()
        .map_err(|_| ClickHouseConnectError::UnresolvableDestination)?
        .take(8)
        .collect();
    if addresses.is_empty() || addresses.iter().any(|address| !address.ip().is_loopback()) {
        return Err(ClickHouseConnectError::DisallowedDestination);
    }
    Ok(ClickHouseConnection {
        host: config.host.clone(),
        port: config.port,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-UNIFIED-DATA-PLANE-R017.2.1
    #[test]
    fn valid_config_round_trips() {
        let config = ClickHouseDriverConfig::build("127.0.0.1", 8123, "default", false).unwrap();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8123);
        assert_eq!(config.database, "default");
        assert!(!config.tls);
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.2.1
    #[test]
    fn empty_host_is_refused() {
        assert_eq!(
            ClickHouseDriverConfig::build("", 8123, "default", false),
            Err(ClickHouseConnectError::EmptyHost)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.2.1
    #[test]
    fn zero_port_is_refused() {
        assert_eq!(
            ClickHouseDriverConfig::build("127.0.0.1", 0, "default", false),
            Err(ClickHouseConnectError::InvalidPort)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.2.1
    #[test]
    fn empty_database_is_refused() {
        assert_eq!(
            ClickHouseDriverConfig::build("127.0.0.1", 8123, "", false),
            Err(ClickHouseConnectError::EmptyDatabase)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.2.1
    #[test]
    fn non_loopback_destination_is_refused_by_connect() {
        // A TEST-NET-3 (RFC 5737) literal IP: resolves with no DNS/network lookup, so
        // this stays deterministic with no live database or network access.
        let config = ClickHouseDriverConfig::build("203.0.113.5", 8123, "default", false)
            .expect("valid config");
        assert_eq!(
            connect(&config),
            Err(ClickHouseConnectError::DisallowedDestination)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R017.2.1
    #[test]
    fn loopback_destination_passes_the_refusal_gate() {
        let config = ClickHouseDriverConfig::build("127.0.0.1", 8123, "default", false).unwrap();
        let connection = connect(&config).expect("loopback destination must pass the gate");
        assert_eq!(connection.host, "127.0.0.1");
        assert_eq!(connection.port, 8123);
    }
}
