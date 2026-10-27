//! Validated destination configuration for the MongoDB/DocumentDB adapter.
//!
//! This slice validates the configured destination and refuses remote or
//! unresolvable hosts before any request is issued. Opening a native driver
//! session and capturing change streams belongs to
//! EG-UNIFIED-DATA-PLANE-R019.2.2.

use std::net::{SocketAddr, ToSocketAddrs};

/// A MongoDB/DocumentDB endpoint after validating its required fields.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DocumentDriverConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
}

/// A validated destination. This does not open a database session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentConnection {
    pub config: DocumentDriverConfig,
    pub addresses: Vec<SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DocumentConnectionError {
    EmptyHost,
    ZeroPort,
    EmptyDatabase,
    ResolveFailed,
    UnsafeDestination,
}

impl std::fmt::Display for DocumentConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::EmptyHost => "document connection: host is empty",
            Self::ZeroPort => "document connection: port must be nonzero",
            Self::EmptyDatabase => "document connection: database is empty",
            Self::ResolveFailed => "document connection: host did not resolve",
            Self::UnsafeDestination => "document connection: destination must be loopback",
        })
    }
}

impl std::error::Error for DocumentConnectionError {}

impl DocumentDriverConfig {
    /// Build a configuration only when host, port, and database are present.
    pub fn new(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
    ) -> Result<Self, DocumentConnectionError> {
        let host = host.into();
        let database = database.into();
        if host.trim().is_empty() {
            return Err(DocumentConnectionError::EmptyHost);
        }
        if port == 0 {
            return Err(DocumentConnectionError::ZeroPort);
        }
        if database.trim().is_empty() {
            return Err(DocumentConnectionError::EmptyDatabase);
        }
        Ok(Self {
            host,
            port,
            database,
        })
    }

    /// Resolve and vet the endpoint before any database request is issued.
    /// All resolved addresses must be loopback; mixed public/private answers
    /// fail closed. The returned handle is configuration only, not a driver.
    pub fn connect(&self) -> Result<DocumentConnection, DocumentConnectionError> {
        let addresses: Vec<_> = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .map_err(|_| DocumentConnectionError::ResolveFailed)?
            .collect();
        if addresses.is_empty() {
            return Err(DocumentConnectionError::ResolveFailed);
        }
        if addresses.iter().any(|address| !address.ip().is_loopback()) {
            return Err(DocumentConnectionError::UnsafeDestination);
        }
        Ok(DocumentConnection {
            config: self.clone(),
            addresses,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_driver_config_and_destination_gate() {
        let config = DocumentDriverConfig::new("localhost", 27017, "catalog").unwrap();
        let round_trip: DocumentDriverConfig =
            serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
        assert_eq!(round_trip, config);
        assert_eq!(config.connect().unwrap().config, config);

        assert_eq!(
            DocumentDriverConfig::new(" ", 27017, "catalog"),
            Err(DocumentConnectionError::EmptyHost)
        );
        assert_eq!(
            DocumentDriverConfig::new("localhost", 0, "catalog"),
            Err(DocumentConnectionError::ZeroPort)
        );
        assert_eq!(
            DocumentDriverConfig::new("localhost", 27017, " "),
            Err(DocumentConnectionError::EmptyDatabase)
        );

        let unresolved = DocumentDriverConfig::new(
            "this-document-host-must-not-resolve.invalid",
            27017,
            "catalog",
        )
        .unwrap();
        assert_eq!(
            unresolved.connect(),
            Err(DocumentConnectionError::ResolveFailed)
        );
        let remote = DocumentDriverConfig::new("192.0.2.1", 27017, "catalog").unwrap();
        assert_eq!(
            remote.connect(),
            Err(DocumentConnectionError::UnsafeDestination)
        );
    }
}
