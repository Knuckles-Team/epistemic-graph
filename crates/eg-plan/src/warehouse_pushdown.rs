//! Warehouse federation pushdown-transport dispatch
//! (EG-UNIFIED-DATA-PLANE-R020.2): deciding, per
//! [`eg_types::warehouse_federation::WarehouseSourceKind`], whether a federated query
//! pushes down over Arrow Flight SQL or the kind's native protocol — extended for
//! Iceberg by `eg-query::sql::iceberg_federation`'s own REST-catalog scan path. This
//! is the typed-model slice (`.2.1`): the dispatch decision plus the refusal for an
//! unvalidated config or a Flight SQL preference a kind does not actually offer. It
//! issues NO request and opens NO connection — [`plan_pushdown`] is pure data-in,
//! data-out. Actually issuing the pushed-down query against each native warehouse or
//! lake engine is the live slice (EG-UNIFIED-DATA-PLANE-R020.2.2+).

use eg_types::warehouse_federation::{
    MissingWarehouseFields, WarehouseSourceConfig, WarehouseSourceKind,
};

/// Which wire protocol a pushed-down query should use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarehousePushdownTransport {
    ArrowFlightSql,
    Native,
}

/// A request to plan pushdown for one federated warehouse/lake source. Pure
/// data: planning reads it but makes no connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarehousePushdownRequest {
    pub config: WarehouseSourceConfig,
    /// Whether the caller wants Arrow Flight SQL used when available. Set
    /// `true` only when the caller actually wants the preference enforced —
    /// requesting it against a kind that does not offer it is refused rather
    /// than silently falling back to native.
    pub prefer_flight_sql: bool,
}

/// Why pushdown planning refused to produce a transport decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarehousePushdownRefusal {
    /// The source config itself was incomplete for its declared (or absent)
    /// kind; a transport can never be chosen for a config that cannot even
    /// connect.
    InvalidConfig(MissingWarehouseFields),
    /// The caller asked for Arrow Flight SQL but the validated kind does not
    /// offer it — refused rather than silently downgraded to native.
    FlightSqlNotOffered(WarehouseSourceKind),
}

impl std::fmt::Display for WarehousePushdownRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidConfig(inner) => write!(f, "{inner}"),
            Self::FlightSqlNotOffered(kind) => {
                write!(f, "{kind:?} does not offer Arrow Flight SQL")
            }
        }
    }
}

impl std::error::Error for WarehousePushdownRefusal {}

/// Decide the pushdown transport for one request, refusing an invalid config
/// or an unmet Flight SQL preference before any query is issued. Performs no
/// I/O.
pub fn plan_pushdown(
    request: &WarehousePushdownRequest,
) -> Result<WarehousePushdownTransport, WarehousePushdownRefusal> {
    let kind = request
        .config
        .validate()
        .map_err(WarehousePushdownRefusal::InvalidConfig)?;
    if request.prefer_flight_sql {
        if kind.offers_flight_sql() {
            Ok(WarehousePushdownTransport::ArrowFlightSql)
        } else {
            Err(WarehousePushdownRefusal::FlightSqlNotOffered(kind))
        }
    } else {
        Ok(WarehousePushdownTransport::Native)
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

    // spec: EG-UNIFIED-DATA-PLANE-R020.2.1
    #[test]
    fn snowflake_prefers_flight_sql_when_requested() {
        let request = WarehousePushdownRequest {
            config: config(
                WarehouseSourceKind::Snowflake,
                &[("account", "acme"), ("database", "db"), ("warehouse", "wh")],
            ),
            prefer_flight_sql: true,
        };
        assert_eq!(
            plan_pushdown(&request),
            Ok(WarehousePushdownTransport::ArrowFlightSql)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.2.1
    #[test]
    fn duckdb_without_flight_sql_preference_uses_native() {
        let request = WarehousePushdownRequest {
            config: config(
                WarehouseSourceKind::DuckDb,
                &[("database_path", "/data/db.duckdb")],
            ),
            prefer_flight_sql: false,
        };
        assert_eq!(
            plan_pushdown(&request),
            Ok(WarehousePushdownTransport::Native)
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.2.1
    #[test]
    fn duckdb_flight_sql_preference_is_refused_not_downgraded() {
        let request = WarehousePushdownRequest {
            config: config(
                WarehouseSourceKind::DuckDb,
                &[("database_path", "/data/db.duckdb")],
            ),
            prefer_flight_sql: true,
        };
        assert_eq!(
            plan_pushdown(&request),
            Err(WarehousePushdownRefusal::FlightSqlNotOffered(
                WarehouseSourceKind::DuckDb
            ))
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.2.1
    #[test]
    fn iceberg_flight_sql_preference_is_refused() {
        let request = WarehousePushdownRequest {
            config: config(WarehouseSourceKind::Iceberg, &[("catalog_uri", "http://x")]),
            prefer_flight_sql: true,
        };
        assert_eq!(
            plan_pushdown(&request),
            Err(WarehousePushdownRefusal::FlightSqlNotOffered(
                WarehouseSourceKind::Iceberg
            ))
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.2.1
    #[test]
    fn invalid_config_is_refused_before_any_transport_decision() {
        let request = WarehousePushdownRequest {
            config: config(WarehouseSourceKind::BigQuery, &[("project", "p")]),
            prefer_flight_sql: false,
        };
        assert!(matches!(
            plan_pushdown(&request),
            Err(WarehousePushdownRefusal::InvalidConfig(_))
        ));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R020.2.1
    #[test]
    fn config_with_no_declared_kind_is_refused() {
        let request = WarehousePushdownRequest {
            config: WarehouseSourceConfig::default(),
            prefer_flight_sql: false,
        };
        assert!(matches!(
            plan_pushdown(&request),
            Err(WarehousePushdownRefusal::InvalidConfig(_))
        ));
    }
}
