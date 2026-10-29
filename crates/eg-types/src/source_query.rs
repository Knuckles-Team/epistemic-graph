//! Approved source queries (MEDULLA-FEDERATED-QUERY-R085.1): the records Medulla
//! issues so a connector may run exactly one read-only, natively
//! parameterized SQL statement against a registered source.
//!
//! Pure data and validation. Issuing, verifying and digesting approvals are
//! later children of MEDULLA-FEDERATED-QUERY-R085.

use serde::{Deserialize, Serialize};

/// Record version of every source-query record.
pub const SOURCE_QUERY_SCHEMA_VERSION: u16 = 1;
/// Upper bound on any single limit, so "unbounded" cannot be spelled as a huge value.
pub const MAX_SOURCE_QUERY_LIMIT: u64 = 1 << 40;
/// Most parameters one statement may bind.
pub const MAX_SOURCE_QUERY_PARAMETERS: usize = 1024;

/// The closed refusal kinds of a source-query record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SourceQueryError {
    #[error("unsupported_dialect")]
    UnsupportedDialect,
    #[error("invalid_query")]
    InvalidQuery,
    #[error("limit_exceeded")]
    LimitExceeded,
}

/// SQL dialects a source may declare. Closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SourceQueryDialect {
    Postgres,
    Mysql,
    Sqlite,
    Mssql,
    Clickhouse,
}

/// One natively bound parameter value. Closed kinds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum SourceQueryValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(String),
    Bytes(
        #[cfg_attr(feature = "contract-schema", schemars(with = "Vec<u8>"))]
        #[serde(with = "serde_bytes")]
        Vec<u8>,
    ),
}

/// A parameter at a 1-based placeholder position. The typed value nests as
/// `{"kind", "value"}` so unknown fields still refuse at every level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SourceQueryParameter {
    pub position: u32,
    pub value: SourceQueryValue,
}

/// Finite, positive execution limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SourceQueryLimits {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub chunk_rows: u64,
    pub chunk_bytes: u64,
    pub queue_depth: u64,
    pub queue_bytes: u64,
    pub deadline_ms: u64,
    pub max_resident_bytes: u64,
}

impl SourceQueryLimits {
    pub fn validate(&self) -> Result<(), SourceQueryError> {
        let all = [
            self.max_rows,
            self.max_bytes,
            self.chunk_rows,
            self.chunk_bytes,
            self.queue_depth,
            self.queue_bytes,
            self.deadline_ms,
            self.max_resident_bytes,
        ];
        if all.iter().any(|v| *v == 0 || *v > MAX_SOURCE_QUERY_LIMIT) {
            return Err(SourceQueryError::LimitExceeded);
        }
        if self.chunk_rows > self.max_rows || self.chunk_bytes > self.max_bytes {
            return Err(SourceQueryError::LimitExceeded);
        }
        Ok(())
    }
}

/// A request for one read-only SQL operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SourceQueryRequest {
    pub schema_version: u16,
    pub source_ref: String,
    pub source_revision: String,
    pub schema_digest: String,
    pub snapshot_ref: String,
    pub dialect: SourceQueryDialect,
    pub sql: String,
    pub parameters: Vec<SourceQueryParameter>,
    pub limits: SourceQueryLimits,
    pub requested_expires_at_ms: u64,
    pub idempotency_key: String,
}

fn require(value: &str) -> Result<(), SourceQueryError> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(SourceQueryError::InvalidQuery);
    }
    Ok(())
}

/// Placeholders the statement declares: the highest `$n` for Postgres, the
/// count of `?` for the others. Quoted text is skipped.
fn placeholder_count(dialect: SourceQueryDialect, sql: &str) -> usize {
    let mut count = 0usize;
    let mut quoted = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => quoted = !quoted,
            _ if quoted => {}
            '?' if dialect != SourceQueryDialect::Postgres => count += 1,
            '$' if dialect == SourceQueryDialect::Postgres => {
                let mut digits = String::new();
                while let Some(d) = chars.peek().filter(|d| d.is_ascii_digit()) {
                    digits.push(*d);
                    chars.next();
                }
                count = count.max(digits.parse().unwrap_or(0));
            }
            _ => {}
        }
    }
    count
}

impl SourceQueryRequest {
    /// Refuse missing identities, mismatched parameters and invalid limits.
    pub fn validate(&self) -> Result<(), SourceQueryError> {
        if self.schema_version != SOURCE_QUERY_SCHEMA_VERSION {
            return Err(SourceQueryError::InvalidQuery);
        }
        for field in [
            &self.source_ref,
            &self.source_revision,
            &self.schema_digest,
            &self.snapshot_ref,
            &self.sql,
            &self.idempotency_key,
        ] {
            require(field)?;
        }
        if self.requested_expires_at_ms == 0 {
            return Err(SourceQueryError::InvalidQuery);
        }
        self.limits.validate()?;
        self.validate_parameters()
    }

    fn validate_parameters(&self) -> Result<(), SourceQueryError> {
        if self.parameters.len() > MAX_SOURCE_QUERY_PARAMETERS
            || self.parameters.len() != placeholder_count(self.dialect, &self.sql)
        {
            return Err(SourceQueryError::InvalidQuery);
        }
        for (index, parameter) in self.parameters.iter().enumerate() {
            if parameter.position as usize != index + 1 {
                return Err(SourceQueryError::InvalidQuery);
            }
            if matches!(parameter.value, SourceQueryValue::Float64(v) if !v.is_finite()) {
                return Err(SourceQueryError::InvalidQuery);
            }
        }
        Ok(())
    }
}

/// The only operation an approval of this version allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SourceQueryOperation {
    ReadSql,
}

/// An immutable approval of one request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ApprovedSourceQuery {
    pub schema_version: u16,
    pub approval_id: String,
    pub query_id: String,
    pub request: SourceQueryRequest,
    pub tenant_ref: String,
    pub owner_ref: String,
    pub actor_ref: String,
    pub authority_digest: String,
    pub policy_digest: String,
    pub identity_digest: String,
    pub allowed_operations: Vec<SourceQueryOperation>,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub hard_expires_at_ms: u64,
    pub operation_digest: String,
    pub parameter_digest: String,
    pub query_digest: String,
}

impl ApprovedSourceQuery {
    pub fn validate(&self) -> Result<(), SourceQueryError> {
        if self.schema_version != SOURCE_QUERY_SCHEMA_VERSION
            || self.allowed_operations != [SourceQueryOperation::ReadSql]
        {
            return Err(SourceQueryError::InvalidQuery);
        }
        for field in [
            &self.approval_id,
            &self.query_id,
            &self.tenant_ref,
            &self.owner_ref,
            &self.actor_ref,
            &self.authority_digest,
            &self.policy_digest,
            &self.identity_digest,
            &self.operation_digest,
            &self.parameter_digest,
            &self.query_digest,
        ] {
            require(field)?;
        }
        if !(self.issued_at_ms < self.expires_at_ms
            && self.expires_at_ms <= self.hard_expires_at_ms)
        {
            return Err(SourceQueryError::InvalidQuery);
        }
        self.request.validate()
    }
}

/// The execution boundary a verification happens at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SourceQueryPhase {
    Start,
    Chunk,
    Complete,
}

/// A request to verify an approval against observed source identities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SourceQueryApprovalCheck {
    pub approved_query: ApprovedSourceQuery,
    pub observed_source_revision: String,
    pub observed_schema_digest: String,
    pub observed_snapshot_ref: String,
    pub phase: SourceQueryPhase,
}

/// A bounded validation result; it authorizes nothing past `valid_until_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct SourceQueryApprovalValidity {
    pub approval_id: String,
    pub query_id: String,
    pub query_digest: String,
    pub lease_revision: u64,
    pub checked_at_ms: u64,
    pub valid_until_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> SourceQueryLimits {
        SourceQueryLimits {
            max_rows: 10_000,
            max_bytes: 1 << 24,
            chunk_rows: 500,
            chunk_bytes: 1 << 20,
            queue_depth: 4,
            queue_bytes: 1 << 22,
            deadline_ms: 30_000,
            max_resident_bytes: 1 << 26,
        }
    }

    fn request() -> SourceQueryRequest {
        SourceQueryRequest {
            schema_version: 1,
            source_ref: "src:orders".into(),
            source_revision: "rev-7".into(),
            schema_digest: "sha256:abc".into(),
            snapshot_ref: "snap-42".into(),
            dialect: SourceQueryDialect::Postgres,
            sql: "SELECT id FROM orders WHERE region = $1 AND total > $2 AND note <> '$9'".into(),
            parameters: vec![
                SourceQueryParameter {
                    position: 1,
                    value: SourceQueryValue::Text("eu".into()),
                },
                SourceQueryParameter {
                    position: 2,
                    value: SourceQueryValue::Float64(10.5),
                },
            ],
            limits: limits(),
            requested_expires_at_ms: 1_000,
            idempotency_key: "idem-1".into(),
        }
    }

    fn approved() -> ApprovedSourceQuery {
        ApprovedSourceQuery {
            schema_version: 1,
            approval_id: "appr-1".into(),
            query_id: "q-1".into(),
            request: request(),
            tenant_ref: "t1".into(),
            owner_ref: "o1".into(),
            actor_ref: "a1".into(),
            authority_digest: "d-auth".into(),
            policy_digest: "d-pol".into(),
            identity_digest: "d-id".into(),
            allowed_operations: vec![SourceQueryOperation::ReadSql],
            issued_at_ms: 100,
            expires_at_ms: 900,
            hard_expires_at_ms: 1_000,
            operation_digest: "d-op".into(),
            parameter_digest: "d-par".into(),
            query_digest: "d-q".into(),
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R085.1
    #[test]
    fn approval_records_round_trip() {
        let approval = approved();
        approval.validate().unwrap();
        let json = serde_json::to_string(&approval).unwrap();
        assert_eq!(
            serde_json::from_str::<ApprovedSourceQuery>(&json).unwrap(),
            approval
        );
        let check = SourceQueryApprovalCheck {
            approved_query: approval.clone(),
            observed_source_revision: "rev-7".into(),
            observed_schema_digest: "sha256:abc".into(),
            observed_snapshot_ref: "snap-42".into(),
            phase: SourceQueryPhase::Chunk,
        };
        let json = serde_json::to_string(&check).unwrap();
        assert_eq!(
            serde_json::from_str::<SourceQueryApprovalCheck>(&json).unwrap(),
            check
        );
        let validity = SourceQueryApprovalValidity {
            approval_id: "appr-1".into(),
            query_id: "q-1".into(),
            query_digest: "d-q".into(),
            lease_revision: 3,
            checked_at_ms: 200,
            valid_until_ms: 300,
        };
        let json = serde_json::to_string(&validity).unwrap();
        assert_eq!(
            serde_json::from_str::<SourceQueryApprovalValidity>(&json).unwrap(),
            validity
        );
        for value in [
            SourceQueryValue::Null,
            SourceQueryValue::Bool(true),
            SourceQueryValue::Int64(i64::MIN),
            SourceQueryValue::Bytes(vec![0, 255]),
        ] {
            let p = SourceQueryParameter { position: 1, value };
            let json = serde_json::to_string(&p).unwrap();
            assert_eq!(
                serde_json::from_str::<SourceQueryParameter>(&json).unwrap(),
                p
            );
        }

        // Unknown fields, unrepresentable values and unknown kinds refuse to decode.
        let mut extra = serde_json::to_value(request()).unwrap();
        extra["credentials"] = "secret".into();
        assert!(serde_json::from_value::<SourceQueryRequest>(extra).is_err());
        assert!(serde_json::from_str::<SourceQueryParameter>(
            r#"{"position":1,"value":{"kind":"int64","value":9223372036854775808}}"#
        )
        .is_err());
        assert!(serde_json::from_str::<SourceQueryParameter>(
            r#"{"position":1,"value":{"kind":"decimal","value":"1.0"}}"#
        )
        .is_err());

        // Invalid references, parameters, limits and approvals refuse.
        let mut no_snapshot = request();
        no_snapshot.snapshot_ref = " ".into();
        assert_eq!(no_snapshot.validate(), Err(SourceQueryError::InvalidQuery));
        let mut nan = request();
        nan.parameters[1].value = SourceQueryValue::Float64(f64::NAN);
        assert_eq!(nan.validate(), Err(SourceQueryError::InvalidQuery));
        let mut missing = request();
        missing.parameters.pop();
        assert_eq!(missing.validate(), Err(SourceQueryError::InvalidQuery));
        let mut misplaced = request();
        misplaced.parameters[1].position = 3;
        assert_eq!(misplaced.validate(), Err(SourceQueryError::InvalidQuery));
        let mut zero = request();
        zero.limits.queue_depth = 0;
        assert_eq!(zero.validate(), Err(SourceQueryError::LimitExceeded));
        let mut unbounded = request();
        unbounded.limits.max_rows = u64::MAX;
        assert_eq!(unbounded.validate(), Err(SourceQueryError::LimitExceeded));
        let mut write = approved();
        write.allowed_operations.push(SourceQueryOperation::ReadSql);
        assert_eq!(write.validate(), Err(SourceQueryError::InvalidQuery));
        let mut expired = approved();
        expired.hard_expires_at_ms = 800;
        assert_eq!(expired.validate(), Err(SourceQueryError::InvalidQuery));
        let mut mysql = request();
        mysql.dialect = SourceQueryDialect::Mysql;
        mysql.sql = "SELECT id FROM orders WHERE region = ? AND total > ?".into();
        mysql.validate().unwrap();
    }
}
