//! Query-plan scope predicates (CONCEPT:MEDULLA-KG.query.plan-scope).
//!
//! This module contains only the immutable scope model. The plan rewriter,
//! served-handler wiring, refusal, trace evidence, and served proof belong to
//! the later `MEDULLA-FEDERATED-QUERY-R078.2` through `.6` requirements.

use serde::{Deserialize, Deserializer, Serialize};

/// The closed set of predicates a [`PlanScope`] can contribute to a plan.
///
/// Keeping this an enum prevents a caller from smuggling an arbitrary
/// post-retrieval filter into a plan scope. The tenant predicate is exact;
/// the clearance predicate carries the labels the caller is allowed to see.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScopePredicate {
    /// Restrict rows to one exact tenant.
    TenantEquals { tenant: String },
    /// Restrict rows to the caller's declared clearance-label set.
    ClearanceSubset { labels: Vec<String> },
}

/// The caller scope that a query plan will carry into later planning stages.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanScope {
    /// The tenant whose rows may be read.
    #[serde(deserialize_with = "deserialize_non_empty_tenant")]
    pub tenant: String,
    /// Clearance labels available to the caller.
    pub clearance_labels: Vec<String>,
    /// Whether the tenant and labels came from verified authority.
    pub verified: bool,
}

/// Validation failures for a plan scope model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanScopeError {
    /// A scope without a tenant cannot safely constrain a query.
    EmptyTenant,
}

impl std::fmt::Display for PlanScopeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyTenant => formatter.write_str("plan scope tenant must be non-empty"),
        }
    }
}

impl std::error::Error for PlanScopeError {}

fn deserialize_non_empty_tenant<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let tenant = String::deserialize(deserializer)?;
    if tenant.trim().is_empty() {
        return Err(serde::de::Error::custom(PlanScopeError::EmptyTenant));
    }
    Ok(tenant)
}

impl PlanScope {
    /// Build a scope and reject an empty or whitespace-only tenant.
    pub fn new(
        tenant: impl Into<String>,
        clearance_labels: Vec<String>,
        verified: bool,
    ) -> Result<Self, PlanScopeError> {
        let scope = Self {
            tenant: tenant.into(),
            clearance_labels,
            verified,
        };
        scope.validate()?;
        Ok(scope)
    }

    /// Validate the fields that are required before a scope can constrain a plan.
    pub fn validate(&self) -> Result<(), PlanScopeError> {
        if self.tenant.trim().is_empty() {
            return Err(PlanScopeError::EmptyTenant);
        }
        Ok(())
    }

    /// Return the complete closed predicate set represented by this scope.
    pub fn predicate(&self) -> Result<Vec<ScopePredicate>, PlanScopeError> {
        self.validate()?;
        Ok(vec![
            ScopePredicate::TenantEquals {
                tenant: self.tenant.clone(),
            },
            ScopePredicate::ClearanceSubset {
                labels: self.clearance_labels.clone(),
            },
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::{PlanScope, ScopePredicate};

    #[test]
    // spec: MEDULLA-FEDERATED-QUERY-R078.1
    fn plan_scope_round_trips_and_rejects_empty_tenant() {
        let scope = PlanScope::new("tenant-a", vec!["public".into(), "restricted".into()], true)
            .expect("valid tenant");
        let encoded = serde_json::to_vec(&scope).expect("serialize plan scope");
        let decoded: PlanScope = serde_json::from_slice(&encoded).expect("deserialize plan scope");

        assert_eq!(decoded, scope);
        assert_eq!(
            decoded.predicate().expect("valid predicates"),
            vec![
                ScopePredicate::TenantEquals {
                    tenant: "tenant-a".into(),
                },
                ScopePredicate::ClearanceSubset {
                    labels: vec!["public".into(), "restricted".into()],
                },
            ]
        );
        assert!(PlanScope::new("  ", Vec::new(), true).is_err());
        assert!(serde_json::from_slice::<PlanScope>(
            br#"{"tenant":"","clearance_labels":[],"verified":true}"#
        )
        .is_err());
    }
}
