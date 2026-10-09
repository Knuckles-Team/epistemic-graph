//! Typed model for cross-application consolidation safety
//! (EG-UNIFIED-DATA-PLANE-R031): the quota and upgrade-coupling policy that
//! bound one attached application's blast radius, and the admission record
//! that scopes it to a tenant and principal before it is exposed through
//! any approved mapping. This is the typed-model slice (`.1`): the model
//! plus the refusal of an admission with no isolation identity (empty
//! tenant/application/principal), a quota with every bound at zero (it
//! blocks the application entirely rather than isolating it), and a second
//! admission that reuses the same tenant/application under a different
//! principal (an isolation-identity collision). The real fault-isolation
//! enforcement and quota metering are later children.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The bounds that cap one attached application's blast radius.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachedApplicationQuota {
    pub max_requests_per_minute: u32,
    pub max_concurrent_connections: u32,
    pub max_storage_bytes: u64,
}

/// How a later upgrade to this application's integration is allowed to
/// couple with other attached applications.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeCouplingPolicy {
    /// May upgrade on its own schedule.
    Independent,
    /// Must upgrade together with a named dependency set.
    LockStep,
    /// Requires a recorded, separate go-ahead before upgrading.
    Manual,
}

/// One attached application's admission: the tenant/principal it is scoped
/// to, its blast-radius quota, and its upgrade-coupling policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplicationAdmission {
    pub application: String,
    pub tenant: String,
    pub principal: String,
    pub quota: AttachedApplicationQuota,
    pub upgrade_coupling: UpgradeCouplingPolicy,
}

/// An admission record failed validation. Carries the offending
/// application name and the reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidApplicationAdmission {
    pub application: String,
    pub reason: String,
}

impl std::fmt::Display for InvalidApplicationAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid admission for application {:?}: {}",
            self.application, self.reason
        )
    }
}

impl std::error::Error for InvalidApplicationAdmission {}

/// Confirm an admission record is well-formed: application, tenant, and
/// principal are all non-empty (isolation needs an explicit identity to
/// isolate), and the quota is not entirely zero (a fully zeroed quota
/// blocks the application rather than bounding its blast radius, and is
/// never a meaningful admission).
pub fn validate_admission(
    admission: &ApplicationAdmission,
) -> Result<(), InvalidApplicationAdmission> {
    if admission.application.is_empty()
        || admission.tenant.is_empty()
        || admission.principal.is_empty()
    {
        return Err(InvalidApplicationAdmission {
            application: admission.application.clone(),
            reason: "application, tenant, and principal must all be non-empty".to_string(),
        });
    }
    let quota = admission.quota;
    if quota.max_requests_per_minute == 0
        && quota.max_concurrent_connections == 0
        && quota.max_storage_bytes == 0
    {
        return Err(InvalidApplicationAdmission {
            application: admission.application.clone(),
            reason: "quota with every bound at zero cannot bound a blast radius".to_string(),
        });
    }
    Ok(())
}

/// The registry of admitted applications, keyed by tenant and application
/// name. `admit` is the one entry point a caller uses instead of mutating
/// `admissions` directly.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsolidationIsolationRegistry {
    admissions: BTreeMap<(String, String), ApplicationAdmission>,
}

impl ConsolidationIsolationRegistry {
    /// Admit an application under its tenant, refusing an invalid record
    /// and refusing to re-admit the same tenant/application pair under a
    /// different principal (an isolation-identity collision — the existing
    /// principal must be the one that re-admits or revokes first).
    pub fn admit(
        &mut self,
        admission: ApplicationAdmission,
    ) -> Result<(), InvalidApplicationAdmission> {
        validate_admission(&admission)?;
        let key = (admission.tenant.clone(), admission.application.clone());
        if let Some(existing) = self.admissions.get(&key) {
            if existing.principal != admission.principal {
                return Err(InvalidApplicationAdmission {
                    application: admission.application.clone(),
                    reason: format!(
                        "already admitted under principal {:?}; refusing re-admission under {:?}",
                        existing.principal, admission.principal
                    ),
                });
            }
        }
        self.admissions.insert(key, admission);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.admissions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.admissions.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admission(tenant: &str, application: &str, principal: &str) -> ApplicationAdmission {
        ApplicationAdmission {
            application: application.to_string(),
            tenant: tenant.to_string(),
            principal: principal.to_string(),
            quota: AttachedApplicationQuota {
                max_requests_per_minute: 600,
                max_concurrent_connections: 10,
                max_storage_bytes: 1_000_000,
            },
            upgrade_coupling: UpgradeCouplingPolicy::Independent,
        }
    }

    #[test]
    fn valid_admission_round_trips_through_admit() {
        let mut registry = ConsolidationIsolationRegistry::default();
        registry
            .admit(admission("tenant-a", "gramps", "alice"))
            .unwrap();
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn empty_identity_is_refused() {
        let mut bad = admission("tenant-a", "gramps", "alice");
        bad.principal = String::new();
        assert!(validate_admission(&bad).is_err());
    }

    #[test]
    fn all_zero_quota_is_refused() {
        let mut bad = admission("tenant-a", "gramps", "alice");
        bad.quota = AttachedApplicationQuota {
            max_requests_per_minute: 0,
            max_concurrent_connections: 0,
            max_storage_bytes: 0,
        };
        let Err(error) = validate_admission(&bad) else {
            panic!("an all-zero quota must be refused");
        };
        assert!(error.reason.contains("blast radius"), "{error}");
    }

    #[test]
    fn same_principal_re_admission_is_idempotent() {
        let mut registry = ConsolidationIsolationRegistry::default();
        registry
            .admit(admission("tenant-a", "gramps", "alice"))
            .unwrap();
        registry
            .admit(admission("tenant-a", "gramps", "alice"))
            .unwrap();
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn different_principal_re_admission_is_refused() {
        let mut registry = ConsolidationIsolationRegistry::default();
        registry
            .admit(admission("tenant-a", "gramps", "alice"))
            .unwrap();
        let result = registry.admit(admission("tenant-a", "gramps", "mallory"));
        assert!(result.is_err());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn invalid_admission_is_refused_not_silently_admitted() {
        let mut registry = ConsolidationIsolationRegistry::default();
        let result = registry.admit(admission("tenant-a", "", "alice"));
        assert!(result.is_err());
        assert!(registry.is_empty());
    }
}
