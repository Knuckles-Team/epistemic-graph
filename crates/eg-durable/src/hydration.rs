//! Typed baseline-hydration model (`EG-REPO-INGEST-R650.1`).
//!
//! These values describe a planned hydration pass.  They carry the tenant
//! boundary and durable source identities, but deliberately do not fetch,
//! classify, split, or write anything.  Those operations belong to the later
//! R650 children and must consume these values through their own bounded
//! admission paths.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

/// The stable model version for hydration records.
pub const HYDRATION_MODEL_VERSION: &str = "eg-durable/hydration-v1";

/// A bounded plan for one tenant's baseline source scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaselinePlan {
    /// The tenant that owns every source in this plan.
    pub tenant_id: String,
    /// The configured source scope, such as a workspace or repository group.
    pub scope: String,
    /// Maximum number of work items admitted for this scope.
    pub max_work_items: u32,
    /// Stages that a worker may execute for each admitted item.
    pub stages: Vec<StagedStage>,
}

impl BaselinePlan {
    /// Validate the identity and the positive work bound before admission.
    pub fn validate(&self) -> Result<(), HydrationModelError> {
        require_text("tenant_id", &self.tenant_id)?;
        require_text("scope", &self.scope)?;
        if self.max_work_items == 0 {
            return Err(HydrationModelError::NonPositiveBound("max_work_items"));
        }
        if self.stages.is_empty() {
            return Err(HydrationModelError::EmptyStages);
        }
        Ok(())
    }
}

/// One typed key in a delta manifest.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeltaManifestKey {
    pub graph: String,
    pub category: String,
    pub uri: String,
}

impl DeltaManifestKey {
    pub fn validate(&self) -> Result<(), HydrationModelError> {
        require_text("graph", &self.graph)?;
        require_text("category", &self.category)?;
        require_text("uri", &self.uri)
    }
}

/// One `(graph, category, uri) -> content_hash` manifest entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaManifestEntry {
    pub key: DeltaManifestKey,
    pub content_hash: String,
}

impl DeltaManifestEntry {
    pub fn validate(&self) -> Result<(), HydrationModelError> {
        self.key.validate()?;
        require_text("content_hash", &self.content_hash)
    }
}

/// The durable content identities observed for one tenant's source set.
/// Entries are kept in canonical key order so serialization is deterministic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaManifest {
    pub tenant_id: String,
    pub entries: Vec<DeltaManifestEntry>,
}

impl DeltaManifest {
    pub fn validate(&self) -> Result<(), HydrationModelError> {
        require_text("tenant_id", &self.tenant_id)?;
        let mut keys = BTreeSet::new();
        for entry in &self.entries {
            entry.validate()?;
            if !keys.insert(&entry.key) {
                return Err(HydrationModelError::DuplicateManifestKey);
            }
        }
        Ok(())
    }

    /// Insert or replace one source identity and retain canonical ordering.
    pub fn record(&mut self, entry: DeltaManifestEntry) -> Result<(), HydrationModelError> {
        entry.validate()?;
        if let Some(existing) = self.entries.iter_mut().find(|item| item.key == entry.key) {
            *existing = entry;
        } else {
            self.entries.push(entry);
        }
        self.entries.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(())
    }
}

/// A deterministic directory-weight split for one repository.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoSplitPlan {
    pub tenant_id: String,
    pub repository_uri: String,
    /// Directory prefix to estimated work weight.  A flat map is sufficient
    /// for planning; the worker owns the later partition admission.
    pub directory_weights: BTreeMap<String, u64>,
}

impl RepoSplitPlan {
    pub fn validate(&self) -> Result<(), HydrationModelError> {
        require_text("tenant_id", &self.tenant_id)?;
        require_text("repository_uri", &self.repository_uri)?;
        if self
            .directory_weights
            .keys()
            .any(|path| path.trim().is_empty())
        {
            return Err(HydrationModelError::EmptyIdentifier("directory"));
        }
        Ok(())
    }
}

/// Classification used by coverage accounting for source and non-source files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileClass {
    Source,
    Test,
    Configuration,
    Documentation,
    Generated,
    Binary,
    Unknown,
}

/// Counts observed during one tenant's baseline scan.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageReport {
    pub tenant_id: String,
    pub files_by_class: BTreeMap<FileClass, u64>,
    pub expected_sources: u64,
    pub observed_sources: u64,
    pub changed_sources: u64,
}

impl CoverageReport {
    pub fn validate(&self) -> Result<(), HydrationModelError> {
        require_text("tenant_id", &self.tenant_id)?;
        if self.observed_sources > self.expected_sources {
            return Err(HydrationModelError::ObservedExceedsExpected);
        }
        if self.changed_sources > self.observed_sources {
            return Err(HydrationModelError::ChangedExceedsObserved);
        }
        Ok(())
    }
}

/// The bounded stages that a hydration work item can report.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagedStage {
    Fetch,
    Parse,
    Enrich,
    Write,
    Reason,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HydrationModelError {
    EmptyIdentifier(&'static str),
    NonPositiveBound(&'static str),
    EmptyStages,
    DuplicateManifestKey,
    ObservedExceedsExpected,
    ChangedExceedsObserved,
}

impl fmt::Display for HydrationModelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyIdentifier(field) => write!(f, "HYDRATION_ID_EMPTY: {field}"),
            Self::NonPositiveBound(field) => write!(f, "HYDRATION_BOUND_INVALID: {field}"),
            Self::EmptyStages => write!(f, "HYDRATION_STAGES_EMPTY"),
            Self::DuplicateManifestKey => write!(f, "HYDRATION_MANIFEST_DUPLICATE_KEY"),
            Self::ObservedExceedsExpected => {
                write!(f, "HYDRATION_COVERAGE_OBSERVED_EXCEEDS_EXPECTED")
            }
            Self::ChangedExceedsObserved => {
                write!(f, "HYDRATION_COVERAGE_CHANGED_EXCEEDS_OBSERVED")
            }
        }
    }
}

impl std::error::Error for HydrationModelError {}

fn require_text(field: &'static str, value: &str) -> Result<(), HydrationModelError> {
    if value.trim().is_empty() {
        Err(HydrationModelError::EmptyIdentifier(field))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(uri: &str) -> DeltaManifestKey {
        DeltaManifestKey {
            graph: "workspace".into(),
            category: "source".into(),
            uri: uri.into(),
        }
    }

    // spec: EG-REPO-INGEST-R650.1
    #[test]
    fn model_types_round_trip_with_stable_enum_names() {
        let plan = BaselinePlan {
            tenant_id: "tenant-a".into(),
            scope: "workspace".into(),
            max_work_items: 8,
            stages: vec![StagedStage::Fetch, StagedStage::Parse, StagedStage::Write],
        };
        plan.validate().unwrap();
        let mut manifest = DeltaManifest {
            tenant_id: plan.tenant_id.clone(),
            entries: Vec::new(),
        };
        manifest
            .record(DeltaManifestEntry {
                key: key("repo://b"),
                content_hash: "sha256:b".into(),
            })
            .unwrap();
        manifest
            .record(DeltaManifestEntry {
                key: key("repo://a"),
                content_hash: "sha256:a".into(),
            })
            .unwrap();
        manifest.validate().unwrap();
        let split = RepoSplitPlan {
            tenant_id: plan.tenant_id.clone(),
            repository_uri: "repo://a".into(),
            directory_weights: BTreeMap::from([(String::from("src"), 5)]),
        };
        split.validate().unwrap();
        let coverage = CoverageReport {
            tenant_id: plan.tenant_id,
            files_by_class: BTreeMap::from([(FileClass::Source, 1), (FileClass::Test, 1)]),
            expected_sources: 2,
            observed_sources: 1,
            changed_sources: 1,
        };
        coverage.validate().unwrap();

        let encoded = serde_json::to_string(&(plan, manifest, split, coverage)).unwrap();
        assert!(encoded.contains("\"fetch\""));
        assert!(encoded.contains("\"source\""));
        let _: (BaselinePlan, DeltaManifest, RepoSplitPlan, CoverageReport) =
            serde_json::from_str(&encoded).unwrap();
    }

    // spec: EG-REPO-INGEST-R650.1
    #[test]
    fn invalid_identity_bounds_and_coverage_are_refused() {
        let bad_plan = BaselinePlan {
            tenant_id: " ".into(),
            scope: "workspace".into(),
            max_work_items: 1,
            stages: vec![StagedStage::Fetch],
        };
        assert_eq!(
            bad_plan.validate().unwrap_err(),
            HydrationModelError::EmptyIdentifier("tenant_id")
        );

        let mut duplicate = DeltaManifest {
            tenant_id: "tenant-a".into(),
            entries: vec![DeltaManifestEntry {
                key: key("repo://a"),
                content_hash: "sha256:a".into(),
            }],
        };
        duplicate.entries.push(DeltaManifestEntry {
            key: key("repo://a"),
            content_hash: "sha256:b".into(),
        });
        assert_eq!(
            duplicate.validate().unwrap_err(),
            HydrationModelError::DuplicateManifestKey
        );

        let coverage = CoverageReport {
            tenant_id: "tenant-a".into(),
            files_by_class: BTreeMap::new(),
            expected_sources: 1,
            observed_sources: 2,
            changed_sources: 0,
        };
        assert_eq!(
            coverage.validate().unwrap_err(),
            HydrationModelError::ObservedExceedsExpected
        );
    }
}
