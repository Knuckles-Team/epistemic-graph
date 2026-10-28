//! Ontology revision diff and change-impact records (EG-TYPED-PACKS-R804.1).
//!
//! Pure data. Computing the diff over canonical axiom sets and the
//! shadow-branch impact pass are later children of `EG-TYPED-PACKS-R804`.

use serde::{Deserialize, Serialize};

/// Most broken items an impact names per category.
pub const MAX_IMPACT_ITEMS: usize = 100;
/// The stable refusal code for a diff across two ontology IRIs.
pub const ONTOLOGY_DIFF_IRI_MISMATCH: &str = "ONTOLOGY_DIFF_IRI_MISMATCH";

/// One ontology revision: the ontology IRI and its revision label.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyRevisionRef {
    pub ontology_iri: String,
    pub revision: String,
}

/// What changed between two revisions of one ontology, by term IRI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyDiff {
    pub from_rev: OntologyRevisionRef,
    pub to_rev: OntologyRevisionRef,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub changed: Vec<String>,
    pub deprecated: Vec<String>,
}

/// A count of broken items and the first ones by id.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ImpactCount {
    pub count: u64,
    /// At most [`MAX_IMPACT_ITEMS`] ids.
    pub first: Vec<String>,
}

/// What activating `to_rev` would break.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChangeImpact {
    pub instances_invalid: ImpactCount,
    pub shapes_broken: ImpactCount,
    pub mappings_broken: ImpactCount,
    pub queries_broken: ImpactCount,
}

/// A record that failed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OntologyDiffError {
    pub code: &'static str,
    pub field: &'static str,
}

impl std::fmt::Display for OntologyDiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.field)
    }
}

impl std::error::Error for OntologyDiffError {}

fn invalid(field: &'static str) -> OntologyDiffError {
    OntologyDiffError {
        code: "INVALID_FIELD",
        field,
    }
}

fn sorted_unique(field: &'static str, terms: &[String]) -> Result<(), OntologyDiffError> {
    if terms.iter().any(|term| term.trim().is_empty())
        || terms.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(invalid(field));
    }
    Ok(())
}

impl OntologyDiff {
    pub fn validate(&self) -> Result<(), OntologyDiffError> {
        if self.from_rev.ontology_iri != self.to_rev.ontology_iri {
            return Err(OntologyDiffError {
                code: ONTOLOGY_DIFF_IRI_MISMATCH,
                field: "to_rev",
            });
        }
        if self.from_rev.ontology_iri.trim().is_empty() {
            return Err(invalid("from_rev"));
        }
        if self.from_rev.revision.trim().is_empty() || self.to_rev.revision.trim().is_empty() {
            return Err(invalid("revision"));
        }
        sorted_unique("added", &self.added)?;
        sorted_unique("removed", &self.removed)?;
        sorted_unique("changed", &self.changed)?;
        sorted_unique("deprecated", &self.deprecated)
    }
}

impl ImpactCount {
    fn validate(&self, field: &'static str) -> Result<(), OntologyDiffError> {
        if self.first.len() > MAX_IMPACT_ITEMS || (self.first.len() as u64) > self.count {
            return Err(invalid(field));
        }
        Ok(())
    }
}

impl ChangeImpact {
    pub fn validate(&self) -> Result<(), OntologyDiffError> {
        self.instances_invalid.validate("instances_invalid")?;
        self.shapes_broken.validate("shapes_broken")?;
        self.mappings_broken.validate("mappings_broken")?;
        self.queries_broken.validate("queries_broken")
    }

    /// Whether activation would break anything.
    pub fn is_breaking(&self) -> bool {
        [
            &self.instances_invalid,
            &self.shapes_broken,
            &self.mappings_broken,
            &self.queries_broken,
        ]
        .iter()
        .any(|impact| impact.count > 0)
    }
}
