//! A typed, versioned domain vocabulary pack (EG-TYPED-PACKS-R097.1).
//!
//! Human-resources, legal, medical and government domain vocabularies —
//! classes, enumerations, properties and their alignment to upper ontologies —
//! publish through this typed model instead of each agent or connector
//! carrying its own model classes. This is the producer-first slice of
//! `EG-TYPED-PACKS-R097` (split per `SPEC-SIZING-AND-DEPENDENCIES.md` §5): the
//! typed model plus validation and a round-trip test for one domain (HR).
//! Wiring this pack through the `connector_pack` import boundary, the
//! remaining three domains, and retiring Agent Utilities' `domains/*` and
//! `models/domains` modules are later child requirements.

use std::collections::BTreeSet;

/// The four domains this requirement names. Closed: a fifth domain is a new
/// variant and a new child requirement, not a free-form string.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum DomainPackKind {
    Hr,
    Legal,
    Medical,
    Government,
}

/// One `owl:Class` in a domain pack, optionally aligned to an upper-ontology
/// class (for example a BFO IRI) via `rdfs:subClassOf`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainClass {
    pub local_name: String,
    pub label: String,
    pub aligns_to_upper_ontology: Option<String>,
}

/// One closed enumeration of named members (for example EEO-1 job categories).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainEnumeration {
    pub local_name: String,
    pub label: String,
    pub members: Vec<String>,
}

/// One `owl:DatatypeProperty` or `owl:ObjectProperty` scoped to a declared
/// domain class.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainProperty {
    pub local_name: String,
    pub domain_class: String,
    pub range: String,
}

/// A rejection reason for an invalid domain pack. Carried through the caller's
/// error channel, never added as a field on a kernel row (consistent with the
/// federated-query spec's rejection-reason rule).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DomainPackError {
    #[error("domain pack {0:?} declares no classes")]
    NoClasses(DomainPackKind),
    #[error("domain pack {kind:?} has a duplicate class local_name {local_name:?}")]
    DuplicateClass {
        kind: DomainPackKind,
        local_name: String,
    },
    #[error("domain pack {kind:?} property {property:?} has an undeclared domain_class {domain_class:?}")]
    UndeclaredPropertyDomain {
        kind: DomainPackKind,
        property: String,
        domain_class: String,
    },
}

/// One typed, versioned domain vocabulary pack.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainPack {
    pub kind: DomainPackKind,
    pub version: u32,
    pub classes: Vec<DomainClass>,
    pub enumerations: Vec<DomainEnumeration>,
    pub properties: Vec<DomainProperty>,
}

impl DomainPack {
    /// Validate the pack's internal consistency: at least one class, no
    /// duplicate class names, and every property's `domain_class` names a
    /// class the pack actually declares.
    pub fn validate(&self) -> Result<(), DomainPackError> {
        if self.classes.is_empty() {
            return Err(DomainPackError::NoClasses(self.kind));
        }

        let mut seen = BTreeSet::new();
        for class in &self.classes {
            if !seen.insert(class.local_name.as_str()) {
                return Err(DomainPackError::DuplicateClass {
                    kind: self.kind,
                    local_name: class.local_name.clone(),
                });
            }
        }

        for prop in &self.properties {
            if !seen.contains(prop.domain_class.as_str()) {
                return Err(DomainPackError::UndeclaredPropertyDomain {
                    kind: self.kind,
                    property: prop.local_name.clone(),
                    domain_class: prop.domain_class.clone(),
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small, real fixture drawn from `crates/eg-core/ontology/hr-v1.ttl`:
    /// `EEOCategory` aligned to the upper-ontology class `bfo:0000031`, plus
    /// `W2Employee`, and one property scoped to `EEOCategory`.
    fn hr_fixture() -> DomainPack {
        DomainPack {
            kind: DomainPackKind::Hr,
            version: 1,
            classes: vec![
                DomainClass {
                    local_name: "EEOCategory".to_string(),
                    label: "EEO Category".to_string(),
                    aligns_to_upper_ontology: Some("bfo:0000031".to_string()),
                },
                DomainClass {
                    local_name: "W2Employee".to_string(),
                    label: "W-2 Employee".to_string(),
                    aligns_to_upper_ontology: None,
                },
            ],
            enumerations: vec![DomainEnumeration {
                local_name: "EEOJobCategory".to_string(),
                label: "EEO-1 job category".to_string(),
                members: vec![
                    "OfficialsAndManagers".to_string(),
                    "Professionals".to_string(),
                ],
            }],
            properties: vec![DomainProperty {
                local_name: "accommodationStatus".to_string(),
                domain_class: "EEOCategory".to_string(),
                range: "xsd:string".to_string(),
            }],
        }
    }

    /// EG-TYPED-PACKS-R097.1: a valid HR domain pack passes validation.
    // spec: EG-TYPED-PACKS-R097.1
    #[test]
    fn hr_fixture_validates() {
        hr_fixture().validate().expect("fixture must be valid");
    }

    /// Round-trip test: serialize then deserialize the pack, and confirm the
    /// published classes and enumerations match the source declarations.
    // spec: EG-TYPED-PACKS-R097.1
    #[test]
    fn round_trip_matches_source_declarations() {
        let original = hr_fixture();
        let encoded = serde_json::to_vec(&original).expect("serialize");
        let decoded: DomainPack = serde_json::from_slice(&encoded).expect("deserialize");
        assert_eq!(decoded.classes, original.classes);
        assert_eq!(decoded.enumerations, original.enumerations);
        assert_eq!(decoded, original);
    }

    /// A pack with zero classes is refused rather than silently accepted.
    // spec: EG-TYPED-PACKS-R097.1
    #[test]
    fn empty_classes_is_refused() {
        let mut pack = hr_fixture();
        pack.classes.clear();
        assert_eq!(
            pack.validate(),
            Err(DomainPackError::NoClasses(DomainPackKind::Hr))
        );
    }

    /// A duplicate class local_name is refused.
    #[test]
    fn duplicate_class_is_refused() {
        let mut pack = hr_fixture();
        let dup = pack.classes[0].clone();
        pack.classes.push(dup);
        assert!(matches!(
            pack.validate(),
            Err(DomainPackError::DuplicateClass { .. })
        ));
    }

    /// A property naming an undeclared domain_class is refused.
    #[test]
    fn property_with_undeclared_domain_class_is_refused() {
        let mut pack = hr_fixture();
        pack.properties.push(DomainProperty {
            local_name: "stray".to_string(),
            domain_class: "NotDeclared".to_string(),
            range: "xsd:string".to_string(),
        });
        assert!(matches!(
            pack.validate(),
            Err(DomainPackError::UndeclaredPropertyDomain { .. })
        ));
    }
}
