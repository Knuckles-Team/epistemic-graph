//! Hierarchical concept-ID grammar and path-consistency contract
//! (MEDULLA-DURABLE-KERNEL-R094.1).
//!
//! A concept ID is `<ROOT>.<segment>(.<segment>)*`: an uppercase root namespace
//! (`MEDULLA-KG`) followed by one to seven lowercase kebab-case segments
//! (`memory.graph-guided-paging`), at most 256 bytes. A [`ConceptPathSet`] holds
//! each path once and names the repository that first claimed it; a second
//! repository claiming the same path is a collision. Served admission is
//! MEDULLA-DURABLE-KERNEL-R094.2.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

pub const MAX_CONCEPT_ID_BYTES: usize = 256;
pub const MAX_CONCEPT_SEGMENTS: usize = 7;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ConceptId(String);

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConceptIdError {
    #[error("concept ID exceeds {MAX_CONCEPT_ID_BYTES} bytes")]
    TooLong,
    #[error("concept ID root {0:?} is not an uppercase namespace")]
    MalformedRoot(String),
    #[error("concept ID needs between 1 and {MAX_CONCEPT_SEGMENTS} path segments")]
    SegmentCount,
    #[error("concept ID segment {0:?} is not lowercase kebab-case")]
    MalformedSegment(String),
    #[error("concept path {path} is owned by {owner}, not {claimant}")]
    PathCollision {
        path: String,
        owner: String,
        claimant: String,
    },
}

fn kebab(part: &str, alpha: fn(u8) -> bool) -> bool {
    !part.is_empty()
        && part
            .split('-')
            .all(|word| !word.is_empty() && word.bytes().all(|b| alpha(b) || b.is_ascii_digit()))
}

impl ConceptId {
    pub fn parse(raw: &str) -> Result<Self, ConceptIdError> {
        if raw.len() > MAX_CONCEPT_ID_BYTES {
            return Err(ConceptIdError::TooLong);
        }
        let mut parts = raw.split('.');
        let root = parts.next().unwrap_or_default();
        if !kebab(root, |b| b.is_ascii_uppercase()) || !root.as_bytes()[0].is_ascii_uppercase() {
            return Err(ConceptIdError::MalformedRoot(root.to_string()));
        }
        let segments: Vec<&str> = parts.collect();
        if segments.is_empty() || segments.len() > MAX_CONCEPT_SEGMENTS {
            return Err(ConceptIdError::SegmentCount);
        }
        if let Some(bad) = segments
            .iter()
            .find(|segment| !kebab(segment, |b| b.is_ascii_lowercase()))
        {
            return Err(ConceptIdError::MalformedSegment((*bad).to_string()));
        }
        Ok(Self(raw.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The root namespace followed by each segment.
    pub fn path(&self) -> impl Iterator<Item = &str> {
        self.0.split('.')
    }

    /// The enclosing concept, or `None` for a single-segment ID.
    pub fn parent(&self) -> Option<ConceptId> {
        let (parent, _) = self.0.rsplit_once('.')?;
        parent.contains('.').then(|| Self(parent.to_string()))
    }
}

impl TryFrom<String> for ConceptId {
    type Error = ConceptIdError;
    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::parse(&raw)
    }
}

impl From<ConceptId> for String {
    fn from(id: ConceptId) -> Self {
        id.0
    }
}

impl fmt::Display for ConceptId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Each concept path, held once with the repository that claimed it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConceptPathSet {
    owners: BTreeMap<ConceptId, String>,
}

impl ConceptPathSet {
    /// Claim `id` for `repository`. Re-claiming by the same repository is a no-op;
    /// a claim by any other repository is a collision and changes nothing.
    pub fn claim(&mut self, id: ConceptId, repository: &str) -> Result<(), ConceptIdError> {
        match self.owners.get(&id) {
            Some(owner) if owner == repository => Ok(()),
            Some(owner) => Err(ConceptIdError::PathCollision {
                path: id.to_string(),
                owner: owner.clone(),
                claimant: repository.to_string(),
            }),
            None => {
                self.owners.insert(id, repository.to_string());
                Ok(())
            }
        }
    }

    pub fn owner(&self, id: &ConceptId) -> Option<&str> {
        self.owners.get(id).map(String::as_str)
    }

    pub fn len(&self) -> usize {
        self.owners.len()
    }

    pub fn is_empty(&self) -> bool {
        self.owners.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-DURABLE-KERNEL-R094.1
    #[test]
    fn valid_and_invalid_concept_id_formats() {
        for good in [
            "MEDULLA-KG.memory",
            "MEDULLA-KG.memory.graph-guided-paging",
            "AU2.harness.run-loop.step-3",
        ] {
            let id = ConceptId::parse(good).unwrap();
            assert_eq!(id.as_str(), good);
            let json = serde_json::to_string(&id).unwrap();
            assert_eq!(serde_json::from_str::<ConceptId>(&json).unwrap(), id);
        }
        let id = ConceptId::parse("MEDULLA-KG.memory.graph-guided-paging").unwrap();
        assert_eq!(
            id.path().collect::<Vec<_>>(),
            ["MEDULLA-KG", "memory", "graph-guided-paging"]
        );
        assert_eq!(id.parent().unwrap().as_str(), "MEDULLA-KG.memory");
        assert_eq!(id.parent().unwrap().parent(), None);

        type Expect = fn(&ConceptIdError) -> bool;
        let cases: [(&str, Expect); 10] = [
            ("", |e| matches!(e, ConceptIdError::MalformedRoot(_))),
            ("medulla-kg.memory", |e| {
                matches!(e, ConceptIdError::MalformedRoot(_))
            }),
            ("1EG.memory", |e| {
                matches!(e, ConceptIdError::MalformedRoot(_))
            }),
            ("EG--KG.memory", |e| {
                matches!(e, ConceptIdError::MalformedRoot(_))
            }),
            ("MEDULLA-KG", |e| matches!(e, ConceptIdError::SegmentCount)),
            ("EG.a.b.c.d.e.f.g.h", |e| {
                matches!(e, ConceptIdError::SegmentCount)
            }),
            ("MEDULLA-KG.Memory", |e| {
                matches!(e, ConceptIdError::MalformedSegment(_))
            }),
            ("MEDULLA-KG.memory..x", |e| {
                matches!(e, ConceptIdError::MalformedSegment(_))
            }),
            ("MEDULLA-KG.graph_paging", |e| {
                matches!(e, ConceptIdError::MalformedSegment(_))
            }),
            ("MEDULLA-KG.paging-", |e| {
                matches!(e, ConceptIdError::MalformedSegment(_))
            }),
        ];
        for (bad, expected) in cases {
            let err = ConceptId::parse(bad).unwrap_err();
            assert!(expected(&err), "{bad:?} gave {err:?}");
        }
        let long = format!("EG.{}", "a".repeat(MAX_CONCEPT_ID_BYTES));
        assert_eq!(ConceptId::parse(&long), Err(ConceptIdError::TooLong));
        assert!(serde_json::from_str::<ConceptId>(r#""medulla.bad""#).is_err());
    }

    // spec: MEDULLA-DURABLE-KERNEL-R094.1
    #[test]
    fn path_collision_across_repositories_is_detected() {
        let mut paths = ConceptPathSet::default();
        let id = ConceptId::parse("MEDULLA-KG.memory.graph-guided-paging").unwrap();
        paths.claim(id.clone(), "medulla").unwrap();
        paths.claim(id.clone(), "medulla").unwrap();
        assert_eq!(paths.len(), 1);

        let err = paths.claim(id.clone(), "meristem").unwrap_err();
        assert_eq!(
            err,
            ConceptIdError::PathCollision {
                path: id.to_string(),
                owner: "medulla".into(),
                claimant: "meristem".into(),
            }
        );
        assert_eq!(paths.owner(&id), Some("medulla"));
        assert_eq!(paths.len(), 1);

        paths
            .claim(ConceptId::parse("MEDULLA-KG.memory").unwrap(), "meristem")
            .unwrap();
        assert_eq!(paths.len(), 2);
    }
}
