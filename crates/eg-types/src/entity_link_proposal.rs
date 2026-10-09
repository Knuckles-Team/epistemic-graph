//! Typed model for cross-application entity-resolution proposals
//! (EG-UNIFIED-DATA-PLANE-R030): a candidate link between an entity in one
//! attached application and an entity in another, classified by a
//! Fellegi-Sunter-style match score, asserted as `owl:sameAs` only once
//! approved. This is the typed-model slice (`.1`): the candidate/proposal
//! shape and the refusal to assert a link before approval. The blocking,
//! field-similarity scoring, and the real Fellegi-Sunter model are later
//! children.

use serde::{Deserialize, Serialize};

/// The attached application an entity reference names. Closed per
/// R030's defined application set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttachedApplication {
    Gramps,
    Immich,
    Twenty,
    Firefly,
}

/// One entity reference: which application and its local entity id.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityRef {
    pub application: AttachedApplication,
    pub entity_id: String,
}

/// A Fellegi-Sunter-style classification of a candidate pair.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchClass {
    Match,
    NonMatch,
    Ambiguous,
}

/// One cross-application entity-link candidate: never itself an assertion,
/// only ever a proposal until a human approves it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityLinkProposal {
    pub left: EntityRef,
    pub right: EntityRef,
    pub match_class: MatchClass,
    /// Scaled 0-1000 (fixed point; avoids floating-point `Eq`/`Ord` issues
    /// this pure-data type needs for its tests and any future dedup).
    pub match_score_milli: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
}

/// A proposal cannot be asserted as `owl:sameAs` for the stated reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CannotAssert {
    NotApproved,
    NotAMatch,
}

impl std::fmt::Display for CannotAssert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotApproved => write!(f, "proposal has no recorded approval"),
            Self::NotAMatch => write!(f, "proposal's match class is not Match"),
        }
    }
}

impl std::error::Error for CannotAssert {}

impl EntityLinkProposal {
    /// Confirm this proposal may be asserted as `owl:sameAs`: classified
    /// `Match` AND an explicit, non-blank approver recorded. Refuses an
    /// `Ambiguous` or `NonMatch` candidate even if marked approved (an
    /// approval cannot override the classifier), and refuses an unapproved
    /// `Match` (a good score is never itself the approval).
    pub fn assert_same_as(&self) -> Result<(&EntityRef, &EntityRef), CannotAssert> {
        if self.match_class != MatchClass::Match {
            return Err(CannotAssert::NotAMatch);
        }
        match &self.approved_by {
            Some(approver) if !approver.trim().is_empty() => Ok((&self.left, &self.right)),
            _ => Err(CannotAssert::NotApproved),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entity(app: AttachedApplication, id: &str) -> EntityRef {
        EntityRef {
            application: app,
            entity_id: id.to_string(),
        }
    }

    fn proposal(class: MatchClass, approved: Option<&str>) -> EntityLinkProposal {
        EntityLinkProposal {
            left: entity(AttachedApplication::Gramps, "p1"),
            right: entity(AttachedApplication::Immich, "person-7"),
            match_class: class,
            match_score_milli: 900,
            approved_by: approved.map(str::to_string),
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R030.1
    #[test]
    fn approved_match_asserts() {
        let proposal = proposal(MatchClass::Match, Some("alice"));
        assert!(proposal.assert_same_as().is_ok());
    }

    // spec: EG-UNIFIED-DATA-PLANE-R030.1
    #[test]
    fn unapproved_match_is_refused() {
        let proposal = proposal(MatchClass::Match, None);
        assert_eq!(proposal.assert_same_as(), Err(CannotAssert::NotApproved));
    }

    // spec: EG-UNIFIED-DATA-PLANE-R030.1
    #[test]
    fn approved_non_match_is_refused() {
        let proposal = proposal(MatchClass::NonMatch, Some("alice"));
        assert_eq!(proposal.assert_same_as(), Err(CannotAssert::NotAMatch));
    }

    #[test]
    fn approved_ambiguous_is_refused() {
        let proposal = proposal(MatchClass::Ambiguous, Some("alice"));
        assert_eq!(proposal.assert_same_as(), Err(CannotAssert::NotAMatch));
    }

    #[test]
    fn blank_approver_is_refused() {
        let proposal = proposal(MatchClass::Match, Some("   "));
        assert_eq!(proposal.assert_same_as(), Err(CannotAssert::NotApproved));
    }

    #[test]
    fn proposal_serializes_round_trip() {
        let proposal = proposal(MatchClass::Match, Some("alice"));
        let encoded = serde_json::to_string(&proposal).unwrap();
        let decoded: EntityLinkProposal = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, proposal);
    }
}
