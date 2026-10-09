//! EG-UNIFIED-DATA-PLANE-R005 — compiling approved relational mappings to named R2RML virtual
//! graphs. This is the `.1` typed-model slice: the mapping's approval-state machine, and the
//! refusal of an unapproved or model-only mapping's query access. Deterministic mapping
//! proposal and the actual R2RML compilation are later children.

use std::fmt;

/// How a mapping reached its current state. Only `Approved` may be queried: a deterministic
/// proposal and a model suggestion are both proposals only, never served.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MappingApprovalState {
    Proposed,
    ModelSuggested,
    Approved,
}

impl fmt::Display for MappingApprovalState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Proposed => "proposed",
            Self::ModelSuggested => "model-suggested",
            Self::Approved => "approved",
        };
        write!(f, "{label}")
    }
}

/// A named, versioned R2RML virtual graph mapping, queryable by name only once approved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VirtualGraphMapping {
    pub name: String,
    pub approval_state: MappingApprovalState,
    pub r2rml: String,
}

/// A mapping was queried before it was approved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MappingNotApproved {
    pub name: String,
    pub state: MappingApprovalState,
}

impl fmt::Display for MappingNotApproved {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "virtual graph mapping {:?} is {}, not approved, and cannot be queried",
            self.name, self.state
        )
    }
}

impl std::error::Error for MappingNotApproved {}

impl VirtualGraphMapping {
    /// Returns this mapping's queryable name, refusing unless `approval_state` is `Approved`.
    pub fn queryable_name(&self) -> Result<&str, MappingNotApproved> {
        match self.approval_state {
            MappingApprovalState::Approved => Ok(self.name.as_str()),
            other => Err(MappingNotApproved {
                name: self.name.clone(),
                state: other,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mapping(state: MappingApprovalState) -> VirtualGraphMapping {
        VirtualGraphMapping {
            name: "gramps_people".to_string(),
            approval_state: state,
            r2rml: "<#TriplesMap1> ...".to_string(),
        }
    }

    #[test]
    fn an_approved_mapping_is_queryable_by_name() {
        let m = mapping(MappingApprovalState::Approved);
        assert_eq!(m.queryable_name().unwrap(), "gramps_people");
    }

    #[test]
    fn a_proposed_mapping_is_refused() {
        let m = mapping(MappingApprovalState::Proposed);
        let err = m.queryable_name().unwrap_err();
        assert_eq!(err.state, MappingApprovalState::Proposed);
        assert!(err.to_string().contains("not approved"));
    }

    #[test]
    fn a_model_suggested_mapping_is_refused() {
        let m = mapping(MappingApprovalState::ModelSuggested);
        let err = m.queryable_name().unwrap_err();
        assert_eq!(err.state, MappingApprovalState::ModelSuggested);
        assert!(err.to_string().contains("model-suggested"));
    }
}
