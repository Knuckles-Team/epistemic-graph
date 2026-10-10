//! EG-DECISION-ENGINE-R116.1: the one typed gate a consumer's call path must
//! pass through before reaching a decision method -- the published,
//! versioned client contract -- refusing any route that bypasses it (a
//! bespoke in-process facade or a private transport), so no second path to
//! EG's decision ladder can exist beside the published contract.

/// How a consumer proposes to reach an EG decision method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsumerCallPath {
    /// The published, versioned client contract (`epistemic_graph.client`),
    /// the only route a consumer may use.
    Contract { contract_version: String },
    /// A bespoke in-process facade, a private transport, or a direct engine
    /// import bypassing the contract -- always refused.
    BespokeFacade { facade_name: String },
}

/// Why a consumer's call path was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallPathRefusal {
    pub reason: String,
}

/// Admit `path` only when it names the published contract with a non-empty
/// version; refuse any bespoke facade or private transport path
/// (EG-DECISION-ENGINE-R116).
pub fn admit_call_path(path: &ConsumerCallPath) -> Result<(), CallPathRefusal> {
    match path {
        ConsumerCallPath::Contract { contract_version } if !contract_version.is_empty() => Ok(()),
        ConsumerCallPath::Contract { .. } => Err(CallPathRefusal {
            reason: "contract call path must name a contract_version".to_string(),
        }),
        ConsumerCallPath::BespokeFacade { facade_name } => Err(CallPathRefusal {
            reason: format!(
                "bespoke facade '{facade_name}' bypasses the published client contract; use epistemic_graph.client instead"
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-DECISION-ENGINE-R116.1
    #[test]
    fn the_published_contract_path_is_admitted() {
        let path = ConsumerCallPath::Contract {
            contract_version: "v1".to_string(),
        };
        assert!(admit_call_path(&path).is_ok());
    }

    // spec: EG-DECISION-ENGINE-R116.1
    #[test]
    fn a_bespoke_facade_is_always_refused() {
        let path = ConsumerCallPath::BespokeFacade {
            facade_name: "InProcessEngine".to_string(),
        };
        let refusal = admit_call_path(&path).unwrap_err();
        assert!(refusal.reason.contains("bespoke facade"));
    }

    // spec: EG-DECISION-ENGINE-R116.1
    #[test]
    fn an_empty_contract_version_is_refused() {
        let path = ConsumerCallPath::Contract {
            contract_version: String::new(),
        };
        assert!(admit_call_path(&path).is_err());
    }
}
