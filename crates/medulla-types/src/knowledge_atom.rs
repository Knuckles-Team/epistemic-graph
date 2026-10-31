//! Typed knowledge atoms with mandatory provenance (MEDULLA-INGEST-WORKER-R003.1).
//!
//! Decomposition, selection and storage are later rows (R003.2 to R003.6).

use serde::{Deserialize, Serialize};

/// The closed set of atom kinds. An unknown kind fails deserialization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum AtomKind {
    Fact,
    Event,
    Preference,
    Procedure,
}

/// One typed unit of knowledge decomposed from a document or interaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KnowledgeAtom {
    pub atom_id: String,
    pub kind: AtomKind,
    pub text: String,
    pub provenance_key: String,
    pub tenant: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // spec: MEDULLA-INGEST-WORKER-R003.1
    #[test]
    fn atom_round_trips_and_rejects_unknown_kind() {
        let atom = KnowledgeAtom {
            atom_id: "atom-1".into(),
            kind: AtomKind::Procedure,
            text: "Restart the worker after upgrade.".into(),
            provenance_key: "doc:runbook#p3".into(),
            tenant: "t1".into(),
        };
        let encoded = serde_json::to_value(&atom).unwrap();
        assert_eq!(encoded["kind"], "procedure");
        assert_eq!(
            serde_json::from_value::<KnowledgeAtom>(encoded.clone()).unwrap(),
            atom
        );
        for kind in ["fact", "event", "preference", "procedure"] {
            let mut value = encoded.clone();
            value["kind"] = json!(kind);
            assert!(serde_json::from_value::<KnowledgeAtom>(value).is_ok());
        }
        let mut unknown = encoded;
        unknown["kind"] = json!("opinion");
        assert!(serde_json::from_value::<KnowledgeAtom>(unknown).is_err());
    }
}
