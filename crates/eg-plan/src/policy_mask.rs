//! Column masking and row-filter policies (MEDULLA-FEDERATED-QUERY-R706.1).

use serde::{Deserialize, Serialize};

/// How a masked column value is rewritten. Closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskMethod {
    Null,
    Hash,
    Partial,
    Redact,
}

/// A masking policy on every column carrying `tag` (MEDULLA-FEDERATED-QUERY-R706.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaskPolicy {
    pub tag: String,
    #[serde(default)]
    pub principals_exempt: Vec<String>,
    pub method: MaskMethod,
}

/// A row filter on every table carrying `tag` (MEDULLA-FEDERATED-QUERY-R706.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowFilterPolicy {
    pub tag: String,
    /// The predicate rows must satisfy, in the shared predicate syntax.
    pub predicate: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-FEDERATED-QUERY-R706.1
    #[test]
    fn policies_round_trip() {
        for method in [
            MaskMethod::Null,
            MaskMethod::Hash,
            MaskMethod::Partial,
            MaskMethod::Redact,
        ] {
            let mask = MaskPolicy {
                tag: "pii.email".into(),
                principals_exempt: vec!["role:dpo".into()],
                method,
            };
            let json = serde_json::to_string(&mask).unwrap();
            assert_eq!(serde_json::from_str::<MaskPolicy>(&json).unwrap(), mask);
        }
        assert!(serde_json::from_str::<MaskMethod>("\"encrypt\"").is_err());
        let filter = RowFilterPolicy {
            tag: "region.eu".into(),
            predicate: "region = 'eu'".into(),
        };
        let json = serde_json::to_string(&filter).unwrap();
        assert_eq!(
            serde_json::from_str::<RowFilterPolicy>(&json).unwrap(),
            filter
        );
        assert!(serde_json::from_str::<RowFilterPolicy>(r#"{"tag":"t"}"#).is_err());
    }
}
