//! Stored, versioned queries (MEDULLA-FEDERATED-QUERY-R708.1).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The shortest periodic refresh.
pub const MIN_REFRESH_PERIOD_S: u64 = 60;

pub const STORED_QUERY_INVALID: &str = "STORED_QUERY_INVALID";

/// The language a stored query is written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredQueryLanguage {
    Uql,
    Sql,
    Sparql,
}

/// The declared type of one parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoredParamType {
    String,
    Int,
    Float,
    Bool,
}

/// When a materialized result recomputes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", deny_unknown_fields)]
pub enum RefreshPolicy {
    /// When the dependency tracker marks an input changed.
    OnChange,
    /// Every `seconds`, at least [`MIN_REFRESH_PERIOD_S`].
    Period { seconds: u64 },
}

/// A stored query (MEDULLA-FEDERATED-QUERY-R708.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredQuery {
    pub name: String,
    pub version: u32,
    pub language: StoredQueryLanguage,
    pub text: String,
    pub params: BTreeMap<String, StoredParamType>,
    pub materialize: bool,
    pub refresh: RefreshPolicy,
}

impl StoredQuery {
    /// Refuse an empty name or text, version zero and a period under 60 s.
    pub fn validate(&self) -> Result<(), (&'static str, String)> {
        let fail = |detail: &str| Err((STORED_QUERY_INVALID, detail.to_string()));
        if self.name.trim().is_empty() {
            return fail("name is required");
        }
        if self.text.trim().is_empty() {
            return fail("text is required");
        }
        if self.version == 0 {
            return fail("version starts at 1");
        }
        match self.refresh {
            RefreshPolicy::Period { seconds } if seconds < MIN_REFRESH_PERIOD_S => {
                fail("refresh period is under 60 s")
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(refresh: RefreshPolicy) -> StoredQuery {
        StoredQuery {
            name: "open_incidents".into(),
            version: 1,
            language: StoredQueryLanguage::Uql,
            text: "MATCH (i:Incident) WHERE i.status = $status RETURN i".into(),
            params: BTreeMap::from([("status".to_string(), StoredParamType::String)]),
            materialize: true,
            refresh,
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R708.1
    #[test]
    fn stored_query_round_trips() {
        for refresh in [
            RefreshPolicy::OnChange,
            RefreshPolicy::Period { seconds: 60 },
        ] {
            let q = query(refresh);
            q.validate().unwrap();
            let json = serde_json::to_string(&q).unwrap();
            assert_eq!(serde_json::from_str::<StoredQuery>(&json).unwrap(), q);
        }
        assert!(query(RefreshPolicy::Period { seconds: 59 })
            .validate()
            .is_err());
        assert!(serde_json::from_str::<RefreshPolicy>(r#"{"kind":"hourly"}"#).is_err());
        let mut unnamed = query(RefreshPolicy::OnChange);
        unnamed.name = String::new();
        assert_eq!(unnamed.validate().unwrap_err().0, STORED_QUERY_INVALID);
    }
}
