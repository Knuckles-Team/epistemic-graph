//! Agent knowledge scopes (MEDULLA-FEDERATED-QUERY-R704.1, R704.2).
//!
//! A scope narrows what an agent's retrieval may read: named knowledge sets,
//! containers, source kinds and a time floor. It is a prefilter applied before
//! candidate truncation, so recall inside the scope is not lost to candidates
//! outside it. A scope only narrows: an empty scope is the caller's access
//! alone.

use serde::{Deserialize, Serialize};

/// An agent's knowledge scope (MEDULLA-FEDERATED-QUERY-R704.1). Empty lists do not
/// constrain that dimension.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeScope {
    pub agent_ref: String,
    #[serde(default)]
    pub knowledge_sets: Vec<String>,
    #[serde(default)]
    pub container_refs: Vec<String>,
    #[serde(default)]
    pub source_kinds: Vec<String>,
    /// Unix seconds; older candidates are out of scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_floor: Option<u64>,
}

/// What the scope tests on one candidate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedCandidate {
    pub id: String,
    pub knowledge_set: Option<String>,
    pub container_ref: Option<String>,
    pub source_kind: String,
    pub timestamp: Option<u64>,
}

fn admits(allowed: &[String], value: Option<&str>) -> bool {
    allowed.is_empty() || value.is_some_and(|v| allowed.iter().any(|a| a == v))
}

impl KnowledgeScope {
    /// Whether the scope places no constraint.
    pub fn is_empty(&self) -> bool {
        self.knowledge_sets.is_empty()
            && self.container_refs.is_empty()
            && self.source_kinds.is_empty()
            && self.time_floor.is_none()
    }

    /// Whether `candidate` lies inside the scope.
    pub fn contains(&self, candidate: &ScopedCandidate) -> bool {
        admits(&self.knowledge_sets, candidate.knowledge_set.as_deref())
            && admits(&self.container_refs, candidate.container_ref.as_deref())
            && admits(&self.source_kinds, Some(candidate.source_kind.as_str()))
            && self
                .time_floor
                .is_none_or(|floor| candidate.timestamp.is_some_and(|t| t >= floor))
    }
}

/// Keep the top `k` scored candidates inside `scope` (MEDULLA-FEDERATED-QUERY-R704.2).
/// `candidates` are what the caller may already read; the scope filters them
/// before truncation, ties by id.
pub fn scoped_top_k(
    scope: &KnowledgeScope,
    candidates: Vec<(ScopedCandidate, f64)>,
    k: usize,
) -> Vec<(ScopedCandidate, f64)> {
    let mut inside: Vec<_> = candidates
        .into_iter()
        .filter(|(c, _)| scope.contains(c))
        .collect();
    inside.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.id.cmp(&b.0.id)));
    inside.truncate(k);
    inside
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> KnowledgeScope {
        KnowledgeScope {
            agent_ref: "agent:support".into(),
            knowledge_sets: vec!["ks:runbooks".into()],
            container_refs: vec![],
            source_kinds: vec!["confluence".into()],
            time_floor: Some(1_000),
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R704.1
    #[test]
    fn scope_round_trips() {
        let s = scope();
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(serde_json::from_str::<KnowledgeScope>(&json).unwrap(), s);
        let minimal: KnowledgeScope = serde_json::from_str(r#"{"agent_ref":"a"}"#).unwrap();
        assert!(minimal.is_empty());
        assert!(
            serde_json::from_str::<KnowledgeScope>(r#"{"agent_ref":"a","widen":true}"#).is_err()
        );
    }

    fn candidate(id: &str, set: &str, kind: &str, ts: u64) -> ScopedCandidate {
        ScopedCandidate {
            id: id.into(),
            knowledge_set: Some(set.into()),
            container_ref: None,
            source_kind: kind.into(),
            timestamp: Some(ts),
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R704.2
    #[test]
    fn scope_prefilters_before_truncation() {
        // The three best-scoring candidates are all out of scope; post-filtering a
        // top-3 would return nothing, prefiltering keeps both in-scope hits.
        let candidates = vec![
            (candidate("x1", "ks:hr", "confluence", 2_000), 0.99),
            (candidate("x2", "ks:runbooks", "slack", 2_000), 0.98),
            (candidate("x3", "ks:runbooks", "confluence", 10), 0.97),
            (candidate("in1", "ks:runbooks", "confluence", 2_000), 0.50),
            (candidate("in2", "ks:runbooks", "confluence", 3_000), 0.40),
        ];
        let kept: Vec<_> = scoped_top_k(&scope(), candidates.clone(), 3)
            .into_iter()
            .map(|(c, _)| c.id)
            .collect();
        assert_eq!(kept, vec!["in1", "in2"]);
        // An empty scope is the caller's access alone.
        let open = KnowledgeScope {
            agent_ref: "a".into(),
            ..Default::default()
        };
        assert_eq!(scoped_top_k(&open, candidates, 3).len(), 3);
    }
}
