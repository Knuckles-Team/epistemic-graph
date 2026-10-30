//! Plan observations and evolution candidates for the federated optimizer
//! (EG-FEDERATED-QUERY-R091.1).
//!
//! Every executed plan records a [`PlanObservation`] keyed by its query shape; the
//! store is bounded per shape and per tenant so a hostile or busy tenant cannot grow
//! it without limit. A [`PlanCandidate`] proposes a change against a baseline plan.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Observations kept per shape.
pub const MAX_OBSERVATIONS_PER_SHAPE: usize = 4_096;
/// Shapes kept per tenant.
pub const MAX_SHAPES_PER_TENANT: usize = 1_024;

/// What one execution of a plan looked like.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanObservation {
    pub shape_digest: String,
    pub plan_digest: String,
    pub sources: Vec<String>,
    pub pushdowns: Vec<String>,
    pub latency_us: u64,
    pub rows_fetched: u64,
    pub bytes_fetched: u64,
    pub cache_hit: bool,
    pub graph_revision: String,
}

/// The closed set of changes a candidate may propose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanCandidateKind {
    Rewrite,
    PushdownChoice,
    CacheSubgraph,
    MaterializeSubgraph,
    IndexSuggestion,
}

/// A proposed change to a shape's plan, measured against `baseline_digest`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanCandidate {
    pub shape_digest: String,
    pub kind: PlanCandidateKind,
    pub plan_digest: String,
    pub baseline_digest: String,
}

#[derive(Debug, Default)]
struct TenantShapes {
    shapes: HashMap<String, VecDeque<PlanObservation>>,
    /// Shape digests from least to most recently seen.
    recency: VecDeque<String>,
}

/// Bounded, tenant-scoped observation store.
#[derive(Debug, Default)]
pub struct ObservationStore {
    tenants: HashMap<String, TenantShapes>,
}

impl ObservationStore {
    /// Record an observation. A full shape drops its oldest observation; a full
    /// tenant evicts its least recently seen shape.
    pub fn record(&mut self, tenant: &str, observation: PlanObservation) {
        let t = self.tenants.entry(tenant.to_string()).or_default();
        let shape = observation.shape_digest.clone();
        if let Some(pos) = t.recency.iter().position(|s| *s == shape) {
            t.recency.remove(pos);
        } else if t.shapes.len() >= MAX_SHAPES_PER_TENANT {
            if let Some(stale) = t.recency.pop_front() {
                t.shapes.remove(&stale);
            }
        }
        t.recency.push_back(shape.clone());
        let q = t.shapes.entry(shape).or_default();
        if q.len() >= MAX_OBSERVATIONS_PER_SHAPE {
            q.pop_front();
        }
        q.push_back(observation);
    }

    pub fn observations(&self, tenant: &str, shape: &str) -> usize {
        self.tenants
            .get(tenant)
            .and_then(|t| t.shapes.get(shape))
            .map_or(0, VecDeque::len)
    }

    pub fn shapes(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.shapes.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn obs(shape: &str, n: u64) -> PlanObservation {
        PlanObservation {
            shape_digest: shape.into(),
            plan_digest: format!("p{n}"),
            sources: vec!["s".into()],
            pushdowns: vec!["filter".into()],
            latency_us: n,
            rows_fetched: 1,
            bytes_fetched: 1,
            cache_hit: false,
            graph_revision: "g1".into(),
        }
    }

    // spec: EG-FEDERATED-QUERY-R091.1
    #[test]
    fn observation_store_bounds() {
        let mut store = ObservationStore::default();
        for n in 0..(MAX_OBSERVATIONS_PER_SHAPE as u64 + 10) {
            store.record("t", obs("hot", n));
        }
        assert_eq!(store.observations("t", "hot"), MAX_OBSERVATIONS_PER_SHAPE);
        for i in 0..MAX_SHAPES_PER_TENANT {
            store.record("t", obs(&format!("s{i}"), 0));
        }
        assert_eq!(store.shapes("t"), MAX_SHAPES_PER_TENANT);
        // `hot` was least recently seen, so it was evicted; the newest survives.
        assert_eq!(store.observations("t", "hot"), 0);
        assert_eq!(store.observations("t", &format!("s{}", MAX_SHAPES_PER_TENANT - 1)), 1);
        // tenants are isolated
        assert_eq!(store.shapes("other"), 0);

        let o = obs("x", 1);
        let back: PlanObservation = serde_json::from_str(&serde_json::to_string(&o).unwrap()).unwrap();
        assert_eq!(back, o);
        let bad = r#"{"shape_digest":"a","kind":"nonsense","plan_digest":"b","baseline_digest":"c"}"#;
        assert!(serde_json::from_str::<PlanCandidate>(bad).is_err());
        let ok = PlanCandidate {
            shape_digest: "a".into(),
            kind: PlanCandidateKind::IndexSuggestion,
            plan_digest: "b".into(),
            baseline_digest: "c".into(),
        };
        assert_eq!(serde_json::from_str::<PlanCandidate>(&serde_json::to_string(&ok).unwrap()).unwrap(), ok);
    }
}
