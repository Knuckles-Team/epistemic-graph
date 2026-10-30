//! Topic-model backends for topic-weighted retrieval fusion
//! (EG-FEDERATED-QUERY-R040.2.1).
//!
//! `TopicBackend` sits beside [`crate::exec::PlanCtx`]: any implementation, however
//! it derives topics, answers with ONE typed [`TopicResult`] so the planner never
//! branches on the backend. Weights, centroids, the corpus revision and the
//! retriever's embedding space travel together so a stale or cross-space result is
//! detectable before it influences ranking.

use eg_types::embedding::EmbeddingSpaceRef;
use serde::{Deserialize, Serialize};

/// One topic's weight for a query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicWeight {
    pub topic_id: String,
    pub weight: f32,
}

/// A backend's answer: topic weights with one centroid per topic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicResult {
    pub weights: Vec<TopicWeight>,
    /// `centroids[i]` is the centroid vector of `weights[i]`.
    pub centroids: Vec<Vec<f32>>,
    pub corpus_revision: String,
    pub space: EmbeddingSpaceRef,
}

impl TopicResult {
    /// Check structure against the retriever's pinned space and corpus revision.
    pub fn validate(
        &self,
        retriever: &EmbeddingSpaceRef,
        corpus_revision: &str,
    ) -> Result<(), String> {
        if self.space.digest != retriever.digest {
            return Err("topic result is from a different embedding space".into());
        }
        if self.corpus_revision != corpus_revision {
            return Err("topic result is for a different corpus revision".into());
        }
        if self.weights.len() != self.centroids.len() {
            return Err("topic result needs one centroid per topic weight".into());
        }
        if self.weights.iter().any(|w| !w.weight.is_finite() || w.weight < 0.0) {
            return Err("topic weights must be finite and non-negative".into());
        }
        if self.centroids.iter().any(|c| c.len() != retriever.dimensions) {
            return Err("topic centroid width differs from the embedding space".into());
        }
        Ok(())
    }
}

/// How topic scores are fused into the ranking.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TopicRankingPolicy {
    /// Fusion weight of the topic component, in `[0, 1]`.
    pub fusion_weight: f32,
    /// Largest number of topics a backend may contribute.
    pub max_topics: usize,
}

impl TopicRankingPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=1.0).contains(&self.fusion_weight) {
            return Err("topic fusion weight must be within [0, 1]".into());
        }
        if self.max_topics == 0 {
            return Err("topic policy needs a non-zero topic bound".into());
        }
        Ok(())
    }
}

/// A source of topic weights and centroids for a query.
pub trait TopicBackend {
    fn topics(&self, query: &str) -> Result<TopicResult, String>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn space() -> EmbeddingSpaceRef {
        EmbeddingSpaceRef::pinned("m", "r1", "mh", "ph", 2, true).unwrap()
    }

    struct Fixed(TopicResult);
    impl TopicBackend for Fixed {
        fn topics(&self, _q: &str) -> Result<TopicResult, String> {
            Ok(self.0.clone())
        }
    }

    /// A second, differently-shaped implementation (computes from the query).
    struct ByLength(EmbeddingSpaceRef);
    impl TopicBackend for ByLength {
        fn topics(&self, q: &str) -> Result<TopicResult, String> {
            Ok(TopicResult {
                weights: vec![TopicWeight { topic_id: "len".into(), weight: q.len() as f32 }],
                centroids: vec![vec![1.0, 0.0]],
                corpus_revision: "c1".into(),
                space: self.0.clone(),
            })
        }
    }

    // spec: EG-FEDERATED-QUERY-R040.2.1
    #[test]
    fn two_backends_satisfy_one_typed_result_contract() {
        let fixed = Fixed(TopicResult {
            weights: vec![TopicWeight { topic_id: "t".into(), weight: 0.5 }],
            centroids: vec![vec![0.0, 1.0]],
            corpus_revision: "c1".into(),
            space: space(),
        });
        let backends: Vec<Box<dyn TopicBackend>> = vec![Box::new(fixed), Box::new(ByLength(space()))];
        for b in &backends {
            let r = b.topics("query").unwrap();
            r.validate(&space(), "c1").unwrap();
            let wire = serde_json::to_string(&r).unwrap();
            assert_eq!(serde_json::from_str::<TopicResult>(&wire).unwrap(), r);
            assert!(r.validate(&space(), "c2").is_err());
        }
        let other = EmbeddingSpaceRef::pinned("m", "r2", "mh", "ph", 2, true).unwrap();
        assert!(backends[0].topics("q").unwrap().validate(&other, "c1").is_err());
        assert!(TopicRankingPolicy { fusion_weight: 1.5, max_topics: 3 }.validate().is_err());
        assert!(TopicRankingPolicy { fusion_weight: 0.3, max_topics: 3 }.validate().is_ok());
    }
}
