//! Bounded, fixed-dimension candidates and their Pareto front.

use serde::{Deserialize, Serialize};

mod score_array {
    use serde::de::{Error, IgnoredAny, SeqAccess, Visitor};
    use serde::ser::SerializeSeq;
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S, const K: usize>(scores: &[f32; K], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(K))?;
        for score in scores {
            sequence.serialize_element(score)?;
        }
        sequence.end()
    }

    pub fn deserialize<'de, D, const K: usize>(deserializer: D) -> Result<[f32; K], D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ScoreArrayVisitor<const K: usize>;

        impl<'de, const K: usize> Visitor<'de> for ScoreArrayVisitor<K> {
            type Value = [f32; K];

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "a score array containing exactly {K} values")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut scores = [0.0; K];
                for (index, score) in scores.iter_mut().enumerate() {
                    *score = sequence
                        .next_element()?
                        .ok_or_else(|| A::Error::invalid_length(index, &self))?;
                }
                if sequence.next_element::<IgnoredAny>()?.is_some() {
                    return Err(A::Error::invalid_length(K + 1, &self));
                }
                Ok(scores)
            }
        }

        deserializer.deserialize_seq(ScoreArrayVisitor::<K>)
    }
}

/// A policy evaluated on one fixed-size set of evaluation metrics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyCandidate<const K: usize> {
    pub id: String,
    pub parent: Option<String>,
    /// Lowercase SHA-256 digest of the candidate's policy genes.
    pub genes_digest: String,
    /// Scores in `[0, 1]`; the array fixes the evaluation-set dimension at compile time.
    #[serde(with = "score_array")]
    pub scores: [f32; K],
    /// Measured cost in integer micro-USD.
    pub cost_micro_usd: u64,
}

impl<const K: usize> PolicyCandidate<K> {
    fn validate(&self) -> Result<(), FrontError> {
        if self.id.trim().is_empty() {
            return Err(FrontError::EmptyId);
        }
        if let Some(parent) = &self.parent {
            if parent.trim().is_empty() {
                return Err(FrontError::EmptyParent);
            }
        }
        if self.genes_digest.len() != 64
            || !self
                .genes_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(FrontError::InvalidGenesDigest);
        }
        if self
            .scores
            .iter()
            .any(|score| !score.is_finite() || !(0.0..=1.0).contains(score))
        {
            return Err(FrontError::ScoreOutOfBounds);
        }
        Ok(())
    }

    fn dominates(&self, other: &Self) -> bool {
        let no_worse = self.cost_micro_usd <= other.cost_micro_usd
            && self
                .scores
                .iter()
                .zip(other.scores.iter())
                .all(|(left, right)| left >= right);
        let strictly_better = self.cost_micro_usd < other.cost_micro_usd
            || self
                .scores
                .iter()
                .zip(other.scores.iter())
                .any(|(left, right)| left > right);
        no_worse && strictly_better
    }
}

/// A typed refusal for malformed candidate identity or score data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrontError {
    EmptyId,
    EmptyParent,
    InvalidGenesDigest,
    ScoreOutOfBounds,
}

/// Non-dominated candidates sharing one compile-time score dimension `K`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParetoFront<const K: usize> {
    candidates: Vec<PolicyCandidate<K>>,
}

impl<const K: usize> Default for ParetoFront<K> {
    fn default() -> Self {
        Self {
            candidates: Vec::new(),
        }
    }
}

impl<const K: usize> ParetoFront<K> {
    pub fn candidates(&self) -> &[PolicyCandidate<K>] {
        &self.candidates
    }

    /// Insert a valid candidate unless an existing candidate dominates it.
    /// Returns `true` when the front changes.
    pub fn insert(&mut self, candidate: PolicyCandidate<K>) -> Result<bool, FrontError> {
        candidate.validate()?;
        if self
            .candidates
            .iter()
            .any(|existing| existing.dominates(&candidate))
        {
            return Ok(false);
        }
        self.candidates
            .retain(|existing| !candidate.dominates(existing));
        self.candidates.push(candidate);
        Ok(true)
    }
}
