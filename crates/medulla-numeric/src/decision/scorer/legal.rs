//! The legal option set and the shortlist (EH-294, EH-296).
//!
//! The scorer never receives an option set: it receives what the
//! deterministic rungs (1a visibility, 1b constraints, 2 entailment) left
//! standing, as a [`LegalSet`] whose only constructor subtracts their
//! eliminations. An eliminated option is not masked -- it never becomes a
//! token, so it cannot move another option's attention context, cannot be
//! scored, and cannot be acted on.
//!
//! Then the cardinality rule: at most the head's shortlist `K` legal options
//! are scored, chosen by the linear pre-score with ties broken by option order
//! (the caller sorts options by id), so the shortlist is a pure function of the
//! stored matrix and replays.

/// The options the deterministic rungs left standing, in option order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LegalSet {
    universe: usize,
    members: Vec<usize>,
}

impl LegalSet {
    /// The legal remainder of `universe` options after the rungs'
    /// `eliminated` indices. The only way to build a legal set.
    pub fn derive(universe: usize, eliminated: &[usize]) -> Self {
        Self {
            universe,
            members: (0..universe).filter(|i| !eliminated.contains(i)).collect(),
        }
    }

    /// How many options the question named, legal or not.
    pub fn universe(&self) -> usize {
        self.universe
    }

    /// The legal options, ascending.
    pub fn members(&self) -> &[usize] {
        &self.members
    }
}

/// Keep the `limit` members with the largest pre-score (ties: lower option
/// index first); return them ascending. `scored` pairs an option index with
/// its pre-score and holds legal options only.
pub fn shortlist(scored: &[(usize, i64)], limit: usize) -> Vec<usize> {
    let mut ranked: Vec<(usize, i64)> = scored.to_vec();
    ranked.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let mut kept: Vec<usize> = ranked.into_iter().take(limit).map(|(i, _)| i).collect();
    kept.sort_unstable();
    kept
}
