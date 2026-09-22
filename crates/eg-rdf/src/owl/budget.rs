//! The derivation-step budget of the EL⁺/RL completion (EH-119 / EH-152; pack import
//! G14 and graph-schema attach K4, `EG-X9-X10-X11-DESIGN.md` §0.2 C5 / §1.3 K4).
//!
//! A step is one NEW fact the completion derives: a subsumption `A ⊑ B`, a role pair
//! `(A, B) ∈ R(r)`, or a refined filler. Seeding (`A ⊑ A`, `A ⊑ ⊤`) is not a step. The
//! budget is a count, not a wall-clock limit, and the completion's rule order is
//! deterministic, so the same axioms under the same budget always reach the same
//! verdict. An exhausted budget is a typed outcome, never a silently partial
//! classification.

use std::fmt;

use super::{Classification, Reasoner};

/// The most derivation steps one classification may take.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DerivationBudget {
    max_steps: u64,
}

impl DerivationBudget {
    pub const fn new(max_steps: u64) -> Self {
        Self { max_steps }
    }

    pub const fn max_steps(self) -> u64 {
        self.max_steps
    }
}

/// The completion needed more than [`DerivationBudget::max_steps`] derivation steps.
/// No verdict (consistent or not) is implied: the classification was not finished.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BudgetExhausted {
    pub max_steps: u64,
}

impl BudgetExhausted {
    /// The wire error code shared by every budgeted validation (pack G14/G21, attach K4).
    pub const CODE: &'static str = "VALIDATION_BUDGET_EXCEEDED";
}

impl fmt::Display for BudgetExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: EL⁺/RL classification needs more than {} derivation steps",
            Self::CODE,
            self.max_steps
        )
    }
}

impl std::error::Error for BudgetExhausted {}

/// Counts derivation steps and refuses the first one past the limit.
#[derive(Clone, Debug, Default)]
pub(super) struct StepMeter {
    steps: u64,
    limit: Option<u64>,
    refused: bool,
}

impl StepMeter {
    fn limited(budget: DerivationBudget) -> Self {
        Self {
            limit: Some(budget.max_steps),
            ..Self::default()
        }
    }

    /// Charge one derivation step. `false` means the budget is spent: the caller must
    /// not add the fact, and the classification reports [`BudgetExhausted`].
    pub(super) fn charge(&mut self) -> bool {
        if self.limit.is_some_and(|limit| self.steps >= limit) {
            self.refused = true;
            return false;
        }
        self.steps += 1;
        true
    }
}

impl Reasoner {
    /// [`Reasoner::classify`] within a derivation-step budget. `Err` when the
    /// completion needed more steps than `budget` allows; the reasoner's closure is
    /// then partial but sound (every fact in it is entailed), and it is unbudgeted
    /// again for the next call.
    pub fn classify_within(
        &mut self,
        budget: DerivationBudget,
    ) -> Result<Classification, BudgetExhausted> {
        self.meter = StepMeter::limited(budget);
        let classification = self.classify();
        let refused = std::mem::take(&mut self.meter).refused;
        if refused {
            return Err(BudgetExhausted {
                max_steps: budget.max_steps,
            });
        }
        Ok(classification)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::parse_turtle;

    fn chain(length: usize) -> Vec<oxrdf::Triple> {
        let mut ttl = String::from(
            "@prefix ex: <http://example.org/> .\n\
             @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n",
        );
        for i in 0..length {
            ttl.push_str(&format!("ex:C{i} rdfs:subClassOf ex:C{} .\n", i + 1));
        }
        parse_turtle(&ttl).unwrap()
    }

    /// A 20-axiom chain over 21 classes derives exactly the 210 subsumptions
    /// `Cᵢ ⊑ Cⱼ` with `i < j` (21·20/2); nothing else is a derivation step.
    #[test]
    fn exact_budget_completes_and_one_less_is_exhausted() {
        let triples = chain(20);
        let needed = 210;
        let complete = Reasoner::from_triples(&triples)
            .classify_within(DerivationBudget::new(needed))
            .expect("the exact budget suffices");
        assert!(complete.entails_subclass("<http://example.org/C0>", "<http://example.org/C20>"));

        let exhausted = Reasoner::from_triples(&triples)
            .classify_within(DerivationBudget::new(needed - 1))
            .unwrap_err();
        assert_eq!(
            exhausted,
            BudgetExhausted {
                max_steps: needed - 1
            }
        );
        assert!(exhausted
            .to_string()
            .starts_with("VALIDATION_BUDGET_EXCEEDED"));
    }

    /// The verdict is a pure function of the axioms and the budget.
    #[test]
    fn the_budget_verdict_is_deterministic_and_the_reasoner_is_reusable() {
        let triples = chain(12);
        for _ in 0..3 {
            let mut reasoner = Reasoner::from_triples(&triples);
            assert!(reasoner.classify_within(DerivationBudget::new(10)).is_err());
            let unbudgeted = reasoner.classify();
            assert!(
                unbudgeted.entails_subclass("<http://example.org/C0>", "<http://example.org/C12>")
            );
        }
    }
}
