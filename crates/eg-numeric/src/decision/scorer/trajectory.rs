//! Trajectory-prefix belief (EH-297): `BELIEF AS OF t` over temporal slices.
//!
//! SalesRLAgent's one durable idea -- a probability per conversation prefix --
//! is a special case of reading one decision state as of successive times.
//! Each prefix is the SAME legal option set read as of its `as_of_ms` (the
//! graph candidate source's `AsOf` stage produces such slices); the head is
//! read once per slice, so the belief trajectory is as reproducible as each
//! single decision.

use eg_types::decision::statistical::head::DecisionHeadBody;
use eg_types::decision::statistical::StatisticalErrorCode;

use super::forward::{EncodedState, Scorer};
use super::legal::LegalSet;
use crate::decision::refusal::{Refusal, RefusalResult};

/// One temporal slice of a decision state: every option's row as of a time.
#[derive(Debug, Clone, Copy)]
pub struct Prefix<'a> {
    pub as_of_ms: u64,
    pub rows: &'a [&'a [i64]],
}

/// The belief at one slice: `None` when a row was out of distribution there
/// (that slice abstains; the trajectory continues).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Belief {
    pub as_of_ms: u64,
    pub probabilities: Option<Vec<i64>>,
}

fn check_prefixes(prefixes: &[Prefix], legal: &LegalSet) -> RefusalResult<()> {
    let increasing = prefixes.windows(2).all(|w| w[0].as_of_ms < w[1].as_of_ms);
    let shaped = prefixes.iter().all(|p| p.rows.len() == legal.universe());
    if increasing && shaped {
        return Ok(());
    }
    Err(Refusal::new(
        StatisticalErrorCode::ParameterInvalid,
        "trajectory prefixes must name one option set at strictly increasing times",
    ))
}

/// The head's belief over `legal` at every prefix, in time order.
pub fn belief(
    head: &DecisionHeadBody,
    prefixes: &[Prefix],
    legal: &LegalSet,
) -> RefusalResult<Vec<Belief>> {
    check_prefixes(prefixes, legal)?;
    let scorer = Scorer::of(head)?;
    Ok(prefixes
        .iter()
        .map(|prefix| Belief {
            as_of_ms: prefix.as_of_ms,
            probabilities: EncodedState::encode(&scorer, prefix.rows, legal)
                .ok()
                .map(|state| state.score(&scorer, &state.scored_options()).probabilities),
        })
        .collect())
}
