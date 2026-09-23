//! Trajectory-prefix belief (EH-297): `BELIEF AS OF t` over temporal slices.
//!
//! SalesRLAgent's one durable idea -- a probability per conversation prefix --
//! is a special case of reading one decision state as of successive times.
//! Each prefix is the SAME option set's rows as of its `as_of_ms`; the pinned
//! head (linear or the resident scorer) is read once per slice through
//! [`read_rows`], so the belief trajectory is as reproducible as each single
//! reading. A slice never adds an option: it re-reads the ones it is given.

use eg_types::decision::statistical::head::DecisionHeadBody;
use eg_types::decision::statistical::StatisticalErrorCode;

use super::head_eval::{read_rows, RowsReading};
use super::refusal::{Refusal, RefusalResult};

/// One temporal slice: every option's row as of a time. An empty slice had
/// no complete matrix (a fact was unknown then).
#[derive(Debug, Clone, Copy)]
pub struct Prefix<'a> {
    pub as_of_ms: u64,
    pub rows: &'a [&'a [i64]],
}

/// The belief at one slice: `None` when the slice is empty, out of the
/// head's fitted range, or the head is not listwise.
#[derive(Debug, Clone, PartialEq)]
pub struct Belief {
    pub as_of_ms: u64,
    pub probabilities: Option<Vec<f64>>,
}

fn check_prefixes(prefixes: &[Prefix], options: usize) -> RefusalResult<()> {
    let increasing = prefixes.windows(2).all(|w| w[0].as_of_ms < w[1].as_of_ms);
    let shaped = prefixes
        .iter()
        .all(|p| p.rows.is_empty() || p.rows.len() == options);
    if increasing && shaped {
        return Ok(());
    }
    Err(Refusal::new(
        StatisticalErrorCode::ParameterInvalid,
        "belief slices must name one option set at strictly increasing times",
    ))
}

fn slice_belief(head: &DecisionHeadBody, prefix: &Prefix) -> RefusalResult<Option<Vec<f64>>> {
    if prefix.rows.is_empty() {
        return Ok(None);
    }
    Ok(match read_rows(head, prefix.rows)? {
        RowsReading::InDistribution(evaluated) => evaluated.probabilities,
        RowsReading::OutOfDistribution { .. } => None,
    })
}

/// The head's belief over `options` options at every prefix, in time order.
pub fn belief(
    head: &DecisionHeadBody,
    prefixes: &[Prefix],
    options: usize,
) -> RefusalResult<Vec<Belief>> {
    check_prefixes(prefixes, options)?;
    prefixes
        .iter()
        .map(|prefix| {
            Ok(Belief {
                as_of_ms: prefix.as_of_ms,
                probabilities: slice_belief(head, prefix)?,
            })
        })
        .collect()
}
