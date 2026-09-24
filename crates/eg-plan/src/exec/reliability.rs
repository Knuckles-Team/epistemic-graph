//! `SOURCE RELIABILITY <id>` (EH-525): re-weight every row by how reliable a source is.
//!
//! The PRIOR is the source's propagated belief confidence in the graph
//! (`eg_epistemic::propagate_confidence` under the plan's `AuthorityPolicy`), held with
//! the policy's `prior_strength` pseudo-counts. When the plan has the caller's decision
//! log bound and that log holds independently evaluated outcomes of the source, the
//! reliability is LEARNED: the prior updated with those (discounted) outcomes, read
//! from the log's `reputation` view — visibility is the log's, so an outcome the caller
//! cannot see never moves the number. Without a log, or with no outcome of the source,
//! the prior is used unchanged (the pre-EH-525 behaviour). The multiplier is uniform
//! over the rows; an unscored row counts as `1.0`. The learned interval is reported as
//! the `reliability_lo` / `reliability_hi` channels.

use super::{belief_policy, PlanCtx};
use crate::rowset::RowSet;

/// A source's reliability and, when it was learned, its credible interval.
pub(super) struct Reliability {
    pub(super) mean: f64,
    pub(super) interval: Option<(f64, f64)>,
}

/// The reliability of `source_id` under `ctx`.
pub(super) fn reliability_of(ctx: &PlanCtx, source_id: &str) -> Result<Reliability, String> {
    let graph = eg_epistemic::BeliefGraph::from_graph_view(ctx.view);
    let policy = belief_policy(ctx);
    let prior = eg_epistemic::propagate_confidence(&graph, source_id, &policy).confidence;
    let learned = match ctx.decisions {
        Some(log) => log.learned_reliability(source_id, prior, policy.prior_strength)?,
        None => None,
    };
    Ok(match learned {
        Some(learned) => Reliability {
            mean: learned.mean,
            interval: Some((learned.lower, learned.upper)),
        },
        None => Reliability {
            mean: prior,
            interval: None,
        },
    })
}

/// `input` re-weighted by `reliability`.
pub(super) fn reweighted(input: &RowSet, reliability: &Reliability) -> RowSet {
    let r = reliability.mean as f32;
    RowSet::from_scored(
        input
            .rows()
            .iter()
            .map(|row| (row.id.clone(), row.score.unwrap_or(1.0) * r)),
    )
}

/// `SourceReliability { source_id }` as a plain stage.
pub(super) fn source_reliability_op(
    ctx: &PlanCtx,
    input: RowSet,
    source_id: &str,
) -> Result<RowSet, String> {
    Ok(reweighted(&input, &reliability_of(ctx, source_id)?))
}
