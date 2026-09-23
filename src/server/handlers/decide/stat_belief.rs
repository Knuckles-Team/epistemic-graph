//! Belief slices of one served decision (EH-297): `BELIEF AS OF t`.
//!
//! A `Decide` that names `belief_as_of` times gets, besides its decision, the
//! pinned head's distribution over the SAME visible options at each time:
//! the features are recomputed with the feature clock at `t` (every
//! time-dependent feature -- `AgeSeconds` -- reads that clock), the head is
//! read over each slice, and each slice's matrix is stored in the record so
//! the log's verify-replay re-derives the belief exactly. A slice re-reads
//! the options the decision already has; it never adds one. Outcome-rate
//! features are read at the decision's clock, not sliced.

use eg_numeric::decision::candidate::CandidateView;
use eg_numeric::decision::features::{feature_matrix, FeatureInputs, MatrixOutcome};
use eg_numeric::decision::quant::q32;
use eg_numeric::decision::trajectory::{belief, Prefix};
use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::belief::{BeliefPoint, BeliefSliceInputs};
use eg_types::decision::statistical::{DecideRequest, StatisticalErrorCode};

use super::stat_executor::{ExecutionContext, Pinned};
use super::stat_support::refusal;

/// A decision's belief slices: their stored inputs and the head's belief.
#[derive(Debug, Clone, Default)]
pub(super) struct Slices {
    pub(super) inputs: Vec<BeliefSliceInputs>,
    pub(super) points: Vec<BeliefPoint>,
}

fn check_request(
    ctx: &ExecutionContext,
    request: &DecideRequest,
    pinned: &Pinned,
) -> Result<(), String> {
    let times = request.belief_as_of.as_slice();
    if pinned.head.is_none() {
        return Err(refusal(
            StatisticalErrorCode::ParameterInvalid,
            "belief slices read a head: pin one",
        ));
    }
    let increasing = times.windows(2).all(|w| w[0] < w[1]);
    if !increasing || times.last().is_some_and(|&t| t > ctx.now_ms) {
        return Err(refusal(
            StatisticalErrorCode::ParameterInvalid,
            "belief_as_of must be strictly increasing and not after the decision's clock",
        ));
    }
    Ok(())
}

fn slice_inputs(
    request: &DecideRequest,
    pinned: &Pinned,
    views: &[CandidateView],
    as_of_ms: u64,
) -> Result<BeliefSliceInputs, String> {
    let features = FeatureInputs {
        params: request.params.as_slice(),
        now_ms: as_of_ms,
    };
    let values = match feature_matrix(&pinned.schema, views, &features).map_err(|r| r.render())? {
        MatrixOutcome::Complete(matrix) => matrix.values,
        MatrixOutcome::UnknownFact { .. } => Vec::new(),
    };
    Ok(BeliefSliceInputs {
        as_of_ms,
        values: BoundedVec::new(values)
            .map_err(|detail| refusal(StatisticalErrorCode::CandidateSetTooLarge, detail))?,
    })
}

fn point(as_of_ms: u64, probabilities: Option<Vec<f64>>) -> Result<BeliefPoint, String> {
    let probabilities = probabilities
        .map(|p| {
            let quantised = p
                .into_iter()
                .map(q32)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|r| r.render())?;
            BoundedVec::new(quantised)
                .map_err(|detail| refusal(StatisticalErrorCode::CandidateSetTooLarge, detail))
        })
        .transpose()?;
    Ok(BeliefPoint {
        as_of_ms,
        probabilities,
    })
}

/// The pinned head's belief over `options` options at every stored slice:
/// the served path and the verify-replay both answer through here.
pub(super) fn points(
    pinned: &Pinned,
    inputs: &[BeliefSliceInputs],
    options: usize,
) -> Result<Vec<BeliefPoint>, String> {
    let Some((head, _)) = &pinned.head else {
        return Ok(Vec::new());
    };
    let width = pinned.schema.names().len().max(1);
    let matrices: Vec<Vec<&[i64]>> = inputs
        .iter()
        .map(|slice| slice.values.as_slice().chunks(width).collect())
        .collect();
    let prefixes: Vec<Prefix> = inputs
        .iter()
        .zip(&matrices)
        .map(|(slice, rows)| Prefix {
            as_of_ms: slice.as_of_ms,
            rows,
        })
        .collect();
    belief(head, &prefixes, options)
        .map_err(|r| r.render())?
        .into_iter()
        .map(|b| point(b.as_of_ms, b.probabilities))
        .collect()
}

/// The belief slices a request asks for, over the decision's visible options.
pub(super) fn live_slices(
    ctx: &ExecutionContext,
    request: &DecideRequest,
    pinned: &Pinned,
    views: &[CandidateView],
) -> Result<Slices, String> {
    if request.belief_as_of.is_empty() {
        return Ok(Slices::default());
    }
    check_request(ctx, request, pinned)?;
    let inputs = request
        .belief_as_of
        .iter()
        .map(|&t| slice_inputs(request, pinned, views, t))
        .collect::<Result<Vec<_>, _>>()?;
    let points = points(pinned, &inputs, views.len())?;
    Ok(Slices { inputs, points })
}
