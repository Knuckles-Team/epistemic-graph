//! `DecisionEval` in replay mode: walk-forward policy replay sealed as an
//! `EvaluationRun` (EH-528, ANALYTICS-HARVEST AH-08).
//!
//! The admitted gold items, in time order, are the steps. The candidate head
//! (refitted per fold on its trailing training window when the spec asks) and
//! the incumbent (a pinned head, or the uniform allocation) each allocate the
//! shared budget over every test step's options. The engine computes every
//! statistic itself -- the deflated Sharpe ratio with the declared trial count,
//! the probability of backtest overfitting over the folds, and Diebold-Mariano
//! against the incumbent -- with the same validation kernels a finance
//! `BacktestRun` seals, and refuses what replay cannot honestly answer: a
//! policy-dependent environment, bandit labels, an understated trial count, a
//! look-ahead.

use eg_compute::finance::quant::{
    deflated_sharpe_ratio, diebold_mariano, probability_of_backtest_overfit,
};
use eg_numeric::decision::admission::Regime;
use eg_numeric::decision::fit::FitSpec;
use eg_numeric::decision::quant::{q32, value_of};
use eg_numeric::decision::refusal::RefusalResult;
use eg_numeric::decision::replay::{
    gold_steps, head_digest, max_drawdown, mean, replay, sharpe, time_ordered, HeadReplay,
    ReplayOutcome, ReplayPolicy, ReplayStep, Uniform,
};
use eg_types::contract::BoundedVec;
use eg_types::decision::replay::{
    EvaluationRun, OptionContribution, ReplayEnvironment, ReplayFoldView, ReplaySpec,
    ReplayValidation, TrialLog,
};
use eg_types::decision::statistical::dataset::LabelledItem;
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::{DecisionEvalRequest, DecisionJobOutput, QuantisedValue};

use super::stat_jobs::{candidate_head, dataset_digest, EvalInputs, JobRun};
use super::stat_support::refusal;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::{
    decode_artifact, encode_artifact, evaluation_run_key,
};

fn rendered<T>(result: RefusalResult<T>) -> Result<T, String> {
    result.map_err(|refused| refused.render())
}

fn quantised(value: f64) -> Result<QuantisedValue, String> {
    rendered(q32(value))
}

/// The assumptions replay rests on, checked before anything is read.
fn check_spec(spec: &ReplaySpec, regime: Regime) -> Result<(), String> {
    if spec.graph.is_empty()
        || spec.graph.len() > 256
        || spec.graph.trim() != spec.graph
        || spec.graph.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(refusal(
            StatisticalErrorCode::ReplaySpecInvalid,
            "replay requires an exact, bounded graph ACL anchor",
        ));
    }
    let dependent = spec.env == ReplayEnvironment::PolicyDependent || regime != Regime::FullLabel;
    if dependent {
        return Err(refusal(
            StatisticalErrorCode::ReplayPolicyDependent,
            "replay evaluates only a policy-independent environment over gold labels; use the off-policy estimators",
        ));
    }
    let TrialLog { declared, searched } = spec.trials;
    if declared == 0 || declared < searched {
        return Err(refusal(
            StatisticalErrorCode::TrialsUnderstated,
            format!("{declared} trials declared, the search log records {searched}"),
        ));
    }
    Ok(())
}

fn check_supersedes(
    store: &AgentLibraryStore,
    tenant_id: &str,
    spec: &ReplaySpec,
) -> Result<(), String> {
    let Some(previous) = &spec.supersedes else {
        return Ok(());
    };
    match store.decision_artifact(tenant_id, &evaluation_run_key(previous))? {
        Some(bytes) => {
            let run: EvaluationRun = decode_artifact(&bytes, "evaluation run")?;
            if run.run_digest != *previous || !run.verify() {
                return Err(refusal(
                    StatisticalErrorCode::ReplaySpecInvalid,
                    "supersedes names a corrupt evaluation run",
                ));
            }
            Ok(())
        }
        None => Err(refusal(
            StatisticalErrorCode::ReplaySpecInvalid,
            format!("supersedes names no evaluation run {previous}"),
        )),
    }
}

/// The candidate and incumbent replayed over the steps.
fn replayed(
    store: &AgentLibraryStore,
    inputs: &EvalInputs,
    request: &DecisionEvalRequest,
    spec: &ReplaySpec,
    items: &[&LabelledItem],
    steps: &[ReplayStep],
) -> Result<ReplayOutcome, String> {
    let (mut candidate, mut incumbent) = policies(store, inputs, request, spec, items, steps)?;
    let cap = value_of(spec.budget.cap);
    rendered(replay(
        steps,
        &spec.folds,
        cap,
        &mut candidate,
        incumbent.as_mut(),
    ))
}

fn policies<'a>(
    store: &AgentLibraryStore,
    inputs: &'a EvalInputs,
    request: &DecisionEvalRequest,
    spec: &ReplaySpec,
    items: &'a [&'a LabelledItem],
    steps: &'a [ReplayStep],
) -> Result<(HeadReplay<'a>, Box<dyn ReplayPolicy + 'a>), String> {
    let refit = spec.refit.map(|refit| FitSpec {
        head_kind: refit.head_kind,
        regime: Regime::FullLabel,
        optimiser: refit.optimiser,
        feature_schema_digest: &inputs.head.feature_schema_digest,
        statistical: &inputs.policy.statistical,
    });
    let candidate = HeadReplay {
        dataset: &inputs.dataset,
        items,
        head: inputs.head.clone(),
        refit,
    };
    let incumbent: Box<dyn ReplayPolicy + 'a> = match &spec.incumbent {
        None => Box::new(Uniform { steps }),
        Some(pin) => Box::new(HeadReplay {
            dataset: &inputs.dataset,
            items,
            head: incumbent_head(store, inputs, &request.tenant_id, pin)?,
            refit: None,
        }),
    };
    Ok((candidate, incumbent))
}

fn incumbent_head(
    store: &AgentLibraryStore,
    inputs: &EvalInputs,
    tenant_id: &str,
    pin: &eg_types::decision::EvalCandidate,
) -> Result<eg_types::decision::statistical::head::DecisionHeadBody, String> {
    let head = candidate_head(store, tenant_id, pin)?;
    if head.feature_schema_digest != inputs.dataset.feature_schema_digest {
        return Err(refusal(
            StatisticalErrorCode::HeadInvalid,
            "the incumbent reads a different feature schema than the dataset",
        ));
    }
    Ok(head)
}

/// Rows = folds, columns = (candidate, incumbent): in-sample is the training
/// window, out-of-sample the test window.
fn overfit_matrices(outcome: &ReplayOutcome) -> (Vec<Vec<f64>>, Vec<Vec<f64>>) {
    outcome
        .folds
        .iter()
        .map(|fold| {
            (
                vec![fold.insample, fold.incumbent_insample],
                vec![mean(&fold.path), mean(&fold.incumbent_path)],
            )
        })
        .unzip()
}

fn negated(path: &[f64]) -> Vec<f64> {
    path.iter().map(|utility| -utility).collect()
}

fn validation(outcome: &ReplayOutcome, trials: TrialLog) -> Result<ReplayValidation, String> {
    let path = outcome.path();
    let observed = sharpe(&path).ok_or_else(|| {
        refusal(
            StatisticalErrorCode::ReplaySpecInvalid,
            "the replayed utility path has no variance; a Sharpe ratio is undefined",
        )
    })?;
    let (insample, oos) = overfit_matrices(outcome);
    let compared = diebold_mariano(&negated(&path), &negated(&outcome.incumbent_path()), 1);
    Ok(ReplayValidation {
        observed_sharpe: quantised(observed)?,
        n_trials: trials.declared,
        deflated_sharpe: quantised(deflated_sharpe_ratio(
            observed,
            trials.declared as usize,
            &path,
        ))?,
        probability_backtest_overfit: quantised(probability_of_backtest_overfit(&insample, &oos))?,
        diebold_mariano: quantised(compared.statistic)?,
        diebold_mariano_p: quantised(compared.p_value)?,
    })
}

fn fold_views(
    outcome: &ReplayOutcome,
    steps: &[ReplayStep],
) -> Result<Vec<ReplayFoldView>, String> {
    outcome
        .folds
        .iter()
        .map(|fold| {
            Ok(ReplayFoldView {
                train_items: fold.fold.train.len() as u32,
                test_items: fold.fold.test.len() as u32,
                first_test_ms: steps[fold.fold.test.start].at_ms,
                last_test_ms: steps[fold.fold.test.end - 1].at_ms,
                head_digest: fold.head_digest.clone(),
                utility: quantised(fold.path.iter().sum())?,
                incumbent_utility: quantised(fold.incumbent_path.iter().sum())?,
                max_drawdown: quantised(max_drawdown(&fold.path))?,
                abstained: fold.abstained,
            })
        })
        .collect()
}

fn contributions(outcome: &ReplayOutcome) -> Result<Vec<OptionContribution>, String> {
    outcome
        .contributions
        .iter()
        .map(|(option_id, (applied, utility))| {
            Ok(OptionContribution {
                option_id: option_id.clone(),
                applied: quantised(*applied)?,
                utility: quantised(*utility)?,
            })
        })
        .collect()
}

fn bounded<T, const N: usize>(values: Vec<T>, what: &str) -> Result<BoundedVec<T, N>, String> {
    BoundedVec::new(values).map_err(|detail| {
        refusal(
            StatisticalErrorCode::ReplaySpecInvalid,
            format!("{what}: {detail}"),
        )
    })
}

/// Seal the run: every output derived here, the digest over all of it.
fn seal(
    inputs: &EvalInputs,
    spec: &ReplaySpec,
    outcome: &ReplayOutcome,
    steps: &[ReplayStep],
) -> Result<EvaluationRun, String> {
    let path = outcome
        .path()
        .into_iter()
        .map(quantised)
        .collect::<Result<Vec<_>, _>>()?;
    let mut run = EvaluationRun {
        run_digest: String::new(),
        head_digest: rendered(head_digest(&inputs.head))?,
        policy_digest: inputs.policy.digest.clone(),
        dataset_digest: dataset_digest(&inputs.dataset)?,
        spec: spec.clone(),
        folds: bounded(fold_views(outcome, steps)?, "folds")?,
        path: bounded(path, "replayed steps")?,
        contributions: bounded(contributions(outcome)?, "option contributions")?,
        validation: validation(outcome, spec.trials)?,
        synthetic: inputs.dataset.synthetic,
    };
    run.run_digest = run.sealed_digest();
    Ok(run)
}

/// Run one replay evaluation to its sealed record and artifact row.
pub(super) fn run(
    store: &AgentLibraryStore,
    inputs: &EvalInputs,
    request: &DecisionEvalRequest,
    spec: &ReplaySpec,
) -> JobRun {
    check_spec(spec, inputs.regime)?;
    check_supersedes(store, &request.tenant_id, spec)?;
    let admitted = inputs.admitted(request.window);
    let items = time_ordered(&admitted.items);
    let steps = rendered(gold_steps(&items))?;
    let outcome = replayed(store, inputs, request, spec, &items, &steps)?;
    let run = seal(inputs, spec, &outcome, &steps)?;
    let row = (evaluation_run_key(&run.run_digest), encode_artifact(&run)?);
    Ok((DecisionJobOutput::Replay { run: Box::new(run) }, vec![row]))
}

/// Resume a claimed replay from its durable per-fold checkpoint. The job's
/// graph and tenant must match the exact request being replayed; the worker
/// validates the request-ref and input-payload digest before calling this.
/// The fold runner validates its checkpoint against that pinned job lineage.
#[cfg(feature = "jobs")]
pub(super) fn run_fenced(
    store: &AgentLibraryStore,
    inputs: &EvalInputs,
    request: &DecisionEvalRequest,
    spec: &ReplaySpec,
    jobs: &eg_jobs::JobStore,
    claim: &eg_jobs::store::WorkerClaim,
) -> JobRun {
    check_spec(spec, inputs.regime)?;
    check_supersedes(store, &request.tenant_id, spec)?;
    if claim.job.policy.tenant != request.tenant_id || claim.job.input_snapshot.graph != spec.graph
    {
        return Err(refusal(
            StatisticalErrorCode::ReplaySpecInvalid,
            "claimed replay does not match its tenant and graph",
        ));
    }
    let admitted = inputs.admitted(request.window);
    let items = time_ordered(&admitted.items);
    let steps = rendered(gold_steps(&items))?;
    let folds = rendered(eg_numeric::decision::replay::walk_forward(
        steps.len(),
        &spec.folds,
    ))?;
    let (mut candidate, mut incumbent) = policies(store, inputs, request, spec, &items, &steps)?;
    let cap = value_of(spec.budget.cap);
    let completed = eg_jobs::run_folds_fenced(
        jobs,
        claim,
        folds.len(),
        |index| {
            rendered(eg_numeric::decision::replay::replay_fold(
                &steps,
                folds[index].clone(),
                cap,
                &mut candidate,
                incumbent.as_mut(),
            ))
        },
        replay_now_ms,
    )
    .map_err(|error| error.to_string())?;
    let outcome = eg_numeric::decision::replay::collect_replay(completed);
    let run = seal(inputs, spec, &outcome, &steps)?;
    let row = (evaluation_run_key(&run.run_digest), encode_artifact(&run)?);
    Ok((DecisionJobOutput::Replay { run: Box::new(run) }, vec![row]))
}

#[cfg(feature = "jobs")]
fn replay_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}
