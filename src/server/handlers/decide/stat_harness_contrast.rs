//! MEDULLA-DECISION-ENGINE-R803: wires the contrastive harness composition
//! decision into the `HarnessContrast` `Decide` question kind.
//!
//! The caller declares each candidate component version as an option
//! (`CandidateSource::Declared`: option id = the version's node id, the four
//! measured deltas as declared numbers, the probe trace id as a declared
//! text) and names the task, the task class, the baseline trace, the
//! identical probe conditions and the evidence graph as typed request
//! params. The handler reads the held versions, the probe runs and the
//! stored contrasts out of the RLS-filtered graph snapshot, runs the pure
//! kernel (`medulla_types::decision::harness_contrast::decide_contrast`) and
//! seals the decision as an `Explored` or `Abstained` outcome over an
//! inline matrix that carries exactly the pinned per-candidate deltas.

use std::collections::{BTreeMap, BTreeSet};

use medulla_numeric::decision::features::FeatureMatrix;
use medulla_numeric::decision::ladder::LadderResult;
use medulla_types::decision::harness_contrast::{
    decide_contrast, matrix_values, ContrastCandidate, ContrastEvidence, HarnessContrastRefusal,
    CONTRAST_FEATURE_NAMES, MAX_CONTRAST_CANDIDATES, MIN_CONTRAST_CANDIDATES,
};
use medulla_types::decision::statistical::declared::DeclaredOption;
use medulla_types::decision::statistical::{
    CandidateSource, DecideRequest, QuestionKind, StatisticalOutcome, TypedValue,
};
use medulla_types::decision::{AbstainReason, UnitRationalWire};
use medulla_types::orchestration::improvement::{
    decode_harness_contrast_write, decode_harness_probe_run_write, HarnessContrast, HarnessProbeRun,
};
use medulla_types::orchestration::is_harness_component_write;

use super::stat_executor::Executed;
use super::stat_support::{filtered_view, invalid_parameter};

/// The closed set of typed params a `harness_contrast` question must carry.
const PARAM_TASK_REF: &str = "task_ref";
const PARAM_TASK_CLASS: &str = "task_class";
const PARAM_CONDITIONS_DIGEST: &str = "conditions_digest";
const PARAM_BASELINE_TRACE: &str = "baseline_trace_id";
const PARAM_GRAPH: &str = "graph";

/// One declared candidate, pulled from its option's facts.
struct DeclaredCandidate {
    version_ref: String,
    probe_trace_id: Option<String>,
    deltas: HarnessContrast,
}

fn typed_param<'a>(request: &'a DecideRequest, name: &str, what: &str) -> Result<&'a str, String> {
    request
        .params
        .iter()
        .find(|param| param.name == name)
        .and_then(|param| match &param.value {
            TypedValue::Text(text) if !text.trim().is_empty() => Some(text.as_str()),
            TypedValue::Iri(iri) if !iri.trim().is_empty() => Some(iri.as_str()),
            _ => None,
        })
        .ok_or_else(|| {
            invalid_parameter(format!(
                "a harness_contrast question requires a {name} ({what}) param"
            ))
        })
}

/// The four declared delta numbers of one option, on the Q32 scale.
fn declared_deltas(option: &DeclaredOption) -> Result<(i64, i64, i64, i64), String> {
    let number = |key: &str| -> Result<i64, String> {
        option
            .numbers
            .iter()
            .find(|number| number.key == key)
            .map(|number| number.q32)
            .ok_or_else(|| {
                invalid_parameter(format!(
                    "a harness_contrast candidate must declare its {key} number"
                ))
            })
    };
    Ok((
        number("retry_delta")?,
        number("tool_selection_delta")?,
        number("eval_score_delta")?,
        number("cost_delta")?,
    ))
}

/// The probe trace id one option declares, if it names one.
fn declared_trace(option: &DeclaredOption) -> Result<Option<String>, String> {
    match option
        .texts
        .iter()
        .find(|text| text.key == "probe_trace_id")
    {
        None => Ok(None),
        Some(text) if text.text.trim().is_empty() => Ok(None),
        Some(text) => Ok(Some(text.text.clone())),
    }
}

/// Open the evidence graph of a `harness_contrast` question before any
/// read: named by its `graph` typed param, read through the same
/// RLS-filtered snapshot a graph-sourced candidate plan gets.
pub(super) async fn contrast_evidence_view(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::state::ServerState>>,
    agent_id: &str,
    request: &DecideRequest,
) -> Result<medulla_core::graph::GraphView, String> {
    let graph = typed_param(request, PARAM_GRAPH, "text")?;
    filtered_view(state, agent_id, graph).await
}

/// `Some` only for a `HarnessContrast` question: the executed decision to
/// seal in place of the ordinary scored ladder.
pub(super) fn contrast_executed(
    request: &DecideRequest,
    graph_view: Option<&medulla_core::graph::GraphView>,
) -> Option<Result<Executed, String>> {
    (request.question.kind == QuestionKind::HarnessContrast).then(|| compute(request, graph_view))
}

/// The stored probe run behind one trace id, when the view holds a
/// well-formed record there.
fn probe_run(view: &medulla_core::graph::GraphView, trace_id: &str) -> Option<HarnessProbeRun> {
    view.node_row_object(&format!("harness-probe-run:{trace_id}"))
        .and_then(|props| decode_harness_probe_run_write(&props).ok())
}

/// The stored contrast behind one node id, when the view holds a
/// well-formed record there.
fn stored_contrast(view: &medulla_core::graph::GraphView, node_id: &str) -> Option<HarnessContrast> {
    view.node_row_object(node_id)
        .and_then(|props| decode_harness_contrast_write(&props).ok())
}

fn refusal_text(
    kind: HarnessContrastRefusal,
    options: &[DeclaredOption],
    held: &BTreeSet<String>,
) -> String {
    use HarnessContrastRefusal as K;
    let detail = match kind {
        K::MinCandidates => {
            format!("a contrast decides over at least {MIN_CONTRAST_CANDIDATES} candidate versions")
        }
        K::TooManyCandidates => {
            format!("a contrast decides over at most {MAX_CONTRAST_CANDIDATES} candidate versions")
        }
        K::VersionUnknown => {
            let missing = options
                .iter()
                .find(|option| !held.contains(&option.option_id))
                .map(|option| option.option_id.clone())
                .unwrap_or_default();
            if missing.is_empty() {
                "the graph does not hold a candidate version".to_string()
            } else {
                format!("the graph does not hold {missing}")
            }
        }
        K::TraceMismatch => {
            "the named probe trace does not agree with the stored evidence".to_string()
        }
    };
    format!("{}: {detail}", kind.code())
}

/// Read the evidence out of the view and run the kernel.
fn compute(
    request: &DecideRequest,
    graph_view: Option<&medulla_core::graph::GraphView>,
) -> Result<Executed, String> {
    let view = graph_view.ok_or_else(|| {
        invalid_parameter("a harness_contrast question reads its evidence from a named graph")
    })?;
    let task_ref = typed_param(request, PARAM_TASK_REF, "text")?;
    let task_class = typed_param(request, PARAM_TASK_CLASS, "iri")?;
    let conditions_digest = typed_param(request, PARAM_CONDITIONS_DIGEST, "text")?;
    let baseline_trace_id = typed_param(request, PARAM_BASELINE_TRACE, "text")?;
    let CandidateSource::Declared { options } = &request.candidates else {
        return Err(invalid_parameter(
            "a harness_contrast question declares its own candidates",
        ));
    };
    if options.len() < MIN_CONTRAST_CANDIDATES || options.len() > MAX_CONTRAST_CANDIDATES {
        return Err(invalid_parameter(format!(
            "a contrast decides over {MIN_CONTRAST_CANDIDATES} to {MAX_CONTRAST_CANDIDATES} declared candidates"
        )));
    }
    let mut declared: Vec<DeclaredCandidate> = Vec::with_capacity(options.len());
    let mut held_versions = BTreeSet::new();
    let mut probe_runs: BTreeMap<String, HarnessProbeRun> = BTreeMap::new();
    let mut stored_contrasts: BTreeMap<String, HarnessContrast> = BTreeMap::new();
    for option in options {
        let (retry, selection, eval, cost) = declared_deltas(option)?;
        let probe_trace_id = declared_trace(option)?;
        let deltas = HarnessContrast {
            task_ref: task_ref.to_string(),
            conditions_digest: conditions_digest.to_string(),
            run_a_trace_id: baseline_trace_id.to_string(),
            run_b_trace_id: probe_trace_id.clone().unwrap_or_default(),
            retry_delta: retry,
            tool_selection_delta: selection,
            eval_score_delta: eval,
            cost_delta: cost,
        };
        if view
            .node_row_object(&option.option_id)
            .is_some_and(|props| is_harness_component_write(&props))
        {
            held_versions.insert(option.option_id.clone());
        }
        if let Some(trace) = &probe_trace_id {
            if let Some(run) = probe_run(view, trace) {
                probe_runs.insert(trace.clone(), run);
            }
        }
        if let Some(baseline) = probe_run(view, baseline_trace_id) {
            probe_runs.insert(baseline_trace_id.to_string(), baseline);
        }
        if let Some(contrast) = stored_contrast(view, &deltas.node_id()) {
            stored_contrasts.insert(deltas.node_id(), contrast);
        }
        declared.push(DeclaredCandidate {
            version_ref: option.option_id.clone(),
            probe_trace_id,
            deltas,
        });
    }
    let evidence = ContrastEvidence {
        held_versions: &held_versions,
        probe_runs: &probe_runs,
        stored_contrasts: &stored_contrasts,
    };
    let candidates: Vec<ContrastCandidate<'_>> = declared
        .iter()
        .map(|candidate| ContrastCandidate {
            version_ref: &candidate.version_ref,
            probe_trace_id: candidate.probe_trace_id.as_deref(),
            deltas: &candidate.deltas,
        })
        .collect();
    let decision = decide_contrast(
        task_ref,
        task_class,
        conditions_digest,
        baseline_trace_id,
        &candidates,
        &evidence,
    )
    .map_err(|kind| refusal_text(kind, options.as_slice(), &held_versions))?;
    let outcome = match &decision.adopted_version_ref {
        Some(ref_) => StatisticalOutcome::Explored {
            option_id: ref_.clone(),
            propensity: UnitRationalWire::new(1, 1)
                .map_err(|detail| invalid_parameter(format!("propensity: {detail}")))?,
        },
        None => StatisticalOutcome::Abstained {
            reasons: medulla_types::contract::BoundedVec::new(vec![
                AbstainReason::InsufficientConfidence,
            ])
            .expect("one reason is inside the bound"),
        },
    };
    Ok(Executed {
        matrix: Some(FeatureMatrix {
            candidate_ids: decision.candidate_version_refs.iter().cloned().collect(),
            feature_names: CONTRAST_FEATURE_NAMES
                .iter()
                .map(|name| name.to_string())
                .collect(),
            values: matrix_values(&decision),
        }),
        ladder: LadderResult {
            outcome,
            calibration: None,
            explained: None,
            audit: None,
            logging: Vec::new(),
        },
        reading: None,
        seed: [0u8; 32],
        harness_contrast: Some(decision),
    })
}
