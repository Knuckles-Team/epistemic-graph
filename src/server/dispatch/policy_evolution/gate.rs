//! Cross-record admission: the rules a policy-evolution record must satisfy
//! against what the graph already holds.
//!
//! Pure over [`RecordLookup`], so every refusal is unit-testable without a
//! server. Structural checks on one body already ran in `eg-types`.

use eg_types::policy_evolution::{
    CaptureEligibility, ModelPolicyVersion, OpenWeightPolicyCapability, PolicyCapture,
    PolicyEvaluation, PolicyEvolutionRecord, PolicyEvolutionRecordBody, PolicyRefusal, TrainingRun,
    TrainingRunStatus, VersionOrigin,
};

/// What admission may read from the request graph.
pub(super) trait RecordLookup {
    /// The verified record under `record_id`, `None` when absent. A stored
    /// record whose body no longer matches its id is a refusal, never a value.
    fn record(&self, record_id: &str) -> Result<Option<PolicyEvolutionRecord>, PolicyRefusal>;
    /// The step count of a `:Trajectory` node, `None` when there is none.
    fn trajectory_steps(&self, trajectory_id: &str) -> Option<u64>;
}

/// Admit `record` against the stored graph, or answer the typed refusal.
pub(super) fn admit(
    record: &PolicyEvolutionRecord,
    lookup: &dyn RecordLookup,
) -> Result<(), PolicyRefusal> {
    match record {
        PolicyEvolutionRecord::Capability { .. } => Ok(()),
        PolicyEvolutionRecord::Capture { record } => admit_capture(record, lookup),
        PolicyEvolutionRecord::ModelPolicyVersion { record } => admit_version(record, lookup),
        PolicyEvolutionRecord::TrainingRun { record } => admit_training_run(record, lookup),
        PolicyEvolutionRecord::PolicyEvaluation { record } => admit_evaluation(record, lookup),
    }
}

/// The stored body of kind `T` under `record_id`; absent or of another kind
/// answers `missing`.
fn fetch<T: PolicyEvolutionRecordBody>(
    lookup: &dyn RecordLookup,
    record_id: &str,
    missing: fn(&str) -> PolicyRefusal,
) -> Result<T, PolicyRefusal> {
    lookup
        .record(record_id)?
        .and_then(T::from_record)
        .ok_or_else(|| missing(record_id))
}

fn record_missing(record_id: &str) -> PolicyRefusal {
    PolicyRefusal::RecordMissing(record_id.to_string())
}

fn capability_missing(_: &str) -> PolicyRefusal {
    PolicyRefusal::CapabilityMissing
}

/// A capture is admitted only under an attested capability whose `capture`
/// control is on, sampled by the version that capability serves, over a
/// trajectory that exists with exactly the step count it claims.
fn admit_capture(capture: &PolicyCapture, lookup: &dyn RecordLookup) -> Result<(), PolicyRefusal> {
    let capability: OpenWeightPolicyCapability =
        fetch(lookup, capture.capability_id.as_str(), capability_missing)?;
    if !capability.controls.capture.enabled {
        return Err(PolicyRefusal::CaptureDisabled);
    }
    let sampler: ModelPolicyVersion =
        fetch(lookup, capture.sampler_version_id.as_str(), record_missing)?;
    check_sampler(capture, &capability, &sampler)?;
    match lookup.trajectory_steps(capture.trajectory_id.as_str()) {
        None => Err(PolicyRefusal::TrajectoryMissing),
        Some(steps) if steps != u64::from(capture.trajectory_steps) => {
            Err(PolicyRefusal::TrajectoryLengthMismatch)
        }
        Some(_) => Ok(()),
    }
}

/// The sampler is the exact artifact the capability attested, and it reports
/// every record the capture carries.
fn check_sampler(
    capture: &PolicyCapture,
    capability: &OpenWeightPolicyCapability,
    sampler: &ModelPolicyVersion,
) -> Result<(), PolicyRefusal> {
    let same_artifact = sampler.checkpoint_digest == capability.base_checkpoint_digest
        && sampler.adapter_digest == capability.adapter_digest
        && sampler.tokenizer_digest == capability.tokenizer_digest;
    if !same_artifact {
        return Err(PolicyRefusal::SamplerMismatch);
    }
    let support = capability.logprobs;
    let unsupported_top_k = capture.sampler_top_k.is_some() && support.top_k == 0;
    let unsupported_draws = capture.auxiliary_draws.is_some() && !support.auxiliary_draws;
    if !support.chosen_token || unsupported_top_k || unsupported_draws {
        return Err(PolicyRefusal::LogprobsUnsupported);
    }
    Ok(())
}

/// A version's parent exists; a trained version is the one new adapter a
/// succeeded run produced on that parent. A failed or cancelled run has no
/// output, so it can never mint a servable version.
fn admit_version(
    version: &ModelPolicyVersion,
    lookup: &dyn RecordLookup,
) -> Result<(), PolicyRefusal> {
    if let Some(parent) = &version.parent_version_id {
        fetch::<ModelPolicyVersion>(lookup, parent.as_str(), record_missing)?;
    }
    let VersionOrigin::Trained { training_run_id } = &version.origin else {
        return Ok(());
    };
    let run: TrainingRun = fetch(lookup, training_run_id.as_str(), record_missing)?;
    let TrainingRunStatus::Succeeded { output } = &run.status else {
        return Err(PolicyRefusal::RunNotSucceeded);
    };
    let produced = version.adapter_digest == Some(output.artifact_digest)
        && version.parent_version_id.as_ref() == Some(&run.base_version_id);
    if !produced {
        return Err(PolicyRefusal::InvalidRecord(
            "a trained version must be the run's output adapter on the run's base".into(),
        ));
    }
    Ok(())
}

/// A run is admitted only under a capability whose `train` control is on,
/// from an existing base version, over trainer-eligible captures.
fn admit_training_run(run: &TrainingRun, lookup: &dyn RecordLookup) -> Result<(), PolicyRefusal> {
    let capability: OpenWeightPolicyCapability =
        fetch(lookup, run.capability_id.as_str(), capability_missing)?;
    if !capability.controls.train.enabled {
        return Err(PolicyRefusal::TrainDisabled);
    }
    fetch::<ModelPolicyVersion>(lookup, run.base_version_id.as_str(), record_missing)?;
    for capture_id in run.input_capture_ids.iter() {
        let capture: PolicyCapture = fetch(lookup, capture_id.as_str(), record_missing)?;
        if capture.eligibility() != CaptureEligibility::Eligible {
            return Err(PolicyRefusal::CaptureIneligible(
                capture_id.as_str().to_string(),
            ));
        }
    }
    Ok(())
}

/// An evaluation names versions that exist.
fn admit_evaluation(
    evaluation: &PolicyEvaluation,
    lookup: &dyn RecordLookup,
) -> Result<(), PolicyRefusal> {
    let versions = std::iter::once(&evaluation.version_id).chain(&evaluation.baseline_version_id);
    for version_id in versions {
        fetch::<ModelPolicyVersion>(lookup, version_id.as_str(), record_missing)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
