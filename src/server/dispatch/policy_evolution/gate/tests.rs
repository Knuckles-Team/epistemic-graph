use std::collections::BTreeMap;

use eg_types::policy_evolution::{
    OpenWeightPolicyCapability, PolicyCapture, PolicyEvolutionRecord, PolicyRefusal, TrainingRun,
};
use serde_json::{json, Value};

use super::{admit, RecordLookup};

const TENANT: &str = "tenant-a";

fn hex(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

/// An in-memory request graph: verified records by id plus trajectory steps.
#[derive(Default)]
struct Graph {
    records: BTreeMap<String, PolicyEvolutionRecord>,
    trajectories: BTreeMap<String, u64>,
}

impl Graph {
    fn put(&mut self, record: PolicyEvolutionRecord) -> String {
        let id = record.record_id(TENANT).unwrap();
        self.records.insert(id.clone(), record);
        id
    }
}

impl RecordLookup for Graph {
    fn record(&self, record_id: &str) -> Result<Option<PolicyEvolutionRecord>, PolicyRefusal> {
        Ok(self.records.get(record_id).cloned())
    }
    fn trajectory_steps(&self, trajectory_id: &str) -> Option<u64> {
        self.trajectories.get(trajectory_id).copied()
    }
}

fn typed<T: serde::de::DeserializeOwned>(value: Value) -> T {
    serde_json::from_value(value).expect("fixture decodes")
}

fn capability_json(capture: bool, train: bool) -> Value {
    let control = |enabled: bool| json!({"enabled": enabled, "scope": enabled.then_some("ops")});
    json!({
        "provider": "vllm", "endpoint_ref": "endpoint:gb10", "base_checkpoint_digest": hex(1),
        "tokenizer_digest": hex(2), "decode_params_digest": hex(3),
        "artifact_destination_ref": "artifacts:new", "probe_digest": hex(4), "probed_at_ms": 1,
        "logprobs": {"chosen_token": true},
        "controls": {"capture": control(capture), "train": control(train)},
    })
}

fn capability(graph: &mut Graph, capture: bool, train: bool) -> String {
    let record: OpenWeightPolicyCapability = typed(capability_json(capture, train));
    graph.put(PolicyEvolutionRecord::Capability { record })
}

fn base_version(graph: &mut Graph, tokenizer: u8) -> String {
    let record = typed(json!({
        "checkpoint_digest": hex(1), "tokenizer_digest": hex(tokenizer),
        "artifact_ref": "artifacts:base", "origin": {"origin": "base"},
    }));
    graph.put(PolicyEvolutionRecord::ModelPolicyVersion { record })
}

fn blob(encoding: &str, elements: u32, width: u64) -> Value {
    json!({"digest": hex(7), "length": u64::from(elements) * width, "encoding": encoding, "elements": elements})
}

fn capture_json(capability_id: &str, sampler_id: &str, steps: u32) -> Value {
    json!({
        "capability_id": capability_id, "sampler_version_id": sampler_id,
        "trajectory_id": "trajectory:0001", "trajectory_steps": steps, "completion": "terminal",
        "token_count": 6, "policy_token_count": 2,
        "token_ids": blob("u32_le", 6, 4), "log_q": blob("f32_le", 2, 4),
        "action_mask": blob("u8_mask", 6, 1),
        "reward": {"value_micros": 1, "verifier_id": "tests", "verifier_digest": hex(9),
                   "evidence": "independent_verifier"},
        "purpose": "training", "trace_fidelity": "full", "captured_at_ms": 5,
    })
}

fn capture(capability_id: &str, sampler_id: &str, steps: u32) -> PolicyEvolutionRecord {
    let record: PolicyCapture = typed(capture_json(capability_id, sampler_id, steps));
    PolicyEvolutionRecord::Capture { record }
}

fn graph_with_trajectory() -> Graph {
    let mut graph = Graph::default();
    graph.trajectories.insert("trajectory:0001".into(), 3);
    graph
}

#[test]
fn a_capture_is_admitted_under_an_enabled_capability_and_its_sampler() {
    let mut graph = graph_with_trajectory();
    let capability_id = capability(&mut graph, true, false);
    let sampler_id = base_version(&mut graph, 2);
    assert_eq!(
        admit(&capture(&capability_id, &sampler_id, 3), &graph),
        Ok(())
    );
}

#[test]
fn a_refused_capability_answers_a_typed_refusal() {
    let mut graph = graph_with_trajectory();
    let sampler_id = base_version(&mut graph, 2);
    let absent = format!("polcap:{}", hex(5));
    assert_eq!(
        admit(&capture(&absent, &sampler_id, 3), &graph),
        Err(PolicyRefusal::CapabilityMissing)
    );
    let disabled = capability(&mut graph, false, true);
    assert_eq!(
        admit(&capture(&disabled, &sampler_id, 3), &graph),
        Err(PolicyRefusal::CaptureDisabled)
    );
}

#[test]
fn a_capture_binds_the_attested_sampler_and_the_frozen_trajectory() {
    let mut graph = graph_with_trajectory();
    let capability_id = capability(&mut graph, true, false);
    let other_tokenizer = base_version(&mut graph, 8);
    assert_eq!(
        admit(&capture(&capability_id, &other_tokenizer, 3), &graph),
        Err(PolicyRefusal::SamplerMismatch)
    );
    let sampler_id = base_version(&mut graph, 2);
    assert_eq!(
        admit(&capture(&capability_id, &sampler_id, 4), &graph),
        Err(PolicyRefusal::TrajectoryLengthMismatch)
    );
    graph.trajectories.clear();
    assert_eq!(
        admit(&capture(&capability_id, &sampler_id, 3), &graph),
        Err(PolicyRefusal::TrajectoryMissing)
    );
}

fn training_run(graph: &mut Graph, train: bool, status: Value) -> (String, TrainingRun) {
    let capability_id = capability(graph, true, train);
    let base_id = base_version(graph, 2);
    let capture_id = graph.put(capture(&capability_id, &base_id, 3));
    let run: TrainingRun = typed(json!({
        "capability_id": capability_id, "work_item_id": "work:1", "base_version_id": base_id,
        "input_capture_ids": [capture_id], "method": {"method": "klpo",
        "estimator": {"estimator": "sampled_token"}}, "adapter": {"adapter": "lora", "rank": 8},
        "trainer_image_digest": hex(10), "hyperparameters_digest": hex(11), "status": status,
    }));
    (base_id, run)
}

fn succeeded() -> Value {
    json!({"status": "succeeded", "output": {"artifact_digest": hex(12), "artifact_ref": "artifacts:v2"}})
}

#[test]
fn training_needs_the_train_control_and_eligible_captures() {
    let mut graph = graph_with_trajectory();
    let (_, run) = training_run(&mut graph, false, succeeded());
    assert_eq!(
        admit(&PolicyEvolutionRecord::TrainingRun { record: run }, &graph),
        Err(PolicyRefusal::TrainDisabled)
    );
    let (_, run) = training_run(&mut graph, true, succeeded());
    assert_eq!(
        admit(&PolicyEvolutionRecord::TrainingRun { record: run }, &graph),
        Ok(())
    );
}

fn trained_version(run_id: &str, parent: &str, adapter: u8) -> PolicyEvolutionRecord {
    let record = typed(json!({
        "checkpoint_digest": hex(1), "adapter_digest": hex(adapter), "tokenizer_digest": hex(2),
        "artifact_ref": "artifacts:v2", "parent_version_id": parent,
        "origin": {"origin": "trained", "training_run_id": run_id},
    }));
    PolicyEvolutionRecord::ModelPolicyVersion { record }
}

#[test]
fn only_a_succeeded_run_output_becomes_a_version() {
    let mut graph = graph_with_trajectory();
    let (base_id, failed) = training_run(&mut graph, true, json!({"status": "failed"}));
    let failed_id = graph.put(PolicyEvolutionRecord::TrainingRun { record: failed });
    assert_eq!(
        admit(&trained_version(&failed_id, &base_id, 12), &graph),
        Err(PolicyRefusal::RunNotSucceeded)
    );
    let (base_id, run) = training_run(&mut graph, true, succeeded());
    let run_id = graph.put(PolicyEvolutionRecord::TrainingRun { record: run });
    assert_eq!(
        admit(&trained_version(&run_id, &base_id, 12), &graph),
        Ok(())
    );
    assert_eq!(
        admit(&trained_version(&run_id, &base_id, 13), &graph)
            .unwrap_err()
            .code(),
        "POLICY_INVALID_RECORD"
    );
}

#[test]
fn an_evaluation_names_existing_versions() {
    let mut graph = Graph::default();
    let version_id = base_version(&mut graph, 2);
    let evaluation = |version: &str| {
        let record = typed(json!({
            "version_id": version, "evaluation_set_digest": hex(20), "evaluator_digest": hex(21),
            "metrics": {"task_success_ppm": 1, "baseline_task_success_ppm": 0, "cost_micros": 0,
                        "latency_p50_ms": 0, "latency_p95_ms": 0, "gpu_seconds": 0,
                        "trace_completeness_ppm": 0, "unsupported_replay_mass_ppm": 0},
            "safety": "passed", "verdict": "accepted",
        }));
        PolicyEvolutionRecord::PolicyEvaluation { record }
    };
    assert_eq!(admit(&evaluation(&version_id), &graph), Ok(()));
    let absent = format!("polver:{}", hex(6));
    assert_eq!(
        admit(&evaluation(&absent), &graph),
        Err(PolicyRefusal::RecordMissing(absent.clone()))
    );
}
