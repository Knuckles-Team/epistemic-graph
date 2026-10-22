//! EG-ORCH-VOCAB-R802.6 hot-path receipt: lesson writes into a 10k-lesson
//! improvement library.
//!
//! Budget (spec): p99 under 10 ms per write as the library grows to 10,000
//! lessons. Each measured op is the engine's own per-write path: the
//! pre-commit admission (strict decode, the evidence-run and confidence
//! checks, the immutability check, the head scan that fills the
//! supersession pointer, and the bitemporal `tx_from` stamp) plus the RAM
//! projection (`GraphCore::add_node`) the projection applies. The
//! generated-client write through `ApplyChangeEnvelopes` pays the same
//! per-op cost (verified instead of stamped — the head scan dominates and is
//! the same call).
//!
//! The library holds 100 task classes of 100 deficiencies each; every
//! (task class, deficiency) pair learns one lesson, so the head scan walks
//! the growing graph on each write. Deterministic: no RNG.
//!
//! Run: cargo bench --features server --bench harness_lessons

#[path = "common/harness_admission.rs"]
mod harness_bench;

use eg_types::contract::BoundedVec;
use eg_types::orchestration::improvement::{HarnessLesson, HarnessProbeRun};
use eg_types::orchestration::HarnessComponentKind;
use epistemic_graph::graph::GraphCore;
use epistemic_graph::protocol::Method;

const TASK_CLASSES: usize = 100;
const DEFICIENCIES: usize = 100;
const LESSONS: usize = TASK_CLASSES * DEFICIENCIES;
/// The spec budget: p99 under 10 ms per write over the 10k-lesson library.
const BUDGET_P99_NS: u128 = 10_000_000;

/// One probe run under the fixed conditions; the lessons cite both of them.
fn probe_run_write(trace_id: &str, tx_now: u64) -> Method {
    let _ = HarnessProbeRun {
        trace_id: trace_id.to_string(),
        conditions_digest: "sha256:conditions".to_string(),
    };
    let mut props = serde_json::Map::new();
    props.insert("_harness_probe_run".into(), serde_json::json!(true));
    props.insert("trace_id".into(), serde_json::json!(trace_id));
    props.insert(
        "conditions_digest".into(),
        serde_json::json!("sha256:conditions"),
    );
    props.insert("tx_from".into(), serde_json::json!(tx_now));
    Method::AddNode {
        node_id: format!("harness-probe-run:{trace_id}"),
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::Value::Object(props)).unwrap(),
    }
}

/// One lesson write as the writer composes it: the record's fields plus the
/// engine-owned write marker, the content-addressed node id, and (the
/// generated-client's) transaction stamp.
fn lesson_write(index: usize, tx_now: u64) -> Method {
    let task_class = format!("task:bench-{:03}", index % TASK_CLASSES);
    let deficiency = format!(
        "deficiency:bench-{:03}-{:03}",
        index / TASK_CLASSES,
        index % DEFICIENCIES
    );
    let record = HarnessLesson {
        task_class: task_class.clone(),
        deficiency: deficiency.clone(),
        situation: "the run retried the failing tool".to_string(),
        changed_component_kind: HarnessComponentKind::SystemPrompt,
        what_changed: format!("tightened the retry policy {index:05}"),
        applicability: BoundedVec::default(),
        confidence: 0.5,
        evidence_run_ids: BoundedVec::new(vec!["trace-a".to_string(), "trace-b".to_string()])
            .unwrap(),
        supersedes_id: None,
    };
    let mut props = serde_json::Map::new();
    props.insert("_harness_lesson".into(), serde_json::json!(true));
    props.insert("task_class".into(), serde_json::json!(task_class));
    props.insert("deficiency".into(), serde_json::json!(deficiency));
    props.insert(
        "situation".into(),
        serde_json::json!("the run retried the failing tool"),
    );
    props.insert(
        "changed_component_kind".into(),
        serde_json::json!("system_prompt"),
    );
    props.insert(
        "what_changed".into(),
        serde_json::json!(record.what_changed),
    );
    props.insert("applicability".into(), serde_json::json!([]));
    props.insert("confidence".into(), serde_json::json!(record.confidence));
    props.insert(
        "evidence_run_ids".into(),
        serde_json::json!(["trace-a", "trace-b"]),
    );
    props.insert("tx_from".into(), serde_json::json!(tx_now));
    Method::AddNode {
        node_id: record.node_id(),
        properties_msgpack: rmp_serde::to_vec_named(&serde_json::Value::Object(props)).unwrap(),
    }
}

fn main() {
    let core = GraphCore::new();

    // The two probe runs the lessons cite, admitted through the same path.
    for (index, trace) in ["trace-a", "trace-b"].into_iter().enumerate() {
        harness_bench::admit_and_project(&core, index, &probe_run_write(trace, 1), 1);
    }

    // The 10k-lesson library, one lesson per (task class, deficiency).
    let mut latencies_ns = Vec::with_capacity(LESSONS);
    for index in 0..LESSONS {
        let method = lesson_write(index, (index as u64 + 1) * 1_000);
        latencies_ns.push(harness_bench::admit_and_project(
            &core,
            index,
            &method,
            (index as u64 + 1) * 1_000,
        ));
    }

    harness_bench::report(
        &format!("harness lesson writes into a {LESSONS}-lesson library"),
        &latencies_ns,
        BUDGET_P99_NS,
    );
}
