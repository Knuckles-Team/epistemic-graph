//! EG-ORCH-VOCAB-R801.6 hot-path receipt: the versioned harness-component
//! write over a 1k-version chain.
//!
//! Budget (spec): p99 under 5 ms per write as the chain grows to 1,000
//! versions. Each measured op is the engine's own per-write path: the
//! pre-commit admission (strict decode, the evidence/digest/interval
//! checks, the immutability and head-relative supersession checks, the
//! bitemporal `tx_from` stamp and the supersession link) plus the RAM
//! projection (`GraphCore::add_node`) the projection applies. The
//! generated-client write through `ApplyChangeEnvelopes` pays the same
//! per-op cost (verified instead of stamped — the head scan dominates and
//! is the same call).
//!
//! Deterministic: one component, fixed content references, no RNG.
//!
//! Run: cargo bench --features server --bench harness_components

use std::time::Instant;

use eg_types::orchestration::HarnessComponentVersion;
use epistemic_graph::graph::GraphCore;
use epistemic_graph::protocol::Method;
use epistemic_graph::server::harness_admission::direct_add_node;

const COMPONENT: &str = "comp:bench-prompt";
const KIND: &str = "system_prompt";
const CHAIN: usize = 1_000;
/// The spec budget: p99 under 5 ms per write over the 1k-version chain.
const BUDGET_P99_NS: u128 = 5_000_000;

/// One version write as the writer composes it: the record's fields plus the
/// engine-owned write marker, content-addressed node id, and (the
/// generated-client's) transaction stamp.
fn version_write(index: usize, tx_now: u64) -> (String, Method) {
    let content_ref = format!("blob:v{index:04}");
    let record = HarnessComponentVersion {
        component_id: COMPONENT.to_string(),
        kind: eg_types::orchestration::HarnessComponentKind::from_wire(KIND).unwrap(),
        content_ref: content_ref.clone(),
        content_digest: HarnessComponentVersion::content_digest(&content_ref),
        valid_from: index as u64 * 10,
        valid_to: None,
        confidence: 0.5 + (index as f64 / 10_000.0),
        evidence_run_id: format!("run-{COMPONENT}-{index:04}"),
        supersedes_id: None,
        applied_to_id: Some("agent:bench".to_string()),
    };
    let mut props = serde_json::Map::new();
    props.insert("_harness_component".into(), serde_json::json!(true));
    props.insert("component_id".into(), serde_json::json!(COMPONENT));
    props.insert("kind".into(), serde_json::json!(KIND));
    props.insert("content_ref".into(), serde_json::json!(content_ref));
    props.insert(
        "content_digest".into(),
        serde_json::json!(record.content_digest),
    );
    props.insert("valid_from".into(), serde_json::json!(record.valid_from));
    props.insert("confidence".into(), serde_json::json!(record.confidence));
    props.insert(
        "evidence_run_id".into(),
        serde_json::json!(record.evidence_run_id),
    );
    props.insert("applied_to_id".into(), serde_json::json!("agent:bench"));
    props.insert("tx_from".into(), serde_json::json!(tx_now));
    let node_id = record.node_id();
    (
        node_id.clone(),
        Method::AddNode {
            node_id,
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::Value::Object(props)).unwrap(),
        },
    )
}

fn main() {
    let core = GraphCore::new();
    let mut latencies_ns = Vec::with_capacity(CHAIN);

    // The 1k-version chain: each write supersedes the current version (the
    // engine fills the pointer) and projects into the same graph.
    for index in 1..=CHAIN {
        let (_node_id, method) = version_write(index, index as u64 * 1_000);
        let started = Instant::now();
        let admitted = direct_add_node(&method, &core, index as u64 * 1_000)
            .unwrap_or_else(|error| panic!("write {index} refused: {error}"));
        let Method::AddNode {
            node_id: stored_id,
            properties_msgpack,
        } = &admitted
        else {
            panic!("admission must keep the AddNode method");
        };
        core.add_node(stored_id.clone(), properties_msgpack.clone());
        latencies_ns.push(started.elapsed().as_nanos());
    }

    // The chain is what the spec measures: 1,000 versions, each linked to
    // the previous, the newest current.
    let (last_id, _) = version_write(CHAIN, CHAIN as u64 * 1_000);
    let (previous_id, _) = version_write(CHAIN - 1, 0);
    let head = core
        .get_node_properties(&last_id)
        .expect("the newest version is stored");
    let head_props =
        eg_types::msgpack::decode_property_object(&head).expect("the stored head decodes");
    assert_eq!(
        head_props
            .get("supersedes_id")
            .and_then(|value| value.as_str()),
        Some(previous_id.as_str()),
        "the newest version links the previous one"
    );

    let mut sorted = latencies_ns.clone();
    sorted.sort_unstable();
    let p50 = sorted[sorted.len() / 2];
    let p99 = sorted[(sorted.len() as f64 * 0.99) as usize];
    let max = *sorted.last().unwrap();
    println!("harness component writes over a {CHAIN}-version chain:");
    println!("  p50 = {p50} ns");
    println!("  p99 = {p99} ns");
    println!("  max = {max} ns");
    if p99 < BUDGET_P99_NS {
        println!("  PASS (budget: p99 under {BUDGET_P99_NS} ns)");
    } else {
        println!("  FAIL (budget: p99 under {BUDGET_P99_NS} ns)");
        std::process::exit(1);
    }
}
