//! One valid sample per method, op and enum variant the 2.27.x contract wave
//! adds.
//!
//! These exist so the round-trip, strictness and dispatch-refusal tests all
//! work from the SAME values. A second, hand-written copy per test is how a
//! sample drifts from the shape it is supposed to pin, and how a test starts
//! proving something about a value nothing else ever sends.

pub mod control;
pub mod decision;
pub mod pack;
pub mod solver;
pub mod statistical;

use crate::agent_component::AgentComponentOp;
use crate::contract::{BoundedVec, Digest256};
use crate::protocol::Method;

/// A distinct, well-formed `sha256:<hex>` digest for `seed`.
pub fn digest_text(seed: u8) -> String {
    format!("sha256:{}", hex::encode(raw_digest(seed).as_bytes()))
}

/// A distinct 32-byte digest for `seed`.
pub fn raw_digest(seed: u8) -> Digest256 {
    Digest256::from_bytes([seed; 32])
}

/// Build a bounded collection from a sample that is known to fit.
pub fn bounded<T, const MAXIMUM: usize>(values: Vec<T>) -> BoundedVec<T, MAXIMUM> {
    BoundedVec::new(values).expect("contract-wave samples are inside their declared bounds")
}

/// One valid `Method` per new variant and per op, labelled by the surface the
/// contract-wave stub refuses it under.
///
/// The label is exactly what `contract_wave::not_yet_served` names, so a test
/// can assert the refusal text without restating it.
pub fn contract_wave_samples() -> Vec<(&'static str, Method)> {
    let mut samples = vec![
        (
            "AgentAssemble",
            Method::AgentAssemble {
                request: Box::new(decision::assembly_request()),
            },
        ),
        (
            "DecisionCommit",
            Method::DecisionCommit {
                request: Box::new(decision::commit_request()),
            },
        ),
        (
            "Decide",
            Method::Decide {
                request: Box::new(statistical::decide_request()),
            },
        ),
        (
            "Solve",
            Method::Solve {
                request: Box::new(solver::request()),
            },
        ),
        ("GraphSchemaList", Method::GraphSchemaList),
        (
            "AgentComponent.content",
            Method::AgentComponent {
                op: AgentComponentOp::Content {
                    request: control::component_content_request(),
                },
            },
        ),
    ];
    samples.extend(
        statistical::fit_ops()
            .into_iter()
            .map(|(label, op)| (label, Method::DecisionFit { op: Box::new(op) })),
    );
    samples.extend(
        statistical::eval_ops()
            .into_iter()
            .map(|(label, op)| (label, Method::DecisionEval { op: Box::new(op) })),
    );
    samples.extend(
        pack::ops()
            .into_iter()
            .map(|(label, op)| (label, Method::ConnectorPack { op: Box::new(op) })),
    );
    samples.extend(
        control::graph_schema_ops()
            .into_iter()
            .map(|(label, op)| (label, Method::GraphSchema { op: Box::new(op) })),
    );
    samples.extend(
        control::outbox_ops()
            .into_iter()
            .map(|(label, op)| (label, Method::MutationOutbox { op: Box::new(op) })),
    );
    samples
}

/// One sample per new METHOD (not per op), for tests that only need to reach
/// each dispatch arm once.
pub fn one_sample_per_method() -> Vec<(&'static str, Method)> {
    let mut seen: Vec<&'static str> = Vec::new();
    contract_wave_samples()
        .into_iter()
        .filter(|(label, _)| {
            let method = label.split('.').next().unwrap_or(label);
            if seen.contains(&method) {
                return false;
            }
            seen.push(method);
            true
        })
        .collect()
}

/// The sample labelled `label`, for a stub test that pins exactly its own
/// surface.
pub fn sample_for(label: &str) -> Method {
    contract_wave_samples()
        .into_iter()
        .find(|(candidate, _)| *candidate == label)
        .map(|(_, method)| method)
        .unwrap_or_else(|| panic!("no contract-wave sample is labelled {label}"))
}
