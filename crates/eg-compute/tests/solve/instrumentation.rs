//! Capture the real solver entry used by Assemble, not a synthetic metric call.
use eg_compute::solve::solve;

use super::support::{model, odd_cycle_cover, wide_config};
#[path = "telemetry_capture.rs"]
mod capture;

// spec: EG-DECISION-ENGINE-R072.1
#[test]
fn assemble_emits_solver_node_count_and_optimality_gap_metrics() {
    let model = model(&odd_cycle_cover(1, 3));
    let config = wide_config();
    let baseline = solve(&model, &config);
    let capture = capture::Capture::default();
    let certificate = tracing::subscriber::with_default(capture.clone(), || solve(&model, &config));
    assert_eq!(
        serde_json::to_vec(&certificate).unwrap(),
        serde_json::to_vec(&baseline).unwrap(),
        "tracing must not change deterministic certificate bytes"
    );
    let events = capture.events();
    assert_eq!(events.len(), 1);
    let fields = &events[0];
    assert_eq!(
        fields["solver_nodes"],
        certificate.nodes_expanded.to_string()
    );
    assert_eq!(fields["optimality_gap_available"], "true");
    assert_eq!(fields["optimality_gap"], "0");
    let names: Vec<_> = fields.keys().map(String::as_str).collect();
    assert_eq!(
        names,
        [
            "message",
            "optimality_gap",
            "optimality_gap_available",
            "solver_nodes"
        ]
    );
}
