//! End-to-end Q7 acceptance: UQL pull -> induced subgraph -> QAOA on eg-quantum-sim
//! -> typed-proposal write-back, all against ONE `GraphCore`, proving:
//!
//!  1. The full pipeline runs and produces a `:QuantumJob`/`:Claim`/`:Evidence`
//!     write-set carrying the FULL Q0 result metadata.
//!  2. The ORIGINAL candidate subgraph's own node/edge properties are BYTE-IDENTICAL
//!     before and after the commit — the rejected anti-pattern (`confidence:
//!     approx_ratio` written directly onto the pre-existing graph edges) cannot
//!     happen here because this crate never touches them at all.
//!  3. The committed claim is queryable/reasonable alongside classical graph data
//!     via the SAME `eg-epistemic` surface every other claim in this engine uses.

use std::collections::BTreeMap;

use eg_core::graph::GraphCore;
use eg_epistemic::{propagate_confidence, AuthorityPolicy, BeliefGraph};
use eg_quantum_workloads::{
    commit_maxcut_proposal, demo::build_concept_graph, pull_candidate_subgraph,
    run_qaoa_maxcut_local_sim, MaxCutProposal, ProposalCommitOutcome, QaoaConfig,
};

fn snapshot_properties(core: &GraphCore, node_ids: &[String]) -> BTreeMap<String, Vec<u8>> {
    node_ids
        .iter()
        .map(|id| (id.clone(), core.get_node_properties(id).unwrap()))
        .collect()
}

fn snapshot_edge_properties(
    core: &GraphCore,
    node_ids: &[String],
) -> BTreeMap<(String, String), Vec<Vec<u8>>> {
    let mut out = BTreeMap::new();
    for i in node_ids {
        for j in node_ids {
            if i == j {
                continue;
            }
            let props = core.get_edge_properties(i, j);
            if !props.is_empty() {
                out.insert((i.clone(), j.clone()), props);
            }
        }
    }
    out
}

#[test]
fn full_pipeline_end_to_end_writes_a_typed_proposal_and_never_touches_the_source_subgraph() {
    // 1. A realistic-shaped candidate universe (30 concept nodes, ~15% edge density)
    //    -- more nodes than the NISQ-sized candidate set the UQL query below pulls,
    //    exactly like a real KG has far more nodes than one Max-Cut instance uses.
    let core = build_concept_graph(30, 0.15, 12345);

    // 2. Pull a NISQ-sized candidate set via UQL and materialize the induced
    //    subgraph -- StateVectorSimulator's max_qubits_statevector is 24; this pulls
    //    12, squarely in the "start 8-16 nodes" NISQ-sized range the charter names.
    let subgraph = pull_candidate_subgraph(&core, "MATCH (:Concept) |> LIMIT 12", 24).unwrap();
    assert_eq!(subgraph.node_ids.len(), 12);

    // Snapshot the ORIGINAL subgraph's node + edge properties BEFORE running/
    // committing anything.
    let node_props_before = snapshot_properties(&core, &subgraph.node_ids);
    let edge_props_before = snapshot_edge_properties(&core, &subgraph.node_ids);

    // 3. Build + classically optimize + run QAOA Max-Cut on eg-quantum-sim (local
    //    simulation, the default -- no hardware, no external provider touched).
    let config = QaoaConfig {
        p: 1,
        grid_resolution: 12,
        shots: 256,
        seed: 2026,
    };
    let run = run_qaoa_maxcut_local_sim(&subgraph, &config).unwrap();
    assert_eq!(run.n_qubits, 12);
    assert_eq!(run.best_partition.len(), 12);

    // 4. Write back as a TYPED PROPOSAL (never a hard constraint).
    let proposal = MaxCutProposal::from_run(&subgraph, run);
    let outcome = commit_maxcut_proposal(&core, &proposal).unwrap();
    let claim_id = match outcome {
        ProposalCommitOutcome::Committed { claim_id } => claim_id,
        other => panic!("expected a fresh Committed outcome, got {other:?}"),
    };

    // 5. The ORIGINAL subgraph's node/edge properties are UNTOUCHED -- byte-
    //    identical before and after. This is the concrete, checkable negation of the
    //    rejected anti-pattern (writing `confidence: approx_ratio` onto edges): if
    //    that had happened, an edge among these 12 nodes would have gained/changed a
    //    property and this assertion would fail.
    let node_props_after = snapshot_properties(&core, &subgraph.node_ids);
    let edge_props_after = snapshot_edge_properties(&core, &subgraph.node_ids);
    assert_eq!(node_props_before, node_props_after);
    assert_eq!(edge_props_before, edge_props_after);

    // 6. The claim is queryable/reasonable alongside classical graph data via the
    //    SAME eg-epistemic surface every other claim uses: build a BeliefGraph over
    //    the WHOLE graph (now including the new Claim/Evidence/SUPPORTS triple) and
    //    propagate confidence for the claim node.
    let view = core.analysis_snapshot();
    let belief_graph = BeliefGraph::from_graph_view(&view);
    let policy = AuthorityPolicy::default();
    let belief = propagate_confidence(&belief_graph, &claim_id, &policy);
    // The claim has exactly one supporting Evidence node (this run's), so it should
    // show up as belief-graph-visible support, not an isolated/unknown node.
    assert_eq!(belief.supporting.len(), 1);
    assert!(belief.contradicting.is_empty());
    assert!(belief.attacking.is_empty());
    assert!((0.0..=1.0).contains(&belief.confidence));

    // 7. The QuantumJob node carries the FULL Q0 result metadata.
    let job_id = eg_quantum_workloads::quantum_job_node_id(
        &proposal.quantum_result().circuit_hash.to_string(),
    );
    let job_blob = core.get_node_properties(&job_id).unwrap();
    let job_props: serde_json::Value = rmp_serde::from_slice(&job_blob).unwrap();
    for field in [
        "backend_id",
        "formalism",
        "seed",
        "shots",
        "circuit_hash",
        "backend_reported_exact",
        "noise_model_id",
        "fidelity_hint",
        "wall_time_ms",
        "peak_memory_bytes",
    ] {
        assert!(
            job_props.get(field).is_some(),
            "QuantumJob node missing Q0 metadata field `{field}`"
        );
    }
    assert_eq!(job_props["backend_id"], "sv-cpu");
    assert_eq!(job_props["shots"], 256);
    assert_eq!(job_props["seed"], 2026);
}
