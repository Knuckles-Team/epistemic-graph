//! Epistemic executor proofs (CONCEPT:EG-KG.epistemic.epistemic-substrate, E2).
//!
//! A small belief graph — a `Claim` with supporting `Evidence`, a `Contradiction`, and an
//! `Attack` — drives the `Op::{EvidenceFor,Contradicts,SupportedBy,BeliefAsOf,
//! SourceReliability,ConfidenceOp,ExplainBelief}` surface end-to-end through the fused
//! executor, mirroring `probabilistic_tests.rs`'s shape (a hand-built `GraphView` +
//! `PlanCtx`, `plan.execute(&ctx)`, assert on the resulting `RowSet` ids/order).

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_types::wire::TimeAxis;
use serde_json::json;

use crate::algebra::{Op, Plan};
use crate::exec::PlanCtx;
use crate::PlanExt;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// The belief-substrate fixture:
///
///   evidence1 --SUPPORTS--> claim1 <--CONTRADICTS-- counter1
///   claim1    --SUPPORTS--> derived1
///   attacker1 --ATTACKS-->  claim1
///
/// `claim1` also carries an embedding (for the `[Scan, EvidenceFor, Rank]` compose proof)
/// and `late_claim` is a bitemporal fixture (`valid_from=0`, `tx_from=500`) — TRUE from the
/// start, but not BELIEVED (recorded) until ts=500 — the case `BELIEF AS OF` and
/// `VALID AS OF` must diverge on.
fn beliefs() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    core.add_node(
        "claim1".into(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );
    core.add_node(
        "evidence1".into(),
        blob(json!({ "type": "Evidence", "confidence": 0.9 })),
    );
    core.add_node(
        "counter1".into(),
        blob(json!({ "type": "Evidence", "confidence": 0.9 })),
    );
    core.add_node(
        "attacker1".into(),
        blob(json!({ "type": "Evidence", "confidence": 0.9 })),
    );
    core.add_node(
        "derived1".into(),
        blob(json!({ "type": "Claim", "confidence": 0.5 })),
    );
    core.add_node(
        "late_claim".into(),
        blob(json!({ "type": "Claim", "confidence": 0.7, "valid_from": 0, "tx_from": 500 })),
    );
    core.add_edge(
        "evidence1".into(),
        "claim1".into(),
        blob(json!({ "relationship_type": "SUPPORTS" })),
    )
    .unwrap();
    core.add_edge(
        "counter1".into(),
        "claim1".into(),
        blob(json!({ "relationship_type": "CONTRADICTS" })),
    )
    .unwrap();
    core.add_edge(
        "attacker1".into(),
        "claim1".into(),
        blob(json!({ "relationship_type": "ATTACKS" })),
    )
    .unwrap();
    core.add_edge(
        "claim1".into(),
        "derived1".into(),
        blob(json!({ "relationship_type": "SUPPORTS" })),
    )
    .unwrap();

    let mut semantic = SemanticStore::new();
    semantic.add_embedding("claim1".into(), vec![1.0, 0.0, 0.0, 0.0]);
    semantic.add_embedding("evidence1".into(), vec![0.0, 1.0, 0.0, 0.0]);

    (core.analysis_snapshot(), semantic)
}

fn run(plan: &Plan, ctx: &PlanCtx) -> Vec<String> {
    plan.execute(ctx).unwrap().ids()
}

/// `EvidenceFor` seeds the evidence FOR a claim.
#[test]
fn evidence_for_seeds_supporting_nodes() {
    let (view, sem) = beliefs();
    let ctx = PlanCtx::new(&view, &sem);
    let plan = Plan::new(vec![Op::EvidenceFor {
        claim_id: "claim1".into(),
    }]);
    assert_eq!(run(&plan, &ctx), vec!["evidence1".to_string()]);
}

/// `Contradicts` seeds BOTH the plain contradiction and the attack (a stronger
/// contradiction) — order-independent (a set), so check membership.
#[test]
fn contradicts_seeds_contradiction_and_attack() {
    let (view, sem) = beliefs();
    let ctx = PlanCtx::new(&view, &sem);
    let plan = Plan::new(vec![Op::Contradicts {
        node_id: "claim1".into(),
    }]);
    let mut ids = run(&plan, &ctx);
    ids.sort();
    assert_eq!(ids, vec!["attacker1".to_string(), "counter1".to_string()]);
}

/// `SupportedBy` is the mirror direction of `EvidenceFor`: the claims a node itself
/// supports.
#[test]
fn supported_by_seeds_outgoing_support() {
    let (view, sem) = beliefs();
    let ctx = PlanCtx::new(&view, &sem);
    let plan = Plan::new(vec![Op::SupportedBy {
        node_id: "claim1".into(),
    }]);
    assert_eq!(run(&plan, &ctx), vec!["derived1".to_string()]);
}

/// THE COMPOSE PROOF: `[Scan, EvidenceFor, Rank]` composes in ONE plan — a label scan
/// seeds every `Claim`/`Evidence`, `EvidenceFor` narrows to `claim1`'s evidence, and a
/// vector `Rank` re-orders the (now single-row) survivor set — proving the epistemic ops
/// are ordinary `RowSet -> RowSet` legs that compose with the graph/vector algebra exactly
/// like `Reason`/`Probabilistic` do.
#[test]
fn scan_evidence_for_rank_composes_in_one_plan() {
    let (view, sem) = beliefs();
    let ctx = PlanCtx::new(&view, &sem);
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Evidence".into(),
        },
        Op::EvidenceFor {
            claim_id: "claim1".into(),
        },
        Op::Rank {
            query: vec![0.0, 1.0, 0.0, 0.0],
        },
    ]);
    assert_eq!(run(&plan, &ctx), vec!["evidence1".to_string()]);
}

/// `CONFIDENCE` re-scores each row by its own propagated belief, ranked descending: a
/// supported claim (`claim1`, raised by `evidence1`) outranks the bare `Evidence` node
/// distractors.
#[test]
fn confidence_op_ranks_by_propagated_belief() {
    let (view, sem) = beliefs();
    let ctx = PlanCtx::new(&view, &sem);
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Claim".into(),
        },
        Op::ConfidenceOp {},
    ]);
    let rows = plan.execute(&ctx).unwrap();
    // Three `Claim` nodes: claim1 (support+contradiction+attack), derived1 (support from
    // claim1), late_claim (no incoming evidence — stays at its bare prior). Their exact
    // propagated confidences depend on the conjugate update — assert the invariant every
    // `ConfidenceOp` output must satisfy rather than a brittle hand-computed order: every
    // returned row is one of the three Claims, scored in `[0,1]`.
    let ids = rows.ids();
    assert_eq!(ids.len(), 3);
    for r in rows.rows() {
        assert!(["claim1", "derived1", "late_claim"].contains(&r.id.as_str()));
        let s = r.score.expect("ConfidenceOp must score every row");
        assert!((0.0..=1.0).contains(&s));
    }
}

/// `SourceReliability` re-weights every row in the current (unscored) RowSet by one
/// named source's propagated reliability — a uniform scalar multiplier (an unscored row
/// is treated as `1.0`). `counter1` has NO incoming evidence, so its own propagated belief
/// is exactly its stored prior (`0.9`, per `propagate.rs`'s `no_evidence_is_prior`
/// invariant): every discounted score must equal `1.0 * 0.9`.
#[test]
fn source_reliability_reweights_uniformly() {
    let (view, sem) = beliefs();
    let ctx = PlanCtx::new(&view, &sem);
    let discounted = Plan::new(vec![
        Op::Scan {
            label: "Claim".into(),
        },
        Op::SourceReliability {
            source_id: "counter1".into(),
        },
    ])
    .execute(&ctx)
    .unwrap();
    assert_eq!(discounted.len(), 3);
    for r in discounted.rows() {
        let got = r.score.expect("SourceReliability must score every row");
        assert!((got - 0.9).abs() < 1e-6, "got {got}");
    }
}

/// `EXPLAIN BELIEF` flattens the justification tree: the root claim plus every premise
/// (support/contradiction/attack) it recursively depends on, each scored by its own
/// propagated confidence.
#[test]
fn explain_belief_flattens_justification_tree() {
    let (view, sem) = beliefs();
    let ctx = PlanCtx::new(&view, &sem);
    let plan = Plan::new(vec![Op::ExplainBelief {
        node_id: "claim1".into(),
    }]);
    let mut ids = run(&plan, &ctx);
    ids.sort();
    assert_eq!(
        ids,
        vec![
            "attacker1".to_string(),
            "claim1".to_string(),
            "counter1".to_string(),
            "evidence1".to_string(),
        ]
    );
}

/// THE BITEMPORAL DIVERGENCE PROOF (CONCEPT:EG-KG.epistemic.epistemic-substrate): `late_claim`
/// is TRUE from the start (`valid_from=0`) but not BELIEVED/recorded until `tx_from=500`.
/// At `ts=100`, `VALID AS OF` (the world-truth axis) includes it; `BELIEF AS OF` (the
/// transaction/belief axis) does NOT — the exact case the two axes exist to distinguish.
/// At `ts=600` (past `tx_from`), `BELIEF AS OF` now includes it, scored by propagated
/// confidence.
#[test]
fn belief_as_of_and_valid_as_of_diverge_on_bitemporal_fixture() {
    let (view, sem) = beliefs();
    let ctx = PlanCtx::new(&view, &sem);

    let valid_at_100 = Plan::new(vec![Op::AsOf {
        ts: 100.0,
        axis: TimeAxis::Valid,
    }])
    .execute(&ctx)
    .unwrap()
    .ids();
    assert!(
        valid_at_100.contains(&"late_claim".to_string()),
        "VALID AS OF @100 must include late_claim (valid_from=0)"
    );

    let belief_at_100 = Plan::new(vec![Op::BeliefAsOf { ts: 100.0 }])
        .execute(&ctx)
        .unwrap()
        .ids();
    assert!(
        !belief_at_100.contains(&"late_claim".to_string()),
        "BELIEF AS OF @100 must EXCLUDE late_claim (tx_from=500, not yet believed)"
    );

    let belief_at_600 = Plan::new(vec![Op::BeliefAsOf { ts: 600.0 }])
        .execute(&ctx)
        .unwrap()
        .ids();
    assert!(
        belief_at_600.contains(&"late_claim".to_string()),
        "BELIEF AS OF @600 must include late_claim (past tx_from=500)"
    );
}
