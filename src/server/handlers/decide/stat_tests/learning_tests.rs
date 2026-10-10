//! The query adapter and the embedding-generation swap (EH-396, EH-397):
//! fitted and receipted by `DecisionLog.learn`, served only after a pointer
//! move with the passing receipt, rolled back by a pointer move, and read
//! (pointers, audit) through the `decision_pointers` relation.

use super::retrieval_tests::{
    attest, clone_executed, evaluate_by, executed_template, learn, outcome, sql,
};
use super::*;
use eg_types::decision::statistical::retrieval::{
    LearningRecorded, LearningWrite, PointerMovement, PointerRef, QueryVector,
};
use eg_types::decision::statistical::retrieval_adapter::{AdapterFitRequest, AdapterFitted};
use eg_types::decision::statistical::retrieval_generation::{
    GenerationEvalItem, GenerationEvalRequest, GenerationEvaluated,
};
use eg_types::decision::statistical::retrieval_pointer::PointerState;
use serde_json::json;

const GRAPH: &str = "kg-retrieval";

async fn pointer_state(
    h: &Harness,
    pointer: PointerRef,
    movement: PointerMovement,
) -> Result<PointerState, String> {
    match learn(
        h,
        "decider",
        LearningWrite::MovePointer { pointer, movement },
    )
    .await?
    {
        LearningRecorded::Pointer(state) => Ok(*state),
        other => panic!("pointer state: {other:?}"),
    }
}

fn activate(target: &str, receipt_digest: &str) -> PointerMovement {
    PointerMovement::Activate {
        target: target.to_string(),
        receipt_digest: receipt_digest.to_string(),
    }
}

/// Nodes `p<i>` (what answers cite) are the unit axis 1 and `n<i>` (what the
/// base query prefers) the unit axis 0; every judged run returned `n<i>` above
/// `p<i>`.
async fn embedded_graph(h: &Harness) -> Arc<eg_core::graph::GraphCore> {
    let rows = (0..40).flat_map(|i| {
        [
            (format!("p{i}"), vec![0.0, 1.0, 0.0]),
            (format!("n{i}"), vec![1.0, 0.0, 0.0]),
        ]
    });
    graph_with(h, GRAPH, "1", 3, rows.collect()).await
}

/// The digest of the test model's space at `revision`.
fn space_of(revision: &str, dims: usize) -> String {
    eg_types::embedding::EmbeddingSpaceRef::pinned(
        "m", revision, "sha256:w", "sha256:p", dims, false,
    )
    .unwrap()
    .digest
}

/// A graph whose store declares model revision `revision` and embeds `rows`.
async fn graph_with(
    h: &Harness,
    name: &str,
    revision: &str,
    dims: usize,
    rows: Vec<(String, Vec<f32>)>,
) -> Arc<eg_core::graph::GraphCore> {
    let mut guard = h.state.write().await;
    guard
        .registry
        .create_graph(name, crate::protocol::GraphType::Team, None)
        .unwrap();
    let core = guard.registry.get(name).unwrap().core.clone();
    let space = eg_types::embedding::EmbeddingSpaceRef::pinned(
        "m", revision, "sha256:w", "sha256:p", dims, false,
    )
    .unwrap();
    assert_eq!(space.digest, space_of(revision, dims));
    core.semantic_store.write().declare_space(space).unwrap();
    for (id, vector) in rows {
        let props = serde_json::json!({"type": "Doc"});
        core.add_node(id.clone(), rmp_serde::to_vec_named(&props).unwrap());
        core.semantic_store
            .write()
            .add_embedding(id, vector)
            .unwrap();
    }
    core
}

fn q16(values: &[f64]) -> BoundedVec<i32, 4096> {
    BoundedVec::new(
        values
            .iter()
            .map(|v| (v * 65_536.0).round() as i32)
            .collect(),
    )
    .unwrap()
}

async fn judged_runs(h: &Harness, space: &str) {
    let template_entry = executed_template(h).await;
    for i in 0..40 {
        let record_id = format!("rec-{i:02}");
        clone_executed(h, &template_entry, &record_id);
        let (n, p) = (format!("n{i}"), format!("p{i}"));
        let mut attested = outcome(&record_id, &[&n, &p], &[&p]);
        attested.query = Some(QueryVector {
            space_digest: space.to_string(),
            q16: q16(&[1.0, 0.9 + f64::from(i) * 0.001, 0.0]),
        });
        let recorded = learn(h, "decider", attest(attested)).await;
        assert!(recorded.is_ok(), "{recorded:?}");
        evaluate_by(h, "evaluator", &record_id, true).await;
    }
}

fn fit_request(space: &str) -> AdapterFitRequest {
    AdapterFitRequest {
        graph: GRAPH.to_string(),
        space_digest: space.to_string(),
        question_id: None,
        window: window(),
        rank: 2,
        max_gain_q16: 1 << 15,
        holdout_per_mille: 300,
        min_eval_items: 5,
    }
}

fn top(core: &eg_core::graph::GraphCore, query: &[f32]) -> String {
    core.semantic_store.read().semantic_search(query, 1)[0]
        .0
        .clone()
}

// spec: EG-DECISION-ENGINE-R101
#[tokio::test]
async fn an_adapter_is_served_only_after_its_passing_receipt_and_rolls_back() {
    let h = Harness::with_isolation(ServerState::test_isolation("decider")).await;
    let core = embedded_graph(&h).await;
    let space = core.semantic_store.read().space().unwrap().digest.clone();
    judged_runs(&h, &space).await;

    let fitted: AdapterFitted = match learn(
        &h,
        "decider",
        LearningWrite::FitAdapter {
            request: Box::new(fit_request(&space)),
        },
    )
    .await
    .unwrap()
    {
        LearningRecorded::Fitted(fitted) => *fitted,
        other => panic!("fit: {other:?}"),
    };
    let receipt = &fitted.receipt;
    assert!(receipt.passed, "{receipt:?}");
    assert!(receipt.adapted_mrr_q16 > receipt.base_mrr_q16);
    assert_eq!(receipt.n_training + receipt.n_eval, 40);

    let tenant = crate::server::access::CarrierAuthority::from_verified(&verified())
        .unwrap()
        .tenant_scope()
        .to_string();
    let served = || super::super::served_adapter::served_for(&h.state, Some(&tenant), GRAPH, &core);
    let query = [1.0_f32, 0.9, 0.0];
    assert!(served().await.is_none(), "fitted is not active");
    assert!(
        top(&core, &query).starts_with('n'),
        "the base query prefers the negatives"
    );

    let adapter_ptr = || PointerRef::Adapter {
        graph: GRAPH.to_string(),
    };
    let zeros = format!("sha256:{}", "0".repeat(64));
    let wrong = pointer_state(&h, adapter_ptr(), activate(&fitted.adapter_digest, &zeros)).await;
    assert!(wrong
        .unwrap_err()
        .starts_with("EVALUATION_RECEIPT_MISMATCH"));
    let moved = activate(&fitted.adapter_digest, &fitted.receipt_digest);
    let active = pointer_state(&h, adapter_ptr(), moved).await.unwrap();
    assert_eq!(active.target(), Some(fitted.adapter_digest.as_str()));
    let adapter = served().await.expect("the activated adapter serves");
    let adapted = adapter.adapt(&query).unwrap();
    assert!(
        top(&core, &adapted).starts_with('p'),
        "the adapter re-aims toward what was cited"
    );
    let audit = "SELECT transition, active FROM decision_pointers ORDER BY seq";
    assert_eq!(
        sql(&h, "decider", audit).await,
        vec![vec![json!("activated"), json!(true)]]
    );

    let rolled = pointer_state(&h, adapter_ptr(), PointerMovement::Rollback)
        .await
        .unwrap();
    assert!(rolled.active.is_none());
    assert_eq!(rolled.history.len(), 2, "both moves are audited");
    assert!(
        served().await.is_none(),
        "rollback serves the base query again"
    );
    let again = pointer_state(&h, adapter_ptr(), PointerMovement::Rollback).await;
    assert!(again.unwrap_err().starts_with("PARAMETER_INVALID"));
}

/// A unified plan's vector ranks (nested ones included) are re-aimed; the
/// rest of the plan is untouched.
#[cfg(feature = "query")]
#[test]
fn a_plan_rewrite_re_aims_every_vector_rank_only() {
    use eg_numeric::decision::adapter::{to_body, Direction};
    let body = to_body(
        &[Direction {
            unit: vec![0.0, 1.0],
            gain: 0.5,
        }],
        "sha256:s",
        2,
        ("sha256:t".to_string(), 1),
    )
    .unwrap();
    let adapter = super::super::served_adapter::ServedAdapter::for_body("d", &body).unwrap();
    let plan = eg_plan::Plan::new(vec![
        eg_plan::Op::Scan {
            label: "Doc".to_string(),
        },
        eg_plan::Op::FuseRrf {
            branches: vec![vec![eg_plan::Op::Rank {
                query: vec![1.0, 1.0],
            }]],
            k: 60.0,
        },
        eg_plan::Op::Rank {
            query: vec![1.0, 1.0],
        },
    ]);
    let rewritten = super::super::served_adapter::adapt_plan(plan, Some(Arc::new(adapter)));
    let expected = eg_plan::Op::Rank {
        query: vec![1.0, 1.5],
    };
    assert_eq!(rewritten.ops[2], expected);
    let eg_plan::Op::FuseRrf { branches, .. } = &rewritten.ops[1] else {
        panic!("fuse")
    };
    assert_eq!(branches[0][0], expected);
    assert!(matches!(rewritten.ops[0], eg_plan::Op::Scan { .. }));
}

fn axis(dims: usize, index: usize, scale: f32) -> Vec<f32> {
    let mut v = vec![0.0; dims];
    v[index] = scale;
    v
}

fn plus(mut left: Vec<f32>, right: &[f32]) -> Vec<f32> {
    left.iter_mut().zip(right).for_each(|(l, r)| *l += r);
    left
}

/// Six judged runs over two generations of the same rows: in the active one
/// the query prefers the shared `n` direction, in the shadow each query finds
/// its own cited row first.
async fn two_generations(h: &Harness) -> Vec<GenerationEvalItem> {
    let active_rows = (0..6).flat_map(|i| {
        [
            (format!("p{i}"), axis(8, i, 1.0)),
            (format!("n{i}"), plus(axis(8, 6, 1.0), &axis(8, i, 0.1))),
        ]
    });
    let shadow_rows = (0..6).flat_map(|i| {
        [
            (format!("p{i}"), axis(8, i, 1.0)),
            (format!("n{i}"), axis(8, 6, 1.0)),
        ]
    });
    graph_with(h, "kg-gen", "1", 8, active_rows.collect()).await;
    graph_with(h, "kg-gen-2", "2", 8, shadow_rows.collect()).await;
    let template_entry = executed_template(h).await;
    let mut items = Vec::new();
    for i in 0..6 {
        let record_id = format!("gen-{i}");
        clone_executed(h, &template_entry, &record_id);
        let (n, p) = (format!("n{i}"), format!("p{i}"));
        let attested = outcome(&record_id, &[&n, &p], &[&p]);
        let recorded = learn(h, "decider", attest(attested)).await;
        assert!(recorded.is_ok(), "{recorded:?}");
        evaluate_by(h, "evaluator", &record_id, true).await;
        let active = plus(axis(8, 6, 1.0), &axis(8, i, 0.5));
        let shadow = axis(8, i, 1.0);
        let to_q16 = |v: Vec<f32>| q16(&v.into_iter().map(f64::from).collect::<Vec<_>>());
        items.push(GenerationEvalItem {
            record_id,
            active_q16: to_q16(active),
            shadow_q16: to_q16(shadow),
        });
    }
    items
}

fn generation_request(
    items: Vec<GenerationEvalItem>,
    max_score_psi_q16: i64,
) -> GenerationEvalRequest {
    GenerationEvalRequest {
        logical: "kg-gen".to_string(),
        active_graph: "kg-gen".to_string(),
        active_space: space_of("1", 8),
        shadow_graph: "kg-gen-2".to_string(),
        shadow_space: space_of("2", 8),
        items: BoundedVec::new(items).unwrap(),
        top_k: 5,
        min_eval_items: 5,
        max_score_psi_q16,
    }
}

async fn evaluate_generation_op(
    h: &Harness,
    request: GenerationEvalRequest,
) -> GenerationEvaluated {
    let write = LearningWrite::EvaluateGeneration {
        request: Box::new(request),
    };
    match learn(h, "decider", write).await.unwrap() {
        LearningRecorded::Generation(evaluated) => *evaluated,
        other => panic!("generation: {other:?}"),
    }
}

#[cfg(feature = "ann")]
#[tokio::test]
async fn a_shadow_generation_is_resolved_only_with_its_receipt_and_rolls_back() {
    let h = Harness::with_isolation(ServerState::test_isolation("decider")).await;
    let items = two_generations(&h).await;
    // Every top-1 score moved (0.93 -> 1.0): past the default PSI bound the
    // shadow cannot pass, however much better it ranks.
    let shifted = evaluate_generation_op(&h, generation_request(items.clone(), 1 << 14)).await;
    assert!(!shifted.receipt.passed, "{:?}", shifted.receipt);
    assert!(shifted.receipt.score_psi_q16.unwrap() > 1 << 14);
    let evaluated = evaluate_generation_op(&h, generation_request(items, 64 << 16)).await;
    let receipt = &evaluated.receipt;
    assert!(receipt.passed, "{receipt:?}");
    assert_eq!((receipt.n_eval, receipt.wins), (6, 6));
    assert!(receipt.shadow_mrr_q16 > receipt.active_mrr_q16);

    let generation = || PointerRef::Generation {
        logical: "kg-gen".to_string(),
    };
    let failed = pointer_state(
        &h,
        generation(),
        activate("kg-gen-2", &shifted.receipt_digest),
    )
    .await;
    assert!(failed
        .unwrap_err()
        .starts_with("EVALUATION_RECEIPT_MISMATCH"));
    let foreign = pointer_state(
        &h,
        generation(),
        activate("kg-other", &evaluated.receipt_digest),
    )
    .await;
    assert!(foreign
        .unwrap_err()
        .starts_with("EVALUATION_RECEIPT_MISMATCH"));
    let moved = pointer_state(
        &h,
        generation(),
        activate("kg-gen-2", &evaluated.receipt_digest),
    )
    .await
    .unwrap();
    assert_eq!(moved.target(), Some("kg-gen-2"));
    let replay = pointer_state(
        &h,
        generation(),
        activate("kg-gen-2", &evaluated.receipt_digest),
    )
    .await
    .unwrap();
    assert_eq!(
        replay, moved,
        "re-activating the active generation is a replay"
    );
    let active =
        "SELECT target FROM decision_pointers WHERE active AND pointer_key = 'generation:kg-gen'";
    assert_eq!(
        sql(&h, "decider", active).await,
        vec![vec![json!("kg-gen-2")]]
    );

    let rolled = pointer_state(&h, generation(), PointerMovement::Rollback)
        .await
        .unwrap();
    assert_eq!(
        rolled.target(),
        None,
        "the logical graph resolves to itself again"
    );
    assert_eq!(rolled.history.len(), 2);
}
