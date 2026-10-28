use std::collections::BTreeMap;

use eg_types::retrieval_verdict::{
    ChunkScorePair, FallbackPolicy, HierarchyCommunity, HierarchyRetrievalPolicy, HierarchyStage,
    RepairStep, ScoreNormalization, ScoreRegion, SelectionPolicy, Verdict,
    RETRIEVAL_BUDGET_EXCEEDED, RETRIEVAL_HIERARCHY_INVALID, RETRIEVAL_NORMALIZATION_INVALID,
    RETRIEVAL_POLICY_UNVERIFIED, RETRIEVAL_ROUND_LIMIT_REQUIRED, RETRIEVAL_SCORE_PAIR_REQUIRED,
};

use super::*;

fn rows(n: usize) -> RowSet {
    RowSet::from_ids((0..n).map(|i| format!("r{i}")))
}

fn call(round: u32, fallback: FallbackPolicy) -> SufficiencyCall {
    SufficiencyCall {
        round,
        round_limit: Some(3),
        k: 8,
        fallback_mode: "text".into(),
        fallback,
    }
}

// spec: EG-FEDERATED-QUERY-R081.2
#[test]
fn repair_order_ends_at_round_limit() {
    let rule = SufficiencyRule::default();
    assert_eq!(rule.name, "min_evidence_count");
    let repairs: Vec<RepairStep> = (1..=3)
        .map(|round| {
            let v =
                judge_sufficiency(&rows(2), &rule, &call(round, FallbackPolicy::Lenient)).unwrap();
            assert_eq!(v.verdict, Verdict::Insufficient);
            assert_eq!(v.evidence_count, 2);
            assert_eq!(v.round, round);
            v.repair
        })
        .collect();
    assert_eq!(
        repairs,
        vec![
            RepairStep::BoostK { k: 16 },
            RepairStep::SwitchMode {
                mode: "text".into()
            },
            RepairStep::Stop,
        ]
    );
    let mut capped = call(1, FallbackPolicy::Lenient);
    capped.k = 48;
    let v = judge_sufficiency(&rows(0), &rule, &capped).unwrap();
    assert_eq!(v.repair, RepairStep::BoostK { k: 64 });

    let enough = judge_sufficiency(&rows(3), &rule, &call(1, FallbackPolicy::Lenient)).unwrap();
    assert_eq!(enough.verdict, Verdict::Sufficient);
    assert_eq!(enough.repair, RepairStep::Stop);

    let replaced = SufficiencyRule {
        name: "min_evidence_count".into(),
        min_evidence: 2,
    };
    let v = judge_sufficiency(&rows(2), &replaced, &call(1, FallbackPolicy::Lenient)).unwrap();
    assert_eq!(v.verdict, Verdict::Sufficient);
}

// spec: EG-FEDERATED-QUERY-R081.4
#[test]
fn strict_insufficient_abstains() {
    let rule = SufficiencyRule::default();
    let mut no_limit = call(1, FallbackPolicy::Strict);
    no_limit.round_limit = None;
    let refusal = judge_sufficiency(&rows(1), &rule, &no_limit).unwrap_err();
    assert_eq!(refusal.code, RETRIEVAL_ROUND_LIMIT_REQUIRED);
    assert!(refusal.detail.contains("round_limit"));

    let strict = judge_sufficiency(&rows(1), &rule, &call(1, FallbackPolicy::Strict)).unwrap();
    assert_eq!(strict.fallback, FallbackPolicy::Strict);
    assert_eq!(answer_mode(&strict), AnswerMode::Abstain);
    let lenient = judge_sufficiency(&rows(1), &rule, &call(1, FallbackPolicy::Lenient)).unwrap();
    assert_eq!(answer_mode(&lenient), AnswerMode::Extractive);
    let enough = judge_sufficiency(&rows(5), &rule, &call(1, FallbackPolicy::Strict)).unwrap();
    assert_eq!(answer_mode(&enough), AnswerMode::Answer);
}

fn policy() -> SelectionPolicy {
    SelectionPolicy {
        policy_revision: "p1".into(),
        corpus_revision: "c1".into(),
        embedding_model_revision: "emb@1".into(),
        cross_encoder_model_revision: "ce@1".into(),
        normalization: ScoreNormalization::Identity,
        embedding_threshold: 0.7,
        relevance_threshold: 0.6,
        fusion_weight: 0.3,
        fusion_threshold: 0.4,
        max_chunks: 3,
        activation_benchmark_digest: Some("sha256:bench".into()),
    }
}

fn pair(id: &str, e: f64, r: f64) -> ChunkScorePair {
    ChunkScorePair {
        chunk_id: id.into(),
        corpus_revision: "c1".into(),
        embedding_score: e,
        cross_encoder_score: r,
        embedding_model_revision: "emb@1".into(),
        cross_encoder_model_revision: "ce@1".into(),
    }
}

const BENCH: Option<&str> = Some("sha256:bench");

// spec: EG-FEDERATED-QUERY-R081.7.2
#[test]
fn dual_score_golden_cases_rescue_reject_and_count() {
    let receipt = select_chunks(
        &policy(),
        BENCH,
        &[
            pair("high", 0.9, 0.9),
            pair("rescue", 0.2, 0.9),
            pair("low", 0.2, 0.1),
            pair("residual-out", 0.9, 0.1),
            pair("residual-in", 0.9, 0.5),
        ],
    )
    .unwrap();
    let region = |id: &str| {
        receipt
            .candidates
            .iter()
            .find(|c| c.chunk_id == id)
            .unwrap()
    };
    assert_eq!(region("high").region, ScoreRegion::DualHigh);
    assert_eq!(region("rescue").region, ScoreRegion::Rescue);
    assert_eq!(region("low").region, ScoreRegion::DualLow);
    assert_eq!(region("residual-out").region, ScoreRegion::Residual);
    assert!((region("residual-out").fused_score - 0.34).abs() < 1e-9);
    assert!(!region("residual-out").selected);
    assert_eq!(receipt.selected, vec!["high", "rescue", "residual-in"]);
    assert_eq!(receipt.selection_inference_calls, 0);

    // The count is query dependent and never backfilled from rejected chunks.
    let narrow = select_chunks(
        &policy(),
        BENCH,
        &[pair("a", 0.9, 0.9), pair("b", 0.1, 0.1)],
    )
    .unwrap();
    assert_eq!(narrow.selected, vec!["a"]);
    // Ties break on the stable chunk id.
    let ties = select_chunks(
        &policy(),
        BENCH,
        &[
            pair("d", 0.8, 0.8),
            pair("b", 0.8, 0.8),
            pair("c", 0.8, 0.8),
            pair("a", 0.8, 0.8),
        ],
    )
    .unwrap();
    assert_eq!(ties.selected, vec!["a", "b", "c"]);
    // Constant columns follow the declared min-max rule.
    let mut minmax = policy();
    minmax.normalization = ScoreNormalization::MinMax {
        constant_value: 1.0,
    };
    let constant =
        select_chunks(&minmax, BENCH, &[pair("x", 3.0, 3.0), pair("y", 3.0, 3.0)]).unwrap();
    assert_eq!(constant.selected, vec!["x", "y"]);
    assert!(select_chunks(&policy(), BENCH, &[])
        .unwrap()
        .selected
        .is_empty());
}

// spec: EG-FEDERATED-QUERY-R081.7.4
#[test]
fn selection_refusals_fail_before_answer_assembly() {
    let missing = select_chunks(&policy(), BENCH, &[pair("a", f64::NAN, 0.9)]).unwrap_err();
    assert_eq!(missing.code, RETRIEVAL_SCORE_PAIR_REQUIRED);
    let mut stale = pair("a", 0.9, 0.9);
    stale.corpus_revision = "c0".into();
    assert_eq!(
        select_chunks(&policy(), BENCH, &[stale]).unwrap_err().code,
        RETRIEVAL_SCORE_PAIR_REQUIRED
    );
    let undefined = select_chunks(&policy(), BENCH, &[pair("a", 4.0, 0.9)]).unwrap_err();
    assert_eq!(undefined.code, RETRIEVAL_NORMALIZATION_INVALID);
    assert_eq!(
        select_chunks(&policy(), None, &[pair("a", 0.9, 0.9)])
            .unwrap_err()
            .code,
        RETRIEVAL_POLICY_UNVERIFIED
    );
    assert_eq!(
        select_chunks(&policy(), Some("sha256:other"), &[pair("a", 0.9, 0.9)])
            .unwrap_err()
            .code,
        RETRIEVAL_POLICY_UNVERIFIED
    );
    // Dual-low-only input selects nothing, and the verdict is insufficient.
    let empty = select_chunks(
        &policy(),
        BENCH,
        &[pair("a", 0.1, 0.1), pair("b", 0.2, 0.2)],
    )
    .unwrap();
    assert!(empty.selected.is_empty());
    let evidence = RowSet::from_ids(empty.selected.clone());
    let v = judge_sufficiency(
        &evidence,
        &SufficiencyRule::default(),
        &call(1, FallbackPolicy::Strict),
    )
    .unwrap();
    assert_eq!(v.verdict, Verdict::Insufficient);
}

struct Fixture {
    scores: BTreeMap<String, f64>,
    tenants: BTreeMap<String, String>,
    calls: Vec<(HierarchyStage, String)>,
}

impl HierarchySource for Fixture {
    fn score(&mut self, stage: HierarchyStage, id: &str) -> StageScore {
        self.calls.push((stage, id.to_string()));
        StageScore {
            score: self.scores.get(id).copied().unwrap_or(0.0),
            tokens: 10,
            cost: 1,
        }
    }

    fn entity_tenant(&self, entity_id: &str) -> Option<String> {
        self.tenants.get(entity_id).cloned()
    }
}

fn community(
    id: &str,
    parent: Option<&str>,
    tenant: &str,
    entities: &[&str],
) -> HierarchyCommunity {
    HierarchyCommunity {
        community_id: id.into(),
        parent_id: parent.map(str::to_string),
        summary_id: format!("s-{id}"),
        summary_revision: "h1".into(),
        tenant: tenant.into(),
        entity_ids: entities.iter().map(|e| e.to_string()).collect(),
    }
}

fn hierarchy() -> Vec<HierarchyCommunity> {
    vec![
        community("A", None, "t1", &[]),
        community("B", None, "t1", &[]),
        community("Z", None, "t2", &[]),
        community("A1", Some("A"), "t1", &["e1", "x9", "e2"]),
        community("A2", Some("A"), "t1", &["e2", "e3"]),
        community("B1", Some("B"), "t1", &["e4"]),
        community("Z1", Some("Z"), "t2", &["z1"]),
    ]
}

fn fixture() -> Fixture {
    let scores = [
        ("s-A", 0.9),
        ("s-B", 0.4),
        ("s-Z", 1.0),
        ("s-A1", 0.8),
        ("s-A2", 0.7),
        ("s-B1", 0.9),
        ("e1", 0.5),
        ("e2", 0.9),
        ("e3", 0.7),
        ("e4", 0.99),
        ("x9", 1.0),
        ("z1", 1.0),
    ];
    let tenants = [
        ("e1", "t1"),
        ("e2", "t1"),
        ("e3", "t1"),
        ("e4", "t1"),
        ("x9", "t2"),
        ("z1", "t2"),
    ];
    Fixture {
        scores: scores.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        tenants: tenants
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        calls: Vec::new(),
    }
}

fn hpolicy(beam: u32) -> HierarchyRetrievalPolicy {
    HierarchyRetrievalPolicy {
        hierarchy_revision: "h1".into(),
        policy_revision: "p1".into(),
        beam_width: beam,
        max_depth: 3,
        max_visited_communities: 16,
        max_entity_candidates: 16,
        max_reranker_calls: 32,
        max_tokens: 1_000,
        max_cost: 100,
    }
}

// spec: EG-FEDERATED-QUERY-R081.9.2
#[test]
fn hierarchy_descent_has_exact_stages_paths_and_entities() {
    let mut source = fixture();
    let receipt = descend_hierarchy(&hpolicy(1), "t1", &hierarchy(), &mut source).unwrap();
    let stages: Vec<_> = source
        .calls
        .iter()
        .map(|(s, id)| (*s, id.as_str()))
        .collect();
    assert_eq!(
        stages,
        vec![
            (HierarchyStage::Summary, "s-A"),
            (HierarchyStage::Summary, "s-B"),
            (HierarchyStage::Community, "s-A1"),
            (HierarchyStage::Community, "s-A2"),
            (HierarchyStage::Entity, "e1"),
            (HierarchyStage::Entity, "e2"),
        ]
    );
    let entity_ids: Vec<_> = receipt.entities.iter().map(|v| v.id.as_str()).collect();
    assert_eq!(entity_ids, vec!["e2"]);
    assert_eq!(receipt.entities[0].path, vec!["A", "A1"]);
    assert_eq!((receipt.reranker_calls, receipt.cost), (6, 6));
    assert!(!receipt.incomplete);

    // A wider beam visits more and pays for it; entities stay deduplicated with
    // their first community path, and another tenant's entity is never scored.
    let mut wide = fixture();
    let receipt = descend_hierarchy(&hpolicy(2), "t1", &hierarchy(), &mut wide).unwrap();
    assert!(receipt.reranker_calls > 6);
    assert_eq!(receipt.cost, u64::from(receipt.reranker_calls));
    let entity_ids: Vec<_> = receipt.entities.iter().map(|v| v.id.as_str()).collect();
    assert_eq!(entity_ids, vec!["e4", "e2"]);
    let e2_paths: Vec<_> = receipt.visits.iter().filter(|v| v.id == "e2").collect();
    assert_eq!(e2_paths.len(), 1);
    assert!(wide
        .calls
        .iter()
        .all(|(_, id)| !["s-Z", "s-Z1", "x9", "z1"].contains(&id.as_str())));
}

// spec: EG-FEDERATED-QUERY-R081.9.4
#[test]
fn hierarchy_refusals_are_typed_and_bounded() {
    let mut cyclic = hierarchy();
    cyclic[0].parent_id = Some("A1".into());
    let mut source = fixture();
    let refused = descend_hierarchy(&hpolicy(1), "t1", &cyclic, &mut source).unwrap_err();
    assert_eq!(refused.refusal.code, RETRIEVAL_HIERARCHY_INVALID);
    assert!(source.calls.is_empty());

    let mut stale = hierarchy();
    stale[1].summary_revision = "h0".into();
    let refused = descend_hierarchy(&hpolicy(1), "t1", &stale, &mut fixture()).unwrap_err();
    assert_eq!(refused.refusal.code, RETRIEVAL_HIERARCHY_INVALID);

    let mut dangling = hierarchy();
    dangling[3].entity_ids.push("ghost".into());
    let refused = descend_hierarchy(&hpolicy(1), "t1", &dangling, &mut fixture()).unwrap_err();
    assert_eq!(refused.refusal.code, RETRIEVAL_HIERARCHY_INVALID);
    assert!(refused.partial.incomplete);

    let mut tight = hpolicy(2);
    tight.max_reranker_calls = 3;
    let mut source = fixture();
    let refused = descend_hierarchy(&tight, "t1", &hierarchy(), &mut source).unwrap_err();
    assert_eq!(refused.refusal.code, RETRIEVAL_BUDGET_EXCEEDED);
    assert_eq!(source.calls.len(), 3);
    assert_eq!(refused.partial.reranker_calls, 3);
    assert!(refused.partial.incomplete);
    assert_eq!(
        refused.partial.visits.len(),
        2,
        "completed summary stage is kept"
    );

    let mut shallow = hpolicy(1);
    shallow.max_depth = 1;
    let mut deep = hierarchy();
    deep.push(community("A1x", Some("A1"), "t1", &["e1"]));
    let refused = descend_hierarchy(&shallow, "t1", &deep, &mut fixture()).unwrap_err();
    assert_eq!(refused.refusal.code, RETRIEVAL_BUDGET_EXCEEDED);
    assert!(refused.refusal.detail.contains("depth"));

    let mut poor = hpolicy(1);
    poor.max_cost = 2;
    let refused = descend_hierarchy(&poor, "t1", &hierarchy(), &mut fixture()).unwrap_err();
    assert_eq!(refused.refusal.code, RETRIEVAL_BUDGET_EXCEEDED);
    assert!(refused.refusal.detail.contains("cost"));
}
