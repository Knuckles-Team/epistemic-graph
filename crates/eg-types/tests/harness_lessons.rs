//! The improvement library (MEDULLA-ORCH-VOCAB-R802): the serde shape of the
//! adopted harness, the contrast and the lesson, the
//! measurable-improvement retention gate, the confidence-ranked bitemporal
//! query, the supersession behavior and the typed refusals.

use eg_types::contract::BoundedVec;
use eg_types::orchestration::improvement::{
    current_adopted_harness, register_adoption, register_lesson, retrieve_lessons, AdoptedHarness,
    HarnessContrast, HarnessLesson, HarnessProbeRun, ImprovementLibraryRefusal, LessonEvidence,
    LessonSupersession,
};
use eg_types::orchestration::HarnessComponentKind;

fn bounded<const MAXIMUM: usize>(values: Vec<String>) -> BoundedVec<String, MAXIMUM> {
    BoundedVec::new(values).unwrap()
}

/// One probe run under the fixed contrast conditions.
fn probe_run(trace_id: &str) -> HarnessProbeRun {
    HarnessProbeRun {
        trace_id: trace_id.to_string(),
        conditions_digest: "sha256:conditions".to_string(),
    }
}

/// One contrast of the two fixed probe runs; the four deltas are B minus A on
/// the Q32 scale, so a negative retry or cost delta and a positive tool
/// selection or evaluation delta is the improving direction.
fn contrast(
    retry_delta: i64,
    tool_delta: i64,
    eval_delta: i64,
    cost_delta: i64,
) -> HarnessContrast {
    HarnessContrast {
        task_ref: "task:demo".to_string(),
        conditions_digest: "sha256:conditions".to_string(),
        run_a_trace_id: "trace-a".to_string(),
        run_b_trace_id: "trace-b".to_string(),
        retry_delta,
        tool_selection_delta: tool_delta,
        eval_score_delta: eval_delta,
        cost_delta,
    }
}

/// One lesson for the fixed task class and deficiency, citing both evidence
/// runs.
fn lesson(confidence: f64, what_changed: &str, applicability: Vec<String>) -> HarnessLesson {
    HarnessLesson {
        task_class: "task:demo".to_string(),
        deficiency: "deficiency:retries".to_string(),
        situation: "the run retried the failing tool".to_string(),
        changed_component_kind: HarnessComponentKind::SystemPrompt,
        what_changed: what_changed.to_string(),
        applicability: bounded::<64>(applicability),
        confidence,
        evidence_run_ids: bounded::<32>(vec!["trace-a".to_string(), "trace-b".to_string()]),
        supersedes_id: None,
    }
}

/// One adoption of the fixed agent and task class.
fn adoption(version_refs: &[&str]) -> AdoptedHarness {
    AdoptedHarness {
        agent_ref: "agent:demo".to_string(),
        task_class: "task:demo".to_string(),
        component_version_refs: bounded::<8>(
            version_refs.iter().map(|ref_| ref_.to_string()).collect(),
        ),
        decision_ref: "decision:demo".to_string(),
        contrast_ref: Some("harness-contrast:sha256:conditions:trace-a:trace-b".to_string()),
    }
}

// spec: MEDULLA-ORCH-VOCAB-R802.1
#[test]
fn improvement_records_round_trip() {
    // The adopted harness keeps every field by name.
    let adoption = adoption(&[
        "harness-component:comp:prompt:sha256:a",
        "harness-component:comp:memory:sha256:b",
    ]);
    let json = serde_json::to_string(&adoption).unwrap();
    let back: AdoptedHarness = serde_json::from_str(&json).unwrap();
    assert_eq!(back, adoption);
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    for field in [
        "agent_ref",
        "task_class",
        "component_version_refs",
        "decision_ref",
        "contrast_ref",
    ] {
        assert!(value.get(field).is_some(), "field {field} missing");
    }

    // The contrast keeps all eight of its fields.
    let contrast = contrast(-1, 1, 2, -1);
    let json = serde_json::to_string(&contrast).unwrap();
    let back: HarnessContrast = serde_json::from_str(&json).unwrap();
    assert_eq!(back, contrast);
    for field in [
        "task_ref",
        "conditions_digest",
        "run_a_trace_id",
        "run_b_trace_id",
        "retry_delta",
        "tool_selection_delta",
        "eval_score_delta",
        "cost_delta",
    ] {
        assert!(value_key(&json, field), "field {field} missing");
    }
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json).unwrap()["retry_delta"],
        -1
    );

    // The lesson keeps all nine of its fields, including the supersession
    // pointer when one is set.
    let mut lesson = lesson(
        0.85,
        "tightened the retry policy",
        vec!["feature:long-context".to_string()],
    );
    lesson.supersedes_id =
        Some("harness-lesson:task:demo:deficiency:retries:sha256:old".to_string());
    let json = serde_json::to_string(&lesson).unwrap();
    let back: HarnessLesson = serde_json::from_str(&json).unwrap();
    assert_eq!(back, lesson);
    for field in [
        "task_class",
        "deficiency",
        "situation",
        "changed_component_kind",
        "what_changed",
        "applicability",
        "confidence",
        "evidence_run_ids",
        "supersedes_id",
    ] {
        assert!(value_key(&json, field), "field {field} missing");
    }
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json).unwrap()["changed_component_kind"],
        "system_prompt"
    );

    // The probe run keeps its trace identity and its conditions digest.
    let run = probe_run("trace-a");
    let json = serde_json::to_string(&run).unwrap();
    let back: HarnessProbeRun = serde_json::from_str(&json).unwrap();
    assert_eq!(back, run);
    for field in ["trace_id", "conditions_digest"] {
        assert!(value_key(&json, field), "field {field} missing");
    }

    // The shapes are strict: an unknown field is refused, not dropped.
    let unknown_adoption = json.replace('}', ",\"unknown_field\":1}");
    assert!(serde_json::from_str::<HarnessProbeRun>(&unknown_adoption).is_err());
    let unknown_lesson = json.replace('}', ",\"unknown_field\":1}");
    assert!(serde_json::from_str::<HarnessLesson>(&unknown_lesson).is_err());
    let unknown_contrast = json.replace('}', ",\"unknown_field\":1}");
    assert!(serde_json::from_str::<HarnessContrast>(&unknown_contrast).is_err());
}

/// Whether `json` (a flat object) carries the named field.
fn value_key(json: &str, field: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(json)
        .unwrap()
        .get(field)
        .is_some()
}

// spec: MEDULLA-ORCH-VOCAB-R802.2
#[test]
fn lesson_requires_measurable_improvement() {
    let mut lessons = Vec::new();
    let mut supersessions = Vec::new();
    let run_a = probe_run("trace-a");
    let run_b = probe_run("trace-b");

    // Every delta moving the wrong way (or not at all) is no improvement.
    let worsening = contrast(1, 0, 0, 1);
    assert_eq!(
        register_lesson(
            &mut lessons,
            &mut supersessions,
            lesson(0.8, "no improvement", vec![]),
            &LessonEvidence {
                contrast: &worsening,
                run_a: &run_a,
                run_b: &run_b,
            },
            10,
            10
        ),
        Err(ImprovementLibraryRefusal::LessonWithoutImprovement)
    );
    assert!(lessons.is_empty());

    // Each of the four improving directions retains on its own: fewer
    // retries, better tool selection, a higher evaluation score, lower cost.
    for (index, deltas) in [
        (0, contrast(-1, 0, 0, 0)),
        (1, contrast(0, 1, 0, 0)),
        (2, contrast(0, 0, 1, 0)),
        (3, contrast(0, 0, 0, -1)),
    ]
    .into_iter()
    {
        let mut kept = lesson(0.5 + index as f64 * 0.1, &format!("change {index}"), vec![]);
        kept.deficiency = format!("deficiency:d{index}");
        register_lesson(
            &mut lessons,
            &mut supersessions,
            kept,
            &LessonEvidence {
                contrast: &deltas,
                run_a: &run_a,
                run_b: &run_b,
            },
            10 + index,
            10 + index,
        )
        .unwrap();
    }
    assert_eq!(lessons.len(), 4);

    // A lesson that does not cite its evidence runs refuses, even when the
    // contrast improves.
    let mut uncited = lesson(0.8, "uncited", vec![]);
    uncited.evidence_run_ids = bounded::<32>(vec!["trace-other".to_string()]);
    assert_eq!(
        register_lesson(
            &mut lessons,
            &mut supersessions,
            uncited,
            &LessonEvidence {
                contrast: &contrast(-1, 0, 0, 0),
                run_a: &run_a,
                run_b: &run_b,
            },
            14,
            14
        ),
        Err(ImprovementLibraryRefusal::LessonWithoutEvidence)
    );
    assert_eq!(lessons.len(), 4);
}

// spec: MEDULLA-ORCH-VOCAB-R802.3
#[test]
fn retrieval_ranks_by_confidence_and_abstains() {
    // The query ranks its current hits by confidence, highest first, and the
    // tie break on node id keeps the order deterministic. The registration
    // path supersedes an older lesson of the same task class and deficiency
    // (the R802.4 test's), so the ranking is exercised over a store state
    // with several current lessons of one task class and deficiency.
    let low = lesson(0.4, "change a", vec!["feature:long-context".to_string()]);
    let mid = lesson(0.7, "change c", vec!["feature:long-context".to_string()]);
    let high = lesson(0.9, "change b", vec![]);
    let lessons = vec![low, mid, high];
    let supersessions: Vec<LessonSupersession> = Vec::new();

    // All three are current: ranked by confidence, highest first.
    let ranked = retrieve_lessons(
        &lessons,
        &supersessions,
        "task:demo",
        "deficiency:retries",
        &[],
        10,
    )
    .unwrap();
    assert_eq!(
        ranked.iter().map(|l| l.confidence).collect::<Vec<_>>(),
        vec![0.9, 0.7, 0.4]
    );

    // The trajectory features filter the hits; a feature none of the lessons
    // carries matches nothing.
    let filtered = retrieve_lessons(
        &lessons,
        &supersessions,
        "task:demo",
        "deficiency:retries",
        &["feature:long-context".to_string()],
        10,
    )
    .unwrap();
    assert_eq!(
        filtered.iter().map(|l| l.confidence).collect::<Vec<_>>(),
        vec![0.7, 0.4]
    );
    assert_eq!(
        retrieve_lessons(
            &lessons,
            &supersessions,
            "task:demo",
            "deficiency:retries",
            &["feature:absent".to_string()],
            10
        ),
        Err(ImprovementLibraryRefusal::NoLessons)
    );

    // A task class or deficiency the library does not hold abstains.
    assert_eq!(
        retrieve_lessons(
            &lessons,
            &supersessions,
            "task:other",
            "deficiency:retries",
            &[],
            10
        ),
        Err(ImprovementLibraryRefusal::NoLessons)
    );
    assert_eq!(
        retrieve_lessons(
            &lessons,
            &supersessions,
            "task:demo",
            "deficiency:other",
            &[],
            10
        ),
        Err(ImprovementLibraryRefusal::NoLessons)
    );

    // Bitemporally with AS OF: the read abstains before the first write,
    // returns the current lesson of each window, and drops the superseded
    // one from its window on.
    let (run_a, run_b, improving) = improving_probe_pair();
    let mut stored = Vec::new();
    let mut stored_supersessions = Vec::new();
    register_lesson(
        &mut stored,
        &mut stored_supersessions,
        lesson(0.5, "first fix", vec![]),
        &LessonEvidence {
            contrast: &improving,
            run_a: &run_a,
            run_b: &run_b,
        },
        10,
        10,
    )
    .unwrap();
    register_lesson(
        &mut stored,
        &mut stored_supersessions,
        lesson(0.8, "second fix", vec![]),
        &LessonEvidence {
            contrast: &improving,
            run_a: &run_a,
            run_b: &run_b,
        },
        20,
        20,
    )
    .unwrap();
    assert_eq!(
        retrieve_lessons(
            &stored,
            &stored_supersessions,
            "task:demo",
            "deficiency:retries",
            &[],
            5
        ),
        Err(ImprovementLibraryRefusal::NoLessons)
    );
    let first_window = retrieve_lessons(
        &stored,
        &stored_supersessions,
        "task:demo",
        "deficiency:retries",
        &[],
        15,
    )
    .unwrap();
    assert_eq!(first_window, vec![&stored[0]]);
    let second_window = retrieve_lessons(
        &stored,
        &stored_supersessions,
        "task:demo",
        "deficiency:retries",
        &[],
        25,
    )
    .unwrap();
    assert_eq!(second_window, vec![&stored[1]]);
}

/// The two probe runs every lesson cites and the improving contrast between
/// them.
fn improving_probe_pair() -> (HarnessProbeRun, HarnessProbeRun, HarnessContrast) {
    (
        probe_run("trace-a"),
        probe_run("trace-b"),
        contrast(-1, 0, 0, 0),
    )
}

// spec: MEDULLA-ORCH-VOCAB-R802.4
#[test]
fn supersession_keeps_old_readable() {
    // Lessons: the newer lesson supersedes the older one of the same task
    // class and deficiency by edge; the older one stays readable and is
    // current at its own time.
    let mut lessons = Vec::new();
    let mut supersessions = Vec::new();
    let (run_a, run_b, improving) = improving_probe_pair();

    let first = lesson(0.5, "first fix", vec![]);
    register_lesson(
        &mut lessons,
        &mut supersessions,
        first.clone(),
        &LessonEvidence {
            contrast: &improving,
            run_a: &run_a,
            run_b: &run_b,
        },
        10,
        10,
    )
    .unwrap();
    let second = lesson(0.8, "second fix", vec![]);
    register_lesson(
        &mut lessons,
        &mut supersessions,
        second.clone(),
        &LessonEvidence {
            contrast: &improving,
            run_a: &run_a,
            run_b: &run_b,
        },
        20,
        20,
    )
    .unwrap();

    // The engine linked the newer lesson to the current one; the head-edge
    // records the first lesson's current-from time.
    assert_eq!(lessons.len(), 2);
    assert_eq!(lessons[1].supersedes_id, Some(first.node_id()));
    assert_eq!(supersessions.len(), 2);
    assert_eq!(supersessions[0].new_version, first.node_id());
    assert_eq!(supersessions[0].superseded_version, String::new());
    assert_eq!(supersessions[1].new_version, second.node_id());
    assert_eq!(supersessions[1].superseded_version, first.node_id());
    assert_eq!(supersessions[1].valid_at, 20);

    // The superseded lesson stays readable in the store.
    assert!(lessons.iter().any(|l| l.node_id() == first.node_id()));

    // Bitemporally: nothing before the first write, the older lesson current
    // before the supersession and gone after, the newer one current from the
    // supersession on.
    assert_eq!(
        retrieve_lessons(
            &lessons,
            &supersessions,
            "task:demo",
            "deficiency:retries",
            &[],
            5
        ),
        Err(ImprovementLibraryRefusal::NoLessons)
    );
    let before = retrieve_lessons(
        &lessons,
        &supersessions,
        "task:demo",
        "deficiency:retries",
        &[],
        15,
    )
    .unwrap();
    assert_eq!(before, vec![&lessons[0]]);
    let after = retrieve_lessons(
        &lessons,
        &supersessions,
        "task:demo",
        "deficiency:retries",
        &[],
        25,
    )
    .unwrap();
    assert_eq!(after, vec![&lessons[1]]);

    // Adoptions: the new adoption supersedes the previous adoption of the
    // same agent and task class by edge; the previous one stays readable.
    let mut adoptions = Vec::new();
    let mut adoption_supersessions = Vec::new();
    let previous = adoption(&["harness-component:comp:prompt:sha256:a"]);
    register_adoption(
        &mut adoptions,
        &mut adoption_supersessions,
        previous.clone(),
        10,
        10,
    )
    .unwrap();
    let next = adoption(&["harness-component:comp:prompt:sha256:b"]);
    register_adoption(
        &mut adoptions,
        &mut adoption_supersessions,
        next.clone(),
        20,
        20,
    )
    .unwrap();

    assert_eq!(adoptions.len(), 2);
    assert_eq!(adoption_supersessions.len(), 2);
    assert_eq!(adoption_supersessions[0].new_version, previous.node_id());
    assert_eq!(adoption_supersessions[0].superseded_version, String::new());
    assert_eq!(adoption_supersessions[1].new_version, next.node_id());
    assert_eq!(
        adoption_supersessions[1].superseded_version,
        previous.node_id()
    );
    assert!(adoptions.iter().any(|a| a.node_id() == previous.node_id()));
    assert!(current_adopted_harness(
        &adoptions,
        &adoption_supersessions,
        "agent:demo",
        "task:demo",
        5
    )
    .is_none());
    assert_eq!(
        current_adopted_harness(
            &adoptions,
            &adoption_supersessions,
            "agent:demo",
            "task:demo",
            15
        ),
        Some(&adoptions[0])
    );
    assert_eq!(
        current_adopted_harness(
            &adoptions,
            &adoption_supersessions,
            "agent:demo",
            "task:demo",
            25
        ),
        Some(&adoptions[1])
    );
    // A different agent or task class is untouched.
    assert!(current_adopted_harness(
        &adoptions,
        &adoption_supersessions,
        "agent:other",
        "task:demo",
        25
    )
    .is_none());
}

// spec: MEDULLA-ORCH-VOCAB-R802.5
#[test]
fn evidence_and_condition_refusals() {
    let mut lessons = Vec::new();
    let mut supersessions = Vec::new();
    let (run_a, run_b, improving) = improving_probe_pair();

    // A lesson without evidence runs refuses, and nothing is stored.
    let mut no_evidence = lesson(0.8, "change", vec![]);
    no_evidence.evidence_run_ids = bounded::<32>(Vec::new());
    assert_eq!(
        register_lesson(
            &mut lessons,
            &mut supersessions,
            no_evidence,
            &LessonEvidence {
                contrast: &improving,
                run_a: &run_a,
                run_b: &run_b,
            },
            10,
            10
        ),
        Err(ImprovementLibraryRefusal::LessonWithoutEvidence)
    );
    assert!(lessons.is_empty());

    // A contrast whose conditions digest differs from its runs refuses.
    let mut mismatched = improving.clone();
    mismatched.conditions_digest = "sha256:other-conditions".to_string();
    assert_eq!(
        register_lesson(
            &mut lessons,
            &mut supersessions,
            lesson(0.8, "change", vec![]),
            &LessonEvidence {
                contrast: &mismatched,
                run_a: &run_a,
                run_b: &run_b,
            },
            10,
            10
        ),
        Err(ImprovementLibraryRefusal::ContrastConditionsMismatch)
    );
    // So does a run that disagrees with the other run's conditions.
    let run_b_wrong = HarnessProbeRun {
        trace_id: "trace-b".to_string(),
        conditions_digest: "sha256:other".to_string(),
    };
    assert_eq!(
        register_lesson(
            &mut lessons,
            &mut supersessions,
            lesson(0.8, "change", vec![]),
            &LessonEvidence {
                contrast: &improving,
                run_a: &run_a,
                run_b: &run_b_wrong,
            },
            10,
            10
        ),
        Err(ImprovementLibraryRefusal::ContrastConditionsMismatch)
    );
    assert!(lessons.is_empty());

    // An adopted harness without a decision reference refuses.
    let mut adoptions = Vec::new();
    let mut adoption_supersessions = Vec::new();
    let mut no_decision = adoption(&["harness-component:comp:prompt:sha256:a"]);
    no_decision.decision_ref = String::new();
    assert_eq!(
        register_adoption(
            &mut adoptions,
            &mut adoption_supersessions,
            no_decision,
            10,
            10
        ),
        Err(ImprovementLibraryRefusal::AdoptedHarnessNoDecision)
    );
    assert!(adoptions.is_empty());
}
