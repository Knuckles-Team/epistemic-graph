//! Event-stream / CEP modality executor proofs (CONCEPT:EG-088).
//!
//! A small `Event` layer, each node carrying `ts`/`key`/`attrs`, drives the CEP surface
//! end-to-end through the fused executor:
//!  * `Op::Cep { pattern }` — interpret the input RowSet as a time-ordered event stream,
//!    run the eg-stream bounded NFA (Sequence/Within/Absence over sliding/tumbling
//!    windows), and keep the rows that participate in a detected match.
//!
//! And a compose proof: `Op::Scan { label: "Event" }` seeds the stream, then `Op::Cep`
//! narrows it — cross-modal graph→stream in ONE plan.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_types::wire::{CepAttrPredSpec, CepMatcherSpec, CepNodeSpec, CepPatternSpec, CepWindowSpec};
use serde_json::json;

use crate::algebra::{Op, Plan};
use crate::exec::PlanCtx;
use crate::PlanExt;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// A layer of `Event` nodes forming two sessions plus a non-event `Doc` distractor:
///   E1 login@10 → E2 logout@15   (a completed session)
///   E3 login@100 → E4 logout@200 (a session whose logout is far away)
///   D0 is a Doc with no `ts` (not an event; must never appear in a CEP result).
fn events() -> GraphView {
    let core = GraphCore::new();
    core.add_node(
        "E1".into(),
        blob(json!({ "type": "Event", "ts": 10, "key": "login" })),
    );
    core.add_node(
        "E2".into(),
        blob(json!({ "type": "Event", "ts": 15, "key": "logout" })),
    );
    core.add_node(
        "E3".into(),
        blob(json!({ "type": "Event", "ts": 100, "key": "login" })),
    );
    core.add_node(
        "E4".into(),
        blob(json!({ "type": "Event", "ts": 200, "key": "logout" })),
    );
    core.add_node("D0".into(), blob(json!({ "type": "Doc", "year": 2025 })));
    core.analysis_snapshot()
}

/// A layer of `Trade` events carrying a numeric `qty` attribute, for the predicate proof:
///   T1 qty=150 @1, T2 qty=5 @2  (a big trade then a small one).
fn trades() -> GraphView {
    let core = GraphCore::new();
    core.add_node(
        "T1".into(),
        blob(json!({ "type": "Trade", "ts": 1, "key": "trade", "attrs": { "qty": 150 } })),
    );
    core.add_node(
        "T2".into(),
        blob(json!({ "type": "Trade", "ts": 2, "key": "trade", "attrs": { "qty": 5 } })),
    );
    core.analysis_snapshot()
}

fn run(plan: &Plan, view: &GraphView) -> Vec<String> {
    let sem = SemanticStore::new();
    let c = PlanCtx::new(view, &sem);
    let mut ids = plan.execute(&c).unwrap().ids();
    ids.sort();
    ids
}

fn key(k: &str) -> CepMatcherSpec {
    CepMatcherSpec {
        key: Some(k.into()),
        preds: vec![],
    }
}

#[test]
fn cep_sequence_within_window_keeps_participants() {
    let view = events();
    // login→logout within a 20-unit sliding window: E1→E2 completes (span 5); the E3
    // login has no logout within 20 (E4 is 100 later). Keep the two matched rows.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::Cep {
            pattern: CepPatternSpec {
                pattern: CepNodeSpec::Sequence(vec![key("login"), key("logout")]),
                window: CepWindowSpec::Sliding { size: 20 },
            },
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["E1", "E2"]);
}

#[test]
fn cep_sequence_exceeding_window_drops_all() {
    let view = events();
    // A 3-unit window is too tight for even E1→E2 (span 5) → no match → no rows kept.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::Cep {
            pattern: CepPatternSpec {
                pattern: CepNodeSpec::Sequence(vec![key("login"), key("logout")]),
                window: CepWindowSpec::Sliding { size: 3 },
            },
        },
    ]);
    assert_eq!(run(&plan, &view), Vec::<String>::new());
}

#[test]
fn cep_absence_keeps_the_unclosed_login() {
    let view = events();
    // login NOT-followed-by logout within 10 units: E1 IS followed (E2 @15, +5) so it is
    // excluded; E3 @100 has no logout within 10 (E4 @200) so E3 is the abandoned session.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::Cep {
            pattern: CepPatternSpec {
                pattern: CepNodeSpec::Absence {
                    a: key("login"),
                    b: key("logout"),
                    within: 10,
                },
                window: CepWindowSpec::Sliding { size: 0 },
            },
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["E3"]);
}

#[test]
fn cep_sequence_with_attribute_predicate() {
    let view = trades();
    // A big trade (qty>100) followed by a small trade (qty<10): T1→T2 satisfies it.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Trade".into(),
        },
        Op::Cep {
            pattern: CepPatternSpec {
                pattern: CepNodeSpec::Sequence(vec![
                    CepMatcherSpec {
                        key: Some("trade".into()),
                        preds: vec![CepAttrPredSpec::Gt {
                            field: "qty".into(),
                            value: 100.0,
                        }],
                    },
                    CepMatcherSpec {
                        key: Some("trade".into()),
                        preds: vec![CepAttrPredSpec::Lt {
                            field: "qty".into(),
                            value: 10.0,
                        }],
                    },
                ]),
                window: CepWindowSpec::Sliding { size: 10 },
            },
        },
        Op::Limit { k: 10 },
    ]);
    assert_eq!(run(&plan, &view), vec!["T1", "T2"]);
}

#[test]
fn cep_within_tightens_and_tumbling_bucketing() {
    let view = events();
    // Within(3) wrapping the login→logout sequence rejects E1→E2 (span 5) → no rows.
    let tight = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::Cep {
            pattern: CepPatternSpec {
                pattern: CepNodeSpec::Within {
                    within: 3,
                    pattern: Box::new(CepNodeSpec::Sequence(vec![key("login"), key("logout")])),
                },
                window: CepWindowSpec::Sliding { size: 100 },
            },
        },
    ]);
    assert_eq!(run(&tight, &view), Vec::<String>::new());

    // A tumbling window of size 100 puts E1(10)/E2(15) in bucket 0 (match) but E3(100) in
    // bucket 1 and E4(200) in bucket 2 — no cross-bucket pair. Only E1,E2 are kept.
    let tumbling = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::Cep {
            pattern: CepPatternSpec {
                pattern: CepNodeSpec::Sequence(vec![key("login"), key("logout")]),
                window: CepWindowSpec::Tumbling { size: 100 },
            },
        },
    ]);
    assert_eq!(run(&tumbling, &view), vec!["E1", "E2"]);
}
