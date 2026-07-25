//! The O(Δ) maintenance-cost proof (CONCEPT:EG-KG.storage.incremental-matview).
//!
//! The DBSP value proposition, made into a checkable assertion: maintaining an
//! incremental view under a delta batch touches a number of state entries bounded by the
//! DELTA size (times a small constant number of membership stages), INDEPENDENT of the
//! view size — O(delta), not O(view). The recompute path, by contrast, rescans the whole
//! graph (O(view)) on every change.
//!
//! Instrument (per the acceptance): `Circuit::apply` returns the rows-touched count; this
//! sweeps the view size across N ∈ {1k, 10k, 100k}, applies a fixed-size delta batch at
//! each rung, and asserts the touched count stays CONSTANT while the view grows 100× — so
//! the touched/view ratio collapses toward 0. The numbers are logged.

#![cfg(feature = "query")]

use eg_plan::{Circuit, Delta, Op, Plan, Pred, ZRow};
use serde_json::{json, Map, Value};

fn doc_props(year: i64) -> Map<String, Value> {
    json!({ "type": "Doc", "year": year })
        .as_object()
        .unwrap()
        .clone()
}

/// Seed a circuit for `plan` with `n` Doc nodes (year 2000+). Seeding is itself O(n) once,
/// at define time — the claim under test is the PER-DELTA maintenance cost AFTER that.
fn seed(plan: &Plan, n: usize) -> Circuit {
    let mut c = Circuit::compile(plan).unwrap();
    for i in 0..n {
        c.apply(&Delta::from(vec![ZRow::insert(
            format!("n{i}"),
            doc_props(2000 + (i as i64 % 25)),
        )]));
    }
    c
}

fn scan_only() -> Plan {
    Plan::new(vec![Op::Scan {
        label: "Doc".into(),
    }])
}

fn scan_filter() -> Plan {
    Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 0.0,
            }],
        },
    ])
}

#[test]
fn maintenance_cost_is_o_delta_not_o_view() {
    let rungs = [1_000usize, 10_000, 100_000];

    // Sweep the view size for BOTH a 1-stage (Scan) and a 2-stage (Scan+Filter) plan; the
    // touched count must be constant across the 100× sweep (== batch_rows × matched_stages),
    // never a function of view size.
    for (label, plan, expected_touched) in [
        ("scan", scan_only(), 2),        // update = retract+insert, 1 stage each  = 2
        ("scan+filter", scan_filter(), 4), // update = retract+insert, 2 stages each = 4
    ] {
        let mut touched_seq = Vec::new();
        for &n in &rungs {
            let mut c = seed(&plan, n);
            // A single-node update (retract old image + insert new) — the canonical DBSP
            // "update" idiom, a batch of 2 signed rows.
            let delta = Delta::from(vec![
                ZRow::retract("n0", doc_props(2000)),
                ZRow::insert("n0", doc_props(2001)),
            ]);
            let touched = c.apply(&delta);
            let view_size = c.current().len();
            let ratio = touched as f64 / view_size as f64;
            println!(
                "O(delta) [{label:>11}] view_size={view_size:>7}  delta_rows={:>2}  \
                 touched={touched:>2}  touched/view={ratio:.6}",
                delta.len(),
            );
            assert_eq!(
                touched, expected_touched,
                "[{label}] maintenance touched {touched} for a 2-row delta at view_size={view_size} \
                 — expected O(delta)={expected_touched}, got what looks like O(view)"
            );
            touched_seq.push((view_size, touched));
        }

        // The ratio must collapse as the view grows 100× while touched stays flat.
        let (small_view, small_touched) = touched_seq.first().copied().unwrap();
        let (big_view, big_touched) = touched_seq.last().copied().unwrap();
        assert_eq!(
            small_touched, big_touched,
            "[{label}] touched must be constant across a 100× view-size sweep (O(delta))"
        );
        assert!(
            big_view >= small_view * 90,
            "[{label}] the sweep must grow the view ~100× (got {small_view} -> {big_view})"
        );
        let big_ratio = big_touched as f64 / big_view as f64;
        assert!(
            big_ratio < 0.001,
            "[{label}] touched/view must collapse toward 0 (got {big_ratio:.6} at \
             view_size={big_view})"
        );
    }
}

#[test]
fn single_insert_touches_one_stage_regardless_of_size() {
    // A 1-stage (Scan) plan: a single matching insert touches exactly one entry; a
    // non-matching row touches nothing — independent of view size.
    for n in [1_000usize, 50_000] {
        let mut c = seed(&scan_only(), n);
        assert_eq!(
            c.apply(&Delta::from(vec![ZRow::insert("fresh", doc_props(2010))])),
            1,
            "a single matching insert touches exactly one stage (view_size={n})"
        );
        let non_doc = json!({ "type": "Other" }).as_object().unwrap().clone();
        assert_eq!(
            c.apply(&Delta::from(vec![ZRow::insert("skip", non_doc)])),
            0,
            "a non-matching row touches nothing (view_size={n})"
        );
    }
}
