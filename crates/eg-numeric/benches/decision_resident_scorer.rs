//! EG-DECISION-ENGINE-R090.3: Linux native resource evidence, JSON on stdout.
//! Run with --profile release; see the owning test-spec for provenance.
#![allow(dead_code, unused_imports)]

#[path = "../tests/decision/common.rs"]
mod common;
#[path = "../tests/decision/fit_eval.rs"]
mod fit_eval;

use common::{bounded, SCHEMA_DIGEST};
use eg_numeric::decision::scorer::fixed::ONE;
use eg_numeric::decision::scorer::forward::read;
use eg_numeric::decision::scorer::legal::LegalSet;
use eg_types::decision::statistical::head::{
    DecisionHeadBody, FeatureStandardisation, FittedRegime, HeadKind, DECISION_HEAD_SCHEMA_VERSION,
};
use eg_types::decision::statistical::scorer::{
    OptionAttentionParams, ResidentScorerBudget, MAX_SCORER_FEATURES, MAX_SCORER_SHORTLIST,
    MAX_SCORER_WIDTH, RESIDENT_SCORER_CEILING,
};
use eg_types::decision::{QuantScaleTag, QuantisedValue};
use serde_json::{json, Value};
use std::hint::black_box;

const SAMPLES: usize = 1_000;
const WARMUP: usize = 100;

fn q(value: i64) -> QuantisedValue {
    QuantisedValue {
        scale: QuantScaleTag::Q32,
        value,
    }
}

fn pattern(n: usize, salt: usize) -> Vec<i64> {
    (0..n)
        .map(|k| (((k * 7 + salt * 13) % 17) as i64 - 8) << 28)
        .collect()
}

fn largest_legal_head() -> DecisionHeadBody {
    let width = MAX_SCORER_WIDTH;
    let features = MAX_SCORER_FEATURES;
    let spec = FeatureStandardisation {
        center: q(ONE / 4),
        scale: q(2 * ONE),
        lower: q(-8 * ONE),
        upper: q(8 * ONE),
    };
    DecisionHeadBody {
        schema_version: DECISION_HEAD_SCHEMA_VERSION,
        kind: HeadKind::OptionAttention,
        regime: FittedRegime::FullLabel,
        feature_schema_digest: SCHEMA_DIGEST.to_string(),
        standardisation: bounded(vec![spec; features]),
        weights: bounded(pattern(features, 0).into_iter().map(q).collect()),
        calibration: None,
        training_records_digest: "sha256:golden".to_string(),
        n_training: 0,
        synthetic: true,
        scorer: Some(Box::new(OptionAttentionParams {
            width: width as u8,
            shortlist: MAX_SCORER_SHORTLIST as u8,
            embed: bounded(pattern(features * width, 1)),
            embed_bias: bounded(pattern(width, 2)),
            query: bounded(pattern(width * width, 3)),
            key: bounded(pattern(width * width, 4)),
            value: bounded(pattern(width * width, 5)),
            self_weight: bounded(pattern(width, 6)),
            context_weight: bounded(pattern(width, 7)),
        })),
    }
}

fn parameter_count(head: &DecisionHeadBody) -> usize {
    head.weights.len()
        + head.scorer.as_ref().map_or(0, |p| {
            p.embed.len()
                + p.embed_bias.len()
                + p.query.len()
                + p.key.len()
                + p.value.len()
                + p.self_weight.len()
                + p.context_weight.len()
        })
}

#[cfg(target_os = "linux")]
fn thread_cpu_ns() -> u64 {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: clock_gettime initializes a properly aligned timespec on success.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, time.as_mut_ptr()) },
        0,
        "thread CPU clock unavailable"
    );
    let time = unsafe { time.assume_init() };
    u64::try_from(time.tv_sec).unwrap() * 1_000_000_000 + u64::try_from(time.tv_nsec).unwrap()
}

#[cfg(target_os = "linux")]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes a properly aligned rusage on success.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0,
        "peak RSS unavailable"
    );
    let usage = unsafe { usage.assume_init() };
    // Linux ru_maxrss is KiB. Includes training, inputs, allocator, libraries and code.
    u64::try_from(usage.ru_maxrss)
        .unwrap()
        .checked_mul(1024)
        .unwrap()
}

fn nearest_rank_p99(samples: &mut [u64]) -> u64 {
    assert!(
        samples.len() >= SAMPLES,
        "at least 1000 individual calls required"
    );
    samples.sort_unstable();
    samples[(99 * samples.len()).div_ceil(100) - 1]
}

fn limits(parameters: usize, p99_ns: u64) -> bool {
    parameters <= RESIDENT_SCORER_CEILING.max_parameters as usize
        && p99_ns <= u64::from(RESIDENT_SCORER_CEILING.max_p99_micros) * 1000
}

fn memory_status(bytes: u64) -> &'static str {
    if bytes <= u64::from(RESIDENT_SCORER_CEILING.max_bytes) {
        "accepted_upper_bound"
    } else {
        "inconclusive_scorer_specific_memory"
    }
}

fn check_boundaries_and_refusals(head: &DecisionHeadBody, rows: &[&[i64]], legal: &LegalSet) {
    let c = RESIDENT_SCORER_CEILING;
    assert!(limits(
        c.max_parameters as usize,
        u64::from(c.max_p99_micros) * 1000
    ));
    assert!(!limits(c.max_parameters as usize + 1, 0));
    assert!(!limits(0, u64::from(c.max_p99_micros) * 1000 + 1));
    assert_eq!(
        memory_status(u64::from(c.max_bytes)),
        "accepted_upper_bound"
    );
    assert_eq!(
        memory_status(u64::from(c.max_bytes) + 1),
        "inconclusive_scorer_specific_memory"
    );
    let mut ranks: Vec<_> = (1..=1000).collect();
    assert_eq!(nearest_rank_p99(&mut ranks), 990);
    assert!(ResidentScorerBudget::declare(c.max_parameters + 1, 0, 0).is_err());
    assert!(ResidentScorerBudget::declare(0, c.max_bytes + 1, 0).is_err());
    assert!(ResidentScorerBudget::declare(0, 0, c.max_p99_micros + 1).is_err());
    let mut invalid = head.clone();
    invalid.scorer.as_mut().unwrap().width = 0;
    assert!(read(&invalid, rows, legal).is_err());
    let mut drifted = rows.iter().map(|row| row.to_vec()).collect::<Vec<_>>();
    drifted[0][0] = i64::MAX;
    let views: Vec<_> = drifted.iter().map(Vec::as_slice).collect();
    assert!(read(head, &views, legal).unwrap().is_err());
}

#[cfg(target_os = "linux")]
fn measure(name: &str, head: &DecisionHeadBody, rows: &[Vec<i64>]) -> Value {
    let views: Vec<&[i64]> = rows.iter().map(Vec::as_slice).collect();
    let legal = LegalSet::derive(rows.len(), &[]);
    head.scorer
        .as_ref()
        .unwrap()
        .check(head.weights.len())
        .unwrap();
    check_boundaries_and_refusals(head, &views, &legal);
    for _ in 0..WARMUP {
        black_box(
            read(black_box(head), black_box(&views), black_box(&legal))
                .unwrap()
                .unwrap(),
        );
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let (head, views, legal) = (black_box(head), black_box(&views), black_box(&legal));
        let start = thread_cpu_ns();
        let result = black_box(read(head, views, legal));
        let end = thread_cpu_ns();
        // Validation, result destruction and sample storage are outside the clock.
        black_box(result.unwrap().unwrap());
        samples.push(end.checked_sub(start).expect("monotonic thread CPU clock"));
    }
    let p99 = nearest_rank_p99(&mut samples);
    let parameters = parameter_count(head);
    json!({
        "name": name, "rows": rows.len(), "features": head.weights.len(),
        "width": head.scorer.as_ref().unwrap().width,
        "shortlist": head.scorer.as_ref().unwrap().shortlist,
        "trainable_parameters": parameters,
        "parameter_scope": "linear_weights_plus_all_seven_attention_tensors",
        "calibration_scalars": usize::from(head.calibration.is_some()),
        "warmup_calls": WARMUP, "measured_calls": SAMPLES,
        "p99_cpu_ns": p99, "p99_cpu_micros": p99 as f64 / 1000.0,
        "parameter_and_latency_accepted": limits(parameters, p99)
    })
}

// Requirement EG-DECISION-ENGINE-R090.3; native receipt pending.
#[cfg(target_os = "linux")]
fn resident_scorer_meets_declared_budget() -> i32 {
    assert!(
        !cfg!(debug_assertions),
        "use the optimized release profile"
    );
    // Same 200-item, seed-0, 60-iteration fit as scorer_promotion.rs.
    let trained = fit_eval::fitted(HeadKind::OptionAttention, &common::gold_dataset(200, 0));
    assert!(trained
        .scorer
        .as_ref()
        .unwrap()
        .self_weight
        .iter()
        .any(|&w| w != 0));
    let trained_rows: Vec<Vec<i64>> = common::rows(0, 0)
        .chunks(common::FEATURES.len())
        .map(<[i64]>::to_vec)
        .collect();
    let maximum = largest_legal_head();
    let maximum_rows: Vec<Vec<i64>> = (0..256)
        .map(|r| {
            (0..MAX_SCORER_FEATURES)
                .map(|f| (((r * 5 + f * 3) % 7) as i64 - 3) << 28)
                .collect()
        })
        .collect();
    let cases = vec![
        measure("fitted_promotion_recipe", &trained, &trained_rows),
        measure("maximum_legal_shape", &maximum, &maximum_rows),
    ];
    let rss = peak_rss_bytes();
    let runner_bytes = std::fs::metadata(std::env::current_exe().unwrap())
        .unwrap()
        .len();
    let cpu_and_parameters = cases
        .iter()
        .all(|v| v["parameter_and_latency_accepted"] == true);
    let memory = memory_status(rss);
    let status = if !cpu_and_parameters {
        "failed"
    } else if memory == "accepted_upper_bound" {
        "accepted_pending_build_provenance"
    } else {
        "inconclusive"
    };
    println!("{}", serde_json::to_string_pretty(&json!({
        "schema_version": 1, "requirement": "EG-DECISION-ENGINE-R090.3",
        "status": status, "clock": "CLOCK_THREAD_CPUTIME_ID", "quantile": "nearest_rank",
        "required_profile": "release", "build_provenance": "external_receipt_required",
        "architecture": std::env::consts::ARCH,
        "ceiling": {"parameters": RESIDENT_SCORER_CEILING.max_parameters, "resident_bytes": RESIDENT_SCORER_CEILING.max_bytes, "p99_cpu_micros": RESIDENT_SCORER_CEILING.max_p99_micros},
        "cases": cases,
        "memory": {"metric": "whole_runner_peak_rss_upper_bound", "bytes": rss, "status": memory, "scorer_specific_resident_bytes": null},
        "runner_binary_bytes": runner_bytes, "server_binary_bytes": null
    })).unwrap());
    if !cpu_and_parameters {
        1
    } else if memory != "accepted_upper_bound" {
        2
    } else {
        0
    }
}

#[cfg(target_os = "linux")]
fn main() {
    std::process::exit(resident_scorer_meets_declared_budget());
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!(
        "R090.3 requires Linux CLOCK_THREAD_CPUTIME_ID and getrusage; no wall-clock fallback"
    );
    std::process::exit(2);
}
