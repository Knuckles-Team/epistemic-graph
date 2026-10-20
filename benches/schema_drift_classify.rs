//! EG-TYPED-PACKS-R712 hot-path receipt: schema-drift classification of a
//! 1,000-field contract.
//!
//! Budget (spec): p50 under 0.5 ms, p99 under 3 ms per 1,000-field contract,
//! memory under 16 MiB. The classifier is O(fields) and allocation-light
//! (the only per-delivery work is the bounded report plus one SHA-256 of the
//! per-delivery shape digest; the contract in force's identity is stable per
//! contract and supplied pre-computed, as in steady state), so this bench
//! measures the hot `classify` call against a 1,000-field approved contract
//! with a rotation of 66 deterministic drift shapes (compatible, widening,
//! narrowing, breaking, rename) and prints p50/p99 beside the process RSS
//! growth across the measured loop as the memory receipt.
//!
//! Deterministic: fixed field names, fixed drift shapes, no RNG.
//!
//! Run: cargo bench --features full --bench schema_drift_classify

use std::collections::BTreeMap;
use std::time::Instant;

use eg_types::graph_schema::approval::approved_candidate_digest;
use eg_types::graph_schema::drift::{
    classify, DriftClass, EvolutionPolicy, InferredField, InferredRecordShape,
};
use eg_types::graph_schema::repair::{FieldContract, JsonType, RecordContract};

const SOURCE_ID: &str = "approved:drift-bench";
const FIELDS: usize = 1_000;
const ITERATIONS: usize = 1_000;
const WARMUP: usize = 100;

fn field_name(index: usize) -> String {
    format!("field_{index:04}")
}

/// The approved 1,000-field contract: even fields are null-or-string, odd
/// fields are string, every field required.
fn approved_contract() -> RecordContract {
    let mut fields = BTreeMap::new();
    for index in 0..FIELDS {
        let types = if index % 2 == 0 {
            vec![JsonType::Null, JsonType::String]
        } else {
            vec![JsonType::String]
        };
        fields.insert(
            field_name(index),
            FieldContract {
                required: true,
                types,
            },
        );
    }
    RecordContract { fields }
}

/// The delivery that fits the contract exactly.
fn base_shape() -> InferredRecordShape {
    let contract = approved_contract();
    InferredRecordShape {
        fields: contract
            .fields
            .iter()
            .map(|(name, spec)| {
                (
                    name.clone(),
                    InferredField {
                        types: spec.types.clone(),
                        always_present: true,
                    },
                )
            })
            .collect(),
    }
}

/// 66 deterministic drift shapes: identical, 16 widenings, 16 narrowings,
/// 16 breakings (retype), 16 breakings (required field missing) and one
/// rename.
fn drift_shapes() -> Vec<InferredRecordShape> {
    let mut shapes = vec![base_shape()];
    for variant in 1..=16 {
        // Widening: one field gains a type the contract does not name.
        let mut shape = base_shape();
        let name = field_name((variant * 67) % FIELDS);
        shape
            .fields
            .get_mut(&name)
            .unwrap()
            .types
            .push(JsonType::Boolean);
        shapes.push(shape);
    }
    for variant in 17..=32 {
        // Narrowing: an even (null-or-string) field sends only strings.
        let mut shape = base_shape();
        let index = ((variant * 67) % 500) * 2;
        shape.fields.get_mut(&field_name(index)).unwrap().types = vec![JsonType::String];
        shapes.push(shape);
    }
    for variant in 33..=48 {
        // Breaking: one field is retyped to a set neither superset nor subset.
        let mut shape = base_shape();
        let name = field_name((variant * 67) % FIELDS);
        shape.fields.get_mut(&name).unwrap().types = vec![JsonType::Integer];
        shapes.push(shape);
    }
    for variant in 49..=64 {
        // Breaking: a required field no longer arrives.
        let mut shape = base_shape();
        shape.fields.remove(&field_name((variant * 67) % FIELDS));
        shapes.push(shape);
    }
    // Rename: exactly one field left and one arrived with the same signature
    // (field_0001 is an odd field: string, required — the arrival carries the
    // same type set).
    let mut shape = base_shape();
    shape.fields.remove(&field_name(1));
    shape.fields.insert(
        "renamed_field_0001".to_string(),
        InferredField {
            types: vec![JsonType::String],
            always_present: true,
        },
    );
    shapes.push(shape);
    shapes
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() - 1) as f64 * fraction).round() as usize]
}

/// The process's resident set in MiB (VmRSS), or NaN off Linux.
fn rss_mib() -> f64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return f64::NAN;
    };
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|rest| rest.trim().strip_suffix(" kB"))
        .and_then(|kilobytes| kilobytes.parse::<f64>().ok())
        .map(|kilobytes| kilobytes / 1024.0)
        .unwrap_or(f64::NAN)
}

fn main() {
    let approved = approved_contract();
    // Stable per contract, not per delivery: pre-computed, as in steady state.
    let approved_digest = approved_candidate_digest(SOURCE_ID, &approved);
    let shapes = drift_shapes();
    assert_eq!(shapes.len(), 66);
    // Pin the classification the bench measures: the receipt is only
    // meaningful if each variant lands in its drift class.
    let expected: Vec<DriftClass> = std::iter::repeat_n(DriftClass::Compatible, 1)
        .chain(std::iter::repeat_n(DriftClass::Widening, 16))
        .chain(std::iter::repeat_n(DriftClass::Narrowing, 16))
        .chain(std::iter::repeat_n(DriftClass::Breaking, 32))
        .chain(std::iter::once(DriftClass::Rename))
        .collect();
    for (index, (shape, want)) in shapes.iter().zip(expected.iter()).enumerate() {
        let report = classify(
            &approved,
            &approved_digest,
            shape,
            EvolutionPolicy::Quarantine,
        );
        assert_eq!(report.class, *want, "drift variant {index} misclassified");
    }

    for _ in 0..WARMUP {
        let report = classify(
            &approved,
            &approved_digest,
            &shapes[1],
            EvolutionPolicy::Quarantine,
        );
        let _ = report.shape_digest.len();
    }

    let rss_before = rss_mib();
    let mut samples = Vec::with_capacity(ITERATIONS);
    let mut digest_bytes = 0usize;
    for index in 0..ITERATIONS {
        let shape = &shapes[index % shapes.len()];
        let start = Instant::now();
        let report = classify(
            &approved,
            &approved_digest,
            shape,
            EvolutionPolicy::Quarantine,
        );
        samples.push(start.elapsed().as_secs_f64() * 1e3);
        digest_bytes += report.shape_digest.len();
    }
    let rss_after = rss_mib();

    let mut sorted = samples.clone();
    sorted.sort_by(f64::total_cmp);
    let p50 = percentile(&sorted, 0.50);
    let p99 = percentile(&sorted, 0.99);
    let mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let max = sorted.last().copied().unwrap_or(0.0);
    let rss_growth = rss_after - rss_before;

    println!(
        "schema_drift_classify: fields={FIELDS} variants={} iters={ITERATIONS} digest_bytes={digest_bytes}",
        shapes.len()
    );
    println!("  p50={p50:.4} ms  p99={p99:.4} ms  mean={mean:.4} ms  max={max:.4} ms");
    println!(
        "  rss_before={:.2} MiB  rss_after={:.2} MiB  rss_growth={rss_growth:.2} MiB",
        rss_before, rss_after
    );
    for (label, value, budget) in [
        ("p50", p50, 0.5),
        ("p99", p99, 3.0),
        ("memory(rss_growth)", rss_growth, 16.0),
    ] {
        println!(
            "  budget {label} < {budget} ms/MiB: {}",
            if value < budget { "PASS" } else { "FAIL" }
        );
    }
}
