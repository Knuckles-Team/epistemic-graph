//! Private implementation module for the analytics job handler.

pub(super) fn native_opaque_ref(namespace: &str, value: &str) -> String {
    use sha2::{Digest, Sha256};
    format!(
        "eg:{namespace}:{}",
        hex::encode(Sha256::digest(value.as_bytes()))
    )
}

pub(super) fn opaque_placement_value(namespace: &str, value: &str) -> String {
    let value = value.trim();
    if value.is_empty() {
        return String::new();
    }
    let prefix = format!("eg:{namespace}:");
    if value.starts_with(&prefix) {
        return value.to_string();
    }
    native_opaque_ref(namespace, value)
}

pub(super) fn opaque_worker_capability(value: &str) -> String {
    let value = value.trim();
    if let Some(pool) = value.strip_prefix("pool:") {
        return format!("pool:{}", opaque_placement_value("job_pool", pool));
    }
    if let Some(region) = value.strip_prefix("region:") {
        return format!("region:{}", opaque_placement_value("job_region", region));
    }
    opaque_placement_value("job_capability", value)
}

pub(super) fn result_quality(
    job: &eg_jobs::AnalyticsJob,
) -> (f64, Option<eg_jobs::CalibrationInput>) {
    let Some(output) = &job.output else {
        return (0.0, None);
    };
    let scores = output
        .rows
        .iter()
        .filter_map(|row| {
            if row.get("kind").and_then(serde_json::Value::as_str)
                == Some("program_optimization_plan_step")
            {
                return None;
            }
            let confidence = row.get("confidence")?.as_f64()?;
            let support = row
                .get("support")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(1.0);
            Some(support * confidence)
        })
        .collect::<Vec<_>>();
    if scores.is_empty() {
        return (0.0, None);
    }
    let confidence = scores.iter().sum::<f64>() / scores.len() as f64;
    let interval = output.calibration.unwrap_or_else(|| {
        (
            scores.iter().copied().fold(f64::INFINITY, f64::min),
            scores.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        )
    });
    (
        confidence,
        Some(eg_jobs::CalibrationInput {
            interval,
            level: 0.95,
            evidence_count: scores.len(),
        }),
    )
}

pub(super) fn unix_ms() -> i64 {
    crate::server::dispatch::authoritative_now_ms() as i64
}
