//! Digest-bound, bounded G-37 performance probes.
//!
//! This module is compiled only into the one `full` server artifact. The release
//! certifier stages that exact binary by digest, sends one schema-validated scenario
//! over stdin, and records raw operation counters, owned-memory accounting, measured
//! latency samples, and semantic equivalence outcomes. It is deliberately not an
//! alternate benchmark executable: every production API used below is the API linked
//! into the served artifact being certified.

use std::collections::BTreeMap;
use std::hint::black_box;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};

mod analytics;
mod contract;
mod modality;
mod query;
mod storage;
mod wire;

const PROTOCOL: &str = "g37.performance-probe.v1";
const SCHEMA_VERSION: &str = "1";
const MAX_REQUEST_BYTES: u64 = 1024 * 1024;
const MAX_SCALE: usize = 100_000;
const MAX_REPETITIONS: usize = 25;

type ProbeError = Box<dyn std::error::Error>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeRequest {
    schema_version: String,
    protocol: String,
    scenario_id: String,
    driver: String,
    seed: u64,
    workload_sha256: String,
    scales: Vec<usize>,
    repetitions: usize,
    rows: Vec<RequestedRow>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestedRow {
    row_id: String,
    equivalence_checks: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ProbeResult {
    schema_version: &'static str,
    protocol: &'static str,
    scenario_id: String,
    driver: String,
    rows: Vec<RowResult>,
}

#[derive(Debug, Serialize)]
struct RowResult {
    row_id: String,
    scales: Vec<ScaleResult>,
    equivalence: BTreeMap<String, bool>,
}

#[derive(Debug, Serialize)]
struct ScaleResult {
    scale: usize,
    work_units: u64,
    memory_bytes: u64,
    latency_ns: Vec<u64>,
}

#[derive(Debug)]
struct Observation {
    work_units: u64,
    memory_bytes: u64,
    latency_ns: u64,
    equivalent: bool,
}

/// Execute exactly one bounded scenario from stdin and emit one JSON document.
pub fn run_stdio(probe_root: &Path) -> Result<(), ProbeError> {
    validate_probe_root(probe_root)?;
    let mut input = Vec::new();
    std::io::stdin()
        .take(MAX_REQUEST_BYTES + 1)
        .read_to_end(&mut input)?;
    if input.is_empty() || input.len() as u64 > MAX_REQUEST_BYTES {
        return Err("exact performance probe request exceeds its bound".into());
    }
    let request: ProbeRequest = serde_json::from_slice(&input)?;
    validate_request(&request)?;

    let mut rows = Vec::with_capacity(request.rows.len());
    for requested in &request.rows {
        rows.push(probe_one_row(requested, &request, probe_root)?);
    }

    let output = ProbeResult {
        schema_version: SCHEMA_VERSION,
        protocol: PROTOCOL,
        scenario_id: request.scenario_id,
        driver: request.driver,
        rows,
    };
    let encoded = serde_json::to_vec(&output)?;
    std::io::stdout().write_all(&encoded)?;
    Ok(())
}

/// Probe one requested row across every scale in `request`, folding per-repetition
/// observations into a [`RowResult`]. Extracted from [`run_stdio`]'s per-row loop.
fn probe_one_row(
    requested: &RequestedRow,
    request: &ProbeRequest,
    probe_root: &Path,
) -> Result<RowResult, ProbeError> {
    let mut scales = Vec::with_capacity(request.scales.len());
    let mut equivalence: BTreeMap<String, bool> = requested
        .equivalence_checks
        .iter()
        .map(|name| (name.clone(), true))
        .collect();
    for &scale in &request.scales {
        scales.push(probe_one_scale(
            requested,
            request,
            scale,
            probe_root,
            &mut equivalence,
        )?);
    }
    Ok(RowResult {
        row_id: requested.row_id.clone(),
        scales,
        equivalence,
    })
}

/// Probe one `(row, scale)` pair across every repetition, folding results into a
/// [`ScaleResult`] and marking every equivalence check `false` if any repetition was
/// not equivalent. Extracted from [`probe_one_row`]'s per-scale loop.
fn probe_one_scale(
    requested: &RequestedRow,
    request: &ProbeRequest,
    scale: usize,
    probe_root: &Path,
    equivalence: &mut BTreeMap<String, bool>,
) -> Result<ScaleResult, ProbeError> {
    let mut work_units = 0;
    let mut memory_bytes = 0;
    let mut latency_ns = Vec::with_capacity(request.repetitions);
    for repetition in 0..request.repetitions {
        let observation = contract::probe_row(
            &requested.row_id,
            scale,
            request.seed,
            repetition,
            probe_root,
        )?;
        work_units = work_units.max(observation.work_units);
        memory_bytes = memory_bytes.max(observation.memory_bytes);
        latency_ns.push(observation.latency_ns.max(1));
        if !observation.equivalent {
            for outcome in equivalence.values_mut() {
                *outcome = false;
            }
        }
    }
    Ok(ScaleResult {
        scale,
        work_units: work_units.max(1),
        memory_bytes: memory_bytes.max(1),
        latency_ns,
    })
}

fn validate_probe_root(root: &Path) -> Result<(), ProbeError> {
    let metadata = std::fs::symlink_metadata(root)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("exact performance probe root must be a private directory".into());
    }
    Ok(())
}

fn validate_request(request: &ProbeRequest) -> Result<(), ProbeError> {
    if !contract::contract_shape_valid(request) {
        return Err("invalid exact performance probe contract".into());
    }
    let Some((expected_driver, expected_rows)) = contract::scenario_contract(&request.scenario_id)
    else {
        return Err("unknown exact performance scenario".into());
    };
    let supplied_rows: Vec<&str> = request.rows.iter().map(|row| row.row_id.as_str()).collect();
    if request.driver != expected_driver || supplied_rows != expected_rows {
        return Err("exact performance scenario identity mismatch".into());
    }
    for row in &request.rows {
        contract::validate_row_equivalence_checks(row)?;
    }
    Ok(())
}

fn timed<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    let started = Instant::now();
    let result = black_box(operation());
    let elapsed = started.elapsed().as_nanos();
    (result, u64::try_from(elapsed).unwrap_or(u64::MAX).max(1))
}

fn allocation_bytes<T>(len: usize) -> u64 {
    u64::try_from(len.saturating_mul(std::mem::size_of::<T>()))
        .unwrap_or(u64::MAX)
        .max(1)
}
