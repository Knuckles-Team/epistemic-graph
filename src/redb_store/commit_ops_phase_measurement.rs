//! EH-290 measurement: decompose `commit_ops`'s durable-write phases on THIS
//! host's storage and report the numbers, per the storage durability
//! performance design's scoped experiment
//! (`plans/refactor/architecture/STORAGE-DURABILITY-PERFORMANCE-DESIGN.md` §5).
//!
//! Ignored by default (`cargo test -- --ignored`): this is a timing
//! measurement, not a correctness assertion. Host load and storage medium
//! change the numbers, so nothing here should ever gate a build; run it
//! explicitly and read stderr for the phase decomposition.

use super::{commit_ops, DurableCrypto};
use crate::redb_store::shard::Shard;
use std::time::Instant;

/// Sum and sample count of one Prometheus histogram family's `_sum`/`_count`
/// series for one exact label match, parsed out of `crate::metrics::render()`'s
/// text exposition. A mean is all a diagnostic needs here -- percentiles would
/// need the bucket histogram, which this does not reproduce.
fn histogram_sum_count(rendered: &str, family: &str, label: &str) -> Option<(f64, u64)> {
    let sum_prefix = format!("{family}_sum{{{label}}} ");
    let count_prefix = format!("{family}_count{{{label}}} ");
    let sum = rendered
        .lines()
        .find_map(|line| line.strip_prefix(sum_prefix.as_str()))
        .and_then(|value| value.trim().parse::<f64>().ok())?;
    let count = rendered
        .lines()
        .find_map(|line| line.strip_prefix(count_prefix.as_str()))
        .and_then(|value| value.trim().parse::<u64>().ok())?;
    Some((sum, count))
}

fn percentile(sorted_secs: &[f64], fraction: f64) -> f64 {
    if sorted_secs.is_empty() {
        return 0.0;
    }
    let index = ((sorted_secs.len() - 1) as f64 * fraction).round() as usize;
    sorted_secs[index]
}

/// One named series' sum/count out of `rendered`, keyed by `{label_key}="{label_value}"`.
/// The one call site both loops in [`eh_290_commit_ops_phase_decomposition`] use --
/// they differ only in how they print the pair, not in how they fetch it.
fn report_series(
    rendered: &str,
    family: &str,
    label_key: &str,
    label_value: &str,
) -> Option<(f64, u64)> {
    let label = format!("{label_key}=\"{label_value}\"");
    histogram_sum_count(rendered, family, &label)
}

/// One raft group's dedicated writer thread committing ONE small log entry per
/// drain, sequentially -- the exact shape EH-290's 617-sample observation
/// measured (queue-wait 6.5ms median, `commit_ops` 1.31s median, 2.80s max).
/// Reproduces it against THIS host's storage and prints the phase
/// decomposition `CommitPhaseTimer` records.
#[test]
#[ignore = "EH-290 measurement: prints a timing report, not a correctness assertion"]
fn eh_290_commit_ops_phase_decomposition() {
    let path = crate::redb_store::temp_path("eg-commit-ops-phase", "decompose");
    let shard = Shard::open(&path).expect("open measurement shard");

    const ITERATIONS: usize = 60;
    let entry = vec![7u8; 256]; // one raft log entry payload, representative size
    let mut totals = Vec::with_capacity(ITERATIONS);

    for index in 0..ITERATIONS {
        let mut ops = Vec::new();
        let mut log = vec![(1u64, index as u64, entry.clone())];
        let drain_id = format!("eh290-bench-{index}");
        let started = Instant::now();
        commit_ops(
            &shard,
            &mut ops,
            &mut log,
            &drain_id,
            index as u64,
            DurableCrypto::none(),
            #[cfg(feature = "security")]
            &mut super::AuditTailCache::new(),
        )
        .expect("commit_ops");
        totals.push(started.elapsed().as_secs_f64());
    }

    drop(shard);
    let _ = std::fs::remove_file(&path);

    let mut sorted = totals;
    sorted.sort_by(|a, b| a.partial_cmp(b).expect("finite duration"));
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let median = percentile(&sorted, 0.5);
    let max = *sorted.last().expect("at least one iteration");

    eprintln!(
        "EH-290 commit_ops total (n={ITERATIONS}): median={median:.6}s mean={mean:.6}s max={max:.6}s"
    );
    let rendered = crate::metrics::render();
    for phase in [
        "acquire_txn",
        "apply_writes",
        "ledger_finish",
        "durability_commit",
    ] {
        match report_series(
            &rendered,
            "epistemic_graph_commit_ops_phase_seconds",
            "phase",
            phase,
        ) {
            Some((sum, count)) => {
                let mean_phase = sum / count.max(1) as f64;
                let share = 100.0 * mean_phase / mean.max(1e-12);
                eprintln!(
                    "  phase={phase:<18} count={count:<5} sum={sum:.6}s mean={mean_phase:.6}s ({share:.1}% of total mean)"
                );
            }
            None => eprintln!("  phase={phase:<18} NO SAMPLES (metrics feature disabled?)"),
        }
    }
    for kind in ["logical", "physical_delta"] {
        if let Some((sum, count)) =
            report_series(&rendered, "epistemic_graph_commit_ops_bytes", "kind", kind)
        {
            eprintln!("  bytes kind={kind:<15} count={count} sum={sum:.0}");
        }
    }
}
