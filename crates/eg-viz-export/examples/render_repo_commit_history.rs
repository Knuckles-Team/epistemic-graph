//! Real chart from a real query (D-VZ-1 lane V3a demo — the deliverable, not a
//! unit test). Runs `git log --format=%ct` against **this repository's own
//! real git history**, buckets real commit counts per real day, ingests the
//! result into a real `eg-viz-columnstore::ColumnStore`, resolves it through
//! the real V1+V3a pipeline (`select_tier` -> reduction -> `ViewResult`), and
//! exports PNG/SVG/PDF. No synthetic/random fixture anywhere in this path.
//!
//! Run: `cargo run -p eg-viz-export --example render_repo_commit_history --release`
//! (optionally set `EG_VIZ_DEMO_OUT_DIR` to control where the three files land;
//! defaults to `/tmp`).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

use eg_viz_columnstore::{ColumnData, ColumnInput, ColumnStore};
use eg_viz_core::{
    EncodingSpec, Encodings, FrameBudget, MarkKind, MarkSpec, ScaleKind, ScaleSpec,
    StaticExportBackend, StaticExportFormat, ViewSpec,
};
use eg_viz_export::ColumnStoreExportBackend;

/// `crates/eg-viz-export` -> the epistemic-graph repo root.
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

fn real_commits_per_day() -> Vec<(f64, f64)> {
    let repo = repo_root();
    let output = Command::new("git")
        .args(["log", "--format=%ct"])
        .current_dir(&repo)
        .output()
        .expect("`git log` must run against the real repository checked out at this path");
    assert!(
        output.status.success(),
        "git log failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    let mut per_day: BTreeMap<i64, u32> = BTreeMap::new();
    for line in stdout.lines() {
        if let Ok(unix) = line.trim().parse::<i64>() {
            let day = unix.div_euclid(86_400); // unix day number
            *per_day.entry(day).or_insert(0) += 1;
        }
    }
    assert!(
        !per_day.is_empty(),
        "expected real commit history from `git log`, found none — is {} a git repo?",
        repo.display()
    );
    per_day
        .into_iter()
        .map(|(day, count)| (day as f64, count as f64))
        .collect()
}

fn main() {
    let series = real_commits_per_day();
    let day_count = series.len();
    let total_commits: f64 = series.iter().map(|(_, c)| c).sum();
    println!(
        "real query: {} real commits across {day_count} distinct real days (git log --format=%ct over {})",
        total_commits as u64,
        repo_root().display()
    );

    let days: Vec<f64> = series.iter().map(|(d, _)| *d).collect();
    let commits: Vec<f64> = series.iter().map(|(_, c)| *c).collect();

    let mut store = ColumnStore::new();
    store
        .ingest_columns(
            "ds:repo-commit-history",
            vec![
                ColumnInput::new("day", ColumnData::F64(days)),
                ColumnInput::new("commits", ColumnData::F64(commits)),
            ],
        )
        .unwrap();

    let mut spec = ViewSpec::new(vec![MarkSpec::new(
        MarkKind::Line,
        "ds:repo-commit-history",
    )
    .with_encodings(Encodings {
        x: Some(EncodingSpec::field("day").with_scale("x")),
        y: Some(EncodingSpec::field("commits").with_scale("y")),
        ..Default::default()
    })]);
    spec.title = Some("epistemic-graph: real commits per day (git log)".to_string());
    spec.scales = vec![
        ScaleSpec::new("x", ScaleKind::Time),
        ScaleSpec::new("y", ScaleKind::Linear),
    ];
    spec.theme.palette = vec!["#4C78A8".to_string()];
    spec.theme.background = Some("#FFFFFF".to_string());

    let backend = ColumnStoreExportBackend::new(&store, 1000, 400);
    let budget = FrameBudget::new(2_000_000, 16 * 1024 * 1024);
    let result = backend
        .resolve(&spec, "ds:repo-commit-history", budget, 1)
        .expect("resolve real commit-history dataset");

    println!(
        "select_tier -> {:?} (row_count={}, exact={})",
        result.lod_tier, result.row_count, result.exact
    );
    assert!(
        result.exact,
        "a repository's real day-bucketed commit history (hundreds-to-thousands of days) must select LodTier::Direct, not a reduced tier"
    );

    let out_dir = std::env::var("EG_VIZ_DEMO_OUT_DIR").unwrap_or_else(|_| "/tmp".to_string());
    for (format, ext) in [
        (StaticExportFormat::Png, "png"),
        (StaticExportFormat::Svg, "svg"),
        (StaticExportFormat::Pdf, "pdf"),
    ] {
        let bytes = backend.export(&spec, &result, format).unwrap();
        let path = format!("{out_dir}/eg-viz-repo-commit-history.{ext}");
        std::fs::write(&path, &bytes).unwrap();
        println!("wrote {path} ({} bytes)", bytes.len());
    }
}
