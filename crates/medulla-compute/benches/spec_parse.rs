//! Spec-artifact parse bench (MEDULLA-SDLC-R014.1): one 500-row requirements
//! file and its 500 task lines, parsed in memory.
//!
//! Run: cargo bench -p medulla-compute --bench spec_parse

use criterion::{criterion_group, criterion_main, Criterion};
use medulla_compute::parser::spec_artifacts::parse_spec_file;
use std::hint::black_box;

fn corpus() -> (String, String) {
    let mut rows =
        String::from("# Requirements\n\n| ID | Requirement | Acceptance |\n|---|---|---|\n");
    let mut tasks = String::from("# Tasks\n\n");
    for n in 1..=500 {
        rows.push_str(&format!(
            "| `DEMO-R{n:03}` | **Row {n}.** Body text. | Test: `tests/a.rs::row_{n}` (L2). |\n"
        ));
        tasks.push_str(&format!("- [ ] T{n:03} [P] [US1] DEMO-R{n:03} Task {n}.\n"));
    }
    (rows, tasks)
}

fn spec_parse(c: &mut Criterion) {
    let (rows, tasks) = corpus();
    let mut group = c.benchmark_group("lifecycle/spec_parse");
    group.bench_function("rows_500", |b| {
        b.iter(|| {
            let mut n = parse_spec_file("requirements.md", black_box(&rows))
                .unwrap()
                .len();
            n += parse_spec_file("tasks.md", black_box(&tasks))
                .unwrap()
                .len();
            black_box(n)
        })
    });
    group.finish();
}

criterion_group!(benches, spec_parse);
criterion_main!(benches);
