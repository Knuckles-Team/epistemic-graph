//! AST parse-throughput baseline (CONCEPT:EG-012).
//!
//! A criterion throughput bench for `parse_files` — the tree-sitter primitive behind
//! the `ParseFiles` protocol op and the code-ingestion phase the EG-011 span now
//! traces. It parses a small synthetic in-bench Rust source sample so the baseline is
//! reproducible without depending on a checked-in corpus, and reports bytes/sec.
//!
//! Gated to `--features ast` (the grammars `parse_files` needs); a default
//! `cargo check --benches` skips it.
//!
//! Run: cargo bench -p eg-compute --features ast --bench parse_bench

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use eg_compute::parser::tree_sitter::parse_files;

/// Build a small in-bench source sample (`n_files` Rust files) so the bench is
/// self-contained and deterministic. Each file has a handful of functions + a struct
/// so the parser does real work (symbols, calls, structural edges).
fn sample_files(n_files: usize) -> Vec<(String, Vec<u8>)> {
    (0..n_files)
        .map(|f| {
            let mut src = String::new();
            src.push_str("use std::collections::HashMap;\n\n");
            src.push_str(&format!(
                "pub struct Widget{f} {{ id: u64, name: String }}\n\n"
            ));
            for g in 0..20 {
                src.push_str(&format!(
                    "pub fn compute_{f}_{g}(x: u64) -> u64 {{\n    \
                     let mut m: HashMap<u64, u64> = HashMap::new();\n    \
                     m.insert(x, x * {g});\n    \
                     helper_{f}(x) + m.get(&x).copied().unwrap_or(0)\n}}\n\n"
                ));
            }
            src.push_str(&format!(
                "fn helper_{f}(v: u64) -> u64 {{ v.wrapping_mul(2654435761) }}\n"
            ));
            (format!("src/widget_{f}.rs"), src.into_bytes())
        })
        .collect()
}

fn bench_parse(c: &mut Criterion) {
    let files = sample_files(32);
    let total_bytes: usize = files.iter().map(|(_, b)| b.len()).sum();

    let mut group = c.benchmark_group("parse_files");
    group.throughput(Throughput::Bytes(total_bytes as u64));
    group.bench_function("rust_32_files", |b| {
        b.iter(|| {
            let out = parse_files(criterion::black_box(&files));
            criterion::black_box(out);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
