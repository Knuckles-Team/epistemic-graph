//! Filtered ranking stage costs (EH-565) on a corpus shaped like the pinned HelixDB
//! comparison corpus (`benches/helix_compare`): 20,000 `Doc` nodes in 16 categories,
//! a 24-word text body and a 32-dim embedding clustered by category.
//!
//! Each stage of `MATCH (:Doc) WHERE category = c |> TEXT/RANK |> LIMIT 10` is timed
//! alone, so a regression names its stage: the label scan, the scan plus category
//! filter, and the two filtered rankings end to end.
//!
//! Run: `cargo bench -p eg-plan --features "query text" --bench filtered_rank`

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, Op, Plan, PlanCtx, Pred};
use eg_text::TextIndex;
use serde_json::json;

const DOCS: usize = 20_000;
const CATEGORIES: usize = 16;
const DIM: usize = 32;
const WORDS: usize = 24;

/// Numerical-Recipes LCG: the corpus is byte-reproducible without a `rand` dep.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn unit(&mut self) -> f32 {
        (self.next() % 20_001) as f32 / 10_000.0 - 1.0
    }
}

struct Corpus {
    view: GraphView,
    semantic: SemanticStore,
    text: TextIndex,
}

/// A skewed vocabulary word: low ranks are frequent, like the pinned corpus's Zipf body.
fn word(rng: &mut Lcg) -> String {
    let rank = (rng.next() % 400) * (rng.next() % 400) / 400;
    format!("w{rank}")
}

fn corpus() -> Corpus {
    let mut rng = Lcg(20_260_924);
    let core = GraphCore::new();
    let mut semantic = SemanticStore::new();
    let mut text = TextIndex::in_memory().expect("in-memory text index");
    let centers: Vec<Vec<f32>> = (0..CATEGORIES)
        .map(|_| (0..DIM).map(|_| rng.unit()).collect())
        .collect();
    for doc in 0..DOCS {
        let id = format!("d{doc:06}");
        let category = doc % CATEGORIES;
        let body: Vec<String> = (0..WORDS).map(|_| word(&mut rng)).collect();
        let body = body.join(" ");
        let props = json!({
            "type": "Doc", "key": id, "category": format!("c{category}"),
            "year": 2000 + doc % 26, "text": body,
        });
        core.add_node(id.clone(), rmp_serde::to_vec_named(&props).unwrap());
        text.upsert(&id, &body);
        let vector = centers[category]
            .iter()
            .map(|c| c + 0.3 * rng.unit())
            .collect();
        semantic.add_embedding(id, vector).unwrap();
    }
    text.commit().unwrap();
    Corpus {
        view: core.analysis_snapshot(),
        semantic,
        text,
    }
}

fn filtered(tail: Vec<Op>) -> Plan {
    let mut ops = vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::Eq {
                prop: "category".into(),
                value: "c7".into(),
            }],
        },
    ];
    ops.extend(tail);
    Plan::new(ops)
}

fn bench_filtered_rank(c: &mut Criterion) {
    let corpus = corpus();
    let ctx = PlanCtx::new(&corpus.view, &corpus.semantic).with_text(&corpus.text);
    let query: Vec<f32> = (0..DIM).map(|i| (i as f32 * 0.37).sin()).collect();
    let plans = [
        (
            "scan",
            Plan::new(vec![Op::Scan {
                label: "Doc".into(),
            }]),
        ),
        ("scan_filter", filtered(Vec::new())),
        (
            "text_prefilter",
            filtered(vec![
                Op::RankText { query: "w3".into() },
                Op::Limit { k: 10 },
            ]),
        ),
        (
            "vector_prefilter",
            filtered(vec![
                Op::Rank {
                    query: query.clone(),
                },
                Op::Limit { k: 10 },
            ]),
        ),
        (
            "vector_all_top10",
            Plan::new(vec![
                Op::Scan {
                    label: "Doc".into(),
                },
                Op::Rank {
                    query: query.clone(),
                },
                Op::Limit { k: 10 },
            ]),
        ),
    ];
    let mut group = c.benchmark_group("filtered_rank_20k");
    group.sample_size(20);
    for (name, plan) in &plans {
        let rows = execute(plan, &ctx).expect("plan executes").len();
        assert!(rows > 0, "{name} returned no rows");
        group.bench_function(*name, |b| {
            b.iter(|| black_box(execute(plan, &ctx).unwrap()))
        });
    }
    group.finish();
    bench_exact_vs_ann(c, &corpus.semantic, &query);
}

/// The exact-vs-ANN crossover behind `EXACT_RANK_MAX`: top-10 over candidate sets of
/// growing size, scored exactly vs by the filtered ANN walk (k·4 oversampled).
fn bench_exact_vs_ann(c: &mut Criterion, semantic: &SemanticStore, query: &[f32]) {
    let mut group = c.benchmark_group("exact_vs_ann_top10");
    group.sample_size(20);
    for size in [1_024usize, 4_096, 8_192, DOCS] {
        let ids: Vec<String> = (0..size)
            .map(|doc| format!("d{:06}", doc * (DOCS / size)))
            .collect();
        let set: std::collections::HashSet<&str> = ids.iter().map(String::as_str).collect();
        group.bench_function(format!("exact_{size}"), |b| {
            b.iter(|| black_box(semantic.exact_rank_candidates(query, set.iter().copied(), 10)))
        });
        group.bench_function(format!("ann_{size}"), |b| {
            b.iter(|| {
                black_box(semantic.semantic_search_filtered(query, 40, |id| set.contains(id)))
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_filtered_rank);
criterion_main!(benches);
