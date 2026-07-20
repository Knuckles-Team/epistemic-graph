//! Perf / recall CI benchmark (CONCEPT:EG-KG.query.hybrid-latency-benches/recall-oracle-bench/synthetic-bench-dataset) — the "Performance & Recall
//! Regression" track from the external review.
//!
//! It measures the two properties a unified cross-modal planner must not silently
//! regress:
//!
//!  * **latency** of the representative HYBRID pipelines the engine exists to fuse
//!    (`criterion` reports p50/mean over bounded iters) —
//!      - `MATCH → TRAVERSE → RANK`  (relational-source → graph → vector),
//!      - `REASON → RANK → TRAVERSE` (OWL-source → vector → graph, `owl`),
//!      - `TsScan → WINDOW`          (time-series source → tumbling aggregate, `timeseries`),
//!      - `FUSE`                     (RRF hybrid of vector+lexical+graph legs, `text`),
//!      - LeanRAG bounded top-k      (wide provenance fan-out → fixed drill/context budget);
//!  * **recall@k** of the VECTOR leg — the ANN/HNSW `semantic_search` top-k vs a
//!    BRUTE-FORCE cosine oracle over the SAME vectors, on a fixed synthetic dataset.
//!    Recall is deterministic (fixed data + fixed HNSW params), so a drop below the
//!    committed floor is a real regression, not noise. The bench ASSERTS the floor
//!    (so a recall regression fails the bench binary directly) AND writes the measured
//!    value to a JSON the CI gate (`scripts/bench_gate.py`) reports.
//!
//! The dataset builder (CONCEPT:EG-KG.query.synthetic-bench-dataset) is a deterministic LCG — NO `rand` dep, NO
//! checked-in corpus — so every run is byte-comparable.
//!
//! Run: `cargo bench -p eg-plan --features "query,owl,text,timeseries"`
//! (a default `cargo bench -p eg-plan` SKIPS it — `required-features = ["query"]`, and
//! the `execute` leg is `query`-gated).

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{
    cost_opt_enabled, execute, execute_ops, optimize, AnnIndex, GraphTopology,
    HierarchicalRetriever, Op, Plan, PlanCtx, Pred, RetrievalParams, Scored,
};
use serde_json::json;

// ── deterministic PRNG (LCG) — no rand dep, so the dataset is byte-reproducible ──

/// A tiny Numerical-Recipes LCG. Seeded once; every embedding component is drawn from
/// it, so the whole dataset — and therefore the measured recall — is deterministic.
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    /// A float in `[-1, 1)`.
    fn next_signed(&mut self) -> f32 {
        // Top 24 bits → [0,1), then map to [-1,1).
        let bits = (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32;
        bits * 2.0 - 1.0
    }
}

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// The synthetic dataset (CONCEPT:EG-KG.query.synthetic-bench-dataset). `n` `Doc` nodes chained by CITES (plus a
/// deterministic skip-edge fan so a multi-hop TRAVERSE has real branching), each with a
/// `dim`-d embedding drawn from the seeded LCG. Returns the off-lock `GraphView`
/// snapshot, the `SemanticStore`, and the raw `(id, vec)` list the brute-force oracle
/// ranks against. `n > 32` so the `SemanticStore` uses its HNSW index (below 32 it is
/// exact brute force — nothing to measure recall against).
fn build_dataset(
    n: usize,
    dim: usize,
    seed: u64,
) -> (GraphView, SemanticStore, Vec<(String, Vec<f32>)>) {
    let core = GraphCore::new();
    let mut rng = Lcg::new(seed);
    let mut vectors: Vec<(String, Vec<f32>)> = Vec::with_capacity(n);

    for k in 0..n {
        let id = format!("d{k}");
        // Two low-cardinality CATEGORICAL columns (schema-on-read node props) the
        // selective-filter ablation equality-filters on. `topic` (1/97) × `lang` (1/13)
        // give a ~0.08%-selective two-predicate filter — one the cost model's per-`Eq`
        // heuristic (0.1 each ⇒ 0.01) recognizes as selective enough to PUSH ahead of the
        // Rank, unlike a numeric range (`GtNum`, heuristic 0.33). The vector/edge structure
        // (and therefore recall) is unchanged — these are extra properties only.
        core.add_node(
            id.clone(),
            blob(json!({
                "type": "Doc",
                "year": 2000 + (k as i64 % 30),
                // A WIDE monotonic numeric column (0..n) the range-pushdown ablation filters
                // on. Unlike `year` (only 30 distinct values ⇒ min 1/30 selectivity), `score`
                // lets a `GtNum` threshold be arbitrarily selective in ABSOLUTE terms — so the
                // column-histogram estimate (CONCEPT:EG-KG.query.column-range-stats) recognizes a
                // top-tail range as selective enough to push filter-first, where the fixed 0.33
                // heuristic never did. A pure extra property: vector/edge structure (recall)
                // is untouched.
                "score": k as i64,
                "topic": format!("t{}", k % 97),
                "lang": format!("l{}", k % 13),
            })),
        );
        let v: Vec<f32> = (0..dim).map(|_| rng.next_signed()).collect();
        vectors.push((id, v));
    }
    // CITES chain d0→d1→…→d{n-1}, plus a +7 skip fan so a `{1,3}`-hop TRAVERSE branches.
    for k in 0..n.saturating_sub(1) {
        core.add_edge(
            format!("d{k}"),
            format!("d{}", k + 1),
            blob(json!({ "relationship": "CITES" })),
        )
        .unwrap();
    }
    for k in 0..n.saturating_sub(7) {
        core.add_edge(
            format!("d{k}"),
            format!("d{}", k + 7),
            blob(json!({ "relationship": "CITES" })),
        )
        .unwrap();
    }

    let mut semantic = SemanticStore::new();
    for (id, v) in &vectors {
        semantic.add_embedding(id.clone(), v.clone());
    }
    (core.analysis_snapshot(), semantic, vectors)
}

/// A deterministic query vector (drawn from a fixed seed so the ranked target is stable).
fn query_vec(dim: usize, seed: u64) -> Vec<f32> {
    let mut rng = Lcg::new(seed);
    (0..dim).map(|_| rng.next_signed()).collect()
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

/// The BRUTE-FORCE oracle: the true top-`k` ids by cosine to `q` over ALL vectors. Ties
/// broken by id so the ordering is total and comparable across runs.
fn brute_force_topk(vectors: &[(String, Vec<f32>)], q: &[f32], k: usize) -> Vec<String> {
    let mut scored: Vec<(String, f32)> = vectors
        .iter()
        .map(|(id, v)| (id.clone(), cosine(v, q)))
        .collect();
    scored.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    scored.into_iter().take(k).map(|(id, _)| id).collect()
}

/// recall@k of the SemanticStore's ANN search vs the brute-force oracle: the fraction of
/// the true top-k the ANN top-k recovered (|ann ∩ truth| / k).
fn recall_at_k(
    semantic: &SemanticStore,
    vectors: &[(String, Vec<f32>)],
    q: &[f32],
    k: usize,
) -> f64 {
    let truth: std::collections::HashSet<String> =
        brute_force_topk(vectors, q, k).into_iter().collect();
    let ann: Vec<String> = semantic
        .semantic_search(q, k)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let hit = ann.iter().filter(|id| truth.contains(*id)).count();
    hit as f64 / k as f64
}

// ── dataset constants (fixed so latency + recall are comparable across runs) ──────

const N: usize = 2_000; // > 32 → the SemanticStore serves via its HNSW index.
const DIM: usize = 32;
const DATA_SEED: u64 = 0x5eed_1234;
const QUERY_SEED: u64 = 0x9971_abcd;
const RECALL_K: usize = 10;
/// The committed recall floor. HNSW over this fixed dataset recovers well above this; a
/// drop below it means the ANN index or the cosine path regressed. Kept a touch below
/// the observed value so ordinary build-to-build float jitter never trips it.
const RECALL_FLOOR: f64 = 0.80;

// ── latency benches ──────────────────────────────────────────────────────────────

/// `MATCH (:Doc) |> TRAVERSE -[:CITES]->{1,3} |> RANK BY ~q |> LIMIT k` — the canonical
/// relational-source → graph → vector hybrid.
fn bench_match_traverse_rank(c: &mut Criterion) {
    let (view, semantic, _) = build_dataset(N, DIM, DATA_SEED);
    let ctx = PlanCtx::new(&view, &semantic);
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 3,
        },
        Op::Rank {
            query: query_vec(DIM, QUERY_SEED),
        },
        Op::Limit { k: 20 },
    ]);
    c.bench_function("match_traverse_rank", |b| {
        b.iter(|| black_box(execute(&plan, &ctx).unwrap()))
    });
}

#[cfg(feature = "owl")]
fn bench_reason_rank_traverse(c: &mut Criterion) {
    // A small OWL TBox + individuals loaded into the graph so `Reason` classifies real
    // inferred members; the reached docs then rank + traverse. Mirrors the harness's
    // `build_reason_graph` shape but self-contained (a bench sees only public API).
    let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .
ex:p1 a ex:Paper . ex:p2 a ex:Article . ex:p3 a ex:Topic . ex:p4 a ex:Paper .
"#;
    let core = GraphCore::new();
    let mut iris = eg_rdf::mapping::IriStore::default();
    eg_rdf::mapping::load_triples(
        &core,
        &mut iris,
        "g",
        eg_rdf::mapping::parse_turtle(ttl).unwrap(),
    )
    .unwrap();
    let mut semantic = SemanticStore::new();
    for (i, p) in ["p1", "p2", "p4"].iter().enumerate() {
        let id = format!("<http://example.org/{p}>");
        core.add_node(id.clone(), blob(json!({ "type": "Doc" })));
        let mut v = vec![0.0f32; 3];
        v[i % 3] = 1.0;
        semantic.add_embedding(id, v);
    }
    for (s, t) in [("p1", "c1"), ("p2", "c2"), ("p4", "c3")] {
        let sid = format!("<http://example.org/{s}>");
        let tid = format!("<http://example.org/{t}>");
        core.add_node(tid.clone(), blob(json!({ "type": "Doc" })));
        semantic.add_embedding(tid.clone(), vec![0.5, 0.5, 0.0]);
        core.add_edge(sid, tid, blob(json!({ "relationship": "CITES" })))
            .unwrap();
    }
    let view = core.analysis_snapshot();
    let ctx = PlanCtx::new(&view, &semantic);
    let plan = Plan::new(vec![
        Op::Reason {
            target_class: "<http://example.org/ScholarlyWork>".into(),
            ontology: String::new(),
        },
        Op::Rank {
            query: vec![1.0, 0.0, 0.0],
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 1,
        },
        Op::Limit { k: 10 },
    ]);
    c.bench_function("reason_rank_traverse", |b| {
        b.iter(|| black_box(execute(&plan, &ctx).unwrap()))
    });
}

#[cfg(feature = "timeseries")]
fn bench_tsscan_window(c: &mut Criterion) {
    use eg_plan::StagedSeries;
    // Stage a dense series (RYOW overlay — no redb file needed) so `TsScan` seeds real
    // rows a tumbling `WINDOW` then aggregates. (RANK after WINDOW is a no-op — the
    // bucket rows carry no embeddings — so the vector leg's recall is proven by the
    // dedicated recall bench; here we measure the tsdb SOURCE + tumbling-window cost.)
    const NS_PER_S: i64 = 1_000_000_000;
    let mut staged = StagedSeries::new();
    let points: Vec<(i64, Vec<f64>)> = (0..5_000i64)
        .map(|i| (i * NS_PER_S / 10, vec![(i as f64 * 0.017).sin()]))
        .collect();
    staged.push_points("sensor.temp", points);

    let core = GraphCore::new();
    let view = core.analysis_snapshot();
    let semantic = SemanticStore::new();
    let ctx = PlanCtx::new(&view, &semantic).with_staged_series(&staged);
    let plan = Plan::new(vec![
        Op::TsScan {
            series: vec!["sensor.temp".into()],
            from: 0.0,
            to: 600.0,
        },
        Op::WindowAgg {
            secs: 30.0,
            agg: "mean".into(),
        },
        Op::Limit { k: 100 },
    ]);
    c.bench_function("tsscan_window", |b| {
        b.iter(|| black_box(execute(&plan, &ctx).unwrap()))
    });
}

#[cfg(feature = "text")]
fn bench_fuse(c: &mut Criterion) {
    // RRF FUSE of the vector + graph-distance legs over the same seed (the `text`
    // feature gates the `FuseRrf` op even when a branch is non-lexical).
    let (view, semantic, _) = build_dataset(N, DIM, DATA_SEED);
    let ctx = PlanCtx::new(&view, &semantic);
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::FuseRrf {
            branches: vec![
                vec![Op::Rank {
                    query: query_vec(DIM, QUERY_SEED),
                }],
                vec![Op::RankNodeDistance {
                    center: "d0".into(),
                }],
            ],
            k: 0.0,
        },
        Op::Limit { k: 20 },
    ]);
    c.bench_function("fuse_rrf", |b| {
        b.iter(|| black_box(execute(&plan, &ctx).unwrap()))
    });
}

// ── recall@k of the vector leg (the tight, deterministic gate) ─────────────────────

/// Measures recall@k of the ANN `semantic_search` vs the brute-force cosine oracle,
/// ASSERTS it stays ≥ [`RECALL_FLOOR`] (so a recall regression fails this bench binary
/// directly), and WRITES the measured value to the JSON the CI gate reports. Also runs a
/// `criterion` timing over the ANN probe so the vector leg's latency is tracked too.
fn bench_recall(c: &mut Criterion) {
    let (_, semantic, vectors) = build_dataset(N, DIM, DATA_SEED);
    let q = query_vec(DIM, QUERY_SEED);

    let recall = recall_at_k(&semantic, &vectors, &q, RECALL_K);
    eprintln!("recall@{RECALL_K} = {recall:.4} (floor {RECALL_FLOOR:.4})");

    // Emit the measured recall for the CI gate BEFORE the floor assertion, so the
    // reported artifact exists even on a failing run. Default to the WORKSPACE `target/`
    // (derived from `CARGO_MANIFEST_DIR` = crates/eg-plan, up two) — cargo runs the bench
    // binary with CWD = the crate dir, so a bare relative path would miss the workspace
    // `target/` the gate reads. `EG_BENCH_RECALL_OUT` overrides.
    let out = std::env::var("EG_BENCH_RECALL_OUT").unwrap_or_else(|_| {
        format!(
            "{}/../../target/eg_plan_recall.json",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if let Some(parent) = std::path::Path::new(&out).parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(
        &out,
        format!(
            "{{\"recall_at_k\": {recall}, \"k\": {RECALL_K}, \"floor\": {RECALL_FLOOR}, \"n\": {N}, \"dim\": {DIM}}}\n"
        ),
    );

    assert!(
        recall >= RECALL_FLOOR,
        "vector-leg recall@{RECALL_K} = {recall:.4} dropped below the floor {RECALL_FLOOR:.4} \
         — the ANN index or cosine path regressed"
    );

    c.bench_function("vector_search_k10", |b| {
        b.iter(|| black_box(semantic.semantic_search(&q, RECALL_K)))
    });
}

// ── scale sweep + the selective-filter optimizer ablation (Phase-2 T-E1) ───────────

/// The N rungs the scale sweep measures. The 1M rung is heavy (HNSW build over 1M
/// vectors + a full-graph traverse/rank) so it is gated behind `EG_BENCH_SCALE_1M=1`; a
/// normal run sweeps {10k, 100k} so it stays tractable. The point is to show the cost
/// optimizer's reorder win GROWING with scale — invisible in variance at the fixed
/// harness N, measurable at 100k/1M.
fn scale_rungs() -> Vec<usize> {
    let mut ns = vec![10_000usize, 100_000];
    if std::env::var("EG_BENCH_SCALE_1M").is_ok() {
        ns.push(1_000_000);
    }
    ns
}

/// The HIGHLY selective filter the ablation pushes ahead of the `Rank`: equality on TWO
/// categorical columns (`topic == "t0"` ∧ `lang == "l0"`), ~0.08% of the nodes. Categorical
/// equality is what a filter-pushdown targets, and the cost model's per-`Eq` heuristic
/// (0.1 each ⇒ 0.01 combined) recognizes it as selective enough to REORDER — so the
/// optimizer fires here. Its numeric-range twin is [`selective_range_preds`], which the
/// column-histogram estimate now pushes too (previously a fixed 0.33 heuristic declined it).
fn selective_filter_preds() -> Vec<Pred> {
    vec![
        Pred::Eq {
            prop: "topic".into(),
            value: "t0".into(),
        },
        Pred::Eq {
            prop: "lang".into(),
            value: "l0".into(),
        },
    ]
}

/// The selective-filter pipeline: `Scan → Rank → Filter(selective) → Limit`. The `Filter`
/// sits AFTER the expensive vector `Rank`. With `EPISTEMIC_GRAPH_COST_OPT=1` (default) the
/// optimizer's filter-pushdown rule moves the `Filter` AHEAD of the `Rank` so the reranker
/// runs over the narrow set; with `=0` the `Rank` scans the whole candidate set first.
fn selective_filter_plan() -> Plan {
    Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Rank {
            query: query_vec(DIM, QUERY_SEED),
        },
        Op::Filter {
            preds: selective_filter_preds(),
        },
        Op::Limit { k: 20 },
    ])
}

/// A HIGHLY selective NUMERIC-RANGE filter (`score > n-50`, i.e. only the top ~50 of `n`
/// nodes) — the range counterpart to [`selective_filter_preds`]. This is the case the fixed
/// per-`GtNum` heuristic (0.33) always DECLINED regardless of true selectivity; with the
/// column-histogram estimate (CONCEPT:EG-KG.query.column-range-stats) its real ~50/n selectivity is
/// read from the data, so the optimizer now pushes it filter-first exactly like the
/// categorical `Eq` case. `n` is the dataset size (so the top tail stays a fixed ~50 rows —
/// absolutely selective at every rung).
fn selective_range_preds(n: usize) -> Vec<Pred> {
    vec![Pred::GtNum {
        prop: "score".into(),
        n: n.saturating_sub(50) as f64,
    }]
}

/// The selective-RANGE pipeline: `Scan → Rank → Filter(score > n-50) → Limit`, mirroring
/// [`selective_filter_plan`] but with a numeric range instead of categorical equality. Demos
/// the range-pushdown win the column stats unlock.
fn selective_range_plan(n: usize) -> Plan {
    Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Rank {
            query: query_vec(DIM, QUERY_SEED),
        },
        Op::Filter {
            preds: selective_range_preds(n),
        },
        Op::Limit { k: 20 },
    ])
}

/// Size-sweep of the optimizer-load-bearing hybrid pipelines across
/// N ∈ {10k, 100k, (1M behind `EG_BENCH_SCALE_1M`)}: `match_traverse_rank` and `fuse_rrf`
/// (the two whose cross-modal reordering is load-bearing) plus the `selective_filter`
/// ablation. Latency only — recall is the fixed-N deterministic [`bench_recall`] gate.
/// The one-time HNSW build is pulled OUT of the timed loop (a throwaway search) so each
/// measured p50 reflects the QUERY, not the index build.
fn bench_scale_sweep(c: &mut Criterion) {
    eprintln!(
        "scale sweep: cost_opt={} rungs={:?}",
        cost_opt_enabled(),
        scale_rungs()
    );
    let mut group = c.benchmark_group("hybrid_scale");
    // Heavy at 100k/1M (full-graph traverse + HNSW retrieval): a small sample keeps the
    // sweep bounded while criterion still reports a stable p50.
    group.sample_size(10);
    for n in scale_rungs() {
        let (view, semantic, _) = build_dataset(n, DIM, DATA_SEED);
        let ctx = PlanCtx::new(&view, &semantic);
        // Build the HNSW index ONCE, outside the measured iterations.
        let _ = semantic.semantic_search(&query_vec(DIM, QUERY_SEED), 1);
        group.throughput(Throughput::Elements(n as u64));

        let mtr = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 3,
            },
            Op::Rank {
                query: query_vec(DIM, QUERY_SEED),
            },
            Op::Limit { k: 20 },
        ]);
        group.bench_with_input(BenchmarkId::new("match_traverse_rank", n), &n, |b, _| {
            b.iter(|| black_box(execute(&mtr, &ctx).unwrap()))
        });

        let sel = selective_filter_plan();
        group.bench_with_input(BenchmarkId::new("selective_filter", n), &n, |b, _| {
            b.iter(|| black_box(execute(&sel, &ctx).unwrap()))
        });

        // The RANGE counterpart: `Scan → Rank → Filter(score > n-50) → Limit`. With column
        // stats the optimizer now pushes this selective range filter-first (the fixed 0.33
        // heuristic never did) — the reorder win the T-E1 range ablation surfaced.
        let sel_range = selective_range_plan(n);
        group.bench_with_input(BenchmarkId::new("selective_range_filter", n), &n, |b, _| {
            b.iter(|| black_box(execute(&sel_range, &ctx).unwrap()))
        });

        #[cfg(feature = "text")]
        {
            let fuse = Plan::new(vec![
                Op::Scan {
                    label: "Doc".into(),
                },
                Op::FuseRrf {
                    branches: vec![
                        vec![Op::Rank {
                            query: query_vec(DIM, QUERY_SEED),
                        }],
                        vec![Op::RankNodeDistance {
                            center: "d0".into(),
                        }],
                    ],
                    k: 0.0,
                },
                Op::Limit { k: 20 },
            ]);
            group.bench_with_input(BenchmarkId::new("fuse_rrf", n), &n, |b, _| {
                b.iter(|| black_box(execute(&fuse, &ctx).unwrap()))
            });
        }
    }
    group.finish();
}

/// The cross-modal 3-OP CHAIN case (Track H / `GlobalChainCost`, CONCEPT:EG-KG.query.global-plan-cost):
/// `Scan → Rank → broad_filter(score>50) → selective_filter(score>n-50) → Limit` — TWO
/// `Filter`s on the same wide `score` column (one broad, one highly selective) both sitting
/// AFTER the `Rank`. The pairwise-only mechanism is structurally stuck here: it costs the
/// adjacent `(Rank, broad_filter)` pair, keeps `Rank` first (broad stays vector-first), and
/// then ADVANCES PAST `selective_filter` without ever comparing it to anything — the exact
/// gap `GlobalChainCost`'s exhaustive whole-chain search closes by finding the true 3!-minimum
/// and pushing the selective filter ahead of the `Rank`.
fn crossmodal_chain_plan(n: usize) -> Plan {
    Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Rank {
            query: query_vec(DIM, QUERY_SEED),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "score".into(),
                n: 50.0,
            }],
        },
        Op::Filter {
            preds: selective_range_preds(n),
        },
        Op::Limit { k: 20 },
    ])
}

/// A fixed-N (gateable) instance of the selective-filter pipeline. The scale sweep above
/// shows the ON-vs-OFF win widening with N; this top-level bench gives the CI gate one
/// stable p50 to ceiling (the grouped sweep IDs are nested and skipped by the gate). Runs
/// under whatever `EPISTEMIC_GRAPH_COST_OPT` the process sets — default ON.
fn bench_selective_filter_rank(c: &mut Criterion) {
    let (view, semantic, _) = build_dataset(N, DIM, DATA_SEED);
    let ctx = PlanCtx::new(&view, &semantic);
    let plan = selective_filter_plan();
    c.bench_function("selective_filter_rank", |b| {
        b.iter(|| black_box(execute(&plan, &ctx).unwrap()))
    });
}

/// Same-process filter-pushdown ablation (Phase-2 T-E1). Times the ORIGINAL logical plan
/// (`Scan → Rank → Filter → Limit`) against the OPTIMIZER-REORDERED plan
/// (`Scan → Filter → Rank → Limit`) BACK-TO-BACK, INTERLEAVED, on ONE built dataset — so
/// identical instantaneous machine load cancels in the ratio. This is the clean read of
/// the optimizer's value: criterion's CROSS-RUN comparison (COST_OPT=1 vs =0 in separate
/// processes) is swamped by ambient load on a shared host — the untouched
/// `match_traverse_rank` control alone swings ±40% between runs, far above any real
/// optimizer delta. Bypasses the env kill-switch by executing the two PHYSICAL op lists
/// directly (`optimize` gives the reordered list; `execute_ops` runs each). Prints
/// median/p50 ms for each and the speedup. Gated by `EG_BENCH_ABLATION=1`; honors
/// `EG_BENCH_SCALE_1M`. When set it runs INSTEAD of the criterion benches (returns early).
fn run_filter_pushdown_ablation() -> bool {
    if std::env::var("EG_BENCH_ABLATION").is_err() {
        return false;
    }
    use std::time::Instant;
    let med = |mut v: Vec<f64>| -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    eprintln!("\n== filter-pushdown ablation (p50 interleaved iters, ms) ==");
    eprintln!(
        "{:>10}  {:>18}  {:>12}  {:>12}  {:>9}  {:>12}",
        "N", "variant", "rank_first", "filter_first", "speedup", "opt_picks"
    );
    for n in scale_rungs() {
        // Fewer iterations at scale so a 1M rung stays bounded (each execute is seconds).
        let iters = if n >= 500_000 { 7 } else { 25 };
        let (view, semantic, _) = build_dataset(n, DIM, DATA_SEED);
        let ctx = PlanCtx::new(&view, &semantic);
        // Build the HNSW index ONCE, off the clock.
        let _ = semantic.semantic_search(&query_vec(DIM, QUERY_SEED), 1);

        // The narrower each variant filters on (categorical `Eq` vs numeric-range `GtNum`);
        // the RANGE one is the case column stats newly push. Each is timed as its two PHYSICAL
        // orderings so the ablation measures the reorder's real cost regardless of the pick.
        let variants: [(&str, Plan); 2] = [
            ("selective_filter", selective_filter_plan()),
            ("selective_range", selective_range_plan(n)),
        ];
        for (name, rank_first) in variants {
            // The filter-first physical ordering: swap the (Rank, Filter) pair to Filter-first.
            let filter_first = Plan::new(vec![
                rank_first.ops[0].clone(), // Scan
                rank_first.ops[2].clone(), // Filter
                rank_first.ops[1].clone(), // Rank
                rank_first.ops[3].clone(), // Limit
            ]);
            // What the cost optimizer WOULD pick — now driven by the REAL data distribution
            // (column histogram) for the range variant, not the fixed 0.33 heuristic.
            let opt_picks = if matches!(
                optimize(&rank_first, &ctx).ops.get(1),
                Some(Op::Filter { .. })
            ) {
                "filter_first"
            } else {
                "rank_first"
            };

            // Untimed warm passes, then INTERLEAVE the two so ambient drift hits both equally.
            for _ in 0..3 {
                let _ = execute_ops(&rank_first.ops, &ctx).unwrap();
                let _ = execute_ops(&filter_first.ops, &ctx).unwrap();
            }
            let mut rf = Vec::with_capacity(iters);
            let mut ff = Vec::with_capacity(iters);
            for _ in 0..iters {
                let t = Instant::now();
                let _ = black_box(execute_ops(&rank_first.ops, &ctx).unwrap());
                rf.push(t.elapsed().as_secs_f64() * 1e3);
                let t = Instant::now();
                let _ = black_box(execute_ops(&filter_first.ops, &ctx).unwrap());
                ff.push(t.elapsed().as_secs_f64() * 1e3);
            }
            let (rf, ff) = (med(rf), med(ff));
            eprintln!(
                "{n:>10}  {name:>18}  {rf:>12.2}  {ff:>12.2}  {:>8.2}x  {opt_picks:>12}",
                rf / ff
            );
        }

        // The 3-op GLOBAL CHAIN case (Track H): naive left-to-right vs whatever
        // `optimize()`'s exhaustive `GlobalChainCost` search actually picks — NOT a
        // hand-forced swap like the two variants above, since this reorder can move either
        // `Filter` past the `Rank` OR past each other (a 3-element permutation, not a pair).
        // The "rank_first"/"filter_first" header columns are repurposed here as
        // naive/optimized.
        {
            let naive = crossmodal_chain_plan(n);
            let opt = optimize(&naive, &ctx);
            let opt_picks = if opt.ops == naive.ops {
                "unchanged"
            } else {
                "reordered"
            };
            for _ in 0..3 {
                let _ = execute_ops(&naive.ops, &ctx).unwrap();
                let _ = execute_ops(&opt.ops, &ctx).unwrap();
            }
            let mut nv = Vec::with_capacity(iters);
            let mut ov = Vec::with_capacity(iters);
            for _ in 0..iters {
                let t = Instant::now();
                let _ = black_box(execute_ops(&naive.ops, &ctx).unwrap());
                nv.push(t.elapsed().as_secs_f64() * 1e3);
                let t = Instant::now();
                let _ = black_box(execute_ops(&opt.ops, &ctx).unwrap());
                ov.push(t.elapsed().as_secs_f64() * 1e3);
            }
            let (nv, ov) = (med(nv), med(ov));
            eprintln!(
                "{n:>10}  {:>18}  {nv:>12.2}  {ov:>12.2}  {:>8.2}x  {opt_picks:>12}",
                "crossmodal_chain",
                nv / ov
            );
        }
    }
    true
}

/// Allocation-light synthetic hierarchy for isolating LeanRAG's bounded child
/// selection. Children and embeddings are generated deterministically on demand,
/// so the benchmark does not hide ranking cost behind a second storage index.
struct LeanRagBenchFixture {
    summaries: usize,
    fanout: usize,
}

impl AnnIndex for LeanRagBenchFixture {
    fn search(
        &self,
        _query: &[f32],
        k: usize,
        allow: Option<&dyn Fn(&str) -> bool>,
    ) -> Vec<Scored> {
        (0..self.summaries)
            .map(|index| Scored {
                id: format!("summary-{index}"),
                score: 1.0 - index as f32 / self.summaries.max(1) as f32,
            })
            .filter(|row| allow.is_none_or(|predicate| predicate(&row.id)))
            .take(k)
            .collect()
    }
}

impl GraphTopology for LeanRagBenchFixture {
    fn label(&self, id: &str) -> Option<String> {
        id.starts_with("summary-")
            .then(|| "SummaryNode".to_string())
    }

    fn children(&self, id: &str) -> Vec<String> {
        let Some(summary) = id.strip_prefix("summary-") else {
            return Vec::new();
        };
        (0..self.fanout)
            .map(|child| format!("leaf-{summary}-{child}"))
            .collect()
    }

    fn embedding(&self, id: &str) -> Option<Vec<f32>> {
        let child = id.rsplit('-').next()?.parse::<usize>().ok()?;
        Some(vec![
            1.0,
            (self.fanout.saturating_sub(child) as f32 + 1.0) / self.fanout.max(1) as f32,
        ])
    }
}

/// Wide fan-outs make an accidental full child sort visible while retaining a
/// small fixed drill/context budget, the intended LeanRAG operating regime.
fn bench_leanrag_bounded_topk(c: &mut Criterion) {
    let mut group = c.benchmark_group("leanrag_bounded_topk");
    for &fanout in &[64usize, 4_096] {
        let fixture = LeanRagBenchFixture {
            summaries: 4,
            fanout,
        };
        let retriever = HierarchicalRetriever::new(&fixture, &fixture);
        let params = RetrievalParams {
            k: 4,
            drill_depth: 1,
            drill_breadth: 8,
            leaf_budget: 16,
        };
        group.throughput(Throughput::Elements((fixture.summaries * fanout) as u64));
        group.bench_with_input(BenchmarkId::from_parameter(fanout), &fanout, |b, _| {
            b.iter(|| black_box(retriever.retrieve(black_box(&[1.0, 1.0]), params)))
        });
    }
    group.finish();
}

fn all_benches(c: &mut Criterion) {
    if run_filter_pushdown_ablation() {
        return; // ablation mode: skip the long criterion sweep
    }
    bench_match_traverse_rank(c);
    bench_selective_filter_rank(c);
    bench_scale_sweep(c);
    bench_recall(c);
    bench_leanrag_bounded_topk(c);
    #[cfg(feature = "owl")]
    bench_reason_rank_traverse(c);
    #[cfg(feature = "timeseries")]
    bench_tsscan_window(c);
    #[cfg(feature = "text")]
    bench_fuse(c);
}

criterion_group! {
    name = benches;
    // Bounded, non-flaky: a short warm-up + a modest sample floor keep wall-clock low
    // (the CI gate is spot-check + recall, not a micro-optimized p50 chase).
    config = Criterion::default()
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(3))
        .sample_size(30);
    targets = all_benches
}
criterion_main!(benches);
