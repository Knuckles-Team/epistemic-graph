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
//!      - `FUSE`                     (RRF hybrid of vector+lexical+graph legs, `text`);
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

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, Op, Plan, PlanCtx};
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
        core.add_node(
            id.clone(),
            blob(json!({ "type": "Doc", "year": 2000 + (k as i64 % 30) })),
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
        #[cfg(feature = "rdf-redb")]
        None,
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

fn all_benches(c: &mut Criterion) {
    bench_match_traverse_rank(c);
    bench_recall(c);
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
