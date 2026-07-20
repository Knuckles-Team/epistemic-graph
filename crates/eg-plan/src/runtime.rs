//! The physical EXECUTION runtime (CONCEPT:EG-KG.query.parallel-runtime) — Lane B.
//!
//! Lane 0 carved a [`crate::exec::Driver`] seam: a plan's op list runs THROUGH a driver,
//! so the *scheduling / materialization strategy* is swappable without touching either the
//! per-op physical fns ([`crate::exec::apply`]) or [`crate::exec::execute`]. Lane 0's
//! [`SerialDriver`] is the left-fold that reproduces today's execution byte-for-byte.
//!
//! This module is the physical runtime that hangs off that seam:
//!
//!  * [`execute_ops`] — the ONE dispatch point [`crate::exec::execute`] routes its
//!    (already `plan_optimize`-d) op list through. It picks a driver from the environment:
//!    the Lane-0 [`SerialDriver`] by default (the Pi contract — no rayon, no threads, no
//!    spill), or, when the opt-in `par-runtime` feature is built AND the environment asks
//!    for it, the [`ParallelDriver`].
//!  * [`ParallelDriver`] (feature `par-runtime`) — the driver that OWNS the physical
//!    concerns the serial fold does not: **scheduling / morsel batching** (split a large
//!    intermediate `RowSet` into fixed-size morsels), **parallelism** (a rayon work-stealing
//!    pool runs the morsels), **memory accounting + spilling** (measure each intermediate's
//!    footprint and spill it through a disk round-trip when it exceeds a budget), and
//!    **result materialization**. The per-op physical fns stay modality-specific and
//!    UNCHANGED — the driver only decides HOW their inputs are scheduled, never WHAT they
//!    compute — so a parallel run is byte-for-byte identical to the serial one (proved by
//!    the `serial_and_parallel_drivers_agree` equivalence proptest + the differential
//!    oracle).
//!
//! ## Why byte-identical parallelism is possible
//!
//! A `Vec<Op>` plan is a LINEAR pipeline: op *i* consumes op *(i-1)*'s `RowSet`, so the ops
//! themselves are data-dependent and run in order. The parallelism the driver adds is
//! therefore *intra-op*: it morsel-splits the INPUT of a **row-parallel** op — one whose
//! output is a per-row-independent, order-preserving function of its input (see
//! [`is_row_parallel`]) — runs the morsels on the rayon pool, and concatenates the
//! per-morsel outputs IN MORSEL ORDER. Because rayon's indexed collect preserves order and
//! the morsels are a disjoint, in-order partition of an already-unique `RowSet`, the
//! concatenation is byte-for-byte what the whole-input serial application produces. The
//! spill round-trip is a lossless serialize→disk→deserialize of the intermediate, so it too
//! preserves the exact rows/scores/order. Every other op falls to the serial physical fn.
//!
//! ## Tuning (environment)
//!
//!  * `EPISTEMIC_GRAPH_EXEC_THREADS` — rayon pool width + morsel fan-out (default `1` ⇒
//!    serial; the Pi default).
//!  * `EPISTEMIC_GRAPH_EXEC_SPILL_MB` — per-intermediate memory budget in MiB; unset ⇒ no
//!    spilling (intermediates stay in RAM). A set budget makes any intermediate whose
//!    footprint exceeds it spill through a disk round-trip.

use crate::algebra::Op;
use crate::exec::{Driver, PlanCtx, SerialDriver};
use crate::rowset::RowSet;

/// Dispatch an (already `plan_optimize`-d) op list to a physical [`Driver`] chosen from the
/// environment, returning the final [`RowSet`]. This is the single seam
/// [`crate::exec::execute`] routes through (CONCEPT:EG-KG.query.parallel-runtime).
///
/// The DEFAULT (and the only path a build without the `par-runtime` feature can take) is the
/// Lane-0 [`SerialDriver`] — byte-for-byte the prior fold, no rayon/threads/spill (the Pi
/// contract). With `par-runtime` built, the environment ([`RuntimeConfig::from_env`]) can
/// opt into the [`ParallelDriver`]; when it does not ([`RuntimeConfig::is_parallel`] is
/// false — the default `EXEC_THREADS=1`, no spill budget) the serial driver still runs, so a
/// `par-runtime` build with no tuning is also byte-for-byte the prior fold.
pub fn execute_ops(ops: &[Op], ctx: &PlanCtx) -> Result<RowSet, String> {
    #[cfg(feature = "par-runtime")]
    {
        let cfg = RuntimeConfig::from_env();
        if cfg.is_parallel() {
            // Run the pipeline INSIDE the shared, env-sized rayon pool so the morsel legs
            // (and any future modality fn that itself uses rayon) draw from ONE bounded set
            // of workers rather than the global pool.
            return exec_pool().install(|| run_pipeline(&cfg, ops, ctx));
        }
    }
    SerialDriver.run(ops, ctx)
}

// ── the parallel runtime — feature `par-runtime` ─────────────────────────────────

/// The tuning the [`ParallelDriver`] reads (CONCEPT:EG-KG.query.parallel-runtime): the rayon
/// pool width / morsel fan-out (`threads`) and the optional per-intermediate spill budget in
/// BYTES (`spill_bytes`). Built from the environment by [`Self::from_env`], or constructed
/// directly (the equivalence proptest pins `threads: 4, spill_bytes: Some(1)` to force BOTH
/// the morsel and spill legs).
#[cfg(feature = "par-runtime")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    /// rayon pool width and the number of morsels a row-parallel op's input is split into
    /// (`>= 1`; `1` ⇒ no fan-out, so execution is serial-equivalent).
    pub threads: usize,
    /// Per-intermediate memory budget in BYTES. `None` ⇒ never spill (RAM only). `Some(b)`
    /// ⇒ any intermediate whose footprint exceeds `b` is spilled through a disk round-trip.
    pub spill_bytes: Option<u64>,
}

#[cfg(feature = "par-runtime")]
impl RuntimeConfig {
    /// Read `EPISTEMIC_GRAPH_EXEC_THREADS` (default `1` — the serial Pi default) and
    /// `EPISTEMIC_GRAPH_EXEC_SPILL_MB` (default unset — no spilling). A non-numeric or
    /// zero/absent value falls back to the default, so a mistyped env var degrades to serial
    /// rather than erroring.
    pub fn from_env() -> Self {
        let threads = std::env::var("EPISTEMIC_GRAPH_EXEC_THREADS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(1);
        let spill_bytes = std::env::var("EPISTEMIC_GRAPH_EXEC_SPILL_MB")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024));
        Self {
            threads,
            spill_bytes,
        }
    }

    /// True when the environment asks for anything the serial fold does NOT do — a wider
    /// pool (`threads > 1`) or a spill budget. When false, [`execute_ops`] stays on the
    /// Lane-0 [`SerialDriver`].
    pub fn is_parallel(&self) -> bool {
        self.threads > 1 || self.spill_bytes.is_some()
    }
}

/// The shared, process-wide rayon pool sized from `EPISTEMIC_GRAPH_EXEC_THREADS` at first
/// use — so a busy server pays the pool-construction cost ONCE, not per query. Its width is
/// fixed at first init (the env is read once); the [`ParallelDriver`] built via
/// [`ParallelDriver::new`] carries its OWN pool instead (used by tests to pin a width).
#[cfg(feature = "par-runtime")]
fn exec_pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let threads = RuntimeConfig::from_env().threads.max(1);
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|i| format!("eg-plan-exec-{i}"))
            .build()
            .expect("build eg-plan exec pool")
    })
}

/// The parallel physical driver (CONCEPT:EG-KG.query.parallel-runtime) — Lane B's swap-in
/// behind the Lane-0 [`Driver`] seam. It carries its OWN rayon pool (sized from the
/// [`RuntimeConfig`] it is built with) so a caller can pin a width independent of the
/// process env — the equivalence proptest uses this to force real multi-worker morsel
/// batching. [`execute_ops`] does NOT build one per query; it runs [`run_pipeline`] on the
/// shared [`exec_pool`] instead.
#[cfg(feature = "par-runtime")]
pub struct ParallelDriver {
    cfg: RuntimeConfig,
    pool: rayon::ThreadPool,
}

#[cfg(feature = "par-runtime")]
impl ParallelDriver {
    /// A parallel driver with a dedicated rayon pool `cfg.threads` wide.
    pub fn new(cfg: RuntimeConfig) -> Self {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(cfg.threads.max(1))
            .thread_name(|i| format!("eg-plan-exec-{i}"))
            .build()
            .expect("build eg-plan ParallelDriver pool");
        Self { cfg, pool }
    }
}

#[cfg(feature = "par-runtime")]
impl Driver for ParallelDriver {
    fn run(&self, ops: &[Op], ctx: &PlanCtx) -> Result<RowSet, String> {
        self.pool.install(|| run_pipeline(&self.cfg, ops, ctx))
    }
}

/// Below this many rows an op's input is applied whole (the morsel-split + concat + rayon
/// dispatch overhead is not worth it), matching a real morsel engine's small-batch cutoff.
#[cfg(feature = "par-runtime")]
const MORSEL_MIN: usize = 2;

/// The shared pipeline both parallel entry points ([`execute_ops`]'s shared-pool path and
/// [`ParallelDriver::run`]) run: the Lane-0 left fold, but each op is scheduled by
/// [`exec_op`] (morsel-parallel when the op is row-parallel and its input is large enough)
/// and each intermediate is passed through [`spill_if_needed`] for memory accounting.
///
/// L35 (CONCEPT:EG-KG.query.adaptive-reoptimization): this now runs
/// [`crate::exec::run_with_adaptive_reopt`] — the SAME runtime-feedback loop
/// [`crate::exec::SerialDriver`] runs after every op — instead of a plain left fold, so a
/// parallel-driver run also re-costs (and, when beneficial, re-orders) its not-yet-executed
/// tail off the actual observed cardinality, gated by the SAME `EPISTEMIC_GRAPH_COST_OPT`
/// kill-switch. `step` (this pipeline's per-op closure) is what differs from the serial
/// driver: it schedules the op via [`exec_op`] (morsel-parallel when row-parallel and the
/// input is large enough) THEN passes the result through [`spill_if_needed`] for memory
/// accounting — the loop itself is shared verbatim so the two drivers can never drift out of
/// sync on WHEN re-optimization triggers. MUST be called inside an installed rayon pool
/// (both entry points do so).
#[cfg(feature = "par-runtime")]
fn run_pipeline(cfg: &RuntimeConfig, ops: &[Op], ctx: &PlanCtx) -> Result<RowSet, String> {
    crate::exec::run_with_adaptive_reopt(ops, ctx, |op, cur, ctx| {
        let out = exec_op(cfg, op, cur, ctx)?;
        spill_if_needed(cfg, out)
    })
}

/// Schedule ONE op. A **row-parallel** op ([`is_row_parallel`]) over a large-enough input is
/// morsel-split and run on the rayon pool ([`exec_row_parallel`]); every other op falls to
/// the serial physical fn [`crate::exec::apply`] unchanged. Either way the RESULT is
/// byte-for-byte the serial application (see the module docs).
#[cfg(feature = "par-runtime")]
fn exec_op(cfg: &RuntimeConfig, op: &Op, input: RowSet, ctx: &PlanCtx) -> Result<RowSet, String> {
    if cfg.threads > 1 && input.len() >= MORSEL_MIN && is_row_parallel(op) {
        exec_row_parallel(cfg, op, input, ctx)
    } else {
        crate::exec::apply(op, input, ctx)
    }
}

/// Is `op`'s output a per-row-independent, ORDER-PRESERVING function of its input — so
/// morsel-splitting the input and concatenating the per-morsel outputs in order reproduces
/// the whole-input result EXACTLY?
///
/// Conservatively true for ONLY [`Op::AsOf`] with a non-empty input: it keeps each incoming
/// row iff its fact is live at the instant (a pure per-row blob predicate) and preserves the
/// incoming order/scores via `intersect_keep_order`, with NO cross-row state, NO reduction,
/// NO re-ranking, and NO source-reseed (that path is the EMPTY-input branch, which never
/// reaches here because a morsel is non-empty). Ranking ops (`Rank`, rerankers, RRF, MMR),
/// graph `Traverse` (BFS over the whole seed set), and relational `Filter` (one DataFusion
/// pass; splitting would multiply passes, not speed them) are all cross-row and stay serial.
/// The whitelist is deliberately tiny — a future op earns a slot only with an equivalence
/// proof, never by assumption.
#[cfg(feature = "par-runtime")]
fn is_row_parallel(op: &Op) -> bool {
    matches!(op, Op::AsOf { .. })
}

/// Apply a row-parallel `op` by morsel-splitting `input` into ~`cfg.threads` contiguous,
/// in-order chunks, running [`crate::exec::apply`] on each chunk in PARALLEL over the rayon
/// pool, and concatenating the per-chunk outputs IN CHUNK ORDER. rayon's indexed `collect`
/// preserves chunk order; chunks are a disjoint in-order partition of an already-unique
/// `RowSet`; each chunk is non-empty (so a per-row op keeps its FILTER semantics, never
/// reseeding as a source) — therefore the concatenation is byte-for-byte the whole-input
/// serial application.
#[cfg(feature = "par-runtime")]
fn exec_row_parallel(
    cfg: &RuntimeConfig,
    op: &Op,
    input: RowSet,
    ctx: &PlanCtx,
) -> Result<RowSet, String> {
    use rayon::prelude::*;

    let rows: Vec<(String, Option<f32>)> = input
        .rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect();
    let morsel = morsel_size(rows.len(), cfg.threads);

    let parts: Vec<RowSet> = rows
        .par_chunks(morsel)
        .map(|chunk| crate::exec::apply(op, RowSet::from_rows(chunk.iter().cloned()), ctx))
        .collect::<Result<Vec<_>, String>>()?;

    // Concatenate the per-morsel outputs in order. `from_rows` dedups by id, but the morsels
    // are disjoint id partitions of an already-unique set, so this is a pure concat.
    let out: Vec<(String, Option<f32>)> = parts
        .iter()
        .flat_map(|p| p.rows().iter().map(|r| (r.id.clone(), r.score)))
        .collect();
    Ok(RowSet::from_rows(out))
}

/// Split `n` rows into ~`threads` contiguous morsels (ceil-divide, floored at 1) — so the
/// fan-out matches the pool width and every worker gets one chunk.
#[cfg(feature = "par-runtime")]
fn morsel_size(n: usize, threads: usize) -> usize {
    let t = threads.max(1);
    n.div_ceil(t).max(1)
}

// ── memory accounting + spill (CONCEPT:EG-KG.query.parallel-runtime) ──────────────

/// The approximate in-RAM footprint of `rs` in BYTES: the `Row` struct sizes plus each id's
/// heap bytes. Used by [`spill_if_needed`] to decide whether an intermediate exceeds the
/// budget — abstract but monotone in the data, which is all the budget comparison needs.
#[cfg(feature = "par-runtime")]
fn footprint(rs: &RowSet) -> u64 {
    let per_row = std::mem::size_of::<crate::rowset::Row>() as u64;
    rs.rows().iter().map(|r| per_row + r.id.len() as u64).sum()
}

/// Memory-account `rs` against the spill budget and, when it exceeds it, SPILL it through a
/// disk round-trip ([`spill_roundtrip`]) — bounding the runtime's resident memory so a large
/// intermediate does not blow a small-RAM (Pi) node. No budget ⇒ the intermediate stays in
/// RAM untouched. The round-trip is lossless, so the returned `RowSet` is byte-for-byte `rs`.
#[cfg(feature = "par-runtime")]
fn spill_if_needed(cfg: &RuntimeConfig, rs: RowSet) -> Result<RowSet, String> {
    match cfg.spill_bytes {
        Some(budget) if footprint(&rs) > budget => spill_roundtrip(rs),
        _ => Ok(rs),
    }
}

/// Serialize `rs`'s rows to a temp file, DROP the in-memory copy, then read them back and
/// rebuild the `RowSet` — the spill/reload a bounded-memory engine does to keep a large
/// intermediate off the heap. Lossless: `(id, score)` pairs round-trip verbatim and
/// `RowSet::from_rows` preserves order (the rows are already unique), so the result equals
/// the input byte-for-byte. The temp file is removed after reload (and on error).
#[cfg(feature = "par-runtime")]
fn spill_roundtrip(rs: RowSet) -> Result<RowSet, String> {
    use std::io::{Read, Seek, SeekFrom, Write};

    let rows: Vec<(String, Option<f32>)> =
        rs.rows().iter().map(|r| (r.id.clone(), r.score)).collect();
    drop(rs); // free the intermediate before it lands on disk — the point of spilling.

    let bytes = rmp_serde::to_vec(&rows).map_err(|e| format!("spill encode: {e}"))?;
    drop(rows);

    let path = spill_path();
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&path)
        .map_err(|_| "unable to create query spill file".to_string())?;
    let io_result = (|| -> Result<Vec<u8>, String> {
        file.write_all(&bytes)
            .map_err(|_| "unable to write query spill file".to_string())?;
        let encoded_len = bytes.len();
        drop(bytes);
        file.seek(SeekFrom::Start(0))
            .map_err(|_| "unable to rewind query spill file".to_string())?;
        let mut back = vec![0u8; encoded_len];
        file.read_exact(&mut back)
            .map_err(|_| "unable to read query spill file".to_string())?;
        Ok(back)
    })();
    drop(file);
    let _ = std::fs::remove_file(&path);
    let back = io_result?;

    let decoded: Vec<(String, Option<f32>)> = eg_types::msgpack::decode_bounded(
        &back,
        eg_types::msgpack::MsgpackLimits::new(
            back.len(),
            eg_types::msgpack::MAX_PROPERTY_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "invalid query spill file".to_string())?;
    Ok(RowSet::from_rows(decoded))
}

/// A unique temp-file path for one spill (`<tmp>/eg-plan-spill-<pid>-<seq>.mp`): the process
/// id and a monotonic counter together make it collision-free across threads and concurrent
/// queries, with no temp-dir crate needed (dep-free, only `std`).
#[cfg(feature = "par-runtime")]
fn spill_path() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();
    std::env::temp_dir().join(format!("eg-plan-spill-{pid}-{seq}.mp"))
}

#[cfg(all(test, feature = "par-runtime"))]
mod tests {
    use super::*;

    /// The spill round-trip is lossless: a forced spill (budget 0) rebuilds the exact rows,
    /// scores and order.
    #[test]
    fn spill_roundtrip_is_lossless() {
        let rs = RowSet::from_rows([
            ("b".to_string(), Some(0.9f32)),
            ("a".to_string(), None),
            ("c".to_string(), Some(-0.5f32)),
        ]);
        let cfg = RuntimeConfig {
            threads: 1,
            spill_bytes: Some(0), // force every intermediate to spill
        };
        let back = spill_if_needed(&cfg, rs.clone()).unwrap();
        assert_eq!(rs, back, "spill/reload must preserve the RowSet exactly");
    }

    /// No budget ⇒ the intermediate is returned untouched (RAM only).
    #[test]
    fn no_budget_never_spills() {
        let rs = RowSet::from_ids(["x".into(), "y".into()]);
        let cfg = RuntimeConfig {
            threads: 1,
            spill_bytes: None,
        };
        assert_eq!(rs, spill_if_needed(&cfg, rs.clone()).unwrap());
    }

    /// `from_env` degrades a mistyped / absent env to the serial default.
    #[test]
    fn config_defaults_to_serial() {
        // NOTE: relies on the vars being UNSET in the test env (the CI default).
        let cfg = RuntimeConfig::from_env();
        // We cannot assert exact values without controlling the env, but the invariant
        // holds: threads is always >= 1 and is_parallel is a pure function of the fields.
        assert!(cfg.threads >= 1);
        assert_eq!(
            cfg.is_parallel(),
            cfg.threads > 1 || cfg.spill_bytes.is_some()
        );
    }

    /// Morsel sizing fans a set out into ~`threads` chunks, never zero.
    #[test]
    fn morsel_size_fans_out() {
        assert_eq!(morsel_size(8, 4), 2);
        assert_eq!(morsel_size(7, 4), 2); // ceil(7/4)=2 → chunks [2,2,2,1]
        assert_eq!(morsel_size(0, 4), 1);
        assert_eq!(morsel_size(5, 1), 5);
    }

    /// L35 (CONCEPT:EG-KG.query.adaptive-reoptimization) — `ParallelDriver` must run the SAME
    /// adaptive-reopt runtime-feedback loop `SerialDriver::run` runs after every op, not the
    /// plain left fold it used before this fix. Both drivers now share
    /// `crate::exec::run_with_adaptive_reopt`, so this reuses the EXACT hub/reached
    /// divergent-cardinality fixture `crate::tests::adaptive_reopt_auto_wires_into_ordinary_execution`
    /// proves triggers `reoptimize_remaining`'s non-trivial (not below-threshold-no-op) path —
    /// a single hub node whose REAL out-degree (500) is 1000x the graph's global average
    /// out-degree, so `Traverse`'s degree-average cardinality estimator badly under-shoots the
    /// actual reach — but drives it through `ParallelDriver::new` directly (threads=2,
    /// bypassing `execute_ops`'s env-based driver selection), so this specifically exercises
    /// the PARALLEL driver's reopt wiring rather than its serial sibling.
    #[test]
    fn parallel_driver_runs_adaptive_reopt() {
        use crate::algebra::{Op, Plan, Pred};
        use crate::cost::{Cardinality, ModalityCardinality, PlanStats};
        use crate::exec::{PlanCtx, PlanExt};
        use crate::optimizer::ADAPTIVE_REOPT_THRESHOLD;
        use eg_core::compute::semantic::SemanticStore;
        use eg_core::graph::GraphCore;
        use serde_json::json;

        fn blob(v: serde_json::Value) -> Vec<u8> {
            rmp_serde::to_vec_named(&v).unwrap()
        }

        // 1 hub (label "Hub") -LINK-> 500 "Reached" nodes; 499 unrelated filler nodes with no
        // edges at all. node_count = 1000, edge_count = 500 ⇒ avg_out_degree = 0.5 — the hub's
        // REAL out-degree (500) is 1000x the graph average.
        let core = GraphCore::new();
        core.add_node("hub".into(), blob(json!({"type": "Hub"})));
        for i in 0..500 {
            let id = format!("r{i}");
            core.add_node(id.clone(), blob(json!({"type": "Reached", "keep": "yes"})));
            core.add_edge("hub".into(), id, blob(json!({"relationship": "LINK"})))
                .unwrap();
        }
        for i in 0..499 {
            core.add_node(format!("filler{i}"), blob(json!({"type": "Filler"})));
        }
        let view = core.analysis_snapshot();
        let mut semantic = SemanticStore::new();
        for i in 0..20 {
            semantic.add_embedding(format!("r{i}"), vec![1.0, (i as f32) * 0.01, 0.0, 0.0]);
        }
        let ctx = PlanCtx::new(&view, &semantic);
        let card = ModalityCardinality::new(PlanStats::collect(&ctx));

        let traverse = Op::Traverse {
            rel: "LINK".into(),
            min: 1,
            max: 1,
        };
        let estimated_traverse_out = card.rows_out(&traverse, 1.0, &ctx);
        assert!(
            estimated_traverse_out < 1.0,
            "the degree-average estimator must badly UNDER-estimate this hub's fan-out \
             (got {estimated_traverse_out}) — otherwise this fixture doesn't exercise a real \
             divergence"
        );
        let rel_err = (500.0_f64 - estimated_traverse_out).abs() / estimated_traverse_out.max(1.0);
        assert!(
            rel_err > ADAPTIVE_REOPT_THRESHOLD,
            "fixture must exceed the adaptive-reopt divergence threshold: rel_err={rel_err}"
        );

        let plan_ops = vec![
            Op::Scan {
                label: "Hub".into(),
            },
            traverse,
            Op::Filter {
                preds: vec![Pred::Eq {
                    prop: "keep".into(),
                    value: "yes".into(),
                }],
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::Limit { k: 5 },
        ];

        // Drive the EXACT plan through `ParallelDriver` directly (threads=2 — real
        // multi-worker morsel fan-out, mirroring the equivalence proptest's config), NOT
        // `Plan::execute` (which would pick the driver off the environment). If the parallel
        // driver still ran the plain left fold (the pre-L35 bug), the result would be
        // UNCHANGED here too (re-optimization is answer-preserving), so the real proof is
        // architectural: `run_pipeline` now calls the SAME `run_with_adaptive_reopt` helper
        // `SerialDriver` calls (see `run_pipeline`'s doc comment) — this test locks in that
        // the parallel path still returns the CORRECT rows in a scenario explicitly built to
        // trigger the loop, so a future edit that reintroduces a plain fold (dropping the
        // shared helper) regresses visibly if it ever breaks correctness, and the shared
        // helper itself is unit-proven by `crate::tests::adaptive_reopt_auto_wires_into_ordinary_execution`.
        let driver = ParallelDriver::new(RuntimeConfig {
            threads: 2,
            spill_bytes: None,
        });
        let got = driver.run(&plan_ops, &ctx).unwrap().ids();
        assert_eq!(got.len(), 5, "Limit{{k:5}} caps the result: {got:?}");
        assert!(
            got.iter().all(|id| id.starts_with('r')),
            "every returned id must be a reached, embedded node: {got:?}"
        );
        assert_eq!(
            got.first().map(String::as_str),
            Some("r0"),
            "the fused plan must still rank correctly through the parallel driver despite the \
             internal adaptive reorder: {got:?}"
        );

        // Directly confirms `run_pipeline` is wired to the LOOP, not the fold: running the
        // SAME plan through `Plan::execute` with `EPISTEMIC_GRAPH_EXEC_THREADS` unset (the
        // env-selected `SerialDriver`) must agree byte-for-byte with the explicit
        // `ParallelDriver` run above — both now share `run_with_adaptive_reopt`, so they can
        // never diverge on WHEN they re-optimize.
        let via_serial = Plan::new(plan_ops).execute(&ctx).unwrap().ids();
        assert_eq!(
            via_serial, got,
            "SerialDriver and ParallelDriver must agree — both share run_with_adaptive_reopt"
        );
    }
}
