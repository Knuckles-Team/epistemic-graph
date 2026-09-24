//! Derived series (EH-522 / EH-524): a [`SeriesExpr`] compiled into a tree of
//! `eg_numeric::series` kernels that advances one observation at a time.
//!
//! A [`Program`] is plain serde data — its serialised form is the CHECKPOINT a materialised
//! derived series stores, and advancing a restored program equals a whole-history run
//! (the same kernels run both ways). Every surface evaluates expressions here: UQL
//! `DERIVE` (eg-plan), a materialised derived series maintained on `TsAppend` (the
//! server's timeseries handlers) — and the SQL `eg_*` window functions and PromQL run
//! the same `eg_numeric::series` kernels one level down.

/// The kernel crate, re-exported so a caller of a derived series names one path.
pub use eg_numeric::series as kernels;

use eg_numeric::series::{
    Arith, KalmanNoise, Map, PairStat, Rolling, Shift, Smoothing, Spec, State,
};
use eg_types::series_expr::{SeriesExpr, SeriesFunc};
use serde::{Deserialize, Serialize};

/// Materialised derived series: definition, checkpoints, advance and revision replay.
pub mod maintain;

/// The kernel generation a digest binds: bump when any kernel's output changes.
pub const KERNEL_VERSION: &str = "eg-series-kernels/1";

/// How a function builds its kernel from its numeric parameters.
#[derive(Clone, Copy)]
enum Build {
    Shift(Shift),
    Rolling(Rolling),
    Span,
    HalfLife,
    Map(Map),
    Clip,
    Arith(Arith),
    Pair(PairStat),
    /// A Kalman filter over `(q, r)`.
    Noise(fn(KalmanNoise) -> Spec),
}

use SeriesFunc as F;

/// Every function's kernel, in `eg_types::series_expr::SIGNATURES` order.
const BUILDS: &[(SeriesFunc, Build)] = &[
    (F::Lag, Build::Shift(Shift::Lag)),
    (F::Diff, Build::Shift(Shift::Diff)),
    (F::Ret, Build::Shift(Shift::Ret)),
    (F::Logret, Build::Shift(Shift::LogRet)),
    (F::Rmean, Build::Rolling(Rolling::Mean)),
    (F::Rstd, Build::Rolling(Rolling::Std)),
    (F::Rsum, Build::Rolling(Rolling::Sum)),
    (F::Rmin, Build::Rolling(Rolling::Min)),
    (F::Rmax, Build::Rolling(Rolling::Max)),
    (F::Rrank, Build::Rolling(Rolling::Rank)),
    (F::Zscore, Build::Rolling(Rolling::Zscore)),
    (F::Ewma, Build::Span),
    (F::EwmaHalflife, Build::HalfLife),
    (F::Abs, Build::Map(Map::Abs)),
    (F::Sign, Build::Map(Map::Sign)),
    (F::Neg, Build::Map(Map::Neg)),
    (F::Clip, Build::Clip),
    (F::Add, Build::Arith(Arith::Add)),
    (F::Sub, Build::Arith(Arith::Sub)),
    (F::Mul, Build::Arith(Arith::Mul)),
    (F::Div, Build::Arith(Arith::Div)),
    (F::Rcorr, Build::Pair(PairStat::Corr)),
    (F::Ic, Build::Pair(PairStat::RankCorr)),
    (F::Wsum, Build::Pair(PairStat::WeightedSum)),
    (F::Kalman, Build::Noise(Spec::KalmanLevel)),
    (F::Kbeta, Build::Noise(Spec::KalmanBeta)),
];

/// The kernel spec `func(…, params)` builds.
pub fn spec_of(func: SeriesFunc, params: &[f64]) -> Spec {
    let build = BUILDS
        .iter()
        .find(|(f, _)| *f == func)
        .map(|(_, b)| *b)
        .unwrap_or_else(|| unreachable!("every SeriesFunc has a BUILDS row"));
    let first = params.first().copied().unwrap_or(0.0);
    let count = first as usize;
    match build {
        Build::Shift(op) => Spec::Shift(op, count),
        Build::Rolling(op) => Spec::Rolling(op, count),
        Build::Span => Spec::Ewma(Smoothing::Span(first)),
        Build::HalfLife => Spec::Ewma(Smoothing::HalfLife(first)),
        Build::Map(map) => Spec::Map(map),
        Build::Clip => Spec::Map(Map::Clip {
            lo: first,
            hi: params.get(1).copied().unwrap_or(first),
        }),
        Build::Arith(op) => Spec::Arith(op),
        Build::Pair(op) => Spec::Pair(op, count),
        Build::Noise(make) => make(KalmanNoise {
            q: first,
            r: params.get(1).copied().unwrap_or(first),
        }),
    }
}

/// One node of a compiled expression.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Node {
    Channel(String),
    Const(f64),
    Kernel { state: State, args: Vec<Node> },
}

impl Node {
    fn compile(expr: &SeriesExpr) -> Result<Self, String> {
        Ok(match expr {
            SeriesExpr::Channel { name } => Node::Channel(name.clone()),
            SeriesExpr::Const { value } => Node::Const(*value),
            SeriesExpr::Call { func, args, params } => Node::Kernel {
                state: State::new(spec_of(*func, params)).map_err(|e| e.to_string())?,
                args: args.iter().map(Node::compile).collect::<Result<_, _>>()?,
            },
        })
    }

    /// Advance on one observation. Every argument advances whether or not this node
    /// produces a value, so a nested kernel never falls behind.
    fn step(&mut self, row: &dyn Fn(&str) -> Option<f64>) -> Option<f64> {
        match self {
            Node::Channel(name) => row(name),
            Node::Const(v) => Some(*v),
            Node::Kernel { state, args } => {
                let mut inputs = args.iter_mut().map(|a| a.step(row));
                let x = inputs.next().flatten();
                let y = inputs.next().flatten();
                state.step(x, y)
            }
        }
    }
}

/// A compiled series expression and its whole incremental state (the checkpoint).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Program {
    root: Node,
}

impl Program {
    /// Compile `expr` (refused when it is malformed or a kernel parameter is out of domain).
    pub fn compile(expr: &SeriesExpr) -> Result<Self, String> {
        expr.check()?;
        Ok(Self {
            root: Node::compile(expr)?,
        })
    }

    /// Advance on one observation whose channels `row` resolves; the value at it, or
    /// `None` while warming up / where undefined.
    pub fn step(&mut self, row: &dyn Fn(&str) -> Option<f64>) -> Option<f64> {
        self.root.step(row)
    }

    /// The checkpoint bytes (MessagePack, bit-exact).
    pub fn checkpoint(&self) -> Result<Vec<u8>, String> {
        rmp_serde::to_vec(self).map_err(|e| e.to_string())
    }

    /// Restore a program from [`Self::checkpoint`] bytes.
    pub fn restore(bytes: &[u8]) -> Result<Self, String> {
        rmp_serde::from_slice(bytes).map_err(|e| e.to_string())
    }
}

/// The provenance digest of a derived column: `sha256:` over the domain, the expression's
/// canonical UQL spelling, and the kernel generation.
pub fn expr_digest(expr: &SeriesExpr) -> Result<String, String> {
    let canonical = eg_types::wire::uql_series_expr(expr).map_err(|e| e.to_string())?;
    Ok(eg_compute::finance::market::digest::framed(
        "eg/series-derive",
        &[canonical.as_bytes(), KERNEL_VERSION.as_bytes()],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::series_expr::SIGNATURES;

    #[test]
    fn every_function_has_a_kernel_in_signature_order() {
        let funcs: Vec<SeriesFunc> = BUILDS.iter().map(|(f, _)| *f).collect();
        let sigs: Vec<SeriesFunc> = SIGNATURES.iter().map(|s| s.func).collect();
        assert_eq!(funcs, sigs);
    }

    fn zscore_of_ewma() -> SeriesExpr {
        let ewma = SeriesExpr::call(F::Ewma, vec![SeriesExpr::channel("v0")], vec![3.0]);
        SeriesExpr::call(F::Zscore, vec![ewma], vec![4.0])
    }

    #[test]
    fn a_restored_checkpoint_continues_exactly() {
        let xs: Vec<f64> = (0..40)
            .map(|i| f64::from(i % 7) * 1.25 + f64::from(i) / 3.0)
            .collect();
        let run = |p: &mut Program, xs: &[f64]| -> Vec<Option<u64>> {
            xs.iter()
                .map(|&x| p.step(&|_| Some(x)).map(f64::to_bits))
                .collect()
        };
        let whole = run(&mut Program::compile(&zscore_of_ewma()).unwrap(), &xs);
        for split in [0, 3, 17, 40] {
            let mut p = Program::compile(&zscore_of_ewma()).unwrap();
            let mut got = run(&mut p, &xs[..split]);
            let mut restored = Program::restore(&p.checkpoint().unwrap()).unwrap();
            got.extend(run(&mut restored, &xs[split..]));
            assert_eq!(got, whole, "split at {split}");
        }
    }

    #[test]
    fn a_nested_kernel_warms_up_on_its_own_inputs() {
        let mut p = Program::compile(&zscore_of_ewma()).unwrap();
        let out: Vec<Option<f64>> = (0..5).map(|i| p.step(&|_| Some(f64::from(i)))).collect();
        assert_eq!(
            &out[..3],
            &[None, None, None],
            "zscore(…, 4) needs 4 ewma values"
        );
        assert!(out[3].is_some());
    }

    #[test]
    fn digests_bind_the_canonical_spelling() {
        let a = expr_digest(&zscore_of_ewma()).unwrap();
        let b = expr_digest(&SeriesExpr::call(
            F::Rmean,
            vec![SeriesExpr::channel("v0")],
            vec![4.0],
        ))
        .unwrap();
        assert!(a.starts_with("sha256:"));
        assert_ne!(a, b);
    }
}
