//! The series-expression algebra (EH-522, ANALYTICS-HARVEST-20260924 §4 AH-02): the wire
//! AST of a UQL `DERIVE` column and of a materialised derived series (EH-524).
//!
//! An expression reads the value channels of a series row (`v0..vk`, `score`, or an
//! earlier `DERIVE` alias), numeric constants, and calls into the fixed function table
//! [`SeriesFunc`]. The UQL spelling is function-call only — `zscore(ewma(v0, 12), 60)`,
//! `div(wsum(v0, v1, 20), rsum(v1, 20))` — so the canonical print (the digest input) has
//! no precedence to normalise. The parser is eg-plan's UQL parser; the printer is
//! `eg_types::wire::uql_series_expr`; the kernels are `eg_numeric::series`.

use serde::{Deserialize, Serialize};

/// One node of a series expression.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SeriesExpr {
    /// A row's value channel: `v0..vk` (a `TSSCAN` field), `score`, or a `DERIVE` alias.
    Channel { name: String },
    /// A constant series.
    Const { value: f64 },
    /// `func(args…, params…)`: `args` are series, `params` numbers (a window, a span…).
    Call {
        func: SeriesFunc,
        args: Vec<SeriesExpr>,
        params: Vec<f64>,
    },
}

/// The fixed function table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SeriesFunc {
    Lag,
    Diff,
    Ret,
    Logret,
    Rmean,
    Rstd,
    Rsum,
    Rmin,
    Rmax,
    Rrank,
    Zscore,
    Ewma,
    EwmaHalflife,
    Abs,
    Sign,
    Neg,
    Clip,
    Add,
    Sub,
    Mul,
    Div,
    Rcorr,
    Ic,
    Wsum,
    Kalman,
    Kbeta,
    Gt,
    Lt,
    Greatest,
    Least,
    Mprofile,
}

/// A function's spelling and signature: how many series arguments, then how many
/// numeric parameters, and whether those parameters are positive integer counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Signature {
    pub func: SeriesFunc,
    pub name: &'static str,
    pub series: usize,
    pub params: usize,
    pub count: Count,
}

/// Whether a function's numeric parameters are counts (a lag, a window).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Count {
    /// Every parameter is a positive integer (lag / window / subsequence length).
    Integer,
    /// Parameters are real numbers (a span, clip bounds) or there are none.
    Real,
}

const fn sig(
    func: SeriesFunc,
    name: &'static str,
    series: usize,
    params: usize,
    count: Count,
) -> Signature {
    Signature {
        func,
        name,
        series,
        params,
        count,
    }
}

use Count::{Integer, Real};
use SeriesFunc as F;

/// Every function, in reference order (the grammar's `series_func` list).
pub const SIGNATURES: &[Signature] = &[
    sig(F::Lag, "lag", 1, 1, Integer),
    sig(F::Diff, "diff", 1, 1, Integer),
    sig(F::Ret, "ret", 1, 1, Integer),
    sig(F::Logret, "logret", 1, 1, Integer),
    sig(F::Rmean, "rmean", 1, 1, Integer),
    sig(F::Rstd, "rstd", 1, 1, Integer),
    sig(F::Rsum, "rsum", 1, 1, Integer),
    sig(F::Rmin, "rmin", 1, 1, Integer),
    sig(F::Rmax, "rmax", 1, 1, Integer),
    sig(F::Rrank, "rrank", 1, 1, Integer),
    sig(F::Zscore, "zscore", 1, 1, Integer),
    sig(F::Ewma, "ewma", 1, 1, Real),
    sig(F::EwmaHalflife, "ewma_halflife", 1, 1, Real),
    sig(F::Abs, "abs", 1, 0, Real),
    sig(F::Sign, "sign", 1, 0, Real),
    sig(F::Neg, "neg", 1, 0, Real),
    sig(F::Clip, "clip", 1, 2, Real),
    sig(F::Add, "add", 2, 0, Real),
    sig(F::Sub, "sub", 2, 0, Real),
    sig(F::Mul, "mul", 2, 0, Real),
    sig(F::Div, "div", 2, 0, Real),
    sig(F::Rcorr, "rcorr", 2, 1, Integer),
    sig(F::Ic, "ic", 2, 1, Integer),
    sig(F::Wsum, "wsum", 2, 1, Integer),
    sig(F::Kalman, "kalman", 1, 2, Real),
    sig(F::Kbeta, "kbeta", 2, 2, Real),
    sig(F::Gt, "gt", 2, 0, Real),
    sig(F::Lt, "lt", 2, 0, Real),
    sig(F::Greatest, "greatest", 2, 0, Real),
    sig(F::Least, "least", 2, 0, Real),
    sig(F::Mprofile, "mprofile", 1, 2, Integer),
];

impl SeriesFunc {
    /// This function's signature row.
    pub fn signature(self) -> Signature {
        SIGNATURES
            .iter()
            .copied()
            .find(|s| s.func == self)
            .unwrap_or_else(|| unreachable!("every SeriesFunc has a SIGNATURES row"))
    }

    /// The function a (case-insensitive) spelling names; `ratio` is `div`.
    pub fn from_name(name: &str) -> Option<Self> {
        let name = name.to_ascii_lowercase();
        let name = if name == "ratio" {
            "div".to_string()
        } else {
            name
        };
        SIGNATURES.iter().find(|s| s.name == name).map(|s| s.func)
    }
}

impl SeriesExpr {
    /// A call node.
    pub fn call(func: SeriesFunc, args: Vec<SeriesExpr>, params: Vec<f64>) -> Self {
        SeriesExpr::Call { func, args, params }
    }

    /// A channel reference.
    pub fn channel(name: &str) -> Self {
        SeriesExpr::Channel {
            name: name.to_string(),
        }
    }

    /// Refuse a call whose arity disagrees with its signature, a non-finite number, or a
    /// count parameter that is not a positive integer. Returns the first problem found.
    pub fn check(&self) -> Result<(), String> {
        match self {
            SeriesExpr::Channel { name } if name.is_empty() => Err("an empty channel name".into()),
            SeriesExpr::Channel { .. } => Ok(()),
            SeriesExpr::Const { value } => finite(*value),
            SeriesExpr::Call { func, args, params } => {
                check_call(func.signature(), args.len(), params)?;
                args.iter().try_for_each(SeriesExpr::check)
            }
        }
    }

    /// Every channel the expression reads, in first-use order, once.
    pub fn channels(&self) -> Vec<&str> {
        let mut out = Vec::new();
        self.collect_channels(&mut out);
        out
    }

    fn collect_channels<'a>(&'a self, out: &mut Vec<&'a str>) {
        match self {
            SeriesExpr::Channel { name } if !out.contains(&name.as_str()) => out.push(name),
            SeriesExpr::Channel { .. } | SeriesExpr::Const { .. } => {}
            SeriesExpr::Call { args, .. } => args.iter().for_each(|a| a.collect_channels(out)),
        }
    }
}

fn finite(v: f64) -> Result<(), String> {
    if v.is_finite() {
        return Ok(());
    }
    Err(format!("a non-finite number {v}"))
}

fn check_call(sig: Signature, args: usize, params: &[f64]) -> Result<(), String> {
    if args != sig.series || params.len() != sig.params {
        return Err(format!(
            "{}() takes {} series and {} number(s), got {args} and {}",
            sig.name,
            sig.series,
            sig.params,
            params.len()
        ));
    }
    params.iter().try_for_each(|p| finite(*p))?;
    let count_ok = sig.count == Real || params.iter().all(|&p| p >= 1.0 && p.fract() == 0.0);
    if count_ok {
        return Ok(());
    }
    Err(format!("{}() needs positive integer counts", sig.name))
}

/// One `DERIVE` column: an expression and the channel name its values are written to.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DeriveColumn {
    pub expr: SeriesExpr,
    pub name: String,
}

/// A UQL `SKILL` stage (EH-522 FeatureSkill): which channels, horizons, IC window and
/// bootstrap.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SkillOp {
    pub feature: String,
    pub outcome: String,
    pub horizons: Vec<u64>,
    pub window: u64,
    /// Bootstrap resamples for the interval on the mean IC (`0`: none).
    #[serde(default)]
    pub resamples: u64,
    #[serde(default)]
    pub seed: u64,
}

/// A UQL `MOTIF` / `DISCORD` stage (EH-529): which value channel is searched, for what,
/// how many hits, and the seed of the matrix profile's anytime diagonal order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MotifOp {
    pub channel: String,
    pub search: MotifSearch,
    pub top: u64,
    #[serde(default)]
    pub seed: u64,
}

/// What a `MOTIF` / `DISCORD` stage looks for in each series.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MotifSearch {
    /// `MOTIF LIKE shape`: the windows z-normalised-closest to `shape` (MASS).
    Like { shape: Vec<f64> },
    /// `MOTIF LENGTH m`: the closest pairs of length-`m` windows (matrix-profile minima).
    Pairs { length: u64 },
    /// `DISCORD LENGTH m`: the length-`m` windows farthest from every other window
    /// (matrix-profile maxima).
    Discord { length: u64 },
}

/// The value channels a `MOTIF` / `DISCORD` row carries.
pub const MOTIF_CHANNELS: [&str; 5] = ["distance", "start", "end", "neighbor", "approximate"];

/// The value channels a `SKILL` stage writes on each report row.
pub const SKILL_CHANNELS: [&str; 8] = [
    "mean_ic", "ic_std", "icir", "ic_lo", "ic_hi", "n", "n_eff", "ir",
];

/// What `TsDefineSeries` answers (EH-524): the definition, its lineage and provenance,
/// and how far maintenance got.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DerivedSeriesReceipt {
    pub series_id: String,
    /// Lineage: the series this one is derived from (`:DerivedSeries derivedFrom :Series`).
    pub derived_from: String,
    /// The expression's canonical UQL spelling.
    pub expr: String,
    /// `sha256:` over the canonical spelling and the kernel generation.
    pub digest: String,
    pub kernel_version: String,
    /// Derived points appended by this call.
    pub appended: u64,
    /// Whether every source point is reflected (`false`: the work budget stopped this
    /// call; the next append or definition continues).
    pub caught_up: bool,
    /// The last source timestamp reflected.
    pub last_ts: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_function_has_one_signature_and_resolves_by_name() {
        for s in SIGNATURES {
            assert_eq!(s.func.signature(), *s);
            assert_eq!(SeriesFunc::from_name(&s.name.to_uppercase()), Some(s.func));
        }
        assert_eq!(SeriesFunc::from_name("ratio"), Some(SeriesFunc::Div));
        assert_eq!(SeriesFunc::from_name("nope"), None);
    }

    #[test]
    fn check_refuses_bad_arity_and_counts() {
        let v0 = SeriesExpr::channel("v0");
        let ok = SeriesExpr::call(SeriesFunc::Zscore, vec![v0.clone()], vec![60.0]);
        assert_eq!(ok.check(), Ok(()));
        let frac = SeriesExpr::call(SeriesFunc::Lag, vec![v0.clone()], vec![1.5]);
        assert!(frac.check().is_err());
        let arity = SeriesExpr::call(SeriesFunc::Add, vec![v0.clone()], vec![]);
        assert!(arity.check().is_err());
        let span = SeriesExpr::call(SeriesFunc::Ewma, vec![v0], vec![2.5]);
        assert_eq!(span.check(), Ok(()), "a span is real");
    }

    #[test]
    fn channels_are_listed_once_in_first_use_order() {
        let e = SeriesExpr::call(
            SeriesFunc::Div,
            vec![
                SeriesExpr::call(
                    SeriesFunc::Wsum,
                    vec![SeriesExpr::channel("v0"), SeriesExpr::channel("v1")],
                    vec![20.0],
                ),
                SeriesExpr::call(
                    SeriesFunc::Rsum,
                    vec![SeriesExpr::channel("v1")],
                    vec![20.0],
                ),
            ],
            vec![],
        );
        assert_eq!(e.channels(), vec!["v0", "v1"]);
    }
}
