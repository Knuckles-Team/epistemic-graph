//! The query-side adapter head (EH-396): fit, evaluate and apply.
//!
//! The model is `q' = q + sum_i g_i (u_i . q) u_i` over orthonormal `u_i` with
//! `|g_i| <= max_gain`. Directions come from the judged contrast operator
//! `M = sum_items 1/2 (q d^T + d q^T)`, `d = mean(cited) - mean(hard negatives)`:
//! its dominant eigen-directions are where moving the query most separates
//! what an independently judged answer cited from what it passed over.
//! Directions are found by projected power iteration (never forming the
//! `d x d` matrix); each gain is then chosen from a fixed grid (zero included)
//! to maximise training pairwise accuracy, so a direction that does not help
//! gets gain zero. Everything is serial over the caller's item order with
//! correctly rounded IEEE arithmetic only, so a fit replays bit-identically.

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::retrieval_adapter::{
    AdapterDirection, QueryAdapterBody, QUERY_ADAPTER_SCHEMA_VERSION, UNIT_SCALE_BITS,
};

/// Power-iteration steps per direction.
const POWER_STEPS: usize = 64;
/// Below this norm a vector is treated as zero.
const EPSILON: f64 = 1e-12;
/// The gain grid, as fractions of `max_gain`, zero first so ties keep zero.
const GAIN_GRID: [f64; 7] = [0.0, 0.25, -0.25, 0.5, -0.5, 1.0, -1.0];
/// The two-sided 95% normal quantile of the Wilson bound.
const WILSON_Z: f64 = 1.959_963_984_540_054;
const Q16: f64 = 65_536.0;

/// One judged retrieval: its query and the stored vectors of what the answer
/// cited (`positives`) and passed over while ranking it higher (`negatives`).
#[derive(Debug, Clone, PartialEq)]
pub struct JudgedItem {
    pub query: Vec<f64>,
    pub positives: Vec<Vec<f64>>,
    pub negatives: Vec<Vec<f64>>,
}

/// One fitted direction.
#[derive(Debug, Clone, PartialEq)]
pub struct Direction {
    pub unit: Vec<f64>,
    pub gain: f64,
}

/// The held-out comparison of adapted against base ranking.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Evaluation {
    pub n: u64,
    pub wins: u64,
    pub losses: u64,
    pub ties: u64,
    pub base_mrr: f64,
    pub adapted_mrr: f64,
    pub win_rate_lower: f64,
}

fn dot(left: &[f64], right: &[f64]) -> f64 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

fn norm(v: &[f64]) -> f64 {
    dot(v, v).sqrt()
}

fn mean(rows: &[Vec<f64>], dim: usize) -> Vec<f64> {
    let mut out = vec![0.0; dim];
    for row in rows {
        for (slot, value) in out.iter_mut().zip(row) {
            *slot += value;
        }
    }
    let n = rows.len() as f64;
    out.iter_mut().for_each(|slot| *slot /= n);
    out
}

/// `(q, d)` per usable item: both sides present and the declared width.
fn contrasts(items: &[JudgedItem], dim: usize) -> Vec<(&[f64], Vec<f64>)> {
    items
        .iter()
        .filter(|item| {
            item.query.len() == dim && !item.positives.is_empty() && !item.negatives.is_empty()
        })
        .map(|item| {
            let pos = mean(&item.positives, dim);
            let neg = mean(&item.negatives, dim);
            let d = pos.iter().zip(&neg).map(|(p, n)| p - n).collect();
            (item.query.as_slice(), d)
        })
        .collect()
}

fn apply_contrast(pairs: &[(&[f64], Vec<f64>)], v: &[f64]) -> Vec<f64> {
    let mut out = vec![0.0; v.len()];
    for (q, d) in pairs {
        let (qv, dv) = (dot(q, v), dot(d, v));
        for ((slot, qi), di) in out.iter_mut().zip(q.iter()).zip(d) {
            *slot += 0.5 * (qi * dv + di * qv);
        }
    }
    out
}

/// Remove every found direction's component from `v`, then normalise it;
/// `None` when nothing is left.
fn orthonormalise(mut v: Vec<f64>, found: &[Vec<f64>]) -> Option<Vec<f64>> {
    for u in found {
        let projection = dot(&v, u);
        v.iter_mut()
            .zip(u)
            .for_each(|(slot, ui)| *slot -= projection * ui);
    }
    let length = norm(&v);
    (length > EPSILON).then(|| v.iter().map(|x| x / length).collect())
}

fn start_vector(pairs: &[(&[f64], Vec<f64>)], dim: usize, found: &[Vec<f64>]) -> Option<Vec<f64>> {
    let mut seed = vec![0.0; dim];
    for (_, d) in pairs {
        seed.iter_mut().zip(d).for_each(|(slot, di)| *slot += di);
    }
    orthonormalise(seed, found).or_else(|| {
        (0..dim).find_map(|axis| {
            let mut basis = vec![0.0; dim];
            basis[axis] = 1.0;
            orthonormalise(basis, found)
        })
    })
}

fn dominant_direction(
    pairs: &[(&[f64], Vec<f64>)],
    dim: usize,
    found: &[Vec<f64>],
) -> Option<Vec<f64>> {
    let mut v = start_vector(pairs, dim, found)?;
    for _ in 0..POWER_STEPS {
        v = orthonormalise(apply_contrast(pairs, &v), found)?;
    }
    Some(v)
}

/// Apply fitted directions to one query vector.
pub fn apply(directions: &[Direction], query: &[f64]) -> Vec<f64> {
    let mut out = query.to_vec();
    for direction in directions {
        let scale = direction.gain * dot(&direction.unit, query);
        out.iter_mut()
            .zip(&direction.unit)
            .for_each(|(slot, ui)| *slot += scale * ui);
    }
    out
}

/// `(correctly ordered pairs, pairs)` of one item under a query.
fn ordered_pairs(item: &JudgedItem, query: &[f64]) -> (u64, u64) {
    let mut right = 0;
    let mut total = 0;
    for p in &item.positives {
        let sp = dot(query, p);
        for n in &item.negatives {
            total += 1;
            right += u64::from(sp > dot(query, n));
        }
    }
    (right, total)
}

fn pairwise_accuracy(items: &[JudgedItem], directions: &[Direction]) -> f64 {
    let (right, total) = items.iter().fold((0_u64, 0_u64), |(r, t), item| {
        let (ri, ti) = ordered_pairs(item, &apply(directions, &item.query));
        (r + ri, t + ti)
    });
    if total == 0 {
        return 0.0;
    }
    right as f64 / total as f64
}

fn best_gain(items: &[JudgedItem], fitted: &mut Vec<Direction>, unit: Vec<f64>, max_gain: f64) {
    let mut best = (f64::MIN, 0.0);
    fitted.push(Direction { unit, gain: 0.0 });
    for fraction in GAIN_GRID {
        let gain = fraction * max_gain;
        if let Some(last) = fitted.last_mut() {
            last.gain = gain;
        }
        let score = pairwise_accuracy(items, fitted);
        if score > best.0 {
            best = (score, gain);
        }
    }
    if let Some(last) = fitted.last_mut() {
        last.gain = best.1;
    }
}

/// Fit at most `rank` directions of width `dim`, gains within `max_gain`.
/// Items of another width, or with no cited or no negative vector, are
/// skipped. An empty result means there was nothing to learn.
pub fn fit(items: &[JudgedItem], dim: usize, rank: usize, max_gain: f64) -> Vec<Direction> {
    let pairs = contrasts(items, dim);
    if pairs.is_empty() {
        return Vec::new();
    }
    let usable: Vec<JudgedItem> = items
        .iter()
        .filter(|item| item.query.len() == dim)
        .cloned()
        .collect();
    let mut units: Vec<Vec<f64>> = Vec::new();
    let mut fitted: Vec<Direction> = Vec::new();
    while units.len() < rank {
        let Some(unit) = dominant_direction(&pairs, dim, &units) else {
            break;
        };
        units.push(unit.clone());
        best_gain(&usable, &mut fitted, unit, max_gain);
    }
    fitted
}

/// The reciprocal rank of the best cited unit among cited and negatives.
fn reciprocal_rank(item: &JudgedItem, query: &[f64]) -> Option<f64> {
    let best = item
        .positives
        .iter()
        .map(|p| dot(query, p))
        .fold(None, |acc: Option<f64>, s| {
            Some(acc.map_or(s, |a| a.max(s)))
        })?;
    let above = item
        .negatives
        .iter()
        .filter(|n| dot(query, n) > best)
        .count();
    Some(1.0 / (above + 1) as f64)
}

/// Wilson 95% lower bound of `wins / n`; zero when `n == 0`.
pub fn wilson_lower(wins: u64, n: u64) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let n_f = n as f64;
    let p = wins as f64 / n_f;
    let z2 = WILSON_Z * WILSON_Z;
    let centre = p + z2 / (2.0 * n_f);
    let spread = WILSON_Z * (p * (1.0 - p) / n_f + z2 / (4.0 * n_f * n_f)).sqrt();
    (centre - spread) / (1.0 + z2 / n_f)
}

/// Compare adapted against base ranking on held-out `items`.
pub fn evaluate(items: &[JudgedItem], directions: &[Direction]) -> Evaluation {
    let mut eval = Evaluation {
        n: 0,
        wins: 0,
        losses: 0,
        ties: 0,
        base_mrr: 0.0,
        adapted_mrr: 0.0,
        win_rate_lower: 0.0,
    };
    for item in items {
        let adapted_query = apply(directions, &item.query);
        let (Some(base), Some(adapted)) = (
            reciprocal_rank(item, &item.query),
            reciprocal_rank(item, &adapted_query),
        ) else {
            continue;
        };
        eval.n += 1;
        eval.base_mrr += base;
        eval.adapted_mrr += adapted;
        eval.wins += u64::from(adapted > base);
        eval.losses += u64::from(adapted < base);
        eval.ties += u64::from(adapted == base);
    }
    if eval.n > 0 {
        eval.base_mrr /= eval.n as f64;
        eval.adapted_mrr /= eval.n as f64;
    }
    eval.win_rate_lower = wilson_lower(eval.wins, eval.wins + eval.losses);
    eval
}

fn quantise_unit(value: f64) -> i32 {
    let scaled = (value * f64::from(1_u32 << UNIT_SCALE_BITS)).round();
    scaled.clamp(f64::from(i32::MIN), f64::from(i32::MAX)) as i32
}

/// A value on `Q16`, rounded.
pub fn to_q16(value: f64) -> i64 {
    (value * Q16).round() as i64
}

/// The fixed-point body of fitted directions.
pub fn to_body(
    directions: &[Direction],
    space_digest: &str,
    dim: usize,
    training: (String, u64),
) -> Result<QueryAdapterBody, String> {
    let directions = directions
        .iter()
        .map(|d| {
            Ok(AdapterDirection {
                gain_q16: i32::try_from(to_q16(d.gain)).map_err(|e| e.to_string())?,
                unit_q30: BoundedVec::new(d.unit.iter().map(|x| quantise_unit(*x)).collect())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let body = QueryAdapterBody {
        schema_version: QUERY_ADAPTER_SCHEMA_VERSION,
        space_digest: space_digest.to_string(),
        dimensions: u32::try_from(dim).map_err(|e| e.to_string())?,
        directions: BoundedVec::new(directions)?,
        training_digest: training.0,
        n_training: training.1,
    };
    body.check()?;
    Ok(body)
}

/// The directions a body encodes, dequantised: what evaluation must score,
/// since the quantised body is what would be served.
pub fn directions_of(body: &QueryAdapterBody) -> Vec<Direction> {
    let unit_scale = f64::from(1_u32 << UNIT_SCALE_BITS);
    body.directions
        .iter()
        .map(|d| Direction {
            unit: d
                .unit_q30
                .iter()
                .map(|x| f64::from(*x) / unit_scale)
                .collect(),
            gain: f64::from(d.gain_q16) / Q16,
        })
        .collect()
}

/// The served form of a checked body: `f32` units and gains.
#[derive(Debug, Clone, PartialEq)]
pub struct AdapterKernel {
    units: Vec<Vec<f32>>,
    gains: Vec<f32>,
}

impl AdapterKernel {
    /// Decode a body; refuses one that fails [`QueryAdapterBody::check`].
    pub fn of(body: &QueryAdapterBody) -> Result<Self, String> {
        body.check()?;
        let directions = directions_of(body);
        let units = directions
            .iter()
            .map(|d| d.unit.iter().map(|x| *x as f32).collect())
            .collect();
        let gains = directions.iter().map(|d| d.gain as f32).collect();
        Ok(Self { units, gains })
    }

    /// Width the kernel applies to.
    pub fn dimensions(&self) -> usize {
        self.units.first().map_or(0, Vec::len)
    }

    /// `q + sum_i g_i (u_i . q) u_i`; `None` for a query of another width.
    pub fn apply(&self, query: &[f32]) -> Option<Vec<f32>> {
        if query.len() != self.dimensions() {
            return None;
        }
        let mut out = query.to_vec();
        for (unit, gain) in self.units.iter().zip(&self.gains) {
            let projection: f32 = unit.iter().zip(query).map(|(u, q)| u * q).sum();
            let scale = gain * projection;
            out.iter_mut()
                .zip(unit)
                .for_each(|(slot, ui)| *slot += scale * ui);
        }
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cited units lie along axis 1, the negatives the query prefers along
    /// axis 0: the base query ranks the negative first.
    fn item(tilt: f64) -> JudgedItem {
        JudgedItem {
            query: vec![1.0, 0.6 + tilt, 0.0],
            positives: vec![vec![0.2, 1.0, 0.0]],
            negatives: vec![vec![1.0, 0.0, 0.1]],
        }
    }

    // spec: EG-DECISION-ENGINE-R100, EG-DECISION-ENGINE-R101, EG-TYPED-PACKS-R078
    #[test]
    fn a_fit_moves_the_query_toward_what_was_cited() {
        let train: Vec<JudgedItem> = (0..8).map(|i| item(f64::from(i) * 0.01)).collect();
        let fitted = fit(&train, 3, 2, 0.5);
        assert!(!fitted.is_empty());
        let held_out: Vec<JudgedItem> = (0..6).map(|i| item(0.05 + f64::from(i) * 0.01)).collect();
        let eval = evaluate(&held_out, &fitted);
        assert_eq!(eval.n, 6);
        assert_eq!(eval.wins, 6);
        assert!(eval.adapted_mrr > eval.base_mrr);
        assert!(fitted.iter().all(|d| d.gain.abs() <= 0.5));
        assert_eq!(
            fit(&train, 3, 2, 0.5),
            fitted,
            "a fit replays bit-identically"
        );
    }

    // spec: EG-DECISION-ENGINE-R100, EG-DECISION-ENGINE-R101, EG-TYPED-PACKS-R078
    #[test]
    fn nothing_to_learn_fits_nothing_and_the_body_round_trips() {
        let mut lone = item(0.0);
        lone.negatives.clear();
        assert!(fit(&[lone], 3, 2, 0.5).is_empty());
        let fitted = fit(&[item(0.0), item(0.02)], 3, 1, 0.5);
        let body = to_body(&fitted, "sha256:s", 3, ("sha256:t".to_string(), 2)).unwrap();
        let kernel = AdapterKernel::of(&body).unwrap();
        let served = kernel.apply(&[1.0, 0.6, 0.0]).unwrap();
        let exact = apply(&fitted, &[1.0, 0.6, 0.0]);
        for (a, b) in served.iter().zip(&exact) {
            assert!((f64::from(*a) - b).abs() < 1e-4);
        }
        assert!(kernel.apply(&[1.0]).is_none());
    }

    // spec: EG-DECISION-ENGINE-R100, EG-DECISION-ENGINE-R101, EG-TYPED-PACKS-R078
    #[test]
    fn the_wilson_bound_needs_evidence() {
        assert_eq!(wilson_lower(0, 0), 0.0);
        assert!(wilson_lower(3, 3) < 0.5);
        assert!(wilson_lower(30, 30) > 0.5);
    }
}
