//! The z-normalised matrix profile by SCRIMP++ (EH-529): for every length-`m` subsequence,
//! the distance to (and index of) its nearest non-trivial neighbour. Its minima are the
//! series' motifs, its maxima its discords.
//!
//! SCRIMP++ is anytime. PreSCRIMP samples every `⌈m/4⌉`-th subsequence, takes its whole
//! distance profile by FFT and refines along its nearest neighbour's diagonal — a close
//! approximation within a small fraction of the work. SCRIMP then walks every diagonal
//! in a seeded order, O(1) per cell (the dot product slides along the diagonal and is
//! re-anchored directly every [`ANCHOR_EVERY`] cells). Work is counted in distance
//! evaluations against `max_work`: a cut returns the best-so-far with
//! `approximate = true`.
//!
//! Determinism: a COMPLETE profile is built only from the full-diagonal walks (each cell
//! once, in fixed arithmetic), so it is bit-identical on every node and for every seed;
//! the FFT-derived PreSCRIMP values enter only an approximate (cut) result. A run whose
//! whole cost fits the budget skips PreSCRIMP.

use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use super::distance::{centred, dot, exclusion_zone, znorm_distance, SubsequenceStats};
use super::fft::Correlator;
use super::mass::check_series;
use super::stampi::ANCHOR_EVERY;
use crate::error::Result;

/// How much work a profile may take, and the diagonal order's seed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProfileOptions {
    pub m: usize,
    /// Most distance evaluations; a larger need is cut (approximate result).
    pub max_work: u64,
    pub seed: u64,
}

/// A matrix profile.
#[derive(Clone, Debug, PartialEq)]
pub struct MatrixProfile {
    pub m: usize,
    /// Nearest non-trivial neighbour distance per subsequence (`inf` where none was seen).
    pub distance: Vec<f64>,
    pub neighbor: Vec<Option<usize>>,
    /// Whether the budget cut the computation (the values are upper bounds).
    pub approximate: bool,
    /// Distance evaluations spent.
    pub work: u64,
}

/// One reported subsequence: where it starts, its neighbour and the distance.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Hit {
    pub start: usize,
    pub neighbor: Option<usize>,
    pub distance: f64,
}

/// Which extreme a selection takes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Extreme {
    /// Smallest distances first (motifs, query matches).
    Nearest,
    /// Largest finite distances first (discords).
    Farthest,
}

/// The best nearest neighbour seen per subsequence.
#[derive(Clone, Debug)]
struct Profile {
    distance: Vec<f64>,
    neighbor: Vec<Option<usize>>,
}

impl Profile {
    fn new(len: usize) -> Self {
        Self {
            distance: vec![f64::INFINITY; len],
            neighbor: vec![None; len],
        }
    }

    /// Offer `d` as subsequence `i`'s distance to `j` (ties keep the lower index).
    fn offer(&mut self, i: usize, j: usize, d: f64) {
        let held = self.distance[i];
        if d < held || (d == held && self.neighbor[i].is_none_or(|k| j < k)) {
            self.distance[i] = d;
            self.neighbor[i] = Some(j);
        }
    }

    fn merge(mut self, other: &Profile) -> Self {
        for i in 0..self.distance.len() {
            if let Some(j) = other.neighbor[i] {
                self.offer(i, j, other.distance[i]);
            }
        }
        self
    }
}

/// Which profile a diagonal walk feeds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Sampled,
    Full,
}

/// Whether a phase ran to completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Done,
    Cut,
}

struct Scrimp<'a> {
    xs: &'a [f64],
    m: usize,
    stats: SubsequenceStats,
    work: u64,
    max_work: u64,
    sampled: Profile,
    full: Profile,
}

impl Scrimp<'_> {
    fn placements(&self) -> usize {
        self.stats.mean.len()
    }

    fn charge(&mut self, cost: usize) -> Phase {
        if self.work + cost as u64 > self.max_work {
            return Phase::Cut;
        }
        self.work += cost as u64;
        Phase::Done
    }

    /// Cells `(i, i + k)` for `i` in `from..to`, feeding `target`.
    fn walk(&mut self, k: usize, from: usize, to: usize, target: Target) {
        let (xs, m) = (self.xs, self.m);
        let mut qt = 0.0;
        for i in from..to {
            let j = i + k;
            qt = if (i - from) % ANCHOR_EVERY == 0 {
                dot(&xs[i..i + m], &xs[j..j + m])
            } else {
                qt - xs[i - 1] * xs[j - 1] + xs[i + m - 1] * xs[j + m - 1]
            };
            let d = znorm_distance(qt, m, self.stats.at(i), self.stats.at(j));
            let profile = match target {
                Target::Sampled => &mut self.sampled,
                Target::Full => &mut self.full,
            };
            profile.offer(i, j, d);
            profile.offer(j, i, d);
        }
    }

    /// PreSCRIMP: sampled FFT distance profiles, each refined along its best diagonal.
    fn pre_scrimp(&mut self) -> Phase {
        let (l, m) = (self.placements(), self.m);
        let step = exclusion_zone(m);
        let correlator = Correlator::new(self.xs, m);
        for i in (0..l).step_by(step) {
            if self.charge(l + 2 * step) == Phase::Cut {
                return Phase::Cut;
            }
            let qt = correlator.dots(&self.xs[i..i + m]);
            if let Some(j) = self.sample_row(i, &qt) {
                let (a, b) = (i.min(j), i.max(j));
                let (from, to) = (a.saturating_sub(step - 1), (a + step).min(l - (b - a)));
                self.walk(b - a, from, to, Target::Sampled);
            }
        }
        Phase::Done
    }

    /// Feed one sampled distance profile; the row's nearest neighbour.
    fn sample_row(&mut self, i: usize, qt: &[f64]) -> Option<usize> {
        let excl = exclusion_zone(self.m);
        let mut best: Option<(f64, usize)> = None;
        for (j, &q) in qt.iter().enumerate() {
            if i.abs_diff(j) <= excl {
                continue;
            }
            let d = znorm_distance(q, self.m, self.stats.at(i), self.stats.at(j));
            self.sampled.offer(i, j, d);
            self.sampled.offer(j, i, d);
            if best.is_none_or(|(held, _)| d < held) {
                best = Some((d, j));
            }
        }
        best.map(|(_, j)| j)
    }

    /// SCRIMP: every non-trivial diagonal, in a seeded order.
    fn scrimp(&mut self, seed: u64) -> Phase {
        let l = self.placements();
        let mut order: Vec<usize> = (exclusion_zone(self.m) + 1..l).collect();
        order.shuffle(&mut ChaCha8Rng::seed_from_u64(seed));
        for k in order {
            if self.charge(l - k) == Phase::Cut {
                return Phase::Cut;
            }
            self.walk(k, 0, l - k, Target::Full);
        }
        Phase::Done
    }
}

/// The matrix profile of `xs` for subsequence length `options.m` under a work budget.
pub fn matrix_profile(xs: &[f64], options: ProfileOptions) -> Result<MatrixProfile> {
    check_series(xs, options.m)?;
    let xs = centred(xs);
    let stats = SubsequenceStats::of(&xs, options.m);
    let l = stats.mean.len();
    let mut run = Scrimp {
        xs: &xs,
        m: options.m,
        stats,
        work: 0,
        max_work: options.max_work,
        sampled: Profile::new(l),
        full: Profile::new(l),
    };
    let whole = (l * l) as u64 / 2;
    let fits = whole <= options.max_work;
    let complete = (fits || run.pre_scrimp() == Phase::Done) && run.scrimp(options.seed) == Phase::Done;
    let profile = if complete {
        run.full
    } else {
        run.full.merge(&run.sampled)
    };
    Ok(MatrixProfile {
        m: options.m,
        distance: profile.distance,
        neighbor: profile.neighbor,
        approximate: !complete,
        work: run.work,
    })
}

/// Up to `k` subsequence starts, best first by `extreme`, no two within `m` of each
/// other (nor of a chosen one's `partner`). Ties go to the lower start.
pub fn select(
    distance: &[f64],
    k: usize,
    m: usize,
    extreme: Extreme,
    partner: &dyn Fn(usize) -> Option<usize>,
) -> Vec<usize> {
    let mut order: Vec<usize> = (0..distance.len()).filter(|&i| distance[i].is_finite()).collect();
    order.sort_by(|&a, &b| match extreme {
        Extreme::Nearest => distance[a].total_cmp(&distance[b]).then(a.cmp(&b)),
        Extreme::Farthest => distance[b].total_cmp(&distance[a]).then(a.cmp(&b)),
    });
    let mut taken: Vec<usize> = Vec::new();
    let mut chosen = Vec::new();
    for i in order {
        if chosen.len() == k {
            break;
        }
        if taken.iter().any(|&t| t.abs_diff(i) < m) {
            continue;
        }
        chosen.push(i);
        taken.push(i);
        taken.extend(partner(i));
    }
    chosen
}

/// The top-`k` motif pairs: each hit's neighbour is its match.
pub fn motifs(profile: &MatrixProfile, k: usize) -> Vec<Hit> {
    let partner = |i: usize| profile.neighbor[i];
    hits(profile, select(&profile.distance, k, profile.m, Extreme::Nearest, &partner))
}

/// The top-`k` discords: the subsequences farthest from their nearest neighbour.
pub fn discords(profile: &MatrixProfile, k: usize) -> Vec<Hit> {
    let partner = |_: usize| None;
    hits(profile, select(&profile.distance, k, profile.m, Extreme::Farthest, &partner))
}

fn hits(profile: &MatrixProfile, starts: Vec<usize>) -> Vec<Hit> {
    starts
        .into_iter()
        .map(|start| Hit {
            start,
            neighbor: profile.neighbor[start],
            distance: profile.distance[start],
        })
        .collect()
}
