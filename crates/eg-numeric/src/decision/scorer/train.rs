//! Fitting the scorer's attention parameters (EH-302). Fit time only.
//!
//! The head's linear weights are fitted first (the listwise logistic fit) and
//! stay fixed: the scorer is a residual on them, and its own-embedding and
//! context weights start at zero, so training starts exactly at the linear
//! head. Each training item is standardised and shortlisted with the SERVED
//! integer path, so the options trained on are the options served. Then
//! full-batch gradient descent on the weighted listwise cross-entropy, with
//! hand-written backpropagation, a fixed step and a fixed epoch count: serial,
//! seeded, float only through correctly rounded operations and the pinned
//! `detkernel` softmax, so the same items give the same bits. The result is
//! quantised onto Q32; calibration then reads the quantised, served scorer.

use eg_types::contract::BoundedVec;
use eg_types::decision::jobs::OptimiserSpec;
use eg_types::decision::statistical::dataset::{LabelledDataset, LabelledItem};
use eg_types::decision::statistical::head::DecisionHeadBody;
use eg_types::decision::statistical::scorer::OptionAttentionParams;
use eg_types::decision::statistical::StatisticalErrorCode;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;

use super::fixed::{dot, to_f64, to_q32};
use super::forward::standardise;
use super::legal::shortlist;
use crate::decision::quant::item_rows;
use crate::decision::refusal::{Refusal, RefusalResult};
use crate::decision::targets::{audit_weight, targets};
use crate::detkernel::kernels::softmax;
use crate::detkernel::quantise::{quantise, QuantScale};

/// Embedding width of a fitted scorer.
pub const SCORER_WIDTH: usize = 8;
/// Shortlist of a fitted scorer.
pub const SCORER_SHORTLIST: usize = 16;
/// Most epochs a fit runs, whatever it asks for.
pub const MAX_EPOCHS: u32 = 200;
/// Gradient step.
pub const LEARNING_RATE: f64 = 0.05;
/// Initial parameters are uniform in `[-INIT_SCALE, INIT_SCALE)`.
pub const INIT_SCALE: f64 = 0.1;
/// Ridge strength.
pub const RIDGE: f64 = 1e-4;
/// Parameters are clamped to this magnitude before quantising.
pub const PARAMETER_LIMIT: f64 = 64.0;

/// One training item: the shortlisted options' standardised rows and linear
/// pre-scores, the target distribution over them, and the item's weight.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TrainItem {
    rows: Vec<Vec<f64>>,
    prescores: Vec<f64>,
    targets: Vec<f64>,
    weight: f64,
}

/// The scorer's parameters in working precision, input-major like the wire.
#[derive(Debug, Clone, PartialEq)]
struct Net {
    features: usize,
    width: usize,
    slots: [Vec<f64>; 7],
}

/// Slot order: embed, bias, query, key, value, own, context.
const EMBED: usize = 0;
const BIAS: usize = 1;
const QUERY: usize = 2;
const KEY: usize = 3;
const VALUE: usize = 4;
const OWN: usize = 5;
const CONTEXT: usize = 6;

/// Every intermediate of one item's forward pass.
struct Pass {
    pre: Vec<Vec<f64>>,
    hidden: Vec<Vec<f64>>,
    projected: [Vec<Vec<f64>>; 3],
    attention: Vec<Vec<f64>>,
    context: Vec<Vec<f64>>,
    probabilities: Vec<f64>,
}

fn uniform(rng: &mut ChaCha8Rng) -> f64 {
    let unit = (rng.next_u64() >> 11) as f64 / (1_u64 << 53) as f64;
    (2.0 * unit - 1.0) * INIT_SCALE
}

fn times(vector: &[f64], matrix: &[f64], width: usize) -> Vec<f64> {
    let mut out = vec![0.0; width];
    for (a, x) in vector.iter().enumerate() {
        for (b, o) in out.iter_mut().enumerate() {
            *o += x * matrix[a * width + b];
        }
    }
    out
}

fn inner(a: &[f64], b: &[f64]) -> f64 {
    let mut total = 0.0;
    for (x, y) in a.iter().zip(b) {
        total += x * y;
    }
    total
}

fn axpy(target: &mut [f64], scale: f64, source: &[f64]) {
    for (t, s) in target.iter_mut().zip(source) {
        *t += scale * s;
    }
}

/// `gradient[a * width + b] += input[a] * output[b]`.
fn outer(gradient: &mut [f64], input: &[f64], output: &[f64], width: usize) {
    for (a, x) in input.iter().enumerate() {
        axpy(&mut gradient[a * width..(a + 1) * width], *x, output);
    }
}

impl Net {
    fn initial(features: usize, width: usize, seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let sizes = [
            features * width,
            width,
            width * width,
            width * width,
            width * width,
        ];
        let mut drawn = sizes.map(|n| (0..n).map(|_| uniform(&mut rng)).collect::<Vec<_>>());
        drawn[BIAS].iter_mut().for_each(|b| *b = 0.0);
        let [embed, bias, query, key, value] = drawn;
        Self {
            features,
            width,
            slots: [
                embed,
                bias,
                query,
                key,
                value,
                vec![0.0; width],
                vec![0.0; width],
            ],
        }
    }

    fn zeros(&self) -> Self {
        Self {
            features: self.features,
            width: self.width,
            slots: self.slots.clone().map(|s| vec![0.0; s.len()]),
        }
    }

    fn forward(&self, item: &TrainItem) -> RefusalResult<Pass> {
        let d = self.width;
        let pre: Vec<Vec<f64>> = item
            .rows
            .iter()
            .map(|x| {
                let mut a = times(x, &self.slots[EMBED], d);
                axpy(&mut a, 1.0, &self.slots[BIAS]);
                a
            })
            .collect();
        let hidden: Vec<Vec<f64>> = pre
            .iter()
            .map(|a| a.iter().map(|v| v.max(0.0)).collect())
            .collect();
        let projected: [Vec<Vec<f64>>; 3] = [QUERY, KEY, VALUE]
            .map(|s| hidden.iter().map(|h| times(h, &self.slots[s], d)).collect());
        let attention = attention_of(&projected)?;
        let context = context_of(&attention, &projected[2], d);
        let logits: Vec<f64> = (0..item.rows.len())
            .map(|i| {
                item.prescores[i]
                    + inner(&self.slots[OWN], &hidden[i])
                    + inner(&self.slots[CONTEXT], &context[i])
            })
            .collect();
        Ok(Pass {
            probabilities: softmax(&logits)?,
            pre,
            hidden,
            projected,
            attention,
            context,
        })
    }

    fn step(&mut self, gradient: &Net, total_weight: f64) {
        for (slot, grad) in self.slots.iter_mut().zip(&gradient.slots) {
            for (theta, g) in slot.iter_mut().zip(grad) {
                *theta -= LEARNING_RATE * (g / total_weight + 2.0 * RIDGE * *theta);
            }
        }
    }
}

fn attention_of(projected: &[Vec<Vec<f64>>; 3]) -> RefusalResult<Vec<Vec<f64>>> {
    let [query, key, _] = projected;
    query
        .iter()
        .map(|q| {
            Ok(softmax(
                &key.iter().map(|k| inner(q, k)).collect::<Vec<_>>(),
            )?)
        })
        .collect()
}

fn context_of(attention: &[Vec<f64>], value: &[Vec<f64>], width: usize) -> Vec<Vec<f64>> {
    attention
        .iter()
        .map(|row| {
            let mut c = vec![0.0; width];
            for (a, v) in row.iter().zip(value) {
                axpy(&mut c, *a, v);
            }
            c
        })
        .collect()
}

/// Gradients reaching each option's hidden units and its three projections.
struct Upstream {
    hidden: Vec<Vec<f64>>,
    projected: [Vec<Vec<f64>>; 3],
}

fn output_gradients(net: &Net, item: &TrainItem, pass: &Pass, grad: &mut Net) -> Upstream {
    let (m, d) = (item.rows.len(), net.width);
    let mut up = Upstream {
        hidden: vec![vec![0.0; d]; m],
        projected: [
            vec![vec![0.0; d]; m],
            vec![vec![0.0; d]; m],
            vec![vec![0.0; d]; m],
        ],
    };
    for i in 0..m {
        let gz = item.weight * (pass.probabilities[i] - item.targets[i]);
        axpy(&mut grad.slots[OWN], gz, &pass.hidden[i]);
        axpy(&mut grad.slots[CONTEXT], gz, &pass.context[i]);
        axpy(&mut up.hidden[i], gz, &net.slots[OWN]);
        let gc: Vec<f64> = net.slots[CONTEXT].iter().map(|r| gz * r).collect();
        attention_gradients(i, &gc, pass, &mut up);
    }
    up
}

/// Back through `c_i = sum_j A_ij v_j` and the row softmax `A_i`.
fn attention_gradients(i: usize, gc: &[f64], pass: &Pass, up: &mut Upstream) {
    let row = &pass.attention[i];
    let [query, key, value] = &pass.projected;
    let ga: Vec<f64> = value.iter().map(|v| inner(gc, v)).collect();
    let mean = inner(row, &ga);
    for (j, (a, g)) in row.iter().zip(&ga).enumerate() {
        axpy(&mut up.projected[2][j], *a, gc);
        let gs = a * (g - mean);
        axpy(&mut up.projected[0][i], gs, &key[j]);
        axpy(&mut up.projected[1][j], gs, &query[i]);
    }
}

/// Back through the projections, the relu and the embedding.
fn input_gradients(net: &Net, item: &TrainItem, pass: &Pass, up: &mut Upstream, grad: &mut Net) {
    let d = net.width;
    for (i, x) in item.rows.iter().enumerate() {
        for (k, slot) in [QUERY, KEY, VALUE].into_iter().enumerate() {
            outer(
                &mut grad.slots[slot],
                &pass.hidden[i],
                &up.projected[k][i],
                d,
            );
            for a in 0..d {
                up.hidden[i][a] += inner(&net.slots[slot][a * d..(a + 1) * d], &up.projected[k][i]);
            }
        }
        let ga: Vec<f64> = up.hidden[i]
            .iter()
            .zip(&pass.pre[i])
            .map(|(g, a)| if *a > 0.0 { *g } else { 0.0 })
            .collect();
        outer(&mut grad.slots[EMBED], x, &ga, d);
        axpy(&mut grad.slots[BIAS], 1.0, &ga);
    }
}

/// The shortlisted training view of one admitted item, or `None` when it
/// carries no positive signal inside its shortlist.
fn train_item(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    item: &LabelledItem,
) -> RefusalResult<Option<TrainItem>> {
    let Some((all_targets, weight)) = targets(item) else {
        return Ok(None);
    };
    let weights: Vec<i64> = head.weights.iter().map(|w| to_q32(*w)).collect();
    let mut standardised = Vec::new();
    for row in item_rows(dataset, item)? {
        let Ok(x) = standardise(head.standardisation.as_slice(), &row) else {
            return Ok(None);
        };
        standardised.push(x);
    }
    let prescored: Vec<(usize, i64)> = standardised
        .iter()
        .enumerate()
        .map(|(i, x)| (i, dot(&weights, x)))
        .collect();
    let kept = shortlist(&prescored, SCORER_SHORTLIST);
    let mass: f64 = kept.iter().map(|&i| all_targets[i]).sum();
    if mass <= 0.0 {
        return Ok(None);
    }
    Ok(Some(TrainItem {
        rows: kept
            .iter()
            .map(|&i| standardised[i].iter().map(|v| to_f64(*v)).collect())
            .collect(),
        prescores: kept.iter().map(|&i| to_f64(prescored[i].1)).collect(),
        targets: kept.iter().map(|&i| all_targets[i] / mass).collect(),
        weight: weight * audit_weight(item),
    }))
}

fn quantised<const N: usize>(values: &[f64]) -> RefusalResult<BoundedVec<i64, N>> {
    let raw = values
        .iter()
        .map(|v| quantise(v.clamp(-PARAMETER_LIMIT, PARAMETER_LIMIT), QuantScale::Q32))
        .collect::<Result<Vec<_>, _>>()?;
    BoundedVec::new(raw).map_err(|detail| Refusal::new(StatisticalErrorCode::HeadInvalid, detail))
}

fn params_of(net: &Net) -> RefusalResult<OptionAttentionParams> {
    let s = &net.slots;
    Ok(OptionAttentionParams {
        width: SCORER_WIDTH as u8,
        shortlist: SCORER_SHORTLIST as u8,
        embed: quantised(&s[EMBED])?,
        embed_bias: quantised(&s[BIAS])?,
        query: quantised(&s[QUERY])?,
        key: quantised(&s[KEY])?,
        value: quantised(&s[VALUE])?,
        self_weight: quantised(&s[OWN])?,
        context_weight: quantised(&s[CONTEXT])?,
    })
}

/// Fit the attention parameters of `head` (whose linear weights and
/// standardisation are already fitted) on the training items.
pub fn train(
    head: &DecisionHeadBody,
    dataset: &LabelledDataset,
    items: &[&LabelledItem],
    optimiser: OptimiserSpec,
) -> RefusalResult<OptionAttentionParams> {
    let mut train_items = Vec::new();
    for item in items {
        train_items.extend(train_item(head, dataset, item)?);
    }
    let total_weight: f64 = train_items.iter().map(|t| t.weight).sum();
    let mut net = Net::initial(head.weights.len(), SCORER_WIDTH, optimiser.seed);
    if total_weight <= 0.0 {
        return params_of(&net);
    }
    for _ in 0..optimiser.max_iterations.min(MAX_EPOCHS) {
        let mut grad = net.zeros();
        for item in &train_items {
            let pass = net.forward(item)?;
            let mut up = output_gradients(&net, item, &pass, &mut grad);
            input_gradients(&net, item, &pass, &mut up, &mut grad);
        }
        net.step(&grad, total_weight);
    }
    params_of(&net)
}
