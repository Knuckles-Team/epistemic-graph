use std::collections::HashSet;

use crate::datascience::training::{adam_step, AdamHyperparameters};

use super::{
    auc, backward, build_layers, flatten_params, forward_all, init_layers, sample_negatives,
    set_params, sigmoid, standardize_stats, FeatureCtx, GradAccum, KanLayer, KanLinkConfig,
    KanLinkModel,
};

struct TrainingData {
    pos: Vec<(usize, usize)>,
    neg: Vec<(usize, usize)>,
    raw: Vec<Vec<f64>>,
    labels: Vec<f64>,
}

pub(super) fn fit_link_predictor(
    ctx: &FeatureCtx,
    positives: &[(usize, usize)],
    config: &KanLinkConfig,
) -> KanLinkModel {
    let data = build_training_data(ctx, positives, config);
    let (feat_mean, feat_std) = standardize_stats(&data.raw, ctx.n_features());
    let x_std = standardize_rows(&data.raw, &feat_mean, &feat_std);
    let layers = train_layers(&x_std, &data.labels, ctx.n_features(), config);
    let mut model = KanLinkModel {
        basis: config.basis,
        degree: config.degree,
        feature_names: ctx.feature_names(),
        layers,
        feat_mean,
        feat_std,
        alpha: config.alpha,
        train_auc: 0.0,
    };
    model.train_auc = training_auc(&model, ctx, &data.pos, &data.neg);
    model
}

fn build_training_data(
    ctx: &FeatureCtx,
    positives: &[(usize, usize)],
    config: &KanLinkConfig,
) -> TrainingData {
    let n_nodes = ctx.node_count();
    // Canonicalise positives to (min, max) and dedupe.
    let pos_set: HashSet<(usize, usize)> = positives
        .iter()
        .filter(|(a, b)| a != b && *a < n_nodes && *b < n_nodes)
        .map(|&(a, b)| if a < b { (a, b) } else { (b, a) })
        .collect();
    let mut pos: Vec<(usize, usize)> = pos_set.iter().copied().collect();
    pos.sort_unstable();
    let n_neg = ((pos.len() as f64) * config.neg_ratio).round() as usize;
    let neg = sample_negatives(n_nodes, &pos_set, n_neg, config.seed);

    // Build the labelled training matrix.
    let mut raw: Vec<Vec<f64>> = Vec::with_capacity(pos.len() + neg.len());
    let mut labels: Vec<f64> = Vec::with_capacity(pos.len() + neg.len());
    for &(a, b) in &pos {
        raw.push(ctx.pair_features(a, b));
        labels.push(1.0);
    }
    for &(a, b) in &neg {
        raw.push(ctx.pair_features(a, b));
        labels.push(0.0);
    }
    TrainingData {
        pos,
        neg,
        raw,
        labels,
    }
}

fn standardize_rows(rows: &[Vec<f64>], feat_mean: &[f64], feat_std: &[f64]) -> Vec<Vec<f64>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .enumerate()
                .map(|(i, &v)| (v - feat_mean[i]) / feat_std[i])
                .collect()
        })
        .collect()
}

fn train_layers(
    x_std: &[Vec<f64>],
    labels: &[f64],
    n_features: usize,
    config: &KanLinkConfig,
) -> Vec<KanLayer> {
    let mut layers = build_layers(n_features, config);
    init_layers(&mut layers, config.seed);
    let mut params = flatten_params(&layers);
    let np = params.len();
    let mut m = vec![0.0; np];
    let mut v = vec![0.0; np];
    let batch = x_std.len().max(1) as f64;
    for epoch in 0..config.epochs {
        let mut gacc = GradAccum::zeros(&layers);
        for (row, &y) in x_std.iter().zip(labels.iter()) {
            let (acts, score) = forward_all(&layers, row);
            let p = sigmoid(score);
            backward(&layers, &acts, p - y, &mut gacc);
        }
        let grad = gacc.flatten(1.0 / batch);
        let step = adam_step(
            &params,
            &grad,
            &m,
            &v,
            AdamHyperparameters {
                lr: config.lr,
                beta1: 0.9,
                beta2: 0.999,
                eps: 1e-8,
            },
            epoch as u64 + 1,
        );
        params = step.params;
        m = step.m;
        v = step.v;
        set_params(&mut layers, &params);
    }
    layers
}

fn training_auc(
    model: &KanLinkModel,
    ctx: &FeatureCtx,
    pos: &[(usize, usize)],
    neg: &[(usize, usize)],
) -> f64 {
    let pos_scores = score_pairs(model, ctx, pos);
    let neg_scores = score_pairs(model, ctx, neg);
    auc(&pos_scores, &neg_scores)
}

fn score_pairs(model: &KanLinkModel, ctx: &FeatureCtx, pairs: &[(usize, usize)]) -> Vec<f64> {
    pairs
        .iter()
        .map(|&(a, b)| model.predict_prob(&ctx.pair_features(a, b)))
        .collect()
}
