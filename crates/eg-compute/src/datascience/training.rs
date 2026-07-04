// CONCEPT:EG-KG.compute.rust-native-training-loss — Rust-native training loss / optimizer kernels
//
// The performance path for the in-house training substrate (Wave C / C1). These
// mirror the pure-Python reference math in agent-utilities
// `graph/training_signals.py` and the torch kernels in data-science-mcp
// `trainers/objectives.py`, but run engine-side so a trainer can batch a step over
// the wire in one round-trip instead of marshalling per element.
//
// Every kernel is forward + (where it drives backprop) an analytic gradient, plus
// the optimizer steps (Adam / SGD). Pure Rust — no candle/torch, no GPU — matching
// the rest of `datascience::primitives`; unit-tested on toy tensors below.

use serde::{Deserialize, Serialize};

/// Numerically-stable softmax with temperature.
pub fn softmax(logits: &[f64], temperature: f64) -> Vec<f64> {
    let t = if temperature.abs() < 1e-12 {
        1.0
    } else {
        temperature
    };
    let max = logits.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let exps: Vec<f64> = logits.iter().map(|&x| ((x - max) / t).exp()).collect();
    let sum: f64 = exps.iter().sum();
    if sum == 0.0 {
        return vec![0.0; logits.len()];
    }
    exps.iter().map(|&e| e / sum).collect()
}

/// Numerically-stable log-softmax.
pub fn log_softmax(logits: &[f64]) -> Vec<f64> {
    let max = logits.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
    let sum_exp: f64 = logits.iter().map(|&x| (x - max).exp()).sum();
    let log_sum = sum_exp.ln();
    logits.iter().map(|&x| (x - max) - log_sum).collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossEntropyResult {
    pub loss: f64,
    /// dL/dlogits, shape == logits (softmax - one_hot, averaged over the batch).
    pub grad: Vec<Vec<f64>>,
}

/// Mean categorical cross-entropy over a batch of rows with integer labels,
/// returning the loss and the analytic gradient w.r.t. the logits.
pub fn cross_entropy(logits: &[Vec<f64>], labels: &[usize]) -> CrossEntropyResult {
    let n = logits.len();
    if n == 0 || n != labels.len() {
        return CrossEntropyResult {
            loss: 0.0,
            grad: vec![],
        };
    }
    let mut loss = 0.0;
    let mut grad = Vec::with_capacity(n);
    for (row, &label) in logits.iter().zip(labels.iter()) {
        let probs = softmax(row, 1.0);
        let p = probs.get(label).copied().unwrap_or(1e-12).max(1e-12);
        loss += -p.ln();
        let mut g = probs;
        if label < g.len() {
            g[label] -= 1.0;
        }
        for v in g.iter_mut() {
            *v /= n as f64;
        }
        grad.push(g);
    }
    CrossEntropyResult {
        loss: loss / n as f64,
        grad,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DpoResult {
    pub loss: f64,
    pub grad_chosen: Vec<f64>,
    pub grad_rejected: Vec<f64>,
}

fn sigmoid(x: f64) -> f64 {
    1.0 / (1.0 + (-x).exp())
}

/// Bradley-Terry DPO loss with a frozen reference, mean over the batch.
/// `loss_i = -log σ(β·((pc−pr) − (rc−rr)))`; returns analytic grads w.r.t. the
/// policy chosen/rejected log-probs (the reference is constant).
pub fn dpo_loss(
    policy_chosen: &[f64],
    policy_rejected: &[f64],
    ref_chosen: &[f64],
    ref_rejected: &[f64],
    beta: f64,
) -> DpoResult {
    let n = policy_chosen.len();
    if n == 0 || policy_rejected.len() != n || ref_chosen.len() != n || ref_rejected.len() != n {
        return DpoResult {
            loss: 0.0,
            grad_chosen: vec![],
            grad_rejected: vec![],
        };
    }
    let mut loss = 0.0;
    let mut grad_chosen = vec![0.0; n];
    let mut grad_rejected = vec![0.0; n];
    for i in 0..n {
        let z =
            beta * ((policy_chosen[i] - policy_rejected[i]) - (ref_chosen[i] - ref_rejected[i]));
        loss += -(sigmoid(z).ln());
        // d/dz [-log σ(z)] = -(1 - σ(z)) = σ(z) - 1 ; chain through β, averaged.
        let dz = (sigmoid(z) - 1.0) * beta / n as f64;
        grad_chosen[i] = dz; // ∂z/∂pc = +β
        grad_rejected[i] = -dz; // ∂z/∂pr = -β
    }
    DpoResult {
        loss: loss / n as f64,
        grad_chosen,
        grad_rejected,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpoResult {
    pub loss: f64,
    pub grad: Vec<f64>,
}

/// PPO/GRPO clipped surrogate (loss to minimise = negated objective), mean over
/// elements, with the analytic gradient w.r.t. `logprob`. In the clipped (saturated)
/// region the gradient is zero, exactly like the autodiff path.
pub fn grpo_surrogate(
    logprob: &[f64],
    old_logprob: &[f64],
    advantage: &[f64],
    clip_eps: f64,
) -> GrpoResult {
    let n = logprob.len();
    if n == 0 || old_logprob.len() != n || advantage.len() != n {
        return GrpoResult {
            loss: 0.0,
            grad: vec![],
        };
    }
    let mut loss = 0.0;
    let mut grad = vec![0.0; n];
    for i in 0..n {
        let ratio = (logprob[i] - old_logprob[i]).exp();
        let unclipped = ratio * advantage[i];
        let clipped = ratio.clamp(1.0 - clip_eps, 1.0 + clip_eps) * advantage[i];
        let obj = unclipped.min(clipped);
        loss += -obj;
        // Gradient only flows when the unclipped term is the active (min) branch.
        if (obj - unclipped).abs() < 1e-12 {
            grad[i] = -(ratio * advantage[i]) / n as f64; // d ratio/d logprob = ratio
        }
    }
    GrpoResult {
        loss: loss / n as f64,
        grad,
    }
}

/// Schulman k3 low-variance KL estimate `E[(r−1) − log r]` with `r = exp(ref − lp)`.
pub fn kl_divergence(logprob: &[f64], ref_logprob: &[f64]) -> f64 {
    let n = logprob.len();
    if n == 0 || ref_logprob.len() != n {
        return 0.0;
    }
    let mut acc = 0.0;
    for i in 0..n {
        let log_r = ref_logprob[i] - logprob[i];
        let r = log_r.exp();
        acc += r - 1.0 - log_r;
    }
    acc / n as f64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdamResult {
    pub params: Vec<f64>,
    pub m: Vec<f64>,
    pub v: Vec<f64>,
}

/// One Adam optimizer step with bias correction at step `t` (1-based).
#[allow(clippy::too_many_arguments)]
pub fn adam_step(
    params: &[f64],
    grads: &[f64],
    m: &[f64],
    v: &[f64],
    lr: f64,
    beta1: f64,
    beta2: f64,
    eps: f64,
    t: u64,
) -> AdamResult {
    let n = params.len();
    if n == 0 || grads.len() != n {
        return AdamResult {
            params: params.to_vec(),
            m: m.to_vec(),
            v: v.to_vec(),
        };
    }
    let m_in = if m.len() == n {
        m.to_vec()
    } else {
        vec![0.0; n]
    };
    let v_in = if v.len() == n {
        v.to_vec()
    } else {
        vec![0.0; n]
    };
    let step = t.max(1) as f64;
    let bc1 = 1.0 - beta1.powf(step);
    let bc2 = 1.0 - beta2.powf(step);
    let mut new_params = vec![0.0; n];
    let mut new_m = vec![0.0; n];
    let mut new_v = vec![0.0; n];
    for i in 0..n {
        new_m[i] = beta1 * m_in[i] + (1.0 - beta1) * grads[i];
        new_v[i] = beta2 * v_in[i] + (1.0 - beta2) * grads[i] * grads[i];
        let m_hat = new_m[i] / bc1;
        let v_hat = new_v[i] / bc2;
        new_params[i] = params[i] - lr * m_hat / (v_hat.sqrt() + eps);
    }
    AdamResult {
        params: new_params,
        m: new_m,
        v: new_v,
    }
}

/// One plain SGD step `params - lr * grads`.
pub fn sgd_step(params: &[f64], grads: &[f64], lr: f64) -> Vec<f64> {
    if params.len() != grads.len() {
        return params.to_vec();
    }
    params
        .iter()
        .zip(grads.iter())
        .map(|(&p, &g)| p - lr * g)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64, tol: f64) -> bool {
        (a - b).abs() < tol
    }

    #[test]
    fn test_softmax_sums_to_one() {
        let p = softmax(&[1.0, 2.0, 3.0], 1.0);
        assert!(approx(p.iter().sum::<f64>(), 1.0, 1e-9));
        assert!(p[2] > p[1] && p[1] > p[0]);
    }

    #[test]
    fn test_log_softmax_matches_log_of_softmax() {
        let logits = [0.5, -1.0, 2.0];
        let ls = log_softmax(&logits);
        let s = softmax(&logits, 1.0);
        for (l, p) in ls.iter().zip(s.iter()) {
            assert!(approx(*l, p.ln(), 1e-9));
        }
    }

    #[test]
    fn test_cross_entropy_grad_sums_to_zero_per_row() {
        let logits = vec![vec![2.0, 1.0, 0.1], vec![0.5, 2.5, 0.3]];
        let r = cross_entropy(&logits, &[0, 1]);
        assert!(r.loss > 0.0);
        // softmax - onehot sums to 0 per row (before the 1/N scale it still does).
        for g in &r.grad {
            assert!(approx(g.iter().sum::<f64>(), 0.0, 1e-9));
        }
    }

    #[test]
    fn test_dpo_loss_decreases_with_margin() {
        let small = dpo_loss(&[0.2], &[0.0], &[0.0], &[0.0], 1.0).loss;
        let big = dpo_loss(&[2.0], &[0.0], &[0.0], &[0.0], 1.0).loss;
        assert!(big < small);
        // grads are antisymmetric chosen vs rejected
        let d = dpo_loss(&[0.5], &[0.1], &[0.0], &[0.0], 1.0);
        assert!(approx(d.grad_chosen[0], -d.grad_rejected[0], 1e-12));
    }

    #[test]
    fn test_grpo_unit_ratio_equals_neg_mean_adv() {
        let lp = [0.5, -0.5, 1.0];
        let r = grpo_surrogate(&lp, &lp, &[1.0, -1.0, 0.5], 0.2);
        let mean_adv = (1.0 - 1.0 + 0.5) / 3.0;
        assert!(approx(r.loss, -mean_adv, 1e-9));
    }

    #[test]
    fn test_kl_zero_at_equality_and_nonneg() {
        let lp = [0.1, -0.2, 0.3];
        assert!(approx(kl_divergence(&lp, &lp), 0.0, 1e-12));
        assert!(kl_divergence(&lp, &[0.6, 0.1, 0.0]) >= 0.0);
    }

    #[test]
    fn test_adam_step_moves_params_down_gradient() {
        let r = adam_step(&[1.0], &[1.0], &[], &[], 0.1, 0.9, 0.999, 1e-8, 1);
        assert!(r.params[0] < 1.0); // positive grad → param decreases
        assert_eq!(r.m.len(), 1);
        assert_eq!(r.v.len(), 1);
    }

    #[test]
    fn test_sgd_step() {
        let p = sgd_step(&[1.0, 2.0], &[0.5, -0.5], 0.1);
        assert!(approx(p[0], 0.95, 1e-12));
        assert!(approx(p[1], 2.05, 1e-12));
    }
}
