//! Supervised-model evaluation metrics (CONCEPT:EG-KG.mining.ml-pipeline-metrics).
//!
//! Small, dependency-free scoring helpers shared by the `MiningPipeline` train/eval
//! path (`handlers/pipeline.rs`) so the composable pipeline reports the SAME held-out
//! numbers regardless of which model family produced the predictions. Classification
//! metrics (accuracy / macro-F1) score integer labels; regression metrics (R² / RMSE /
//! MAE) score continuous targets. (Link-prediction AUC already lives in
//! `graphlearn::link_predict::auc`.)

use std::collections::BTreeSet;

/// Fraction of exactly-correct predictions over `y_true` (`0.0` for an empty set).
pub fn accuracy(y_true: &[i64], y_pred: &[i64]) -> f64 {
    let n = y_true.len().min(y_pred.len());
    if n == 0 {
        return 0.0;
    }
    let correct = (0..n).filter(|&i| y_true[i] == y_pred[i]).count();
    correct as f64 / n as f64
}

/// Macro-averaged F1: the unweighted mean of the per-class F1 over the union of the
/// classes appearing in `y_true`/`y_pred`. Robust to class imbalance (each class
/// contributes equally); `0.0` when there is nothing to score. A class with an
/// undefined precision AND recall (never true, never predicted — impossible here since
/// the class is in the union) contributes 0.
pub fn macro_f1(y_true: &[i64], y_pred: &[i64]) -> f64 {
    let n = y_true.len().min(y_pred.len());
    if n == 0 {
        return 0.0;
    }
    let classes: BTreeSet<i64> = y_true[..n]
        .iter()
        .chain(y_pred[..n].iter())
        .copied()
        .collect();
    if classes.is_empty() {
        return 0.0;
    }
    let mut f1_sum = 0.0;
    for &c in &classes {
        let mut tp = 0usize;
        let mut fp = 0usize;
        let mut fn_ = 0usize;
        for i in 0..n {
            let actual = y_true[i] == c;
            let predicted = y_pred[i] == c;
            match (actual, predicted) {
                (true, true) => tp += 1,
                (false, true) => fp += 1,
                (true, false) => fn_ += 1,
                (false, false) => {}
            }
        }
        let precision = if tp + fp == 0 {
            0.0
        } else {
            tp as f64 / (tp + fp) as f64
        };
        let recall = if tp + fn_ == 0 {
            0.0
        } else {
            tp as f64 / (tp + fn_) as f64
        };
        let f1 = if precision + recall == 0.0 {
            0.0
        } else {
            2.0 * precision * recall / (precision + recall)
        };
        f1_sum += f1;
    }
    f1_sum / classes.len() as f64
}

/// Coefficient of determination R² = 1 − SS_res/SS_tot. Returns `0.0` when the target
/// has zero variance (SS_tot == 0) — the conventional degenerate case, avoiding a
/// divide-by-zero (a constant target is perfectly "explained" by its mean but R² is
/// undefined; 0.0 is the neutral report).
pub fn r2(y_true: &[f64], y_pred: &[f64]) -> f64 {
    let n = y_true.len().min(y_pred.len());
    if n == 0 {
        return 0.0;
    }
    let mean = y_true[..n].iter().sum::<f64>() / n as f64;
    let mut ss_res = 0.0;
    let mut ss_tot = 0.0;
    for i in 0..n {
        ss_res += (y_true[i] - y_pred[i]).powi(2);
        ss_tot += (y_true[i] - mean).powi(2);
    }
    if ss_tot == 0.0 {
        0.0
    } else {
        1.0 - ss_res / ss_tot
    }
}

/// Root mean squared error (`0.0` for an empty set).
pub fn rmse(y_true: &[f64], y_pred: &[f64]) -> f64 {
    mse(y_true, y_pred).sqrt()
}

/// Mean squared error (`0.0` for an empty set).
pub fn mse(y_true: &[f64], y_pred: &[f64]) -> f64 {
    let n = y_true.len().min(y_pred.len());
    if n == 0 {
        return 0.0;
    }
    (0..n).map(|i| (y_true[i] - y_pred[i]).powi(2)).sum::<f64>() / n as f64
}

/// Mean absolute error (`0.0` for an empty set).
pub fn mae(y_true: &[f64], y_pred: &[f64]) -> f64 {
    let n = y_true.len().min(y_pred.len());
    if n == 0 {
        return 0.0;
    }
    (0..n).map(|i| (y_true[i] - y_pred[i]).abs()).sum::<f64>() / n as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accuracy_and_f1_perfect_and_empty() {
        let y = [0, 1, 1, 0, 2];
        assert_eq!(accuracy(&y, &y), 1.0);
        assert_eq!(macro_f1(&y, &y), 1.0);
        assert_eq!(accuracy(&[], &[]), 0.0);
        assert_eq!(macro_f1(&[], &[]), 0.0);
    }

    #[test]
    fn accuracy_counts_matches() {
        // 3 of 4 correct.
        assert!((accuracy(&[0, 1, 0, 1], &[0, 1, 1, 1]) - 0.75).abs() < 1e-12);
    }

    #[test]
    fn macro_f1_balances_classes() {
        // Class 0: predicted {0,0}, true {0,1} → tp=1 fp=1 fn=0 → P=.5 R=1 F1=.667
        // Class 1: predicted {1}, true {1} at idx3; idx1 true=1 pred=0 → tp=1 fp=0 fn=1 → F1=.667
        let f1 = macro_f1(&[0, 1, 0, 1], &[0, 0, 1, 1]);
        assert!(f1 > 0.0 && f1 < 1.0, "f1={f1}");
    }

    #[test]
    fn r2_perfect_and_degenerate() {
        let y = [1.0, 2.0, 3.0, 4.0];
        assert!((r2(&y, &y) - 1.0).abs() < 1e-12);
        // Zero-variance target → 0.0 (not NaN/inf).
        assert_eq!(r2(&[5.0, 5.0], &[5.0, 5.0]), 0.0);
    }

    #[test]
    fn rmse_mae_zero_on_exact() {
        let y = [1.0, -2.0, 3.5];
        assert_eq!(rmse(&y, &y), 0.0);
        assert_eq!(mae(&y, &y), 0.0);
        assert!((rmse(&[0.0, 0.0], &[1.0, 1.0]) - 1.0).abs() < 1e-12);
        assert!((mae(&[0.0, 0.0], &[2.0, 4.0]) - 3.0).abs() < 1e-12);
    }
}
