//! The shared-budget allocator and the utility-path statistics.

/// Scale `requests` so their absolute sum is at most `cap`: every request by
/// the same factor, signs kept. Requests already within the cap pass through.
pub fn proportional(requests: &[f64], cap: f64) -> Vec<f64> {
    let total: f64 = requests.iter().map(|request| request.abs()).sum();
    if total <= cap || total == 0.0 {
        return requests.to_vec();
    }
    let scale = cap / total;
    requests.iter().map(|request| request * scale).collect()
}

/// The largest peak-to-trough fall of the cumulative utility, `>= 0`.
pub fn max_drawdown(path: &[f64]) -> f64 {
    let (mut cumulative, mut peak, mut worst) = (0.0f64, 0.0f64, 0.0f64);
    for utility in path {
        cumulative += utility;
        peak = peak.max(cumulative);
        worst = worst.max(peak - cumulative);
    }
    worst
}

/// Mean over the sample standard deviation; `None` below two steps or with
/// no variance.
pub fn sharpe(path: &[f64]) -> Option<f64> {
    if path.len() < 2 {
        return None;
    }
    let n = path.len() as f64;
    let mean = path.iter().sum::<f64>() / n;
    let variance = path.iter().map(|u| (u - mean) * (u - mean)).sum::<f64>() / (n - 1.0);
    (variance > 0.0).then(|| mean / variance.sqrt())
}

/// The arithmetic mean; `0` for an empty slice.
pub fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}
