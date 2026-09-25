//! `Method::FinanceSignalModels`: Bayesian signal fusion and the strategic
//! insider under dynamic legal risk (EH-423 / AUD-30).
//!
//! Pure compute over what the request carries; every op answers its declared
//! result or a typed refusal rendered `"CODE: detail"`.

use std::collections::BTreeMap;

use eg_compute::finance::market::{MarketError, MarketResult, INVALID_REQUEST};
use eg_compute::finance::signal_models::{insider_equilibrium, FinanceSignalModelsOp};
use eg_epistemic::fusion::{fuse, Evidence};
use eg_types::compute_result::signal_models::{BayesFuseRequest, BayesFusion, FusionSource};
use eg_types::result_contract::compute as results;

use super::market::answer;
use crate::protocol::{Method, Response};

fn invalid(detail: &str) -> MarketError {
    MarketError::new(INVALID_REQUEST, detail)
}

fn probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

/// Finance-only eligibility; the posterior math lives in eg-epistemic.
fn bayes_fuse(request: &BayesFuseRequest) -> MarketResult<BayesFusion> {
    if !probability(request.prior)
        || !probability(request.default_weight)
        || !probability(request.default_accuracy)
        || !request.min_sharpe.is_finite()
        || !request.max_pbo.is_finite()
    {
        return Err(invalid(
            "fusion parameters must be finite and probabilities in [0, 1]",
        ));
    }
    let mut seeded = BTreeMap::new();
    for prior in &request.priors {
        if !probability(prior.directional_accuracy)
            || !prior.standalone_sharpe.is_finite()
            || !prior.pbo.is_finite()
        {
            return Err(invalid("source prior is malformed"));
        }
        if prior.name.is_empty()
            || prior.pbo > request.max_pbo
            || prior.standalone_sharpe <= request.min_sharpe
        {
            continue;
        }
        seeded.insert(
            prior.name.clone(),
            FusionSource {
                name: prior.name.clone(),
                weight: (prior.directional_accuracy * prior.standalone_sharpe).clamp(0.0, 1.0),
                accuracy: prior.directional_accuracy,
                seeded: true,
            },
        );
    }
    let sources: Vec<_> = request
        .directions
        .keys()
        .map(|name| {
            seeded.get(name).cloned().unwrap_or_else(|| FusionSource {
                name: name.clone(),
                weight: request.default_weight,
                accuracy: request.default_accuracy,
                seeded: false,
            })
        })
        .collect();
    let evidence: Vec<_> = request
        .directions
        .values()
        .zip(&sources)
        .map(|(direction, source)| Evidence {
            direction: *direction,
            reliability: source.accuracy,
            weight: source.weight,
        })
        .collect();
    let posterior_up = fuse(request.prior, &evidence)
        .map_err(|_| invalid("fusion evidence is malformed or exceeds its work budget"))?;
    Ok(BayesFusion {
        posterior_up,
        seeded: u32::try_from(seeded.len()).unwrap_or(u32::MAX),
        sources,
    })
}

fn handle(req_id: u64, op: FinanceSignalModelsOp) -> Response {
    match op {
        FinanceSignalModelsOp::BayesFuse { request } => {
            answer::<results::FinanceSignalModelsBayesFuse>(req_id, bayes_fuse(&request))
        }
        FinanceSignalModelsOp::InsiderEquilibrium { request } => {
            answer::<results::FinanceSignalModelsInsiderEquilibrium>(
                req_id,
                insider_equilibrium(&request),
            )
        }
    }
}

/// The route-family entry: a `FinanceSignalModels` request, or the method handed back.
pub(super) fn handle_signal_models(req_id: u64, method: Method) -> Result<Response, Method> {
    match method {
        Method::FinanceSignalModels { op } => Ok(handle(req_id, *op)),
        other => Err(other),
    }
}

#[cfg(test)]
mod tests {
    use super::bayes_fuse;
    use eg_types::compute_result::signal_models::{BayesFuseRequest, FusionPrior};

    fn request() -> BayesFuseRequest {
        BayesFuseRequest {
            prior: 0.5,
            priors: vec![
                FusionPrior {
                    name: "a".into(),
                    directional_accuracy: 0.7,
                    standalone_sharpe: 0.8,
                    pbo: 0.2,
                },
                FusionPrior {
                    name: "b".into(),
                    directional_accuracy: 0.6,
                    standalone_sharpe: 0.5,
                    pbo: 0.9,
                },
            ],
            directions: [("a".to_string(), 1), ("c".to_string(), -1)].into(),
            min_sharpe: 0.0,
            max_pbo: 0.5,
            default_weight: 0.5,
            default_accuracy: 0.55,
        }
    }

    #[test]
    fn finance_entry_preserves_reference_and_filters_overfit_source() {
        let out = bayes_fuse(&request()).unwrap();
        assert!((out.posterior_up - 0.587_710_310_965_630_1).abs() < 1e-12);
        assert_eq!(out.seeded, 1);
        assert_eq!(
            out.sources
                .iter()
                .map(|s| s.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "c"]
        );
        assert!(out.sources[0].seeded && !out.sources[1].seeded);
        let mut bad = request();
        bad.directions.insert("d".into(), 2);
        assert!(bayes_fuse(&bad).is_err());
    }
}
