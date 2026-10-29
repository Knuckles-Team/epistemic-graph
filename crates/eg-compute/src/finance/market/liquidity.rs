//! Point-in-time net liquidity, with exact unit conversion and sourced cells.

use std::collections::BTreeMap;

use super::{
    LiquidityDirection, LiquidityHeatCell, LiquidityObservation, LiquidityPanel,
    LiquidityPanelRequest, LiquiditySeries, MarketError, MarketResult, INVALID_REQUEST, OVERFLOW,
};

const MAX_OBSERVATIONS: usize = 10_000;
const DAY_MS: i64 = 86_400_000;
const SERIES: [LiquiditySeries; 3] = [
    LiquiditySeries::Walcl,
    LiquiditySeries::Wdtgal,
    LiquiditySeries::Rrpontsyd,
];

fn invalid(detail: &str) -> MarketError {
    MarketError::new(INVALID_REQUEST, detail)
}

fn dollars(observation: &LiquidityObservation) -> MarketResult<i64> {
    let factor = match observation.series {
        LiquiditySeries::Walcl => 1_000,
        LiquiditySeries::Wdtgal => -1_000,
        LiquiditySeries::Rrpontsyd => -1_000_000,
    };
    observation
        .value_milliunits
        .checked_mul(factor)
        .ok_or_else(|| MarketError::new(OVERFLOW, "liquidity component overflows USD"))
}

fn valid(observation: &LiquidityObservation, as_of: i64) -> bool {
    observation.value_milliunits >= 0
        && observation.observation_ms >= 0
        && observation.observation_ms % DAY_MS == 0
        && observation.vintage_ms % DAY_MS == 0
        && observation.observation_ms <= observation.vintage_ms
        && observation.vintage_ms <= observation.observed_ms
        && observation.observed_ms <= as_of
        && !observation.source_ref.trim().is_empty()
        && observation.source_ref.len() <= 512
}

fn component(
    series: LiquiditySeries,
    observations: &BTreeMap<(LiquiditySeries, i64, i64), &LiquidityObservation>,
    request: &LiquidityPanelRequest,
) -> MarketResult<LiquidityHeatCell> {
    let mut matching = observations
        .iter()
        .rev()
        .filter(|((kind, _, _), _)| *kind == series)
        .map(|(_, value)| *value);
    let current = matching
        .next()
        .ok_or_else(|| invalid("missing liquidity component"))?;
    if request.as_of_ms - current.observation_ms > request.max_age_ms {
        return Err(invalid("stale liquidity component"));
    }
    let current_usd = dollars(current)?;
    let previous = matching.find(|item| item.observation_ms < current.observation_ms);
    let previous =
        previous.filter(|item| current.observation_ms - item.observation_ms <= request.max_age_ms);
    let change_usd = match previous {
        Some(item) => Some(
            current_usd
                .checked_sub(dollars(item)?)
                .ok_or_else(|| MarketError::new(OVERFLOW, "liquidity change overflows USD"))?,
        ),
        _ => None,
    };
    let direction = match change_usd {
        Some(value) if value > 0 => LiquidityDirection::Increasing,
        Some(value) if value < 0 => LiquidityDirection::Decreasing,
        Some(_) => LiquidityDirection::Unchanged,
        None => LiquidityDirection::Unavailable,
    };
    Ok(LiquidityHeatCell {
        series,
        contribution_usd: current_usd,
        change_usd,
        direction,
        previous_source_ref: previous.map(|item| item.source_ref.clone()),
        previous_observation_ms: previous.map(|item| item.observation_ms),
        previous_vintage_ms: previous.map(|item| item.vintage_ms),
        previous_observed_ms: previous.map(|item| item.observed_ms),
        observation_ms: current.observation_ms,
        vintage_ms: current.vintage_ms,
        observed_ms: current.observed_ms,
        source_ref: current.source_ref.clone(),
    })
}

/// Resolve only captured, nonfuture vintages and compute a v1 FRED liquidity panel.
pub fn compute(request: &LiquidityPanelRequest) -> MarketResult<LiquidityPanel> {
    if request.version != 1 || request.as_of_ms < 0 || request.max_age_ms <= 0 {
        return Err(invalid("invalid liquidity policy"));
    }
    if request.observations.len() > MAX_OBSERVATIONS {
        return Err(invalid("liquidity observation budget exceeded"));
    }
    let mut observations = BTreeMap::new();
    for observation in &request.observations {
        if !valid(observation, request.as_of_ms) {
            return Err(invalid("invalid or future liquidity observation"));
        }
        let key = (
            observation.series,
            observation.observation_ms,
            observation.vintage_ms,
        );
        if observations.insert(key, observation).is_some() {
            return Err(invalid("duplicate liquidity observation vintage"));
        }
    }
    let heat: Vec<_> = SERIES
        .iter()
        .map(|&series| component(series, &observations, request))
        .collect::<MarketResult<_>>()?;
    let net_liquidity_usd = heat.iter().try_fold(0_i64, |sum, cell| {
        sum.checked_add(cell.contribution_usd)
            .ok_or_else(|| MarketError::new(OVERFLOW, "liquidity composite overflows USD"))
    })?;
    Ok(LiquidityPanel {
        version: 1,
        as_of_ms: request.as_of_ms,
        max_age_ms: request.max_age_ms,
        net_liquidity_usd,
        heat,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(series: LiquiditySeries, day: i64, value: i64, name: &str) -> LiquidityObservation {
        LiquidityObservation {
            series,
            observation_ms: day * DAY_MS,
            vintage_ms: (day + 1) * DAY_MS,
            observed_ms: (day + 1) * DAY_MS + 1,
            value_milliunits: value,
            source_ref: name.into(),
        }
    }

    fn request() -> LiquidityPanelRequest {
        LiquidityPanelRequest {
            version: 1,
            as_of_ms: 12 * DAY_MS,
            max_age_ms: 10 * DAY_MS,
            observations: vec![
                item(LiquiditySeries::Walcl, 9, 6_000_000_000, "walcl-new"),
                item(LiquiditySeries::Walcl, 2, 5_900_000_000, "walcl-prior"),
                item(LiquiditySeries::Wdtgal, 9, 1_000_000_000, "tga"),
                item(LiquiditySeries::Rrpontsyd, 10, 200_000, "rrp"),
            ],
        }
    }

    #[test]
    fn exact_units_and_sourced_heat_changes() {
        let panel = compute(&request()).expect("valid panel");
        assert_eq!(panel.net_liquidity_usd, 4_800_000_000_000);
        assert_eq!(panel.heat[0].contribution_usd, 6_000_000_000_000);
        assert_eq!(panel.heat[0].change_usd, Some(100_000_000_000));
        assert_eq!(panel.heat[0].direction, LiquidityDirection::Increasing);
        assert_eq!(
            panel.heat[0].previous_source_ref.as_deref(),
            Some("walcl-prior")
        );
        assert_eq!(panel.heat[1].change_usd, None);
        assert_eq!(panel.heat[1].direction, LiquidityDirection::Unavailable);
        assert_eq!(panel.heat[2].contribution_usd, -200_000_000_000);
    }

    #[test]
    fn rejects_missing_future_stale_and_conflicting_vintages() {
        let mut missing = request();
        missing.observations.pop();
        assert!(compute(&missing).is_err());
        let mut future = request();
        future.observations[0].observed_ms = future.as_of_ms + 1;
        assert!(compute(&future).is_err());
        let mut stale = request();
        stale.max_age_ms = DAY_MS;
        assert!(compute(&stale).is_err());
        let mut duplicate = request();
        duplicate
            .observations
            .push(duplicate.observations[0].clone());
        assert!(compute(&duplicate).is_err());
    }

    #[test]
    fn resolves_latest_vintage_and_refuses_overflow() {
        let mut revised = request();
        let mut revision = revised.observations[0].clone();
        revision.vintage_ms += DAY_MS;
        revision.observed_ms += DAY_MS;
        revision.value_milliunits += 1_000;
        revision.source_ref = "revision".into();
        revised.observations.push(revision);
        let panel = compute(&revised).expect("valid revision");
        assert_eq!(panel.heat[0].source_ref, "revision");
        assert_eq!(panel.net_liquidity_usd, 4_800_001_000_000);
        revised.observations[0].value_milliunits = i64::MAX;
        revised.observations.pop();
        assert_eq!(compute(&revised).unwrap_err().code, OVERFLOW);
    }
}
