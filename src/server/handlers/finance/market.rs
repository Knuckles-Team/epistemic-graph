//! `Method::FinanceMarket`: market bars and trend signals (EH-413..EH-418).
//!
//! Pure compute over what the request carries — no store, no clock, no order
//! authority. Inputs are bounded by the transport's request-size limit (and a
//! replay by its own work budget); every op answers its declared result or a
//! typed refusal rendered `"CODE: detail"`.

use eg_compute::finance::market::{
    backtest_run, codec, confidence, indicators, rollup, scan, signal, FinanceMarketOp,
    MarketResult,
};
use eg_types::result_contract::compute as results;
use eg_types::result_contract::MethodResult;

use crate::protocol::{Method, Response, ResultPayload};

fn answer<M: MethodResult>(req_id: u64, result: MarketResult<M::Body>) -> Response {
    match result {
        Ok(body) => Response::ok(req_id, ResultPayload::of::<M>(body)),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

/// Serve one `FinanceMarket` op.
fn handle(req_id: u64, op: FinanceMarketOp) -> Response {
    match op {
        FinanceMarketOp::EncodePoints { records } => {
            answer::<results::FinanceMarketEncodePoints>(req_id, codec::encode_all(&records))
        }
        FinanceMarketOp::Resolve {
            records,
            points,
            as_of,
            finality,
        } => answer::<results::FinanceMarketResolve>(
            req_id,
            codec::resolve_with_points(records, &points, as_of, finality),
        ),
        FinanceMarketOp::Rollup {
            bars,
            calendar,
            timeframe,
            watermark,
        } => answer::<results::FinanceMarketRollup>(
            req_id,
            rollup::rollup(&bars, &calendar, timeframe, watermark),
        ),
        FinanceMarketOp::Indicators { bars, spec } => {
            answer::<results::FinanceMarketIndicators>(req_id, indicators::compute(&bars, &spec))
        }
        FinanceMarketOp::SignalReplay { request } => {
            answer::<results::FinanceMarketSignalReplay>(req_id, signal::replay(&request))
        }
        FinanceMarketOp::SignalAdvance { state, bars } => {
            answer::<results::FinanceMarketSignalAdvance>(
                req_id,
                signal::advance_wire(&state, &bars),
            )
        }
        FinanceMarketOp::SignalScan { request } => {
            answer::<results::FinanceMarketSignalScan>(req_id, scan::scan(&request))
        }
        FinanceMarketOp::FlipConfidence { request } => {
            answer::<results::FinanceMarketFlipConfidence>(
                req_id,
                confidence::flip_confidence(&request),
            )
        }
        FinanceMarketOp::BacktestRun { draft } => {
            answer::<results::FinanceMarketBacktestRun>(req_id, backtest_run::seal(&draft))
        }
    }
}

/// The route-family entry: a `FinanceMarket` request, or the method handed back.
pub(super) fn handle_market(req_id: u64, method: Method) -> Result<Response, Method> {
    match method {
        Method::FinanceMarket { op } => Ok(handle(req_id, *op)),
        other => Err(other),
    }
}
