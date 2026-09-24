# Market bars and trend signals

The `FinanceMarket` method serves the market-signal layer of the finance
program (ledger rows EH-411 and EH-413 to EH-418). It covers typed OHLCV bars
over the time-series store, deterministic indicators, a per-series trend
signal with its flip records, calibrated flip confidence, and the backtest-run
provenance record.

Everything here is **informational only**. No op, record or result authorises
an order. The engine contains no language model: explanations of a flip belong
to the application layer, which must show the mathematical trigger first.

## Ontology (`finance-v1`)

`crates/eg-core/ontology/finance-v1.ttl` is a thin core module in the style of
the world-model modules. It sits under the BFO layer and maps (never imports)
to FIBO and Wikidata. It declares nine market classes:

| Class | Placement | Notes |
|---|---|---|
| `FinancialInstrument` | generically dependent continuant | Declares the class `energy_geopolitics`'s `Commodity` already specialises. |
| `Venue` | `System` | A place of trade. |
| `Listing` | generically dependent continuant | One instrument, one venue, one quote instrument and one listing type. |
| `BarSeries` | `Dataset` | Names the store series that holds the bars. |
| `IndicatorSpec` | generically dependent continuant | Identified by a version and a parameter hash. |
| `SignalState` | generically dependent continuant | The latest state of one signal. |
| `TrendFlip` | `Event` | `revisesFlip` links a revision to the flip it revises. |
| `MacroEvent` | `Event` | A policy action with its announcement time. |
| `BacktestRun` | generically dependent continuant | The provenance record of one backtest. |

It also holds the eight trading classes that used to live in `company_infra`
(folded in EH-517, IRIs unchanged): `TradingStrategy` and `TradingDebate`
(processes), `ExchangeBackend` and `PortfolioPosition` (independent
continuants), and the records `TradingSignal`, `BacktestResult`,
`VersionedOrder` and `RiskSnapshot` (generically dependent continuants, like
every other record here). A `BacktestResult` is an external backtester's metric
summary; a `BacktestRun` is the engine's sealed, content-addressed record.
`VersionedOrder` documents an order; nothing here authorises one.

`finance-v1.shapes.ttl` holds the ABox shapes. Closed vocabularies such as the
listing type, the price basis, the data status and the direction are `sh:in`
lists, so a misspelled value fails validation. Timeframes and hashes are
checked by pattern. With these two documents the core catalog holds 37 of its
64 sources.

## Bars (EH-413)

A bar is one store point. `ts` is the bar's open time, and the fields follow
`codec::BAR_FIELDS`:

- the close time and the `known_at` time, each split into 32-bit halves;
- the open, high, low and close, as integer ticks;
- the volume, as integer units;
- the status (provisional or final) and the revision.

Every field is an integer that an `f64` holds exactly.

The store only appends, and several versions of one bar coexist at one
timestamp. A correction is therefore a new point with a higher revision.
`encode_points` writes records in this layout for `TsAppend`. `resolve` decodes
stored points (and takes plain records) and builds the view as of any time:
for each open time it takes the highest revision known at that time.

`resolve` refuses these inputs:

- two different bodies at one revision;
- a provisional version that would revise a final bar;
- a final bar that claims to be known before it closes;
- overlapping bars.

`rollup` folds whole bars onto a calendar timeframe. A rolled bar is final only
when all its parts are final and the caller's watermark has passed the end of
its bucket.

There are two kinds of trading calendar:

- **`utc24x7`** aligns days to UTC midnight, weeks to the ISO Monday, and
  months to the civil month.
- **Exchange calendars** are entirely data: offset spans that cover daylight
  saving, one session, the trading weekdays, holidays and early closes. They
  read no time-zone database, so they replay exactly.

## Indicators (EH-414)

All kernels are incremental: each closed bar is one step. The kernels are
integer-only; values are in milli-ticks and every division rounds half away
from zero. The kernels are:

- Wilder ATR, seeded by the mean of the first `period` true ranges;
- the ATR trailing-trend line (SuperTrend), on a raw or Heikin-Ashi basis;
- SMA and EMA (the EMA is seeded by an SMA);
- the 20/21 band;
- Heikin-Ashi candles.

The 200-week SMA is an SMA of period 200 over bars rolled up to weekly.

The trailing line follows the public TradingView `ta.supertrend` recurrence:

- The upper band only ratchets down and the lower band only ratchets up.
- The first computable bar is bearish, and no flip is recorded there.
- A flip needs the close to be strictly beyond the band. A close equal to the
  band holds the current direction.

The fixture `crates/eg-compute/tests/fixtures/finance_market/reference.py` is an
independent Python implementation. It writes `golden.json`, and the Rust
golden tests must reproduce every digest bit for bit on every build host.

`supertrend_companion.pine`, beside it, is the TradingView companion: a Pine v5
indicator that replays the same integer Wilder ATR and trailing-line recurrence
(raw or Heikin-Ashi basis) on the chart's own bars, plots TradingView's
`ta.supertrend` next to it, and counts the bars where the two disagree in
direction or by more than a tick on the band. Its defaults are the fixture's
`super_trend_10_3`; `tests/test_finance_pine_companion.py` keeps the defaults,
the fixed-point scale and the recurrence rules in step with the reference. It
plots only and never places an order.

## Signal, flips and the scanner (EH-415)

`SignalKey` is built from five parts:

- the listing;
- the price basis;
- the timeframe and calendar;
- the indicator version;
- the parameter hash.

`signal_advance` moves a `SignalState` forward by final bars that come after
its last bar. Each bar is one O(1) step from the state's exact kernel
checkpoint. A bar at or before the last bar is refused: that change needs a
replay.

`signal_replay` processes every bar version in the order it became knowable:

- **An appended bar** advances the signal incrementally.
- **A revised or back-filled bar** triggers a recomputation, which appends
  revision records. A changed flip gets a new `emitted` record that `revises`
  the old record. A flip that no longer exists gets a `retracted` record.

No record is ever edited in place. A flip's `event_id` is derived from the
signal key and the bar close, so replays are idempotent. A replay `as_of` a
past time reproduces exactly what was known at that time.

`signal_scan` keeps one latest state per key and never lets a state regress.
It counts states against a stated denominator of one state per key, and orders
rows by the most recent flip.

`finance::market::events` turns each flip into a `finance.flip` event for
eg-stream's CEP engine. This lets compound signals, such as weekly-then-daily
agreement, run on the same NFA as the live standing queries.

## Calibrated flip confidence (EH-417)

`flip_confidence` scores a candidate flip at one horizon, using the smoothed
follow-through rate of its feature cell. Calibration uses the Decide layer's
own kernels:

- the conformal minimum-sample gate;
- a Clopper-Pearson drift check between the earlier and recent halves of the
  history;
- a class-conditional binary conformal set built on leave-one-out scores.

Only a singleton set is a claim. In every other case the op abstains with a
typed reason:

- data that is not valid (warming up, stale or unavailable);
- history thinner than the gate;
- an unsupported regime;
- drift;
- an ambiguous set.

## Backtest-run records (EH-418)

`backtest_run` seals a draft into an immutable, content-addressed record.
Sealing requires complete provenance:

- the signal keys;
- the data revisions;
- a point-in-time universe;
- the costs;
- the fill rule.

Every fill must pass the no-look-ahead check: the signal was known at or before
the fill, and the listing was in the universe at the time of the fill.

The engine's existing kernels compute the mandatory validation outputs:
purged combinatorial CV, the deflated Sharpe ratio, and the probability of
backtest overfitting. The caller never supplies these values.

`backtest_run::verify` re-derives a record from its own draft. A revised run is
a new record whose `supersedes` names the old digest.

The validation outputs use only correctly rounded IEEE operations and the pinned
soft-float kernel (`eg_numeric::detkernel::math`), never the platform `libm`,
`powi` or `powf`. A sealed digest is therefore the same on every host, and the
test `a_sealed_backtest_run_digest_is_pinned_across_hosts` pins one.
