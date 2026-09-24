# Market bars and trend signals

The `FinanceMarket` method serves the market-signal layer of the finance
program (ledger rows EH-411, EH-413 to EH-418, EH-420 and EH-421). It covers typed OHLCV bars
over the time-series store, deterministic indicators, a per-series trend
signal with its flip records, calibrated flip confidence, and the backtest-run
provenance record, server-side chart decimation, and the analysis-snapshot
record behind shared analysis links.

Everything here is **informational only**. No op, record or result authorises
an order. The engine contains no language model: explanations of a flip belong
to the application layer, which must show the mathematical trigger first.

## Ontology (`finance-v1`)

`crates/eg-core/ontology/finance-v1.ttl` is a thin core module in the style of
the world-model modules. It sits under the BFO layer and maps (never imports)
to FIBO and Wikidata. It declares ten classes:

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
| `AnalysisSnapshot` | generically dependent continuant | An immutable, content-addressed record of one shared analysis. |

A `FinancialInstrument` may carry one `assetClass` from a closed vocabulary:
crypto, stock, etf, fund, commodity, forex, index or bond. A `BarSeries` also
carries its `tickSize` (required) and `volumeStep`
(optional, default 1), both `xsd:decimal`. Bar prices are integer ticks, so a
reader needs the tick size to show a price.

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

## Chart decimation (EH-420)

`decimate` thins bars, and indicator series aligned to them, to a pixel width
of 16 to 8,192 columns. Run it after the indicators, which are computed over
the full history. Bars are assigned to columns by open time:

- Each column's bars fold into one candle: the first open, the highest high,
  the lowest low, the last close and the summed volume. This is M4 for
  candles, so the values a column draws are exact. A column is final only when
  every bar in it is final.
- Each line keeps its first, last, lowest and highest point in every column.
  A band keeps both of its lines.
- A trailing line also keeps every change of direction, with the point before
  it, so the colour boundaries of the line stay exact.
- A Heikin-Ashi series folds one candle per column, like the bars.

A request that already fits comes back unchanged with `decimated: false`. The
arithmetic is integer-only, so the result is the same on every target.

## Analysis snapshots (EH-421)

`analysis_snapshot` seals an analysis as an immutable, content-addressed
record. Sealing refuses the draft unless it is self-consistent:

- The signal key must be the key of the spec over the series.
- Every flip must belong to that key, carry its derived event id, lie inside
  the bar window and follow the previous flip in time.
- The source revision must be a `sha256:` digest of the bar versions read.
- Every claim must cite at least one source. A source is titled and located by
  an http(s) URL or an engine record reference. A claim with no source is
  refused with `UNSOURCED_CLAIM`.

The engine stamps three notices on every snapshot: informational only, the
hallucination warning, and the mechanical trigger. A caller can neither omit
nor reword them, because they are part of the digest. A snapshot never
includes positions (`excludes_positions` is always true).

To verify a stored record, seal its draft again and compare the digests. An
application stores the record as an `AnalysisSnapshot` graph node, created only
if absent, whose id derives from the digest; `analysisOf` names the listing and
`analysisDigest` holds the digest. It shares the record through a
control lease of kind `share.read` that names the digest. The lease is bound to
the tenant, has an expiry and can be revoked.

## Signal models (EH-423)

`FinanceSignalModels` is a separate method that holds two closed-form models.
They moved here from agent-utilities so that each model has one owner. Both are
pure compute and informational only.

`bayes_fuse` fuses directional calls (`1` up, `-1` down, `0` no call) into
`P(up)`:

- A measured prior (`directional_accuracy`, `standalone_sharpe`, `pbo`) seeds a
  source with weight `accuracy × sharpe`, clamped to `[0, 1]`.
- A prior is dropped when it is overfit (`pbo > max_pbo`) or has no edge
  (`sharpe <= min_sharpe`).
- A source that calls without a seeded prior takes the default weight and
  accuracy.
- Each call moves the probability toward `P(up | call)` by the source's
  weight. Calls are applied in source-name order, so the answer does not depend
  on how the caller built its map.

`insider_equilibrium` models the Kyle insider under a dynamic legal-risk hazard
(Qiao & Xia, arXiv:2605.27684). It is a surveillance-design aid, not a trading
tool. It returns three parts:

- **The equilibrium.** The optimal intensity is
  `β* = (Σ − eκC) / (2Σ(λ + eκr))`, floored at zero. The answer includes the
  detection hazard, the expected profit, the penalty and the binding lever.
- **The schedule.** Enforcement decays with the remaining window, so the
  insider accelerates toward its end.
- **The penalty policy.** The comparative statics in the criminal and civil
  penalties, the criminal cost that suppresses the insider (`null` when no
  finite cost does), and a typed verdict: `enforcement_gated`,
  `criminal_suppresses` or `criminal_is_the_lever`.
