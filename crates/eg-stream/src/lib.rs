//! # eg-stream — the event-stream + complex-event-processing (CEP) modality (CONCEPT:EG-088)
//!
//! A pure-Rust leaf crate (a sibling of `eg-ann` / `eg-geo` / `eg-tensor`) giving the
//! engine a **bounded NFA-based CEP engine** over a time-ordered event stream, with **NO
//! async runtime / NO tokio / NO C dependency** — the Raspberry-Pi contract. It provides:
//!
//! * [`Event`] — a `{ ts: u64, key: String, attrs: serde_json::Map }` record (the shape
//!   fixed by the concept row). serde-serializable so events persist / ride the wire.
//! * [`CepPattern`] — the pattern algebra: [`CepPattern::Sequence`] (matchers that must
//!   fire in order within the window), [`CepPattern::Within`] (a duration constraint
//!   wrapping an inner pattern), and [`CepPattern::Absence`] (an `a` NOT-followed-by `b`
//!   within a horizon). Each step is an [`EventMatcher`] = an optional event `key` +
//!   a set of per-event [`AttrPredicate`]s (`Eq` / `Gt` / `Lt` / `Exists`).
//! * [`Window`] — the [`Window::Sliding`] / [`Window::Tumbling`] windowing model.
//! * [`run`] — `run(pattern, events, window) -> Vec<Match>`: a **bounded** NFA (the count
//!   of live partial matches is capped, so a pathological stream can never blow memory)
//!   that emits every [`Match`] the pattern detects.
//!
//! ## How it ties into the engine
//!
//! * **Windowing primitive (EG-067).** [`Window::Sliding`] / [`Window::Tumbling`] are the
//!   CEP-side windows; the engine's `Op::Window { secs }` (CONCEPT:EG-067) is the plan-AST
//!   windowing primitive an `Op::Cep` sits beside — the two share the "trailing window over
//!   a time-ordered RowSet" model.
//! * **Wire variant.** The wire algebra `Op::Cep { pattern }` lives in `eg-types::wire`
//!   (pure-serde `CepPatternSpec`, Pi-safe, no eg-stream dep); the executor that drives
//!   THIS crate over an input RowSet lives in `eg-plan::exec` behind eg-plan's `stream`
//!   feature. The batch `Op::Cep` over a RowSet + this engine is what lands.
//! * **Live standing queries (EG-064) — the `stream` feature.** The DEFERRED push half is
//!   now built in [`live`] (behind the `stream` feature, the only thing that pulls
//!   `tokio`): [`live::CepEngine`] owns N standing queries, fans each incoming [`Event`]
//!   to every query's incremental NFA state (the SAME steppers [`run`] is built from —
//!   see [`live`]), and pushes completed matches out per-query
//!   [`tokio::sync::broadcast`] channels ([`live::CepSubscription`]) with drop-oldest +
//!   lag-count backpressure. Sliding + tumbling windows and `within`-timeout expiry all
//!   work on the live path, and `eg088_live_equals_batch_*` asserts the live emission set
//!   equals [`run`] for the same ts-ordered stream. The default / Pi build enables
//!   neither `tokio` nor [`live`] — the core stays synchronous + runtime-free. Wiring the
//!   EG-064 `ChangeNotifier` → `Event` adapter and any Method/WebSocket subscription route
//!   into the SERVER is the remaining documented follow-up (eg-stream is a leaf crate and
//!   must not depend on eg-core; exactly as `eg_graphql::LiveQuery` leaves its transport
//!   to the server).
//!
//! The batch core is dependency-light (serde + serde_json); the `stream` feature adds
//! `tokio`. The crate is folded into the `node`/`full` serving tiers, kept OUT of `pi`.

mod cep;
mod event;
#[cfg(feature = "stream")]
pub mod live;

pub use cep::{run, CepPattern, Match, Window, MAX_ACTIVE_RUNS};
pub use event::{AttrPredicate, Event, EventMatcher};
