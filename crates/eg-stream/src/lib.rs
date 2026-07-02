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
//! * **Live standing queries (EG-064) — documented follow-up.** The EG-064 CDC
//!   `ChangeNotifier` broadcast bus can feed live windows so a *standing* CEP query
//!   subscribes and matches incrementally. That live loop is an acceptable follow-up; it
//!   is deliberately NOT built here so the engine stays synchronous and runtime-free
//!   (hence trivially unit-testable). The engine's [`run`] is the reusable core the live
//!   loop would call per advanced window.
//!
//! This crate is dependency-light (serde + serde_json — see `Cargo.toml`) and is folded
//! into the `node`/`full` serving tiers, kept OUT of `pi`.

mod cep;
mod event;

pub use cep::{run, CepPattern, Match, Window, MAX_ACTIVE_RUNS};
pub use event::{AttrPredicate, Event, EventMatcher};
