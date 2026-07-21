//! Incremental (DBSP-style) materialized-view maintenance
//! (CONCEPT:EG-KG.storage.incremental-matview).
//!
//! A dep-free Z-set delta model ([`zset`]) + a circuit compiler ([`circuit`]) that
//! generalizes `src/server/cdc.rs`'s hand-rolled two-aggregate `ContinuousQuery`/
//! `maintain()` (proved equal to a full re-run by its own unit tests) to an arbitrary
//! SUPPORTED `wire::Plan`. A plan the compiler cannot handle returns [`UnsupportedOp`]
//! naming the first offending op, so the server keeps that view on today's
//! recompute-on-`Get` path — a per-view fallback, never a silently-wrong answer.
//!
//! v1 scope (provably safe, directly modeled on this repo's `ContinuousQuery`
//! precedent): the LINEAR operators `Scan`/`Filter`(`Eq`/`GtNum`/`LtNum`)/`AsOf`, the
//! LINEAR tumbling aggregate `WindowAgg`(`count`/`sum`/`mean`/`avg`), and a `Limit`
//! applied at read. Bounded-hop `Traverse` (incremental Datalog / semi-naive
//! evaluation — a recursive fixpoint, not a relational join) and the ranking fusions
//! (`RankMmr`/`FuseRrf`) are a separately-proven v2 and fall back here.
//!
//! Gated on `query` (it names the `query`-gated `Op`/`Pred`/`TimeAxis` types), beside
//! `dag`/`dag_exec`.

pub mod circuit;
pub mod zset;

pub use circuit::{Circuit, UnsupportedOp};
pub use zset::{Delta, ZRow};
