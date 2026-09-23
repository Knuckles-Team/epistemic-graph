//! The resident option-attention scorer (Decide v2, EH-291..EH-303).
//!
//! A fourth-rung head, read exactly where a listwise logistic head is read:
//! it scores ONLY the legal options the deterministic rungs derived
//! ([`legal`]), at most its shortlist of them, in fixed-point integer
//! arithmetic ([`fixed`]) so a decision replays to the same bits on every
//! host. [`forward`] is the served path; [`train`] fits the parameters at fit
//! time; [`trajectory`] reads one state over temporal prefixes.
//!
//! Module DAG: `fixed` <- `legal` <- `forward` <- {`train`, `trajectory`}.

pub mod fixed;
pub mod forward;
pub mod legal;
pub mod train;
pub mod trajectory;
