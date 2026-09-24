//! Evaluation kernels (EH-530): the generic forms of what the finance module computed
//! in private — the normal special functions, backtest validation (purged CPCV,
//! Deflated Sharpe, PBO, Diebold-Mariano) and predictive skill (IC, IR, effective
//! breadth, the FeatureSkill report). The finance Methods call these; so do the
//! decision layer and UQL.

pub mod backtest;
pub mod skill;
pub mod special;
