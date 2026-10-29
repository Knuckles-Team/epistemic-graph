//! Automatic store migration metrics (MEDULLA-DURABLE-KERNEL-R070.1).
//!
//! The engine migrates before it serves, so these samples are on the registry
//! when the exposition endpoint starts. Labels come from closed sets: the step
//! (`run`, `resume`, `journal`, `store`) and its outcome or refusal code.

#[cfg(feature = "metrics")]
mod imp {
    use lazy_static::lazy_static;
    use prometheus::IntCounterVec;

    use super::super::imp::counter_vec;

    lazy_static! {
        static ref STEPS_TOTAL: IntCounterVec = counter_vec(
            "epistemic_graph_store_migration_steps_total",
            "Automatic store migration steps at open, by step and outcome",
            &["step", "outcome"],
        );
    }

    /// Count one step of an automatic store migration.
    pub fn store_migration_step(step: &str, outcome: &str) {
        STEPS_TOTAL.with_label_values(&[step, outcome]).inc();
    }
}

#[cfg(not(feature = "metrics"))]
mod imp {
    pub fn store_migration_step(_step: &str, _outcome: &str) {}
}

pub use imp::*;
