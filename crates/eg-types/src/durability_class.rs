//! The declared durability class for a KV namespace or table
//! (EG-DURABLE-KERNEL-R038): `ephemeral` (in-memory with TTL, no durable
//! log), `async` (group commit with a measured loss-window bound), or
//! `sync` (commit before acknowledgment -- the default). This is the typed
//! model slice (`.1`): the enum, its declared bound, and the refusal for an
//! unrecognized class name. The enforcement path (namespace open routing
//! the declared class to the matching commit discipline, and the exported
//! measured loss-window metric) is a later child.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Milliseconds. `DurabilityClass::Async`'s declared ceiling on its measured
/// commit loss window (EG-DURABLE-KERNEL-R038's acceptance clause).
pub const ASYNC_MAX_LOSS_WINDOW_MS: u64 = 100;

/// A KV namespace or table's declared durability class. Serialized as its
/// lowercase wire name so a stored declaration is stable across releases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityClass {
    /// In-memory with TTL; no durable log entry is written.
    Ephemeral,
    /// Group commit; the measured loss window must stay within
    /// [`ASYNC_MAX_LOSS_WINDOW_MS`].
    Async,
    /// Commit before acknowledgment. The default when a namespace declares
    /// none explicitly.
    Sync,
}

impl Default for DurabilityClass {
    fn default() -> Self {
        Self::Sync
    }
}

impl DurabilityClass {
    /// The declared loss-window ceiling in milliseconds, or `None` when the
    /// class admits no loss window (ephemeral never persists; sync never
    /// acknowledges ahead of its log write).
    pub const fn declared_loss_window_ms(self) -> Option<u64> {
        match self {
            Self::Async => Some(ASYNC_MAX_LOSS_WINDOW_MS),
            Self::Ephemeral | Self::Sync => None,
        }
    }

    /// Parse the wire name a namespace declaration carries. Refuses (rather
    /// than silently downgrading to a default) any name that is not one of
    /// the three declared classes, including case variants and whitespace --
    /// a namespace's durability guarantee is never inferred.
    pub fn parse_declared(name: &str) -> Result<Self, UnknownDurabilityClass> {
        match name {
            "ephemeral" => Ok(Self::Ephemeral),
            "async" => Ok(Self::Async),
            "sync" => Ok(Self::Sync),
            other => Err(UnknownDurabilityClass(other.to_string())),
        }
    }

    /// Confirm a measured loss window stays within this class's declared
    /// bound. `Ephemeral`/`Sync` accept only an exact-zero measurement --
    /// a nonzero loss window attributed to them is a measurement defect,
    /// never a silent widening of their guarantee.
    pub fn validate_measured_loss_window(
        self,
        measured_ms: u64,
    ) -> Result<(), LossWindowExceeded> {
        let bound = self.declared_loss_window_ms().unwrap_or(0);
        if measured_ms > bound {
            return Err(LossWindowExceeded {
                class: self,
                bound_ms: bound,
                measured_ms,
            });
        }
        Ok(())
    }
}

/// How a write-path commit must be driven for one class
/// (EG-DURABLE-KERNEL-R038.2's write-path entry point). The write path calls
/// [`DurabilityClass::write_plan`] once per namespace open / write admission
/// to decide whether it must wait for its own durable commit before
/// acknowledging, and how wide a group-commit window it may batch into.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurabilityWritePlan {
    /// `true`: acknowledge only after THIS write's own durable commit
    /// (`sync`). `false`: acknowledge is not gated on this write's own
    /// commit (`async` batches into the next group commit; `ephemeral`
    /// never durably commits at all).
    pub wait_for_own_durable_commit: bool,
    /// The widest a group-commit batch may delay this write, or `None` when
    /// the class admits no batching window (`sync` commits immediately per
    /// write; `ephemeral` never reaches the durable log to batch into).
    pub max_batch_delay_ms: Option<u64>,
}

impl DurabilityClass {
    /// The write path's one entry point: how THIS class drives a commit.
    pub const fn write_plan(self) -> DurabilityWritePlan {
        match self {
            Self::Sync => DurabilityWritePlan {
                wait_for_own_durable_commit: true,
                max_batch_delay_ms: None,
            },
            Self::Async => DurabilityWritePlan {
                wait_for_own_durable_commit: false,
                max_batch_delay_ms: Some(ASYNC_MAX_LOSS_WINDOW_MS),
            },
            Self::Ephemeral => DurabilityWritePlan {
                wait_for_own_durable_commit: false,
                max_batch_delay_ms: None,
            },
        }
    }

    /// Refuse a write path that is about to append to the durable (redb)
    /// log on behalf of an `ephemeral` namespace: ephemeral declares NO
    /// durable log, so a durable-log append under its name is a write-path
    /// defect, never silently accepted as a (then unenforced) durability
    /// upgrade. `sync`/`async` both declare a durable log and are allowed.
    pub fn refuse_if_durable_log_write(self) -> Result<(), EphemeralWriteRefusesDurableLog> {
        match self {
            Self::Ephemeral => Err(EphemeralWriteRefusesDurableLog),
            Self::Sync | Self::Async => Ok(()),
        }
    }
}

/// A write path attempted a durable (redb) log append for an `ephemeral`
/// namespace, which declares no durable log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EphemeralWriteRefusesDurableLog;

impl fmt::Display for EphemeralWriteRefusesDurableLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "write path refused: an ephemeral-class namespace declares no durable log, \
             so a durable-log append under it is never attempted",
        )
    }
}

impl std::error::Error for EphemeralWriteRefusesDurableLog {}

/// A namespace declared a durability class name this engine does not
/// recognize. Carries the rejected name verbatim so the operator sees
/// exactly what was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownDurabilityClass(String);

impl fmt::Display for UnknownDurabilityClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "durability class {:?} is not one of ephemeral, async, sync",
            self.0
        )
    }
}

impl std::error::Error for UnknownDurabilityClass {}

/// A class's measured commit loss window exceeded its declared bound.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LossWindowExceeded {
    pub class: DurabilityClass,
    pub bound_ms: u64,
    pub measured_ms: u64,
}

impl fmt::Display for LossWindowExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} durability class measured a {} ms loss window, exceeding its declared {} ms bound",
            self.class, self.measured_ms, self.bound_ms
        )
    }
}

impl std::error::Error for LossWindowExceeded {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_classes_round_trip_through_their_wire_name() {
        for (class, name) in [
            (DurabilityClass::Ephemeral, "ephemeral"),
            (DurabilityClass::Async, "async"),
            (DurabilityClass::Sync, "sync"),
        ] {
            assert_eq!(DurabilityClass::parse_declared(name), Ok(class));
            let wire = serde_json::to_string(&class).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            let back: DurabilityClass = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, class);
        }
    }

    #[test]
    fn sync_is_the_declared_default() {
        assert_eq!(DurabilityClass::default(), DurabilityClass::Sync);
    }

    #[test]
    fn unknown_class_name_is_refused_not_downgraded() {
        for bad in ["Async", " async", "asynchronous", "", "none"] {
            let err = DurabilityClass::parse_declared(bad).unwrap_err();
            assert!(err.to_string().contains("is not one of"));
        }
    }

    #[test]
    fn async_loss_window_within_bound_is_accepted() {
        DurabilityClass::Async
            .validate_measured_loss_window(ASYNC_MAX_LOSS_WINDOW_MS)
            .unwrap();
        DurabilityClass::Async.validate_measured_loss_window(0).unwrap();
    }

    #[test]
    fn async_loss_window_past_bound_is_refused() {
        let err = DurabilityClass::Async
            .validate_measured_loss_window(ASYNC_MAX_LOSS_WINDOW_MS + 1)
            .unwrap_err();
        assert_eq!(err.bound_ms, ASYNC_MAX_LOSS_WINDOW_MS);
        assert_eq!(err.measured_ms, ASYNC_MAX_LOSS_WINDOW_MS + 1);
    }

    #[test]
    fn ephemeral_and_sync_never_admit_a_nonzero_loss_window() {
        assert!(DurabilityClass::Ephemeral
            .validate_measured_loss_window(1)
            .is_err());
        assert!(DurabilityClass::Sync.validate_measured_loss_window(1).is_err());
        DurabilityClass::Ephemeral
            .validate_measured_loss_window(0)
            .unwrap();
        DurabilityClass::Sync.validate_measured_loss_window(0).unwrap();
    }

    #[test]
    fn sync_write_plan_waits_for_its_own_commit_with_no_batch_window() {
        let plan = DurabilityClass::Sync.write_plan();
        assert!(plan.wait_for_own_durable_commit);
        assert_eq!(plan.max_batch_delay_ms, None);
    }

    #[test]
    fn async_write_plan_batches_within_the_declared_loss_window() {
        let plan = DurabilityClass::Async.write_plan();
        assert!(!plan.wait_for_own_durable_commit);
        assert_eq!(plan.max_batch_delay_ms, Some(ASYNC_MAX_LOSS_WINDOW_MS));
    }

    #[test]
    fn ephemeral_write_plan_never_waits_and_never_batches_into_a_log() {
        let plan = DurabilityClass::Ephemeral.write_plan();
        assert!(!plan.wait_for_own_durable_commit);
        assert_eq!(plan.max_batch_delay_ms, None);
    }

    #[test]
    fn ephemeral_write_path_refuses_a_durable_log_append() {
        let err = DurabilityClass::Ephemeral
            .refuse_if_durable_log_write()
            .unwrap_err();
        assert!(err.to_string().contains("no durable log"));
    }

    #[test]
    fn sync_and_async_write_paths_allow_a_durable_log_append() {
        DurabilityClass::Sync.refuse_if_durable_log_write().unwrap();
        DurabilityClass::Async.refuse_if_durable_log_write().unwrap();
    }
}
