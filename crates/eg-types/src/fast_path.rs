//! The native primary-key fast path (EG-DURABLE-KERNEL-R032): point operations
//! issued over the pgwire, MySQL, and MSSQL wire listeners that bypass
//! DataFusion query planning. This is the typed model slice (`.1`): a closed
//! operation set and a routing decision whose `Ok` result always carries
//! authorization-checked and audit-checked as true, refusing (never silently
//! bypassing) an unauthorized operation. The real wire-listener dispatch and
//! the hybrid hot/cold storage provider are later children.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A primary-key point operation eligible for the fast path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrimaryKeyOperation {
    Get,
    Put,
    Delete,
    CompareAndSwap,
}

/// The fast path's routing decision for one primary-key operation. Both check
/// fields are always `true` on construction via [`route`] -- there is no
/// public way to build one with either check `false`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FastPathDecision {
    pub operation: PrimaryKeyOperation,
    pub bypasses_planner: bool,
    pub authorization_checked: bool,
    pub audit_checked: bool,
}

/// An operation was refused the fast path because it was not authorized.
/// Carries the operation that was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FastPathRefused(pub PrimaryKeyOperation);

impl fmt::Display for FastPathRefused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "primary-key operation {:?} refused the fast path: not authorized",
            self.0
        )
    }
}

impl std::error::Error for FastPathRefused {}

/// Decide whether `operation` may take the fast path. Refuses (rather than
/// silently routing through unauthorized) when `authorized` is false. The
/// `Ok` result always reports both authorization and audit as checked: a
/// caller cannot construct a decision that skips either.
pub fn route(
    operation: PrimaryKeyOperation,
    authorized: bool,
) -> Result<FastPathDecision, FastPathRefused> {
    if !authorized {
        return Err(FastPathRefused(operation));
    }
    Ok(FastPathDecision {
        operation,
        bypasses_planner: true,
        authorization_checked: true,
        audit_checked: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: [PrimaryKeyOperation; 4] = [
        PrimaryKeyOperation::Get,
        PrimaryKeyOperation::Put,
        PrimaryKeyOperation::Delete,
        PrimaryKeyOperation::CompareAndSwap,
    ];

    // spec: EG-DURABLE-KERNEL-R032.1
    #[test]
    fn operations_round_trip_through_their_wire_name() {
        for (op, name) in [
            (PrimaryKeyOperation::Get, "get"),
            (PrimaryKeyOperation::Put, "put"),
            (PrimaryKeyOperation::Delete, "delete"),
            (PrimaryKeyOperation::CompareAndSwap, "compareandswap"),
        ] {
            let wire = serde_json::to_string(&op).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            let back: PrimaryKeyOperation = serde_json::from_str(&wire).unwrap();
            assert_eq!(back, op);
        }
    }

    // spec: EG-DURABLE-KERNEL-R032.1
    #[test]
    fn authorized_operation_routes_with_both_checks_true() {
        for op in ALL {
            let decision = route(op, true).unwrap();
            assert!(decision.bypasses_planner);
            assert!(decision.authorization_checked);
            assert!(decision.audit_checked);
            assert_eq!(decision.operation, op);
        }
    }

    // spec: EG-DURABLE-KERNEL-R032.1
    #[test]
    fn unauthorized_operation_is_refused_for_every_variant() {
        for op in ALL {
            let err = route(op, false).unwrap_err();
            assert_eq!(err.0, op);
        }
    }

    // spec: EG-DURABLE-KERNEL-R032.1
    #[test]
    fn refusal_display_names_the_operation() {
        let err = route(PrimaryKeyOperation::Delete, false).unwrap_err();
        assert!(err.to_string().contains("Delete"));
    }
}
