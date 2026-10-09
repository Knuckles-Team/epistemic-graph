//! The workload classification every wire listener (native, RESP, pgwire,
//! MySQL, MSSQL) must agree on before routing an operation
//! (EG-DURABLE-KERNEL-R037): a `Point`/`Structure` operation goes to the
//! native `KvStore`/`GraphCore` path without allocating a DataFusion
//! session; a `Sql` statement goes to the planner. This is the typed-model
//! slice (`.1`): the enum and a pure classifier over the operation's
//! declared kind name, with a refusal for a name no listener declares. The
//! per-listener wiring and the per-prepared-statement cache are later
//! children.

use std::fmt;

/// Where a classified operation routes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WorkloadClass {
    /// A single-key read/write (GET/SET/INCR and similar). Routes to the
    /// native `KvStore` path.
    Point,
    /// A data-structure operation (hash/sorted-set/list/set members and
    /// similar). Routes to the native `GraphCore`/structure path.
    Structure,
    /// A SQL statement. Routes to the DataFusion planner.
    Sql,
}

impl WorkloadClass {
    /// `true` for the two classes that must never allocate a DataFusion
    /// session (this requirement's core acceptance clause).
    pub const fn bypasses_sql_planner(self) -> bool {
        !matches!(self, Self::Sql)
    }

    /// Classify a wire listener's declared operation-kind name. Every
    /// listener (native, RESP, pgwire, MySQL, MSSQL) must funnel its
    /// operation names through this one function so no listener invents its
    /// own routing decision. An operation kind no listener declares is
    /// refused rather than defaulted to either path.
    pub fn classify(operation_kind: &str) -> Result<Self, UnclassifiedOperation> {
        match operation_kind {
            "get" | "set" | "incrby" | "cas" | "del" | "expire" => Ok(Self::Point),
            "hash" | "sorted_set" | "list" | "set_members" => Ok(Self::Structure),
            "sql" => Ok(Self::Sql),
            other => Err(UnclassifiedOperation(other.to_string())),
        }
    }
}

/// The dispatch entry point (`EG-DURABLE-KERNEL-R037.2`): what a wire
/// listener's dispatcher calls, once per received operation, in place of
/// unconditionally allocating a DataFusion session. Carries the decision a
/// dispatcher needs — not just the class, but whether it must allocate a
/// session for this specific operation — so no listener has to re-derive
/// `bypasses_sql_planner` itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DispatchRoute {
    pub class: WorkloadClass,
    /// `true` only for `Sql`: every listener's dispatcher gates its
    /// DataFusion session allocation on this field, never on matching the
    /// class directly, so a future class added here cannot be missed by an
    /// `if class == Sql` check at a call site.
    pub allocate_datafusion_session: bool,
}

/// Classify a wire listener's declared operation-kind name AND decide the
/// dispatcher's one routing action from it: this is the single function
/// every listener's dispatch loop calls before deciding whether to touch
/// DataFusion.
pub fn route(operation_kind: &str) -> Result<DispatchRoute, UnclassifiedOperation> {
    let class = WorkloadClass::classify(operation_kind)?;
    Ok(DispatchRoute {
        class,
        allocate_datafusion_session: !class.bypasses_sql_planner(),
    })
}

/// A wire listener declared an operation-kind name no classifier rule
/// recognizes. Carries the rejected name verbatim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnclassifiedOperation(String);

impl fmt::Display for UnclassifiedOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "workload classifier does not recognize operation kind {:?}",
            self.0
        )
    }
}

impl std::error::Error for UnclassifiedOperation {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_and_structure_operations_bypass_the_sql_planner() {
        for kind in ["get", "set", "incrby", "cas", "del", "expire"] {
            assert_eq!(WorkloadClass::classify(kind), Ok(WorkloadClass::Point));
        }
        for kind in ["hash", "sorted_set", "list", "set_members"] {
            assert_eq!(WorkloadClass::classify(kind), Ok(WorkloadClass::Structure));
        }
        assert!(WorkloadClass::Point.bypasses_sql_planner());
        assert!(WorkloadClass::Structure.bypasses_sql_planner());
    }

    #[test]
    fn sql_routes_to_the_planner() {
        assert_eq!(WorkloadClass::classify("sql"), Ok(WorkloadClass::Sql));
        assert!(!WorkloadClass::Sql.bypasses_sql_planner());
    }

    #[test]
    fn unrecognized_operation_kind_is_refused_not_defaulted() {
        for bad in ["GET", "scan", "", "select"] {
            let err = WorkloadClass::classify(bad).unwrap_err();
            assert!(err.to_string().contains("does not recognize"));
        }
    }

    #[test]
    fn dispatch_route_never_allocates_a_datafusion_session_for_point_or_structure() {
        for kind in ["get", "set", "hash", "list"] {
            let decision = route(kind).unwrap();
            assert!(!decision.allocate_datafusion_session, "{kind}");
            assert!(decision.class.bypasses_sql_planner());
        }
    }

    #[test]
    fn dispatch_route_allocates_a_datafusion_session_for_sql() {
        let decision = route("sql").unwrap();
        assert!(decision.allocate_datafusion_session);
        assert_eq!(decision.class, WorkloadClass::Sql);
    }

    #[test]
    fn dispatch_route_propagates_the_classifier_refusal() {
        assert!(route("unknown-op").is_err());
    }
}
