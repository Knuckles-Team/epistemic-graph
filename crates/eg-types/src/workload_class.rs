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
}
