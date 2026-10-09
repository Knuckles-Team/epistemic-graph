//! EG-UNIFIED-DATA-PLANE-R007 — federating attached tables as DataFusion table providers with
//! dialect-aware sub-plan pushdown. This is the `.1` typed-model slice: the named pushdown
//! operations and a dialect's declared capability set, refusing an unknown operation name.
//! Wiring real `TableProvider`s is a later child.

use std::collections::BTreeSet;
use std::fmt;

/// One kind of sub-plan DataFusion may push down into a dialect's source query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PushdownOperation {
    Projection,
    Filter,
    Limit,
    Join,
    Aggregate,
}

impl PushdownOperation {
    /// Parses one of the five lowercase operation names this requirement lists.
    pub fn parse(name: &str) -> Result<Self, UnknownPushdownOperation> {
        match name {
            "projection" => Ok(Self::Projection),
            "filter" => Ok(Self::Filter),
            "limit" => Ok(Self::Limit),
            "join" => Ok(Self::Join),
            "aggregate" => Ok(Self::Aggregate),
            other => Err(UnknownPushdownOperation(other.to_string())),
        }
    }
}

/// A declared capability name is not one of the five pushdown operations this requirement names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownPushdownOperation(String);

impl fmt::Display for UnknownPushdownOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} is not a known pushdown operation (projection, filter, limit, join, aggregate)",
            self.0
        )
    }
}

impl std::error::Error for UnknownPushdownOperation {}

/// The set of pushdown operations one dialect's `TableProvider` declares it supports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DialectPushdownCapabilities {
    pub supported: BTreeSet<PushdownOperation>,
}

impl DialectPushdownCapabilities {
    /// Declares a capability set from operation names, refusing outright if any name is
    /// unrecognized rather than silently admitting a partial, wrong capability set.
    pub fn declare(names: &[&str]) -> Result<Self, UnknownPushdownOperation> {
        let mut supported = BTreeSet::new();
        for name in names {
            supported.insert(PushdownOperation::parse(name)?);
        }
        Ok(Self { supported })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_five_names_parse() {
        for (name, op) in [
            ("projection", PushdownOperation::Projection),
            ("filter", PushdownOperation::Filter),
            ("limit", PushdownOperation::Limit),
            ("join", PushdownOperation::Join),
            ("aggregate", PushdownOperation::Aggregate),
        ] {
            assert_eq!(PushdownOperation::parse(name), Ok(op));
        }
    }

    #[test]
    fn an_unknown_name_is_refused() {
        let err = PushdownOperation::parse("explode").unwrap_err();
        assert!(err.to_string().contains("not a known pushdown operation"));
    }

    #[test]
    fn declaring_all_valid_names_succeeds() {
        let caps =
            DialectPushdownCapabilities::declare(&["projection", "filter", "limit", "join", "aggregate"]).unwrap();
        assert_eq!(caps.supported.len(), 5);
    }

    #[test]
    fn declaring_one_bad_name_refuses_the_whole_set() {
        let err = DialectPushdownCapabilities::declare(&["projection", "explode"]).unwrap_err();
        assert!(err.to_string().contains("explode"));
    }
}
