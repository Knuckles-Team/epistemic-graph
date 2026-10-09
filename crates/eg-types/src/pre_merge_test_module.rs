//! The named categories of test module EG-DURABLE-KERNEL-R011 requires CI to
//! run before any change is accepted (raft, backup, persistence), and a
//! typed declaration of one required module within a category. This is the
//! typed-model slice (`.1`): the category enum, the declaration type and its
//! validation, and the refusal for an unrecognized category name. Wiring a
//! concrete manifest of real crate/test-filter entries into the CI gate (and
//! moving that gate to run pre-merge rather than only on `main`) is a later
//! child.

use std::fmt;

/// One of the three test-module categories this requirement names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RequiredTestModuleCategory {
    Raft,
    Backup,
    Persistence,
}

impl RequiredTestModuleCategory {
    /// Parse a category's declared name. Refuses any name the requirement
    /// does not list, rather than silently admitting an unrelated test
    /// suite into the pre-merge-required set.
    pub fn parse(name: &str) -> Result<Self, UnrecognizedTestModuleCategory> {
        match name {
            "raft" => Ok(Self::Raft),
            "backup" => Ok(Self::Backup),
            "persistence" => Ok(Self::Persistence),
            other => Err(UnrecognizedTestModuleCategory(other.to_string())),
        }
    }
}

/// A declared category name is not one of `raft`/`backup`/`persistence`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnrecognizedTestModuleCategory(String);

impl fmt::Display for UnrecognizedTestModuleCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} is not a required pre-merge test-module category (raft, backup, persistence)",
            self.0
        )
    }
}

impl std::error::Error for UnrecognizedTestModuleCategory {}

/// One test module CI must run, pre-merge, before any change naming its
/// category is accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredTestModule {
    pub category: RequiredTestModuleCategory,
    /// The Cargo package the module's tests live in (e.g. `epistemic-graph`).
    pub crate_name: String,
    /// The `cargo test` filter that selects exactly this module (e.g.
    /// `raft::placement_admin_cluster`).
    pub cargo_test_filter: String,
}

impl RequiredTestModule {
    /// Refuse a declaration with no package name or no test filter: either
    /// one being empty means CI cannot actually select and run the module,
    /// which would make the "before any change is accepted" guarantee
    /// vacuous.
    pub fn validate(&self) -> Result<(), InvalidRequiredTestModule> {
        if self.crate_name.trim().is_empty() {
            return Err(InvalidRequiredTestModule::EmptyCrateName);
        }
        if self.cargo_test_filter.trim().is_empty() {
            return Err(InvalidRequiredTestModule::EmptyTestFilter);
        }
        Ok(())
    }
}

/// Why a `RequiredTestModule` declaration was refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvalidRequiredTestModule {
    EmptyCrateName,
    EmptyTestFilter,
}

impl fmt::Display for InvalidRequiredTestModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::EmptyCrateName => "names no crate",
            Self::EmptyTestFilter => "names no cargo test filter",
        };
        write!(f, "required test module declaration {reason}")
    }
}

impl std::error::Error for InvalidRequiredTestModule {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_three_named_categories_parse() {
        assert_eq!(
            RequiredTestModuleCategory::parse("raft"),
            Ok(RequiredTestModuleCategory::Raft)
        );
        assert_eq!(
            RequiredTestModuleCategory::parse("backup"),
            Ok(RequiredTestModuleCategory::Backup)
        );
        assert_eq!(
            RequiredTestModuleCategory::parse("persistence"),
            Ok(RequiredTestModuleCategory::Persistence)
        );
    }

    #[test]
    fn an_unrecognized_category_name_is_refused() {
        for bad in ["Raft", "raft ", "smoke", ""] {
            let err = RequiredTestModuleCategory::parse(bad).unwrap_err();
            assert!(err.to_string().contains("not a required pre-merge"));
        }
    }

    #[test]
    fn a_complete_declaration_validates() {
        let module = RequiredTestModule {
            category: RequiredTestModuleCategory::Raft,
            crate_name: "epistemic-graph".to_string(),
            cargo_test_filter: "raft::placement_admin_cluster".to_string(),
        };
        module.validate().unwrap();
    }

    #[test]
    fn a_declaration_with_no_crate_name_is_refused() {
        let module = RequiredTestModule {
            category: RequiredTestModuleCategory::Backup,
            crate_name: "  ".to_string(),
            cargo_test_filter: "backup::restore_round_trip".to_string(),
        };
        assert_eq!(
            module.validate().unwrap_err(),
            InvalidRequiredTestModule::EmptyCrateName
        );
    }

    #[test]
    fn a_declaration_with_no_test_filter_is_refused() {
        let module = RequiredTestModule {
            category: RequiredTestModuleCategory::Persistence,
            crate_name: "epistemic-graph".to_string(),
            cargo_test_filter: String::new(),
        };
        assert_eq!(
            module.validate().unwrap_err(),
            InvalidRequiredTestModule::EmptyTestFilter
        );
    }
}
