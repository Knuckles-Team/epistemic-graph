//! Required raft, backup, and persistence library-test selections for
//! EG-DURABLE-KERNEL-R011. The declaration and concrete manifest are the
//! `.1` and `.2` slices. Executing this manifest before merge and recording
//! exact-head CI receipts remain the `.3` and `.4` obligations.

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
    /// Explicit Cargo features, with defaults disabled for reproducibility.
    pub cargo_features: Vec<String>,
}

impl RequiredTestModule {
    /// Construct the Cargo arguments for this library-test selection.
    pub fn cargo_test_args(&self) -> Result<Vec<String>, InvalidRequiredTestModule> {
        self.validate()?;
        let mut args = vec![
            "test".to_string(),
            "--locked".to_string(),
            "--lib".to_string(),
            "--no-default-features".to_string(),
            "-p".to_string(),
            self.crate_name.clone(),
        ];
        if !self.cargo_features.is_empty() {
            args.extend(["--features".to_string(), self.cargo_features.join(",")]);
        }
        args.push(self.cargo_test_filter.clone());
        Ok(args)
    }

    /// Refuse a discovery inventory with no test matching this libtest filter.
    /// Callers must obtain names from the compiled target's `--list` output;
    /// this check is not evidence that the selected tests have passed.
    pub fn validate_selection(&self, test_names: &[&str]) -> Result<(), InvalidRequiredTestModule> {
        self.validate()?;
        if !test_names
            .iter()
            .any(|name| name.contains(&self.cargo_test_filter))
        {
            return Err(InvalidRequiredTestModule::EmptyTestSelection);
        }
        Ok(())
    }

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
    EmptyTestSelection,
}

impl fmt::Display for InvalidRequiredTestModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::EmptyCrateName => "names no crate",
            Self::EmptyTestFilter => "names no cargo test filter",
            Self::EmptyTestSelection => "selects no discovered tests",
        };
        write!(f, "required test module declaration {reason}")
    }
}

impl std::error::Error for InvalidRequiredTestModule {}

/// The complete three-category manifest. These are module filters, not exact
/// individual-test names, so new tests in each module are included automatically.
/// `cluster` supplies raft; `full` supplies server and redb persistence.
/// The transaction backup module is unconditional and needs no Cargo features.
pub fn required_test_modules() -> Vec<RequiredTestModule> {
    use RequiredTestModuleCategory::{Backup, Persistence, Raft};
    [
        (Raft, "epistemic-graph", "raft::", &["cluster"][..]),
        (Backup, "eg-transaction", "tests::backup_replay::", &[][..]),
        (
            Persistence,
            "epistemic-graph",
            "server::persistence::",
            &["full"][..],
        ),
    ]
    .into_iter()
    .map(
        |(category, crate_name, cargo_test_filter, features)| RequiredTestModule {
            category,
            crate_name: crate_name.to_string(),
            cargo_test_filter: cargo_test_filter.to_string(),
            cargo_features: features
                .iter()
                .map(|feature| (*feature).to_string())
                .collect(),
        },
    )
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-DURABLE-KERNEL-R011.1
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

    // spec: EG-DURABLE-KERNEL-R011.1
    #[test]
    fn an_unrecognized_category_name_is_refused() {
        for bad in ["Raft", "raft ", "smoke", ""] {
            let err = RequiredTestModuleCategory::parse(bad).unwrap_err();
            assert!(err.to_string().contains("not a required pre-merge"));
        }
    }

    // spec: EG-DURABLE-KERNEL-R011.1
    #[test]
    fn a_complete_declaration_validates() {
        let module = RequiredTestModule {
            category: RequiredTestModuleCategory::Raft,
            crate_name: "epistemic-graph".to_string(),
            cargo_test_filter: "raft::placement_admin_cluster".to_string(),
            cargo_features: vec!["cluster".to_string()],
        };
        module.validate().unwrap();
    }

    // spec: EG-DURABLE-KERNEL-R011.1
    #[test]
    fn a_declaration_with_no_crate_name_is_refused() {
        let module = RequiredTestModule {
            category: RequiredTestModuleCategory::Backup,
            crate_name: "  ".to_string(),
            cargo_test_filter: "backup::restore_round_trip".to_string(),
            cargo_features: vec![],
        };
        assert_eq!(
            module.validate().unwrap_err(),
            InvalidRequiredTestModule::EmptyCrateName
        );
    }

    // spec: EG-DURABLE-KERNEL-R011.1
    #[test]
    fn a_declaration_with_no_test_filter_is_refused() {
        let module = RequiredTestModule {
            category: RequiredTestModuleCategory::Persistence,
            crate_name: "epistemic-graph".to_string(),
            cargo_test_filter: String::new(),
            cargo_features: vec![],
        };
        assert_eq!(
            module.validate().unwrap_err(),
            InvalidRequiredTestModule::EmptyTestFilter
        );
    }

    // spec: EG-DURABLE-KERNEL-R011.2
    #[test]
    fn required_manifest_covers_runtime_categories() {
        use RequiredTestModuleCategory::{Backup, Persistence, Raft};
        let manifest = required_test_modules();
        assert_eq!(manifest.len(), 3);
        let expected = [
            (Raft, "epistemic-graph", "raft::", vec!["cluster"]),
            (Backup, "eg-transaction", "tests::backup_replay::", vec![]),
            (
                Persistence,
                "epistemic-graph",
                "server::persistence::",
                vec!["full"],
            ),
        ];
        for (module, (category, package, filter, features)) in manifest.iter().zip(expected) {
            module.validate().unwrap();
            assert_eq!(
                (
                    module.category,
                    module.crate_name.as_str(),
                    module.cargo_test_filter.as_str()
                ),
                (category, package, filter)
            );
            assert_eq!(module.cargo_features, features);
            let args = module.cargo_test_args().unwrap();
            assert_eq!(
                &args[..6],
                [
                    "test",
                    "--locked",
                    "--lib",
                    "--no-default-features",
                    "-p",
                    package
                ]
            );
            assert_eq!(args.last().unwrap(), filter);
            assert_eq!(
                args.iter().any(|arg| arg == "--features"),
                !features.is_empty()
            );
        }
    }

    // spec: EG-DURABLE-KERNEL-R011.2
    #[test]
    fn required_manifest_rejects_empty_selection() {
        for module in required_test_modules() {
            for inventory in [&[][..], &["unrelated::test"][..]] {
                assert_eq!(
                    module.validate_selection(inventory),
                    Err(InvalidRequiredTestModule::EmptyTestSelection)
                );
            }
            // A discovery fixture checks matching semantics, not runtime success.
            let matching = format!("{}example", module.cargo_test_filter);
            module.validate_selection(&[&matching]).unwrap();
        }
    }

    // spec: EG-DURABLE-KERNEL-R011.2
    #[test]
    fn required_manifest_anchors_exist_in_source() {
        // Source bindings catch stale anchor names. Compiled discovery and runtime
        // success are deliberately left to the R011.3 gate.
        let anchors = [
            (
                include_str!("../../../src/raft/tests.rs"),
                "fn durable_log_replays_from_redb_after_restart(",
            ),
            (
                include_str!("../../eg-transaction/src/tests/backup_replay.rs"),
                "fn a_coordinator_backup_preserves_every_consumed_nonce_and_receipt(",
            ),
            (
                include_str!("../../../src/server/persistence/backup.rs"),
                "fn backup_restore_roundtrip_preserves_everything(",
            ),
        ];
        for (source, declaration) in anchors {
            assert!(source.contains(declaration), "missing {declaration}");
        }
        assert!(
            include_str!("../../eg-transaction/src/tests/mod.rs").contains("mod backup_replay;")
        );
        assert!(include_str!("../../../Cargo.toml").contains("cluster = [\"full\", \"raft\""));
    }
}
