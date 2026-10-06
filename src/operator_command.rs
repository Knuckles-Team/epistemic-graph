//! Offline operator commands of the `epistemic-graph-server` binary.
//!
//! They run instead of the service: the process parses the command, does the
//! work against a stopped engine's data directory, prints a report whose last
//! line is one JSON object, and exits with the report's status.

use std::path::PathBuf;

use epistemic_graph::server::persistence::store_upgrade::{self, ApplyOptions};

#[derive(clap::Subcommand, Debug)]
pub(super) enum OperatorCommand {
    /// Inspect or apply the registered offline store-format upgrades of a
    /// stopped engine's data directory.
    StoreUpgrade {
        #[command(subcommand)]
        verb: StoreUpgradeVerb,
    },
}

#[derive(clap::Subcommand, Debug)]
pub(super) enum StoreUpgradeVerb {
    /// List every store, its format, and whether a registered upgrade applies.
    /// Read-only. Exit status: 0 nothing to do, 10 upgrade available, 20 a
    /// store is in a format this build neither opens nor upgrades, 30 a store
    /// could not be read, 1 refused.
    Inspect {
        /// The engine's persistence directory.
        #[arg(env = "GRAPH_SERVICE_PERSIST_DIR", value_name = "DATA_DIR")]
        data_dir: PathBuf,
    },
    /// Run every applicable registered upgrade in place. The engine must be
    /// stopped. Exit status: 0 done or nothing to do, 20 a store remains that
    /// this build neither opens nor upgrades, 30 a store could not be read and
    /// no upgrade admitted it, 1 refused or an upgrade failed.
    Apply {
        /// The engine's persistence directory.
        #[arg(env = "GRAPH_SERVICE_PERSIST_DIR", value_name = "DATA_DIR")]
        data_dir: PathBuf,
        /// Required: confirm that store files may be upgraded in place.
        #[arg(long)]
        confirm: bool,
        /// Private local directory for the scratch copy an inspection reads:
        /// mode 0700, inside a mode 0700 directory, not under /tmp. Defaults
        /// to one this command creates inside the data directory.
        #[arg(long, value_name = "DIRECTORY")]
        staging_dir: Option<PathBuf>,
        /// Disk budget of one store's scratch copy. Defaults to the store's
        /// size plus a quarter and 64 MiB.
        #[arg(long, value_name = "BYTES")]
        max_store_bytes: Option<u64>,
    },
}

/// Run an offline operator command instead of the service, when the command
/// line asked for one. Returns only when it did not.
pub(super) fn exit_if_requested(command: Option<OperatorCommand>) {
    if let Some(command) = command {
        std::process::exit(command.run());
    }
}

impl OperatorCommand {
    /// Run the command, print its report, and return the exit status.
    pub(super) fn run(self) -> i32 {
        let Self::StoreUpgrade { verb } = self;
        let report = match verb {
            StoreUpgradeVerb::Inspect { data_dir } => store_upgrade::inspect(&data_dir),
            StoreUpgradeVerb::Apply {
                data_dir,
                confirm,
                staging_dir,
                max_store_bytes,
            } => store_upgrade::apply(
                &data_dir,
                &ApplyOptions {
                    confirm,
                    staging_dir,
                    max_store_bytes,
                },
            ),
        };
        print!("{}", report.render());
        if let Some(error) = &report.error {
            eprintln!("error: store-upgrade {} refused: {error}", report.verb);
        }
        report.exit_code
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// The real command line of the server binary.
    type Cli = super::super::Args;

    #[test]
    fn the_documented_command_lines_parse() {
        let inspect = Cli::try_parse_from(["server", "store-upgrade", "inspect", "/data"]).unwrap();
        assert!(matches!(
            inspect.command,
            Some(OperatorCommand::StoreUpgrade {
                verb: StoreUpgradeVerb::Inspect { .. }
            })
        ));
        let apply = Cli::try_parse_from(["server", "store-upgrade", "apply", "/data", "--confirm"])
            .unwrap();
        assert!(matches!(
            apply.command,
            Some(OperatorCommand::StoreUpgrade {
                verb: StoreUpgradeVerb::Apply { confirm: true, .. }
            })
        ));
        assert!(Cli::try_parse_from(["server"]).unwrap().command.is_none());
    }

    #[test]
    fn apply_without_confirmation_is_refused_before_anything_is_opened() {
        let command = OperatorCommand::StoreUpgrade {
            verb: StoreUpgradeVerb::Apply {
                data_dir: PathBuf::from("/nonexistent-store-upgrade-directory"),
                confirm: false,
                staging_dir: None,
                max_store_bytes: None,
            },
        };
        assert_eq!(command.run(), store_upgrade::EXIT_FAILED);
    }
}
