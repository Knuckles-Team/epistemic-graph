//! Offline tools folded into the single `epistemic-graph-server` executable.
//!
//! `epistemic-graph-server <tool> ...` runs the tool instead of the service and
//! exits. With no tool name the process serves, so existing invocations keep
//! working. The tool is recognised by the first argument only; the server's own
//! flags never collide with these names.

use std::ffi::OsString;

#[cfg(feature = "redb")]
mod migrate_shards;
#[cfg(feature = "redb")]
mod restore;
#[cfg(feature = "ann-redb")]
mod rf019_observe;
#[cfg(feature = "lake")]
mod lake_fixture_export;

/// Names of the offline tools, whether or not this build compiled them in.
const TOOLS: &[&str] = &[
    "restore",
    "migrate-shards",
    "epistemic-graph-rf019-observe",
    "lake-fixture-export",
];

fn missing_feature(tool: &str, feature: &str) -> ! {
    eprintln!("epistemic-graph-server {tool}: this build lacks the `{feature}` feature");
    std::process::exit(2);
}

/// Run the offline tool named by the first argument and exit, or return when
/// the arguments name no tool (the process then serves).
pub(crate) fn run_if_requested() {
    let argv: Vec<OsString> = std::env::args_os().skip(1).collect();
    let Some(name) = argv.first().and_then(|a| a.to_str()) else {
        return;
    };
    if !TOOLS.contains(&name) {
        return;
    }
    let name = name.to_owned();
    // argv[0] of the tool is the subcommand name; clap prints the rest.
    match name.as_str() {
        #[cfg(feature = "redb")]
        "restore" => restore::run(argv),
        #[cfg(not(feature = "redb"))]
        "restore" => missing_feature("restore", "redb"),
        #[cfg(feature = "redb")]
        "migrate-shards" => migrate_shards::run(argv),
        #[cfg(not(feature = "redb"))]
        "migrate-shards" => missing_feature("migrate-shards", "redb"),
        #[cfg(feature = "ann-redb")]
        "epistemic-graph-rf019-observe" => rf019_observe::run(argv),
        #[cfg(not(feature = "ann-redb"))]
        "epistemic-graph-rf019-observe" => missing_feature(&name, "ann-redb"),
        #[cfg(feature = "lake")]
        "lake-fixture-export" => lake_fixture_export::run(argv),
        #[cfg(not(feature = "lake"))]
        "lake-fixture-export" => missing_feature("lake-fixture-export", "lake"),
        _ => return,
    }
    std::process::exit(0);
}
