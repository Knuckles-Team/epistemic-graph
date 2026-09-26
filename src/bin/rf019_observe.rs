//! Offline RF-019 migration observer. It derives facts under EG's engine lease
//! and exits; it never verifies global receipts or grants serving authority.

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "epistemic-graph-rf019-observe")]
struct Args {
    #[arg(long)]
    persist_dir: PathBuf,
    #[arg(long)]
    tenant: String,
    #[arg(long = "binding", required = true)]
    bindings: Vec<String>,
    #[arg(long)]
    max_source_bytes: u64,
    #[arg(long)]
    max_owner_bytes: u64,
}

fn main() {
    let args = Args::parse();
    let result = epistemic_graph::server::observe_rf019_tenant_owner(
        &args.persist_dir,
        &args.tenant,
        &args.bindings,
        args.max_source_bytes,
        args.max_owner_bytes,
    );
    match result {
        Ok(value) => {
            let sorted: std::collections::BTreeMap<String, serde_json::Value> =
                serde_json::from_value(value).expect("closed observation is an object");
            let mut encoded = serde_json::to_vec(&sorted).expect("closed observation is JSON");
            encoded.push(b'\n');
            if let Err(error) = std::io::Write::write_all(&mut std::io::stdout(), &encoded) {
                eprintln!("RF-019 observation output failed: {error}");
                std::process::exit(1);
            }
        }
        Err(_error) => {
            // Source identities and filenames can be sensitive. The privileged
            // operator sees only a refusal, never unverified partial facts.
            eprintln!("RF-019 verified migration observation refused");
            std::process::exit(1);
        }
    }
}
