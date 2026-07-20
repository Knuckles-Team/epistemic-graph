//! `restore` — rebuild a durable persist-dir from an EG-090 backup bundle (CONCEPT:EG-KG.sharding.reshard-on-restore).
//!
//! Reads a backup bundle produced by the online backup path (`Method::Backup` /
//! `RedbBackend::backup`) — a directory of verbatim `graph-<n>.redb` shard
//! files + a `MANIFEST.json` — and rebuilds a fresh persist-dir from it, verbatim (value
//! blobs, encryption-at-rest ciphertext, and the KG-2.231 audit chain are copied
//! byte-for-byte). Mirrors the offline `migrate-shards` CLI (CONCEPT:EG-KG.sharding.atomic-shard-swap) — restore
//! delegates to that same verbatim-import engine.
//!
//! Run with NOTHING serving out of `--persist-dir` (redb holds an exclusive per-file
//! lock); `--persist-dir` must be empty of target shard files (it refuses to clobber).
//!
//! ```text
//! # Restore at the bundle's own shard count K (state K explicitly).
//! restore --bundle /backups/eg-2026-07-01 --persist-dir /var/lib/epistemic-graph --shards 1
//!
//! # Re-shard on restore: rebuild at a DIFFERENT K (each graph re-routed by EG-026).
//! restore --bundle /backups/eg-2026-07-01 --persist-dir /var/lib/eg-k8 --shards 8
//! ```

use std::path::Path;

use clap::Parser;

use epistemic_graph::server::persistence::backup;

#[derive(Parser, Debug)]
#[command(name = "restore")]
#[command(
    about = "Rebuild a durable persist-dir from an EG-090 backup bundle (CONCEPT:EG-KG.sharding.reshard-on-restore)"
)]
struct Args {
    /// Backup bundle directory (holds graph*.redb + MANIFEST.json).
    #[arg(long)]
    bundle: String,

    /// Destination persist dir to rebuild (must be free of target shard files).
    #[arg(long, env = "GRAPH_SERVICE_PERSIST_DIR")]
    persist_dir: String,

    /// Required target shard count K. Use the bundle's K for a 1:1 restore or a
    /// different K to re-shard through EG-026 `FNV-1a % K`.
    #[arg(long)]
    shards: usize,
}

fn main() {
    let args = Args::parse();

    match backup::restore_bundle(
        Path::new(&args.bundle),
        Path::new(&args.persist_dir),
        args.shards,
    ) {
        Ok(r) => {
            let m = &r.manifest;
            let mig = &r.migration;
            println!(
                "restore OK: label_ref={} K={} -> {} shards | \
                 graphs={} nodes={} edges={} ledger={} semantic={} audit={} global={} \
                 admin_batches={} encrypted_recovery_plans={} xshard_decisions={}",
                m.label_ref,
                m.shard_count,
                r.restored_shards,
                mig.graphs,
                mig.nodes,
                mig.edges,
                mig.ledger,
                mig.semantic,
                mig.audit,
                mig.global,
                r.admin_mutations.batches,
                r.admin_mutations.encrypted_private_payloads,
                m.xshard_decisions,
            );
        }
        Err(e) => {
            eprintln!("restore FAILED: error_ref={}", opaque_ref(&e));
            std::process::exit(1);
        }
    }
}

fn opaque_ref(value: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write as _;

    let digest = Sha256::digest(value.as_bytes());
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        let _ = write!(encoded, "{byte:02x}");
    }
    format!("sha256:{encoded}")
}
