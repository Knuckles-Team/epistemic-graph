//! `restore` — rebuild a durable persist-dir from an EG-090 backup bundle (CONCEPT:EG-090).
//!
//! Reads a backup bundle produced by the online backup path (`Method::Backup` /
//! `RedbBackend::backup`) — a directory of verbatim `graph.redb`/`graph-<n>.redb` shard
//! files + a `MANIFEST.json` — and rebuilds a fresh persist-dir from it, verbatim (value
//! blobs, encryption-at-rest ciphertext, and the KG-2.231 audit chain are copied
//! byte-for-byte). Mirrors the offline `migrate-shards` CLI (CONCEPT:EG-030) — restore
//! delegates to that same verbatim-import engine.
//!
//! Run with NOTHING serving out of `--persist-dir` (redb holds an exclusive per-file
//! lock); `--persist-dir` must be empty of target shard files (it refuses to clobber).
//!
//! ```text
//! # Restore at the bundle's own shard count K.
//! restore --bundle /backups/eg-2026-07-01 --persist-dir /var/lib/epistemic-graph
//!
//! # Re-shard on restore: rebuild at a DIFFERENT K (each graph re-routed by EG-026).
//! restore --bundle /backups/eg-2026-07-01 --persist-dir /var/lib/eg-k8 --shards 8
//! ```

use std::path::Path;

use clap::Parser;

use epistemic_graph::server::persistence::backup;

#[derive(Parser, Debug)]
#[command(name = "restore")]
#[command(about = "Rebuild a durable persist-dir from an EG-090 backup bundle (CONCEPT:EG-090)")]
struct Args {
    /// Backup bundle directory (holds graph*.redb + MANIFEST.json).
    #[arg(long)]
    bundle: String,

    /// Destination persist dir to rebuild (must be free of target shard files).
    #[arg(long, env = "GRAPH_SERVICE_PERSIST_DIR")]
    persist_dir: String,

    /// Optional target shard count K. Omit to restore at the bundle's own K; set to
    /// RE-SHARD on restore (each graph re-routed by EG-026 `FNV-1a % K`).
    #[arg(long)]
    shards: Option<usize>,
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
                "restore OK: bundle(engine={} ts={} label={:?} K={}) -> {} shards | \
                 graphs={} nodes={} edges={} ledger={} semantic={} audit={} global={}",
                m.engine_version,
                m.timestamp,
                m.label,
                m.shard_count,
                r.restored_shards,
                mig.graphs,
                mig.nodes,
                mig.edges,
                mig.ledger,
                mig.semantic,
                mig.audit,
                mig.global,
            );
        }
        Err(e) => {
            eprintln!("restore FAILED: {e}");
            std::process::exit(1);
        }
    }
}
