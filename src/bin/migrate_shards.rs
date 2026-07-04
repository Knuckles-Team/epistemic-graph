//! `migrate-shards` — OFFLINE K-shard migration CLI (CONCEPT:EG-KG.sharding.atomic-shard-swap, M3).
//!
//! Rewrites an existing durable redb store (`graph.redb` for K=1, or a
//! `graph-<n>.redb` set) into a NEW shard count K, routing every graph with the SAME
//! EG-026 `FNV-1a(name) % K`, so the running engine finds each graph in the shard it
//! looks for. Run with the engine STOPPED (redb holds an exclusive per-file lock).
//!
//! ```text
//! # In-place (default): swap shard files, leave a recoverable backup.
//! migrate-shards --persist-dir /var/lib/epistemic-graph --shards 4
//!
//! # Out-of-place: write the new K into a fresh dir, swap manually after verifying.
//! migrate-shards --persist-dir /var/lib/eg --shards 4 --dest-dir /var/lib/eg-k4
//! ```

use std::path::Path;

use clap::Parser;

use epistemic_graph::server::persistence::shard_migrate;

#[derive(Parser, Debug)]
#[command(name = "migrate-shards")]
#[command(
    about = "Offline K-shard migration for the epistemic-graph durable store (CONCEPT:EG-KG.sharding.atomic-shard-swap)"
)]
struct Args {
    /// Persist dir holding the existing redb shard files (the source).
    #[arg(long, env = "GRAPH_SERVICE_PERSIST_DIR")]
    persist_dir: String,

    /// Target shard count K to migrate to (clamped to >= 1).
    #[arg(long)]
    shards: usize,

    /// Optional destination dir. When set, the new shards are written there (the source
    /// is left untouched). When omitted, the migration runs IN PLACE: new shards are
    /// swapped in and the old files moved to a recoverable backup dir.
    #[arg(long)]
    dest_dir: Option<String>,
}

fn main() {
    let args = Args::parse();

    let report = if let Some(dest) = args.dest_dir.as_deref() {
        shard_migrate::migrate_shards(Path::new(&args.persist_dir), Path::new(dest), args.shards)
    } else {
        shard_migrate::migrate_in_place(&args.persist_dir, args.shards)
    };

    match report {
        Ok(r) => {
            println!(
                "migration OK: {} -> {} shards | graphs={} nodes={} edges={} ledger={} \
                 semantic={} audit={} global={}",
                r.source_shards,
                r.dest_shards,
                r.graphs,
                r.nodes,
                r.edges,
                r.ledger,
                r.semantic,
                r.audit,
                r.global,
            );
        }
        Err(e) => {
            eprintln!("migration FAILED: {e}");
            std::process::exit(1);
        }
    }
}
