//! `nemesis` — the longer fault-injection SOAK runner (CONCEPT:KG-2.212).
//!
//! The `cargo test --features raft harness::` gauntlet is bounded + deterministic for
//! CI. This bin runs the SAME machinery for an unbounded, seeded soak: spin a cluster,
//! drive load, inject random faults round after round, and assert the invariants each
//! round — surfacing rare durability/consensus bugs a short test won't hit.
//!
//! Usage:
//!   cargo run --features harness --bin nemesis -- [--nodes 3] [--rounds 10]
//!       [--seed 42] [--writers 6] [--rate 200] [--round-secs 12]
//!
//! Exits non-zero the moment an invariant fails (a lost acked write / split-brain),
//! printing the failing round's verdict — so it is usable in a nightly CI soak job.

use std::time::Duration;

use epistemic_graph::raft::harness::loadgen::LoadConfig;
use epistemic_graph::raft::harness::nemesis::Nemesis;
use epistemic_graph::raft::harness::run_gauntlet;

struct Opts {
    nodes: usize,
    rounds: usize,
    seed: u64,
    writers: usize,
    rate: u64,
    round_secs: u64,
}

fn parse() -> Opts {
    let mut o = Opts {
        nodes: 3,
        rounds: 10,
        seed: 42,
        writers: 6,
        rate: 200,
        round_secs: 12,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = || args.next().expect("missing value for flag");
        match a.as_str() {
            "--nodes" => o.nodes = val().parse().expect("nodes"),
            "--rounds" => o.rounds = val().parse().expect("rounds"),
            "--seed" => o.seed = val().parse().expect("seed"),
            "--writers" => o.writers = val().parse().expect("writers"),
            "--rate" => o.rate = val().parse().expect("rate"),
            "--round-secs" => o.round_secs = val().parse().expect("round-secs"),
            "-h" | "--help" => {
                eprintln!(
                    "nemesis soak — flags: --nodes --rounds --seed --writers --rate --round-secs"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown flag: {other}");
                std::process::exit(2);
            }
        }
    }
    o
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .init();
    let o = parse();
    println!(
        "nemesis soak: nodes={} rounds={} seed={} writers={} rate={} round_secs={}",
        o.nodes, o.rounds, o.seed, o.writers, o.rate, o.round_secs
    );

    // The fault schedule per round is drawn from a seeded RNG so a `--seed N` run
    // reproduces exactly. Each round draws 1-2 faults that keep a quorum.
    let mut chooser = Nemesis::new(o.seed);
    let ids: Vec<u64> = (1..=o.nodes as u64).collect();

    for round in 1..=o.rounds {
        let f1 = chooser.random_fault(&ids);
        let f2 = chooser.random_fault(&ids);
        let load = LoadConfig {
            writers: o.writers,
            rate_per_sec: o.rate,
            duration: Duration::from_secs(o.round_secs),
        };
        let tag = format!("soak-r{round}");
        // The round's own seed is derived so the in-round nemesis timing also varies.
        let round_seed = o.seed.wrapping_add(round as u64);
        let outcome = run_gauntlet(
            o.nodes,
            &tag,
            round_seed,
            load,
            vec![f1.clone(), f2.clone()],
        )
        .await
        .unwrap_or_else(|e| {
            eprintln!("round {round}: gauntlet setup failed: {e}");
            std::process::exit(1);
        });

        let v = &outcome.verdict;
        println!(
            "round {round}/{}: {} | faults=[{}] | {}",
            o.rounds,
            if v.passed { "PASS" } else { "FAIL" },
            outcome
                .fault_reports
                .iter()
                .map(|r| format!(
                    "{}({})",
                    r.fault,
                    if r.recovered { "ok" } else { "NO-LEADER" }
                ))
                .collect::<Vec<_>>()
                .join(", "),
            v.summary
        );
        if !v.passed {
            eprintln!("ROUND {round} FOUND A BUG:");
            for f in &v.failures {
                eprintln!("  - {f}");
            }
            std::process::exit(1);
        }
    }
    println!(
        "nemesis soak: all {} rounds PASSED — no lost write, no split-brain",
        o.rounds
    );
}
