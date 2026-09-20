//! Equal inputs give byte-identical certificates and digests.

use eg_compute::solve::model::Model;
use eg_compute::solve::{solve, Certificate, SolverConfig};

use super::generators::{planted_assembly, AssemblyShape};
use super::support::model;

const SHAPE: AssemblyShape = AssemblyShape {
    components: 64,
    capabilities: 32,
    models: 4,
    unknown_cost_decoys: 6,
};

/// Certificate digest of `planted_assembly(SHAPE, 7)` under the default config.
/// A change here is a change to the committed replay contract, not a refresh.
///
/// EH-310: this constant was updated once, deliberately, to track a real contract
/// change, not refreshed to make a failing test pass. FX-SOLVER's own build/test
/// cycle (its own lane run records, retained outside this repository, covering
/// `9edb38bca`..`b1d55baa1`) verified the previous value,
/// `71a64f581943d7cadfaecbcba26644c7d2fd2176ff714a9c1b840dc6659d8f4e`, against a
/// tree where `Algorithm`'s sole variant still carried the version suffix
/// RF-ADR-006 bans. The very next edit before landing — commit
/// `f795bcebb073a7a19030eb67a6e959880910da6d` (squashed into main as `4b11f6289`;
/// `git show f795bcebb0 -- crates/eg-compute/src/solve/certificate.rs` shows the
/// rename) — dropped that suffix from the variant in `certificate.rs` and its one
/// construction site in `search/status.rs` to satisfy the repo's no-version-suffix
/// policy. `Algorithm` carries `#[serde(rename_all = "snake_case")]`, so renaming the
/// variant changed its wire tag by the same three characters. That is a real,
/// intentional byte-content change to `Certificate`'s JSON encoding, so it correctly
/// moves `Certificate::digest()`. No test was re-run against that final tree before
/// it was committed, so the pre-rename value landed instead and this test has never
/// passed on any committed tree (EH-311: a gate gap that hid this exact regression,
/// not an empty one — the gate's coverage claim was false).
///
/// Verified host-portable and dependency-stable before this change, ruling out both
/// other candidate explanations: `cargo test -p eg-compute --all-features --test
/// solve determinism::the_certificate_digest_matches_the_committed_golden_vector` on
/// both R820 and R710 computed the identical new value below (rules out
/// nondeterminism — the solve path and generator use no maps, floats, clock or
/// threads); `Cargo.lock`'s package set is byte-identical across
/// `4b11f6289..be0f1acb0` and between FX-SOLVER's own branch tip `64d870556` and
/// `main` — 0 added/removed/version-changed packages, including `serde`,
/// `serde_json`, `sha2` and `digest` (rules out dependency drift).
const GOLDEN_DIGEST: &str = "1bc99ac3ed5d12211a9074b23cafde1f35f7ed90d70608f7e47c4a0429f4ea88";

#[test]
fn repeated_solves_are_byte_identical() {
    let instance = model(&planted_assembly(SHAPE, 7).spec);
    let first = solve(&instance, &SolverConfig::default());
    for _ in 0..3 {
        let again = solve(&instance, &SolverConfig::default());
        assert_eq!(again, first);
        assert_eq!(again.digest(), first.digest());
    }
}

#[test]
fn wire_round_trips_preserve_the_model_digest_and_the_certificate() {
    let instance = model(&planted_assembly(SHAPE, 7).spec);
    let decoded: Model =
        serde_json::from_str(&serde_json::to_string(&instance).expect("encode")).expect("decode");
    assert_eq!(decoded.digest(), instance.digest());
    let certificate = solve(&decoded, &SolverConfig::default());
    let wire = serde_json::to_string(&certificate).expect("encode");
    let back: Certificate = serde_json::from_str(&wire).expect("decode");
    assert_eq!(back, certificate);
    assert_eq!(
        back.digest(),
        solve(&instance, &SolverConfig::default()).digest()
    );
}

#[test]
fn the_certificate_digest_matches_the_committed_golden_vector() {
    let instance = model(&planted_assembly(SHAPE, 7).spec);
    let digest = String::from(solve(&instance, &SolverConfig::default()).digest());
    assert_eq!(digest, GOLDEN_DIGEST);
}
