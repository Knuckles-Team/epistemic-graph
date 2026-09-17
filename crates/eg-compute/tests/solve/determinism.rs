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
const GOLDEN_DIGEST: &str = "71a64f581943d7cadfaecbcba26644c7d2fd2176ff714a9c1b840dc6659d8f4e";

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
