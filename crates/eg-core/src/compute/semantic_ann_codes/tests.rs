//! Negative and positive proofs for the kernel-backed ANN code tier.
//!
//! Layer (b) of RF-RULING-007's replacement for the deleted domain guard: the
//! guard said semantic mutations "remain unserved"; what actually protects the
//! semantic owner tables is that a handle bound to one `(tenant, binding,
//! generation)` cannot write, read or retire another's rows.

use std::sync::Arc;

use super::{generation_identity, GenerationRetirement, SemanticCodeStore};
use crate::compute::semantic::SemanticStore;
use crate::test_scope_grant::{TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF};
use eg_storage::OwnerLayout;

const TENANT: &str = "native";
const BINDING: &str = "semantic-binding-a";

/// A unique temp dir per test invocation (no external dev-dep needed).
fn tmp_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "eg-semantic-codes-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ))
}

fn open_store(dir: &std::path::Path) -> SemanticCodeStore {
    SemanticCodeStore::open(
        dir,
        Arc::new(TestScopeVerifier {
            layout: OwnerLayout::SemanticIndex,
        }),
        TEST_PRINCIPAL,
        TEST_PROOF,
        TENANT,
        BINDING,
    )
    .unwrap()
}

/// A warmed semantic store with `extra` members beyond the build threshold, and
/// one query vector drawn from it.
fn warmed_store(extra: usize) -> (SemanticStore, Vec<f32>) {
    let dim = 16;
    let n = crate::compute::semantic_ann::ANN_BUILD_THRESHOLD + 50 + extra;
    let mut store = SemanticStore::new();
    let mut seed = 0x5eed_u64;
    let mut rng = || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((seed >> 33) as f32 / (1u64 << 31) as f32) - 0.5
    };
    let centers: Vec<Vec<f32>> = (0..24)
        .map(|_| (0..dim).map(|_| rng() * 2.0).collect())
        .collect();
    let mut query = Vec::new();
    for i in 0..n {
        let c = &centers[i % centers.len()];
        let v: Vec<f32> = (0..dim).map(|j| c[j] + rng() * 0.2).collect();
        if i == 100 {
            query = v.clone();
        }
        store.add_embedding(format!("n{i}"), v).unwrap();
    }
    store.warm("test");
    assert!(store.is_ready());
    (store, query)
}

#[test]
fn a_generation_round_trips_through_the_mutation_kernel() {
    let dir = tmp_dir("roundtrip");
    let codes = open_store(&dir);
    let (store, query) = warmed_store(0);
    let before = store.semantic_search(&query, 10);
    let image = store.export_generation().unwrap();

    assert!(codes.read(1).unwrap().is_none(), "nothing activated yet");
    codes.activate(1, &image).unwrap();
    let restored = codes.read(1).unwrap().expect("generation 1 must be durable");
    assert_eq!(restored, image);

    // The restored image activates a COLD store with no rebuild.
    let cold = store.clone();
    assert!(!cold.is_ready());
    cold.adopt_generation(&restored).unwrap();
    assert!(cold.is_ready());
    assert_eq!(
        before.iter().map(|r| r.0.clone()).collect::<Vec<_>>(),
        cold.semantic_search(&query, 10)
            .iter()
            .map(|r| r.0.clone())
            .collect::<Vec<_>>()
    );

    // Re-activating the identical image is a no-op, not a second activation
    // and not a conflict: `activate` reads the durable generation first.
    codes.activate(1, &image).unwrap();
    assert_eq!(codes.read(1).unwrap().unwrap(), image);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Plan L1. The previous durable form wrote the flat keys `meta`/`codes`/
/// `refine`, so building generation `N+1` destroyed the generation `N` that was
/// still serving. `(tenant, binding, generation, part)` is what makes them
/// coexist, and `purge_scope_with` is what retires one of them.
#[test]
fn two_generations_coexist_and_retiring_one_leaves_the_other_serving() {
    let dir = tmp_dir("generations");
    let codes = open_store(&dir);
    let (first, _) = warmed_store(0);
    let (second, query) = warmed_store(7);
    let one = first.export_generation().unwrap();
    let two = second.export_generation().unwrap();
    assert_ne!(one, two, "the two generations must be distinguishable");

    codes.activate(1, &one).unwrap();
    codes.activate(2, &two).unwrap();
    assert_eq!(codes.read(1).unwrap().unwrap(), one);
    assert_eq!(codes.read(2).unwrap().unwrap(), two);

    codes.retire(1).unwrap();
    assert!(
        codes.read(1).unwrap().is_none(),
        "the retired generation's rows must be gone"
    );
    let survivor = codes
        .read(2)
        .unwrap()
        .expect("the serving generation must survive its predecessor's retirement");
    assert_eq!(survivor, two);
    let cold = second.clone();
    cold.adopt_generation(&survivor).unwrap();
    assert!(!cold.semantic_search(&query, 10).is_empty());

    // A real image of the OTHER generation's member set is refused rather than
    // activated: moving activation off the filesystem did not weaken the
    // identity binding between an artifact and the arena it describes.
    assert!(second.clone().adopt_generation(&one).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

/// The fail-closed admission proof that replaces the deleted domain guard: a
/// batch naming one generation's scope cannot be admitted against another
/// generation's bound handle, even though both live in the same physical file
/// and the same owner layout.
#[test]
fn a_batch_for_another_generation_is_refused_at_admission() {
    let dir = tmp_dir("cross-binding");
    let codes = open_store(&dir);
    let (store, _) = warmed_store(0);
    let image = store.export_generation().unwrap();
    codes.activate(1, &image).unwrap();

    let one = codes.handle(1).unwrap();
    let two = codes.handle(2).unwrap();
    let foreign = codes.generation_batch(&two, 2, &image).unwrap();
    let error = codes
        .mutations
        .admit_maintenance(&one, &foreign)
        .map(|_| ())
        .expect_err("a batch for generation 2 must not be admitted against generation 1");
    assert!(
        error.contains("does not serve this scope"),
        "admission must fail closed on the scope, not incidentally: {error}"
    );

    // Generation 1's rows are untouched by the refused attempt.
    assert_eq!(codes.read(1).unwrap().unwrap(), image);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The retirement carries the generation it sweeps, so pairing it with another
/// generation's purge is refused rather than silently sweeping the wrong rows.
#[test]
fn a_retirement_describing_another_generation_is_refused() {
    let dir = tmp_dir("retirement");
    let codes = open_store(&dir);
    let (store, _) = warmed_store(0);
    let image = store.export_generation().unwrap();
    codes.activate(1, &image).unwrap();
    codes.activate(2, &image).unwrap();

    let one = codes.handle(1).unwrap();
    let identity = generation_identity(TENANT, BINDING, 1).unwrap();
    let mismatched = GenerationRetirement {
        tenant: TENANT.to_string(),
        binding: BINDING.to_string(),
        generation: 2,
    };
    let error = codes
        .mutations
        .purge_scope_with(&one, &identity, &mismatched)
        .expect_err("a retirement for generation 2 must not purge generation 1");
    assert!(error.contains("does not describe the scope being purged"), "{error}");

    assert_eq!(codes.read(1).unwrap().unwrap(), image);
    assert_eq!(codes.read(2).unwrap().unwrap(), image);
    let _ = std::fs::remove_dir_all(&dir);
}
