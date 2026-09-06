//! Positive and negative proofs for the kernel-backed ANN code tier.
//!
//! Layer (b) of RF-RULING-007's replacement for the deleted domain guard: the
//! guard said semantic mutations "remain unserved"; what actually protects the
//! semantic owner tables is that a handle bound to one `(tenant, binding,
//! generation)` cannot admit, write or retire another's rows -- and that a read
//! creates no authority at all.

use std::sync::Arc;

use super::rows::{BoundBindingRows, BoundCodeRows};
use super::{generation_identity, GenerationRetirement, SemanticCodeStore};
use crate::compute::semantic::SemanticStore;
use crate::test_scope_grant::{TestScopeVerifier, TEST_PRINCIPAL, TEST_PROOF};
use eg_storage::{OwnerLayout, ANN_CODES, SEMANTIC_POINTERS};

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

/// Byte fingerprint of every file under `dir`, so "this call wrote nothing" is
/// an assertion about the durable state rather than about the absence of an
/// error.
fn store_fingerprint(dir: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_file() {
            out.push((
                path.file_name().unwrap().to_string_lossy().to_string(),
                std::fs::read(&path).unwrap(),
            ));
        }
    }
    out.sort_by(|left, right| left.0.cmp(&right.0));
    out
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

    assert!(codes.read_live().unwrap().is_none(), "nothing activated yet");
    codes.activate(1, &image).unwrap();
    let (live, restored) = codes.read_live().unwrap().expect("generation 1 must serve");
    assert_eq!(live, 1);
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

    // Re-activating the identical image is a no-op decided INSIDE the admitted
    // write, not a second activation and not a conflict.
    codes.activate(1, &image).unwrap();
    assert_eq!(codes.read_live().unwrap().unwrap().1, image);
    let _ = std::fs::remove_dir_all(&dir);
}

/// P0-1. A read is a read: it binds no scope, bootstraps no ledger, and
/// therefore cannot resurrect a generation that was retired.
#[test]
fn reads_write_nothing_and_cannot_resurrect_a_retired_generation() {
    let dir = tmp_dir("read-only");
    let codes = open_store(&dir);
    let (store, _) = warmed_store(0);
    let image = store.export_generation().unwrap();
    codes.activate(7, &image).unwrap();

    // Probing generations that were never activated must not grow the store.
    let before = store_fingerprint(&dir);
    for generation in [1u64, 2, 3, 999_999] {
        assert!(codes.read_generation(generation).unwrap().is_none());
    }
    assert!(codes.read_live().unwrap().is_some());
    assert_eq!(
        store_fingerprint(&dir),
        before,
        "a read of an unbound generation must write nothing"
    );

    // After retirement the generation stays gone however often it is read.
    codes.retire(7).unwrap();
    assert!(codes.read_live().unwrap().is_none());
    let after_retire = store_fingerprint(&dir);
    for _ in 0..3 {
        assert!(
            codes.read_generation(7).unwrap().is_none(),
            "a read must not resurrect a retired generation"
        );
    }
    assert_eq!(store_fingerprint(&dir), after_retire);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Plan L1. The previous durable form wrote the flat keys `meta`/`codes`/
/// `refine`, so building generation `N+1` destroyed the generation `N` that was
/// still serving. `(tenant, binding, generation, part)` is what makes them
/// coexist, and `purge_scope_with` is what retires one of them -- over a
/// bounded prefix range, leaving every other generation byte-for-byte intact.
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
    assert_eq!(codes.read_generation(1).unwrap().unwrap(), one);
    assert_eq!(codes.read_generation(2).unwrap().unwrap(), two);
    // Exactly one live generation, and it is the newest activated.
    assert_eq!(codes.live_generation().unwrap(), Some(2));

    codes.retire(1).unwrap();
    assert!(
        codes.read_generation(1).unwrap().is_none(),
        "the retired generation's rows must be gone"
    );
    let survivor = codes
        .read_generation(2)
        .unwrap()
        .expect("the serving generation must survive its predecessor's retirement");
    assert_eq!(survivor, two, "byte-for-byte intact");
    assert_eq!(codes.live_generation().unwrap(), Some(2));
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

    let one = codes.bind_for_write(1).unwrap();
    let two = codes.bind_for_write(2).unwrap();
    let foreign = codes.generation_batch(&two, 2, "deadbeef", 0);
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
    assert_eq!(codes.read_generation(1).unwrap().unwrap(), image);
    let _ = std::fs::remove_dir_all(&dir);
}

/// P1-1. `AdmittedOwnerWrite::open_table` returns a RAW `redb::Table` -- the
/// kernel bounds an owner write to its layout, not to its row keys, because
/// owner tables in general carry no scope component. For the semantic tables
/// the key does carry it, so the row-key ACL is this owner's obligation and the
/// bound accessors are the only writer path in this crate.
#[test]
fn a_bound_accessor_refuses_another_tenants_or_generations_rows() {
    let dir = tmp_dir("row-acl");
    let codes = open_store(&dir);
    let (store, _) = warmed_store(0);
    let image = store.export_generation().unwrap();
    codes.activate(1, &image).unwrap();

    let owner = codes.bind_for_write(1).unwrap();
    let expected =
        eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let batch = codes.generation_batch(&owner, 1, "acl-probe", expected);
    let write = codes.mutations.open_write(&owner).unwrap();
    assert!(matches!(
        write.begin_maintenance(&batch).unwrap(),
        eg_transaction::Begin::Apply { .. }
    ));
    let rows = write.owner_rows(&owner, &batch).unwrap();
    {
        let mut bound = BoundCodeRows::new(rows.open_table(ANN_CODES).unwrap(), TENANT, BINDING, 1);
        // The kernel would accept every one of these; the accessor does not.
        for foreign in [
            ("tenant-b", BINDING, 1u64, "meta"),
            (TENANT, "binding-z", 1, "meta"),
            (TENANT, BINDING, 2, "meta"),
        ] {
            let refused = bound
                .insert(foreign, b"forged")
                .expect_err("a foreign key must be refused");
            assert!(
                refused.to_string().contains("another generation's rows"),
                "{refused}"
            );
        }
        // Its own key is accepted, so the refusal is an ACL and not a stub.
        bound.insert((TENANT, BINDING, 1, "meta"), b"own").unwrap();

        let mut binding_rows = BoundBindingRows::new(
            rows.open_table(SEMANTIC_POINTERS).unwrap(),
            TENANT,
            BINDING,
        );
        let refused = binding_rows
            .insert(("tenant-b", BINDING), b"forged")
            .expect_err("a foreign binding key must be refused");
        assert!(
            refused.to_string().contains("another binding's rows"),
            "{refused}"
        );
    }
    rows.finish_owner().unwrap();
    write.abort().unwrap();
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

    let one = codes.bind_for_write(1).unwrap();
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
    assert!(
        error.contains("does not describe the scope being purged"),
        "{error}"
    );

    assert_eq!(codes.read_generation(1).unwrap().unwrap(), image);
    assert_eq!(codes.read_generation(2).unwrap().unwrap(), image);
    let _ = std::fs::remove_dir_all(&dir);
}

/// P1-4. The binding carries a durable model identity and width, written at
/// first activation and compared thereafter: a generation of a different model
/// or a different dimension is refused, not silently activated.
#[test]
fn a_generation_of_another_width_is_refused_against_the_binding_authority() {
    let dir = tmp_dir("authority");
    let codes = open_store(&dir);
    let (narrow, _) = warmed_store(0);
    let image = narrow.export_generation().unwrap();
    codes.activate(1, &image).unwrap();
    let (dimensions, _) = image.identity().unwrap();

    let mut wider = SemanticStore::new();
    let n = crate::compute::semantic_ann::ANN_BUILD_THRESHOLD + 50;
    for i in 0..n {
        let value = (i % 97) as f32 / 97.0;
        wider
            .add_embedding(format!("w{i}"), vec![value; dimensions * 2])
            .unwrap();
    }
    wider.warm("test");
    let wide_image = wider.export_generation().unwrap();
    assert_ne!(wide_image.identity().unwrap().0, dimensions);

    let refused = codes
        .activate(2, &wide_image)
        .expect_err("a generation of another width must be refused");
    assert!(refused.to_string().contains("dimensions"), "{refused}");
    assert!(
        codes.read_generation(2).unwrap().is_none(),
        "a refused activation must leave no rows"
    );
    assert_eq!(codes.live_generation().unwrap(), Some(1));
    let _ = std::fs::remove_dir_all(&dir);
}

/// P1-3. Two activations of one generation that both decided against the same
/// version: the first commits, the second is refused by the ledger rather than
/// overwriting it. Deterministic rather than threaded -- the race is the stale
/// version expectation, and this reproduces exactly that state.
#[test]
fn a_second_activation_racing_the_same_version_fails_closed() {
    let dir = tmp_dir("race");
    let codes = open_store(&dir);
    let (first, _) = warmed_store(0);
    let (second, _) = warmed_store(7);
    let one = first.export_generation().unwrap();
    let two = second.export_generation().unwrap();

    let owner = codes.bind_for_write(1).unwrap();
    let stale_version =
        eg_transaction::version(&codes.kernel.read_scope(&owner).unwrap()).unwrap();
    let loser = codes.generation_batch(&owner, 1, "loser-digest", stale_version);

    // The winner commits and moves the scope version.
    codes.activate(1, &one).unwrap();
    assert_eq!(codes.read_generation(1).unwrap().unwrap(), one);

    // The loser was built against the version the winner consumed.
    let error = codes
        .mutations
        .admit_maintenance(&owner, &loser)
        .map(|_| ())
        .expect_err("an activation racing a committed one must fail closed");
    assert!(error.contains("STALE_VERSION"), "{error}");
    assert_eq!(
        codes.read_generation(1).unwrap().unwrap(),
        one,
        "the loser must not have overwritten the winner"
    );
    assert_ne!(one, two);
    let _ = std::fs::remove_dir_all(&dir);
}
