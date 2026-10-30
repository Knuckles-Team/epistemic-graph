use super::*;

/// Four clusters of six items in eight dimensions, jittered by a fixed rule.
fn catalog() -> Vec<Vec<f32>> {
    (0..24)
        .map(|i| {
            let cluster = i / 6;
            (0..8)
                .map(|d| {
                    let base = if d / 2 == cluster { 1.0 } else { 0.0 };
                    base + ((i * 7 + d * 3) % 11) as f32 * 0.01
                })
                .collect()
        })
        .collect()
}

fn ranking(seed: u64) -> Vec<(usize, [u8; 4])> {
    let quantizer = ResidualQuantizer::fit(&catalog(), 2, 4, seed).unwrap();
    let index = RecommendIndex::build(quantizer, catalog()).unwrap();
    let query = vec![0.9, 1.0, 0.1, 0.0, 0.0, 0.1, 0.0, 0.0];
    index
        .rank(&query, &[1, 2], 2, 4)
        .unwrap()
        .into_iter()
        .map(|r| (r.item, r.codes))
        .collect()
}

// spec: EG-DECISION-ENGINE-R175.2
#[test]
fn seeded_fixture_ranks_the_same_on_two_runs() {
    let first = ranking(17);
    assert_eq!(first, ranking(17), "same seed, same ranking and codes");
    assert_eq!(
        first.len(),
        4,
        "the query's cluster minus its two history items"
    );
    assert!(
        first.iter().all(|(item, _)| *item < 6),
        "the refined query ranks its own cluster: {first:?}"
    );
}

#[test]
fn shape_dimension_and_empty_catalog_are_refused() {
    assert_eq!(
        ResidualQuantizer::fit(&catalog(), 5, 4, 1),
        Err(RecommendError::BadShape)
    );
    assert_eq!(
        ResidualQuantizer::fit(&[], 2, 4, 1),
        Err(RecommendError::EmptyCatalog)
    );
    let quantizer = ResidualQuantizer::fit(&catalog(), 2, 4, 1).unwrap();
    assert_eq!(
        quantizer.encode(&[1.0, 2.0]),
        Err(RecommendError::DimensionMismatch {
            expected: 8,
            found: 2
        })
    );
}
