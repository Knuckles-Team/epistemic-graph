//! EH-393 / EH-400 — edge-type and embedding-generation dimensions, and the per-class
//! invalidation feed the clock publishes.

use super::*;

fn edge_type(t: &str) -> Dim {
    Dim::EdgeType(t.to_string())
}

fn edge_write(types: &[&str]) -> WriteFootprint {
    WriteFootprint {
        edge_changed: true,
        edge_types: types.iter().map(|t| t.to_string()).collect(),
        ..Default::default()
    }
}

fn commit(clock: &DepClock, fp: &WriteFootprint, version: u64) {
    clock.note_footprint(fp, version);
    clock.note_version_bump(version);
}

// spec: EG-FEDERATED-QUERY-R017, EG-FEDERATED-QUERY-R018
#[test]
fn traversal_survives_an_edge_write_of_another_type() {
    let clock = DepClock::new();
    let traversal = DepSet::new(vec![Dim::Label("A".into()), edge_type("KNOWS")]);
    commit(&clock, &edge_write(&["LIKES"]), 6);
    assert!(
        clock.is_valid(&traversal, 5),
        "a LIKES edge cannot change a KNOWS traversal"
    );
}

// spec: EG-FEDERATED-QUERY-R017, EG-FEDERATED-QUERY-R018
#[test]
fn traversal_is_invalidated_by_an_edge_write_of_its_type() {
    let clock = DepClock::new();
    let traversal = DepSet::new(vec![Dim::Label("A".into()), edge_type("KNOWS")]);
    commit(&clock, &edge_write(&["KNOWS"]), 6);
    assert!(!clock.is_valid(&traversal, 5));
}

#[test]
fn an_unattributed_edge_change_invalidates_every_edge_type_but_not_node_scans() {
    let clock = DepClock::new();
    let traversal = DepSet::new(vec![edge_type("KNOWS")]);
    let scan = DepSet::new(vec![Dim::Label("A".into())]);
    let fp = WriteFootprint {
        edge_changed: true,
        edge_unattributed: true,
        ..Default::default()
    };
    commit(&clock, &fp, 6);
    assert!(
        !clock.is_valid(&traversal, 5),
        "an edge change of unknown type may be a KNOWS edge"
    );
    assert!(clock.is_valid(&scan, 5), "no node dimension moved");
    assert!(!clock.is_valid(&DepSet::new(vec![Dim::AllEdges]), 5));
}

#[test]
fn embedding_generation_is_valid_only_against_the_same_live_stamp() {
    let clock = DepClock::new();
    let ranked = DepSet::new(vec![Dim::Label("A".into()), Dim::EmbeddingGeneration(41)]);
    assert!(DepProbe::new(&clock, Some(41)).is_valid(&ranked, 5));
    assert!(
        !DepProbe::new(&clock, Some(42)).is_valid(&ranked, 5),
        "a changed embedding store must invalidate a vector-ranked result"
    );
    assert!(
        !clock.is_valid(&ranked, 5),
        "a probe that cannot see the store must refuse an embedding-dependent entry"
    );
    assert!(!DepProbe::from(&clock).is_valid(&ranked, 5));
}

#[test]
fn every_touching_commit_publishes_a_class_scoped_event() {
    let clock = DepClock::new();
    let fp = WriteFootprint {
        labels: vec!["Person".into(), "Doc".into(), "Person".into()],
        node_changed: true,
        ..Default::default()
    };
    commit(&clock, &fp, 3);
    commit(&clock, &edge_write(&["CITES"]), 4);
    let page = clock.invalidation_log().read_after(0, 0);
    assert_eq!(
        page.records,
        vec![
            InvalidationRecord::classes(3, &["Doc".into(), "Person".into()], &[]),
            InvalidationRecord::classes(4, &[], &["CITES".into()]),
        ]
    );
    assert_eq!(page.head_version, 4);
}

#[test]
fn bypass_and_uncaptured_writes_publish_coarse_events() {
    let clock = DepClock::new();
    clock.note_version_bump(7);
    let uncaptured = WriteFootprint {
        node_changed: true,
        coarse_node: true,
        ..Default::default()
    };
    commit(&clock, &uncaptured, 8);
    let scopes: Vec<_> = clock
        .invalidation_log()
        .read_after(0, 0)
        .records
        .into_iter()
        .map(|record| (record.version, record.scope))
        .collect();
    assert_eq!(
        scopes,
        vec![(7, InvalidationScope::All), (8, InvalidationScope::All)]
    );
}

#[test]
fn a_vector_only_commit_publishes_nothing() {
    let clock = DepClock::new();
    commit(&clock, &WriteFootprint::default(), 3);
    assert!(clock.invalidation_log().read_after(0, 0).records.is_empty());
}
