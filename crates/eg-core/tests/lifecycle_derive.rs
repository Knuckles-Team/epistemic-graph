//! Lifecycle edge derivation (EG-SDLC-R001.2, R001.4, R001.5).

use std::collections::BTreeMap;

use eg_core::lifecycle::derive::{
    derive_lifecycle_edges, CodeGraphView, Culprit, ReleaseHead, Span, SymbolFact, TestBinding,
    TestCall, RULE_VERSION,
};
use eg_core::lifecycle::store::LifecycleStore;
use eg_types::lifecycle::{LifecycleEdgeKind as E, LifecycleKind as K};
use eg_types::source_changeset::{ChangedPath, Changeset, PathOp, Trailers};

fn changeset(id: &str, paths: &[&str], trailers: Trailers) -> Changeset {
    Changeset {
        id: id.into(),
        repo: "medulla".into(),
        parents: vec![],
        author_principal: "alice".into(),
        message: "m".into(),
        changed_paths: paths
            .iter()
            .map(|p| ChangedPath {
                path: (*p).into(),
                op: PathOp::Modify,
                old_blob: None,
                new_blob: None,
                mode: 0o644,
            })
            .collect(),
        created_at: 1,
        trailers,
    }
}

fn symbol(id: &str, path: &str, start: u32, end: u32) -> SymbolFact {
    SymbolFact {
        id: id.into(),
        path: path.into(),
        span: Span { start, end },
    }
}

fn triples(edges: &[eg_core::lifecycle::derive::DerivedEdge]) -> Vec<(E, &str, &str, &str)> {
    edges
        .iter()
        .map(|e| (e.kind, e.from.as_str(), e.to.as_str(), e.rule.as_str()))
        .collect()
}

fn full_view() -> CodeGraphView {
    CodeGraphView {
        symbols: vec![
            symbol("sym::f", "a.rs", 0, 50),
            symbol("sym::g", "a.rs", 60, 90),
            symbol("sym::h", "b.rs", 0, 10),
        ],
        changed_spans: BTreeMap::from([("a.rs".to_string(), vec![Span { start: 40, end: 70 }])]),
        on_default_branch: true,
        test_bindings: vec![TestBinding {
            requirement: "EG-X-R001".into(),
            test: "test::t1".into(),
        }],
        test_calls: vec![TestCall {
            test: "test::t1".into(),
            symbol: "sym::f".into(),
        }],
        chain: vec!["c0".into(), "c1".into(), "c2".into()],
        releases: vec![ReleaseHead {
            release: "medulla@1.0".into(),
            head: "c2".into(),
            previous_head: Some("c0".into()),
        }],
        culprits: vec![Culprit {
            defect: "def-1".into(),
            changeset: "c1".into(),
        }],
    }
}

// spec: EG-SDLC-R001.2
#[test]
fn rules_emit_expected_edges() {
    let trailers = Trailers {
        spec_ids: vec!["EG-X-R001".into()],
        fixes: vec!["def-2".into()],
        ..Trailers::default()
    };
    let cs = changeset("c1", &["a.rs"], trailers);
    let edges = derive_lifecycle_edges(&cs, &full_view());
    let got = triples(&edges);
    let expected = [
        (E::Changes, "c1", "sym::f", "changes.span_overlap"),
        (E::Changes, "c1", "sym::g", "changes.span_overlap"),
        (E::CitesRequirement, "c1", "EG-X-R001", "cites.trailer"),
        (
            E::Implements,
            "sym::f",
            "EG-X-R001",
            "implements.cited_change",
        ),
        (
            E::Implements,
            "sym::g",
            "EG-X-R001",
            "implements.cited_change",
        ),
        (E::VerifiedBy, "EG-X-R001", "test::t1", "verified.binding"),
        (E::Exercises, "test::t1", "sym::f", "exercises.call_graph"),
        (E::FixedBy, "def-2", "c1", "fixed.trailer"),
        (E::IntroducedBy, "def-1", "c1", "introduced.bisect"),
        (E::Contains, "medulla@1.0", "c1", "contains.release_range"),
    ];
    let mut want: Vec<_> = expected.to_vec();
    want.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
    assert_eq!(got, want);
    assert!(edges
        .iter()
        .all(|e| e.rule_version == RULE_VERSION && e.input_digests.len() == 2));

    // c2 sits in the (c0, c2] release range; c1 does too, c0 does not.
    let in_range = |id: &str| {
        let cs = changeset(id, &[], Trailers::default());
        derive_lifecycle_edges(&cs, &full_view())
            .iter()
            .any(|e| e.kind == E::Contains && e.to == id)
    };
    assert!(in_range("c1") && in_range("c2") && !in_range("c0"));

    // Off the default branch nothing implements; an untouched path changes nothing.
    let mut off = full_view();
    off.on_default_branch = false;
    let cs_b = changeset("c1", &["b.rs"], Trailers::default());
    let off_edges = derive_lifecycle_edges(&cs_b, &off);
    assert!(!off_edges
        .iter()
        .any(|e| matches!(e.kind, E::Implements | E::Changes)));

    // Deterministic: equal inputs, equal edges.
    assert_eq!(derive_lifecycle_edges(&cs, &full_view()), edges);
}

fn seeded_store() -> LifecycleStore {
    let mut store = LifecycleStore::default();
    for (id, kind) in [
        ("sym::f", K::CodeSymbol),
        ("sym::g", K::CodeSymbol),
        ("sym::h", K::CodeSymbol),
        ("EG-X-R001", K::Requirement),
        ("test::t1", K::TestCase),
        ("def-1", K::Defect),
        ("medulla@1.0", K::Release),
        ("dep-1", K::Deployment),
        ("gv-1", K::GateVerdict),
    ] {
        store.add_node(id, kind);
    }
    store
}

// spec: EG-SDLC-R001.4
#[test]
fn manual_edge_write_is_refused() {
    let mut store = seeded_store();
    let before = store.clone();
    for kind in E::ALL.into_iter().filter(|k| k.is_derived()) {
        let err = store.write_edge(kind, "sym::f", "EG-X-R001").unwrap_err();
        assert_eq!(err.code(), "LIFECYCLE_EDGE_DERIVED_ONLY");
    }
    let err = store
        .write_edge(E::Deploys, "dep-1", "nowhere")
        .unwrap_err();
    assert_eq!(err.code(), "LIFECYCLE_ENDPOINT_UNKNOWN");
    assert_eq!(store, before);

    // A derived edge to an absent endpoint refuses and writes nothing.
    let trailers = Trailers {
        spec_ids: vec!["EG-NOT-THERE".into()],
        ..Trailers::default()
    };
    let cs = changeset("c1", &["a.rs"], trailers);
    let err = store.accept_changeset(&cs, &full_view()).unwrap_err();
    assert_eq!(err.code(), "LIFECYCLE_ENDPOINT_UNKNOWN");
    assert_eq!(store, before);

    // A non-derived kind between known nodes is a legal direct write.
    store
        .write_edge(E::Deploys, "dep-1", "medulla@1.0")
        .unwrap();
    assert_eq!(store.edge_count(), 1);
}

// spec: EG-SDLC-R001.5
#[test]
fn rebuild_equals_incremental() {
    let seed = seeded_store();
    let seed_nodes = seed.nodes().clone();
    let mut store = seed;
    let mut log = Vec::new();
    for i in 0..200u32 {
        let trailers = Trailers {
            spec_ids: if i % 3 == 0 {
                vec!["EG-X-R001".into()]
            } else {
                vec![]
            },
            fixes: if i % 50 == 7 {
                vec!["def-1".into()]
            } else {
                vec![]
            },
            ..Trailers::default()
        };
        let id = format!("c{i:03}");
        let cs = changeset(&id, &["a.rs"], trailers);
        let mut view = full_view();
        view.chain = (0..=i).map(|n| format!("c{n:03}")).collect();
        view.releases = vec![ReleaseHead {
            release: "medulla@1.0".into(),
            head: "c100".into(),
            previous_head: Some("c010".into()),
        }];
        view.culprits = vec![Culprit {
            defect: "def-1".into(),
            changeset: "c042".into(),
        }];
        store.accept_changeset(&cs, &view).unwrap();
        log.push((cs, view));
    }
    assert!(store.edge_count() > 400);
    let rebuilt = LifecycleStore::rebuild(&seed_nodes, &log).unwrap();
    assert_eq!(rebuilt.edge_set_bytes(), store.edge_set_bytes());
    assert_eq!(rebuilt, store);
}
