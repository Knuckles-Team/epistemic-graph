//! EH-393 — which dependency set each unified plan shape gets (or why it gets none).

use super::plan_dependency_set;
use eg_core::dep_scope::Dim;
use eg_plan::{Op, Plan, Pred};

fn deps(ops: Vec<Op>, generation: Option<u64>) -> Option<Vec<Dim>> {
    plan_dependency_set(&Plan::new(ops), generation).map(|set| set.dims().to_vec())
}

fn scan(label: &str) -> Op {
    Op::Scan {
        label: label.into(),
    }
}

fn traverse(rel: &str) -> Op {
    Op::Traverse {
        rel: rel.into(),
        min: 1,
        max: 2,
    }
}

#[test]
fn a_typed_traversal_depends_on_its_edge_type_and_row_visibility() {
    assert_eq!(
        deps(vec![scan("A"), traverse("KNOWS"), Op::Limit { k: 5 }], None),
        Some(vec![
            Dim::Label("A".into()),
            Dim::EdgeType("KNOWS".into()),
            Dim::RowVisibility,
        ])
    );
}

#[test]
fn a_filter_over_traversed_rows_reads_every_node() {
    let filter = Op::Filter {
        preds: vec![Pred::Eq {
            prop: "status".into(),
            value: "open".into(),
        }],
    };
    let dims = deps(vec![scan("A"), traverse("KNOWS"), filter.clone()], None).unwrap();
    assert!(dims.contains(&Dim::AllNodes));
    let label_only = deps(vec![scan("A"), filter], None).unwrap();
    assert_eq!(
        label_only,
        vec![Dim::Label("A".into())],
        "a filter over label-scoped rows is covered by the label"
    );
}

#[test]
fn a_vector_rank_needs_the_live_embedding_generation() {
    let rank = Op::Rank {
        query: vec![1.0, 0.0],
    };
    assert_eq!(
        deps(vec![scan("A"), rank.clone()], None),
        None,
        "without the live stamp a vector-ranked plan cannot be proven fresh"
    );
    assert_eq!(
        deps(vec![scan("A"), rank], Some(7)),
        Some(vec![Dim::Label("A".into()), Dim::EmbeddingGeneration(7)])
    );
}

#[test]
fn topology_ranks_depend_on_every_edge() {
    let dims = deps(
        vec![
            scan("A"),
            Op::RankNodeDistance {
                center: "a0".into(),
            },
        ],
        None,
    )
    .unwrap();
    assert!(dims.contains(&Dim::AllEdges) && dims.contains(&Dim::RowVisibility));
}

#[test]
fn unmodelled_reads_fall_back_to_the_version_keyed_path() {
    let as_of = Op::AsOf {
        ts: 1.0,
        axis: eg_types::wire::TimeAxis::Valid,
    };
    assert_eq!(deps(vec![scan("A"), as_of], None), None);
    let foreign = Op::Foreign { name: "crm".into() };
    assert_eq!(deps(vec![scan("A"), foreign], None), None);
    assert_eq!(
        deps(vec![Op::Rank { query: vec![1.0] }], Some(1)),
        None,
        "no graph source: not a bounded graph read"
    );
}

#[cfg(feature = "text")]
#[test]
fn a_fused_plan_depends_on_every_branch_and_lexical_ranking_falls_back() {
    let fused = Op::FuseRrf {
        branches: vec![vec![Op::Rank { query: vec![1.0] }], vec![traverse("CITES")]],
        k: 0.0,
    };
    let dims = deps(vec![scan("A"), fused], Some(3)).unwrap();
    assert!(dims.contains(&Dim::EmbeddingGeneration(3)));
    assert!(dims.contains(&Dim::EdgeType("CITES".into())));
    let lexical = Op::FuseRrf {
        branches: vec![vec![Op::RankText { query: "q".into() }]],
        k: 0.0,
    };
    assert_eq!(deps(vec![scan("A"), lexical], Some(3)), None);
}

#[test]
fn match_all_and_return_channels_keep_the_dependency_scope() {
    let project = Op::Project {
        channels: vec!["similarity".into()],
    };
    assert_eq!(
        deps(vec![Op::ScanAll {}, project], None),
        Some(vec![Dim::AllNodes]),
        "`MATCH ()` reads every node; `RETURN` passes rows through"
    );
    let decisions = Op::DecisionScan { preds: vec![] };
    assert_eq!(
        deps(vec![decisions], None),
        None,
        "the decision log is not graph state"
    );
}
