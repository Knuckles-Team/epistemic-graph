//! Statements (UQL-11): version pragma, EXPLAIN/PROFILE, LET/FROM/JOIN DAGs.

use crate::uql::print::{dag_to_uql, statement_to_uql};
use crate::uql::{parse, parse_statement, Body, DagNode, Mode, Params, UqlCode};
use eg_types::wire::Op;

fn stmt(src: &str) -> crate::uql::Statement {
    parse_statement(src, &Params::new()).unwrap()
}

#[test]
fn version_pragma_and_modes() {
    let s = stmt("UQL 1; EXPLAIN MATCH (:Doc) |> LIMIT 2");
    assert_eq!((s.version, s.mode), (1, Mode::Explain));
    assert_eq!(stmt("PROFILE MATCH ()").mode, Mode::Profile);
    // `EXPLAIN BELIEF` is the epistemic stage, not the EXPLAIN mode.
    let belief = parse_statement("EXPLAIN BELIEF 'c1'", &Params::new());
    if cfg!(feature = "epistemic") {
        assert_eq!(belief.unwrap().mode, Mode::Run);
    } else {
        assert_eq!(belief.unwrap_err().code, UqlCode::FeatureNotInBuild);
    }
    let v2 = parse_statement("UQL 2; MATCH ()", &Params::new()).unwrap_err();
    assert_eq!(v2.code, UqlCode::UnsupportedVersion);
    assert_eq!(
        parse("EXPLAIN MATCH ()").unwrap_err().code,
        UqlCode::StatementNotPipeline
    );
}

#[test]
fn let_from_join_builds_a_dag_in_first_reference_order() {
    let s = stmt(
        "LET docs = MATCH (:Doc) WHERE year > 2020;\n\
         LET cited = FROM docs |> TRAVERSE -[:CITES]->;\n\
         JOIN docs, cited |> LIMIT 5",
    );
    let Body::Dag(nodes) = s.body else {
        panic!("expected a DAG")
    };
    assert_eq!(nodes.len(), 4);
    assert_eq!(nodes[0].inputs, Vec::<usize>::new());
    assert_eq!(nodes[1].inputs, vec![0]);
    assert!(matches!(nodes[2].op, Op::Traverse { .. }));
    assert_eq!(nodes[2].inputs, vec![1]);
    assert_eq!(
        nodes[3],
        DagNode {
            op: Op::Limit { k: 5 },
            inputs: vec![1, 2]
        }
    );
    // … and the printer reproduces the program exactly.
    let text = dag_to_uql(&nodes).unwrap();
    assert_eq!(stmt(&text).body, Body::Dag(nodes));
}

#[test]
fn binding_errors_are_typed() {
    let unknown = parse_statement("FROM nope |> LIMIT 1", &Params::new()).unwrap_err();
    assert_eq!(unknown.code, UqlCode::UnknownBinding);
    let dup = parse_statement("LET a = MATCH (); LET a = MATCH (); FROM a", &Params::new());
    assert_eq!(dup.unwrap_err().code, UqlCode::DuplicateBinding);
    let unused = parse_statement("LET a = MATCH (); MATCH (:X)", &Params::new());
    assert_eq!(unused.unwrap_err().code, UqlCode::UnusedBinding);
    let join_needs_stage = parse_statement(
        "LET a = MATCH (); LET b = MATCH (); JOIN a, b",
        &Params::new(),
    );
    assert_eq!(join_needs_stage.unwrap_err().code, UqlCode::UnexpectedToken);
}

#[cfg(feature = "text")]
#[test]
fn fuse_inlines_named_bindings_as_branches() {
    let s = stmt("LET v = RANK BY ~[1, 0]; LET t = TEXT 'q'; MATCH (:Doc) |> FUSE (v, t)");
    let Body::Pipeline(plan) = &s.body else {
        panic!("FUSE-only bindings keep the program linear")
    };
    assert_eq!(
        plan.ops[1],
        Op::FuseRrf {
            branches: vec![
                vec![Op::Rank {
                    query: vec![1.0, 0.0]
                }],
                vec![Op::RankText { query: "q".into() }]
            ],
            k: 0.0
        }
    );
    assert_eq!(
        statement_to_uql(&s).map(|t| stmt(&t).body),
        Ok(s.body.clone())
    );
}

#[test]
fn a_transform_at_the_head_warns() {
    let s = stmt("LIMIT 5");
    assert_eq!(s.warnings.len(), 1);
    assert!(stmt("MATCH () |> LIMIT 5").warnings.is_empty());
}
