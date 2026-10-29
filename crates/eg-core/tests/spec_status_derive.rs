//! Requirement state derived from the changeset log (MEDULLA-SDLC-R005.2).

use eg_core::lifecycle::derive::TestBinding;
use eg_core::lifecycle::status::derive_spec_status;
use eg_types::lifecycle::{SpecRowState, SpecState};
use eg_types::source_changeset::{Changeset, Trailers};

fn cs(id: &str, spec: &[&str], withdraw: &[&str]) -> Changeset {
    Changeset {
        id: id.into(),
        repo: "medulla".into(),
        parents: vec![],
        author_principal: "a".into(),
        message: String::new(),
        changed_paths: vec![],
        created_at: 0,
        trailers: Trailers {
            spec_ids: spec.iter().map(|s| s.to_string()).collect(),
            withdraw: withdraw.iter().map(|s| s.to_string()).collect(),
            ..Trailers::default()
        },
    }
}

fn bind(requirement: &str, test: &str) -> TestBinding {
    TestBinding {
        requirement: requirement.into(),
        test: test.into(),
    }
}

fn ids(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

fn row<'a>(rows: &'a [SpecRowState], id: &str) -> &'a SpecRowState {
    rows.iter().find(|r| r.id == id).unwrap()
}

// spec: MEDULLA-SDLC-R005.2
#[test]
fn rules_match_the_standard() {
    let all = ids(&["R1", "R2", "R3", "R4", "R5", "R5.1", "R5.2", "R6"]);
    let chain = [
        cs("c1", &["R1", "R2", "R5.1"], &[]),
        cs("c2", &["R3", "R5.2", "R6"], &[]),
        cs("c3", &[], &["R3", "R6"]),
        cs("c4", &["R6"], &[]),
    ];
    let bindings = [
        bind("R1", "t1"),
        bind("R1", "t1b"),
        bind("R3", "t3"),
        bind("R5.1", "t51"),
        bind("R5.2", "t52"),
        bind("R6", "t6"),
        bind("R4", "t4"),
    ];
    let answer = derive_spec_status("medulla", "c4", &all, &chain, &bindings);
    assert_eq!(
        (answer.repo.as_str(), answer.rev.as_str()),
        ("medulla", "c4")
    );
    let rows = &answer.rows;

    // Each state: VERIFIED (landed + bound), LANDED (landed, no test), SPECIFIED (not cited).
    assert_eq!(row(rows, "R1").state, SpecState::Verified);
    assert_eq!(row(rows, "R1").landed_by, ["c1"]);
    assert_eq!(row(rows, "R1").verified_by, ["t1", "t1b"]);
    assert_eq!(row(rows, "R2").state, SpecState::Landed);
    assert!(row(rows, "R2").verified_by.is_empty());
    // A binding without a landing never verifies.
    assert_eq!(row(rows, "R4").state, SpecState::Specified);
    assert!(row(rows, "R4").verified_by.is_empty());

    // Withdrawal removes the evidence; a later landing restores it.
    assert_eq!(row(rows, "R3").state, SpecState::Specified);
    assert!(row(rows, "R3").withdrawn && row(rows, "R3").landed_by.is_empty());
    assert!(row(rows, "R3").verified_by.is_empty());
    assert_eq!(row(rows, "R6").state, SpecState::Verified);
    assert_eq!(row(rows, "R6").landed_by, ["c4"]);
    assert!(!row(rows, "R6").withdrawn);

    // A rollup takes the lowest child state: R5.1 VERIFIED, R5.2 VERIFIED.
    assert_eq!(row(rows, "R5").state, SpecState::Verified);
    assert_eq!(row(rows, "R5").landed_by, ["c1", "c2"]);
    let lower = derive_spec_status("medulla", "c4", &all, &chain, &bindings[..4]);
    assert_eq!(row(&lower.rows, "R5.2").state, SpecState::Landed);
    assert_eq!(row(&lower.rows, "R5").state, SpecState::Landed);
    let none = derive_spec_status("medulla", "c0", &all, &[], &[]);
    assert!(none.rows.iter().all(|r| r.state == SpecState::Specified));

    // Rows keep the order of the requirement list.
    let order: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(order, all.iter().map(String::as_str).collect::<Vec<_>>());
}
