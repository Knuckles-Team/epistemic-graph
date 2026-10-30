use super::*;

fn path(p: &str, op: PathOp) -> ChangedPath {
    let (old_blob, new_blob) = match op {
        PathOp::Add => (None, Some(format!("new-{p}"))),
        PathOp::Delete => (Some(format!("old-{p}")), None),
        _ => (Some(format!("old-{p}")), Some(format!("new-{p}"))),
    };
    ChangedPath {
        path: p.to_owned(),
        op,
        old_blob,
        new_blob,
        mode: 0o100644,
    }
}

fn submission(paths: Vec<ChangedPath>, parents: Vec<String>) -> ChangesetSubmission {
    ChangesetSubmission {
        tenant_id: "t".into(),
        repo: "r".into(),
        branch: "main".into(),
        parents,
        message: "feat: x\n\nSpec: EG-SCM-R002".into(),
        changed_paths: paths,
        run_trace_id: Some("run-1".into()),
    }
}

fn proposed(paths: &[&str]) -> SourceChangesetRecord {
    let paths = paths.iter().map(|p| path(p, PathOp::Modify)).collect();
    propose(submission(paths, vec!["base".into()]), "alice", "agent", true, 1).unwrap()
}

fn landed(id: &str, paths: &[&str]) -> LandedEntry {
    LandedEntry {
        id: id.into(),
        paths: paths.iter().map(|p| (*p).to_owned()).collect(),
    }
}

#[test]
fn propose_records_only_changed_paths_and_trailers() {
    let record = proposed(&["a.rs", "b.rs"]);
    assert_eq!(record.changeset.changed_paths.len(), 2);
    assert_eq!(record.changeset.trailers.spec_ids, ["EG-SCM-R002"]);
    assert_eq!(record.state, ChangesetState::Proposed);
    assert_eq!(record.changeset.id.len(), 64);
    assert_eq!(proposed(&["a.rs", "b.rs"]).changeset.id, record.changeset.id);
}

#[test]
fn propose_refuses_empty_bad_and_duplicate_paths() {
    let empty = propose(submission(vec![], vec![]), "a", "a", false, 0);
    assert_eq!(empty.unwrap_err(), SourceRefusal::EmptyChangeset);
    let mut bad = path("x", PathOp::Add);
    bad.old_blob = Some("o".into());
    let err = propose(submission(vec![bad], vec![]), "a", "a", false, 0).unwrap_err();
    assert_eq!(err.code(), "SOURCE_INVALID_PATH");
    let dup = vec![path("x", PathOp::Modify), path("x", PathOp::Modify)];
    let err = propose(submission(dup, vec![]), "a", "a", false, 0).unwrap_err();
    assert_eq!(err.code(), "SOURCE_INVALID_PATH");
    let escape = vec![path("../x", PathOp::Modify)];
    let err = propose(submission(escape, vec![]), "a", "a", false, 0).unwrap_err();
    assert_eq!(err.code(), "SOURCE_INVALID_PATH");
}

#[test]
fn agent_changeset_without_run_trace_refuses() {
    let mut sub = submission(vec![path("a", PathOp::Add)], vec![]);
    sub.run_trace_id = None;
    let err = propose(sub.clone(), "a", "bot", true, 0).unwrap_err();
    assert_eq!(err, SourceRefusal::RunTraceRequired);
    let human = propose(sub, "a", "a", false, 0).unwrap();
    assert_eq!(human.run_trace_id, None);
}

#[test]
fn disjoint_lands_and_overlap_conflicts_with_every_path() {
    let record = proposed(&["a.rs", "b.rs"]);
    judge_landing(&record, true, &[landed("x", &["c.rs"])]).unwrap();
    let err = judge_landing(
        &record,
        true,
        &[landed("x", &["b.rs"]), landed("y", &["a.rs", "z"])],
    )
    .unwrap_err();
    assert_eq!(err, SourceRefusal::Conflict(vec!["a.rs".into(), "b.rs".into()]));
    assert_eq!(err.to_string(), "SOURCE_CONFLICT: a.rs,b.rs");
    let stale = judge_landing(&record, false, &[]).unwrap_err();
    assert_eq!(stale, SourceRefusal::StaleBase("base".into()));
}

#[test]
fn failed_verdict_blocks_landing_and_verdicts_are_sealed() {
    let mut record = proposed(&["a.rs"]);
    let verdict = |gate: &str, verdict| GateVerdict {
        gate_id: gate.into(),
        input_digest: record.changeset.id.clone(),
        verdict,
        evidence_digest: "e".into(),
        duration_ms: 3,
        recorded_by: "ci".into(),
        recorded_at_ms: 4,
    };
    let (pass, fail) = (verdict("fmt", GateOutcome::Succeeded), verdict("tests", GateOutcome::Failed));
    seal_verdict(&mut record, pass.clone()).unwrap();
    let again = seal_verdict(&mut record, pass).unwrap_err();
    assert_eq!(again, SourceRefusal::VerdictSealed("fmt".into()));
    seal_verdict(&mut record, fail).unwrap();
    let err = judge_landing(&record, true, &[]).unwrap_err();
    assert_eq!(err, SourceRefusal::GateFailed("tests".into()));
}

#[test]
fn revert_inverts_paths_and_refuses_dependents() {
    let mut target = propose(
        submission(vec![path("a", PathOp::Add), path("b", PathOp::Modify)], vec![]),
        "a",
        "a",
        false,
        0,
    )
    .unwrap();
    let not_landed = build_revert(&target, "h", &[], "r".into(), "a", "a", 1);
    assert_eq!(not_landed.unwrap_err().code(), "SOURCE_NOT_LANDED");
    target.state = ChangesetState::Landed;
    let revert = build_revert(&target, "h", &[landed("x", &["c"])], "r".into(), "a", "a", 1)
        .unwrap();
    assert_eq!(revert.revert_of.as_deref(), Some(target.changeset.id.as_str()));
    assert_eq!(revert.changeset.parents, ["h"]);
    assert_eq!(revert.changeset.changed_paths[0].op, PathOp::Delete);
    assert_eq!(revert.changeset.changed_paths[0].old_blob.as_deref(), Some("new-a"));
    assert_eq!(revert.changeset.changed_paths[1].new_blob.as_deref(), Some("old-b"));
    let err = build_revert(&target, "h", &[landed("x", &["b"])], "r".into(), "a", "a", 1);
    assert_eq!(err.unwrap_err(), SourceRefusal::RevertDependent(vec!["x".into()]));
}
