//! Deterministic spec-artifact parser (MEDULLA-SDLC-R014.1).

use std::path::PathBuf;

use medulla_compute::parser::spec_artifacts::{
    parse_spec_dir, parse_spec_file, parse_task_line, SpecArtifactRefusal, SDD_NS,
};
use medulla_types::ontology_inspection::{RdfObject, RdfTriple};

const REQUIREMENTS: &str = "# Requirements\n\n## Rows\n\n\
| ID | Requirement | Acceptance |\n|----|----|----|\n\
| `DEMO-R001` | **Parse rows.** The parser reads rows. | Test: `crates/demo/tests/a.rs::reads_rows` (L2). |\n";

const TASKS: &str = "# Tasks\n\n\
- [ ] T001 [P] [US1] DEMO-R001 Parse the rows.\n\
- [x] T002 [US2] DEMO-R002 Link the rows.\n\
- [ ] DEMO-R003 — row-only task.\n";

fn fixture_dir(name: &str, files: &[(&str, &str)]) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("spec-artifacts-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for (rel, text) in files {
        std::fs::write(dir.join(rel), text).unwrap();
    }
    dir
}

fn has(triples: &[RdfTriple], subject: &str, property: &str, lexical: &str) -> bool {
    triples.iter().any(|t| {
        t.subject == subject
            && t.predicate == format!("{SDD_NS}{property}")
            && matches!(&t.object, RdfObject::Literal { lexical: l, .. } if l == lexical)
    })
}

// spec: MEDULLA-SDLC-R014.1
#[test]
fn same_bytes_give_same_triples_without_model_calls() {
    let files = [("requirements.md", REQUIREMENTS), ("tasks.md", TASKS)];
    let first = parse_spec_dir(&fixture_dir("a", &files)).unwrap();
    let second = parse_spec_dir(&fixture_dir("b", &files)).unwrap();
    assert!(!first.is_empty());
    assert_eq!(first, second);

    let req = format!("{SDD_NS}req:DEMO-R001");
    assert!(has(&first, &req, "title", "Parse rows"));
    assert!(has(
        &first,
        &req,
        "testBinding",
        "crates/demo/tests/a.rs::reads_rows"
    ));
    let mut sorted = first.clone();
    sorted.sort_by(|a, b| (&a.subject, &a.predicate).cmp(&(&b.subject, &b.predicate)));
    let keys = |v: &[RdfTriple]| {
        v.iter()
            .map(|t| (t.subject.clone(), t.predicate.clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&first), keys(&sorted));

    let trailer = parse_spec_file("notes.md", "Spec: DEMO-R001, DEMO-R002\n").unwrap();
    let cited = trailer
        .iter()
        .filter(|t| t.predicate.ends_with("citesRequirement"))
        .count();
    assert_eq!(cited, 2);
}

// spec: MEDULLA-SDLC-R014.1
#[test]
fn task_line_fields_are_extracted() {
    let task = parse_task_line(false, "T001 [P] [US1] DEMO-R001 Parse the rows.").unwrap();
    assert_eq!(task.task_id.as_deref(), Some("T001"));
    assert!(task.parallel);
    assert_eq!(task.story.as_deref(), Some("US1"));
    assert_eq!(task.row.as_deref(), Some("DEMO-R001"));
    assert!(!task.ticked);
    assert_eq!(task.text, "Parse the rows.");

    let triples = parse_spec_file("tasks.md", TASKS).unwrap();
    let t002 = format!("{SDD_NS}file:tasks.md#T002");
    assert!(has(&triples, &t002, "ticked", "true"));
    assert!(has(&triples, &t002, "parallel", "false"));
    assert!(has(&triples, &t002, "story", "US2"));

    let refused =
        parse_spec_file("tasks.md", "# Tasks\n\n- [ ] **not a task form**\n").unwrap_err();
    assert_eq!(
        refused,
        SpecArtifactRefusal::Unparseable {
            file: "tasks.md".into(),
            line: 3
        }
    );
    assert!(refused
        .to_string()
        .starts_with("SPEC_ARTIFACT_UNPARSEABLE: tasks.md:3"));
}
