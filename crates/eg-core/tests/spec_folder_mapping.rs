//! EG-ORCH-VOCAB-R012.1, R012.2, R012.4 and R012.5: spec folder files get a
//! closed node kind from named default rules, or a typed refusal.

use eg_core::spec_folder::{
    classify_spec_file, parse_front_matter, spec_file_node, FrontMatter, SpecFolderRefusal,
    SpecNodeKind,
};

const SDD: &str = include_str!("../ontology/sdd-v1.ttl");

fn none() -> FrontMatter {
    FrontMatter::new()
}

// spec: EG-ORCH-VOCAB-R012.1
#[test]
fn node_kinds_are_closed_and_declared() {
    assert_eq!(SpecNodeKind::ALL.len(), 5);
    for kind in SpecNodeKind::ALL {
        let class = format!("\n:{} a owl:Class", kind.class_iri());
        assert!(SDD.contains(&class), "sdd-v1.ttl declares {}", kind.class_iri());
    }
    assert_eq!(SpecNodeKind::Document.class_iri(), "Specification");
    assert!(serde_json::from_str::<SpecNodeKind>("\"epic\"").is_err());
}

// spec: EG-ORCH-VOCAB-R012.2
#[test]
fn default_rules_classify_each_file_kind() {
    let kind = |path: &str, front: &FrontMatter| classify_spec_file(path, front).unwrap().kind;
    assert_eq!(kind("specs/a/tasks.md", &none()), SpecNodeKind::Task);
    assert_eq!(kind("specs/a/spec.md", &none()), SpecNodeKind::Document);
    assert_eq!(kind("specs/a/plan.md", &none()), SpecNodeKind::Document);
    assert_eq!(kind("specs/constitution.md", &none()), SpecNodeKind::Policy);
    assert_eq!(kind("specs/memory/notes.md", &none()), SpecNodeKind::Memory);
    let goal = parse_front_matter(b"---\ngoal: ship the thing\n---\nbody");
    assert_eq!(kind("specs/a/objective.md", &goal), SpecNodeKind::Goal);
}

// spec: EG-ORCH-VOCAB-R012.4
#[test]
fn unknown_file_is_refused_not_guessed() {
    let refusal = classify_spec_file("specs/a/notes.md", &none()).unwrap_err();
    assert_eq!(
        refusal,
        SpecFolderRefusal::UnknownKind {
            relative_path: "specs/a/notes.md".to_string()
        }
    );
    let (node_type, properties) = spec_file_node("specs/a/notes.md", b"no front matter");
    assert_eq!(node_type, "Document");
    assert!(!properties.contains_key("classifiedByRule"));
}

#[test]
fn node_records_deciding_rule() {
    let (node_type, properties) = spec_file_node("specs/a/tasks.md", b"- [ ] one");
    assert_eq!(node_type, "SpecTask");
    assert_eq!(properties["classifiedByRule"], "tasks_file");
    assert_eq!(properties["source_path"], "specs/a/tasks.md");
    assert!(SDD.contains(":classifiedByRule a owl:DatatypeProperty"));
}
