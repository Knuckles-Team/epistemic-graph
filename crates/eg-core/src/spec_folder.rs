//! Spec-folder file classification (EG-ORCH-VOCAB-R012.1, R012.2, R012.4,
//! R012.5).
//!
//! Each rule is a named, replaceable default. A file that matches no rule is
//! refused with a typed reason; the indexer then stores a plain `:Document`
//! and never guesses a kind.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The closed set of spec folder node kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpecNodeKind {
    Task,
    Goal,
    Document,
    Memory,
    Policy,
}

impl SpecNodeKind {
    /// Every kind, in declaration order.
    pub const ALL: [SpecNodeKind; 5] = [
        Self::Task,
        Self::Goal,
        Self::Document,
        Self::Memory,
        Self::Policy,
    ];

    /// The ontology class this kind is written as. Documents reuse
    /// `:Specification`.
    pub const fn class_iri(self) -> &'static str {
        match self {
            Self::Task => "SpecTask",
            Self::Goal => "SpecGoal",
            Self::Document => "Specification",
            Self::Memory => "SpecMemory",
            Self::Policy => "SpecPolicy",
        }
    }
}

/// Why a file was not given a kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "refusal", rename_all = "snake_case")]
pub enum SpecFolderRefusal {
    /// No default rule matched the file.
    UnknownKind { relative_path: String },
}

/// A decided kind and the rule that decided it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Classified {
    pub kind: SpecNodeKind,
    pub rule: &'static str,
}

/// The front matter keys of a file, already parsed by the caller.
pub type FrontMatter = BTreeMap<String, String>;

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn constitution_file(path: &str, _front: &FrontMatter) -> Option<SpecNodeKind> {
    let name = file_name(path).to_ascii_lowercase();
    (name.starts_with("constitution.")).then_some(SpecNodeKind::Policy)
}

fn memory_folder(path: &str, _front: &FrontMatter) -> Option<SpecNodeKind> {
    let mut parts: Vec<&str> = path.split('/').collect();
    parts.pop();
    parts
        .iter()
        .any(|part| part.eq_ignore_ascii_case("memory"))
        .then_some(SpecNodeKind::Memory)
}

fn tasks_file(path: &str, _front: &FrontMatter) -> Option<SpecNodeKind> {
    (file_name(path) == "tasks.md").then_some(SpecNodeKind::Task)
}

fn document_file(path: &str, _front: &FrontMatter) -> Option<SpecNodeKind> {
    matches!(file_name(path), "spec.md" | "plan.md").then_some(SpecNodeKind::Document)
}

fn goal_line(_path: &str, front: &FrontMatter) -> Option<SpecNodeKind> {
    front
        .get("goal")
        .is_some_and(|goal| !goal.trim().is_empty())
        .then_some(SpecNodeKind::Goal)
}

type Rule = (&'static str, fn(&str, &FrontMatter) -> Option<SpecNodeKind>);

/// The default rules, in precedence order.
const DEFAULT_RULES: [Rule; 5] = [
    ("constitution_file", constitution_file),
    ("memory_folder", memory_folder),
    ("tasks_file", tasks_file),
    ("document_file", document_file),
    ("goal_line", goal_line),
];

/// Classify one file of a spec folder by `relative_path` and `front_matter`.
pub fn classify_spec_file(
    relative_path: &str,
    front_matter: &FrontMatter,
) -> Result<Classified, SpecFolderRefusal> {
    DEFAULT_RULES
        .iter()
        .find_map(|(rule, check)| {
            check(relative_path, front_matter).map(|kind| Classified { kind, rule })
        })
        .ok_or_else(|| SpecFolderRefusal::UnknownKind {
            relative_path: relative_path.to_string(),
        })
}

/// The front matter of a markdown file: `key: value` lines between two `---`
/// fences at the top. Anything else yields no keys.
pub fn parse_front_matter(source: &[u8]) -> FrontMatter {
    let text = String::from_utf8_lossy(source);
    let mut lines = text.lines();
    let mut out = FrontMatter::new();
    if lines.next().map(str::trim) != Some("---") {
        return out;
    }
    for line in lines {
        if line.trim() == "---" {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            out.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    out
}

/// The node a file becomes: the classified class, or a plain `:Document` for
/// a refused file. Carries the deciding rule and the source path (R012.5).
pub fn spec_file_node(relative_path: &str, source: &[u8]) -> (String, BTreeMap<String, String>) {
    let front = parse_front_matter(source);
    let mut properties = BTreeMap::new();
    properties.insert("source_path".to_string(), relative_path.to_string());
    let node_type = match classify_spec_file(relative_path, &front) {
        Ok(classified) => {
            properties.insert("classifiedByRule".to_string(), classified.rule.to_string());
            classified.kind.class_iri()
        }
        Err(SpecFolderRefusal::UnknownKind { .. }) => "Document",
    };
    (node_type.to_string(), properties)
}
