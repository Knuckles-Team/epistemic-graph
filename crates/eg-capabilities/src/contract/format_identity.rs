//! Collect the storage/recovery format identities scattered across the tree.
//!
//! RF-RULING-003 makes EG the owner of "storage/recovery format identities", but they
//! live as ~30 independent `const` declarations under `src/` and `crates/` with no one
//! place naming them. This pass is that one place: it finds every declaration whose name
//! ends in a format-identity suffix and records its value and declaration sites into
//! `contract/receipt.json`, so a renamed or re-versioned identity is a receipt diff.

use std::path::Path;

/// One collected format-identity constant.
pub struct FormatIdentity {
    pub name: String,
    pub value: String,
    pub sites: Vec<String>,
}

const SUFFIXES: &[&str] = &[
    "SCHEMA_VERSION",
    "INCARNATION",
    "FORMAT_VERSION",
    "WIRE_VERSION",
];

const ROOTS: &[&str] = &["src", "crates"];

fn is_identity_name(name: &str) -> bool {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    {
        return false;
    }
    SUFFIXES.iter().any(|suffix| name.ends_with(suffix))
}

/// `const NAME: TYPE = VALUE;` — return `(name, value)` when the name is an identity.
fn parse_const(line: &str) -> Option<(String, String)> {
    let after = line.split_once("const ")?.1;
    let (name, rest) = after.split_once(':')?;
    let name = name.trim();
    if !is_identity_name(name) {
        return None;
    }
    let value = rest
        .split_once('=')
        .map(|(_, v)| v.trim().trim_end_matches(';').trim())
        .unwrap_or("")
        .to_string();
    Some((name.to_string(), value))
}

/// Every format-identity constant in the tree, sorted by name then site.
pub fn collect_format_identities(root: &Path) -> Vec<FormatIdentity> {
    let mut files = Vec::new();
    for dir in ROOTS {
        super::collect_files(&root.join(dir), &mut files);
    }
    files.retain(|p| p.extension().and_then(|e| e.to_str()) == Some("rs"));
    files.sort();
    let mut found: std::collections::BTreeMap<String, (String, Vec<String>)> = Default::default();
    for path in files {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let rel = path
            .strip_prefix(root)
            .unwrap_or(path.as_path())
            .to_string_lossy()
            .to_string();
        for (number, line) in text.lines().enumerate() {
            let Some((name, value)) = parse_const(line) else {
                continue;
            };
            let entry = found.entry(name).or_insert_with(|| (value, Vec::new()));
            entry.1.push(format!("{rel}:{}", number + 1));
        }
    }
    found
        .into_iter()
        .map(|(name, (value, sites))| FormatIdentity { name, value, sites })
        .collect()
}
