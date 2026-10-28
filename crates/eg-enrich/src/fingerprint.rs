//! Structural fingerprints keep incremental re-indexing to real structural
//! change (EG-REPO-INGEST-R603).
//!
//! Each file has a content hash and a structure hash over its signatures.
//! Equal content is `None`, equal structure is `Cosmetic`, otherwise
//! `Structural`; only `Structural` files re-enter enrichment. A file that fails
//! to parse has no structure hash and is always `Structural`
//! (`FINGERPRINT_UNPARSEABLE`), so it is never silently skipped.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{sha256_hex, EnrichError, Id};

/// The stable code recorded for a file that failed to parse.
pub const FINGERPRINT_UNPARSEABLE: &str = "FINGERPRINT_UNPARSEABLE";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructuralFingerprint {
    pub path: Id,
    pub content_hash: String,
    /// `None` when the file failed to parse.
    pub structure_hash: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeLevel {
    None,
    Cosmetic,
    Structural,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlastRadius {
    pub symbol: Id,
    pub usages: u64,
    pub low_usage: bool,
}

/// A hierarchical outline: directory, file and symbol nodes with containment
/// edges.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeMap {
    pub nodes: BTreeSet<String>,
    pub edges: BTreeSet<(String, String)>,
}

/// Fingerprint one file from its bytes and the parser's signatures (or its
/// parse failure).
pub fn fingerprint(
    path: Id,
    content: &[u8],
    signatures: Result<&[String], &str>,
) -> StructuralFingerprint {
    StructuralFingerprint {
        path,
        content_hash: sha256_hex(content),
        structure_hash: signatures.ok().map(|s| sha256_hex(s.join("\n").as_bytes())),
    }
}

/// The change level from `old` (absent for a new file) to `new`, plus the
/// refusal recorded when the new file could not be parsed.
pub fn classify(
    old: Option<&StructuralFingerprint>,
    new: &StructuralFingerprint,
) -> (ChangeLevel, Option<EnrichError>) {
    let Some(new_structure) = &new.structure_hash else {
        return (
            ChangeLevel::Structural,
            Some(EnrichError::new(
                FINGERPRINT_UNPARSEABLE,
                format!("{} failed to parse; classed structural", new.path),
            )),
        );
    };
    let level = match old {
        Some(old) if old.content_hash == new.content_hash => ChangeLevel::None,
        Some(old) if old.structure_hash.as_ref() == Some(new_structure) => ChangeLevel::Cosmetic,
        _ => ChangeLevel::Structural,
    };
    (level, None)
}

/// Usages of `symbol` across `files` (path -> lines), excluding the definition
/// lines; `low_usage` when at most `low_usage_max` remain.
pub fn blast_radius(
    symbol: &Id,
    name: &str,
    files: &BTreeMap<String, Vec<String>>,
    definitions: &BTreeSet<(String, usize)>,
    low_usage_max: u64,
) -> BlastRadius {
    let mut usages = 0u64;
    for (path, lines) in files {
        for (number, line) in lines.iter().enumerate() {
            if line.contains(name) && !definitions.contains(&(path.clone(), number + 1)) {
                usages += 1;
            }
        }
    }
    BlastRadius {
        symbol: symbol.clone(),
        usages,
        low_usage: usages <= low_usage_max,
    }
}

/// The outline of `symbols` (path, symbol) as directories, files and symbols.
pub fn code_map(symbols: &[(String, String)]) -> CodeMap {
    let mut map = CodeMap::default();
    for (path, symbol) in symbols {
        let mut parent = String::from("/");
        map.nodes.insert(parent.clone());
        let mut current = String::new();
        for part in path.split('/') {
            current = if current.is_empty() {
                part.to_string()
            } else {
                format!("{current}/{part}")
            };
            map.nodes.insert(current.clone());
            map.edges.insert((parent, current.clone()));
            parent = current.clone();
        }
        let symbol_node = format!("{path}::{symbol}");
        map.nodes.insert(symbol_node.clone());
        map.edges.insert((parent, symbol_node));
    }
    map
}

/// Per-run evidence (R603.5).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintRunReport {
    pub files_by_level: BTreeMap<ChangeLevel, u64>,
    pub unparseable: u64,
    pub low_usage_symbols: u64,
}

/// Classify every file against the stored fingerprints and return the paths
/// that re-enter enrichment.
pub fn plan_reindex(
    stored: &BTreeMap<Id, StructuralFingerprint>,
    current: &[StructuralFingerprint],
    radii: &[BlastRadius],
) -> (Vec<Id>, FingerprintRunReport) {
    let mut report = FingerprintRunReport::default();
    let mut reindex = Vec::new();
    for fingerprint in current {
        let (level, refusal) = classify(stored.get(&fingerprint.path), fingerprint);
        *report.files_by_level.entry(level).or_insert(0) += 1;
        report.unparseable += u64::from(refusal.is_some());
        if level == ChangeLevel::Structural {
            reindex.push(fingerprint.path.clone());
        }
    }
    report.low_usage_symbols = radii.iter().filter(|r| r.low_usage).count() as u64;
    (reindex, report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }

    fn sigs(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| s.to_string()).collect()
    }

    // spec: EG-REPO-INGEST-R603.1
    #[test]
    fn fingerprint_types_round_trip_and_invalid_identifiers_are_refused() {
        let print = fingerprint(id("src/a.rs"), b"fn a() {}", Ok(&sigs(&["fn a()"])));
        assert_eq!(
            serde_json::from_value::<StructuralFingerprint>(serde_json::to_value(&print).unwrap())
                .unwrap(),
            print
        );
        let radius = BlastRadius {
            symbol: id("a"),
            usages: 1,
            low_usage: true,
        };
        assert_eq!(
            serde_json::from_value::<BlastRadius>(serde_json::to_value(&radius).unwrap()).unwrap(),
            radius
        );
        let map = code_map(&[("src/a.rs".into(), "a".into())]);
        assert_eq!(
            serde_json::from_value::<CodeMap>(serde_json::to_value(&map).unwrap()).unwrap(),
            map
        );
        assert_eq!(
            serde_json::from_value::<ChangeLevel>(serde_json::json!("cosmetic")).unwrap(),
            ChangeLevel::Cosmetic
        );
        let bad = serde_json::json!({"symbol": "", "usages": 0, "low_usage": true});
        assert!(serde_json::from_value::<BlastRadius>(bad).is_err());
    }

    // spec: EG-REPO-INGEST-R603.2
    #[test]
    fn levels_blast_radius_and_code_map_follow_the_rules() {
        let old = fingerprint(id("a.rs"), b"fn a() {}", Ok(&sigs(&["fn a()"])));
        let same = fingerprint(id("a.rs"), b"fn a() {}", Ok(&sigs(&["fn a()"])));
        let cosmetic = fingerprint(id("a.rs"), b"fn a() { }\n", Ok(&sigs(&["fn a()"])));
        let structural = fingerprint(id("a.rs"), b"fn a(x: u8) {}", Ok(&sigs(&["fn a(x: u8)"])));
        assert_eq!(classify(Some(&old), &same).0, ChangeLevel::None);
        assert_eq!(classify(Some(&old), &cosmetic).0, ChangeLevel::Cosmetic);
        assert_eq!(classify(Some(&old), &structural).0, ChangeLevel::Structural);
        assert_eq!(classify(None, &same).0, ChangeLevel::Structural);

        let files = BTreeMap::from([
            (
                "a.rs".to_string(),
                vec!["fn helper() {}".to_string(), "helper();".to_string()],
            ),
            ("b.rs".to_string(), vec!["helper();".to_string()]),
        ]);
        let definitions = BTreeSet::from([("a.rs".to_string(), 1)]);
        let radius = blast_radius(&id("helper"), "helper", &files, &definitions, 2);
        assert_eq!(radius.usages, 2, "the definition line is excluded");
        assert!(radius.low_usage, "at the boundary");
        assert!(!blast_radius(&id("helper"), "helper", &files, &definitions, 1).low_usage);

        let map = code_map(&[("src/a.rs".into(), "a".into())]);
        assert!(map.edges.contains(&("/".to_string(), "src".to_string())));
        assert!(map
            .edges
            .contains(&("src".to_string(), "src/a.rs".to_string())));
        assert!(map
            .edges
            .contains(&("src/a.rs".to_string(), "src/a.rs::a".to_string())));
    }

    // spec: EG-REPO-INGEST-R603.4
    #[test]
    fn an_unparseable_file_is_structural_and_never_skipped() {
        let old = fingerprint(id("a.rs"), b"fn a() {}", Ok(&sigs(&["fn a()"])));
        let broken = fingerprint(id("a.rs"), b"fn a() {}", Err("unexpected token"));
        // Same bytes, but the parse failed: never classed None or Cosmetic.
        let (level, refusal) = classify(Some(&old), &broken);
        assert_eq!(level, ChangeLevel::Structural);
        assert_eq!(refusal.unwrap().code, FINGERPRINT_UNPARSEABLE);
        let stored = BTreeMap::from([(id("a.rs"), old)]);
        let (reindex, _) = plan_reindex(&stored, &[broken], &[]);
        assert_eq!(reindex, [id("a.rs")]);
    }

    // spec: EG-REPO-INGEST-R603.5
    #[test]
    fn the_run_report_counts_levels_and_low_usage_symbols() {
        let old = fingerprint(id("a.rs"), b"fn a() {}", Ok(&sigs(&["fn a()"])));
        let stored = BTreeMap::from([(id("a.rs"), old.clone())]);
        let current = [old, fingerprint(id("b.rs"), b"x", Err("bad"))];
        let radii = [BlastRadius {
            symbol: id("a"),
            usages: 0,
            low_usage: true,
        }];
        let (reindex, report) = plan_reindex(&stored, &current, &radii);
        assert_eq!(reindex, [id("b.rs")]);
        assert_eq!(
            report.files_by_level,
            BTreeMap::from([(ChangeLevel::None, 1), (ChangeLevel::Structural, 1)])
        );
        assert_eq!(report.unparseable, 1);
        assert_eq!(report.low_usage_symbols, 1);
    }
}
