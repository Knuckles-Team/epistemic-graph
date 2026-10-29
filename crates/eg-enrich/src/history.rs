//! Commit history as a graph of commits, authors, files and change coupling
//! (EG-REPO-INGEST-R601).
//!
//! Each commit is applied once per digest; a re-run adds only unseen commits.
//! A history above the configured bound is truncated at a commit boundary and
//! reported with `HISTORY_BOUND_EXCEEDED`, never half-applied.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{EnrichError, Id};

/// The stable code of a history truncated at the commit bound.
pub const HISTORY_BOUND_EXCEEDED: &str = "HISTORY_BOUND_EXCEEDED";
/// The stable code of a commit digest that is not 40 or 64 lowercase hex.
pub const INVALID_COMMIT_DIGEST: &str = "INVALID_COMMIT_DIGEST";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    Added,
    Modified,
    Deleted,
    Renamed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileChange {
    pub path: Id,
    pub kind: ChangeKind,
    pub added_lines: u32,
    pub removed_lines: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "CommitRecordWire")]
pub struct CommitRecord {
    digest: String,
    pub author: Id,
    pub timestamp_ms: u64,
    pub parents: Vec<String>,
    pub changes: Vec<FileChange>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CommitRecordWire {
    digest: String,
    author: Id,
    timestamp_ms: u64,
    parents: Vec<String>,
    changes: Vec<FileChange>,
}

impl TryFrom<CommitRecordWire> for CommitRecord {
    type Error = EnrichError;

    fn try_from(wire: CommitRecordWire) -> Result<Self, Self::Error> {
        Self::new(
            &wire.digest,
            wire.author,
            wire.timestamp_ms,
            wire.parents,
            wire.changes,
        )
    }
}

fn valid_digest(digest: &str) -> bool {
    matches!(digest.len(), 40 | 64)
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl CommitRecord {
    pub fn new(
        digest: &str,
        author: Id,
        timestamp_ms: u64,
        parents: Vec<String>,
        changes: Vec<FileChange>,
    ) -> Result<Self, EnrichError> {
        for value in std::iter::once(digest).chain(parents.iter().map(String::as_str)) {
            if !valid_digest(value) {
                return Err(EnrichError::new(
                    INVALID_COMMIT_DIGEST,
                    format!("{value:?} is not a 40 or 64 hex commit digest"),
                ));
            }
        }
        Ok(Self {
            digest: digest.to_string(),
            author,
            timestamp_ms,
            parents,
            changes,
        })
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }
}

/// Churn per file or per author.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChurnAggregate {
    pub commits: u64,
    pub added_lines: u64,
    pub removed_lines: u64,
}

/// Two files that change together: `support` co-changing commits, and
/// `confidence_milli` = 1000 x support / min(changes of a, changes of b).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CouplingEdge {
    pub a: Id,
    pub b: Id,
    pub support: u64,
    pub confidence_milli: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HistoryConfig {
    pub max_commits: usize,
    pub support_floor: u64,
    pub confidence_floor_milli: u64,
    /// The most files of one commit that enter pair counting.
    pub max_files_per_commit: usize,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            max_commits: 100_000,
            support_floor: 2,
            confidence_floor_milli: 500,
            max_files_per_commit: 64,
        }
    }
}

/// The history graph accumulated over runs.
#[derive(Clone, Debug, Default)]
pub struct HistoryGraph {
    pub commits: BTreeSet<String>,
    pub file_churn: BTreeMap<Id, ChurnAggregate>,
    pub author_churn: BTreeMap<Id, ChurnAggregate>,
    pair_support: BTreeMap<(Id, Id), u64>,
}

/// Per-run evidence (R601.5).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryRunReport {
    pub commits_added: u64,
    pub commits_skipped: u64,
    pub coupling_edges: u64,
    pub truncated: Option<String>,
}

fn add_churn(aggregate: &mut ChurnAggregate, added: u32, removed: u32) {
    aggregate.commits += 1;
    aggregate.added_lines += u64::from(added);
    aggregate.removed_lines += u64::from(removed);
}

impl HistoryGraph {
    fn apply(&mut self, commit: &CommitRecord, config: &HistoryConfig) {
        self.commits.insert(commit.digest.clone());
        let (mut added, mut removed) = (0, 0);
        let mut files = BTreeSet::new();
        for change in &commit.changes {
            add_churn(
                self.file_churn.entry(change.path.clone()).or_default(),
                change.added_lines,
                change.removed_lines,
            );
            added += change.added_lines;
            removed += change.removed_lines;
            files.insert(change.path.clone());
        }
        add_churn(
            self.author_churn.entry(commit.author.clone()).or_default(),
            added,
            removed,
        );
        let files: Vec<_> = files
            .into_iter()
            .take(config.max_files_per_commit)
            .collect();
        for (i, a) in files.iter().enumerate() {
            for b in &files[i + 1..] {
                *self.pair_support.entry((a.clone(), b.clone())).or_insert(0) += 1;
            }
        }
    }

    /// Coupling edges above both floors.
    pub fn coupling(&self, config: &HistoryConfig) -> Vec<CouplingEdge> {
        self.pair_support
            .iter()
            .filter(|(_, support)| **support >= config.support_floor)
            .filter_map(|((a, b), support)| {
                let changes = |f: &Id| self.file_churn.get(f).map_or(0, |c| c.commits);
                let base = changes(a).min(changes(b)).max(1);
                let confidence_milli = support * 1000 / base;
                (confidence_milli >= config.confidence_floor_milli).then(|| CouplingEdge {
                    a: a.clone(),
                    b: b.clone(),
                    support: *support,
                    confidence_milli,
                })
            })
            .collect()
    }

    /// Apply the unseen commits of `history`, at most `config.max_commits` of
    /// them, each whole.
    pub fn ingest_history(
        &mut self,
        history: &[CommitRecord],
        config: &HistoryConfig,
    ) -> HistoryRunReport {
        let mut report = HistoryRunReport::default();
        for commit in history {
            if self.commits.contains(&commit.digest) {
                report.commits_skipped += 1;
                continue;
            }
            if report.commits_added as usize >= config.max_commits {
                report.truncated = Some(format!(
                    "{HISTORY_BOUND_EXCEEDED}: stopped before {} after {} commits",
                    commit.digest, config.max_commits
                ));
                break;
            }
            self.apply(commit, config);
            report.commits_added += 1;
        }
        report.coupling_edges = self.coupling(config).len() as u64;
        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> Id {
        Id::new(value).unwrap()
    }

    fn digest(n: u8) -> String {
        format!("{n:040x}")
    }

    fn commit(n: u8, files: &[&str]) -> CommitRecord {
        CommitRecord::new(
            &digest(n),
            id("alice"),
            u64::from(n),
            vec![],
            files
                .iter()
                .map(|f| FileChange {
                    path: id(f),
                    kind: ChangeKind::Modified,
                    added_lines: 2,
                    removed_lines: 1,
                })
                .collect(),
        )
        .unwrap()
    }

    // spec: EG-REPO-INGEST-R601.1
    #[test]
    fn history_types_round_trip_and_invalid_identifiers_are_refused() {
        let record = commit(1, &["a.rs"]);
        let wire = serde_json::to_value(&record).unwrap();
        assert_eq!(
            serde_json::from_value::<CommitRecord>(wire).unwrap(),
            record
        );
        let edge = CouplingEdge {
            a: id("a.rs"),
            b: id("b.rs"),
            support: 3,
            confidence_milli: 750,
        };
        assert_eq!(
            serde_json::from_value::<CouplingEdge>(serde_json::to_value(&edge).unwrap()).unwrap(),
            edge
        );
        let churn = ChurnAggregate {
            commits: 1,
            added_lines: 2,
            removed_lines: 1,
        };
        assert_eq!(
            serde_json::from_value::<ChurnAggregate>(serde_json::to_value(&churn).unwrap())
                .unwrap(),
            churn
        );
        let mut forged = serde_json::to_value(&record).unwrap();
        forged["digest"] = serde_json::json!("XYZ");
        assert!(serde_json::from_value::<CommitRecord>(forged).is_err());
        let refused = CommitRecord::new("not-hex", id("alice"), 0, vec![], vec![]).unwrap_err();
        assert_eq!(refused.code, INVALID_COMMIT_DIGEST);
        assert!(Id::new("bad path").is_err());
    }

    // spec: EG-REPO-INGEST-R601.2
    #[test]
    fn commits_apply_once_churn_aggregates_and_coupling_respects_floors() {
        let config = HistoryConfig::default();
        let history = [
            commit(1, &["a.rs", "b.rs"]),
            commit(2, &["a.rs", "b.rs"]),
            commit(3, &["a.rs", "c.rs"]),
        ];
        let mut graph = HistoryGraph::default();
        let first = graph.ingest_history(&history, &config);
        assert_eq!(first.commits_added, 3);
        assert_eq!(graph.file_churn[&id("a.rs")].commits, 3);
        assert_eq!(graph.author_churn[&id("alice")].added_lines, 12);
        let coupling = graph.coupling(&config);
        // a-b: support 2 (at the floor), confidence 2/2; a-c: support 1 (below).
        assert_eq!(coupling.len(), 1);
        assert_eq!(
            (coupling[0].support, coupling[0].confidence_milli),
            (2, 1000)
        );
        // A re-run adds only unseen commits.
        let again = graph.ingest_history(&history, &config);
        assert_eq!((again.commits_added, again.commits_skipped), (0, 3));
        assert_eq!(graph.file_churn[&id("a.rs")].commits, 3);
    }

    // spec: EG-REPO-INGEST-R601.4
    #[test]
    fn a_history_above_the_bound_is_truncated_at_a_commit_boundary() {
        let config = HistoryConfig {
            max_commits: 2,
            ..HistoryConfig::default()
        };
        let history = [
            commit(1, &["a.rs"]),
            commit(2, &["b.rs"]),
            commit(3, &["c.rs"]),
        ];
        let mut graph = HistoryGraph::default();
        let report = graph.ingest_history(&history, &config);
        assert_eq!(report.commits_added, 2);
        assert!(report
            .truncated
            .as_deref()
            .unwrap()
            .starts_with(HISTORY_BOUND_EXCEEDED));
        assert!(!graph.commits.contains(&digest(3)));
        assert!(
            !graph.file_churn.contains_key(&id("c.rs")),
            "no partial write"
        );
    }

    // spec: EG-REPO-INGEST-R601.5
    #[test]
    fn the_run_report_counts_added_skipped_and_coupling() {
        let config = HistoryConfig::default();
        let mut graph = HistoryGraph::default();
        let ok = graph.ingest_history(
            &[commit(1, &["a.rs", "b.rs"]), commit(2, &["a.rs", "b.rs"])],
            &config,
        );
        assert_eq!(
            (ok.commits_added, ok.commits_skipped, ok.coupling_edges),
            (2, 0, 1)
        );
        assert_eq!(ok.truncated, None);
        let bounded = HistoryConfig {
            max_commits: 0,
            ..config
        };
        let refused = graph.ingest_history(&[commit(1, &["a.rs"]), commit(9, &["z.rs"])], &bounded);
        assert_eq!((refused.commits_added, refused.commits_skipped), (0, 1));
        assert!(refused.truncated.is_some());
    }
}
