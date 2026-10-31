//! Defect intake (MEDULLA-SDLC-R003.2): symptom normalization, the signature digest
//! and deduplication. An open defect with the same signature absorbs a new
//! symptom; a closed one reopens.

use medulla_types::lifecycle::{sha256_hex, Defect, DefectIntake, DefectStatus, Severity, Symptom};

fn is_hex_address(token: &str) -> bool {
    token
        .strip_prefix("0x")
        .is_some_and(|rest| !rest.is_empty() && rest.chars().all(|c| c.is_ascii_hexdigit()))
}

fn is_temp_path(token: &str) -> bool {
    token.contains('/')
        && token
            .split('/')
            .any(|part| matches!(part, "tmp" | "folders"))
}

/// Drop trailing `:line` and `:line:col` suffixes from a path-like token.
fn strip_line_suffix(token: &str) -> &str {
    let mut end = token.len();
    for _ in 0..2 {
        match token[..end].rsplit_once(':') {
            Some((head, tail)) if !tail.is_empty() && tail.chars().all(|c| c.is_ascii_digit()) => {
                end = head.len();
            }
            _ => break,
        }
    }
    &token[..end]
}

fn is_line_number(token: &str) -> bool {
    let digits = token.trim_end_matches([',', ':', ';', ')']);
    !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit())
}

/// Normalize a symptom: strip addresses, line numbers and temporary paths, and
/// collapse whitespace.
pub fn normalize_symptom(text: &str) -> String {
    let mut out: Vec<String> = Vec::new();
    let mut tokens = text.split_whitespace().peekable();
    while let Some(token) = tokens.next() {
        let bare = token.trim_matches(|c| matches!(c, '(' | ')' | ',' | ';' | '"' | '\''));
        if bare.eq_ignore_ascii_case("line") && tokens.peek().is_some_and(|n| is_line_number(n)) {
            tokens.next();
            continue;
        }
        out.push(if is_hex_address(bare) {
            "<addr>".to_owned()
        } else if is_temp_path(bare) {
            "<tmp>".to_owned()
        } else {
            strip_line_suffix(token).to_owned()
        });
    }
    out.join(" ")
}

/// The signature digest of a symptom: SHA-256 of its normalized text.
pub fn signature_digest(text: &str) -> String {
    sha256_hex(normalize_symptom(text).as_bytes())
}

/// What `FileDefect` did with a symptom.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileOutcome {
    Created(String),
    Absorbed(String),
    Reopened(String),
}

/// The defects of one repository, deduplicated by signature.
#[derive(Clone, Debug, Default)]
pub struct DefectBook {
    defects: Vec<Defect>,
}

fn is_open(status: DefectStatus) -> bool {
    !matches!(status, DefectStatus::Closed | DefectStatus::Rejected)
}

impl DefectBook {
    pub fn defects(&self) -> &[Defect] {
        &self.defects
    }

    /// Move a defect to a status. Transition legality is the caller's rule.
    pub fn set_status(&mut self, id: &str, status: DefectStatus) -> bool {
        match self.defects.iter_mut().find(|d| d.id == id) {
            Some(defect) => {
                defect.status = status;
                true
            }
            None => false,
        }
    }

    fn find(&self, repo: &str, signature: &str) -> Option<usize> {
        let matching = |d: &&Defect| d.repo == repo && d.signature_digest == signature;
        let by_state = |open: bool| {
            self.defects.iter().position(|d| {
                matching(&d) && is_open(d.status) == open && d.status != DefectStatus::Rejected
            })
        };
        by_state(true).or_else(|| by_state(false))
    }

    /// File a symptom. Equal normalized symptoms land on one defect.
    pub fn file(
        &mut self,
        repo: &str,
        intake: DefectIntake,
        title: &str,
        severity_hint: Option<Severity>,
        symptom: Symptom,
    ) -> FileOutcome {
        let signature = signature_digest(&symptom.text);
        let Some(index) = self.find(repo, &signature) else {
            let id = format!("def-{:04}", self.defects.len() + 1);
            self.defects.push(Defect {
                id: id.clone(),
                repo: repo.to_owned(),
                title: title.to_owned(),
                intake,
                signature_digest: signature,
                severity: severity_hint.unwrap_or(Severity::Medium),
                status: DefectStatus::New,
                created_at: symptom.observed_at,
                symptoms: vec![symptom],
            });
            return FileOutcome::Created(id);
        };
        let defect = &mut self.defects[index];
        let reopened = !is_open(defect.status);
        if reopened {
            defect.status = DefectStatus::New;
        }
        defect.symptoms.push(symptom);
        if reopened {
            FileOutcome::Reopened(defect.id.clone())
        } else {
            FileOutcome::Absorbed(defect.id.clone())
        }
    }
}
