//! CONCEPT:EH-280 — the class of inline material an envelope carries, and the
//! host-identity rule its text values are screened with.
//!
//! The persistence privacy policy exists to keep HOST identity (a real home or
//! mount path, a local file URI, an e-mail address) out of durable rows. Both
//! classes screen material VALUES by what a value IS, never by which single
//! characters it contains: ordinary document text legitimately carries `@`
//! (a decorator, a handle, `a @ b`) and `\` (a LaTeX macro, an escape, a
//! regex), so refusing those characters refused normal documents
//! (spec: EG-DURABLE-KERNEL-R043.7).
//!
//! Caller-attested material is screened for a host path ANYWHERE in the text
//! (a document can mention one mid-sentence), a local file URI, a Windows
//! profile path, or an e-mail address. Repository snapshot material is
//! code: `app/home/views.py` or a string literal is the repository's own
//! content, so only a value that IS an absolute host path is refused there.
//! Property keys and envelope identifiers are engine-chosen names and keep
//! the strict character rule in every class. Every refusal carries the
//! declared `INVALID_ARGUMENT` code and names the shape it found without
//! echoing the value (spec: EG-DURABLE-KERNEL-R043.8).

use serde::{Deserialize, Serialize};

/// How an envelope's inline material values are screened for host identity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum MaterialClass {
    /// Caller-attested material: every text value is screened strictly.
    #[default]
    Attested,
    /// Content of a repository snapshot, lowered by the engine's own
    /// repository indexer. A caller cannot claim this class: the request
    /// boundary refuses it on the public envelope methods.
    RepositorySnapshot,
}

/// A text-value screening rule.
pub(super) type TextRule = fn(&str) -> Result<(), String>;

impl MaterialClass {
    /// The default class is omitted on the wire, so existing envelopes keep
    /// their exact serialized form and digest.
    pub fn is_attested(&self) -> bool {
        matches!(self, Self::Attested)
    }

    pub(super) fn value_rule(self) -> TextRule {
        match self {
            Self::Attested => validate_attested_text,
            Self::RepositorySnapshot => validate_repository_text,
        }
    }
}

const HOST_PATH_PREFIXES: &[&str] = &["/home/", "/users/", "/mnt/", "/root/"];

/// Separators around an e-mail-shaped token in code text.
fn is_token_boundary(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '"' | '\'' | '`' | '<' | '>' | '(' | ')' | ',' | ';')
}

/// `local@domain.tld` with a non-empty local part and an alphabetic top-level
/// label. A decorator (`@dataclass`, `@app.route`) has no local part and a
/// version pin (`pkg@1.2.3`) has a numeric last label: neither is an address.
/// Sentence punctuation after the domain (`dev@example.com.`) is ignored.
fn is_email_token(token: &str) -> bool {
    token.split_once('@').is_some_and(|(local, domain)| {
        let domain = domain.trim_end_matches(['.', ',', ':', ';', '!', '?']);
        let dotted = domain.contains('.') && !domain.starts_with('.');
        let tld = domain.rsplit('.').next().unwrap_or_default();
        let alphabetic_tld = tld.len() >= 2 && tld.bytes().all(|b| b.is_ascii_alphabetic());
        !local.is_empty() && dotted && alphabetic_tld
    })
}

fn has_email(value: &str) -> bool {
    value.split(is_token_boundary).any(is_email_token)
}

/// A host-path prefix that starts a path rather than continuing a relative one:
/// `/home/` at the start or after a separator is a host path; `app/home/` is not.
fn has_host_path_anywhere(lower: &str) -> bool {
    HOST_PATH_PREFIXES.iter().any(|prefix| {
        lower.match_indices(prefix).any(|(at, _)| {
            lower[..at].chars().next_back().is_none_or(|ch| {
                !(ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '/'))
            })
        })
    })
}

/// `C:\Users\…` / `C:\Documents and Settings\…` — a Windows profile path.
fn has_windows_profile_path(lower: &str) -> bool {
    lower.contains(":\\users\\") || lower.contains(":\\documents and settings\\")
}

/// The declared-code refusal for a host-identity finding. Names the shape found
/// and the material class; never echoes the refused value.
fn host_identity_refusal(shape: &str, class: &str) -> String {
    format!("INVALID_ARGUMENT: persistence privacy policy rejected {shape} in {class}")
}

/// Caller-attested material values: refuse host identity, admit `@` and `\`.
pub(super) fn validate_attested_text(value: &str) -> Result<(), String> {
    let lower = value.to_ascii_lowercase();
    let shape = if lower.contains("file://") {
        "a local file URI"
    } else if has_host_path_anywhere(&lower) || has_windows_profile_path(&lower) {
        "a host path"
    } else if has_email(value) {
        "an e-mail address"
    } else {
        return Ok(());
    };
    Err(host_identity_refusal(shape, "attested material"))
}

pub(super) fn validate_repository_text(value: &str) -> Result<(), String> {
    let lower = value.to_ascii_lowercase();
    let host_path = HOST_PATH_PREFIXES
        .iter()
        .any(|prefix| lower.starts_with(prefix));
    if host_path || lower.contains("file://") || has_email(value) {
        return Err(host_identity_refusal("host identity", "repository content"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repository_content_keeps_code_and_relative_paths() {
        let rule = MaterialClass::RepositorySnapshot.value_rule();
        for value in [
            "@dataclass",
            "@app.route",
            "app/home/views.py",
            "src/users/model.py",
            "a\\b",
        ] {
            assert!(rule(value).is_ok(), "{value} is repository content");
        }
    }

    #[test]
    fn repository_content_still_refuses_host_identity() {
        let rule = MaterialClass::RepositorySnapshot.value_rule();
        for value in [
            "/home/alice/src/x.py",
            "/Users/bob",
            "file:///etc/passwd",
            "mail dev@example.com now",
        ] {
            assert!(rule(value).is_err(), "{value} carries host identity");
        }
    }

    // spec: EG-DURABLE-KERNEL-R043.7
    #[test]
    fn attested_material_admits_ordinary_document_text() {
        let rule = MaterialClass::Attested.value_rule();
        for value in [
            "@dataclass",
            "ping @alice in the channel",
            "a @ b",
            "pin pkg@1.2.3",
            "\\frac{a}{b} and \\alpha",
            "C:\\Program Files\\Tool",
            "line one\\nline two",
            "app/home/views.py",
            "see src/users/model.py",
        ] {
            assert!(rule(value).is_ok(), "{value} is ordinary document text");
        }
        assert!(MaterialClass::default().is_attested());
    }

    // spec: EG-DURABLE-KERNEL-R043.7
    #[test]
    fn attested_material_still_refuses_host_identity() {
        let rule = MaterialClass::Attested.value_rule();
        for value in [
            "/home/person/notes.md",
            "notes live at /home/person/x",
            "path=/Users/person",
            "open file:///etc/hosts",
            "C:\\Users\\person\\Desktop",
            "mail dev@example.com now",
            "write to dev@example.com.",
        ] {
            assert!(rule(value).is_err(), "{value} carries host identity");
        }
    }

    // spec: EG-DURABLE-KERNEL-R043.8
    #[test]
    fn host_identity_refusals_are_coded_and_do_not_echo_the_value() {
        let secret = "dev@example.com";
        for err in [
            MaterialClass::Attested.value_rule()(secret).unwrap_err(),
            MaterialClass::RepositorySnapshot.value_rule()(secret).unwrap_err(),
        ] {
            let (code, detail) = err.split_once(": ").unwrap();
            assert!(crate::contract::declared_error_code(code), "{err}");
            assert!(!detail.contains(secret), "{err}");
        }
    }
}
