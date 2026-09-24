//! CONCEPT:EH-280 — the class of inline material an envelope carries, and the
//! host-identity rule its text values are screened with.
//!
//! The persistence privacy policy exists to keep HOST identity (a real home or
//! mount path, a local file URI, an e-mail address) out of durable rows. Most
//! material is attested by its caller and screened strictly. Repository
//! snapshot material is different: symbol names, repository-relative paths
//! and code text ARE the repository's content, so `@dataclass` or
//! `app/home/views.py` are legitimate values there. That content is still
//! screened for host identity, with a rule that looks at what a value IS (an
//! absolute host path, a file URI, an e-mail address) rather than at which
//! characters it contains. Property keys are engine-chosen names and keep the
//! strict rule in every class.

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
            Self::Attested => super::validate_safe_text,
            Self::RepositorySnapshot => validate_repository_text,
        }
    }
}

const HOST_PATH_PREFIXES: &[&str] = &["/home/", "/users/", "/mnt/", "/root/"];

/// Separators around an e-mail-shaped token in code text.
fn is_token_boundary(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '"' | '\'' | '`' | '<' | '>' | '(' | ')' | ',' | ';')
}

/// `local@domain.tld` with a non-empty local part. A decorator (`@dataclass`,
/// `@app.route`) has no local part and is not an address.
fn is_email_token(token: &str) -> bool {
    token.split_once('@').is_some_and(|(local, domain)| {
        let dotted = domain.contains('.') && !domain.starts_with('.') && !domain.ends_with('.');
        !local.is_empty() && dotted
    })
}

fn validate_repository_text(value: &str) -> Result<(), String> {
    let lower = value.to_ascii_lowercase();
    let host_path = HOST_PATH_PREFIXES
        .iter()
        .any(|prefix| lower.starts_with(prefix));
    let email = value.split(is_token_boundary).any(is_email_token);
    if host_path || lower.contains("file://") || email {
        return Err(
            "persistence privacy policy rejected host identity in repository content".to_string(),
        );
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

    #[test]
    fn attested_material_keeps_the_strict_rule() {
        let rule = MaterialClass::Attested.value_rule();
        assert!(rule("@dataclass").is_err());
        assert!(rule("app/home/views.py").is_err());
        assert!(MaterialClass::default().is_attested());
    }
}
