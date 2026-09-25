//! IDM-02: the multi-issuer trust list for the primary `eg2.` envelope.
//!
//! The primary issuer (`EPISTEMIC_GRAPH_OIDC_JWT_*`) keeps today's meaning:
//! it may vouch for any principal. `EPISTEMIC_GRAPH_OIDC_TRUST` adds more
//! issuers, each RESTRICTED to the principal kinds it may vouch for:
//!
//! ```json
//! [{"issuer": "https://kc.example/realms/homelab", "audience": "agent-services",
//!   "jwks_url": "https://kc.example/realms/homelab/protocol/openid-connect/certs",
//!   "allowed_kinds": "service"}]
//! ```
//!
//! (the homelab target: graph-os's local issuer is primary; Keycloak stays
//! trusted directly for SERVICE accounts only, so a Keycloak token for a human
//! is refused -- human Keycloak tokens are exchanged by graph-os).
//!
//! A token is routed to exactly one issuer by its `iss` claim, read WITHOUT
//! trust only to choose the validator; that validator then verifies the
//! signature, issuer, audience and expiry as before. A restricted issuer's
//! principal kind is proven against the identity store, never read from the
//! token.

use eg_types::identity::UserKind;
use serde::Deserialize;

use super::JwtValidator;

/// Env: the additional trusted issuers (a JSON array).
const TRUST_ENV: &str = "EPISTEMIC_GRAPH_OIDC_TRUST";

/// Which principals an issuer may vouch for: `None` is any principal.
pub(crate) type KindRestriction = Option<UserKind>;

/// One configured issuer.
pub(crate) struct TrustedIssuer {
    pub(crate) validator: JwtValidator,
    pub(crate) restriction: KindRestriction,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustEntry {
    issuer: String,
    audience: String,
    jwks_url: String,
    allowed_kinds: String,
}

fn restriction(allowed: &str) -> Result<KindRestriction, String> {
    match allowed {
        "any" => Ok(None),
        "service" => Ok(Some(UserKind::Service)),
        "human" => Ok(Some(UserKind::Human)),
        other => Err(format!("{TRUST_ENV}: unknown allowed_kinds '{other}'")),
    }
}

/// Parse the trust list. Duplicate issuers and incomplete entries are
/// configuration errors (the engine refuses to start), never a fallback.
pub(crate) fn parse_trust(text: &str) -> Result<Vec<TrustedIssuer>, String> {
    let entries: Vec<TrustEntry> =
        serde_json::from_str(text).map_err(|error| format!("{TRUST_ENV}: {error}"))?;
    let mut seen = std::collections::BTreeSet::new();
    let mut issuers = Vec::new();
    for entry in entries {
        let complete = [&entry.issuer, &entry.audience, &entry.jwks_url]
            .iter()
            .all(|field| !field.trim().is_empty());
        if !complete || !seen.insert(entry.issuer.clone()) {
            return Err(format!(
                "{TRUST_ENV}: an entry is incomplete or its issuer repeats"
            ));
        }
        issuers.push(TrustedIssuer {
            restriction: restriction(&entry.allowed_kinds)?,
            validator: JwtValidator::from_trust_entry(entry.issuer, entry.audience, entry.jwks_url),
        });
    }
    Ok(issuers)
}

/// The configured additional issuers (empty when unset). Tests install
/// their own list instead of reading the environment.
#[cfg(not(test))]
pub(crate) fn additional_trust() -> Result<&'static [TrustedIssuer], String> {
    static TRUST: std::sync::OnceLock<Result<Vec<TrustedIssuer>, String>> =
        std::sync::OnceLock::new();
    let parsed = TRUST.get_or_init(|| match std::env::var(TRUST_ENV) {
        Ok(text) if !text.trim().is_empty() => parse_trust(&text),
        _ => Ok(Vec::new()),
    });
    match parsed {
        Ok(issuers) => Ok(issuers.as_slice()),
        Err(error) => Err(error.clone()),
    }
}

/// Decode one base64url (unpadded) segment.
fn base64url(segment: &str) -> Option<Vec<u8>> {
    let mut bits: u32 = 0;
    let mut width = 0u32;
    let mut out = Vec::with_capacity(segment.len() * 3 / 4);
    for byte in segment.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        bits = (bits << 6) | u32::from(value);
        width += 6;
        if width >= 8 {
            width -= 8;
            out.push((bits >> width) as u8);
            bits &= (1 << width) - 1;
        }
    }
    Some(out)
}

/// The UNVERIFIED `iss` of a compact JWT, used only to pick a validator.
pub(crate) fn unverified_issuer(token: &str) -> Option<String> {
    #[derive(Deserialize)]
    struct Issuer {
        iss: String,
    }
    let payload = token.split('.').nth(1)?;
    let bytes = base64url(payload)?;
    serde_json::from_slice::<Issuer>(&bytes)
        .ok()
        .map(|claims| claims.iss)
}

/// The validator and restriction for `token`: the primary issuer (any
/// principal) or exactly one additional issuer. An unknown issuer is `None`.
pub(crate) fn route<'a>(
    primary: &'a JwtValidator,
    additional: &'a [TrustedIssuer],
    token: &str,
) -> Option<(&'a JwtValidator, KindRestriction)> {
    let issuer = unverified_issuer(token)?;
    if issuer == primary.issuer() {
        return Some((primary, None));
    }
    additional
        .iter()
        .find(|trusted| trusted.validator.issuer() == issuer)
        .map(|trusted| (&trusted.validator, trusted.restriction))
}

#[cfg(test)]
mod tests;
