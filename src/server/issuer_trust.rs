//! Multi-issuer OIDC trust list (EG-IDENTITY-R001, `.1` slice).
//!
//! The primary OIDC verifier ([`super::oidc`], [`super::auth`]) today
//! validates against one configured issuer/audience pair. This module adds
//! the typed trust-list model an ordered, multi-issuer configuration needs:
//! each entry names its issuer, JWKS source, audience, and the principal
//! kinds it may authenticate (so a Keycloak-backed issuer can be restricted
//! to minting only service principals, never human ones). Resolution walks
//! the list in order and fails closed on an unknown, ambiguous (duplicate
//! issuer), or disallowed-kind match. Wiring this into [`super::auth`]'s
//! request-verification path (replacing the single `expected_audience`
//! check) is the `.2` entry-point slice.

use eg_types::identity::UserKind;

/// One trusted issuer's verification parameters and principal-kind limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuerTrustEntry {
    /// The `iss` claim this entry matches, exactly.
    pub issuer: String,
    /// Where this issuer's signing keys are fetched (a JWKS URL today; kept
    /// as an opaque string so a future source kind needs no model change).
    pub jwks_source: String,
    /// The `aud` claim a token from this issuer must carry.
    pub audience: String,
    /// The principal kinds this issuer may authenticate. Empty is rejected
    /// at construction — an issuer trusted for nothing is a configuration
    /// error, not a silent no-op.
    pub allowed_kinds: Vec<UserKind>,
}

/// Why an issuer trust list rejected a candidate before method dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IssuerTrustRefusal {
    /// No entry's issuer matched the token's `iss` claim.
    UnknownIssuer,
    /// More than one entry matched the same issuer — the list itself is
    /// misconfigured, so every lookup against it fails closed.
    AmbiguousIssuer,
    /// The issuer matched, but not for the requested principal kind.
    DisallowedKind,
}

/// An ordered, validated multi-issuer trust list.
///
/// Construction validates that no two entries share an issuer and that
/// every entry allows at least one principal kind; [`IssuerTrustList::new`]
/// is the only way to obtain one, so a validated list can never be
/// constructed around bad data.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IssuerTrustList {
    entries: Vec<IssuerTrustEntry>,
}

impl IssuerTrustList {
    /// Build a trust list, failing closed on a duplicate issuer or an entry
    /// with no allowed principal kinds.
    pub fn new(entries: Vec<IssuerTrustEntry>) -> Result<Self, IssuerTrustRefusal> {
        for (i, entry) in entries.iter().enumerate() {
            if entry.allowed_kinds.is_empty() {
                return Err(IssuerTrustRefusal::DisallowedKind);
            }
            if entries[..i]
                .iter()
                .any(|prior| prior.issuer == entry.issuer)
            {
                return Err(IssuerTrustRefusal::AmbiguousIssuer);
            }
        }
        Ok(Self { entries })
    }

    /// Resolve an issuer for a candidate principal kind, in list order.
    ///
    /// Fails closed: an issuer absent from the list, or present but not
    /// trusted for `kind`, is refused before any method dispatch runs.
    pub fn resolve(
        &self,
        issuer: &str,
        kind: UserKind,
    ) -> Result<&IssuerTrustEntry, IssuerTrustRefusal> {
        let matches: Vec<&IssuerTrustEntry> =
            self.entries.iter().filter(|e| e.issuer == issuer).collect();
        match matches.as_slice() {
            [] => Err(IssuerTrustRefusal::UnknownIssuer),
            [entry] => {
                if entry.allowed_kinds.contains(&kind) {
                    Ok(entry)
                } else {
                    Err(IssuerTrustRefusal::DisallowedKind)
                }
            }
            _ => Err(IssuerTrustRefusal::AmbiguousIssuer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keycloak_service_only() -> IssuerTrustEntry {
        IssuerTrustEntry {
            issuer: "https://keycloak.example/realms/engine".to_string(),
            jwks_source: "https://keycloak.example/realms/engine/protocol/openid-connect/certs"
                .to_string(),
            audience: "epistemic-graph".to_string(),
            allowed_kinds: vec![UserKind::Service],
        }
    }

    #[test]
    fn unknown_issuer_is_refused_before_dispatch() {
        let list = IssuerTrustList::new(vec![keycloak_service_only()]).unwrap();
        let outcome = list.resolve("https://not-trusted.example/realm", UserKind::Service);
        assert_eq!(outcome, Err(IssuerTrustRefusal::UnknownIssuer));
    }

    #[test]
    fn disallowed_principal_kind_is_refused() {
        let list = IssuerTrustList::new(vec![keycloak_service_only()]).unwrap();
        let outcome = list.resolve("https://keycloak.example/realms/engine", UserKind::Human);
        assert_eq!(outcome, Err(IssuerTrustRefusal::DisallowedKind));
    }

    #[test]
    fn ambiguous_duplicate_issuer_is_refused_at_construction() {
        let dup = keycloak_service_only();
        let outcome = IssuerTrustList::new(vec![dup.clone(), dup]);
        assert_eq!(outcome, Err(IssuerTrustRefusal::AmbiguousIssuer));
    }

    #[test]
    fn allowed_kind_resolves_to_its_entry() {
        let list = IssuerTrustList::new(vec![keycloak_service_only()]).unwrap();
        let entry = list
            .resolve("https://keycloak.example/realms/engine", UserKind::Service)
            .unwrap();
        assert_eq!(entry.audience, "epistemic-graph");
    }
}
