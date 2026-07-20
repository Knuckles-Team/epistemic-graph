//! OIDC JWT validation for the KV-cache HTTP surface (CONCEPT:EG-KG.backend.is-configured-so-co).
//!
//! Pairs the KV surface's auth with the platform's overall auth (the same OIDC
//! client-credentials tokens graph-os validates inbound and mints outbound for the
//! MCP fleet) instead of a separate/static mechanism. A client (the
//! `EpistemicGraphKVBackend` connector, vLLM/LMCache) presents an OIDC access
//! token as `Authorization: Bearer <jwt>`; we verify its RSA signature against the
//! realm's JWKS, plus issuer, audience, and expiry.
//!
//! Only compiled under `--features kvcache-server` (never a `pi` build), which is
//! also where the `ureq`/rustls + `jsonwebtoken`/aws-lc-rs crypto stack lives — so the
//! Pi contract (no ring/rustls in `pi`) is preserved.
//!
//! JWKS keys are primed once at startup and lazily re-fetched on a `kid` miss so a
//! Keycloak signing-key rotation self-heals without a restart. Validation NEVER
//! panics — any failure is a rejected request (`false`), the fail-closed posture.

use std::collections::HashMap;
use std::sync::RwLock;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;

/// Env: explicit issuer, else reuse the platform's inbound-JWT / OIDC issuer.
const ISSUER_ENVS: &[&str] = &[
    "EPISTEMIC_GRAPH_KVCACHE_JWT_ISSUER",
    "FASTMCP_SERVER_AUTH_JWT_ISSUER",
    "OIDC_ISSUER",
];
/// Env: explicit audience, else the platform's fleet audience.
const AUDIENCE_ENVS: &[&str] = &[
    "EPISTEMIC_GRAPH_KVCACHE_JWT_AUDIENCE",
    "FASTMCP_SERVER_AUTH_JWT_AUDIENCE",
    "OIDC_AUDIENCE",
];
/// Env: explicit JWKS URL. Discovery and vendor-specific URL construction belong at
/// the deployment boundary; the engine never guesses an identity-provider layout.
const JWKS_URL_ENV: &str = "EPISTEMIC_GRAPH_KVCACHE_JWKS_URL";

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|n| {
        std::env::var(n)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

/// Minimal claims — signature/iss/aud/exp are enforced by [`Validation`]; we do not
/// need any claim value beyond that, so this is deliberately empty.
#[derive(Debug, Deserialize)]
struct Claims {}

/// A Keycloak-realm JWT validator with a lazily-refreshed JWKS key cache.
pub struct JwtValidator {
    issuer: String,
    audience: String,
    jwks_url: String,
    keys: RwLock<HashMap<String, DecodingKey>>, // kid -> RSA decoding key
}

impl std::fmt::Debug for JwtValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JwtValidator")
            .field(
                "cached_kids",
                &self.keys.read().map(|m| m.len()).unwrap_or(0),
            )
            .finish()
    }
}

fn configured_identity(
    issuer: Option<String>,
    audience: Option<String>,
    jwks_url: Option<String>,
) -> Result<Option<(String, String, String)>, String> {
    let Some(issuer) = issuer else {
        return Ok(None);
    };
    let audience = audience
        .ok_or_else(|| "kvcache JWT issuer requires an explicit configured audience".to_string())?;
    let jwks_url = jwks_url
        .ok_or_else(|| "kvcache JWT issuer requires an explicit configured JWKS URL".to_string())?;
    Ok(Some((issuer, audience, jwks_url)))
}

impl JwtValidator {
    /// Build from the environment. No issuer means JWT mode is not selected. Once
    /// an issuer is present, audience and JWKS URL are mandatory and any incomplete
    /// configuration is an error rather than a deployment-specific fallback.
    pub fn from_env() -> Result<Option<Self>, String> {
        let Some((issuer, audience, jwks_url)) = configured_identity(
            env_first(ISSUER_ENVS),
            env_first(AUDIENCE_ENVS),
            env_first(&[JWKS_URL_ENV]),
        )?
        else {
            return Ok(None);
        };
        let v = JwtValidator {
            issuer,
            audience,
            jwks_url,
            keys: RwLock::new(HashMap::new()),
        };
        // Prime the key cache once; a failure here is non-fatal (validate() re-fetches
        // on demand) but we log it so a misconfigured JWKS URL is visible at boot.
        if v.refresh().is_err() {
            tracing::warn!("kvcache-server: JWKS prime failed; will retry on first token");
        } else {
            tracing::info!("kvcache-server: JWT auth armed");
        }
        Ok(Some(v))
    }

    /// Fetch the realm JWKS and rebuild the `kid -> DecodingKey` cache.
    fn refresh(&self) -> Result<(), String> {
        let body = ureq::get(&self.jwks_url)
            .call()
            .map_err(|e| format!("jwks fetch: {e}"))?
            .into_string()
            .map_err(|e| format!("jwks read: {e}"))?;
        let set: JwkSet = serde_json::from_str(&body).map_err(|e| format!("jwks parse: {e}"))?;
        let mut map = HashMap::new();
        for jwk in &set.keys {
            if let Some(kid) = jwk.common.key_id.clone() {
                if let Ok(key) = DecodingKey::from_jwk(jwk) {
                    map.insert(kid, key);
                }
            }
        }
        if map.is_empty() {
            return Err("jwks contained no usable RSA keys".to_string());
        }
        *self.keys.write().map_err(|_| "keys lock poisoned")? = map;
        Ok(())
    }

    /// Validate a bearer token: RSA signature (against a cached JWKS key) + issuer +
    /// audience + expiry. Fail-closed: any error ⇒ `false`.
    pub fn validate(&self, token: &str) -> bool {
        let header = match decode_header(token) {
            Ok(h) => h,
            Err(_) => return false,
        };
        // Only RSA-signed tokens (Keycloak's RS256 family); never `none`/HMAC — an
        // attacker must not be able to downgrade the algorithm.
        if !matches!(
            header.alg,
            Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512
        ) {
            return false;
        }
        let kid = match header.kid {
            Some(k) => k,
            None => return false,
        };

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);
        // exp is validated by default; require it to be present.
        validation.required_spec_claims = ["exp"].iter().map(|s| s.to_string()).collect();

        // Fast path: kid already cached.
        {
            let guard = match self.keys.read() {
                Ok(g) => g,
                Err(_) => return false,
            };
            if let Some(key) = guard.get(&kid) {
                return decode::<Claims>(token, key, &validation).is_ok();
            }
        }
        // kid miss ⇒ a possible signing-key rotation: re-fetch once, then retry.
        if self.refresh().is_err() {
            return false;
        }
        let guard = match self.keys.read() {
            Ok(g) => g,
            Err(_) => return false,
        };
        match guard.get(&kid) {
            Some(key) => decode::<Claims>(token, key, &validation).is_ok(),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::configured_identity;

    #[test]
    fn jwt_configuration_is_absent_without_an_issuer() {
        assert_eq!(configured_identity(None, None, None).unwrap(), None);
    }

    #[test]
    fn jwt_configuration_requires_explicit_audience_and_jwks() {
        let issuer = Some("https://identity.example.test".to_string());
        assert!(configured_identity(issuer.clone(), None, None).is_err());
        assert!(configured_identity(issuer, Some("runtime-audience".to_string()), None).is_err());
    }

    #[test]
    fn jwt_configuration_accepts_a_complete_provider_neutral_contract() {
        let configured = configured_identity(
            Some("https://identity.example.test".to_string()),
            Some("runtime-audience".to_string()),
            Some("https://identity.example.test/keys".to_string()),
        )
        .unwrap();
        assert!(configured.is_some());
    }
}
