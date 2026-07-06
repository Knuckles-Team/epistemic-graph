//! Keycloak JWT validation for the KV-cache HTTP surface (CONCEPT:EG-KG.backend.is-configured-so-co).
//!
//! Pairs the KV surface's auth with the platform's OVERALL auth (the same Keycloak
//! client-credentials tokens graph-os validates inbound and mints outbound for the
//! `*-mcp` fleet) instead of a separate/static mechanism. A client (the
//! `EpistemicGraphKVBackend` connector, vLLM/LMCache) presents a Keycloak access
//! token as `Authorization: Bearer <jwt>`; we verify its RSA signature against the
//! realm's JWKS, plus issuer, audience, and expiry.
//!
//! Only compiled under `--features kvcache-server` (never a `pi` build), which is
//! also where the `ureq`/rustls + `jsonwebtoken`/ring crypto stack lives — so the
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
/// Env: explicit JWKS URL override (else derived from the issuer, Keycloak layout).
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
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("jwks_url", &self.jwks_url)
            .field(
                "cached_kids",
                &self.keys.read().map(|m| m.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl JwtValidator {
    /// Build from the environment. Returns `None` (⇒ JWT mode off, fall back to the
    /// static-token guard) unless an issuer is configured.
    pub fn from_env() -> Option<Self> {
        let issuer = env_first(ISSUER_ENVS)?;
        let audience = env_first(AUDIENCE_ENVS).unwrap_or_else(|| "agent-services".to_string());
        let jwks_url = env_first(&[JWKS_URL_ENV]).unwrap_or_else(|| {
            format!(
                "{}/protocol/openid-connect/certs",
                issuer.trim_end_matches('/')
            )
        });
        let v = JwtValidator {
            issuer,
            audience,
            jwks_url,
            keys: RwLock::new(HashMap::new()),
        };
        // Prime the key cache once; a failure here is non-fatal (validate() re-fetches
        // on demand) but we log it so a misconfigured JWKS URL is visible at boot.
        if let Err(e) = v.refresh() {
            tracing::warn!(
                "kvcache-server: JWKS prime failed ({}) for {} — will retry on first token",
                e,
                v.jwks_url
            );
        } else {
            tracing::info!(
                "kvcache-server: JWT auth armed (issuer={}, audience={}, jwks={})",
                v.issuer,
                v.audience,
                v.jwks_url
            );
        }
        Some(v)
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
