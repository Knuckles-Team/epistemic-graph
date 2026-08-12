//! Shared OIDC/JWKS bearer-token verification core (feature `oidc`).
//!
//! One RSA-signature-against-JWKS verifier (issuer/audience/expiry,
//! Keycloak-compatible) reused by three independently-configured surfaces:
//!
//! * the ancillary KV-cache HTTP surface (`server::kvcache_http`,
//!   `EPISTEMIC_GRAPH_KVCACHE_JWT_*`) — [`JwtValidator::from_env`] +
//!   [`JwtValidator::validate`] (a bare bool: does this bearer token belong to
//!   a caller allowed to touch KV pages);
//! * the `/sparql` SELECT/CONSTRUCT/ASK read leg (`server::sparql_http`, A18,
//!   `EPISTEMIC_GRAPH_SPARQL_JWT_*`) — [`JwtValidator::from_env_sparql`] +
//!   [`JwtValidator::validate`], the SAME bearer/JWT credential SHAPE as the
//!   KV-cache surface (a caller entitled to KV pages is not automatically
//!   entitled to run federated SPARQL reads, so it is independently
//!   configured rather than sharing the KV-cache surface's issuer);
//! * the primary `eg2.` request envelope (`server::auth`,
//!   `EPISTEMIC_GRAPH_OIDC_JWT_*`) — [`JwtValidator::from_env_primary`] +
//!   [`JwtValidator::validate_claims`] (the verified subject/tenant/roles/
//!   scopes, so `server::auth` can bind the envelope's SELF-ASSERTED
//!   principal/tenant/roles/scopes to a REAL external identity and reject a
//!   mismatch — closing the gap where a holder of the shared HMAC secret
//!   could otherwise claim to be anyone).
//!
//! All three entry points share one verification core ([`JwtValidator::decode_with`])
//! so a signature/issuer/audience/expiry check behaves identically everywhere;
//! only the claim projection differs (a bare boolean vs. a typed claim set).
//! This module writes NO signature/JWKS code of its own beyond what already
//! shipped for the KV-cache surface — each additional surface only adds an
//! independently configured identity over the same checks.
//!
//! JWKS keys are primed once at startup and lazily re-fetched on a `kid` miss so
//! a Keycloak signing-key rotation self-heals without a restart. Validation
//! NEVER panics — any failure is a rejected request (`None`/`false`), the
//! fail-closed posture.

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;

/// Env: explicit KV-cache-surface issuer, else reuse the platform's inbound-JWT
/// / OIDC issuer.
const ISSUER_ENVS: &[&str] = &[
    "EPISTEMIC_GRAPH_KVCACHE_JWT_ISSUER",
    "FASTMCP_SERVER_AUTH_JWT_ISSUER",
    "OIDC_ISSUER",
];
/// Env: explicit KV-cache-surface audience, else the platform's fleet audience.
const AUDIENCE_ENVS: &[&str] = &[
    "EPISTEMIC_GRAPH_KVCACHE_JWT_AUDIENCE",
    "FASTMCP_SERVER_AUTH_JWT_AUDIENCE",
    "OIDC_AUDIENCE",
];
/// Env: explicit KV-cache-surface JWKS URL. Discovery and vendor-specific URL
/// construction belong at the deployment boundary; the engine never guesses an
/// identity-provider layout.
const JWKS_URL_ENV: &str = "EPISTEMIC_GRAPH_KVCACHE_JWKS_URL";

/// Env: explicit `/sparql` SELECT/CONSTRUCT/ASK read-leg issuer (A18), else
/// reuse the platform's inbound-JWT / OIDC issuer. Independent of the
/// KV-cache surface's issuer — a caller entitled to KV pages is not
/// automatically entitled to run federated SPARQL reads.
const SPARQL_ISSUER_ENVS: &[&str] = &[
    "EPISTEMIC_GRAPH_SPARQL_JWT_ISSUER",
    "FASTMCP_SERVER_AUTH_JWT_ISSUER",
    "OIDC_ISSUER",
];
/// Env: explicit `/sparql` read-leg audience, else the platform's fleet audience.
const SPARQL_AUDIENCE_ENVS: &[&str] = &[
    "EPISTEMIC_GRAPH_SPARQL_JWT_AUDIENCE",
    "FASTMCP_SERVER_AUTH_JWT_AUDIENCE",
    "OIDC_AUDIENCE",
];
/// Env: explicit `/sparql` read-leg JWKS URL. No generic fallback, same
/// reasoning as [`JWKS_URL_ENV`].
const SPARQL_JWKS_URL_ENV: &str = "EPISTEMIC_GRAPH_SPARQL_JWKS_URL";

/// Env: explicit primary-`eg2.`-protocol issuer, else the platform's shared
/// `OIDC_ISSUER`. Independent of the KV-cache surface's issuer — a deployment
/// may point the two at different realms/audiences, though most will share one.
const PRIMARY_ISSUER_ENVS: &[&str] = &["EPISTEMIC_GRAPH_OIDC_JWT_ISSUER", "OIDC_ISSUER"];
/// Env: explicit primary-protocol audience, else the platform's `OIDC_AUDIENCE`.
const PRIMARY_AUDIENCE_ENVS: &[&str] = &["EPISTEMIC_GRAPH_OIDC_JWT_AUDIENCE", "OIDC_AUDIENCE"];
/// Env: explicit primary-protocol JWKS URL. No generic fallback, same reasoning
/// as [`JWKS_URL_ENV`].
const PRIMARY_JWKS_URL_ENV: &str = "EPISTEMIC_GRAPH_OIDC_JWKS_URL";

fn env_first(names: &[&str]) -> Option<String> {
    names.iter().find_map(|n| {
        std::env::var(n)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

/// Minimal claims for the bare validity check — signature/iss/aud/exp are
/// enforced by [`Validation`]; [`JwtValidator::validate`] needs no claim value
/// beyond that, so this is deliberately empty. [`BindingClaims`] is the richer
/// projection [`JwtValidator::validate_claims`] uses.
#[derive(Debug, Deserialize)]
struct Claims {}

/// Claim shape read for primary-protocol identity binding. Signature/iss/aud/
/// exp are already enforced by [`Validation`] before this is ever populated
/// (see [`JwtValidator::decode_with`]) — this only projects the claim body.
///
/// Deliberately NOT `deny_unknown_fields`: a real access token carries many
/// other standard/vendor claims (`iat`, `jti`, `azp`, `email`, ...) this
/// module does not model.
///
/// Claim locations mirror agent-utilities' `actor_from_claims`
/// (`agent_utilities/security/request_identity.py`) so both verification
/// boundaries in this ecosystem agree on where subject/tenant/roles/scopes
/// live in a Keycloak-issued access token.
#[derive(Debug, Deserialize)]
struct BindingClaims {
    /// Authenticated subject — the identity anchor. Bound against the
    /// envelope's `principal` (the raw, pre-delegation caller), never
    /// `agent_id` (the possibly-delegated effective actor; delegation is
    /// verified separately by `validate_context_claims`'s chain check).
    sub: String,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    tenant: Option<String>,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    tid: Option<String>,
    #[serde(default)]
    org: Option<String>,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(default)]
    realm_access: Option<RealmAccess>,
    #[serde(default)]
    resource_access: Option<HashMap<String, ResourceAccessEntry>>,
    /// Standard OAuth2 space-delimited scope claim.
    #[serde(default)]
    scope: Option<String>,
    /// Some issuers use `scp` (a list or a space-delimited string) instead of
    /// `scope`; only the space-delimited-string shape is projected here,
    /// matching this deployment's issuer convention.
    #[serde(default)]
    scp: Option<String>,
}

/// Keycloak realm-level role grants (`realm_access.roles`).
#[derive(Debug, Deserialize, Default)]
struct RealmAccess {
    #[serde(default)]
    roles: Vec<String>,
}

/// Keycloak client-level role grants (one entry of `resource_access`).
#[derive(Debug, Deserialize, Default)]
struct ResourceAccessEntry {
    #[serde(default)]
    roles: Vec<String>,
}

/// Verified bearer-token claims projected for `eg2.` identity binding. Every
/// field here has already passed RSA/JWKS signature, issuer, audience, and
/// expiry verification — this is trusted data, not caller input.
#[derive(Debug, Clone, Default)]
pub(crate) struct VerifiedTokenClaims {
    pub(crate) subject: String,
    pub(crate) tenant: Option<String>,
    pub(crate) roles: HashSet<String>,
    pub(crate) scopes: HashSet<String>,
}

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
        .ok_or_else(|| "OIDC JWT issuer requires an explicit configured audience".to_string())?;
    let jwks_url = jwks_url
        .ok_or_else(|| "OIDC JWT issuer requires an explicit configured JWKS URL".to_string())?;
    Ok(Some((issuer, audience, jwks_url)))
}

impl JwtValidator {
    /// Build the KV-cache HTTP surface's validator from the environment. No
    /// issuer means JWT mode is not selected. Once an issuer is present,
    /// audience and JWKS URL are mandatory and any incomplete configuration is
    /// an error rather than a deployment-specific fallback.
    pub fn from_env() -> Result<Option<Self>, String> {
        Self::from_envs("kvcache-server", ISSUER_ENVS, AUDIENCE_ENVS, JWKS_URL_ENV)
    }

    /// Build the `/sparql` SELECT/CONSTRUCT/ASK read leg's validator from the
    /// environment (A18, `server::sparql_http`). Independently configured
    /// from [`from_env`](Self::from_env)'s KV-cache surface and
    /// [`from_env_primary`](Self::from_env_primary)'s primary `eg2.` envelope
    /// — all three may point at different realms/audiences, though most
    /// deployments will share one. No issuer configured ⇒ `Ok(None)`: the
    /// read leg falls back to (or stays on) the static-secret /
    /// unconfigured-and-denied posture.
    pub fn from_env_sparql() -> Result<Option<Self>, String> {
        Self::from_envs(
            "sparql-http-client",
            SPARQL_ISSUER_ENVS,
            SPARQL_AUDIENCE_ENVS,
            SPARQL_JWKS_URL_ENV,
        )
    }

    /// Build the primary `eg2.` protocol's OIDC identity-binding validator
    /// from the environment. Independently configured from
    /// [`from_env`](Self::from_env)'s KV-cache surface (the two may point at
    /// different realms/audiences) but falls back to the same platform-wide
    /// `OIDC_ISSUER`/`OIDC_AUDIENCE` when a dedicated value is not set. No
    /// issuer configured ⇒ `Ok(None)`: `eg2.` keeps today's HMAC-only
    /// behavior (unauthenticated local/dev deployments are unaffected).
    pub(crate) fn from_env_primary() -> Result<Option<Self>, String> {
        Self::from_envs(
            "eg2-oidc",
            PRIMARY_ISSUER_ENVS,
            PRIMARY_AUDIENCE_ENVS,
            PRIMARY_JWKS_URL_ENV,
        )
    }

    fn from_envs(
        label: &str,
        issuer_envs: &[&str],
        audience_envs: &[&str],
        jwks_url_env: &str,
    ) -> Result<Option<Self>, String> {
        let Some((issuer, audience, jwks_url)) = configured_identity(
            env_first(issuer_envs),
            env_first(audience_envs),
            env_first(&[jwks_url_env]),
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
        // Prime the key cache once; a failure here is non-fatal (validate()
        // re-fetches on demand) but we log it so a misconfigured JWKS URL is
        // visible at boot.
        if v.refresh().is_err() {
            tracing::warn!("{label}: JWKS prime failed; will retry on first token");
        } else {
            tracing::info!("{label}: JWT auth armed");
        }
        Ok(Some(v))
    }

    /// Construct directly from an already-resolved issuer/audience/key set —
    /// the network-free path tests use to exercise the exact same signature/
    /// issuer/audience/expiry checks a live JWKS fetch would produce, without
    /// a real HTTP server. `jwks_url` is left empty, so a `kid` miss fails
    /// closed (no network fetch is attempted) rather than refreshing.
    #[cfg(test)]
    pub(crate) fn from_parts(
        issuer: impl Into<String>,
        audience: impl Into<String>,
        keys: HashMap<String, DecodingKey>,
    ) -> Self {
        JwtValidator {
            issuer: issuer.into(),
            audience: audience.into(),
            jwks_url: String::new(),
            keys: RwLock::new(keys),
        }
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

    /// Network-free verification core: RSA signature (against the cached JWKS
    /// key) + issuer + audience + expiry. Every public entry point funnels
    /// through this so a live HTTP-fetched JWKS and a test fixture key are
    /// checked identically. Fail-closed: any error ⇒ `None`.
    fn decode_with<T: serde::de::DeserializeOwned>(&self, token: &str) -> Option<T> {
        let header = decode_header(token).ok()?;
        // Only RSA-signed tokens (Keycloak's RS256 family); never `none`/HMAC —
        // an attacker must not be able to downgrade the algorithm.
        if !matches!(
            header.alg,
            Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512
        ) {
            return None;
        }
        let kid = header.kid?;

        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);
        // exp is validated by default; require it to be present.
        validation.required_spec_claims = ["exp"].iter().map(|s| s.to_string()).collect();

        // Fast path: kid already cached.
        {
            let guard = self.keys.read().ok()?;
            if let Some(key) = guard.get(&kid) {
                return decode::<T>(token, key, &validation)
                    .ok()
                    .map(|data| data.claims);
            }
        }
        // kid miss ⇒ a possible signing-key rotation: re-fetch once, then retry.
        if self.refresh().is_err() {
            return None;
        }
        let guard = self.keys.read().ok()?;
        let key = guard.get(&kid)?;
        decode::<T>(token, key, &validation)
            .ok()
            .map(|data| data.claims)
    }

    /// Validate a bearer token: RSA signature (against a cached JWKS key) +
    /// issuer + audience + expiry. Fail-closed: any error ⇒ `false`.
    pub fn validate(&self, token: &str) -> bool {
        self.decode_with::<Claims>(token).is_some()
    }

    /// Validate a bearer token and project its claims for `eg2.` primary-
    /// protocol identity binding. Same fail-closed checks as
    /// [`validate`](Self::validate) — this additionally reads the claim body
    /// so the caller can bind it against the envelope's asserted identity.
    /// `None` on any validation failure.
    pub(crate) fn validate_claims(&self, token: &str) -> Option<VerifiedTokenClaims> {
        let claims = self.decode_with::<BindingClaims>(token)?;
        let mut roles: HashSet<String> = claims.roles.into_iter().collect();
        if let Some(realm) = claims.realm_access {
            roles.extend(realm.roles);
        }
        if let Some(resource) = claims.resource_access {
            for entry in resource.into_values() {
                roles.extend(entry.roles);
            }
        }
        let mut scopes = HashSet::new();
        if let Some(scope) = claims.scope.as_deref() {
            scopes.extend(scope.split_whitespace().map(str::to_string));
        }
        if let Some(scp) = claims.scp.as_deref() {
            scopes.extend(scp.split_whitespace().map(str::to_string));
        }
        let tenant = claims
            .tenant_id
            .or(claims.tenant)
            .or(claims.org_id)
            .or(claims.tid)
            .or(claims.org);
        Some(VerifiedTokenClaims {
            subject: claims.sub,
            tenant,
            roles,
            scopes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use std::time::{SystemTime, UNIX_EPOCH};

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

    // ── real RSA sign/verify round trips ────────────────────────────────
    //
    // RSA-2048 keypair generated solely for this test suite (`openssl genrsa`)
    // and used nowhere else — not a production key, not committed key
    // material for any real deployment. Private key: PKCS#1 `RSAPrivateKey`
    // DER (RFC 8017), matching what `aws_lc_rs::signature::RsaKeyPair::from_der`
    // (the backend `EncodingKey::from_rsa_der` feeds) expects.

    const TEST_RSA_PRIVATE_KEY_PKCS1_DER_HEX: &str = "308204a30201000282010100be4725fd791744d873c4c82cc04ba74db85707a72581e4773e3f9041531b15ea57dcccda092adecbfa818521f10de4f849de2f6b359a20ad4eeec7da6aa550baf49a8f471089348b5c677a4c3d9b7f027395d3a08fa87345e4f842d3f5e6d9846f139883cb9ed94e1a868f85a741a5cb1262beaa4b395c6f9bc82fc46e65267cd50d7d752d2194b69a03ca41f3c135a9862f48d7697f74e8da8dca840cdf4f2cda9addc48ea6445574ffbc79f23144a520ba9aaa3ea8b549c25a89188a869a8ee7f05a096a66bfa4f49d4b5900f49579e88da8c25da9baea53f93cb69e744e5d80b55a41e0de41449bb437b53b57f6ef179eae0b3815a20b1df65fbdf28fc3b7020301000102820100019495093241f2381b5b62ba3f17f71a1b2785e5bfd700af1e323da027f0e2a6b6a21bdacd16b1110aa746becdc21573c67bf4f2dead700b60761fecd2d3f0040d820c7744f8e419d58e4fcd65a443fd7638f95aad0c1e20fcd23463e44d4d8ddf0a4fa0509c4f7bbeebfd31d95374981232b06e0e5539f7a75895fa50b1c061bcb1816d44e1c9155192cc37707747c6abf0af131a3b7d94a774fdc8a491d949ca0049b5845aca493b71352800d31d6f8d4e6beb352571f1586e9c9184a7a691cc556e53953ac5fc7995fed28d0fd92918b2dac30a4892595f70083f18d42a8768bb76077625bc917b347a8c3ec245db23f0eaaebeff571a7141891df5aa380102818100f6cae082d13337d73a723d4672f5a8b7113dfc820251e05380a672055c27dbab82c044f73fdb5d1a3fce5894fda55e57372fcf5f2704ee0ae927fd73c0e80eead6832d5a5938c3c63e69cab78d53e15b535d8a724e93eadf2d9ad45ce6bd2ae3653d087583fd0c7c8e9dac3c33c1f5bc651a2f69f898c379cc3722a85a163c0102818100c5607fbcc1a5a3ae9fa1a3c2469c17dd6d402515ecc724957d7fec575517254acf1dfc70c915390d8f489fae188c17372548603d442b06ad8195c74f8ee8bf51cfa22a2b4740d9e43e35d1942e4e4be545baf43127910c1c7e983f0f5ff5852f85311a56dc8d27fb1b5f669b0f7e83971f99ada964c1f4c6233299a84666dfb702818100d4186938a417d37eca4111be30e044fe07f870c13ec324fa3e8f4d60a3e1b15d46027d82cc4377512ed2e4b82f00e702277094549f51124f18300117710b3e7ebe9a7fe8acd3271581e02392fa07c39e5c1800fad9e32fb05c1e3b32182f2ce3bec6e4353298d0195febcbf0f53e553572e23d2b62b5cf1126db9f9275d1b40102818001a2a60c4b527303bc60db797d9a477c572e63e045a0f4c5a44f8e06bf36bce15ccbf3ce7f6c0497ff2aebdfc6664abef339214b00a8969a936b49467879a734275341a43027f26638b9bb6dcde06a32911c566f9dd34ed5619b23529e49eb7b944feed6ef66e000ed9e21bc81295c2fc15c459b14b1a2b48d901ac3d129830b0281807b5d9e95bf0e2892ff7ee7251fa14bec34d00c031d216c0f06dfa698407ec750e3d357e800907812a61d90281ce93320ad4a50d33364429710f249b87bc925ba89c5f675ed99229d09399943934811b25f4bac5a6cba9303dcd82ccbd31216092e1b9fe5ab1921188bd3e96256c692602be876e09c919c04735638b19646a658";
    const TEST_RSA_MODULUS_HEX: &str = "BE4725FD791744D873C4C82CC04BA74DB85707A72581E4773E3F9041531B15EA57DCCCDA092ADECBFA818521F10DE4F849DE2F6B359A20AD4EEEC7DA6AA550BAF49A8F471089348B5C677A4C3D9B7F027395D3A08FA87345E4F842D3F5E6D9846F139883CB9ED94E1A868F85A741A5CB1262BEAA4B395C6F9BC82FC46E65267CD50D7D752D2194B69A03CA41F3C135A9862F48D7697F74E8DA8DCA840CDF4F2CDA9ADDC48EA6445574FFBC79F23144A520BA9AAA3EA8B549C25A89188A869A8EE7F05A096A66BFA4F49D4B5900F49579E88DA8C25DA9BAEA53F93CB69E744E5D80B55A41E0DE41449BB437B53B57F6EF179EAE0B3815A20B1DF65FBDF28FC3B7";
    const TEST_RSA_EXPONENT_HEX: &str = "010001";

    const ISSUER: &str = "https://identity.example.test/realms/eg";
    const AUDIENCE: &str = "epistemic-graph";
    const KID: &str = "test-kid-1";

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn sign(kid: &str, claims: &serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        let der = hex::decode(TEST_RSA_PRIVATE_KEY_PKCS1_DER_HEX).expect("test key hex");
        let key = EncodingKey::from_rsa_der(&der);
        encode(&header, claims, &key).expect("sign test token")
    }

    fn validator() -> JwtValidator {
        let mut keys = HashMap::new();
        let n = hex::decode(TEST_RSA_MODULUS_HEX).expect("modulus hex");
        let e = hex::decode(TEST_RSA_EXPONENT_HEX).expect("exponent hex");
        keys.insert(
            KID.to_string(),
            DecodingKey::from_rsa_raw_components(&n, &e),
        );
        JwtValidator::from_parts(ISSUER, AUDIENCE, keys)
    }

    fn base_claims() -> serde_json::Value {
        serde_json::json!({
            "sub": "agent:planner",
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": now() + 300,
            "tenant_id": "tenant-a",
            "roles": ["kg:read"],
            "scope": "kg:read kg:write",
        })
    }

    #[test]
    fn valid_token_verifies_and_projects_claims() {
        let token = sign(KID, &base_claims());
        assert!(validator().validate(&token));
        let claims = validator().validate_claims(&token).expect("claims");
        assert_eq!(claims.subject, "agent:planner");
        assert_eq!(claims.tenant.as_deref(), Some("tenant-a"));
        assert!(claims.roles.contains("kg:read"));
        assert!(claims.scopes.contains("kg:read"));
        assert!(claims.scopes.contains("kg:write"));
    }

    #[test]
    fn tampered_signature_fails_closed() {
        let mut token = sign(KID, &base_claims());
        token.push('x'); // corrupt the signature segment
        assert!(!validator().validate(&token));
        assert!(validator().validate_claims(&token).is_none());
    }

    #[test]
    fn expired_token_fails_closed() {
        let mut claims = base_claims();
        // Comfortably past jsonwebtoken's default 60s leeway (clock-skew
        // tolerance), so this is unambiguously expired rather than sitting on
        // the leeway boundary.
        claims["exp"] = serde_json::json!(now() - 3600);
        let token = sign(KID, &claims);
        assert!(!validator().validate(&token));
        assert!(validator().validate_claims(&token).is_none());
    }

    #[test]
    fn wrong_audience_fails_closed() {
        let mut claims = base_claims();
        claims["aud"] = serde_json::json!("some-other-service");
        let token = sign(KID, &claims);
        assert!(!validator().validate(&token));
        assert!(validator().validate_claims(&token).is_none());
    }

    #[test]
    fn wrong_issuer_fails_closed() {
        let mut claims = base_claims();
        claims["iss"] = serde_json::json!("https://not-trusted.example.test/realms/eg");
        let token = sign(KID, &claims);
        assert!(!validator().validate(&token));
    }

    #[test]
    fn missing_exp_claim_fails_closed() {
        let mut claims = base_claims();
        claims.as_object_mut().unwrap().remove("exp");
        let token = sign(KID, &claims);
        assert!(!validator().validate(&token));
    }

    #[test]
    fn unknown_kid_fails_closed_without_a_reachable_jwks_url() {
        // validator()'s jwks_url is empty (test fixture) so the kid-miss
        // refresh path fails closed rather than serving a stale/absent key.
        let token = sign("some-other-kid", &base_claims());
        assert!(!validator().validate(&token));
    }
}
