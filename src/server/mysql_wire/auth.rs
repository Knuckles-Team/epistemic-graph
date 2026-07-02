//! MySQL wire authentication bridged to the engine identity (CONCEPT:EG-076).
//!
//! Mirrors the pgwire auth policy (CONCEPT:KG-2.202) for the MySQL wire: a connection's
//! `user` IS an engine `agent_id`, and its password is a deterministic per-user token
//! DERIVED from the engine's shared `GRAPH_SERVICE_AUTH_SECRET`:
//! `derived_password(user) = hex(HMAC-SHA256(secret, "mysql:" || user))`. An operator
//! who holds the engine secret can compute each agent's MySQL password offline; the
//! engine validates the `mysql_native_password` proof against the same derivation
//! WITHOUT storing per-user passwords. On a successful login the connection's actor is
//! set to `user`, so every subsequent query runs under that `AgentIdentity` against the
//! engine ACL (`IsolationLayer::check_access`).
//!
//! ## Modes (`EPISTEMIC_GRAPH_MYSQL_AUTH`)
//!   * `trust` — no authentication; the zero-infra/dev path. DEFAULT only when no
//!     engine secret is configured (the auth-plugin-data is still sent, but any client
//!     response is accepted and the connection stays anonymous).
//!   * `native` — `mysql_native_password` (what mysql/mariadb clients negotiate by
//!     default). The DEFAULT when a non-empty `GRAPH_SERVICE_AUTH_SECRET` is set.
//!
//! ## The `mysql_native_password` exchange
//! The server sends a 20-byte random scramble (`seed`) in the handshake. The client
//! replies with `SHA1(password) XOR SHA1(seed || SHA1(SHA1(password)))`. The server,
//! which knows `stored = SHA1(SHA1(password))`, validates the proof by recovering
//! `SHA1(password) = client_response XOR SHA1(seed || stored)` and checking
//! `SHA1(that) == stored`. No plaintext (nor `SHA1(password)`) is ever stored.

use hmac::{Hmac, Mac};
use sha1::{Digest, Sha1};
use sha2::Sha256;

/// Env var selecting the MySQL wire auth mode: `trust` | `native`. Unset ⇒ the safe
/// default (native when an engine secret is configured, else trust).
pub const MYSQL_AUTH_ENV: &str = "EPISTEMIC_GRAPH_MYSQL_AUTH";

/// The name of the auth plugin the server advertises + validates.
pub const MYSQL_NATIVE_PASSWORD: &str = "mysql_native_password";

type HmacSha256 = Hmac<Sha256>;

/// The resolved MySQL wire auth mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MysqlAuthMode {
    /// No authentication (dev / zero-infra) — any client response is accepted and the
    /// connection stays anonymous (no ACL actor).
    Trust,
    /// `mysql_native_password` bridged to the engine secret.
    Native,
}

impl MysqlAuthMode {
    /// Resolve the mode from `EPISTEMIC_GRAPH_MYSQL_AUTH` and the engine secret.
    /// Explicit env wins; otherwise default to NATIVE when a non-empty secret is
    /// present (an authenticated deployment), else TRUST (a bare dev run). Mirrors
    /// `PgWireAuthMode::resolve`.
    pub fn resolve(auth_secret: &str) -> Self {
        match std::env::var(MYSQL_AUTH_ENV)
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("native") => MysqlAuthMode::Native,
            Some("trust") => MysqlAuthMode::Trust,
            _ if !auth_secret.is_empty() => MysqlAuthMode::Native,
            _ => MysqlAuthMode::Trust,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            MysqlAuthMode::Trust => "trust",
            MysqlAuthMode::Native => "native",
        }
    }

    /// Whether an authenticated login under this mode maps the connecting `user` to the
    /// engine ACL actor (CONCEPT:KG-2.202). True for NATIVE; false for TRUST (anonymous).
    pub fn maps_actor(&self) -> bool {
        matches!(self, MysqlAuthMode::Native)
    }
}

/// `derived_password(user) = hex(HMAC-SHA256(secret, "mysql:" || user))` — the per-user
/// MySQL password an authorized operator computes from the engine secret. Mirrors
/// pgwire's `derive_pg_password` (different domain-separation prefix).
pub fn derive_mysql_password(secret: &str, user: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"mysql:");
    mac.update(user.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn sha1(bytes: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(bytes);
    h.finalize().into()
}

/// The `mysql_native_password` "stored" digest for a password: `SHA1(SHA1(password))`.
/// This is what the server keeps (or, here, derives on the fly) — never the plaintext.
pub fn native_password_stored(password: &str) -> [u8; 20] {
    sha1(&sha1(password.as_bytes()))
}

/// The client-side `mysql_native_password` scramble for a password + 20-byte seed:
/// `SHA1(password) XOR SHA1(seed || SHA1(SHA1(password)))`. Exposed so an in-process
/// test (and an operator tool) can produce the exact token a real client sends.
pub fn native_password_scramble(password: &str, seed: &[u8]) -> [u8; 20] {
    let hash1 = sha1(password.as_bytes());
    let stored = sha1(&hash1);
    let mut concat = Vec::with_capacity(seed.len() + stored.len());
    concat.extend_from_slice(seed);
    concat.extend_from_slice(&stored);
    let seed_hash = sha1(&concat);
    let mut out = [0u8; 20];
    for (o, (a, b)) in out.iter_mut().zip(hash1.iter().zip(seed_hash.iter())) {
        *o = a ^ b;
    }
    out
}

/// Verify a client's `mysql_native_password` response against the `stored`
/// (`SHA1(SHA1(password))`) digest and the `seed` the server sent. Recovers
/// `SHA1(password) = response XOR SHA1(seed || stored)` and checks
/// `SHA1(that) == stored`. A response of the wrong length never matches.
pub fn verify_native_password(response: &[u8], seed: &[u8], stored: &[u8; 20]) -> bool {
    if response.len() != 20 {
        // An EMPTY response means the client sent no password — only valid if the
        // account itself has an empty password (stored == SHA1(SHA1(""))).
        if response.is_empty() {
            return stored == &native_password_stored("");
        }
        return false;
    }
    let mut concat = Vec::with_capacity(seed.len() + stored.len());
    concat.extend_from_slice(seed);
    concat.extend_from_slice(stored);
    let seed_hash = sha1(&concat);
    let mut hash1 = [0u8; 20];
    for (h, (r, s)) in hash1.iter_mut().zip(response.iter().zip(seed_hash.iter())) {
        *h = r ^ s;
    }
    &sha1(&hash1) == stored
}

/// Verify a login for the NATIVE mode: derive the user's stored digest from the engine
/// secret, then check the client's proof against it. Returns true on success.
pub fn verify_login(secret: &str, user: &str, response: &[u8], seed: &[u8]) -> bool {
    let password = derive_mysql_password(secret, user);
    let stored = native_password_stored(&password);
    verify_native_password(response, seed, &stored)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_password_is_stable_and_secret_dependent() {
        let p1 = derive_mysql_password("s3cret", "agent:planner");
        let p2 = derive_mysql_password("s3cret", "agent:planner");
        assert_eq!(p1, p2, "derivation is deterministic");
        assert_ne!(p1, derive_mysql_password("other", "agent:planner"));
        assert_ne!(p1, derive_mysql_password("s3cret", "agent:worker"));
        assert_eq!(p1.len(), 64, "hex of a 32-byte HMAC");
    }

    #[test]
    fn native_password_roundtrip_verifies() {
        // A client scrambles the derived password against the server's seed; the server
        // validates against the stored SHA1(SHA1(password)) digest.
        let secret = "s3cret";
        let user = "agent:planner";
        let password = derive_mysql_password(secret, user);
        let seed: Vec<u8> = (1u8..=20).collect();
        let scramble = native_password_scramble(&password, &seed);
        assert!(
            verify_login(secret, user, &scramble, &seed),
            "correct proof accepts"
        );
        // A wrong password (different secret) must fail.
        let wrong = native_password_scramble(&derive_mysql_password("other", user), &seed);
        assert!(
            !verify_login(secret, user, &wrong, &seed),
            "wrong proof rejects"
        );
        // A tampered byte must fail.
        let mut bad = scramble;
        bad[0] ^= 0xff;
        assert!(
            !verify_login(secret, user, &bad, &seed),
            "tampered proof rejects"
        );
    }

    #[test]
    fn empty_response_matches_only_empty_password() {
        let seed: Vec<u8> = (0u8..20).collect();
        let empty_stored = native_password_stored("");
        assert!(verify_native_password(&[], &seed, &empty_stored));
        let nonempty_stored = native_password_stored("hunter2");
        assert!(!verify_native_password(&[], &seed, &nonempty_stored));
    }

    #[test]
    fn mode_resolution_default_is_secret_driven() {
        std::env::remove_var(MYSQL_AUTH_ENV);
        assert_eq!(MysqlAuthMode::resolve("s3cret"), MysqlAuthMode::Native);
        assert_eq!(MysqlAuthMode::resolve(""), MysqlAuthMode::Trust);
        std::env::set_var(MYSQL_AUTH_ENV, "trust");
        assert_eq!(MysqlAuthMode::resolve("s3cret"), MysqlAuthMode::Trust);
        std::env::set_var(MYSQL_AUTH_ENV, "native");
        assert_eq!(MysqlAuthMode::resolve(""), MysqlAuthMode::Native);
        std::env::remove_var(MYSQL_AUTH_ENV);
        assert!(MysqlAuthMode::Native.maps_actor());
        assert!(!MysqlAuthMode::Trust.maps_actor());
    }
}
