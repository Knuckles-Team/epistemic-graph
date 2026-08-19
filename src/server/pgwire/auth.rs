//! pg-wire authentication bridged to the engine identity (CONCEPT:EG-KG.query.concept-13).
//!
//! Authentication is mandatory SCRAM-SHA-256, bridged to the engine's existing
//! shared secret (`GRAPH_SERVICE_AUTH_SECRET`) and ACL identity model. There is no
//! anonymous fallback.
//!
//! ## How a pg user maps to an engine identity
//! A pg connection's `user` IS an engine `agent_id`. The connection's password is a
//! deterministic per-user token DERIVED from the engine's shared
//! `GRAPH_SERVICE_AUTH_SECRET`: `derived_password(user) = hex(HMAC-SHA256(secret,
//! "pgwire:" || user))`. An operator who holds the engine secret can compute each
//! agent's pg password offline; the engine validates the SCRAM proof against the
//! same derivation WITHOUT storing per-user passwords. The SCRAM salt is likewise
//! derived deterministically from the secret + user, so the same login always
//! re-validates (no stored salt table). On a successful login the connection's
//! actor is set to `user`, so every subsequent query runs under that
//! `AgentIdentity` against the engine ACL (`IsolationLayer::check_access`).
//!
//! ## Mode (`EPISTEMIC_GRAPH_PGWIRE_AUTH`)
//!   * `scram` — SCRAM-SHA-256 (what modern drivers negotiate). This is the only
//!     accepted mode and requires a non-empty `GRAPH_SERVICE_AUTH_SECRET`.
//!
//! Native TLS paths are configured independently through
//! `EPISTEMIC_GRAPH_PGWIRE_TLS_CERT`, `EPISTEMIC_GRAPH_PGWIRE_TLS_KEY`, and the
//! optional `EPISTEMIC_GRAPH_PGWIRE_TLS_CLIENT_CA` (mTLS) variable.
//!
//! Native TLS is optional for loopback deployments and required before a listener
//! can bind a non-loopback address. When a certificate is configured, SCRAM also
//! advertises `SCRAM-SHA-256-PLUS`, binding clients that select channel binding to
//! the configured server certificate. The SCRAM proof still binds the pg user to
//! the ACL actor independently of the transport identity.
//!
//! The mode is resolved ONCE at `serve()` startup and logged.

use std::sync::Arc;

use async_trait::async_trait;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use pgwire::api::auth::sasl::scram::{gen_salted_password, ScramAuth, SCRAM_ITERATIONS};
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::ClientInfo;
use pgwire::error::PgWireResult;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Env var selecting the pgwire auth mode. Only `scram` is accepted; unset also
/// selects SCRAM.
pub const PGWIRE_AUTH_ENV: &str = "EPISTEMIC_GRAPH_PGWIRE_AUTH";

/// The resolved pgwire auth mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgWireAuthMode {
    /// SCRAM-SHA-256 bridged to the engine secret.
    Scram,
}

impl PgWireAuthMode {
    /// Resolve the sole secure mode. Empty key material and every legacy or
    /// unknown value are startup errors rather than compatibility fallbacks.
    pub fn resolve(auth_secret: &str) -> std::io::Result<Self> {
        if auth_secret.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "pgwire requires non-empty authentication key material",
            ));
        }
        match std::env::var(PGWIRE_AUTH_ENV) {
            Err(std::env::VarError::NotPresent) => Ok(Self::Scram),
            Ok(value) if value.trim().eq_ignore_ascii_case("scram") => Ok(Self::Scram),
            Ok(_) | Err(std::env::VarError::NotUnicode(_)) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "pgwire authentication mode must be scram",
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            PgWireAuthMode::Scram => "scram",
        }
    }

    /// Whether this mode and secret cryptographically bind a successful login's
    /// user to the ACL actor. SCRAM with an empty root secret is not a verified
    /// deployment, even though the protocol exchange itself can run.
    pub fn verified_identity_binding(self, auth_secret: &str) -> bool {
        matches!(self, PgWireAuthMode::Scram) && !auth_secret.is_empty()
    }
}

type HmacSha256 = Hmac<Sha256>;

/// `derived_password(user) = hex(HMAC-SHA256(secret, "pgwire:" || user))` — the
/// per-user pg password an authorized operator computes from the engine secret.
pub fn derive_pg_password(secret: &str, user: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"pgwire:");
    mac.update(user.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// A deterministic 16-byte SCRAM salt derived from the secret + user, so the same
/// login re-validates without a stored salt table.
fn derive_salt(secret: &str, user: &str) -> Vec<u8> {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"pgwire-salt:");
    mac.update(user.as_bytes());
    mac.finalize().into_bytes()[..16].to_vec()
}

/// `AuthSource` that yields the SCRAM-salted form of the derived per-user password
/// (CONCEPT:EG-KG.query.concept-13). For SCRAM, the server stores/returns the SALTED password
/// (`Hi(password, salt, iters)`) plus the salt; pgwire's SCRAM handler verifies the
/// client's proof against it. We compute both deterministically from the engine
/// secret + the connecting user, so no password is ever persisted.
#[derive(Debug)]
struct EngineAuthSource {
    secret: String,
}

#[async_trait]
impl AuthSource for EngineAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        // A missing user can never match a derived password — return a salted form
        // of an empty user so the proof check fails cleanly (no panic, no bypass).
        let user = login.user().unwrap_or("");
        let salt = derive_salt(&self.secret, user);
        let derived = derive_pg_password(&self.secret, user);
        let salted = gen_salted_password(&derived, &salt, SCRAM_ITERATIONS);
        Ok(Password::new(Some(salt), salted))
    }
}

/// The per-connection SCRAM startup handler. One wrapper keeps the
/// `PgWireServerHandlers::startup_handler`
/// return type (`Arc<impl StartupHandler>`) is a single concrete type that dispatches
/// internally — pgwire builds a fresh SASL state machine per connection, so the SCRAM
/// variant is constructed per accepted connection (see `EngineBackendFactory`).
pub enum EngineStartupHandler {
    Scram(Box<SASLAuthStartupHandler<DefaultServerParameterProvider>>),
}

impl EngineStartupHandler {
    /// Build the SCRAM startup handler over the engine key material.
    ///
    /// `certificate_pem` is optional for loopback/plain deployments. When it is
    /// supplied, the handler advertises SCRAM channel binding and verifies the
    /// `tls-server-end-point` proof for clients that select `SCRAM-SHA-256-PLUS`.
    /// Startup validates the PEM before a listener is opened; the `expect` below
    /// therefore protects an already-validated invariant rather than accepting
    /// malformed operator input at connection time.
    pub fn new(mode: PgWireAuthMode, auth_secret: &str, certificate_pem: Option<&[u8]>) -> Self {
        match mode {
            PgWireAuthMode::Scram => {
                let source = Arc::new(EngineAuthSource {
                    secret: auth_secret.to_string(),
                });
                let mut scram = ScramAuth::new(source);
                if let Some(certificate_pem) = certificate_pem {
                    scram
                        .configure_certificate(certificate_pem)
                        .expect("pgwire TLS certificate was validated before listener startup");
                }
                let handler = SASLAuthStartupHandler::new(Arc::new(
                    DefaultServerParameterProvider::default(),
                ))
                .with_scram(scram);
                EngineStartupHandler::Scram(Box::new(handler))
            }
        }
    }
}

/// Validate the exact certificate bytes used by native TLS before a listener is
/// opened. This keeps SCRAM channel-binding failures in the fail-closed startup
/// path instead of discovering an invalid X.509 chain only after a client logs in.
pub fn validate_certificate(certificate_pem: &[u8]) -> PgWireResult<()> {
    let source = Arc::new(EngineAuthSource {
        secret: ["pgwire", "tls", "validation"].join("-"),
    });
    let mut scram = ScramAuth::new(source);
    scram.configure_certificate(certificate_pem)
}

#[async_trait]
impl StartupHandler for EngineStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        pgwire::error::PgWireError: From<<C as futures::Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            EngineStartupHandler::Scram(h) => h.on_startup(client, message).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_password_is_stable_and_secret_dependent() {
        let p1 = derive_pg_password("s3cret", "agent:planner");
        let p2 = derive_pg_password("s3cret", "agent:planner");
        assert_eq!(p1, p2, "derivation is deterministic");
        assert_ne!(
            p1,
            derive_pg_password("other", "agent:planner"),
            "a different secret yields a different password"
        );
        assert_ne!(
            p1,
            derive_pg_password("s3cret", "agent:worker"),
            "a different user yields a different password"
        );
        assert_eq!(p1.len(), 64, "hex of a 32-byte HMAC");
    }

    #[test]
    fn mode_resolution_is_scram_only_and_fail_closed() {
        std::env::remove_var(PGWIRE_AUTH_ENV);
        assert_eq!(
            PgWireAuthMode::resolve("s3cret").unwrap(),
            PgWireAuthMode::Scram
        );
        assert!(PgWireAuthMode::resolve("").is_err());
        std::env::set_var(PGWIRE_AUTH_ENV, "trust");
        assert!(PgWireAuthMode::resolve("s3cret").is_err());
        std::env::set_var(PGWIRE_AUTH_ENV, "scram");
        assert_eq!(
            PgWireAuthMode::resolve("s3cret").unwrap(),
            PgWireAuthMode::Scram
        );
        std::env::remove_var(PGWIRE_AUTH_ENV);
    }

    #[test]
    fn only_scram_with_nonempty_secret_is_verified_identity_binding() {
        assert!(PgWireAuthMode::Scram.verified_identity_binding("secret"));
        assert!(!PgWireAuthMode::Scram.verified_identity_binding(""));
    }
}
