//! Credential resolution for Q10 provider adapters.
//!
//! ★ Charter: "Credentials go to OpenBao. Never a ConfigMap, never committed, never a
//! plaintext env default." This module does not talk to OpenBao directly -- it
//! follows the SAME convention already established elsewhere in this codebase for
//! exactly this situation (`src/server/s3/mod.rs`'s `S3_SECRET_KEY_ENV`,
//! `src/server/kvcache_http/mod.rs`'s `KVCACHE_TOKEN_ENV`, `src/server/nl.rs`'s
//! `client_secret_env`): **read a named environment variable, with no default**. At
//! deploy time, the k8s layer is responsible for getting the real secret from
//! OpenBao into that env var via the External Secrets Operator -- writing directly to
//! OpenBao, never `kubectl patch`-ing the derived `*-mcp-secrets` Secret, which the
//! operator silently reverts (see this workspace's own incident notes on that
//! failure mode). None of that deploy-time wiring is this crate's concern; this
//! module's only job is to refuse to run with a missing or empty credential rather
//! than silently falling back to some default.

/// Resolves a named credential. The default [`EnvCredentials`] reads process
/// environment variables; tests substitute [`StaticCredentials`] so unit tests never
/// depend on ambient process environment state.
pub trait CredentialSource: Send + Sync {
    fn require(&self, var: &'static str) -> Result<String, CredentialError>;

    /// Convenience: same as `require`, but returns `Ok(None)` instead of erroring
    /// when unset -- for genuinely optional configuration (e.g. a non-default API
    /// base URL), never for a secret itself.
    fn optional(&self, var: &'static str) -> Option<String> {
        self.require(var).ok()
    }
}

#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "required credential env var {0} is not set. This is expected to be populated from \
     OpenBao (via the External Secrets Operator writing a k8s Secret consumed as this \
     container's env) at deploy time -- never a ConfigMap, never committed, never a \
     plaintext default. Local/dev use: export it from `openbao-mcp`/`secret-vault-manager` \
     before enabling the `quantum-hardware` feature."
)]
pub struct CredentialError(pub &'static str);

/// Reads real process environment variables. This is what every provider adapter's
/// `::new()` constructor uses.
#[derive(Debug, Default, Clone, Copy)]
pub struct EnvCredentials;

impl CredentialSource for EnvCredentials {
    fn require(&self, var: &'static str) -> Result<String, CredentialError> {
        match std::env::var(var) {
            Ok(v) if !v.is_empty() => Ok(v),
            _ => Err(CredentialError(var)),
        }
    }
}

/// A fixed, in-memory credential set for tests -- never constructed from a literal
/// secret in production code, only from test fixtures.
#[derive(Debug, Default, Clone)]
pub struct StaticCredentials(std::collections::BTreeMap<&'static str, String>);

impl StaticCredentials {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with(mut self, var: &'static str, value: impl Into<String>) -> Self {
        self.0.insert(var, value.into());
        self
    }
}

impl CredentialSource for StaticCredentials {
    fn require(&self, var: &'static str) -> Result<String, CredentialError> {
        self.0.get(var).cloned().ok_or(CredentialError(var))
    }
}
