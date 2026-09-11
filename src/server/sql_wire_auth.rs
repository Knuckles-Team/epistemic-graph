//! Shared configuration and credential primitives for the SQL wire adapters.
//!
//! Protocol modules own their wire-specific authentication exchanges. This
//! module owns only the policy that is identical at that boundary: a mode is
//! resolved after key material is checked, and user credentials are derived
//! with a domain-separated HMAC.

use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

/// Resolve a fail-closed SQL-wire mode while preserving the adapter's public
/// error wording and validation order.
pub(crate) fn resolve_mode(
    auth_secret: &str,
    env_var: &str,
    expected: &str,
    missing_secret_error: &'static str,
    invalid_mode_error: &'static str,
) -> std::io::Result<()> {
    if auth_secret.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            missing_secret_error,
        ));
    }
    match std::env::var(env_var) {
        Err(std::env::VarError::NotPresent) => Ok(()),
        Ok(value) if value.trim().eq_ignore_ascii_case(expected) => Ok(()),
        Ok(_) | Err(std::env::VarError::NotUnicode(_)) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            invalid_mode_error,
        )),
    }
}

/// Derive a fixed-size domain-separated credential digest.
pub(crate) fn derive_hmac_digest(secret: &str, domain: &[u8], principal: &str) -> [u8; 32] {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(domain);
    mac.update(principal.as_bytes());
    let digest = mac.finalize().into_bytes();
    let mut output = [0_u8; 32];
    output.copy_from_slice(&digest);
    output
}

/// Derive the public hex form used by the SQL-wire password conventions.
pub(crate) fn derive_hmac_hex(secret: &str, domain: &[u8], principal: &str) -> String {
    hex::encode(derive_hmac_digest(secret, domain, principal))
}
