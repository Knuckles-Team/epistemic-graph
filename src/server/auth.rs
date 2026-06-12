//! HMAC-SHA256 request authentication.

/// Compute the HMAC-SHA256 hex token for a request id. Shared by `verify_auth`
/// and trusted in-process callers (tests) that dispatch requests without going
/// through a socket client.
pub fn compute_auth_token(secret: &str, request_id: u64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;

    type HmacSha256 = Hmac<Sha256>;

    let Ok(mut mac) = HmacSha256::new_from_slice(secret.as_bytes()) else {
        return String::new();
    };
    mac.update(request_id.to_string().as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Verify HMAC-SHA256 authentication token.
///
/// An empty secret accepts everything — the binary only allows that
/// configuration behind the explicit insecure opt-out enforced at startup
/// (`--allow-insecure` / `EPISTEMIC_GRAPH_ALLOW_INSECURE=1`, see `main.rs`).
pub(crate) fn verify_auth(secret: &str, request_id: u64, token: &str) -> bool {
    if secret.is_empty() {
        return true; // Explicitly opted-out of authentication at startup.
    }
    token == compute_auth_token(secret, request_id)
}
