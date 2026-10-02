//! The identity store's cryptography, all at the request boundary:
//! argon2id for passwords, domain-separated SHA-256 for high-entropy tokens,
//! sealing of TOTP secrets, and RFC 6238 TOTP verification.
//!
//! Nothing here keeps a secret: every function takes the plaintext by
//! reference and returns a hash, a verdict or a sealed blob.

use std::sync::OnceLock;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// argon2id parameters (§3.3): 64 MiB, 3 passes, 1 lane. Tests use a small
/// cost so the suite stays fast; the verdicts are identical.
#[cfg(not(test))]
const COST: (u32, u32, u32) = (64 * 1024, 3, 1);
#[cfg(test)]
const COST: (u32, u32, u32) = (1024, 1, 1);

fn hasher() -> Result<Argon2<'static>, String> {
    let (memory_kib, passes, lanes) = COST;
    let params = Params::new(memory_kib, passes, lanes, None).map_err(|error| error.to_string())?;
    Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
}

/// The argon2id PHC string of `password` under a fresh random salt.
pub(super) fn hash_password(password: &str) -> Result<String, String> {
    let mut salt = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut salt);
    let salt = SaltString::encode_b64(&salt).map_err(|error| error.to_string())?;
    hasher()?
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|error| error.to_string())
}

/// A fixed hash verified against when the username is unknown, so a sign-in
/// costs the same whether or not the account exists.
fn dummy_hash() -> &'static str {
    static DUMMY: OnceLock<String> = OnceLock::new();
    DUMMY.get_or_init(|| hash_password("eg-identity-dummy-password").unwrap_or_default())
}

/// Whether `candidate` matches `stored` (or the dummy hash when `None`, which
/// never matches). An unparseable stored hash never matches.
pub(super) fn verify_password(candidate: &str, stored: Option<&str>) -> bool {
    let Ok(hasher) = hasher() else {
        return false;
    };
    let target = stored.unwrap_or_else(|| dummy_hash());
    let verified = PasswordHash::new(target)
        .map(|parsed| {
            hasher
                .verify_password(candidate.as_bytes(), &parsed)
                .is_ok()
        })
        .unwrap_or(false);
    verified && stored.is_some()
}

/// Whether a stored hash uses other parameters than the current cost (and
/// should be replaced on the next successful sign-in).
pub(super) fn is_stale(stored: &str) -> bool {
    let (memory_kib, passes, lanes) = COST;
    let current = PasswordHash::new(stored)
        .ok()
        .and_then(|parsed| Params::try_from(&parsed).ok())
        .map(|params| (params.m_cost(), params.t_cost(), params.p_cost()));
    current != Some((memory_kib, passes, lanes))
}

/// The stored hash of a high-entropy token (a session id, a one-time token,
/// an API-key secret, a recovery code).
pub(super) fn token_hash(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/identity-token/v1\0");
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

/// Where a sealed TOTP secret's key came from.
const SEALED_AT_REST: &str = "k1:";
const SEALED_SERVICE: &str = "s1:";

type KeySource = (crate::crypto::ValueCipher, &'static str);

/// The configured data key, else a key derived from the service secret
/// (MFA is optional for everyone, so it must not depend on at-rest
/// encryption being configured).
fn key_source(at_rest: Option<crate::crypto::ValueCipher>, service_secret: &str) -> KeySource {
    if let Some(cipher) = at_rest {
        return (cipher, SEALED_AT_REST);
    }
    let mut material = b"eg/identity/mfa-seal/v1\0".to_vec();
    material.extend_from_slice(service_secret.as_bytes());
    (
        crate::crypto::ValueCipher::from_key_material(&material),
        SEALED_SERVICE,
    )
}

fn configured_key_source(service_secret: &str) -> Result<KeySource, String> {
    Ok(key_source(
        crate::crypto::ValueCipher::from_env_checked()?,
        service_secret,
    ))
}

fn seal_with((cipher, tag): &KeySource, secret: &[u8]) -> String {
    format!("{tag}{}", hex::encode(cipher.seal(secret)))
}

fn unseal_with((cipher, tag): &KeySource, sealed: &str) -> Result<Vec<u8>, String> {
    let body = sealed
        .strip_prefix(*tag)
        .ok_or("the sealed secret was sealed under another key source")?;
    let bytes = hex::decode(body).map_err(|error| error.to_string())?;
    cipher.unseal(&bytes)
}

pub(super) fn seal(secret: &[u8], service_secret: &str) -> Result<String, String> {
    Ok(seal_with(&configured_key_source(service_secret)?, secret))
}

pub(super) fn unseal(sealed: &str, service_secret: &str) -> Result<Vec<u8>, String> {
    unseal_with(&configured_key_source(service_secret)?, sealed)
}

/// Decode an RFC 4648 base32 secret (no padding, case-insensitive).
pub(super) fn base32_decode(text: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut bits: u64 = 0;
    let mut width = 0u32;
    let mut out = Vec::new();
    for character in text.trim_end_matches('=').bytes() {
        let value = ALPHABET
            .iter()
            .position(|symbol| *symbol == character.to_ascii_uppercase())?;
        bits = (bits << 5) | value as u64;
        width += 5;
        if width >= 8 {
            width -= 8;
            out.push((bits >> width) as u8);
            bits &= (1 << width) - 1;
        }
    }
    Some(out)
}

/// RFC 6238 step length.
const STEP_SECS: u64 = 30;

/// The 6-digit RFC 4226 code of `secret` at `step`.
fn hotp(secret: &[u8], step: u64) -> Option<u32> {
    let mut mac = Hmac::<sha1::Sha1>::new_from_slice(secret).ok()?;
    mac.update(&step.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = usize::from(digest[digest.len() - 1] & 0x0f);
    let window: [u8; 4] = digest.get(offset..offset + 4)?.try_into().ok()?;
    Some((u32::from_be_bytes(window) & 0x7fff_ffff) % 1_000_000)
}

/// The step (±1 around `now_secs`) whose code is `code`, if any.
pub(super) fn totp_step(secret: &[u8], code: &str, now_secs: u64) -> Option<u64> {
    let valid = code.len() == 6 && code.bytes().all(|b| b.is_ascii_digit());
    let wanted: u32 = code.parse().ok().filter(|_| valid)?;
    let now_step = now_secs / STEP_SECS;
    [now_step.saturating_sub(1), now_step, now_step + 1]
        .into_iter()
        .find(|step| hotp(secret, *step) == Some(wanted))
}

#[cfg(test)]
mod tests;
