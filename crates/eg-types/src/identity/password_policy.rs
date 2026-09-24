//! The password policy (§3.3, NIST 800-63B): length only, no composition
//! rules, and refusal of the account's own names and of well-known breached
//! passwords. Evaluated at the request boundary, where the candidate exists;
//! the store never sees it.

use super::config::MAX_PASSWORD_CHARS;
use super::IdentityRefusal;

/// A small bundled list of the most common breached passwords. The design's
/// full offline corpus (top-100k) is a packaging follow-up; this list refuses
/// the passwords every credential-stuffing run tries first.
const COMMON_PASSWORDS: [&str; 40] = [
    "123456789012", "password1234", "qwertyuiop12", "111111111111", "123123123123",
    "000000000000", "passwordpassword", "iloveyou1234", "abc123abc123", "letmein12345",
    "welcome12345", "administrator", "changeme1234", "qwerty123456", "1q2w3e4r5t6y",
    "password123!", "p@ssw0rd1234", "trustno11234", "monkey123456", "dragon123456",
    "football1234", "baseball1234", "superman1234", "sunshine1234", "princess1234",
    "starwars1234", "whatever1234", "qazwsxedcrfv", "zaq12wsxcde3", "1qaz2wsx3edc",
    "asdfghjkl123", "zxcvbnm12345", "master123456", "shadow123456", "michael12345",
    "123qweasdzxc", "password0000", "adminadmin12", "rootroot1234", "graphos12345",
];

/// Whether `candidate` is acceptable for an account named `username` (and
/// e-mail `email`) under a minimum of `min_chars` characters.
pub fn check_password(
    candidate: &str,
    username: &str,
    email: Option<&str>,
    min_chars: u32,
) -> Result<(), IdentityRefusal> {
    let chars = candidate.chars().count();
    if chars < min_chars as usize || chars > MAX_PASSWORD_CHARS {
        return Err(IdentityRefusal::WeakPassword);
    }
    let lowered = candidate.to_lowercase();
    let email_local = email.and_then(|email| email.split('@').next()).unwrap_or("");
    let names_itself = lowered == username.to_lowercase()
        || (!email_local.is_empty() && lowered.contains(&email_local.to_lowercase()))
        || (username.len() >= 4 && lowered.contains(&username.to_lowercase()));
    if names_itself || COMMON_PASSWORDS.contains(&lowered.as_str()) {
        return Err(IdentityRefusal::WeakPassword);
    }
    Ok(())
}
