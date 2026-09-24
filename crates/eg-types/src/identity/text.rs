//! Field validation shared by every identity record.

use super::{IdentityRefusal, MIN_TOKEN_CHARS};

/// Longest username (after normalization).
pub const MAX_USERNAME_BYTES: usize = 64;
/// Longest principal id, display name, e-mail, role/group/IdP id or name.
pub const MAX_ID_BYTES: usize = 256;
pub const MAX_NAME_BYTES: usize = 256;
pub const MAX_EMAIL_BYTES: usize = 320;
/// Longest free-text field (descriptions, justifications, config JSON).
pub const MAX_TEXT_BYTES: usize = 4096;
/// Longest caller-supplied high-entropy token.
pub const MAX_TOKEN_CHARS: usize = 512;

/// Normalize a username: trimmed, ASCII-lowercased, and restricted to
/// `[a-z0-9._@+-]`. ASCII-only is deliberate: it removes the homoglyph and
/// Unicode-normalization classes of account-confusion attacks entirely.
pub fn normalize_username(raw: &str) -> Result<String, IdentityRefusal> {
    let lowered = raw.trim().to_ascii_lowercase();
    let allowed = |c: char| c.is_ascii_alphanumeric() || "._@+-".contains(c);
    if lowered.is_empty() || lowered.len() > MAX_USERNAME_BYTES || !lowered.chars().all(allowed) {
        return Err(IdentityRefusal::InvalidRequest);
    }
    Ok(lowered)
}

/// A principal id is opaque but bounded: printable, no whitespace, no
/// control characters. Existing identity-provider subjects (e.g. Keycloak
/// UUIDs) are valid principal ids verbatim (§4 homelab migration).
pub fn validate_principal_id(principal_id: &str) -> Result<(), IdentityRefusal> {
    let printable = principal_id
        .chars()
        .all(|c| !c.is_control() && !c.is_whitespace());
    if principal_id.is_empty() || principal_id.len() > MAX_ID_BYTES || !printable {
        return Err(IdentityRefusal::InvalidRequest);
    }
    Ok(())
}

/// A bounded, non-empty, control-free text field.
pub(crate) fn bounded(value: &str, max: usize) -> Result<(), IdentityRefusal> {
    let clean = !value.trim().is_empty() && !value.chars().any(char::is_control);
    if clean && value.len() <= max {
        Ok(())
    } else {
        Err(IdentityRefusal::InvalidRequest)
    }
}

/// An optional bounded text field.
pub(crate) fn bounded_opt(value: Option<&str>, max: usize) -> Result<(), IdentityRefusal> {
    value.map_or(Ok(()), |text| bounded(text, max))
}

/// An identifier: `[a-z0-9._:-]`, bounded.
pub(crate) fn identifier(value: &str) -> Result<(), IdentityRefusal> {
    let allowed = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit() || "._:-".contains(c);
    if value.is_empty() || value.len() > MAX_ID_BYTES || !value.chars().all(allowed) {
        return Err(IdentityRefusal::InvalidRequest);
    }
    Ok(())
}

/// A caller-generated high-entropy token: URL-safe base64 alphabet, at least
/// [`MIN_TOKEN_CHARS`] characters. The engine cannot measure entropy; the
/// length and alphabet floor refuse the obvious mistakes (a counter, a word).
pub(crate) fn high_entropy_token(value: &str) -> Result<(), IdentityRefusal> {
    let allowed = |c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_';
    let length_ok = (MIN_TOKEN_CHARS..=MAX_TOKEN_CHARS).contains(&value.len());
    if length_ok && value.chars().all(allowed) {
        Ok(())
    } else {
        Err(IdentityRefusal::InvalidRequest)
    }
}

/// A minimal e-mail shape check: one `@`, non-empty local part and domain.
pub(crate) fn email(value: &str) -> Result<(), IdentityRefusal> {
    bounded(value, MAX_EMAIL_BYTES)?;
    match value.split_once('@') {
        Some((local, domain))
            if !local.is_empty()
                && domain.contains('.')
                && !domain.contains('@')
                && !value.contains(char::is_whitespace) =>
        {
            Ok(())
        }
        _ => Err(IdentityRefusal::InvalidRequest),
    }
}
