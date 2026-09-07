/// Existing canonical AEAD/integrity authority supplied by the composition
/// root. The storage kernel never implements or derives a second crypto key.
pub trait PrivatePayloadIntegrity: Send + Sync {
    fn authenticate(&self, sealed: &[u8], expected_plaintext_digest: &str) -> Result<(), String>;
}

pub(crate) fn authenticate_with(
    integrity: Option<&dyn PrivatePayloadIntegrity>,
    sealed: &[u8],
    digest: &str,
) -> Result<(), String> {
    integrity
        .ok_or_else(|| "private recovery integrity authority is unavailable".to_string())?
        .authenticate(sealed, digest)
        .map_err(|_| "private recovery payload failed canonical authentication".to_string())
}
