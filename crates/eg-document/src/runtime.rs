//! Native dependency-light served document runtime.
//!
//! Plain UTF-8 bytes are decoded to the typed page/block/span hierarchy and then
//! committed through the same governed runtime as every other modality. Source
//! bytes are never retained in the runtime snapshot; only their content address and
//! normalized structural coordinates are stored.

use eg_modality::{
    ApplyOutcome, ArtifactBundle, GovernedModality, NativePredicate, NativeProductionProbe,
    OccurrenceId, OpaqueRef, ServedError, ServedIngest, ServedModalityRuntime, ServedPolicyScope,
};

use crate::{DocumentData, DocumentDecoder, LexemeEncoder, NativeTextDecoder};

#[derive(Clone, Debug, Default)]
pub struct NativeDocumentRuntime {
    served: ServedModalityRuntime<DocumentData>,
}

/// The verified-authority ingest request for [`NativeDocumentRuntime::ingest_text_authorized`],
/// bundled into one type so the entry point stays under clippy's `too_many_arguments`
/// threshold without dropping any of the fields the authority check + governed
/// `ServedIngest` construction need.
pub struct TextIngestAuthorized<'a> {
    pub idempotency_ref: OpaqueRef,
    pub target_occurrence_id: OccurrenceId,
    /// `None` creates a new occurrence; `Some(v)` is an OCC compare-and-swap update.
    pub expected_version: Option<u64>,
    pub bundle: ArtifactBundle,
    pub bytes: &'a [u8],
    pub lexemes: &'a dyn LexemeEncoder,
}

pub fn production_probe() -> NativeProductionProbe {
    let lexeme = |value: &str| {
        Some(format!(
            "eg:lexeme:{}",
            crate::content_hash(value.as_bytes())
        ))
    };
    let decoded = NativeTextDecoder.decode(b"alpha beta\nleft | right", &lexeme);
    let codec = decoded.is_some();
    let normalized_payload = decoded
        .as_ref()
        .is_some_and(GovernedModality::validate_governed_payload);
    let secondary_index = decoded
        .as_ref()
        .is_some_and(|value| !value.native_index_keys().is_empty());
    let alpha = lexeme("alpha").and_then(|value| OpaqueRef::new(value).ok());
    let typed_query = decoded
        .as_ref()
        .zip(alpha.as_ref())
        .is_some_and(|(value, token)| {
            value.matches_native_predicate(&NativePredicate::DocumentLexeme {
                lexeme_ref: token.clone(),
                page: Some(1),
            })
        });
    let too_many_pages = vec![b'\x0c'; 4_097];
    let malformed_and_resource_bounds = NativeTextDecoder.decode(&[0xff], &lexeme).is_none()
        && NativeTextDecoder.decode(&too_many_pages, &lexeme).is_none();
    NativeProductionProbe {
        codec,
        normalized_payload,
        secondary_index,
        typed_query,
        malformed_and_resource_bounds,
    }
}

impl NativeDocumentRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode one bounded source into its source-free governed representation.
    /// The live graph handler uses this entry point before applying a multi-record
    /// atomic stream through the shared served runtime.
    pub fn decode_text(bytes: &[u8], lexemes: &dyn LexemeEncoder) -> Option<DocumentData> {
        NativeTextDecoder.decode(bytes, lexemes)
    }

    /// Verified-authority variant used by the live server. The target occurrence
    /// must match the signed request's tenant, policy, purpose, and classification
    /// before the dependency-light decoder is allowed to stage durable state.
    pub fn ingest_text_authorized(
        &mut self,
        scope: &ServedPolicyScope,
        request: TextIngestAuthorized<'_>,
    ) -> Result<ApplyOutcome, ServedError> {
        let TextIngestAuthorized {
            idempotency_ref,
            target_occurrence_id,
            expected_version,
            bundle,
            bytes,
            lexemes,
        } = request;
        let occurrence = bundle
            .occurrences
            .iter()
            .find(|value| value.id == target_occurrence_id)
            .ok_or(ServedError::InvalidBundle)?;
        if !scope.authorizes_occurrence(occurrence) {
            return Err(ServedError::Forbidden);
        }
        let value = Self::decode_text(bytes, lexemes).ok_or(ServedError::Codec)?;
        self.served.ingest(ServedIngest {
            idempotency_ref,
            target_occurrence_id,
            expected_version,
            bundle,
            value,
        })
    }

    pub fn served(&self) -> &ServedModalityRuntime<DocumentData> {
        &self.served
    }

    pub fn served_mut(&mut self) -> &mut ServedModalityRuntime<DocumentData> {
        &mut self.served
    }

    pub fn snapshot(&self) -> Result<Vec<u8>, ServedError> {
        self.served.snapshot()
    }

    pub fn recover(snapshot: &[u8]) -> Result<Self, ServedError> {
        Ok(Self {
            served: ServedModalityRuntime::recover(snapshot)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dependency_light_decoder_keeps_only_structure_and_content_address() {
        let lexeme = |value: &str| {
            Some(format!(
                "eg:lexeme:{}",
                crate::content_hash(value.as_bytes())
            ))
        };
        let value = NativeTextDecoder
            .decode(b"non-identifying fixture", &lexeme)
            .expect("valid UTF-8");
        assert_eq!(value.pages.len(), 1);
        assert_eq!(value.pages[0].blocks.len(), 1);
        assert_eq!(value.pages[0].blocks[0].spans[0].start, 0);
        assert_eq!(value.pages[0].blocks[0].spans[0].end, 23);
        let encoded = serde_json::to_string(&value).unwrap();
        assert!(!encoded.contains("non-identifying fixture"));
        assert_eq!(value.lexical_postings.len(), 3);
    }
}
