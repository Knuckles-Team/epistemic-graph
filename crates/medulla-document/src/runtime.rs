//! Native dependency-light served document runtime.
//!
//! Plain UTF-8 bytes are decoded to the typed page/block/span hierarchy and then
//! committed through the same governed runtime as every other modality. Source
//! bytes are never retained in the runtime snapshot; only their content address and
//! normalized structural coordinates are stored.

use medulla_modality::{
    ApplyOutcome, ArtifactBundle, GovernedModality, NativePredicate, NativeProductionProbe,
    OccurrenceId, OpaqueRef, ServedError, ServedIngest, ServedModalityRuntime, ServedPolicyScope,
};
use medulla_types::contract::Digest256;

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

/// The maximum line size used by the transcript source reader unless a caller
/// supplies a stricter source-specific cap.  The parser consumes a borrowed
/// line at a time; it never builds an unbounded collection of source records.
//
// spec: MEDULLA-REPO-INGEST-R800.2
pub const DEFAULT_TRANSCRIPT_MAX_LINE_BYTES: usize = 1_048_576;

/// A borrowed, digest-addressed line from a line-delimited transcript source.
///
/// `byte_start..byte_end` addresses the source bytes without its line ending.
/// CRLF and LF therefore produce the same line digest, while the file digest
/// still binds the exact source bytes in [`TranscriptPrefixDelta`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TranscriptLine<'a> {
    pub ordinal: u64,
    pub bytes: &'a [u8],
    pub byte_start: usize,
    pub byte_end: usize,
    pub digest: Digest256,
}

/// Stable refusal reasons for the bounded transcript line reader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TranscriptLineError {
    EmptyLineLimit,
    LineTooLong { ordinal: u64, max_bytes: usize },
    InvalidUtf8 { ordinal: u64 },
    OrdinalOverflow,
}

/// A restartable source cursor over line-delimited transcript bytes.
///
/// This is deliberately a pull iterator: each successful item borrows the
/// caller's source, so memory use is bounded by the configured line cap rather
/// than by the complete transcript. Dialect parsers can consume this primitive
/// and project each accepted line into the canonical `TranscriptTurn` model.
//
// spec: MEDULLA-REPO-INGEST-R800.2
pub struct TranscriptLineReader<'a> {
    bytes: &'a [u8],
    offset: usize,
    ordinal: u64,
    max_line_bytes: usize,
}

impl<'a> TranscriptLineReader<'a> {
    pub fn new(bytes: &'a [u8], max_line_bytes: usize) -> Result<Self, TranscriptLineError> {
        if max_line_bytes == 0 {
            return Err(TranscriptLineError::EmptyLineLimit);
        }
        Ok(Self {
            bytes,
            offset: 0,
            ordinal: 0,
            max_line_bytes,
        })
    }

    pub fn with_default_limit(bytes: &'a [u8]) -> Self {
        // The constant is non-zero by construction and covered by the reader
        // tests; keeping this constructor infallible makes source adapters easy
        // to compose while `new` remains available for configured limits.
        Self {
            bytes,
            offset: 0,
            ordinal: 0,
            max_line_bytes: DEFAULT_TRANSCRIPT_MAX_LINE_BYTES,
        }
    }
}

impl<'a> Iterator for TranscriptLineReader<'a> {
    type Item = Result<TranscriptLine<'a>, TranscriptLineError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.bytes.len() {
            return None;
        }

        let start = self.offset;
        let newline = self.bytes[start..].iter().position(|byte| *byte == b'\n');
        let raw_end = newline.map_or(self.bytes.len(), |relative| start + relative);
        let next_offset = newline.map_or(self.bytes.len(), |relative| start + relative + 1);
        let end = raw_end
            .checked_sub(usize::from(
                raw_end > start && self.bytes[raw_end - 1] == b'\r',
            ))
            .unwrap_or(raw_end);
        let line = &self.bytes[start..end];
        self.offset = next_offset;
        let ordinal = self.ordinal;
        self.ordinal = match self.ordinal.checked_add(1) {
            Some(next) => next,
            None => return Some(Err(TranscriptLineError::OrdinalOverflow)),
        };

        if line.len() > self.max_line_bytes {
            return Some(Err(TranscriptLineError::LineTooLong {
                ordinal,
                max_bytes: self.max_line_bytes,
            }));
        }
        if std::str::from_utf8(line).is_err() {
            return Some(Err(TranscriptLineError::InvalidUtf8 { ordinal }));
        }

        Some(Ok(TranscriptLine {
            ordinal,
            bytes: line,
            byte_start: start,
            byte_end: end,
            digest: Digest256::sha256(line),
        }))
    }
}

/// Result of comparing the current line digests with the prior accepted prefix.
/// The `last_ordinal` and `file_digest` fields are the bounded values an adapter
/// writes into the existing `medulla_types::source_ingestion::SourceCheckpoint`
/// position/content-hash fields; this scan result is not a second durable
/// checkpoint contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TranscriptPrefixDelta {
    pub last_ordinal: Option<u64>,
    pub file_digest: Digest256,
    pub first_changed_ordinal: Option<u64>,
    pub line_count: u64,
}

/// Check the exact source bytes against the content identity already carried by
/// the shared source-ingestion checkpoint. The checkpoint's position remains
/// the authority for the restart ordinal; this helper only performs the
/// file-level identity check.
//
// spec: MEDULLA-REPO-INGEST-R800.2
pub fn transcript_file_matches_checkpoint(
    checkpoint: &medulla_types::source_ingestion::SourceCheckpoint,
    bytes: &[u8],
) -> bool {
    checkpoint.content_hash == Some(Digest256::sha256(bytes))
}

/// Read a transcript once, retaining only the first changed ordinal and the
/// per-file checkpoint. A changed line or a newly appended line restarts at its
/// first differing ordinal; a truncated current file restarts at its new end.
/// The prior digest slice is owned by the checkpointing layer, so this function
/// does not allocate proportional to the current source.
//
// spec: MEDULLA-REPO-INGEST-R800.2
pub fn compare_transcript_prefix(
    bytes: &[u8],
    previous_line_digests: &[Digest256],
    max_line_bytes: usize,
) -> Result<TranscriptPrefixDelta, TranscriptLineError> {
    let reader = TranscriptLineReader::new(bytes, max_line_bytes)?;
    let mut first_changed_ordinal = None;
    let mut line_count = 0_u64;

    for line in reader {
        let line = line?;
        let prior = usize::try_from(line.ordinal)
            .ok()
            .and_then(|index| previous_line_digests.get(index));
        if first_changed_ordinal.is_none() && prior.is_none_or(|digest| *digest != line.digest) {
            first_changed_ordinal = Some(line.ordinal);
        }
        line_count = line.ordinal.saturating_add(1);
    }

    if first_changed_ordinal.is_none()
        && usize::try_from(line_count)
            .ok()
            .is_some_and(|count| previous_line_digests.len() > count)
    {
        first_changed_ordinal = Some(line_count);
    }

    Ok(TranscriptPrefixDelta {
        last_ordinal: line_count.checked_sub(1),
        file_digest: Digest256::sha256(bytes),
        first_changed_ordinal,
        line_count,
    })
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

    // spec: MEDULLA-REPO-INGEST-R800.2
    #[test]
    fn transcript_reader_keeps_line_bounds_and_normalizes_crlf_digest() {
        let source = b"alpha\r\nbeta\n";
        let mut reader = TranscriptLineReader::new(source, 5).unwrap();

        let first = reader.next().unwrap().unwrap();
        assert_eq!(first.ordinal, 0);
        assert_eq!(first.bytes, b"alpha");
        assert_eq!((first.byte_start, first.byte_end), (0, 5));
        assert_eq!(first.digest, Digest256::sha256(b"alpha"));

        let second = reader.next().unwrap().unwrap();
        assert_eq!(second.ordinal, 1);
        assert_eq!(second.bytes, b"beta");
        assert_eq!((second.byte_start, second.byte_end), (7, 11));
        assert!(reader.next().is_none());

        let lf = TranscriptLineReader::new(b"alpha\nbeta\n", 5).unwrap();
        let lf_digests = lf.map(|line| line.unwrap().digest).collect::<Vec<_>>();
        assert_eq!(lf_digests, vec![first.digest, second.digest]);
    }

    // spec: MEDULLA-REPO-INGEST-R800.2
    #[test]
    fn transcript_reader_refuses_zero_or_overlong_or_non_utf8_lines() {
        assert!(matches!(
            TranscriptLineReader::new(b"line", 0),
            Err(TranscriptLineError::EmptyLineLimit)
        ));

        let mut overlong = TranscriptLineReader::new(b"12345\nnext", 4).unwrap();
        assert_eq!(
            overlong.next().unwrap(),
            Err(TranscriptLineError::LineTooLong {
                ordinal: 0,
                max_bytes: 4
            })
        );
        assert_eq!(overlong.next().unwrap().unwrap().ordinal, 1);

        let mut invalid = TranscriptLineReader::new(b"valid\n\xff", 16).unwrap();
        assert_eq!(invalid.next().unwrap().unwrap().ordinal, 0);
        assert_eq!(
            invalid.next().unwrap(),
            Err(TranscriptLineError::InvalidUtf8 { ordinal: 1 })
        );
    }

    // spec: MEDULLA-REPO-INGEST-R800.2
    #[test]
    fn transcript_prefix_delta_restarts_at_first_changed_or_removed_line() {
        let baseline = b"one\ntwo\nthree\n";
        let baseline_digests = TranscriptLineReader::new(baseline, 32)
            .unwrap()
            .map(|line| line.unwrap().digest)
            .collect::<Vec<_>>();

        let unchanged = compare_transcript_prefix(baseline, &baseline_digests, 32).unwrap();
        assert_eq!(unchanged.first_changed_ordinal, None);
        assert_eq!(unchanged.line_count, 3);
        assert_eq!(unchanged.last_ordinal, Some(2));
        assert_eq!(unchanged.file_digest, Digest256::sha256(baseline));

        let changed =
            compare_transcript_prefix(b"one\nTWO\nthree\n", &baseline_digests, 32).unwrap();
        assert_eq!(changed.first_changed_ordinal, Some(1));

        let appended =
            compare_transcript_prefix(b"one\ntwo\nthree\nfour\n", &baseline_digests, 32).unwrap();
        assert_eq!(appended.first_changed_ordinal, Some(3));
        assert_eq!(appended.last_ordinal, Some(3));

        let truncated = compare_transcript_prefix(b"one\ntwo\n", &baseline_digests, 32).unwrap();
        assert_eq!(truncated.first_changed_ordinal, Some(2));
        assert_eq!(truncated.last_ordinal, Some(1));
    }
}
