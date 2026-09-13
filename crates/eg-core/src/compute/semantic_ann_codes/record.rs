//! Canonical owner-row access.
//!
//! Every semantic owner row is one canonical-CBOR record, and every read of
//! one is the same three steps: get the key's bytes, decode them, and map the
//! contract error into this owner's error. These helpers are that one path, so
//! the lifecycle modules state what a row must prove rather than how its bytes
//! are fetched.

use eg_types::semantic_index::{
    SemanticActivePointer, SemanticAnnIndexManifest, SemanticAuthorizationReceipt, SemanticBinding,
    SemanticBindingStateTransition, SemanticDeadLetter, SemanticGenerationCheckpoint,
    SemanticGraphProjectionManifest, SemanticIndexError, SemanticIndexMutation,
    SemanticLexicalIndexManifest, SemanticSourceProgress, SemanticSqlSourceManifest,
    SemanticStageIntent, SemanticStageTransition, SemanticTombstone, SemanticVector,
};
use redb::AccessGuard;

use super::{kernel_error, semantic_contract_error, SemanticCodeError};

/// A record stored as canonical CBOR in a semantic owner table.
pub(super) trait CanonicalRow: Sized {
    fn to_row(&self) -> Result<Vec<u8>, SemanticIndexError>;
    fn from_row(bytes: &[u8]) -> Result<Self, SemanticIndexError>;
}

/// A canonical row whose contract publishes its own validation.
pub(super) trait ValidatedRow: CanonicalRow {
    fn validate_row(&self) -> Result<(), SemanticIndexError>;
}

macro_rules! canonical_rows {
    ($($record:ty),+ $(,)?) => {$(
        impl CanonicalRow for $record {
            fn to_row(&self) -> Result<Vec<u8>, SemanticIndexError> {
                self.to_canonical_cbor()
            }

            fn from_row(bytes: &[u8]) -> Result<Self, SemanticIndexError> {
                Self::from_canonical_cbor(bytes)
            }
        }
    )+};
}

macro_rules! validated_rows {
    ($($record:ty),+ $(,)?) => {$(
        impl ValidatedRow for $record {
            fn validate_row(&self) -> Result<(), SemanticIndexError> {
                self.validate()
            }
        }
    )+};
}

canonical_rows!(
    SemanticActivePointer,
    SemanticAnnIndexManifest,
    SemanticAuthorizationReceipt,
    SemanticBinding,
    SemanticBindingStateTransition,
    SemanticDeadLetter,
    SemanticGenerationCheckpoint,
    SemanticGraphProjectionManifest,
    SemanticIndexMutation,
    SemanticLexicalIndexManifest,
    SemanticSourceProgress,
    SemanticSqlSourceManifest,
    SemanticStageIntent,
    SemanticStageTransition,
    SemanticTombstone,
    SemanticVector,
);

validated_rows!(
    SemanticActivePointer,
    SemanticAnnIndexManifest,
    SemanticBinding,
    SemanticGenerationCheckpoint,
    SemanticIndexMutation,
    SemanticLexicalIndexManifest,
    SemanticSourceProgress,
    SemanticSqlSourceManifest,
);

pub(super) fn encode<T: CanonicalRow>(record: &T) -> Result<Vec<u8>, SemanticCodeError> {
    record.to_row().map_err(semantic_contract_error)
}

/// Validate, then encode: the shape of every row a write inserts.
pub(super) fn encode_valid<T: ValidatedRow>(record: &T) -> Result<Vec<u8>, SemanticCodeError> {
    record.validate_row().map_err(semantic_contract_error)?;
    encode(record)
}

pub(super) fn decode<T: CanonicalRow>(bytes: &[u8]) -> Result<T, SemanticCodeError> {
    T::from_row(bytes).map_err(semantic_contract_error)
}

/// Decode, then validate: the shape of every row a reader trusts.
pub(super) fn decode_valid<T: ValidatedRow>(bytes: &[u8]) -> Result<T, SemanticCodeError> {
    let record = decode::<T>(bytes)?;
    record.validate_row().map_err(semantic_contract_error)?;
    Ok(record)
}

/// The bytes of one owner-table lookup, whichever table kind answered it:
/// a serving snapshot, an admitted write, or a read inside that write.
pub(super) fn row_bytes<E: std::fmt::Display>(
    found: Result<Option<AccessGuard<'_, &'static [u8]>>, E>,
) -> Result<Option<Vec<u8>>, SemanticCodeError> {
    Ok(found
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec()))
}

/// The record of one owner-table lookup, if any.
pub(super) fn row<R: CanonicalRow, E: std::fmt::Display>(
    found: Result<Option<AccessGuard<'_, &'static [u8]>>, E>,
) -> Result<Option<R>, SemanticCodeError> {
    row_bytes(found)?
        .map(|bytes| decode::<R>(&bytes))
        .transpose()
}
