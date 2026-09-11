use std::fmt;

/// Served errors never include rejected data, ids, paths, endpoints, or payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServedError {
    InvalidBundle,
    UnsafePayload,
    Codec,
    IdempotencyConflict,
    VersionConflict,
    Forbidden,
    LegalHold,
    NotFound,
    InvalidLifecycle,
    CorruptSnapshot,
    InvalidQuery,
    QueryTooBroad,
    CorruptIndex,
}

impl fmt::Display for ServedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str((*self).message())
    }
}

impl ServedError {
    fn message(self) -> &'static str {
        match self {
            Self::InvalidBundle => "invalid governed modality bundle",
            Self::UnsafePayload => "unsafe governed modality payload",
            Self::Codec => "modality codec failure",
            Self::IdempotencyConflict => "idempotency conflict",
            Self::VersionConflict => "observation version conflict",
            Self::Forbidden => "modality operation forbidden",
            Self::LegalHold => "modality occurrence is under legal hold",
            Self::NotFound => "modality occurrence not found",
            Self::InvalidLifecycle => "invalid modality lifecycle transition",
            Self::CorruptSnapshot => "corrupt modality snapshot",
            Self::InvalidQuery => "invalid native modality query",
            Self::QueryTooBroad => "native modality query exceeds resource limits",
            Self::CorruptIndex => "corrupt modality index",
        }
    }
}

impl std::error::Error for ServedError {}
