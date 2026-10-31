use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Number, Value};

use super::digest::cbor;
use super::{
    SemanticActivePointer, SemanticAnnIndexManifest, SemanticAuthorizationReceipt, SemanticBinding,
    SemanticBindingStateTransition, SemanticDeadLetter, SemanticGenerationCheckpoint,
    SemanticGraphProjectionManifest, SemanticIndexError, SemanticIndexManifest,
    SemanticIndexMutation, SemanticLexicalIndexManifest, SemanticLineage,
    SemanticSourceDirtyIntent, SemanticSourceProgress, SemanticSqlSourceManifest,
    SemanticStageIntent, SemanticStageTransition, SemanticTombstone, SemanticVector,
};

pub const SEMANTIC_CANONICAL_RECORD_MAX_BYTES: usize = 256 * 1024;
pub const SEMANTIC_STAGE_TRANSITION_SCHEMA: &str = "semantic-stage-transition/v1";
pub const SEMANTIC_STAGE_INTENT_SCHEMA: &str = "semantic-stage-intent/v1";
pub const SEMANTIC_GENERATION_CHECKPOINT_SCHEMA: &str = "semantic-generation-checkpoint/v1";
pub const SEMANTIC_BINDING_STATE_TRANSITION_SCHEMA: &str = "semantic-binding-state-transition/v1";
pub const SEMANTIC_INDEX_MUTATION_SCHEMA: &str = "semantic-index-mutation/v1";
pub const SEMANTIC_ACTIVE_POINTER_SCHEMA: &str = "semantic-active-pointer/v1";
pub const SEMANTIC_SOURCE_DIRTY_INTENT_SCHEMA: &str = "semantic-source-dirty-intent/v1";
pub const SEMANTIC_SOURCE_DIRTY_TOPIC: &str = "semantic.index.source_dirty/v1";
pub const SEMANTIC_SOURCE_PROGRESS_SCHEMA: &str = "semantic-source-progress/v1";
pub const SEMANTIC_LEXICAL_INDEX_MANIFEST_SCHEMA: &str = "semantic-lexical-index-manifest/v1";
pub const SEMANTIC_ANN_INDEX_MANIFEST_SCHEMA: &str = "semantic-ann-index-manifest/v1";
pub const SEMANTIC_INDEX_MANIFEST_SCHEMA: &str = "semantic-index-manifest/v1";
pub const SEMANTIC_DEAD_LETTER_SCHEMA: &str = "semantic-dead-letter/v1";
pub const SEMANTIC_TOMBSTONE_SCHEMA: &str = "semantic-tombstone/v1";
pub const SEMANTIC_SQL_SOURCE_MANIFEST_SCHEMA: &str = "semantic-sql-source-manifest/v1";
pub const SEMANTIC_GRAPH_PROJECTION_MANIFEST_SCHEMA: &str = "semantic-graph-projection-manifest/v1";
pub const SEMANTIC_AUTHORIZATION_RECEIPT_SCHEMA: &str = "semantic-authorization-receipt/v1";
pub const SEMANTIC_LINEAGE_SCHEMA: &str = "semantic-lineage/v1";

const MAX_CONTAINER_ITEMS: usize = 8_192;
const MAX_NESTING_DEPTH: usize = 32;
const MAX_TEXT_BYTES: usize = 4 * 1024;

trait CanonicalSemanticRecord: Serialize + DeserializeOwned + Sized {
    const SCHEMA: &'static str;

    fn validate_record(&self) -> Result<(), SemanticIndexError>;
}

fn encode_record<T: CanonicalSemanticRecord>(record: &T) -> Result<Vec<u8>, SemanticIndexError> {
    record.validate_record()?;
    let value =
        serde_json::to_value(record).map_err(|_| canonical_error("record serialization"))?;
    let encoded = encode_value(&Value::Object(Map::from_iter([
        ("record".to_string(), value),
        ("schema".to_string(), Value::String(T::SCHEMA.to_string())),
    ])))?;
    if encoded.len() > SEMANTIC_CANONICAL_RECORD_MAX_BYTES {
        return Err(SemanticIndexError::CanonicalRecordTooLarge);
    }
    Ok(encoded)
}

#[cfg(test)]
pub(super) fn encode_test_envelope(schema: &str, record: Value) -> Vec<u8> {
    encode_value(&Value::Object(Map::from_iter([
        ("record".to_string(), record),
        ("schema".to_string(), Value::String(schema.to_string())),
    ])))
    .unwrap()
}

fn decode_record<T: CanonicalSemanticRecord>(bytes: &[u8]) -> Result<T, SemanticIndexError> {
    if bytes.len() > SEMANTIC_CANONICAL_RECORD_MAX_BYTES {
        return Err(SemanticIndexError::CanonicalRecordTooLarge);
    }
    let value = Decoder::new(bytes).decode()?;
    let envelope = value
        .as_object()
        .ok_or_else(|| canonical_error("record envelope"))?;
    if envelope.len() != 2 || envelope.get("schema").and_then(Value::as_str) != Some(T::SCHEMA) {
        return Err(canonical_error("record schema"));
    }
    let record = envelope
        .get("record")
        .cloned()
        .ok_or_else(|| canonical_error("record payload"))?;
    let record: T = serde_json::from_value(record).map_err(|_| canonical_error("record shape"))?;
    record.validate_record()?;
    if encode_record(&record)?.as_slice() != bytes {
        return Err(SemanticIndexError::NonCanonicalRecord);
    }
    Ok(record)
}

fn encode_value(value: &Value) -> Result<Vec<u8>, SemanticIndexError> {
    match value {
        Value::Null => Ok(vec![0xf6]),
        Value::Bool(value) => Ok(cbor::boolean(*value)),
        Value::Number(value) => encode_number(value),
        Value::String(value) if value.len() <= MAX_TEXT_BYTES => Ok(cbor::text(value)),
        Value::String(_) => Err(canonical_error("text bound")),
        Value::Array(values) if values.len() <= MAX_CONTAINER_ITEMS => values
            .iter()
            .map(encode_value)
            .collect::<Result<Vec<_>, _>>()
            .map(cbor::array),
        Value::Array(_) => Err(canonical_error("array bound")),
        Value::Object(values) if values.len() <= MAX_CONTAINER_ITEMS => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), encode_value(value)?)))
            .collect::<Result<Vec<_>, SemanticIndexError>>()
            .map(cbor::map_owned),
        Value::Object(_) => Err(canonical_error("map bound")),
    }
}

fn encode_number(value: &Number) -> Result<Vec<u8>, SemanticIndexError> {
    if let Some(value) = value.as_u64() {
        return Ok(cbor::unsigned(value));
    }
    let value = value
        .as_f64()
        .ok_or_else(|| canonical_error("numeric representation"))?;
    let narrowed = value as f32;
    if !value.is_finite() || narrowed as f64 != value {
        return Err(canonical_error("finite f32 representation"));
    }
    cbor::finite_f32(narrowed).map_err(|_| canonical_error("finite f32 representation"))
}

fn canonical_error(reason: &'static str) -> SemanticIndexError {
    SemanticIndexError::MalformedCanonicalRecord {
        reason: reason.to_string(),
    }
}

struct Decoder<'a> {
    cursor: Cursor<'a>,
    items: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            cursor: Cursor { bytes, offset: 0 },
            items: 0,
        }
    }

    fn decode(mut self) -> Result<Value, SemanticIndexError> {
        let value = self.value(0)?;
        if self.cursor.offset != self.cursor.bytes.len() {
            return Err(canonical_error("trailing bytes"));
        }
        Ok(value)
    }

    fn value(&mut self, depth: usize) -> Result<Value, SemanticIndexError> {
        if depth > MAX_NESTING_DEPTH || self.items >= MAX_CONTAINER_ITEMS {
            return Err(canonical_error("nesting or item bound"));
        }
        self.items += 1;
        let initial = self.cursor.byte()?;
        let major = initial >> 5;
        let additional = initial & 0x1f;
        if major <= 3 {
            return self.scalar(major, additional);
        }
        self.container_or_simple(major, additional, depth)
    }

    fn scalar(&mut self, major: u8, additional: u8) -> Result<Value, SemanticIndexError> {
        match major {
            0 => Ok(Value::Number(Number::from(
                self.cursor.argument(additional)?,
            ))),
            1 => self.negative(additional),
            2 => Err(canonical_error("byte strings are not record fields")),
            3 => self.text(additional),
            _ => Err(canonical_error("CBOR scalar type")),
        }
    }

    fn container_or_simple(
        &mut self,
        major: u8,
        additional: u8,
        depth: usize,
    ) -> Result<Value, SemanticIndexError> {
        match major {
            4 => self.array(additional, depth),
            5 => self.map(additional, depth),
            6 => Err(canonical_error("CBOR tags are not record fields")),
            7 => self.simple(additional),
            _ => Err(canonical_error("CBOR major type")),
        }
    }

    fn negative(&mut self, additional: u8) -> Result<Value, SemanticIndexError> {
        let magnitude = self.cursor.argument(additional)?;
        let value = i64::try_from(magnitude)
            .ok()
            .and_then(|value| value.checked_add(1))
            .and_then(|value| value.checked_neg())
            .ok_or_else(|| canonical_error("negative integer bound"))?;
        Ok(Value::Number(Number::from(value)))
    }

    fn text(&mut self, additional: u8) -> Result<Value, SemanticIndexError> {
        let length = self.cursor.length(additional, MAX_TEXT_BYTES)?;
        let bytes = self.cursor.take(length)?;
        let value = std::str::from_utf8(bytes).map_err(|_| canonical_error("UTF-8 text"))?;
        Ok(Value::String(value.to_string()))
    }

    fn array(&mut self, additional: u8, depth: usize) -> Result<Value, SemanticIndexError> {
        let length = self.cursor.length(additional, MAX_CONTAINER_ITEMS)?;
        let mut values = Vec::with_capacity(length);
        for _ in 0..length {
            values.push(self.value(depth + 1)?);
        }
        Ok(Value::Array(values))
    }

    fn map(&mut self, additional: u8, depth: usize) -> Result<Value, SemanticIndexError> {
        let length = self.cursor.length(additional, MAX_CONTAINER_ITEMS)?;
        let mut values = Map::new();
        for _ in 0..length {
            let key = self.value(depth + 1)?;
            let key = key
                .as_str()
                .ok_or_else(|| canonical_error("text map key"))?
                .to_string();
            if values.contains_key(&key) {
                return Err(canonical_error("duplicate map key"));
            }
            values.insert(key, self.value(depth + 1)?);
        }
        Ok(Value::Object(values))
    }

    fn simple(&mut self, additional: u8) -> Result<Value, SemanticIndexError> {
        match additional {
            20 => Ok(Value::Bool(false)),
            21 => Ok(Value::Bool(true)),
            22 => Ok(Value::Null),
            25 => self.cursor.float16(),
            26 => self.cursor.float32(),
            27 => self.cursor.float64(),
            _ => Err(canonical_error("CBOR simple value")),
        }
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn float16(&mut self) -> Result<Value, SemanticIndexError> {
        let bits = u16::from_be_bytes(self.take(2)?.try_into().unwrap());
        number_value(cbor::f16_to_f32(bits) as f64)
    }

    fn float32(&mut self) -> Result<Value, SemanticIndexError> {
        let bits = u32::from_be_bytes(self.take(4)?.try_into().unwrap());
        number_value(f32::from_bits(bits) as f64)
    }

    fn float64(&mut self) -> Result<Value, SemanticIndexError> {
        let bits = u64::from_be_bytes(self.take(8)?.try_into().unwrap());
        number_value(f64::from_bits(bits))
    }

    fn length(&mut self, additional: u8, maximum: usize) -> Result<usize, SemanticIndexError> {
        let value = self.argument(additional)?;
        let value = usize::try_from(value).map_err(|_| canonical_error("container length"))?;
        if value > maximum {
            return Err(canonical_error("container length bound"));
        }
        Ok(value)
    }

    fn argument(&mut self, additional: u8) -> Result<u64, SemanticIndexError> {
        match additional {
            0..=23 => Ok(additional.into()),
            24 => Ok(self.byte()?.into()),
            25 => Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()).into()),
            26 => Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()).into()),
            27 => Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap())),
            _ => Err(canonical_error("indefinite or reserved argument")),
        }
    }

    fn byte(&mut self) -> Result<u8, SemanticIndexError> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| canonical_error("truncated input"))?;
        self.offset += 1;
        Ok(value)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], SemanticIndexError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| canonical_error("truncated input"))?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

fn number_value(value: f64) -> Result<Value, SemanticIndexError> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| canonical_error("non-finite float"))
}

macro_rules! impl_canonical_codec {
    ($record:ty, $schema:expr, $validate:expr) => {
        impl CanonicalSemanticRecord for $record {
            const SCHEMA: &'static str = $schema;

            fn validate_record(&self) -> Result<(), SemanticIndexError> {
                ($validate)(self)
            }
        }

        impl $record {
            pub fn to_canonical_cbor(&self) -> Result<Vec<u8>, SemanticIndexError> {
                encode_record(self)
            }

            pub fn from_canonical_cbor(bytes: &[u8]) -> Result<Self, SemanticIndexError> {
                decode_record(bytes)
            }
        }
    };
}

impl_canonical_codec!(
    SemanticBinding,
    super::SEMANTIC_BINDING_SCHEMA,
    |value: &SemanticBinding| value.validate()
);
impl_canonical_codec!(
    SemanticVector,
    super::SEMANTIC_VECTOR_SCHEMA,
    |value: &SemanticVector| value.validate()
);
impl_canonical_codec!(
    SemanticStageIntent,
    SEMANTIC_STAGE_INTENT_SCHEMA,
    |value: &SemanticStageIntent| value.validate()
);
impl_canonical_codec!(
    SemanticSourceDirtyIntent,
    SEMANTIC_SOURCE_DIRTY_INTENT_SCHEMA,
    |value: &SemanticSourceDirtyIntent| value.validate()
);
impl_canonical_codec!(
    SemanticStageTransition,
    SEMANTIC_STAGE_TRANSITION_SCHEMA,
    |value: &SemanticStageTransition| value.validate()
);
impl_canonical_codec!(
    SemanticGenerationCheckpoint,
    SEMANTIC_GENERATION_CHECKPOINT_SCHEMA,
    |value: &SemanticGenerationCheckpoint| value.validate()
);
impl_canonical_codec!(
    SemanticBindingStateTransition,
    SEMANTIC_BINDING_STATE_TRANSITION_SCHEMA,
    |value: &SemanticBindingStateTransition| value.validate_receipt()
);
impl_canonical_codec!(
    SemanticIndexMutation,
    SEMANTIC_INDEX_MUTATION_SCHEMA,
    |value: &SemanticIndexMutation| value.validate()
);
impl_canonical_codec!(
    SemanticActivePointer,
    SEMANTIC_ACTIVE_POINTER_SCHEMA,
    |value: &SemanticActivePointer| value.validate()
);
impl_canonical_codec!(
    SemanticSourceProgress,
    SEMANTIC_SOURCE_PROGRESS_SCHEMA,
    |value: &SemanticSourceProgress| value.validate()
);
impl_canonical_codec!(
    SemanticLexicalIndexManifest,
    SEMANTIC_LEXICAL_INDEX_MANIFEST_SCHEMA,
    |value: &SemanticLexicalIndexManifest| value.validate()
);
impl_canonical_codec!(
    SemanticAnnIndexManifest,
    SEMANTIC_ANN_INDEX_MANIFEST_SCHEMA,
    |value: &SemanticAnnIndexManifest| value.validate()
);
impl_canonical_codec!(
    SemanticIndexManifest,
    SEMANTIC_INDEX_MANIFEST_SCHEMA,
    |value: &SemanticIndexManifest| value.validate()
);
impl_canonical_codec!(
    SemanticDeadLetter,
    SEMANTIC_DEAD_LETTER_SCHEMA,
    |value: &SemanticDeadLetter| value.validate()
);
impl_canonical_codec!(
    SemanticTombstone,
    SEMANTIC_TOMBSTONE_SCHEMA,
    |value: &SemanticTombstone| value.validate()
);
impl_canonical_codec!(
    SemanticSqlSourceManifest,
    SEMANTIC_SQL_SOURCE_MANIFEST_SCHEMA,
    |value: &SemanticSqlSourceManifest| value.validate()
);
impl_canonical_codec!(
    SemanticGraphProjectionManifest,
    SEMANTIC_GRAPH_PROJECTION_MANIFEST_SCHEMA,
    |value: &SemanticGraphProjectionManifest| value.validate()
);
impl_canonical_codec!(
    SemanticAuthorizationReceipt,
    SEMANTIC_AUTHORIZATION_RECEIPT_SCHEMA,
    |value: &SemanticAuthorizationReceipt| value.validate()
);
impl_canonical_codec!(
    SemanticLineage,
    SEMANTIC_LINEAGE_SCHEMA,
    |value: &SemanticLineage| value.validate()
);
