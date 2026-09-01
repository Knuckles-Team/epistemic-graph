//! Stable raw-vector serde for the default semantic store.

use super::*;

impl Serialize for SemanticStore {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("SemanticStore", 2)?;
        state.serialize_field("embeddings", &self.embeddings)?;
        state.serialize_field("space", &self.space)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for SemanticStore {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            #[serde(default)]
            embeddings: HashMap<String, Vec<f32>>,
            #[serde(default)]
            space: Option<EmbeddingSpaceRef>,
        }
        let raw = Raw::deserialize(deserializer)?;
        if let Some(space) = raw.space.as_ref() {
            space.validate().map_err(serde::de::Error::custom)?;
        }
        validate_persisted_embeddings(&raw.embeddings, raw.space.as_ref())
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            embeddings: raw.embeddings,
            space: raw.space,
            index: RwLock::new(HnswIndex::empty()),
        })
    }
}
