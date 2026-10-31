//! Extended state — the machine's *context* (CONCEPT:INT-P2-2).
//!
//! A finite-state machine without extended state can only remember "which state am
//! I in". A *statechart* carries a small typed data record alongside the discrete
//! state — the **context** — which guards read and actions transform. Keeping the
//! context here (rather than inlining a `BTreeMap` everywhere) gives the transition
//! function ONE well-defined, serializable data value to thread through, and gives
//! the durable [`crate::instance::MachineInstance`] one field to persist.
//!
//! The context is deliberately a plain ordered `String -> JSON` map: it is data the
//! ENGINE never interprets structurally (only guards/actions the *definition*
//! declares do), it serializes deterministically (a `BTreeMap` is key-sorted, so two
//! equal contexts always encode to identical bytes — the property `def_id`/OCC rely
//! on), and it crosses the wire as a JSON object with no bespoke schema.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The machine's extended state: an ordered `String -> JSON` map (CONCEPT:INT-P2-2).
///
/// Ordered (`BTreeMap`) on purpose — see the module doc: deterministic serialization
/// is what lets a `(state, context)` instance row compare/round-trip byte-stably.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Context(pub BTreeMap<String, serde_json::Value>);

impl Context {
    /// The empty context.
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Read a context key.
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.0.get(key)
    }

    /// Whether a key is present (and not JSON `null`). Backing store for
    /// [`crate::guard::Guard::Exists`].
    pub fn has(&self, key: &str) -> bool {
        matches!(self.0.get(key), Some(value) if !value.is_null())
    }

    /// Set (insert or overwrite) a context key.
    pub fn set(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.0.insert(key.into(), value);
    }

    /// Remove a context key, returning the previous value if any.
    pub fn remove(&mut self, key: &str) -> Option<serde_json::Value> {
        self.0.remove(key)
    }

    /// Number of keys held.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the context holds no keys.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Build a context from a JSON value. A JSON object becomes the map directly;
    /// JSON `null` is treated as the empty context (the common "no context supplied"
    /// wire shape). Any other JSON shape is a hard error — the context is a record,
    /// never a scalar or array.
    pub fn from_value(value: serde_json::Value) -> Result<Self, String> {
        match value {
            serde_json::Value::Null => Ok(Self::new()),
            serde_json::Value::Object(map) => Ok(Self(map.into_iter().collect())),
            _ => Err("statechart context must be a JSON object (or null)".to_string()),
        }
    }

    /// Render the context back to a JSON object value.
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::Value::Object(self.0.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

impl From<BTreeMap<String, serde_json::Value>> for Context {
    fn from(map: BTreeMap<String, serde_json::Value>) -> Self {
        Self(map)
    }
}

/// An event delivered to the machine (CONCEPT:INT-P2-2): a symbol from the
/// definition's alphabet Σ, plus an optional JSON payload guards and actions may
/// read. The payload is data, never control — it can only be *observed* by declared
/// guards (`EventEq`) and *copied into* context by declared actions
/// (`ActionValue::FromEvent`); it can never itself pick a transition.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EventInput {
    /// The event symbol (must be a member of the chart's alphabet Σ).
    pub name: String,
    /// Optional structured payload; JSON `null` when the event carries no data.
    #[serde(default)]
    pub payload: serde_json::Value,
}

impl EventInput {
    /// An event carrying no payload.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            payload: serde_json::Value::Null,
        }
    }

    /// An event carrying a JSON payload.
    pub fn with_payload(name: impl Into<String>, payload: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            payload,
        }
    }

    /// Read a field from an object payload (`None` for a non-object payload or a
    /// missing key). The primitive behind `ActionValue::FromEvent`/`Guard::EventEq`.
    pub fn payload_field(&self, key: &str) -> Option<&serde_json::Value> {
        self.payload.as_object().and_then(|object| object.get(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_value_accepts_object_and_null_but_rejects_scalars() {
        assert!(Context::from_value(serde_json::json!({"a": 1})).is_ok());
        assert!(Context::from_value(serde_json::Value::Null)
            .unwrap()
            .is_empty());
        assert!(Context::from_value(serde_json::json!(3)).is_err());
        assert!(Context::from_value(serde_json::json!([1, 2])).is_err());
    }

    #[test]
    fn has_treats_null_as_absent() {
        let mut ctx = Context::new();
        ctx.set("k", serde_json::Value::Null);
        assert!(ctx.get("k").is_some());
        assert!(!ctx.has("k"));
        ctx.set("k", serde_json::json!(1));
        assert!(ctx.has("k"));
    }

    #[test]
    fn roundtrips_through_json_deterministically() {
        let ctx = Context::from_value(serde_json::json!({"b": 2, "a": 1})).unwrap();
        // BTreeMap ordering => keys always emerge sorted, regardless of input order.
        let bytes = serde_json::to_vec(&ctx).unwrap();
        assert_eq!(bytes, br#"{"a":1,"b":2}"#);
    }
}
