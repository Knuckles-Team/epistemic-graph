//! The event model + per-event matcher (CONCEPT:EG-088): an [`Event`] is a
//! `ts`/`key`/`attrs` record; an [`EventMatcher`] tests one event by an optional event
//! `key` plus a set of per-attribute [`AttrPredicate`]s that must ALL hold.
//!
//! Everything derives serde so a pattern (which embeds matchers) persists / rides the
//! wire, and so an event stream can be logged/replayed. The attribute model is
//! `serde_json` — the same value model `eg-plan` already decodes node blobs into — so
//! the executor maps a node blob's `attrs` object straight onto an [`Event`].

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A single stream event: an epoch timestamp `ts` (arbitrary monotonic unit — seconds,
/// millis, …; the window/`within` durations are in the SAME unit), a semantic `key`
/// (the event "type", matched by [`EventMatcher::key`]), and a bag of `attrs` (the
/// per-event payload, tested by [`AttrPredicate`]s). The shape is fixed by the concept
/// row (`{ ts, key, attrs }`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub ts: u64,
    pub key: String,
    #[serde(default)]
    pub attrs: Map<String, Value>,
}

impl Event {
    /// Convenience constructor (mostly for tests): an event with no attributes.
    pub fn new(ts: u64, key: impl Into<String>) -> Event {
        Event {
            ts,
            key: key.into(),
            attrs: Map::new(),
        }
    }

    /// Builder: set one attribute (chainable).
    pub fn with_attr(mut self, field: impl Into<String>, value: impl Into<Value>) -> Event {
        self.attrs.insert(field.into(), value.into());
        self
    }
}

/// A per-attribute predicate on an [`Event`]'s `attrs`. `Eq` compares the raw
/// `serde_json::Value`; `Gt`/`Lt` coerce the attribute to `f64` (non-numeric ⇒ no
/// match); `Exists` merely checks presence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AttrPredicate {
    /// `attrs[field] == value` (exact JSON value equality).
    Eq { field: String, value: Value },
    /// `attrs[field]` coerces to a number `> value`.
    Gt { field: String, value: f64 },
    /// `attrs[field]` coerces to a number `< value`.
    Lt { field: String, value: f64 },
    /// `attrs` contains `field`.
    Exists { field: String },
}

impl AttrPredicate {
    /// Evaluate this predicate against an event's attribute bag.
    pub fn eval(&self, attrs: &Map<String, Value>) -> bool {
        match self {
            AttrPredicate::Eq { field, value } => attrs.get(field) == Some(value),
            AttrPredicate::Gt { field, value } => attrs
                .get(field)
                .and_then(Value::as_f64)
                .is_some_and(|x| x > *value),
            AttrPredicate::Lt { field, value } => attrs
                .get(field)
                .and_then(Value::as_f64)
                .is_some_and(|x| x < *value),
            AttrPredicate::Exists { field } => attrs.contains_key(field),
        }
    }
}

/// One step of a CEP pattern: match an event whose `key` equals [`EventMatcher::key`]
/// (when `Some`; `None` matches any key) AND whose attributes satisfy EVERY
/// [`AttrPredicate`] in `preds` (an empty `preds` is "no attribute constraint").
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EventMatcher {
    /// The event `key` to match (`None` ⇒ any key).
    #[serde(default)]
    pub key: Option<String>,
    /// Per-attribute predicates — ALL must hold.
    #[serde(default)]
    pub preds: Vec<AttrPredicate>,
}

impl EventMatcher {
    /// A matcher that keys on `key` with no attribute predicates.
    pub fn key(key: impl Into<String>) -> EventMatcher {
        EventMatcher {
            key: Some(key.into()),
            preds: Vec::new(),
        }
    }

    /// Builder: add an attribute predicate (chainable).
    pub fn with_pred(mut self, pred: AttrPredicate) -> EventMatcher {
        self.preds.push(pred);
        self
    }

    /// Does `event` satisfy this matcher?
    pub fn matches(&self, event: &Event) -> bool {
        if let Some(k) = &self.key {
            if &event.key != k {
                return false;
            }
        }
        self.preds.iter().all(|p| p.eval(&event.attrs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn attr_predicates() {
        let e = Event::new(1, "trade")
            .with_attr("sym", "ACME")
            .with_attr("qty", 150);
        assert!(AttrPredicate::Eq {
            field: "sym".into(),
            value: json!("ACME")
        }
        .eval(&e.attrs));
        assert!(!AttrPredicate::Eq {
            field: "sym".into(),
            value: json!("OTHER")
        }
        .eval(&e.attrs));
        assert!(AttrPredicate::Gt {
            field: "qty".into(),
            value: 100.0
        }
        .eval(&e.attrs));
        assert!(!AttrPredicate::Lt {
            field: "qty".into(),
            value: 100.0
        }
        .eval(&e.attrs));
        assert!(AttrPredicate::Exists {
            field: "sym".into()
        }
        .eval(&e.attrs));
        assert!(!AttrPredicate::Exists {
            field: "nope".into()
        }
        .eval(&e.attrs));
        // Gt on a non-numeric attribute never matches.
        assert!(!AttrPredicate::Gt {
            field: "sym".into(),
            value: 0.0
        }
        .eval(&e.attrs));
    }

    #[test]
    fn matcher_key_and_preds() {
        let e = Event::new(1, "trade").with_attr("qty", 150);
        let m = EventMatcher::key("trade").with_pred(AttrPredicate::Gt {
            field: "qty".into(),
            value: 100.0,
        });
        assert!(m.matches(&e));
        // Wrong key.
        assert!(!m.matches(&Event::new(1, "quote").with_attr("qty", 150)));
        // Right key, failing pred.
        assert!(!m.matches(&Event::new(1, "trade").with_attr("qty", 50)));
        // key=None matches any key.
        let any = EventMatcher::default();
        assert!(any.matches(&e));
    }

    #[test]
    fn event_serde_round_trip() {
        let e = Event::new(42, "trade")
            .with_attr("sym", "ACME")
            .with_attr("qty", 3);
        let json = serde_json::to_string(&e).unwrap();
        let back: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }
}
