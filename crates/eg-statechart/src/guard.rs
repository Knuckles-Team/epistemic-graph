//! Transition **guards** — the Mealy predicate that selects among transitions on the
//! same event (CONCEPT:INT-P2-2).
//!
//! A guard is a pure boolean predicate over `(context, event)`. It is a small,
//! CLOSED expression language — not an embedded scripting engine — precisely so it
//! stays: (a) side-effect-free and total (a guard can never diverge, panic, or do
//! I/O), (b) serializable as typed data (so a whole chart is inert data that can be
//! persisted, shipped, and — see [`crate::kg`] — projected into the KG as `:Guard`
//! nodes), and (c) exhaustively testable. Guards NEVER mutate anything; only
//! [`crate::action::Action`]s do.
//!
//! When several transitions share a `(from_state, event)`, the transition function
//! evaluates their guards in declaration order and fires the FIRST whose guard holds
//! — this first-match rule is what makes δ deterministic (see [`crate::transition`]).

use serde::{Deserialize, Serialize};

use crate::context::{Context, EventInput};

/// A pure boolean predicate over the machine's context and the incoming event.
///
/// `#[serde(tag = "op")]` so a guard reads as self-describing typed data
/// (`{"op":"eq","key":"n","value":3}`) both on the wire and as a KG node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Guard {
    /// Always true — an unguarded transition. (A transition with no guard at all is
    /// treated as `Always`; this variant lets a chart state it explicitly.)
    Always,
    /// Always false — a permanently disabled edge (useful as a placeholder/veto).
    Never,
    /// The context key exists and is not JSON `null`.
    Exists { key: String },
    /// The context key is absent or JSON `null`.
    Missing { key: String },
    /// The context key equals `value` (structural JSON equality).
    Eq {
        key: String,
        value: serde_json::Value,
    },
    /// The context key is present and does NOT equal `value`.
    Ne {
        key: String,
        value: serde_json::Value,
    },
    /// The context key is a number strictly greater than `value`.
    Gt { key: String, value: f64 },
    /// The context key is a number greater than or equal to `value`.
    Ge { key: String, value: f64 },
    /// The context key is a number strictly less than `value`.
    Lt { key: String, value: f64 },
    /// The context key is a number less than or equal to `value`.
    Le { key: String, value: f64 },
    /// A field of the (object) event payload equals `value`.
    EventEq {
        key: String,
        value: serde_json::Value,
    },
    /// Conjunction — true iff every child holds (empty ⇒ true).
    All { guards: Vec<Guard> },
    /// Disjunction — true iff any child holds (empty ⇒ false).
    Any { guards: Vec<Guard> },
    /// Negation.
    Not { guard: Box<Guard> },
}

impl Guard {
    /// Evaluate the predicate against a context and an event. Pure and total.
    pub fn eval(&self, context: &Context, event: &EventInput) -> bool {
        match self {
            Guard::Always => true,
            Guard::Never => false,
            Guard::Exists { key } => context.has(key),
            Guard::Missing { key } => !context.has(key),
            Guard::Eq { key, value } => context.get(key) == Some(value),
            Guard::Ne { key, value } => {
                matches!(context.get(key), Some(current) if current != value)
            }
            Guard::Gt { key, value } => number(context, key).is_some_and(|n| n > *value),
            Guard::Ge { key, value } => number(context, key).is_some_and(|n| n >= *value),
            Guard::Lt { key, value } => number(context, key).is_some_and(|n| n < *value),
            Guard::Le { key, value } => number(context, key).is_some_and(|n| n <= *value),
            Guard::EventEq { key, value } => event.payload_field(key) == Some(value),
            Guard::All { guards } => guards.iter().all(|guard| guard.eval(context, event)),
            Guard::Any { guards } => guards.iter().any(|guard| guard.eval(context, event)),
            Guard::Not { guard } => !guard.eval(context, event),
        }
    }

    /// The maximum nesting depth of this guard (1 for a leaf). Used by the
    /// definition validator to bound a chart's guard complexity before storage.
    pub fn depth(&self) -> usize {
        match self {
            Guard::All { guards } | Guard::Any { guards } => {
                1 + guards.iter().map(Guard::depth).max().unwrap_or(0)
            }
            Guard::Not { guard } => 1 + guard.depth(),
            _ => 1,
        }
    }
}

fn number(context: &Context, key: &str) -> Option<f64> {
    context.get(key).and_then(serde_json::Value::as_f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(pairs: &[(&str, serde_json::Value)]) -> Context {
        let mut c = Context::new();
        for (k, v) in pairs {
            c.set(*k, v.clone());
        }
        c
    }

    #[test]
    fn comparison_guards_require_a_number() {
        let c = ctx(&[("n", serde_json::json!(5))]);
        let e = EventInput::new("tick");
        assert!(Guard::Gt {
            key: "n".into(),
            value: 4.0
        }
        .eval(&c, &e));
        assert!(!Guard::Gt {
            key: "n".into(),
            value: 5.0
        }
        .eval(&c, &e));
        assert!(Guard::Ge {
            key: "n".into(),
            value: 5.0
        }
        .eval(&c, &e));
        // A missing / non-numeric key never satisfies an ordering guard.
        assert!(!Guard::Lt {
            key: "missing".into(),
            value: 9.0
        }
        .eval(&c, &e));
        let s = ctx(&[("n", serde_json::json!("five"))]);
        assert!(!Guard::Gt {
            key: "n".into(),
            value: 0.0
        }
        .eval(&s, &e));
    }

    #[test]
    fn boolean_combinators_and_event_predicates() {
        let c = ctx(&[("ready", serde_json::json!(true))]);
        let e = EventInput::with_payload("go", serde_json::json!({"force": true}));
        let g = Guard::All {
            guards: vec![
                Guard::Eq {
                    key: "ready".into(),
                    value: serde_json::json!(true),
                },
                Guard::Any {
                    guards: vec![
                        Guard::EventEq {
                            key: "force".into(),
                            value: serde_json::json!(true),
                        },
                        Guard::Never,
                    ],
                },
            ],
        };
        assert!(g.eval(&c, &e));
        assert!(Guard::Not {
            guard: Box::new(Guard::Never)
        }
        .eval(&c, &e));
        // Empty All ⇒ true, empty Any ⇒ false (identity elements).
        assert!(Guard::All { guards: vec![] }.eval(&c, &e));
        assert!(!Guard::Any { guards: vec![] }.eval(&c, &e));
    }
}
