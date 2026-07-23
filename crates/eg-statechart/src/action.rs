//! Actions — the effects a machine DECIDES (CONCEPT:INT-P2-2).
//!
//! Two flavours, both modeled here as one typed, serializable data value:
//!
//! * **Context mutations** (`Assign`/`Remove`) — deterministic edits to the extended
//!   state. These are applied BY the pure transition function itself (they are a pure
//!   function of `(context, event)`), so the `next_context` a transition returns
//!   already reflects them. This is what makes the machine a *Mealy/Moore* machine
//!   with real data flow while keeping [`crate::transition::transition`] free of I/O.
//! * **Emitted effects** (`Emit`/`Log`/`Custom`) — declarations of side effects the
//!   machine WANTS performed. The transition function never performs them; it returns
//!   them in order and an *interpreter* (the dispatch handler, an agent's executor,
//!   …) decides how/whether to run them. This is the classic "the transition function
//!   is a pure decision; the interpreter is the only impure part" split.
//!
//! Because actions are inert typed data, a chart's whole action set can be persisted,
//! shipped, and projected into the KG as `:Action` nodes (see [`crate::kg`]) — the
//! engine never needs executable code to describe what a transition does.

use serde::{Deserialize, Serialize};

use crate::context::{Context, EventInput};

/// Where an [`Action::Assign`] gets its value from.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "from", rename_all = "snake_case")]
pub enum ActionValue {
    /// A literal constant.
    Const { value: serde_json::Value },
    /// Copy a field out of the (object) event payload; absent ⇒ JSON `null`.
    Event { key: String },
    /// Copy a value out of the current context; absent ⇒ JSON `null`.
    Context { key: String },
}

impl ActionValue {
    fn resolve(&self, context: &Context, event: &EventInput) -> serde_json::Value {
        match self {
            ActionValue::Const { value } => value.clone(),
            ActionValue::Event { key } => event
                .payload_field(key)
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            ActionValue::Context { key } => {
                context.get(key).cloned().unwrap_or(serde_json::Value::Null)
            }
        }
    }
}

/// One effect a transition (or a state's entry/exit) declares.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "do", rename_all = "snake_case")]
pub enum Action {
    /// Set a context key. Pure — applied to `next_context` by the transition fn.
    Assign { key: String, value: ActionValue },
    /// Delete a context key. Pure — applied to `next_context` by the transition fn.
    Remove { key: String },
    /// Emit a named signal for the interpreter to route (e.g. raise another event,
    /// notify a subscriber). Never touches context.
    Emit { signal: String },
    /// Emit a diagnostic/log line for the interpreter. Never touches context.
    Log { message: String },
    /// A named, arbitrary effect the interpreter knows how to perform. Never touches
    /// context (the engine has no idea what it does — that is the interpreter's job).
    Custom {
        name: String,
        args: serde_json::Value,
    },
}

impl Action {
    /// Whether this action mutates context (and is therefore applied by the pure
    /// transition function) versus being an interpreter-executed side effect.
    pub fn is_context_mutation(&self) -> bool {
        matches!(self, Action::Assign { .. } | Action::Remove { .. })
    }

    /// Apply this action's context effect (if any) in place. A no-op for the
    /// interpreter-executed variants. Pure.
    pub fn apply(&self, context: &mut Context, event: &EventInput) {
        match self {
            Action::Assign { key, value } => {
                let resolved = value.resolve(context, event);
                context.set(key.clone(), resolved);
            }
            Action::Remove { key } => {
                context.remove(key);
            }
            Action::Emit { .. } | Action::Log { .. } | Action::Custom { .. } => {}
        }
    }
}

/// Apply an ordered action list to a context, in place, returning it. The canonical
/// way the transition function folds `exit ++ transition ++ entry` actions into the
/// `next_context`.
pub fn apply_all(mut context: Context, actions: &[Action], event: &EventInput) -> Context {
    for action in actions {
        action.apply(&mut context, event);
    }
    context
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assign_resolves_const_event_and_context_sources() {
        let mut ctx = Context::new();
        ctx.set("carry", serde_json::json!(7));
        let event = EventInput::with_payload("e", serde_json::json!({"amount": 42}));

        let actions = vec![
            Action::Assign {
                key: "a".into(),
                value: ActionValue::Const {
                    value: serde_json::json!("x"),
                },
            },
            Action::Assign {
                key: "b".into(),
                value: ActionValue::Event {
                    key: "amount".into(),
                },
            },
            Action::Assign {
                key: "c".into(),
                value: ActionValue::Context {
                    key: "carry".into(),
                },
            },
            Action::Assign {
                key: "missing".into(),
                value: ActionValue::Event { key: "nope".into() },
            },
        ];
        let out = apply_all(ctx, &actions, &event);
        assert_eq!(out.get("a"), Some(&serde_json::json!("x")));
        assert_eq!(out.get("b"), Some(&serde_json::json!(42)));
        assert_eq!(out.get("c"), Some(&serde_json::json!(7)));
        assert_eq!(out.get("missing"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn remove_deletes_and_effects_do_not_touch_context() {
        let mut ctx = Context::new();
        ctx.set("gone", serde_json::json!(1));
        let event = EventInput::new("e");
        let actions = vec![
            Action::Remove { key: "gone".into() },
            Action::Emit {
                signal: "beep".into(),
            },
            Action::Log {
                message: "hi".into(),
            },
            Action::Custom {
                name: "http".into(),
                args: serde_json::json!({"url": "x"}),
            },
        ];
        assert!(!actions[1].is_context_mutation());
        let out = apply_all(ctx, &actions, &event);
        assert_eq!(out.get("gone"), None);
        assert!(out.is_empty());
    }
}
