//! The **definition model** — a statechart as typed, serializable data
//! (CONCEPT:INT-P2-2).
//!
//! This is the formal machine M = (S, Σ, δ, s₀, F) made concrete:
//!
//! * **S** — [`StatechartDef::states`], a list of [`State`]s (each with optional
//!   Moore `entry`/`exit` actions).
//! * **Σ** — [`StatechartDef::alphabet`], the explicit event symbols.
//! * **δ** — [`StatechartDef::transitions`], a list of [`Transition`]s
//!   (`from × event → to`, each with an optional Mealy guard + actions).
//! * **s₀** — [`StatechartDef::initial`].
//! * **F** — [`StatechartDef::finals`].
//!
//! The in-memory typed form here is the **source of truth**. It can ALSO be projected
//! into the KG as typed nodes/edges (see [`crate::kg`]) and persisted in redb (see
//! [`crate::store`]), but those are derived views — never the authority.
//!
//! ## Hierarchy / parallel / history (executable)
//!
//! [`State`] carries the DATA for composite states (`children`/`initial_child`),
//! parallel regions (`parallel`), and history markers (`history`) — see requirement 6 —
//! and [`crate::transition::step`] now EXECUTES that shape with SCXML-style semantics: a
//! parent's transition applies to any active descendant, entry runs outer-in / exit
//! inner-out, orthogonal regions each see every event, and a history composite resumes
//! its remembered children. [`crate::check::validate`] accordingly ACCEPTS composite /
//! parallel / history charts (checking their containment structure) instead of rejecting
//! them; a running instance is a [`crate::instance::Configuration`] (a set of active
//! states), not a single state string.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::action::Action;
use crate::guard::Guard;

/// A state identifier (unique within a chart).
pub type StateId = String;
/// An event symbol (a member of the alphabet Σ).
pub type EventName = String;
/// A content-addressed statechart-definition id (`eg:statechart:<hex>`).
pub type DefId = String;

/// A history pseudo-state marker (CONCEPT:INT-P2-2, phase-1 schema-only). Present so
/// a chart can DECLARE shallow/deep history now; the flat interpreter does not yet
/// act on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryKind {
    /// Remember only the immediate active child of this composite state.
    Shallow,
    /// Remember the full nested active configuration.
    Deep,
}

/// A state in S (CONCEPT:INT-P2-2). Atomic in phase-1; the composite fields exist for
/// forward compatibility (see the module doc).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct State {
    /// Unique id within the chart.
    pub id: StateId,
    /// Moore actions performed on ENTERING this state (applied to context in order).
    #[serde(default)]
    pub entry: Vec<Action>,
    /// Moore actions performed on LEAVING this state (applied to context in order).
    #[serde(default)]
    pub exit: Vec<Action>,

    // ── composite-ready, phase-1 schema-only (requirement 6) ────────────────────
    /// Child state ids, when this is a composite (hierarchical) state. Empty ⇒ atomic.
    #[serde(default)]
    pub children: Vec<StateId>,
    /// Whether this composite state's children are orthogonal PARALLEL regions
    /// (all active at once) rather than exclusive sub-states.
    #[serde(default)]
    pub parallel: bool,
    /// The default child entered when this composite state is entered without history.
    #[serde(default)]
    pub initial_child: Option<StateId>,
    /// A history marker, if this composite state remembers its last active child.
    #[serde(default)]
    pub history: Option<HistoryKind>,

    /// Free-form author metadata (never interpreted by the engine).
    #[serde(default)]
    pub meta: BTreeMap<String, serde_json::Value>,
}

impl State {
    /// A minimal atomic state.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            entry: Vec::new(),
            exit: Vec::new(),
            children: Vec::new(),
            parallel: false,
            initial_child: None,
            history: None,
            meta: BTreeMap::new(),
        }
    }

    /// Builder: set entry actions.
    pub fn with_entry(mut self, entry: Vec<Action>) -> Self {
        self.entry = entry;
        self
    }

    /// Builder: set exit actions.
    pub fn with_exit(mut self, exit: Vec<Action>) -> Self {
        self.exit = exit;
        self
    }

    /// Whether this state uses ANY composite/parallel/history feature — i.e. whether
    /// it is beyond what the phase-1 flat interpreter can execute. The single gate the
    /// validator uses to keep flat semantics honest.
    pub fn is_composite(&self) -> bool {
        !self.children.is_empty()
            || self.parallel
            || self.initial_child.is_some()
            || self.history.is_some()
    }
}

/// A transition in δ (CONCEPT:INT-P2-2): `from × event → to`, with an optional Mealy
/// guard and ordered Mealy actions. `guard: None` means an unguarded (`Always`) edge.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Transition {
    /// Source state id.
    pub from: StateId,
    /// The event symbol that triggers this edge (must be in Σ).
    pub event: EventName,
    /// Target state id.
    pub to: StateId,
    /// Guard predicate; `None` ⇒ always enabled. When several transitions share a
    /// `(from, event)`, the first whose guard holds (in declaration order) fires.
    #[serde(default)]
    pub guard: Option<Guard>,
    /// Mealy actions performed AS the transition fires (between exit and entry).
    #[serde(default)]
    pub actions: Vec<Action>,
    /// Optional author label (surfaced in coverage reports and KG projection).
    #[serde(default)]
    pub label: Option<String>,
}

impl Transition {
    /// A minimal unguarded, action-free transition.
    pub fn new(from: impl Into<String>, event: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from: from.into(),
            event: event.into(),
            to: to.into(),
            guard: None,
            actions: Vec::new(),
            label: None,
        }
    }

    /// Builder: attach a guard.
    pub fn with_guard(mut self, guard: Guard) -> Self {
        self.guard = Some(guard);
        self
    }

    /// Builder: attach actions.
    pub fn with_actions(mut self, actions: Vec<Action>) -> Self {
        self.actions = actions;
        self
    }

    /// Whether this edge's guard holds for `(context, event)`. An absent guard is
    /// `Always`.
    pub fn guard_holds(
        &self,
        context: &crate::context::Context,
        event: &crate::context::EventInput,
    ) -> bool {
        self.guard
            .as_ref()
            .is_none_or(|guard| guard.eval(context, event))
    }
}

/// A complete statechart definition: M = (S, Σ, δ, s₀, F) (CONCEPT:INT-P2-2).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatechartDef {
    /// Human-facing chart name (not the id — the id is content-addressed).
    pub name: String,
    /// Author's own semantic version of the chart's shape (NOT the OCC/instance
    /// version). Advisory; part of the content hash.
    #[serde(default)]
    pub schema_version: u32,
    /// S — the states.
    pub states: Vec<State>,
    /// Σ — the event alphabet.
    pub alphabet: Vec<EventName>,
    /// δ — the transitions.
    pub transitions: Vec<Transition>,
    /// s₀ — the initial state id.
    pub initial: StateId,
    /// F — the final state ids.
    #[serde(default)]
    pub finals: Vec<StateId>,
    /// Free-form author metadata (never interpreted by the engine).
    #[serde(default)]
    pub meta: BTreeMap<String, serde_json::Value>,
}

impl StatechartDef {
    /// Look up a state by id.
    pub fn state(&self, id: &str) -> Option<&State> {
        self.states.iter().find(|state| state.id == id)
    }

    /// Whether `id` is a declared state.
    pub fn has_state(&self, id: &str) -> bool {
        self.states.iter().any(|state| state.id == id)
    }

    /// Whether `id` is a final state (member of F).
    pub fn is_final(&self, id: &str) -> bool {
        self.finals.iter().any(|final_id| final_id == id)
    }

    /// Whether `event` is a member of the alphabet Σ.
    pub fn in_alphabet(&self, event: &str) -> bool {
        self.alphabet.iter().any(|symbol| symbol == event)
    }

    /// Every transition whose `(from, event)` matches, in declaration order — the
    /// candidate set the transition function evaluates guards over.
    pub fn transitions_from<'a>(
        &'a self,
        state: &'a str,
        event: &'a str,
    ) -> impl Iterator<Item = &'a Transition> + 'a {
        self.transitions
            .iter()
            .filter(move |t| t.from == state && t.event == event)
    }

    /// The set of declared state ids.
    pub fn state_ids(&self) -> BTreeSet<&str> {
        self.states.iter().map(|state| state.id.as_str()).collect()
    }

    /// The content-addressed definition id (CONCEPT:INT-P2-2), a pure function of the
    /// definition's bytes — so re-defining a byte-identical chart converges on the
    /// SAME id and its store write is idempotent (mirroring `eg-jobs`' `result_ref`
    /// determinism). Determinism note: `serde_json` serializes maps (our `meta` +
    /// nested JSON objects) in sorted key order and preserves list order, so a given
    /// `StatechartDef` value always serializes to identical bytes within a build.
    pub fn def_id(&self) -> DefId {
        let mut hasher = Sha256::new();
        hasher.update(b"eg-statechart.def_id.v1\0");
        let bytes = serde_json::to_vec(self).unwrap_or_default();
        hasher.update(&bytes);
        format!("eg:statechart:{}", hex::encode(&hasher.finalize()[..16]))
    }
}
