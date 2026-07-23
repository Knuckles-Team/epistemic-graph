//! `eg-statechart` — a native finite-state-machine / statechart engine
//! (CONCEPT:INT-P2-2).
//!
//! A first-class, formally-grounded state primitive: a machine **M = (S, Σ, δ, s₀,
//! F)** with **extended state** (a context), **guards** (Mealy predicates that select
//! among transitions), **actions** (Moore entry/exit + Mealy transition effects), a
//! **pure deterministic transition function**, definition-time **completeness /
//! reachability** checks, and a **durable, rehydratable** machine-instance store.
//!
//! It is the disciplined sibling of [`eg-jobs`](../eg_jobs)' `AnalyticsJob`: a typed
//! enum state model, per-call guarded transitions (an invalid *request* is a hard
//! error, never a silent overwrite), terminal/`is_final` semantics, a durable redb
//! record with an OCC `version`, a single wire `Method` variant carrying multiple ops,
//! and a dispatch handler — all mirrored here.
//!
//! ## The design in one paragraph
//!
//! A chart is [`model::StatechartDef`] — inert, serializable data (the SOURCE OF
//! TRUTH). [`transition::transition`] is the ONE semantic function: pure, total, and
//! side-effect-free — given `(def, state, context, event)` it DECIDES the next state,
//! next context, and an ordered list of actions, and RETURNS them. An interpreter (the
//! dispatch handler, an agent executor) is the only impure part; it runs the returned
//! effects. An event with no defined transition is a well-defined **no-op**, which is
//! what makes illegal transitions unrepresentable. A running machine is just a
//! [`instance::MachineInstance`] `(state, context)` row in redb ([`store::StatechartStore`]),
//! so an agent waiting days on an event is durable state on disk, not a live task.
//!
//! ## What lives where
//!
//! * [`model`] — the definition model: [`model::StatechartDef`], [`model::State`]
//!   (composite-ready), [`model::Transition`], ids, `def_id` content addressing.
//! * [`context`] — the extended state [`context::Context`] + the [`context::EventInput`].
//! * [`guard`] — the closed [`guard::Guard`] predicate language.
//! * [`action`] — [`action::Action`] (context mutations applied by δ; effects returned
//!   for the interpreter).
//! * [`transition`] — the pure [`transition::transition`] function + its outcome/errors.
//! * [`instance`] — the durable [`instance::MachineInstance`] record.
//! * [`store`] — [`store::StatechartStore`], the redb-backed definition + instance
//!   store with OCC.
//! * [`check`] — [`check::validate`] (completeness/reachability) + [`check::coverage_matrix`]
//!   (the exhaustive S × Σ table-walk).
//! * [`kg`] — [`kg::project`], the typed KG node/edge projection.
//!
//! ## Phase-1 vs deferred
//!
//! **Hierarchy phase (this crate): FULL statechart execution.** A running instance is a
//! [`instance::Configuration`] (a SET of active states + history memory), and
//! [`transition::step`] implements SCXML-style compound (hierarchy), parallel-region,
//! and shallow/deep history semantics: a parent's transition applies to any active
//! descendant, entry runs outer-in / exit inner-out, orthogonal regions each see every
//! event, and a history composite resumes its remembered children. [`check::validate`]
//! now ACCEPTS composite/parallel/history charts (checking their containment structure)
//! rather than rejecting them. The flat [`transition::transition`] is retained for
//! single-state charts. **Deferred:** Raft-ordered
//! transitions through the consensus `MutationBatch` gateway (the OCC `version` gives
//! single-node correctness today; cluster ordering slots in where `eg-jobs`'
//! `mutate_job_batch` sits).

pub mod action;
pub mod check;
pub mod context;
pub mod guard;
pub mod instance;
pub mod kg;
pub mod model;
pub mod store;
pub mod transition;

pub use action::{Action, ActionValue};
pub use check::{
    coverage, coverage_matrix, validate, CompletenessReport, Coverage, DefError, DefWarning,
};
pub use context::{Context, EventInput};
pub use guard::Guard;
pub use instance::{Configuration, InstanceId, InstanceStatus, MachineInstance};
pub use kg::{project, KgEdge, KgNode, KgProjection};
pub use model::{DefId, EventName, HistoryKind, State, StateId, StatechartDef, Transition};
pub use store::{SendOutcome, StatechartError, StatechartStore};
pub use transition::{
    initial_configuration, step, transition, NoOpReason, StepOutcome, TransitionError,
    TransitionOutcome,
};
