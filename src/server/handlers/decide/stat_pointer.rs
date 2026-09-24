//! The governed pointer store (EH-396, EH-397): one compare-and-set row per
//! pointer in the Agent Library control owner, under the verified carrier's
//! tenant scope. Activation pushes the previous target onto the rollback stack;
//! rollback pops it; both append an audited event. A pointer that moved since
//! it was read is refused, never overwritten.

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::retrieval_pointer::{
    PointerEvent, PointerState, PointerTransition, MAX_POINTER_HISTORY,
};
use eg_types::decision::statistical::StatisticalErrorCode;

use super::stat_support::refusal;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::{decode_artifact, encode_artifact};

/// Who moves which pointer, and when.
pub(super) struct PointerMove<'a> {
    pub(super) tenant: &'a str,
    pub(super) key: String,
    pub(super) principal: &'a str,
    pub(super) now_ms: u64,
}

/// A target and the receipt that qualified it.
pub(super) struct Qualified {
    pub(super) target: String,
    pub(super) receipt_digest: String,
}

fn artifact_key(key: &str) -> String {
    format!("pointer:{key}")
}

/// A pointer's stored bytes (for the compare-and-set) and its state; an
/// absent pointer is the empty state of `key`.
pub(super) fn read_pointer(
    store: &AgentLibraryStore,
    tenant: &str,
    key: &str,
) -> Result<(Option<Vec<u8>>, PointerState), String> {
    let Some(bytes) = store.decision_artifact(tenant, &artifact_key(key))? else {
        let state = PointerState {
            key: key.to_string(),
            ..PointerState::default()
        };
        return Ok((None, state));
    };
    let state = decode_artifact(&bytes, "pointer state")?;
    Ok((Some(bytes), state))
}

fn bounded_push(
    list: &BoundedVec<PointerEvent, MAX_POINTER_HISTORY>,
    event: PointerEvent,
) -> Result<BoundedVec<PointerEvent, MAX_POINTER_HISTORY>, String> {
    let mut events: Vec<PointerEvent> = list.iter().cloned().collect();
    events.push(event);
    let excess = events.len().saturating_sub(MAX_POINTER_HISTORY);
    BoundedVec::new(events.split_off(excess))
}

fn write(
    store: &AgentLibraryStore,
    at: &PointerMove,
    expected: Option<Vec<u8>>,
    state: &PointerState,
) -> Result<(), String> {
    let key = artifact_key(&at.key);
    store.swap_decision_artifact(at.tenant, &key, expected, encode_artifact(state)?)
}

fn event(at: &PointerMove, transition: PointerTransition, to: Option<&Qualified>) -> PointerEvent {
    PointerEvent {
        transition,
        target: to.map(|q| q.target.clone()),
        receipt_digest: to.map(|q| q.receipt_digest.clone()),
        principal: at.principal.to_string(),
        at_ms: at.now_ms,
    }
}

/// Point at a qualified target. Re-activating the active target is a replay.
pub(super) fn activate(
    store: &AgentLibraryStore,
    at: &PointerMove,
    to: &Qualified,
) -> Result<PointerState, String> {
    let (expected, state) = read_pointer(store, at.tenant, &at.key)?;
    if state.target() == Some(to.target.as_str()) {
        return Ok(state);
    }
    let activated = event(at, PointerTransition::Activated, Some(to));
    let stack = match state.active.clone() {
        Some(previous) => bounded_push(&state.stack, previous)?,
        None => state.stack.clone(),
    };
    let next = PointerState {
        key: at.key.clone(),
        active: Some(activated.clone()),
        stack,
        history: bounded_push(&state.history, activated)?,
    };
    write(store, at, expected, &next)?;
    Ok(next)
}

/// Return to the target active before the current one (or to the default
/// when there was none).
pub(super) fn rollback(
    store: &AgentLibraryStore,
    at: &PointerMove,
) -> Result<PointerState, String> {
    let (expected, state) = read_pointer(store, at.tenant, &at.key)?;
    if state.active.is_none() {
        return Err(refusal(
            StatisticalErrorCode::ParameterInvalid,
            "nothing is active behind this pointer",
        ));
    }
    let mut stack: Vec<PointerEvent> = state.stack.iter().cloned().collect();
    let restored = stack.pop().map(|previous| PointerEvent {
        transition: PointerTransition::RolledBack,
        principal: at.principal.to_string(),
        at_ms: at.now_ms,
        ..previous
    });
    let logged = restored
        .clone()
        .unwrap_or_else(|| event(at, PointerTransition::RolledBack, None));
    let next = PointerState {
        key: at.key.clone(),
        active: restored,
        stack: BoundedVec::new(stack)?,
        history: bounded_push(&state.history, logged)?,
    };
    write(store, at, expected, &next)?;
    Ok(next)
}

/// A pointer's state (empty when it never moved).
pub(super) fn status(store: &AgentLibraryStore, at: &PointerMove) -> Result<PointerState, String> {
    Ok(read_pointer(store, at.tenant, &at.key)?.1)
}
