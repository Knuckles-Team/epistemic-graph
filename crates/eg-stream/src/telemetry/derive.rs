//! CEP-derived `HealthAnomaly` and `Incident` facts (EH-409).
//!
//! Anomalies and incidents are not computed by bespoke thresholds: they come
//! from **declared CEP patterns** run by the same bounded NFA ([`crate::run`])
//! every other CEP query uses.
//!
//! * Each [`BehaviourObservation`] becomes one [`Event`] keyed
//!   [`OBSERVATION_EVENT`] at its window start, with its numbers as attributes
//!   (see [`observation_event`]). An [`AnomalyRule`] runs its pattern over ONE
//!   entity's events at a time, so a pattern can never stitch two entities'
//!   windows into one match. Overlapping matches of the same rule on the same
//!   entity are coalesced into one [`HealthAnomaly`] covering their union, with
//!   every matched observation as evidence.
//! * Each anomaly becomes one [`Event`] keyed [`ANOMALY_EVENT`]. An
//!   [`IncidentRule`] runs its pattern across ALL entities; a match counts only
//!   when it spans at least `min_entities` distinct entities, and overlapping
//!   qualifying matches coalesce into one [`Incident`].
//!
//! Output order and ids are functions of the inputs alone.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::binding::EntityRef;
use super::rollup::BehaviourObservation;
use crate::cep::{run, CepPattern, Match, Window};
use crate::event::Event;

/// The event key an observation is presented to CEP under.
pub const OBSERVATION_EVENT: &str = "BehaviourObservation";
/// The event key an anomaly is presented to CEP under.
pub const ANOMALY_EVENT: &str = "HealthAnomaly";

/// A declared pattern over one entity's observations.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnomalyRule {
    pub id: String,
    /// The anomaly kind recorded on the fact (`error_burst`, `latency_regression`, …).
    pub kind: String,
    pub pattern: CepPattern,
    pub window: Window,
}

/// A declared pattern over anomalies across entities.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IncidentRule {
    pub id: String,
    pub pattern: CepPattern,
    pub window: Window,
    /// The fewest distinct entities a match must involve.
    pub min_entities: usize,
}

/// One entity observed off its declared behaviour over `[start_ms, end_ms]`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthAnomaly {
    pub id: String,
    pub rule: String,
    pub kind: String,
    pub entity: EntityRef,
    pub start_ms: u64,
    pub end_ms: u64,
    /// The observation ids the pattern matched.
    pub evidence: Vec<String>,
}

/// Correlated anomalies on several entities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Incident {
    pub id: String,
    pub rule: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub entities: Vec<EntityRef>,
    /// The anomaly ids the pattern matched.
    pub anomalies: Vec<String>,
}

/// An observation as a CEP event. Attributes: `observation`, `entity`, `class`,
/// `requests`, `errors`, `error_ratio`, `rate_per_sec`, and `latency_p50_ms` /
/// `latency_p95_ms` when the window had latency samples.
pub fn observation_event(observation: &BehaviourObservation) -> Event {
    let mut event = Event::new(observation.window_start_ms, OBSERVATION_EVENT)
        .with_attr("observation", observation.id.clone())
        .with_attr("entity", observation.entity.id.clone())
        .with_attr("class", observation.entity.class.label())
        .with_attr("requests", observation.requests)
        .with_attr("errors", observation.errors)
        .with_attr("error_ratio", observation.error_ratio)
        .with_attr("rate_per_sec", observation.rate_per_sec);
    for (field, value) in [
        ("latency_p50_ms", observation.latency_p50_ms),
        ("latency_p95_ms", observation.latency_p95_ms),
    ] {
        if let Some(value) = value {
            event = event.with_attr(field, value);
        }
    }
    event
}

fn anomaly_event(anomaly: &HealthAnomaly) -> Event {
    Event::new(anomaly.start_ms, ANOMALY_EVENT)
        .with_attr("anomaly", anomaly.id.clone())
        .with_attr("entity", anomaly.entity.id.clone())
        .with_attr("class", anomaly.entity.class.label())
        .with_attr("kind", anomaly.kind.clone())
}

/// A match reduced to its interval and the ids it matched.
struct Interval {
    start_ms: u64,
    end_ms: u64,
    ids: BTreeSet<String>,
}

/// Merge overlapping (or touching) intervals, ascending by start.
fn coalesce(mut spans: Vec<Interval>) -> Vec<Interval> {
    spans.sort_by_key(|span| (span.start_ms, span.end_ms));
    let mut merged: Vec<Interval> = Vec::new();
    for span in spans {
        match merged.last_mut() {
            Some(last) if span.start_ms <= last.end_ms => {
                last.end_ms = last.end_ms.max(span.end_ms);
                last.ids.extend(span.ids);
            }
            _ => merged.push(span),
        }
    }
    merged
}

fn match_span(found: &Match, id_field: &str) -> Interval {
    Interval {
        start_ms: found.start_ts,
        end_ms: found.end_ts,
        ids: found
            .events
            .iter()
            .filter_map(|event| event.attrs.get(id_field).and_then(Value::as_str))
            .map(str::to_string)
            .collect(),
    }
}

/// Run every anomaly rule over every entity's observations.
pub fn detect_anomalies(
    rules: &[AnomalyRule],
    observations: &[BehaviourObservation],
) -> Vec<HealthAnomaly> {
    let mut per_entity: BTreeMap<&EntityRef, Vec<Event>> = BTreeMap::new();
    for observation in observations {
        per_entity
            .entry(&observation.entity)
            .or_default()
            .push(observation_event(observation));
    }
    let window_ends: BTreeMap<&str, u64> = observations
        .iter()
        .map(|observation| (observation.id.as_str(), observation.window_end_ms))
        .collect();
    let mut anomalies = Vec::new();
    for rule in rules {
        for (entity, events) in &per_entity {
            let spans = run(&rule.pattern, events, rule.window)
                .iter()
                .map(|found| match_span(found, "observation"))
                .collect();
            anomalies.extend(
                coalesce(spans)
                    .into_iter()
                    .map(|span| anomaly(rule, entity, span, &window_ends)),
            );
        }
    }
    anomalies
}

/// One coalesced match as an anomaly. It ends where its last matched window
/// ends, not where that window starts.
fn anomaly(
    rule: &AnomalyRule,
    entity: &EntityRef,
    span: Interval,
    window_ends: &BTreeMap<&str, u64>,
) -> HealthAnomaly {
    let end_ms = span
        .ids
        .iter()
        .filter_map(|id| window_ends.get(id.as_str()).copied())
        .fold(span.end_ms, u64::max);
    HealthAnomaly {
        id: format!(
            "anomaly:{}:{}:{}:{}:{}",
            rule.id,
            entity.class.label(),
            entity.id,
            span.start_ms,
            end_ms
        ),
        rule: rule.id.clone(),
        kind: rule.kind.clone(),
        entity: entity.clone(),
        start_ms: span.start_ms,
        end_ms,
        evidence: span.ids.into_iter().collect(),
    }
}

/// Run every incident rule over all anomalies.
pub fn correlate_incidents(rules: &[IncidentRule], anomalies: &[HealthAnomaly]) -> Vec<Incident> {
    let by_id: BTreeMap<&str, &HealthAnomaly> = anomalies
        .iter()
        .map(|anomaly| (anomaly.id.as_str(), anomaly))
        .collect();
    let mut events: Vec<Event> = anomalies.iter().map(anomaly_event).collect();
    events.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then_with(|| anomaly_key(left).cmp(anomaly_key(right)))
    });
    let mut incidents = Vec::new();
    for rule in rules {
        let spans = run(&rule.pattern, &events, rule.window)
            .iter()
            .map(|found| match_span(found, "anomaly"))
            .filter(|span| entities_of(&span.ids, &by_id).len() >= rule.min_entities)
            .collect();
        incidents.extend(
            coalesce(spans)
                .into_iter()
                .map(|span| incident(rule, span, &by_id)),
        );
    }
    incidents
}

fn anomaly_key(event: &Event) -> &str {
    event
        .attrs
        .get("anomaly")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn entities_of(
    ids: &BTreeSet<String>,
    by_id: &BTreeMap<&str, &HealthAnomaly>,
) -> BTreeSet<EntityRef> {
    ids.iter()
        .filter_map(|id| by_id.get(id.as_str()))
        .map(|anomaly| anomaly.entity.clone())
        .collect()
}

/// One coalesced match as an incident. It ends where its last anomaly ends.
fn incident(
    rule: &IncidentRule,
    span: Interval,
    by_id: &BTreeMap<&str, &HealthAnomaly>,
) -> Incident {
    let end_ms = span
        .ids
        .iter()
        .filter_map(|id| by_id.get(id.as_str()))
        .map(|anomaly| anomaly.end_ms)
        .fold(span.end_ms, u64::max);
    Incident {
        id: format!("incident:{}:{}:{}", rule.id, span.start_ms, end_ms),
        rule: rule.id.clone(),
        start_ms: span.start_ms,
        end_ms,
        entities: entities_of(&span.ids, by_id).into_iter().collect(),
        anomalies: span.ids.into_iter().collect(),
    }
}
