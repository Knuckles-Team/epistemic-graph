//! Health anomalies and correlated incidents (MEDULLA-DURABLE-KERNEL-R852.1, .2, .4).
//!
//! An anomaly on a resource joins that resource's open incident while it falls
//! inside the correlation window; otherwise it opens a new incident. Incidents
//! move strictly `open -> acknowledged -> resolved -> closed`. Served operations
//! and durable storage are MEDULLA-DURABLE-KERNEL-R852.3 and later children.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The default correlation window: 15 minutes.
pub const DEFAULT_CORRELATION_WINDOW_MS: u64 = 15 * 60 * 1_000;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthAnomaly {
    pub resource: String,
    pub signal: String,
    pub value: f64,
    pub baseline: f64,
    pub observed_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnomalyRef {
    pub resource: String,
    pub signal: String,
    pub observed_at: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    Open,
    Acknowledged,
    Resolved,
    Closed,
}

impl IncidentStatus {
    fn next(self) -> Option<Self> {
        match self {
            Self::Open => Some(Self::Acknowledged),
            Self::Acknowledged => Some(Self::Resolved),
            Self::Resolved => Some(Self::Closed),
            Self::Closed => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Incident {
    pub id: u64,
    pub resources: Vec<String>,
    pub anomalies: Vec<AnomalyRef>,
    pub status: IncidentStatus,
    pub opened_at: u64,
    pub route: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum IncidentError {
    #[error("incident status {from:?} cannot move to {to:?}")]
    StatusOrder {
        from: IncidentStatus,
        to: IncidentStatus,
    },
    #[error("a health anomaly must name its resource")]
    ResourceRequired,
    #[error("unknown incident {0}")]
    UnknownIncident(u64),
}

impl IncidentError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::StatusOrder { .. } => "INCIDENT_STATUS_ORDER",
            Self::ResourceRequired => "ANOMALY_RESOURCE_REQUIRED",
            Self::UnknownIncident(_) => "NOT_FOUND",
        }
    }
}

impl Incident {
    /// Advance exactly one step in the state order; any other change is refused
    /// and leaves the incident unchanged.
    pub fn set_status(&mut self, to: IncidentStatus) -> Result<(), IncidentError> {
        if self.status.next() != Some(to) {
            return Err(IncidentError::StatusOrder {
                from: self.status,
                to,
            });
        }
        self.status = to;
        Ok(())
    }
}

/// Correlates anomalies into incidents by resource and window.
#[derive(Clone, Debug)]
pub struct IncidentBook {
    window_ms: u64,
    incidents: BTreeMap<u64, Incident>,
    open_by_resource: BTreeMap<String, u64>,
    next_id: u64,
}

impl IncidentBook {
    pub fn new(window_ms: u64) -> Self {
        Self {
            window_ms,
            incidents: BTreeMap::new(),
            open_by_resource: BTreeMap::new(),
            next_id: 1,
        }
    }

    pub fn incidents(&self) -> impl Iterator<Item = &Incident> {
        self.incidents.values()
    }

    pub fn get(&self, id: u64) -> Option<&Incident> {
        self.incidents.get(&id)
    }

    /// Attach `anomaly` to its resource's open incident when it lies within the
    /// window of that incident's latest anomaly; otherwise open a new incident.
    pub fn ingest(&mut self, anomaly: &HealthAnomaly) -> Result<u64, IncidentError> {
        if anomaly.resource.trim().is_empty() {
            return Err(IncidentError::ResourceRequired);
        }
        let reference = AnomalyRef {
            resource: anomaly.resource.clone(),
            signal: anomaly.signal.clone(),
            observed_at: anomaly.observed_at,
        };
        if let Some(incident) = self.correlated_open_incident(anomaly) {
            incident.anomalies.push(reference);
            return Ok(incident.id);
        }
        let id = self.next_id;
        self.next_id += 1;
        self.incidents.insert(
            id,
            Incident {
                id,
                resources: vec![anomaly.resource.clone()],
                anomalies: vec![reference],
                status: IncidentStatus::Open,
                opened_at: anomaly.observed_at,
                route: None,
            },
        );
        self.open_by_resource.insert(anomaly.resource.clone(), id);
        Ok(id)
    }

    pub fn set_status(&mut self, id: u64, to: IncidentStatus) -> Result<(), IncidentError> {
        let incident = self
            .incidents
            .get_mut(&id)
            .ok_or(IncidentError::UnknownIncident(id))?;
        incident.set_status(to)?;
        if to != IncidentStatus::Acknowledged {
            self.open_by_resource.retain(|_, open| *open != id);
        }
        Ok(())
    }

    fn correlated_open_incident(&mut self, anomaly: &HealthAnomaly) -> Option<&mut Incident> {
        let id = *self.open_by_resource.get(&anomaly.resource)?;
        let window_ms = self.window_ms;
        let incident = self.incidents.get_mut(&id)?;
        let latest = incident.anomalies.last()?.observed_at;
        (anomaly.observed_at.abs_diff(latest) <= window_ms).then_some(incident)
    }
}

impl Default for IncidentBook {
    fn default() -> Self {
        Self::new(DEFAULT_CORRELATION_WINDOW_MS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anomaly(resource: &str, observed_at: u64) -> HealthAnomaly {
        HealthAnomaly {
            resource: resource.into(),
            signal: "p99_latency_ms".into(),
            value: 950.0,
            baseline: 120.0,
            observed_at,
        }
    }

    // spec: MEDULLA-DURABLE-KERNEL-R852.1
    #[test]
    fn anomaly_and_incident_round_trip_through_the_wire_codec() {
        let a = anomaly("svc:checkout", 1_000);
        let packed = rmp_serde::to_vec_named(&a).unwrap();
        assert_eq!(rmp_serde::from_slice::<HealthAnomaly>(&packed).unwrap(), a);

        let incident = Incident {
            id: 9,
            resources: vec!["svc:checkout".into()],
            anomalies: vec![AnomalyRef {
                resource: "svc:checkout".into(),
                signal: "p99_latency_ms".into(),
                observed_at: 1_000,
            }],
            status: IncidentStatus::Acknowledged,
            opened_at: 1_000,
            route: Some("oncall:payments".into()),
        };
        let packed = rmp_serde::to_vec_named(&incident).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<Incident>(&packed).unwrap(),
            incident
        );
        assert_eq!(
            serde_json::to_value(&incident).unwrap()["status"],
            "acknowledged"
        );
    }

    // spec: MEDULLA-DURABLE-KERNEL-R852.2
    #[test]
    fn anomalies_correlate_by_resource_and_window() {
        let mut book = IncidentBook::default();
        let minute = 60_000;
        let first = book.ingest(&anomaly("svc:a", 0)).unwrap();
        assert_eq!(book.ingest(&anomaly("svc:a", 5 * minute)).unwrap(), first);
        assert_eq!(book.ingest(&anomaly("svc:a", 19 * minute)).unwrap(), first);
        let other = book.ingest(&anomaly("svc:b", 6 * minute)).unwrap();
        assert_ne!(other, first);
        assert_eq!(book.incidents().count(), 2);
        assert_eq!(book.get(first).unwrap().anomalies.len(), 3);

        let late = book.ingest(&anomaly("svc:a", 40 * minute)).unwrap();
        assert_ne!(late, first, "outside the window opens a new incident");

        book.set_status(other, IncidentStatus::Acknowledged)
            .unwrap();
        book.set_status(other, IncidentStatus::Resolved).unwrap();
        let after_resolve = book.ingest(&anomaly("svc:b", 7 * minute)).unwrap();
        assert_ne!(
            after_resolve, other,
            "a resolved incident takes no anomalies"
        );
    }

    // spec: MEDULLA-DURABLE-KERNEL-R852.4
    #[test]
    fn out_of_order_status_and_resourceless_anomaly_are_refused() {
        let mut book = IncidentBook::default();
        let id = book.ingest(&anomaly("svc:a", 0)).unwrap();
        let before = book.get(id).unwrap().clone();
        for to in [
            IncidentStatus::Resolved,
            IncidentStatus::Closed,
            IncidentStatus::Open,
        ] {
            let err = book.set_status(id, to).unwrap_err();
            assert_eq!(err.code(), "INCIDENT_STATUS_ORDER");
            assert_eq!(book.get(id).unwrap(), &before);
        }

        let err = book.ingest(&anomaly(" ", 1)).unwrap_err();
        assert_eq!(err.code(), "ANOMALY_RESOURCE_REQUIRED");
        assert_eq!(book.incidents().count(), 1);
        assert_eq!(book.get(id).unwrap(), &before);
    }
}
