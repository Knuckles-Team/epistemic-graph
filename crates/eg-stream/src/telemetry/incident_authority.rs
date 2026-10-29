//! Served incident authority (MEDULLA-UNIFIED-DATA-PLANE-R760). The engine owns
//! anomaly records and incidents per tenant: correlation groups anomalies by
//! entity and time window in one pass, an open incident with the same
//! signature is reused, and a status moves only open -> acknowledged ->
//! resolved. Every call carries a tenant scope; a read of another tenant's
//! incident is refused.
//!
//! It sits beside the CEP-derived facts of [`super::derive`], which detect
//! anomalies from declared patterns; this module keeps their served,
//! tenant-scoped lifecycle.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

/// One anomaly reading as recorded through `incident.record_anomaly`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AnomalyReading {
    pub entity_ref: String,
    pub signal: String,
    pub observed: f64,
    pub baseline: f64,
    pub at_ms: u64,
}

/// Incident severity, from the largest relative deviation in its evidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentSeverity {
    Low,
    Medium,
    High,
}

/// Incident status; it only moves forward one step at a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncidentStatus {
    Open,
    Acknowledged,
    Resolved,
}

impl IncidentStatus {
    fn next(self) -> Option<Self> {
        match self {
            Self::Open => Some(Self::Acknowledged),
            Self::Acknowledged => Some(Self::Resolved),
            Self::Resolved => None,
        }
    }
}

/// One served incident and its evidence.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IncidentRecord {
    pub id: String,
    pub tenant: String,
    pub signature_digest: String,
    pub severity: IncidentSeverity,
    pub status: IncidentStatus,
    pub opened_at_ms: u64,
    pub evidence: Vec<AnomalyReading>,
    pub status_history: Vec<IncidentStatus>,
}

/// A refused call, with its stable code. A refusal changes no state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IncidentRefusal {
    /// `TENANT_SCOPE_REQUIRED`
    TenantScopeRequired,
    /// `INCIDENT_STATUS_SKIP`
    StatusSkip {
        from: IncidentStatus,
        to: IncidentStatus,
    },
    /// `INCIDENT_NOT_FOUND`: absent, or owned by another tenant.
    NotFound(String),
}

impl IncidentRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::TenantScopeRequired => "TENANT_SCOPE_REQUIRED",
            Self::StatusSkip { .. } => "INCIDENT_STATUS_SKIP",
            Self::NotFound(_) => "INCIDENT_NOT_FOUND",
        }
    }
}

fn scope(tenant: Option<&str>) -> Result<&str, IncidentRefusal> {
    match tenant.map(str::trim) {
        Some(t) if !t.is_empty() => Ok(t),
        _ => Err(IncidentRefusal::TenantScopeRequired),
    }
}

/// FNV-1a 64-bit digest, hex encoded: stable across runs and targets.
fn digest(text: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn severity(evidence: &[AnomalyReading]) -> IncidentSeverity {
    let worst = evidence
        .iter()
        .map(|a| {
            let base = a.baseline.abs().max(f64::EPSILON);
            (a.observed - a.baseline).abs() / base
        })
        .fold(0.0_f64, f64::max);
    if worst >= 1.0 {
        IncidentSeverity::High
    } else if worst >= 0.25 {
        IncidentSeverity::Medium
    } else {
        IncidentSeverity::Low
    }
}

/// The per-tenant incident store.
#[derive(Clone, Debug, Default)]
pub struct IncidentAuthority {
    incidents: BTreeMap<String, IncidentRecord>,
    open_by_signature: HashMap<(String, String), String>,
    next_id: u64,
}

impl IncidentAuthority {
    /// Correlate `anomalies` for `tenant`: one pass groups them by entity
    /// and `window_ms` bucket; each group opens an incident or joins the open
    /// incident with the same signature. Returns the touched incident ids.
    pub fn correlate(
        &mut self,
        tenant: Option<&str>,
        anomalies: Vec<AnomalyReading>,
        window_ms: u64,
    ) -> Result<Vec<String>, IncidentRefusal> {
        let tenant = scope(tenant)?.to_string();
        let window = window_ms.max(1);
        let mut groups: BTreeMap<(String, u64), Vec<AnomalyReading>> = BTreeMap::new();
        for anomaly in anomalies {
            let key = (anomaly.entity_ref.clone(), anomaly.at_ms / window);
            groups.entry(key).or_default().push(anomaly);
        }
        let mut touched = BTreeSet::new();
        for ((entity, _bucket), evidence) in groups {
            let signals: BTreeSet<&str> = evidence.iter().map(|a| a.signal.as_str()).collect();
            let signature = digest(&format!(
                "{tenant}|{entity}|{}",
                signals.into_iter().collect::<Vec<_>>().join(",")
            ));
            let id = self.open_or_join(&tenant, signature, evidence);
            touched.insert(id);
        }
        Ok(touched.into_iter().collect())
    }

    fn open_or_join(
        &mut self,
        tenant: &str,
        signature: String,
        evidence: Vec<AnomalyReading>,
    ) -> String {
        let key = (tenant.to_string(), signature.clone());
        if let Some(id) = self.open_by_signature.get(&key) {
            let record = self.incidents.get_mut(id).expect("indexed incident exists");
            record.evidence.extend(evidence);
            record.severity = record.severity.max(severity(&record.evidence));
            return id.clone();
        }
        self.next_id += 1;
        let id = format!("inc-{}-{}", tenant, self.next_id);
        let opened_at_ms = evidence.iter().map(|a| a.at_ms).min().unwrap_or(0);
        let record = IncidentRecord {
            id: id.clone(),
            tenant: tenant.to_string(),
            signature_digest: signature,
            severity: severity(&evidence),
            status: IncidentStatus::Open,
            opened_at_ms,
            evidence,
            status_history: vec![IncidentStatus::Open],
        };
        self.incidents.insert(id.clone(), record);
        self.open_by_signature.insert(key, id.clone());
        id
    }

    /// Read one incident in `tenant`'s scope.
    pub fn evidence(
        &self,
        tenant: Option<&str>,
        id: &str,
    ) -> Result<&IncidentRecord, IncidentRefusal> {
        let tenant = scope(tenant)?;
        self.incidents
            .get(id)
            .filter(|record| record.tenant == tenant)
            .ok_or_else(|| IncidentRefusal::NotFound(id.to_string()))
    }

    /// Move an incident one status forward. A resolved incident stops
    /// absorbing new anomalies of its signature.
    pub fn set_status(
        &mut self,
        tenant: Option<&str>,
        id: &str,
        to: IncidentStatus,
    ) -> Result<(), IncidentRefusal> {
        let from = self.evidence(tenant, id)?.status;
        if from.next() != Some(to) {
            return Err(IncidentRefusal::StatusSkip { from, to });
        }
        let record = self.incidents.get_mut(id).expect("checked above");
        record.status = to;
        record.status_history.push(to);
        if to == IncidentStatus::Resolved {
            let key = (record.tenant.clone(), record.signature_digest.clone());
            self.open_by_signature.remove(&key);
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.incidents.len()
    }

    pub fn is_empty(&self) -> bool {
        self.incidents.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(entity: &str, signal: &str, observed: f64, at_ms: u64) -> AnomalyReading {
        AnomalyReading {
            entity_ref: entity.to_string(),
            signal: signal.to_string(),
            observed,
            baseline: 100.0,
            at_ms,
        }
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R760.1
    #[test]
    fn incident_model_round_trips() {
        let mut authority = IncidentAuthority::default();
        let ids = authority
            .correlate(
                Some("acme"),
                vec![reading("svc:api", "latency", 250.0, 10)],
                60_000,
            )
            .unwrap();
        let record = authority.evidence(Some("acme"), &ids[0]).unwrap().clone();
        assert_eq!(record.tenant, "acme");
        assert_eq!(record.status, IncidentStatus::Open);
        assert_eq!(record.severity, IncidentSeverity::High);
        assert_eq!(record.opened_at_ms, 10);
        assert_eq!(record.signature_digest.len(), 16);
        let json = serde_json::to_string(&record).unwrap();
        assert_eq!(
            serde_json::from_str::<IncidentRecord>(&json).unwrap(),
            record
        );
        assert!(serde_json::from_str::<IncidentStatus>("\"closed\"").is_err());
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R760.2
    #[test]
    fn correlation_groups_reuses_open_incident_and_orders_status() {
        let mut authority = IncidentAuthority::default();
        let anomalies: Vec<AnomalyReading> = (0..10_000u64)
            .map(|i| reading(&format!("svc:{}", i % 4), "errors", 130.0, i))
            .collect();
        let ids = authority
            .correlate(Some("acme"), anomalies, 1_000_000)
            .unwrap();
        assert_eq!(ids.len(), 4, "one incident per entity in the window");
        let again = authority
            .correlate(
                Some("acme"),
                vec![reading("svc:1", "errors", 140.0, 5)],
                1_000_000,
            )
            .unwrap();
        assert!(ids.contains(&again[0]), "an open incident is reused");
        assert_eq!(authority.len(), 4);

        let id = &again[0];
        authority
            .set_status(Some("acme"), id, IncidentStatus::Acknowledged)
            .unwrap();
        authority
            .set_status(Some("acme"), id, IncidentStatus::Resolved)
            .unwrap();
        let fresh = authority
            .correlate(
                Some("acme"),
                vec![reading("svc:1", "errors", 140.0, 6)],
                1_000_000,
            )
            .unwrap();
        assert_ne!(&fresh[0], id, "a resolved incident is not reused");
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R760.4
    #[test]
    fn refusals_change_nothing() {
        let mut authority = IncidentAuthority::default();
        let error = authority
            .correlate(None, vec![reading("svc:a", "cpu", 1.0, 1)], 10)
            .unwrap_err();
        assert_eq!(error.code(), "TENANT_SCOPE_REQUIRED");
        assert_eq!(
            authority.correlate(Some(" "), vec![], 10).unwrap_err(),
            IncidentRefusal::TenantScopeRequired
        );
        assert!(authority.is_empty());

        let ids = authority
            .correlate(Some("acme"), vec![reading("svc:a", "cpu", 1.0, 1)], 10)
            .unwrap();
        let error = authority
            .set_status(Some("acme"), &ids[0], IncidentStatus::Resolved)
            .unwrap_err();
        assert_eq!(error.code(), "INCIDENT_STATUS_SKIP");
        assert_eq!(
            authority.evidence(Some("acme"), &ids[0]).unwrap().status,
            IncidentStatus::Open
        );
        let error = authority.evidence(Some("globex"), &ids[0]).unwrap_err();
        assert_eq!(error.code(), "INCIDENT_NOT_FOUND");
        let error = authority
            .set_status(Some("globex"), &ids[0], IncidentStatus::Acknowledged)
            .unwrap_err();
        assert_eq!(error.code(), "INCIDENT_NOT_FOUND");
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R760.5
    #[test]
    fn evidence_per_signature_and_state_order() {
        let mut authority = IncidentAuthority::default();
        let batch = vec![
            reading("svc:a", "cpu", 120.0, 1),
            reading("svc:a", "cpu", 125.0, 2),
            reading("svc:b", "cpu", 300.0, 3),
        ];
        let ids = authority.correlate(Some("acme"), batch, 1_000).unwrap();
        assert_eq!(ids.len(), 2);
        let a = authority.evidence(Some("acme"), &ids[0]).unwrap().clone();
        let b = authority.evidence(Some("acme"), &ids[1]).unwrap().clone();
        assert_ne!(a.signature_digest, b.signature_digest);
        let (a, b) = if a.evidence[0].entity_ref == "svc:a" {
            (a, b)
        } else {
            (b, a)
        };
        assert_eq!(a.evidence.len(), 2);
        assert_eq!(b.evidence.len(), 1);
        assert_eq!(a.severity, IncidentSeverity::Medium);
        authority
            .set_status(Some("acme"), &a.id, IncidentStatus::Acknowledged)
            .unwrap();
        authority
            .set_status(Some("acme"), &a.id, IncidentStatus::Resolved)
            .unwrap();
        assert_eq!(
            authority
                .evidence(Some("acme"), &a.id)
                .unwrap()
                .status_history,
            [
                IncidentStatus::Open,
                IncidentStatus::Acknowledged,
                IncidentStatus::Resolved
            ]
        );
    }
}
