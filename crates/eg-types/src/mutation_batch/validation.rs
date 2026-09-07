use super::*;

const MAX_MUTATION_OPERATIONS: usize = 100_000;
const MAX_MUTATION_OUTBOX_INTENTS: usize = 100_000;
const MAX_MUTATION_OUTBOX_HEADERS: usize = 100_000;
const MAX_MUTATION_WRITE_BYTES: usize = 64 * 1024 * 1024;

impl MutationBatch {
    /// Validate invariants required by every persistence implementation.
    pub fn validate(&self) -> Result<(), String> {
        validate_record_schema(self.schema_version, "batch")?;
        self.validate_identity()?;
        validate_principal(&self.context.principal)?;
        validate_required(&self.batch_id, "mutation batch_id")?;
        validate_required(&self.idempotency_key, "mutation idempotency_key")?;
        validate_operations(self)?;
        validate_version_expectation(self)?;
        validate_authoritative_state(self)?;
        validate_outbox(&self.outbox)?;
        validate_placement(self)
    }

    /// Bound write-side collections and directly owned bytes before any
    /// MessagePack serializer is invoked.
    pub fn validate_write_budget(&self) -> Result<(), String> {
        self.validate()?;
        if self.operations.len() > MAX_MUTATION_OPERATIONS
            || self.outbox.len() > MAX_MUTATION_OUTBOX_INTENTS
        {
            return Err("mutation write exceeds its collection budget".to_string());
        }
        let mut bytes = 0usize;
        let mut headers = 0usize;
        for value in [
            self.batch_id.as_str(),
            self.idempotency_key.as_str(),
            self.context.principal.as_str(),
        ] {
            account_write_bytes(&mut bytes, value.len())?;
        }
        for value in [
            self.context.purpose.as_deref(),
            self.context.policy_fingerprint.as_deref(),
            self.context.trace_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            account_write_bytes(&mut bytes, value.len())?;
        }
        if let Some(state) = &self.authoritative_state {
            account_write_bytes(&mut bytes, state.algorithm.len())?;
            account_write_bytes(&mut bytes, state.digest.len())?;
        }
        for intent in &self.outbox {
            account_write_bytes(&mut bytes, intent.topic.len())?;
            account_write_bytes(&mut bytes, intent.key.len())?;
            account_write_bytes(&mut bytes, intent.payload.len())?;
            headers = headers
                .checked_add(intent.headers.len())
                .filter(|count| *count <= MAX_MUTATION_OUTBOX_HEADERS)
                .ok_or_else(|| "mutation write exceeds its header budget".to_string())?;
            for (key, value) in &intent.headers {
                account_write_bytes(&mut bytes, key.len())?;
                account_write_bytes(&mut bytes, value.len())?;
            }
        }
        Ok(())
    }
}

fn account_write_bytes(total: &mut usize, added: usize) -> Result<(), String> {
    *total = total
        .checked_add(added)
        .filter(|count| *count <= MAX_MUTATION_WRITE_BYTES)
        .ok_or_else(|| "mutation write exceeds its byte budget".to_string())?;
    Ok(())
}

fn validate_principal(principal: &str) -> Result<(), String> {
    let valid = principal
        .strip_prefix("principal:sha256:")
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        });
    if valid {
        Ok(())
    } else {
        Err("mutation principal authority must be an opaque digest".to_string())
    }
}

fn validate_required(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
        return Err(format!("{label} is invalid"));
    }
    Ok(())
}

fn validate_operations(batch: &MutationBatch) -> Result<(), String> {
    if batch.operations.is_empty() {
        return Err("mutation batch must contain at least one operation".to_string());
    }
    for (expected, operation) in batch.operations.iter().enumerate() {
        if operation.ordinal as usize != expected {
            return Err(format!(
                "mutation operation ordinal {} is not contiguous at index {}",
                operation.ordinal, expected
            ));
        }
        match batch.identity.scope() {
            // A graph scope carries every domain whose authoritative state is the
            // graph itself -- row writes, snapshots, RDF, and the lifecycle /
            // control-plane / cross-modal / multi-graph families that are versioned
            // by the same `MUTATION_GRAPH_VERSION` counter. Only a
            // store-authoritative domain (one with its own counter) is rejected
            // here; see `DurabilityDomain::requires_native_scope`.
            MutationScope::Graph { .. } if operation.domain.forbidden_in_graph_scope() => {
                return Err("graph mutation scope contains a store-authoritative operation".to_string());
            }
            MutationScope::Native { domain, .. } if operation.domain != *domain => {
                return Err("native mutation scope domain does not match its operation".to_string());
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_version_expectation(batch: &MutationBatch) -> Result<(), String> {
    match (batch.identity.scope(), batch.version_expectation) {
        (MutationScope::Graph { .. }, VersionExpectation::Graph(_)) => Ok(()),
        (MutationScope::Native { .. }, VersionExpectation::Native(_)) => Ok(()),
        (MutationScope::Native { domain, .. }, VersionExpectation::Unversioned) => {
            let authorized_domain = matches!(
                domain,
                DurabilityDomain::ControlPlane | DurabilityDomain::Lifecycle
            );
            let authorized_capability = batch
                .context
                .verified_capabilities
                .contains(&MutationCapability::UnversionedSystemMutation);
            if batch.identity.tenant().is_system() && authorized_domain && authorized_capability {
                Ok(())
            } else {
                Err(
                    "unversioned mutation requires reserved-system tenant, control-plane/lifecycle scope, and verified capability"
                        .to_string(),
                )
            }
        }
        _ => Err("mutation version expectation does not match its typed scope".to_string()),
    }
}

fn validate_placement(batch: &MutationBatch) -> Result<(), String> {
    if batch.placement_epoch > 0 && batch.fencing_token.is_none() {
        return Err("placed mutation requires a fencing token".to_string());
    }
    Ok(())
}

fn validate_authoritative_state(batch: &MutationBatch) -> Result<(), String> {
    let Some(state) = &batch.authoritative_state else {
        return Ok(());
    };
    let supported_algorithm = matches!(state.algorithm.as_str(), "sha256" | "sha256-row-delta-v2");
    let valid_digest = state.digest.len() == 64
        && state
            .digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !supported_algorithm || !valid_digest {
        return Err(
            "authoritative state requires a supported lowercase sha256 descriptor".to_string(),
        );
    }
    let expected_target = state
        .source_graph_version
        .checked_add(1)
        .ok_or_else(|| "authoritative state source version overflow".to_string())?;
    if state.target_graph_version != expected_target {
        return Err(
            "authoritative state target version must be exactly source version plus one"
                .to_string(),
        );
    }
    if !matches!(
        batch.version_expectation,
        VersionExpectation::Graph(version) if version == state.source_graph_version
    ) {
        return Err(
            "authoritative graph state source version must equal graph version expectation"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_outbox(outbox: &[MutationOutboxIntent]) -> Result<(), String> {
    for intent in outbox {
        validate_required(&intent.topic, "mutation outbox topic")?;
        validate_required(&intent.key, "mutation outbox key")?;
    }
    Ok(())
}

impl MutationBatchRecord {
    pub fn validate(&self) -> Result<(), String> {
        self.batch.validate()?;
        self.validate_identity()?;
        match self.status {
            MutationBatchStatus::Prepared | MutationBatchStatus::Aborted
                if self.committed_version == CommittedVersion::None =>
            {
                Ok(())
            }
            MutationBatchStatus::Committed => validate_committed_expectation(
                self.batch.version_expectation,
                self.committed_version,
            ),
            _ => Err("mutation receipt status has invalid committed version semantics".to_string()),
        }
    }

    pub fn validate_write_budget(&self) -> Result<(), String> {
        self.validate()?;
        self.batch.validate_write_budget()?;
        if let Some(result) = &self.result_msgpack {
            let mut bytes = 0usize;
            account_write_bytes(&mut bytes, result.len())?;
        }
        Ok(())
    }
}

impl MutationOutboxRecord {
    pub fn validate(&self) -> Result<(), String> {
        validate_record_schema(self.schema_version, "outbox")?;
        self.validate_identity()?;
        validate_required(&self.batch_id, "mutation outbox batch_id")?;
        validate_required(&self.intent.topic, "mutation outbox topic")?;
        validate_required(&self.intent.key, "mutation outbox key")?;
        validate_committed_scope(self.identity.scope(), self.committed_version, "outbox")
    }

    pub fn validate_write_budget(&self) -> Result<(), String> {
        self.validate()?;
        let mut bytes = 0usize;
        for size in [
            self.batch_id.len(),
            self.intent.topic.len(),
            self.intent.key.len(),
            self.intent.payload.len(),
        ] {
            account_write_bytes(&mut bytes, size)?;
        }
        if self.intent.headers.len() > MAX_MUTATION_OUTBOX_HEADERS {
            return Err("mutation write exceeds its header budget".to_string());
        }
        for (key, value) in &self.intent.headers {
            account_write_bytes(&mut bytes, key.len())?;
            account_write_bytes(&mut bytes, value.len())?;
        }
        Ok(())
    }
}

impl MutationProjectionCursor {
    pub fn validate(&self) -> Result<(), String> {
        validate_record_schema(self.schema_version, "projection cursor")?;
        self.validate_identity()?;
        validate_required(&self.projection, "mutation projection")?;
        validate_required(&self.batch_id, "mutation projection batch_id")?;
        validate_committed_scope(
            self.identity.scope(),
            self.committed_version,
            "projection cursor",
        )
    }
}

impl MutationBatchCommit {
    pub fn validate(&self) -> Result<(), String> {
        self.record.validate()?;
        self.validate_identity()?;
        if self.record.status != MutationBatchStatus::Committed {
            return Err("mutation commit envelope must contain a committed receipt".to_string());
        }
        Ok(())
    }
}

fn validate_record_schema(version: u16, record_name: &str) -> Result<(), String> {
    if version != MUTATION_BATCH_VERSION {
        return Err(format!(
            "unsupported mutation {record_name} version {version} (expected {MUTATION_BATCH_VERSION})"
        ));
    }
    Ok(())
}

fn validate_committed_expectation(
    expectation: VersionExpectation,
    committed: CommittedVersion,
) -> Result<(), String> {
    match (expectation, committed) {
        (VersionExpectation::Graph(expected), CommittedVersion::Graph { source, target })
            if source == expected && expected.checked_add(1) == Some(target) =>
        {
            Ok(())
        }
        (VersionExpectation::Native(expected), CommittedVersion::Native { source, target })
            if source == expected && expected.checked_add(1) == Some(target) =>
        {
            Ok(())
        }
        (VersionExpectation::Unversioned, CommittedVersion::None) => Ok(()),
        _ => Err("committed mutation version does not match its expectation".to_string()),
    }
}

fn validate_committed_scope(
    scope: &MutationScope,
    committed: CommittedVersion,
    record_name: &str,
) -> Result<(), String> {
    let valid = match (scope, committed) {
        (MutationScope::Graph { .. }, CommittedVersion::Graph { source, target }) => {
            source.checked_add(1) == Some(target)
        }
        (MutationScope::Native { .. }, CommittedVersion::Native { source, target }) => {
            source.checked_add(1) == Some(target)
        }
        (MutationScope::Native { domain, .. }, CommittedVersion::None) => matches!(
            domain,
            DurabilityDomain::ControlPlane | DurabilityDomain::Lifecycle
        ),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(format!(
            "mutation {record_name} committed version does not match its typed scope"
        ))
    }
}

#[cfg(test)]
mod commit_tests {
    use std::collections::BTreeSet;

    use crate::protocol::Method;

    use super::*;

    fn graph_batch(identity: MutationScopeIdentity) -> MutationBatch {
        MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "commit-validation".to_string(),
            context: MutationRequestContext {
                request_id: 7,
                principal: format!("principal:sha256:{}", "a".repeat(64)),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
                verified_capabilities: BTreeSet::new(),
            },
            identity,
            placement_epoch: 0,
            idempotency_key: "commit-validation-key".to_string(),
            version_expectation: VersionExpectation::Graph(4),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Graph,
                domain: DurabilityDomain::GraphRows,
                method: Method::RemoveNode {
                    node_id: "node-a".to_string(),
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 10,
        }
    }

    fn graph_commit(status: MutationBatchStatus) -> MutationBatchCommit {
        let identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-a").unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:commit-validation").unwrap(),
        );
        MutationBatchCommit {
            record: MutationBatchRecord {
                batch: graph_batch(identity.clone()),
                identity: identity.clone(),
                status,
                committed_version: if status == MutationBatchStatus::Committed {
                    CommittedVersion::Graph {
                        source: 4,
                        target: 5,
                    }
                } else {
                    CommittedVersion::None
                },
                result_msgpack: None,
                committed_at_ms: 11,
            },
            identity,
            replayed: false,
        }
    }

    #[test]
    fn commit_envelope_requires_a_complete_committed_record() {
        graph_commit(MutationBatchStatus::Committed)
            .validate()
            .unwrap();

        for status in [MutationBatchStatus::Prepared, MutationBatchStatus::Aborted] {
            let error = graph_commit(status).validate().unwrap_err();
            assert!(error.contains("must contain a committed receipt"));
        }

        let mut wrong_version = graph_commit(MutationBatchStatus::Committed);
        wrong_version.record.committed_version = CommittedVersion::Native {
            source: 4,
            target: 5,
        };
        assert!(wrong_version.validate().is_err());

        let mut wrong_identity = graph_commit(MutationBatchStatus::Committed);
        wrong_identity.identity = MutationScopeIdentity::graph(
            ScopeTenantId::new("tenant-b").unwrap(),
            LogicalName::new("graph-a").unwrap(),
            IncarnationId::new("incarnation:commit-validation").unwrap(),
        );
        assert!(wrong_identity.validate().is_err());
    }
}
