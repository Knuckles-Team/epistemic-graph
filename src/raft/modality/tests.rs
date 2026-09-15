use super::*;
use crate::raft::{NativeMutationCommand, ReplicatedMutation};

fn authority_binding() -> (&'static str, &'static str, &'static str) {
    (
        "carrier-tenant:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "docs/a#b",
        "docs~2fa~23b",
    )
}

fn result() -> Vec<u8> {
    let outcome = eg_modality::ApplyOutcome {
        disposition: eg_modality::ApplyDisposition::Applied,
        observation_version: 1,
        event_sequence: 1,
    };
    rmp_serde::to_vec_named(&encode_sanitized_modality_payload(&outcome).unwrap()).unwrap()
}

fn stream_result() -> Vec<u8> {
    let outcomes = vec![
        eg_modality::ApplyOutcome {
            disposition: eg_modality::ApplyDisposition::Applied,
            observation_version: 1,
            event_sequence: 1,
        },
        eg_modality::ApplyOutcome {
            disposition: eg_modality::ApplyDisposition::Applied,
            observation_version: 2,
            event_sequence: 2,
        },
    ];
    rmp_serde::to_vec_named(&encode_sanitized_modality_payload(&outcomes).unwrap()).unwrap()
}

fn worst_case_stream_result(items: usize) -> Vec<u8> {
    let outcomes = vec![
        eg_modality::ApplyOutcome {
            disposition: eg_modality::ApplyDisposition::IdempotentReplay,
            observation_version: u64::MAX,
            event_sequence: u64::MAX,
        };
        items
    ];
    rmp_serde::to_vec_named(&encode_sanitized_modality_payload(&outcomes).unwrap()).unwrap()
}

fn valid_command() -> SanitizedModalityRaftCommand {
    let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
    SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Document,
        SanitizedModalityMutation::Ingest,
        format!("__eg_internal_served_document_{}", "a".repeat(64)),
        cipher.seal(b"opaque runtime state"),
        format!("sha256:{}", "b".repeat(64)),
        result(),
    )
    .unwrap()
}

#[test]
fn encrypted_command_round_trips_without_raw_source() {
    let source = b"ephemeral non-identifying source fixture";
    let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
    let sealed = cipher.seal(source);
    let command = SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Document,
        SanitizedModalityMutation::Ingest,
        format!("__eg_internal_served_document_{}", "a".repeat(64)),
        sealed,
        format!("sha256:{}", "b".repeat(64)),
        result(),
    )
    .unwrap();
    let replicated = ReplicatedMutation::served_modality(command.clone());
    let encoded = rmp_serde::to_vec_named(&replicated).unwrap();
    assert!(!encoded.windows(source.len()).any(|window| window == source));
    let decoded: ReplicatedMutation = rmp_serde::from_slice(&encoded).unwrap();
    let ReplicatedMutation::Native {
        command: NativeMutationCommand::ServedModality { command: decoded },
    } = decoded
    else {
        panic!("sanitized modality command did not round-trip as its typed variant");
    };
    decoded.validate("cluster-auth-secret").unwrap();
    assert_eq!(decoded.schema_version, SANITIZED_MODALITY_CODEC_VERSION);
    assert_eq!(
        decoded.result.schema_version,
        SANITIZED_MODALITY_CODEC_VERSION
    );
    assert_eq!(decoded.result.kind, SanitizedModalityResultKind::Single);
}

#[test]
fn stream_result_uses_the_typed_bounded_result_schema() {
    let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
    let command = SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Document,
        SanitizedModalityMutation::IngestStream,
        format!("__eg_internal_served_document_{}", "a".repeat(64)),
        cipher.seal(b"opaque runtime state"),
        format!("sha256:{}", "b".repeat(64)),
        stream_result(),
    )
    .unwrap();
    assert_eq!(command.result.kind, SanitizedModalityResultKind::Stream);
    assert_eq!(command.result.outcomes.len(), 2);
    command.validate("cluster-auth-secret").unwrap();
}

#[test]
fn worst_case_stream_max_fits_and_one_more_is_rejected() {
    let accepted = worst_case_stream_result(MAX_INGEST_STREAM_ITEMS);
    assert!(accepted.len() <= MAX_REPLICATED_MODALITY_RESULT_BYTES);
    let decoded = SanitizedModalityResult::from_wire(
        eg_types::ServedModalityKind::Document,
        SanitizedModalityMutation::IngestStream,
        &accepted,
    )
    .unwrap();
    assert_eq!(decoded.outcomes.len(), MAX_INGEST_STREAM_ITEMS);

    let rejected = worst_case_stream_result(MAX_INGEST_STREAM_ITEMS + 1);
    assert!(rejected.len() > MAX_REPLICATED_MODALITY_RESULT_BYTES);
    assert!(SanitizedModalityResult::from_wire(
        eg_types::ServedModalityKind::Document,
        SanitizedModalityMutation::IngestStream,
        &rejected,
    )
    .is_err());
}

#[test]
fn malformed_result_type_length_version_and_digest_fail_closed() {
    let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
    let node_id = format!("__eg_internal_served_audio_{}", "a".repeat(64));
    let receipt = format!("sha256:{}", "b".repeat(64));
    let wrong_type = rmp_serde::to_vec_named(&crate::protocol::ResultPayload::Bool(true)).unwrap();
    assert!(SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Audio,
        SanitizedModalityMutation::Delete,
        node_id.clone(),
        cipher.seal(b"opaque runtime state"),
        receipt.clone(),
        wrong_type,
    )
    .is_err());

    assert!(SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Audio,
        SanitizedModalityMutation::Delete,
        node_id.clone(),
        cipher.seal(b"opaque runtime state"),
        receipt.clone(),
        vec![0u8; MAX_REPLICATED_MODALITY_RESULT_BYTES + 1],
    )
    .is_err());

    let mut command = SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Audio,
        SanitizedModalityMutation::Delete,
        node_id,
        cipher.seal(b"opaque runtime state"),
        receipt,
        result(),
    )
    .unwrap();
    command.schema_version = SANITIZED_MODALITY_CODEC_VERSION + 1;
    assert!(command.validate("cluster-auth-secret").is_err());

    let mut command = SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Audio,
        SanitizedModalityMutation::Delete,
        format!("__eg_internal_served_audio_{}", "a".repeat(64)),
        cipher.seal(b"opaque runtime state"),
        format!("sha256:{}", "b".repeat(64)),
        result(),
    )
    .unwrap();
    command.result.operation = SanitizedModalityMutation::IngestStream;
    assert!(command.validate("cluster-auth-secret").is_err());

    let mut command = SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Audio,
        SanitizedModalityMutation::Delete,
        format!("__eg_internal_served_audio_{}", "a".repeat(64)),
        cipher.seal(b"opaque runtime state"),
        format!("sha256:{}", "b".repeat(64)),
        result(),
    )
    .unwrap();
    command.result_sha256 = "0".repeat(64);
    assert!(command.validate("cluster-auth-secret").is_err());

    let mut command = SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Audio,
        SanitizedModalityMutation::Delete,
        format!("__eg_internal_served_audio_{}", "a".repeat(64)),
        cipher.seal(b"opaque runtime state"),
        format!("sha256:{}", "b".repeat(64)),
        result(),
    )
    .unwrap();
    command.result.outcomes[0].event_sequence = 99;
    assert!(command.validate("cluster-auth-secret").is_err());
}

#[test]
fn unsealed_or_forged_replica_state_fails_closed() {
    assert!(SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Audio,
        SanitizedModalityMutation::Delete,
        format!("__eg_internal_served_audio_{}", "a".repeat(64)),
        b"plaintext".to_vec(),
        format!("sha256:{}", "b".repeat(64)),
        result(),
    )
    .is_err());

    let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
    let command = SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Video,
        SanitizedModalityMutation::Restore,
        format!("__eg_internal_served_video_{}", "a".repeat(64)),
        cipher.seal(b"opaque state"),
        format!("sha256:{}", "b".repeat(64)),
        result(),
    )
    .unwrap();
    assert!(command.validate("wrong-secret").is_err());
}

#[test]
fn command_transplant_across_tenant_or_graph_fails_closed() {
    let command = valid_command();
    let (tenant_scope, graph_name, graph_fname) = authority_binding();
    command
        .validate_for_request("cluster-auth-secret", tenant_scope, graph_name, graph_fname)
        .unwrap();

    assert!(command
        .validate_for_request(
            "cluster-auth-secret",
            "carrier-tenant:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            graph_name,
            graph_fname,
        )
        .is_err());
    let other_graph = "tenant-a:docs/other";
    assert!(command
        .validate_for_request(
            "cluster-auth-secret",
            tenant_scope,
            other_graph,
            &crate::persist::sanitize(other_graph),
        )
        .is_err());
    assert!(command
        .validate_for_request(
            "cluster-auth-secret",
            tenant_scope,
            graph_name,
            "different-physical-key",
        )
        .is_err());
}

#[test]
fn authority_fields_are_inside_the_command_hmac() {
    let command = valid_command();

    let mut changed_tenant = command.clone();
    changed_tenant.authority.tenant_scope =
        "carrier-tenant:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            .to_string();
    assert!(changed_tenant.validate("cluster-auth-secret").is_err());

    let mut changed_graph = command.clone();
    changed_graph.authority.graph_name = "tenant-a:docs/other".to_string();
    changed_graph.authority.graph_fname =
        crate::persist::sanitize(&changed_graph.authority.graph_name);
    assert!(changed_graph.validate("cluster-auth-secret").is_err());

    let mut changed_physical = command;
    changed_physical.authority.graph_fname = "different-physical-key".to_string();
    assert!(changed_physical.validate("cluster-auth-secret").is_err());
}

#[test]
fn mutation_batch_audit_and_outbox_retain_only_the_safe_receipt() {
    let source = b"ephemeral source excluded from durable coordination";
    let cipher = crate::crypto::ValueCipher::from_key_material(b"replica-state-key");
    let sealed = cipher.seal(source);
    let command = SanitizedModalityRaftCommand::new(
        "cluster-auth-secret",
        authority_binding(),
        eg_types::ServedModalityKind::Image,
        SanitizedModalityMutation::MoveToCold,
        format!("__eg_internal_served_image_{}", "a".repeat(64)),
        sealed.clone(),
        format!("sha256:{}", "b".repeat(64)),
        result(),
    )
    .unwrap();
    let safe_receipt = command.receipt_method();
    let batch = crate::server::mutation_batch::compile_methods(
        crate::server::mutation_batch::CompileBatch {
            batch_id: "opaque-batch",
            request_id: 1,
            attempt_nonce: None,
            principal: Some(
                "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            tenant: "opaque-tenant",
            graph: "opaque-graph",
            placement_epoch: 1,
            idempotency_key: "opaque-idempotency",
            expected_graph_version: Some(0),
            fencing_token: Some(1),
            created_at_ms: 1,
            default_surface: crate::mutation_batch::MutationSurface::Graph,
            authoritative_state: Some(crate::mutation_batch::MutationStateDescriptor {
                algorithm: "sha256".to_string(),
                digest: "c".repeat(64),
                source_graph_version: 0,
                target_graph_version: 1,
            }),
        },
        vec![safe_receipt],
    )
    .unwrap();
    let encoded = rmp_serde::to_vec_named(&batch).unwrap();
    assert!(!encoded.windows(source.len()).any(|window| window == source));
    assert!(!encoded.windows(sealed.len()).any(|window| window == sealed));
    assert_eq!(batch.outbox.len(), 1);
    assert!(crate::audit::audit_line(&batch.operations[0].method)
        .is_some_and(|line| line.starts_with("AUTHORITATIVE_STATE_MUTATION|sha256:")));
}
