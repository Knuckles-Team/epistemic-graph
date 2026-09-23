use super::*;

fn digest(byte: u8) -> Digest256 {
    Digest256::from_bytes([byte; 32])
}

fn id(raw: &str) -> OpaqueId {
    OpaqueId::new(raw).unwrap()
}

fn resource(raw: &str) -> crate::contract::ResourceId {
    crate::contract::ResourceId::new(raw).unwrap()
}

fn capability(chosen_token: bool, capture_enabled: bool) -> OpenWeightPolicyCapability {
    OpenWeightPolicyCapability {
        provider: resource("vllm"),
        endpoint_ref: id("endpoint:gb10-a"),
        base_checkpoint_digest: digest(1),
        adapter_digest: None,
        tokenizer_digest: digest(2),
        decode_params_digest: digest(3),
        artifact_destination_ref: id("artifacts:policy"),
        logprobs: LogprobSupport {
            chosen_token,
            top_k: 0,
            auxiliary_draws: false,
        },
        controls: PolicyControls {
            capture: ControlSetting {
                enabled: capture_enabled,
                scope: capture_enabled.then(|| resource("policy:capture-write")),
            },
            ..PolicyControls::default()
        },
        probe_digest: digest(4),
        probed_at_ms: 7,
    }
}

fn array(encoding: ArrayEncoding, elements: u32, width: u64) -> HeldBlobRef {
    HeldBlobRef {
        digest: digest(elements as u8),
        length: u64::from(elements) * width,
        encoding,
        elements,
    }
}

fn capture(completion: CaptureCompletion, evidence: Option<RewardEvidenceSource>) -> PolicyCapture {
    PolicyCapture {
        capability_id: id("polcap:x"),
        sampler_version_id: id("polver:x"),
        trajectory_id: id("trajectory:0001"),
        trajectory_steps: 3,
        completion,
        token_count: 10,
        policy_token_count: 4,
        token_ids: array(ArrayEncoding::U32Le, 10, 4),
        log_q: array(ArrayEncoding::F32Le, 4, 4),
        action_mask: array(ArrayEncoding::U8Mask, 10, 1),
        sampler_top_k: None,
        auxiliary_draws: None,
        reward: evidence.map(|evidence| RewardRecord {
            value_micros: 1_000_000,
            verifier_id: resource("unit-tests"),
            verifier_digest: digest(9),
            evidence,
        }),
        purpose: resource("evaluation"),
        trace_fidelity: CaptureTraceFidelity::Full,
        captured_at_ms: 11,
    }
}

#[test]
fn record_identity_is_content_addressed_and_immutable() {
    let record = PolicyEvolutionRecord::Capability {
        record: capability(true, true),
    };
    let first = record.record_id("tenant-a").unwrap();
    assert_eq!(first, record.clone().record_id("tenant-a").unwrap());
    assert!(first.starts_with("polcap:"));
    assert_eq!(
        PolicyRecordKind::of_record_id(&first),
        Some(PolicyRecordKind::Capability)
    );
    // Any change to the body is a different record, never an overwrite.
    let mut changed = capability(true, true);
    changed.probed_at_ms += 1;
    let changed = PolicyEvolutionRecord::Capability { record: changed };
    assert_ne!(first, changed.record_id("tenant-a").unwrap());
    // The tenant is bound into the id.
    assert_ne!(first, record.record_id("tenant-b").unwrap());
}

#[test]
fn record_ids_parse_only_with_a_known_prefix_and_a_digest() {
    for (kind, prefix) in KIND_PREFIXES {
        assert_eq!(kind.prefix(), prefix);
        let id = format!("{prefix}:{}", digest(5).to_hex());
        assert_eq!(PolicyRecordKind::of_record_id(&id), Some(kind));
    }
    assert_eq!(PolicyRecordKind::of_record_id("polcap:nothex"), None);
    let foreign = format!("fleetobs:{}", digest(5).to_hex());
    assert_eq!(PolicyRecordKind::of_record_id(&foreign), None);
}

#[test]
fn capture_without_chosen_token_logprobs_fails_closed() {
    assert_eq!(
        capability(false, true).validate(),
        Err(PolicyRefusal::LogprobsUnsupported)
    );
    assert!(capability(false, false).validate().is_ok());
    let mut unscoped = capability(true, true);
    unscoped.controls.capture.scope = None;
    assert_eq!(
        unscoped.validate().unwrap_err().code(),
        "POLICY_INVALID_RECORD"
    );
}

#[test]
fn controls_default_off() {
    let controls = PolicyControls::default();
    assert!(!controls.capture.enabled && !controls.train.enabled && !controls.promote.enabled);
    let decoded: PolicyControls = serde_json::from_value(serde_json::json!({})).unwrap();
    assert_eq!(decoded, controls);
}

#[test]
fn capture_dimensions_must_agree() {
    assert!(capture(CaptureCompletion::Terminal, None)
        .validate()
        .is_ok());
    let mut short_log_q = capture(CaptureCompletion::Terminal, None);
    short_log_q.log_q = array(ArrayEncoding::F32Le, 3, 4);
    assert!(short_log_q.validate().is_err());
    let mut wrong_length = capture(CaptureCompletion::Terminal, None);
    wrong_length.token_ids.length += 1;
    assert!(wrong_length.validate().is_err());
    let mut swapped = capture(CaptureCompletion::Terminal, None);
    swapped.action_mask = array(ArrayEncoding::U32Le, 10, 4);
    assert!(swapped.validate().is_err());
    let mut too_many_policy_tokens = capture(CaptureCompletion::Terminal, None);
    too_many_policy_tokens.policy_token_count = 11;
    assert!(too_many_policy_tokens.validate().is_err());
    assert_eq!(
        capture(CaptureCompletion::Terminal, None)
            .held_blobs()
            .len(),
        3
    );
}

#[test]
fn only_terminal_verified_captures_are_trainer_eligible() {
    let cases = [
        (
            CaptureCompletion::Terminal,
            Some(RewardEvidenceSource::IndependentVerifier),
            CaptureEligibility::Eligible,
        ),
        (
            CaptureCompletion::Terminal,
            Some(RewardEvidenceSource::Rubric),
            CaptureEligibility::Eligible,
        ),
        (
            CaptureCompletion::Terminal,
            Some(RewardEvidenceSource::SelfReported),
            CaptureEligibility::UnverifiedReward,
        ),
        (
            CaptureCompletion::Terminal,
            None,
            CaptureEligibility::NoReward,
        ),
        (
            CaptureCompletion::Truncated,
            Some(RewardEvidenceSource::Rubric),
            CaptureEligibility::NotTerminal,
        ),
        (
            CaptureCompletion::Aborted,
            None,
            CaptureEligibility::NotTerminal,
        ),
    ];
    for (completion, evidence, expected) in cases {
        assert_eq!(capture(completion, evidence).eligibility(), expected);
    }
}

#[test]
fn ops_carry_their_authz_action_and_wire_tag() {
    let get = PolicyEvolutionOp::Get {
        request: PolicyRecordGetRequest {
            record_id: id("polver:x"),
        },
    };
    assert!(!get.is_mutation());
    assert_eq!(get.authz_action(), "policy:read");
    let put = PolicyEvolutionOp::PutCapability {
        request: capability(true, true),
    };
    assert!(put.is_mutation());
    assert_eq!(put.authz_action(), "admin:policy-capability");
    let encoded = rmp_serde::to_vec_named(&put).unwrap();
    let tag = rmp_serde::from_slice::<serde_json::Value>(&encoded).unwrap()["op"].clone();
    assert_eq!(tag, serde_json::json!(put.name()));
    let decoded: PolicyEvolutionOp = rmp_serde::from_slice(&encoded).unwrap();
    assert_eq!(decoded, put);
    assert_eq!(
        put.into_record().unwrap().kind(),
        PolicyRecordKind::Capability
    );
    assert!(get.into_record().is_err());
}

#[test]
fn a_trained_version_names_its_adapter_and_parent() {
    let mut version = ModelPolicyVersion {
        checkpoint_digest: digest(1),
        adapter_digest: None,
        tokenizer_digest: digest(2),
        artifact_ref: id("artifacts:v2"),
        parent_version_id: None,
        origin: VersionOrigin::Trained {
            training_run_id: id("poltrain:x"),
        },
    };
    assert!(version.validate().is_err());
    version.adapter_digest = Some(digest(8));
    version.parent_version_id = Some(id("polver:base"));
    assert!(version.validate().is_ok());
}

#[test]
fn refusals_render_their_stable_code_first() {
    let refusal = PolicyRefusal::RecordMissing("polver:x".into());
    assert_eq!(refusal.to_string(), "POLICY_RECORD_MISSING: polver:x");
    assert_eq!(
        PolicyRefusal::CaptureDisabled.to_string(),
        "POLICY_CAPTURE_DISABLED: "
    );
}
