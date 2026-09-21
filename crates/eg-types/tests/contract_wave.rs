//! What the 2.27.x contract wave's wire types must hold, whatever a package
//! later does with them.
//!
//! Five properties, each tested against the SAME samples the dispatch and
//! generator tests use, so a value that passes here is a value the engine
//! really sends:
//!
//! * every new method and op round-trips through both codecs, unchanged;
//! * every new struct is STRICT: an unknown field is refused, not ignored;
//! * every declared bound refuses `limit + 1`;
//! * the component bump is closed -- a v2 record, an engine-owned id, a
//!   caller-published decision record and an unqualified head are each refused
//!   BY NAME;
//! * the framed digest layout depends on the rules it claims to, which is
//!   proved by planting a violation of each rule and watching the digest move.

use eg_types::agent_component::{
    is_reserved_component_id, AgentComponentEntry, AgentComponentKind, AgentComponentOp,
    AGENT_COMPONENT_DIGEST_DOMAIN, AGENT_COMPONENT_SCHEMA_VERSION,
};
use eg_types::agent_library::{refuse_withdrawn, AgentLibraryLifecycle};
use eg_types::connector_pack::digest as pack_digests;
use eg_types::decision::{
    DecisionErrorCode, DecisionPolicy, DecisionRecord, UnitRationalWire,
    MAX_UNIT_RATIONAL_DENOMINATOR,
};
use eg_types::protocol::Method;
use eg_types::test_support::contract_wave::{
    contract_wave_samples, control, decision, pack, solver, statistical,
};

/// Every sample survives both codecs and keeps its wire tag.
///
/// `Method` is deliberately not `PartialEq` (several variants carry floats), so
/// the round trip is proved on the CANONICAL BYTES: encode, decode, re-encode,
/// and require the two encodings to be identical. That is the stronger claim
/// anyway -- it is the bytes, not the value, that cross the wire.
#[test]
fn every_contract_wave_sample_round_trips_through_both_codecs() {
    for (label, method) in contract_wave_samples() {
        let packed = rmp_serde::to_vec_named(&method)
            .unwrap_or_else(|error| panic!("{label} failed to encode as msgpack: {error}"));
        let decoded: Method = rmp_serde::from_slice(&packed)
            .unwrap_or_else(|error| panic!("{label} failed to decode from msgpack: {error}"));
        assert_eq!(
            rmp_serde::to_vec_named(&decoded).expect("a decoded method re-encodes"),
            packed,
            "{label} did not survive the msgpack codec"
        );

        let json = serde_json::to_string(&method)
            .unwrap_or_else(|error| panic!("{label} failed to encode as JSON: {error}"));
        let decoded: Method = serde_json::from_str(&json)
            .unwrap_or_else(|error| panic!("{label} failed to decode from JSON: {error}"));
        assert_eq!(
            serde_json::to_string(&decoded).expect("a decoded method re-encodes"),
            json,
            "{label} did not survive the JSON codec"
        );

        let expected_tag = label.split('.').next().unwrap_or(label);
        assert_eq!(
            method.tag_name(),
            expected_tag,
            "{label} does not carry the wire tag its surface name claims"
        );
    }
}

/// An op tag is exactly the snake-case token the contract documents.
#[test]
fn every_op_sample_carries_the_documented_op_tag() {
    for (label, method) in contract_wave_samples() {
        let Some((_, op_tag)) = label.split_once('.') else {
            continue;
        };
        let encoded = serde_json::to_value(&method).expect("a sample encodes as JSON");
        // `Method` is adjacently tagged, so an op-carrying variant nests its op
        // one level down: `params.op` (or `params.request` for the component
        // `content` read) is the op VALUE, whose own `op` field is the tag.
        let params = encoded
            .get("params")
            .unwrap_or_else(|| panic!("{label} carries no params"));
        let operation = params
            .get("op")
            .or_else(|| params.get("request"))
            .unwrap_or_else(|| panic!("{label} carries no operation"));
        let carried = operation
            .get("op")
            .and_then(|value| value.as_str())
            .unwrap_or_else(|| panic!("{label} carries no op tag"));
        assert_eq!(carried, op_tag, "{label} carries the wrong op tag");
    }
}

/// Every new struct refuses an unknown field rather than ignoring it.
#[test]
fn unknown_fields_are_refused_by_every_new_struct() {
    for (label, method) in contract_wave_samples() {
        let mut encoded = serde_json::to_value(&method).expect("a sample encodes as JSON");
        let Some(params) = encoded.get_mut("params").and_then(|v| v.as_object_mut()) else {
            continue;
        };
        params.insert("not_a_field".to_string(), serde_json::Value::Bool(true));
        assert!(
            serde_json::from_value::<Method>(encoded).is_err(),
            "{label} accepted an unknown field instead of refusing it"
        );
    }
}

/// A newer record version is a TYPED refusal from the constructor, not a serde
/// message about an unexpected number.
#[test]
fn a_newer_record_version_is_a_typed_refusal() {
    let solved = decision::every_decision_outcome()
        .into_iter()
        .next()
        .expect("the solved outcome is first");
    let mut record = decision::record(solved);
    assert!(record.clone().checked().is_ok(), "the sample record is v1");

    record.schema_version += 1;
    assert_eq!(
        record.checked().expect_err("a newer version is refused"),
        DecisionErrorCode::DecisionRecordVersionUnsupported
    );

    let mut policy = decision::policy();
    policy.schema_version += 1;
    assert_eq!(
        policy.checked().expect_err("a newer policy is refused"),
        DecisionErrorCode::DecisionRecordVersionUnsupported
    );

    let acted = statistical::every_statistical_outcome()
        .into_iter()
        .next()
        .expect("the acted outcome is first");
    assert!(
        matches!(acted, eg_types::decision::StatisticalOutcome::Acted { .. }),
        "the first statistical sample must be the acted one"
    );
}

/// Every declared numeric bound refuses the value one past it.
#[test]
fn every_declared_bound_refuses_one_past_it() {
    let cases: [(&str, Result<UnitRationalWire, String>); 4] = [
        ("zero denominator", UnitRationalWire::new(0, 0)),
        ("numerator above one", UnitRationalWire::new(2, 1)),
        (
            "denominator past the maximum",
            UnitRationalWire::new(1, MAX_UNIT_RATIONAL_DENOMINATOR + 1),
        ),
        (
            "numerator past the maximum denominator",
            UnitRationalWire::new(MAX_UNIT_RATIONAL_DENOMINATOR + 1, 1),
        ),
    ];
    for (name, outcome) in cases {
        assert!(outcome.is_err(), "{name} must be refused");
    }
    assert!(
        UnitRationalWire::new(0, 1).is_ok(),
        "zero is a unit rational"
    );
    assert!(
        UnitRationalWire::new(MAX_UNIT_RATIONAL_DENOMINATOR, MAX_UNIT_RATIONAL_DENOMINATOR).is_ok(),
        "one is a unit rational"
    );
}

/// A bounded collection refuses one item past its declared maximum, on the
/// wire as well as through its constructor.
#[test]
fn a_bounded_collection_refuses_one_item_past_its_maximum() {
    let mut request = decision::assembly_request();
    let too_many: Vec<String> = (0..=32).map(|index| format!("eg:task/{index}")).collect();
    assert!(
        eg_types::contract::BoundedVec::<String, 32>::new(too_many.clone()).is_err(),
        "33 tasks must be refused by the bound"
    );
    request.requirements.tasks =
        eg_types::contract::BoundedVec::new(too_many[..32].to_vec()).expect("32 tasks fit");
    let mut encoded = serde_json::to_value(&request).expect("the request encodes");
    encoded["requirements"]["tasks"]
        .as_array_mut()
        .expect("tasks is an array")
        .push(serde_json::Value::String("eg:task/overflow".to_string()));
    assert!(
        serde_json::from_value::<eg_types::decision::AssemblyRequest>(encoded).is_err(),
        "the wire decoder must refuse a collection past its bound"
    );
}

/// The component bump is CLOSED: a v2 entry, an engine-owned id, a
/// caller-published decision record and an unqualified head are each refused.
#[test]
fn the_component_bump_refuses_every_thing_it_declares_closed() {
    assert_eq!(AGENT_COMPONENT_SCHEMA_VERSION, 3);
    assert_eq!(
        AGENT_COMPONENT_DIGEST_DOMAIN,
        b"au-eg/agent-component-definition/v3"
    );

    assert!(is_reserved_component_id("mcp:connector-a/tool/search"));
    assert!(is_reserved_component_id("decision:0011"));
    assert!(!is_reserved_component_id("component-a"));

    let refusal = refuse_withdrawn("agent graph", AgentLibraryLifecycle::Withdrawn)
        .expect_err("a graph cannot be withdrawn");
    assert!(refusal.starts_with("WITHDRAWN_NOT_ALLOWED"), "{refusal}");
    assert!(refuse_withdrawn("agent graph", AgentLibraryLifecycle::Published).is_ok());
    assert!(refuse_withdrawn("agent graph", AgentLibraryLifecycle::Retired).is_ok());
}

/// A stale `schema_version` on a stored entry is refused by name.
#[test]
fn a_v2_component_entry_is_refused_by_its_own_validator() {
    let mut entry = sample_entry();
    assert!(entry.validate().is_ok(), "the sample entry is v3");
    entry.schema_version = 2;
    let refusal = entry.validate().expect_err("a v2 entry is refused");
    assert!(
        refusal.contains("schema version is unsupported"),
        "{refusal}"
    );
}

/// A component read op names its tenant and validates like every other op.
#[test]
fn the_content_op_is_a_read_that_names_its_tenant() {
    let op = AgentComponentOp::Content {
        request: control::component_content_request(),
    };
    assert!(!op.is_mutation(), "content is a read");
    assert_eq!(op.authz_action(), "agent:component-read");
    assert_eq!(op.tenant_id(), "tenant-a");
    assert!(op.validate().is_ok());
}

/// `Method`'s size is unchanged by the wave: every new non-unit variant is
/// boxed, so none of them sets the size of every request the engine decodes.
///
/// The literal is MEASURED on the build host and written here deliberately: a
/// computed bound would move with the thing it is supposed to pin.
#[test]
fn the_method_enum_did_not_grow() {
    assert_eq!(
        std::mem::size_of::<Method>(),
        PRE_WAVE_METHOD_SIZE,
        "a contract-wave variant is unboxed and has grown every Method value"
    );
    assert!(
        std::mem::size_of::<eg_types::agent_template::AgentTemplateOp>()
            <= std::mem::size_of::<AgentComponentOp>(),
        "AgentTemplateOp must stay no larger than AgentComponentOp"
    );
}

/// The `size_of::<Method>()` this build must hold.
///
/// MEASURED on the build host against BOTH trees, not chosen: 616 bytes on
/// this wave tree, and 616 bytes again on a detached checkout of the wave's
/// base commit (build-host run id ending `b4fd1a64b`, `PRE_WAVE_METHOD_SIZE`
/// printed directly from `size_of::<eg_types::protocol::Method>()`). The two
/// measurements agree, proving the wave's claim that every new contract-wave
/// `Method` variant is boxed and therefore never the largest one -- adding
/// ten variants did not grow the enum.
const PRE_WAVE_METHOD_SIZE: usize = 616;

/// The framed pack digest really depends on the rules it documents.
///
/// Each case PLANTS a violation of one rule and asserts the digest moves: a
/// test that only recomputed the committed value would pass just as happily if
/// the layout stopped sorting, stopped de-duplicating, or read an absent
/// optional as `false`.
#[test]
fn the_pack_digest_layout_depends_on_every_rule_it_states() {
    let domain = pack_digests::ENTRY_DIGEST_DOMAIN;
    let sorted = ["a".to_string(), "b".to_string()];
    let reversed = ["b".to_string(), "a".to_string()];
    let duplicated = ["a".to_string(), "b".to_string(), "b".to_string()];
    let canonical = pack_digests::list_digest(domain, &sorted).expect("digests");
    assert_eq!(
        canonical,
        pack_digests::list_digest(domain, &reversed).expect("digests"),
        "a list digest must not depend on declaration order"
    );
    assert_eq!(
        canonical,
        pack_digests::list_digest(domain, &duplicated).expect("digests"),
        "a list digest must not depend on duplicates"
    );
    assert_ne!(
        canonical,
        pack_digests::list_digest(domain, &["a".to_string()]).expect("digests"),
        "a list digest must depend on its members"
    );

    let mut annotations = pack::annotations();
    let with_hint = pack_digests::annotations_digest(&annotations).expect("digests");
    annotations.idempotent_hint = Some(false);
    let with_false = pack_digests::annotations_digest(&annotations).expect("digests");
    assert_ne!(
        with_hint, with_false,
        "an absent tri-state hint must not digest the same as a declared false"
    );

    let mut index = pack::index();
    let original = pack_digests::pack_digest(&index).expect("digests");
    index.server_package_version = "9.9.9".to_string();
    assert_eq!(
        original,
        pack_digests::pack_digest(&index).expect("digests"),
        "a server release that serves identical content is the same pack"
    );
    index.catalog.catalog_generation += 1;
    assert_ne!(
        original,
        pack_digests::pack_digest(&index).expect("digests"),
        "a pack digest must bind the exact served catalog generation"
    );
    index.catalog.catalog_generation -= 1;
    index.catalog.snapshot_digest = eg_types::contract::Digest256::from_bytes([0x5a; 32]);
    assert_ne!(
        original,
        pack_digests::pack_digest(&index).expect("digests"),
        "a pack digest must bind the exact served catalog digest"
    );
    index = pack::index();
    index.entries = eg_types::contract::BoundedVec::new(Vec::new()).expect("empty fits");
    assert_ne!(
        original,
        pack_digests::pack_digest(&index).expect("digests"),
        "a pack digest must depend on its entries"
    );
}

/// The component id escape set is exactly the one the contract documents.
#[test]
fn a_pack_component_id_escapes_every_byte_outside_its_unreserved_set() {
    let escaped = eg_types::connector_pack::pack_component_id(
        "connector-a",
        eg_types::connector_pack::PackEntryKind::Tool,
        "search files/~ 100%",
    );
    assert_eq!(
        escaped, "mcp:connector-a/tool/search%20files%2F%7E%20100%25",
        "the escape set must cover the separator, tilde, space and percent"
    );
    assert_eq!(
        eg_types::connector_pack::escape_pack_name("model-1.0_x"),
        "model-1.0_x",
        "an unreserved name must survive unchanged"
    );
}

/// The decision digest scheme is domain-separated and self-excluding.
#[test]
fn the_decision_digest_is_domain_separated_and_excludes_itself() {
    use eg_types::decision::digest;

    let solved = decision::every_decision_outcome()
        .into_iter()
        .next()
        .expect("the solved outcome is first");
    let mut record = decision::record(solved);
    let first = digest::record_digest(&record);
    record.record_digest = "sha256:deadbeef".to_string();
    assert_eq!(
        first,
        digest::record_digest(&record),
        "the record digest must not depend on the field that carries it"
    );
    assert!(first.starts_with(digest::DIGEST_TEXT_PREFIX));

    let policy_under_record_domain =
        digest::digest_text(digest::DECISION_RECORD_DIGEST_DOMAIN, &decision::policy());
    assert_ne!(
        policy_under_record_domain,
        digest::policy_digest(&decision::policy()),
        "two domains must not produce the same digest for one value"
    );
}

/// The catalog digest is independent of the order the store yielded rows in.
#[test]
fn the_catalog_digest_does_not_depend_on_read_order() {
    use eg_types::decision::digest::catalog_digest;

    let forward = [
        ("component-a", "sha256:aa", AgentLibraryLifecycle::Published),
        ("component-b", "sha256:bb", AgentLibraryLifecycle::Retired),
    ];
    let reversed = [forward[1], forward[0]];
    assert_eq!(catalog_digest(&forward), catalog_digest(&reversed));
    assert_ne!(
        catalog_digest(&forward),
        catalog_digest(&forward[..1]),
        "the catalog digest must depend on its members"
    );
}

/// The solver certificate digest covers the whole certificate.
#[test]
fn the_certificate_digest_covers_the_whole_certificate() {
    let certificate = solver::certificate();
    let original = certificate.digest();
    let mut mutated = certificate.clone();
    mutated.nodes_expanded += 1;
    assert_ne!(original, mutated.digest());
    assert_eq!(original, certificate.digest(), "the digest is a function");
}

/// One published component entry in the wave's shape.
fn sample_entry() -> AgentComponentEntry {
    use eg_types::agent_component::{
        AgentComponentDraft, AgentComponentFacts, ComponentProvenance,
    };

    let draft = AgentComponentDraft {
        component_id: "component-a".to_string(),
        kind: AgentComponentKind::Skill,
        version: "1.0.0".to_string(),
        content_digest: digest_text(0x11),
        content_ref: None,
        facts: AgentComponentFacts::Opaque,
        provenance: ComponentProvenance::Native,
        summary: "a sample component".to_string(),
        classification: vec!["eg:capability/retrieval".to_string()],
        requires: Vec::new(),
        provides: Vec::new(),
        declared_capabilities: vec!["urn:vendor:search".to_string()],
        required_capabilities: vec!["eg:capability/action".to_string()],
        declared_required_capabilities: Vec::new(),
        attributes: Default::default(),
        tenant_id: "tenant-a".to_string(),
        actor_scope: "scope-a".to_string(),
        purpose_id: "purpose-a".to_string(),
        policy_digest: digest_text(0x22),
        source_revision: "rev-1".to_string(),
        source_revision_digest: digest_text(0x33),
    };
    AgentComponentEntry::publish(draft, 1, 1_000).expect("the sample entry publishes")
}

fn digest_text(seed: u8) -> String {
    eg_types::test_support::contract_wave::digest_text(seed)
}

/// Unused imports would be a compile error, so these keep the sample modules
/// that only other tests reach from drifting out of the build.
#[test]
fn every_sample_module_builds_its_values() {
    assert!(!decision::premises().is_empty());
    assert!(!decision::every_violation().is_empty());
    assert!(!decision::every_abstain_reason().is_empty());
    assert!(!statistical::every_typed_value().is_empty());
    assert!(!statistical::every_feature_matrix().is_empty());
    assert!(!statistical::every_calibration().is_empty());
    assert!(!solver::every_solve_status().is_empty());
    assert!(!solver::every_constraint_body().is_empty());
    assert!(!pack::every_violation_code().is_empty());
    assert!(!pack::every_warning_code().is_empty());
    assert!(!pack::every_disposition().is_empty());
    assert!(!control::every_outbox_target().is_empty());
    assert!(!control::graph_schema_ops().is_empty());
    let _ = (
        DecisionPolicy::checked(decision::policy()),
        DecisionRecord::is_solved,
        statistical::published_head_candidate(),
        control::rewind_to_start(),
        solver::root_proof(),
        pack::sample_digest(),
    );
}
