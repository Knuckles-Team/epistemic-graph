use super::*;
use crate::redb_store::work_item::test_shard::{open, with_nodes, GRAPH};
use eg_types::host_start::HostStartInterfaces;

fn request() -> IssueHostStartGrantRequest {
    let mut request = eg_types::test_support::host_start::host_start_request("tenant-a");
    request.binding.interfaces = HostStartInterfaces::Terminal;
    request.binding.policy_revision = "revision-1".into();
    request
}

#[test]
fn native_writer_requires_issuer_and_stamps_immutable_top_level_provenance() {
    let temp = open("host-start-native");
    with_nodes(&temp.shard, "host-issuer", |nodes| {
        assert!(issue(GRAPH, &request(), None, nodes, DurableCrypto::none(), 1_000).is_err());
        assert!(load_row(nodes, GRAPH, "host-grant", DurableCrypto::none())?.is_none());
        let result = issue(
            GRAPH,
            &request(),
            Some("native-actor"),
            nodes,
            DurableCrypto::none(),
            1_000,
        )?;
        let view = result.lease.unwrap();
        assert_eq!(
            view.host_start_provenance.unwrap().issuer_ref,
            "native-actor"
        );
        assert_eq!(view.expires_at_ms, 5_000);
        let collision = issue(
            GRAPH,
            &request(),
            Some("other-actor"),
            nodes,
            DurableCrypto::none(),
            1_001,
        )?;
        assert_eq!(collision.outcome, ControlLeaseIssueOutcome::Collision);
        let row = load_row(nodes, GRAPH, "host-grant", DurableCrypto::none())?.unwrap();
        assert_eq!(row["host_start_provenance"]["issuer_ref"], "native-actor");
        Ok(())
    });
    assert!(super::super::read_control_lease(
        &temp.shard,
        GRAPH,
        "other-tenant",
        "host-grant",
        DurableCrypto::none()
    )
    .unwrap()
    .is_none());
}

#[test]
fn generic_transition_can_revoke_but_cannot_consume_host_grants() {
    use eg_types::control_lease::{
        ControlLeaseStatus, ControlLeaseTarget, TransitionControlLeaseRequest,
    };
    let temp = open("host-start-transitions");
    with_nodes(&temp.shard, "host-transitions", |nodes| {
        issue(
            GRAPH,
            &request(),
            Some("native-actor"),
            nodes,
            DurableCrypto::none(),
            1_000,
        )?;
        let mut transition = TransitionControlLeaseRequest {
            tenant: "tenant-a".into(),
            lease_id: "host-grant".into(),
            expected_revision: 1,
            to: ControlLeaseTarget::Consumed,
            idempotency_key: "transition-host".into(),
            pending_call_consumption: None,
        };
        assert!(super::super::apply_transition_control_lease_row(
            GRAPH,
            &transition,
            nodes,
            1_001,
            DurableCrypto::none()
        )
        .is_err());
        transition.to = ControlLeaseTarget::Revoked;
        super::super::apply_transition_control_lease_row(
            GRAPH,
            &transition,
            nodes,
            1_002,
            DurableCrypto::none(),
        )?;
        let row = load_row(nodes, GRAPH, "host-grant", DurableCrypto::none())?.unwrap();
        let view = ControlLeaseView::from_row("host-grant", &row)?;
        assert_eq!(view.status, ControlLeaseStatus::Revoked);
        assert_eq!(
            view.host_start_provenance.unwrap().issuer_ref,
            "native-actor"
        );
        Ok(())
    });
}
