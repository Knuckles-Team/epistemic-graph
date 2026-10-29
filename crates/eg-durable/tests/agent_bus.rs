use eg_durable::{BusParticipant, HubPeer};

// spec: EG-DURABLE-KERNEL-R095.1000
#[test]
fn participant_and_hub_peer_round_trip() {
    let participant = BusParticipant {
        session_ref: "session:01".into(),
        tenant: "tenant-a".into(),
        host_ref: "host:builder-3".into(),
        capabilities: ["code-review", "test-runner"]
            .into_iter()
            .map(String::from)
            .collect(),
        last_seen_ms: 1_761_000_000_000,
    };
    let packed = rmp_serde::to_vec_named(&participant).unwrap();
    assert_eq!(
        rmp_serde::from_slice::<BusParticipant>(&packed).unwrap(),
        participant
    );

    let peer = HubPeer {
        hub_ref: "hub:east".into(),
        url_ref: "config://hubs/east".into(),
        scope: ["tenant-a/builds".to_string()].into_iter().collect(),
    };
    let packed = rmp_serde::to_vec_named(&peer).unwrap();
    assert_eq!(rmp_serde::from_slice::<HubPeer>(&packed).unwrap(), peer);

    let mut json = serde_json::to_value(&participant).unwrap();
    json["unexpected"] = true.into();
    assert!(serde_json::from_value::<BusParticipant>(json).is_err());
}
