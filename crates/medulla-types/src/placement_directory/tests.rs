use super::*;
use crate::epistemic_operations::PlacementRouteSchemaVersion;
use crate::result_contract::cluster::{
    ClusterGroup, ClusterLeader, ClusterMemberCertificate, DiscoveryAuthBinding,
};

fn digest(prefix: &str, byte: char) -> String {
    format!("{prefix}{}", byte.to_string().repeat(64))
}

fn member(node_id: u64, role: &str) -> ClusterMember {
    ClusterMember {
        node_id,
        member_identity: format!("node-{node_id}"),
        role: role.to_string(),
        client_endpoint: format!("https://node-{node_id}.cluster:7400"),
        tls_name: Some(format!("node-{node_id}.cluster")),
        health: "healthy".to_string(),
        certificate: ClusterMemberCertificate {
            id: Some("cert-1".to_string()),
            rotation_epoch: 1,
            not_before_ms: None,
            not_after_ms: None,
        },
    }
}

fn snapshot() -> ClusterDiscoverySnapshot {
    ClusterDiscoverySnapshot {
        schema_version: 1,
        cluster_id: digest("sha256:", 'a'),
        membership_epoch: 3,
        placement_epoch: 7,
        leader: Some(ClusterLeader {
            group_id: 1,
            node_id: 1,
        }),
        leaders: vec![ClusterLeader {
            group_id: 1,
            node_id: 1,
        }],
        groups: vec![ClusterGroup {
            group_id: 1,
            leader_id: Some(1),
            members: vec![member(1, "leader"), member(2, "follower")],
        }],
        auth_binding: DiscoveryAuthBinding {
            tenant_digest: digest("sha256:", 'b'),
            principal_digest: digest("sha256:", 'c'),
            agent_digest: digest("sha256:", 'd'),
        },
        signature: digest("hmac-sha256:", 'e'),
    }
}

fn route() -> PlacementRoute {
    PlacementRoute {
        schema_version: PlacementRouteSchemaVersion::V1,
        route_id: "request:1".to_string(),
        tenant_ref: "tenant-a".to_string(),
        partition_ref: "graph-a".to_string(),
        authoritative: true,
        placed: true,
        group: 1,
        epoch: 7,
        fencing_token: 1,
        stale: false,
        leader_ref: Some("node-1".to_string()),
    }
}

// spec: MEDULLA-PLACEMENT-R611.1
#[test]
fn cluster_snapshot_round_trips_and_rejects_invalid_identifiers() {
    let valid = ClusterSnapshot::new(snapshot()).expect("valid snapshot");
    let json = serde_json::to_string(&valid).unwrap();
    let back: ClusterSnapshot = serde_json::from_str(&json).unwrap();
    assert_eq!(back, valid);
    assert_eq!((back.membership_epoch(), back.placement_epoch()), (3, 7));

    let mut bad = snapshot();
    bad.cluster_id = "cluster-1".to_string();
    assert!(matches!(
        ClusterSnapshot::new(bad.clone()),
        Err(PlacementModelError::InvalidIdentifier {
            field: "cluster_id",
            ..
        })
    ));
    let json = serde_json::to_string(&bad).unwrap();
    assert!(serde_json::from_str::<ClusterSnapshot>(&json).is_err());

    let mut bad = snapshot();
    bad.groups[0].members[1].member_identity = " node-2".to_string();
    assert!(ClusterSnapshot::new(bad).is_err());
    let mut bad = snapshot();
    bad.groups[0].members[1].node_id = 1;
    assert!(ClusterSnapshot::new(bad).is_err());
    let mut bad = snapshot();
    bad.groups[0].leader_id = Some(9);
    assert!(ClusterSnapshot::new(bad).is_err());
}

// spec: MEDULLA-PLACEMENT-R611.1
#[test]
fn cluster_member_round_trips_and_rejects_invalid_identifiers() {
    let valid = member(1, "leader");
    valid.validate().unwrap();
    let back: ClusterMember =
        serde_json::from_str(&serde_json::to_string(&valid).unwrap()).unwrap();
    assert_eq!(back, valid);
    for mutate in [
        (|m: &mut ClusterMember| m.member_identity.clear()) as fn(&mut ClusterMember),
        |m| m.client_endpoint = "https://a b".to_string(),
        |m| m.role = "boss".to_string(),
        |m| m.health = "fine".to_string(),
        |m| m.tls_name = Some("bad\nname".to_string()),
    ] {
        let mut bad = member(1, "leader");
        mutate(&mut bad);
        assert!(bad.validate().is_err(), "{bad:?}");
    }
}

// spec: MEDULLA-PLACEMENT-R611.1
#[test]
fn placement_route_round_trips_and_rejects_invalid_identifiers() {
    let valid = route();
    valid.validate().unwrap();
    let back: PlacementRoute =
        serde_json::from_str(&serde_json::to_string(&valid).unwrap()).unwrap();
    assert_eq!(back, valid);
    let mut bad = route();
    bad.tenant_ref = String::new();
    assert!(bad.validate().is_err());
    let mut bad = route();
    bad.partition_ref = "x".repeat(MAX_IDENTIFIER_BYTES + 1);
    assert!(bad.validate().is_err());
    let mut bad = route();
    bad.epoch = 0;
    assert_eq!(
        bad.validate(),
        Err(PlacementModelError::Invalid("a placed route has epoch 0"))
    );

    let wire = PlacementRouteWire {
        schema_version: PlacementRouteSchemaVersion::V1,
        route_id: "request:1".to_string(),
        tenant_ref: "tenant-a".to_string(),
        partition_ref: "graph-a".to_string(),
        authoritative: true,
        placed: false,
        group: 0,
        epoch: 0,
        fencing_token: 0,
        stale: false,
        leader_ref: None,
        endpoints: vec!["https://node-1:7400".to_string()],
    };
    wire.validate().unwrap();
    let mut bad = wire;
    bad.endpoints.push(String::new());
    assert!(bad.validate().is_err());
}

// spec: MEDULLA-PLACEMENT-R611.1
#[test]
fn breaker_policy_round_trips_and_breaker_opens_then_half_opens() {
    let policy = BreakerPolicy::new(2, 1_000).unwrap();
    let back: BreakerPolicy =
        serde_json::from_str(&serde_json::to_string(&policy).unwrap()).unwrap();
    assert_eq!(back, policy);
    assert!(BreakerPolicy::new(0, 1).is_err());
    assert!(
        serde_json::from_str::<BreakerPolicy>(r#"{"failure_threshold":1,"cooldown_ms":0}"#)
            .is_err()
    );
    let state: BreakerState = serde_json::from_str(r#""half_open""#).unwrap();
    assert_eq!(state, BreakerState::HalfOpen);

    let breaker = Breaker::new(policy);
    let t0 = Instant::now();
    assert!(breaker.allow(t0));
    breaker.on_failure(t0);
    assert_eq!(breaker.state_at(t0), BreakerState::Closed);
    breaker.on_failure(t0);
    assert_eq!(breaker.state_at(t0), BreakerState::Open);
    assert!(!breaker.allow(t0));
    let t1 = t0 + Duration::from_millis(1_001);
    assert_eq!(breaker.state_at(t1), BreakerState::HalfOpen);
    assert!(breaker.allow(t1));
    assert!(!breaker.allow(t1), "one trial only");
    breaker.on_failure(t1);
    assert_eq!(
        breaker.state_at(t1),
        BreakerState::Open,
        "failed trial reopens"
    );
    let t2 = t1 + Duration::from_millis(1_001);
    assert!(breaker.allow(t2));
    breaker.on_success();
    assert_eq!(breaker.state_at(t2), BreakerState::Closed);
}

// spec: MEDULLA-PLACEMENT-R611.1
#[test]
fn tenant_pool_is_a_bounded_lru_and_rejects_invalid_tenants() {
    assert!(TenantPool::<u32>::new(0).is_err());
    let mut pool = TenantPool::new(2).unwrap();
    assert_eq!(pool.get("a").unwrap(), None);
    assert_eq!(pool.insert("a", 1).unwrap(), None);
    assert_eq!(pool.insert("b", 2).unwrap(), None);
    assert_eq!(pool.get("a").unwrap(), Some(&1), "hit reuses the client");
    assert_eq!(pool.insert("c", 3).unwrap(), Some(("b".to_string(), 2)));
    assert_eq!(
        pool.tenants(),
        vec!["a", "c"],
        "least recently used b evicted"
    );
    assert_eq!((pool.len(), pool.evictions()), (2, 1));
    assert_eq!(pool.insert("a", 9).unwrap(), Some(("a".to_string(), 1)));
    assert_eq!(pool.tenants(), vec!["c", "a"]);
    assert!(pool.insert("", 4).is_err());
    assert!(pool.get("bad\u{0}").is_err());
    assert_eq!(pool.len(), 2, "a refused tenant changes nothing");
}
