//! Typed A2A peer record (MEDULLA-DURABLE-KERNEL-R851.1).
//!
//! The engine keys peers by `name`. A peer carries only a reference to its
//! credential, never the credential value. Register, delete, list and their
//! refusals are MEDULLA-DURABLE-KERNEL-R851.2 onwards.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillRef {
    pub skill_id: String,
    pub version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentPeer {
    pub name: String,
    pub endpoint: String,
    /// Digest of the peer's published agent card.
    pub card_digest: String,
    pub skills: Vec<SkillRef>,
    /// Reference into the secret store; never the secret itself.
    pub credential_ref: Option<String>,
    pub refreshed_at: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-DURABLE-KERNEL-R851.1
    #[test]
    fn peer_round_trips_through_the_wire_codec() {
        let peer = AgentPeer {
            name: "research-agent".into(),
            endpoint: "https://agents.example.invalid/research".into(),
            card_digest: "sha256:".to_string() + &"0f".repeat(32),
            skills: vec![
                SkillRef {
                    skill_id: "web-search".into(),
                    version: Some("2".into()),
                },
                SkillRef {
                    skill_id: "summarize".into(),
                    version: None,
                },
            ],
            credential_ref: Some("secret://a2a/research-agent".into()),
            refreshed_at: 1_761_000_000_000,
        };
        let packed = rmp_serde::to_vec_named(&peer).unwrap();
        assert_eq!(rmp_serde::from_slice::<AgentPeer>(&packed).unwrap(), peer);
        let json = serde_json::to_value(&peer).unwrap();
        assert_eq!(
            serde_json::from_value::<AgentPeer>(json.clone()).unwrap(),
            peer
        );

        let mut with_secret = json;
        with_secret["credential_value"] = "hunter2".into();
        assert!(
            serde_json::from_value::<AgentPeer>(with_secret).is_err(),
            "the record has no field for a credential value"
        );
    }
}
