//! Skill consumer-contract upgrade receipt (EG-DECISION-ENGINE-R167.1).

use serde::{Deserialize, Serialize};

use crate::contract::{Digest256, OpaqueId};

/// Records the old and new consumer contracts and any declared migration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkillUpgradeReceipt {
    pub previous_consumer_contract_digest: Digest256,
    pub new_consumer_contract_digest: Digest256,
    pub declared_migration_id: Option<OpaqueId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(migration: Option<&str>) -> SkillUpgradeReceipt {
        SkillUpgradeReceipt {
            previous_consumer_contract_digest: Digest256::from_bytes([1; 32]),
            new_consumer_contract_digest: Digest256::from_bytes([2; 32]),
            declared_migration_id: migration.map(|id| OpaqueId::new(id).unwrap()),
        }
    }

    // spec: EG-DECISION-ENGINE-R167.1
    #[test]
    fn receipt_round_trips_with_and_without_migration() {
        for original in [receipt(Some("migration:skill-v2")), receipt(None)] {
            let bytes = rmp_serde::to_vec_named(&original).unwrap();
            let decoded = rmp_serde::from_slice::<SkillUpgradeReceipt>(&bytes).unwrap();
            assert_eq!(decoded, original);
            assert_ne!(
                decoded.previous_consumer_contract_digest,
                decoded.new_consumer_contract_digest
            );
        }
    }
}
