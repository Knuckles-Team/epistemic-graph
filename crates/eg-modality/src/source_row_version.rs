//! Row-version evidence locus for source records (MEDULLA-INGEST-WORKER-R901.1).
//!
//! Reuses the [`EvidenceAddress::RowVersion`] locus and adds the opaque table
//! reference and the evidence digest. Storage and supersession are later rows
//! (R901.2 onward).

use serde::{Deserialize, Serialize};

use crate::artifact::{EvidenceAddress, OpaqueRef, ProtocolError};

/// A versioned source row whose text is held as an evidence artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, try_from = "SourceRowVersionWire")]
pub struct SourceRowVersion {
    pub table_ref: OpaqueRef,
    pub row_ref: OpaqueRef,
    pub version: u64,
    /// Lowercase hex SHA-256 of the stored evidence bytes.
    pub evidence_digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceRowVersionWire {
    table_ref: OpaqueRef,
    row_ref: OpaqueRef,
    version: u64,
    evidence_digest: String,
}

impl TryFrom<SourceRowVersionWire> for SourceRowVersion {
    type Error = ProtocolError;

    fn try_from(wire: SourceRowVersionWire) -> Result<Self, Self::Error> {
        Self::new(
            wire.table_ref,
            wire.row_ref,
            wire.version,
            wire.evidence_digest,
        )
    }
}

impl SourceRowVersion {
    pub fn new(
        table_ref: OpaqueRef,
        row_ref: OpaqueRef,
        version: u64,
        evidence_digest: String,
    ) -> Result<Self, ProtocolError> {
        let digest_ok = evidence_digest.len() == 64
            && evidence_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'));
        if !digest_ok {
            return Err(ProtocolError::InvalidEvidenceAddress);
        }
        Ok(Self {
            table_ref,
            row_ref,
            version,
            evidence_digest,
        })
    }

    /// The shared evidence locus this row version occupies.
    pub fn locus(&self) -> EvidenceAddress {
        EvidenceAddress::RowVersion {
            row_ref: self.row_ref.clone(),
            version: self.version,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn r(namespace: &str, suffix: u8) -> OpaqueRef {
        OpaqueRef::scoped(namespace, &format!("00000000000000{suffix:02x}")).unwrap()
    }

    // spec: MEDULLA-INGEST-WORKER-R901.1
    #[test]
    fn row_version_round_trips_and_rejects_empty_table() {
        let row = SourceRowVersion::new(r("table", 1), r("row", 2), 7, "ab".repeat(32)).unwrap();
        assert_eq!(
            row.locus(),
            EvidenceAddress::RowVersion {
                row_ref: r("row", 2),
                version: 7
            }
        );
        let encoded = serde_json::to_value(&row).unwrap();
        assert_eq!(
            serde_json::from_value::<SourceRowVersion>(encoded.clone()).unwrap(),
            row
        );

        let mut empty_table = encoded.clone();
        empty_table["table_ref"] = json!("");
        assert!(serde_json::from_value::<SourceRowVersion>(empty_table).is_err());
        let mut missing_table = encoded.clone();
        missing_table.as_object_mut().unwrap().remove("table_ref");
        assert!(serde_json::from_value::<SourceRowVersion>(missing_table).is_err());
        let mut raw_path = encoded.clone();
        raw_path["table_ref"] = json!("/var/db/customers");
        assert!(serde_json::from_value::<SourceRowVersion>(raw_path).is_err());
        let mut bad_digest = encoded;
        bad_digest["evidence_digest"] = json!("not-a-digest");
        assert!(serde_json::from_value::<SourceRowVersion>(bad_digest).is_err());
    }
}
