//! Ordered topic-log records (MEDULLA-DURABLE-KERNEL-R850.1).
//!
//! One ordered key-value table keys entries by `(topic, seq)`, so an append is
//! one O(log n) insert and a read of k entries is one O(log n + k) range scan.
//! Append, read and their refusals are MEDULLA-DURABLE-KERNEL-R850.2 onwards.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicEntry {
    pub topic: String,
    pub seq: u64,
    pub sender: String,
    /// Reference to the stored body; the log itself never holds the body.
    pub body_ref: String,
    pub redaction_digest: String,
    pub appended_at: u64,
}

impl TopicEntry {
    /// The entry's key in the ordered table.
    pub fn key(&self) -> (&str, u64) {
        (&self.topic, self.seq)
    }
}

/// The next sequence number a reader of one topic asks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TopicCursor(pub u64);

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    // spec: MEDULLA-DURABLE-KERNEL-R850.1
    #[test]
    fn entry_and_cursor_round_trip_through_the_wire_codec() {
        let entry = TopicEntry {
            topic: "tenant-a/builds".into(),
            seq: 42,
            sender: "session:7f".into(),
            body_ref: "blob:sha256:aa".into(),
            redaction_digest: "sha256:bb".into(),
            appended_at: 1_761_000_000_000,
        };
        let packed = rmp_serde::to_vec_named(&entry).unwrap();
        assert_eq!(rmp_serde::from_slice::<TopicEntry>(&packed).unwrap(), entry);

        let cursor = TopicCursor(43);
        let packed = rmp_serde::to_vec(&cursor).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<TopicCursor>(&packed).unwrap(),
            cursor
        );
        assert_eq!(serde_json::to_string(&cursor).unwrap(), "43");

        let mut table = BTreeMap::new();
        for (topic, seq) in [("b", 1), ("a", 2), ("a", 1)] {
            let mut e = entry.clone();
            e.topic = topic.into();
            e.seq = seq;
            table.insert((e.topic.clone(), e.seq), e);
        }
        let order: Vec<_> = table.values().map(|e| e.key()).collect();
        assert_eq!(order, [("a", 1), ("a", 2), ("b", 1)]);
    }
}
