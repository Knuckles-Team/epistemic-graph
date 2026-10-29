//! Optimistic concurrency footprints (MEDULLA-ENGINE-PARITY-R002.1, R004.1).
//!
//! A transaction reads from the snapshot at `snapshot_seq` and records the
//! table key ranges it read and wrote. At commit the footprint validates
//! against the writes committed after its snapshot: an overlap with either
//! set is a conflict. Footprints that do not overlap are `disjoint` and
//! validate without waiting on each other.

/// A half-open key range `[start, end)` within one table.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeyRange {
    pub table: String,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

impl KeyRange {
    /// The range holding exactly `key`.
    pub fn point(table: &str, key: &[u8]) -> Self {
        let mut end = key.to_vec();
        end.push(0);
        Self {
            table: table.to_string(),
            start: key.to_vec(),
            end,
        }
    }

    /// Whether the two ranges share at least one key.
    pub fn overlaps(&self, other: &KeyRange) -> bool {
        self.table == other.table && self.start < other.end && other.start < self.end
    }
}

/// The read and write sets of one transaction over its snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TxnFootprint {
    pub snapshot_seq: u64,
    pub reads: Vec<KeyRange>,
    pub writes: Vec<KeyRange>,
}

/// One committed transaction's write set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedWrites {
    pub commit_seq: u64,
    pub writes: Vec<KeyRange>,
}

/// `TXN_CONFLICT`: the keys of the footprint that changed after its snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnConflict {
    pub keys: Vec<KeyRange>,
}

impl TxnConflict {
    pub const CODE: &'static str = "TXN_CONFLICT";
}

impl std::fmt::Display for TxnConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} key range(s) changed",
            Self::CODE,
            self.keys.len()
        )
    }
}

impl std::error::Error for TxnConflict {}

fn touched(footprint: &TxnFootprint) -> impl Iterator<Item = &KeyRange> {
    footprint.reads.iter().chain(footprint.writes.iter())
}

/// Whether two footprints share no key: neither writes a range the other
/// reads or writes.
pub fn disjoint(a: &TxnFootprint, b: &TxnFootprint) -> bool {
    let clashes = |writer: &TxnFootprint, other: &TxnFootprint| {
        writer
            .writes
            .iter()
            .any(|w| touched(other).any(|k| k.overlaps(w)))
    };
    !clashes(a, b) && !clashes(b, a)
}

impl TxnFootprint {
    /// Validate against `committed`: refuse with every footprint key that a
    /// commit after `snapshot_seq` wrote.
    pub fn validate(&self, committed: &[CommittedWrites]) -> Result<(), TxnConflict> {
        let mut keys: Vec<KeyRange> = Vec::new();
        for commit in committed
            .iter()
            .filter(|c| c.commit_seq > self.snapshot_seq)
        {
            for key in touched(self) {
                if commit.writes.iter().any(|w| w.overlaps(key)) && !keys.contains(key) {
                    keys.push(key.clone());
                }
            }
        }
        if keys.is_empty() {
            Ok(())
        } else {
            keys.sort();
            Err(TxnConflict { keys })
        }
    }
}
