//! In-memory structures and atomic primitives (EG-DURABLE-KERNEL-R039):
//! hash/sorted-set/list/set working state for ephemeral and async
//! namespaces, with redb as the sole durability log. This is the typed
//! model slice (`.1`): every structure and primitive is typed, and a
//! declaration structurally always names redb as sole durability authority.
//! The real in-memory structures, the atomic operations, and recovery/
//! rebuild from the redb log are later children.

use serde::{Deserialize, Serialize};

/// An in-memory working-state data structure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataStructureKind {
    Hash,
    SortedSet,
    List,
    Set,
}

impl DataStructureKind {
    pub const ALL: [DataStructureKind; 4] = [
        DataStructureKind::Hash,
        DataStructureKind::SortedSet,
        DataStructureKind::List,
        DataStructureKind::Set,
    ];
}

/// A native atomic primitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AtomicPrimitiveKind {
    IncrbyWithExpiry,
    SetNxEx,
    CasLease,
    TokenBucket,
    BoundedCounter,
    WorkClaim,
}

impl AtomicPrimitiveKind {
    pub const ALL: [AtomicPrimitiveKind; 6] = [
        AtomicPrimitiveKind::IncrbyWithExpiry,
        AtomicPrimitiveKind::SetNxEx,
        AtomicPrimitiveKind::CasLease,
        AtomicPrimitiveKind::TokenBucket,
        AtomicPrimitiveKind::BoundedCounter,
        AtomicPrimitiveKind::WorkClaim,
    ];
}

/// The namespace class this requirement scopes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Namespace {
    Ephemeral,
    Async,
}

/// What a durability declaration is about: an in-memory structure or an
/// atomic primitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurableSubject {
    Structure(DataStructureKind),
    Primitive(AtomicPrimitiveKind),
}

/// One subject's durability declaration for a namespace. Only constructible
/// via [`DurabilityDeclaration::new`], which always sets
/// `redb_is_sole_authority` true -- the field is private, so nothing outside
/// this module can flip it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurabilityDeclaration {
    pub subject: DurableSubject,
    pub namespace: Namespace,
    redb_is_sole_authority: bool,
}

impl DurabilityDeclaration {
    pub fn new(subject: DurableSubject, namespace: Namespace) -> Self {
        Self {
            subject,
            namespace,
            redb_is_sole_authority: true,
        }
    }

    pub fn redb_is_sole_authority(&self) -> bool {
        self.redb_is_sole_authority
    }
}

/// Enumerate one durability declaration per structure and per primitive for
/// `namespace` -- the inventory a later recovery/rebuild implementation
/// would consult.
pub fn declare_all(namespace: Namespace) -> Vec<DurabilityDeclaration> {
    let mut out = Vec::with_capacity(DataStructureKind::ALL.len() + AtomicPrimitiveKind::ALL.len());
    for structure in DataStructureKind::ALL {
        out.push(DurabilityDeclaration::new(
            DurableSubject::Structure(structure),
            namespace,
        ));
    }
    for primitive in AtomicPrimitiveKind::ALL {
        out.push(DurabilityDeclaration::new(
            DurableSubject::Primitive(primitive),
            namespace,
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // spec: EG-DURABLE-KERNEL-R039.1
    #[test]
    fn kinds_round_trip_through_their_wire_name() {
        for (kind, name) in [
            (DataStructureKind::Hash, "hash"),
            (DataStructureKind::SortedSet, "sorted_set"),
            (DataStructureKind::List, "list"),
            (DataStructureKind::Set, "set"),
        ] {
            let wire = serde_json::to_string(&kind).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            assert_eq!(
                serde_json::from_str::<DataStructureKind>(&wire).unwrap(),
                kind
            );
        }
        for (kind, name) in [
            (AtomicPrimitiveKind::IncrbyWithExpiry, "incrby_with_expiry"),
            (AtomicPrimitiveKind::SetNxEx, "set_nx_ex"),
            (AtomicPrimitiveKind::CasLease, "cas_lease"),
            (AtomicPrimitiveKind::TokenBucket, "token_bucket"),
            (AtomicPrimitiveKind::BoundedCounter, "bounded_counter"),
            (AtomicPrimitiveKind::WorkClaim, "work_claim"),
        ] {
            let wire = serde_json::to_string(&kind).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            assert_eq!(
                serde_json::from_str::<AtomicPrimitiveKind>(&wire).unwrap(),
                kind
            );
        }
        for (ns, name) in [
            (Namespace::Ephemeral, "ephemeral"),
            (Namespace::Async, "async"),
        ] {
            let wire = serde_json::to_string(&ns).unwrap();
            assert_eq!(wire, format!("\"{name}\""));
            assert_eq!(serde_json::from_str::<Namespace>(&wire).unwrap(), ns);
        }
    }

    // spec: EG-DURABLE-KERNEL-R039.1
    #[test]
    fn declaration_always_reports_redb_as_sole_authority() {
        for namespace in [Namespace::Ephemeral, Namespace::Async] {
            for structure in DataStructureKind::ALL {
                let d = DurabilityDeclaration::new(DurableSubject::Structure(structure), namespace);
                assert!(d.redb_is_sole_authority());
            }
            for primitive in AtomicPrimitiveKind::ALL {
                let d = DurabilityDeclaration::new(DurableSubject::Primitive(primitive), namespace);
                assert!(d.redb_is_sole_authority());
            }
        }
    }

    // spec: EG-DURABLE-KERNEL-R039.1
    #[test]
    fn declare_all_covers_every_structure_and_primitive_exactly_once() {
        for namespace in [Namespace::Ephemeral, Namespace::Async] {
            let declared = declare_all(namespace);
            assert_eq!(declared.len(), 10);
            assert!(declared.iter().all(|d| d.redb_is_sole_authority()));
            let unique: HashSet<DurableSubject> = declared.iter().map(|d| d.subject).collect();
            assert_eq!(unique.len(), 10);
        }
    }
}
