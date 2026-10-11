//! Production owner-open acceptance for the ConnectorPack catalog.

use super::*;
use eg_types::contract::ResourceId;

// spec: EG-DECISION-ENGINE-R073.3
#[test]
fn fresh_open_initializes_all_pack_tables_and_reopens_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(AGENT_LIBRARY_FILE);
    assert!(!path.exists());
    for _ in 0..2 {
        let store = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap();
        let read = store.read().unwrap();
        assert_eq!(
            eg_storage::owner_table_names(eg_storage::OwnerLayout::AgentLibrary).len(),
            21
        );
        macro_rules! empty_table {
            ($table:ident) => {
                assert_eq!(
                    read.open_owner_table(eg_storage::$table)
                        .unwrap()
                        .len()
                        .unwrap(),
                    0,
                    stringify!($table)
                );
            };
        }
        empty_table!(CONNECTOR_PACK_HEADS);
        empty_table!(CONNECTOR_PACK_MEMBERS);
        empty_table!(CONNECTOR_PACK_IMPORTS);
        empty_table!(CONNECTOR_PACK_BODY_HOLDERS);
        empty_table!(CONNECTOR_PACK_BINDINGS);
        let status = store
            .connector_pack_status("tenant-a", &ResourceId::new("fresh-pack").unwrap())
            .unwrap();
        assert!(status.head.is_none());
        assert!(status.importer.is_none());
        assert_eq!(
            status.members,
            eg_types::connector_pack::PackMemberCounts::default()
        );
        assert!(path.exists());
    }
}

// spec: EG-DECISION-ENGINE-R073.3
#[test]
fn production_open_refuses_each_genuine_predecessor_without_writing() {
    let predecessors = eg_storage::layout_predecessors(eg_storage::OwnerLayout::AgentLibrary);
    assert!(!predecessors.is_empty());
    for predecessor in predecessors {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(AGENT_LIBRARY_FILE);
        eg_storage::create_predecessor_owner_file(
            &path,
            PhysicalStoreIdentity::new(AGENT_LIBRARY_PHYSICAL_STORE).unwrap(),
            predecessor,
        )
        .unwrap();
        let before = std::fs::read(&path).unwrap();
        for _ in 0..2 {
            let error = AgentLibraryStore::open(dir.path().to_str().unwrap()).unwrap_err();
            assert!(error.starts_with(&predecessor.error_code()), "{error}");
            assert!(error.contains(predecessor.label), "{error}");
            assert!(error.ends_with(&predecessor.operator_step()), "{error}");
            assert_eq!(
                std::fs::read(&path).unwrap(),
                before,
                "refusal wrote the owner file"
            );
        }
    }
}
