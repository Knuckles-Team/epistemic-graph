//! Focused typed-pack contract fixture for inherited shell-centric task facts.

use std::collections::BTreeMap;

use eg_types::agent_ontology::{shell_centric_for_task, TASK_ROOT};

#[test]
fn shell_centric_inherits_or_is_absent() {
    // This map represents the values read from the composed
    // `swarm:shellCentric` property.  The reader itself owns inheritance;
    // callers do not apply a false default when the property is absent.
    let mut facts = BTreeMap::from([(TASK_ROOT.to_string(), false)]);
    let read = |iri: &str| facts.get(iri).copied();
    assert_eq!(
        shell_centric_for_task("eg:task/research", read),
        Some(false)
    );

    facts.insert("eg:task/research".to_string(), true);
    let read = |iri: &str| facts.get(iri).copied();
    assert_eq!(shell_centric_for_task("eg:task/research", read), Some(true));

    let unstated = BTreeMap::<String, bool>::new();
    let read = |iri: &str| unstated.get(iri).copied();
    assert_eq!(shell_centric_for_task("eg:task/research", read), None);
}
