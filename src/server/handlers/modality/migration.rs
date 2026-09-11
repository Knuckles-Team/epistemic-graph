use std::collections::BTreeMap;

use eg_modality::{
    export_all, verify_round_trip, ExportedRows, GovernedModality, OccurrenceId, OpaqueRef,
    ServedModalityRuntime, ServedRecord,
};
use eg_types::ServedModalityKind;
use serde::{de::DeserializeOwned, Serialize};

use super::{delta_row_node_id, load_runtime, DeltaManifest, ModalityAuthority};
use crate::graph::GraphCore;

/// BUG-017 one-time snapshot-to-rows migration backfill. The plan keeps the
/// legacy snapshot and its exported rows together while the durable row writes
/// and read-back proof run as one cohesive migration phase.
#[allow(dead_code)]
pub(super) fn migrate_partition_to_rows<T>(
    core: &GraphCore,
    authority: &ModalityAuthority,
    modality: ServedModalityKind,
) -> Result<(), String>
where
    T: GovernedModality + Clone + PartialEq + std::fmt::Debug + Serialize + DeserializeOwned,
{
    let plan: MigrationPlan<T> = MigrationPlan::load(core, authority, modality)?;
    plan.write_rows(core, authority)?;
    let rows = plan.read_rows(core, authority)?;
    verify_round_trip(&plan.original, rows)
        .map(|_| ())
        .map_err(|error| error.to_string())
}

struct MigrationPlan<T> {
    original: ServedModalityRuntime<T>,
    exported: ExportedRows<T>,
    partition: String,
}

impl<T> MigrationPlan<T>
where
    T: GovernedModality + Clone + PartialEq + std::fmt::Debug + Serialize + DeserializeOwned,
{
    fn load(
        core: &GraphCore,
        authority: &ModalityAuthority,
        modality: ServedModalityKind,
    ) -> Result<Self, String> {
        let original = load_runtime(core, authority, modality)?;
        Ok(Self {
            exported: export_all(&original),
            partition: authority.node_id(modality),
            original,
        })
    }

    fn write_rows(&self, core: &GraphCore, authority: &ModalityAuthority) -> Result<(), String> {
        for (occurrence_id, record) in &self.exported.records {
            self.write_row(
                core,
                authority,
                "record",
                occurrence_id.as_ref().as_str(),
                record,
            )?;
        }
        for event in &self.exported.events {
            self.write_row(core, authority, "event", &event.sequence.to_string(), event)?;
        }
        for (key, entry) in &self.exported.idempotency {
            self.write_row(core, authority, "idem", key.as_str(), entry)?;
        }
        self.write_row(
            core,
            authority,
            "manifest",
            "",
            &DeltaManifest {
                next_sequence: self.exported.next_sequence,
            },
        )
    }

    fn write_row<S: Serialize>(
        &self,
        core: &GraphCore,
        authority: &ModalityAuthority,
        kind: &str,
        key: &str,
        value: &S,
    ) -> Result<(), String> {
        let bytes =
            serde_json::to_vec(value).map_err(|_| "modality row codec failure".to_string())?;
        core.add_node(
            delta_row_node_id(&self.partition, kind, key),
            authority.cipher.seal(&bytes),
        );
        Ok(())
    }

    fn read_rows(
        &self,
        core: &GraphCore,
        authority: &ModalityAuthority,
    ) -> Result<ExportedRows<T>, String> {
        let records = self
            .exported
            .records
            .keys()
            .map(|occurrence_id| {
                let bytes = self.read_row(
                    core,
                    authority,
                    "record",
                    occurrence_id.as_ref().as_str(),
                    "missing record row after migration write",
                )?;
                let record = serde_json::from_slice(&bytes)
                    .map_err(|_| "corrupt migrated record row".to_string())?;
                Ok((occurrence_id.clone(), record))
            })
            .collect::<Result<BTreeMap<OccurrenceId, ServedRecord<T>>, String>>()?;
        let events = self
            .exported
            .events
            .iter()
            .map(|event| {
                let bytes = self.read_row(
                    core,
                    authority,
                    "event",
                    &event.sequence.to_string(),
                    "missing event row after migration write",
                )?;
                serde_json::from_slice(&bytes).map_err(|_| "corrupt migrated event row".to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let idempotency = self
            .exported
            .idempotency
            .keys()
            .map(|key| {
                let bytes = self.read_row(
                    core,
                    authority,
                    "idem",
                    key.as_str(),
                    "missing idempotency row after migration write",
                )?;
                let entry = serde_json::from_slice(&bytes)
                    .map_err(|_| "corrupt migrated idempotency row".to_string())?;
                Ok((key.clone(), entry))
            })
            .collect::<Result<BTreeMap<OpaqueRef, _>, String>>()?;
        let bytes = self.read_row(
            core,
            authority,
            "manifest",
            "",
            "missing manifest row after migration write",
        )?;
        let manifest: DeltaManifest = serde_json::from_slice(&bytes)
            .map_err(|_| "corrupt migrated manifest row".to_string())?;
        Ok(ExportedRows {
            records,
            events,
            next_sequence: manifest.next_sequence,
            idempotency,
        })
    }

    fn read_row(
        &self,
        core: &GraphCore,
        authority: &ModalityAuthority,
        kind: &str,
        key: &str,
        missing: &str,
    ) -> Result<Vec<u8>, String> {
        let node_id = delta_row_node_id(&self.partition, kind, key);
        let sealed = core
            .get_node_properties(&node_id)
            .ok_or_else(|| missing.to_string())?;
        authority.cipher.unseal(&sealed)
    }
}
