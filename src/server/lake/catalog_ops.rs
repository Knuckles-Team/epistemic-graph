//! Catalog reads, visibility projections, and mutation operations for [`LakeManager`].

use serde_json::{json, Value};

use crate::server::blob::store::{hex_digest, BlobManifest, ChunkStore, DEFAULT_CHUNK_SIZE};

use super::{
    CreateTableError, LakeManager, LakeOp, LakeVisibility, LoadTableAsOfError, MaterializeInput,
    RenameTableError, TableEntry,
};

pub(super) struct StagedArtifact {
    path: String,
    digest: String,
}

pub(super) struct PreparedMaterialization {
    pub(super) candidate: TableEntry,
    pub(super) location: String,
    pub(super) rel_path: String,
    pub(super) bytes_len: u64,
    pub(super) num_rows: u64,
    pub(super) snapshot_id: i64,
    pub(super) metadata_location: String,
    pub(super) artifacts: Vec<(String, Vec<u8>)>,
}

fn stage_path_bytes(
    store: &dyn ChunkStore,
    path: &str,
    bytes: &[u8],
) -> Result<StagedArtifact, String> {
    let mut chunks = Vec::new();
    let mut chunk_lens = Vec::new();
    for part in bytes.chunks(DEFAULT_CHUNK_SIZE) {
        let (digest, _was_new) = store.put_chunk(part)?;
        chunks.push(digest);
        chunk_lens.push(part.len() as u32);
    }
    let manifest = BlobManifest {
        schema_version: crate::server::blob::BLOB_MANIFEST_VERSION,
        owner_scope: crate::server::blob::ENGINE_BLOB_OWNER_SCOPE.to_string(),
        chunks,
        chunk_lens,
        len: bytes.len() as u64,
        chunk_size: 0,
    };
    let mbytes = rmp_serde::to_vec_named(&manifest).map_err(|error| error.to_string())?;
    let digest = hex_digest(&mbytes);
    store.put_manifest(&digest, &manifest)?;
    store.incref(&digest)?;
    Ok(StagedArtifact {
        path: path.to_string(),
        digest,
    })
}

pub(super) fn rollback_artifacts(
    store: &dyn ChunkStore,
    artifacts: &[StagedArtifact],
) -> Result<(), String> {
    let mut failures = Vec::new();
    for artifact in artifacts.iter().rev() {
        if let Err(error) = store.decref(&artifact.digest) {
            failures.push(format!("{} ({error})", artifact.path));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join(", "))
    }
}

pub(super) fn stage_artifacts(
    store: &dyn ChunkStore,
    artifacts: &[(String, Vec<u8>)],
) -> Result<Vec<StagedArtifact>, String> {
    let mut staged = Vec::with_capacity(artifacts.len());
    for (path, bytes) in artifacts {
        match stage_path_bytes(store, path, bytes) {
            Ok(artifact) => staged.push(artifact),
            Err(error) => {
                return match rollback_artifacts(store, &staged) {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(format!("{error}; rollback failed: {cleanup}")),
                };
            }
        }
    }
    Ok(staged)
}

pub(super) fn publish_artifacts(
    paths: &parking_lot::Mutex<std::collections::HashMap<String, String>>,
    artifacts: Vec<StagedArtifact>,
) {
    let mut paths = paths.lock();
    for artifact in artifacts {
        paths.insert(artifact.path, artifact.digest);
    }
}

pub(super) fn prepare_materialization(
    input: &MaterializeInput<'_>,
    existing: Option<&TableEntry>,
    lsn: eg_lake::snapshot::Lsn,
    ts_ms: i64,
) -> Result<PreparedMaterialization, String> {
    let mut candidate = existing.cloned().unwrap_or_else(|| TableEntry {
        table: eg_lake::LakeTable::new(
            input.namespace.to_string(),
            input.table.to_string(),
            input.schema.clone(),
            LakeManager::location_for(input.namespace, input.table),
        ),
        source_series: input.source_series.map(str::to_string),
        owner_tenant: input.owner_tenant.map(str::to_string),
        mutation_revision: 0,
    });
    let location = candidate.table.location.clone();
    for rel in input.retire_paths {
        candidate.table.snapshot.remove_file(rel, lsn);
    }
    let (rel_path, bytes) = candidate.table.materialize(input.batch, lsn)?;
    let bytes_len = bytes.len() as u64;
    let num_rows = input.batch.num_rows() as u64;
    let delta = candidate
        .table
        .delta_log(ts_ms)
        .into_iter()
        .last()
        .ok_or_else(|| "materialization produced no Delta commit".to_string())?;
    let manifests = candidate.table.iceberg_manifests()?;
    eg_lake::record_iceberg_commit(&mut candidate.table, ts_ms);
    candidate.mutation_revision = candidate.mutation_revision.wrapping_add(1);
    let iceberg = candidate.table.iceberg(ts_ms);
    let artifacts = vec![
        (format!("{location}/{rel_path}"), bytes),
        (
            format!("{location}/{}", delta.path),
            delta.content.into_bytes(),
        ),
        (manifests.manifest_path, manifests.manifest_avro),
        (manifests.manifest_list_path, manifests.manifest_list_avro),
        (
            iceberg.metadata_location.clone(),
            iceberg.metadata_json.into_bytes(),
        ),
    ];
    Ok(PreparedMaterialization {
        candidate,
        location,
        rel_path,
        bytes_len,
        num_rows,
        snapshot_id: manifests.snapshot_id,
        metadata_location: iceberg.metadata_location,
        artifacts,
    })
}

fn namespace_visible(manager: &LakeManager, namespace: &str, visibility: &LakeVisibility) -> bool {
    let tables = manager.tables.lock();
    tables.iter().any(|((candidate, _), entry)| {
        candidate == namespace && visibility.allows(entry.owner_tenant.as_deref())
    })
}

impl LakeManager {
    pub fn list_namespaces(&self) -> Value {
        self.catalog.lock().list_namespaces()
    }

    pub fn list_tables(&self, namespace: &str) -> Value {
        self.catalog.lock().list_tables(namespace)
    }

    pub fn load_table(&self, namespace: &str, table: &str) -> Option<Value> {
        self.catalog.lock().load_table(namespace, table)
    }

    /// Resolve an Iceberg table at one concrete, committed engine LSN.
    pub fn load_table_as_of(
        &self,
        namespace: &str,
        table: &str,
        lsn: u64,
        visibility: &LakeVisibility,
    ) -> Result<Option<Value>, LoadTableAsOfError> {
        let tables = self.tables.lock();
        let Some(entry) = tables.get(&(namespace.to_string(), table.to_string())) else {
            return Ok(None);
        };
        if !visibility.allows(entry.owner_tenant.as_deref()) {
            return Ok(None);
        }
        let current_lsn = entry.table.current_lsn().value();
        if lsn > i64::MAX as u64 || !self.is_committed_lsn(lsn) {
            return Err(LoadTableAsOfError::LsnUnavailable {
                requested: lsn,
                current_lsn,
            });
        }
        let requested = eg_lake::snapshot::Lsn(lsn);
        if !eg_lake::has_iceberg_snapshot_as_of(&entry.table, requested) {
            return Ok(None);
        }
        let ts_ms = super::lineage::now_ms();
        let iceberg = entry.table.iceberg_as_of(requested, ts_ms as i64);
        let metadata = serde_json::from_str(&iceberg.metadata_json).unwrap_or(Value::Null);
        Ok(Some(json!({
            "metadata-location": iceberg.metadata_location,
            "metadata": metadata,
            "config": {},
        })))
    }

    pub fn namespace_exists(&self, namespace: &str) -> bool {
        let catalog = self.catalog.lock();
        catalog.list_namespaces()["namespaces"]
            .as_array()
            .map(|levels| levels.iter().any(|name| name[0] == namespace))
            .unwrap_or(false)
    }

    /// Accept an Iceberg CommitTable request by running the engine-owned compaction
    /// path and returning the resulting LoadTable response.
    pub fn commit_table(
        &self,
        store: &dyn ChunkStore,
        namespace: &str,
        table: &str,
    ) -> Result<Value, String> {
        self.compact(store, namespace, table)?;
        self.load_table(namespace, table)
            .ok_or_else(|| format!("no such table: {namespace}.{table}"))
    }

    pub(crate) fn list_namespaces_visible(
        &self,
        visibility: &LakeVisibility,
        page_token: Option<&str>,
        page_size: Option<usize>,
    ) -> Value {
        let mut namespaces: Vec<String> = {
            let tables = self.tables.lock();
            tables
                .iter()
                .filter(|(_, entry)| visibility.allows(entry.owner_tenant.as_deref()))
                .map(|((namespace, _), _)| namespace.clone())
                .collect()
        };
        namespaces.sort();
        namespaces.dedup();
        let (page, next) = super::paginate(&namespaces, page_token, page_size);
        let identifiers: Vec<Value> = page
            .iter()
            .map(|namespace| json!(super::namespace_levels(namespace)))
            .collect();
        let mut response = json!({ "namespaces": identifiers });
        if let Some(token) = next {
            response["next-page-token"] = json!(token);
        }
        response
    }

    pub(crate) fn namespace_exists_visible(
        &self,
        namespace: &str,
        visibility: &LakeVisibility,
    ) -> bool {
        namespace_visible(self, namespace, visibility)
    }

    pub(crate) fn list_tables_visible(
        &self,
        namespace: &str,
        visibility: &LakeVisibility,
        page_token: Option<&str>,
        page_size: Option<usize>,
    ) -> Value {
        let mut names: Vec<String> = {
            let tables = self.tables.lock();
            tables
                .iter()
                .filter(|((candidate, _), entry)| {
                    candidate == namespace && visibility.allows(entry.owner_tenant.as_deref())
                })
                .map(|((_, name), _)| name.clone())
                .collect()
        };
        names.sort();
        let (page, next) = super::paginate(&names, page_token, page_size);
        let levels = super::namespace_levels(namespace);
        let identifiers: Vec<Value> = page
            .iter()
            .map(|name| json!({ "namespace": levels, "name": name }))
            .collect();
        let mut response = json!({ "identifiers": identifiers });
        if let Some(token) = next {
            response["next-page-token"] = json!(token);
        }
        response
    }

    pub(crate) fn load_table_visible(
        &self,
        namespace: &str,
        table: &str,
        visibility: &LakeVisibility,
    ) -> Option<Value> {
        {
            let tables = self.tables.lock();
            let entry = tables.get(&(namespace.to_string(), table.to_string()))?;
            if !visibility.allows(entry.owner_tenant.as_deref()) {
                return None;
            }
        }
        self.load_table(namespace, table)
    }

    /// Register an empty table through the same materialization pipeline used by
    /// append and rewrite operations.
    pub fn create_table(
        &self,
        store: &dyn ChunkStore,
        namespace: &str,
        table: &str,
        schema: eg_lake::schema::LakeSchema,
        owner_tenant: Option<&str>,
    ) -> Result<Value, CreateTableError> {
        {
            let tables = self.tables.lock();
            if tables.contains_key(&(namespace.to_string(), table.to_string())) {
                return Err(CreateTableError::AlreadyExists);
            }
        }
        let batch = eg_lake::schema::LakeBatch::new(schema.clone(), Vec::new())
            .map_err(CreateTableError::Other)?;
        self.materialize_batch(
            store,
            MaterializeInput {
                namespace,
                table,
                schema: &schema,
                batch: &batch,
                source_series: None,
                op_hint: LakeOp::Create,
                input_dataset: None,
                owner_tenant,
                retire_paths: &[],
                expected_lsn: None,
                expected_revision: None,
            },
        )
        .map_err(CreateTableError::Other)?;
        self.load_table(namespace, table).ok_or_else(|| {
            CreateTableError::Other(format!(
                "table {namespace}.{table} vanished immediately after create"
            ))
        })
    }

    /// Remove a visible table from the manager, catalog, and virtual path index.
    pub fn drop_table(&self, namespace: &str, table: &str, visibility: &LakeVisibility) -> bool {
        let key = (namespace.to_string(), table.to_string());
        let removed = {
            let mut tables = self.tables.lock();
            match tables.get(&key) {
                Some(entry) if visibility.allows(entry.owner_tenant.as_deref()) => {
                    tables.remove(&key);
                    true
                }
                _ => false,
            }
        };
        if removed {
            self.catalog.lock().remove(namespace, table);
            let prefix = format!("{}/", Self::location_for(namespace, table));
            self.paths
                .lock()
                .retain(|path, _| !path.starts_with(&prefix));
        }
        removed
    }

    /// Re-key a visible table in the manager and catalog without moving data files.
    pub fn rename_table(
        &self,
        from_ns: &str,
        from_table: &str,
        to_ns: &str,
        to_table: &str,
        visibility: &LakeVisibility,
    ) -> Result<(), RenameTableError> {
        let from_key = (from_ns.to_string(), from_table.to_string());
        let to_key = (to_ns.to_string(), to_table.to_string());
        let mut tables = self.tables.lock();
        match tables.get(&from_key) {
            Some(entry) if visibility.allows(entry.owner_tenant.as_deref()) => {}
            _ => return Err(RenameTableError::SourceNotFound),
        }
        if from_key != to_key && tables.contains_key(&to_key) {
            return Err(RenameTableError::DestinationExists);
        }
        let mut entry = tables.remove(&from_key).expect("checked above");
        entry.table.namespace = to_ns.to_string();
        entry.table.name = to_table.to_string();
        entry.mutation_revision = entry.mutation_revision.wrapping_add(1);
        let timestamp_ms = super::lineage::now_ms();
        {
            let mut catalog = self.catalog.lock();
            catalog.remove(from_ns, from_table);
            entry.table.register_in(&mut catalog, timestamp_ms as i64);
        }
        tables.insert(to_key, entry);
        Ok(())
    }
}
