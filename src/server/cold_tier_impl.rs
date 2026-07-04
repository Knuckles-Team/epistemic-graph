//! Facade-side [`ColdTier`](crate::cold_tier::ColdTier) implementations
//! (CONCEPT:EG-KG.coordination.distributed-cache-coherence): the durable redb-backed default and the object-store (S3/
//! MinIO) variant behind the SEPARATE `cold-tier-s3` feature.
//!
//! The seam + the dep-free `InMemoryColdTier` live DAG-low in eg-core
//! (`crate::cold_tier`). These impls layer durability on top — exactly the same
//! split as the blob `ChunkStore` (native redb default; S3 behind a feature), so the
//! lean `cold-tier` build links NO object-store SDK and the Pi contract holds.
//!
//! A cold graph's whole serialized `to_msgpack` blob is stored as ONE keyed value:
//!   * redb: a row in a `cold_graphs` table in `{persist_dir}/cold.redb`.
//!   * S3:   one object at `{prefix}/{sanitized_graph}` in the configured bucket.

use eg_core::cold_tier::ColdTier;
use redb::{Database, ReadableDatabase, TableDefinition};
use std::sync::Arc;

/// `graph_name → serialized graph blob`. One table; one row per offloaded graph.
const COLD_GRAPHS: TableDefinition<&str, &[u8]> = TableDefinition::new("cold_graphs");

/// redb-backed durable cold tier. Survives a restart — an offloaded graph stays
/// offloaded across process lifetimes until rehydrated. Self-contained in
/// `{persist_dir}/cold.redb` (separate file, like the blob CAS).
#[derive(Debug)]
pub struct RedbColdTier {
    db: Arc<Database>,
}

impl RedbColdTier {
    /// Open (or create) `{persist_dir}/cold.redb` and ensure the table exists.
    pub fn open(persist_dir: &str) -> Result<Self, String> {
        std::fs::create_dir_all(persist_dir).map_err(|e| e.to_string())?;
        let path = std::path::Path::new(persist_dir).join("cold.redb");
        let db = Database::create(&path).map_err(|e| e.to_string())?;
        let wtx = db.begin_write().map_err(|e| e.to_string())?;
        wtx.open_table(COLD_GRAPHS).map_err(|e| e.to_string())?;
        wtx.commit().map_err(|e| e.to_string())?;
        Ok(Self { db: Arc::new(db) })
    }
}

impl ColdTier for RedbColdTier {
    fn offload(&self, graph_name: &str, bytes: &[u8]) -> Result<(), String> {
        let mut wtx = self.db.begin_write().map_err(|e| e.to_string())?;
        wtx.set_durability(redb::Durability::Immediate)
            .map_err(|e| e.to_string())?;
        {
            let mut t = wtx.open_table(COLD_GRAPHS).map_err(|e| e.to_string())?;
            t.insert(graph_name, bytes).map_err(|e| e.to_string())?;
        }
        wtx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn rehydrate(&self, graph_name: &str) -> Result<Option<Vec<u8>>, String> {
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let t = rtx.open_table(COLD_GRAPHS).map_err(|e| e.to_string())?;
        Ok(t.get(graph_name)
            .map_err(|e| e.to_string())?
            .map(|g| g.value().to_vec()))
    }

    fn is_offloaded(&self, graph_name: &str) -> Result<bool, String> {
        let rtx = self.db.begin_read().map_err(|e| e.to_string())?;
        let t = rtx.open_table(COLD_GRAPHS).map_err(|e| e.to_string())?;
        Ok(t.get(graph_name).map_err(|e| e.to_string())?.is_some())
    }

    fn remove(&self, graph_name: &str) -> Result<(), String> {
        let mut wtx = self.db.begin_write().map_err(|e| e.to_string())?;
        wtx.set_durability(redb::Durability::Immediate)
            .map_err(|e| e.to_string())?;
        {
            let mut t = wtx.open_table(COLD_GRAPHS).map_err(|e| e.to_string())?;
            t.remove(graph_name).map_err(|e| e.to_string())?;
        }
        wtx.commit().map_err(|e| e.to_string())?;
        Ok(())
    }
}

// ── S3/object-store cold tier (feature `cold-tier-s3`) ───────────────────────

#[cfg(feature = "cold-tier-s3")]
mod s3 {
    use super::ColdTier;
    use object_store::aws::AmazonS3Builder;
    use object_store::path::Path as ObjPath;
    use object_store::ObjectStore;
    use tokio::runtime::Runtime;

    /// Object-store-backed cold tier. A cold graph's blob is ONE object at
    /// `{prefix}/{sanitized_graph}`. Reuses the SAME AWS env config as the blob-s3
    /// backend (`EPISTEMIC_GRAPH_BLOB_S3_BUCKET`, AWS endpoint/creds); the cold
    /// prefix is `EPISTEMIC_GRAPH_COLD_S3_PREFIX` (default `cold/graphs`).
    pub struct S3ColdTier {
        store: Box<dyn ObjectStore>,
        prefix: String,
        rt: Runtime,
    }

    impl std::fmt::Debug for S3ColdTier {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("S3ColdTier")
                .field("prefix", &self.prefix)
                .finish()
        }
    }

    impl S3ColdTier {
        pub fn open() -> Result<Self, String> {
            let bucket = std::env::var("EPISTEMIC_GRAPH_BLOB_S3_BUCKET").map_err(|_| {
                "EPISTEMIC_GRAPH_BLOB_S3_BUCKET is required for the cold-tier-s3 backend"
            })?;
            let prefix = std::env::var("EPISTEMIC_GRAPH_COLD_S3_PREFIX")
                .unwrap_or_else(|_| "cold/graphs".into());
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| e.to_string())?;
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| e.to_string())?;
            Ok(Self {
                store: Box::new(store),
                prefix,
                rt,
            })
        }

        fn path(&self, graph_name: &str) -> ObjPath {
            // Sanitize the logical name into a single path segment.
            let safe: String = graph_name
                .chars()
                .map(|c| if c.is_alphanumeric() { c } else { '_' })
                .collect();
            ObjPath::from(format!("{}/{}", self.prefix, safe))
        }
    }

    impl ColdTier for S3ColdTier {
        fn offload(&self, graph_name: &str, bytes: &[u8]) -> Result<(), String> {
            let path = self.path(graph_name);
            self.rt.block_on(async {
                self.store
                    .put(&path, bytes.to_vec().into())
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(())
            })
        }

        fn rehydrate(&self, graph_name: &str) -> Result<Option<Vec<u8>>, String> {
            let path = self.path(graph_name);
            self.rt.block_on(async {
                match self.store.get(&path).await {
                    Ok(r) => Ok(Some(r.bytes().await.map_err(|e| e.to_string())?.to_vec())),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(e.to_string()),
                }
            })
        }

        fn is_offloaded(&self, graph_name: &str) -> Result<bool, String> {
            let path = self.path(graph_name);
            self.rt.block_on(async {
                match self.store.head(&path).await {
                    Ok(_) => Ok(true),
                    Err(object_store::Error::NotFound { .. }) => Ok(false),
                    Err(e) => Err(e.to_string()),
                }
            })
        }

        fn remove(&self, graph_name: &str) -> Result<(), String> {
            let path = self.path(graph_name);
            self.rt.block_on(async {
                match self.store.delete(&path).await {
                    Ok(_) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(e.to_string()),
                }
            })
        }
    }
}

#[cfg(feature = "cold-tier-s3")]
pub use s3::S3ColdTier;

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir() -> String {
        let d = std::env::temp_dir().join(format!(
            "eg-cold-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        d.to_string_lossy().into_owned()
    }

    #[test]
    fn redb_cold_tier_round_trip_and_durable_reopen() {
        let dir = tmp_dir();
        let bytes = b"graph-blob".to_vec();
        {
            let tier = RedbColdTier::open(&dir).unwrap();
            assert!(!tier.is_offloaded("g").unwrap());
            tier.offload("g", &bytes).unwrap();
            assert!(tier.is_offloaded("g").unwrap());
            assert_eq!(tier.rehydrate("g").unwrap().as_deref(), Some(&bytes[..]));
        }
        // Reopen the SAME dir: the offload is durable across "process" lifetimes.
        {
            let tier = RedbColdTier::open(&dir).unwrap();
            assert!(tier.is_offloaded("g").unwrap());
            assert_eq!(tier.rehydrate("g").unwrap().as_deref(), Some(&bytes[..]));
            tier.remove("g").unwrap();
            assert!(!tier.is_offloaded("g").unwrap());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
