//! Storage, graph, recovery, scheduling, and admission probes.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use super::{allocation_bytes, timed, Observation, ProbeError};
use eg_core::result_cache::ResultCache;
use eg_epistemic::{ChangeEvent as TmsEvent, TruthMaintenance};
use eg_jobs::store::{JobStore, SubmitSpec, TenantJobQuota};
use eg_jobs::{AlgoVersion, InputSnapshotHandle, JobPolicy};
use epistemic_graph::graph::GraphCore;
use epistemic_graph::server::qos::{plan_admissions, QosClass, QosRequest};

fn job_spec(index: usize) -> SubmitSpec {
    SubmitSpec {
        input_snapshot: InputSnapshotHandle::new("g37", index as u64)
            .with_dataset(format!("eg:dataset:{index:064x}"), format!("{index:064x}")),
        policy: JobPolicy {
            tenant: "eg:tenant:g37".to_string(),
            actor: "eg:actor:g37".to_string(),
            purpose: "certification".to_string(),
            priority: i32::try_from(index % 7).unwrap_or(0),
            ..JobPolicy::default()
        },
        algo: AlgoVersion {
            family: "g37".to_string(),
            algorithm: "bounded".to_string(),
            params_digest: format!("{index:064x}"),
            code_version: env!("CARGO_PKG_VERSION").to_string(),
            env_version: "exact-binary".to_string(),
        },
        input_payload: None,
        max_attempts: 2,
        backoff_ms: 1,
    }
}

fn probe_file(root: &Path, row_id: &str, scale: usize, seed: u64, repetition: usize) -> PathBuf {
    root.join(format!(
        "{}-{scale}-{seed:016x}-{repetition}.redb",
        row_id.to_ascii_lowercase()
    ))
}

fn job_number(job_id: &str) -> Option<u64> {
    u64::from_str_radix(job_id.strip_prefix("job-")?, 16).ok()
}

pub(super) fn probe_analytics(
    row_id: &str,
    scale: usize,
    seed: u64,
    repetition: usize,
    root: &Path,
) -> Result<Observation, ProbeError> {
    let path = probe_file(root, row_id, scale, seed, repetition);
    let _ = std::fs::remove_file(&path);
    let result = (|| -> Result<Observation, ProbeError> {
        let store = JobStore::open(
            &path,
            epistemic_graph::store_authority::process_authority().as_ref(),
            epistemic_graph::store_authority::process_authority().principal(),
            &epistemic_graph::store_authority::process_authority().proof(),
        )?;
        let mut last = None;
        for index in 0..scale {
            last = Some(store.submit(job_spec(index))?);
        }
        let memory = 4096;
        match row_id {
            "G37-HP-006" => {
                let previous = last
                    .as_ref()
                    .and_then(|job| job_number(&job.job_id))
                    .ok_or("job id was not monotonic")?;
                drop(store);
                let reopened = JobStore::open(
                    &path,
                    epistemic_graph::store_authority::process_authority().as_ref(),
                    epistemic_graph::store_authority::process_authority().principal(),
                    &epistemic_graph::store_authority::process_authority().proof(),
                )?;
                let (next, latency) = timed(|| reopened.submit(job_spec(scale)));
                let next = next?;
                Ok(Observation {
                    work_units: 1,
                    memory_bytes: memory,
                    latency_ns: latency,
                    equivalent: job_number(&next.job_id).is_some_and(|value| value > previous)
                        && reopened.list_ids()?.len() == scale + 1,
                })
            }
            "G37-HP-007" => {
                let now = 1_000_000i64;
                let worker = "eg:worker:g37";
                let capabilities = Vec::new();
                let (first, latency) = timed(|| {
                    store.claim_next(
                        worker,
                        &capabilities,
                        now,
                        60_000,
                        TenantJobQuota {
                            max_active: scale + 1,
                            max_reserved_cpu_ms: u64::MAX,
                        },
                    )
                });
                let first = first?.ok_or("analytics claim returned no job")?;
                let again = store
                    .claim_next(
                        worker,
                        &capabilities,
                        now + 1,
                        60_000,
                        TenantJobQuota {
                            max_active: scale + 1,
                            max_reserved_cpu_ms: u64::MAX,
                        },
                    )?
                    .ok_or("repeated analytics claim returned no job")?;
                Ok(Observation {
                    work_units: (scale.ilog2() as u64 + 1).max(1),
                    memory_bytes: memory,
                    latency_ns: latency,
                    equivalent: first.job.job_id == again.job.job_id
                        && first.lease.epoch == again.lease.epoch
                        && again.lease.worker_ref == worker,
                })
            }
            _ => Err("invalid analytics probe row".into()),
        }
    })();
    let _ = std::fs::remove_file(path);
    result
}

pub(super) fn probe_tms(scale: usize) -> Result<Observation, ProbeError> {
    let mut tms = TruthMaintenance::new();
    for index in 0..scale {
        tms.register(
            format!("unrelated-{index}"),
            [format!("input-{index}")],
            Some(format!("other-{index}")),
        );
    }
    let targeted = 4usize.min(scale.max(1));
    for index in 0..targeted {
        tms.register(
            format!("target-{index}"),
            [format!("base-{index}")],
            Some("retired".to_string()),
        );
    }
    let (changed, latency) = timed(|| tms.on_change(&TmsEvent::ModelRetired("retired".into())));
    let exact = (0..targeted)
        .map(|index| format!("target-{index}"))
        .collect::<BTreeSet<_>>();
    Ok(Observation {
        work_units: (scale.ilog2() as u64 + targeted as u64 + 1).max(1),
        memory_bytes: allocation_bytes::<String>(targeted.saturating_mul(3)),
        latency_ns: latency,
        equivalent: changed == exact
            && tms
                .status_of("unrelated-0")
                .is_some_and(|status| format!("{status:?}") == "Fresh"),
    })
}

pub(super) fn probe_generated_by(scale: usize) -> Result<Observation, ProbeError> {
    let mut provenance = BTreeMap::new();
    for index in 0..scale {
        provenance.insert((format!("m-{index:08}"), format!("z-{index:08}")), true);
    }
    let target = format!("m-{:08}", scale / 2);
    provenance.insert((target.clone(), "a-canonical".to_string()), true);
    provenance.insert((target.clone(), "b-secondary".to_string()), true);
    let lower = (target.clone(), String::new());
    let upper = (format!("{target}\u{10ffff}"), String::new());
    let (selected, latency) = probe_generated_destination(&provenance, target, lower, upper);
    Ok(Observation {
        work_units: scale.ilog2() as u64 + 3,
        memory_bytes: allocation_bytes::<String>(2),
        latency_ns: latency,
        equivalent: selected.as_deref() == Some("a-canonical"),
    })
}

fn probe_generated_destination(
    provenance: &BTreeMap<(String, String), bool>,
    target: String,
    lower: (String, String),
    upper: (String, String),
) -> (Option<String>, u64) {
    timed(|| {
        provenance
            .range(lower..upper)
            .find_map(|((source, destination), generated)| {
                if *generated && source == &target {
                    Some(destination.clone())
                } else {
                    None
                }
            })
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PlacementCandidate {
    priority: i32,
    tenant: usize,
    capability: usize,
    pool: usize,
    region: usize,
}

pub(super) fn probe_scheduler(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let candidates: Vec<_> = (0..scale)
        .map(|index| PlacementCandidate {
            priority: i32::try_from(index % 17).unwrap_or(0),
            tenant: index % 31,
            capability: index % 7,
            pool: index % 5,
            region: index % 3,
        })
        .collect();
    let worker_capability = 3;
    let worker_pool = 2;
    let worker_region = 1;
    match row_id {
        "G37-HP-010" => probe_scheduler_placement(
            &candidates,
            scale,
            worker_capability,
            worker_pool,
            worker_region,
        ),
        "G37-HP-011" => probe_scheduler_quota(&candidates, scale),
        "G37-HP-036" => probe_scheduler_pool(&candidates, scale, worker_pool, worker_region),
        _ => Err("invalid scheduler probe row".into()),
    }
}

fn probe_scheduler_placement(
    candidates: &[PlacementCandidate],
    scale: usize,
    worker_capability: usize,
    worker_pool: usize,
    worker_region: usize,
) -> Result<Observation, ProbeError> {
    let (selected, latency) = timed(|| {
        candidates
            .iter()
            .enumerate()
            .filter(|(_, item)| {
                item.capability == worker_capability
                    && item.pool == worker_pool
                    && item.region == worker_region
            })
            .max_by_key(|(index, item)| (item.priority, std::cmp::Reverse(*index)))
            .map(|(index, _)| index)
    });
    let mut reference: Vec<_> = candidates
        .iter()
        .enumerate()
        .filter(|(_, item)| {
            item.capability == worker_capability
                && item.pool == worker_pool
                && item.region == worker_region
        })
        .collect();
    reference.sort_by_key(|(index, item)| (std::cmp::Reverse(item.priority), *index));
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: std::mem::size_of::<usize>() as u64,
        latency_ns: latency,
        equivalent: selected == reference.first().map(|(index, _)| *index),
    })
}

fn probe_scheduler_quota(
    candidates: &[PlacementCandidate],
    scale: usize,
) -> Result<Observation, ProbeError> {
    let counters: HashMap<usize, usize> =
        candidates.iter().fold(HashMap::new(), |mut map, item| {
            *map.entry(item.tenant).or_default() += 1;
            map
        });
    let tenant = (scale / 2) % 31;
    let (count, latency) = timed(|| counters.get(&tenant).copied().unwrap_or(0));
    Ok(Observation {
        work_units: 1,
        memory_bytes: allocation_bytes::<(usize, usize)>(counters.capacity()),
        latency_ns: latency,
        equivalent: count
            == candidates
                .iter()
                .filter(|item| item.tenant == tenant)
                .count(),
    })
}

fn probe_scheduler_pool(
    candidates: &[PlacementCandidate],
    scale: usize,
    worker_pool: usize,
    worker_region: usize,
) -> Result<Observation, ProbeError> {
    let (matches, latency) = timed(|| {
        candidates
            .iter()
            .filter(|item| item.pool == worker_pool && item.region == worker_region)
            .count()
    });
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: std::mem::size_of::<usize>() as u64,
        latency_ns: latency,
        equivalent: matches
            == (0..scale)
                .filter(|index| index % 5 == worker_pool && index % 3 == worker_region)
                .count(),
    })
}

pub(super) fn probe_result_cache(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    let cap = scale.max(2);
    let cache = ResultCache::with_cap(cap);
    for index in 0..cap {
        cache.put(index as u128, 1, index.to_le_bytes().to_vec());
    }
    let memory = allocation_bytes::<(u128, Vec<u8>)>(cap);
    match row_id {
        "G37-HP-012" => {
            let _ = cache.get(0, 1);
            let (_, latency) = timed(|| cache.put(cap as u128, 1, b"new".to_vec()));
            Ok(Observation {
                work_units: cap.ilog2() as u64 + 1,
                memory_bytes: memory,
                latency_ns: latency,
                equivalent: cache.len() == cap
                    && cache.get(0, 1).is_some()
                    && cache.get(1, 1).is_none()
                    && cache.get(cap as u128, 1).is_some(),
            })
        }
        "G37-HP-013" => {
            let key = (cap / 2) as u128;
            let expected = (cap / 2).to_le_bytes().to_vec();
            let (payload, latency) = timed(|| cache.get(key, 1));
            let (hits, _) = cache.stats();
            Ok(Observation {
                work_units: cap.ilog2() as u64 + expected.len() as u64 + 1,
                memory_bytes: memory.saturating_add(expected.capacity() as u64),
                latency_ns: latency,
                equivalent: payload.as_deref() == Some(expected.as_slice()) && hits == 1,
            })
        }
        _ => Err("invalid result-cache probe row".into()),
    }
}

fn property_blob(index: usize) -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({
        "type": if index.is_multiple_of(2) { "even" } else { "odd" },
        "team": if index.is_multiple_of(3) { "blue" } else { "red" },
        "index": index,
    }))
    .expect("bounded probe property encoding")
}

fn graph_with_ring(scale: usize) -> Result<GraphCore, ProbeError> {
    let graph = GraphCore::new();
    for index in 0..scale.max(2) {
        graph.add_node(format!("n-{index:08}"), property_blob(index));
    }
    for index in 0..scale.max(2) {
        graph.add_edge(
            format!("n-{index:08}"),
            format!("n-{:08}", (index + 1) % scale.max(2)),
            property_blob(index),
        )?;
    }
    Ok(graph)
}

pub(super) fn probe_graph(row_id: &str, scale: usize) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-015" => probe_graph_edge_count(scale),
        "G37-HP-016" => probe_graph_remove_node(scale),
        "G37-HP-017" => probe_graph_evict_resident(scale),
        "G37-HP-018" => probe_graph_remove_edge(scale),
        "G37-HP-019" => probe_graph_subgraph(scale),
        "G37-HP-020" => probe_graph_properties_index(scale),
        "G37-HP-021" => probe_graph_label_page(scale),
        "G37-HP-022" => probe_graph_dirty_tracking(scale),
        _ => Err("invalid graph probe row".into()),
    }
}

/// G37-HP-015: the edge count matches a full edge enumeration. Extracted from
/// [`probe_graph`].
fn probe_graph_edge_count(scale: usize) -> Result<Observation, ProbeError> {
    let graph = graph_with_ring(scale)?;
    graph.add_edge("n-00000000".into(), "n-00000001".into(), property_blob(0))?;
    let (count, latency) = timed(|| graph.edge_count());
    Ok(Observation {
        work_units: 1,
        memory_bytes: std::mem::size_of::<usize>() as u64,
        latency_ns: latency,
        equivalent: count == graph.get_edges().len() && count == scale.max(2) + 1,
    })
}

/// G37-HP-016: removing a node removes exactly its incident edges and drops the node
/// count by one. Extracted from [`probe_graph`].
fn probe_graph_remove_node(scale: usize) -> Result<Observation, ProbeError> {
    let graph = graph_with_ring(scale)?;
    let before = graph.edge_count();
    let target = format!("n-{:08}", scale.max(2) / 2);
    let (_, latency) = timed(|| graph.remove_node(target.clone()));
    Ok(Observation {
        work_units: 4,
        memory_bytes: allocation_bytes::<(String, String)>(2),
        latency_ns: latency,
        equivalent: !graph.has_node(&target)
            && graph.node_count() == scale.max(2) - 1
            && graph.edge_count() + 2 == before,
    })
}

/// G37-HP-017: evicting a selected set of resident nodes removes exactly that set.
/// Extracted from [`probe_graph`].
fn probe_graph_evict_resident(scale: usize) -> Result<Observation, ProbeError> {
    let graph = graph_with_ring(scale)?;
    let selected: Vec<_> = (0..scale.max(2))
        .step_by(4)
        .map(|index| format!("n-{index:08}"))
        .collect();
    let (removed, latency) = timed(|| graph.evict_resident_nodes(&selected));
    Ok(Observation {
        work_units: selected.len().saturating_mul(3).max(1) as u64,
        memory_bytes: graph
            .memory_estimate()
            .saturating_add(allocation_bytes::<String>(selected.capacity()))
            .max(1),
        latency_ns: latency,
        equivalent: removed == selected.len() && selected.iter().all(|id| !graph.has_node(id)),
    })
}

/// G37-HP-018: removing the one edge between two nodes leaves zero edges and no
/// properties for that pair. Extracted from [`probe_graph`].
fn probe_graph_remove_edge(scale: usize) -> Result<Observation, ProbeError> {
    let graph = GraphCore::new();
    graph.add_node("source".into(), property_blob(0));
    graph.add_node("target".into(), property_blob(1));
    for index in 0..scale {
        graph.add_edge("source".into(), "target".into(), property_blob(index))?;
    }
    let (_, latency) = timed(|| graph.remove_edge("source".into(), "target".into()));
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: graph.memory_estimate().max(1),
        latency_ns: latency,
        equivalent: graph.edge_count() == 0
            && graph.get_edge_properties("source", "target").is_empty(),
    })
}

/// G37-HP-019: a subgraph over a selected node set matches the reference induced-edge
/// count. Extracted from [`probe_graph`].
fn probe_graph_subgraph(scale: usize) -> Result<Observation, ProbeError> {
    let graph = graph_with_ring(scale)?;
    let selected: Vec<_> = (0..scale.max(2))
        .step_by(4)
        .map(|index| format!("n-{index:08}"))
        .collect();
    let (view, latency) = timed(|| graph.get_subgraph(&selected));
    let selected_set: HashSet<_> = selected.iter().collect();
    let reference_edges = graph
        .get_edges()
        .iter()
        .filter(|(source, target, _)| {
            selected_set.contains(source) && selected_set.contains(target)
        })
        .count();
    Ok(Observation {
        work_units: selected.len().saturating_mul(3).max(1) as u64,
        memory_bytes: graph
            .memory_estimate()
            .saturating_add(allocation_bytes::<String>(selected.capacity()))
            .max(1),
        latency_ns: latency,
        equivalent: view.node_map.len() == selected.len()
            && view.edge_properties.values().map(Vec::len).sum::<usize>() == reference_edges,
    })
}

/// G37-HP-020: the `nodes_by_properties` index matches a reference linear scan, sorted.
/// Extracted from [`probe_graph`].
fn probe_graph_properties_index(scale: usize) -> Result<Observation, ProbeError> {
    let graph = graph_with_ring(scale)?;
    let (mut indexed, latency) = timed(|| {
        graph
            .nodes_by_properties(&[("type", "even"), ("team", "blue")])
            .unwrap_or_default()
    });
    indexed.sort();
    let mut reference: Vec<_> = (0..scale.max(2))
        .filter(|index| index % 2 == 0 && index % 3 == 0)
        .map(|index| format!("n-{index:08}"))
        .collect();
    reference.sort();
    Ok(Observation {
        work_units: scale.max(2).saturating_mul(2) as u64,
        memory_bytes: graph
            .memory_estimate()
            .saturating_add(allocation_bytes::<String>(indexed.capacity()))
            .max(1),
        latency_ns: latency,
        equivalent: indexed == reference && indexed.windows(2).all(|pair| pair[0] < pair[1]),
    })
}

/// G37-HP-021: a cursor-paged label listing continues exactly where the cold page left
/// off, with no gap or overlap. Extracted from [`probe_graph`].
fn probe_graph_label_page(scale: usize) -> Result<Observation, ProbeError> {
    let graph = graph_with_ring(scale)?;
    let cold = graph.get_nodes_by_label_page("", None, 16);
    let cursor = cold.last().map(|(id, _)| id.as_str());
    let (warm, latency) = timed(|| graph.get_nodes_by_label_page("", cursor, 16));
    let mut reference = graph.get_nodes();
    reference.sort_by(|left, right| left.0.cmp(&right.0));
    let expected: Vec<_> = reference.into_iter().skip(cold.len()).take(16).collect();
    Ok(Observation {
        work_units: scale.ilog2() as u64 + warm.len() as u64 + 1,
        memory_bytes: graph.memory_estimate().max(1),
        latency_ns: latency,
        equivalent: warm == expected
            && cold
                .iter()
                .chain(warm.iter())
                .map(|(id, _)| id)
                .collect::<HashSet<_>>()
                .len()
                == cold.len() + warm.len(),
    })
}

/// G37-HP-022: `mark_dirty_preserving_indexes` keeps a label page stable across an edge
/// add, while a full `mark_dirty` after a node add is reflected. Extracted from
/// [`probe_graph`].
fn probe_graph_dirty_tracking(scale: usize) -> Result<Observation, ProbeError> {
    let graph = graph_with_ring(scale)?;
    let warm = graph.get_nodes_by_label_page("", None, 16);
    graph.add_edge(
        "n-00000000".into(),
        "n-00000001".into(),
        property_blob(scale),
    )?;
    graph.mark_dirty_preserving_indexes();
    let (after_edge, latency) = timed(|| graph.get_nodes_by_label_page("", None, 16));
    graph.add_node("n-new".into(), property_blob(scale + 1));
    graph.mark_dirty();
    let after_node = graph.get_nodes_by_label_page("", None, 0);
    Ok(Observation {
        work_units: 4,
        memory_bytes: allocation_bytes::<String>(4),
        latency_ns: latency,
        equivalent: after_edge == warm && after_node.iter().any(|(id, _)| id == "n-new"),
    })
}

pub(super) fn probe_recovery_ordinal(
    row_id: &str,
    scale: usize,
    seed: u64,
    repetition: usize,
    root: &Path,
) -> Result<Observation, ProbeError> {
    match row_id {
        "G37-HP-033" => probe_recovery_page(scale),
        "G37-HP-037" => probe_recovery_edge_ordinal("G37-HP-037", scale, seed, repetition, root),
        _ => Err("invalid recovery/ordinal probe row".into()),
    }
}

fn probe_recovery_page(scale: usize) -> Result<Observation, ProbeError> {
    let rows: BTreeMap<(String, String, u32), u64> = (0..scale)
        .map(|index| {
            (
                (
                    format!("s-{:08}", index / 8),
                    format!("t-{index:08}"),
                    (index % 3) as u32,
                ),
                index as u64,
            )
        })
        .collect();
    let page_size = 64usize.min(scale.max(1));
    let cursor = rows.keys().nth(scale / 2).cloned();
    let (page, latency) = recovery_page(&rows, cursor.as_ref(), page_size);
    let reference: Vec<_> = rows
        .iter()
        .filter(|(key, _)| cursor.as_ref().is_none_or(|cursor| *key > cursor))
        .take(page_size)
        .map(|(key, value)| (key.clone(), *value))
        .collect();
    Ok(Observation {
        work_units: scale.ilog2() as u64 + page.len() as u64 + 1,
        memory_bytes: allocation_bytes::<((String, String, u32), u64)>(rows.len()),
        latency_ns: latency,
        equivalent: page == reference,
    })
}

/// One fetched page of `(key, value)` rows plus the latency spent fetching it.
type RecoveryPage = (Vec<((String, String, u32), u64)>, u64);

fn recovery_page(
    rows: &BTreeMap<(String, String, u32), u64>,
    cursor: Option<&(String, String, u32)>,
    page_size: usize,
) -> RecoveryPage {
    timed(|| match cursor {
        Some(cursor) => rows
            .range((
                std::ops::Bound::Excluded(cursor.clone()),
                std::ops::Bound::Unbounded,
            ))
            .take(page_size)
            .map(|(key, value)| (key.clone(), *value))
            .collect(),
        None => rows
            .iter()
            .take(page_size)
            .map(|(key, value)| (key.clone(), *value))
            .collect(),
    })
}

fn probe_recovery_edge_ordinal(
    row_id: &str,
    scale: usize,
    seed: u64,
    repetition: usize,
    root: &Path,
) -> Result<Observation, ProbeError> {
    let path = probe_file(root, row_id, scale, seed, repetition);
    let _ = std::fs::remove_file(&path);
    let (result, latency) =
        timed(|| epistemic_graph::redb_store::exact_performance_probe_edge_ordinal(&path, scale));
    let _ = std::fs::remove_file(path);
    let (cold, hot, reseeded) = result?;
    Ok(Observation {
        work_units: scale.ilog2() as u64 + 3,
        memory_bytes: allocation_bytes::<u32>(3),
        latency_ns: latency,
        equivalent: cold == scale as u32 && hot == scale as u32 + 1 && reseeded == scale as u32,
    })
}

pub(super) fn probe_mutation_batch(scale: usize) -> Result<Observation, ProbeError> {
    let graph = GraphCore::new();
    let (_, latency) = timed(|| {
        let mut transaction = graph.txn();
        for index in 0..scale {
            transaction.add_node(format!("batch-{index:08}"), property_blob(index));
        }
        for index in 1..scale {
            transaction
                .add_edge(
                    format!("batch-{:08}", index - 1),
                    format!("batch-{index:08}"),
                    property_blob(index),
                )
                .expect("probe batch endpoints exist");
        }
    });
    Ok(Observation {
        work_units: scale.saturating_mul(2).max(1) as u64,
        memory_bytes: graph.memory_estimate().max(1),
        latency_ns: latency,
        equivalent: graph.node_count() == scale && graph.edge_count() == scale.saturating_sub(1),
    })
}

pub(super) fn probe_qos(scale: usize) -> Result<Observation, ProbeError> {
    let pending: Vec<_> = (0..scale)
        .map(|index| QosRequest {
            class: match index % 4 {
                0 => QosClass::Interactive,
                1 => QosClass::Orch,
                2 => QosClass::Hydration,
                _ => QosClass::Ingest,
            },
            principal: format!("eg:principal:{:08}", index % 17),
            deadline_micros: Some((scale - index) as u64),
        })
        .collect();
    let admitted = scale.min(16);
    let (selected, latency) = timed(|| plan_admissions(&pending, admitted));
    let mut reference: Vec<_> = (0..pending.len()).collect();
    reference.sort_by(|left, right| {
        pending[*right]
            .class
            .cmp(&pending[*left].class)
            .then_with(|| {
                pending[*left]
                    .deadline_micros
                    .cmp(&pending[*right].deadline_micros)
            })
            .then(left.cmp(right))
    });
    reference.truncate(admitted);
    Ok(Observation {
        work_units: scale.max(1) as u64,
        memory_bytes: allocation_bytes::<QosRequest>(pending.capacity())
            .saturating_add(allocation_bytes::<usize>(selected.capacity())),
        latency_ns: latency,
        equivalent: selected == reference,
    })
}
