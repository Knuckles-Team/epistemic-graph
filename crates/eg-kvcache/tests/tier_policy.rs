use eg_kvcache::{AdmissionRefusal, CacheEntryMeta, GenerationRef, PolicyTier, Tier, TierPolicy};

const FOREVER: u64 = u64::MAX;

fn meta(key: &str, bytes: u64, hits: u64, rebuild_cost_ms: u64) -> CacheEntryMeta {
    CacheEntryMeta {
        key_digest: key.to_string(),
        tier: Tier::Hot,
        bytes,
        hits,
        rebuild_cost_ms,
        deps: Vec::new(),
    }
}

fn tier(max_bytes: u64, min_worth: f64) -> PolicyTier {
    PolicyTier::new(TierPolicy {
        max_bytes,
        min_worth,
    })
}

// spec: MEDULLA-DURABLE-KERNEL-R800.1
#[test]
fn low_worth_entry_is_not_admitted() {
    let entry = meta("cheap", 100, 1, 10);
    assert_eq!(entry.worth(), 0.1);
    assert_eq!(meta("empty", 0, 5, 5).worth(), 0.0);

    let mut cache = tier(1_000, 0.5);
    assert_eq!(
        cache.admit("t1", entry, vec![1; 100], FOREVER),
        Err(AdmissionRefusal::BelowMinWorth)
    );
    assert!(cache.is_empty());
    assert_eq!(cache.used_bytes(), 0);

    cache
        .admit("t1", meta("valuable", 100, 10, 10), vec![2; 100], FOREVER)
        .unwrap();
    assert_eq!(cache.lookup("t1", "valuable", 0), Some(vec![2; 100]));
}

// spec: MEDULLA-DURABLE-KERNEL-R800.2
#[test]
fn lowest_worth_is_evicted_first() {
    let mut cache = tier(300, 0.0);
    cache
        .admit("t1", meta("mid", 100, 5, 20), vec![0; 100], FOREVER)
        .unwrap();
    cache
        .admit("t1", meta("low", 100, 1, 20), vec![0; 100], FOREVER)
        .unwrap();
    cache
        .admit("t1", meta("high", 100, 50, 20), vec![0; 100], FOREVER)
        .unwrap();
    // Hits raise worth: "low" gains enough to overtake "mid".
    for _ in 0..10 {
        assert!(cache.lookup("t1", "low", 0).is_some());
    }

    cache
        .admit("t1", meta("new", 100, 10, 20), vec![0; 100], FOREVER)
        .unwrap();
    assert!(!cache.contains("t1", "mid"), "lowest worth leaves first");
    for kept in ["low", "high", "new"] {
        assert!(cache.contains("t1", kept), "{kept} stays resident");
    }
    assert_eq!(cache.used_bytes(), 300);
    assert_eq!(
        cache.evict_lowest().map(|m| m.key_digest),
        Some("new".into())
    );
}

// spec: MEDULLA-DURABLE-KERNEL-R800.2
#[test]
fn dependency_advance_and_volatility_window_end_the_hit() {
    let mut cache = tier(1_000, 0.0);
    cache.set_generation("graph:g1", 7);
    let mut answer = meta("answer", 10, 1, 1);
    answer.deps = vec![GenerationRef {
        source: "graph:g1".into(),
        generation: 7,
    }];
    cache
        .admit("t1", answer.clone(), b"v7".to_vec(), FOREVER)
        .unwrap();
    assert_eq!(cache.lookup("t1", "answer", 0), Some(b"v7".to_vec()));

    cache.set_generation("graph:g1", 8);
    assert_eq!(cache.lookup("t1", "answer", 0), None, "never a stale hit");
    assert!(!cache.contains("t1", "answer"));
    assert_eq!(cache.used_bytes(), 0);

    let mut volatile = meta("volatile", 10, 1, 1);
    volatile.deps.clear();
    cache.admit("t1", volatile, b"now".to_vec(), 100).unwrap();
    assert_eq!(cache.lookup("t1", "volatile", 99), Some(b"now".to_vec()));
    assert_eq!(cache.lookup("t1", "volatile", 100), None);
}

// spec: MEDULLA-DURABLE-KERNEL-R800.4
#[test]
fn oversize_entry_is_refused() {
    let mut cache = tier(100, 0.0);
    cache
        .admit("t1", meta("resident", 50, 1, 1), vec![0; 50], FOREVER)
        .unwrap();

    assert_eq!(
        cache.admit("t1", meta("huge", 101, 100, 100), vec![0; 101], FOREVER),
        Err(AdmissionRefusal::Oversize {
            bytes: 101,
            max_bytes: 100
        })
    );
    assert_eq!(cache.lookup("t1", "huge", 0), None);

    let mut unknown = meta("unknown-dep", 10, 1, 1);
    unknown.deps = vec![GenerationRef {
        source: "graph:never-declared".into(),
        generation: 1,
    }];
    assert!(matches!(
        cache.admit("t1", unknown, vec![0; 10], FOREVER),
        Err(AdmissionRefusal::UnknownGeneration(_))
    ));
    assert_eq!(cache.lookup("t1", "unknown-dep", 0), None);

    assert_eq!(
        cache.lookup("t2", "resident", 0),
        None,
        "other tenant misses"
    );
    assert!(cache.lookup("t1", "resident", 0).is_some());
    assert_eq!(cache.len(), 1, "refusals stored nothing");
    assert_eq!(cache.used_bytes(), 50);
}
