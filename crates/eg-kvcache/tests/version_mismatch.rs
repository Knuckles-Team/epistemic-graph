use eg_kvcache::{KvBlockKey, SharedKvIndex};

// spec: EG-DURABLE-KERNEL-R083.2
#[test]
fn model_revision_mismatch_is_a_typed_refusal_and_preserves_the_block() {
    let mut cache = SharedKvIndex::new();
    let revision_one = KvBlockKey::new("model-a", "weights-v1", "prefix-123");
    let revision_two = KvBlockKey::new("model-a", "weights-v2", "prefix-123");
    let bytes = b"kv-page-from-v1".to_vec();

    assert_eq!(
        cache.put_model_block(&revision_one, bytes.clone()),
        Ok(true)
    );
    assert_eq!(
        cache.get_model_block(&revision_one),
        Ok(Some(bytes.clone()))
    );

    let error = cache.get_model_block(&revision_two).unwrap_err();
    assert_eq!(error.model_id, "model-a");
    assert_eq!(error.token_prefix_hash, "prefix-123");
    assert_eq!(error.stored_version, "weights-v1");
    assert_eq!(error.requested_version, "weights-v2");

    // A refused revision cannot replace or hide the stored bytes.
    assert_eq!(
        cache.put_model_block(&revision_two, b"wrong-revision".to_vec()),
        Err(error)
    );
    assert_eq!(cache.get_model_block(&revision_one), Ok(Some(bytes)));
}

#[test]
fn model_and_prefix_identities_remain_independent() {
    let mut cache = SharedKvIndex::new();
    let first = KvBlockKey::new("model-a", "weights-v1", "same-prefix");
    let second_model = KvBlockKey::new("model-b", "weights-v2", "same-prefix");
    let second_prefix = KvBlockKey::new("model-a", "weights-v2", "other-prefix");

    cache.put_model_block(&first, b"a".to_vec()).unwrap();
    cache.put_model_block(&second_model, b"b".to_vec()).unwrap();
    cache
        .put_model_block(&second_prefix, b"c".to_vec())
        .unwrap();

    assert_eq!(cache.get_model_block(&first), Ok(Some(b"a".to_vec())));
    assert_eq!(
        cache.get_model_block(&second_model),
        Ok(Some(b"b".to_vec()))
    );
    assert_eq!(
        cache.get_model_block(&second_prefix),
        Ok(Some(b"c".to_vec()))
    );
}
