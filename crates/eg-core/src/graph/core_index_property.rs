use super::*;

impl GraphCore {
    pub(super) fn property_index_add(
        &self,
        id: &str,
        val: &serde_json::Value,
        target_version: u64,
    ) {
        let mut guard = self.property_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        for (key, by_value) in index.keys.iter_mut() {
            if let Some(vk) = val.get(key).and_then(Self::property_value_key) {
                insert_sorted(by_value.entry(vk).or_default(), id);
            }
        }
        self.index_stamps
            .property
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Unfile `id` from the WARM property index. With captured `val`, only its own value posting
    /// per indexed key is touched; without it, every value posting is scanned. No-op when cold.
    pub(super) fn property_index_remove(
        &self,
        id: &str,
        val: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        let mut guard = self.property_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        for (key, by_value) in index.keys.iter_mut() {
            match val
                .and_then(|v| v.get(key))
                .and_then(Self::property_value_key)
            {
                Some(vk) => {
                    if let Some(ids) = by_value.get_mut(&vk) {
                        remove_sorted(ids, id);
                    }
                }
                None => {
                    for ids in by_value.values_mut() {
                        remove_sorted(ids, id);
                    }
                }
            }
        }
        self.index_stamps
            .property
            .store(target_version, std::sync::atomic::Ordering::Release);
    }

    /// Re-file `id` in the WARM property index for the CHANGED keys only: for each changed key
    /// that is indexed, remove the id from that key's value postings then re-add it under its
    /// current value. No-op when cold. Stamped.
    pub(super) fn property_index_refile(
        &self,
        id: &str,
        changed_fields: &[String],
        current: Option<&serde_json::Value>,
        target_version: u64,
    ) {
        let mut guard = self.property_index.write();
        let Some(index) = guard.as_mut() else {
            return;
        };
        let mut touched = false;
        for field in changed_fields {
            let Some(by_value) = index.keys.get_mut(field) else {
                continue;
            };
            touched = true;
            for ids in by_value.values_mut() {
                remove_sorted(ids, id);
            }
            if let Some(vk) = current
                .and_then(|v| v.get(field))
                .and_then(Self::property_value_key)
            {
                insert_sorted(by_value.entry(vk).or_default(), id);
            }
        }
        if touched {
            self.index_stamps
                .property
                .store(target_version, std::sync::atomic::Ordering::Release);
        }
    }
}
