use crate::data_model::{
    AggregateCore, AggregationType, KeyByLabelValues, MergeableAccumulator,
    MultipleSubpopulationAggregate, SerializableToSink,
};
use asap_sketchlib::{message_pack_format::MessagePackCodec, DeltaResult};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use promql_utilities::query_logics::enums::Statistic;

/// Accumulator that tracks sets of added and removed keys.
/// Used for delta aggregation to track changes in cardinality.
/// Wire format (DeltaResult) and msgpack serde live in `asap_sketchlib::sketches`.
#[derive(Debug, Clone)]
pub struct DeltaSetAggregatorAccumulator {
    pub added: HashSet<KeyByLabelValues>,
    pub removed: HashSet<KeyByLabelValues>,
}

impl DeltaSetAggregatorAccumulator {
    pub fn new() -> Self {
        Self {
            added: HashSet::new(),
            removed: HashSet::new(),
        }
    }

    pub fn new_with_sets(
        added: HashSet<KeyByLabelValues>,
        removed: HashSet<KeyByLabelValues>,
    ) -> Self {
        Self { added, removed }
    }

    pub fn add_key(&mut self, key: KeyByLabelValues) {
        self.added.insert(key);
    }

    pub fn remove_key(&mut self, key: KeyByLabelValues) {
        self.removed.insert(key);
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }

    pub fn deserialize_from_json(data: &Value) -> Result<Self, Box<dyn std::error::Error>> {
        let mut added = HashSet::new();
        let mut removed = HashSet::new();

        if let Some(added_array) = data["added"].as_array() {
            for item in added_array {
                let key_data = if let Some(values) = item.get("values") {
                    values
                } else {
                    item
                };
                let key = KeyByLabelValues::deserialize_from_json(key_data)?;
                added.insert(key);
            }
        }

        if let Some(removed_array) = data["removed"].as_array() {
            for item in removed_array {
                let key_data = if let Some(values) = item.get("values") {
                    values
                } else {
                    item
                };
                let key = KeyByLabelValues::deserialize_from_json(key_data)?;
                removed.insert(key);
            }
        }

        Ok(Self { added, removed })
    }

    pub fn deserialize_from_bytes(buffer: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut offset = 0;
        let mut added = HashSet::new();
        let mut removed = HashSet::new();

        // Read added set
        if offset + 4 > buffer.len() {
            return Err("Buffer too short for added set size".into());
        }
        let added_size = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        for _ in 0..added_size {
            if offset + 4 > buffer.len() {
                return Err("Buffer too short for added item size".into());
            }
            let item_size = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + item_size > buffer.len() {
                return Err("Buffer too short for added item data".into());
            }
            let key =
                KeyByLabelValues::deserialize_from_bytes(&buffer[offset..offset + item_size])?;
            offset += item_size;
            added.insert(key);
        }

        // Read removed set
        if offset + 4 > buffer.len() {
            return Err("Buffer too short for removed set size".into());
        }
        let removed_size = u32::from_le_bytes([
            buffer[offset],
            buffer[offset + 1],
            buffer[offset + 2],
            buffer[offset + 3],
        ]) as usize;
        offset += 4;

        for _ in 0..removed_size {
            if offset + 4 > buffer.len() {
                return Err("Buffer too short for removed item size".into());
            }
            let item_size = u32::from_le_bytes([
                buffer[offset],
                buffer[offset + 1],
                buffer[offset + 2],
                buffer[offset + 3],
            ]) as usize;
            offset += 4;

            if offset + item_size > buffer.len() {
                return Err("Buffer too short for removed item data".into());
            }
            let key =
                KeyByLabelValues::deserialize_from_bytes(&buffer[offset..offset + item_size])?;
            offset += item_size;
            removed.insert(key);
        }

        Ok(Self { added, removed })
    }

    pub fn deserialize_from_bytes_arroyo(
        buffer: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Delegate to sketch-core canonical DeltaResult msgpack format
        let delta = DeltaResult::from_msgpack(buffer)
            .map_err(|e| -> Box<dyn std::error::Error> { e.to_string().into() })?;

        let mut added = HashSet::new();
        for item in &delta.added {
            added.insert(KeyByLabelValues::from_semicolon_str(item));
        }

        let mut removed = HashSet::new();
        for item in &delta.removed {
            removed.insert(KeyByLabelValues::from_semicolon_str(item));
        }

        Ok(Self { added, removed })
    }
}

impl Default for DeltaSetAggregatorAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl SerializableToSink for DeltaSetAggregatorAccumulator {
    fn serialize_to_json(&self) -> Value {
        let added_json: Vec<Value> = self
            .added
            .iter()
            .map(|key| key.serialize_to_json())
            .collect();
        let removed_json: Vec<Value> = self
            .removed
            .iter()
            .map(|key| key.serialize_to_json())
            .collect();
        serde_json::json!({ "added": added_json, "removed": removed_json })
    }

    fn serialize_to_bytes(&self) -> Vec<u8> {
        // Delegate to sketch-core canonical DeltaResult msgpack format
        let added: HashSet<String> = self
            .added
            .iter()
            .map(|key| key.to_semicolon_str())
            .collect();
        let removed: HashSet<String> = self
            .removed
            .iter()
            .map(|key| key.to_semicolon_str())
            .collect();
        DeltaResult { added, removed }
            .to_msgpack()
            .unwrap_or_default()
    }
}

impl AggregateCore for DeltaSetAggregatorAccumulator {
    fn type_name(&self) -> &'static str {
        "DeltaSetAggregatorAccumulator"
    }

    fn clone_boxed_core(&self) -> Box<dyn AggregateCore> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn merge_with(
        &self,
        other: &dyn AggregateCore,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        if other.get_accumulator_type() != self.get_accumulator_type() {
            return Err(format!(
                "Cannot merge DeltaSetAggregatorAccumulator with {}",
                other.get_accumulator_type()
            )
            .into());
        }

        let other_delta = other
            .as_any()
            .downcast_ref::<DeltaSetAggregatorAccumulator>()
            .ok_or("Failed to downcast to DeltaSetAggregatorAccumulator")?;

        let merged = Self::merge_accumulators(vec![self.clone(), other_delta.clone()])?;
        Ok(Box::new(merged))
    }

    fn get_accumulator_type(&self) -> AggregationType {
        AggregationType::DeltaSetAggregator
    }

    fn get_keys(&self) -> Option<Vec<KeyByLabelValues>> {
        // A well-formed accumulator (raw or merged) never has the same key in
        // both sets — see merge_accumulators, which enforces this at every
        // fold step. `difference` is a defensive no-op under that invariant;
        // debug_assert catches it loudly if the invariant is ever violated.
        debug_assert!(
            self.added.is_disjoint(&self.removed),
            "DeltaSetAggregatorAccumulator invariant violated: {} key(s) present in both added and removed",
            self.added.intersection(&self.removed).count()
        );
        Some(self.added.difference(&self.removed).cloned().collect())
    }

    fn query_statistic(
        &self,
        statistic: promql_utilities::query_logics::enums::Statistic,
        key: &Option<KeyByLabelValues>,
        query_kwargs: &std::collections::HashMap<String, String>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        use crate::data_model::MultipleSubpopulationAggregate;
        let key_val = key
            .as_ref()
            .ok_or("Key required for DeltaSetAggregatorAccumulator")?;
        self.query(statistic, key_val, Some(query_kwargs))
    }
}

impl MultipleSubpopulationAggregate for DeltaSetAggregatorAccumulator {
    fn query(
        &self,
        _statistic: Statistic,
        _key: &KeyByLabelValues,
        _query_kwargs: Option<&HashMap<String, String>>,
    ) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
        Err("DeltaSetAggregatorAccumulator does not support query operation".into())
    }

    fn clone_boxed(&self) -> Box<dyn MultipleSubpopulationAggregate> {
        Box::new(self.clone())
    }
}

impl MergeableAccumulator<DeltaSetAggregatorAccumulator> for DeltaSetAggregatorAccumulator {
    /// Unlike its sibling accumulators, this merge is **not** commutative:
    /// `added`/`removed` represent chronological key churn, so `accumulators`
    /// must already be in chronological (ascending bucket start-timestamp)
    /// order, and the first element is treated as the starting state (it may
    /// itself already be a merged multi-bucket result, e.g. `self` in a
    /// pairwise `merge_with` fold — not necessarily a single raw bucket).
    /// Each subsequent bucket is folded in as: a key it removes is cleared
    /// from the running `added` set, a key it adds is cleared from the
    /// running `removed` set, then its own added/removed keys are recorded —
    /// so the result always reflects the current, order-correct state
    /// (present vs. known-explicitly-absent) rather than a naive union of
    /// every bucket's sets.
    fn merge_accumulators(
        accumulators: Vec<DeltaSetAggregatorAccumulator>,
    ) -> Result<DeltaSetAggregatorAccumulator, Box<dyn std::error::Error + Send + Sync>> {
        if accumulators.is_empty() {
            return Err("No accumulators to merge".into());
        }

        // A well-formed bucket never has the same key in both its own
        // added/removed -- a single window can't both gain and lose the
        // same key. This is a hard error, not a self-heal: it means the
        // input itself is corrupt, not just an artifact of folding order.
        fn check_disjoint(
            acc: &DeltaSetAggregatorAccumulator,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            if !acc.added.is_disjoint(&acc.removed) {
                return Err(format!(
                    "DeltaSetAggregatorAccumulator bucket has {} key(s) in both added and removed",
                    acc.added.intersection(&acc.removed).count()
                )
                .into());
            }
            Ok(())
        }

        let mut iter = accumulators.into_iter();
        let first = iter.next().unwrap();
        check_disjoint(&first)?;
        let mut added = first.added;
        let mut removed = first.removed;

        for accumulator in iter {
            check_disjoint(&accumulator)?;

            // A bucket can only remove a key the fold so far believes is
            // present -- a key can't disappear before it's ever appeared.
            // Holds because real callers always grow this fold forward from
            // a true starting point (e.g. NaiveMerger only ever appends
            // later buckets, never merges an arbitrary mid-range fragment).
            debug_assert!(
                accumulator.removed.is_subset(&added),
                "DeltaSetAggregatorAccumulator merge received a bucket removing {} key(s) \
                 not currently known present -- buckets must be chronologically ordered \
                 and the fold must start from a valid prior state",
                accumulator.removed.difference(&added).count()
            );
            for key in &accumulator.removed {
                added.remove(key);
            }
            for key in &accumulator.added {
                removed.remove(key);
            }
            added.extend(accumulator.added);
            removed.extend(accumulator.removed);
        }

        Ok(DeltaSetAggregatorAccumulator { added, removed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_key(service: &str) -> KeyByLabelValues {
        KeyByLabelValues::new_with_labels(vec![service.to_string()])
    }

    #[test]
    fn test_delta_set_aggregator_creation() {
        let acc = DeltaSetAggregatorAccumulator::new();
        assert!(acc.added.is_empty());
        assert!(acc.removed.is_empty());
    }

    #[test]
    fn test_delta_set_aggregator_add_remove() {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        acc.add_key(key1.clone());
        acc.remove_key(key2.clone());
        assert!(acc.added.contains(&key1));
        assert!(acc.removed.contains(&key2));
        assert_eq!(acc.added.len(), 1);
        assert_eq!(acc.removed.len(), 1);
    }

    #[test]
    ///
    /// Checks the merged result after each prefix of buckets (through t1,
    /// through t1+t2, through t1+t2+t3), not just the final one -- a fold
    /// that's only correct at the end can't hide here. The first bucket
    /// (t1) only adds keys, never removes -- a bucket can't legitimately
    /// remove a key that no earlier bucket ever added, and t1 has no
    /// earlier bucket. `key2` is removed at t2 and re-added at t3, so from
    /// t3 onward it must be *present* (`added`), not cancelled out of both
    /// sets the way the old union-then-strip-conflicts algorithm used to
    /// leave it.
    fn test_delta_set_aggregator_merge() {
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        let key3 = create_test_key("db");
        let key4 = create_test_key("cache");

        // t1: key1, key2, key3 all first appear. No removals -- valid first bucket.
        let mut acc1 = DeltaSetAggregatorAccumulator::new();
        acc1.add_key(key1.clone());
        acc1.add_key(key2.clone());
        acc1.add_key(key3.clone());

        // t2: key2 and key3 disappear (both were added at t1).
        let mut acc2 = DeltaSetAggregatorAccumulator::new();
        acc2.remove_key(key2.clone());
        acc2.remove_key(key3.clone());

        // t3: key2 reappears, key4 appears for the first time.
        let mut acc3 = DeltaSetAggregatorAccumulator::new();
        acc3.add_key(key2.clone());
        acc3.add_key(key4.clone());

        let buckets = [acc1, acc2, acc3];
        let expected: [(Vec<KeyByLabelValues>, Vec<KeyByLabelValues>); 3] = [
            (vec![key1.clone(), key2.clone(), key3.clone()], vec![]),
            (vec![key1.clone()], vec![key2.clone(), key3.clone()]),
            (
                vec![key1.clone(), key2.clone(), key4.clone()],
                vec![key3.clone()],
            ),
        ];

        for (i, (expected_added, expected_removed)) in expected.iter().enumerate() {
            let prefix = buckets[..=i].to_vec();
            let merged = DeltaSetAggregatorAccumulator::merge_accumulators(prefix).unwrap();
            assert_eq!(
                merged.added,
                expected_added.iter().cloned().collect(),
                "added set wrong after folding through t{}",
                i + 1
            );
            assert_eq!(
                merged.removed,
                expected_removed.iter().cloned().collect(),
                "removed set wrong after folding through t{}",
                i + 1
            );
        }
    }

    #[test]
    fn test_delta_set_aggregator_serialization() {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        let key1 = create_test_key("web");
        let key2 = create_test_key("api");
        acc.add_key(key1.clone());
        acc.remove_key(key2.clone());

        // Test binary (msgpack) serialization roundtrip
        let bytes = acc.serialize_to_bytes();
        let deserialized_bytes =
            DeltaSetAggregatorAccumulator::deserialize_from_bytes_arroyo(&bytes).unwrap();

        assert_eq!(deserialized_bytes.added.len(), 1);
        assert_eq!(deserialized_bytes.removed.len(), 1);
        assert!(deserialized_bytes.added.contains(&key1));
        assert!(deserialized_bytes.removed.contains(&key2));
    }

    #[test]
    fn test_delta_set_aggregator_query() {
        let acc = DeltaSetAggregatorAccumulator::new();
        let key = create_test_key("test");
        assert!(acc.query(Statistic::Sum, &key, None).is_err());
    }

    /// Bug #586 (get_keys #3): a key removed at any point in this
    /// accumulator's history must not hide unrelated keys that are still
    /// currently present. Ordinary label churn (some key was removed at
    /// some point) is not corrupted state.
    #[test]
    fn test_get_keys_returns_present_keys_despite_unrelated_removal() {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        let present_key = create_test_key("web");
        let long_gone_key = create_test_key("retired-service");
        acc.add_key(present_key.clone());
        acc.remove_key(long_gone_key.clone());

        let keys = acc
            .get_keys()
            .expect("get_keys must return Some even when removed is non-empty");
        assert_eq!(keys, vec![present_key]);
    }

    /// Bug #586 (#1): `merge_accumulators` must fold buckets in chronological
    /// order, not union all added/removed sets and strip same-key
    /// "conflicts". A key toggled more than twice across the merged buckets
    /// only nets out correctly if order is respected.
    ///
    /// Scenario: base window adds K, window A removes K, window B re-adds K,
    /// window C removes K again -> chronologically K ends absent, and the
    /// merge should retain that it was explicitly removed (not just silently
    /// forgotten), so it can be told apart from a key nobody ever saw.
    #[test]
    fn test_merge_accumulators_folds_multi_toggle_chronologically() {
        let key = create_test_key("flaky-host");

        let mut base = DeltaSetAggregatorAccumulator::new();
        base.add_key(key.clone());
        let mut window_a = DeltaSetAggregatorAccumulator::new();
        window_a.remove_key(key.clone());
        let mut window_b = DeltaSetAggregatorAccumulator::new();
        window_b.add_key(key.clone());
        let mut window_c = DeltaSetAggregatorAccumulator::new();
        window_c.remove_key(key.clone());

        let merged = DeltaSetAggregatorAccumulator::merge_accumulators(vec![
            base, window_a, window_b, window_c,
        ])
        .unwrap();

        assert!(
            !merged.added.contains(&key),
            "key removed last chronologically must not remain in added"
        );
        assert!(
            merged.removed.contains(&key),
            "key removed last chronologically must be recorded as removed, \
             not silently dropped from both sets"
        );
    }

    /// The chronological-fold invariant (a bucket can't remove a key the
    /// fold doesn't yet believe is present) is a `debug_assert!`, not a hard
    /// `Err` -- only checked in debug builds, so this test is too.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "not currently known present")]
    fn test_merge_accumulators_debug_asserts_on_removal_without_prior_add() {
        let key = create_test_key("phantom");

        // First bucket is a valid, empty starting state -- it never saw `key`.
        let starting_state = DeltaSetAggregatorAccumulator::new();

        // Second bucket claims to remove a key nothing before it ever added.
        let mut removes_unseen_key = DeltaSetAggregatorAccumulator::new();
        removes_unseen_key.remove_key(key);

        let _ = DeltaSetAggregatorAccumulator::merge_accumulators(vec![
            starting_state,
            removes_unseen_key,
        ]);
    }

    /// A bucket with the same key in both its own `added` and `removed` is
    /// corrupt input, not a folding artifact -- merge_accumulators must
    /// reject it with a hard `Err` (checked in all builds, unlike the
    /// chronological-order debug_assert above), whether it's the seed
    /// (first) element or a later one in the fold.
    #[test]
    fn test_merge_accumulators_errors_on_bucket_with_key_in_both_sets() {
        let key = create_test_key("corrupt");

        let mut malformed_seed = DeltaSetAggregatorAccumulator::new();
        malformed_seed.add_key(key.clone());
        malformed_seed.remove_key(key.clone());
        let valid = {
            let mut acc = DeltaSetAggregatorAccumulator::new();
            acc.add_key(create_test_key("unrelated"));
            acc
        };

        let err = DeltaSetAggregatorAccumulator::merge_accumulators(vec![
            malformed_seed.clone(),
            valid.clone(),
        ])
        .expect_err("malformed seed bucket must be rejected");
        assert!(err.to_string().contains("both added and removed"));

        let err = DeltaSetAggregatorAccumulator::merge_accumulators(vec![valid, malformed_seed])
            .expect_err("malformed later bucket must be rejected");
        assert!(err.to_string().contains("both added and removed"));
    }

    #[test]
    fn test_trait_object() {
        let mut acc = DeltaSetAggregatorAccumulator::new();
        let key = create_test_key("web");
        acc.add_key(key.clone());

        let trait_obj: Box<dyn AggregateCore> = Box::new(acc);
        assert_eq!(trait_obj.type_name(), "DeltaSetAggregatorAccumulator");

        let multi_trait_obj: Box<dyn MultipleSubpopulationAggregate> =
            Box::new(DeltaSetAggregatorAccumulator::new());
        let keys = multi_trait_obj.get_keys().unwrap();
        assert_eq!(keys.len(), 0);
    }
}
