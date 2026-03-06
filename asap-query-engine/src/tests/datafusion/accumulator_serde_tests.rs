//! Accumulator Serde Round-Trip Tests
//!
//! Tests that exercise accumulator_serde.rs directly (no engine needed).
//! Verifies serialize -> deserialize round-trip for all accumulator types.

#[cfg(test)]
mod tests {
    use crate::data_model::SerializableToSink;
    use crate::data_model::{KeyByLabelValues, Measurement};
    use crate::engines::physical::accumulator_serde::{
        deserialize_accumulator, deserialize_keys_accumulator, deserialize_multiple_subpopulation,
        deserialize_single_subpopulation, serialize_accumulator_arroyo,
    };
    use crate::precompute_operators::{
        CountMinSketchAccumulator, DatasketchesKLLAccumulator, DeltaSetAggregatorAccumulator,
        HydraKllSketchAccumulator, IncreaseAccumulator, MultipleIncreaseAccumulator,
        SetAggregatorAccumulator, SumAccumulator,
    };
    use datafusion_summary_library::SketchType;
    use promql_utilities::query_logics::enums::Statistic;
    use std::collections::HashMap;

    // ========================================================================
    // Full round-trip tests (serialize_arroyo -> deserialize)
    // ========================================================================

    #[test]
    fn test_round_trip_sum() {
        let acc = SumAccumulator::with_sum(42.5);
        let bytes = serialize_accumulator_arroyo(&acc);
        let restored = deserialize_accumulator(&bytes, &SketchType::Sum).unwrap();
        assert_eq!(restored.get_accumulator_type(), "SumAccumulator");

        // Query the restored accumulator via single subpopulation
        let restored_single = deserialize_single_subpopulation(&bytes, &SketchType::Sum).unwrap();
        let value = restored_single.query(Statistic::Sum, None).unwrap();
        assert!((value - 42.5).abs() < 1e-10, "Expected 42.5, got {}", value);
    }

    #[test]
    fn test_round_trip_kll() {
        let mut kll = DatasketchesKLLAccumulator::new(200);
        for v in [1.0, 2.0, 3.0, 4.0, 5.0] {
            kll._update(v);
        }

        let bytes = serialize_accumulator_arroyo(&kll);
        let restored = deserialize_accumulator(&bytes, &SketchType::KLL).unwrap();
        assert_eq!(
            restored.get_accumulator_type(),
            "DatasketchesKLLAccumulator"
        );

        // Query quantile via single subpopulation
        let restored_single = deserialize_single_subpopulation(&bytes, &SketchType::KLL).unwrap();
        let mut kwargs = HashMap::new();
        kwargs.insert("quantile".to_string(), "0.5".to_string());
        let median = restored_single
            .query(Statistic::Quantile, Some(&kwargs))
            .unwrap();
        // Median of [1,2,3,4,5] should be ~3.0
        assert!(
            (1.0..=5.0).contains(&median),
            "Median should be in [1,5], got {}",
            median
        );
    }

    #[test]
    fn test_round_trip_set_aggregator() {
        let mut set_acc = SetAggregatorAccumulator::new();
        set_acc.add_key(KeyByLabelValues {
            labels: vec!["web".to_string()],
        });
        set_acc.add_key(KeyByLabelValues {
            labels: vec!["api".to_string()],
        });
        set_acc.add_key(KeyByLabelValues {
            labels: vec!["worker".to_string()],
        });

        let bytes = serialize_accumulator_arroyo(&set_acc);
        let restored = deserialize_accumulator(&bytes, &SketchType::SetAggregator).unwrap();
        let keys = restored.get_keys().unwrap();
        assert_eq!(keys.len(), 3, "Expected 3 keys, got {}", keys.len());
    }

    #[test]
    fn test_round_trip_delta_set_aggregator() {
        let mut delta_set = DeltaSetAggregatorAccumulator::new();
        delta_set.add_key(KeyByLabelValues {
            labels: vec!["endpoint-a".to_string()],
        });
        delta_set.add_key(KeyByLabelValues {
            labels: vec!["endpoint-b".to_string()],
        });

        let bytes = serialize_accumulator_arroyo(&delta_set);
        let restored = deserialize_accumulator(&bytes, &SketchType::DeltaSetAggregator).unwrap();
        let keys = restored.get_keys().unwrap();
        assert_eq!(keys.len(), 2, "Expected 2 keys, got {}", keys.len());
    }

    #[test]
    fn test_round_trip_multiple_increase() {
        let key1 = KeyByLabelValues {
            labels: vec!["web".to_string()],
        };
        let key2 = KeyByLabelValues {
            labels: vec!["api".to_string()],
        };
        let mut increases = HashMap::new();
        increases.insert(
            key1.clone(),
            IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(100.0), 10),
        );
        increases.insert(
            key2.clone(),
            IncreaseAccumulator::new(Measurement::new(0.0), 0, Measurement::new(200.0), 10),
        );

        let acc = MultipleIncreaseAccumulator::new_with_increases(increases);
        let bytes = serialize_accumulator_arroyo(&acc);

        let restored = deserialize_accumulator(&bytes, &SketchType::MultipleIncrease).unwrap();
        let keys = restored.get_keys().unwrap();
        assert_eq!(keys.len(), 2, "Expected 2 keys, got {}", keys.len());

        // Query via multiple subpopulation
        let restored_multi =
            deserialize_multiple_subpopulation(&bytes, &SketchType::MultipleIncrease).unwrap();
        let val = restored_multi
            .query(Statistic::Increase, &key1, None)
            .unwrap();
        assert!(
            (val - 100.0).abs() < 1e-10,
            "Expected increase=100.0 for key1, got {}",
            val
        );
    }

    #[test]
    fn test_round_trip_hydra_kll() {
        // HydraKLL: serialize_to_bytes IS MessagePack, so serialize_accumulator_arroyo
        // falls through to serialize_to_bytes
        let mut hydra = HydraKllSketchAccumulator::new(1, 1, 200);
        // Use the public update method with a key
        hydra.update(
            &KeyByLabelValues {
                labels: vec!["sub-key".to_string()],
            },
            42.0,
        );

        let bytes = serialize_accumulator_arroyo(&hydra);
        let restored = deserialize_accumulator(&bytes, &SketchType::HydraKLL).unwrap();
        assert_eq!(restored.get_accumulator_type(), "HydraKllSketchAccumulator");
    }

    #[test]
    fn test_round_trip_count_min_sketch() {
        // CountMinSketch: serialize_to_bytes IS MessagePack
        // Supported by both deserialize_accumulator (for merging) and
        // deserialize_multiple_subpopulation (for querying by sub-key)
        let cms = CountMinSketchAccumulator::new(2, 3);

        let bytes = serialize_accumulator_arroyo(&cms);

        let restored = deserialize_accumulator(&bytes, &SketchType::CountMinSketch).unwrap();
        assert_eq!(restored.get_accumulator_type(), "CountMinSketchAccumulator");

        let restored =
            deserialize_multiple_subpopulation(&bytes, &SketchType::CountMinSketch).unwrap();
        assert_eq!(
            restored.clone_boxed().as_ref().get_accumulator_type(),
            "CountMinSketchAccumulator"
        );
    }

    // ========================================================================
    // Keys accumulator round-trip
    // ========================================================================

    #[test]
    fn test_deserialize_keys_delta_set() {
        let mut delta_set = DeltaSetAggregatorAccumulator::new();
        delta_set.add_key(KeyByLabelValues {
            labels: vec!["key-a".to_string()],
        });
        let bytes = serialize_accumulator_arroyo(&delta_set);

        let restored =
            deserialize_keys_accumulator(&bytes, &SketchType::DeltaSetAggregator).unwrap();
        let keys = restored.get_keys().unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn test_deserialize_keys_set_aggregator() {
        let mut set_acc = SetAggregatorAccumulator::new();
        set_acc.add_key(KeyByLabelValues {
            labels: vec!["k1".to_string()],
        });
        let bytes = serialize_accumulator_arroyo(&set_acc);

        let restored = deserialize_keys_accumulator(&bytes, &SketchType::SetAggregator).unwrap();
        let keys = restored.get_keys().unwrap();
        assert_eq!(keys.len(), 1);
    }

    // ========================================================================
    // Not-implemented types
    // ========================================================================

    #[test]
    fn test_not_implemented_increase() {
        let bytes = vec![1, 2, 3, 4];
        let result = deserialize_accumulator(&bytes, &SketchType::Increase);
        assert!(result.is_err());
        let err_msg = match result {
            Err(e) => format!("{}", e),
            Ok(_) => panic!("Expected error"),
        };
        assert!(
            err_msg.contains("Not") || err_msg.contains("not"),
            "Expected NotImplemented error, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_not_implemented_minmax() {
        let bytes = vec![1, 2, 3, 4];
        let result = deserialize_accumulator(&bytes, &SketchType::MinMax);
        assert!(result.is_err());
    }

    // ========================================================================
    // Error handling tests
    // ========================================================================

    #[test]
    fn test_corrupted_bytes_sum() {
        // MessagePack can decode some short byte sequences as valid integers/floats
        // (e.g., 0xFF = -1 as negative fixint). Use 0xCB (float64 marker) followed
        // by insufficient bytes to force a decode error.
        let garbage = vec![0xCB, 0x01, 0x02];
        let result = deserialize_accumulator(&garbage, &SketchType::Sum);
        assert!(
            result.is_err(),
            "Corrupted bytes should produce an error, not a panic"
        );
    }

    #[test]
    fn test_corrupted_bytes_kll() {
        let garbage = vec![0xFF, 0xFE, 0xFD, 0xFC];
        let result = deserialize_accumulator(&garbage, &SketchType::KLL);
        assert!(
            result.is_err(),
            "Corrupted bytes should produce an error, not a panic"
        );
    }

    #[test]
    fn test_empty_bytes_sum() {
        let result = deserialize_accumulator(&[], &SketchType::Sum);
        assert!(result.is_err(), "Empty bytes should produce an error");
    }

    #[test]
    fn test_empty_bytes_kll() {
        let result = deserialize_accumulator(&[], &SketchType::KLL);
        assert!(result.is_err(), "Empty bytes should produce an error");
    }

    #[test]
    fn test_empty_bytes_set_aggregator() {
        let result = deserialize_accumulator(&[], &SketchType::SetAggregator);
        assert!(result.is_err(), "Empty bytes should produce an error");
    }

    #[test]
    fn test_empty_bytes_delta_set() {
        let result = deserialize_accumulator(&[], &SketchType::DeltaSetAggregator);
        assert!(result.is_err(), "Empty bytes should produce an error");
    }

    // ========================================================================
    // Serialize dispatch verification
    // ========================================================================

    #[test]
    fn test_serialize_arroyo_dispatch_sum_uses_arroyo_path() {
        // SumAccumulator has a separate arroyo format (MessagePack f64)
        // while its native serialize_to_bytes uses little-endian f64
        let acc = SumAccumulator::with_sum(42.0);
        let arroyo_bytes = serialize_accumulator_arroyo(&acc);
        let native_bytes = acc.serialize_to_bytes();

        // They should differ because arroyo uses MessagePack
        assert_ne!(
            arroyo_bytes, native_bytes,
            "SumAccumulator arroyo and native serialization should differ"
        );

        // Verify the arroyo bytes can be deserialized
        let restored = deserialize_accumulator(&arroyo_bytes, &SketchType::Sum).unwrap();
        assert_eq!(restored.get_accumulator_type(), "SumAccumulator");
    }

    #[test]
    fn test_serialize_arroyo_dispatch_kll_uses_native() {
        // KLL's serialize_to_bytes already uses MessagePack, so arroyo falls through
        let mut kll = DatasketchesKLLAccumulator::new(200);
        kll._update(1.0);
        let arroyo_bytes = serialize_accumulator_arroyo(&kll);
        let native_bytes = kll.serialize_to_bytes();

        // They should be the same since KLL's native IS MessagePack
        assert_eq!(
            arroyo_bytes, native_bytes,
            "KLL arroyo and native serialization should be the same"
        );
    }

    #[test]
    fn test_unsupported_keys_type() {
        let bytes = vec![1, 2, 3, 4];
        let result = deserialize_keys_accumulator(&bytes, &SketchType::Sum);
        assert!(
            result.is_err(),
            "Sum should not be supported as a keys accumulator"
        );
    }
}
