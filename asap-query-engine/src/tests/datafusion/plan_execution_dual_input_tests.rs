//! Plan Execution Multi-Population Tests
//!
//! Tests for multi-population accumulator execution:
//!
//! **Self-keyed (single-input):** MultipleIncrease, MultipleSum, MultipleMinMax
//!   These carry their own keys via get_keys(). No separate keys stream needed.
//!   grouping_labels = [], aggregated_labels = [all output labels]
//!
//! **Dual-input:** HydraKLL, CountMinSketch
//!   These need a separate DeltaSetAggregator keys stream to enumerate sub-keys.

#[cfg(test)]
mod tests {
    use crate::data_model::{KeyByLabelValues, Measurement};
    use crate::precompute_operators::{
        CountMinSketchAccumulator, DeltaSetAggregatorAccumulator, HydraKllSketchAccumulator,
        IncreaseAccumulator, MultipleIncreaseAccumulator,
    };
    use crate::tests::test_utilities::engine_factories::*;
    use std::collections::HashMap;

    /// Helper: create a MultipleIncreaseAccumulator with given sub-keys.
    /// Each key is a Vec of label values (matching aggregated_labels order).
    fn make_multi_increase(
        keys_and_values: Vec<(Vec<&str>, f64, f64)>,
    ) -> MultipleIncreaseAccumulator {
        let mut increases = HashMap::new();
        for (key_labels, start_val, end_val) in keys_and_values {
            let key = KeyByLabelValues {
                labels: key_labels.iter().map(|s| s.to_string()).collect(),
            };
            increases.insert(
                key,
                IncreaseAccumulator::new(
                    Measurement::new(start_val),
                    0,
                    Measurement::new(end_val),
                    10,
                ),
            );
        }
        MultipleIncreaseAccumulator::new_with_increases(increases)
    }

    // ========================================================================
    // Self-keyed: MultipleIncrease (single-input, keys from accumulator)
    // MultipleIncrease is a collection of Increase accumulators.
    // grouping_labels = [], aggregated_labels = [host, endpoint]
    // Query is simply increase(metric[window]).
    // ========================================================================

    #[tokio::test]
    async fn test_self_keyed_multiple_increase() {
        // Sub-keys: (host-a, endpoint-1) increase=100, (host-a, endpoint-2) increase=200
        let acc = make_multi_increase(vec![
            (vec!["host-a", "endpoint-1"], 0.0, 100.0),
            (vec!["host-a", "endpoint-2"], 0.0, 200.0),
        ]);

        let engine = create_engine_single_pop_with_aggregated(
            "http_requests_total",
            "MultipleIncreaseAccumulator",
            vec![],
            vec!["host", "endpoint"],
            vec![(None, Box::new(acc))],
            "increase(http_requests_total[10s])",
        );

        let results = execute_new_plan(&engine, "increase(http_requests_total[10s])", 1000.0).await;

        assert!(
            !results.is_empty(),
            "Self-keyed multi-pop should produce results; got 0"
        );

        // Should have 2 results (one per sub-key)
        assert_eq!(
            results.len(),
            2,
            "Expected 2 results (2 sub-keys), got {}",
            results.len()
        );
    }

    #[tokio::test]
    async fn test_self_keyed_multiple_increase_single_key() {
        let acc = make_multi_increase(vec![(vec!["host-a", "svc-web"], 0.0, 50.0)]);

        let engine = create_engine_single_pop_with_aggregated(
            "http_requests_total",
            "MultipleIncreaseAccumulator",
            vec![],
            vec!["host", "service"],
            vec![(None, Box::new(acc))],
            "increase(http_requests_total[10s])",
        );

        let results = execute_new_plan(&engine, "increase(http_requests_total[10s])", 1000.0).await;

        assert_eq!(
            results.len(),
            1,
            "Expected 1 result (1 sub-key), got {}",
            results.len()
        );
    }

    // ========================================================================
    // HydraKLL + DeltaSetAggregator (quantile dual-input)
    // ========================================================================

    #[tokio::test]
    async fn test_dual_hydra_kll_delta_set() {
        // HydraKLL: single accumulator, no spatial GROUP BY.
        // Sub-keys are ["host", "endpoint"] — tracked by DeltaSet, queryable in HydraKLL.
        // 2 columns per sub-key (host, endpoint).
        let mut hydra = HydraKllSketchAccumulator::new(1, 2, 200);
        hydra.update(
            &KeyByLabelValues {
                labels: vec!["host-a".to_string(), "endpoint-a".to_string()],
            },
            10.0,
        );
        hydra.update(
            &KeyByLabelValues {
                labels: vec!["host-a".to_string(), "endpoint-a".to_string()],
            },
            20.0,
        );
        hydra.update(
            &KeyByLabelValues {
                labels: vec!["host-a".to_string(), "endpoint-b".to_string()],
            },
            100.0,
        );

        // DeltaSet enumerates which sub-keys exist
        let mut keys = DeltaSetAggregatorAccumulator::new();
        keys.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "endpoint-a".to_string()],
        });
        keys.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "endpoint-b".to_string()],
        });

        let engine = create_engine_dual_input(
            "request_duration",
            "HydraKllSketchAccumulator",
            "DeltaSetAggregator",
            vec![],                        // grouping_labels: no store GROUP BY
            vec!["host", "endpoint"],      // aggregated_labels: sub-keys tracked by DeltaSet
            vec![(None, Box::new(hydra))], // store key = None
            vec![(None, Box::new(keys))],  // store key = None
            "quantile(0.5, request_duration) by (host, endpoint)",
        );

        let results = execute_new_plan(
            &engine,
            "quantile(0.5, request_duration) by (host, endpoint)",
            1000.0,
        )
        .await;

        // Should have 2 results: one per (host, endpoint) sub-key
        assert_eq!(
            results.len(),
            2,
            "HydraKLL dual-input should produce 2 results, got {}",
            results.len()
        );
    }

    // ========================================================================
    // CountMinSketch + DeltaSetAggregator (frequency dual-input)
    // ========================================================================

    #[tokio::test]
    async fn test_dual_count_min_delta_set() {
        // CountMinSketch: single accumulator, no spatial GROUP BY.
        // Sub-keys are ["host", "event"] — tracked by DeltaSet, queryable in CMS.
        let cms = CountMinSketchAccumulator::new(2, 3);

        let mut keys = DeltaSetAggregatorAccumulator::new();
        keys.add_key(KeyByLabelValues {
            labels: vec!["host-a".to_string(), "evt-1".to_string()],
        });

        let engine = create_engine_dual_input(
            "event_frequency",
            "CountMinSketchAccumulator",
            "DeltaSetAggregator",
            vec![],                       // grouping_labels: no store GROUP BY
            vec!["host", "event"],        // aggregated_labels: sub-keys tracked by DeltaSet
            vec![(None, Box::new(cms))],  // store key = None
            vec![(None, Box::new(keys))], // store key = None
            "count(event_frequency) by (host, event)",
        );

        let results =
            execute_new_plan(&engine, "count(event_frequency) by (host, event)", 1000.0).await;

        // CMS query may return 0 for un-updated keys, but should not error
        assert!(!results.is_empty(), "CMS dual-input should produce results");
    }

    // ========================================================================
    // Self-keyed: multiple accumulators (multiple store entries)
    // ========================================================================

    #[tokio::test]
    async fn test_self_keyed_multiple_accumulators() {
        // Two separate MultipleIncrease accumulators in the store (both with key=None)
        // acc1 has (host-a, ep-1) and (host-a, ep-2)
        // acc2 has (host-b, ep-3)
        let acc1 = make_multi_increase(vec![
            (vec!["host-a", "ep-1"], 0.0, 10.0),
            (vec!["host-a", "ep-2"], 0.0, 20.0),
        ]);
        let acc2 = make_multi_increase(vec![(vec!["host-b", "ep-3"], 0.0, 30.0)]);

        let engine = create_engine_single_pop_with_aggregated(
            "requests",
            "MultipleIncreaseAccumulator",
            vec![],
            vec!["host", "endpoint"],
            vec![(None, Box::new(acc1)), (None, Box::new(acc2))],
            "increase(requests[10s])",
        );

        let results = execute_new_plan(&engine, "increase(requests[10s])", 1000.0).await;

        // 2 from acc1 + 1 from acc2 = 3 total
        assert_eq!(
            results.len(),
            3,
            "Expected 3 results (2 + 1), got {}",
            results.len()
        );
    }

    // ========================================================================
    // Self-keyed: empty accumulator
    // ========================================================================

    #[tokio::test]
    async fn test_self_keyed_empty_keys() {
        // MultipleIncrease with no sub-keys -> 0 results
        let empty = MultipleIncreaseAccumulator::new_with_increases(HashMap::new());

        let engine = create_engine_single_pop_with_aggregated(
            "requests",
            "MultipleIncreaseAccumulator",
            vec![],
            vec!["host", "endpoint"],
            vec![(None, Box::new(empty))],
            "increase(requests[10s])",
        );

        let results = execute_new_plan(&engine, "increase(requests[10s])", 1000.0).await;

        assert!(
            results.is_empty(),
            "Empty MultipleIncrease should give 0 results, got {}",
            results.len()
        );
    }

    // ========================================================================
    // Dual-input: mismatched spatial groups (HydraKLL)
    // ========================================================================

    #[tokio::test]
    async fn test_dual_mismatched_spatial_groups() {
        // No spatial GROUP BY: single HydraKLL and single DeltaSet.
        // HydraKLL has data for (host-a, ep-1), DeltaSet tracks (host-b, ep-2).
        // The DeltaSet key doesn't match any data in HydraKLL, so query returns 0/default.
        let mut hydra = HydraKllSketchAccumulator::new(1, 2, 200);
        hydra.update(
            &KeyByLabelValues {
                labels: vec!["host-a".to_string(), "ep-1".to_string()],
            },
            42.0,
        );

        let mut keys = DeltaSetAggregatorAccumulator::new();
        keys.add_key(KeyByLabelValues {
            labels: vec!["host-b".to_string(), "ep-2".to_string()],
        });

        let engine = create_engine_dual_input(
            "request_duration",
            "HydraKllSketchAccumulator",
            "DeltaSetAggregator",
            vec![],                   // no store GROUP BY
            vec!["host", "endpoint"], // aggregated_labels
            vec![(None, Box::new(hydra))],
            vec![(None, Box::new(keys))],
            "quantile(0.5, request_duration) by (host, endpoint)",
        );

        let results = execute_new_plan(
            &engine,
            "quantile(0.5, request_duration) by (host, endpoint)",
            1000.0,
        )
        .await;

        // DeltaSet enumerates (host-b, ep-2) but HydraKLL has no data for that key.
        // We just verify it doesn't panic.
        let _ = results;
    }
}
