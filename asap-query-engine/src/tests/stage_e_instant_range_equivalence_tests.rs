//! Stage-E prep for #581 (unify PromQL instant/range fetch+merge paths):
//! equivalence tests between the instant pipeline (`handle_query_promql`)
//! and a single-step range pipeline (`handle_range_query_promql`), across
//! window-shape, dual-population, and keys-aggregator-kind axes. All of
//! stage E's blockers (B/#612, C/#596/#614, #608/#621) are closed on main as
//! of this writing, so every case here is expected to PASS -- there's no
//! known bug being pinned. A single-step range query and the equivalent
//! instant query at the same timestamp read and merge the exact same
//! underlying buckets, so they're compared for exact equality, not
//! approximate.
//!
//! Two corrections to the originally-scoped matrix, discovered while writing
//! these (both from reading `execute_range_query_pipeline` and its callers,
//! not from any doc):
//!
//! 1. Sliding lookbacks may now be positive integer multiples of
//!    `window_size_ms` (#554), composed from non-overlapping W-spaced stored
//!    windows. This older matrix retains an exact-width overlapping Sliding
//!    case; wider instant/range cases live in
//!    `window_semantics_consistency_tests`.
//!
//! 2. `SetAggregator`/`DeltaSetAggregator` are keys-side (dual-population)
//!    aggregation types in this codebase -- never a general value-aggregation
//!    choice (see every existing dual-population fixture in
//!    `native_range_query_tests.rs`). And `DeltaSetAggregator`'s
//!    Tumbling-only restriction (#588/#606) is about ITS OWN window_type,
//!    independent of whatever window_type the paired VALUE aggregation uses
//!    -- `RangeQueryExecutionContext::keys_window_type`'s doc comment says as
//!    much ("can legitimately differ from window_type"). So there's no
//!    Sliding-value-excludes-DeltaSetAgg-keys interaction to exclude: keys
//!    config is fixed at Tumbling for DeltaSetAgg regardless of the value
//!    shape under test, and every {value shape} x {no keys / SetAgg keys /
//!    DeltaSetAgg keys} x {instant / range} cell is constructible. 4 value
//!    shapes x 3 accumulator configs x 2 paths = 24 cases, no exclusions.

#[cfg(test)]
mod tests {
    use crate::data_model::{
        AggregationConfig, AggregationReference, AggregationType, CleanupPolicy, InferenceConfig,
        KeyByLabelValues, PrecomputedOutput, PromQLSchema, QueryConfig, QueryLanguage,
        SchemaConfig, StreamingConfig, WindowType,
    };
    use crate::engines::query_result::QueryResult;
    use crate::engines::simple_engine::SimpleEngine;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::precompute_operators::{
        CountMinSketchWithHeapAccumulator, DeltaSetAggregatorAccumulator, SetAggregatorAccumulator,
    };
    use crate::stores::simple_map_store::SimpleMapStore;
    use crate::stores::Store;
    use crate::AggregateCore;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// One value-side window shape under test. `query` is a complete
    /// `sum_over_time(cpu_load[Ns])` selector sized to exercise this shape's
    /// lookback-to-bucket-width ratio (Tumbling) or to satisfy the
    /// Sliding lookback==window_size_ms invariant.
    struct ValueShape {
        window_type: WindowType,
        window_size_ms: u64,
        slide_interval_ms: u64,
        query: &'static str,
        /// Output timestamp (seconds) both the instant query and the
        /// single-step range query are evaluated at.
        query_time_s: f64,
    }

    const VALUE_SHAPES: [ValueShape; 4] = [
        ValueShape {
            window_type: WindowType::Tumbling,
            window_size_ms: 1000,
            slide_interval_ms: 1000,
            query: "sum_over_time(cpu_load[1s])",
            query_time_s: 3.0,
        },
        ValueShape {
            window_type: WindowType::Tumbling,
            window_size_ms: 1000,
            slide_interval_ms: 1000,
            query: "sum_over_time(cpu_load[2s])",
            query_time_s: 3.0,
        },
        ValueShape {
            window_type: WindowType::Tumbling,
            window_size_ms: 1000,
            slide_interval_ms: 1000,
            query: "sum_over_time(cpu_load[3s])",
            query_time_s: 3.0,
        },
        ValueShape {
            window_type: WindowType::Sliding,
            window_size_ms: 2000,
            slide_interval_ms: 1000,
            query: "sum_over_time(cpu_load[2s])",
            query_time_s: 3.0,
        },
    ];

    /// Which keys/labels accumulator (if any) is paired with the value
    /// aggregation for a given matrix cell.
    enum KeysConfig {
        None,
        SetAgg,
        DeltaSetAgg,
    }

    /// Builds an engine with one value aggregation (id=1, Sum, shaped by
    /// `shape`) covering timestamps 1000/2000/3000 for group "host-a", and
    /// optionally a second keys aggregation (id=2) that resolves the same
    /// group's label key. `keys` is fixed at Tumbling, window_size_ms ==
    /// slide_interval_ms == 1000, with one bucket at [2000,3000) -- SetAgg's
    /// instant window is [end-window_size, end] so this always resolves at
    /// query_time_s=3.0 regardless of the value shape's own ratio; DeltaSetAgg's
    /// instant window is [0, end] so it finds this (and would find any
    /// earlier bucket) regardless too.
    fn build_engine(shape: &ValueShape, keys: KeysConfig) -> SimpleEngine {
        let grouping_labels = vec!["host".to_string()];
        let host_a = Some(KeyByLabelValues {
            labels: vec!["host-a".to_string()],
        });

        let mut aggregation_configs = HashMap::new();
        aggregation_configs.insert(
            1u64,
            AggregationConfig {
                aggregation_id: 1,
                aggregation_type: AggregationType::Sum,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::new(grouping_labels.clone()),
                aggregated_labels: KeyByLabelNames::empty(),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size_ms: shape.window_size_ms,
                slide_interval_ms: shape.slide_interval_ms,
                window_type: shape.window_type,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: "cpu_load".to_string(),
                num_aggregates_to_retain: None,
                read_count_threshold: None,
                table_name: None,
                value_column: None,
            },
        );

        let mut query_config = QueryConfig::new(shape.query.to_string())
            .add_aggregation(AggregationReference::new(1, None));

        if !matches!(keys, KeysConfig::None) {
            let key_agg_type = match keys {
                KeysConfig::SetAgg => AggregationType::SetAggregator,
                KeysConfig::DeltaSetAgg => AggregationType::DeltaSetAggregator,
                KeysConfig::None => unreachable!(),
            };
            let (key_window_size_ms, key_slide_interval_ms, key_window_type) = match keys {
                KeysConfig::SetAgg => (
                    shape.window_size_ms,
                    shape.slide_interval_ms,
                    shape.window_type,
                ),
                KeysConfig::DeltaSetAgg => (1000, 1000, WindowType::Tumbling),
                KeysConfig::None => unreachable!(),
            };
            aggregation_configs.insert(
                2u64,
                AggregationConfig {
                    aggregation_id: 2,
                    aggregation_type: key_agg_type,
                    aggregation_sub_type: String::new(),
                    parameters: HashMap::new(),
                    grouping_labels: KeyByLabelNames::new(grouping_labels.clone()),
                    aggregated_labels: KeyByLabelNames::new(vec!["host".to_string()]),
                    rollup_labels: KeyByLabelNames::empty(),
                    original_yaml: String::new(),
                    window_size_ms: key_window_size_ms,
                    slide_interval_ms: key_slide_interval_ms,
                    window_type: key_window_type,
                    spatial_filter: String::new(),
                    spatial_filter_normalized: String::new(),
                    metric: "cpu_load".to_string(),
                    num_aggregates_to_retain: None,
                    read_count_threshold: None,
                    table_name: None,
                    value_column: None,
                },
            );
            query_config = query_config.add_aggregation(AggregationReference::new(2, None));
        }

        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs,
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));

        // Value data: three consecutive 1000ms-wide panes. For Tumbling
        // shapes these ARE the stored buckets directly. For the Sliding
        // shape, worker.rs-style pre-merging isn't done here -- instead we
        // insert the two window_size_ms=2000-wide merged buckets a real
        // Sliding worker would have produced from these same panes
        // ([0,2000) from panes at 1000/2000, [1000,3000) from panes at
        // 2000/3000), matching `create_engine_multi_timestamp_with_window`'s
        // own merge logic but explicit here since this factory also needs a
        // second (keys) aggregation those single-aggregation factories don't
        // support.
        let value_buckets: Vec<(u64, u64, f64)> = match shape.window_type {
            WindowType::Tumbling => vec![(0, 1000, 1.0), (1000, 2000, 10.0), (2000, 3000, 100.0)],
            WindowType::Sliding => vec![(0, 2000, 11.0), (1000, 3000, 110.0)],
        };
        for (start, end, value) in value_buckets {
            let output = PrecomputedOutput::new(start, end, host_a.clone(), 1);
            store
                .insert_precomputed_output(output, Box::new(SumAccumulator::with_sum(value)))
                .unwrap();
        }

        if !matches!(keys, KeysConfig::None) {
            let acc: Box<dyn AggregateCore> = match keys {
                KeysConfig::SetAgg => {
                    let mut a = SetAggregatorAccumulator::new();
                    a.add_key(KeyByLabelValues {
                        labels: vec!["host-a".to_string()],
                    });
                    Box::new(a)
                }
                KeysConfig::DeltaSetAgg => {
                    let mut a = DeltaSetAggregatorAccumulator::new();
                    a.add_key(KeyByLabelValues {
                        labels: vec!["host-a".to_string()],
                    });
                    Box::new(a)
                }
                KeysConfig::None => unreachable!(),
            };
            let key_window_size_ms = match keys {
                KeysConfig::SetAgg => shape.window_size_ms,
                KeysConfig::DeltaSetAgg => 1000,
                KeysConfig::None => unreachable!(),
            };
            let output = PrecomputedOutput::new(3000 - key_window_size_ms, 3000, host_a.clone(), 2);
            store.insert_precomputed_output(output, acc).unwrap();
        }

        let promql_schema = PromQLSchema::new().add_metric(
            "cpu_load".to_string(),
            KeyByLabelNames::new(grouping_labels),
        );
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };

        SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            1000,
            QueryLanguage::promql,
        )
    }

    /// Runs `shape`'s query as both an instant query (at `shape.query_time_s`)
    /// and a single-step range query (start == end == `shape.query_time_s`),
    /// and asserts they produce the exact same (label, value) set. Panics
    /// with a diagnostic including `case_name` on mismatch -- this is a
    /// correctness check, not a fixture to adjust if it fails.
    fn assert_instant_range_equivalent(shape: &ValueShape, keys: KeysConfig, case_name: &str) {
        let engine = build_engine(shape, keys);

        let (_, instant_result) = engine
            .handle_query_promql(shape.query.to_string(), shape.query_time_s)
            .unwrap_or_else(|| panic!("{case_name}: instant query returned None"));
        let mut instant_pairs: Vec<(Vec<String>, f64)> = match instant_result {
            QueryResult::Vector(v) => v
                .values
                .into_iter()
                .map(|e| (e.labels.labels, e.value))
                .collect(),
            QueryResult::Matrix(_) => panic!("{case_name}: instant query returned a Matrix"),
        };

        // start must be strictly < end (validate_range_query_params); end is
        // set half a step past start so the per-step loop
        // (`while current_time <= end_ms`) still fires exactly once, at
        // current_time == start == shape.query_time_s -- the same instant
        // the instant query above ran at.
        let (_, range_result) = engine
            .handle_range_query_promql(
                shape.query.to_string(),
                shape.query_time_s,
                shape.query_time_s + 0.5,
                1.0,
            )
            .unwrap_or_else(|| panic!("{case_name}: range query returned None"));
        let mut range_pairs: Vec<(Vec<String>, f64)> = match range_result {
            QueryResult::Matrix(m) => m
                .values
                .into_iter()
                .map(|e| {
                    assert_eq!(
                        e.samples.len(),
                        1,
                        "{case_name}: single-step range query produced {} samples for {:?}, expected exactly 1",
                        e.samples.len(),
                        e.labels.labels
                    );
                    (e.labels.labels, e.samples[0].value)
                })
                .collect(),
            QueryResult::Vector(_) => panic!("{case_name}: range query returned a Vector"),
        };

        instant_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        range_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            instant_pairs, range_pairs,
            "{case_name}: instant and single-step range results diverge"
        );
    }

    macro_rules! equivalence_test {
        ($test_name:ident, $shape_index:expr, $keys:expr) => {
            #[tokio::test(flavor = "multi_thread")]
            async fn $test_name() {
                assert_instant_range_equivalent(
                    &VALUE_SHAPES[$shape_index],
                    $keys,
                    stringify!($test_name),
                );
            }
        };
    }

    // ── single accumulator (no keys_query) ──────────────────────────────
    equivalence_test!(equiv_single_tumbling_ratio1, 0, KeysConfig::None);
    equivalence_test!(equiv_single_tumbling_ratio2, 1, KeysConfig::None);
    equivalence_test!(equiv_single_tumbling_ratio3, 2, KeysConfig::None);
    equivalence_test!(equiv_single_sliding_overlapping, 3, KeysConfig::None);

    // ── dual accumulator, SetAggregator keys ────────────────────────────
    equivalence_test!(equiv_dual_setagg_tumbling_ratio1, 0, KeysConfig::SetAgg);
    equivalence_test!(equiv_dual_setagg_tumbling_ratio2, 1, KeysConfig::SetAgg);
    equivalence_test!(equiv_dual_setagg_tumbling_ratio3, 2, KeysConfig::SetAgg);
    equivalence_test!(equiv_dual_setagg_sliding_overlapping, 3, KeysConfig::SetAgg);

    // ── dual accumulator, DeltaSetAggregator keys ───────────────────────
    equivalence_test!(
        equiv_dual_deltasetagg_tumbling_ratio1,
        0,
        KeysConfig::DeltaSetAgg
    );
    equivalence_test!(
        equiv_dual_deltasetagg_tumbling_ratio2,
        1,
        KeysConfig::DeltaSetAgg
    );
    equivalence_test!(
        equiv_dual_deltasetagg_tumbling_ratio3,
        2,
        KeysConfig::DeltaSetAgg
    );
    equivalence_test!(
        equiv_dual_deltasetagg_sliding_overlapping,
        3,
        KeysConfig::DeltaSetAgg
    );

    // ════════════════════════════════════════════════════════════════════
    // ── Computed top-k in range queries (new plumbing, this PR) ─────────
    // ════════════════════════════════════════════════════════════════════
    //
    // IMPORTANT, discovered while writing these: `Statistic::Topk` as a
    // per-group value query (`AggregateCore::query_statistic`) is only
    // answerable by accumulator types that special-case or ignore it --
    // `CountMinSketchWithHeapAccumulator::query` ignores the `_statistic`
    // argument entirely and always returns `query_key`.
    // `SumAccumulator::query` strictly matches on `Statistic::Sum | Count`
    // and returns `Err("Unsupported statistic")` for anything else,
    // including Topk. An earlier version of these tests tried a per-group
    // plain `SumAccumulator` (`get_keys() == None`, to force resolution
    // through the fallback_key path rather than self-keyed expansion) to
    // isolate "ranking/truncation across independently-resolved groups"
    // from "self-keyed expansion within one group" -- both instant AND
    // range returned zero results, identically, because the per-group value
    // query fails before ranking ever runs. This is pre-existing on `main`,
    // unrelated to this PR's range changes, and it means "topk over an
    // arbitrary non-self-keyed expression" (e.g. `topk(5, rate(foo[5m]))`)
    // is not a supported query shape ANYWHERE in this codebase today, not
    // just missing in range -- the scope of "topk in range" that's
    // actually constructible is narrower than originally framed.
    //
    // So these tests use the one accumulator shape that both (a) actually
    // answers Topk queries and (b) is how this codebase's own working topk
    // fixture (`build_topk_engine` in promql.rs, `topk_pipeline_tests`) is
    // built: one ungrouped `CountMinSketchWithHeapAccumulator` per output
    // timestamp. `range_query_self_keyed_topk_expands_*`
    // (native_range_query_tests.rs, #595) already covers "expansion
    // resolves correctly within one step" -- these add the genuinely new
    // case #595 didn't: MULTIPLE steps, where the top-k set changes from
    // step to step (step-major ranking, this PR's new plumbing) and,
    // separately, that a range query now truncates to k at all (before
    // this PR, `execute_range_query_pipeline` had no truncation step, so
    // every self-keyed candidate below `heap_size` would have come back
    // untruncated).

    /// (bucket_start_ms, bucket_end_ms, [(host, value), ...]).
    type TopkBucket<'a> = (u64, u64, &'a [(&'a str, f64)]);

    fn build_range_topk_engine(query: &str, buckets: &[TopkBucket]) -> SimpleEngine {
        let mut aggregation_configs = HashMap::new();
        aggregation_configs.insert(
            1u64,
            AggregationConfig {
                aggregation_id: 1,
                aggregation_type: AggregationType::CountMinSketchWithHeap,
                aggregation_sub_type: String::new(),
                parameters: HashMap::new(),
                grouping_labels: KeyByLabelNames::empty(),
                aggregated_labels: KeyByLabelNames::new(vec!["host".to_string()]),
                rollup_labels: KeyByLabelNames::empty(),
                original_yaml: String::new(),
                window_size_ms: 1000,
                slide_interval_ms: 1000,
                window_type: WindowType::Tumbling,
                spatial_filter: String::new(),
                spatial_filter_normalized: String::new(),
                metric: "cpu_load".to_string(),
                num_aggregates_to_retain: None,
                read_count_threshold: None,
                table_name: None,
                value_column: None,
            },
        );
        let streaming_config = Arc::new(StreamingConfig {
            aggregation_configs,
        });
        let store = Arc::new(SimpleMapStore::new(
            streaming_config.clone(),
            CleanupPolicy::NoCleanup,
        ));
        for (start, end, candidates) in buckets {
            let mut sketch = CountMinSketchWithHeapAccumulator::new(3, 1024, 32);
            for (host, value) in *candidates {
                sketch.inner.update(host, *value);
            }
            let output = PrecomputedOutput::new(*start, *end, None, 1);
            store
                .insert_precomputed_output(output, Box::new(sketch))
                .unwrap();
        }
        let promql_schema = PromQLSchema::new().add_metric(
            "cpu_load".to_string(),
            KeyByLabelNames::new(vec!["host".to_string()]),
        );
        let query_config =
            QueryConfig::new(query.to_string()).add_aggregation(AggregationReference::new(1, None));
        let inference_config = InferenceConfig {
            schema: SchemaConfig::PromQL(promql_schema),
            query_configs: vec![query_config],
            cleanup_policy: CleanupPolicy::NoCleanup,
        };
        SimpleEngine::new(
            store,
            inference_config,
            streaming_config,
            1000,
            QueryLanguage::promql,
        )
    }

    fn topk_samples_for(
        elements: &[crate::engines::query_result::RangeVectorElement],
        host: &str,
    ) -> Vec<(u64, f64)> {
        elements
            .iter()
            .find(|e| e.labels.labels.contains(&host.to_string()))
            .map(|e| e.samples.iter().map(|s| (s.timestamp, s.value)).collect())
            .unwrap_or_default()
    }

    /// The ranked-and-truncated set of surviving hosts must differ from
    /// step to step, ranked independently at each timestamp (step-major) --
    /// NOT a single global ranking applied to every step (group-major would
    /// wrongly keep/drop the same hosts at every timestamp regardless of
    /// that timestamp's actual values). Before this PR, range had no
    /// truncation at all, so this would have returned all 3 hosts at both
    /// steps.
    ///
    /// t=1000: host-a=100, host-b=50, host-c=10 -> top2 = {a, b}, c dropped.
    /// t=2000: host-a=5,   host-b=50, host-c=100 -> top2 = {b, c}, a dropped.
    #[tokio::test(flavor = "multi_thread")]
    async fn topk_range_step_major_ranking_differs_per_step() {
        let query = "topk(2, cpu_load)";
        let engine = build_range_topk_engine(
            query,
            &[
                (
                    0,
                    1000,
                    &[("host-a", 100.0), ("host-b", 50.0), ("host-c", 10.0)],
                ),
                (
                    1000,
                    2000,
                    &[("host-a", 5.0), ("host-b", 50.0), ("host-c", 100.0)],
                ),
            ],
        );

        let (_, result) = engine
            .handle_range_query_promql(query.to_string(), 1.0, 2.5, 1.0)
            .expect("range topk query failed");
        let elements = match result {
            QueryResult::Matrix(m) => m.values,
            QueryResult::Vector(_) => panic!("expected a Matrix"),
        };

        assert_eq!(
            topk_samples_for(&elements, "host-a"),
            vec![(1000, 100.0)],
            "host-a should survive only t=1000's top-2 (100 > 50,10), not t=2000's (5 is last of 3)"
        );
        assert_eq!(
            topk_samples_for(&elements, "host-b"),
            vec![(1000, 50.0), (2000, 50.0)],
            "host-b (50) is top-2 at both steps (t=1000: 100,50 beat 10; t=2000: 100,50 beat 5)"
        );
        assert_eq!(
            topk_samples_for(&elements, "host-c"),
            vec![(2000, 100.0)],
            "host-c should survive only t=2000's top-2 (100 > 50,5), not t=1000's (10 is last of 3)"
        );
    }

    /// A single-step range topk query must match the equivalent instant
    /// topk query exactly -- both gated identically on
    /// `Statistic::Topk` + `query_kwargs["k"]`, and both now truncate.
    #[tokio::test(flavor = "multi_thread")]
    async fn topk_range_single_step_matches_instant() {
        let query = "topk(2, cpu_load)";
        let engine = build_range_topk_engine(
            query,
            &[(
                0,
                1000,
                &[("host-a", 100.0), ("host-b", 50.0), ("host-c", 10.0)],
            )],
        );

        let (_, instant_result) = engine
            .handle_query_promql(query.to_string(), 1.0)
            .expect("instant topk query failed");
        let mut instant_pairs: Vec<(Vec<String>, f64)> = match instant_result {
            QueryResult::Vector(v) => v
                .values
                .into_iter()
                .map(|e| (e.labels.labels, e.value))
                .collect(),
            QueryResult::Matrix(_) => panic!("expected a Vector"),
        };

        let (_, range_result) = engine
            .handle_range_query_promql(query.to_string(), 1.0, 1.5, 1.0)
            .expect("range topk query failed");
        let mut range_pairs: Vec<(Vec<String>, f64)> = match range_result {
            QueryResult::Matrix(m) => m
                .values
                .into_iter()
                .map(|e| {
                    assert_eq!(e.samples.len(), 1);
                    (e.labels.labels, e.samples[0].value)
                })
                .collect(),
            QueryResult::Vector(_) => panic!("expected a Matrix"),
        };

        assert_eq!(
            instant_pairs.len(),
            2,
            "topk(2, ...) must truncate to 2 results"
        );
        instant_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        range_pairs.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(instant_pairs, range_pairs);
    }

    /// The new range plumbing must not interfere with the existing
    /// self-keyed range topk path (#595): a single-step range query over a
    /// dual-population self-keyed group, exercised the same way
    /// `native_range_query_tests.rs`'s `range_query_self_keyed_topk_expands_*`
    /// tests do, must still resolve and expand correctly.
    #[tokio::test(flavor = "multi_thread")]
    async fn topk_range_does_not_break_existing_self_keyed_expansion() {
        let query = "topk(5, cpu_load)";
        let engine =
            build_range_topk_engine(query, &[(0, 1000, &[("host-a", 30.0), ("host-b", 20.0)])]);

        let (_, result) = engine
            .handle_range_query_promql(query.to_string(), 1.0, 1.5, 1.0)
            .expect("range topk query failed");
        let elements = match result {
            QueryResult::Matrix(m) => m.values,
            QueryResult::Vector(_) => panic!("expected a Matrix"),
        };

        assert_eq!(
            topk_samples_for(&elements, "host-a"),
            vec![(1000, 30.0)],
            "self-keyed expansion (#595) must still resolve host-a"
        );
        assert_eq!(
            topk_samples_for(&elements, "host-b"),
            vec![(1000, 20.0)],
            "self-keyed expansion (#595) must still resolve host-b"
        );
    }

    // Regression test for the deterministic tie-break in `apply_range_topk`.
    // Candidates are ordered by value descending, then label values ascending,
    // so a tie at the k-th boundary is stable despite HashMap iteration order.
    #[tokio::test(flavor = "multi_thread")]
    async fn topk_range_tie_break_is_deterministic_by_label_values() {
        let query = "topk(2, cpu_load)";
        // host-a is the clear #1. host-b and host-c are tied at 50.0 for the
        // single remaining top-2 slot -- "host-b" < "host-c" lexicographically,
        // so host-b should always win the tie once the sort is deterministic.
        let engine = build_range_topk_engine(
            query,
            &[(
                0,
                1000,
                &[("host-a", 100.0), ("host-b", 50.0), ("host-c", 50.0)],
            )],
        );

        let (_, result) = engine
            .handle_range_query_promql(query.to_string(), 1.0, 1.5, 1.0)
            .expect("range topk query failed");
        let elements = match result {
            QueryResult::Matrix(m) => m.values,
            QueryResult::Vector(_) => panic!("expected a Matrix"),
        };

        assert_eq!(
            topk_samples_for(&elements, "host-a"),
            vec![(1000, 100.0)],
            "host-a is the clear #1, always kept"
        );
        assert_eq!(
            topk_samples_for(&elements, "host-b"),
            vec![(1000, 50.0)],
            "host-b should deterministically win the tie against host-c (\"host-b\" < \"host-c\")"
        );
        assert_eq!(
            topk_samples_for(&elements, "host-c"),
            vec![],
            "host-c should deterministically lose the tie against host-b"
        );
    }
}
