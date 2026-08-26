pub mod capability_matching_tests;
pub mod clickhouse_forwarding_tests;
pub mod dispatch_arithmetic_tests;
pub mod elastic_dsl_query_tests;
pub mod elastic_forwarding_tests;
pub mod native_binary_arithmetic_plan_tests;
pub mod native_binary_instant_tests;
pub mod native_pipeline_merge_tests;
pub mod native_range_query_tests;
pub mod prometheus_forwarding_tests;
pub mod query_equivalence_tests;
pub mod range_query_arithmetic_tests;
pub mod sql_pattern_matching_tests;
pub mod store_correctness_tests;
pub mod structural_matching_tests;
pub mod trait_design_tests;

#[cfg(test)]
pub mod test_utilities;
