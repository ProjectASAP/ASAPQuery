//! DataFusion execution path tests.
//!
//! Tests for the logical plan builder, physical execution operators,
//! and accumulator serialization that back the DataFusion-based query path.

pub mod accumulator_serde_tests;
pub mod dispatch_arithmetic_tests;
pub mod plan_builder_binary_tests;
pub mod plan_builder_regression_tests;
pub mod plan_execution_arithmetic_tests;
pub mod plan_execution_dual_input_tests;
pub mod plan_execution_temporal_tests;
pub mod plan_execution_tests;
pub mod structural_matching_tests;
