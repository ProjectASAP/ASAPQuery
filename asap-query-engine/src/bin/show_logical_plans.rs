//! Standalone binary that constructs diverse QueryExecutionContext structures,
//! converts each to a DataFusion logical plan, and prints each plan along with
//! the schema of every edge and key internal variables.
//!
//! Covers 4 queries x multiple accumulator configurations = 10 test cases:
//!
//! 1. sum by (host) (data)              — spatial sum
//! 2. quantile by (host) (0.5, data)    — spatial quantile
//! 3. sum_over_time(data[1m])           — temporal sum
//! 4. quantile_over_time(0.5, data[1m]) — temporal quantile
//!
//! data has columns: host, service, region

use datafusion::logical_expr::LogicalPlan;
use datafusion_summary_library::{PrecomputedSummaryRead, SummaryInfer, SummaryMergeMultiple};
use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use query_engine_rust::engines::simple_engine::{
    AggregationIdInfo, QueryExecutionContext, QueryMetadata, StoreQueryParams, StoreQueryPlan,
};
use std::collections::HashMap;

// ============================================================================
// Context builders
// ============================================================================

/// Build a QueryExecutionContext with full control over all parameters.
#[allow(clippy::too_many_arguments)]
fn build_context(
    metric: &str,
    statistic: Statistic,
    query_output_labels: Vec<&str>,
    grouping_labels: Vec<&str>,
    aggregated_labels: Vec<&str>,
    agg_type_value: &str,
    agg_type_key: &str,
    agg_id_value: u64,
    agg_id_key: u64,
    keys_query: Option<StoreQueryParams>,
    do_merge: bool,
    is_exact_query: bool,
    kwargs: HashMap<String, String>,
) -> QueryExecutionContext {
    QueryExecutionContext {
        metric: metric.to_string(),
        metadata: QueryMetadata {
            query_output_labels: KeyByLabelNames {
                labels: query_output_labels.into_iter().map(String::from).collect(),
            },
            statistic_to_compute: statistic,
            query_kwargs: kwargs,
        },
        store_plan: StoreQueryPlan {
            values_query: StoreQueryParams {
                metric: metric.to_string(),
                aggregation_id: agg_id_value,
                start_timestamp: if do_merge { 1000 } else { 2000 },
                end_timestamp: 2000,
                is_exact_query,
            },
            keys_query,
        },
        agg_info: AggregationIdInfo {
            aggregation_id_for_key: agg_id_key,
            aggregation_id_for_value: agg_id_value,
            aggregation_type_for_key: agg_type_key.to_string(),
            aggregation_type_for_value: agg_type_value.to_string(),
        },
        do_merge,
        spatial_filter: String::new(),
        query_time: 2000,
        grouping_labels: KeyByLabelNames {
            labels: grouping_labels.into_iter().map(String::from).collect(),
        },
        aggregated_labels: KeyByLabelNames {
            labels: aggregated_labels.into_iter().map(String::from).collect(),
        },
    }
}

fn make_keys_query(metric: &str, agg_id: u64) -> StoreQueryParams {
    StoreQueryParams {
        metric: metric.to_string(),
        aggregation_id: agg_id,
        start_timestamp: 0, // DeltaSetAggregator reads from beginning of time
        end_timestamp: 2000,
        is_exact_query: false, // keys are always range queries
    }
}

// ============================================================================
// Plan printing utilities
// ============================================================================

/// Recursively print the plan tree with indentation, showing each node's
/// explain text and output schema.
fn print_plan_tree(plan: &LogicalPlan, indent: usize) {
    let prefix = "  ".repeat(indent);
    let connector = if indent > 0 { "└─► " } else { "" };

    match plan {
        LogicalPlan::Extension(ext) => {
            // Print node name and explain text
            println!("{prefix}{connector}{}", ext.node.name());

            // Print detailed properties by downcasting each node
            print_node_details(plan, indent + 2);

            // Print output schema
            let schema = ext.node.schema();
            print!("{}    schema: [", prefix);
            for (i, field) in schema.fields().iter().enumerate() {
                if i > 0 {
                    print!(", ");
                }
                print!("{}:{}", field.name(), field.data_type());
            }
            println!("]");

            // Recurse into inputs
            let inputs = ext.node.inputs();
            for (i, input) in inputs.iter().enumerate() {
                if inputs.len() > 1 {
                    println!("{}    input {}:", prefix, i);
                }
                print_plan_tree(input, indent + 2);
            }
        }
        _ => {
            println!("{prefix}{connector}Unknown: {:?}", plan);
        }
    }
}

/// Print detailed properties of a plan node by downcasting.
fn print_node_details(plan: &LogicalPlan, indent: usize) {
    let prefix = "  ".repeat(indent);
    if let LogicalPlan::Extension(ext) = plan {
        if let Some(infer) = ext.node.as_any().downcast_ref::<SummaryInfer>() {
            println!(
                "{prefix}operations: {:?}",
                infer
                    .operations
                    .iter()
                    .map(|op| format!("{}", op))
                    .collect::<Vec<_>>()
            );
            println!("{prefix}output_names: {:?}", infer.output_names);
            println!("{prefix}group_key_columns: {:?}", infer.group_key_columns);
            println!("{prefix}has_keys_input: {}", infer.keys_input.is_some());
        } else if let Some(merge) = ext.node.as_any().downcast_ref::<SummaryMergeMultiple>() {
            println!("{prefix}group_by: {:?}", merge.group_by());
            println!("{prefix}sketch_column: {:?}", merge.sketch_column());
            println!("{prefix}summary_type: {}", merge.summary_type());
        } else if let Some(read) = ext.node.as_any().downcast_ref::<PrecomputedSummaryRead>() {
            println!("{prefix}metric: {:?}", read.metric());
            println!("{prefix}aggregation_id: {}", read.aggregation_id());
            println!(
                "{prefix}range: [{}, {}]",
                read.start_timestamp(),
                read.end_timestamp()
            );
            println!("{prefix}is_exact_query: {}", read.is_exact_query());
            println!("{prefix}summary_type: {}", read.summary_type());
            println!("{prefix}output_labels: {:?}", read.output_labels());
        }
    }
}

/// Print key internal variables about a QueryExecutionContext.
fn print_context_variables(ctx: &QueryExecutionContext) {
    let has_separate_keys = ctx.store_plan.keys_query.is_some()
        && ctx.agg_info.aggregation_id_for_key != ctx.agg_info.aggregation_id_for_value;
    let has_aggregated_labels = !ctx.aggregated_labels.labels.is_empty();

    println!("  Internal variables:");
    println!("    has_separate_keys (dual input): {}", has_separate_keys);
    println!(
        "    has_aggregated_labels (multi-population): {}",
        has_aggregated_labels
    );
    println!("    do_merge (temporal): {}", ctx.do_merge);
    println!(
        "    keys_included: {}",
        has_separate_keys || has_aggregated_labels
    );
    println!(
        "    value_agg: {} (id={})",
        ctx.agg_info.aggregation_type_for_value, ctx.agg_info.aggregation_id_for_value
    );
    println!(
        "    key_agg: {} (id={})",
        ctx.agg_info.aggregation_type_for_key, ctx.agg_info.aggregation_id_for_key
    );
    println!(
        "    query_output_labels: {:?}",
        ctx.metadata.query_output_labels.labels
    );
    println!("    grouping_labels: {:?}", ctx.grouping_labels.labels);
    println!("    aggregated_labels: {:?}", ctx.aggregated_labels.labels);
    println!("    statistic: {:?}", ctx.metadata.statistic_to_compute);
    if !ctx.metadata.query_kwargs.is_empty() {
        println!("    query_kwargs: {:?}", ctx.metadata.query_kwargs);
    }
}

// ============================================================================
// Test case definitions
// ============================================================================

struct TestCase {
    title: String,
    query: String,
    description: String,
    context: QueryExecutionContext,
}

fn build_all_test_cases() -> Vec<TestCase> {
    let metric = "data";
    let mut cases = Vec::new();

    // ========================================================================
    // Query 1: sum by (host) (data)
    // ========================================================================

    // Case 1a: SumAccumulator only
    // Simple single-population. Store groups by host, one Sum per host.
    cases.push(TestCase {
        title: "sum by (host) — SumAccumulator".into(),
        query: "sum by (host) (data)".into(),
        description:
            "Single-population exact sum. Store groups by [host], one scalar sum per group key."
                .into(),
        context: build_context(
            metric,
            Statistic::Sum,
            vec!["host"],     // query_output_labels
            vec!["host"],     // grouping_labels (store GROUP BY)
            vec![],           // aggregated_labels (none)
            "SumAccumulator", // value accumulator
            "SumAccumulator", // key accumulator (same = single)
            42,
            42,    // same agg_id
            None,  // no keys_query
            false, // not temporal
            true,  // exact (sliding window)
            HashMap::new(),
        ),
    });

    // Case 1b: MultipleSumAccumulator only (self-keyed)
    // The accumulator internally tracks sums for each host value.
    // Store doesn't group by host; the accumulator maps host -> sum.
    cases.push(TestCase {
        title: "sum by (host) — MultipleSumAccumulator (self-keyed)".into(),
        query: "sum by (host) (data)".into(),
        description: "Self-keyed multi-population. Store groups by [] (no spatial grouping). \
                       MultipleSumAccumulator internally maps host -> sum."
            .into(),
        context: build_context(
            metric,
            Statistic::Sum,
            vec!["host"], // query_output_labels
            vec![],       // grouping_labels (no store grouping)
            vec!["host"], // aggregated_labels (host tracked internally)
            "MultipleSumAccumulator",
            "MultipleSumAccumulator", // same type = single agg_id
            42,
            42,
            None,
            false,
            true,
            HashMap::new(),
        ),
    });

    // Case 1c: CountMinSketch + DeltaSetAggregator (dual-input)
    // CountMinSketch estimates frequency per key; DeltaSetAggregator enumerates keys.
    cases.push(TestCase {
        title: "sum by (host) — CountMinSketch + DeltaSetAggregator (dual-input)".into(),
        query: "sum by (host) (data)".into(),
        description: "Dual-input plan. CountMinSketch for value estimation per host key, \
                       DeltaSetAggregator enumerates which hosts exist."
            .into(),
        context: build_context(
            metric,
            Statistic::Sum,
            vec!["host"],
            vec![],       // grouping_labels (no store grouping)
            vec!["host"], // aggregated_labels
            "CountMinSketch",
            "DeltaSetAggregator",
            42,
            99, // different agg_ids
            Some(make_keys_query(metric, 99)),
            false,
            true,
            HashMap::new(),
        ),
    });

    // ========================================================================
    // Query 2: quantile by (host) (0.5, data)
    // ========================================================================

    let mut q_kwargs = HashMap::new();
    q_kwargs.insert("quantile".to_string(), "0.5".to_string());

    // Case 2a: KLL only
    cases.push(TestCase {
        title: "quantile by (host) (0.5) — KLL".into(),
        query: "quantile by (host) (0.5, data)".into(),
        description:
            "Single-population quantile. Store groups by [host], one KLL sketch per group key."
                .into(),
        context: build_context(
            metric,
            Statistic::Quantile,
            vec!["host"],
            vec!["host"],
            vec![],
            "KLL",
            "KLL",
            42,
            42,
            None,
            false,
            true,
            q_kwargs.clone(),
        ),
    });

    // Case 2b: HydraKLL + DeltaSetAggregator (dual-input)
    cases.push(TestCase {
        title: "quantile by (host) (0.5) — HydraKLL + DeltaSetAggregator (dual-input)".into(),
        query: "quantile by (host) (0.5, data)".into(),
        description: "Dual-input quantile. HydraKLL has per-host KLL sketches internally. \
                       DeltaSetAggregator enumerates which hosts exist."
            .into(),
        context: build_context(
            metric,
            Statistic::Quantile,
            vec!["host"],
            vec![],       // no store grouping
            vec!["host"], // host tracked internally
            "HydraKLL",
            "DeltaSetAggregator",
            42,
            99,
            Some(make_keys_query(metric, 99)),
            false,
            true,
            q_kwargs.clone(),
        ),
    });

    // ========================================================================
    // Query 3: sum_over_time(data[1m])
    // Temporal — all labels preserved, do_merge=true
    // ========================================================================

    // Case 3a: SumAccumulator only
    cases.push(TestCase {
        title: "sum_over_time(data[1m]) — SumAccumulator".into(),
        query: "sum_over_time(data[1m])".into(),
        description: "Temporal sum, single-population. All labels preserved. \
                       do_merge=true to merge tumbling windows across the 1m range."
            .into(),
        context: build_context(
            metric,
            Statistic::Sum,
            vec!["host", "service", "region"],
            vec!["host", "service", "region"],
            vec![],
            "SumAccumulator",
            "SumAccumulator",
            42,
            42,
            None,
            true,  // temporal merge
            false, // tumbling window (range query)
            HashMap::new(),
        ),
    });

    // Case 3b: MultipleSumAccumulator only (self-keyed)
    cases.push(TestCase {
        title: "sum_over_time(data[1m]) — MultipleSumAccumulator (self-keyed)".into(),
        query: "sum_over_time(data[1m])".into(),
        description: "Temporal sum, self-keyed multi-population. Store groups by [host]. \
                       MultipleSumAccumulator internally maps (service, region) -> sum."
            .into(),
        context: build_context(
            metric,
            Statistic::Sum,
            vec!["host", "service", "region"],
            vec!["host"],              // store groups by host only
            vec!["service", "region"], // rest tracked internally
            "MultipleSumAccumulator",
            "MultipleSumAccumulator",
            42,
            42,
            None,
            true,
            false,
            HashMap::new(),
        ),
    });

    // Case 3c: CountMinSketch + DeltaSetAggregator (dual-input)
    cases.push(TestCase {
        title: "sum_over_time(data[1m]) — CountMinSketch + DeltaSetAggregator (dual-input)".into(),
        query: "sum_over_time(data[1m])".into(),
        description: "Temporal sum, dual-input. Store groups by [host]. \
                       CountMinSketch estimates per (service, region). \
                       DeltaSetAggregator enumerates (service, region) keys."
            .into(),
        context: build_context(
            metric,
            Statistic::Sum,
            vec!["host", "service", "region"],
            vec!["host"],
            vec!["service", "region"],
            "CountMinSketch",
            "DeltaSetAggregator",
            42,
            99,
            Some(make_keys_query(metric, 99)),
            true,
            false,
            HashMap::new(),
        ),
    });

    // ========================================================================
    // Query 4: quantile_over_time(0.5, data[1m])
    // Temporal — all labels preserved, do_merge=true
    // ========================================================================

    // Case 4a: KLL only
    cases.push(TestCase {
        title: "quantile_over_time(0.5, data[1m]) — KLL".into(),
        query: "quantile_over_time(0.5, data[1m])".into(),
        description: "Temporal quantile, single-population. All labels preserved. \
                       One KLL sketch per (host, service, region) group."
            .into(),
        context: build_context(
            metric,
            Statistic::Quantile,
            vec!["host", "service", "region"],
            vec!["host", "service", "region"],
            vec![],
            "KLL",
            "KLL",
            42,
            42,
            None,
            true,
            false,
            q_kwargs.clone(),
        ),
    });

    // Case 4b: HydraKLL + DeltaSetAggregator (dual-input)
    cases.push(TestCase {
        title: "quantile_over_time(0.5, data[1m]) — HydraKLL + DeltaSetAggregator (dual-input)"
            .into(),
        query: "quantile_over_time(0.5, data[1m])".into(),
        description: "Temporal quantile, dual-input. Store groups by [host]. \
                       HydraKLL has per-(service, region) KLL sketches. \
                       DeltaSetAggregator enumerates (service, region) keys."
            .into(),
        context: build_context(
            metric,
            Statistic::Quantile,
            vec!["host", "service", "region"],
            vec!["host"],
            vec!["service", "region"],
            "HydraKLL",
            "DeltaSetAggregator",
            42,
            99,
            Some(make_keys_query(metric, 99)),
            true,
            false,
            q_kwargs.clone(),
        ),
    });

    cases
}

// ============================================================================
// Main
// ============================================================================

fn main() {
    let cases = build_all_test_cases();

    for (i, case) in cases.iter().enumerate() {
        println!("╔══════════════════════════════════════════════════════════════════════");
        println!("║ Case {}: {}", i + 1, case.title);
        println!("║ Query: {}", case.query);
        println!("║ {}", case.description);
        println!("╚══════════════════════════════════════════════════════════════════════");
        println!();

        // Print key internal variables
        print_context_variables(&case.context);
        println!();

        // Convert to logical plan
        match case.context.to_logical_plan() {
            Ok(plan) => {
                println!("  Logical Plan Tree:");
                println!("  ──────────────────");
                print_plan_tree(&plan, 2);
            }
            Err(e) => {
                println!("  ERROR converting to logical plan: {}", e);
            }
        }

        println!();
        println!();
    }
}
