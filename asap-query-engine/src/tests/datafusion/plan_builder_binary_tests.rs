//! Binary plan builder tests.
//!
//! Tests that `build_binary_vector_plan` and `build_scalar_plan` produce correct
//! DataFusion logical plan structures.

#[cfg(test)]
mod tests {
    use crate::engines::logical::plan_builder::{build_binary_vector_plan, build_scalar_plan};
    use crate::engines::simple_engine::{
        AggregationIdInfo, QueryExecutionContext, QueryMetadata, StoreQueryParams, StoreQueryPlan,
    };
    use datafusion::logical_expr::LogicalPlan;
    use promql_parser::parser::token::{TokenType, T_ADD, T_DIV, T_MOD, T_MUL, T_POW, T_SUB};
    use promql_utilities::data_model::KeyByLabelNames;
    use promql_utilities::query_logics::enums::Statistic;
    use std::collections::HashMap;

    fn make_context(
        metric: &str,
        statistic: Statistic,
        labels: Vec<&str>,
    ) -> QueryExecutionContext {
        let label_strings: Vec<String> = labels.into_iter().map(String::from).collect();
        QueryExecutionContext {
            metric: metric.to_string(),
            metadata: QueryMetadata {
                query_output_labels: KeyByLabelNames::new(label_strings.clone()),
                statistic_to_compute: statistic,
                query_kwargs: HashMap::new(),
            },
            store_plan: StoreQueryPlan {
                values_query: StoreQueryParams {
                    metric: metric.to_string(),
                    aggregation_id: 1,
                    start_timestamp: 1000,
                    end_timestamp: 2000,
                    is_exact_query: true,
                },
                keys_query: None,
            },
            agg_info: AggregationIdInfo {
                aggregation_id_for_key: 1,
                aggregation_id_for_value: 1,
                aggregation_type_for_key: "SumAggregator".to_string(),
                aggregation_type_for_value: "SumAggregator".to_string(),
            },
            do_merge: false,
            spatial_filter: String::new(),
            query_time: 2000,
            grouping_labels: KeyByLabelNames::new(label_strings.clone()),
            aggregated_labels: KeyByLabelNames::empty(),
        }
    }

    fn collect_node_names(plan: &LogicalPlan) -> Vec<String> {
        let mut names = Vec::new();
        collect_recursive(plan, &mut names);
        names
    }

    fn collect_recursive(plan: &LogicalPlan, names: &mut Vec<String>) {
        match plan {
            LogicalPlan::Extension(ext) => {
                names.push(ext.node.name().to_string());
                for input in ext.node.inputs() {
                    collect_recursive(input, names);
                }
            }
            LogicalPlan::Projection(p) => {
                names.push("Projection".to_string());
                collect_recursive(&p.input, names);
            }
            LogicalPlan::Join(j) => {
                names.push("Join".to_string());
                collect_recursive(&j.left, names);
                collect_recursive(&j.right, names);
            }
            LogicalPlan::SubqueryAlias(a) => {
                names.push("SubqueryAlias".to_string());
                collect_recursive(&a.input, names);
            }
            other => {
                names.push(
                    format!("{:?}", other)
                        .split('(')
                        .next()
                        .unwrap_or("Unknown")
                        .to_string(),
                );
            }
        }
    }

    fn contains_node(plan: &LogicalPlan, name: &str) -> bool {
        collect_node_names(plan).iter().any(|n| n == name)
    }

    #[test]
    fn test_binary_vector_plan_structure_divide() {
        let lhs_ctx = make_context("errors", Statistic::Sum, vec!["host"]);
        let rhs_ctx = make_context("requests", Statistic::Sum, vec!["host"]);
        let lhs_plan = lhs_ctx.to_logical_plan().unwrap();
        let rhs_plan = rhs_ctx.to_logical_plan().unwrap();

        let op = TokenType::new(T_DIV);
        let plan =
            build_binary_vector_plan(lhs_plan, rhs_plan, &op, vec!["host".to_string()]).unwrap();

        let names = collect_node_names(&plan);
        assert_eq!(names[0], "Projection", "Root should be Projection");
        assert!(contains_node(&plan, "Join"), "Plan should contain a Join");
        let alias_count = names.iter().filter(|n| *n == "SubqueryAlias").count();
        assert_eq!(
            alias_count, 2,
            "Plan should contain two SubqueryAlias nodes"
        );
    }

    #[test]
    fn test_binary_vector_plan_all_operators() {
        let ops = [T_ADD, T_SUB, T_MUL, T_DIV, T_POW, T_MOD];
        for op_id in ops {
            let lhs_ctx = make_context("metric_a", Statistic::Sum, vec!["host"]);
            let rhs_ctx = make_context("metric_b", Statistic::Sum, vec!["host"]);
            let lhs_plan = lhs_ctx.to_logical_plan().unwrap();
            let rhs_plan = rhs_ctx.to_logical_plan().unwrap();

            let op = TokenType::new(op_id);
            let result =
                build_binary_vector_plan(lhs_plan, rhs_plan, &op, vec!["host".to_string()]);
            assert!(
                result.is_ok(),
                "Operator {:?} should produce a valid plan",
                op
            );
            let names = collect_node_names(&result.unwrap());
            assert_eq!(names[0], "Projection");
        }
    }

    #[test]
    fn test_scalar_right_plan_structure() {
        let ctx = make_context("errors", Statistic::Sum, vec!["host"]);
        let vector_plan = ctx.to_logical_plan().unwrap();

        let op = TokenType::new(T_MUL);
        let plan =
            build_scalar_plan(vector_plan, 100.0, &op, false, vec!["host".to_string()]).unwrap();

        let names = collect_node_names(&plan);
        assert_eq!(names[0], "Projection", "Root should be Projection");
        assert!(
            !contains_node(&plan, "Join"),
            "Scalar plan should not have a Join"
        );
        assert!(
            !contains_node(&plan, "SubqueryAlias"),
            "Scalar plan should not have SubqueryAlias"
        );
    }

    #[test]
    fn test_scalar_left_plan_structure() {
        let ctx = make_context("success", Statistic::Sum, vec!["host"]);
        let vector_plan = ctx.to_logical_plan().unwrap();

        let op = TokenType::new(T_SUB);
        let plan =
            build_scalar_plan(vector_plan, 1.0, &op, true, vec!["host".to_string()]).unwrap();

        let names = collect_node_names(&plan);
        assert_eq!(names[0], "Projection");
        assert!(!contains_node(&plan, "Join"));
    }

    #[test]
    fn test_scalar_left_division_plan_structure() {
        // 1.0 / rate(metric[5m]) — scalar on left with Div
        let ctx = make_context("metric", Statistic::Sum, vec!["host"]);
        let vector_plan = ctx.to_logical_plan().unwrap();

        let op = TokenType::new(T_DIV);
        let result = build_scalar_plan(vector_plan, 1.0, &op, true, vec!["host".to_string()]);
        assert!(
            result.is_ok(),
            "scalar-left division plan should build without error"
        );
    }
}
