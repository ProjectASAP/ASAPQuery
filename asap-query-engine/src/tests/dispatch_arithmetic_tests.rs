//! Dispatch-level tests for binary arithmetic PromQL handling.
//!
//! Tests that `handle_query_promql` routes binary expressions correctly:
//! returning `Some` for acceleratable queries and `None` for non-acceleratable
//! ones (graceful fallback to Prometheus).

#[cfg(test)]
mod tests {
    use crate::data_model::AggregationType;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::tests::test_utilities::engine_factories::{
        create_engine_single_pop, create_engine_two_metrics,
    };

    const QUERY_TIME: f64 = 1000.0;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_query_promql_binary_returns_result() {
        let engine = create_engine_two_metrics(
            "errors_total",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)) as Box<dyn crate::AggregateCore>,
            )],
            "sum(errors_total) by (host)",
            "requests_total",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(200.0)) as Box<dyn crate::AggregateCore>,
            )],
            "sum(requests_total) by (host)",
        );

        let result = engine.handle_query_promql(
            "sum(errors_total) by (host) / sum(requests_total) by (host)".to_string(),
            QUERY_TIME,
        );
        assert!(result.is_some(), "Binary query should return Some");
        let (labels, qr) = result.unwrap();
        assert!(!labels.labels.is_empty(), "Should have output label names");
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 1);
        assert!((elements[0].value - 0.5).abs() < 1e-10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_query_promql_non_acceleratable_arm_returns_none() {
        // Only requests_total is configured; foo() is not a known function.
        let engine = create_engine_single_pop(
            "requests_total",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(200.0)),
            )],
            "sum(requests_total) by (host)",
        );

        // foo() is not a supported PromQL function → arm lookup fails → returns None
        let result = engine.handle_query_promql(
            "foo(errors_total[5m]) / sum(requests_total) by (host)".to_string(),
            QUERY_TIME,
        );
        assert!(
            result.is_none(),
            "Should return None for non-acceleratable arm (graceful fallback)"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_query_promql_scalar_binary_returns_result() {
        let engine = create_engine_single_pop(
            "errors_total",
            AggregationType::Sum,
            vec!["host"],
            vec![(
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(7.0)),
            )],
            "sum(errors_total) by (host)",
        );

        let result =
            engine.handle_query_promql("sum(errors_total) by (host) * 100".to_string(), QUERY_TIME);
        assert!(result.is_some(), "Scalar binary should return Some");
        let (_, qr) = result.unwrap();
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 1);
        assert!((elements[0].value - 700.0).abs() < 1e-10, "7 * 100 = 700");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_handle_query_promql_single_metric_still_works() {
        // Regression: single-metric queries continue to work after binary dispatch is wired in.
        let engine = create_engine_single_pop(
            "http_requests",
            AggregationType::Sum,
            vec!["host"],
            vec![
                (
                    Some(vec!["host-a".to_string()]),
                    Box::new(SumAccumulator::with_sum(100.0)),
                ),
                (
                    Some(vec!["host-b".to_string()]),
                    Box::new(SumAccumulator::with_sum(200.0)),
                ),
            ],
            "sum(http_requests) by (host)",
        );

        let result =
            engine.handle_query_promql("sum(http_requests) by (host)".to_string(), QUERY_TIME);
        assert!(
            result.is_some(),
            "Single-metric query should still work after binary dispatch"
        );
        let (_, qr) = result.unwrap();
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 2);
        let mut values: Vec<f64> = elements.iter().map(|e| e.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!((values[0] - 100.0).abs() < 1e-10);
        assert!((values[1] - 200.0).abs() < 1e-10);
    }
}
