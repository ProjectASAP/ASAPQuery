//! Binary arithmetic plan execution integration tests.
//!
//! Verify that binary arithmetic queries (vector/vector and scalar/vector)
//! produce numerically correct results when executed end-to-end through
//! `handle_binary_expr_promql` via DataFusion.

#[cfg(test)]
mod tests {
    use crate::data_model::AggregationType;
    use crate::precompute_operators::sum_accumulator::SumAccumulator;
    use crate::tests::test_utilities::engine_factories::{
        create_engine_three_metrics, create_engine_two_metrics,
    };

    const QUERY_TIME: f64 = 1000.0;

    fn host_a_b_data(
        val_a: f64,
        val_b: f64,
    ) -> (
        crate::tests::test_utilities::engine_factories::AccumulatorData,
        crate::tests::test_utilities::engine_factories::AccumulatorData,
    ) {
        let data_a = vec![
            (
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(val_a)) as Box<dyn crate::AggregateCore>,
            ),
            (
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(val_a / 2.0)),
            ),
        ];
        let data_b = vec![
            (
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(val_b)) as Box<dyn crate::AggregateCore>,
            ),
            (
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(val_b / 2.0)),
            ),
        ];
        (data_a, data_b)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vector_vector_divide_produces_ratio() {
        // errors/host-a = 100, requests/host-a = 200 → ratio 0.5
        let (data_errors, data_requests) = host_a_b_data(100.0, 200.0);
        let engine = create_engine_two_metrics(
            "errors_total",
            AggregationType::Sum,
            vec!["host"],
            data_errors,
            "sum(errors_total) by (host)",
            "requests_total",
            AggregationType::Sum,
            vec!["host"],
            data_requests,
            "sum(requests_total) by (host)",
        );

        let query = "sum(errors_total) by (host) / sum(requests_total) by (host)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        assert!(result.is_some(), "Expected Some result for binary query");
        let (_, qr) = result.unwrap();
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 2, "Expected 2 result rows");
        for elem in &elements {
            let approx = (elem.value - 0.5).abs();
            assert!(
                approx < 1e-10,
                "Expected ratio 0.5, got {} for labels {:?}",
                elem.value,
                elem.labels
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vector_vector_multiply() {
        let (data_a, data_b) = host_a_b_data(3.0, 4.0);
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            data_a,
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["host"],
            data_b,
            "sum(metric_b) by (host)",
        );

        let query = "sum(metric_a) by (host) * sum(metric_b) by (host)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        let (_, qr) = result.expect("Expected result");
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 2);
        // host-a: 3 * 4 = 12, host-b: 1.5 * 2.0 = 3.0
        let mut values: Vec<f64> = elements.iter().map(|e| e.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!((values[0] - 3.0).abs() < 1e-10);
        assert!((values[1] - 12.0).abs() < 1e-10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vector_vector_add() {
        let (data_a, data_b) = host_a_b_data(10.0, 20.0);
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            data_a,
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["host"],
            data_b,
            "sum(metric_b) by (host)",
        );

        let query = "sum(metric_a) by (host) + sum(metric_b) by (host)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        let (_, qr) = result.expect("Expected result");
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 2);
        // host-a: 10+20=30, host-b: 5+10=15
        let mut values: Vec<f64> = elements.iter().map(|e| e.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!((values[0] - 15.0).abs() < 1e-10);
        assert!((values[1] - 30.0).abs() < 1e-10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vector_vector_subtract() {
        let (data_a, data_b) = host_a_b_data(50.0, 30.0);
        let engine = create_engine_two_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            data_a,
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["host"],
            data_b,
            "sum(metric_b) by (host)",
        );

        let query = "sum(metric_a) by (host) - sum(metric_b) by (host)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        let (_, qr) = result.expect("Expected result");
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 2);
        // host-a: 50-30=20, host-b: 25-15=10
        let mut values: Vec<f64> = elements.iter().map(|e| e.value).collect();
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert!((values[0] - 10.0).abs() < 1e-10);
        assert!((values[1] - 20.0).abs() < 1e-10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_vector_vector_inner_join_drops_unmatched() {
        // errors has host-a and host-b; requests only has host-a
        let data_errors = vec![
            (
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)) as Box<dyn crate::AggregateCore>,
            ),
            (
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(50.0)),
            ),
        ];
        let data_requests = vec![(
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(200.0)) as Box<dyn crate::AggregateCore>,
        )];

        let engine = create_engine_two_metrics(
            "errors_total",
            AggregationType::Sum,
            vec!["host"],
            data_errors,
            "sum(errors_total) by (host)",
            "requests_total",
            AggregationType::Sum,
            vec!["host"],
            data_requests,
            "sum(requests_total) by (host)",
        );

        let query = "sum(errors_total) by (host) / sum(requests_total) by (host)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        let (_, qr) = result.expect("Expected result");
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        // Inner join: only host-a is present in both → 1 result
        assert_eq!(
            elements.len(),
            1,
            "Inner join should drop unmatched label set (host-b)"
        );
        assert!((elements[0].value - 0.5).abs() < 1e-10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scalar_right_multiply() {
        // sum(errors_total) by (host) * 100
        let data = vec![(
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(5.0)) as Box<dyn crate::AggregateCore>,
        )];
        let engine = create_engine_two_metrics(
            "errors_total",
            AggregationType::Sum,
            vec!["host"],
            data,
            "sum(errors_total) by (host)",
            // second metric not used but factory requires it; use empty data
            "dummy",
            AggregationType::Sum,
            vec!["host"],
            vec![],
            "sum(dummy) by (host)",
        );

        let query = "sum(errors_total) by (host) * 100";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        let (_, qr) = result.expect("Expected result for scalar-right multiply");
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 1);
        assert!(
            (elements[0].value - 500.0).abs() < 1e-10,
            "5.0 * 100 = 500.0"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_scalar_left_subtract() {
        // 1 - sum(success_total) by (host)
        let data = vec![(
            Some(vec!["host-a".to_string()]),
            Box::new(SumAccumulator::with_sum(0.9)) as Box<dyn crate::AggregateCore>,
        )];
        let engine = create_engine_two_metrics(
            "success_total",
            AggregationType::Sum,
            vec!["host"],
            data,
            "sum(success_total) by (host)",
            "dummy",
            AggregationType::Sum,
            vec!["host"],
            vec![],
            "sum(dummy) by (host)",
        );

        let query = "1 - sum(success_total) by (host)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        let (_, qr) = result.expect("Expected result for scalar-left subtract");
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(elements.len(), 1);
        assert!(
            (elements[0].value - 0.1).abs() < 1e-10,
            "1 - 0.9 = 0.1, got {}",
            elements[0].value
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_nested_binary_expression() {
        // (sum(metric_a) by (host) + sum(metric_b) by (host)) / sum(metric_c) by (host)
        // host-a: a=100, b=200, c=300 → (100+200)/300 = 1.0
        // host-b: a=50,  b=100, c=150 → (50+100)/150  = 1.0
        let data_a = vec![
            (
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)) as Box<dyn crate::AggregateCore>,
            ),
            (
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(50.0)) as Box<dyn crate::AggregateCore>,
            ),
        ];
        let data_b = vec![
            (
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(200.0)) as Box<dyn crate::AggregateCore>,
            ),
            (
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(100.0)) as Box<dyn crate::AggregateCore>,
            ),
        ];
        let data_c = vec![
            (
                Some(vec!["host-a".to_string()]),
                Box::new(SumAccumulator::with_sum(300.0)) as Box<dyn crate::AggregateCore>,
            ),
            (
                Some(vec!["host-b".to_string()]),
                Box::new(SumAccumulator::with_sum(150.0)) as Box<dyn crate::AggregateCore>,
            ),
        ];

        let engine = create_engine_three_metrics(
            "metric_a",
            AggregationType::Sum,
            vec!["host"],
            data_a,
            "sum(metric_a) by (host)",
            "metric_b",
            AggregationType::Sum,
            vec!["host"],
            data_b,
            "sum(metric_b) by (host)",
            "metric_c",
            AggregationType::Sum,
            vec!["host"],
            data_c,
            "sum(metric_c) by (host)",
        );

        let query = "(sum(metric_a) by (host) + sum(metric_b) by (host)) / sum(metric_c) by (host)";
        let result = engine.handle_query_promql(query.to_string(), QUERY_TIME);
        let (_, qr) = result.expect("Expected result for nested binary expression");
        let elements = match qr {
            crate::engines::query_result::QueryResult::Vector(iv) => iv.values,
            _ => panic!("Expected vector result"),
        };
        assert_eq!(
            elements.len(),
            2,
            "Expected 2 result rows (host-a and host-b)"
        );
        for elem in &elements {
            assert!(
                (elem.value - 1.0).abs() < 1e-10,
                "Expected (a+b)/c = 1.0, got {} for labels {:?}",
                elem.value,
                elem.labels
            );
        }
    }
}
