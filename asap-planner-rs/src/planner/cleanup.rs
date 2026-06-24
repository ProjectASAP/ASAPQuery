use asap_types::enums::{CleanupPolicy, WindowType};
use promql_utilities::ast_matching::PromQLMatchResult;
use promql_utilities::query_logics::enums::QueryPatternType;

use super::window::get_effective_repeat;

pub fn get_cleanup_param(
    cleanup_policy: CleanupPolicy,
    query_pattern_type: QueryPatternType,
    match_result: &PromQLMatchResult,
    t_repeat_ms: u64,
    window_type: WindowType,
    range_duration_ms: u64,
    step_ms: u64,
) -> Result<u64, String> {
    // Validation
    if (range_duration_ms == 0) != (step_ms == 0) {
        return Err(format!(
            "range_duration_ms and step_ms must both be 0 or both > 0. Got range_duration_ms={}, step_ms={}",
            range_duration_ms, step_ms
        ));
    }

    let is_range_query = step_ms > 0;

    let t_lookback: u64 = if query_pattern_type == QueryPatternType::OnlySpatial {
        t_repeat_ms
    } else {
        match_result
            .get_range_duration()
            .map(|d| d.num_milliseconds() as u64)
            .ok_or_else(|| "No range_vector token found".to_string())?
    };

    if window_type == WindowType::Sliding {
        let result = if is_range_query {
            range_duration_ms / step_ms + 1
        } else {
            1
        };
        return Ok(result);
    }

    // Tumbling
    let effective_repeat = get_effective_repeat(t_repeat_ms, step_ms);

    let result = match cleanup_policy {
        CleanupPolicy::CircularBuffer => {
            // ceil((t_lookback + range_duration_ms) / effective_repeat)
            let numerator = t_lookback + range_duration_ms;
            numerator.div_ceil(effective_repeat)
        }
        CleanupPolicy::ReadBased => {
            // ceil(t_lookback / effective_repeat) * (range_duration_ms / step_ms + 1)
            let lookback_buckets = t_lookback.div_ceil(effective_repeat);
            let num_steps = if is_range_query {
                range_duration_ms / step_ms + 1
            } else {
                1
            };
            lookback_buckets * num_steps
        }
        CleanupPolicy::NoCleanup => {
            return Err("NoCleanup policy should not call get_cleanup_param".to_string());
        }
    };

    Ok(result)
}

/// SQL cleanup param — SQL queries are always instant (no range_duration/step).
pub fn get_sql_cleanup_param(
    cleanup_policy: CleanupPolicy,
    t_lookback: u64,
    t_repeat: u64,
) -> Result<u64, String> {
    match cleanup_policy {
        CleanupPolicy::CircularBuffer | CleanupPolicy::ReadBased => {
            if t_repeat == 0 {
                return Err(
                    "repetition_delay must be > 0 for cleanup param calculation; \
                     set a non-zero repetition_delay in your query group config"
                        .to_string(),
                );
            }
            Ok(t_lookback.div_ceil(t_repeat))
        }
        CleanupPolicy::NoCleanup => {
            Err("NoCleanup policy should not call get_sql_cleanup_param".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::patterns::build_patterns;

    use promql_utilities::ast_matching::PromQLMatchResult;
    use promql_utilities::query_logics::enums::QueryPatternType;

    fn match_query(query: &str) -> (QueryPatternType, PromQLMatchResult) {
        let ast = promql_parser::parser::parse(query).unwrap();
        let patterns = build_patterns();
        for (pt, pattern) in &patterns {
            let result = pattern.matches(&ast);
            if result.matches {
                return (*pt, result);
            }
        }
        panic!("no pattern matched query: {}", query);
    }

    #[test]
    fn cleanup_param_circular_buffer_spatial_instant_query() {
        let (pt, mr) = match_query("sum(some_metric)");
        assert_eq!(pt, QueryPatternType::OnlySpatial);
        // t_lookback = t_repeat_ms = 300_000 (OnlySpatial path)
        // effective_repeat = 300_000 (step_ms=0)
        // ceil((300_000 + 0) / 300_000) = 1
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            pt,
            &mr,
            300_000,
            WindowType::Tumbling,
            0,
            0,
        )
        .unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn cleanup_param_circular_buffer_spatial_range_query() {
        let (pt, mr) = match_query("sum(some_metric)");
        // t_lookback = t_repeat_ms = 300_000, effective_repeat = min(300_000, 30_000) = 30_000
        // ceil((300_000 + 3_600_000) / 30_000) = ceil(130) = 130
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            pt,
            &mr,
            300_000,
            WindowType::Tumbling,
            3_600_000,
            30_000,
        )
        .unwrap();
        assert_eq!(result, 130);
    }

    #[test]
    fn cleanup_param_read_based_spatial_instant_query() {
        let (pt, mr) = match_query("sum(some_metric)");
        // lookback_buckets = ceil(300_000/300_000) = 1, num_steps = 1 → result = 1
        let result = get_cleanup_param(
            CleanupPolicy::ReadBased,
            pt,
            &mr,
            300_000,
            WindowType::Tumbling,
            0,
            0,
        )
        .unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn cleanup_param_read_based_spatial_range_query() {
        let (pt, mr) = match_query("sum(some_metric)");
        // lookback_buckets = ceil(300_000/30_000) = 10, num_steps = 3_600_000/30_000 + 1 = 121
        // result = 10 * 121 = 1210
        let result = get_cleanup_param(
            CleanupPolicy::ReadBased,
            pt,
            &mr,
            300_000,
            WindowType::Tumbling,
            3_600_000,
            30_000,
        )
        .unwrap();
        assert_eq!(result, 1210);
    }

    #[test]
    fn cleanup_param_circular_buffer_temporal_instant_query() {
        let (pt, mr) = match_query("rate(some_metric[5m])");
        assert_eq!(pt, QueryPatternType::OnlyTemporal);
        // t_lookback = 5m = 300_000ms (from [5m] range vector), range_duration_ms=0, step_ms=0
        // effective_repeat = 60_000, ceil((300_000 + 0) / 60_000) = 5
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            pt,
            &mr,
            60_000,
            WindowType::Tumbling,
            0,
            0,
        )
        .unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn cleanup_param_no_cleanup_returns_error() {
        let (pt, mr) = match_query("sum(some_metric)");
        let result = get_cleanup_param(
            CleanupPolicy::NoCleanup,
            pt,
            &mr,
            300_000,
            WindowType::Tumbling,
            0,
            0,
        );
        assert!(result.is_err());
    }

    #[test]
    fn cleanup_param_mismatched_range_and_step_returns_error() {
        let (pt, mr) = match_query("sum(some_metric)");
        // range_duration_ms > 0 but step_ms == 0 is invalid
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            pt,
            &mr,
            300_000,
            WindowType::Tumbling,
            3_600_000,
            0,
        );
        assert!(result.is_err());
    }
}
