use asap_types::enums::{CleanupPolicy, WindowType};

use super::window::get_effective_repeat;

/// `data_range_ms` is the query's own requested duration (from
/// `QueryRequirements.data_range_ms` — for a spatial-only query this equals
/// `data_ingestion_interval_ms` by construction), used directly as the
/// lookback: once `set_window_parameters`'s invariant holds
/// (`data_range_ms >= t_repeat_ms >= data_ingestion_interval_ms`), this is
/// exactly equivalent to the old pattern-type-gated `t_repeat_ms`-vs-range-duration
/// split, with no shape check needed (see #508).
#[allow(clippy::too_many_arguments)]
pub fn get_cleanup_param(
    cleanup_policy: CleanupPolicy,
    data_range_ms: u64,
    t_repeat_ms: u64,
    window_type: WindowType,
    window_size_ms: u64,
    slide_interval_ms: u64,
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
    let t_lookback: u64 = data_range_ms;

    if window_type == WindowType::Sliding {
        if cleanup_policy == CleanupPolicy::NoCleanup {
            return Err("NoCleanup policy should not call get_cleanup_param".to_string());
        }
        if window_size_ms == 0 || slide_interval_ms == 0 {
            return Err("Sliding cleanup requires positive window and slide sizes".to_string());
        }
        if !data_range_ms.is_multiple_of(window_size_ms) {
            return Err(format!(
                "Sliding query lookback ({data_range_ms}ms) must be a multiple of window_size_ms ({window_size_ms}ms)"
            ));
        }
        let num_steps = if is_range_query {
            range_duration_ms / step_ms + 1
        } else {
            1
        };
        return match cleanup_policy {
            CleanupPolicy::ReadBased => (data_range_ms / window_size_ms)
                .checked_mul(num_steps)
                .ok_or_else(|| "Sliding read-count cleanup threshold overflowed".to_string()),
            CleanupPolicy::CircularBuffer => data_range_ms
                .checked_add(range_duration_ms)
                .map(|span_ms| span_ms.div_ceil(slide_interval_ms))
                .ok_or_else(|| "Sliding circular-buffer retention span overflowed".to_string()),
            CleanupPolicy::NoCleanup => unreachable!(),
        };
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
    t_lookback_ms: u64,
    t_repeat_ms: u64,
) -> Result<u64, String> {
    match cleanup_policy {
        CleanupPolicy::CircularBuffer | CleanupPolicy::ReadBased => {
            if t_repeat_ms == 0 {
                return Err(
                    "repetition_delay_ms must be > 0 for cleanup param calculation; \
                     set a non-zero repetition_delay_ms in your query group config"
                        .to_string(),
                );
            }
            Ok(t_lookback_ms.div_ceil(t_repeat_ms))
        }
        CleanupPolicy::NoCleanup => {
            Err("NoCleanup policy should not call get_sql_cleanup_param".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_param_circular_buffer_spatial_instant_query() {
        // data_range_ms = t_lookback = 300_000 (a spatial-only query's canonical range)
        // effective_repeat = 300_000 (step_ms=0)
        // ceil((300_000 + 0) / 300_000) = 1
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            300_000,
            300_000,
            WindowType::Tumbling,
            300_000,
            300_000,
            0,
            0,
        )
        .unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn cleanup_param_circular_buffer_spatial_range_query() {
        // t_lookback = data_range_ms = 300_000, effective_repeat = min(300_000, 30_000) = 30_000
        // ceil((300_000 + 3_600_000) / 30_000) = ceil(130) = 130
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            300_000,
            300_000,
            WindowType::Tumbling,
            30_000,
            30_000,
            3_600_000,
            30_000,
        )
        .unwrap();
        assert_eq!(result, 130);
    }

    #[test]
    fn cleanup_param_read_based_spatial_instant_query() {
        // lookback_buckets = ceil(300_000/300_000) = 1, num_steps = 1 → result = 1
        let result = get_cleanup_param(
            CleanupPolicy::ReadBased,
            300_000,
            300_000,
            WindowType::Tumbling,
            300_000,
            300_000,
            0,
            0,
        )
        .unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn cleanup_param_read_based_spatial_range_query() {
        // lookback_buckets = ceil(300_000/30_000) = 10, num_steps = 3_600_000/30_000 + 1 = 121
        // result = 10 * 121 = 1210
        let result = get_cleanup_param(
            CleanupPolicy::ReadBased,
            300_000,
            300_000,
            WindowType::Tumbling,
            30_000,
            30_000,
            3_600_000,
            30_000,
        )
        .unwrap();
        assert_eq!(result, 1210);
    }

    #[test]
    fn cleanup_param_circular_buffer_temporal_instant_query() {
        // data_range_ms = 5m = 300_000ms (from a [5m] range vector), range_duration_ms=0, step_ms=0
        // effective_repeat = 60_000, ceil((300_000 + 0) / 60_000) = 5
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            300_000,
            60_000,
            WindowType::Tumbling,
            60_000,
            60_000,
            0,
            0,
        )
        .unwrap();
        assert_eq!(result, 5);
    }

    #[test]
    fn cleanup_param_no_cleanup_returns_error() {
        let result = get_cleanup_param(
            CleanupPolicy::NoCleanup,
            300_000,
            300_000,
            WindowType::Tumbling,
            300_000,
            300_000,
            0,
            0,
        );
        assert!(result.is_err());
    }

    #[test]
    fn cleanup_param_mismatched_range_and_step_returns_error() {
        // range_duration_ms > 0 but step_ms == 0 is invalid
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            300_000,
            300_000,
            WindowType::Tumbling,
            300_000,
            300_000,
            3_600_000,
            0,
        );
        assert!(result.is_err());
    }

    #[test]
    fn cleanup_param_read_based_sliding_counts_each_constituent_window() {
        let result = get_cleanup_param(
            CleanupPolicy::ReadBased,
            10_000,
            5_000,
            WindowType::Sliding,
            5_000,
            1_000,
            0,
            0,
        )
        .unwrap();

        assert_eq!(result, 2);
    }

    #[test]
    fn cleanup_param_circular_sliding_retains_cover_span() {
        let result = get_cleanup_param(
            CleanupPolicy::CircularBuffer,
            10_000,
            5_000,
            WindowType::Sliding,
            5_000,
            1_000,
            5_000,
            1_000,
        )
        .unwrap();
        assert_eq!(result, 15);
    }

    #[test]
    fn cleanup_param_sliding_rejects_invalid_shape_and_sizes() {
        for (window_size_ms, slide_interval_ms, lookback_ms) in [
            (0, 1_000, 10_000),
            (5_000, 0, 10_000),
            (6_000, 1_000, 10_000),
        ] {
            assert!(get_cleanup_param(
                CleanupPolicy::ReadBased,
                lookback_ms,
                5_000,
                WindowType::Sliding,
                window_size_ms,
                slide_interval_ms,
                0,
                0,
            )
            .is_err());
        }
    }

    #[test]
    fn cleanup_param_no_cleanup_sliding_is_rejected() {
        assert!(get_cleanup_param(
            CleanupPolicy::NoCleanup,
            10_000,
            5_000,
            WindowType::Sliding,
            5_000,
            1_000,
            0,
            0,
        )
        .is_err());
    }
}
