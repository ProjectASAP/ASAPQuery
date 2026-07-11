use asap_types::enums::{CleanupPolicy, WindowType};

use super::window::get_effective_repeat;

/// `data_range_ms` is the query's own requested duration (from
/// `QueryRequirements.data_range_ms` — for a spatial-only query this equals
/// `data_ingestion_interval_ms` by construction), used directly as the
/// lookback: once `set_window_parameters`'s invariant holds
/// (`data_range_ms >= t_repeat_ms >= data_ingestion_interval_ms`), this is
/// exactly equivalent to the old pattern-type-gated `t_repeat_ms`-vs-range-duration
/// split, with no shape check needed (see #508).
pub fn get_cleanup_param(
    cleanup_policy: CleanupPolicy,
    data_range_ms: u64,
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
    let t_lookback: u64 = data_range_ms;

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
            3_600_000,
            0,
        );
        assert!(result.is_err());
    }
}
