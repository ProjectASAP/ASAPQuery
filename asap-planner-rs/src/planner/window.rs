use asap_types::enums::WindowType;
use promql_utilities::query_logics::enums::QueryPatternType;

pub fn get_effective_repeat(t_repeat_ms: u64, step_ms: u64) -> u64 {
    if step_ms > 0 {
        t_repeat_ms.min(step_ms)
    } else {
        t_repeat_ms
    }
}

pub fn should_use_sliding_window(
    _query_pattern_type: QueryPatternType,
    _aggregation_type: &str,
) -> bool {
    // HARDCODED: sliding windows crash Arroyo
    false
}

pub fn set_window_parameters(
    query_pattern_type: QueryPatternType,
    t_repeat_ms: u64,
    data_ingestion_interval_ms: u64,
    aggregation_type: &str,
    step_ms: u64,
    config: &mut IntermediateWindowConfig,
) {
    let effective_repeat = get_effective_repeat(t_repeat_ms, step_ms);
    let _use_sliding = should_use_sliding_window(query_pattern_type, aggregation_type);
    // use_sliding is always false, so always tumbling
    set_tumbling_window_parameters(
        query_pattern_type,
        effective_repeat,
        data_ingestion_interval_ms,
        config,
    );
}

fn set_tumbling_window_parameters(
    query_pattern_type: QueryPatternType,
    effective_repeat: u64,
    data_ingestion_interval_ms: u64,
    config: &mut IntermediateWindowConfig,
) {
    match query_pattern_type {
        QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
            config.window_size_ms = effective_repeat;
            config.slide_interval_ms = effective_repeat;
            config.window_type = WindowType::Tumbling;
        }
        QueryPatternType::OnlySpatial => {
            config.window_size_ms = data_ingestion_interval_ms;
            config.slide_interval_ms = data_ingestion_interval_ms;
            config.window_type = WindowType::Tumbling;
        }
    }
}

/// A mutable window config holder used during planning
#[derive(Debug, Clone, Default)]
pub struct IntermediateWindowConfig {
    pub window_size_ms: u64,
    pub slide_interval_ms: u64,
    pub window_type: WindowType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_repeat_no_step() {
        assert_eq!(get_effective_repeat(300_000, 0), 300_000);
    }

    #[test]
    fn effective_repeat_step_smaller_than_t_repeat() {
        assert_eq!(get_effective_repeat(300_000, 30_000), 30_000);
    }

    #[test]
    fn effective_repeat_step_larger_than_t_repeat() {
        assert_eq!(get_effective_repeat(30_000, 300_000), 30_000);
    }
}
