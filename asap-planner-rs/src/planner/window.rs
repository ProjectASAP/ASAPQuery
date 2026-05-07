use asap_types::enums::WindowType;
use promql_utilities::query_logics::enums::QueryPatternType;

pub fn get_effective_repeat(t_repeat: u64, step: u64) -> u64 {
    if step > 0 {
        t_repeat.min(step)
    } else {
        t_repeat
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
    t_repeat: u64,
    prometheus_scrape_interval: u64,
    aggregation_type: &str,
    step: u64,
    config: &mut IntermediateWindowConfig,
) {
    let effective_repeat = get_effective_repeat(t_repeat, step);
    let _use_sliding = should_use_sliding_window(query_pattern_type, aggregation_type);
    // use_sliding is always false, so always tumbling
    set_tumbling_window_parameters(
        query_pattern_type,
        effective_repeat,
        prometheus_scrape_interval,
        config,
    );
}

fn set_tumbling_window_parameters(
    query_pattern_type: QueryPatternType,
    effective_repeat: u64,
    prometheus_scrape_interval: u64,
    config: &mut IntermediateWindowConfig,
) {
    match query_pattern_type {
        QueryPatternType::OnlyTemporal | QueryPatternType::OneTemporalOneSpatial => {
            config.window_size = effective_repeat;
            config.slide_interval = effective_repeat;
            config.window_type = WindowType::Tumbling;
        }
        QueryPatternType::OnlySpatial => {
            config.window_size = prometheus_scrape_interval;
            config.slide_interval = prometheus_scrape_interval;
            config.window_type = WindowType::Tumbling;
        }
    }
}

/// A mutable window config holder used during planning
#[derive(Debug, Clone, Default)]
pub struct IntermediateWindowConfig {
    pub window_size: u64,
    pub slide_interval: u64,
    pub window_type: WindowType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_repeat_no_step() {
        assert_eq!(get_effective_repeat(300, 0), 300);
    }

    #[test]
    fn effective_repeat_step_smaller_than_t_repeat() {
        assert_eq!(get_effective_repeat(300, 30), 30);
    }

    #[test]
    fn effective_repeat_step_larger_than_t_repeat() {
        assert_eq!(get_effective_repeat(30, 300), 30);
    }
}
