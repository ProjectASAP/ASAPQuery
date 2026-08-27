use asap_types::enums::WindowType;
use std::fmt;

use crate::config::input::{WindowingConfig, WindowingType};

pub fn get_effective_repeat(t_repeat_ms: u64, step_ms: u64) -> u64 {
    if step_ms > 0 {
        t_repeat_ms.min(step_ms)
    } else {
        t_repeat_ms
    }
}

pub fn should_use_sliding_window() -> bool {
    // HARDCODED: sliding windows crash Arroyo
    false
}

/// Sets tumbling window parameters for a query, enforcing the same invariant
/// SQL's `compute_sql_window` does (`asap-planner-rs/src/planner/sql.rs`,
/// kept consistent with this function — see the relaxation note there too):
/// `t_repeat_ms >= data_ingestion_interval_ms` (can't refresh faster than raw
/// ingestion), plus, for a genuinely multi-interval query, `data_range_ms >=
/// t_repeat_ms` (a precompute window can't outlive the query range it's sized
/// for). `step_ms >= data_ingestion_interval_ms` is also required for range
/// queries (can't sample more finely than raw ingestion).
///
/// Relaxation: when `data_range_ms == data_ingestion_interval_ms`, the query's
/// own range is exactly one scrape interval — always true for a spatial-only
/// query (which has no range of its own, so its canonical range is defined as
/// the interval), and sometimes true for a genuinely narrow temporal query
/// (e.g. `rate(x[15s])` at a 15s scrape interval). Either way, such a query
/// only ever concerns a single precomputed bucket — it asks for "the
/// current/latest bucket," not "the last N buckets" — so re-reading that one
/// bucket less often than it's produced is always safe: there's no fresher
/// answer to give. The `t_repeat_ms <= data_range_ms` upper bound is skipped
/// in this case, and the window is sized to exactly one interval regardless
/// of `t_repeat_ms`. Without this relaxation, an ordinary dashboard panel
/// running `sum(x)` refreshed once a minute over 15s-scraped data would be
/// rejected outright, even though serving it is trivially correct (see #508).
///
/// Sliding windows are never used (they crash Arroyo), so this always
/// produces a tumbling window.
pub fn set_window_parameters(
    data_range_ms: u64,
    t_repeat_ms: u64,
    data_ingestion_interval_ms: u64,
    step_ms: u64,
    config: &mut IntermediateWindowConfig,
) -> Result<(), String> {
    if t_repeat_ms < data_ingestion_interval_ms {
        return Err(format!(
            "t_repeat_ms ({t_repeat_ms}ms) must be >= data_ingestion_interval_ms ({data_ingestion_interval_ms}ms)"
        ));
    }
    if step_ms > 0 && step_ms < data_ingestion_interval_ms {
        return Err(format!(
            "step_ms ({step_ms}ms) must be >= data_ingestion_interval_ms ({data_ingestion_interval_ms}ms)"
        ));
    }

    let _use_sliding = should_use_sliding_window();
    // use_sliding is always false, so always tumbling
    let window_size_ms = if data_range_ms == data_ingestion_interval_ms {
        data_ingestion_interval_ms
    } else {
        if data_range_ms < t_repeat_ms {
            return Err(format!(
                "query data range ({data_range_ms}ms) must be >= t_repeat_ms ({t_repeat_ms}ms)"
            ));
        }
        get_effective_repeat(t_repeat_ms, step_ms)
    };

    // A range query reads `step_ms`-spaced samples by summing whole tumbling
    // buckets (see `validate_range_query_params` in
    // asap-query-engine/src/engines/simple_engine/mod.rs, which rejects a
    // range query at query time unless `step % tumbling_window_ms == 0`).
    // Catch a window size incompatible with its own planning-time step_ms
    // here, at planning time, instead of provisioning a window that would
    // reject every range query using exactly the step_ms it was planned for.
    if step_ms > 0 && !step_ms.is_multiple_of(window_size_ms) {
        return Err(format!(
            "step_ms ({step_ms}ms) must be a multiple of the computed window size ({window_size_ms}ms)"
        ));
    }

    config.window_size_ms = window_size_ms;
    config.slide_interval_ms = window_size_ms;
    config.window_type = WindowType::Tumbling;
    Ok(())
}

pub fn apply_windowing_override(
    config: &mut IntermediateWindowConfig,
    data_range_ms: u64,
    windowing: Option<&WindowingConfig>,
) -> Result<(), WindowingError> {
    let Some(windowing) = windowing else {
        return Ok(());
    };
    windowing
        .validate()
        .map_err(WindowingError::InvalidConfig)?;

    match windowing.window_type {
        WindowingType::Tumbling => {
            config.window_type = WindowType::Tumbling;
            config.window_size_ms = windowing.window_size_ms;
            config.slide_interval_ms = config.window_size_ms;
        }
        WindowingType::Sliding => {
            let slide_interval_ms = match windowing.slide_interval_ms {
                Some(slide_interval_ms) => slide_interval_ms,
                None => {
                    return Err(WindowingError::InvalidConfig(
                        "windowing.slide_interval_ms is required for sliding windows".to_string(),
                    ));
                }
            };
            config.window_size_ms = windowing.window_size_ms;
            if !config.window_size_ms.is_multiple_of(slide_interval_ms) {
                return Err(WindowingError::WindowSizeNotDivisible {
                    window_size_ms: config.window_size_ms,
                    slide_interval_ms,
                });
            }
            if !data_range_ms.is_multiple_of(config.window_size_ms) {
                return Err(WindowingError::DataRangeNotDivisible {
                    data_range_ms,
                    window_size_ms: config.window_size_ms,
                });
            }
            config.window_type = WindowType::Sliding;
            config.slide_interval_ms = slide_interval_ms;
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowingError {
    InvalidConfig(String),
    WindowSizeNotDivisible {
        window_size_ms: u64,
        slide_interval_ms: u64,
    },
    DataRangeNotDivisible {
        data_range_ms: u64,
        window_size_ms: u64,
    },
}

impl fmt::Display for WindowingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => f.write_str(message),
            Self::WindowSizeNotDivisible {
                window_size_ms,
                slide_interval_ms,
            } => write!(
                f,
                "windowing.window_size_ms ({window_size_ms}) must be evenly divisible by windowing.slide_interval_ms ({slide_interval_ms})"
            ),
            Self::DataRangeNotDivisible {
                data_range_ms,
                window_size_ms,
            } => write!(
                f,
                "data_range_ms ({data_range_ms}) must be evenly divisible by window_size_ms ({window_size_ms})"
            ),
        }
    }
}

impl std::error::Error for WindowingError {}

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

    #[test]
    fn set_window_parameters_temporal_shape() {
        let mut config = IntermediateWindowConfig::default();
        set_window_parameters(300_000, 60_000, 15_000, 0, &mut config).unwrap();
        assert_eq!(config.window_size_ms, 60_000);
        assert_eq!(config.slide_interval_ms, 60_000);
        assert_eq!(config.window_type, WindowType::Tumbling);
    }

    #[test]
    fn set_window_parameters_spatial_shape_reproduces_interval() {
        // data_range_ms == data_ingestion_interval_ms (spatial-only query),
        // t_repeat_ms also equal to the interval: unaffected by the relaxation.
        let mut config = IntermediateWindowConfig::default();
        set_window_parameters(15_000, 15_000, 15_000, 0, &mut config).unwrap();
        assert_eq!(config.window_size_ms, 15_000);
    }

    #[test]
    fn set_window_parameters_spatial_shape_allows_slower_repeat_than_interval() {
        // A spatial-only query refreshed less often than the scrape interval
        // (e.g. a dashboard panel polling every 60s over 15s-scraped data) is
        // always safe to serve: re-reading the single precomputed bucket at
        // any cadence gives the latest available answer. window_size stays
        // exactly one interval regardless of t_repeat_ms.
        let mut config = IntermediateWindowConfig::default();
        set_window_parameters(15_000, 60_000, 15_000, 0, &mut config).unwrap();
        assert_eq!(config.window_size_ms, 15_000);
    }

    #[test]
    fn set_window_parameters_rejects_t_repeat_below_interval() {
        let mut config = IntermediateWindowConfig::default();
        assert!(set_window_parameters(300_000, 10_000, 15_000, 0, &mut config).is_err());
    }

    #[test]
    fn set_window_parameters_rejects_data_range_below_t_repeat() {
        let mut config = IntermediateWindowConfig::default();
        assert!(set_window_parameters(30_000, 60_000, 15_000, 0, &mut config).is_err());
    }

    #[test]
    fn set_window_parameters_rejects_step_below_interval() {
        let mut config = IntermediateWindowConfig::default();
        assert!(set_window_parameters(300_000, 60_000, 15_000, 10_000, &mut config).is_err());
    }

    #[test]
    fn set_window_parameters_rejects_step_not_multiple_of_window_size() {
        // t_repeat_ms (40_000) < step_ms (100_000), so window_size_ms =
        // effective_repeat = t_repeat_ms = 40_000. But 100_000 is not a
        // multiple of 40_000 (100_000 % 40_000 == 20_000) — this would
        // otherwise provision a window that query-engine's
        // validate_range_query_params rejects for exactly this step_ms.
        let mut config = IntermediateWindowConfig::default();
        let result = set_window_parameters(300_000, 40_000, 10_000, 100_000, &mut config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be a multiple of"));
    }

    #[test]
    fn sliding_override_rejects_data_range_not_multiple_of_window_size() {
        let mut config = IntermediateWindowConfig {
            window_size_ms: 60_000,
            slide_interval_ms: 60_000,
            window_type: WindowType::Tumbling,
        };
        let windowing = WindowingConfig {
            window_type: WindowingType::Sliding,
            window_size_ms: 60_000,
            slide_interval_ms: Some(15_000),
        };

        let error = apply_windowing_override(&mut config, 90_000, Some(&windowing)).unwrap_err();

        assert_eq!(
            error,
            WindowingError::DataRangeNotDivisible {
                data_range_ms: 90_000,
                window_size_ms: 60_000,
            }
        );
    }
}
