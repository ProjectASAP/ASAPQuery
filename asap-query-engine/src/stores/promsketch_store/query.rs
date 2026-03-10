use sketchlib_rust::{EHSketchList, SketchInput, UniformSampling};

use super::series::PromSketchMemSeries;

/// Evaluate a PromQL aggregation function over sketches for a given time range.
///
/// # Arguments
/// * `func_name` - PromQL function name (e.g. "quantile_over_time")
/// * `series` - The PromSketchMemSeries containing sketch instances
/// * `args` - Extra argument (e.g. quantile phi value)
/// * `mint` - Start of query time range (milliseconds)
/// * `maxt` - End of query time range (milliseconds)
pub fn eval_function(
    func_name: &str,
    series: &PromSketchMemSeries,
    args: f64,
    mint: u64,
    maxt: u64,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    match func_name {
        "entropy_over_time" => eval_univmon(series, "entropy", mint, maxt),
        "distinct_over_time" => eval_univmon(series, "cardinality", mint, maxt),
        "l1_over_time" => eval_univmon(series, "l1", mint, maxt),
        "l2_over_time" => eval_univmon(series, "l2", mint, maxt),
        "quantile_over_time" => eval_kll_quantile(series, args, mint, maxt),
        "min_over_time" => eval_kll_quantile(series, 0.0, mint, maxt),
        "max_over_time" => eval_kll_quantile(series, 1.0, mint, maxt),
        "avg_over_time" => eval_sampling_stat(series, "avg", mint, maxt),
        "count_over_time" => eval_sampling_stat(series, "count", mint, maxt),
        "sum_over_time" => eval_sampling_stat(series, "sum", mint, maxt),
        "sum2_over_time" => eval_sampling_stat(series, "sum2", mint, maxt),
        "stddev_over_time" => eval_sampling_stat(series, "stddev", mint, maxt),
        "stdvar_over_time" => eval_sampling_stat(series, "stdvar", mint, maxt),
        _ => Err(format!("unsupported function: {}", func_name).into()),
    }
}

/// Evaluate UnivMon-based functions (entropy, cardinality, L1, L2)
/// using the optimized EHUnivOptimized backend.
fn eval_univmon(
    series: &PromSketchMemSeries,
    stat: &str,
    mint: u64,
    maxt: u64,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let eh = series
        .sketch_instances
        .eh_univ
        .as_ref()
        .ok_or("eh_univ not initialized")?;

    let result = eh
        .query_interval(mint, maxt)
        .ok_or("no buckets cover the requested time range for UnivMon")?;

    match stat {
        "entropy" => Ok(result.calc_entropy()),
        "cardinality" => Ok(result.calc_card()),
        "l1" => Ok(result.calc_l1()),
        "l2" => Ok(result.calc_l2()),
        _ => Err(format!("unknown univmon stat: {}", stat).into()),
    }
}

/// Evaluate KLL-based functions (quantile, min, max).
fn eval_kll_quantile(
    series: &PromSketchMemSeries,
    phi: f64,
    mint: u64,
    maxt: u64,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let eh = series
        .sketch_instances
        .eh_kll
        .as_ref()
        .ok_or("eh_kll not initialized")?;

    let merged = eh
        .query_interval_merge(mint, maxt)
        .ok_or("no volumes cover the requested time range for KLL")?;

    merged
        .query(&SketchInput::F64(phi))
        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { e.into() })
}

/// Evaluate sampling-based functions (avg, count, sum, sum2, stddev, stdvar).
///
/// Since sketchlib-rust's `UniformSampling` exposes `samples()` and `total_seen()`
/// but not dedicated query methods like the Go version, we compute statistics
/// from the raw merged samples.
fn eval_sampling_stat(
    series: &PromSketchMemSeries,
    stat: &str,
    mint: u64,
    maxt: u64,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let eh = series
        .sketch_instances
        .eh_sampling
        .as_ref()
        .ok_or("eh_sampling not initialized")?;

    let merged = eh
        .query_interval_merge(mint, maxt)
        .ok_or("no volumes cover the requested time range for sampling")?;

    let sampler = match &merged {
        EHSketchList::UNIFORM(us) => us,
        _ => return Err("merged EHSketchList is not UniformSampling".into()),
    };

    compute_sampling_stat(sampler, stat)
}

/// Compute a statistic from a merged UniformSampling instance.
fn compute_sampling_stat(
    sampler: &UniformSampling,
    stat: &str,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let samples = sampler.samples();
    if samples.is_empty() {
        return Err("no samples available".into());
    }

    let n = samples.len() as f64;
    let total_seen = sampler.total_seen() as f64;

    match stat {
        "count" => {
            // Estimate total count from sample rate
            Ok(total_seen)
        }
        "sum" => {
            let sample_sum: f64 = samples.iter().sum();
            // Scale up by (total_seen / n_samples) to estimate population sum
            Ok(sample_sum * (total_seen / n))
        }
        "sum2" => {
            let sample_sum2: f64 = samples.iter().map(|x| x * x).sum();
            Ok(sample_sum2 * (total_seen / n))
        }
        "avg" => {
            let sample_sum: f64 = samples.iter().sum();
            Ok(sample_sum / n)
        }
        "stddev" => {
            let mean = samples.iter().sum::<f64>() / n;
            let variance = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
            Ok(variance.sqrt())
        }
        "stdvar" => {
            let mean = samples.iter().sum::<f64>() / n;
            let variance = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
            Ok(variance)
        }
        _ => Err(format!("unknown sampling stat: {}", stat).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stores::promsketch_store::config::PromSketchConfig;
    use crate::stores::promsketch_store::series::PromSketchMemSeries;
    use crate::stores::promsketch_store::PromSketchType;

    fn create_test_series_with_kll_data() -> PromSketchMemSeries {
        let config = PromSketchConfig::default();
        let mut series = PromSketchMemSeries::new("test".to_string());
        series
            .sketch_instances
            .ensure_initialized(PromSketchType::EHKLL, &config);

        // Insert values 1..=100 at successive timestamps
        for i in 1..=100u64 {
            let input = SketchInput::F64(i as f64);
            if let Some(ref mut eh) = series.sketch_instances.eh_kll {
                eh.update(i, &input);
            }
        }
        series
    }

    fn create_test_series_with_sampling_data() -> PromSketchMemSeries {
        let config = PromSketchConfig::default();
        let mut series = PromSketchMemSeries::new("test".to_string());
        series
            .sketch_instances
            .ensure_initialized(PromSketchType::USampling, &config);

        for i in 1..=1000u64 {
            let input = SketchInput::F64(i as f64);
            if let Some(ref mut eh) = series.sketch_instances.eh_sampling {
                eh.update(i, &input);
            }
        }
        series
    }

    fn create_test_series_with_univmon_data() -> PromSketchMemSeries {
        let config = PromSketchConfig::default();
        let mut series = PromSketchMemSeries::new("test".to_string());
        series
            .sketch_instances
            .ensure_initialized(PromSketchType::EHUniv, &config);

        for i in 1..=100u64 {
            let input = SketchInput::F64(i as f64);
            if let Some(ref mut eh) = series.sketch_instances.eh_univ {
                eh.update(i, &input, 1);
            }
        }
        series
    }

    #[test]
    fn test_eval_kll_quantile() {
        let series = create_test_series_with_kll_data();
        let result = eval_function("quantile_over_time", &series, 0.5, 1, 100);
        assert!(result.is_ok());
        let val = result.unwrap();
        // Median of 1..100 should be around 50
        assert!(val > 30.0 && val < 70.0, "median was {}", val);
    }

    #[test]
    fn test_eval_min_max() {
        let series = create_test_series_with_kll_data();

        let min_result = eval_function("min_over_time", &series, 0.0, 1, 100);
        assert!(min_result.is_ok());
        let min_val = min_result.unwrap();
        assert!(min_val <= 5.0, "min was {}", min_val);

        let max_result = eval_function("max_over_time", &series, 0.0, 1, 100);
        assert!(max_result.is_ok());
        let max_val = max_result.unwrap();
        assert!(max_val >= 95.0, "max was {}", max_val);
    }

    #[test]
    fn test_eval_sampling_avg() {
        let series = create_test_series_with_sampling_data();
        let result = eval_function("avg_over_time", &series, 0.0, 1, 1000);
        assert!(result.is_ok());
        let val = result.unwrap();
        // avg of 1..1000 should be around 500.5
        assert!(
            val > 300.0 && val < 700.0,
            "avg was {} (expected ~500.5)",
            val
        );
    }

    #[test]
    fn test_eval_sampling_count() {
        let series = create_test_series_with_sampling_data();
        let result = eval_function("count_over_time", &series, 0.0, 1, 1000);
        assert!(result.is_ok());
        let val = result.unwrap();
        // total_seen should be 1000
        assert!(
            val > 500.0 && val <= 1000.0,
            "count was {} (expected ~1000)",
            val
        );
    }

    #[test]
    fn test_eval_univmon_entropy() {
        let series = create_test_series_with_univmon_data();
        let result = eval_function("entropy_over_time", &series, 0.0, 1, 100);
        assert!(result.is_ok());
        // UnivMon entropy with small data can be 0; verify query dispatches correctly
        let val = result.unwrap();
        assert!(val >= 0.0, "entropy was {}", val);
    }

    #[test]
    fn test_unsupported_function() {
        let series = PromSketchMemSeries::new("test".to_string());
        let result = eval_function("nonexistent_func", &series, 0.0, 1, 100);
        assert!(result.is_err());
    }
}
