//! The `AggregationConfig`-based build specification: each `AggregationConfig`
//! is one windowed build, and `RoutingConfigSet` routes series to them by
//! metric name and spatial filter.

use crate::data_model::{AggregateCore, AggregationType, KeyByLabelValues};
use crate::precompute_engine::accumulator_factory::{
    create_accumulator_updater, AccumulatorUpdater,
};
use crate::precompute_engine::build_spec::{SeriesRouting, WindowedBuild};
use crate::precompute_engine::worker::{
    extract_metric_name, parse_labels_from_series_key, PipelineWorker,
};
use crate::precompute_operators::delta_set_aggregator_accumulator::DeltaSetAggregatorAccumulator;
use asap_types::aggregation_config::AggregationConfig;
use asap_types::streaming_config::StreamingConfig;
use promql_parser::label::Matcher;
use promql_parser::parser::{parse, Expr};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;

/// A worker running `AggregationConfig` builds. An alias rather than a
/// default type parameter so `Worker::new` infers without annotations.
pub type Worker = PipelineWorker<AggregationConfig>;

impl WindowedBuild for AggregationConfig {
    /// Key population of the previous non-empty DeltaSetAggregator window.
    /// DeltaSetAggregator outputs are differences between consecutive window
    /// populations, so this state lives at group scope rather than inside a
    /// single pane updater.
    type CrossWindowState = HashSet<KeyByLabelValues>;

    fn window_size_ms(&self) -> u64 {
        self.window_size_ms
    }

    fn slide_interval_ms(&self) -> u64 {
        self.slide_interval_ms
    }

    fn new_pane(
        &self,
    ) -> Result<Box<dyn AccumulatorUpdater>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(create_accumulator_updater(self)?)
    }

    fn apply_sample(
        &self,
        pane: &mut dyn AccumulatorUpdater,
        series_key: &str,
        value: f64,
        timestamp_ms: i64,
    ) {
        apply_sample(pane, series_key, value, timestamp_ms, self);
    }

    /// DeltaSetAggregator windows are stateful key deltas, so a late sample
    /// cannot be written as a standalone state.
    fn forwards_late_samples(&self) -> bool {
        self.aggregation_type != AggregationType::DeltaSetAggregator
    }

    fn finalize_window(
        &self,
        merged: Box<dyn AggregateCore>,
        state: &mut Self::CrossWindowState,
    ) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
        finalize_closed_accumulator(merged, self, state)
    }
}

/// Extract grouping label values from a series key string based on the
/// aggregation config's `grouping_labels`.
///
/// The series key format is: `metric_name{label1="val1",label2="val2",...}`
pub fn extract_key_from_series(series_key: &str, config: &AggregationConfig) -> KeyByLabelValues {
    let labels = parse_labels_from_series_key(series_key);
    let mut values = Vec::new();

    for label_name in &config.grouping_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(val.to_string());
        } else {
            values.push(String::new());
        }
    }

    KeyByLabelValues::new_with_labels(values)
}

/// Route a single sample to `updater`, dispatching keyed vs. non-keyed based on config.
///
/// For keyed accumulators (MultipleSum, CMS, HydraKLL), the key is extracted
/// from the series' **aggregated_labels** — these are the labels that become
/// the key dimension *inside* the sketch (e.g., which bucket in a CMS, which
/// entry in a MultipleSumAccumulator's HashMap). This matches the Arroyo SQL
/// pattern: `udf(concat_ws(';', aggregated_labels), value)`.
fn apply_sample(
    updater: &mut dyn AccumulatorUpdater,
    series_key: &str,
    val: f64,
    ts: i64,
    config: &AggregationConfig,
) {
    // Parse the series labels at most once and share them across value resolution
    // and key extraction — both walk the same label string. Skip parsing entirely
    // when neither path needs labels (non-keyed accumulator with no value_column),
    // which is the common SUM/quantile case on the per-sample ingest hot path.
    let keyed = updater.is_keyed();
    let labels = if keyed || config.value_column.is_some() {
        parse_labels_from_series_key(series_key)
    } else {
        HashMap::new()
    };

    let value = resolve_sample_value(&labels, val, config);
    if keyed {
        let key = extract_aggregated_key_from_series(&labels, config);
        updater.update_keyed(&key, value, ts);
    } else {
        updater.update_single(value, ts);
    }
}

/// Resolve which scalar to aggregate for a sample.
///
/// The wire format collapses each sample to a single scalar (`wire_val`) plus a
/// set of labels carried in `series_key`. `wire_val` is whatever the source
/// considered the default value column (e.g. `pkt_len` for the netflow dataset).
/// But an aggregation may target a *different* column via `config.value_column`
/// — most importantly `COUNT(DISTINCT dstip)`, where the HLL sketch must hash
/// `dstip`, not the wire `pkt_len`.
///
/// Rule (general, self-correcting): if `config.value_column` names a column that
/// is present among the series labels, aggregate that label's value; otherwise
/// fall back to `wire_val`. Numeric value columns such as `pkt_len` are never
/// sent as labels, so SUM / quantile / top-k transparently keep using the wire
/// scalar, while label-present distinct targets such as `dstip` get substituted.
///
/// `labels` is the already-parsed label map for the sample's series key (parsed
/// once by `apply_sample` and shared with key extraction), so this is allocation-
/// free on the hot path.
///
/// The label value is parsed as `f64`. This is lossless for the netflow IPv4
/// `u32` columns (≤ 2^32 < 2^53) and consistent with the replay's int→f64
/// convention; cardinality only requires distinct inputs to map to distinct
/// hashes, so the exact hash need not match the baseline engine.
///
/// # Panics
/// Panics if `value_column` names a label whose value is not numeric. Non-numeric
/// distinct targets (e.g. `proto`) are not yet supported; this is a temporary
/// guard until the path is refactored to return a `Result` (see follow-up for a
/// byte/string hashing path).
pub(crate) fn resolve_sample_value(
    labels: &HashMap<&str, &str>,
    wire_val: f64,
    config: &AggregationConfig,
) -> f64 {
    let Some(col) = config.value_column.as_deref() else {
        return wire_val;
    };

    let Some(raw) = labels.get(col) else {
        return wire_val;
    };

    match raw.parse::<f64>() {
        Ok(v) => v,
        // Non-numeric distinct targets (e.g. COUNT(DISTINCT proto)) are not yet
        // supported: silently falling back to the wire value would produce an
        // INCORRECT aggregate, so fail loudly instead. This panic is a temporary
        // measure — the longer-term fix is to make this path return a `Result`
        // and propagate the error up to the caller.
        Err(_) => panic!(
            "value_column '{col}' label value {raw:?} is not numeric; non-numeric distinct \
             targets (e.g. COUNT(DISTINCT proto)) are not yet supported"
        ),
    }
}

/// Extract aggregated label values from a series key string.
/// These are the labels that form the key dimension *inside* keyed accumulators
/// (MultipleSum, CMS, HydraKLL), matching Arroyo's `agg_columns`.
fn extract_aggregated_key_from_series(
    labels: &HashMap<&str, &str>,
    config: &AggregationConfig,
) -> KeyByLabelValues {
    let mut values = Vec::new();

    for label_name in &config.aggregated_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(val.to_string());
        } else {
            values.push(String::new());
        }
    }

    KeyByLabelValues::new_with_labels(values)
}

/// Convert a closed DeltaSetAggregator population into the stateful delta
/// format used by the Arroyo implementation: keys newly present in this
/// window go in `added`, and keys absent from this window go in `removed`.
///
/// The updater can only collect the current window's observed keys. The
/// previous population therefore belongs to the group's cross-window state,
/// which survives the per-pane updater lifecycle and is isolated for each
/// `(agg_id, group_key)`.
fn finalize_closed_accumulator(
    accumulator: Box<dyn AggregateCore>,
    config: &AggregationConfig,
    previous_delta_set_keys: &mut HashSet<KeyByLabelValues>,
) -> Result<Box<dyn AggregateCore>, Box<dyn std::error::Error + Send + Sync>> {
    if config.aggregation_type != AggregationType::DeltaSetAggregator {
        return Ok(accumulator);
    }

    let delta = accumulator
        .as_any()
        .downcast_ref::<DeltaSetAggregatorAccumulator>()
        .ok_or_else(|| {
            format!(
                "DeltaSetAggregator config produced {} instead of DeltaSetAggregatorAccumulator",
                accumulator.type_name()
            )
        })?;
    let current_keys: HashSet<KeyByLabelValues> = delta
        .get_keys()
        .ok_or("DeltaSetAggregator accumulator could not resolve its current keys")?
        .into_iter()
        .collect();

    let added = current_keys
        .difference(previous_delta_set_keys)
        .cloned()
        .collect();
    let removed = previous_delta_set_keys
        .difference(&current_keys)
        .cloned()
        .collect();
    *previous_delta_set_keys = current_keys;

    Ok(Box::new(DeltaSetAggregatorAccumulator::new_with_sets(
        added, removed,
    )))
}

/// Aggregation configuration enriched with its precompiled spatial filter.
#[derive(Clone)]
pub(crate) struct RoutingAggregationConfig {
    pub(crate) config: Arc<AggregationConfig>,
    spatial_matchers: Vec<Matcher>,
}

impl RoutingAggregationConfig {
    fn matches_spatial_filter(&self, labels: &HashMap<&str, &str>) -> bool {
        self.spatial_matchers.iter().all(|matcher| {
            let value = labels.get(matcher.name.as_str()).copied().unwrap_or("");
            matcher.is_match(value)
        })
    }
}

/// The complete, atomically swappable routing configuration for ingest.
/// The default routes nothing.
#[derive(Default)]
pub struct RoutingConfigSet {
    configs: Vec<RoutingAggregationConfig>,
}

impl RoutingConfigSet {
    pub(crate) fn from_streaming_config(config: &StreamingConfig) -> Result<Self, String> {
        Self::from_aggregation_configs(config.get_all_aggregation_configs().values().cloned())
    }

    pub(crate) fn from_aggregation_configs(
        configs: impl IntoIterator<Item = AggregationConfig>,
    ) -> Result<Self, String> {
        let configs = configs
            .into_iter()
            .map(|config| {
                let spatial_matchers = compile_spatial_filter(&config)?;
                Ok(RoutingAggregationConfig {
                    config: Arc::new(config),
                    spatial_matchers,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Self { configs })
    }

    pub(crate) fn configs(&self) -> &[RoutingAggregationConfig] {
        &self.configs
    }
}

impl SeriesRouting for RoutingConfigSet {
    type Build = AggregationConfig;

    fn route_series<F: FnMut(u64, String)>(&self, series_key: &str, mut emit: F) {
        let metric_name = extract_metric_name(series_key);
        let labels = parse_labels_from_series_key(series_key);
        for routing_config in &self.configs {
            let config = &routing_config.config;
            if (config.metric != metric_name && config.table_name.as_deref() != Some(metric_name))
                || !routing_config.matches_spatial_filter(&labels)
            {
                continue;
            }
            emit(config.aggregation_id, extract_group_key(&labels, config));
        }
    }

    fn log_routing_diagnostic(&self, series_key: &str) {
        let sample_metric = extract_metric_name(series_key);
        warn!(
            sample_metric,
            sample_labels = %series_key,
            num_agg_configs = self.configs.len(),
            "routing: first batch diagnostic"
        );
        for routing_config in &self.configs {
            let cfg = &routing_config.config;
            warn!(
                agg_id = cfg.aggregation_id,
                config_metric = %cfg.metric,
                config_spatial_filter = %cfg.spatial_filter,
                table_name = ?cfg.table_name,
                "routing: agg config metric"
            );
        }
    }
}

fn compile_spatial_filter(config: &AggregationConfig) -> Result<Vec<Matcher>, String> {
    // SQL aggregation configs do not use PromQL label selectors.
    if config.table_name.is_some() || config.spatial_filter.trim().is_empty() {
        return Ok(Vec::new());
    }

    let filter = config.spatial_filter.trim();
    let selector_body = if filter.starts_with('{') || filter.ends_with('}') {
        if !filter.starts_with('{') || !filter.ends_with('}') {
            return Err(format!(
                "aggregation_id {} has invalid spatialFilter {:?}: unmatched selector braces",
                config.aggregation_id, config.spatial_filter
            ));
        }
        filter.to_string()
    } else {
        format!("{{{filter}}}")
    };
    let selector = format!("{}{}", config.metric, selector_body);
    let Expr::VectorSelector(vector_selector) = parse(&selector).map_err(|error| {
        format!(
            "aggregation_id {} has invalid spatialFilter {:?}: {error}",
            config.aggregation_id, config.spatial_filter
        )
    })?
    else {
        return Err(format!(
            "aggregation_id {} has invalid spatialFilter {:?}: expected a vector selector",
            config.aggregation_id, config.spatial_filter
        ));
    };

    let matchers = vector_selector.matchers;
    if !matchers.or_matchers.is_empty() {
        return Err(format!(
            "aggregation_id {} spatialFilter must not use selector-level or",
            config.aggregation_id
        ));
    }
    let matchers = matchers.matchers;
    if matchers.iter().any(|matcher| matcher.name == "__name__") {
        return Err(format!(
            "aggregation_id {} spatialFilter must not match __name__; use metric instead",
            config.aggregation_id
        ));
    }
    Ok(matchers)
}

pub(crate) fn extract_group_key(
    labels: &HashMap<&str, &str>,
    config: &AggregationConfig,
) -> String {
    let mut values = Vec::new();
    for label_name in &config.grouping_labels.labels {
        if let Some(val) = labels.get(label_name.as_str()) {
            values.push(*val);
        } else {
            values.push("");
        }
    }
    values.join(";")
}
