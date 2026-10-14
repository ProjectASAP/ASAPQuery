//! Metrics from a sketch-bench workload table's `workloads[0].e2e.metrics`,
//! and the facts check that the generated series match their description.
//!
//! A label has `cardinality` values, or, with `child_of`, that many under each
//! value of its parent. A root label's values are `0`, `1`, …; a child's are
//! its parent's value, a `.`, and the child index (`job="3"`,
//! `label_0="3.17"`), so a child value names its parent and is unique across
//! parents. Labels without `child_of` form a cross product, and the metric
//! has the product of all cardinalities as series.

use crate::workload::{splitmix64, Kind, LabelSpec, Metric, Workload};
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

#[derive(Deserialize)]
struct Table {
    workloads: Vec<TableWorkload>,
}

#[derive(Deserialize)]
struct TableWorkload {
    e2e: E2e,
}

#[derive(Deserialize)]
struct E2e {
    metrics: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricSpec {
    #[serde(rename = "type")]
    kind: String,
    labels: Vec<LabelSpec>,
    samples_per_sec_per_series: u32,
    values: Value,
}

/// Zipf(`zipf`) skew over `keys` keys; a key is a series.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeySkew {
    zipf: f64,
    keys: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GaugeValues {
    value: KeySkew,
    dist: ParetoDist,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ParetoDist {
    pareto: f64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CounterValues {
    increment: KeySkew,
}

/// The metrics of `workloads[0].e2e.metrics` in the table at `path`, in name
/// order, or only `only`. `samples_per_sec` overrides every metric's rate.
pub fn load(
    path: &str,
    only: Option<&str>,
    samples_per_sec: Option<u32>,
    seed: u64,
) -> Result<Workload> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {path}"))?;
    let table: Table = serde_json::from_str(&text)
        .with_context(|| format!("{path}: no workloads[0].e2e.metrics"))?;
    let Some(first) = table.workloads.into_iter().next() else {
        bail!("{path}: `workloads` is empty");
    };
    let mut metrics = Vec::new();
    let mut rates = Vec::new();
    for (name, spec) in &first.e2e.metrics {
        if only.is_some_and(|only| only != name) {
            continue;
        }
        let (metric, rate) =
            metric(name, spec, seed).with_context(|| format!("metric `{name}`"))?;
        metrics.push(metric);
        rates.push(rate);
    }
    if let Some(only) = only {
        ensure!(!metrics.is_empty(), "{path}: no metric `{only}`");
    }
    ensure!(
        !metrics.is_empty(),
        "{path}: workloads[0].e2e.metrics is empty"
    );
    let samples_per_sec = match samples_per_sec {
        Some(rate) => rate,
        // One feeder sends every metric at one rate.
        None if rates.iter().all(|&r| r == rates[0]) => rates[0],
        None => bail!(
            "metrics have different samples_per_sec_per_series ({rates:?}); \
             send them with separate feeders (--metric) or set --samples-per-sec"
        ),
    };
    Ok(Workload {
        samples_per_sec,
        metrics,
    })
}

fn metric(name: &str, spec: &Value, seed: u64) -> Result<(Metric, u32)> {
    ensure!(
        is_identifier(name),
        "the name must match [A-Za-z_][A-Za-z0-9_]* (it is also the ClickHouse table)"
    );
    let kind = spec.get("type").and_then(Value::as_str).unwrap_or_default();
    if kind != "gauge" && kind != "counter" {
        bail!("type `{kind}` is not supported; the feeder generates `gauge` and `counter` metrics");
    }
    let spec: MetricSpec = serde_json::from_value(spec.clone())?;
    let series = series_labels(&spec.labels)?;
    let (skew, kind) = if spec.kind == "gauge" {
        let values: GaugeValues = serde_json::from_value(spec.values).context("gauge `values`")?;
        ensure!(
            values.dist.pareto > 0.0,
            "the Pareto shape must be positive"
        );
        let kind = Kind::Gauge {
            pareto_shape: values.dist.pareto,
            pareto_scale: 1.0,
        };
        (values.value, kind)
    } else {
        let values: CounterValues =
            serde_json::from_value(spec.values).context("counter `values`")?;
        (values.increment, Kind::Counter)
    };
    ensure!(
        skew.keys == series.len() as u64,
        "Zipf keys ({}) must equal the number of series ({}): each series is a key",
        skew.keys,
        series.len()
    );
    ensure!(skew.zipf >= 0.0, "the Zipf skew must be non-negative");
    // The ranks depend on the seed alone, so a key is equally heavy in every
    // metric of the table.
    let scales = zipf_scales(series.len(), skew.zipf, seed);
    let metric = Metric {
        name: name.to_string(),
        labels: spec.labels,
        series,
        scales,
        kind,
        seed: splitmix64(seed ^ fnv1a(name)),
    };
    Ok((metric, spec.samples_per_sec_per_series))
}

/// Each series' label values, in `labels` order. Series are numbered in mixed
/// radix with the last label varying fastest.
pub fn series_labels(labels: &[LabelSpec]) -> Result<Vec<Vec<String>>> {
    ensure!(!labels.is_empty(), "a metric needs at least one label");
    // Index of each label's parent, which must come earlier.
    let mut parents = Vec::with_capacity(labels.len());
    for (i, label) in labels.iter().enumerate() {
        let name = &label.name;
        ensure!(
            is_identifier(name) && !name.starts_with("__"),
            "label `{name}` must match [A-Za-z_][A-Za-z0-9_]* and not start with `__`"
        );
        ensure!(
            name != "ts" && name != "value",
            "label `{name}` clashes with a ClickHouse column"
        );
        ensure!(
            labels[..i].iter().all(|l| &l.name != name),
            "label `{name}` is repeated"
        );
        ensure!(label.cardinality > 0, "label `{name}` has cardinality 0");
        parents.push(match &label.child_of {
            None => None,
            Some(parent) => Some(
                labels[..i]
                    .iter()
                    .position(|l| &l.name == parent)
                    .with_context(|| {
                        format!("label `{name}`: child_of `{parent}` must name an earlier label")
                    })?,
            ),
        });
    }
    let num_series = labels
        .iter()
        .try_fold(1usize, |n, l| {
            n.checked_mul(usize::try_from(l.cardinality).ok()?)
        })
        .context("too many series")?;

    let mut series = Vec::with_capacity(num_series);
    let mut index = vec![0u64; labels.len()];
    for _ in 0..num_series {
        let mut values: Vec<String> = Vec::with_capacity(labels.len());
        for (i, parent) in parents.iter().enumerate() {
            values.push(match parent {
                None => index[i].to_string(),
                Some(p) => format!("{}.{}", values[*p], index[i]),
            });
        }
        series.push(values);
        // Advance the mixed-radix index, last label fastest.
        for i in (0..labels.len()).rev() {
            index[i] += 1;
            if index[i] < labels[i].cardinality {
                break;
            }
            index[i] = 0;
        }
    }
    Ok(series)
}

/// Each series' value scale for Zipf(`theta`) skew over the series: the
/// series of rank `r` (0 is the heaviest) gets `(r + 1)^-theta`, normalized so
/// the scales average 1. Ranks are a seeded shuffle of the series, so the
/// heavy series spread across parent labels instead of filling the first one.
pub fn zipf_scales(num_series: usize, theta: f64, seed: u64) -> Vec<f64> {
    let weights: Vec<f64> = (0..num_series)
        .map(|r| ((r + 1) as f64).powf(-theta))
        .collect();
    let norm = num_series as f64 / weights.iter().sum::<f64>();
    zipf_ranks(num_series, seed)
        .into_iter()
        .map(|r| weights[r] * norm)
        .collect()
}

/// A seeded permutation: the Zipf rank of each series (Fisher-Yates).
pub fn zipf_ranks(num_series: usize, seed: u64) -> Vec<usize> {
    let mut ranks: Vec<usize> = (0..num_series).collect();
    let mut state = splitmix64(seed ^ fnv1a("zipf ranks"));
    for i in (1..num_series).rev() {
        state = splitmix64(state);
        ranks.swap(i, (state % (i as u64 + 1)) as usize);
    }
    ranks
}

/// One row of the facts check: the distinct value tuples of a label subset.
#[derive(Debug, Clone, PartialEq)]
pub struct Fact {
    pub labels: Vec<String>,
    /// The product of the fan-outs of `labels` closed under `child_of`.
    pub expected: u64,
    /// Distinct tuples among the generated series.
    pub actual: u64,
}

/// For every non-empty subset of `labels`, the expected and the generated
/// number of distinct label-value tuples.
pub fn facts(labels: &[LabelSpec], series: &[Vec<String>]) -> Vec<Fact> {
    let n = labels.len();
    assert!(n < 32, "too many labels to enumerate their subsets");
    (1u32..1 << n)
        .map(|mask| {
            let subset: Vec<usize> = (0..n).filter(|i| mask & (1 << i) != 0).collect();
            let actual = series
                .iter()
                .map(|values| {
                    subset
                        .iter()
                        .map(|&i| values[i].as_str())
                        .collect::<Vec<_>>()
                })
                .collect::<HashSet<_>>()
                .len() as u64;
            Fact {
                labels: subset.iter().map(|&i| labels[i].name.clone()).collect(),
                expected: closed_cardinality(labels, &subset),
                actual,
            }
        })
        .collect()
}

/// The product of the cardinalities of `subset` and every ancestor of its
/// labels: a child value names its parent, so grouping by a child groups by
/// its ancestors too.
fn closed_cardinality(labels: &[LabelSpec], subset: &[usize]) -> u64 {
    let mut closed = HashSet::new();
    let mut todo: Vec<usize> = subset.to_vec();
    while let Some(i) = todo.pop() {
        if closed.insert(i) {
            if let Some(parent) = &labels[i].child_of {
                todo.extend(labels.iter().position(|l| &l.name == parent));
            }
        }
    }
    closed
        .iter()
        .map(|&i| labels[i].cardinality)
        .fold(1u64, u64::saturating_mul)
}

fn is_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// 64-bit FNV-1a, to derive a per-name seed.
fn fnv1a(s: &str) -> u64 {
    s.bytes().fold(0xcbf2_9ce4_8422_2325, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn label(name: &str, cardinality: u64, child_of: Option<&str>) -> LabelSpec {
        LabelSpec {
            name: name.to_string(),
            cardinality,
            child_of: child_of.map(str::to_string),
        }
    }

    fn classic_labels() -> Vec<LabelSpec> {
        vec![label("job", 10, None), label("label_0", 100, Some("job"))]
    }

    #[test]
    fn child_labels_fan_out_under_their_parent() {
        let series = series_labels(&classic_labels()).unwrap();
        assert_eq!(series.len(), 1_000);
        assert_eq!(series[0], ["0", "0.0"]);
        assert_eq!(series[1], ["0", "0.1"]);
        assert_eq!(series[100], ["1", "1.0"]);
        assert_eq!(series[999], ["9", "9.99"]);
        // A child value names its parent, so it is unique across parents.
        let children: HashSet<&str> = series.iter().map(|s| s[1].as_str()).collect();
        assert_eq!(children.len(), 1_000);
        assert!(series
            .iter()
            .all(|s| s[1].starts_with(&format!("{}.", s[0]))));
    }

    #[test]
    fn root_labels_form_a_cross_product() {
        let labels = vec![
            label("region", 2, None),
            label("service", 3, None),
            label("endpoint", 2, Some("service")),
        ];
        let series = series_labels(&labels).unwrap();
        assert_eq!(series.len(), 12);
        let pairs: HashSet<(&str, &str)> = series
            .iter()
            .map(|s| (s[0].as_str(), s[1].as_str()))
            .collect();
        assert_eq!(pairs.len(), 6);
        assert_eq!(series[11], ["1", "2", "2.1"]);
    }

    #[test]
    fn rejects_a_child_before_its_parent() {
        let labels = vec![label("label_0", 3, Some("job")), label("job", 2, None)];
        assert!(series_labels(&labels).is_err());
    }

    #[test]
    fn facts_hold_on_generated_series() {
        let labels = vec![
            label("region", 4, None),
            label("service", 5, None),
            label("endpoint", 3, Some("service")),
            label("status", 2, None),
        ];
        let series = series_labels(&labels).unwrap();
        let facts = facts(&labels, &series);
        assert_eq!(facts.len(), 15);
        assert!(facts.iter().all(|f| f.expected == f.actual), "{facts:?}");
        let by = |names: &[&str]| facts.iter().find(|f| f.labels == names).unwrap().expected;
        // `endpoint` alone groups by its service too.
        assert_eq!(by(&["endpoint"]), 15);
        assert_eq!(by(&["region", "endpoint"]), 60);
        assert_eq!(by(&["region", "service", "endpoint", "status"]), 120);
    }

    #[test]
    fn facts_catch_a_wrong_expectation() {
        let labels = classic_labels();
        let series = series_labels(&labels).unwrap();
        // Claiming label_0 is a cross product rather than a fan-out expects
        // 100 label_0 values, but the generated series have 1000.
        let wrong = vec![label("job", 10, None), label("label_0", 100, None)];
        let facts = facts(&wrong, &series);
        let label_0 = facts.iter().find(|f| f.labels == ["label_0"]).unwrap();
        assert_eq!((label_0.expected, label_0.actual), (100, 1_000));
    }

    #[test]
    fn zipf_scales_average_one_and_spread_the_heavy_series() {
        let scales = zipf_scales(1_000, 1.1, 42);
        let mean = scales.iter().sum::<f64>() / 1_000.0;
        assert!((mean - 1.0).abs() < 1e-9, "mean {mean}");

        let ranks = zipf_ranks(1_000, 42);
        let mut sorted = ranks.clone();
        sorted.sort_unstable();
        assert!(sorted.into_iter().eq(0..1_000));
        // Scales decrease with rank.
        let by_rank = |r: usize| scales[ranks.iter().position(|&x| x == r).unwrap()];
        assert!((1..1_000).all(|r| by_rank(r) < by_rank(r - 1)));
        // The 10 heaviest series fall in several of the 10 jobs (100 series
        // each), not all in the first.
        let jobs: HashSet<usize> = (0..1_000)
            .filter(|&s| ranks[s] < 10)
            .map(|s| s / 100)
            .collect();
        assert!(jobs.len() >= 5, "{jobs:?}");
        // The ranks depend on the seed.
        assert_ne!(ranks, zipf_ranks(1_000, 43));
    }

    fn write_table(metrics: Value) -> String {
        let path = std::env::temp_dir().join(format!(
            "paced-feeder-table-{}-{}.json",
            std::process::id(),
            fnv1a(&metrics.to_string())
        ));
        let table = json!({"workloads": [{"dataset": "x", "e2e": {"metrics": metrics}}]});
        std::fs::write(&path, table.to_string()).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn classic_metrics() -> Value {
        let labels = json!([{"name": "job", "cardinality": 2},
                            {"name": "label_0", "cardinality": 3, "child_of": "job"}]);
        let keys = json!({"zipf": 1.1, "keys": 6});
        json!({
            "data": {"type": "gauge", "labels": labels, "samples_per_sec_per_series": 4,
                     "values": {"value": keys, "dist": {"pareto": 2.0}}},
            "data_total": {"type": "counter", "labels": labels, "samples_per_sec_per_series": 4,
                           "values": {"increment": keys}},
        })
    }

    #[test]
    fn loads_gauge_and_counter_from_a_table() {
        let path = write_table(classic_metrics());
        let w = load(&path, None, None, 1).unwrap();
        assert_eq!(w.samples_per_sec, 4);
        assert_eq!(w.num_series(), 12);
        let (data, total) = (&w.metrics[0], &w.metrics[1]);
        assert_eq!(
            (data.name.as_str(), total.name.as_str()),
            ("data", "data_total")
        );
        assert_eq!(
            data.kind,
            Kind::Gauge {
                pareto_shape: 2.0,
                pareto_scale: 1.0
            }
        );
        assert_eq!(total.kind, Kind::Counter);
        // The same series is heavy in both, but the gauges' draws differ by metric.
        assert_eq!(data.scales, total.scales);
        assert_ne!(data.seed, total.seed);

        let only = load(&path, Some("data_total"), Some(2), 1).unwrap();
        assert_eq!(only.metrics.len(), 1);
        assert_eq!(only.samples_per_sec, 2);
        assert!(load(&path, Some("nope"), None, 1).is_err());
    }

    #[test]
    fn rejects_records_and_mismatched_keys() {
        let path = write_table(json!({"http": {"type": "records", "labels": [], "rate": 1}}));
        let err = format!("{:#}", load(&path, None, None, 1).unwrap_err());
        assert!(err.contains("type `records` is not supported"), "{err}");

        let mut metrics = classic_metrics();
        metrics["data"]["values"]["value"]["keys"] = json!(7);
        let path = write_table(metrics);
        let err = format!("{:#}", load(&path, Some("data"), None, 1).unwrap_err());
        assert!(err.contains("must equal the number of series"), "{err}");
    }
}
