//! `records` metrics of a workload table: a stream of records, each with a
//! value of every label and of every value field, e.g. sketch-bench's `http`
//! (`region`, `service`, `endpoint`, `status`; `latency`, `user_id`).
//!
//! Record `n` depends only on `(metric seed, n)`: every draw is a stateless
//! hash of the seed, the record index and the draw's stream (label `i` is
//! stream `i`, value field `j` is stream `labels + j`, the burst coin is
//! stream `labels + fields`). The data matches sketch-bench's
//! `label_shares()`:
//!
//! - **Labels.** Each label is drawn independently (a child given its parent)
//!   from Zipf(`skew`) over its `cardinality` values (a child's: under its
//!   parent), the value of index `i` having weight `(i+1)^-skew`, so `0` is the
//!   heaviest. Values are named as for gauges: `"3"`, or a child's `"3.17"`.
//! - **Keys** (`{"zipf": θ, "keys": K}`): the key of Zipf(θ) rank `r` over `K`
//!   keys is `r`.
//! - **Burst** (`"burst": {"label", "value", "share", "keys"}` on a keys field):
//!   with probability `share` a record is a burst record: its `label` is
//!   `value` (a child index for a child label) and its key is uniform over the
//!   `keys` burst keys `K, K+1, …`, disjoint from the background keys. Other
//!   records draw both as above, so `label`'s shares are Zipf mixed with the
//!   burst: `(1 - share)·p_i`, plus `share` at `value`.
//! - **Pareto** (`{"pareto": a}`): `PARETO_SCALE · U^(-1/a)`, times, with
//!   `"scaled_by": L`, the fixed factor of the record's value of `L`
//!   (log-uniform in `SCALE_RANGE`, from a hash of the value and the feeder's
//!   seed), times, with `"anomaly": {"label": L, "factor": f}`, `f` when the
//!   record's value of `L` is its first one (`0`, or `0.0` for a child: the
//!   heaviest). Scaling keeps the tail index `a`.

use crate::table::{closed_cardinality, fnv1a, is_identifier, label_parents, Fact};
use crate::workload::{splitmix64, unit_uniform, LabelSpec};
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::ops::Range;

/// Records are timestamped to 10 ms slots and spread evenly across them.
pub const SLOTS_PER_SEC: u32 = 100;
const SLOT_MS: i64 = 1_000 / SLOTS_PER_SEC as i64;
/// Scale of `pareto` values, as in sketch-bench's
/// `configs/datagen/hydra_http_latency.yaml` (`scale: 1000.0`).
pub const PARETO_SCALE: f64 = 1_000.0;
/// `[lo, hi]` of the `scaled_by` factors, as in the same spec's `scale_range`.
pub const SCALE_RANGE: [f64; 2] = [1.0, 10.0];
/// Keys are sent as `Float64`, which holds integers exactly up to 2^53.
const MAX_KEY: u64 = 1 << 53;
/// The most values a Zipf draws from (a label's values or a field's keys): it
/// keeps a CDF entry and a `u32` guide entry per value, 1.2 GB at this cap.
pub const MAX_ZIPF_VALUES: u64 = 100_000_000;
/// The facts check counts shares over this many records.
pub const SHARE_SAMPLE_RECORDS: u64 = 1_000_000;
/// A share's check fails if it is off by more than this many standard errors
/// of a share estimated from the sample, `sqrt(p(1-p)/n)`.
pub const SHARE_TOLERANCE_SIGMAS: f64 = 5.0;
/// The facts check shows each label's heaviest values.
const TOP_SHARES: usize = 3;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecordsSpec {
    #[serde(rename = "type")]
    _kind: String,
    labels: Vec<LabelSpec>,
    records_per_sec: u64,
    values: BTreeMap<String, FieldSpec>,
}

/// Either `zipf` and `keys` (with an optional `burst`), or `pareto` (with
/// optional `scaled_by` and `anomaly`).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FieldSpec {
    zipf: Option<f64>,
    keys: Option<u64>,
    burst: Option<BurstSpec>,
    rotating_pool: Option<RotatingPoolSpec>,
    pareto: Option<f64>,
    scaled_by: Option<String>,
    anomaly: Option<AnomalySpec>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RotatingPoolSpec {
    universe_keys: u64,
    keys_per_series: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BurstSpec {
    label: String,
    value: u64,
    share: f64,
    keys: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AnomalySpec {
    label: String,
    factor: f64,
}

/// Zipf over `n` values, value `i` with weight `(i+1)^-theta`, drawn by
/// inverse transform: the first value whose CDF reaches `u`. A guide table
/// narrows the binary search to the values whose CDF crosses `u`'s bucket, so
/// a draw over 1e6 keys touches a few cache lines instead of 20.
#[derive(Debug, Clone, PartialEq)]
pub struct Zipf {
    cdf: Vec<f64>,
    /// `guide[j]`: the first value whose CDF reaches `j / n`, for `j` in `0..=n`.
    guide: Vec<u32>,
}

/// Zipf(`theta`) shares of `n` values, the value of index `i` with weight
/// `(i+1)^-theta`.
pub fn zipf_shares(n: u64, theta: f64) -> Vec<f64> {
    let weights: Vec<f64> = (0..n).map(|i| ((i + 1) as f64).powf(-theta)).collect();
    let total: f64 = weights.iter().sum();
    weights.into_iter().map(|w| w / total).collect()
}

impl Zipf {
    pub fn new(n: u64, theta: f64) -> Self {
        assert!(n <= MAX_ZIPF_VALUES, "a Zipf over {n} values");
        let mut cdf: Vec<f64> = zipf_shares(n, theta)
            .into_iter()
            .scan(0.0, |sum, p| {
                *sum += p;
                Some(*sum)
            })
            .collect();
        // Rounding can leave the sum just under 1; any `u` must find a value.
        *cdf.last_mut().expect("a Zipf has values") = 1.0;
        let buckets = cdf.len();
        let mut guide = Vec::with_capacity(buckets + 1);
        let mut i = 0;
        for j in 0..=buckets {
            let bound = j as f64 / buckets as f64;
            while cdf[i] < bound {
                i += 1;
            }
            guide.push(i as u32);
        }
        Self { cdf, guide }
    }

    /// The value index for `u` in (0, 1].
    pub fn draw(&self, u: f64) -> u64 {
        // `u` is in bucket `j`: (j/n, (j+1)/n], so its value lies in
        // `guide[j]..=guide[j+1]`; one bucket more on each side absorbs
        // rounding in `u·n`.
        let buckets = self.cdf.len();
        let j = ((u * buckets as f64).ceil() as usize).clamp(1, buckets) - 1;
        let lo = self.guide[j.saturating_sub(1)] as usize;
        let hi = self.guide[(j + 2).min(buckets)] as usize;
        (lo + self.cdf[lo..=hi].partition_point(|&c| c < u)) as u64
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Burst {
    /// The label and value (a child index for a child label) of burst records.
    pub label: usize,
    pub value: u64,
    pub share: f64,
    /// The keys field and its number of burst keys, numbered after its keys.
    pub field: usize,
    pub keys: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum Field {
    Keys {
        zipf: Zipf,
        keys: u64,
    },
    RotatingPool {
        universe_keys: u64,
        keys_per_series: u64,
    },
    Pareto {
        shape: f64,
        /// The `scaled_by` label and each of its values' factor.
        scaled_by: Option<(usize, Vec<f64>)>,
        /// The anomaly label and factor; the anomalous value is the label's first.
        anomaly: Option<(usize, f64)>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordMetric {
    pub name: String,
    pub labels: Vec<LabelSpec>,
    /// Every value of each label, a child's across all parents: value `v` of a
    /// record is `label_values[label][v]`.
    pub label_values: Vec<Vec<String>>,
    parents: Vec<Option<usize>>,
    label_zipfs: Vec<Zipf>,
    /// Value field names, in name order.
    pub fields: Vec<String>,
    field_kinds: Vec<Field>,
    round_robin_series: bool,
    series_count: u64,
    pub burst: Option<Burst>,
    pub records_per_sec: u64,
    seed: u64,
}

/// Records `indices` of a metric, with event time from `start_ms`. Records
/// are generated as they are encoded, so each request's are generated by its
/// own task.
#[derive(Debug, Clone, PartialEq)]
pub struct Records {
    pub start_ms: i64,
    pub indices: Range<u64>,
}

impl Records {
    pub fn len(&self) -> usize {
        (self.indices.end - self.indices.start) as usize
    }
}

impl RecordMetric {
    /// A `records` metric from its table description. `records_per_sec`
    /// overrides the table's rate; `seed` is the feeder's.
    pub fn from_table(
        name: &str,
        spec: &Value,
        records_per_sec: Option<u64>,
        seed: u64,
    ) -> Result<Self> {
        let spec: RecordsSpec = serde_json::from_value(spec.clone())?;
        let labels = spec.labels;
        let parents = label_parents(&labels)?;
        let label_index = |name: &str| -> Result<usize> {
            labels
                .iter()
                .position(|l| l.name == name)
                .with_context(|| format!("no label `{name}`"))
        };
        let mut label_zipfs = Vec::new();
        for label in &labels {
            let skew = label
                .skew
                .with_context(|| format!("label `{}` needs a `skew`", label.name))?;
            ensure!(skew >= 0.0, "label `{}`: skew must be ≥ 0", label.name);
            ensure!(
                label.cardinality <= MAX_ZIPF_VALUES,
                "label `{}`: cardinality {} is over the {MAX_ZIPF_VALUES} values a Zipf draws from",
                label.name,
                label.cardinality
            );
            label_zipfs.push(Zipf::new(label.cardinality, skew));
        }
        let label_values = label_values(&labels, &parents)?;

        ensure!(!spec.values.is_empty(), "a records metric needs values");
        let mut burst = None;
        let mut fields = Vec::new();
        let mut field_kinds = Vec::new();
        for (field, value) in spec.values {
            ensure!(
                is_identifier(&field) && field != "ts" && label_index(&field).is_err(),
                "value `{field}` must match [A-Za-z_][A-Za-z0-9_]* and not be `ts` or a label"
            );
            let kind = match value {
                FieldSpec {
                    zipf: Some(theta),
                    keys: Some(keys),
                    burst: field_burst,
                    rotating_pool: None,
                    pareto: None,
                    scaled_by: None,
                    anomaly: None,
                } => {
                    ensure!(theta >= 0.0, "value `{field}`: zipf must be ≥ 0");
                    ensure!(
                        keys > 0 && keys <= MAX_ZIPF_VALUES,
                        "value `{field}`: keys must be in [1, {MAX_ZIPF_VALUES}], the values a Zipf draws from"
                    );
                    if let Some(b) = field_burst {
                        ensure!(burst.is_none(), "at most one value may have a burst");
                        let label = label_index(&b.label)
                            .with_context(|| format!("value `{field}`: burst"))?;
                        ensure!(
                            b.value < labels[label].cardinality,
                            "value `{field}`: the burst value is not a value of `{}`",
                            b.label
                        );
                        ensure!(
                            b.share > 0.0 && b.share < 1.0,
                            "value `{field}`: the burst share must be in (0, 1)"
                        );
                        ensure!(
                            b.keys > 0 && keys.saturating_add(b.keys) <= MAX_KEY,
                            "value `{field}`: burst keys must be positive, and all keys ≤ 2^53"
                        );
                        burst = Some(Burst {
                            label,
                            value: b.value,
                            share: b.share,
                            field: fields.len(),
                            keys: b.keys,
                        });
                    }
                    Field::Keys {
                        zipf: Zipf::new(keys, theta),
                        keys,
                    }
                }
                FieldSpec {
                    zipf: None,
                    keys: None,
                    burst: None,
                    rotating_pool: Some(pool),
                    pareto: None,
                    scaled_by: None,
                    anomaly: None,
                } => {
                    ensure!(
                        pool.universe_keys > 0 && pool.keys_per_series > 0,
                        "value `{field}`: rotating pool sizes must be positive"
                    );
                    ensure!(
                        pool.keys_per_series <= pool.universe_keys,
                        "value `{field}`: rotating pool must fit in its universe"
                    );
                    ensure!(
                        pool.universe_keys <= MAX_KEY,
                        "value `{field}`: rotating pool keys must be ≤ 2^53"
                    );
                    Field::RotatingPool {
                        universe_keys: pool.universe_keys,
                        keys_per_series: pool.keys_per_series,
                    }
                }
                FieldSpec {
                    zipf: None,
                    keys: None,
                    burst: None,
                    rotating_pool: None,
                    pareto: Some(shape),
                    scaled_by,
                    anomaly,
                } => {
                    ensure!(shape > 0.0, "value `{field}`: pareto must be positive");
                    let scaled_by = match scaled_by {
                        None => None,
                        Some(by) => {
                            let label = label_index(&by)
                                .with_context(|| format!("value `{field}`: scaled_by"))?;
                            let factors = label_values[label]
                                .iter()
                                .map(|v| scale_factor(v, seed))
                                .collect();
                            Some((label, factors))
                        }
                    };
                    let anomaly = match anomaly {
                        None => None,
                        Some(a) => {
                            ensure!(
                                a.factor > 0.0,
                                "value `{field}`: the anomaly factor must be positive"
                            );
                            let label = label_index(&a.label)
                                .with_context(|| format!("value `{field}`: anomaly"))?;
                            Some((label, a.factor))
                        }
                    };
                    Field::Pareto {
                        shape,
                        scaled_by,
                        anomaly,
                    }
                }
                _ => bail!(
                    "value `{field}` must be either `zipf` and `keys` (and `burst`), \
                     or `pareto` (and `scaled_by`, `anomaly`)"
                ),
            };
            fields.push(field);
            field_kinds.push(kind);
        }
        let records_per_sec = records_per_sec.unwrap_or(spec.records_per_sec);
        ensure!(records_per_sec > 0, "records per second must be positive");
        let series_count = labels
            .iter()
            .try_fold(1u64, |count, label| count.checked_mul(label.cardinality))
            .context("too many record label combinations")?;
        let round_robin_series = field_kinds
            .iter()
            .any(|field| matches!(field, Field::RotatingPool { .. }));
        if round_robin_series {
            ensure!(
                records_per_sec % series_count == 0,
                "records per second must be a multiple of record label combinations for rotating pools"
            );
        }
        Ok(Self {
            name: name.to_string(),
            labels,
            label_values,
            parents,
            label_zipfs,
            fields,
            field_kinds,
            round_robin_series,
            series_count,
            burst,
            records_per_sec,
            seed: splitmix64(seed ^ fnv1a(name)),
        })
    }

    /// Record `index`: writes each label's value index to `labels` and each
    /// field's value to `values`.
    pub fn record(&self, index: u64, labels: &mut [u32], values: &mut [f64]) {
        let num_labels = self.labels.len() as u64;
        let u = |stream: u64| unit_uniform(self.seed, stream, index);
        let burst = self.burst.as_ref().filter(|b| {
            let stream = num_labels + self.fields.len() as u64;
            u(stream) <= b.share
        });
        if self.round_robin_series {
            let mut series = index % self.series_count;
            let mut children = vec![0u64; self.labels.len()];
            for i in (0..self.labels.len()).rev() {
                let cardinality = self.labels[i].cardinality;
                children[i] = series % cardinality;
                series /= cardinality;
            }
            for i in 0..self.labels.len() {
                let cardinality = self.labels[i].cardinality;
                let child = children[i];
                labels[i] = match self.parents[i] {
                    None => child,
                    Some(p) => u64::from(labels[p]) * cardinality + child,
                } as u32;
            }
        } else {
            for (i, zipf) in self.label_zipfs.iter().enumerate() {
                let child = match burst {
                    Some(b) if b.label == i => b.value,
                    _ => zipf.draw(u(i as u64)),
                };
                labels[i] = match self.parents[i] {
                    None => child,
                    Some(p) => u64::from(labels[p]) * self.labels[i].cardinality + child,
                } as u32;
            }
        }
        for (j, kind) in self.field_kinds.iter().enumerate() {
            let u = u(num_labels + j as u64);
            values[j] = match kind {
                Field::Keys { zipf, keys } => match burst {
                    // `u` in (0, 1] maps to 0..burst keys.
                    Some(b) if b.field == j => {
                        (keys + (u * b.keys as f64).ceil() as u64 - 1) as f64
                    }
                    _ => zipf.draw(u) as f64,
                },
                Field::RotatingPool {
                    universe_keys,
                    keys_per_series,
                } => {
                    let series = index % self.series_count;
                    let pool_start = splitmix64(self.seed ^ series) % universe_keys;
                    let rotation = (index / self.series_count) % keys_per_series;
                    ((pool_start + rotation) % universe_keys) as f64
                }
                Field::Pareto {
                    shape,
                    scaled_by,
                    anomaly,
                } => {
                    let mut value = PARETO_SCALE * u.powf(-1.0 / shape);
                    if let Some((label, factors)) = scaled_by {
                        value *= factors[labels[*label] as usize];
                    }
                    if let Some((label, factor)) = anomaly {
                        if labels[*label] == 0 {
                            value *= factor;
                        }
                    }
                    value
                }
            };
        }
    }

    /// The records of one-second tick `tick`, numbered from
    /// `tick · records_per_sec`, with event time starting at `start_ms`.
    pub fn tick(&self, tick: u64, start_ms: i64) -> Records {
        let first = tick * self.records_per_sec;
        Records {
            start_ms,
            indices: first..first + self.records_per_sec,
        }
    }

    /// The event time of record `index`: the end of its 10 ms slot, so a
    /// tick starting at `t` has its records in `(t, t + 1 s]`. A second's
    /// records are spread evenly over its slots, slot `s` holding offsets
    /// `[s · r / 100, (s + 1) · r / 100)`.
    pub fn timestamp_ms(&self, index: u64, start_ms: i64) -> i64 {
        let (tick, offset) = (index / self.records_per_sec, index % self.records_per_sec);
        let slot = (u64::from(SLOTS_PER_SEC) * (offset + 1) - 1) / self.records_per_sec;
        start_ms + tick as i64 * 1_000 + (slot as i64 + 1) * SLOT_MS
    }

    /// Each value's share of the records (a child's: given its parent), as
    /// sketch-bench's `label_shares()`: Zipf(`skew`), mixed with the burst.
    pub fn label_shares(&self, label: usize) -> Vec<f64> {
        let spec = &self.labels[label];
        let mut shares = zipf_shares(spec.cardinality, spec.skew.unwrap_or_default());
        if let Some(b) = self.burst.as_ref().filter(|b| b.label == label) {
            for p in &mut shares {
                *p *= 1.0 - b.share;
            }
            shares[b.value as usize] += b.share;
        }
        shares
    }

    /// For every non-empty subset of the labels, the number of label-value
    /// tuples a record can have (as for series: closed under `child_of`) and
    /// the distinct tuples among records `0..n`. A child's value index names
    /// its parent, so the tuples need not hold the ancestors.
    pub fn label_coverage(&self, n: u64) -> Vec<Fact> {
        let num_labels = self.labels.len();
        assert!(
            num_labels < 32,
            "too many labels to enumerate their subsets"
        );
        let (mut labels, mut values) = (vec![0u32; num_labels], vec![0.0; self.fields.len()]);
        let mut drawn = Vec::with_capacity(n as usize * num_labels);
        for index in 0..n {
            self.record(index, &mut labels, &mut values);
            drawn.extend_from_slice(&labels);
        }
        (1u32..1 << num_labels)
            .map(|mask| {
                let subset: Vec<usize> = (0..num_labels).filter(|i| mask & (1 << i) != 0).collect();
                let actual = drawn
                    .chunks(num_labels)
                    .map(|record| subset.iter().map(|&i| record[i]).collect::<Vec<u32>>())
                    .collect::<HashSet<_>>()
                    .len() as u64;
                Fact {
                    labels: subset
                        .iter()
                        .map(|&i| self.labels[i].name.clone())
                        .collect(),
                    expected: closed_cardinality(&self.labels, &subset),
                    actual,
                }
            })
            .collect()
    }

    /// The share checks of the facts check over records `0..n`: each label's
    /// heaviest values (a child's index pooled over parents, so its share given
    /// the parent), the burst value and the share of burst records.
    pub fn share_checks(&self, n: u64) -> ShareReport {
        let (num_labels, num_fields) = (self.labels.len(), self.fields.len());
        let mut counts: Vec<Vec<u64>> = self
            .labels
            .iter()
            .map(|l| vec![0; l.cardinality as usize])
            .collect();
        let (mut burst_records, mut burst_keys) = (0u64, HashSet::new());
        let (mut labels, mut values) = (vec![0u32; num_labels], vec![0.0; num_fields]);
        for index in 0..n {
            self.record(index, &mut labels, &mut values);
            for (i, &v) in labels.iter().enumerate() {
                counts[i][(u64::from(v) % self.labels[i].cardinality) as usize] += 1;
            }
            if let Some(b) = &self.burst {
                let Field::Keys { keys, .. } = self.field_kinds[b.field] else {
                    unreachable!("a burst is on a keys field");
                };
                let key = values[b.field] as u64;
                if key >= keys {
                    burst_records += 1;
                    burst_keys.insert(key);
                }
            }
        }
        let check = |label: String, value: String, expected: f64, count: u64| ShareCheck {
            label,
            value,
            expected,
            actual: count as f64 / n as f64,
            tolerance: SHARE_TOLERANCE_SIGMAS * (expected * (1.0 - expected) / n as f64).sqrt(),
        };
        let mut checks = Vec::new();
        for (i, label) in self.labels.iter().enumerate() {
            let shares = self.label_shares(i);
            let mut shown: Vec<usize> = (0..shares.len()).collect();
            shown.sort_by(|&a, &b| shares[b].total_cmp(&shares[a]).then(a.cmp(&b)));
            shown.truncate(TOP_SHARES);
            if let Some(b) = self.burst.as_ref().filter(|b| b.label == i) {
                if !shown.contains(&(b.value as usize)) {
                    shown.push(b.value as usize);
                }
            }
            for v in shown {
                let value = match &label.child_of {
                    None => v.to_string(),
                    Some(parent) => format!("*.{v} (given {parent})"),
                };
                checks.push(check(label.name.clone(), value, shares[v], counts[i][v]));
            }
        }
        let burst = self.burst.as_ref().map(|b| {
            checks.push(check(
                "burst".to_string(),
                "records".to_string(),
                b.share,
                burst_records,
            ));
            (burst_keys.len() as u64, b.keys)
        });
        ShareReport {
            records: n,
            checks,
            burst_keys: burst,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShareCheck {
    pub label: String,
    pub value: String,
    pub expected: f64,
    pub actual: f64,
    pub tolerance: f64,
}

impl ShareCheck {
    pub fn holds(&self) -> bool {
        (self.actual - self.expected).abs() <= self.tolerance
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShareReport {
    pub records: u64,
    pub checks: Vec<ShareCheck>,
    /// Distinct burst keys among the records, and the burst's keys.
    pub burst_keys: Option<(u64, u64)>,
}

/// Every value of each label: a root's `0`, `1`, …; a child's, for each parent
/// value in order, `parent.0`, `parent.1`, …
fn label_values(labels: &[LabelSpec], parents: &[Option<usize>]) -> Result<Vec<Vec<String>>> {
    let mut values: Vec<Vec<String>> = Vec::with_capacity(labels.len());
    for (label, parent) in labels.iter().zip(parents) {
        let children = 0..label.cardinality;
        let label_values: Vec<String> = match parent {
            None => children.map(|i| i.to_string()).collect(),
            Some(p) => values[*p]
                .iter()
                .flat_map(|parent| children.clone().map(move |i| format!("{parent}.{i}")))
                .collect(),
        };
        ensure!(
            label_values.len() <= u32::MAX as usize,
            "label `{}` has too many values",
            label.name
        );
        values.push(label_values);
    }
    Ok(values)
}

/// The fixed `scaled_by` factor of label value `value`, as aqpbm-datagen's
/// `scale_factor`: a SplitMix64 hash of the value's FNV-1a and the seed, as
/// `u` in [0, 1), mapped to `lo·(hi/lo)^u`.
pub fn scale_factor(value: &str, seed: u64) -> f64 {
    let [lo, hi] = SCALE_RANGE;
    let u = (splitmix64(fnv1a(value) ^ seed) >> 11) as f64 / (1u64 << 53) as f64;
    lo * (hi / lo).powf(u)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use serde_json::json;

    /// sketch-bench's `http` schema, as an e2e table describes it.
    pub fn http_spec() -> Value {
        json!({"type": "records", "records_per_sec": 2_000_000,
               "labels": [{"name": "region", "cardinality": 4, "skew": 0.5},
                          {"name": "service", "cardinality": 25, "skew": 1.1},
                          {"name": "endpoint", "cardinality": 25, "child_of": "service", "skew": 1.1},
                          {"name": "status", "cardinality": 4, "skew": 2.0}],
               "values": {"latency": {"pareto": 2.0, "scaled_by": "service",
                                      "anomaly": {"label": "endpoint", "factor": 10}},
                          "user_id": {"zipf": 0.8, "keys": 1_000_000}}})
    }

    /// sketch-bench's `flows` schema, as an e2e table describes it.
    pub fn flows_spec() -> Value {
        json!({"type": "records", "records_per_sec": 4_000_000,
               "labels": [{"name": "dst_subnet", "cardinality": 1000, "skew": 1.1},
                          {"name": "dst_port", "cardinality": 1000, "skew": 1.2},
                          {"name": "proto", "cardinality": 3, "skew": 0.0}],
               "values": {"src_ip": {"zipf": 0.0, "keys": 1_000_000,
                                     "burst": {"label": "dst_subnet", "value": 999,
                                               "share": 0.05, "keys": 10_000}}}})
    }

    fn metric(name: &str, spec: &Value) -> RecordMetric {
        RecordMetric::from_table(name, spec, None, 42).unwrap()
    }

    #[test]
    fn rotating_pool_cycles_each_leafs_overlapping_key_pool() {
        let spec = json!({
            "type": "records", "records_per_sec": 12,
            "labels": [
                {"name": "region", "cardinality": 2, "skew": 0.0},
                {"name": "leaf", "cardinality": 3, "child_of": "region", "skew": 0.0}
            ],
            "values": {"user_id": {"rotating_pool": {
                "universe_keys": 10, "keys_per_series": 4
            }}}
        });
        let metric = metric("events", &spec);
        let rows = records(&metric, 24);

        for (index, (labels, _)) in rows.iter().enumerate() {
            let series = index as u64 % 6;
            assert_eq!(labels[0], (series / 3) as u32);
            assert_eq!(labels[1], series as u32);
        }
        for series in 0..6 {
            let values: HashSet<u64> = rows
                .iter()
                .skip(series)
                .step_by(6)
                .map(|(_, values)| values[0] as u64)
                .collect();
            assert_eq!(values.len(), 4);
            assert!(values.iter().all(|&value| value < 10));
        }
    }

    /// Records `0..n` as (label value indices, values).
    fn records(m: &RecordMetric, n: u64) -> Vec<(Vec<u32>, Vec<f64>)> {
        (0..n)
            .map(|index| {
                let mut labels = vec![0; m.labels.len()];
                let mut values = vec![0.0; m.fields.len()];
                m.record(index, &mut labels, &mut values);
                (labels, values)
            })
            .collect()
    }

    fn within(actual: f64, expected: f64, n: usize) -> bool {
        let sigma = (expected * (1.0 - expected) / n as f64).sqrt();
        (actual - expected).abs() <= SHARE_TOLERANCE_SIGMAS * sigma
    }

    #[test]
    fn zipf_guide_table_draws_as_a_plain_cdf_search() {
        for (n, theta) in [(1, 0.0), (4, 2.0), (25, 1.1), (1_000, 0.0), (100_000, 0.8)] {
            let zipf = Zipf::new(n, theta);
            let plain = |u: f64| zipf.cdf.partition_point(|&c| c < u) as u64;
            // Hashed draws, every bucket boundary and the ends of (0, 1].
            let us = (0..20_000)
                .map(|i| unit_uniform(7, 0, i))
                .chain((1..=n.min(1_000)).map(|j| j as f64 / n as f64))
                .chain([f64::MIN_POSITIVE, 1e-12, 1.0 - 1e-16, 1.0]);
            for u in us {
                assert_eq!(zipf.draw(u), plain(u), "n={n} theta={theta} u={u}");
            }
        }
    }

    #[test]
    fn label_shares_match_sketch_bench() {
        // `label_shares()` of scripts/export_autosketch_eval_table.py.
        let http = metric("http", &http_spec());
        let round = |p: f64| (p * 1e5).round() / 1e5;
        let top3 = |m: &RecordMetric, l| -> Vec<f64> {
            m.label_shares(l)[..3].iter().map(|&p| round(p)).collect()
        };
        assert_eq!(top3(&http, 0), [0.35914, 0.25395, 0.20735]);
        assert_eq!(top3(&http, 1), [0.29841, 0.13922, 0.08912]);
        assert_eq!(top3(&http, 2), [0.29841, 0.13922, 0.08912]);
        assert_eq!(top3(&http, 3), [0.70244, 0.17561, 0.07805]);
        let flows = metric("flows", &flows_spec());
        assert_eq!(top3(&flows, 0), [0.17047, 0.07953, 0.05091]);
        assert_eq!(round(flows.label_shares(0)[999]), 0.05009);
        assert!((flows.label_shares(0).iter().sum::<f64>() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn labels_follow_their_shares_and_children_their_parent() {
        let http = metric("http", &http_spec());
        let n = 200_000;
        let records = records(&http, n);
        for (l, label) in http.labels.iter().enumerate() {
            let shares = http.label_shares(l);
            for (v, &expected) in shares.iter().enumerate().take(5) {
                let count = records
                    .iter()
                    .filter(|(labels, _)| labels[l] as u64 % label.cardinality == v as u64)
                    .count();
                let actual = count as f64 / n as f64;
                assert!(
                    within(actual, expected, n as usize),
                    "{}={v}: {actual} vs {expected}",
                    label.name
                );
            }
        }
        // A child value names its parent, and given a parent the children
        // follow their own Zipf shares.
        let (service, endpoint) = (1, 2);
        for (labels, _) in &records {
            let name = &http.label_values[endpoint][labels[endpoint] as usize];
            let parent = &http.label_values[service][labels[service] as usize];
            assert!(
                name.starts_with(&format!("{parent}.")),
                "{name} under {parent}"
            );
        }
        let under = |s: u32| -> Vec<u32> {
            records
                .iter()
                .filter(|(labels, _)| labels[service] == s)
                .map(|(labels, _)| labels[endpoint] % 25)
                .collect()
        };
        let shares = http.label_shares(endpoint);
        for s in [0, 3] {
            let children = under(s);
            for (i, &expected) in shares.iter().enumerate().take(3) {
                let actual = children.iter().filter(|&&c| c == i as u32).count() as f64
                    / children.len() as f64;
                assert!(
                    within(actual, expected, children.len()),
                    "service {s} endpoint {i}"
                );
            }
        }
    }

    #[test]
    fn burst_records_hit_the_burst_value_from_their_own_keys() {
        let flows = metric("flows", &flows_spec());
        let n = 400_000;
        let records = records(&flows, n);
        let (keys, burst_keys) = (1_000_000.0, 10_000.0);
        let burst: Vec<_> = records.iter().filter(|(_, v)| v[0] >= keys).collect();
        assert!(
            within(burst.len() as f64 / n as f64, 0.05, n as usize),
            "{}",
            burst.len()
        );
        assert!(burst.iter().all(|(labels, _)| labels[0] == 999));
        assert!(burst
            .iter()
            .all(|(_, v)| v[0] < keys + burst_keys && v[0].fract() == 0.0));
        // About 20,000 draws over 10,000 keys see 1 - e^-2 of them.
        let distinct: HashSet<u64> = burst.iter().map(|(_, v)| v[0] as u64).collect();
        assert!(
            (8_400..=8_900).contains(&distinct.len()),
            "{}",
            distinct.len()
        );
        // The background is uniform over its keys and every subnet.
        let background: Vec<_> = records.iter().filter(|(_, v)| v[0] < keys).collect();
        let mean = background.iter().map(|(_, v)| v[0]).sum::<f64>() / background.len() as f64;
        assert!((mean / keys - 0.5).abs() < 0.01, "{mean}");
        let subnet_999 = records.iter().filter(|(l, _)| l[0] == 999).count() as f64 / n as f64;
        assert!(
            within(subnet_999, flows.label_shares(0)[999], n as usize),
            "{subnet_999}"
        );
    }

    #[test]
    fn latency_is_scaled_by_service_and_the_anomaly_endpoint() {
        let http = metric("http", &http_spec());
        let (service, endpoint) = (1, 2);
        let factors: Vec<f64> = http.label_values[service]
            .iter()
            .map(|v| scale_factor(v, 42))
            .collect();
        assert!(factors.iter().all(|f| (1.0..=10.0).contains(f)));
        assert!(factors.windows(2).any(|w| w[0] != w[1]));
        assert_ne!(scale_factor("3", 42), scale_factor("3", 43));
        // Every latency is exactly the Pareto draw of its stream, times its
        // service's factor, times 10 on the first endpoint ("0.0").
        let latency_stream = http.labels.len() as u64;
        for (index, (labels, values)) in records(&http, 20_000).iter().enumerate() {
            let u = unit_uniform(http.seed, latency_stream, index as u64);
            let anomaly = if http.label_values[endpoint][labels[endpoint] as usize] == "0.0" {
                10.0
            } else {
                1.0
            };
            let expected =
                PARETO_SCALE * u.powf(-0.5) * factors[labels[service] as usize] * anomaly;
            assert_eq!(values[0], expected);
        }
    }

    #[test]
    fn keys_follow_zipf_ranks() {
        let http = metric("http", &http_spec());
        let n = 200_000;
        let user_ids: Vec<f64> = records(&http, n).iter().map(|(_, v)| v[1]).collect();
        assert!(user_ids.iter().all(|&k| k < 1e6 && k.fract() == 0.0));
        let shares = zipf_shares(1_000_000, 0.8);
        for key in [0.0, 1.0, 9.0] {
            let actual = user_ids.iter().filter(|&&k| k == key).count() as f64 / n as f64;
            assert!(
                within(actual, shares[key as usize], n as usize),
                "key {key}: {actual}"
            );
        }
    }

    #[test]
    fn a_tick_spreads_its_records_over_slots_ending_in_the_second() {
        // 250 records/s: slots of 2 or 3 records.
        let m = RecordMetric::from_table("http", &http_spec(), Some(250), 42).unwrap();
        let tick = m.tick(2, 10_000);
        assert_eq!(tick.indices, 500..750);
        let mut per_timestamp: BTreeMap<i64, usize> = BTreeMap::new();
        for index in tick.indices {
            *per_timestamp
                .entry(m.timestamp_ms(index, 10_000))
                .or_default() += 1;
        }
        // Slot s of tick 2 is at 12_000 + 10 · (s + 1): within (12 s, 13 s].
        assert!(per_timestamp
            .keys()
            .copied()
            .eq((1..=100).map(|s| 12_000 + 10 * s)));
        assert!(per_timestamp.values().all(|n| (2..=3).contains(n)));
        // The first record of the next tick starts the next second's slots.
        assert_eq!(m.timestamp_ms(750, 10_000), 13_010);
    }

    #[test]
    fn records_depend_only_on_seed_name_and_index() {
        let a = metric("http", &http_spec());
        assert_eq!(
            records(&a, 100),
            records(&metric("http", &http_spec()), 100)
        );
        // Another metric of the same shape draws other records.
        assert_ne!(
            records(&a, 100),
            records(&metric("data_1_http", &http_spec()), 100)
        );
        let reseeded = RecordMetric::from_table("http", &http_spec(), None, 43).unwrap();
        assert_ne!(records(&a, 100), records(&reseeded, 100));
    }

    #[test]
    fn label_coverage_counts_distinct_tuples_within_the_domain() {
        let coverage = metric("http", &http_spec()).label_coverage(200_000);
        assert_eq!(coverage.len(), 15);
        let by_labels = |names: &str| {
            let fact = coverage
                .iter()
                .find(|f| f.labels.join(",") == names)
                .unwrap();
            (fact.expected, fact.actual)
        };
        // Small domains are covered; an endpoint names its service.
        assert_eq!(by_labels("region"), (4, 4));
        assert_eq!(by_labels("endpoint"), (625, 625));
        assert_eq!(by_labels("service,endpoint"), (625, 625));
        // Heavy skew leaves some of the 10,000 full tuples unseen.
        let (expected, actual) = by_labels("region,service,endpoint,status");
        assert!(expected == 10_000 && actual < expected, "{actual}");
        assert!(coverage.iter().all(|f| f.actual <= f.expected));
    }

    #[test]
    fn share_checks_pass_and_catch_a_wrong_skew() {
        for spec in [http_spec(), flows_spec()] {
            let report = metric("m", &spec).share_checks(200_000);
            assert!(report.checks.iter().all(ShareCheck::holds), "{report:?}");
        }
        let report = metric("flows", &flows_spec()).share_checks(200_000);
        let burst = report.checks.iter().find(|c| c.label == "burst").unwrap();
        assert_eq!(burst.expected, 0.05);
        assert!(report
            .checks
            .iter()
            .any(|c| c.label == "dst_subnet" && c.value == "999"));
        // About 10,000 burst draws over 10,000 keys see 1 - 1/e of them.
        let (seen, keys) = report.burst_keys.unwrap();
        assert!((6_000..=6_600).contains(&seen) && keys == 10_000, "{seen}");

        // Records drawn with a flatter service skew than described.
        let mut wrong = metric("http", &http_spec());
        wrong.label_zipfs[1] = Zipf::new(25, 0.5);
        let report = wrong.share_checks(200_000);
        let service_0 = report
            .checks
            .iter()
            .find(|c| c.label == "service" && c.value == "0")
            .unwrap();
        assert!(!service_0.holds(), "{service_0:?}");
    }

    #[test]
    fn rejects_malformed_records_specs() {
        let err = |spec: Value| -> String {
            format!(
                "{:#}",
                RecordMetric::from_table("m", &spec, None, 1).unwrap_err()
            )
        };
        let mut spec = http_spec();
        spec["labels"][0].as_object_mut().unwrap().remove("skew");
        assert!(err(spec).contains("needs a `skew`"));

        let mut spec = http_spec();
        spec["values"]["latency"]["scaled_by"] = json!("nope");
        assert!(err(spec).contains("no label `nope`"));

        let mut spec = http_spec();
        spec["values"]["latency"]["zipf"] = json!(1.0);
        assert!(err(spec).contains("must be either"));

        let mut spec = flows_spec();
        spec["values"]["src_ip"]["burst"]["value"] = json!(1000);
        assert!(err(spec).contains("not a value of `dst_subnet`"));

        let mut spec = flows_spec();
        spec["values"]["dst_port"] = json!({"zipf": 0.0, "keys": 5});
        assert!(err(spec).contains("not be `ts` or a label"));

        // A Zipf's tables are capped; burst keys are drawn uniformly, so only
        // their numbering is.
        let mut spec = flows_spec();
        spec["values"]["src_ip"]["keys"] = json!(MAX_ZIPF_VALUES + 1);
        assert!(err(spec).contains("keys must be in [1, 100000000]"));
        let mut spec = http_spec();
        spec["labels"][0]["cardinality"] = json!(MAX_ZIPF_VALUES + 1);
        assert!(err(spec).contains("cardinality 100000001 is over"));
        let mut spec = flows_spec();
        spec["values"]["src_ip"]["burst"]["keys"] = json!(1u64 << 53);
        assert!(err(spec).contains("all keys ≤ 2^53"));

        assert!(RecordMetric::from_table("m", &http_spec(), Some(0), 1).is_err());
    }
}
