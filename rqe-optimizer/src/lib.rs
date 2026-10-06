//! v1 brute-force solver for the RAQE -> sketch-deployment mapping problem.
//! `docs/rqe_sketch_deployment_v1.md` is the problem statement; doc comments
//! cite its section numbers.
//!
//! Three independent stages, so an ILP can replace [`enumerate`] alone:
//! [`candidates`] builds 𝒟 and per-RAQE eligibility (§3), [`enumerate`]
//! searches (§4), [`analytical_cost_model`] scores (§5).
//!
//! Every number the algorithm uses is caller-supplied, except the query
//! output size estimates in [`analytical_cost_model`].

pub mod analytical_cost_model;
pub mod autosketch;
pub mod candidates;
pub mod enumerate;
pub mod milp;
pub mod pareto;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub use aqpbm_core::{AtomicCostEntry, AtomicCostTable};

/// Whole milliseconds. Window alignment is expressed with exact `%` checks.
pub type Millis = u64;

/// Rates (CPU-sec/sec, samples/sec) stay per second.
pub(crate) fn secs(ms: Millis) -> f64 {
    ms as f64 / 1000.0
}

/// A group-by key, compared as a set (§3's `labels_i == labels_D` rule).
pub type LabelSet = BTreeSet<String>;

/// What an RAQE's statistic needs (§1). A variant becomes a candidate only
/// if it is in both `families()` and [`DEPLOYABLE_FAMILIES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Capability {
    SumOrCount,
    Min,
    Max,
    RateOrIncrease,
    Quantile,
    Cardinality,
    TopK,
}

impl Capability {
    pub const ALL: [Capability; 7] = [
        Capability::SumOrCount,
        Capability::Min,
        Capability::Max,
        Capability::RateOrIncrease,
        Capability::Quantile,
        Capability::Cardinality,
        Capability::TopK,
    ];

    /// `AtomicCostEntry.sketch` values serving this capability. These are
    /// sketch-bench *variant* strings, not algorithm names: fastpath and
    /// regularpath have different cost profiles, so `"cms"` would be
    /// ambiguous. Lists only what `export_rqe_optimizer_costs.sh` measures;
    /// an unmeasured name here is harmless, it just matches no row.
    pub fn families(self) -> &'static [&'static str] {
        match self {
            // Exact multi-subpopulation accumulators, not sketches. Sum also
            // serves count (sum of 1s). Min and max are separate, so a min
            // query never lands on, or shares, a max accumulator.
            Capability::SumOrCount => &["exact-sum"],
            Capability::Min => &["exact-min"],
            Capability::Max => &["exact-max"],
            Capability::RateOrIncrease => &["exact-increase"],
            Capability::Quantile => &["kll-percall", "dd"],
            // univmon-cardinality is registered under KeyedCardinality in
            // sketch-bench, but its ground truth (`KeyedCardinalityGT`) is
            // "count of keys with a nonzero total" -- plain distinct-key
            // count, same target as HLL. Second candidate, not a second
            // capability.
            Capability::Cardinality => &["hll", "univmon-cardinality"],
            Capability::TopK => &[
                "cms-heap-topk-fastpath-vector2d",
                "countsketch-heap-topk-fastpath-vector2d",
                "univmon-topk",
            ],
        }
    }

    /// The variants the MILP and AutoSketch plan with: the subset of
    /// [`Capability::families`] in [`DEPLOYABLE_FAMILIES`], or all of them
    /// when `allow_undeployable_families` (for studies; the plan can't be
    /// deployed).
    pub fn candidate_families(
        self,
        allow_undeployable_families: bool,
    ) -> impl Iterator<Item = &'static str> {
        self.families().iter().copied().filter(move |family| {
            allow_undeployable_families || DEPLOYABLE_FAMILIES.contains(family)
        })
    }
}

/// Variants ASAPQuery can deploy, each with the `AggregationType` its query
/// engine runs. Both sides use `asap_sketchlib` for the sketches.
pub const DEPLOYABLE_FAMILIES: &[&str] = &[
    "exact-sum",                       // MultipleSum
    "exact-min",                       // MultipleMinMax, sub_type "min"
    "exact-max",                       // MultipleMinMax, sub_type "max"
    "exact-increase",                  // MultipleIncrease
    "kll-percall",                     // DatasketchesKLL
    "hll",                             // HLL
    "cms-heap-topk-fastpath-vector2d", // CountMinSketchWithHeap
];

/// Facts about one metric, given as input. RAQEs and deployments on the same
/// metric read the same samples.
#[derive(Debug, Clone)]
pub struct MetricFacts {
    /// Every label the metric's series carry.
    pub labels: LabelSet,
    /// Each series yields one sample per scrape.
    pub scrape_interval_ms: Millis,
    /// `card(X)`: distinct value combinations of each label set `X` in use.
    /// Must include `labels` itself, whose cardinality is the series count.
    pub cardinality: BTreeMap<LabelSet, u64>,
    /// `(lo, hi)`: smallest and largest positive sample value, if known.
    /// Sizes DDSketch; without it, DDSketch keeps the measured memory.
    pub value_range: Option<(f64, f64)>,
}

impl MetricFacts {
    /// `λ`: samples/sec across all of the metric's series.
    pub fn arrival_rate_per_sec(&self) -> f64 {
        self.cardinality[&self.labels] as f64 / secs(self.scrape_interval_ms)
    }
}

/// [`MetricFacts`] keyed by metric name.
pub type WorkloadFacts = BTreeMap<String, MetricFacts>;

/// Every problem with `raqes` and the `facts` serving them. The rest of the
/// crate indexes `facts` without checks, so call this first on caller input.
pub fn validate_facts(raqes: &[Raqe], facts: &WorkloadFacts) -> Result<(), Vec<String>> {
    let mut problems = BTreeSet::new();
    for raqe in raqes {
        let id = &raqe.id;
        if raqe.lookback_ms == 0 || raqe.interval_ms == 0 {
            problems.insert(format!("{id}: lookback and interval must be nonzero"));
        }
        if !raqe.spatial_filter.is_empty() {
            problems.insert(format!("{id}: spatial filters are not supported yet"));
        }
        if let Some(limit) = raqe.latency_sla_ms {
            if !(limit.is_finite() && limit > 0.0) {
                problems.insert(format!("{id}: latency SLA {limit} ms is not positive"));
            }
        }
        let (metric, grouping) = (&raqe.metric, &raqe.grouping_labels);
        let Some(metric_facts) = facts.get(metric) else {
            problems.insert(format!("{metric}: no facts for this metric"));
            continue;
        };
        let all_labels = &metric_facts.labels;
        let cardinality = &metric_facts.cardinality;
        let scrape_ms = metric_facts.scrape_interval_ms;
        if scrape_ms == 0 {
            problems.insert(format!("{metric}: scrape interval is 0"));
        } else if !(raqe.lookback_ms.is_multiple_of(scrape_ms)
            && raqe.interval_ms.is_multiple_of(scrape_ms))
        {
            problems.insert(format!(
                "{id}: lookback {} ms and interval {} ms must be multiples of the {scrape_ms} ms scrape interval",
                raqe.lookback_ms, raqe.interval_ms
            ));
        }
        if let Some((lo, hi)) = metric_facts.value_range {
            if !(lo > 0.0 && hi >= lo && hi.is_finite()) {
                problems.insert(format!(
                    "{metric}: value range ({lo}, {hi}) needs 0 < lo <= hi < inf"
                ));
            }
        }
        if !grouping.is_subset(all_labels) {
            problems.insert(format!(
                "{metric}: grouping {grouping:?} is not a subset of its labels {all_labels:?}"
            ));
        }
        for labels in [grouping, all_labels] {
            match cardinality.get(labels) {
                None => problems.insert(format!("{metric}: no cardinality for {labels:?}")),
                Some(0) => problems.insert(format!("{metric}: cardinality of {labels:?} is 0")),
                Some(_) => false,
            };
        }
        if let (Some(&groups), Some(&series)) =
            (cardinality.get(grouping), cardinality.get(all_labels))
        {
            if groups > series {
                problems.insert(format!(
                    "{metric}: {grouping:?} has {groups} groups but only {series} series"
                ));
            }
        }
    }
    if problems.is_empty() {
        Ok(())
    } else {
        Err(problems.into_iter().collect())
    }
}

/// Which way an accuracy metric runs. Most comparators report an error
/// (lower better); top-k reports `precision_at_k`/`recall_at_k` (higher
/// better), so its tolerance is a floor.
///
/// Named per RAQE, never inferred from the metric name: an unanticipated name
/// would get a silent wrong default, and the failure mode is a hard
/// constraint accepting exactly what it should reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccuracyDirection {
    /// `measured <= tolerance` passes -- the metric is an error.
    LowerIsBetter,
    /// `measured >= tolerance` passes -- the metric is a score.
    HigherIsBetter,
}

/// One repeating query expression (§1).
#[derive(Debug, Clone)]
pub struct Raqe {
    pub id: String,
    pub capability: Capability,
    /// `L_i`.
    pub lookback_ms: Millis,
    /// `T_i`: how often this RAQE is queried.
    pub interval_ms: Millis,
    pub metric: String,
    /// Series selector beyond the metric name, as canonical PromQL matcher
    /// text; empty for none. Part of stream identity, compared as an opaque
    /// string.
    // ponytail: validate_facts rejects non-empty filters until facts carry
    // per-filter cardinality; without it a filtered stream is costed as the
    // whole metric.
    pub spatial_filter: String,
    /// `G`: the query's group-by labels.
    pub grouping_labels: LabelSet,
    /// Which `AtomicCostEntry::query_accuracy` key to check. Comparators
    /// name their metrics differently per capability, so the RAQE picks.
    pub accuracy_metric: String,
    /// `tol_i`: hard constraint. Read as a ceiling or a floor depending on
    /// [`Raqe::accuracy_direction`].
    pub accuracy_sla: f64,
    pub accuracy_direction: AccuracyDirection,
    /// Hard ceiling on modeled query latency; `None` for no limit. Only the
    /// MILP enforces it.
    pub latency_sla_ms: Option<f64>,
}

impl Raqe {
    /// The one place comparison direction is decided -- both the
    /// candidate prefilter and eligibility route here so they can't drift.
    pub fn accuracy_ok(&self, value: f64) -> bool {
        match self.accuracy_direction {
            AccuracyDirection::LowerIsBetter => value <= self.accuracy_sla,
            AccuracyDirection::HigherIsBetter => value >= self.accuracy_sla,
        }
    }

    /// Whether `config` measured this RAQE's metric and clears it. Missing
    /// is not passing.
    pub fn accuracy_ok_for(&self, config: &AtomicCostEntry) -> bool {
        config
            .query_accuracy
            .get(&self.accuracy_metric)
            .is_some_and(|&v| self.accuracy_ok(v))
    }
}

/// A candidate deployment (§3): one configuration, one grouped stream, and a
/// sliding sketch window. Retention is an execution/storage concern, not an
/// optimizer candidate dimension.
#[derive(Debug, Clone, PartialEq)]
pub struct Deployment {
    pub capability: Capability,
    pub metric: String,
    pub spatial_filter: String,
    /// `G`: one instance per group, per window.
    pub grouping_labels: LabelSet,
    pub config: AtomicCostEntry,
    /// `x`: materialized sketch-window size.
    pub window_ms: Millis,
    /// `y`: materialized sketch slide.
    pub slide_ms: Millis,
}

impl Deployment {
    /// Number of active overlapping instances receiving each item.
    pub fn active_instance_count(&self) -> Option<u64> {
        if self.slide_ms == 0 || !self.window_ms.is_multiple_of(self.slide_ms) {
            return None;
        }
        Some(self.window_ms / self.slide_ms)
    }

    /// Number of non-overlapping instances merged for one query window.
    pub fn query_instance_count(&self, lookback_ms: Millis) -> Option<u64> {
        if self.window_ms == 0 || !lookback_ms.is_multiple_of(self.window_ms) {
            return None;
        }
        Some(lookback_ms / self.window_ms)
    }

    /// Closed instances kept to serve a lookback: those wholly inside it,
    /// which start in `[now − L, now − x]`, so `(L − x) / y + 1`.
    pub fn closed_instance_count(&self, lookback_ms: Millis) -> Option<u64> {
        self.query_instance_count(lookback_ms)?;
        self.active_instance_count()?;
        Some((lookback_ms - self.window_ms) / self.slide_ms + 1)
    }
}

/// A full mapping: one deployment index (into the `deployments` slice passed
/// to `enumerate::brute_force`) per RAQE, aligned with the `raqes` slice's
/// order. `z_{i,D} = 1 <=> mapping[i] == D`'s index; `u_D = 1 <=>` `D`'s
/// index appears anywhere in `mapping` (§4) -- `y` is never stored
/// separately, it's always derived from `mapping`.
pub type Mapping = Vec<usize>;

/// Fixtures shared by the unit tests: one metric `m`, grouped by nothing,
/// served by top-k.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub const METRIC: &str = "m";

    /// `groups` groups and `series` raw series scraped every second, so
    /// `λ = series`. Not validated: unit tests set `groups` freely.
    pub fn facts(groups: u64, series: u64) -> WorkloadFacts {
        let labels = LabelSet::from(["series".to_string()]);
        WorkloadFacts::from([(
            METRIC.to_string(),
            MetricFacts {
                labels: labels.clone(),
                scrape_interval_ms: 1_000,
                cardinality: BTreeMap::from([(LabelSet::new(), groups), (labels, series)]),
                value_range: None,
            },
        )])
    }

    pub fn raqe(lookback_ms: Millis, interval_ms: Millis) -> Raqe {
        Raqe {
            id: "r".into(),
            capability: Capability::TopK,
            lookback_ms,
            interval_ms,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            accuracy_metric: "err".into(),
            accuracy_sla: 1.0,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
            latency_sla_ms: None,
        }
    }

    /// Per-instance memory and insert/merge/query CPU, then window and slide.
    pub fn deployment(
        memory: f64,
        insert: f64,
        merge: f64,
        query: f64,
        window_ms: Millis,
        slide_ms: Millis,
    ) -> Deployment {
        Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: AtomicCostEntry {
                sketch: "cms-heap-topk-fastpath-vector2d".into(),
                sketch_config: serde_json::json!(null),
                mem_bytes_per_instance: memory,
                insert_cpu_secs: insert,
                merge_cpu_secs: merge,
                query_cpu_secs: query,
                query_accuracy: BTreeMap::from([("err".into(), 0.0)]),
            },
            window_ms,
            slide_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_support::{deployment, raqe, METRIC};

    fn labels(names: &[&str]) -> LabelSet {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn every_deployable_family_serves_a_capability() {
        for family in DEPLOYABLE_FAMILIES {
            assert!(
                Capability::ALL
                    .iter()
                    .any(|c| c.families().contains(family)),
                "{family} serves no capability"
            );
        }
    }

    #[test]
    fn closed_instances_lie_wholly_inside_the_lookback() {
        // 10-minute windows every minute, over an hour: starts in minutes
        // [now − 60, now − 10].
        let sliding = deployment(1.0, 1.0, 1.0, 1.0, 600_000, 60_000);
        assert_eq!(sliding.closed_instance_count(3_600_000), Some(51));
        assert_eq!(sliding.closed_instance_count(600_000), Some(1));
        assert_eq!(sliding.closed_instance_count(900_000), None); // not tiled by x
    }

    #[test]
    fn arrival_rate_is_series_over_scrape_interval() {
        let metric_facts = MetricFacts {
            scrape_interval_ms: 15_000,
            ..test_support::facts(4, 600)[METRIC].clone()
        };
        assert_eq!(metric_facts.arrival_rate_per_sec(), 40.0);
    }

    #[test]
    fn valid_facts_pass() {
        let facts = WorkloadFacts::from([(
            METRIC.to_string(),
            MetricFacts {
                labels: labels(&["service", "endpoint"]),
                scrape_interval_ms: 15_000,
                cardinality: BTreeMap::from([
                    (labels(&["service"]), 5),
                    (labels(&["service", "endpoint"]), 50),
                ]),
                value_range: None,
            },
        )]);
        let r = Raqe {
            grouping_labels: labels(&["service"]),
            ..raqe(60_000, 60_000)
        };
        assert_eq!(validate_facts(&[r], &facts), Ok(()));
    }

    #[test]
    fn reports_every_problem_at_once() {
        let facts = WorkloadFacts::from([(
            METRIC.to_string(),
            MetricFacts {
                labels: labels(&["service"]),
                scrape_interval_ms: 0,
                cardinality: BTreeMap::from([(labels(&["service"]), 0)]),
                value_range: Some((0.0, 1.0)),
            },
        )]);
        let raqes = [
            Raqe {
                metric: "missing".into(),
                ..raqe(60_000, 60_000)
            },
            Raqe {
                grouping_labels: labels(&["pod"]),
                ..raqe(60_000, 60_000)
            },
            Raqe {
                id: "filtered".into(),
                grouping_labels: labels(&["service"]),
                spatial_filter: r#"{job="api"}"#.into(),
                latency_sla_ms: Some(f64::NAN),
                ..raqe(60_000, 60_000)
            },
        ];
        let problems = validate_facts(&raqes, &facts).unwrap_err();
        let has = |needle: &str| problems.iter().any(|p| p.contains(needle));
        assert!(has("missing: no facts"));
        assert!(has("scrape interval is 0"));
        assert!(has("not a subset"));
        assert!(has("no cardinality for {\"pod\"}"));
        assert!(has("cardinality of {\"service\"} is 0"));
        assert!(has("filtered: spatial filters are not supported"));
        assert!(has("filtered: latency SLA NaN ms is not positive"));
        assert!(has("value range (0, 1) needs 0 < lo <= hi < inf"));
        assert_eq!(problems.len(), 8, "{problems:?}");
    }

    #[test]
    fn rejects_zero_and_scrape_unaligned_times() {
        // 1 s scrape. 3_600 looks like seconds left unconverted.
        let raqes = [
            Raqe {
                id: "zero".into(),
                ..raqe(0, 60_000)
            },
            Raqe {
                id: "unaligned".into(),
                ..raqe(3_600, 60_000)
            },
        ];
        let problems = validate_facts(&raqes, &test_support::facts(1, 1)).unwrap_err();
        assert_eq!(problems.len(), 2, "{problems:?}");
        assert!(problems[0].starts_with("unaligned: lookback 3600 ms"));
        assert!(problems[1].starts_with("zero: lookback and interval must be nonzero"));
    }

    #[test]
    fn rejects_more_groups_than_series() {
        let facts = WorkloadFacts::from([(
            METRIC.to_string(),
            MetricFacts {
                labels: labels(&["service", "endpoint"]),
                scrape_interval_ms: 15_000,
                cardinality: BTreeMap::from([
                    (labels(&["service"]), 60),
                    (labels(&["service", "endpoint"]), 50),
                ]),
                value_range: None,
            },
        )]);
        let r = Raqe {
            grouping_labels: labels(&["service"]),
            ..raqe(60_000, 60_000)
        };
        let problems = validate_facts(&[r], &facts).unwrap_err();
        assert!(problems[0].contains("60 groups but only 50 series"));
    }
}
