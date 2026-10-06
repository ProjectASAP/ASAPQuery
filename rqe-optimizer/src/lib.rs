//! v1 brute-force solver for the RQE -> sketch-deployment mapping problem.
//! `docs/rqe_sketch_deployment_v1.md` is the problem statement; doc comments
//! cite its section numbers.
//!
//! Three independent stages, so an ILP can replace [`enumerate`] alone:
//! [`candidates`] builds 𝒟 and per-RQE eligibility (§3), [`enumerate`]
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

/// Whole seconds. Window alignment is expressed with exact `%` checks.
pub type Seconds = u64;

/// A group-by key, compared as a set (§3's `labels_i == labels_D` rule).
pub type LabelSet = BTreeSet<String>;

/// What an RQE's statistic needs (§1). `families()` is the only place
/// capability scope is encoded; widening it means editing that match arm.
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
}

/// Facts about one metric, given as input. RQEs and deployments on the same
/// metric read the same samples.
#[derive(Debug, Clone)]
pub struct MetricFacts {
    /// Every label the metric's series carry.
    pub labels: LabelSet,
    /// Each series yields one sample per scrape.
    pub scrape_interval_secs: Seconds,
    /// `card(X)`: distinct value combinations of each label set `X` in use.
    /// Must include `labels` itself, whose cardinality is the series count.
    pub cardinality: BTreeMap<LabelSet, u64>,
}

impl MetricFacts {
    /// `λ`: samples/sec across all of the metric's series.
    pub fn arrival_rate_per_sec(&self) -> f64 {
        self.cardinality[&self.labels] as f64 / self.scrape_interval_secs as f64
    }
}

/// [`MetricFacts`] keyed by metric name.
pub type WorkloadFacts = BTreeMap<String, MetricFacts>;

/// Every problem with `facts` for serving `rqes`. The rest of the crate
/// indexes `facts` without checks, so call this first on caller input.
pub fn validate_facts(rqes: &[Rqe], facts: &WorkloadFacts) -> Result<(), Vec<String>> {
    let mut problems = BTreeSet::new();
    for rqe in rqes {
        let (metric, grouping) = (&rqe.metric, &rqe.grouping_labels);
        let Some(metric_facts) = facts.get(metric) else {
            problems.insert(format!("{metric}: no facts for this metric"));
            continue;
        };
        let all_labels = &metric_facts.labels;
        let cardinality = &metric_facts.cardinality;
        if metric_facts.scrape_interval_secs == 0 {
            problems.insert(format!("{metric}: scrape interval is 0"));
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
/// Named per RQE, never inferred from the metric name: an unanticipated name
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
pub struct Rqe {
    pub id: String,
    pub capability: Capability,
    /// `L_i`.
    pub lookback_secs: Seconds,
    /// `T_i`: how often this RQE is queried.
    pub interval_secs: Seconds,
    pub metric: String,
    /// `G`: the query's group-by labels.
    pub grouping_labels: LabelSet,
    /// Which `AtomicCostEntry::query_accuracy` key to check. Comparators
    /// name their metrics differently per capability, so the RQE picks.
    pub accuracy_metric: String,
    /// `tol_i`: hard constraint. Read as a ceiling or a floor depending on
    /// [`Rqe::accuracy_direction`].
    pub accuracy_tolerance: f64,
    pub accuracy_direction: AccuracyDirection,
}

impl Rqe {
    /// The one place comparison direction is decided -- both the
    /// candidate prefilter and eligibility route here so they can't drift.
    pub fn accuracy_ok(&self, value: f64) -> bool {
        match self.accuracy_direction {
            AccuracyDirection::LowerIsBetter => value <= self.accuracy_tolerance,
            AccuracyDirection::HigherIsBetter => value >= self.accuracy_tolerance,
        }
    }

    /// Whether `config` measured this RQE's metric and clears it. Missing
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
    /// `G`: one instance per group, per window.
    pub grouping_labels: LabelSet,
    pub config: AtomicCostEntry,
    /// `x`: materialized sketch-window size.
    pub window_secs: Seconds,
    /// `y`: materialized sketch slide.
    pub slide_secs: Seconds,
}

impl Deployment {
    /// Number of active overlapping instances receiving each item.
    pub fn active_instance_count(&self) -> Option<u64> {
        if self.slide_secs == 0 || !self.window_secs.is_multiple_of(self.slide_secs) {
            return None;
        }
        Some(self.window_secs / self.slide_secs)
    }

    /// Number of non-overlapping instances merged for one query window.
    pub fn query_instance_count(&self, lookback_secs: Seconds) -> Option<u64> {
        if self.window_secs == 0 || !lookback_secs.is_multiple_of(self.window_secs) {
            return None;
        }
        Some(lookback_secs / self.window_secs)
    }

    /// Closed instances kept to serve a lookback: those wholly inside it,
    /// which start in `[now − L, now − x]`, so `(L − x) / y + 1`.
    pub fn closed_instance_count(&self, lookback_secs: Seconds) -> Option<u64> {
        self.query_instance_count(lookback_secs)?;
        self.active_instance_count()?;
        Some((lookback_secs - self.window_secs) / self.slide_secs + 1)
    }
}

/// A full mapping: one deployment index (into the `deployments` slice passed
/// to `enumerate::brute_force`) per RQE, aligned with the `rqes` slice's
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
                scrape_interval_secs: 1,
                cardinality: BTreeMap::from([(LabelSet::new(), groups), (labels, series)]),
            },
        )])
    }

    pub fn rqe(lookback_secs: Seconds, interval_secs: Seconds) -> Rqe {
        Rqe {
            id: "r".into(),
            capability: Capability::TopK,
            lookback_secs,
            interval_secs,
            metric: METRIC.into(),
            grouping_labels: LabelSet::new(),
            accuracy_metric: "err".into(),
            accuracy_tolerance: 1.0,
            accuracy_direction: AccuracyDirection::LowerIsBetter,
        }
    }

    /// Per-instance memory and insert/merge/query CPU, then window and slide.
    pub fn deployment(
        memory: f64,
        insert: f64,
        merge: f64,
        query: f64,
        window_secs: Seconds,
        slide_secs: Seconds,
    ) -> Deployment {
        Deployment {
            capability: Capability::TopK,
            metric: METRIC.into(),
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
            window_secs,
            slide_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_support::{deployment, rqe, METRIC};

    fn labels(names: &[&str]) -> LabelSet {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn closed_instances_lie_wholly_inside_the_lookback() {
        // 10-minute windows every minute, over an hour: starts in minutes
        // [now − 60, now − 10].
        let sliding = deployment(1.0, 1.0, 1.0, 1.0, 600, 60);
        assert_eq!(sliding.closed_instance_count(3_600), Some(51));
        assert_eq!(sliding.closed_instance_count(600), Some(1));
        assert_eq!(sliding.closed_instance_count(900), None); // not tiled by x
    }

    #[test]
    fn arrival_rate_is_series_over_scrape_interval() {
        let metric_facts = MetricFacts {
            scrape_interval_secs: 15,
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
                scrape_interval_secs: 15,
                cardinality: BTreeMap::from([
                    (labels(&["service"]), 5),
                    (labels(&["service", "endpoint"]), 50),
                ]),
            },
        )]);
        let r = Rqe {
            grouping_labels: labels(&["service"]),
            ..rqe(60, 60)
        };
        assert_eq!(validate_facts(&[r], &facts), Ok(()));
    }

    #[test]
    fn reports_every_problem_at_once() {
        let facts = WorkloadFacts::from([(
            METRIC.to_string(),
            MetricFacts {
                labels: labels(&["service"]),
                scrape_interval_secs: 0,
                cardinality: BTreeMap::from([(labels(&["service"]), 0)]),
            },
        )]);
        let rqes = [
            Rqe {
                metric: "missing".into(),
                ..rqe(60, 60)
            },
            Rqe {
                grouping_labels: labels(&["pod"]),
                ..rqe(60, 60)
            },
        ];
        let problems = validate_facts(&rqes, &facts).unwrap_err();
        let has = |needle: &str| problems.iter().any(|p| p.contains(needle));
        assert!(has("missing: no facts"));
        assert!(has("scrape interval is 0"));
        assert!(has("not a subset"));
        assert!(has("no cardinality for {\"pod\"}"));
        assert!(has("cardinality of {\"service\"} is 0"));
        assert_eq!(problems.len(), 5, "{problems:?}");
    }

    #[test]
    fn rejects_more_groups_than_series() {
        let facts = WorkloadFacts::from([(
            METRIC.to_string(),
            MetricFacts {
                labels: labels(&["service", "endpoint"]),
                scrape_interval_secs: 15,
                cardinality: BTreeMap::from([
                    (labels(&["service"]), 60),
                    (labels(&["service", "endpoint"]), 50),
                ]),
            },
        )]);
        let r = Rqe {
            grouping_labels: labels(&["service"]),
            ..rqe(60, 60)
        };
        let problems = validate_facts(&[r], &facts).unwrap_err();
        assert!(problems[0].contains("60 groups but only 50 series"));
    }
}
