//! v1 brute-force solver for the RAQE -> sketch-deployment mapping problem.
//! `docs/rqe_sketch_deployment_v1.md` is the problem statement;
//! `rqe_optimizer_candidates.md` and `rqe_optimizer_cost_model.md` hold the
//! detailed planning design.
//!
//! Three independent stages, so an ILP can replace [`enumerate`] alone:
//! [`candidates`] builds 𝒟 and per-RAQE eligibility, [`enumerate`] searches,
//! and [`analytical_cost_model`] scores.
//!
//! Every number the algorithm uses is caller-supplied, except the query
//! output size estimates in [`analytical_cost_model`].

pub mod analytical_cost_model;
pub mod autosketch;
pub mod candidates;
pub mod enumerate;
pub mod milp;
pub mod pareto;
pub mod saturation;
pub mod theory;
pub mod usage;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

pub use aqpbm_core::{AtomicCostEntry, AtomicCostTable};

/// Whole milliseconds. Window alignment is expressed with exact `%` checks.
pub type Millis = u64;

/// Rates (CPU-sec/sec, samples/sec) stay per second.
pub(crate) fn secs(ms: Millis) -> f64 {
    ms as f64 / 1000.0
}

/// A group-by key, compared as a set: a deployment serves a RAQE with the
/// same one, or a subset of it when the family is
/// [`mergeable_across_groups`](FamilyProperties::mergeable_across_groups).
pub type LabelSet = BTreeSet<String>;

/// What an RAQE's statistic needs. A variant becomes a candidate only
/// if it is in both `families()` and [`DEPLOYABLE_FAMILIES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Capability {
    Sum,
    Count,
    Min,
    Max,
    RateOrIncrease,
    Quantile,
    Cardinality,
    /// `topk(k, sum_over_time(x[w]))`: ranked by summed value.
    TopKByValue,
    /// `topk(k, count_over_time(x[w]))`: ranked by sample count.
    TopKByCount,
}

impl Capability {
    pub const ALL: [Capability; 9] = [
        Capability::Sum,
        Capability::Count,
        Capability::Min,
        Capability::Max,
        Capability::RateOrIncrease,
        Capability::Quantile,
        Capability::Cardinality,
        Capability::TopKByValue,
        Capability::TopKByCount,
    ];

    /// `AtomicCostEntry.sketch` values serving this capability. These are
    /// sketch-bench *variant* strings, not algorithm names: fastpath and
    /// regularpath have different cost profiles, so `"cms"` would be
    /// ambiguous. Lists what `study_saturation.py --phase optimizer-cost`
    /// measures, plus families it doesn't run yet;
    /// an unmeasured name here is harmless, it just matches no row.
    pub fn families(self) -> &'static [&'static str] {
        match self {
            // Exact multi-subpopulation accumulators, not sketches. Count is
            // a sum of 1s, so both use `exact-sum`, but as separate
            // capabilities they never share one accumulator. Likewise min and
            // max, so a min query never lands on, or shares, a max one.
            Capability::Sum | Capability::Count => &["exact-sum"],
            Capability::Min => &["exact-min"],
            Capability::Max => &["exact-max"],
            Capability::RateOrIncrease => &["exact-increase"],
            // Hydra (one grid for every subgrouping of a schema) is planned only
            // with undeployable families allowed: ASAPQuery doesn't run it yet.
            Capability::Quantile => &["kll-percall", "dd", "hydra-kll"],
            // univmon-cardinality is registered under KeyedCardinality in
            // sketch-bench, but its ground truth (`KeyedCardinalityGT`) is
            // "count of keys with a nonzero total" -- plain distinct-key
            // count, same target as HLL. Second candidate, not a second
            // capability.
            Capability::Cardinality => &[
                "hll",
                "univmon-cardinality",
                "hydra-hll",
                "hydra-univmon-cardinality",
            ],
            // The sketch is the same; ASAPQuery deploys the two kinds with
            // different `count_events`, so they never share one.
            Capability::TopKByValue | Capability::TopKByCount => &[
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
    "dd",                              // DDSketch
    "hll",                             // HLL
    "cms-heap-topk-fastpath-vector2d", // CountMinSketchWithHeap
];

/// The variant every [`FamilyProperties::needs_delta_set_key_tracker`]
/// deployment pays for alongside its sketch. Never a candidate on its own.
pub const KEY_TRACKER_FAMILY: &str = "exact-delta-set";

/// The `k` a top-k RAQE asks for when it sets no [`Raqe::topk_k`], and the
/// heap capacity of a row that sets no `heap` (sketch-bench's `TOPK_K`).
pub const TOPK_K: u64 = 32;

/// Top-k families whose heap capacity is a config knob (sketch-bench's
/// `heap=`). A deployment answering from `m` merged windows keeps a heap of
/// `m · k`, so the merged heaps still hold the true top `k`
/// (approximately: a working assumption, not a guarantee).
pub fn has_heap(sketch: &str) -> bool {
    matches!(
        sketch,
        "cms-heap-topk-fastpath-vector2d" | "countsketch-heap-topk-fastpath-vector2d"
    )
}

/// The heap a heap top-k deployment with `window_ms` windows needs to serve
/// a top-`k` RAQE over `lookback_ms`: `m · k` for its `m` merged windows, or
/// `None` when the lookback isn't whole windows. [`Deployment::heap_needed`]
/// for a built one.
pub fn heap_needed(lookback_ms: Millis, window_ms: Millis, k: u64) -> Option<u64> {
    if window_ms == 0 || !lookback_ms.is_multiple_of(window_ms) {
        return None;
    }
    (lookback_ms / window_ms).checked_mul(k)
}

/// The `k` a top-k `topk_k` param names, in any JSON form (10 or 10.0):
/// absent means [`TOPK_K`]. The one decoding of the knob, as sketch-bench's
/// `answered_k` reads it.
pub fn params_topk_k(topk_k: Option<f64>) -> u64 {
    topk_k.map_or(TOPK_K, |k| k.round() as u64)
}

/// The `topk_k` param that encodes `k`: none at [`TOPK_K`], which rows and
/// curves leave implicit, else `k`. The one encoding of the knob.
pub fn topk_k_param(k: u64) -> Option<u64> {
    (k != TOPK_K).then_some(k)
}

/// The `k` a top-k cost row or deployment's `topk_k` param names.
pub fn config_topk_k(config: &AtomicCostEntry) -> u64 {
    params_topk_k(config.sketch_config["params"]["topk_k"].as_f64())
}

/// The `k` a top-k config answers: a heap family's `topk_k`; a fixed-k
/// family (UnivMon's top-k) answers [`TOPK_K`] only.
pub fn answered_k(config: &AtomicCostEntry) -> u64 {
    if has_heap(&config.sketch) {
        config_topk_k(config)
    } else {
        TOPK_K
    }
}

/// Whether `config` can answer a top-`k` query: a heap family answering at
/// least `k` (cut to `k`), or a fixed-k family at exactly its `k`.
pub fn serves_topk_k(config: &AtomicCostEntry, k: u64) -> bool {
    if has_heap(&config.sketch) {
        config_topk_k(config) >= k
    } else {
        k == TOPK_K
    }
}

/// A heap family's capacity: its `heap` param, else its `k` (sketch-bench's
/// default). `None` for other families.
pub fn heap_capacity(config: &AtomicCostEntry) -> Option<u64> {
    has_heap(&config.sketch).then(|| {
        let heap = &config.sketch_config["params"]["heap"];
        // A number in any JSON form (128, 128.0); absent is k.
        heap.as_f64()
            .map_or_else(|| config_topk_k(config), |h| h.round() as u64)
    })
}

/// What a variant's algorithm can do, as opposed to what it measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FamilyProperties {
    /// Windows fold into one. A family without it only serves `L == x`.
    pub mergeable_across_windows: bool,
    /// Groups' states fold into their parent group's: a deployment grouped by
    /// `G` serves a RAQE grouped by any subset of `G` (a roll-up). Not top-k,
    /// whose heaps drop items that are in the parent's top k but no child's.
    pub mergeable_across_groups: bool,
    /// One sketch of fixed size holds every group: the design doc's Shared +
    /// Fixed cell. Memory, merge and storage don't scale with `card(G)`.
    /// Exact accumulators are Shared + PerKey and `false` here, because the
    /// export prices them per group.
    pub one_fixed_size_sketch_for_all_groups: bool,
    /// The sketch can't list its groups, so a DeltaSet records which keys
    /// each window saw, for the query to probe.
    pub needs_delta_set_key_tracker: bool,
    /// Hydra: one grid over a label schema `Λ` (the deployment's
    /// `grouping_labels`) answers any non-empty subgrouping of it directly,
    /// from the subkeys every insert fans out to. Not a roll-up: nothing is
    /// merged across groups. Its accuracy is measured per grouping
    /// (`hydra_saturation.csv`).
    pub answers_any_subgrouping: bool,
    /// Answers without error, so its accuracy comes from the cost table
    /// rather than a saturation curve.
    pub exact: bool,
    /// The accuracy key this variant's comparator reports, and which way it
    /// runs. `None` only for [`KEY_TRACKER_FAMILY`], which is never checked.
    pub accuracy: Option<(&'static str, AccuracyDirection)>,
}

/// Panics on a variant with no entry: guessing would misprice it.
/// Accuracy keys are the metrics `study_saturation.py` records per variant,
/// which every cost-table row names as its `accuracy_metric`.
pub fn family_properties(variant: &str) -> FamilyProperties {
    use AccuracyDirection::{HigherIsBetter, LowerIsBetter};
    let one_sketch_per_group = |metric, direction| FamilyProperties {
        mergeable_across_windows: true,
        mergeable_across_groups: true,
        one_fixed_size_sketch_for_all_groups: false,
        needs_delta_set_key_tracker: false,
        answers_any_subgrouping: false,
        exact: false,
        accuracy: Some((metric, direction)),
    };
    // One grid holds every group; a DeltaSet lists the keys to probe.
    let hydra = |metric| FamilyProperties {
        mergeable_across_windows: true,
        mergeable_across_groups: false,
        one_fixed_size_sketch_for_all_groups: true,
        needs_delta_set_key_tracker: true,
        answers_any_subgrouping: true,
        exact: false,
        accuracy: Some((metric, LowerIsBetter)),
    };
    // `relative_error` is the worst group's, not `relative_error_mean`.
    let exact = FamilyProperties {
        exact: true,
        ..one_sketch_per_group("relative_error", LowerIsBetter)
    };
    match variant {
        "exact-sum" | "exact-min" | "exact-max" => exact,
        // Not rolled up: its merge joins two pieces of one counter in time
        // (the later start after a drop is a reset), so merging two groups'
        // counters is wrong. A coarse increase is the sum of per-series
        // increases, which needs per-series state and a sum, not this merge.
        "exact-increase" => FamilyProperties {
            mergeable_across_groups: false,
            ..exact
        },
        KEY_TRACKER_FAMILY => FamilyProperties {
            accuracy: None,
            ..exact
        },
        "hll" | "univmon-cardinality" => one_sketch_per_group("relative_error", LowerIsBetter),
        "kll-percall" => one_sketch_per_group("mean_rank_err", LowerIsBetter),
        "dd" => one_sketch_per_group("mean_relative_value_error", LowerIsBetter),
        "cms-heap-topk-fastpath-vector2d"
        | "countsketch-heap-topk-fastpath-vector2d"
        | "univmon-topk" => FamilyProperties {
            mergeable_across_groups: false,
            ..one_sketch_per_group("precision_at_k", HigherIsBetter)
        },
        "hydra-hll" | "hydra-univmon-cardinality" => hydra("relative_error"),
        "hydra-kll" => hydra("mean_rank_err"),
        _ => panic!("{variant} has no FamilyProperties; add it to family_properties"),
    }
}

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
    /// A label set never has more groups than any superset of it.
    pub cardinality: BTreeMap<LabelSet, u64>,
    /// `(lo, hi)`: smallest and largest positive sample value, if known.
    /// Sizes DDSketch; without it, DDSketch keeps the measured memory.
    pub value_range: Option<(f64, f64)>,
    /// Fitted data parameters per grouping `G`, for reading sketch accuracy
    /// off the saturation curves. Without one, no sketch serves `G`.
    pub data_shape: BTreeMap<LabelSet, saturation::DataShape>,
    /// The `hydra_saturation.csv` dataset whose schema and shares match this
    /// metric's, and that schema `Λ` (its labels; `cardinality` must hold
    /// it). Hydra grids are built only over `Λ`, the width the study
    /// measures, from cost rows measured on the dataset. Without one, no
    /// Hydra candidate is built on the metric.
    pub hydra_dataset: Option<(String, LabelSet)>,
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
        // An SLA no candidate's metric can meet would read as "unservable"
        // rather than bad input. Every metric is >= 0, and every
        // higher-is-better one is a fraction (precision), so its floor is <= 1.
        let sla = raqe.accuracy_sla;
        let floors_a_fraction = raqe.capability.families().iter().any(|family| {
            family_properties(family)
                .accuracy
                .is_some_and(|(_, direction)| direction == AccuracyDirection::HigherIsBetter)
        });
        let max = if floors_a_fraction {
            1.0
        } else {
            f64::INFINITY
        };
        if !(sla.is_finite() && (0.0..=max).contains(&sla)) {
            problems.insert(format!("{id}: accuracy SLA {sla} is outside [0, {max}]"));
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
        if let Some((_, schema)) = &metric_facts.hydra_dataset {
            if !schema.is_subset(all_labels) {
                problems.insert(format!(
                    "{metric}: Hydra schema {schema:?} is not a subset of its labels {all_labels:?}"
                ));
            }
            if cardinality.get(schema).is_none_or(|&groups| groups == 0) {
                problems.insert(format!(
                    "{metric}: Hydra schema {schema:?} needs a nonzero cardinality"
                ));
            }
        }
    }
    // Dropping labels never adds groups. A roll-up's merge count,
    // `card(G_d) · L/x − card(G_r)`, relies on it, so it is checked between
    // the label sets a plan can use: every RAQE grouping on the metric, and
    // its full label set. A label set no RAQE groups by is never read.
    let mut used: BTreeMap<&str, BTreeSet<&LabelSet>> = BTreeMap::new();
    for raqe in raqes {
        if let Some(metric_facts) = facts.get(&raqe.metric) {
            used.entry(&raqe.metric)
                .or_insert_with(|| BTreeSet::from([&metric_facts.labels]))
                .insert(&raqe.grouping_labels);
        }
    }
    for (metric, label_sets) in &used {
        let cardinality = &facts[*metric].cardinality;
        for coarse in label_sets {
            for fine in label_sets {
                if let (Some(&coarse_groups), Some(&fine_groups)) =
                    (cardinality.get(*coarse), cardinality.get(*fine))
                {
                    if coarse.is_subset(fine) && coarse_groups > fine_groups {
                        problems.insert(format!(
                            "{metric}: {coarse:?} has {coarse_groups} groups but its superset \
                             {fine:?} only {fine_groups}"
                        ));
                    }
                }
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
/// Named per family in [`family_properties`], never inferred from the metric
/// name: an unanticipated name would get a silent wrong default, and the
/// failure mode is a hard constraint accepting exactly what it should reject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccuracyDirection {
    /// `measured <= tolerance` passes -- the metric is an error.
    LowerIsBetter,
    /// `measured >= tolerance` passes -- the metric is a score.
    HigherIsBetter,
}

/// One repeating query expression.
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
    /// `tol_i`: hard constraint on each candidate's own
    /// [accuracy metric](FamilyProperties::accuracy), a ceiling or a floor by
    /// its direction. One number means different guarantees across metrics
    /// (sketch-bench#172).
    pub accuracy_sla: f64,
    /// Hard ceiling on modeled query latency; `None` for no limit. Only the
    /// MILP enforces it.
    pub latency_sla_ms: Option<f64>,
    /// A top-k RAQE's `k`; `None` means [`TOPK_K`]. Ignored for other
    /// capabilities.
    pub topk_k: Option<u64>,
    /// The groups the accuracy SLA covers: those holding at least this
    /// share of the metric's records, plus always the largest; `None` for
    /// every group. Read only by Hydra's accuracy, measured per coverage.
    pub accuracy_covers_share: Option<f64>,
}

impl Raqe {
    /// The `k` this RAQE asks for: its [`Raqe::topk_k`], else [`TOPK_K`].
    pub fn topk_k(&self) -> u64 {
        self.topk_k.unwrap_or(TOPK_K)
    }

    /// Whether `accuracy`, in `sketch`'s [`accuracy_key`], clears the SLA.
    /// Unknown or non-finite never passes. The MILP's eligibility and the
    /// AutoSketch baseline both decide here.
    pub fn meets_sla(&self, sketch: &str, accuracy: Option<f64>) -> bool {
        let (_, direction) = accuracy_key(sketch);
        accuracy.is_some_and(|value| {
            value.is_finite()
                && match direction {
                    AccuracyDirection::LowerIsBetter => value <= self.accuracy_sla,
                    AccuracyDirection::HigherIsBetter => value >= self.accuracy_sla,
                }
        })
    }
}

/// `sketch`'s [accuracy metric](FamilyProperties::accuracy) and direction.
/// Panics for [`KEY_TRACKER_FAMILY`], which is never a candidate.
pub fn accuracy_key(sketch: &str) -> (&'static str, AccuracyDirection) {
    family_properties(sketch)
        .accuracy
        .unwrap_or_else(|| panic!("{sketch} has no accuracy metric, so it can't be a candidate"))
}

/// The accuracy `raqe` gets from a deployment, or `None` when unknown --
/// unknown never passes. Production uses
/// [`saturation::SaturationCurves::accuracy`].
pub type Accuracy<'a> = dyn Fn(&Raqe, &Deployment) -> Option<f64> + 'a;

/// The cost table's measured value, which is the exact accumulators'
/// accuracy: they merge without loss, so the window size doesn't matter.
pub fn table_accuracy(_raqe: &Raqe, deployment: &Deployment) -> Option<f64> {
    Some(row_accuracy(&deployment.config))
}

/// `config`'s accuracy: its `query_accuracy[accuracy_metric]`. Panics when
/// `accuracy_metric` isn't its family's [`accuracy_key`] or the score is
/// missing: either means the cost export is broken.
pub fn row_accuracy(config: &AtomicCostEntry) -> f64 {
    let (metric, _) = accuracy_key(&config.sketch);
    assert_eq!(
        config.accuracy_metric, metric,
        "{} {}: the row's accuracy_metric isn't its family's; the cost export is broken",
        config.sketch, config.sketch_config
    );
    config.accuracy().unwrap_or_else(|| {
        panic!(
            "{} {} has no {metric}; the cost export is broken",
            config.sketch, config.sketch_config
        )
    })
}

/// A candidate deployment (`docs/rqe_optimizer_candidates.md`): one configuration, one grouped stream, and a
/// sliding sketch window. Retention is an execution/storage concern, not an
/// optimizer candidate dimension.
#[derive(Debug, Clone, PartialEq)]
pub struct Deployment {
    pub capability: Capability,
    pub metric: String,
    pub spatial_filter: String,
    /// `G`: one instance per group, per window, unless the family keeps one
    /// for all groups.
    pub grouping_labels: LabelSet,
    pub config: AtomicCostEntry,
    /// `x`: materialized sketch-window size.
    pub window_ms: Millis,
    /// `y`: materialized sketch slide.
    pub slide_ms: Millis,
    /// The [`KEY_TRACKER_FAMILY`] row, for a family that
    /// [needs one](FamilyProperties::needs_delta_set_key_tracker).
    pub key_tracker: Option<AtomicCostEntry>,
}

impl Deployment {
    pub fn properties(&self) -> FamilyProperties {
        family_properties(&self.config.sketch)
    }

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

    /// The heap a heap top-k deployment needs to serve `lookback_ms`: `m · k`
    /// for its `m` merged windows. `None` when the lookback isn't whole
    /// windows.
    pub fn heap_needed(&self, lookback_ms: Millis, k: u64) -> Option<u64> {
        heap_needed(lookback_ms, self.window_ms, k)
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
/// index appears anywhere in `mapping` -- `y` is never stored
/// separately, it's always derived from `mapping`.
pub type Mapping = Vec<usize>;

/// Fixtures shared by the unit tests: one metric `m`, grouped by nothing,
/// served by top-k.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub const METRIC: &str = "m";

    /// Clears an SLA of 0.5 on every family's metric: errors 0, precision 1,
    /// so a row renamed to any family stays feasible.
    pub fn perfect_accuracy() -> BTreeMap<String, f64> {
        [
            "relative_error",
            "mean_rank_err",
            "mean_relative_value_error",
        ]
        .map(|metric| (metric.to_string(), 0.0))
        .into_iter()
        .chain([("precision_at_k".to_string(), 1.0)])
        .collect()
    }

    /// The accuracy metric a row of `sketch` names: its family's.
    pub fn metric_of(sketch: &str) -> String {
        accuracy_key(sketch).0.to_string()
    }

    /// Where a fixture row was "measured": the cost table's canonical point.
    pub fn measured_at() -> aqpbm_core::MeasuredAt {
        aqpbm_core::MeasuredAt {
            items_per_instance: 1_000_000,
            keys_per_instance: Some(10_000),
            value_range: None,
            merge_operand_items: None,
            distribution: None,
            dataset: None,
            schema_width: None,
        }
    }

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
                data_shape: BTreeMap::new(),
                hydra_dataset: None,
            },
        )])
    }

    pub fn raqe(lookback_ms: Millis, interval_ms: Millis) -> Raqe {
        Raqe {
            id: "r".into(),
            capability: Capability::TopKByValue,
            lookback_ms,
            interval_ms,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            accuracy_sla: 0.5,
            latency_sla_ms: None,
            topk_k: None,
            accuracy_covers_share: None,
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
            capability: Capability::TopKByValue,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: AtomicCostEntry {
                sketch: "cms-heap-topk-fastpath-vector2d".into(),
                // A heap that holds any merge, so heap top-k fixtures merge freely.
                sketch_config: serde_json::json!({"params": {"heap": 1u64 << 40}}),
                mem_bytes_per_instance: memory,
                insert_cpu_secs: insert,
                merge_cpu_secs: merge,
                query_cpu_secs: query,
                query_accuracy: perfect_accuracy(),
                accuracy_metric: "precision_at_k".into(),
                measured_at: measured_at(),
            },
            window_ms,
            slide_ms,
            key_tracker: None,
        }
    }

    pub fn label_set(names: &[&str]) -> LabelSet {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// 5 services × 10 endpoints, one series per endpoint, scraped every
    /// second: the coarse and fine groupings of a roll-up.
    pub fn service_endpoint_facts() -> WorkloadFacts {
        let service = label_set(&["service"]);
        let service_endpoint = label_set(&["service", "endpoint"]);
        WorkloadFacts::from([(
            METRIC.to_string(),
            MetricFacts {
                labels: service_endpoint.clone(),
                scrape_interval_ms: 1_000,
                cardinality: BTreeMap::from([(service, 5), (service_endpoint, 50)]),
                value_range: None,
                data_shape: BTreeMap::new(),
                hydra_dataset: None,
            },
        )])
    }

    /// A per-group KLL deployment by `grouping`, every per-instance cost 1.
    pub fn kll_by(grouping: &[&str], window_ms: Millis) -> Deployment {
        let base = deployment(1.0, 1.0, 1.0, 1.0, window_ms, window_ms);
        Deployment {
            capability: Capability::Quantile,
            grouping_labels: label_set(grouping),
            config: AtomicCostEntry {
                sketch: "kll-percall".into(),
                sketch_config: serde_json::json!({"params": {"k": 200}}),
                accuracy_metric: metric_of("kll-percall"),
                ..base.config
            },
            ..base
        }
    }

    pub fn quantile_by(grouping: &[&str], lookback_ms: Millis, interval_ms: Millis) -> Raqe {
        Raqe {
            capability: Capability::Quantile,
            grouping_labels: label_set(grouping),
            ..raqe(lookback_ms, interval_ms)
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

    /// One quantile SLA: KLL is checked by rank error, DD by relative value
    /// error, each against the same number.
    #[test]
    fn one_quantile_raqe_checks_each_family_by_its_own_metric() {
        let r = Raqe {
            capability: Capability::Quantile,
            accuracy_sla: 0.05,
            ..raqe(60_000, 60_000)
        };
        let row = |sketch: &str, metric: &str, value: f64| AtomicCostEntry {
            sketch: sketch.into(),
            query_accuracy: BTreeMap::from([(metric.to_string(), value)]),
            accuracy_metric: metric.to_string(),
            ..deployment(0.0, 0.0, 0.0, 0.0, 60_000, 60_000).config
        };
        let ok = |c: AtomicCostEntry| r.meets_sla(&c.sketch, Some(row_accuracy(&c)));
        assert!(ok(row("kll-percall", "mean_rank_err", 0.01)));
        assert!(!ok(row("kll-percall", "mean_rank_err", 0.1)));
        assert!(ok(row("dd", "mean_relative_value_error", 0.01)));
        assert!(!ok(row("dd", "mean_relative_value_error", 0.1)));
    }

    /// DD omits its metric when every true quantile is 0; a row without it
    /// must not pass, nor be skipped silently.
    #[test]
    #[should_panic(expected = "has no mean_relative_value_error")]
    fn a_row_missing_its_familys_metric_panics() {
        let config = AtomicCostEntry {
            sketch: "dd".into(),
            query_accuracy: BTreeMap::from([("mean_rank_err".to_string(), 0.0)]),
            accuracy_metric: "mean_relative_value_error".into(),
            ..deployment(0.0, 0.0, 0.0, 0.0, 60_000, 60_000).config
        };
        row_accuracy(&config);
    }

    /// The row's declared metric is the one read, and it must be the
    /// family's: a row naming another is a broken export, not a guess.
    #[test]
    #[should_panic(expected = "the row's accuracy_metric isn't its family's")]
    fn a_row_naming_another_metric_panics() {
        let config = AtomicCostEntry {
            sketch: "kll-percall".into(),
            query_accuracy: BTreeMap::from([("max_rank_err".to_string(), 0.0)]),
            accuracy_metric: "max_rank_err".into(),
            ..deployment(0.0, 0.0, 0.0, 0.0, 60_000, 60_000).config
        };
        row_accuracy(&config);
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
    fn every_family_has_properties() {
        for capability in Capability::ALL {
            for family in capability.families() {
                family_properties(family);
            }
        }
        family_properties(KEY_TRACKER_FAMILY);
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
                data_shape: BTreeMap::new(),
                hydra_dataset: None,
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
                data_shape: BTreeMap::new(),
                hydra_dataset: None,
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

    /// No metric can meet a negative SLA, nor a precision floor above 1; an
    /// error ceiling above 1 is merely loose.
    #[test]
    fn rejects_an_accuracy_sla_no_metric_can_meet() {
        let sla = |id: &str, capability, accuracy_sla| Raqe {
            id: id.into(),
            capability,
            accuracy_sla,
            ..raqe(60_000, 60_000)
        };
        let raqes = [
            sla("negative", Capability::Quantile, -0.1),
            sla("nan", Capability::Quantile, f64::NAN),
            sla("precision_above_1", Capability::TopKByValue, 1.5),
            sla("loose_error", Capability::Cardinality, 1.5),
            sla("exact_precision", Capability::TopKByValue, 1.0),
        ];
        let problems = validate_facts(&raqes, &test_support::facts(1, 1)).unwrap_err();
        assert_eq!(
            problems,
            [
                "nan: accuracy SLA NaN is outside [0, inf]",
                "negative: accuracy SLA -0.1 is outside [0, inf]",
                "precision_above_1: accuracy SLA 1.5 is outside [0, 1]",
            ]
        );
    }

    /// Facts where `(service)` has more groups than its superset
    /// `(service, endpoint)`, and `(pod)` has none at all.
    fn inconsistent_facts() -> WorkloadFacts {
        WorkloadFacts::from([(
            METRIC.to_string(),
            MetricFacts {
                labels: labels(&["service", "endpoint", "pod"]),
                scrape_interval_ms: 15_000,
                cardinality: BTreeMap::from([
                    (labels(&["service"]), 60),
                    (labels(&["service", "endpoint"]), 50),
                    (labels(&["pod"]), 0),
                    (labels(&["service", "endpoint", "pod"]), 30_000),
                ]),
                value_range: None,
                data_shape: BTreeMap::new(),
                hydra_dataset: None,
            },
        )])
    }

    /// Between two RAQE groupings, the subset may not have more groups.
    #[test]
    fn rejects_a_used_label_set_with_more_groups_than_a_used_superset() {
        let grouped = |l: &[&str]| Raqe {
            grouping_labels: labels(l),
            ..raqe(60_000, 60_000)
        };
        assert_eq!(
            validate_facts(
                &[grouped(&["service"]), grouped(&["service", "endpoint"])],
                &inconsistent_facts()
            )
            .unwrap_err(),
            [format!(
                "{METRIC}: {{\"service\"}} has 60 groups but its superset \
                 {{\"endpoint\", \"service\"}} only 50"
            )]
        );
    }

    /// A RAQE grouping may not have more groups than the series.
    #[test]
    fn rejects_a_grouping_with_more_groups_than_series() {
        let mut facts = inconsistent_facts();
        let cardinality = &mut facts.get_mut(METRIC).unwrap().cardinality;
        cardinality.insert(labels(&["service", "endpoint", "pod"]), 40);
        let r = Raqe {
            grouping_labels: labels(&["service"]),
            ..raqe(60_000, 60_000)
        };
        assert_eq!(
            validate_facts(&[r], &facts).unwrap_err(),
            [format!(
                "{METRIC}: {{\"service\"}} has 60 groups but its superset \
                 {{\"endpoint\", \"pod\", \"service\"}} only 40"
            )]
        );
    }

    /// Label sets no RAQE groups by are never read, so inconsistencies there
    /// don't reject the workload.
    #[test]
    fn ignores_label_sets_no_raqe_uses() {
        let r = Raqe {
            grouping_labels: labels(&["service"]),
            ..raqe(60_000, 60_000)
        };
        assert_eq!(validate_facts(&[r], &inconsistent_facts()), Ok(()));
    }

    /// Hydra's schema must be the metric's labels with a known group count.
    #[test]
    fn rejects_a_hydra_schema_outside_the_labels_or_without_cardinality() {
        let mut facts = test_support::facts(1, 1);
        let metric_facts = facts.get_mut(METRIC).unwrap();
        let mut outside = metric_facts.labels.clone();
        outside.insert("pod".into());
        metric_facts.hydra_dataset = Some(("hydra_test".into(), outside.clone()));
        let problems = validate_facts(&[raqe(60_000, 60_000)], &facts).unwrap_err();
        assert_eq!(
            problems,
            [
                format!(
                    "{METRIC}: Hydra schema {outside:?} is not a subset of its labels {:?}",
                    facts[METRIC].labels
                ),
                format!("{METRIC}: Hydra schema {outside:?} needs a nonzero cardinality"),
            ]
        );
        let metric_facts = facts.get_mut(METRIC).unwrap();
        let schema = metric_facts.labels.clone();
        metric_facts.cardinality.insert(schema.clone(), 0);
        metric_facts.hydra_dataset = Some(("hydra_test".into(), schema));
        let problems = validate_facts(&[raqe(60_000, 60_000)], &facts).unwrap_err();
        assert!(
            problems.iter().any(|p| p.contains("Hydra schema")),
            "{problems:?}"
        );
    }
}
