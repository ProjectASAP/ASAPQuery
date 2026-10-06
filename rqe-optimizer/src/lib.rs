//! v1 brute-force solver for the RQE -> sketch-deployment mapping problem.
//! `docs/rqe_sketch_deployment_v1.md` is the problem statement; doc comments
//! cite its section numbers.
//!
//! Three independent stages, so an ILP can replace [`enumerate`] alone:
//! [`candidates`] builds 𝒟 and per-RQE eligibility (§3), [`enumerate`]
//! searches (§4), [`objectives`] scores (§5).
//!
//! Every number the algorithm uses is caller-supplied -- no constants here.

pub mod autosketch;
pub mod candidates;
pub mod enumerate;
pub mod milp;
pub mod objectives;
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

/// Property of a label set, not of an RQE (§1): RQEs sharing a label set
/// read the same grouped stream. Given as input, never estimated.
#[derive(Debug, Clone, Copy)]
pub struct LabelSetInfo {
    /// `card(labels)`: distinct label-value combinations, i.e. how many
    /// physical sketch instances one deployment for this label set needs.
    pub cardinality: u64,
    /// `λ(labels)`: aggregate items/sec flowing into this grouped stream,
    /// across all of its groups.
    pub arrival_rate_per_sec: f64,
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
    pub labels: LabelSet,
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

/// A candidate deployment (§3): one configuration, one label set, and a
/// sliding sketch window. Retention is an execution/storage concern, not an
/// optimizer candidate dimension.
#[derive(Debug, Clone, PartialEq)]
pub struct Deployment {
    pub capability: Capability,
    pub labels: LabelSet,
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

    /// Instances held to serve a lookback: `x / y` open ones plus the closed
    /// ones still inside the lookback, `(x + S) / y`.
    pub fn retained_instance_count(&self, lookback_secs: Seconds) -> Option<u64> {
        self.query_instance_count(lookback_secs)?;
        self.active_instance_count()?;
        Some((self.window_secs + lookback_secs) / self.slide_secs)
    }
}

/// A full mapping: one deployment index (into the `deployments` slice passed
/// to `enumerate::brute_force`) per RQE, aligned with the `rqes` slice's
/// order. `z_{i,D} = 1 <=> mapping[i] == D`'s index; `u_D = 1 <=>` `D`'s
/// index appears anywhere in `mapping` (§4) -- `y` is never stored
/// separately, it's always derived from `mapping`.
pub type Mapping = Vec<usize>;

/// `LabelSetInfo` lookup used throughout -- keyed by the same `LabelSet`
/// type deployments and RQEs carry.
pub type LabelSetTable = BTreeMap<LabelSet, LabelSetInfo>;
