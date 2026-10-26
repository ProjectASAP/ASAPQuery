//! Sketch accuracy read from the saturation study's error-vs-N curves
//! (`scripts/study_saturation.py`, #130, #140) at the number of items a
//! deployment's answer covers. Design decisions: #156.
//!
//! The curve is read at the number of items one group receives over the
//! RAQE's whole lookback: series per group times scrapes per lookback. For
//! sketches that merge exactly, that holds whether the answer is one sketch
//! or a merge of many smaller-window sketches. KLL and top-k merge lossily
//! (#131): an answer merged from `L / x` windows reads the curve of the sketch
//! merged from that many shards (#158).
//!
//! The cost table's sketch accuracies are not read: each is one point on a
//! curve, at the row's `measured_at`, and [`SaturationCurves::check_cost_table`]
//! checks that the two agree there (#171). Both come from one study run
//! (`study_saturation.py --phase optimizer-cost`, #174).
//!
//! Hydra reads none of these curves: its accuracy is measured per dataset
//! and grouping (`hydra_saturation.csv`, `docs/rqe_optimizer_hydra.md`
//! §2.4), see [`SaturationCurves::accuracy`].

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use crate::autosketch::window_adapter;
use crate::theory;
use crate::{
    accuracy_key, family_properties, has_heap, heap_capacity, params_topk_k, table_accuracy,
    AccuracyDirection, AtomicCostEntry, Capability, Deployment, LabelSet, MetricFacts, Raqe,
    WorkloadFacts, TOPK_K,
};
use aqpbm_core::MeasuredShape;

/// The data parameters a saturation curve is keyed by, fitted from the
/// dataset per (metric, grouping) as the worst case over windows and groups.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DataShape {
    /// Zipf skew θ of the keys inserted into one group's sketch (CMS, top-k,
    /// HLL). Describes whichever insertion mode the engine uses: once per
    /// sample, or weighted by sample value.
    pub zipf_s: f64,
    /// `K`: distinct keys per group per window.
    pub distinct_keys: f64,
    /// Pareto tail index `a` of the values (KLL, DDSketch).
    pub tail_index: f64,
}

/// The two runs' output directories under `--saturation-dir`. A point in
/// both is taken whole from the later one.
const RUN_DIRS: [&str; 2] = ["out_grid_1e7_cost", "out_1e9"];

/// The cost table under the same directory, written by
/// `study_saturation.py --phase optimizer-cost --out DIR/optimizer_cost`.
pub const COST_TABLE: &str = "optimizer_cost/rqe_atomic_costs.json";

/// Hydra's measurements under the same directory, written by
/// `study_saturation.py --phase hydra` on datasets shaped like the
/// workload's metrics. Optional: without it no Hydra deployment has an
/// accuracy.
pub const HYDRA_SATURATION: &str = "hydra_saturation.csv";

/// `hydra_saturation.csv`'s error columns, by the share of records a group
/// must hold to be covered (the largest group always is): every group, then
/// the two measured thresholds.
const HYDRA_COVERAGE: [(f64, &str); 3] = [
    (0.0, "err_max"),
    (0.01, "err_max_cov_0.01"),
    (0.05, "err_max_cov_0.05"),
];

/// Hydra measurements of one (variant, config, dataset, grouping), by merge
/// shards: each [`HYDRA_COVERAGE`] column's worst over the records and seeds
/// that filled it, `None` where every row left it empty.
type HydraErrors = BTreeMap<u64, [Option<f64>; 3]>;

/// One (sketch, config, data shape) point of the study.
#[derive(Debug, Clone, PartialEq)]
struct GridPoint {
    params: BTreeMap<String, f64>,
    shape: MeasuredShape,
    error_metric: String,
    /// `None`: still changing at the largest measured N.
    n_sat: Option<f64>,
    /// `(n, seed_mean_error, seed_se)`, ascending in `n`.
    curve: Vec<(f64, f64, f64)>,
    /// The same, for the sketch merged from `m` shards of the stream, keyed
    /// by `m > 1` (`saturation_merge_curve.csv`, #131, #158).
    merged: BTreeMap<u64, Vec<(f64, f64, f64)>>,
}

/// How far a cost-table value may sit outside the curve's range at its N:
/// this many of the curve's seed standard errors, plus [`AGREEMENT_RELATIVE`]
/// of the curve's value, because the table row is one seed of its own.
const AGREEMENT_SES: f64 = 3.0;
const AGREEMENT_RELATIVE: f64 = 0.05;

impl GridPoint {
    /// Zipf θ, or the Pareto tail index for quantile sketches.
    fn shape_param(&self) -> f64 {
        match self.shape {
            MeasuredShape::Zipf { skew, .. } => skew,
            MeasuredShape::Pareto { tail_index } => tail_index,
        }
    }

    /// `K`; `None` for quantile sketches.
    fn distinct_keys(&self) -> Option<f64> {
        match self.shape {
            MeasuredShape::Zipf { keys, .. } => Some(keys),
            MeasuredShape::Pareto { .. } => None,
        }
    }

    /// Whether a value measured at `n` agrees with the curve: within
    /// tolerance of the checkpoint at `n`, or of the range between the two
    /// either side. `None` where the curve can't say (as in
    /// [`GridPoint::error_at`]).
    fn agrees_at(&self, n: f64, value: f64) -> Option<bool> {
        let &(first_n, ..) = self.curve.first()?;
        let &last = self.curve.last()?;
        let neighbours = if n < first_n {
            return None;
        } else if n >= last.0 {
            // The plateau, as in `curve_error_at`.
            self.n_sat.filter(|&sat| sat <= last.0)?;
            [last, last]
        } else {
            let above = self
                .curve
                .partition_point(|&(checkpoint, ..)| checkpoint < n);
            if self.curve[above].0 == n {
                [self.curve[above]; 2]
            } else {
                [self.curve[above - 1], self.curve[above]]
            }
        };
        let [(_, a, a_se), (_, b, b_se)] = neighbours;
        let (low, high) = (a.min(b), a.max(b));
        let tolerance = AGREEMENT_SES * a_se.max(b_se) + AGREEMENT_RELATIVE * high.abs();
        Some(value >= low - tolerance && value <= high + tolerance)
    }

    /// Q6 of #156: between checkpoints, the worse neighbour; below the first,
    /// unmeasured; past the last, the plateau only if the point saturated.
    fn error_at(&self, n: f64, direction: AccuracyDirection) -> Option<f64> {
        curve_error_at(&self.curve, self.n_sat, n, direction)
    }

    /// The curves an answer merged from `merges` instances reads: the plain
    /// curve alone, or the measured shard counts either side of `merges`
    /// (one is the plain curve). Past the largest count, only the largest.
    fn bracketing_curves(&self, merges: u64) -> Vec<&[(f64, f64, f64)]> {
        if merges <= 1 {
            return vec![&self.curve];
        }
        let below = self
            .merged
            .range(..=merges)
            .next_back()
            .map_or(&self.curve[..], |(_, c)| &c[..]);
        match self.merged.range(merges..).next() {
            Some((_, above)) => vec![below, above],
            None => self
                .merged
                .values()
                .next_back()
                .map_or(vec![below], |c| vec![&c[..]]),
        }
    }

    /// The smallest N the study measured the curves an answer merged from
    /// `merges` instances reads.
    fn first_measured_n(&self, merges: u64) -> Option<f64> {
        self.bracketing_curves(merges)
            .iter()
            .map(|c| c.first().map(|p| p.0))
            .try_fold(0.0, |max: f64, first| first.map(|f| max.max(f)))
    }

    /// The worst last error measured on the curves an answer merged from
    /// `merges` instances reads: the floor a fallback extending them can't
    /// beat.
    fn last_measured_error(&self, merges: u64, direction: AccuracyDirection) -> Option<f64> {
        self.bracketing_curves(merges)
            .iter()
            .filter_map(|c| c.last().map(|p| p.1))
            .reduce(|a, b| worse(a, b, direction))
    }

    /// The error at `n` items of the sketch merged from `merges` instances:
    /// the worse of the measured shard counts either side of `merges` (one
    /// is the plain sketch). `None` past the largest measured count, past the
    /// merge curve's last N, or with no merge curve: what the study didn't
    /// measure is unknown.
    fn merged_error_at(&self, merges: u64, n: f64, direction: AccuracyDirection) -> Option<f64> {
        if merges <= 1 {
            return self.error_at(n, direction);
        }
        let below = self.merged.range(..=merges).next_back().map(|(&m, _)| m);
        let (&above, _) = self.merged.range(merges..).next()?;
        // Past a merge curve's last N it has no plateau: nothing shows the
        // merged sketch stopped changing there.
        let at = |m: u64| match m {
            1 => self.error_at(n, direction),
            m => curve_error_at(&self.merged[&m], None, n, direction),
        };
        let below = at(below.unwrap_or(1))?;
        Some(worse(below, at(above)?, direction))
    }
}

/// [`GridPoint::error_at`] on any of the point's curves.
fn curve_error_at(
    curve: &[(f64, f64, f64)],
    n_sat: Option<f64>,
    n: f64,
    direction: AccuracyDirection,
) -> Option<f64> {
    let &(first_n, ..) = curve.first()?;
    let &(last_n, last_error, _) = curve.last()?;
    if n < first_n {
        return None;
    }
    if n == last_n {
        return Some(last_error);
    }
    // Past the last checkpoint, the plateau if the curve saturated by then.
    if n > last_n {
        return n_sat.filter(|&sat| sat <= last_n).map(|_| last_error);
    }
    let above = curve.partition_point(|&(checkpoint, ..)| checkpoint < n);
    let (checkpoint, error, _) = curve[above];
    if checkpoint == n {
        Some(error)
    } else {
        Some(worse(error, curve[above - 1].1, direction))
    }
}

/// KLL, the heap top-k sketches and UnivMon lose accuracy when merged
/// (#131): a merged answer reads their merge curve. UnivMon's counters add
/// exactly, but each level's heavy-hitter heap is rebuilt from the union of
/// the two heaps, so a key heavy only in the union is lost. The other
/// sketches merge exactly. Hydra's cells merge as their sketch does.
fn merges_lossily(sketch: &str) -> bool {
    matches!(
        sketch,
        "kll-percall"
            | "univmon-cardinality"
            | "hydra-kll"
            | "hydra-univmon-cardinality"
            | "cms-heap-topk-fastpath-vector2d"
            | "countsketch-heap-topk-fastpath-vector2d"
            | "univmon-topk"
    )
}

/// Whether some capability plans `sketch`.
fn is_candidate(sketch: &str) -> bool {
    Capability::ALL
        .iter()
        .any(|capability| capability.families().contains(&sketch))
}

fn worse(a: f64, b: f64, direction: AccuracyDirection) -> f64 {
    match direction {
        AccuracyDirection::LowerIsBetter => a.max(b),
        AccuracyDirection::HigherIsBetter => a.min(b),
    }
}

/// Where an accuracy came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccuracySource {
    /// The cost table (exact accumulators) or the study's curves.
    Measured,
    /// The algorithm's published guarantee, where the study measured nothing.
    Theory,
}

/// What [`SaturationCurves::check_cost_table`] found, one line per row.
#[derive(Debug, Default, PartialEq)]
pub struct CostTableCheck {
    /// Rows whose accuracy disagrees with the curve at their `measured_at`,
    /// whose config the grid lacks, or that lack the curve's metric.
    pub mismatched: Vec<String>,
    /// Rows with no curve at their measured shape or N, such as KLL: the
    /// table measures Zipf ranks, the study Pareto values.
    pub unchecked: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SaturationCurves {
    points_by_sketch: BTreeMap<String, Vec<GridPoint>>,
    /// [`HYDRA_SATURATION`], keyed by variant, config params (as
    /// [`config_params`] prints them), dataset and grouping.
    hydra: BTreeMap<(String, String, String, LabelSet), HydraErrors>,
}

impl SaturationCurves {
    /// Reads `saturation.csv` (for the saturation point), `saturation_curve.csv`
    /// and, where present, `saturation_merge_curve.csv` from each of
    /// [`RUN_DIRS`] under `dir`. Fails on a candidate family's curve
    /// for a metric other than its [`accuracy_key`]: that study is stale.
    pub fn load(dir: &Path) -> io::Result<Self> {
        type PointKey = (String, String, String, String, String);
        let key = |row: &CsvRow| -> PointKey {
            let field = |name: &str| row[name].clone();
            (
                field("sketch"),
                field("config"),
                field("dist"),
                field("param"),
                field("cardinality"),
            )
        };
        let mut points: BTreeMap<PointKey, (String, GridPoint)> = BTreeMap::new();
        for run in RUN_DIRS {
            let run_dir = dir.join(run);
            let mut run_points = BTreeMap::new();
            let columns = [
                "sketch",
                "config",
                "dist",
                "param",
                "cardinality",
                "n_sat",
                "error_metric",
            ];
            for row in read_csv(&run_dir.join("saturation.csv"), &columns)? {
                let point = GridPoint {
                    params: parse_config(&row["config"])?,
                    shape: parse_shape(&row)?,
                    error_metric: row["error_metric"].clone(),
                    n_sat: match row["n_sat"].as_str() {
                        "not_saturated" => None,
                        n => Some(parse_number(n)?),
                    },
                    curve: Vec::new(),
                    merged: BTreeMap::new(),
                };
                let sketch = row["sketch"].as_str();
                if is_candidate(sketch) && point.error_metric != accuracy_key(sketch).0 {
                    return Err(invalid(format!(
                        "{run}: {sketch} curve measured {}, but the optimizer reads {}; \
                         rerun the study",
                        point.error_metric,
                        accuracy_key(sketch).0
                    )));
                }
                run_points.insert(key(&row), (row["sketch"].clone(), point));
            }
            let columns = [
                "sketch",
                "config",
                "dist",
                "param",
                "cardinality",
                "n",
                "seed_mean_error",
                "seed_se",
            ];
            for row in read_csv(&run_dir.join("saturation_curve.csv"), &columns)? {
                let Some((_, point)) = run_points.get_mut(&key(&row)) else {
                    return Err(invalid(format!(
                        "{run}: curve row for a point not in saturation.csv: {row:?}"
                    )));
                };
                point.curve.push((
                    parse_number(&row["n"])?,
                    parse_number(&row["seed_mean_error"])?,
                    parse_number(&row["seed_se"])?,
                ));
            }
            let merge_path = run_dir.join("saturation_merge_curve.csv");
            if merge_path.exists() {
                let columns = [
                    "sketch",
                    "config",
                    "dist",
                    "param",
                    "cardinality",
                    "shards",
                    "n",
                    "seed_mean_error",
                    "seed_se",
                ];
                for row in read_csv(&merge_path, &columns)? {
                    let Some((_, point)) = run_points.get_mut(&key(&row)) else {
                        return Err(invalid(format!(
                            "{run}: merge curve row for a point not in saturation.csv: {row:?}"
                        )));
                    };
                    let shards: u64 = row["shards"]
                        .parse()
                        .map_err(|_| invalid(format!("{run}: shards is not a count: {row:?}")))?;
                    if shards == 0 {
                        return Err(invalid(format!("{run}: zero shards: {row:?}")));
                    }
                    if shards > 1 {
                        point.merged.entry(shards).or_default().push((
                            parse_number(&row["n"])?,
                            parse_number(&row["seed_mean_error"])?,
                            parse_number(&row["seed_se"])?,
                        ));
                    }
                }
            }
            // A later run's point wins, but keeps an earlier run's merge
            // curves at shard counts it didn't measure (the 1e9 run measures
            // none).
            for (key, (sketch, mut point)) in run_points {
                if let Some((_, earlier)) = points.remove(&key) {
                    for (shards, curve) in earlier.merged {
                        point.merged.entry(shards).or_insert(curve);
                    }
                }
                points.insert(key, (sketch, point));
            }
        }
        // A lossy candidate sketch with no merge curve at all (a study run
        // without --merge-shards-list for it) would leave every merged
        // deployment of it silently without accuracy; refuse it. A single
        // point without one (only the 1e9 run has it) just has no merged
        // accuracy.
        let mut lossy: BTreeMap<&str, bool> = BTreeMap::new();
        for (sketch, point) in points.values() {
            // Heap top-k merges as one sketch when sized m · k, so only the
            // non-heap lossy sketches (KLL, univmon-cardinality, univmon-topk)
            // need the curves.
            if is_candidate(sketch) && merges_lossily(sketch) && !has_heap(sketch) {
                *lossy.entry(sketch).or_default() |= !point.merged.is_empty();
            }
        }
        if let Some((sketch, _)) = lossy.iter().find(|(_, &merged)| !merged) {
            return Err(invalid(format!(
                "no merge curves for {sketch}; rerun the study with --merge-shards-list"
            )));
        }
        let mut points_by_sketch: BTreeMap<String, Vec<GridPoint>> = BTreeMap::new();
        for (sketch, mut point) in points.into_values() {
            point.curve.sort_by(|a, b| a.0.total_cmp(&b.0));
            for curve in point.merged.values_mut() {
                curve.sort_by(|a, b| a.0.total_cmp(&b.0));
            }
            points_by_sketch.entry(sketch).or_default().push(point);
        }
        // The optimizer brackets a shape between grid points, so each
        // config's Zipf grid must be a full cross of its θ and K values: a
        // hole would quietly leave every shape around it without accuracy.
        for (sketch, points) in &points_by_sketch {
            let mut by_config: BTreeMap<String, BTreeSet<(u64, u64)>> = BTreeMap::new();
            for point in points {
                if let MeasuredShape::Zipf { skew, keys } = point.shape {
                    by_config
                        .entry(format!("{:?}", point.params))
                        .or_default()
                        .insert((skew.to_bits(), keys.to_bits()));
                }
            }
            for (config, shapes) in by_config {
                let thetas: BTreeSet<u64> = shapes.iter().map(|&(t, _)| t).collect();
                let keys: BTreeSet<u64> = shapes.iter().map(|&(_, k)| k).collect();
                let missing: Vec<(f64, f64)> = thetas
                    .iter()
                    .flat_map(|&t| keys.iter().map(move |&k| (t, k)))
                    .filter(|shape| !shapes.contains(shape))
                    .map(|(t, k)| (f64::from_bits(t), f64::from_bits(k)))
                    .collect();
                if !missing.is_empty() {
                    return Err(invalid(format!(
                        "{sketch} {config}: the grid isn't a full cross of θ and K; \
                         missing (θ, K) {missing:?}. Rerun the study's accuracy grid for them"
                    )));
                }
            }
        }
        let hydra_path = dir.join(HYDRA_SATURATION);
        let hydra = if hydra_path.exists() {
            load_hydra(&hydra_path)?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            points_by_sketch,
            hydra,
        })
    }

    /// Hydra's accuracy for `raqe` on `deployment`: the measurement at the
    /// metric's [`MetricFacts::hydra_dataset`], the deployment's config and the
    /// RAQE's grouping, in the column of the RAQE's
    /// [`Raqe::accuracy_covers_share`] (a share between the measured ones
    /// reads the next stricter column), worst over records and seeds. A
    /// lossy merge of `L / x` windows reads the worse of the measured shard
    /// counts either side; past the largest, or with no measurement, it is
    /// unknown.
    // Assumes error depends on group shares, not on N, at fixed shares; the
    // study's N sweep backs it.
    fn hydra_accuracy(
        &self,
        raqe: &Raqe,
        deployment: &Deployment,
        facts: &WorkloadFacts,
    ) -> Option<f64> {
        let sketch = &deployment.config.sketch;
        let key = (
            sketch.clone(),
            format!("{:?}", config_params(&deployment.config)?),
            facts[&deployment.metric].hydra_dataset.as_ref()?.0.clone(),
            raqe.grouping_labels.clone(),
        );
        let by_shards = self.hydra.get(&key)?;
        let merges = if merges_lossily(sketch) {
            deployment.query_instance_count(raqe.lookback_ms)?
        } else {
            1
        };
        let column = HYDRA_COVERAGE
            .iter()
            .rposition(|&(share, _)| raqe.accuracy_covers_share.is_some_and(|t| t >= share))
            .unwrap_or(0);
        let (_, below) = by_shards.range(..=merges).next_back()?;
        let (_, above) = by_shards.range(merges..).next()?;
        Some(worse(
            below[column]?,
            above[column]?,
            accuracy_key(sketch).1,
        ))
    }

    /// The accuracy `raqe` gets from `deployment`, or `None` when it can't be
    /// read: no fitted [`DataShape`], a config or shape outside the grid, or
    /// too few items. [`SaturationCurves::load`] guarantees the curve's metric
    /// is the family's.
    /// Exact accumulators keep the cost table's value. `facts` must pass
    /// [`crate::validate_facts`].
    pub fn accuracy(
        &self,
        raqe: &Raqe,
        deployment: &Deployment,
        facts: &WorkloadFacts,
    ) -> Option<f64> {
        self.accuracy_with_source(raqe, deployment, facts)
            .map(|(value, _)| value)
    }

    /// [`SaturationCurves::accuracy`], and where the returned (worst) value
    /// came from: measured, or the algorithm's guarantee
    /// ([`crate::theory`]) for a bracketing point the study didn't measure
    /// (past an unsaturated curve, past the measured merge counts or N, or
    /// with no merge curve). Below the curves' first N, or outside the grid,
    /// it stays unknown.
    pub fn accuracy_with_source(
        &self,
        raqe: &Raqe,
        deployment: &Deployment,
        facts: &WorkloadFacts,
    ) -> Option<(f64, AccuracySource)> {
        let properties = family_properties(&deployment.config.sketch);
        if properties.exact {
            return table_accuracy(raqe, deployment).map(|v| (v, AccuracySource::Measured));
        }
        if properties.answers_any_subgrouping {
            return self
                .hydra_accuracy(raqe, deployment, facts)
                .map(|v| (v, AccuracySource::Measured));
        }
        let metric_facts = &facts[&deployment.metric];
        // The answered groups: the RAQE's, coarser than the deployment's on a
        // roll-up, whose answer is the merged coarse group's.
        let grouping = &raqe.grouping_labels;
        let shape = metric_facts.data_shape.get(grouping)?;
        let k = raqe.topk_k();
        let base = with_topk_k(&config_params(&deployment.config)?, TOPK_K);
        // A heap top-k reads the curves at the RAQE's k, else at the next
        // larger measured k whose curves bracket the shape (harder); past every
        // such k, the guarantee at k, no better than the largest bracketing k.
        let (points, past_measured_k) = if has_heap(&deployment.config.sketch) {
            self.curves_at_k(&deployment.config.sketch, &base, k, shape)?
        } else {
            (
                self.bracketing_points(&deployment.config.sketch, &base, shape)?,
                false,
            )
        };
        let covered = items_per_group(metric_facts, grouping, raqe.lookback_ms);
        let (_, direction) = accuracy_key(&deployment.config.sketch);
        let merges = if merges_lossily(&deployment.config.sketch) {
            // A roll-up also merges each coarse group's children, the average
            // `card(G_d) / card(G_r)` of them.
            // ponytail: average fan-out; the largest group's is sketch-bench#189.
            let fan_out = metric_facts.cardinality[&deployment.grouping_labels]
                .div_ceil(metric_facts.cardinality[grouping]);
            let merges = deployment.query_instance_count(raqe.lookback_ms)? * fan_out;
            // A heap of m · k merged from m windows reads as one sketch: the
            // merged heaps are taken to still hold the true top k.
            match (
                heap_capacity(&deployment.config),
                deployment.heap_needed(raqe.lookback_ms, k),
            ) {
                (Some(heap), Some(need)) if heap >= need => 1,
                _ => merges,
            }
        } else {
            1
        };
        // The guarantee is at the RAQE's own k.
        let params = with_topk_k(&base, k);
        let mut worst: Option<(f64, AccuracySource)> = None;
        for point in points {
            let measured = (!past_measured_k)
                .then(|| point.merged_error_at(merges, covered, direction))
                .flatten();
            let found = match measured {
                Some(error) => (error, AccuracySource::Measured),
                // Past every measured k as well as past the curve: the
                // guarantee at k, within the curves' N range as everywhere
                // else, and no better than what was measured.
                None if past_measured_k || covered >= point.first_measured_n(merges)? => {
                    if covered < point.first_measured_n(merges)? {
                        return None;
                    }
                    let bound =
                        theory::bound(&deployment.config.sketch, &params, point.shape, merges)?;
                    // No better than the measurement it extends: past every k,
                    // the largest k's at this N (else its last); past the
                    // curve, its last.
                    let floor = if past_measured_k {
                        point
                            .merged_error_at(merges, covered, direction)
                            .or_else(|| point.last_measured_error(merges, direction))
                    } else {
                        point.last_measured_error(merges, direction)
                    };
                    let error = match floor {
                        Some(last) => worse(bound, last, direction),
                        None => bound,
                    };
                    (error, AccuracySource::Theory)
                }
                None => return None,
            };
            if worst.is_none_or(|(w, _)| worse(w, found.0, direction) != w) {
                worst = Some(found);
            }
        }
        worst
    }

    /// The oracle for [`crate::autosketch::plan`]: AutoSketch keeps one
    /// unmerged sketch per query window.
    pub fn autosketch_accuracy(
        &self,
        raqe: &Raqe,
        config: &AtomicCostEntry,
        facts: &WorkloadFacts,
    ) -> Option<f64> {
        let (window_ms, slide_ms) = window_adapter(raqe);
        let deployment = Deployment {
            capability: raqe.capability,
            metric: raqe.metric.clone(),
            spatial_filter: raqe.spatial_filter.clone(),
            grouping_labels: raqe.grouping_labels.clone(),
            config: config.clone(),
            window_ms,
            slide_ms,
            key_tracker: None,
        };
        self.accuracy(raqe, &deployment, facts)
    }

    /// Checks the cost table against the family table and the curves. A
    /// candidate family's row must name the family's metric as its
    /// `accuracy_metric`. A sketch row's accuracy is the curve of its config
    /// and `measured_at.data_shape()`, read at `measured_at.items_per_instance`.
    /// Rows of sketches the study didn't run (exact accumulators among them)
    /// skip the curve check.
    pub fn check_cost_table(&self, costs: &[AtomicCostEntry]) -> CostTableCheck {
        let mut check = CostTableCheck::default();
        for row in costs {
            let name = format!("{} {}", row.sketch, row.sketch_config["params"]);
            let metric = &row.accuracy_metric;
            if is_candidate(&row.sketch) {
                if let Some((family_metric, _)) = family_properties(&row.sketch).accuracy {
                    if metric != family_metric {
                        check.mismatched.push(format!(
                            "{name}: accuracy_metric is {metric}, but the optimizer reads \
                             {family_metric}"
                        ));
                        continue;
                    }
                }
            }
            let Some(points) = self.points_by_sketch.get(&row.sketch) else {
                continue;
            };
            // Curves are measured at a heap of k; larger heaps are cost-only.
            if heap_capacity(row).is_some_and(|heap| heap != TOPK_K) {
                continue;
            }
            let params = config_params(row);
            let mut same_config = points
                .iter()
                .filter(|point| Some(&point.params) == params.as_ref())
                .peekable();
            if same_config.peek().is_none() {
                check
                    .mismatched
                    .push(format!("{name}: config not in the saturation grid"));
                continue;
            }
            let shape = row.measured_at.data_shape();
            let Some(point) = same_config.find(|point| Some(point.shape) == shape) else {
                check
                    .unchecked
                    .push(format!("{name}: no grid point at {shape:?}"));
                continue;
            };
            if &point.error_metric != metric {
                check.mismatched.push(format!(
                    "{name}: accuracy_metric is {metric}, the curve's {}",
                    point.error_metric
                ));
                continue;
            }
            let Some(value) = row.accuracy() else {
                check
                    .mismatched
                    .push(format!("{name}: the table has no {metric}"));
                continue;
            };
            let n = row.measured_at.items_per_instance as f64;
            match point.agrees_at(n, value) {
                None => check
                    .unchecked
                    .push(format!("{name}: N = {n} is outside the curve")),
                Some(false) => check.mismatched.push(format!(
                    "{name}: {metric} = {value} at N = {n}, curve {:?}",
                    point.curve
                )),
                Some(true) => {}
            }
        }
        // A heap top-k config priced at one heap only can't serve any heap
        // above it: every merged top-k candidate would quietly vanish.
        let mut heaps: BTreeMap<(String, String), BTreeSet<u64>> = BTreeMap::new();
        for row in costs.iter().filter(|row| is_candidate(&row.sketch)) {
            if let Some(heap) = heap_capacity(row) {
                let key = (row.sketch.clone(), format!("{:?}", config_params(row)));
                heaps.entry(key).or_default().insert(heap);
            }
        }
        for ((sketch, params), measured) in heaps {
            if measured.len() < 2 {
                check.mismatched.push(format!(
                    "{sketch} {params}: priced at heap {measured:?} only; rerun \
                     study_saturation.py --phase optimizer-cost for the heap sizes"
                ));
            }
        }
        check
    }

    /// Every grid point of `config` bracketing `shape` (Q8 of #156), or
    /// `None` when `config` isn't in the grid, `shape` lies outside it, or a
    /// bracketing point is missing.
    fn bracketing_points(
        &self,
        sketch: &str,
        params: &BTreeMap<String, f64>,
        shape: &DataShape,
    ) -> Option<Vec<&GridPoint>> {
        let points: Vec<&GridPoint> = self
            .points_by_sketch
            .get(sketch)?
            .iter()
            .filter(|point| &point.params == params)
            .collect();
        let quantile = points.first()?.distinct_keys().is_none();
        let shape_params = if quantile {
            bracket(points.iter().map(|p| p.shape_param()), shape.tail_index)?
        } else {
            bracket(points.iter().map(|p| p.shape_param()), shape.zipf_s)?
        };
        let keys = if quantile {
            vec![None]
        } else {
            let keys = points.iter().filter_map(|p| p.distinct_keys());
            bracket(keys, shape.distinct_keys)?
                .into_iter()
                .map(Some)
                .collect()
        };
        let bracketing: Vec<&GridPoint> = points
            .into_iter()
            .filter(|p| {
                shape_params.contains(&p.shape_param()) && keys.contains(&p.distinct_keys())
            })
            .collect();
        (bracketing.len() == shape_params.len() * keys.len()).then_some(bracketing)
    }
}

impl SaturationCurves {
    /// The grid points a top-`k` answer of `sketch` with `base` params reads
    /// on `shape`: those of the smallest measured k at or above `k` whose
    /// curves bracket `shape`, with `false`; else those of the largest k that
    /// brackets it, with `true` (only the guarantee at `k` applies, floored
    /// by them). `None` when no k's curves bracket `shape`.
    fn curves_at_k(
        &self,
        sketch: &str,
        base: &BTreeMap<String, f64>,
        k: u64,
        shape: &DataShape,
    ) -> Option<(Vec<&GridPoint>, bool)> {
        let measured: BTreeSet<u64> = self
            .points_by_sketch
            .get(sketch)?
            .iter()
            .filter(|point| same_but_topk_k(&point.params, base))
            .map(|point| params_topk_k(point.params.get("topk_k").copied()))
            .collect();
        let bracket_at = |at: u64| self.bracketing_points(sketch, &with_topk_k(base, at), shape);
        if let Some(points) = measured.range(k..).find_map(|&at| bracket_at(at)) {
            return Some((points, false));
        }
        measured
            .iter()
            .rev()
            .find_map(|&at| bracket_at(at))
            .map(|points| (points, true))
    }
}

/// Whether `a` and `b` name the same config, `topk_k` aside.
fn same_but_topk_k(a: &BTreeMap<String, f64>, b: &BTreeMap<String, f64>) -> bool {
    let rest = |m: &BTreeMap<String, f64>| m.iter().filter(|(name, _)| *name != "topk_k").count();
    rest(a) == rest(b)
        && a.iter()
            .filter(|(name, _)| *name != "topk_k")
            .all(|(name, value)| b.get(name) == Some(value))
}

/// `params` keyed at top-k `k`: a `topk_k` entry unless `k` is [`TOPK_K`],
/// which the grid and cost rows leave implicit.
fn with_topk_k(params: &BTreeMap<String, f64>, k: u64) -> BTreeMap<String, f64> {
    let mut params = params.clone();
    params.remove("topk_k");
    if let Some(k) = crate::topk_k_param(k) {
        params.insert("topk_k".to_string(), k as f64);
    }
    params
}

/// Items one group receives in `window_ms`: series per group times scrapes
/// in the window.
fn items_per_group(facts: &MetricFacts, grouping: &LabelSet, window_ms: u64) -> f64 {
    let series = facts.cardinality[&facts.labels] as f64;
    let groups = facts.cardinality[grouping] as f64;
    series / groups * window_ms as f64 / facts.scrape_interval_ms as f64
}

/// The grid values equal to `target`, else the two either side of it;
/// `None` outside the grid.
fn bracket(values: impl Iterator<Item = f64>, target: f64) -> Option<Vec<f64>> {
    let mut values: Vec<f64> = values.collect();
    values.sort_by(f64::total_cmp);
    values.dedup();
    if values.contains(&target) {
        return Some(vec![target]);
    }
    let above = values.partition_point(|&v| v < target);
    (above > 0 && above < values.len()).then(|| vec![values[above - 1], values[above]])
}

/// `config`'s params, less a top-k `heap`: `{"params": {"rows": 3, "cols":
/// 1024, "heap": 128}}` → `{rows: 3, cols: 1024}`. Curves are measured at a
/// heap of `TOPK_K`, and a larger heap reads the same curve.
fn config_params(config: &AtomicCostEntry) -> Option<BTreeMap<String, f64>> {
    config.sketch_config["params"]
        .as_object()?
        .iter()
        .filter(|(name, _)| *name != "heap")
        .map(|(name, value)| Some((name.clone(), value.as_f64()?)))
        .collect()
}

/// [`HYDRA_SATURATION`]'s rows of the Hydra variants some capability plans
/// (the rest, such as hydra-cms, are skipped), each column's worst over
/// records and seeds.
fn load_hydra(
    path: &Path,
) -> io::Result<BTreeMap<(String, String, String, LabelSet), HydraErrors>> {
    let mut out: BTreeMap<_, HydraErrors> = BTreeMap::new();
    let columns = [
        "variant",
        "config",
        "dataset",
        "group_columns",
        "merge_shards",
        HYDRA_COVERAGE[0].1,
        HYDRA_COVERAGE[1].1,
        HYDRA_COVERAGE[2].1,
    ];
    for row in read_csv(path, &columns)? {
        let variant = row["variant"].as_str();
        if !is_candidate(variant) || !family_properties(variant).answers_any_subgrouping {
            continue;
        }
        let shards: u64 = row["merge_shards"]
            .parse()
            .map_err(|_| invalid(format!("{HYDRA_SATURATION}: merge_shards: {row:?}")))?;
        let mut errors = [None; 3];
        for (error, (_, column)) in errors.iter_mut().zip(HYDRA_COVERAGE) {
            if !row[column].is_empty() {
                *error = Some(parse_number(&row[column])?);
            }
        }
        let key = (
            variant.to_string(),
            format!("{:?}", parse_config(&row["config"])?),
            row["dataset"].clone(),
            row["group_columns"]
                .split(',')
                .map(str::to_string)
                .collect(),
        );
        let direction = accuracy_key(variant).1;
        let by_shards = out.entry(key).or_default();
        match by_shards.get_mut(&shards) {
            None => {
                by_shards.insert(shards, errors);
            }
            Some(worst) => {
                // An empty cell means no covered group was scored in that
                // run (say, all had zero truth): it bounds nothing.
                for (worst, error) in worst.iter_mut().zip(errors) {
                    *worst = match (*worst, error) {
                        (Some(a), Some(b)) => Some(worse(a, b, direction)),
                        (a, b) => a.or(b),
                    };
                }
            }
        }
    }
    Ok(out)
}

/// `"rows=3 cols=1024"` → `{rows: 3, cols: 1024}`; the grid's config strings.
/// Fails on a pair that isn't `name=number`.
fn parse_config(config: &str) -> io::Result<BTreeMap<String, f64>> {
    config
        .split_whitespace()
        .map(|pair| {
            let (name, value) = pair
                .split_once('=')
                .ok_or_else(|| invalid(format!("config {config:?}: {pair:?} is not name=value")))?;
            let value = value
                .parse()
                .map_err(|_| invalid(format!("config {config:?}: {name} is not a number")))?;
            Ok((name.to_string(), value))
        })
        .collect()
}

/// A grid row's `dist`, `param` and `cardinality` as a [`MeasuredShape`].
fn parse_shape(row: &CsvRow) -> io::Result<MeasuredShape> {
    let param = parse_number(&row["param"])?;
    match row["dist"].as_str() {
        "zipf" => Ok(MeasuredShape::Zipf {
            skew: param,
            keys: parse_number(&row["cardinality"])?,
        }),
        "pareto" => Ok(MeasuredShape::Pareto { tail_index: param }),
        dist => Err(invalid(format!("unknown dist {dist:?}"))),
    }
}

fn parse_number(text: &str) -> io::Result<f64> {
    text.parse()
        .map_err(|_| invalid(format!("not a number: {text:?}")))
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

type CsvRow = BTreeMap<String, String>;

/// Fields may be quoted: `hydra_saturation.csv`'s groupings hold commas.
/// Fails unless the header has every `required` column and every row
/// exactly one field per header column.
fn read_csv(path: &Path, required: &[&str]) -> io::Result<Vec<CsvRow>> {
    let failed = |e: csv::Error| invalid(format!("{}: {e}", path.display()));
    let text = std::fs::read_to_string(path)
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(text.as_bytes());
    let header = reader.headers().map_err(failed)?.clone();
    let missing: Vec<_> = required
        .iter()
        .filter(|&&column| !header.iter().any(|name| name == column))
        .collect();
    if !missing.is_empty() {
        return Err(invalid(format!(
            "{}: missing columns {missing:?}",
            path.display()
        )));
    }
    reader
        .records()
        .map(|record| {
            let record = record.map_err(failed)?;
            if record.len() != header.len() {
                return Err(invalid(format!(
                    "{}: line {}: {} fields, the header has {}",
                    path.display(),
                    record.position().map_or(0, |p| p.line()),
                    record.len(),
                    header.len()
                )));
            }
            Ok(header
                .iter()
                .zip(record.iter())
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{facts, raqe, METRIC};
    use crate::Capability;

    const TOPK: &str = "cms-heap-topk-fastpath-vector2d";

    /// UnivMon's heaps are rebuilt on merge, so its cardinality reads merge
    /// curves like KLL; HLL's register max and DDSketch's bucket sums are
    /// exact.
    #[test]
    fn univmon_cardinality_merges_lossily_hll_and_dd_do_not() {
        assert!(merges_lossily("univmon-cardinality"));
        assert!(!merges_lossily("hll"));
        assert!(!merges_lossily("dd"));
    }

    /// Top-k rows=3 cols=1024 at θ ∈ {1.0, 1.2}, K ∈ {1e3, 1e5}. Precision
    /// falls with N; θ = 1.0 is worse, and K = 1e5 never saturates. Merged
    /// from 4 shards it loses 0.02 more, from 16 shards 0.1.
    fn curves() -> SaturationCurves {
        let curve = |offset: f64| {
            vec![
                (1e3, 1.0 - offset, 0.0),
                (1e4, 0.95 - offset, 0.01),
                (1e5, 0.9 - offset, 0.0),
            ]
        };
        let point = |theta: f64, keys: f64, offset: f64, n_sat: Option<f64>| GridPoint {
            params: parse_config("rows=3 cols=1024").unwrap(),
            shape: MeasuredShape::Zipf { skew: theta, keys },
            error_metric: "precision_at_k".into(),
            n_sat,
            curve: curve(offset),
            merged: BTreeMap::from([(4, curve(offset + 0.02)), (16, curve(offset + 0.1))]),
        };
        SaturationCurves {
            points_by_sketch: BTreeMap::from([(
                TOPK.to_string(),
                vec![
                    point(1.0, 1e3, 0.05, Some(1e4)),
                    point(1.2, 1e3, 0.0, Some(1e4)),
                    point(1.0, 1e5, 0.1, None),
                    point(1.2, 1e5, 0.08, None),
                ],
            )]),
            ..SaturationCurves::default()
        }
    }

    /// One group of `series` series scraped every second, shaped `shape`.
    fn workload(series: u64, shape: DataShape) -> WorkloadFacts {
        let mut facts = facts(1, series);
        let metric = facts.get_mut(METRIC).unwrap();
        metric.data_shape.insert(LabelSet::new(), shape);
        facts
    }

    fn shape(zipf_s: f64, distinct_keys: f64) -> DataShape {
        DataShape {
            zipf_s,
            distinct_keys,
            tail_index: 2.0,
        }
    }

    fn topk_raqe(lookback_ms: u64) -> Raqe {
        Raqe {
            capability: Capability::TopKByValue,
            ..raqe(lookback_ms, lookback_ms)
        }
    }

    fn deployment(sketch: &str, window_ms: u64) -> Deployment {
        Deployment {
            capability: Capability::TopKByValue,
            metric: METRIC.into(),
            spatial_filter: String::new(),
            grouping_labels: LabelSet::new(),
            config: AtomicCostEntry {
                sketch: sketch.into(),
                sketch_config: serde_json::json!({"params": {"rows": 3, "cols": 1024}}),
                mem_bytes_per_instance: 1.0,
                insert_cpu_secs: 1.0,
                merge_cpu_secs: 1.0,
                query_cpu_secs: 1.0,
                query_accuracy: BTreeMap::from([("precision_at_k".into(), 1.0)]),
                accuracy_metric: crate::test_support::metric_of(sketch),
                measured_at: crate::test_support::measured_at(),
            },
            window_ms,
            slide_ms: window_ms,
            key_tracker: None,
        }
    }

    #[test]
    fn reads_the_worse_neighbour_between_checkpoints_and_the_plateau_past_the_last() {
        let point = &curves().points_by_sketch[TOPK][1];
        let higher = AccuracyDirection::HigherIsBetter;
        assert_eq!(point.error_at(1e4, higher), Some(0.95));
        assert_eq!(point.error_at(3e4, higher), Some(0.9));
        assert_eq!(point.error_at(5e2, higher), None);
        assert_eq!(point.error_at(1e9, higher), Some(0.9));
        let unsaturated = &curves().points_by_sketch[TOPK][2];
        assert_eq!(unsaturated.error_at(1e9, higher), None);
    }

    #[test]
    fn takes_the_worst_bracketing_grid_point() {
        // 10 series × 1000 s = 1e4 items; θ = 1.1 brackets 1.0 and 1.2.
        let facts = workload(10, shape(1.1, 1e3));
        let r = topk_raqe(1_000_000);
        let value = curves().accuracy(&r, &deployment(TOPK, 1_000_000), &facts);
        assert_eq!(
            value,
            Some(0.95 - 0.05),
            "θ = 1.0 at 1e4 is worse than θ = 1.2"
        );
    }

    /// A hole in the grid (a bracketing point the study didn't measure)
    /// leaves the shape unknown rather than bridging to farther points:
    /// accuracy needn't be monotone in K.
    #[test]
    fn a_hole_in_the_grid_has_no_accuracy() {
        let mut curves = curves();
        // Drop θ = 1.2, K = 1e3.
        let hole = MeasuredShape::Zipf {
            skew: 1.2,
            keys: 1e3,
        };
        curves
            .points_by_sketch
            .get_mut(TOPK)
            .unwrap()
            .retain(|p| p.shape != hole);
        let r = topk_raqe(1_000_000);
        let d = deployment(TOPK, 1_000_000);
        assert_eq!(
            curves.accuracy(&r, &d, &workload(10, shape(1.1, 1e3))),
            None
        );
        // θ = 1.0 at K = 1e3 is still measured.
        assert!(curves
            .accuracy(&r, &d, &workload(10, shape(1.0, 1e3)))
            .is_some());
    }

    #[test]
    fn a_shape_outside_the_grid_or_without_a_fit_has_no_accuracy() {
        let r = topk_raqe(1_000_000);
        let d = deployment(TOPK, 1_000_000);
        assert_eq!(
            curves().accuracy(&r, &d, &workload(10, shape(0.5, 1e3))),
            None
        );
        assert_eq!(
            curves().accuracy(&r, &d, &workload(10, shape(1.0, 1e7))),
            None
        );
        assert_eq!(curves().accuracy(&r, &d, &facts(1, 10)), None);
    }

    #[test]
    fn merged_answers_read_the_merge_curve_at_the_lookback_count() {
        // 10 items/s: the 1e4 s lookback covers 1e5 items whatever the
        // window; top-k reads the curve of the sketch merged from L/x.
        let facts = workload(10, shape(1.2, 1e3));
        let r = topk_raqe(10_000_000);
        let at = |window_ms| curves().accuracy(&r, &deployment(TOPK, window_ms), &facts);
        // One window: the plain curve.
        assert_eq!(at(10_000_000), Some(0.9));
        // 4 windows: the 4-shard curve.
        assert!((at(2_500_000).unwrap() - 0.88).abs() < 1e-12);
        // 10 windows sit between 4 and 16 shards: the worse, 16.
        assert!((at(1_000_000).unwrap() - 0.8).abs() < 1e-12);
        // 100 windows: past the largest measured count, unmeasured.
        assert_eq!(at(100_000), None);
    }

    /// p99 by service from KLL by (service, endpoint) reads the coarse
    /// group's shape and items, merged from its 10 endpoints.
    #[test]
    fn a_kll_roll_up_reads_the_merge_curve_at_fan_out_times_windows() {
        use crate::test_support::{kll_by, label_set, quantile_by, service_endpoint_facts};
        let curve = |error: f64| vec![(1e3, error, 0.0), (1e4, error + 0.01, 0.0)];
        let kll = GridPoint {
            params: parse_config("k=200").unwrap(),
            shape: MeasuredShape::Pareto { tail_index: 2.0 },
            error_metric: "mean_rank_err".into(),
            n_sat: None,
            curve: curve(0.01),
            merged: BTreeMap::from([(4, curve(0.02)), (16, curve(0.05))]),
        };
        let curves = SaturationCurves {
            points_by_sketch: BTreeMap::from([("kll-percall".to_string(), vec![kll])]),
            ..SaturationCurves::default()
        };
        let mut facts = service_endpoint_facts();
        let metric = facts.get_mut(METRIC).unwrap();
        // Only the coarse grouping's shape is on the grid.
        metric
            .data_shape
            .insert(label_set(&["service"]), shape(1.0, 1.0));
        metric.data_shape.insert(
            label_set(&["service", "endpoint"]),
            DataShape {
                tail_index: 9.0,
                ..shape(1.0, 1.0)
            },
        );
        // 10 series per service × 100 s = 1e3 items, in one window.
        let by_service = quantile_by(&["service"], 100_000, 100_000);
        let accuracy =
            |grouping: &[&str]| curves.accuracy(&by_service, &kll_by(grouping, 100_000), &facts);
        assert_eq!(accuracy(&["service"]), Some(0.01));
        // 10 merged endpoints sit between 4 and 16 shards: the worse, 16.
        assert_eq!(accuracy(&["service", "endpoint"]), Some(0.05));
    }

    /// A heap of m · k merged from m windows reads as one sketch: the plain
    /// curve, not the merge curve, and no merge curve is needed.
    #[test]
    fn a_heap_of_m_times_k_reads_the_plain_curve() {
        let facts = workload(10, shape(1.2, 1e3));
        let r = topk_raqe(10_000_000);
        let with_heap = |window_ms, heap: u64| {
            let mut d = deployment(TOPK, window_ms);
            d.config.sketch_config["params"]["heap"] = heap.into();
            d
        };
        let mut plain_only = curves();
        for point in plain_only.points_by_sketch.get_mut(TOPK).unwrap() {
            point.merged.clear();
        }
        // 4 windows: a heap of 4k reads the plain curve; 2k doesn't, and
        // without a merge curve has no accuracy.
        assert_eq!(
            plain_only.accuracy(&r, &with_heap(2_500_000, 4 * TOPK_K), &facts),
            Some(0.9)
        );
        assert_eq!(
            plain_only.accuracy(&r, &with_heap(2_500_000, 2 * TOPK_K), &facts),
            None
        );
        // With merge curves, a small heap still reads them.
        assert!(
            (curves()
                .accuracy(&r, &with_heap(2_500_000, TOPK_K), &facts)
                .unwrap()
                - 0.88)
                .abs()
                < 1e-12
        );
    }

    /// Past a merge curve's last checkpoint the merged answer is unknown,
    /// even when the plain curve saturated: nothing shows the merged sketch
    /// stopped changing.
    #[test]
    fn a_merge_curve_has_no_plateau() {
        let point = &curves().points_by_sketch[TOPK][1];
        let higher = AccuracyDirection::HigherIsBetter;
        assert_eq!(point.n_sat, Some(1e4));
        assert!((point.merged_error_at(4, 1e5, higher).unwrap() - 0.88).abs() < 1e-12);
        assert_eq!(point.merged_error_at(4, 1e9, higher), None);
        assert_eq!(point.error_at(1e9, higher), Some(0.9));
    }

    /// Without a merge curve, a merged top-k answer has no known accuracy;
    /// one window still reads the plain curve.
    #[test]
    fn a_lossy_merge_without_a_merge_curve_is_unknown() {
        let mut curves = curves();
        for point in curves.points_by_sketch.get_mut(TOPK).unwrap() {
            point.merged.clear();
        }
        let facts = workload(10, shape(1.2, 1e3));
        let r = topk_raqe(10_000_000);
        assert_eq!(
            curves.accuracy(&r, &deployment(TOPK, 1_000_000), &facts),
            None
        );
        assert_eq!(
            curves.accuracy(&r, &deployment(TOPK, 10_000_000), &facts),
            Some(0.9)
        );
    }

    #[test]
    fn load_reads_merge_curves_beyond_one_shard() {
        let dir = std::env::temp_dir().join(format!("rqe-merge-{}", std::process::id()));
        let header = "family,sketch,config,dist,param,cardinality";
        let point = "quantile,kll-percall,k=200,pareto,2.0,";
        for run in RUN_DIRS {
            std::fs::create_dir_all(dir.join(run)).unwrap();
            std::fs::write(
                dir.join(run).join("saturation.csv"),
                format!(
                    "{header},n_sat,final_error,error_metric\n{point},1000,0.01,mean_rank_err\n"
                ),
            )
            .unwrap();
            std::fs::write(
                dir.join(run).join("saturation_curve.csv"),
                format!("{header},n,seed_mean_error,seed_se\n{point},1000,0.01,0\n"),
            )
            .unwrap();
        }
        // One shard is the plain curve. The later run measured only 16
        // shards: it keeps the earlier run's 4.
        let merge = |rows: &str| format!("{header},n,shards,seed_mean_error,seed_se\n{rows}");
        let merge_path = |run| dir.join(run).join("saturation_merge_curve.csv");
        std::fs::write(
            merge_path(RUN_DIRS[0]),
            merge(&format!("{point},1000,1,0.01,0\n{point},1000,4,0.02,0\n")),
        )
        .unwrap();
        std::fs::write(
            merge_path(RUN_DIRS[1]),
            merge(&format!("{point},1000,16,0.03,0\n")),
        )
        .unwrap();
        let loaded = SaturationCurves::load(&dir);
        // A merged KLL with no merge curve anywhere is refused.
        std::fs::remove_file(merge_path(RUN_DIRS[0])).unwrap();
        std::fs::write(merge_path(RUN_DIRS[1]), merge("")).unwrap();
        let missing = SaturationCurves::load(&dir);
        std::fs::remove_dir_all(&dir).unwrap();
        let point = &loaded.unwrap().points_by_sketch["kll-percall"][0];
        assert_eq!(
            point.merged,
            BTreeMap::from([(4, vec![(1e3, 0.02, 0.0)]), (16, vec![(1e3, 0.03, 0.0)])])
        );
        assert!(missing.unwrap_err().to_string().contains("no merge curve"));
    }

    /// Past an unsaturated curve the study measured nothing: the answer
    /// falls back to the algorithm's guarantee, and says so. A merged top-k
    /// answer has no guarantee.
    #[test]
    fn an_unmeasured_point_falls_back_to_the_guarantee() {
        // θ = 1.0, K = 1e5 never saturates; 10 series × 1e8 s = 1e9 items.
        let facts = workload(10, shape(1.0, 1e5));
        let r = topk_raqe(100_000_000);
        let one_window = curves().accuracy_with_source(&r, &deployment(TOPK, 100_000_000), &facts);
        let params = BTreeMap::from([("rows".to_string(), 3.0), ("cols".to_string(), 1024.0)]);
        let zipf = MeasuredShape::Zipf {
            skew: 1.0,
            keys: 1e5,
        };
        let bound = theory::bound(TOPK, &params, zipf, 1).unwrap();
        assert_eq!(one_window, Some((bound, AccuracySource::Theory)));
        let merged = curves().accuracy_with_source(&r, &deployment(TOPK, 10_000_000), &facts);
        assert_eq!(merged, None);
        // Measured where the curve has it.
        let measured = curves().accuracy_with_source(
            &r,
            &deployment(TOPK, 100_000_000),
            &workload(10, shape(1.2, 1e3)),
        );
        assert_eq!(
            measured.map(|(_, source)| source),
            Some(AccuracySource::Measured)
        );
    }

    /// A fallback's floor is the worse last measurement of both shard
    /// counts around the merge count, not only the smaller.
    #[test]
    fn a_fallback_floor_reads_both_bracketing_merge_curves() {
        let point = &curves().points_by_sketch[TOPK][1];
        let higher = AccuracyDirection::HigherIsBetter;
        // 4 shards end at 0.88, 16 at 0.80: 8 merges floor at 0.80.
        assert!((point.last_measured_error(8, higher).unwrap() - 0.8).abs() < 1e-12);
        assert!((point.last_measured_error(4, higher).unwrap() - 0.88).abs() < 1e-12);
        assert!((point.last_measured_error(1, higher).unwrap() - 0.9).abs() < 1e-12);
        // Past the largest count, the largest's.
        assert!((point.last_measured_error(100, higher).unwrap() - 0.8).abs() < 1e-12);
    }

    /// A top-k answer reads the curve at its k, else the next larger
    /// measured k; past every measured k, only the guarantee at k.
    #[test]
    fn a_topk_answer_reads_the_curve_at_its_k() {
        // The fixture's curves are at k = 32; add the same config at k = 100,
        // 0.2 lower in precision.
        let mut curves = curves();
        let at_100: Vec<GridPoint> = curves.points_by_sketch[TOPK]
            .iter()
            .map(|p| {
                let mut p = p.clone();
                p.params.insert("topk_k".into(), 100.0);
                p.curve.iter_mut().for_each(|c| c.1 -= 0.2);
                p
            })
            .collect();
        curves
            .points_by_sketch
            .get_mut(TOPK)
            .unwrap()
            .extend(at_100);
        // 10 series × 1e4 s = 1e5 items on θ = 1.2, K = 1e3: k = 32 reads 0.9.
        let facts = workload(10, shape(1.2, 1e3));
        let read = |k| {
            let r = Raqe {
                topk_k: Some(k),
                ..topk_raqe(10_000_000)
            };
            let mut d = deployment(TOPK, 10_000_000);
            d.config.sketch_config["params"]["heap"] = (k.max(TOPK_K)).into();
            curves.accuracy_with_source(&r, &d, &facts)
        };
        let close = |got: Option<(f64, AccuracySource)>, want: f64, source| {
            let (v, s) = got.unwrap();
            assert!((v - want).abs() < 1e-12 && s == source, "{v} {s:?}");
        };
        close(read(32), 0.9, AccuracySource::Measured);
        // k = 10 isn't measured: the next larger, 32.
        close(read(10), 0.9, AccuracySource::Measured);
        // k = 50 reads k = 100's curve.
        close(read(50), 0.7, AccuracySource::Measured);
        close(read(100), 0.7, AccuracySource::Measured);
        // k = 200: past every measured k, the guarantee at 200, no better than
        // k = 100's measured 0.7.
        let (value, source) = read(200).unwrap();
        assert_eq!(source, AccuracySource::Theory);
        assert!(value <= 0.7 + 1e-12, "{value}");
    }

    /// A larger k whose curves don't bracket the shape is skipped: the
    /// answer falls to the guarantee, floored by a k that does bracket it.
    #[test]
    fn a_k_whose_curves_miss_the_shape_falls_to_the_guarantee() {
        // k = 100 measured at K = 1e3 only; the shape below needs K = 1e3..1e5.
        let mut curves = curves();
        let at_100: Vec<GridPoint> = curves.points_by_sketch[TOPK]
            .iter()
            .filter(|p| p.distinct_keys() == Some(1e3))
            .map(|p| {
                let mut p = p.clone();
                p.params.insert("topk_k".into(), 100.0);
                p
            })
            .collect();
        curves
            .points_by_sketch
            .get_mut(TOPK)
            .unwrap()
            .extend(at_100);
        let facts = workload(10, shape(1.2, 1e4));
        let r = Raqe {
            topk_k: Some(50),
            ..topk_raqe(10_000_000)
        };
        let mut d = deployment(TOPK, 10_000_000);
        d.config.sketch_config["params"]["heap"] = 50.into();
        let (_, source) = curves.accuracy_with_source(&r, &d, &facts).unwrap();
        assert_eq!(source, AccuracySource::Theory);
    }

    /// A top-`k` RAQE merged from m windows needs a heap of m·k.
    #[test]
    fn the_heap_needed_is_merges_times_k() {
        assert_eq!(crate::heap_needed(60_000, 15_000, 10), Some(40));
        assert_eq!(crate::heap_needed(60_000, 15_000, TOPK_K), Some(4 * TOPK_K));
        assert_eq!(crate::heap_needed(60_000, 7_000, 10), None);
    }

    #[test]
    fn exact_accumulators_keep_the_cost_table_value() {
        let r = topk_raqe(1_000_000);
        let mut d = deployment("exact-sum", 1_000_000);
        d.config.query_accuracy = BTreeMap::from([("relative_error".into(), 0.0)]);
        let value = curves().accuracy(&r, &d, &facts(1, 10));
        assert_eq!(value, Some(0.0));
    }

    /// A top-k row measured at θ = 1.2, K = 1e3 and `items`, scoring
    /// `precision`.
    fn measured_row(cols: u64, items: u64, precision: f64) -> AtomicCostEntry {
        use aqpbm_core::{DataDistribution, MeasuredAt, ZipfParameter};
        AtomicCostEntry {
            sketch_config: serde_json::json!({"params": {"rows": 3, "cols": cols}}),
            query_accuracy: BTreeMap::from([("precision_at_k".into(), precision)]),
            measured_at: MeasuredAt {
                items_per_instance: items,
                keys_per_instance: Some(1_000),
                value_range: None,
                merge_operand_items: None,
                distribution: Some(DataDistribution::Zipf(ZipfParameter {
                    skewness: 1.2,
                    population_size: 1_000,
                    seed: 1,
                })),
                dataset: None,
                schema_width: None,
            },
            ..deployment(TOPK, 1).config
        }
    }

    #[test]
    fn the_cost_table_is_a_point_on_the_curve() {
        // Each top-k row also priced at a large heap, as the study does.
        let check = |rows: &[AtomicCostEntry]| {
            let mut all = rows.to_vec();
            for row in rows.iter().filter(|row| heap_capacity(row).is_some()) {
                let mut large = row.clone();
                large.sketch_config["params"]["heap"] = (64 * TOPK_K).into();
                all.push(large);
            }
            curves().check_cost_table(&all)
        };
        // Priced at one heap only: no merged top-k candidate could be built.
        let lone = curves().check_cost_table(&[measured_row(1024, 10_000, 0.93)]);
        assert_eq!(lone.mismatched.len(), 1, "{lone:?}");
        assert!(lone.mismatched[0].contains("priced at heap"), "{lone:?}");
        // A heap of k is checked like no heap; a larger heap is cost-only.
        let with_heap = |heap: u64, precision| {
            let mut row = measured_row(1024, 10_000, precision);
            row.sketch_config["params"]["heap"] = heap.into();
            row
        };
        assert_eq!(check(&[with_heap(TOPK_K, 0.8)]).mismatched.len(), 1);
        assert_eq!(
            check(&[with_heap(4 * TOPK_K, 0.8)]),
            CostTableCheck::default()
        );
        // θ = 1.2, K = 1e3 reads 0.95 ± 3 × 0.01 + 5% at N = 1e4.
        assert_eq!(
            check(&[measured_row(1024, 10_000, 0.93)]),
            CostTableCheck::default()
        );
        assert_eq!(
            check(&[measured_row(1024, 10_000, 0.8)]).mismatched.len(),
            1
        );
        // Between checkpoints: the range of the two either side.
        assert!(check(&[measured_row(1024, 30_000, 0.92)])
            .mismatched
            .is_empty());
        // A config off the grid is a mismatch; a shape or N the study didn't
        // run is unchecked.
        assert_eq!(
            check(&[measured_row(2048, 10_000, 0.95)]).mismatched.len(),
            1
        );
        assert_eq!(check(&[measured_row(1024, 100, 0.95)]).unchecked.len(), 1);
        let mut pareto = measured_row(1024, 10_000, 0.95);
        pareto.measured_at.distribution = None;
        assert_eq!(check(&[pareto]).unchecked.len(), 1);
        // A row naming another metric than its family's is a mismatch.
        let mut other_metric = measured_row(1024, 10_000, 0.95);
        other_metric.query_accuracy = BTreeMap::from([("recall_at_k".into(), 0.95)]);
        other_metric.accuracy_metric = "recall_at_k".into();
        // (Its large-heap copy names the same wrong metric.)
        let wrong = check(&[other_metric]).mismatched;
        assert!(
            !wrong.is_empty() && wrong.iter().all(|m| m.contains("recall_at_k")),
            "{wrong:?}"
        );
        // Sketches the study didn't run, exact ones among them, skip the
        // curve, but still name their family's metric.
        let exact = AtomicCostEntry {
            sketch: "exact-sum".into(),
            query_accuracy: BTreeMap::from([("relative_error".into(), 0.0)]),
            accuracy_metric: "relative_error".into(),
            ..measured_row(1024, 10_000, 0.0)
        };
        assert_eq!(
            check(std::slice::from_ref(&exact)),
            CostTableCheck::default()
        );
        let misnamed = AtomicCostEntry {
            accuracy_metric: "relative_error_mean".into(),
            ..exact
        };
        assert_eq!(check(&[misnamed]).mismatched.len(), 1);
    }

    /// Each top-k k is its own grid: k = 32 on θ 1.0 and k = 10 on θ 1.2
    /// are each a full cross, though their union isn't, and load accepts them.
    /// A hole within one k is still refused.
    #[test]
    fn the_full_cross_is_checked_per_k() {
        let write = |tag: &str, points: &[String]| {
            let dir = std::env::temp_dir().join(format!("rqe-k-{tag}-{}", std::process::id()));
            let header = "family,sketch,config,dist,param,cardinality";
            for run in RUN_DIRS {
                std::fs::create_dir_all(dir.join(run)).unwrap();
                let summary: String = points
                    .iter()
                    .map(|p| format!("{p},1000,0.9,precision_at_k\n"))
                    .collect();
                let curve: String = points.iter().map(|p| format!("{p},1000,0.9,0\n")).collect();
                std::fs::write(
                    dir.join(run).join("saturation.csv"),
                    format!("{header},n_sat,final_error,error_metric\n{summary}"),
                )
                .unwrap();
                std::fs::write(
                    dir.join(run).join("saturation_curve.csv"),
                    format!("{header},n,seed_mean_error,seed_se\n{curve}"),
                )
                .unwrap();
            }
            let loaded = SaturationCurves::load(&dir).map(|_| ());
            std::fs::remove_dir_all(&dir).unwrap();
            loaded
        };
        let point = |config: &str, shape: &str| format!("topk,{TOPK},{config},zipf,{shape}");
        let per_k = [
            point("rows=3 cols=1024", "1.0,1000"),
            point("rows=3 cols=1024", "1.0,100000"),
            point("rows=3 cols=1024 topk_k=10", "1.2,1000"),
            point("rows=3 cols=1024 topk_k=10", "1.2,100000"),
        ];
        write("ok", &per_k).expect("each k is a full cross");
        let holed = [
            per_k[0].clone(),
            per_k[1].clone(),
            point("rows=3 cols=1024 topk_k=10", "1.0,1000"),
            point("rows=3 cols=1024 topk_k=10", "1.2,100000"),
        ];
        let err = write("hole", &holed).unwrap_err().to_string();
        assert!(
            err.contains("topk_k") && err.contains("full cross"),
            "{err}"
        );
    }

    #[test]
    fn load_refuses_a_grid_that_isnt_a_full_cross() {
        let dir = std::env::temp_dir().join(format!("rqe-cross-{}", std::process::id()));
        let header = "family,sketch,config,dist,param,cardinality";
        let points = ["1.0,1000", "1.2,1000", "1.0,100000"]
            .map(|shape| format!("topk,{TOPK},rows=3 cols=1024,zipf,{shape}"));
        for run in RUN_DIRS {
            std::fs::create_dir_all(dir.join(run)).unwrap();
            let summary: String = points
                .iter()
                .map(|p| format!("{p},1000,0.9,precision_at_k\n"))
                .collect();
            let curve: String = points.iter().map(|p| format!("{p},1000,0.9,0\n")).collect();
            std::fs::write(
                dir.join(run).join("saturation.csv"),
                format!("{header},n_sat,final_error,error_metric\n{summary}"),
            )
            .unwrap();
            std::fs::write(
                dir.join(run).join("saturation_curve.csv"),
                format!("{header},n,seed_mean_error,seed_se\n{curve}"),
            )
            .unwrap();
        }
        let loaded = SaturationCurves::load(&dir);
        std::fs::remove_dir_all(&dir).unwrap();
        let err = loaded.unwrap_err().to_string();
        assert!(
            err.contains("full cross") && err.contains("(1.2, 100000.0)"),
            "{err}"
        );
    }

    #[test]
    fn load_takes_a_point_in_both_runs_from_the_1e9_run() {
        let dir = std::env::temp_dir().join(format!("rqe-saturation-{}", std::process::id()));
        let header = "family,sketch,config,dist,param,cardinality";
        for (run, n_sat, last) in [
            (RUN_DIRS[0], "not_saturated", "1e7"),
            (RUN_DIRS[1], "1e8", "1e9"),
        ] {
            std::fs::create_dir_all(dir.join(run)).unwrap();
            let point = "quantile,kll-percall,k=200,pareto,2.0,";
            std::fs::write(
                dir.join(run).join("saturation.csv"),
                format!(
                    "{header},n_sat,final_error,error_metric\n{point},{n_sat},0.01,mean_rank_err\n"
                ),
            )
            .unwrap();
            std::fs::write(
                dir.join(run).join("saturation_curve.csv"),
                format!("{header},n,seed_mean_error,seed_se\n{point},1000,0.05,0\n{point},{last},0.01,0\n"),
            )
            .unwrap();
            std::fs::write(
                dir.join(run).join("saturation_merge_curve.csv"),
                format!("{header},n,shards,seed_mean_error,seed_se\n{point},1000,4,0.06,0\n"),
            )
            .unwrap();
        }
        let loaded = SaturationCurves::load(&dir).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        let points = &loaded.points_by_sketch["kll-percall"];
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].n_sat, Some(1e8));
        assert_eq!(points[0].curve, vec![(1e3, 0.05, 0.0), (1e9, 0.01, 0.0)]);
        assert_eq!(points[0].shape, MeasuredShape::Pareto { tail_index: 2.0 });
        assert_eq!(points[0].params, BTreeMap::from([("k".to_string(), 200.0)]));
    }

    /// Top-k merges as one sketch when its heap holds m · k, so a study needs
    /// no merge curves for it.
    #[test]
    fn load_needs_no_merge_curves_for_heap_topk() {
        let dir = std::env::temp_dir().join(format!("rqe-topk-nomerge-{}", std::process::id()));
        let header = "family,sketch,config,dist,param,cardinality";
        let point = format!("topk,{TOPK},rows=3 cols=1024,zipf,1.2,1000");
        for run in RUN_DIRS {
            std::fs::create_dir_all(dir.join(run)).unwrap();
            std::fs::write(
                dir.join(run).join("saturation.csv"),
                format!(
                    "{header},n_sat,final_error,error_metric\n{point},1000,0.9,precision_at_k\n"
                ),
            )
            .unwrap();
            std::fs::write(
                dir.join(run).join("saturation_curve.csv"),
                format!("{header},n,seed_mean_error,seed_se\n{point},1000,0.9,0\n"),
            )
            .unwrap();
        }
        let loaded = SaturationCurves::load(&dir);
        std::fs::remove_dir_all(&dir).unwrap();
        assert!(loaded.is_ok(), "{:?}", loaded.err());
    }

    /// A curve for another metric than the family's, such as DD's max error
    /// before #175, is a stale study: refused at load, whatever the lookback.
    #[test]
    fn load_refuses_a_curve_for_another_metric() {
        let dir = std::env::temp_dir().join(format!("rqe-stale-{}", std::process::id()));
        let header = "family,sketch,config,dist,param,cardinality";
        let point = "quantile,dd,alpha=0.01,pareto,2.0,";
        for run in RUN_DIRS {
            std::fs::create_dir_all(dir.join(run)).unwrap();
            std::fs::write(
                dir.join(run).join("saturation.csv"),
                format!(
                    "{header},n_sat,final_error,error_metric\n{point},1e8,0.01,max_relative_value_error\n"
                ),
            )
            .unwrap();
            std::fs::write(
                dir.join(run).join("saturation_curve.csv"),
                format!("{header},n,seed_mean_error,seed_se\n"),
            )
            .unwrap();
        }
        let loaded = SaturationCurves::load(&dir);
        std::fs::remove_dir_all(&dir).unwrap();
        let error = loaded.unwrap_err().to_string();
        assert!(
            error.contains("dd curve measured max_relative_value_error"),
            "{error}"
        );
    }

    const HYDRA_HEADER: &str = "variant,config,R,W,dataset,group_columns,schema_width,\
        records,fanned_mass,merge_shards,seed,err_mean,err_p50,err_p90,err_max,groups_scored,\
        err_max_cov_0.01,err_max_cov_0.05";

    /// A `hydra_saturation.csv` row on `hydra_http` at rows=3 cols=1024:
    /// `variant`, `grouping` (label names), records, merge shards, seed, then
    /// err_max, err_max_cov_0.01 and err_max_cov_0.05 as written.
    fn hydra_row(
        variant: &str,
        grouping: &str,
        records: f64,
        shards: u64,
        seed: u64,
        errors: [&str; 3],
    ) -> String {
        let [all, cov_1, cov_5] = errors;
        format!(
            "{variant},rows=3 cols=1024,3,1024,hydra_http,\"{grouping}\",2,{records},0,{shards},\
             {seed},0,0,0,{all},10,{cov_1},{cov_5}"
        )
    }

    /// A saturation dir with empty curves and, when `rows` is `Some`, a
    /// `hydra_saturation.csv` of them, loaded. `tag` keeps parallel tests'
    /// dirs apart.
    fn hydra_curves(tag: &str, rows: Option<&[String]>) -> SaturationCurves {
        let csv = rows.map(|rows| format!("{HYDRA_HEADER}\n{}\n", rows.join("\n")));
        load_hydra_csv(tag, csv).unwrap()
    }

    /// [`hydra_curves`] with `csv`, when `Some`, as `hydra_saturation.csv`.
    fn load_hydra_csv(tag: &str, csv: Option<String>) -> io::Result<SaturationCurves> {
        let dir = std::env::temp_dir().join(format!("rqe-hydra-{tag}-{}", std::process::id()));
        for run in RUN_DIRS {
            std::fs::create_dir_all(dir.join(run)).unwrap();
            let header = "family,sketch,config,dist,param,cardinality";
            std::fs::write(
                dir.join(run).join("saturation.csv"),
                format!("{header},n_sat,final_error,error_metric\n"),
            )
            .unwrap();
            std::fs::write(
                dir.join(run).join("saturation_curve.csv"),
                format!("{header},n,seed_mean_error,seed_se\n"),
            )
            .unwrap();
        }
        if let Some(csv) = csv {
            std::fs::write(dir.join(HYDRA_SATURATION), csv).unwrap();
        }
        let loaded = SaturationCurves::load(&dir);
        std::fs::remove_dir_all(&dir).unwrap();
        loaded
    }

    /// A malformed `hydra_saturation.csv` fails the load, naming the
    /// problem, rather than leaving Hydra silently without accuracy.
    #[test]
    fn a_malformed_hydra_csv_is_an_error() {
        let row = hydra_row("hydra-hll", "service", 1e5, 1, 0, ["0.1"; 3]);
        let error =
            |tag: &str, csv: String| load_hydra_csv(tag, Some(csv)).unwrap_err().to_string();
        let no_cov = HYDRA_HEADER.trim_end_matches(",err_max_cov_0.05");
        let missing = error("missing", format!("{no_cov}\n{row}\n"));
        assert!(
            missing.contains("missing columns [\"err_max_cov_0.05\"]"),
            "{missing}"
        );
        let (short_row, _) = row.rsplit_once(',').unwrap();
        let short = error("short", format!("{HYDRA_HEADER}\n{short_row}\n"));
        assert!(
            short.contains("line 2: 17 fields, the header has 18"),
            "{short}"
        );
        let long = error("long", format!("{HYDRA_HEADER}\n{row},extra\n"));
        assert!(
            long.contains("line 2: 19 fields, the header has 18"),
            "{long}"
        );
        let wide = row.replace("cols=1024", "cols=wide");
        let config = error("config", format!("{HYDRA_HEADER}\n{wide}\n"));
        assert!(config.contains("cols is not a number"), "{config}");
    }

    /// 5 services × 10 endpoints, measured as `hydra_http`.
    fn hydra_facts() -> WorkloadFacts {
        let mut facts = crate::test_support::service_endpoint_facts();
        let schema = crate::test_support::label_set(&["service", "endpoint"]);
        facts.get_mut(METRIC).unwrap().hydra_dataset = Some(("hydra_http".into(), schema));
        facts
    }

    fn tracker() -> AtomicCostEntry {
        AtomicCostEntry {
            sketch: crate::KEY_TRACKER_FAMILY.into(),
            sketch_config: serde_json::json!({"params": {}}),
            mem_bytes_per_instance: 10.0,
            accuracy_metric: "relative_error".into(),
            ..deployment(TOPK, 60_000).config
        }
    }

    /// A `sketch` grid at rows=3 cols=1024 over (service, endpoint), with
    /// its key tracker.
    fn hydra(sketch: &str, window_ms: u64) -> Deployment {
        let base = deployment(sketch, window_ms);
        Deployment {
            capability: if sketch == "hydra-kll" {
                Capability::Quantile
            } else {
                Capability::Cardinality
            },
            grouping_labels: crate::test_support::label_set(&["service", "endpoint"]),
            config: AtomicCostEntry {
                mem_bytes_per_instance: 2_000.0,
                ..base.config.clone()
            },
            key_tracker: Some(tracker()),
            ..base
        }
    }

    /// A distinct count by `grouping` over `lookback_ms` within 5%, covering
    /// groups of share `covers` (`None`: every group).
    fn distinct_by(grouping: &[&str], lookback_ms: u64, covers: Option<f64>) -> Raqe {
        Raqe {
            capability: Capability::Cardinality,
            accuracy_sla: 0.05,
            accuracy_covers_share: covers,
            ..crate::test_support::quantile_by(grouping, lookback_ms, lookback_ms)
        }
    }

    #[test]
    fn hydra_reads_the_column_its_coverage_selects() {
        let rows = [hydra_row(
            "hydra-hll",
            "service",
            1e5,
            1,
            0,
            ["0.3", "0.2", "0.1"],
        )];
        let curves = hydra_curves("coverage", Some(&rows));
        let at = |covers| {
            curves.accuracy(
                &distinct_by(&["service"], 60_000, covers),
                &hydra("hydra-hll", 60_000),
                &hydra_facts(),
            )
        };
        assert_eq!(at(None), Some(0.3));
        assert_eq!(at(Some(0.01)), Some(0.2));
        assert_eq!(at(Some(0.05)), Some(0.1));
        assert_eq!(at(Some(0.5)), Some(0.1));
        // Between the measured shares, the next stricter column; below the
        // smallest, every group's.
        assert_eq!(at(Some(0.03)), Some(0.2));
        assert_eq!(at(Some(0.001)), Some(0.3));
    }

    /// Error is taken to depend on shares, not N: the worst over the
    /// measured records and seeds. An empty cell (no covered group scored)
    /// is skipped; a column every row left empty is unknown.
    #[test]
    fn hydra_takes_the_worst_over_records_and_seeds() {
        let rows = [
            hydra_row("hydra-hll", "service", 1e5, 1, 0, ["0.1", "", "0.1"]),
            hydra_row("hydra-hll", "service", 1e6, 1, 0, ["0.15", "0.1", ""]),
            hydra_row("hydra-hll", "service", 1e6, 1, 1, ["0.12", "", ""]),
        ];
        let curves = hydra_curves("worst", Some(&rows));
        let at = |covers| {
            curves.accuracy(
                &distinct_by(&["service"], 60_000, covers),
                &hydra("hydra-hll", 60_000),
                &hydra_facts(),
            )
        };
        assert_eq!(at(None), Some(0.15));
        assert_eq!(at(Some(0.01)), Some(0.1));
        assert_eq!(at(Some(0.05)), Some(0.1));
        let rows = [hydra_row(
            "hydra-hll",
            "service",
            1e5,
            1,
            0,
            ["0.1", "0.1", ""],
        )];
        let curves = hydra_curves("all_empty", Some(&rows));
        let at_5pct = curves.accuracy(
            &distinct_by(&["service"], 60_000, Some(0.05)),
            &hydra("hydra-hll", 60_000),
            &hydra_facts(),
        );
        assert_eq!(at_5pct, None);
    }

    /// hydra-kll merges lossily: `L / x` windows read the worse of the
    /// measured shard counts either side, and past the largest nothing.
    /// hydra-hll merges exactly and reads one shard at any merge count.
    #[test]
    fn a_lossy_hydra_merge_reads_the_worse_bracketing_shard_count() {
        let rows: Vec<String> = ["hydra-kll", "hydra-hll"]
            .iter()
            .flat_map(|variant| {
                [(1, "0.01"), (4, "0.02"), (16, "0.05")]
                    .map(|(shards, err)| hydra_row(variant, "service", 1e5, shards, 0, [err; 3]))
            })
            .collect();
        let curves = hydra_curves("merges", Some(&rows));
        let at = |sketch: &str, merges: u64| {
            let mut r = distinct_by(&["service"], 60_000 * merges, None);
            if sketch == "hydra-kll" {
                r.capability = Capability::Quantile;
            }
            curves.accuracy(&r, &hydra(sketch, 60_000), &hydra_facts())
        };
        assert_eq!(at("hydra-kll", 1), Some(0.01));
        assert_eq!(at("hydra-kll", 2), Some(0.02));
        assert_eq!(at("hydra-kll", 4), Some(0.02));
        assert_eq!(at("hydra-kll", 8), Some(0.05));
        assert_eq!(at("hydra-kll", 16), Some(0.05));
        assert_eq!(at("hydra-kll", 32), None);
        assert_eq!(at("hydra-hll", 32), Some(0.01));
    }

    /// Hydra's accuracy is its own measurement or nothing: no file, no
    /// dataset for the metric, or an unmeasured grouping leaves it unknown.
    #[test]
    fn without_a_hydra_measurement_no_hydra_deployment_has_accuracy() {
        let r = distinct_by(&["service"], 60_000, None);
        let d = hydra("hydra-hll", 60_000);
        let rows = [hydra_row("hydra-hll", "service", 1e5, 1, 0, ["0.01"; 3])];
        let measured = hydra_curves("measured", Some(&rows));
        assert_eq!(measured.accuracy(&r, &d, &hydra_facts()), Some(0.01));
        let absent = hydra_curves("absent", None);
        assert_eq!(absent.accuracy(&r, &d, &hydra_facts()), None);
        let no_dataset = crate::test_support::service_endpoint_facts();
        assert_eq!(measured.accuracy(&r, &d, &no_dataset), None);
        let by_endpoint = distinct_by(&["service", "endpoint"], 60_000, None);
        assert_eq!(measured.accuracy(&by_endpoint, &d, &hydra_facts()), None);
    }

    /// Distinct counts by (service) and (service, endpoint). Per-group HLL
    /// at (service, endpoint) serves both, a roll-up for (service), at
    /// 50 · 1000 B per window; one Hydra grid over the same schema holds
    /// them in 2000 B plus a 10 B tracker key per group. Memory-priced, the
    /// grid wins while its measured error clears the SLA, and loses
    /// otherwise.
    #[test]
    fn a_hydra_grid_beats_per_group_hll_on_memory_only_when_accurate() {
        use crate::candidates::build_all_candidates;
        use crate::milp::minimize_usage_cost;
        let facts = hydra_facts();
        let raqes = [
            distinct_by(&["service"], 60_000, None),
            distinct_by(&["service", "endpoint"], 60_000, None),
        ];
        let named = |sketch: &str, memory: f64| AtomicCostEntry {
            mem_bytes_per_instance: memory,
            accuracy_metric: "relative_error".into(),
            measured_at: aqpbm_core::MeasuredAt {
                dataset: Some("hydra_http".into()),
                ..crate::test_support::measured_at()
            },
            ..deployment(sketch, 60_000).config
        };
        let costs = [
            named("hll", 1_000.0),
            named("hydra-hll", 2_000.0),
            tracker(),
        ];
        let plan = |error: &str| {
            let rows = ["service", "service,endpoint"]
                .map(|grouping| hydra_row("hydra-hll", grouping, 1e5, 1, 0, [error; 3]));
            let curves = hydra_curves(&format!("milp-{error}"), Some(&rows));
            // HLL passes everywhere; Hydra reads its measurement.
            let accuracy = |r: &Raqe, d: &Deployment| {
                if d.properties().answers_any_subgrouping {
                    curves.accuracy(r, d, &facts)
                } else {
                    Some(0.0)
                }
            };
            let without = build_all_candidates(&raqes, &costs, &facts, false, &accuracy);
            assert!(without.iter().all(|d| d.config.sketch == "hll"));
            let candidates = build_all_candidates(&raqes, &costs, &facts, true, &accuracy);
            let solved =
                minimize_usage_cost(&raqes, &candidates, &facts, 0.0, 1.0, &accuracy, None, None)
                    .unwrap();
            solved
                .mapping
                .iter()
                .map(|&d| candidates[d].clone())
                .collect::<Vec<_>>()
        };
        let accurate = plan("0.01");
        assert!(accurate.iter().all(|d| d.config.sketch == "hydra-hll"));
        assert_eq!(accurate[0], accurate[1], "one grid serves both");
        let inaccurate = plan("0.2");
        assert!(inaccurate.iter().all(|d| d.config.sketch == "hll"));
    }
}
