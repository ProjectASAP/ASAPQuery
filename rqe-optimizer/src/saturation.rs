//! Sketch accuracy read from the saturation study's error-vs-N curves
//! (`scripts/study_saturation.py`, #130, #140) at the number of items a
//! deployment's answer covers. Design decisions: #156.
//!
//! The curve is read at the number of items one group receives over the
//! RAQE's whole lookback: series per group times scrapes per lookback. That
//! holds whether the answer is one sketch or a merge of many smaller-window
//! sketches; the window size doesn't matter.
//! KLL and top-k merge lossily (#131); their merge penalty is #158.
//!
//! The cost table's sketch accuracies are not read: each is one point on a
//! curve, at the row's `measured_at`, and [`SaturationCurves::check_cost_table`]
//! checks that the two agree there (#171).

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use crate::autosketch::window_adapter;
use crate::{
    accuracy_key, family_properties, table_accuracy, AccuracyDirection, AtomicCostEntry,
    Capability, Deployment, LabelSet, MetricFacts, Raqe, WorkloadFacts,
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
            self.n_sat?;
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
        let &(first_n, ..) = self.curve.first()?;
        let &(last_n, last_error, _) = self.curve.last()?;
        if n < first_n {
            return None;
        }
        if n >= last_n {
            return self.n_sat.map(|_| last_error);
        }
        let above = self
            .curve
            .partition_point(|&(checkpoint, ..)| checkpoint < n);
        let (checkpoint, error, _) = self.curve[above];
        if checkpoint == n {
            Some(error)
        } else {
            Some(worse(error, self.curve[above - 1].1, direction))
        }
    }
}

fn worse(a: f64, b: f64, direction: AccuracyDirection) -> f64 {
    match direction {
        AccuracyDirection::LowerIsBetter => a.max(b),
        AccuracyDirection::HigherIsBetter => a.min(b),
    }
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
}

impl SaturationCurves {
    /// Reads `saturation.csv` (for the saturation point) and `saturation_curve.csv` from
    /// each of [`RUN_DIRS`] under `dir`. Fails on a candidate family's curve
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
            for row in read_csv(&run_dir.join("saturation.csv"))? {
                let point = GridPoint {
                    params: parse_config(&row["config"]),
                    shape: parse_shape(&row)?,
                    error_metric: row["error_metric"].clone(),
                    n_sat: match row["n_sat"].as_str() {
                        "not_saturated" => None,
                        n => Some(parse_number(n)?),
                    },
                    curve: Vec::new(),
                };
                let sketch = row["sketch"].as_str();
                let is_candidate = Capability::ALL
                    .iter()
                    .any(|capability| capability.families().contains(&sketch));
                if is_candidate && point.error_metric != accuracy_key(sketch).0 {
                    return Err(invalid(format!(
                        "{run}: {sketch} curve measured {}, but the optimizer reads {}; \
                         rerun the study",
                        point.error_metric,
                        accuracy_key(sketch).0
                    )));
                }
                run_points.insert(key(&row), (row["sketch"].clone(), point));
            }
            for row in read_csv(&run_dir.join("saturation_curve.csv"))? {
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
            points.extend(run_points);
        }
        let mut points_by_sketch: BTreeMap<String, Vec<GridPoint>> = BTreeMap::new();
        for (sketch, mut point) in points.into_values() {
            point.curve.sort_by(|a, b| a.0.total_cmp(&b.0));
            points_by_sketch.entry(sketch).or_default().push(point);
        }
        Ok(Self { points_by_sketch })
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
        if family_properties(&deployment.config.sketch).exact {
            return table_accuracy(raqe, deployment);
        }
        let metric_facts = &facts[&deployment.metric];
        let grouping = &deployment.grouping_labels;
        let shape = metric_facts.data_shape.get(grouping)?;
        let points = self.bracketing_points(&deployment.config, shape)?;
        let covered = items_per_group(metric_facts, grouping, raqe.lookback_ms);
        let (_, direction) = accuracy_key(&deployment.config.sketch);
        let mut worst = None;
        for point in points {
            let error = point.error_at(covered, direction)?;
            worst = Some(worst.map_or(error, |w| worse(w, error, direction)));
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

    /// Checks the cost table against the curves: a sketch row's
    /// `query_accuracy` is the curve of its config and
    /// `measured_at.data_shape()`, read at `measured_at.items_per_instance`.
    /// Rows of sketches the study didn't run (exact accumulators among them)
    /// are skipped.
    pub fn check_cost_table(&self, costs: &[AtomicCostEntry]) -> CostTableCheck {
        let mut check = CostTableCheck::default();
        for row in costs {
            let Some(points) = self.points_by_sketch.get(&row.sketch) else {
                continue;
            };
            let name = format!("{} {}", row.sketch, row.sketch_config["params"]);
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
            let Some(measured_at) = &row.measured_at else {
                check.unchecked.push(format!("{name}: no measured_at"));
                continue;
            };
            let shape = measured_at.data_shape();
            let Some(point) = same_config.find(|point| Some(point.shape) == shape) else {
                check
                    .unchecked
                    .push(format!("{name}: no grid point at {shape:?}"));
                continue;
            };
            let metric = &point.error_metric;
            let Some(&value) = row.query_accuracy.get(metric) else {
                check
                    .mismatched
                    .push(format!("{name}: the table has no {metric}"));
                continue;
            };
            let n = measured_at.items_per_instance as f64;
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
        check
    }

    /// Every grid point of `config` bracketing `shape` (Q8 of #156), or
    /// `None` when `config` isn't in the grid, `shape` lies outside it, or a
    /// bracketing point is missing.
    fn bracketing_points(
        &self,
        config: &AtomicCostEntry,
        shape: &DataShape,
    ) -> Option<Vec<&GridPoint>> {
        let params = config_params(config)?;
        let points: Vec<&GridPoint> = self
            .points_by_sketch
            .get(&config.sketch)?
            .iter()
            .filter(|point| point.params == params)
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

/// `{"params": {"rows": 3, "cols": 1024}}` → `{rows: 3, cols: 1024}`.
fn config_params(config: &AtomicCostEntry) -> Option<BTreeMap<String, f64>> {
    config.sketch_config["params"]
        .as_object()?
        .iter()
        .map(|(name, value)| Some((name.clone(), value.as_f64()?)))
        .collect()
}

/// `"rows=3 cols=1024"` → `{rows: 3, cols: 1024}`; the grid's config strings.
fn parse_config(config: &str) -> BTreeMap<String, f64> {
    config
        .split_whitespace()
        .filter_map(|pair| {
            let (name, value) = pair.split_once('=')?;
            Some((name.to_string(), value.parse().ok()?))
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

// ponytail: plain comma split; the study's CSVs quote nothing (configs hold
// spaces, never commas). Use the csv crate if that changes.
fn read_csv(path: &Path) -> io::Result<Vec<CsvRow>> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
    let mut lines = text.lines();
    let header: Vec<&str> = lines.next().unwrap_or_default().split(',').collect();
    Ok(lines
        .filter(|line| !line.is_empty())
        .map(|line| {
            header
                .iter()
                .zip(line.split(','))
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect()
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{facts, raqe, METRIC};
    use crate::Capability;

    const TOPK: &str = "cms-heap-topk-fastpath-vector2d";

    /// Top-k rows=3 cols=1024 at θ ∈ {1.0, 1.2}, K ∈ {1e3, 1e5}. Precision
    /// falls with N; θ = 1.0 is worse, and K = 1e5 never saturates.
    fn curves() -> SaturationCurves {
        let point = |theta: f64, keys: f64, offset: f64, n_sat: Option<f64>| GridPoint {
            params: parse_config("rows=3 cols=1024"),
            shape: MeasuredShape::Zipf { skew: theta, keys },
            error_metric: "precision_at_k".into(),
            n_sat,
            curve: vec![
                (1e3, 1.0 - offset, 0.0),
                (1e4, 0.95 - offset, 0.01),
                (1e5, 0.9 - offset, 0.0),
            ],
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
                merge_accuracy: BTreeMap::new(),
                measured_at: None,
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
    fn merged_answers_read_the_curve_at_the_lookback_count_whatever_the_window() {
        // 10 items/s: a 100 s window holds 1e3 items, below the 1e4
        // saturation point, but the 1e4 s lookback covers 1e5 items whatever
        // the window.
        let facts = workload(10, shape(1.2, 1e3));
        let r = topk_raqe(10_000_000);
        for window_ms in [100_000, 1_000_000, 10_000_000] {
            assert_eq!(
                curves().accuracy(&r, &deployment(TOPK, window_ms), &facts),
                Some(0.9)
            );
        }
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
            measured_at: Some(MeasuredAt {
                items_per_instance: items,
                keys_per_instance: Some(1_000),
                value_range: None,
                merge_operand_items: None,
                distribution: Some(DataDistribution::Zipf(ZipfParameter {
                    skewness: 1.2,
                    population_size: 1_000,
                    seed: 1,
                })),
            }),
            ..deployment(TOPK, 1).config
        }
    }

    #[test]
    fn the_cost_table_is_a_point_on_the_curve() {
        let check = |rows: &[AtomicCostEntry]| curves().check_cost_table(rows);
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
        pareto.measured_at.as_mut().unwrap().distribution = None;
        assert_eq!(check(&[pareto]).unchecked.len(), 1);
        // A table without the curve's metric is a mismatch.
        let mut other_metric = measured_row(1024, 10_000, 0.95);
        other_metric.query_accuracy = BTreeMap::from([("recall_at_k".into(), 0.95)]);
        assert_eq!(check(&[other_metric]).mismatched.len(), 1);
        // Sketches the study didn't run, exact ones among them, are skipped.
        let exact = AtomicCostEntry {
            sketch: "exact-sum".into(),
            ..measured_row(1024, 10_000, 0.0)
        };
        assert_eq!(check(&[exact]), CostTableCheck::default());
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
}
